// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `UpdateItem` operation handler.
//!
//! REQ-DATA-003: `UpdateItem` with SET and REMOVE actions.
//! REQ-DATA-004: `ReturnValues` (`NONE`, `ALL_OLD`, `UPDATED_OLD`, `ALL_NEW`, `UPDATED_NEW`).

use std::collections::HashMap;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{ExpressionKind, ExpressionMaps, PathElement, UpdateAction};
use extenddb_core::types::{
    AttributeValue, Item, ReturnValues, TableKeyInfo, UpdateItemInput, UpdateItemOutput,
    item_size_bytes,
};

use crate::OperationContext;
use crate::capacity_helpers;
use crate::create_table::{storage_err_to_dynamo, storage_err_to_dynamo_with_ccf};
use crate::expression_helpers::resolve_condition;
use crate::serialize_output;
use crate::stream_capture;
use crate::{DispatchMetrics, DispatchResult};

/// Handle an `UpdateItem` request.
///
/// Validates the input, applies the update expression, and returns the
/// appropriate item snapshot based on `ReturnValues`.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, missing tables, or storage errors.
pub async fn handle_update_item(
    body: Value,
    ctx: &OperationContext,
) -> Result<DispatchResult, DynamoDbError> {
    // Sequential rather than aggregated, deliberately: UpdateItem stops at the
    // FIRST invalid enum and reports one error, where PutItem and DeleteItem
    // aggregate all of theirs (behaviour change the ground truth captured
    // 2026-06-08; re-confirmed by the 2026-08-24 runs in both regions).
    crate::validate_enum_fields(
        &body,
        &[(
            "ReturnValues",
            "returnValues",
            &["NONE", "ALL_OLD", "ALL_NEW", "UPDATED_OLD", "UPDATED_NEW"],
        )],
    )?;
    crate::validate_enum_fields(
        &body,
        &[(
            "ReturnConsumedCapacity",
            "returnConsumedCapacity",
            &["INDEXES", "TOTAL", "NONE"],
        )],
    )?;
    let input: UpdateItemInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

    extenddb_core::validation::validate_table_name(&input.table_name, &ctx.limits)?;

    // Validate Update/Condition expressions (and AttributeUpdates
    // desugaring) before the existence check; key, item-content nesting, and
    // the no-key-update checks stay after (post-existence on Amazon DynamoDB).

    let has_update_expr = input
        .update_expression
        .as_ref()
        .is_some_and(|s| !s.is_empty());
    let has_condition = input
        .condition_expression
        .as_ref()
        .is_some_and(|s| !s.is_empty());
    let has_expected = input.expected.as_ref().is_some_and(|m| !m.is_empty());
    let has_cond_op = input.conditional_operator.is_some();

    extenddb_core::validation::validate_no_expression_param_mixing(
        &[
            ("AttributeUpdates", input.attribute_updates.is_some()),
            ("Expected", has_expected),
            ("ConditionalOperator", has_cond_op),
        ],
        &[
            ("UpdateExpression", input.update_expression.is_some()),
            ("ConditionExpression", has_condition),
        ],
    )?;

    // Desugar legacy AttributeUpdates into UpdateExpression if present.
    let (effective_update_expr, extra_expr_values, extra_expr_names) =
        if let Some(attr_updates) = &input.attribute_updates {
            desugar_attribute_updates(attr_updates)?
        } else {
            (
                input.update_expression.clone(),
                HashMap::new(),
                HashMap::new(),
            )
        };

    // Merge extra expression values from desugaring with any existing ones.
    let effective_expr_values = if extra_expr_values.is_empty() {
        input.expression_attribute_values.clone()
    } else {
        let mut merged = input
            .expression_attribute_values
            .clone()
            .unwrap_or_default();
        merged.extend(extra_expr_values);
        Some(merged)
    };

    // Merge extra expression attribute names from desugaring with any existing ones.
    let effective_expr_names = if extra_expr_names.is_empty() {
        input.expression_attribute_names.clone()
    } else {
        let mut merged = input.expression_attribute_names.clone().unwrap_or_default();
        merged.extend(extra_expr_names);
        Some(merged)
    };

    let has_expr = has_update_expr || has_condition;
    extenddb_core::expression::validate_expression_param_usage(
        input.expression_attribute_names.as_ref(),
        has_expr,
        input.expression_attribute_values.as_ref(),
        has_expr,
        &[ExpressionKind::Update, ExpressionKind::Condition],
    )?;

    // ConditionalOperator requires an Expected with two or more conditions.
    extenddb_core::validation::validate_conditional_operator_usage(
        has_cond_op,
        input.expected.as_ref().map_or(0, HashMap::len),
    )?;

    let (condition, maps) = resolve_condition(
        input.condition_expression.as_deref(),
        effective_expr_names.as_ref(),
        effective_expr_values.as_ref(),
        input.expected.as_ref(),
        input.conditional_operator,
        &ctx.limits,
    )?;

    // No UpdateExpression and no AttributeUpdates: no-op upsert.
    // Some("") still errors via parse_update_expr.
    let actions = if let Some(update_expr) = effective_update_expr.as_deref() {
        crate::expression_helpers::parse_update_expr(update_expr, &ctx.limits)?
    } else {
        Vec::new()
    };

    // Reject an undefined #name before the existence check
    // (otherwise it is only raised later, during execution).
    for action in &actions {
        let path = match action {
            UpdateAction::Set { path, .. }
            | UpdateAction::Remove { path }
            | UpdateAction::Add { path, .. }
            | UpdateAction::Delete { path, .. } => path,
        };
        for el in path {
            if let PathElement::Attribute(name) = el
                && let Some(ref_name) = name.strip_prefix('#')
            {
                maps.resolve_name(ref_name)?;
            }
        }
    }

    if input.expected.is_none()
        || input
            .expected
            .as_ref()
            .is_some_and(std::collections::HashMap::is_empty)
    {
        let exprs: Vec<&extenddb_core::expression::Expr> = condition.iter().collect();
        extenddb_core::expression::validate_unused_attributes(
            &maps.names,
            &maps.values,
            &exprs,
            &actions.iter().collect::<Vec<_>>(),
            &std::collections::HashSet::new(),
            &std::collections::HashSet::new(),
        )?;
    }

    let key_info = ctx
        .table_key_info(&input.table_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    extenddb_core::validation::validate_update_item(
        &input,
        &ctx.limits,
        &key_info.key_schema,
        &key_info.attribute_definitions,
    )?;

    // Vector and search-schema attributes are deliberately NOT validated here.
    // Their validity is a property of the stored value, and the stored value
    // does not exist until the actions have been evaluated against the
    // pre-update image, which only the storage layer holds. Validating
    // expression forms here instead would silently miss every RHS that is not
    // a bare placeholder (`list_append`, `if_not_exists`, attribute copies, and
    // anything added later). The authoritative check is
    // `expression::apply_update_validated`, which every backend applies to the
    // evaluated image and which no expression form can bypass.

    // Amazon DynamoDB enforces the nesting-depth limit on EVERY
    // ExpressionAttributeValue, condition-only placeholders included: a
    // 32-level value referenced solely by a ConditionExpression answers the
    // nesting ValidationException rather than evaluating the condition.
    // An earlier measurement had condition-only values exempt, and some
    // regions did behave that way; re-measured 2026-08-24, both us-east-1 and
    // eu-west-2 apply the limit to the whole map, so the per-placeholder walk
    // this replaced is gone. A 31-level value still passes and the condition
    // evaluates normally.
    if let Some(values) = &input.expression_attribute_values {
        extenddb_core::validation::validate_attribute_values_nesting_depth(values.values())?;
    }

    // Validate that no update action targets a key attribute (REQ-DATA-003)
    validate_no_key_updates(&actions, &key_info, &maps)?;

    let return_old = matches!(
        input.return_values,
        ReturnValues::AllOld | ReturnValues::UpdatedOld
    );

    let view_type = stream_capture::stream_view_type(&key_info);
    let stream = view_type.map(|vt| extenddb_storage::StreamCapture {
        view_type: vt,
        user_identity: None,
        region: ctx.region.clone(),
    });
    let need_old_for_stream = stream.is_some();
    let need_old_for_capacity =
        input.return_consumed_capacity != extenddb_core::types::ReturnConsumedCapacity::None;

    let (old_item, new_item) = ctx
        .storage
        .update_item(
            &key_info,
            &input.key,
            &actions,
            return_old || need_old_for_stream || need_old_for_capacity,
            true, // always fetch new item for WCU calculation
            condition.as_ref(),
            &maps,
            stream.as_ref(),
        )
        .await
        .map_err(|e| {
            storage_err_to_dynamo_with_ccf(e, input.return_values_on_condition_check_failure)
        })?;

    // Stream records are now captured atomically within the storage transaction.

    // Capacity metering: WCU based on the larger of old/new item size.
    let old_bytes = old_item.as_ref().map_or(0, item_size_bytes);
    let new_bytes = new_item.as_ref().map_or(0, item_size_bytes);
    let wcu = capacity_helpers::write_capacity_units(old_bytes.max(new_bytes));

    // Index capacity follows the old-to-new transition, so removing a sparse
    // index key charges the deleted projection and changing an index key charges
    // both the old deletion and new insertion. Metadata comes from cached
    // `TableKeyInfo`; no describe-table round-trip is needed.
    let consumed_capacity = capacity_helpers::write_capacity_indexed(
        input.return_consumed_capacity,
        &input.table_name,
        wcu,
        old_item.as_ref(),
        new_item.as_ref(),
        false,
        &key_info,
    );

    // Select the appropriate return value.
    // UPDATED_OLD and UPDATED_NEW return only the attributes that were
    // targeted by the update expression (top-level attribute of each action path).
    let attributes = match input.return_values {
        ReturnValues::None => None,
        ReturnValues::AllOld => old_item,
        ReturnValues::AllNew => new_item,
        ReturnValues::UpdatedOld => old_item
            .map(|item| filter_to_updated_attrs(&item, &actions, &maps))
            .filter(|item| !item.is_empty()),
        ReturnValues::UpdatedNew => new_item
            .map(|item| filter_to_updated_attrs(&item, &actions, &maps))
            .filter(|item| !item.is_empty()),
    };

    let output = UpdateItemOutput {
        attributes,
        consumed_capacity,
        item_collection_metrics: capacity_helpers::item_metrics(
            input.return_item_collection_metrics,
            &key_info.key_schema,
            &input.key,
            key_info.has_lsi,
        ),
    };
    let body = serialize_output(&output)?;
    Ok(DispatchResult {
        body,
        metrics: DispatchMetrics {
            write_capacity_units: wcu,
            ..Default::default()
        },
    })
}

/// Validate that no update action targets a key attribute.
///
/// `DynamoDB` returns `ValidationException` if an `UpdateExpression` attempts
/// to SET or REMOVE a key attribute.
fn validate_no_key_updates(
    actions: &[UpdateAction],
    key_info: &TableKeyInfo,
    maps: &ExpressionMaps,
) -> Result<(), DynamoDbError> {
    for action in actions {
        let path = match action {
            UpdateAction::Set { path, .. }
            | UpdateAction::Remove { path }
            | UpdateAction::Add { path, .. }
            | UpdateAction::Delete { path, .. } => path,
        };
        if let Some(PathElement::Attribute(name)) = path.first() {
            let resolved = if let Some(ref_name) = name.strip_prefix('#') {
                maps.resolve_name(ref_name)?
            } else {
                name.as_str()
            };
            for ks in &key_info.key_schema {
                if ks.attribute_name == resolved {
                    return Err(DynamoDbError::ValidationException(format!(
                        "One or more parameter values were invalid: Cannot update attribute {}. \
                         This attribute is part of the key",
                        ks.attribute_name
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Filter an item to only the top-level attributes targeted by update actions.
///
/// `UPDATED_OLD` and `UPDATED_NEW` return only the attributes that were
/// modified by the update expression. The top-level attribute name is
/// extracted from each action's path (resolving `#name` references).
fn filter_to_updated_attrs(item: &Item, actions: &[UpdateAction], maps: &ExpressionMaps) -> Item {
    let mut result = Item::new();
    for action in actions {
        let path = match action {
            UpdateAction::Set { path, .. }
            | UpdateAction::Remove { path }
            | UpdateAction::Add { path, .. }
            | UpdateAction::Delete { path, .. } => path,
        };
        if path.is_empty() {
            continue;
        }
        // Resolve the top-level attribute name
        let top_name = match &path[0] {
            PathElement::Attribute(name) => {
                let resolved = name
                    .strip_prefix('#')
                    .and_then(|r| maps.names.get(r).map(String::as_str))
                    .unwrap_or(name.as_str());
                resolved.to_owned()
            }
            _ => continue,
        };

        // Top-level path (len == 1): return the whole attribute
        if path.len() == 1 {
            if let Some(val) = item.get(&top_name) {
                result.insert(top_name, val.clone());
            }
            continue;
        }

        // Nested path: extract only the leaf value and wrap in path structure
        let Some(top_val) = item.get(&top_name) else {
            continue;
        };
        if let Some(leaf) = resolve_path_value(top_val, &path[1..], maps) {
            let wrapped = wrap_leaf_in_path(&path[1..], &leaf, maps);
            // Merge into existing top-level entry if multiple subpaths target
            // the same attribute (e.g., SET a.b = :x, a.c = :y).
            if let Some(existing) = result.get(&top_name) {
                if let Some(merged) = merge_maps(existing, &wrapped) {
                    result.insert(top_name, merged);
                } else {
                    result.insert(top_name, wrapped);
                }
            } else {
                result.insert(top_name, wrapped);
            }
        }
    }
    result
}

/// Resolve a value at a nested path within an `AttributeValue`.
fn resolve_path_value(
    val: &AttributeValue,
    path: &[PathElement],
    maps: &ExpressionMaps,
) -> Option<AttributeValue> {
    if path.is_empty() {
        return Some(val.clone());
    }
    match (&path[0], val) {
        (PathElement::Attribute(name), AttributeValue::M(map)) => {
            let resolved = name
                .strip_prefix('#')
                .and_then(|r| maps.names.get(r).map(String::as_str))
                .unwrap_or(name.as_str());
            map.get(resolved)
                .and_then(|v| resolve_path_value(v, &path[1..], maps))
        }
        (PathElement::Index(idx), AttributeValue::L(list)) => list
            .get(*idx)
            .and_then(|v| resolve_path_value(v, &path[1..], maps)),
        _ => None,
    }
}

/// Wrap a leaf value in the path structure (building from inside out).
fn wrap_leaf_in_path(
    path: &[PathElement],
    leaf: &AttributeValue,
    maps: &ExpressionMaps,
) -> AttributeValue {
    if path.is_empty() {
        return leaf.clone();
    }
    let inner = wrap_leaf_in_path(&path[1..], leaf, maps);
    match &path[0] {
        PathElement::Attribute(name) => {
            let resolved = name
                .strip_prefix('#')
                .and_then(|r| maps.names.get(r).map(String::as_str))
                .unwrap_or(name.as_str());
            let mut map = std::collections::BTreeMap::new();
            map.insert(resolved.to_owned(), inner);
            AttributeValue::M(map)
        }
        PathElement::Index(_) => AttributeValue::L(vec![inner]),
    }
}

/// Recursively merge two Map `AttributeValues`. Returns None if either isn't a Map.
fn merge_maps(a: &AttributeValue, b: &AttributeValue) -> Option<AttributeValue> {
    match (a, b) {
        (AttributeValue::M(ma), AttributeValue::M(mb)) => {
            let mut merged = ma.clone();
            for (k, v) in mb {
                if let Some(existing) = merged.get(k) {
                    if let Some(deep) = merge_maps(existing, v) {
                        merged.insert(k.clone(), deep);
                    } else {
                        merged.insert(k.clone(), v.clone());
                    }
                } else {
                    merged.insert(k.clone(), v.clone());
                }
            }
            Some(AttributeValue::M(merged))
        }
        _ => None,
    }
}

/// Desugared legacy `AttributeUpdates`: (expression, values, names).
type DesugarResult = (
    Option<String>,
    HashMap<String, AttributeValue>,
    HashMap<String, String>,
);

/// Desugar legacy `AttributeUpdates` into an `UpdateExpression` string and
/// corresponding `ExpressionAttributeValues` and `ExpressionAttributeNames`.
///
/// The legacy API uses `AttributeValueUpdate` objects with `Action` (PUT, DELETE, ADD)
/// and an optional `Value`. This converts them to the modern expression syntax:
/// - `PUT` → `SET #attr = :val`
/// - `DELETE` with value (set type) → `DELETE #attr :val`
/// - `DELETE` without value → `REMOVE #attr`
/// - `ADD` → `ADD #attr :val`
fn desugar_attribute_updates(
    updates: &HashMap<String, extenddb_core::types::AttributeValueUpdate>,
) -> Result<DesugarResult, DynamoDbError> {
    if updates.is_empty() {
        return Ok((None, HashMap::new(), HashMap::new()));
    }

    let mut set_clauses = Vec::new();
    let mut remove_clauses = Vec::new();
    let mut add_clauses = Vec::new();
    let mut delete_clauses = Vec::new();
    let mut expr_values = HashMap::new();
    let mut expr_names = HashMap::new();

    for (idx, (attr_name, update)) in updates.iter().enumerate() {
        let action = update.action.to_uppercase();
        let val_placeholder = format!(":_au{idx}");
        // Use name placeholders to avoid reserved-word collisions.
        let name_placeholder = format!("#_an{idx}");
        expr_names.insert(name_placeholder.clone(), attr_name.clone());

        match action.as_str() {
            "PUT" => {
                let value = update.value.clone().ok_or_else(|| {
                    DynamoDbError::ValidationException(format!(
                        "One or more parameter values were invalid: Value must be specified for PUT action on attribute {attr_name}"
                    ))
                })?;
                set_clauses.push(format!("{name_placeholder} = {val_placeholder}"));
                expr_values.insert(val_placeholder, value);
            }
            "DELETE" => {
                if let Some(value) = &update.value {
                    // DELETE with value removes elements from a set.
                    delete_clauses.push(format!("{name_placeholder} {val_placeholder}"));
                    expr_values.insert(val_placeholder, value.clone());
                } else {
                    // DELETE without value removes the attribute entirely.
                    remove_clauses.push(name_placeholder);
                }
            }
            "ADD" => {
                let value = update.value.clone().ok_or_else(|| {
                    DynamoDbError::ValidationException(format!(
                        "One or more parameter values were invalid: Value must be specified for ADD action on attribute {attr_name}"
                    ))
                })?;
                add_clauses.push(format!("{name_placeholder} {val_placeholder}"));
                expr_values.insert(val_placeholder, value);
            }
            other => {
                return Err(DynamoDbError::ValidationException(format!(
                    "1 validation error detected: Value '{other}' at 'attributeUpdates.{attr_name}.member.action' \
                     failed to satisfy constraint: Member must satisfy enum value set: [ADD, PUT, DELETE]"
                )));
            }
        }
    }

    let mut parts = Vec::new();
    if !set_clauses.is_empty() {
        parts.push(format!("SET {}", set_clauses.join(", ")));
    }
    if !remove_clauses.is_empty() {
        parts.push(format!("REMOVE {}", remove_clauses.join(", ")));
    }
    if !add_clauses.is_empty() {
        parts.push(format!("ADD {}", add_clauses.join(", ")));
    }
    if !delete_clauses.is_empty() {
        parts.push(format!("DELETE {}", delete_clauses.join(", ")));
    }

    let expr = parts.join(" ");
    Ok((Some(expr), expr_values, expr_names))
}
