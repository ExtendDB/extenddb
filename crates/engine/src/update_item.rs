// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `UpdateItem` operation handler.
//!
//! REQ-DATA-003: `UpdateItem` with SET and REMOVE actions.
//! REQ-DATA-004: `ReturnValues` (`NONE`, `ALL_OLD`, `UPDATED_OLD`, `ALL_NEW`, `UPDATED_NEW`).

use std::collections::HashMap;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{
    ExpressionMaps, PathElement, UpdateAction, parse_update_from, tokenize_for,
    validate_no_reserved_words,
};
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
    crate::validate_enum_fields(
        &body,
        &[
            (
                "ReturnValues",
                "returnValues",
                &["NONE", "ALL_OLD", "ALL_NEW", "UPDATED_OLD", "UPDATED_NEW"],
            ),
            (
                "ReturnConsumedCapacity",
                "returnConsumedCapacity",
                &["INDEXES", "TOTAL", "NONE"],
            ),
        ],
    )?;
    let input: UpdateItemInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

    extenddb_core::validation::validate_table_name(&input.table_name, &ctx.limits)?;

    let key_info = ctx
        .table_key_info(&input.table_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    // Desugar legacy AttributeUpdates into UpdateExpression if present.
    // N.B. The literal `{AttributeUpdates}` / `{UpdateExpression}` in the error message
    // matches real DynamoDB's format — they are not Rust format placeholders.
    let (effective_update_expr, extra_expr_values, extra_expr_names) = if let Some(attr_updates) =
        &input.attribute_updates
    {
        if input.update_expression.is_some() {
            return Err(DynamoDbError::ValidationException(
                "Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {AttributeUpdates} Expression parameters: {UpdateExpression}".to_owned(),
            ));
        }
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

    extenddb_core::validation::validate_update_item(
        &input,
        &ctx.limits,
        &key_info.key_schema,
        &key_info.attribute_definitions,
    )?;

    let (condition, maps) = resolve_condition(
        input.condition_expression.as_deref(),
        effective_expr_names.as_ref(),
        effective_expr_values.as_ref(),
        input.expected.as_ref(),
        input.conditional_operator,
        &ctx.limits,
    )?;

    // Parse the update expression
    let update_expr = effective_update_expr.as_deref().unwrap_or("");
    let update_tokens = tokenize_for(
        update_expr,
        ctx.limits.max_expression_tokens,
        "UpdateExpression",
    )?;
    if ctx.limits.enforce_reserved_keywords {
        validate_no_reserved_words(&update_tokens)?;
    }
    let actions = parse_update_from(&update_tokens, update_expr)?;

    if input.expected.is_none() || input.expected.as_ref().is_some_and(|m| m.is_empty()) {
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

    let (old_item, new_item) = ctx
        .storage
        .update_item(
            &key_info,
            &input.key,
            &actions,
            return_old || need_old_for_stream,
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

    // Select the appropriate return value.
    // UPDATED_OLD and UPDATED_NEW return only the attributes that were
    // targeted by the update expression (top-level attribute of each action path).
    let attributes = match input.return_values {
        ReturnValues::None => None,
        ReturnValues::AllOld => old_item,
        ReturnValues::AllNew => new_item,
        ReturnValues::UpdatedOld => {
            old_item.map(|item| filter_to_updated_attrs(&item, &actions, &maps))
        }
        ReturnValues::UpdatedNew => {
            new_item.map(|item| filter_to_updated_attrs(&item, &actions, &maps))
        }
    };

    let output = UpdateItemOutput {
        attributes,
        consumed_capacity: capacity_helpers::write_capacity(
            input.return_consumed_capacity,
            &input.table_name,
            wcu,
        ),
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
        if let Some(PathElement::Attribute(name)) = path.first() {
            let resolved = name
                .strip_prefix('#')
                .and_then(|r| maps.names.get(r).map(String::as_str))
                .unwrap_or(name.as_str());
            if let Some(val) = item.get(resolved) {
                result.insert(resolved.to_owned(), val.clone());
            }
        }
    }
    result
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
