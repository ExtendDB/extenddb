// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `Query` operation handler.

use std::collections::HashMap;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::PathElement;
use extenddb_core::expression::{
    ExpressionKind, ExpressionMaps, parse_key_condition, tokenize_for,
};
use extenddb_core::types::{
    IndexType, KeyType, QueryInput, QueryOutput, Select, TableKeyInfo, extract_key, item_size_bytes,
};

use crate::OperationContext;
use crate::capacity_helpers;
use crate::create_table::storage_err_to_dynamo;
use crate::expression_helpers::{build_expression_maps, parse_optional_filter};
use crate::index_helpers::{combined_lek_key_schema, validate_query_exclusive_start_key};
use crate::legacy_filter::{desugar_filter, desugar_key_conditions};
use crate::read_helpers::apply_post_read;
use crate::serialize_output;
use crate::{DispatchMetrics, DispatchResult};

/// Handle a `Query` request.
///
/// Parses `KeyConditionExpression`, queries the storage layer, applies
/// `FilterExpression` post-read, applies `ProjectionExpression`, and
/// enforces the 1 MB response size limit.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, missing tables, or storage errors.
#[allow(clippy::cast_possible_wrap)] // item counts won't exceed i64::MAX
pub async fn handle_query(
    body: Value,
    ctx: &OperationContext,
) -> Result<DispatchResult, DynamoDbError> {
    let input: QueryInput = serde_json::from_value(body).map_err(crate::deserialize_error)?;

    // P118: Fetch key_info first so we can use table_id for index lookup.
    let key_info = ctx
        .table_key_info(&input.table_name)
        .await
        .map_err(storage_err_to_dynamo)?;

    // GSI/LSI: resolve index metadata if querying a secondary index.
    // Uses table_id from pre-fetched key_info to skip redundant table lookup (P118 #4).
    let index_info = if let Some(ref idx_name) = input.index_name {
        Some(
            ctx.storage
                .index_info_by_table_id(&key_info.table_id, idx_name)
                .await
                .map_err(storage_err_to_dynamo)?,
        )
    } else {
        None
    };

    // ConsistentRead is not supported on GSI queries (tenet 1: fidelity).
    if input.consistent_read == Some(true)
        && let Some(ref idx) = index_info
        && idx.index_type == IndexType::Gsi
    {
        return Err(DynamoDbError::ValidationException(
            "Consistent reads are not supported on global secondary indexes".to_owned(),
        ));
    }

    // Validate Limit >= 1 (REQ-QUERY-001)
    if let Some(limit) = input.limit
        && limit < 1
    {
        return Err(DynamoDbError::ValidationException(
                "1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member must have value greater than or equal to 1".to_owned(),
            ));
    }

    // For index queries, build a key_info that reflects the index's key schema
    // so the storage layer uses the correct SK column for the index table.
    let query_key_info = if let Some(ref idx) = index_info {
        TableKeyInfo {
            table_name: key_info.table_name.clone(),
            account_id: key_info.account_id.clone(),
            table_id: key_info.table_id.clone(),
            key_schema: idx.key_schema.clone(),
            base_key_schema: key_info.key_schema.clone(),
            attribute_definitions: key_info.attribute_definitions.clone(),
            has_lsi: key_info.has_lsi,
            stream_specification: None, // Queries don't capture stream records
        }
    } else {
        key_info.clone()
    };

    // --- Legacy vs expression mutual exclusivity checks ---
    let has_kce = input.key_condition_expression.is_some();
    let has_kc = input.key_conditions.as_ref().is_some_and(|m| !m.is_empty());

    if has_kce && has_kc {
        return Err(DynamoDbError::ValidationException(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {KeyConditions} Expression parameters: {KeyConditionExpression}"
                .to_owned(),
        ));
    }

    let has_fe = input.filter_expression.is_some();
    let has_qf = input.query_filter.as_ref().is_some_and(|m| !m.is_empty());

    if has_fe && has_qf {
        return Err(DynamoDbError::ValidationException(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {QueryFilter} Expression parameters: {FilterExpression}"
                .to_owned(),
        ));
    }

    let has_pe = input.projection_expression.is_some();
    let has_atg = input
        .attributes_to_get
        .as_ref()
        .is_some_and(|a| !a.is_empty());

    if has_pe && has_atg {
        return Err(DynamoDbError::ValidationException(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}"
                .to_owned(),
        ));
    }

    // Build expression maps from request (used for expression-based parameters)
    let maps = build_expression_maps(
        input.expression_attribute_names.as_ref(),
        input.expression_attribute_values.as_ref(),
    );

    // Reject EAN/EAV supplied with no expression that references them. Legacy
    // KeyConditions does not count as an expression. Query emits no values suffix.
    let has_kce = input
        .key_condition_expression
        .as_ref()
        .is_some_and(|s| !s.is_empty());
    let has_filter_expr = input
        .filter_expression
        .as_ref()
        .is_some_and(|s| !s.is_empty());
    let has_proj_expr = input
        .projection_expression
        .as_ref()
        .is_some_and(|s| !s.is_empty());
    extenddb_core::expression::validate_expression_param_usage(
        input.expression_attribute_names.as_ref(),
        has_kce || has_filter_expr || has_proj_expr,
        input.expression_attribute_values.as_ref(),
        has_kce || has_filter_expr,
        &[],
    )?;

    // Parse KeyConditionExpression or desugar legacy KeyConditions
    let (mut key_condition, legacy_kc_maps) = if let Some(kce_str) =
        input.key_condition_expression.as_deref()
    {
        let parsed = tokenize_for(
            kce_str,
            ctx.limits.max_expression_tokens,
            ExpressionKind::KeyCondition,
        )
        .and_then(|tokens| {
            if ctx.limits.enforce_reserved_keywords {
                extenddb_core::expression::validate_no_reserved_words(&tokens)?;
            }
            parse_key_condition(&tokens)
        })
        .map_err(|e| {
            crate::expression_helpers::prefix_expression_error(e, ExpressionKind::KeyCondition)
        })?;
        (parsed, None)
    } else if let Some(ref kc) = input.key_conditions {
        let key_schema_pairs: Vec<(String, bool)> = query_key_info
            .key_schema
            .iter()
            .map(|ks| (ks.attribute_name.clone(), ks.key_type == KeyType::Hash))
            .collect();
        let (kc_parsed, kc_maps) = desugar_key_conditions(kc, &key_schema_pairs)?;
        (kc_parsed, Some(kc_maps))
    } else {
        return Err(DynamoDbError::ValidationException(
            "Either the KeyConditions or KeyConditionExpression parameter must be specified in the request."
                .to_owned(),
        ));
    };

    // Use legacy maps for key condition resolution if KeyConditions was used
    let effective_maps = if let Some(ref kc_maps) = legacy_kc_maps {
        kc_maps
    } else {
        &maps
    };

    // Correct PK/SK assignment when both clauses are equality comparisons.
    // The parser can't distinguish PK from SK without the key schema.
    let pk_attr = &query_key_info.key_schema[0].attribute_name;
    key_condition.resolve_pk_sk(pk_attr, &effective_maps.names)?;

    // Validate that the partition key is actually referenced in the condition.
    let pk_resolved = resolve_path_attr_name(&key_condition.pk_path, &effective_maps.names);
    if pk_resolved.as_deref() != Some(pk_attr.as_str()) {
        return Err(DynamoDbError::ValidationException(format!(
            "Query condition missed key schema element: {pk_attr}"
        )));
    }

    // For multi-part key schemas (GSIs with >1 HASH attribute), reclassify
    // the parsed conditions so all HASH attributes go to pk_path/extra_pk_conditions
    // and the RANGE condition stays as sk_condition.
    if extenddb_core::types::is_multipart_key_schema(&query_key_info.key_schema) {
        let hash_elements = extenddb_core::types::hash_key_elements(&query_key_info.key_schema);
        let hash_attrs: Vec<&str> = hash_elements
            .iter()
            .map(|ks| ks.attribute_name.as_str())
            .collect();
        key_condition.resolve_multipart(&hash_attrs, &effective_maps.names)?;

        // Validate all HASH attributes are present in the KeyConditionExpression.
        let provided_count = 1 + key_condition.extra_pk_conditions.len();
        if provided_count != hash_attrs.len() {
            // Find the first missing HASH attribute for the error message.
            let missing = hash_attrs
                .iter()
                .find(|attr| {
                    let pk_name =
                        resolve_path_attr_name(&key_condition.pk_path, &effective_maps.names);
                    if pk_name.as_deref() == Some(*attr) {
                        return false;
                    }
                    !key_condition.extra_pk_conditions.iter().any(|(path, _)| {
                        resolve_path_attr_name(path, &effective_maps.names).as_deref()
                            == Some(*attr)
                    })
                })
                .unwrap_or(&hash_attrs[0]);
            return Err(DynamoDbError::ValidationException(format!(
                "Query condition missed key schema element: {missing}"
            )));
        }
    }

    // Parse FilterExpression or desugar legacy QueryFilter
    let (filter, filter_maps) = if let Some(ref qf) = input.query_filter {
        if !qf.is_empty() {
            let cond_op = input.conditional_operator.unwrap_or_default();
            let (expr, fmaps) = desugar_filter(qf, cond_op)?;
            (Some(expr), Some(fmaps))
        } else {
            (
                parse_optional_filter(input.filter_expression.as_deref(), &ctx.limits)?,
                None,
            )
        }
    } else {
        (
            parse_optional_filter(input.filter_expression.as_deref(), &ctx.limits)?,
            None,
        )
    };

    // Validate #name references in filter are defined in ExpressionAttributeNames
    if let Some(ref filter_expr) = filter {
        let names = input.expression_attribute_names.as_ref();
        validate_name_refs_in_expr(filter_expr, names, ExpressionKind::Filter)?;
    }

    // Parse ProjectionExpression or desugar legacy AttributesToGet
    let (effective_projection_str, extra_proj_names) = if input.projection_expression.is_some() {
        (input.projection_expression.clone(), HashMap::new())
    } else if let Some(ref attrs) = input.attributes_to_get {
        let mut names_map = HashMap::new();
        let placeholders: Vec<String> = attrs
            .iter()
            .enumerate()
            .map(|(i, attr)| {
                let placeholder = format!("#_ag{i}");
                names_map.insert(placeholder.clone(), attr.clone());
                placeholder
            })
            .collect();
        (Some(placeholders.join(", ")), names_map)
    } else {
        (None, HashMap::new())
    };

    let projection = if let Some(ref proj_str) = effective_projection_str {
        Some(crate::expression_helpers::parse_projection_expr(
            proj_str,
            &ctx.limits,
        )?)
    } else {
        None
    };

    // Validate unused expression attributes
    {
        let exprs: Vec<&extenddb_core::expression::Expr> = filter.iter().collect();
        let (mut kc_names, kc_values) =
            extenddb_core::expression::collect_key_condition_refs(&key_condition);
        // Collect #name refs from projection paths
        if let Some(ref proj) = projection {
            for path in proj {
                for el in path {
                    if let PathElement::Attribute(name) = el
                        && let Some(ref_name) = name.strip_prefix('#')
                    {
                        kc_names.insert(ref_name.to_owned());
                    }
                }
            }
        }
        extenddb_core::expression::validate_unused_attributes(
            &maps.names,
            &maps.values,
            &exprs,
            &[],
            &kc_names,
            &kc_values,
        )?;
    }

    // Validate Select vs ProjectionExpression and index requirements
    if let Some(Select::SpecificAttributes) = input.select
        && effective_projection_str.is_none()
    {
        return Err(DynamoDbError::ValidationException(
                "1 validation error detected: Must specify the AttributesToGet or ProjectionExpression when choosing to get SPECIFIC_ATTRIBUTES".to_owned(),
            ));
    }
    if let Some(Select::AllProjectedAttributes) = input.select
        && index_info.is_none()
    {
        return Err(DynamoDbError::ValidationException(
            "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName".to_owned(),
        ));
    }
    if let Some(Select::Count) = input.select
        && effective_projection_str.is_some()
    {
        return Err(DynamoDbError::ValidationException(
            "Cannot specify the ProjectionExpression when Select is COUNT".to_owned(),
        ));
    }

    // When Select=ALL_PROJECTED_ATTRIBUTES, capture the index info for post-read filtering.
    let index_proj = if matches!(input.select, Some(Select::AllProjectedAttributes)) {
        index_info.as_ref()
    } else {
        None
    };

    // Build the combined expression maps used for storage query and post-read evaluation.
    // Merges the base request maps with any legacy desugared maps and projection name maps.
    let combined_maps = {
        let mut names = maps.names.clone();
        let mut values = maps.values.clone();

        if let Some(ref kc_maps) = legacy_kc_maps {
            values.extend(kc_maps.values.clone());
        }
        if let Some(ref fm) = filter_maps {
            values.extend(fm.values.clone());
        }
        if !extra_proj_names.is_empty() {
            for (k, v) in &extra_proj_names {
                let stripped = k.strip_prefix('#').unwrap_or(k);
                names.insert(stripped.to_owned(), v.clone());
            }
        }
        ExpressionMaps::new(names, values)
    };

    // Validate begins_with operand types upfront (before any rows are read).
    if let Some(ref f) = filter {
        extenddb_core::expression::validate_begins_with_operands(f, &combined_maps).map_err(
            |e| crate::expression_helpers::prefix_expression_error(e, ExpressionKind::Filter),
        )?;
    }

    // Validate ExclusiveStartKey matches the key schema
    if let Some(ref start_key) = input.exclusive_start_key {
        validate_query_exclusive_start_key(start_key, &key_info, index_info.as_ref())?;
    }

    // Query storage
    let (raw_items, storage_last_key) = ctx
        .storage
        .query(
            &query_key_info,
            &key_condition,
            &combined_maps,
            input.scan_index_forward,
            input.limit,
            input.exclusive_start_key.as_ref(),
            input.index_name.as_deref(),
        )
        .await
        .map_err(storage_err_to_dynamo)?;

    // Capacity metering: RCU based on total pre-projection size of all scanned items.
    let pre_projection_bytes: usize = raw_items.iter().map(item_size_bytes).sum();
    let strongly_consistent = input.consistent_read == Some(true);
    let rcu = capacity_helpers::read_capacity_units(pre_projection_bytes, strongly_consistent);

    // Determine which key schema to use for LastEvaluatedKey extraction.
    // For index queries, the LEK includes both the index key and the base table key.
    let lek_key_schema = combined_lek_key_schema(&key_info.key_schema, index_info.as_ref());

    // For index queries, enrich the storage LEK with base table key attributes.
    let enriched_storage_last_key = if storage_last_key.is_some() && index_info.is_some() {
        raw_items
            .last()
            .map(|item| extract_key(item, &lek_key_schema))
    } else {
        storage_last_key
    };

    // Apply FilterExpression, ProjectionExpression, and 1 MB limit
    let result = apply_post_read(
        &raw_items,
        enriched_storage_last_key,
        &filter,
        &projection,
        &combined_maps,
        &lek_key_schema,
        input.select.as_ref(),
        index_proj,
        &key_info.key_schema,
    )?;

    let output = QueryOutput {
        items: result.items,
        count: result.count,
        scanned_count: result.scanned_count,
        last_evaluated_key: result.last_evaluated_key,
        consumed_capacity: capacity_helpers::read_capacity(
            input.return_consumed_capacity,
            &input.table_name,
            rcu,
        ),
    };

    let body = serialize_output(&output)?;
    #[allow(clippy::cast_sign_loss)] // count is non-negative
    Ok(DispatchResult {
        body,
        metrics: DispatchMetrics {
            read_capacity_units: rcu,
            returned_item_count: result.count as u64,
            returned_bytes: pre_projection_bytes as u64,
            index_name: input.index_name,
            ..Default::default()
        },
    })
}

/// Resolve a path's top-level attribute name, handling `#name` references.
/// Returns `None` if the path is empty or the name reference is unresolved.
fn resolve_path_attr_name(
    path: &[PathElement],
    names: &std::collections::HashMap<String, String>,
) -> Option<String> {
    match path.first() {
        Some(PathElement::Attribute(name)) => {
            if let Some(ref_name) = name.strip_prefix('#') {
                names.get(ref_name).cloned()
            } else {
                Some(name.clone())
            }
        }
        _ => None,
    }
}

/// Validate that all `#name` references in an expression AST are defined.
fn validate_name_refs_in_expr(
    expr: &extenddb_core::expression::Expr,
    names: Option<&HashMap<String, String>>,
    expr_type: ExpressionKind,
) -> Result<(), DynamoDbError> {
    use extenddb_core::expression::Expr;
    match expr {
        Expr::Path(elements) => {
            for el in elements {
                if let PathElement::Attribute(name) = el
                    && let Some(ref_name) = name.strip_prefix('#')
                {
                    let key_with_hash = format!("#{ref_name}");
                    let defined = names.as_ref().is_some_and(|m| {
                        m.contains_key(ref_name) || m.contains_key(key_with_hash.as_str())
                    });
                    if !defined {
                        return Err(DynamoDbError::ValidationException(format!(
                            "Invalid {expr_type}: An expression attribute name used in the document path is not defined; attribute name: #{ref_name}"
                        )));
                    }
                }
            }
            Ok(())
        }
        Expr::Compare { left, right, .. } => {
            validate_name_refs_in_expr(left, names, expr_type)?;
            validate_name_refs_in_expr(right, names, expr_type)
        }
        Expr::And(l, r) | Expr::Or(l, r) => {
            validate_name_refs_in_expr(l, names, expr_type)?;
            validate_name_refs_in_expr(r, names, expr_type)
        }
        Expr::Not(inner) => validate_name_refs_in_expr(inner, names, expr_type),
        Expr::Between { operand, low, high } => {
            validate_name_refs_in_expr(operand, names, expr_type)?;
            validate_name_refs_in_expr(low, names, expr_type)?;
            validate_name_refs_in_expr(high, names, expr_type)
        }
        Expr::In { operand, list } => {
            validate_name_refs_in_expr(operand, names, expr_type)?;
            for item in list {
                validate_name_refs_in_expr(item, names, expr_type)?;
            }
            Ok(())
        }
        Expr::Function { args, .. } => {
            for arg in args {
                validate_name_refs_in_expr(arg, names, expr_type)?;
            }
            Ok(())
        }
        Expr::Arithmetic { left, right, .. } => {
            validate_name_refs_in_expr(left, names, expr_type)?;
            validate_name_refs_in_expr(right, names, expr_type)
        }
        Expr::Placeholder(_) => Ok(()),
    }
}
