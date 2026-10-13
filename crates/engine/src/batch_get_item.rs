// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BatchGetItem` operation handler.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::Projection;
use extenddb_core::types::{BatchGetItemInput, BatchGetItemOutput, Item, item_size_bytes};
use extenddb_core::validation::{validate_batch_key_only, validate_key_sizes};

use crate::OperationContext;
use crate::capacity_helpers;
use crate::create_table::storage_err_to_dynamo;
use crate::expression_helpers::build_expression_maps;
use crate::serialize_output;
use crate::{DispatchMetrics, DispatchResult};

/// Maximum number of keys across all tables in a single `BatchGetItem` request.
const MAX_BATCH_GET_KEYS: usize = 100;

/// Handle a `BatchGetItem` request.
///
/// Reads items from one or more tables by primary key. Each table's keys are
/// fetched individually via `get_item`. `DynamoDB` limits: max 100 keys total,
/// max 16 MB response size.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, missing tables, or storage errors.
pub async fn handle_batch_get_item(
    body: Value,
    ctx: &OperationContext,
) -> Result<DispatchResult, DynamoDbError> {
    let input: BatchGetItemInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    // Validate: RequestItems must not be empty. The message is the request
    // model's length constraint (measured; note "Value at", with no value
    // echoed). BatchWriteItem keeps its distinct required-parameter sentence,
    // which is likewise measured.
    if input.request_items.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "1 validation error detected: Value at 'RequestItems' failed to satisfy \
             constraint: Member must have length greater than or equal to 1"
                .to_owned(),
        ));
    }

    // Validate: per-table keys <= 100
    for (table_name, ka) in &input.request_items {
        if ka.keys.len() > MAX_BATCH_GET_KEYS {
            return Err(DynamoDbError::ValidationException(format!(
                "1 validation error detected: Value at 'RequestItems.{table_name}.member.Keys' failed to satisfy constraint: \
                 Member must have length less than or equal to 100"
            )));
        }
    }

    // Validate: total keys across all tables <= 100
    let total_keys: usize = input.request_items.values().map(|ka| ka.keys.len()).sum();
    if total_keys > MAX_BATCH_GET_KEYS {
        return Err(DynamoDbError::ValidationException(
            "Too many items requested for the BatchGetItem call".to_owned(),
        ));
    }

    // Validate: each table must have at least one key
    for (table_name, ka) in &input.request_items {
        if ka.keys.is_empty() {
            return Err(DynamoDbError::ValidationException(format!(
                "1 validation error detected: Value '[]' at 'requestItems.{table_name}.member.keys' failed to satisfy constraint: Member must have length greater than or equal to 1"
            )));
        }
    }

    // Reject mixing expression and non-expression parameters anywhere in the
    // request. The check is request-wide: ProjectionExpression on one table and
    // AttributesToGet on another is still a mix, matching Amazon DynamoDB.
    let has_projection_expr = input
        .request_items
        .values()
        .any(|ka| ka.projection_expression.is_some());
    let has_attributes_to_get = input
        .request_items
        .values()
        .any(|ka| ka.attributes_to_get.as_ref().is_some_and(|a| !a.is_empty()));
    if has_projection_expr && has_attributes_to_get {
        return Err(DynamoDbError::ValidationException(
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {AttributesToGet} Expression parameters: {ProjectionExpression}"
                .to_owned(),
        ));
    }

    // Validate every table's projection and duplicate keys before
    // any existence lookup (Amazon DynamoDB validates the whole request first).
    // Compiled projections are reused below; per-key schema check stays in the loop.
    let mut table_projections: HashMap<String, Option<Projection>> = HashMap::new();
    for (table_name, ka) in &input.request_items {
        extenddb_core::expression::validate_expression_param_usage(
            ka.expression_attribute_names.as_ref(),
            ka.projection_expression
                .as_ref()
                .is_some_and(|s| !s.is_empty()),
            None,
            true,
            &[],
        )?;

        let (effective_proj_str, extra_proj_names) = if ka.projection_expression.is_some() {
            (ka.projection_expression.clone(), HashMap::new())
        } else if let Some(attrs) = &ka.attributes_to_get {
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

        let parsed_projection = if let Some(ref proj_str) = effective_proj_str {
            Some(crate::expression_helpers::parse_projection_expr(
                proj_str,
                &ctx.limits,
            )?)
        } else {
            None
        };
        let maps = if extra_proj_names.is_empty() {
            build_expression_maps(ka.expression_attribute_names.as_ref(), None)
        } else {
            // Merge extra names with any user-provided names.
            let mut merged = ka.expression_attribute_names.clone().unwrap_or_default();
            merged.extend(extra_proj_names);
            build_expression_maps(Some(&merged), None)
        };
        // Compile once per table: resolves #names and rejects overlapping
        // paths. DynamoDB checks overlap before the unused-name check, so this
        // runs first. Overlap rejection is scoped to a user-supplied expression.
        let projection = match parsed_projection {
            Some(ref paths) => Some(Projection::compile(
                paths,
                &maps,
                ka.projection_expression.is_some(),
            )?),
            None => None,
        };
        // Then reject ExpressionAttributeNames entries the projection never
        // references, matching Amazon DynamoDB. Scoped to a user-supplied
        // ProjectionExpression.
        if ka.projection_expression.is_some()
            && let Some(ref paths) = parsed_projection
        {
            crate::read_helpers::validate_projection_unused_names(
                ka.expression_attribute_names.as_ref(),
                paths,
            )?;
        }

        // Reject duplicate keys (pre-existence on Amazon DynamoDB).
        let mut seen_keys: HashSet<Vec<u8>> = HashSet::with_capacity(ka.keys.len());
        for key in &ka.keys {
            if !seen_keys.insert(serialize_key_for_dedup(key)) {
                return Err(DynamoDbError::ValidationException(
                    "Provided list of item keys contains duplicates".to_owned(),
                ));
            }
        }

        table_projections.insert(table_name.clone(), projection);
    }

    let mut responses: HashMap<String, Vec<Item>> = HashMap::new();
    let mut total_rcu: f64 = 0.0;
    let mut total_pre_proj_bytes: usize = 0;
    let mut returned_count: u64 = 0;
    let mut per_table_rcu: HashMap<String, f64> = HashMap::new();

    for (table_name, ka) in &input.request_items {
        let key_info = ctx
            .storage
            .table_key_info(&ctx.account_id, table_name)
            .await
            .map_err(storage_err_to_dynamo)?;

        let projection = table_projections.get(table_name).and_then(Option::as_ref);

        let mut table_items: Vec<Item> = Vec::new();
        for key in &ka.keys {
            validate_batch_key_only(key, &key_info.key_schema, &key_info.attribute_definitions)?;
            validate_key_sizes(key, &key_info.key_schema, &ctx.limits)?;

            if let Some(item) = ctx
                .storage
                .get_item(&key_info, key)
                .await
                .map_err(storage_err_to_dynamo)?
            {
                let size = item_size_bytes(&item);
                let strongly_consistent = ka.consistent_read == Some(true);
                let item_rcu = capacity_helpers::read_capacity_units(size, strongly_consistent);
                total_rcu += item_rcu;
                *per_table_rcu.entry(table_name.clone()).or_default() += item_rcu;
                total_pre_proj_bytes += size;
                returned_count += 1;
                let item = if let Some(projection) = projection {
                    projection.apply(&item)
                } else {
                    item
                };
                table_items.push(item);
            }
        }
        responses.insert(table_name.clone(), table_items);
    }

    let consumed_capacity = capacity_helpers::batch_read_capacity(
        input.return_consumed_capacity,
        per_table_rcu.iter().map(|(t, cu)| (t.as_str(), *cu)),
    );

    // Per-item RCU already accumulated above (M-1: DynamoDB rounds per item, then sums).
    let rcu = total_rcu;

    let output = BatchGetItemOutput {
        responses,
        unprocessed_keys: HashMap::new(),
        consumed_capacity,
    };
    let body = serialize_output(&output)?;
    Ok(DispatchResult {
        body,
        metrics: DispatchMetrics {
            read_capacity_units: rcu,
            returned_item_count: returned_count,
            returned_bytes: total_pre_proj_bytes as u64,
            ..Default::default()
        },
    })
}

fn serialize_key_for_dedup(key: &Item) -> Vec<u8> {
    serde_json::to_vec(key).unwrap_or_default()
}
