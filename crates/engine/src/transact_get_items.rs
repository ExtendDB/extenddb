// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `TransactGetItems` operation handler.

use std::collections::HashSet;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::{apply_projection, parse_projection, tokenize_for};
use extenddb_core::types::{
    ItemResponse, TransactGetItemsInput, TransactGetItemsOutput, item_size_bytes,
};
use extenddb_storage::TransactGetOp;

use crate::OperationContext;
use crate::capacity_helpers;
use crate::create_table::storage_err_to_dynamo;
use crate::expression_helpers::build_expression_maps;
use crate::serialize_output;
use crate::{DispatchMetrics, DispatchResult};

/// Maximum number of items in a single `TransactGetItems` request.
const MAX_TRANSACT_GET_ITEMS: usize = 100;

/// Handle a `TransactGetItems` request.
///
/// Reads up to 100 items atomically in a single consistent snapshot.
/// All items are returned in the same order as the request.
///
/// # Errors
///
/// Returns `DynamoDbError` for validation failures, missing tables, or storage errors.
pub async fn handle_transact_get_items(
    body: Value,
    ctx: &OperationContext,
) -> Result<DispatchResult, DynamoDbError> {
    let input: TransactGetItemsInput =
        serde_json::from_value(body).map_err(crate::deserialize_error)?;

    if input.transact_items.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1".to_owned(),
        ));
    }

    if input.transact_items.len() > MAX_TRANSACT_GET_ITEMS {
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '[{}]' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100",
            input
                .transact_items
                .iter()
                .map(|_| "TransactGetItem")
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    // Resolve table key info for each item
    let mut seen_keys: HashSet<Vec<u8>> = HashSet::with_capacity(input.transact_items.len());
    for tgi in &input.transact_items {
        let dedup_key =
            serde_json::to_vec(&(&tgi.get.table_name, &tgi.get.key)).unwrap_or_default();
        if !seen_keys.insert(dedup_key) {
            return Err(DynamoDbError::ValidationException(
                "Transaction request cannot include multiple operations on one item".to_owned(),
            ));
        }
    }

    let mut key_infos = Vec::with_capacity(input.transact_items.len());
    for tgi in &input.transact_items {
        let key_info = ctx
            .storage
            .table_key_info(&ctx.account_id, &tgi.get.table_name)
            .await
            .map_err(storage_err_to_dynamo)?;
        // Key type validation is deferred to the storage layer so that
        // mismatches produce TransactionCanceledException with ValidationError
        // cancellation reasons, matching real DynamoDB behavior.
        key_infos.push(key_info);
    }

    // Build storage operations
    let ops: Vec<TransactGetOp<'_>> = input
        .transact_items
        .iter()
        .zip(key_infos.iter())
        .map(|(tgi, ki)| TransactGetOp {
            key_info: ki,
            key: &tgi.get.key,
        })
        .collect();

    let items = ctx
        .storage
        .transact_get_items(&ops)
        .await
        .map_err(storage_err_to_dynamo)?;

    // Capacity metering: RCU rounded per item, then summed (M-1).
    // Capacity metering: TransactGetItems costs 2 RCU per item (transactions
    // double the read cost). Missing items still cost 2 RCU.
    let mut per_table_rcu: std::collections::HashMap<String, f64> =
        std::collections::HashMap::new();
    let rcu: f64 = items
        .iter()
        .zip(input.transact_items.iter())
        .map(|(opt, tgi)| {
            let base_rcu = match opt {
                Some(item) => capacity_helpers::read_capacity_units(item_size_bytes(item), true),
                None => 1.0, // minimum 1 RCU for missing item
            };
            let txn_rcu = base_rcu * 2.0; // transactions cost 2x
            *per_table_rcu.entry(tgi.get.table_name.clone()).or_default() += txn_rcu;
            txn_rcu
        })
        .sum();
    let total_pre_proj_bytes: usize = items
        .iter()
        .filter_map(|opt| opt.as_ref())
        .map(item_size_bytes)
        .sum();
    let returned_count = items.iter().filter(|opt| opt.is_some()).count() as u64;

    // Apply per-item projection
    let responses: Vec<ItemResponse> = items
        .into_iter()
        .zip(input.transact_items.iter())
        .map(|(opt, tgi)| {
            let maps = build_expression_maps(tgi.get.expression_attribute_names.as_ref(), None);
            if let Some(ref proj_str) = tgi.get.projection_expression {
                let proj_tokens = tokenize_for(
                    proj_str,
                    ctx.limits.max_expression_tokens,
                    "ProjectionExpression",
                )?;
                let projection = parse_projection(&proj_tokens)?;
                let mut extra_names = std::collections::HashSet::new();
                for path in &projection {
                    for el in path {
                        if let extenddb_core::expression::PathElement::Attribute(name) = el {
                            if let Some(ref_name) = name.strip_prefix('#') {
                                extra_names.insert(ref_name.to_owned());
                            }
                        }
                    }
                }
                extenddb_core::expression::validate_unused_attributes(
                    &maps.names,
                    &maps.values,
                    &[],
                    &[],
                    &extra_names,
                    &std::collections::HashSet::new(),
                )?;
                let item = opt
                    .map(|item| apply_projection(&item, &projection, &maps))
                    .transpose()?
                    .filter(|i| !i.is_empty());
                Ok(ItemResponse { item })
            } else {
                extenddb_core::expression::validate_unused_attributes(
                    &maps.names,
                    &maps.values,
                    &[],
                    &[],
                    &std::collections::HashSet::new(),
                    &std::collections::HashSet::new(),
                )?;
                Ok(ItemResponse { item: opt })
            }
        })
        .collect::<Result<Vec<_>, DynamoDbError>>()?;

    let consumed_capacity = capacity_helpers::batch_read_capacity(
        input.return_consumed_capacity,
        per_table_rcu.iter().map(|(t, cu)| (t.as_str(), *cu)),
    );

    let output = TransactGetItemsOutput {
        responses,
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
