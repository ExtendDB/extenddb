// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `TransactGetItems` operation handler.

use std::collections::HashSet;

use serde_json::Value;

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::Projection;
use extenddb_core::types::{
    CancellationReason, ItemResponse, TransactGetItemsInput, TransactGetItemsOutput,
    item_size_bytes,
};
use extenddb_core::validation;
use extenddb_storage::TransactGetOp;
use extenddb_storage::error::StorageError;

use crate::OperationContext;
use crate::capacity_helpers;
use crate::create_table::storage_err_to_dynamo;
use crate::expression_helpers::build_expression_maps;
use crate::per_table_capacity::PerTableCapacity;
use crate::request_limits;
use crate::serialize_output;
use crate::{DispatchMetrics, DispatchResult};

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

    // At least one and at most 100 items. The server runs the same check
    // before authorization.
    request_limits::check_transact_count(input.transact_items.len(), "TransactGetItem")?;

    // Validation runs in the order the service applies it, each phase over
    // every item before the next phase starts: projection grammar and alias
    // resolution (top-level ValidationException), then table lookup
    // (ResourceNotFoundException), then key schema and key size
    // (TransactionCanceledException with a ValidationError reason per
    // offending item), then duplicate keys (top-level ValidationException).
    // Measured against DynamoDB for #108.
    let mut projections: Vec<Option<Projection>> = Vec::with_capacity(input.transact_items.len());
    for tgi in &input.transact_items {
        if let Some(ref proj_str) = tgi.get.projection_expression {
            let maps = build_expression_maps(tgi.get.expression_attribute_names.as_ref(), None);
            let projection =
                crate::expression_helpers::parse_projection_expr(proj_str, &ctx.limits)?;
            let compiled = Projection::compile(&projection, &maps, true)?;
            let mut extra_names = HashSet::new();
            for path in &projection {
                for el in path {
                    if let extenddb_core::expression::PathElement::Attribute(name) = el
                        && let Some(ref_name) = name.strip_prefix('#')
                    {
                        extra_names.insert(ref_name.to_owned());
                    }
                }
            }
            extenddb_core::expression::validate_unused_attributes(
                &maps.names,
                &maps.values,
                &[],
                &[],
                &extra_names,
                &HashSet::new(),
            )?;
            projections.push(Some(compiled));
        } else {
            // No projection: transactions accept names with no expression.
            projections.push(None);
        }
    }

    let mut key_infos = Vec::with_capacity(input.transact_items.len());
    for tgi in &input.transact_items {
        let key_info = ctx
            .table_key_info_uncached(&tgi.get.table_name)
            .await
            .map_err(storage_err_to_dynamo)?;
        key_infos.push(key_info);
    }

    // A key with the wrong type, extra or missing attributes, or over the size
    // limit cancels the transaction with a ValidationError reason on the FIRST
    // offending item and None on every other, even when several keys fail
    // (measured 2026-10-14: items 1 and 3 mistyped report
    // [ValidationError, None, None]).
    let first_offender = input
        .transact_items
        .iter()
        .zip(key_infos.iter())
        .enumerate()
        .find_map(|(i, (tgi, ki))| {
            validation::validate_batch_key_only(
                &tgi.get.key,
                &ki.key_schema,
                &ki.attribute_definitions,
            )
            .and_then(|()| {
                validation::validate_key_size_limits(&tgi.get.key, &ki.key_schema, &ctx.limits)
            })
            .err()
            .map(|e| (i, e))
        });
    if let Some((index, err)) = first_offender {
        let reasons: Vec<CancellationReason> = (0..input.transact_items.len())
            .map(|i| {
                if i == index {
                    CancellationReason::validation_error(err.to_string())
                } else {
                    CancellationReason::none()
                }
            })
            .collect();
        return Err(storage_err_to_dynamo(StorageError::TransactionCanceled(
            reasons,
        )));
    }

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
    let mut per_table_rcu = PerTableCapacity::default();
    let rcu: f64 = items
        .iter()
        .zip(key_infos.iter())
        .map(|(opt, ki)| {
            let base_rcu = match opt {
                Some(item) => capacity_helpers::read_capacity_units(item_size_bytes(item), true),
                None => 1.0, // minimum 1 RCU for missing item
            };
            let txn_rcu = base_rcu * 2.0; // transactions cost 2x
            per_table_rcu.add(ki, txn_rcu);
            txn_rcu
        })
        .sum();
    let total_pre_proj_bytes: usize = items
        .iter()
        .filter_map(|opt| opt.as_ref())
        .map(item_size_bytes)
        .sum();
    let returned_count = items.iter().filter(|opt| opt.is_some()).count() as u64;

    // Apply per-item projection using the projections compiled before the read.
    let responses: Vec<ItemResponse> = items
        .into_iter()
        .zip(projections)
        .map(|(opt, proj)| {
            let item = match proj {
                Some(p) => opt.map(|item| p.apply(&item)).filter(|i| !i.is_empty()),
                None => opt,
            };
            ItemResponse { item }
        })
        .collect();

    let consumed_capacity = capacity_helpers::transact_read_capacity(
        input.return_consumed_capacity,
        per_table_rcu.ordered().into_iter(),
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
