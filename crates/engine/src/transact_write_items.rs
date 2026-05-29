// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `TransactWriteItems` operation handler.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::OperationContext;
use crate::capacity_helpers;
use crate::create_table::storage_err_to_dynamo;
use crate::expression_helpers::build_expression_maps;
use crate::serialize_output;
use crate::stream_capture;
use crate::transact_write_helpers::{
    PreparedOp, compute_fingerprint, parse_optional_condition, validate_client_request_token,
    validate_no_key_updates,
};
use crate::{DispatchMetrics, DispatchResult};
use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::parse_update;
use extenddb_core::types::{TransactWriteItem, TransactWriteItemsInput, TransactWriteItemsOutput};
use extenddb_core::validation::{
    validate_attribute_name_sizes, validate_attribute_values_nesting_depth,
    validate_item_nesting_depth, validate_item_size, validate_key_sizes,
};

/// Maximum number of items in a single `TransactWriteItems` request.
const MAX_TRANSACT_WRITE_ITEMS: usize = 100;

/// Handle a `TransactWriteItems` request.
///
/// Executes up to 100 write operations atomically. All operations succeed
/// or all are rolled back. Supports `Put`, `Delete`, `Update`, and `ConditionCheck`.
///
/// # Errors
///
/// Returns `TransactionCanceledException` if any condition fails.
/// Returns `ValidationException` for input validation failures.
/// Returns `IdempotentParameterMismatchException` for token conflicts.
pub async fn handle_transact_write_items(
    body: Value,
    ctx: &OperationContext,
) -> Result<DispatchResult, DynamoDbError> {
    let input: TransactWriteItemsInput =
        serde_json::from_value(body.clone()).map_err(crate::deserialize_error)?;

    // Compute fingerprint keyed by the client request token for collision
    // resistance. Must happen after parsing so the token is available.
    let fingerprint = input
        .client_request_token
        .as_deref()
        .map(|t| compute_fingerprint(&body, t))
        .unwrap_or_default();

    if input.transact_items.is_empty() {
        return Err(DynamoDbError::ValidationException(
            "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1".to_owned(),
        ));
    }

    if input.transact_items.len() > MAX_TRANSACT_WRITE_ITEMS {
        let items_repr = input
            .transact_items
            .iter()
            .map(|_| "TransactWriteItem")
            .collect::<Vec<_>>()
            .join(", ");
        return Err(DynamoDbError::ValidationException(format!(
            "1 validation error detected: Value '[{items_repr}]' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100"
        )));
    }

    // Validate each item has exactly one operation
    for (i, twi) in input.transact_items.iter().enumerate() {
        let count = [
            twi.condition_check.is_some(),
            twi.put.is_some(),
            twi.delete.is_some(),
            twi.update.is_some(),
        ]
        .iter()
        .filter(|&&b| b)
        .count();
        if count != 1 {
            return Err(DynamoDbError::ValidationException(format!(
                "TransactItems[{i}] must contain exactly one of ConditionCheck, Put, Delete, or Update"
            )));
        }
    }

    // Resolve table key info, validate inputs, parse expressions, and check
    // for duplicate targets — all in a single pass to avoid redundant
    // `table_key_info` lookups.
    let mut prepared: Vec<PreparedOp> = Vec::with_capacity(input.transact_items.len());
    let mut seen_targets: HashSet<String> = HashSet::with_capacity(input.transact_items.len());
    for twi in &input.transact_items {
        let op = prepare_write_op(twi, ctx).await?;
        let target_key = op.canonical_target();
        if !seen_targets.insert(target_key) {
            return Err(DynamoDbError::ValidationException(
                "Transaction request cannot include multiple operations on one item".to_owned(),
            ));
        }
        prepared.push(op);
    }

    // Validate total transaction size <= 4MB
    let total_size: usize = prepared.iter().map(|op| op.item_size()).sum();
    if total_size > 4 * 1024 * 1024 {
        return Err(DynamoDbError::ValidationException(
            "Transaction item size has exceeded the 4 MB limit".to_owned(),
        ));
    }

    // Build storage operations
    let ops: Vec<extenddb_storage::TransactWriteOp<'_>> =
        prepared.iter().map(|p| p.to_storage_op()).collect();

    // Idempotency token validation (REQ-TRANSACT-003).
    // The token is passed to the storage layer and checked atomically within
    // the write transaction, guaranteeing that token storage and data writes
    // commit together.
    if let Some(ref token) = input.client_request_token {
        validate_client_request_token(token)?;
    }

    let token_pair = input
        .client_request_token
        .as_deref()
        .map(|t| (t, fingerprint.as_str()));

    match ctx.storage.transact_write_items(&ops, token_pair).await {
        Ok(()) => {}
        Err(extenddb_storage::error::StorageError::IdempotentReplay) => {
            let output = TransactWriteItemsOutput {
                consumed_capacity: None,
                item_collection_metrics: None,
            };
            return Ok(DispatchResult::body_only(serialize_output(&output)?));
        }
        Err(extenddb_storage::error::StorageError::IdempotentMismatch) => {
            return Err(DynamoDbError::IdempotentParameterMismatchException(
                "The request uses the same client token as a previous, \
                 but different, request."
                    .to_owned(),
            ));
        }
        Err(e) => return Err(storage_err_to_dynamo(e)),
    }

    // Stream records are now captured atomically within the storage transaction.

    // Per-item WCU: round each item individually, then sum (M-1).
    let mut per_table_wcu: HashMap<String, f64> = HashMap::new();
    let wcu: f64 = prepared
        .iter()
        .map(|op| {
            let item_wcu = capacity_helpers::write_capacity_units(op.write_bytes());
            *per_table_wcu.entry(op.table_name().to_owned()).or_default() += item_wcu;
            item_wcu
        })
        .sum();

    let consumed_capacity = capacity_helpers::batch_write_capacity(
        input.return_consumed_capacity,
        per_table_wcu.iter().map(|(t, cu)| (t.as_str(), *cu)),
    );

    // Collect ItemCollectionMetrics per table for write operations.
    let mut all_icm: HashMap<String, Vec<extenddb_core::types::ItemCollectionMetrics>> =
        HashMap::new();
    for p in &prepared {
        if let Some(m) = p.item_collection_metric(input.return_item_collection_metrics) {
            all_icm
                .entry(p.table_name().to_owned())
                .or_default()
                .push(m);
        }
    }

    let output = TransactWriteItemsOutput {
        consumed_capacity,
        item_collection_metrics: if all_icm.is_empty() {
            None
        } else {
            Some(all_icm)
        },
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

/// Parse and validate a single `TransactWriteItem`, returning a `PreparedOp`.
async fn prepare_write_op(
    twi: &TransactWriteItem,
    ctx: &OperationContext,
) -> Result<PreparedOp, DynamoDbError> {
    if let Some(put) = &twi.put {
        let key_info = ctx
            .storage
            .table_key_info(&ctx.account_id, &put.table_name)
            .await
            .map_err(storage_err_to_dynamo)?;
        validate_item_nesting_depth(&put.item)?;
        validate_item_size(&put.item, ctx.limits.max_item_size_bytes)?;
        validate_attribute_name_sizes(&put.item, &ctx.limits)?;
        validate_key_sizes(&put.item, &key_info.key_schema, &ctx.limits)?;
        let maps = build_expression_maps(
            put.expression_attribute_names.as_ref(),
            put.expression_attribute_values.as_ref(),
        );
        let condition = parse_optional_condition(put.condition_expression.as_deref(), &ctx.limits)?;
        {
            let exprs: Vec<&extenddb_core::expression::Expr> = condition.iter().collect();
            extenddb_core::expression::validate_unused_attributes(
                &maps.names,
                &maps.values,
                &exprs,
                &[],
                &HashSet::new(),
                &HashSet::new(),
            )?;
        }
        let stream =
            stream_capture::stream_view_type(&key_info).map(|vt| extenddb_storage::StreamCapture {
                view_type: vt,
                user_identity: None,
                region: ctx.region.clone(),
            });
        return Ok(PreparedOp::Put {
            key_info,
            item: put.item.clone(),
            condition,
            maps,
            return_values_on_ccf: put.return_values_on_condition_check_failure,
            stream,
        });
    }

    if let Some(del) = &twi.delete {
        let key_info = ctx
            .storage
            .table_key_info(&ctx.account_id, &del.table_name)
            .await
            .map_err(storage_err_to_dynamo)?;
        let maps = build_expression_maps(
            del.expression_attribute_names.as_ref(),
            del.expression_attribute_values.as_ref(),
        );
        let condition = parse_optional_condition(del.condition_expression.as_deref(), &ctx.limits)?;
        {
            let exprs: Vec<&extenddb_core::expression::Expr> = condition.iter().collect();
            extenddb_core::expression::validate_unused_attributes(
                &maps.names,
                &maps.values,
                &exprs,
                &[],
                &HashSet::new(),
                &HashSet::new(),
            )?;
        }
        let stream =
            stream_capture::stream_view_type(&key_info).map(|vt| extenddb_storage::StreamCapture {
                view_type: vt,
                user_identity: None,
                region: ctx.region.clone(),
            });
        return Ok(PreparedOp::Delete {
            key_info,
            key: del.key.clone(),
            condition,
            maps,
            return_values_on_ccf: del.return_values_on_condition_check_failure,
            stream,
        });
    }

    if let Some(upd) = &twi.update {
        let key_info = ctx
            .storage
            .table_key_info(&ctx.account_id, &upd.table_name)
            .await
            .map_err(storage_err_to_dynamo)?;
        let maps = build_expression_maps(
            upd.expression_attribute_names.as_ref(),
            upd.expression_attribute_values.as_ref(),
        );
        let update_tokens =
            crate::expression_helpers::tokenize_expression(&upd.update_expression, &ctx.limits)?;
        let actions = parse_update(&update_tokens)?;
        validate_no_key_updates(&actions, &key_info, &maps)?;

        // Validate nesting depth of EAV values that get stored via SET actions.
        {
            let mut placeholders: Vec<String> = Vec::new();
            for action in &actions {
                if let extenddb_core::expression::UpdateAction::Set { value, .. } = action {
                    extenddb_core::expression::collect_value_placeholders(value, &mut placeholders);
                }
            }
            let stored: Vec<&extenddb_core::types::AttributeValue> = placeholders
                .iter()
                .filter_map(|name| maps.values.get(name))
                .collect();
            validate_attribute_values_nesting_depth(stored)?;
        }
        let condition = parse_optional_condition(upd.condition_expression.as_deref(), &ctx.limits)?;
        {
            let exprs: Vec<&extenddb_core::expression::Expr> = condition.iter().collect();
            extenddb_core::expression::validate_unused_attributes(
                &maps.names,
                &maps.values,
                &exprs,
                &actions.iter().collect::<Vec<_>>(),
                &HashSet::new(),
                &HashSet::new(),
            )?;
        }
        let stream =
            stream_capture::stream_view_type(&key_info).map(|vt| extenddb_storage::StreamCapture {
                view_type: vt,
                user_identity: None,
                region: ctx.region.clone(),
            });
        return Ok(PreparedOp::Update {
            key_info,
            key: upd.key.clone(),
            actions,
            condition,
            maps,
            return_values_on_ccf: upd.return_values_on_condition_check_failure,
            stream,
        });
    }

    if let Some(cc) = &twi.condition_check {
        let key_info = ctx
            .storage
            .table_key_info(&ctx.account_id, &cc.table_name)
            .await
            .map_err(storage_err_to_dynamo)?;
        let maps = build_expression_maps(
            cc.expression_attribute_names.as_ref(),
            cc.expression_attribute_values.as_ref(),
        );
        let tokens =
            crate::expression_helpers::tokenize_expression(&cc.condition_expression, &ctx.limits)?;
        let condition = extenddb_core::expression::parse_condition_with_depth_limit(
            &tokens,
            ctx.limits.max_expression_depth,
        )?;
        {
            let exprs: Vec<&extenddb_core::expression::Expr> = vec![&condition];
            extenddb_core::expression::validate_unused_attributes(
                &maps.names,
                &maps.values,
                &exprs,
                &[],
                &HashSet::new(),
                &HashSet::new(),
            )?;
        }
        return Ok(PreparedOp::ConditionCheck {
            key_info,
            key: cc.key.clone(),
            condition,
            maps,
            return_values_on_ccf: cc.return_values_on_condition_check_failure,
        });
    }

    // Should be unreachable due to earlier validation
    Err(DynamoDbError::ValidationException(
        "TransactWriteItem must contain exactly one operation".to_owned(),
    ))
}
