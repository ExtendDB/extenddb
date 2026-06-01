// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transactional read/write implementations for the `TiDB` backend.

use std::collections::HashMap;

use extenddb_core::expression::{self, ExpressionMaps};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    AttributeValue, CancellationReason, Item, ReturnValuesOnConditionCheckFailure,
};
use extenddb_core::validation;
use extenddb_storage::error::StorageError;
use extenddb_storage::{TransactGetOp, TransactWriteOp};

use super::index::{
    WriteIndexKeys, fetch_write_index_key_schemas, has_potential_secondary_index_keys,
    item_has_potential_secondary_index_key, validate_item_index_key_constraints,
};
use super::tx_helpers::{
    StreamSequenceAllocator, check_idempotency_token_in_tx, delete_item_in_tx,
    fetch_item_for_update, fetch_item_in_tx, finalize_stream_records_best_effort,
    upsert_item_in_tx, write_stream_record_in_tx,
};
use crate::TidbEngine;
use crate::tidb_util::retry_tidb_idempotent_operation;

impl TidbEngine {
    /// Implementation of `DataEngine::transact_get_items`.
    pub(crate) async fn transact_get_items_impl(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> Result<Vec<Option<Item>>, StorageError> {
        // Validate key types inside the transaction so mismatches produce
        // TransactionCanceledException with ValidationError cancellation
        // reasons, matching real DynamoDB behavior.
        let mut reasons: Vec<CancellationReason> = Vec::with_capacity(ops.len());
        let mut any_failed = false;
        for op in ops {
            match validation::validate_key_only(
                op.key,
                &op.key_info.key_schema,
                &op.key_info.attribute_definitions,
            ) {
                Ok(()) => reasons.push(CancellationReason::none()),
                Err(e) => {
                    any_failed = true;
                    reasons.push(CancellationReason::validation_error(e.to_string()));
                }
            }
        }
        if any_failed {
            return Err(StorageError::TransactionCanceled(reasons));
        }

        // Plain reads inside one TiDB transaction share a snapshot. TiDB treats
        // MySQL's READ ONLY transaction syntax as a disabled no-op feature, so
        // do not use START TRANSACTION READ ONLY here.
        let mut tx = self
            .data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut results = Vec::with_capacity(ops.len());
        for op in ops {
            let item = fetch_item_in_tx(&mut tx, op.key_info, op.key).await?;
            results.push(item);
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(results)
    }

    /// Implementation of `DataEngine::transact_write_items`.
    pub(crate) async fn transact_write_items_impl(
        &self,
        ops: &[TransactWriteOp<'_>],
        token: Option<(&str, &str)>,
    ) -> Result<(), StorageError> {
        if token.is_some() {
            match retry_tidb_idempotent_operation("transact_write_items", || async {
                self.transact_write_items_once(ops, token).await
            })
            .await
            {
                Err(StorageError::IdempotentReplay) => Ok(()),
                result => result,
            }
        } else {
            self.transact_write_items_once(ops, token).await
        }
    }

    async fn transact_write_items_once(
        &self,
        ops: &[TransactWriteOp<'_>],
        token: Option<(&str, &str)>,
    ) -> Result<(), StorageError> {
        // Pre-fetch secondary-index key schemas only for tables whose writes can
        // touch secondary-index key attributes. TiDB generated columns/native
        // indexes own maintenance; this fetch is only for DynamoDB validation
        // messages before a write reaches TiDB.
        let mut table_indexes: HashMap<String, Vec<WriteIndexKeys>> = HashMap::new();
        for op in ops {
            let table_id = transact_op_table_id(op);
            if !table_indexes.contains_key(table_id) {
                let indexes = if transact_op_needs_secondary_index_validation(op) {
                    fetch_write_index_key_schemas(table_id, &self.pool).await?
                } else {
                    Vec::new()
                };
                table_indexes.insert(table_id.to_owned(), indexes);
            }
        }

        let mut tx = self
            .data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Check idempotency token within the transaction.
        if let Some((tok, fp)) = token {
            check_idempotency_token_in_tx(&mut tx, tok, fp).await?;
        }

        let mut reasons: Vec<CancellationReason> = Vec::with_capacity(ops.len());
        // Collect old/new items from each op for stream records.
        let mut op_items: Vec<(Option<Item>, Option<Item>)> = Vec::with_capacity(ops.len());
        let mut any_failed = false;

        for op in ops {
            let indexes = &table_indexes[transact_op_table_id(op)];
            let reason = execute_transact_write_op(&mut tx, op, indexes, &self.limits).await;
            match reason {
                Ok(items) => {
                    op_items.push(items);
                    reasons.push(CancellationReason::none());
                }
                Err(TxnOpError::Cancel(r)) => {
                    op_items.push((None, None));
                    any_failed = true;
                    reasons.push(r);
                }
                Err(TxnOpError::Storage(e)) => {
                    // Infrastructure error — abort the entire transaction
                    // without leaking internal details into cancellation reasons.
                    return Err(StorageError::Internal(e.to_string()));
                }
            }
        }

        if any_failed {
            return Err(StorageError::TransactionCanceled(reasons));
        }

        // Write stream records atomically within the transaction.
        let mut sequence_allocator = StreamSequenceAllocator::default();
        for (op, (old_item, new_item)) in ops.iter().zip(op_items.iter()) {
            let capture = match op {
                TransactWriteOp::Put { stream, .. }
                | TransactWriteOp::Delete { stream, .. }
                | TransactWriteOp::Update { stream, .. } => stream.as_ref(),
                TransactWriteOp::ConditionCheck { .. } => None,
            };
            if let Some(capture) = capture {
                write_stream_record_in_tx(
                    &mut tx,
                    &mut sequence_allocator,
                    match op {
                        TransactWriteOp::Put { key_info, .. }
                        | TransactWriteOp::Delete { key_info, .. }
                        | TransactWriteOp::Update { key_info, .. }
                        | TransactWriteOp::ConditionCheck { key_info, .. } => key_info,
                    },
                    capture,
                    old_item.as_ref(),
                    new_item.as_ref(),
                )
                .await?;
            }
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        finalize_stream_records_best_effort(
            &self.data_pool,
            "transact_write_items",
            sequence_allocator.pending_records(),
        )
        .await;

        Ok(())
    }

    /// Implementation of `DataEngine::cleanup_expired_idempotency_tokens`.
    pub(crate) async fn cleanup_expired_idempotency_tokens_impl(
        &self,
        _max_age_seconds: i64,
    ) -> Result<u64, StorageError> {
        // TiDB native TTL owns background retention for this table. The
        // transaction write path still handles same-token expiry so client
        // idempotency semantics do not depend on TTL job timing.
        Ok(0)
    }
}

/// Extract the table_id from a transactional write operation.
fn transact_op_table_id<'a>(op: &'a TransactWriteOp<'_>) -> &'a str {
    match op {
        TransactWriteOp::Put { key_info, .. }
        | TransactWriteOp::Delete { key_info, .. }
        | TransactWriteOp::Update { key_info, .. }
        | TransactWriteOp::ConditionCheck { key_info, .. } => &key_info.table_id,
    }
}

fn transact_op_needs_secondary_index_validation(op: &TransactWriteOp<'_>) -> bool {
    match op {
        TransactWriteOp::Put { key_info, item, .. } => item_has_potential_secondary_index_key(
            item,
            &key_info.key_schema,
            &key_info.attribute_definitions,
        ),
        TransactWriteOp::Update { key_info, .. } => has_potential_secondary_index_keys(
            &key_info.key_schema,
            &key_info.attribute_definitions,
        ),
        TransactWriteOp::Delete { .. } | TransactWriteOp::ConditionCheck { .. } => false,
    }
}

fn transact_write_needs_existing_item(
    condition: Option<&extenddb_core::expression::Expr>,
    stream: &Option<extenddb_storage::StreamCapture>,
) -> bool {
    condition.is_some() || stream.is_some()
}

/// Error type for individual transactional write operations.
///
/// Separates user-driven cancellations (condition failures, validation errors)
/// from infrastructure errors (connection failures, transaction errors).
/// This prevents internal error details from leaking into client-visible
/// cancellation reasons.
enum TxnOpError {
    /// User-driven failure — becomes a per-item cancellation reason.
    Cancel(CancellationReason),
    /// Infrastructure failure — bubbles up as `StorageError::Internal`.
    Storage(StorageError),
}

impl From<CancellationReason> for TxnOpError {
    fn from(r: CancellationReason) -> Self {
        Self::Cancel(r)
    }
}

/// Execute a single transactional write operation, including native index-key validation.
/// Returns `(old_item, new_item)` on success for stream capture.
async fn execute_transact_write_op(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    op: &TransactWriteOp<'_>,
    indexes: &[WriteIndexKeys],
    limits: &LimitsConfig,
) -> Result<(Option<Item>, Option<Item>), TxnOpError> {
    match op {
        TransactWriteOp::Put {
            key_info,
            item,
            condition,
            maps,
            return_values_on_ccf,
            stream,
            ..
        } => {
            // Key type validation inside the transaction so mismatches produce
            // TransactionCanceledException with ValidationError cancellation
            // reasons, matching real DynamoDB behavior.
            validation::validate_item_keys(
                item,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )
            .map_err(|e| TxnOpError::Cancel(CancellationReason::validation_error(e.to_string())))?;
            validate_txn_index_key_constraints(
                item,
                indexes,
                &key_info.attribute_definitions,
                limits,
            )?;
            let existing = if transact_write_needs_existing_item(*condition, stream) {
                let existing = fetch_item_for_update(tx, key_info, item)
                    .await
                    .map_err(TxnOpError::Storage)?;
                let empty = Item::new();
                eval_condition(
                    *condition,
                    existing.as_ref().unwrap_or(&empty),
                    maps,
                    *return_values_on_ccf,
                    existing.as_ref(),
                )?;
                existing
            } else {
                None
            };
            upsert_item_in_tx(tx, key_info, item)
                .await
                .map_err(TxnOpError::Storage)?;
            Ok((existing, Some((*item).clone())))
        }
        TransactWriteOp::Delete {
            key_info,
            key,
            condition,
            maps,
            return_values_on_ccf,
            stream,
            ..
        } => {
            validation::validate_key_only(
                key,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )
            .map_err(|e| TxnOpError::Cancel(CancellationReason::validation_error(e.to_string())))?;
            let existing = if transact_write_needs_existing_item(*condition, stream) {
                let existing = fetch_item_for_update(tx, key_info, key)
                    .await
                    .map_err(TxnOpError::Storage)?;
                let empty = Item::new();
                eval_condition(
                    *condition,
                    existing.as_ref().unwrap_or(&empty),
                    maps,
                    *return_values_on_ccf,
                    existing.as_ref(),
                )?;
                existing
            } else {
                None
            };
            delete_item_in_tx(tx, key_info, key)
                .await
                .map_err(TxnOpError::Storage)?;
            Ok((existing, None))
        }
        TransactWriteOp::Update {
            key_info,
            key,
            actions,
            condition,
            maps,
            return_values_on_ccf,
            ..
        } => {
            validation::validate_key_only(
                key,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )
            .map_err(|e| TxnOpError::Cancel(CancellationReason::validation_error(e.to_string())))?;
            let existing = fetch_item_for_update(tx, key_info, key)
                .await
                .map_err(TxnOpError::Storage)?;
            let mut item = existing.clone().unwrap_or_else(|| (*key).clone());
            // Evaluate condition against empty item if non-existent (DynamoDB semantics)
            let condition_item = if existing.is_some() {
                &item
            } else {
                &std::collections::BTreeMap::new()
            };
            eval_condition(
                *condition,
                condition_item,
                maps,
                *return_values_on_ccf,
                existing.as_ref(),
            )?;
            expression::apply_update(actions, &mut item, maps).map_err(|e| {
                TxnOpError::Cancel(CancellationReason::validation_error(e.to_string()))
            })?;
            // Validate post-update item size
            validation::validate_item_size(&item, limits.max_item_size_bytes).map_err(|e| {
                TxnOpError::Cancel(CancellationReason::validation_error(e.to_string()))
            })?;
            validate_txn_index_key_constraints(
                &item,
                indexes,
                &key_info.attribute_definitions,
                limits,
            )?;
            upsert_item_in_tx(tx, key_info, &item)
                .await
                .map_err(TxnOpError::Storage)?;
            Ok((existing, Some(item)))
        }
        TransactWriteOp::ConditionCheck {
            key_info,
            key,
            condition,
            maps,
            return_values_on_ccf,
        } => {
            validation::validate_key_only(
                key,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )
            .map_err(|e| TxnOpError::Cancel(CancellationReason::validation_error(e.to_string())))?;
            let existing = fetch_item_for_update(tx, key_info, key)
                .await
                .map_err(TxnOpError::Storage)?;
            let empty = Item::new();
            let check_against = existing.as_ref().unwrap_or(&empty);
            eval_condition(
                Some(condition),
                check_against,
                maps,
                *return_values_on_ccf,
                existing.as_ref(),
            )?;
            Ok((None, None))
        }
    }
}

fn validate_txn_index_key_constraints(
    item: &Item,
    indexes: &[WriteIndexKeys],
    attr_defs: &[extenddb_core::types::AttributeDefinition],
    limits: &LimitsConfig,
) -> Result<(), TxnOpError> {
    validate_item_index_key_constraints(item, indexes, attr_defs, limits).map_err(|err| match err {
        StorageError::Validation(message) => {
            TxnOpError::Cancel(CancellationReason::validation_error(message))
        }
        other => TxnOpError::Storage(other),
    })
}

/// Evaluate a condition expression, returning a `CancellationReason` on failure.
///
/// When `return_values_on_ccf` is `AllOld`, the existing item is included in the
/// cancellation reason so the client can see what caused the condition to fail.
fn eval_condition(
    condition: Option<&extenddb_core::expression::Expr>,
    item: &std::collections::BTreeMap<String, AttributeValue>,
    maps: &ExpressionMaps,
    return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
    existing: Option<&Item>,
) -> Result<(), CancellationReason> {
    if let Some(cond) = condition {
        let passed = expression::evaluate_condition(cond, item, maps)
            .map_err(|e| CancellationReason::validation_error(e.to_string()))?;
        if !passed {
            let item_to_return =
                if return_values_on_ccf == ReturnValuesOnConditionCheckFailure::AllOld {
                    existing.cloned()
                } else {
                    None
                };
            return Err(CancellationReason::condition_check_failed_with_item(
                item_to_return,
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use extenddb_core::expression::Expr;
    use extenddb_core::types::StreamViewType;
    use extenddb_storage::StreamCapture;

    use super::transact_write_needs_existing_item;

    fn condition() -> Expr {
        Expr::Function {
            name: "attribute_exists".to_owned(),
            args: vec![Expr::Path(vec![
                extenddb_core::expression::PathElement::Attribute("pk".to_owned()),
            ])],
        }
    }

    fn stream_capture() -> StreamCapture {
        StreamCapture {
            view_type: StreamViewType::KeysOnly,
            user_identity: None,
            region: Arc::from("us-east-1"),
        }
    }

    #[test]
    fn unconditional_transaction_write_without_stream_skips_existing_item_read() {
        assert!(!transact_write_needs_existing_item(None, &None));
    }

    #[test]
    fn transaction_write_with_condition_or_stream_needs_existing_item_read() {
        let condition = condition();
        assert!(transact_write_needs_existing_item(Some(&condition), &None));
        assert!(transact_write_needs_existing_item(
            None,
            &Some(stream_capture())
        ));
    }
}
