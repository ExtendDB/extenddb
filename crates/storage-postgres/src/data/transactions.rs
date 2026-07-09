// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transactional read/write implementations for the `PostgreSQL` backend.

use std::collections::HashMap;

use extenddb_core::expression::{self, ExpressionMaps};
use extenddb_core::types::{
    AttributeValue, CancellationReason, Item, ReturnValuesOnConditionCheckFailure,
};
use extenddb_core::validation;
use extenddb_storage::error::StorageError;
use extenddb_storage::{TransactGetOp, TransactWriteOp};

use super::index::{IndexMeta, enqueue_async_indexes, fetch_indexes_for_table, sync_indexes};
use super::tx_helpers::{
    check_idempotency_token_in_tx, delete_item_in_tx, fetch_item_for_update, fetch_item_in_tx,
    upsert_item_in_tx, write_stream_record_in_tx,
};
use crate::PostgresEngine;

impl PostgresEngine {
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
        // Pre-fetch indexes for each unique table involved in the transaction.
        let mut table_indexes: HashMap<String, Vec<IndexMeta>> = HashMap::new();
        for op in ops {
            let name = transact_op_table_name(op);
            if !table_indexes.contains_key(name) {
                let tid = transact_op_table_id(op);
                let indexes = fetch_indexes_for_table(tid, &self.pool).await?;
                table_indexes.insert(name.to_owned(), indexes);
            }
        }

        // D-4: Read system default delay from cache (P119).
        let sys_delay = self
            .gsi_default_delay_ms
            .load(std::sync::atomic::Ordering::Relaxed);

        let mut tx = self
            .data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Check idempotency token within the transaction (BLOCKER #2 fix).
        if let Some((tok, fp)) = token {
            check_idempotency_token_in_tx(&mut tx, tok, fp).await?;
        }

        let mut reasons: Vec<CancellationReason> = Vec::with_capacity(ops.len());
        // M-3: Collect old/new items from each op for async GSI enqueue after commit.
        let mut op_items: Vec<(Option<Item>, Option<Item>)> = Vec::with_capacity(ops.len());
        let mut any_failed = false;

        for op in ops {
            let indexes = &table_indexes[transact_op_table_name(op)];
            let reason = execute_transact_write_op(
                &mut tx,
                op,
                indexes,
                self.max_item_size_bytes,
                sys_delay,
            )
            .await;
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
                Err(TxnOpError::Validation(msg)) => {
                    // Up-front input validation (e.g. empty secondary-index key):
                    // abort the whole transaction with a top-level
                    // ValidationException, not a per-item cancellation reason.
                    return Err(StorageError::Validation(msg));
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

        // Write stream records atomically within the transaction (BLOCKER #1 fix).
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

        // Persist async GSI work inside the transaction.
        let mut needs_notify = false;
        for (op, (old_item, new_item)) in ops.iter().zip(op_items.iter()) {
            // ConditionCheck (and any op that touched no item) — no index changes.
            if old_item.is_none() && new_item.is_none() {
                continue;
            }
            let indexes = &table_indexes[transact_op_table_name(op)];
            let key_info = match op {
                TransactWriteOp::Put { key_info, .. }
                | TransactWriteOp::Delete { key_info, .. }
                | TransactWriteOp::Update { key_info, .. }
                | TransactWriteOp::ConditionCheck { key_info, .. } => key_info,
            };
            // One row per async index, each honoring its own propagation delay.
            let n = enqueue_async_indexes(
                &mut tx,
                key_info,
                indexes,
                old_item.as_ref(),
                new_item.as_ref(),
                sys_delay,
            )
            .await?;
            if n > 0 {
                needs_notify = true;
            }
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if needs_notify && let Some(ref q) = self.gsi_queue {
            q.notify_workers();
        }

        Ok(())
    }

    /// Implementation of `DataEngine::cleanup_expired_idempotency_tokens`.
    pub(crate) async fn cleanup_expired_idempotency_tokens_impl(
        &self,
        max_age_seconds: i64,
    ) -> Result<u64, StorageError> {
        // Cast i64→integer for PG 15 compat; safe for realistic values (<68 years).
        // P54 Bug 1: idempotency_tokens lives in the data database.
        let result = sqlx::query(
            "DELETE FROM idempotency_tokens WHERE created_at < NOW() - make_interval(secs => $1::integer)",
        )
        .bind(max_age_seconds)
        .execute(&self.data_pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(result.rows_affected())
    }
}

/// Extract the table name from a transactional write operation.
fn transact_op_table_name<'a>(op: &'a TransactWriteOp<'_>) -> &'a str {
    match op {
        TransactWriteOp::Put { key_info, .. }
        | TransactWriteOp::Delete { key_info, .. }
        | TransactWriteOp::Update { key_info, .. }
        | TransactWriteOp::ConditionCheck { key_info, .. } => &key_info.table_name,
    }
}

/// Extract the `table_id` from a transactional write operation.
fn transact_op_table_id<'a>(op: &'a TransactWriteOp<'_>) -> &'a str {
    match op {
        TransactWriteOp::Put { key_info, .. }
        | TransactWriteOp::Delete { key_info, .. }
        | TransactWriteOp::Update { key_info, .. }
        | TransactWriteOp::ConditionCheck { key_info, .. } => &key_info.table_id,
    }
}

/// Error type for individual transactional write operations.
///
/// Separates user-driven cancellations (condition failures, validation errors)
/// from infrastructure errors (PG connection failures, serialization errors).
/// This prevents internal error details from leaking into client-visible
/// cancellation reasons (BLOCKER #3 fix).
/// Build [`validation::IndexKeyRef`] views over the table's indexes for
/// secondary-index key validation.
fn index_key_refs(indexes: &[IndexMeta]) -> Vec<validation::IndexKeyRef<'_>> {
    indexes
        .iter()
        .map(|idx| validation::IndexKeyRef {
            index_name: &idx.index_name,
            key_schema: &idx.key_schema,
        })
        .collect()
}

enum TxnOpError {
    /// User-driven failure — becomes a per-item cancellation reason.
    Cancel(CancellationReason),
    /// Up-front input validation failure — aborts the whole transaction with a
    /// top-level `ValidationException` (not a per-item cancellation reason).
    Validation(String),
    /// Infrastructure failure — bubbles up as `StorageError::Internal`.
    Storage(StorageError),
}

impl From<CancellationReason> for TxnOpError {
    fn from(r: CancellationReason) -> Self {
        Self::Cancel(r)
    }
}

/// Execute a single transactional write operation, including sync GSI/LSI updates.
///
/// Only sync indexes (delay=0) are processed here. Async indexes are enqueued
/// by the caller after the transaction commits.
///
/// Returns `(old_item, new_item)` on success for async GSI enqueue.
async fn execute_transact_write_op(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    op: &TransactWriteOp<'_>,
    indexes: &[IndexMeta],
    max_item_size_bytes: usize,
    sys_delay: u64,
) -> Result<(Option<Item>, Option<Item>), TxnOpError> {
    match op {
        TransactWriteOp::Put {
            key_info,
            item,
            condition,
            maps,
            return_values_on_ccf,
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
            // Secondary-index key faults split by kind: a type mismatch is a
            // per-item cancellation reason; an empty index key is up-front input
            // validation (a top-level ValidationException).
            let idx_refs = index_key_refs(indexes);
            validation::validate_index_key_types(item, &idx_refs, &key_info.attribute_definitions)
                .map_err(|e| {
                    TxnOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;
            validation::validate_index_key_not_empty(
                item,
                &idx_refs,
                validation::SecondaryIndexEmptyContext::Item,
            )
            .map_err(|e| TxnOpError::Validation(e.to_string()))?;
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
            upsert_item_in_tx(tx, key_info, item)
                .await
                .map_err(TxnOpError::Storage)?;
            if !indexes.is_empty() {
                sync_indexes(
                    tx,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                    indexes,
                    existing.as_ref(),
                    Some(item),
                    sys_delay,
                )
                .await
                .map_err(TxnOpError::Storage)?;
            }
            Ok((existing, Some((*item).clone())))
        }
        TransactWriteOp::Delete {
            key_info,
            key,
            condition,
            maps,
            return_values_on_ccf,
            ..
        } => {
            validation::validate_batch_key_only(
                key,
                &key_info.key_schema,
                &key_info.attribute_definitions,
            )
            .map_err(|e| TxnOpError::Cancel(CancellationReason::validation_error(e.to_string())))?;
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
            delete_item_in_tx(tx, key_info, key)
                .await
                .map_err(TxnOpError::Storage)?;
            if !indexes.is_empty() {
                sync_indexes(
                    tx,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                    indexes,
                    existing.as_ref(),
                    None,
                    sys_delay,
                )
                .await
                .map_err(TxnOpError::Storage)?;
            }
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
            validation::validate_batch_key_only(
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
            validation::validate_item_size(&item, max_item_size_bytes).map_err(|e| {
                TxnOpError::Cancel(CancellationReason::validation_error(e.to_string()))
            })?;
            // Secondary-index key validation on the post-update item: a type
            // mismatch is a cancellation reason; setting an index key to an
            // empty value is a top-level ValidationException.
            let idx_refs = index_key_refs(indexes);
            validation::validate_index_key_types(&item, &idx_refs, &key_info.attribute_definitions)
                .map_err(|e| {
                    TxnOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;
            validation::validate_index_key_not_empty(
                &item,
                &idx_refs,
                validation::SecondaryIndexEmptyContext::UpdateExpression,
            )
            .map_err(|e| TxnOpError::Validation(e.to_string()))?;
            upsert_item_in_tx(tx, key_info, &item)
                .await
                .map_err(TxnOpError::Storage)?;
            if !indexes.is_empty() {
                sync_indexes(
                    tx,
                    &key_info.key_schema,
                    &key_info.attribute_definitions,
                    indexes,
                    existing.as_ref(),
                    Some(&item),
                    sys_delay,
                )
                .await
                .map_err(TxnOpError::Storage)?;
            }
            Ok((existing, Some(item)))
        }
        TransactWriteOp::ConditionCheck {
            key_info,
            key,
            condition,
            maps,
            return_values_on_ccf,
        } => {
            validation::validate_batch_key_only(
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
