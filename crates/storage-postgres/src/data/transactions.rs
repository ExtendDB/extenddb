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
use extenddb_storage::{IdempotencyKey, TransactGetOp, TransactWriteOp};

use super::index::{IndexMeta, enqueue_async_indexes, fetch_write_path_indexes, sync_indexes};
use super::tx_helpers::{
    check_idempotency_token_in_tx, delete_item_in_tx, fetch_item_for_update, fetch_item_in_tx,
    insert_item_if_absent_in_tx, upsert_item_in_tx, write_stream_record_in_tx,
};
use crate::PostgresEngine;

/// Bound on insert retries when a transactional write to a nonexistent item
/// keeps losing the create race to writers that then roll back. Mirrors the
/// same bound on the non-transactional `UpdateItem` path (`update_item.rs`).
const MAX_CREATE_RACE_ATTEMPTS: u32 = 5;

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
        idempotency: Option<IdempotencyKey<'_>>,
    ) -> Result<(), StorageError> {
        // Pre-fetch indexes for each unique table involved in the transaction.
        //
        // Vector indexes are read here too, once per table rather than once per op,
        // and from the catalog rather than from the cached key info: a cached empty
        // set would make a transaction skip an index another request has just
        // created. One read per table also keeps a multi-op transaction from
        // re-asking for the same answer.
        let mut table_indexes: HashMap<String, Vec<IndexMeta>> = HashMap::new();
        let mut table_vector_metas: HashMap<
            String,
            Vec<(extenddb_storage::vector_lifecycle::VectorIndexMeta, String)>,
        > = HashMap::new();
        for op in ops {
            let name = transact_op_table_name(op);
            if !table_indexes.contains_key(name) {
                let tid = transact_op_table_id(op);
                let (indexes, vector_metas) = fetch_write_path_indexes(tid, &self.pool).await?;
                table_indexes.insert(name.to_owned(), indexes);
                table_vector_metas.insert(name.to_owned(), vector_metas);
            }
        }

        // D-4: Read the system default delay live (P119), so a runtime change
        // applies to this transaction rather than up to 30 s later.
        let sys_delay = self.index_propagation_delay().await;

        let mut tx = self
            .data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Check the idempotency token within the transaction so token storage
        // and data writes commit together. The token is scoped to its account.
        if let Some(key) = idempotency {
            check_idempotency_token_in_tx(&mut tx, key.account_id, key.token, key.fingerprint)
                .await?;
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

            // Vector maintenance for all three write kinds in one place, rather
            // than in each branch above: this loop already visits exactly the ops
            // that changed an item, with both images in hand, and it runs inside
            // the same transaction. Three call sites would have been three chances
            // to diverge on which image is passed.
            let vector_n = crate::data::vector_index::maintain_vector_indexes(
                &mut tx,
                &table_vector_metas[transact_op_table_name(op)],
                &key_info.table_id,
                &key_info.key_schema,
                &key_info.attribute_definitions,
                old_item.as_ref(),
                new_item.as_ref(),
                sys_delay,
            )
            .await?;
            if n > 0 || vector_n > 0 {
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
            let mut existing = fetch_item_for_update(tx, key_info, item)
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
            if existing.is_some() {
                // The locking read above holds the row: no concurrent writer can
                // slip between the condition check and this write.
                upsert_item_in_tx(tx, key_info, item)
                    .await
                    .map_err(TxnOpError::Storage)?;
            } else {
                // No row existed for the read above to lock, so a concurrent
                // transaction may create the item first. Arbitrate with an
                // atomic insert-if-absent. On losing, re-read FOR UPDATE (the
                // arbiter insert already waited out the winner, so its row is
                // committed and lockable) and re-evaluate
                // the condition against the winner's committed item, so an
                // `attribute_not_exists` racer cancels with
                // ConditionalCheckFailed instead of silently overwriting the
                // winner, and `sync_indexes` sees the winner as the old item
                // instead of colliding on a bare index insert. Same race, same
                // shape, as the measured fix on the non-transactional
                // UpdateItem path.
                let mut attempt: u32 = 0;
                loop {
                    attempt += 1;
                    if insert_item_if_absent_in_tx(tx, key_info, item)
                        .await
                        .map_err(TxnOpError::Storage)?
                    {
                        break;
                    }
                    let winner = fetch_item_for_update(tx, key_info, item)
                        .await
                        .map_err(TxnOpError::Storage)?;
                    if let Some(winner) = winner {
                        eval_condition(
                            *condition,
                            &winner,
                            maps,
                            *return_values_on_ccf,
                            Some(&winner),
                        )?;
                        // The re-read locked the winner's row; safe to overwrite.
                        upsert_item_in_tx(tx, key_info, item)
                            .await
                            .map_err(TxnOpError::Storage)?;
                        existing = Some(winner);
                        break;
                    }
                    // No committed winner is visible: the conflicting insert was
                    // followed by a delete before our re-read. Retry the insert.
                    if attempt >= MAX_CREATE_RACE_ATTEMPTS {
                        // Sustained create-then-delete churn. Surface as a
                        // canceled transaction with a TransactionConflict
                        // reason, the DDB-canonical contention shape, never a
                        // 500 (matches the MongoDB backend's exhaustion path).
                        return Err(TxnOpError::Cancel(CancellationReason {
                            code: "TransactionConflict".to_owned(),
                            message: Some("Transaction is ongoing for the item".to_owned()),
                            item: None,
                        }));
                    }
                }
            }
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
            // Only delete a row the locking read actually saw (and locked).
            // When the read found nothing there is nothing to lock, so a
            // concurrent transaction can create and commit the item before our
            // DELETE runs; its fresh READ COMMITTED snapshot would then see
            // and kill the winner's row with no stream record and orphaned
            // index rows. Deleting a nonexistent item is a no-op in the real
            // service, so skipping the write is the faithful serialization
            // (this delete simply ordered before the concurrent create).
            if existing.is_some() {
                delete_item_in_tx(tx, key_info, key)
                    .await
                    .map_err(TxnOpError::Storage)?;
            }
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
            let mut existing = fetch_item_for_update(tx, key_info, key)
                .await
                .map_err(TxnOpError::Storage)?;
            // Compute and validate the post-update item from a given base.
            // Runs once on the fast path, and again when a create race is
            // lost and the loser's expression must re-apply on top of the
            // winner's committed item (the merge semantics measured against
            // the real service on the non-transactional UpdateItem path).
            let idx_refs = index_key_refs(indexes);
            let compute_item = |base: Option<&Item>| -> Result<Item, TxnOpError> {
                let mut item = base.cloned().unwrap_or_else(|| (*key).clone());
                expression::apply_update_validated(
                    actions,
                    &mut item,
                    maps,
                    &key_info.vector_indexes,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| {
                    TxnOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;
                // Validate post-update item size
                validation::validate_item_size(&item, max_item_size_bytes).map_err(|e| {
                    TxnOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;
                // Secondary-index key validation on the post-update item: a type
                // mismatch is a cancellation reason; setting an index key to an
                // empty value is a top-level ValidationException.
                validation::validate_index_key_types(
                    &item,
                    &idx_refs,
                    &key_info.attribute_definitions,
                )
                .map_err(|e| {
                    TxnOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;
                validation::validate_index_key_not_empty(
                    &item,
                    &idx_refs,
                    validation::SecondaryIndexEmptyContext::UpdateExpression,
                )
                .map_err(|e| TxnOpError::Validation(e.to_string()))?;
                Ok(item)
            };
            // Evaluate condition against empty item if non-existent (DynamoDB semantics)
            let empty = Item::new();
            eval_condition(
                *condition,
                existing.as_ref().unwrap_or(&empty),
                maps,
                *return_values_on_ccf,
                existing.as_ref(),
            )?;
            let mut item = compute_item(existing.as_ref())?;
            if existing.is_some() {
                // The locking read above holds the row: no concurrent writer can
                // slip between the condition check and this write.
                upsert_item_in_tx(tx, key_info, &item)
                    .await
                    .map_err(TxnOpError::Storage)?;
            } else {
                // Same create race as the transactional Put above: no row
                // existed to lock, so arbitrate with insert-if-absent and, on
                // losing, re-evaluate the condition against the winner and
                // re-apply the update expression on top of its item.
                let mut attempt: u32 = 0;
                loop {
                    attempt += 1;
                    if insert_item_if_absent_in_tx(tx, key_info, &item)
                        .await
                        .map_err(TxnOpError::Storage)?
                    {
                        break;
                    }
                    let winner = fetch_item_for_update(tx, key_info, key)
                        .await
                        .map_err(TxnOpError::Storage)?;
                    if let Some(winner) = winner {
                        eval_condition(
                            *condition,
                            &winner,
                            maps,
                            *return_values_on_ccf,
                            Some(&winner),
                        )?;
                        item = compute_item(Some(&winner))?;
                        // The re-read locked the winner's row; safe to overwrite.
                        upsert_item_in_tx(tx, key_info, &item)
                            .await
                            .map_err(TxnOpError::Storage)?;
                        existing = Some(winner);
                        break;
                    }
                    // No committed winner is visible: the conflicting insert was
                    // followed by a delete before our re-read. Retry the insert.
                    if attempt >= MAX_CREATE_RACE_ATTEMPTS {
                        // Sustained create-then-delete churn. Same
                        // TransactionConflict cancellation as the Put arm.
                        return Err(TxnOpError::Cancel(CancellationReason {
                            code: "TransactionConflict".to_owned(),
                            message: Some("Transaction is ongoing for the item".to_owned()),
                            item: None,
                        }));
                    }
                }
            }
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
