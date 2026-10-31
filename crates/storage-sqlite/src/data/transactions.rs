// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `TransactGetItems` / `TransactWriteItems` and idempotency-token cleanup.
//!
//! Writes run under the engine write lock (D1) in a single transaction, so all
//! operations, the idempotency check, and stream capture commit atomically or
//! roll back together. Reads run in one transaction for a consistent snapshot.

use std::collections::HashMap;

use extenddb_core::expression::{self, ExpressionMaps};
use extenddb_core::types::{CancellationReason, Item, ReturnValuesOnConditionCheckFailure};
use extenddb_core::validation;
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    IdempotencyKey, TransactGetOp, TransactWriteOp, transact_key_check_pass, transact_op_key_checks,
};

use super::index::{
    IndexMeta, enqueue_async_indexes, fetch_indexes_for_table, fresh_attr_defs, sync_indexes,
};
use super::tx_helpers::{
    check_idempotency_token_in_tx, delete_item_in_tx, fetch_item_for_update, fetch_item_in_tx,
    upsert_item_in_tx, write_stream_record_in_tx,
};
use crate::sqlite_util::format_timestamp;
use crate::store::SqliteEngine;

impl SqliteEngine {
    pub(crate) async fn transact_get_items_impl(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> Result<Vec<Option<Item>>, StorageError> {
        // Validate keys first, collecting per-item reasons (all-or-nothing).
        let mut reasons = Vec::with_capacity(ops.len());
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
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let mut results = Vec::with_capacity(ops.len());
        for op in ops {
            results.push(fetch_item_in_tx(&mut tx, op.key_info, op.key).await?);
        }
        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(results)
    }

    pub(crate) async fn transact_write_items_impl(
        &self,
        ops: &[TransactWriteOp<'_>],
        idempotency: Option<IdempotencyKey<'_>>,
    ) -> Result<(), StorageError> {
        // Read the propagation delay BEFORE taking the write lock. It is a
        // runtime setting, not an invariant of this write, so it does not need
        // to be read under the lock, and the lock serialises every write in the
        // process: work done inside it is the backend's throughput bottleneck.
        let system_delay = self.index_propagation_delay().await;
        let _writer = self.write_lock.lock().await;
        // Fetch index metadata per distinct table AFTER acquiring the write lock,
        // so a GSI added by a concurrent UpdateTable (same lock) is not missed and
        // left unmaintained by these writes.
        let mut table_indexes: HashMap<String, Vec<IndexMeta>> = HashMap::new();
        for op in ops {
            let name = op_table_name(op);
            if !table_indexes.contains_key(name) {
                let indexes = fetch_indexes_for_table(op_table_id(op), &self.pool).await?;
                table_indexes.insert(name.to_owned(), indexes);
            }
        }

        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if let Some(key) = idempotency {
            // Idempotency is per-account: the same ClientRequestToken in two
            // accounts must not collide. The engine passes the caller's
            // account explicitly.
            check_idempotency_token_in_tx(&mut tx, key.account_id, key.token, key.fingerprint)
                .await?;
        }

        // Key checks first, for every member, before any row is read or
        // locked. They need only the request and the table metadata, and a
        // fault among them decides the request under the service's precedence
        // (the first such fault in request order), so a decided request never
        // waits on a later member's row. Dropping `tx` rolls back the
        // idempotency token checked above.
        transact_key_check_pass(ops.iter().map(|op| {
            let indexes = &table_indexes[op_table_name(op)];
            transact_op_key_checks(
                op,
                &index_key_refs(indexes),
                fresh_attr_defs(indexes, &op_key_info(op).attribute_definitions),
            )
        }))?;

        let mut reasons: Vec<CancellationReason> = Vec::with_capacity(ops.len());
        let mut op_items: Vec<(Option<Item>, Option<Item>)> = Vec::with_capacity(ops.len());
        let mut any_failed = false;

        for op in ops {
            let indexes = &table_indexes[op_table_name(op)];
            match execute_transact_write_op(
                &mut tx,
                op,
                indexes,
                self.max_item_size_bytes,
                system_delay,
            )
            .await
            {
                Ok(items) => {
                    op_items.push(items);
                    reasons.push(CancellationReason::none());
                }
                Err(TxnOpError::Cancel(r)) => {
                    op_items.push((None, None));
                    any_failed = true;
                    reasons.push(r);
                }
                Err(TxnOpError::Storage(e)) => return Err(e),
            }
        }

        if any_failed {
            // Dropping `tx` without commit rolls back all writes. Key faults
            // left the members as internal reasons; the shared resolver applies
            // the service's cross-member precedence.
            return Err(match CancellationReason::resolve_key_faults(reasons) {
                Ok(reasons) => StorageError::TransactionCanceled(reasons),
                Err(message) => StorageError::Validation(message),
            });
        }

        // Capture stream records after all writes are staged.
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
                    op_key_info(op),
                    capture,
                    old_item.as_ref(),
                    new_item.as_ref(),
                )
                .await?;
            }
        }

        // Persist index work for each op inside the same transaction: async GSI
        // rows, and vector maintenance which is applied here when the propagation
        // delay is 0 and enqueued otherwise.
        //
        // Deliberately after every op is staged rather than inside each op, so
        // there is one call site instead of three. Both paths stay atomic with the
        // item writes either way: the synchronous apply runs in this transaction,
        // and a pending row is inserted into it, so a cancelled transaction leaves
        // neither behind. The vector apply reads only the op's own before/after
        // items, never the base table, so its position relative to the other ops'
        // staged writes cannot change its result.
        let mut needs_notify = false;
        for (op, (old_item, new_item)) in ops.iter().zip(op_items.iter()) {
            let indexes = &table_indexes[op_table_name(op)];
            if old_item.is_some() || new_item.is_some() {
                let n = enqueue_async_indexes(
                    &mut tx,
                    op_key_info(op),
                    indexes,
                    old_item.as_ref(),
                    new_item.as_ref(),
                    system_delay,
                )
                .await?;
                if n > 0 {
                    needs_notify = true;
                }
                let key_info = op_key_info(op);
                if !key_info.vector_indexes.is_empty() {
                    let n = crate::data::vector_index::maintain_vector_indexes(
                        &mut tx,
                        &key_info.table_id,
                        &key_info.key_schema,
                        &key_info.attribute_definitions,
                        old_item.as_ref(),
                        new_item.as_ref(),
                        system_delay,
                    )
                    .await?;
                    if n > 0 {
                        needs_notify = true;
                    }
                }
            }
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if needs_notify {
            self.gsi_notify.notify_waiters();
        }
        Ok(())
    }

    pub(crate) async fn cleanup_expired_idempotency_tokens_impl(
        &self,
        max_age_seconds: i64,
    ) -> Result<u64, StorageError> {
        // D1: every writer holds the engine write lock.
        let _writer = self.write_lock.lock().await;
        let cutoff = format_timestamp(
            time::OffsetDateTime::now_utc() - time::Duration::seconds(max_age_seconds),
        );
        let result = sqlx::query("DELETE FROM idempotency_tokens WHERE created_at < ?")
            .bind(&cutoff)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(result.rows_affected())
    }
}

fn op_table_name<'a>(op: &'a TransactWriteOp<'_>) -> &'a str {
    &op_key_info(op).table_name
}

fn op_table_id<'a>(op: &'a TransactWriteOp<'_>) -> &'a str {
    &op_key_info(op).table_id
}

fn op_key_info<'a>(op: &'a TransactWriteOp<'_>) -> &'a extenddb_core::types::TableKeyInfo {
    match op {
        TransactWriteOp::Put { key_info, .. }
        | TransactWriteOp::Delete { key_info, .. }
        | TransactWriteOp::Update { key_info, .. }
        | TransactWriteOp::ConditionCheck { key_info, .. } => key_info,
    }
}

enum TxnOpError {
    Cancel(CancellationReason),
    Storage(StorageError),
}

/// Build [`validation::IndexKeyRef`] views over the table's indexes for
/// secondary-index key validation.
pub(crate) fn index_key_refs(indexes: &[IndexMeta]) -> Vec<validation::IndexKeyRef<'_>> {
    indexes
        .iter()
        .map(|idx| validation::IndexKeyRef {
            index_name: &idx.index_name,
            key_schema: &idx.key_schema,
        })
        .collect()
}

/// Execute one transact-write op, returning `(old, new)` images for stream
/// capture, or a cancellation reason on a failed condition / validation.
async fn execute_transact_write_op(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    op: &TransactWriteOp<'_>,
    indexes: &[IndexMeta],
    max_item_size_bytes: usize,
    system_delay: u64,
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
            // Key checks ran in the pre-pass; repeated so the arm stays
            // correct if called on its own.
            transact_op_key_checks(
                op,
                &index_key_refs(indexes),
                fresh_attr_defs(indexes, &key_info.attribute_definitions),
            )
            .map_err(TxnOpError::Cancel)?;
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
                    system_delay,
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
            .map_err(|e| {
                TxnOpError::Cancel(CancellationReason::key_type_mismatch(e.to_string()))
            })?;
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
                    system_delay,
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
            let idx_refs = index_key_refs(indexes);
            let attr_defs = fresh_attr_defs(indexes, &key_info.attribute_definitions);
            transact_op_key_checks(op, &idx_refs, attr_defs).map_err(TxnOpError::Cancel)?;
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
            let mut item = existing.clone().unwrap_or_else(|| (*key).clone());
            expression::apply_update_validated(
                actions,
                &mut item,
                maps,
                &key_info.vector_indexes,
                &key_info.attribute_definitions,
            )
            .map_err(|e| TxnOpError::Cancel(CancellationReason::validation_error(e.to_string())))?;
            // A transacted Update is sized on the finished item, with the update
            // wording in the cancellation reason.
            validation::validate_transact_update_item_size(&item, max_item_size_bytes).map_err(
                |e| TxnOpError::Cancel(CancellationReason::validation_error(e.to_string())),
            )?;
            // Secondary-index keys on the written item: a wrong-typed value
            // the statement wrote from an attribute reference or from
            // if_not_exists, reported with the service's computed-value
            // wording, and a key left empty. With the path absent the
            // if_not_exists fallback is a literal from the request, which the
            // service ranks with the values taken from the item. The type
            // fault is a plain positional reason: the service leaves the
            // other members' outcomes in place beside it, where a wrong type
            // supplied in the request (the pre-pass) masks them.
            validation::validate_index_key_types_after_update(&item, &idx_refs, attr_defs)
                .map_err(|e| {
                    TxnOpError::Cancel(CancellationReason::validation_error(e.to_string()))
                })?;
            validation::validate_index_key_not_empty(
                &item,
                &idx_refs,
                validation::SecondaryIndexEmptyContext::UpdateExpression,
            )
            .map_err(|e| {
                TxnOpError::Cancel(CancellationReason::index_key_empty_abort(e.to_string()))
            })?;
            // REMOVE and DELETE on a key that holds no item change nothing: the
            // service writes no item and emits no stream record (issue #510).
            // The actions were still applied above so that their validation
            // errors surface the same way as on an existing item, and the
            // condition was evaluated above, so a failing condition still
            // cancels the transaction. Returning no images keeps the stream
            // record and the index work out of this op.
            if existing.is_none() && expression::removes_only(actions) {
                return Ok((None, None));
            }
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
                    system_delay,
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
            .map_err(|e| {
                TxnOpError::Cancel(CancellationReason::key_type_mismatch(e.to_string()))
            })?;
            let existing = fetch_item_for_update(tx, key_info, key)
                .await
                .map_err(TxnOpError::Storage)?;
            let empty = Item::new();
            eval_condition(
                Some(condition),
                existing.as_ref().unwrap_or(&empty),
                maps,
                *return_values_on_ccf,
                existing.as_ref(),
            )?;
            Ok((None, None))
        }
    }
}

/// Evaluate a transaction op's condition, producing a `CancellationReason` on
/// failure (attaching the old item when `ReturnValuesOnConditionCheckFailure`
/// is `AllOld`).
fn eval_condition(
    condition: Option<&expression::Expr>,
    item: &Item,
    maps: &ExpressionMaps,
    return_values_on_ccf: ReturnValuesOnConditionCheckFailure,
    existing: Option<&Item>,
) -> Result<(), TxnOpError> {
    if let Some(cond) = condition {
        let passed = expression::evaluate_condition(cond, item, maps)
            .map_err(|e| TxnOpError::Cancel(CancellationReason::validation_error(e.to_string())))?;
        if !passed {
            let returned = if return_values_on_ccf == ReturnValuesOnConditionCheckFailure::AllOld {
                existing.cloned()
            } else {
                None
            };
            return Err(TxnOpError::Cancel(
                CancellationReason::condition_check_failed_with_item(returned),
            ));
        }
    }
    Ok(())
}
