// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transactional read/write implementations for the `TiDB` backend.

use extenddb_core::expression::{self, ExpressionMaps};
use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    AttributeValue, CancellationReason, Item, KeySchemaElement,
    ReturnValuesOnConditionCheckFailure, ScalarAttributeType, StreamEventName, TableKeyInfo,
};
use extenddb_core::validation;
use extenddb_storage::StreamCapture;
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{SortKeyValue, parse_sk, sk_column, sk_info};
use extenddb_storage::{TransactGetOp, TransactWriteOp};

use super::batch_write::{
    PreparedDelete, PreparedPut, execute_batch_deletes, execute_batch_puts, prepare_batch_delete,
    prepare_batch_put,
};
use super::index::validate_item_index_key_constraints;
use super::tx_helpers::{
    StreamSequenceAllocator, check_idempotency_token_in_tx, delete_item_in_tx,
    delete_item_without_old_item_in_tx, fetch_item_for_update, finalize_stream_records_best_effort,
    put_item_without_old_item_in_tx, stream_capture_needs_old_item, upsert_item_in_tx,
    write_stream_record_for_event_in_tx, write_stream_record_in_tx,
};
use super::{data_table_name, json_to_item, physical_pk_bytes, repeat_tuple_placeholders};
use crate::TidbEngine;
use crate::tidb_util::retry_tidb_idempotent_operation;

type TxnGetRowsQuery<'q, O> = sqlx::query::QueryAs<'q, sqlx::MySql, O, sqlx::mysql::MySqlArguments>;

#[derive(Clone, Debug, PartialEq)]
struct TransactGetLookupKey {
    pk: Vec<u8>,
    sk: Option<TransactGetSortKey>,
}

#[derive(Clone, Debug, PartialEq)]
enum TransactGetSortKey {
    Bytes(Vec<u8>),
    Number(bigdecimal::BigDecimal),
}

struct TransactGetEntry {
    result_index: usize,
    lookup_key: TransactGetLookupKey,
}

struct TransactGetGroup<'a> {
    key_info: &'a TableKeyInfo,
    entries: Vec<TransactGetEntry>,
}

#[derive(Default)]
struct NativeTxnWriteBatch<'a> {
    groups: Vec<NativeTxnWriteGroup<'a>>,
}

struct NativeTxnWriteGroup<'a> {
    key_info: &'a TableKeyInfo,
    puts: Vec<PreparedPut>,
    deletes: Vec<PreparedDelete>,
}

impl TidbEngine {
    /// Implementation of `DataEngine::transact_get_items`.
    pub(crate) async fn transact_get_items_impl(
        &self,
        ops: &[TransactGetOp<'_>],
    ) -> Result<Vec<Option<Item>>, StorageError> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }

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

        let groups = transact_get_groups(ops)?;

        // Plain reads inside one TiDB transaction share a snapshot. TiDB treats
        // MySQL's READ ONLY transaction syntax as a disabled no-op feature, so
        // do not use START TRANSACTION READ ONLY here.
        let mut tx = self
            .data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut results = vec![None; ops.len()];
        for group in &groups {
            fetch_transact_get_group(&mut tx, group, &mut results).await?;
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
        let mut op_outcomes = Vec::with_capacity(ops.len());
        let mut native_batch = NativeTxnWriteBatch::default();
        let mut any_failed = false;

        for op in ops {
            let indexes = &transact_op_key_info(op).secondary_index_key_schemas;
            let reason = match stage_native_transact_write_op(
                op,
                indexes,
                &self.limits,
                &mut native_batch,
            ) {
                Ok(Some(outcome)) => Ok(outcome),
                Ok(None) => execute_transact_write_op(&mut tx, op, indexes, &self.limits).await,
                Err(err) => Err(err),
            };
            match reason {
                Ok(outcome) => {
                    op_outcomes.push(outcome);
                    reasons.push(CancellationReason::none());
                }
                Err(TxnOpError::Cancel(r)) => {
                    op_outcomes.push(TxnWriteOutcome::NoStream);
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

        native_batch.execute(&mut tx).await?;

        // Write stream records atomically within the transaction.
        let mut sequence_allocator = StreamSequenceAllocator::default();
        for (op, outcome) in ops.iter().zip(op_outcomes.iter()) {
            let Some(capture) = transact_op_stream_capture(op) else {
                continue;
            };
            let key_info = transact_op_key_info(op);
            match outcome {
                TxnWriteOutcome::NoStream => {}
                TxnWriteOutcome::StreamFromItems { old_item, new_item } => {
                    write_stream_record_in_tx(
                        &mut tx,
                        &mut sequence_allocator,
                        key_info,
                        capture,
                        old_item.as_ref(),
                        new_item.as_ref(),
                    )
                    .await?;
                }
                TxnWriteOutcome::StreamFromEvent {
                    event,
                    source_item,
                    old_item,
                    new_item,
                } => {
                    write_stream_record_for_event_in_tx(
                        &mut tx,
                        &mut sequence_allocator,
                        key_info,
                        capture,
                        *event,
                        source_item,
                        old_item.as_ref(),
                        new_item.as_ref(),
                    )
                    .await?;
                }
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

fn transact_get_groups<'a>(
    ops: &'a [TransactGetOp<'a>],
) -> Result<Vec<TransactGetGroup<'a>>, StorageError> {
    let mut groups: Vec<TransactGetGroup<'a>> = Vec::new();

    for (result_index, op) in ops.iter().enumerate() {
        let lookup_key = transact_get_lookup_key(op.key_info, op.key)?;
        if let Some(group) = groups
            .iter_mut()
            .find(|group| group.key_info.table_id == op.key_info.table_id)
        {
            group.entries.push(TransactGetEntry {
                result_index,
                lookup_key,
            });
        } else {
            groups.push(TransactGetGroup {
                key_info: op.key_info,
                entries: vec![TransactGetEntry {
                    result_index,
                    lookup_key,
                }],
            });
        }
    }

    Ok(groups)
}

fn transact_get_lookup_key(
    key_info: &TableKeyInfo,
    key: &Item,
) -> Result<TransactGetLookupKey, StorageError> {
    let pk = physical_pk_bytes(key, &key_info.key_schema)?;
    let sk = if let Some((sk_name, sk_type)) =
        sk_info(&key_info.key_schema, &key_info.attribute_definitions)
    {
        let sk_value = key.get(sk_name).ok_or_else(|| {
            StorageError::Internal(format!("missing sort key attribute {sk_name}"))
        })?;
        let sk = parse_sk(sk_value, sk_type)?;
        Some(transact_get_sort_key(sk))
    } else {
        None
    };
    Ok(TransactGetLookupKey { pk, sk })
}

fn transact_get_sort_key(sk: SortKeyValue) -> TransactGetSortKey {
    match sk {
        SortKeyValue::S(s) => TransactGetSortKey::Bytes(s.into_bytes()),
        SortKeyValue::N(n) => TransactGetSortKey::Number(n),
        SortKeyValue::B(b) => TransactGetSortKey::Bytes(b),
    }
}

async fn fetch_transact_get_group(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    group: &TransactGetGroup<'_>,
    results: &mut [Option<Item>],
) -> Result<(), StorageError> {
    let ddb_table = data_table_name(&group.key_info.table_id);
    let fetched = if let Some((_, sk_type)) = sk_info(
        &group.key_info.key_schema,
        &group.key_info.attribute_definitions,
    ) {
        let sk_col = sk_column(sk_type);
        let sql = transact_get_pk_sk_sql(&ddb_table, sk_col, group.entries.len());
        match sk_type {
            ScalarAttributeType::S | ScalarAttributeType::B => {
                let mut query = sqlx::query_as::<_, (Vec<u8>, Vec<u8>, serde_json::Value)>(&sql);
                for entry in &group.entries {
                    query = query.bind(entry.lookup_key.pk.clone());
                    query = bind_transact_get_sort_key(
                        query,
                        entry.lookup_key.sk.as_ref().ok_or_else(|| {
                            StorageError::Internal(
                                "missing prepared transaction sort key".to_owned(),
                            )
                        })?,
                    );
                }
                let rows = query
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                rows.into_iter()
                    .map(|(pk, sk, json)| {
                        Ok((
                            TransactGetLookupKey {
                                pk,
                                sk: Some(TransactGetSortKey::Bytes(sk)),
                            },
                            json_to_item(json)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, StorageError>>()?
            }
            ScalarAttributeType::N => {
                let mut query =
                    sqlx::query_as::<_, (Vec<u8>, bigdecimal::BigDecimal, serde_json::Value)>(&sql);
                for entry in &group.entries {
                    query = query.bind(entry.lookup_key.pk.clone());
                    query = bind_transact_get_sort_key(
                        query,
                        entry.lookup_key.sk.as_ref().ok_or_else(|| {
                            StorageError::Internal(
                                "missing prepared transaction sort key".to_owned(),
                            )
                        })?,
                    );
                }
                let rows = query
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                rows.into_iter()
                    .map(|(pk, sk, json)| {
                        Ok((
                            TransactGetLookupKey {
                                pk,
                                sk: Some(TransactGetSortKey::Number(sk)),
                            },
                            json_to_item(json)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, StorageError>>()?
            }
        }
    } else {
        let sql = transact_get_pk_sql(&ddb_table, group.entries.len());
        let mut query = sqlx::query_as::<_, (Vec<u8>, serde_json::Value)>(&sql);
        for entry in &group.entries {
            query = query.bind(entry.lookup_key.pk.clone());
        }
        let rows = query
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        rows.into_iter()
            .map(|(pk, json)| Ok((TransactGetLookupKey { pk, sk: None }, json_to_item(json)?)))
            .collect::<Result<Vec<_>, StorageError>>()?
    };

    assign_transact_get_group_results(group, &fetched, results);
    Ok(())
}

fn bind_transact_get_sort_key<'q, O>(
    query: TxnGetRowsQuery<'q, O>,
    sk: &TransactGetSortKey,
) -> TxnGetRowsQuery<'q, O> {
    match sk {
        TransactGetSortKey::Bytes(bytes) => query.bind(bytes.clone()),
        TransactGetSortKey::Number(number) => query.bind(number.clone()),
    }
}

fn assign_transact_get_group_results(
    group: &TransactGetGroup<'_>,
    fetched: &[(TransactGetLookupKey, Item)],
    results: &mut [Option<Item>],
) {
    for entry in &group.entries {
        if let Some((_, item)) = fetched
            .iter()
            .find(|(lookup_key, _)| lookup_key == &entry.lookup_key)
        {
            results[entry.result_index] = Some(item.clone());
        }
    }
}

fn transact_get_pk_sql(table: &str, key_count: usize) -> String {
    format!(
        "SELECT pk, item_data FROM {table} WHERE pk IN ({})",
        repeat_tuple_placeholders(key_count, 1)
    )
}

fn transact_get_pk_sk_sql(table: &str, sk_col: &str, key_count: usize) -> String {
    format!(
        "SELECT pk, {sk_col}, item_data FROM {table} WHERE (pk, {sk_col}) IN ({})",
        repeat_tuple_placeholders(key_count, 2)
    )
}

fn transact_op_key_info<'a>(op: &'a TransactWriteOp<'_>) -> &'a extenddb_core::types::TableKeyInfo {
    match op {
        TransactWriteOp::Put { key_info, .. }
        | TransactWriteOp::Delete { key_info, .. }
        | TransactWriteOp::Update { key_info, .. }
        | TransactWriteOp::ConditionCheck { key_info, .. } => key_info,
    }
}

fn transact_op_stream_capture<'a>(op: &'a TransactWriteOp<'_>) -> Option<&'a StreamCapture> {
    match op {
        TransactWriteOp::Put { stream, .. }
        | TransactWriteOp::Delete { stream, .. }
        | TransactWriteOp::Update { stream, .. } => stream.as_ref(),
        TransactWriteOp::ConditionCheck { .. } => None,
    }
}

fn transact_put_needs_existing_item(
    condition: Option<&extenddb_core::expression::Expr>,
    stream: &Option<StreamCapture>,
) -> bool {
    condition.is_some() || stream.as_ref().is_some_and(stream_capture_needs_old_item)
}

fn transact_delete_needs_existing_item(
    condition: Option<&extenddb_core::expression::Expr>,
    stream: &Option<StreamCapture>,
) -> bool {
    condition.is_some() || stream.as_ref().is_some_and(stream_capture_needs_old_item)
}

impl<'a> NativeTxnWriteBatch<'a> {
    fn push_put(&mut self, key_info: &'a TableKeyInfo, item: &Item) -> Result<(), StorageError> {
        let sk = sk_info(&key_info.key_schema, &key_info.attribute_definitions);
        let prepared = prepare_batch_put(key_info, item, sk)?;
        self.group_mut(key_info).puts.push(prepared);
        Ok(())
    }

    fn push_delete(&mut self, key_info: &'a TableKeyInfo, key: &Item) -> Result<(), StorageError> {
        let sk = sk_info(&key_info.key_schema, &key_info.attribute_definitions);
        let prepared = prepare_batch_delete(key_info, key, sk)?;
        self.group_mut(key_info).deletes.push(prepared);
        Ok(())
    }

    fn group_mut(&mut self, key_info: &'a TableKeyInfo) -> &mut NativeTxnWriteGroup<'a> {
        if let Some(pos) = self
            .groups
            .iter()
            .position(|group| group.key_info.table_id == key_info.table_id)
        {
            return &mut self.groups[pos];
        }

        self.groups.push(NativeTxnWriteGroup {
            key_info,
            puts: Vec::new(),
            deletes: Vec::new(),
        });
        self.groups.last_mut().expect("just pushed write group")
    }

    async fn execute(
        self,
        tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    ) -> Result<(), StorageError> {
        for group in self.groups {
            execute_native_txn_write_group(tx, group).await?;
        }
        Ok(())
    }
}

async fn execute_native_txn_write_group(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    group: NativeTxnWriteGroup<'_>,
) -> Result<(), StorageError> {
    let ddb_table = data_table_name(&group.key_info.table_id);
    let sk_type = sk_info(
        &group.key_info.key_schema,
        &group.key_info.attribute_definitions,
    )
    .map(|(_, ty)| ty);

    if !group.puts.is_empty() {
        execute_batch_puts(&mut **tx, &ddb_table, sk_type, group.puts).await?;
    }
    if !group.deletes.is_empty() {
        execute_batch_deletes(&mut **tx, &ddb_table, sk_type, group.deletes).await?;
    }

    Ok(())
}

fn stage_native_transact_write_op<'a>(
    op: &TransactWriteOp<'a>,
    indexes: &[Vec<KeySchemaElement>],
    limits: &LimitsConfig,
    batch: &mut NativeTxnWriteBatch<'a>,
) -> Result<Option<TxnWriteOutcome>, TxnOpError> {
    match op {
        TransactWriteOp::Put {
            key_info,
            item,
            condition: None,
            stream: None,
            ..
        } => {
            validate_transact_put(key_info, item, indexes, limits)?;
            batch
                .push_put(key_info, item)
                .map_err(TxnOpError::Storage)?;
            Ok(Some(TxnWriteOutcome::NoStream))
        }
        TransactWriteOp::Delete {
            key_info,
            key,
            condition: None,
            stream: None,
            ..
        } => {
            validate_transact_key_only(key_info, key)?;
            batch
                .push_delete(key_info, key)
                .map_err(TxnOpError::Storage)?;
            Ok(Some(TxnWriteOutcome::NoStream))
        }
        _ => Ok(None),
    }
}

/// Error type for individual transactional write operations.
///
/// Separates user-driven cancellations (condition failures, validation errors)
/// from infrastructure errors (connection failures, transaction errors).
/// This prevents internal error details from leaking into client-visible
/// cancellation reasons.
#[derive(Debug)]
enum TxnOpError {
    /// User-driven failure — becomes a per-item cancellation reason.
    Cancel(CancellationReason),
    /// Infrastructure failure — bubbles up as `StorageError::Internal`.
    Storage(StorageError),
}

enum TxnWriteOutcome {
    NoStream,
    StreamFromItems {
        old_item: Option<Item>,
        new_item: Option<Item>,
    },
    StreamFromEvent {
        event: StreamEventName,
        source_item: Item,
        old_item: Option<Item>,
        new_item: Option<Item>,
    },
}

impl From<CancellationReason> for TxnOpError {
    fn from(r: CancellationReason) -> Self {
        Self::Cancel(r)
    }
}

/// Execute a single transactional write operation, including native index-key validation.
/// Returns stream capture material on success.
async fn execute_transact_write_op(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    op: &TransactWriteOp<'_>,
    indexes: &[Vec<KeySchemaElement>],
    limits: &LimitsConfig,
) -> Result<TxnWriteOutcome, TxnOpError> {
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
            validate_transact_put(key_info, item, indexes, limits)?;
            if !transact_put_needs_existing_item(*condition, stream) {
                let event = if stream.is_some() {
                    Some(
                        put_item_without_old_item_in_tx(tx, key_info, item)
                            .await
                            .map_err(TxnOpError::Storage)?,
                    )
                } else {
                    upsert_item_in_tx(tx, key_info, item)
                        .await
                        .map_err(TxnOpError::Storage)?;
                    None
                };
                return Ok(match event {
                    Some(event) => TxnWriteOutcome::StreamFromEvent {
                        event,
                        source_item: (*item).clone(),
                        old_item: None,
                        new_item: Some((*item).clone()),
                    },
                    None => TxnWriteOutcome::NoStream,
                });
            }

            let existing = {
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
            };
            upsert_item_in_tx(tx, key_info, item)
                .await
                .map_err(TxnOpError::Storage)?;
            Ok(if stream.is_some() {
                TxnWriteOutcome::StreamFromItems {
                    old_item: existing,
                    new_item: Some((*item).clone()),
                }
            } else {
                TxnWriteOutcome::NoStream
            })
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
            validate_transact_key_only(key_info, key)?;
            if !transact_delete_needs_existing_item(*condition, stream) {
                let removed = delete_item_without_old_item_in_tx(tx, key_info, key)
                    .await
                    .map_err(TxnOpError::Storage)?;
                return Ok(if stream.is_some() && removed {
                    TxnWriteOutcome::StreamFromEvent {
                        event: StreamEventName::Remove,
                        source_item: (*key).clone(),
                        old_item: None,
                        new_item: None,
                    }
                } else {
                    TxnWriteOutcome::NoStream
                });
            }

            let existing = {
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
            };
            delete_item_in_tx(tx, key_info, key)
                .await
                .map_err(TxnOpError::Storage)?;
            Ok(if stream.is_some() {
                TxnWriteOutcome::StreamFromItems {
                    old_item: existing,
                    new_item: None,
                }
            } else {
                TxnWriteOutcome::NoStream
            })
        }
        TransactWriteOp::Update {
            key_info,
            key,
            actions,
            condition,
            maps,
            return_values_on_ccf,
            stream,
            ..
        } => {
            validate_transact_key_only(key_info, key)?;
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
            Ok(if stream.is_some() {
                TxnWriteOutcome::StreamFromItems {
                    old_item: existing,
                    new_item: Some(item),
                }
            } else {
                TxnWriteOutcome::NoStream
            })
        }
        TransactWriteOp::ConditionCheck {
            key_info,
            key,
            condition,
            maps,
            return_values_on_ccf,
        } => {
            validate_transact_key_only(key_info, key)?;
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
            Ok(TxnWriteOutcome::NoStream)
        }
    }
}

fn validate_transact_put(
    key_info: &TableKeyInfo,
    item: &Item,
    indexes: &[Vec<KeySchemaElement>],
    limits: &LimitsConfig,
) -> Result<(), TxnOpError> {
    validation::validate_item_keys(item, &key_info.key_schema, &key_info.attribute_definitions)
        .map_err(|e| TxnOpError::Cancel(CancellationReason::validation_error(e.to_string())))?;
    validate_txn_index_key_constraints(item, indexes, &key_info.attribute_definitions, limits)
}

fn validate_transact_key_only(key_info: &TableKeyInfo, key: &Item) -> Result<(), TxnOpError> {
    validation::validate_key_only(key, &key_info.key_schema, &key_info.attribute_definitions)
        .map_err(|e| TxnOpError::Cancel(CancellationReason::validation_error(e.to_string())))
}

fn validate_txn_index_key_constraints(
    item: &Item,
    indexes: &[Vec<KeySchemaElement>],
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

    use extenddb_core::expression::{Expr, ExpressionMaps};
    use extenddb_core::limits::LimitsConfig;
    use extenddb_core::types::{
        AttributeDefinition, AttributeValue, KeySchemaElement, KeyType,
        ReturnValuesOnConditionCheckFailure, ScalarAttributeType, StreamViewType, TableKeyInfo,
    };
    use extenddb_storage::{StreamCapture, TransactWriteOp};

    use super::{
        NativeTxnWriteBatch, TransactGetEntry, TransactGetGroup, TransactGetLookupKey, TxnOpError,
        TxnWriteOutcome, assign_transact_get_group_results, stage_native_transact_write_op,
        transact_delete_needs_existing_item, transact_get_pk_sk_sql, transact_get_pk_sql,
        transact_put_needs_existing_item,
    };

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

    fn key_info() -> TableKeyInfo {
        TableKeyInfo {
            table_name: "table".to_owned(),
            account_id: "acct".to_owned(),
            table_id: "tableid".to_owned(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            }],
            attribute_definitions: vec![
                AttributeDefinition {
                    attribute_name: "pk".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "gpk".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                },
            ],
            secondary_index_key_schemas: vec![vec![KeySchemaElement {
                attribute_name: "gpk".to_owned(),
                key_type: KeyType::Hash,
            }]],
            has_lsi: false,
            stream_specification: None,
        }
    }

    fn item(value: &str) -> extenddb_core::types::Item {
        let mut item = extenddb_core::types::Item::new();
        item.insert("pk".to_owned(), AttributeValue::S(value.to_owned()));
        item
    }

    #[test]
    fn unconditional_transaction_write_without_stream_skips_existing_item_read() {
        assert!(!transact_put_needs_existing_item(None, &None));
        assert!(!transact_delete_needs_existing_item(None, &None));
    }

    #[test]
    fn transaction_put_reads_existing_item_for_conditions_or_old_image_streams() {
        let condition = condition();
        assert!(transact_put_needs_existing_item(Some(&condition), &None));
        assert!(!transact_put_needs_existing_item(
            None,
            &Some(stream_capture())
        ));
        assert!(transact_put_needs_existing_item(
            None,
            &Some(StreamCapture {
                view_type: StreamViewType::OldImage,
                user_identity: None,
                region: Arc::from("us-east-1"),
            })
        ));
        assert!(transact_put_needs_existing_item(
            None,
            &Some(StreamCapture {
                view_type: StreamViewType::NewAndOldImages,
                user_identity: None,
                region: Arc::from("us-east-1"),
            })
        ));
    }

    #[test]
    fn transaction_delete_reads_existing_item_only_for_conditions_or_old_image_streams() {
        let condition = condition();
        assert!(transact_delete_needs_existing_item(Some(&condition), &None));
        assert!(!transact_delete_needs_existing_item(
            None,
            &Some(stream_capture())
        ));
        assert!(!transact_delete_needs_existing_item(
            None,
            &Some(StreamCapture {
                view_type: StreamViewType::NewImage,
                user_identity: None,
                region: Arc::from("us-east-1"),
            })
        ));
        assert!(transact_delete_needs_existing_item(
            None,
            &Some(StreamCapture {
                view_type: StreamViewType::OldImage,
                user_identity: None,
                region: Arc::from("us-east-1"),
            })
        ));
        assert!(transact_delete_needs_existing_item(
            None,
            &Some(StreamCapture {
                view_type: StreamViewType::NewAndOldImages,
                user_identity: None,
                region: Arc::from("us-east-1"),
            })
        ));
    }

    #[test]
    fn transaction_get_sql_uses_native_primary_key_batch_shape() {
        assert_eq!(
            transact_get_pk_sql("`_ddb_table`", 3),
            "SELECT pk, item_data FROM `_ddb_table` WHERE pk IN (?, ?, ?)"
        );
        assert_eq!(
            transact_get_pk_sk_sql("`_ddb_table`", "sk_n", 2),
            "SELECT pk, sk_n, item_data FROM `_ddb_table` WHERE (pk, sk_n) IN ((?, ?), (?, ?))"
        );
    }

    #[test]
    fn transaction_get_assignment_restores_request_order() {
        let key_info = key_info();
        let first = TransactGetLookupKey {
            pk: b"first".to_vec(),
            sk: None,
        };
        let second = TransactGetLookupKey {
            pk: b"second".to_vec(),
            sk: None,
        };
        let group = TransactGetGroup {
            key_info: &key_info,
            entries: vec![
                TransactGetEntry {
                    result_index: 0,
                    lookup_key: first.clone(),
                },
                TransactGetEntry {
                    result_index: 1,
                    lookup_key: second.clone(),
                },
            ],
        };
        let fetched = vec![(second, item("second")), (first, item("first"))];
        let mut results = vec![None, None];

        assign_transact_get_group_results(&group, &fetched, &mut results);

        assert_eq!(results, vec![Some(item("first")), Some(item("second"))]);
    }

    #[test]
    fn transaction_put_validates_index_schemas_from_table_key_info() {
        let key_info = key_info();
        let maps = ExpressionMaps::default();
        let limits = LimitsConfig::default();
        let mut batch = NativeTxnWriteBatch::default();
        let mut put_item = item("put");
        put_item.insert("gpk".to_owned(), AttributeValue::B(vec![1]));
        let put_op = TransactWriteOp::Put {
            key_info: &key_info,
            item: &put_item,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        };

        let result = stage_native_transact_write_op(
            &put_op,
            &key_info.secondary_index_key_schemas,
            &limits,
            &mut batch,
        );

        let Err(TxnOpError::Cancel(reason)) = result else {
            panic!("index key validation should cancel the transaction item");
        };
        assert_eq!(reason.code, "ValidationError");
        assert!(
            reason
                .message
                .as_deref()
                .is_some_and(|message| message.contains("Type mismatch for key attribute gpk"))
        );
    }

    #[test]
    fn unconditional_transaction_put_delete_without_stream_stage_native_batch() {
        let key_info = key_info();
        let maps = ExpressionMaps::default();
        let limits = LimitsConfig::default();
        let put_item = item("put");
        let delete_key = item("delete");
        let mut batch = NativeTxnWriteBatch::default();

        let put_op = TransactWriteOp::Put {
            key_info: &key_info,
            item: &put_item,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        };
        let put_outcome =
            stage_native_transact_write_op(&put_op, &[], &limits, &mut batch).unwrap();

        let delete_op = TransactWriteOp::Delete {
            key_info: &key_info,
            key: &delete_key,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        };
        let delete_outcome =
            stage_native_transact_write_op(&delete_op, &[], &limits, &mut batch).unwrap();

        assert!(matches!(put_outcome, Some(TxnWriteOutcome::NoStream)));
        assert!(matches!(delete_outcome, Some(TxnWriteOutcome::NoStream)));
        assert_eq!(batch.groups.len(), 1);
        assert_eq!(batch.groups[0].puts.len(), 1);
        assert_eq!(batch.groups[0].deletes.len(), 1);
    }

    #[test]
    fn transaction_writes_with_conditions_or_streams_do_not_stage_native_batch() {
        let key_info = key_info();
        let maps = ExpressionMaps::default();
        let limits = LimitsConfig::default();
        let put_item = item("put");
        let delete_key = item("delete");
        let condition = condition();
        let mut batch = NativeTxnWriteBatch::default();

        let conditioned_put = TransactWriteOp::Put {
            key_info: &key_info,
            item: &put_item,
            condition: Some(&condition),
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        };
        let streamed_delete = TransactWriteOp::Delete {
            key_info: &key_info,
            key: &delete_key,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(stream_capture()),
        };

        assert!(
            stage_native_transact_write_op(&conditioned_put, &[], &limits, &mut batch)
                .unwrap()
                .is_none()
        );
        assert!(
            stage_native_transact_write_op(&streamed_delete, &[], &limits, &mut batch)
                .unwrap()
                .is_none()
        );
        assert!(batch.groups.is_empty());
    }
}
