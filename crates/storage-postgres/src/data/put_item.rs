// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `put_item`, `put_items_batch`, and `get_item` implementations for the
//! `PostgreSQL` backend.

use extenddb_core::expression::{Expr, ExpressionMaps};
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{
    SortKeyValue, composite_pk_to_text, parse_sk, pk_to_text, sk_column, sk_info,
};
use extenddb_storage::vector_lifecycle::VectorIndexMeta;
use extenddb_storage::{BatchRow, BatchRowResult, BulkRowMarker, RowOutcome, StreamCapture};

use super::gsi_build::{GRACE_WRITE, WriteStamp, fix_up_stale_write};
use super::index::{
    IndexMeta, db_error, enqueue_async_indexes, fetch_write_path_indexes, fresh_attr_defs,
    sync_indexes,
};
use super::query::check_condition;
use super::tx_helpers::write_stream_record_in_tx;
use super::{data_table_name, json_to_item};
use crate::PostgresEngine;

mod batch;

/// What one table's write path needs, read once per call. A batch reads it
/// once for all its rows, so the whole batch sees one catalog state and one
/// stamp, and the stale-write repair runs once for the batch, before its
/// commit (`batch::fix_up_stale_batch`).
struct WritePlan {
    ddb_table: String,
    indexes: Vec<IndexMeta>,
    vector_metas: Vec<(VectorIndexMeta, String)>,
    /// When the catalog read behind `indexes` happened. A write that commits
    /// more than `GRACE_WRITE` after it re-reads the catalog and queues a
    /// repair for each GSI it did not know (`gsi_build::fix_up_stale_write`).
    stamp: WriteStamp,
    sys_delay: u64,
    /// SQL for the transactional path, shaped once by the key schema.
    select_sql: String,
    update_sql: String,
    insert_sql: String,
}

impl WritePlan {
    /// Whether a write with these attributes needs the transactional path at
    /// all. A plain row on a table with no indexes, no stream, and no marker
    /// is one upsert statement. A write that is already slow takes the
    /// transaction path even with no index to maintain, so that a repair for
    /// a GSI created meanwhile commits with the write instead of after it.
    fn needs_tx(
        &self,
        condition: Option<&Expr>,
        return_old: bool,
        stream: Option<&StreamCapture>,
        marker: Option<&BulkRowMarker>,
    ) -> bool {
        condition.is_some()
            || return_old
            || !self.indexes.is_empty()
            || !self.vector_metas.is_empty()
            || stream.is_some()
            || marker.is_some()
            || self.stamp.elapsed() > GRACE_WRITE
    }
}

/// The primary key of one row as the data table stores it.
struct RowKey {
    pk_text: String,
    sk: Option<SortKeyValue>,
}

impl RowKey {
    fn of(item: &Item, key_info: &TableKeyInfo) -> Result<Self, StorageError> {
        let pk_text = composite_pk_to_text(item, &key_info.key_schema)?;
        let sk = match sk_info(&key_info.key_schema, &key_info.attribute_definitions) {
            Some((sk_name, sk_type)) => {
                let sk_value = item
                    .get(sk_name)
                    .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
                Some(parse_sk(sk_value, sk_type)?)
            }
            None => None,
        };
        Ok(Self { pk_text, sk })
    }
}

/// Bind `pk` and, when present, `sk` as `$1` and `$2`.
fn bind_key<'q, O>(
    query: sqlx::query::QueryAs<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments>,
    key: &'q RowKey,
) -> sqlx::query::QueryAs<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments> {
    let query = query.bind(key.pk_text.as_str());
    match &key.sk {
        Some(SortKeyValue::S(s)) => query.bind(s),
        Some(SortKeyValue::N(n)) => query.bind(n),
        Some(SortKeyValue::B(b)) => query.bind(b),
        None => query,
    }
}

/// Bind `pk`, the optional `sk`, and then `item_data` as the last parameter.
fn bind_key_and_item<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    key: &'q RowKey,
    item_json: &'q serde_json::Value,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    let query = query.bind(key.pk_text.as_str());
    let query = match &key.sk {
        Some(SortKeyValue::S(s)) => query.bind(s),
        Some(SortKeyValue::N(n)) => query.bind(n),
        Some(SortKeyValue::B(b)) => query.bind(b),
        None => query,
    };
    query.bind(item_json)
}

/// The locking read of a row's current image inside a transaction.
async fn fetch_locked(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    sql: &str,
    key: &RowKey,
) -> Result<Option<serde_json::Value>, StorageError> {
    let row: Option<(serde_json::Value,)> = bind_key(sqlx::query_as(sql), key)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db_error)?;
    Ok(row.map(|(v,)| v))
}

/// What one row's write inside a transaction produced.
struct RowWrite {
    /// The replaced image, when one existed.
    old: Option<Item>,
    /// Async index rows enqueued, so the caller can wake the workers after
    /// commit.
    async_enqueued: usize,
}

impl PostgresEngine {
    /// Read the write plan for one table: the catalog's index families and
    /// the propagation delay, plus the statements the key schema shapes.
    ///
    /// Vector indexes come from this fresh read rather than from the cached
    /// key info, and the answer decides two things: whether a write needs a
    /// transaction at all, and what maintenance runs inside it. A cached
    /// empty set would send a write down the no-maintenance fast path and
    /// silently leave an index missing a row. What remains is a window
    /// between this read and the data transaction's commit, in which an
    /// index created concurrently is missed. No transaction can close it,
    /// because the catalog and the data tables are different databases. For
    /// a GSI it is closed by time instead: the read's stamp lets a write that
    /// was slow to commit queue a repair for any GSI it did not see
    /// (`gsi_build::fix_up_stale_write`), and a fast write is one the
    /// backfill sees. Vector indexes still have the window.
    async fn write_plan(&self, key_info: &TableKeyInfo) -> Result<WritePlan, StorageError> {
        let ddb_table = data_table_name(&key_info.table_id);
        let (indexes, vector_metas, stamp) =
            fetch_write_path_indexes(&key_info.table_id, &self.pool).await?;
        // Read whenever anything can propagate, secondary or vector. Gating
        // this on the secondary set alone made a vector-only table ignore
        // the configured delay and apply its vector index inline, while a
        // TransactWriteItems on the same table read the delay
        // unconditionally and enqueued.
        let sys_delay = if indexes.is_empty() && vector_metas.is_empty() {
            0
        } else {
            self.index_propagation_delay().await
        };
        let (select_sql, update_sql, insert_sql) =
            match sk_info(&key_info.key_schema, &key_info.attribute_definitions) {
                Some((_, sk_type)) => {
                    let sk_col = sk_column(sk_type);
                    (
                        format!(
                            "SELECT item_data FROM {ddb_table} WHERE pk = $1 AND {sk_col} = $2 \
                             FOR UPDATE"
                        ),
                        format!(
                            "UPDATE {ddb_table} SET item_data = $3 WHERE pk = $1 AND {sk_col} = $2"
                        ),
                        format!(
                            "INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES ($1, $2, $3) \
                             ON CONFLICT (pk, {sk_col}) DO NOTHING"
                        ),
                    )
                }
                None => (
                    format!("SELECT item_data FROM {ddb_table} WHERE pk = $1 FOR UPDATE"),
                    format!("UPDATE {ddb_table} SET item_data = $2 WHERE pk = $1"),
                    format!(
                        "INSERT INTO {ddb_table} (pk, item_data) VALUES ($1, $2) \
                         ON CONFLICT (pk) DO NOTHING"
                    ),
                ),
            };
        Ok(WritePlan {
            ddb_table,
            indexes,
            vector_metas,
            stamp,
            sys_delay,
            select_sql,
            update_sql,
            insert_sql,
        })
    }

    /// Index key attributes present in the item must match their declared
    /// scalar type and be non-empty, matching real DynamoDB. This is
    /// up-front input validation (a top-level ValidationException), so it
    /// runs before any write work. The types come from the definitions read
    /// with the indexes, not the cached key info: an UpdateTable that adds a
    /// GSI adds its key attributes' definitions, and the cache is dropped
    /// only when that UpdateTable returns (`IndexMeta::attribute_definitions`).
    fn validate_index_keys(
        plan: &WritePlan,
        key_info: &TableKeyInfo,
        item: &Item,
    ) -> Result<(), StorageError> {
        if plan.indexes.is_empty() {
            return Ok(());
        }
        let index_refs: Vec<extenddb_core::validation::IndexKeyRef<'_>> = plan
            .indexes
            .iter()
            .map(|idx| extenddb_core::validation::IndexKeyRef {
                index_name: &idx.index_name,
                key_schema: &idx.key_schema,
            })
            .collect();
        extenddb_core::validation::validate_index_keys(
            item,
            &index_refs,
            fresh_attr_defs(&plan.indexes, &key_info.attribute_definitions),
        )
        .map_err(|e| StorageError::Validation(e.to_string()))
    }

    /// One row's write inside an open transaction: the locking read, the
    /// condition against the current image, the write (with the create-race
    /// loop), and index and stream maintenance. The caller records a marked
    /// row's outcome from the returned image: a single write right after
    /// this returns, a batch with its other rows before its commit.
    ///
    /// A failed condition returns `Err(ConditionFailed(old))` after the
    /// locking read and before any statement that changes a row, so the
    /// transaction stays usable and a batch can continue with its next row
    /// while the lock on this row is held to commit.
    async fn put_row_in_tx(
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        plan: &WritePlan,
        key_info: &TableKeyInfo,
        item: &Item,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
    ) -> Result<RowWrite, StorageError> {
        let key = RowKey::of(item, key_info)?;
        let item_json =
            serde_json::to_value(item).map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut old = fetch_locked(tx, &plan.select_sql, &key).await?;

        let mut attempt: u32 = 0;
        loop {
            if let Some(ref old_json) = old {
                let old_item: Item = json_to_item(old_json.clone())?;
                match check_condition(condition, &old_item, maps) {
                    Ok(()) => {}
                    Err(StorageError::ConditionFailed(_)) => {
                        return Err(StorageError::ConditionFailed(Some(old_item)));
                    }
                    Err(e) => return Err(e),
                }
                // Row exists, condition passed: update in place.
                bind_key_and_item(sqlx::query(&plan.update_sql), &key, &item_json)
                    .execute(&mut **tx)
                    .await
                    .map_err(db_error)?;
                break;
            }
            // No existing item: the condition checks against an empty item.
            let empty = std::collections::BTreeMap::new();
            match check_condition(condition, &empty, maps) {
                Ok(()) => {}
                Err(StorageError::ConditionFailed(_)) => {
                    return Err(StorageError::ConditionFailed(None));
                }
                Err(e) => return Err(e),
            }
            let result = bind_key_and_item(sqlx::query(&plan.insert_sql), &key, &item_json)
                .execute(&mut **tx)
                .await
                .map_err(db_error)?;
            if result.rows_affected() == 1 {
                break;
            }
            // Lost the create race. The winner has committed (the insert
            // waited for it if it was in flight). The locking read returns
            // its row, and this put overwrites it after checking its
            // condition against it. If a delete committed since, the read
            // returns no row and the insert is retried.
            attempt += 1;
            if attempt >= MAX_CREATE_RACE_ATTEMPTS {
                return Err(create_race_exhausted(attempt));
            }
            old = fetch_locked(tx, &plan.select_sql, &key).await?;
        }

        let old_item = old.map(json_to_item).transpose()?;

        // Sync GSI/LSI update within the transaction (D-4).
        if !plan.indexes.is_empty() {
            sync_indexes(
                tx,
                &key_info.key_schema,
                &plan.indexes,
                old_item.as_ref(),
                Some(item),
                plan.sys_delay,
            )
            .await?;
        }

        // Write the stream record atomically within the transaction.
        if let Some(capture) = stream {
            write_stream_record_in_tx(tx, key_info, capture, old_item.as_ref(), Some(item)).await?;
        }

        // Persist async GSI work inside the same transaction: one row per
        // async index, each honoring its own propagation delay.
        let mut async_enqueued = enqueue_async_indexes(
            tx,
            key_info,
            &plan.indexes,
            old_item.as_ref(),
            Some(item),
            plan.sys_delay,
        )
        .await?;

        // Vector indexes. The old image is needed even when no secondary
        // index wanted it: the vector row is keyed by the base item, so a
        // partition move is a delete of the previous row.
        async_enqueued += crate::data::vector_index::maintain_vector_indexes(
            tx,
            &plan.vector_metas,
            &key_info.table_id,
            &key_info.key_schema,
            &key_info.attribute_definitions,
            old_item.as_ref(),
            Some(item),
            plan.sys_delay,
        )
        .await?;

        Ok(RowWrite {
            old: old_item,
            async_enqueued,
        })
    }

    /// Implementation of `DataEngine::put_item`.
    ///
    /// When `marker` is `Some` and the write commits, the row's applied
    /// outcome is inserted into `bulk_row_outcomes` inside the same
    /// transaction, so the marker and the write are durable together or not
    /// at all. The condition-failed outcome is recorded by the caller, since
    /// a failed condition leaves no transaction to ride.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn put_item_impl(
        &self,
        key_info: &TableKeyInfo,
        item: Item,
        return_old: bool,
        condition: Option<&Expr>,
        maps: &ExpressionMaps,
        stream: Option<&StreamCapture>,
        marker: Option<&BulkRowMarker>,
    ) -> Result<Option<Item>, StorageError> {
        let plan = self.write_plan(key_info).await?;
        Self::validate_index_keys(&plan, key_info, &item)?;

        if !plan.needs_tx(condition, return_old, stream, marker) {
            return self.upsert_plain(&plan, key_info, &item).await;
        }

        let mut tx = self
            .data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let mut written =
            Self::put_row_in_tx(&mut tx, &plan, key_info, &item, condition, maps, stream).await?;
        // The row's outcome, durable with the write: commit covers both or
        // neither, which is what lets a resumed run find the row applied and
        // account for it without writing it again.
        if let Some(m) = marker {
            super::bulk_row_outcomes::insert_outcome_in_tx(
                &mut tx,
                m,
                RowOutcome::Applied,
                written.old.as_ref(),
            )
            .await?;
        }
        written.async_enqueued += fix_up_stale_write(
            &mut tx,
            &self.pool,
            plan.stamp,
            key_info,
            &plan.indexes,
            &item,
        )
        .await?;
        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        self.wake_index_workers(written.async_enqueued);

        Ok(if return_old { written.old } else { None })
    }

    /// The one-statement path for a plain row on a table with nothing to
    /// maintain: no condition, no replaced image wanted, no indexes, no
    /// stream, no marker, and a catalog read recent enough. A write that
    /// became slow after that decision queues its GSI repair after the
    /// statement (`fix_up_autocommit`).
    async fn upsert_plain(
        &self,
        plan: &WritePlan,
        key_info: &TableKeyInfo,
        item: &Item,
    ) -> Result<Option<Item>, StorageError> {
        let key = RowKey::of(item, key_info)?;
        let item_json =
            serde_json::to_value(item).map_err(|e| StorageError::Internal(e.to_string()))?;
        let ddb_table = &plan.ddb_table;
        let upsert_sql = match sk_info(&key_info.key_schema, &key_info.attribute_definitions) {
            Some((_, sk_type)) => {
                let sk_col = sk_column(sk_type);
                format!(
                    "INSERT INTO {ddb_table} (pk, {sk_col}, item_data) VALUES ($1, $2, $3) \
                     ON CONFLICT (pk, {sk_col}) DO UPDATE SET item_data = EXCLUDED.item_data"
                )
            }
            None => format!(
                "INSERT INTO {ddb_table} (pk, item_data) VALUES ($1, $2) \
                 ON CONFLICT (pk) DO UPDATE SET item_data = EXCLUDED.item_data"
            ),
        };
        super::gsi_build::pause_before_autocommit_statement().await;
        bind_key_and_item(sqlx::query(&upsert_sql), &key, &item_json)
            .execute(&self.data_pool)
            .await
            .map_err(db_error)?;
        self.fix_up_autocommit(plan.stamp, key_info, item).await;
        Ok(None)
    }

    fn wake_index_workers(&self, async_enqueued: usize) {
        if async_enqueued > 0
            && let Some(ref q) = self.gsi_queue
        {
            q.notify_workers();
        }
    }

    /// Implementation of `DataEngine::get_item`.
    pub(crate) async fn get_item_impl(
        &self,
        key_info: &TableKeyInfo,
        key: &Item,
    ) -> Result<Option<Item>, StorageError> {
        let ddb_table = data_table_name(&key_info.table_id);

        let pk_name = &key_info.key_schema[0].attribute_name;
        let pk_value = key
            .get(pk_name)
            .ok_or_else(|| StorageError::Internal("missing partition key".to_owned()))?;
        let pk_text = pk_to_text(pk_value)?;

        let json_opt = if let Some((sk_name, sk_type)) =
            sk_info(&key_info.key_schema, &key_info.attribute_definitions)
        {
            let sk_value = key
                .get(sk_name)
                .ok_or_else(|| StorageError::Internal("missing sort key".to_owned()))?;
            let sk = parse_sk(sk_value, sk_type)?;
            let sk_col = sk_column(sk_type);
            let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = $1 AND {sk_col} = $2");
            let row: Option<(serde_json::Value,)> =
                bind_sk_fetch_optional!(&sql, pk_text.as_ref(), &sk, &self.data_pool)?;
            row.map(|(v,)| v)
        } else {
            let sql = format!("SELECT item_data FROM {ddb_table} WHERE pk = $1");
            let row: Option<(serde_json::Value,)> = sqlx::query_as(&sql)
                .bind(pk_text.as_ref())
                .fetch_optional(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            row.map(|(v,)| v)
        };

        json_opt.map(json_to_item).transpose()
    }
}

/// Bound on insert retries when a put to a missing item keeps losing the
/// create race to a winner that is deleted again before the re-read. Same
/// bound as `UpdateItem`.
const MAX_CREATE_RACE_ATTEMPTS: u32 = 5;

/// The error after `MAX_CREATE_RACE_ATTEMPTS` lost create races. Like the one
/// `UpdateItem` returns, it is an internal error (HTTP 500).
fn create_race_exhausted(attempt: u32) -> StorageError {
    StorageError::Internal(format!(
        "PutItem could not create the item after {attempt} attempts: each insert lost the \
         create race to a concurrent writer"
    ))
}
