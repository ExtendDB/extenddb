// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The data-plane side of a store-backed restore on SQLite
//! ([`RestoreEngine`]): the items of one verified data file written into the
//! CREATING target, the progress record, the ACTIVE flip and the count.
//!
//! One write transaction (`BEGIN IMMEDIATE` under the engine write lock) per
//! batch of at most [`RESTORE_BATCH_ITEMS`] items or [`RESTORE_BATCH_BYTES`]
//! of serialized item text (a batch closes at the first item that takes its
//! `item_data` text past the bound, so the batch term of a restore's memory
//! is bounded in bytes, not only in items): the base rows (plain INSERTs, so
//! a key the backup holds twice, within a file or across files, fails the
//! file instead of collapsing two items), each secondary index's rows and
//! each vector index's rows beside them, and last the record's progress,
//! `UPDATE table_restores SET file_index, items_applied` conditioned on the
//! claim's generation and on RUNNING: a batch commits whole with its
//! progress or not at all, and a batch whose record another owner took
//! rolls back and ends the file as a lost claim. Other writers on the server
//! wait for a batch rather than the whole file. A resume reads the record's
//! `(file_index, items_applied)` and the engine skips that many items of the
//! file before the stream starts. An empty file records its progress too
//! (`items_applied = start_at`), so a resume does not re-read it.
//!
//! The restore's own buffer is this one batch of `(Item, String)`, bounded
//! by [`RESTORE_BATCH_BYTES`] whatever the table's indexes are. A table with
//! secondary or vector indexes nonetheless restores in more resident memory
//! than one without (about 90 MiB over idle at 100,000 200-byte items with
//! two full-projection GSIs against 43 MiB with none), and the excess is
//! SQLite's own: its per-connection page cache (`PRAGMA cache_size =
//! -32000`, 32 MiB) holds the index tables' b-tree pages, which a load of
//! scattered index keys dirties across the whole tree, and glibc's
//! per-arena free lists hold freed blocks (the figure falls by about a third
//! under `MALLOC_ARENA_MAX=1`). The excess is bounded by `cache_size` and
//! grows only sub-linearly with the table (about 90 MiB at 100,000 items,
//! 175 MiB at 300,000), not with the restore's buffers; a smaller batch does
//! not lower it (measured: the peak is unchanged at a third of the item
//! cap), because the cache persists across batches. The published restore
//! figures are for a table with no secondary index and state that shape.
//!
//! A job's heartbeat on SQLite (`SqliteJobClaim::heartbeat`) is an `UPDATE`
//! under the same engine write lock, so a beat waits for the batch that
//! holds the lock and for every writer queued ahead of it: other jobs'
//! batches, item writes, the GSI backfill, the TTL sweep, a table's drop.
//! The bench record (`docs/design/15-backup-restore-bench-pr9.md`) puts one
//! batch at about 1.1 s for items of 27.7 MB (24 of them, one per batch,
//! restored in 26.8 s with their download and decode) and about 0.27 s for
//! 10,000 items of 200 bytes (1,000,000 in 26.8 s). A beat that has not
//! returned within one heartbeat period (15 s at the least) ends the job as
//! a lost claim: it stops with its record RUNNING and makes no further
//! table access, its slot is freed, and the reconciler's next cycle takes
//! the record over and resumes it from the last committed batch. At the
//! default four jobs a beat waits a few seconds at most; many jobs of large
//! items, or the drop of a large table, can hold it past a period, which
//! costs a restart of the file from its last batch and a lost-claim log
//! line, not data.
//!
//! `finish_restore` is [`activate_restore_target`], the one transaction the
//! legacy copy ends with too: the record DONE conditioned on the generation
//! and on RUNNING, the table ACTIVE conditioned on CREATING, and every
//! secondary and vector index of the table ACTIVE.

use extenddb_core::types::{AttributeDefinition, Item, KeySchemaElement, Projection};
use extenddb_storage::backup_catalog::{
    JobClaim, RESTORE_SQLITE_MID_BATCH, RestoreTargetKey, restore_pause_point,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::restore_engine::{BulkLoadFile, Finished, RestoreEngine};
pub(crate) use extenddb_storage::restore_engine::{RESTORE_BATCH_BYTES, RESTORE_BATCH_ITEMS};
use extenddb_storage::util::{composite_pk_to_text, parse_sk, sk_column_n};
use extenddb_storage::vector_lifecycle::VectorIndexMeta;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::SqliteEngine;
use crate::backup_catalog::{NOW, generation_column};
use crate::data::vector_index::{
    VectorRowConflict, base_key_columns, fetch_vector_indexes_for_table, insert_vector_row,
};
use crate::data::{
    BoundValue, all_sort_key_info, data_table_name, index_table_name, insert_index_row_multi,
    item_has_index_keys, project_item_for_index, sk_bound,
};
use crate::sqlite_util::is_unique_violation;

fn db_err(e: sqlx::Error) -> StorageError {
    StorageError::Internal(format!("Database error: {e}"))
}

/// The error for a key the target already holds: the backup carries it
/// twice, within the file or in a file applied before it. The engine
/// prefixes the file's key, so the file is not named here.
fn duplicate_key_error(e: &sqlx::Error) -> StorageError {
    StorageError::Validation(format!(
        "holds a primary key the target already has (the backup holds a key twice): {e}"
    ))
}

/// The key shape of a CREATING restore target, read from its catalog row.
struct TargetShape {
    key_schema: Vec<KeySchemaElement>,
    attribute_definitions: Vec<AttributeDefinition>,
}

/// A secondary index of the target, as the load writes it.
struct TargetIndex {
    table: String,
    key_schema: Vec<KeySchemaElement>,
    projection: Projection,
}

/// What `bulk_load` writes per item and per batch, read once per file.
struct LoadPlan {
    insert_sql: String,
    shape: TargetShape,
    indexes: Vec<TargetIndex>,
    vector_key_columns: Vec<String>,
    file: BulkLoadFile,
    generation: u64,
    /// The file as an error names it.
    source: String,
}

/// What [`activate_restore_target`] matched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Activated {
    /// The record was RUNNING under the generation and is DONE.
    pub record: bool,
    /// The table was CREATING and is ACTIVE.
    pub table: bool,
}

/// The end of a restore, inside the caller's write transaction: the record
/// DONE (`completed_at` now; `items_applied` left as the progress stands
/// unless the caller gives a total) only while it is RUNNING under
/// `generation`; the table ACTIVE with its counts only from CREATING; every
/// secondary and vector index of the table ACTIVE (`backfilling` cleared on
/// the vector rows, which a restore target creates CREATING with the table).
/// Reports what each conditional write matched; the caller rolls back when
/// it needed a match it did not get.
pub(crate) async fn activate_restore_target(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table_id: &str,
    generation: u64,
    item_count: i64,
    table_size_bytes: i64,
    items_applied: Option<i64>,
) -> Result<Activated, StorageError> {
    let record = sqlx::query(&format!(
        "UPDATE table_restores SET status = 'DONE', \
         items_applied = COALESCE(?, items_applied), completed_at = {NOW} \
         WHERE table_id = ? AND status = 'RUNNING' AND owner_generation = ?"
    ))
    .bind(items_applied)
    .bind(table_id)
    .bind(generation_column(generation))
    .execute(&mut **tx)
    .await
    .map_err(db_err)?
    .rows_affected()
        == 1;
    if !record {
        return Ok(Activated {
            record,
            table: false,
        });
    }
    let table = sqlx::query(
        "UPDATE tables SET item_count = ?, table_size_bytes = ?, \
         table_status = 'ACTIVE', status_transition_at = NULL \
         WHERE table_id = ? AND table_status = 'CREATING'",
    )
    .bind(item_count)
    .bind(table_size_bytes)
    .bind(table_id)
    .execute(&mut **tx)
    .await
    .map_err(db_err)?
    .rows_affected()
        == 1;
    sqlx::query("UPDATE indexes SET index_status = 'ACTIVE' WHERE table_id = ? AND index_status <> 'ACTIVE'")
        .bind(table_id)
        .execute(&mut **tx)
        .await
        .map_err(db_err)?;
    sqlx::query(
        "UPDATE vector_indexes SET index_status = 'ACTIVE', backfilling = NULL \
         WHERE table_id = ? AND index_status <> 'ACTIVE'",
    )
    .bind(table_id)
    .execute(&mut **tx)
    .await
    .map_err(db_err)?;
    Ok(Activated { record, table })
}

impl SqliteEngine {
    /// The target's key schema and attribute definitions, when `key` names
    /// a CREATING restore target with this table id.
    async fn restore_target_shape(
        &self,
        key: &RestoreTargetKey,
    ) -> Result<TargetShape, StorageError> {
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT t.key_schema, t.attribute_definitions FROM tables t \
             JOIN table_restores r ON r.table_id = t.table_id \
             WHERE t.table_id = ? AND t.account_id = ? AND t.table_name = ? \
             AND t.table_status = 'CREATING'",
        )
        .bind(&key.table_id)
        .bind(&key.account_id)
        .bind(&key.table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        let (key_schema, attribute_definitions) = row.ok_or_else(|| {
            StorageError::TableNotFound(format!(
                "{} is not a CREATING restore target with table id {}",
                key.table_name, key.table_id
            ))
        })?;
        Ok(TargetShape {
            key_schema: serde_json::from_str(&key_schema)
                .map_err(|e| StorageError::Internal(format!("key schema: {e}")))?,
            attribute_definitions: serde_json::from_str(&attribute_definitions)
                .map_err(|e| StorageError::Internal(format!("attribute definitions: {e}")))?,
        })
    }

    /// Everything a file's writes need that does not change between batches.
    async fn load_plan(
        &self,
        key: &RestoreTargetKey,
        file: BulkLoadFile,
        generation: u64,
    ) -> Result<LoadPlan, StorageError> {
        let shape = self.restore_target_shape(key).await?;
        let sort_keys = all_sort_key_info(&shape.key_schema, &shape.attribute_definitions);
        let mut cols = vec!["pk".to_owned()];
        cols.extend(
            sort_keys
                .iter()
                .enumerate()
                .map(|(i, &(_, t))| sk_column_n(i, t)),
        );
        cols.push("item_data".to_owned());
        let data_table = data_table_name(&key.table_id);
        let insert_sql = format!(
            "INSERT INTO {data_table} ({}) VALUES ({})",
            cols.join(", "),
            vec!["?"; cols.len()].join(", ")
        );
        let index_rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT index_id, key_schema, projection FROM indexes WHERE table_id = ?",
        )
        .bind(&key.table_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        let mut indexes = Vec::with_capacity(index_rows.len());
        for (index_id, ks, proj) in index_rows {
            indexes.push(TargetIndex {
                table: index_table_name(&index_id),
                key_schema: serde_json::from_str(&ks)
                    .map_err(|e| StorageError::Internal(format!("index key schema: {e}")))?,
                projection: serde_json::from_str(&proj)
                    .map_err(|e| StorageError::Internal(format!("index projection: {e}")))?,
            });
        }
        let vector_key_columns = base_key_columns(&shape.key_schema, &shape.attribute_definitions);
        Ok(LoadPlan {
            insert_sql,
            shape,
            indexes,
            vector_key_columns,
            file,
            generation,
            source: format!("data file {}", file.file_index),
        })
    }

    /// One batch: the base rows, the index rows, the vector rows and the
    /// record's progress in one write transaction under the write lock.
    /// `applied` is the file's running total before this batch, `start_at`
    /// included; each item comes with its `item_data` text, serialized when
    /// the batch was collected. `Ok(false)` when the record was not RUNNING
    /// under the plan's generation: nothing of the batch is committed.
    async fn write_restore_batch(
        &self,
        key: &RestoreTargetKey,
        plan: &LoadPlan,
        applied: u64,
        batch: &mut Vec<(Item, String)>,
    ) -> Result<bool, StorageError> {
        let batch_len = batch.len();
        let source = plan.source.as_str();
        let _writer = self.write_lock.lock().await;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_err)?;
        let shape = &plan.shape;
        let sort_keys = all_sort_key_info(&shape.key_schema, &shape.attribute_definitions);
        // The vector indexes, read inside the transaction as the write path
        // does (the id names the data table).
        let vector_metas: Vec<VectorIndexMeta> =
            fetch_vector_indexes_for_table(&mut tx, &key.table_id).await?;
        for (item, item_json) in batch.drain(..) {
            let mut values = vec![BoundValue::Text(composite_pk_to_text(
                &item,
                &shape.key_schema,
            )?)];
            for &(name, sk_type) in &sort_keys {
                let value = item.get(name).ok_or_else(|| {
                    StorageError::Validation(format!(
                        "{source} holds an item without its sort key {name}"
                    ))
                })?;
                values.push(sk_bound(&parse_sk(value, sk_type)?));
            }
            values.push(BoundValue::Text(item_json));
            let mut q = sqlx::query(&plan.insert_sql);
            for v in values {
                q = crate::data::bind_bound!(q, v);
            }
            q.execute(&mut *tx).await.map_err(|e| {
                if is_unique_violation(&e) {
                    duplicate_key_error(&e)
                } else {
                    db_err(e)
                }
            })?;
            for idx in &plan.indexes {
                if !item_has_index_keys(&item, &idx.key_schema) {
                    continue;
                }
                let idx_sks = all_sort_key_info(&idx.key_schema, &shape.attribute_definitions);
                let projected = project_item_for_index(
                    &item,
                    &idx.key_schema,
                    &shape.key_schema,
                    &idx.projection,
                );
                insert_index_row_multi(
                    &mut tx,
                    &idx.table,
                    &item,
                    &projected,
                    &idx.key_schema,
                    &shape.key_schema,
                    &idx_sks,
                    &sort_keys,
                )
                .await?;
            }
            for meta in &vector_metas {
                insert_vector_row(
                    &mut tx,
                    &key.table_id,
                    meta,
                    &item,
                    &shape.key_schema,
                    &shape.attribute_definitions,
                    &plan.vector_key_columns,
                    VectorRowConflict::Fail,
                )
                .await?;
            }
        }
        // The progress, last and in the same transaction, conditioned on the
        // generation and on RUNNING: a record another owner took rolls the
        // batch back with it.
        let total = applied.saturating_add(u64::try_from(batch_len).unwrap_or(u64::MAX));
        let matched = sqlx::query(
            "UPDATE table_restores SET file_index = ?, items_applied = ? \
             WHERE table_id = ? AND owner_generation = ? AND status = 'RUNNING'",
        )
        .bind(i64::from(plan.file.file_index))
        .bind(i64::try_from(total).unwrap_or(i64::MAX))
        .bind(&key.table_id)
        .bind(generation_column(plan.generation))
        .execute(&mut *tx)
        .await
        .map_err(db_err)?
        .rows_affected();
        if matched != 1 {
            tx.rollback().await.map_err(db_err)?;
            return Ok(false);
        }
        tx.commit().await.map_err(db_err)?;
        Ok(true)
    }
}

impl RestoreEngine for SqliteEngine {
    fn bulk_load<'a>(
        &'a self,
        target: &'a RestoreTargetKey,
        claim: &'a dyn JobClaim,
        file: BulkLoadFile,
        mut items: BoxStream<'a, Result<Item, StorageError>>,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        Box::pin(async move {
            use futures::StreamExt;

            let generation = claim.generation();
            let plan = self.load_plan(target, file, generation).await?;
            let mut batch: Vec<(Item, String)> = Vec::with_capacity(RESTORE_BATCH_ITEMS.min(1024));
            let mut batch_bytes = 0usize;
            let mut applied: u64 = 0;
            let mut batches_written = 0u32;
            let mut ended = false;
            while !ended {
                // Collect a batch, closed at the count cap or at the first
                // item whose text takes it past the byte bound; an `Err`
                // item ends the file with that error, and the batch in hand
                // is never written.
                while batch.len() < RESTORE_BATCH_ITEMS && batch_bytes < RESTORE_BATCH_BYTES {
                    match items.next().await {
                        Some(item) => {
                            let item = item?;
                            let item_json = serde_json::to_string(&item)
                                .map_err(|e| StorageError::Internal(format!("item: {e}")))?;
                            batch_bytes = batch_bytes.saturating_add(item_json.len());
                            batch.push((item, item_json));
                        }
                        None => {
                            ended = true;
                            break;
                        }
                    }
                }
                if batch.is_empty() && batches_written > 0 {
                    break;
                }
                if batches_written > 0 {
                    restore_pause_point(RESTORE_SQLITE_MID_BATCH).await;
                }
                let batch_len = batch.len();
                let written = self
                    .write_restore_batch(
                        target,
                        &plan,
                        file.start_at.saturating_add(applied),
                        &mut batch,
                    )
                    .await?;
                if !written {
                    return Err(StorageError::Internal(format!(
                        "claim lost: the job record of {} is not RUNNING under generation \
                         {generation}; the batch was rolled back",
                        target.table_name
                    )));
                }
                applied += u64::try_from(batch_len).unwrap_or(u64::MAX);
                batches_written += 1;
                batch_bytes = 0;
            }
            Ok(applied)
        })
    }

    fn finish_restore<'a>(
        &'a self,
        target: &'a RestoreTargetKey,
        claim: &'a dyn JobClaim,
        item_count: u64,
        size_bytes: u64,
    ) -> BoxFuture<'a, Result<Finished, StorageError>> {
        Box::pin(async move {
            // The key names the table; the flip itself is conditioned on the
            // table id, CREATING, RUNNING and the generation.
            let is_target: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE table_id = ? AND account_id = ? \
                 AND table_name = ?)",
            )
            .bind(&target.table_id)
            .bind(&target.account_id)
            .bind(&target.table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(db_err)?;
            if !is_target {
                return Ok(Finished::NotMatched(format!(
                    "{} is not a table with id {} in account {}",
                    target.table_name, target.table_id, target.account_id
                )));
            }
            let _writer = self.write_lock.lock().await;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(db_err)?;
            let activated = activate_restore_target(
                &mut tx,
                &target.table_id,
                claim.generation(),
                i64::try_from(item_count).unwrap_or(i64::MAX),
                i64::try_from(size_bytes).unwrap_or(i64::MAX),
                None,
            )
            .await?;
            if !(activated.record && activated.table) {
                tx.rollback().await.map_err(db_err)?;
                return Ok(Finished::NotMatched(format!(
                    "the job record of {} is not RUNNING under generation {}, or the table is \
                     not CREATING",
                    target.table_name,
                    claim.generation()
                )));
            }
            tx.commit().await.map_err(db_err)?;
            Ok(Finished::Active)
        })
    }

    fn count_restore_target<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        Box::pin(async move {
            // The id names the data table; the key's account and name are
            // checked against the catalog so a bare id reads no other
            // account's table.
            self.restore_target_shape(key).await?;
            let count: i64 = sqlx::query_scalar(&format!(
                "SELECT COUNT(*) FROM {}",
                data_table_name(&key.table_id)
            ))
            .fetch_one(&self.pool)
            .await
            .map_err(db_err)?;
            Ok(u64::try_from(count).unwrap_or_default())
        })
    }

    fn restore_target_ready<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            // The catalog row first, so a bare id reads no other account's
            // table; then the data table's existence (the registration
            // commits the catalog transaction before the data DDL).
            // `data_table_name` is a quoted identifier; `sqlite_master.name`
            // holds it unquoted.
            self.restore_target_shape(key).await?;
            let quoted = data_table_name(&key.table_id);
            let name = quoted.trim_matches('"');
            let exists: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
            )
            .bind(name)
            .fetch_one(&self.pool)
            .await
            .map_err(db_err)?;
            Ok(exists > 0)
        })
    }
}
