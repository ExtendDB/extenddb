// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BackupEngine` for the SQLite backend.
//!
//! A backup snapshots every item's `item_data` into `backup_items` and the
//! table's definition (secondary indexes, billing mode and throughput, table
//! class, encryption, and for DescribeBackup only TTL and stream settings)
//! into `backup_definitions`.
//!
//! RestoreTableFromBackup receives the target's `CreateTableInput` already
//! resolved and validated by the engine. It creates the target CREATING and
//! registers the restore in one write transaction, returns, and copies the
//! snapshot in a detached task, filling the secondary indexes as it goes in
//! bounded batches. The task flips the target ACTIVE, or removes it if the
//! copy fails; the startup sweep removes a target a process exit left
//! CREATING.

use extenddb_core::types::{
    AttributeDefinition, BackupDescription, BackupDetails, BackupSummary,
    ContinuousBackupsDescription, GsiInput, Item, KeySchemaElement, LsiInput,
    PointInTimeRecoveryDescription, Projection, SourceTableDetails, StreamSpecification,
    TableDescription, TimeToLiveDescription, TimeToLiveStatus,
};
use extenddb_storage::backup_definition::{
    BACKUP_DEFINITION_VERSION, BackupTableDefinition, COPY_BATCH_BYTES, COPY_BATCH_ITEMS,
    throughput_from_catalog,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::{composite_pk_to_text, parse_sk, sk_column_n};
use extenddb_storage::{BackupEngine, BackupRestoreSource, RestoreRequest};
use futures::future::BoxFuture;

use crate::create_table::RestoreRegistration;
use crate::data::{
    BoundValue, all_sort_key_info, data_table_name, index_table_name, insert_index_row_multi,
    item_has_index_keys, project_item_for_index, sk_bound,
};
use crate::sqlite_util::{format_epoch_seconds, timestamp_epoch_seconds as ts_to_epoch};
use crate::store::SqliteEngine;

/// Upper limit on [`backup_server_slot_count`].
const BACKUP_SERVER_SLOTS_MAX: usize = 16;

/// How long a CreateBackup waits for a server slot before it is refused with
/// `LimitExceeded`.
pub(crate) const BACKUP_SLOT_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Backups the whole process may run at once, across accounts:
/// `pool_size / 2 - 1`, clamped to `1..=16`.
///
/// A file-backed backup holds one pooled connection for its whole duration
/// (the WAL snapshot its items are read from) and its batches and final
/// commit take a second one, under the engine write lock, so at most one
/// backup writes at a time: `n` running backups hold `n + 1` connections.
/// Without a bound, enough concurrent CreateBackups (16 threads of one
/// tenant on the default pool of 10) hold every connection, and every other
/// request, every other tenant's reads included, waits for the pool's
/// 30 s acquire timeout and answers an internal error for as long as the
/// flood lasts. This keeps at least half the pool free for them.
pub(crate) fn backup_server_slot_count(pool_size: u32) -> usize {
    usize::try_from(pool_size / 2)
        .unwrap_or(usize::MAX)
        .saturating_sub(1)
        .clamp(1, BACKUP_SERVER_SLOTS_MAX)
}

/// Take a server slot for a backup, waiting at most `wait`; `LimitExceeded`
/// when none frees up in time.
async fn acquire_backup_permit(
    slots: std::sync::Arc<tokio::sync::Semaphore>,
    slot_count: usize,
    wait: std::time::Duration,
) -> Result<tokio::sync::OwnedSemaphorePermit, StorageError> {
    tokio::time::timeout(wait, slots.acquire_owned())
        .await
        .map_err(|_| {
            StorageError::LimitExceeded(format!(
                "Too many backups are in progress on this server (at most {slot_count} at once); \
                 retry later"
            ))
        })?
        .map_err(|e| StorageError::Internal(format!("backup slot: {e}")))
}

/// Read the next batch of `(cursor, item_data)` from `table` after
/// `last_cursor`, restricted by `filter` (a fixed SQL condition with one `?`
/// bound to `filter_arg`, or `None`). `cursor` is `rowid` for immutable source
/// table snapshots and `id` for backup_items, whose values survive VACUUM.
///
/// Two statements: the first reads only row lengths, so the batch can be cut
/// to the byte budget before any item text is loaded. Both run in the caller's
/// transaction, so no row appears or moves between them.
async fn next_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    cursor: &str,
    filter: Option<(&str, &str)>,
    last_cursor: i64,
) -> Result<Vec<(i64, String)>, StorageError> {
    let (cond, arg) = filter.map_or(("1 = 1", None), |(c, a)| (c, Some(a)));
    let sizes_sql = format!(
        "SELECT {cursor}, length(CAST(item_data AS BLOB)) FROM {table} \
         WHERE {cond} AND {cursor} > ? ORDER BY {cursor} LIMIT ?"
    );
    let mut q = sqlx::query_as::<_, (i64, i64)>(&sizes_sql);
    if let Some(a) = arg {
        q = q.bind(a);
    }
    let sizes = q
        .bind(last_cursor)
        .bind(i64::try_from(COPY_BATCH_ITEMS).unwrap_or(i64::MAX))
        .fetch_all(&mut **tx)
        .await
        .map_err(db_err)?;
    let Some(&(first, _)) = sizes.first() else {
        return Ok(Vec::new());
    };
    let mut upto = first;
    let mut bytes: usize = 0;
    for &(value, len) in &sizes {
        let len = usize::try_from(len).unwrap_or(usize::MAX);
        if value != first && bytes.saturating_add(len) > COPY_BATCH_BYTES {
            break;
        }
        bytes = bytes.saturating_add(len);
        upto = value;
    }
    let rows_sql = format!(
        "SELECT {cursor}, item_data FROM {table} \
         WHERE {cond} AND {cursor} > ? AND {cursor} <= ? ORDER BY {cursor}"
    );
    let mut q = sqlx::query_as::<_, (i64, String)>(&rows_sql);
    if let Some(a) = arg {
        q = q.bind(a);
    }
    q.bind(last_cursor)
        .bind(upto)
        .fetch_all(&mut **tx)
        .await
        .map_err(db_err)
}

fn db_err(e: sqlx::Error) -> StorageError {
    StorageError::Internal(e.to_string())
}

fn parse_json<T: serde::de::DeserializeOwned>(s: &str, what: &str) -> Result<T, StorageError> {
    serde_json::from_str(s).map_err(|e| StorageError::Internal(format!("{what}: {e}")))
}

/// A secondary index of a restore target, as the copy needs it.
struct RestoreIndex {
    table: String,
    key_schema: Vec<KeySchemaElement>,
    projection: Projection,
}

impl SqliteEngine {
    /// Read the parts of a table's definition a restore recreates.
    async fn capture_table_definition(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        table_id: &str,
    ) -> Result<BackupTableDefinition, StorageError> {
        #[allow(clippy::type_complexity)]
        let (billing_mode, pt, table_class, sse, on_demand, ttl_attribute, stream): (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT billing_mode, provisioned_throughput, table_class, sse_specification, \
             on_demand_throughput, ttl_attribute, stream_specification \
             FROM tables WHERE table_id = ?",
        )
        .bind(table_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(db_err)?;

        let rows: Vec<(String, String, String, String, Option<String>)> = sqlx::query_as(
            "SELECT index_name, index_type, key_schema, projection, provisioned_throughput \
             FROM indexes WHERE table_id = ? ORDER BY index_name",
        )
        .bind(table_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(db_err)?;
        let mut gsis = Vec::new();
        let mut lsis = Vec::new();
        for (index_name, index_type, ks, proj, ipt) in rows {
            let key_schema: Vec<KeySchemaElement> = parse_json(&ks, "index key schema")?;
            let projection: Projection = parse_json(&proj, "index projection")?;
            if index_type == "LSI" {
                lsis.push(LsiInput {
                    index_name,
                    key_schema,
                    projection,
                });
            } else {
                let ipt: Option<serde_json::Value> = ipt
                    .as_deref()
                    .map(|s| parse_json(s, "index throughput"))
                    .transpose()?;
                gsis.push(GsiInput {
                    index_name,
                    key_schema,
                    projection,
                    provisioned_throughput: throughput_from_catalog(ipt.as_ref()),
                });
            }
        }

        let vector_index_names: Vec<String> = sqlx::query_scalar(
            "SELECT index_name FROM vector_indexes WHERE table_id = ? ORDER BY index_name",
        )
        .bind(table_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(db_err)?;

        let pt: Option<serde_json::Value> = pt
            .as_deref()
            .map(|s| parse_json(s, "throughput"))
            .transpose()?;
        Ok(BackupTableDefinition {
            version: BACKUP_DEFINITION_VERSION,
            billing_mode,
            provisioned_throughput: throughput_from_catalog(pt.as_ref()),
            global_secondary_indexes: gsis,
            local_secondary_indexes: lsis,
            table_class,
            sse_specification: sse.as_deref().map(|s| parse_json(s, "sse")).transpose()?,
            on_demand_throughput: on_demand
                .as_deref()
                .map(|s| parse_json(s, "on-demand throughput"))
                .transpose()?,
            vector_index_names,
            // What DescribeTimeToLive reports for the table at this instant.
            time_to_live: Some(match ttl_attribute {
                Some(attr) => TimeToLiveDescription {
                    time_to_live_status: TimeToLiveStatus::Enabled,
                    attribute_name: Some(attr),
                },
                None => TimeToLiveDescription {
                    time_to_live_status: TimeToLiveStatus::Disabled,
                    attribute_name: None,
                },
            }),
            // As DescribeTable reports it: the stored specification, if any.
            stream_specification: stream
                .as_deref()
                .map(|s| parse_json::<StreamSpecification>(s, "stream specification"))
                .transpose()?,
        })
    }

    /// Copy a backup's items into a freshly created restore target and fill
    /// its secondary indexes, inside the caller's write transaction.
    ///
    /// Every key column is derived from the item (all HASH parts into `pk`,
    /// each RANGE part into its typed column), and each row is a plain
    /// INSERT, so two backup rows for one key fail the restore instead of
    /// collapsing into one item. Items are read and written in byte-bounded
    /// batches, each in its own write transaction, and the target is flipped
    /// to ACTIVE in the last one. Returns the number of items copied.
    async fn copy_backup_items(
        &self,
        desc: &TableDescription,
        backup_arn: &str,
    ) -> Result<i64, StorageError> {
        let ddb_table = data_table_name(&desc.table_id);
        let sort_keys = all_sort_key_info(&desc.key_schema, &desc.attribute_definitions);
        let mut cols = vec!["pk".to_owned()];
        cols.extend(
            sort_keys
                .iter()
                .enumerate()
                .map(|(i, &(_, t))| sk_column_n(i, t)),
        );
        cols.push("item_data".to_owned());
        let insert_sql = format!(
            "INSERT INTO {ddb_table} ({}) VALUES ({})",
            cols.join(", "),
            vec!["?"; cols.len()].join(", ")
        );

        let index_rows: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT index_id, key_schema, projection FROM indexes WHERE table_id = ?",
        )
        .bind(&desc.table_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        let mut indexes = Vec::with_capacity(index_rows.len());
        for (index_id, ks, proj) in index_rows {
            indexes.push(RestoreIndex {
                table: index_table_name(&index_id),
                key_schema: parse_json(&ks, "index key schema")?,
                projection: parse_json(&proj, "index projection")?,
            });
        }
        let base_sks = all_sort_key_info(&desc.key_schema, &desc.attribute_definitions);

        let mut last_rowid: i64 = 0;
        let mut count: i64 = 0;
        loop {
            // One write transaction per batch, so other writers on the server
            // wait for a batch rather than the whole restore. The target is
            // CREATING throughout, which refuses every data-plane request, so
            // no one sees it part-filled. Each batch first re-checks, under
            // the write lock, that the backup is still AVAILABLE (DeleteBackup
            // removes its rows and marks it DELETED in one transaction) and
            // that the target is still this restore's.
            let _writer = self.write_lock.lock().await;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(db_err)?;
            Self::ensure_restore_can_continue(&mut tx, desc, backup_arn).await?;
            let batch = next_batch(
                &mut tx,
                "backup_items",
                "id",
                Some(("backup_arn = ?", backup_arn)),
                last_rowid,
            )
            .await?;
            let Some(&(tail, _)) = batch.last() else {
                let (table_size,): (i64,) = sqlx::query_as(&format!(
                    "SELECT COALESCE(SUM(length(item_data)), 0) FROM {ddb_table}"
                ))
                .fetch_one(&mut *tx)
                .await
                .map_err(db_err)?;
                // Only from CREATING, which the check above confirmed under
                // the same lock.
                sqlx::query(
                    "UPDATE tables SET item_count = ?, table_size_bytes = ?, \
                     table_status = 'ACTIVE', status_transition_at = NULL \
                     WHERE table_id = ? AND table_status = 'CREATING'",
                )
                .bind(count)
                .bind(table_size)
                .bind(&desc.table_id)
                .execute(&mut *tx)
                .await
                .map_err(db_err)?;
                tx.commit().await.map_err(db_err)?;
                return Ok(count);
            };
            last_rowid = tail;
            for (_, item_json) in &batch {
                let item: Item = parse_json(item_json, "backup item")?;
                let mut values = vec![BoundValue::Text(composite_pk_to_text(
                    &item,
                    &desc.key_schema,
                )?)];
                for &(name, sk_type) in &sort_keys {
                    let value = item.get(name).ok_or_else(|| {
                        StorageError::Internal(format!(
                            "backup {backup_arn} has an item without sort key {name}"
                        ))
                    })?;
                    values.push(sk_bound(&parse_sk(value, sk_type)?));
                }
                values.push(BoundValue::Text(item_json.clone()));
                let mut q = sqlx::query(&insert_sql);
                for v in values {
                    q = crate::data::bind_bound!(q, v);
                }
                q.execute(&mut *tx).await.map_err(db_err)?;

                for idx in &indexes {
                    if !item_has_index_keys(&item, &idx.key_schema) {
                        continue;
                    }
                    let idx_sks = all_sort_key_info(&idx.key_schema, &desc.attribute_definitions);
                    let projected = project_item_for_index(
                        &item,
                        &idx.key_schema,
                        &desc.key_schema,
                        &idx.projection,
                    );
                    insert_index_row_multi(
                        &mut tx,
                        &idx.table,
                        &item,
                        &projected,
                        &idx.key_schema,
                        &desc.key_schema,
                        &idx_sks,
                        &base_sks,
                    )
                    .await?;
                }
                count += 1;
            }
            tx.commit().await.map_err(db_err)?;
        }
    }

    /// Fail a restore whose backup was deleted or whose target is no longer
    /// the one it created.
    async fn ensure_restore_can_continue(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        desc: &TableDescription,
        backup_arn: &str,
    ) -> Result<(), StorageError> {
        let available: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM backups WHERE backup_arn = ? \
             AND backup_status = 'AVAILABLE')",
        )
        .bind(backup_arn)
        .fetch_one(&mut **tx)
        .await
        .map_err(db_err)?;
        if !available {
            return Err(StorageError::Validation(format!(
                "Backup not found: {backup_arn}"
            )));
        }
        let still_target: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM tables t \
             JOIN table_restores r ON r.table_id = t.table_id \
             WHERE t.table_id = ? AND r.source_backup_arn = ? \
             AND t.table_status = 'CREATING' AND t.status_transition_at IS NULL)",
        )
        .bind(&desc.table_id)
        .bind(backup_arn)
        .fetch_one(&mut **tx)
        .await
        .map_err(db_err)?;
        if !still_target {
            return Err(StorageError::TableNotFound(format!(
                "restore of {backup_arn} into {} did not complete: the table was deleted \
                 while it was being restored",
                desc.table_name
            )));
        }
        Ok(())
    }

    /// The detached half of RestoreTableFromBackup: copy the backup into the
    /// registered CREATING target, which flips it ACTIVE in the copy's last
    /// batch, or remove the target if the copy fails. Logs either outcome; a
    /// process exit part-way is cleaned up by the startup sweep.
    async fn finish_restore(&self, desc: &TableDescription, backup_arn: &str) {
        let target = &desc.table_name;
        let started = std::time::Instant::now();
        match self.copy_backup_items(desc, backup_arn).await {
            Ok(items) => tracing::info!(
                "restore of {backup_arn} into table '{target}' finished: {items} items in \
                 {:.3}s; the table is ACTIVE",
                started.elapsed().as_secs_f64()
            ),
            Err(e) => {
                tracing::error!(
                    "restore of {backup_arn} into table '{target}' failed, removing the \
                     partial table: {e}"
                );
                match self.abort_restore(&desc.table_id).await {
                    Ok(true) => tracing::info!(
                        "removed partially restored table '{target}' ({})",
                        desc.table_id
                    ),
                    // Deleted or already gone: nothing of this restore remains.
                    Ok(false) => {}
                    Err(cleanup) => tracing::error!(
                        "could not remove partially restored table '{target}' ({}); the next \
                         startup will: {cleanup}",
                        desc.table_id
                    ),
                }
            }
        }
    }

    /// Remove a restore target whose copy failed or was abandoned: its
    /// catalog row (cascading its index rows) and every data table. Only a
    /// target still CREATING with no scheduled transition is removed, and by
    /// table id, so this can never touch a table the restore did not create
    /// or one a client already deleted.
    async fn abort_restore(&self, table_id: &str) -> Result<bool, StorageError> {
        let _writer = self.write_lock.lock().await;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_err)?;
        let index_ids: Vec<String> =
            sqlx::query_scalar("SELECT index_id FROM indexes WHERE table_id = ?")
                .bind(table_id)
                .fetch_all(&mut *tx)
                .await
                .map_err(db_err)?;
        let removed = sqlx::query(
            "DELETE FROM tables WHERE table_id = ? AND table_status = 'CREATING' \
             AND status_transition_at IS NULL",
        )
        .bind(table_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?
        .rows_affected()
            > 0;
        if removed {
            for id in &index_ids {
                Self::drop_index_data_table(&mut tx, id).await?;
            }
            Self::drop_data_table(&mut tx, table_id).await?;
        }
        tx.commit().await.map_err(db_err)?;
        Ok(removed)
    }

    /// Create a file-backed backup from one WAL snapshot while writers run
    /// between bounded backup_items insert batches.
    async fn create_backup_from_wal_snapshot(
        &self,
        account_id: &str,
        table_name: &str,
        backup_name: &str,
        backup_arn: &str,
        created_at: f64,
    ) -> Result<BackupDetails, StorageError> {
        // The writer lock is held only while the definition is read and while
        // the item snapshot is established. No engine writer can change the
        // table between those two instants. Later backup_items inserts take the
        // lock for one bounded batch each, so unrelated writers do not wait for
        // the whole backup.
        let writer = self.write_lock.lock().await;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_err)?;
        let row: Option<(String, String, String, String)> = sqlx::query_as(
            "SELECT table_id, key_schema, attribute_definitions, billing_mode \
             FROM tables WHERE account_id = ? AND table_name = ? AND table_status = 'ACTIVE'",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db_err)?;
        let (table_id, key_schema_json, attr_defs, billing_mode) =
            row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;
        let key_schema: Vec<KeySchemaElement> = parse_json(&key_schema_json, "key schema")?;
        let definition = Self::capture_table_definition(&mut tx, &table_id).await?;
        let definition = serde_json::to_string(&definition.to_json()?)
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        sqlx::query(
            "INSERT INTO backups (backup_arn, backup_name, table_id, table_name, account_id, \
             backup_status, backup_size_bytes, item_count, key_schema, attribute_definitions, \
             billing_mode, created_at) VALUES (?, ?, ?, ?, ?, 'CREATING', 0, 0, ?, ?, ?, ?)",
        )
        .bind(backup_arn)
        .bind(backup_name)
        .bind(&table_id)
        .bind(table_name)
        .bind(account_id)
        .bind(&key_schema_json)
        .bind(&attr_defs)
        .bind(&billing_mode)
        .bind(format_epoch_seconds(created_at))
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        sqlx::query("INSERT INTO backup_definitions (backup_arn, definition) VALUES (?, ?)")
            .bind(backup_arn)
            .bind(&definition)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;

        let ddb_table = data_table_name(&table_id);
        let mut read_tx = self.pool.begin().await.map_err(db_err)?;
        // BEGIN is deferred. This first table read fixes its WAL snapshot while
        // the writer lock still excludes every engine write, so the definition,
        // source row and items describe one table state.
        let establish_snapshot = format!("SELECT 1 FROM {ddb_table} LIMIT 1");
        let _: Option<i64> = sqlx::query_scalar(&establish_snapshot)
            .fetch_optional(&mut *read_tx)
            .await
            .map_err(db_err)?;
        drop(writer);

        let mut last_rowid = 0_i64;
        let mut item_count = 0_i64;
        let mut size_bytes = 0_i64;
        loop {
            let batch = next_batch(&mut read_tx, &ddb_table, "rowid", None, last_rowid).await?;
            let Some(&(tail, _)) = batch.last() else {
                break;
            };
            last_rowid = tail;

            let mut prepared = Vec::with_capacity(batch.len());
            for (_, item_data) in batch {
                let item: Item = parse_json(&item_data, "item")?;
                let pk = composite_pk_to_text(&item, &key_schema)?;
                let size =
                    i64::try_from(extenddb_core::types::item_size_bytes(&item)).unwrap_or(i64::MAX);
                prepared.push((pk, item_data, size));
            }

            let _writer = self.write_lock.lock().await;
            let mut write_tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(db_err)?;
            let creating: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM backups \
                 WHERE backup_arn = ? AND backup_status = 'CREATING')",
            )
            .bind(backup_arn)
            .fetch_one(&mut *write_tx)
            .await
            .map_err(db_err)?;
            if !creating {
                return Err(StorageError::Validation(format!(
                    "Backup not found: {backup_arn}"
                )));
            }
            for (pk, item_data, size) in prepared {
                sqlx::query(
                    "INSERT INTO backup_items (backup_arn, pk, sk, item_data) \
                     VALUES (?, ?, NULL, ?)",
                )
                .bind(backup_arn)
                .bind(pk)
                .bind(item_data)
                .execute(&mut *write_tx)
                .await
                .map_err(db_err)?;
                item_count = item_count.saturating_add(1);
                size_bytes = size_bytes.saturating_add(size);
            }
            write_tx.commit().await.map_err(db_err)?;
        }
        read_tx.commit().await.map_err(db_err)?;

        let _writer = self.write_lock.lock().await;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_err)?;
        let updated = sqlx::query(
            "UPDATE backups SET backup_status = 'AVAILABLE', item_count = ?, \
             backup_size_bytes = ? WHERE backup_arn = ? AND backup_status = 'CREATING'",
        )
        .bind(item_count)
        .bind(size_bytes)
        .bind(backup_arn)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?
        .rows_affected();
        if updated != 1 {
            return Err(StorageError::Validation(format!(
                "Backup not found: {backup_arn}"
            )));
        }
        tx.commit().await.map_err(db_err)?;

        Ok(BackupDetails {
            backup_arn: backup_arn.to_owned(),
            backup_name: backup_name.to_owned(),
            backup_status: "AVAILABLE".to_owned(),
            backup_type: "USER".to_owned(),
            backup_size_bytes: size_bytes,
            backup_creation_date_time: created_at,
        })
    }

    /// In-memory SQLite has one connection, so it cannot hold a read snapshot
    /// and open a writer transaction concurrently. Keep its backup atomic on
    /// that connection; there is no second connection whose writes could make
    /// progress, and item memory remains bounded by one batch.
    async fn create_backup_single_connection(
        &self,
        account_id: &str,
        table_name: &str,
        backup_name: &str,
        backup_arn: &str,
        created_at: f64,
    ) -> Result<BackupDetails, StorageError> {
        let _writer = self.write_lock.lock().await;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_err)?;
        let row: Option<(String, String, String, String)> = sqlx::query_as(
            "SELECT table_id, key_schema, attribute_definitions, billing_mode \
             FROM tables WHERE account_id = ? AND table_name = ? AND table_status = 'ACTIVE'",
        )
        .bind(account_id)
        .bind(table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db_err)?;
        let (table_id, key_schema_json, attr_defs, billing_mode) =
            row.ok_or_else(|| StorageError::TableNotFound(table_name.to_owned()))?;
        let key_schema: Vec<KeySchemaElement> = parse_json(&key_schema_json, "key schema")?;
        let definition = Self::capture_table_definition(&mut tx, &table_id).await?;
        let definition = serde_json::to_string(&definition.to_json()?)
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        sqlx::query(
            "INSERT INTO backups (backup_arn, backup_name, table_id, table_name, account_id, \
             backup_status, backup_size_bytes, item_count, key_schema, attribute_definitions, \
             billing_mode, created_at) VALUES (?, ?, ?, ?, ?, 'CREATING', 0, 0, ?, ?, ?, ?)",
        )
        .bind(backup_arn)
        .bind(backup_name)
        .bind(&table_id)
        .bind(table_name)
        .bind(account_id)
        .bind(&key_schema_json)
        .bind(&attr_defs)
        .bind(&billing_mode)
        .bind(format_epoch_seconds(created_at))
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        sqlx::query("INSERT INTO backup_definitions (backup_arn, definition) VALUES (?, ?)")
            .bind(backup_arn)
            .bind(definition)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;

        let ddb_table = data_table_name(&table_id);
        let mut last_rowid = 0_i64;
        let mut item_count = 0_i64;
        let mut size_bytes = 0_i64;
        loop {
            let batch = next_batch(&mut tx, &ddb_table, "rowid", None, last_rowid).await?;
            let Some(&(tail, _)) = batch.last() else {
                break;
            };
            last_rowid = tail;
            for (_, item_data) in batch {
                let item: Item = parse_json(&item_data, "item")?;
                let pk = composite_pk_to_text(&item, &key_schema)?;
                size_bytes = size_bytes.saturating_add(
                    i64::try_from(extenddb_core::types::item_size_bytes(&item)).unwrap_or(i64::MAX),
                );
                sqlx::query(
                    "INSERT INTO backup_items (backup_arn, pk, sk, item_data) \
                     VALUES (?, ?, NULL, ?)",
                )
                .bind(backup_arn)
                .bind(pk)
                .bind(item_data)
                .execute(&mut *tx)
                .await
                .map_err(db_err)?;
                item_count = item_count.saturating_add(1);
            }
        }
        sqlx::query(
            "UPDATE backups SET backup_status = 'AVAILABLE', item_count = ?, \
             backup_size_bytes = ? WHERE backup_arn = ? AND backup_status = 'CREATING'",
        )
        .bind(item_count)
        .bind(size_bytes)
        .bind(backup_arn)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;

        Ok(BackupDetails {
            backup_arn: backup_arn.to_owned(),
            backup_name: backup_name.to_owned(),
            backup_status: "AVAILABLE".to_owned(),
            backup_type: "USER".to_owned(),
            backup_size_bytes: size_bytes,
            backup_creation_date_time: created_at,
        })
    }

    async fn remove_incomplete_backup(&self, backup_arn: &str) -> Result<bool, StorageError> {
        let _writer = self.write_lock.lock().await;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_err)?;
        let removed =
            sqlx::query("DELETE FROM backups WHERE backup_arn = ? AND backup_status = 'CREATING'")
                .bind(backup_arn)
                .execute(&mut *tx)
                .await
                .map_err(db_err)?
                .rows_affected()
                > 0;
        tx.commit().await.map_err(db_err)?;
        Ok(removed)
    }

    /// Remove backups left CREATING by a process that died during its copy.
    /// The backups delete cascades to definitions and any copied item rows.
    pub async fn sweep_incomplete_backups(&self) -> Result<Vec<String>, StorageError> {
        let _writer = self.write_lock.lock().await;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_err)?;
        let removed: Vec<String> = sqlx::query_scalar(
            "DELETE FROM backups WHERE backup_status = 'CREATING' RETURNING backup_arn",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok(removed)
    }

    /// Remove restore targets left by a process that died mid-restore.
    ///
    /// A restore target is the only table that sits CREATING with no
    /// scheduled transition. Called once at startup, before any request is
    /// served, so no restore of this process can be in flight and every such
    /// table is abandoned.
    ///
    /// This assumes one server process per database file, which the backend
    /// already depends on: writers are serialized by an in-process lock, so a
    /// second process writing the same file is unsupported regardless. A
    /// second process started against the file while the first is mid-restore
    /// would remove that restore's target.
    pub(crate) async fn sweep_abandoned_restores(&self) -> Result<Vec<String>, StorageError> {
        let candidates: Vec<(String, String)> = sqlx::query_as(
            "SELECT table_id, table_name FROM tables \
             WHERE table_status = 'CREATING' AND status_transition_at IS NULL",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        let mut removed = Vec::new();
        for (table_id, table_name) in candidates {
            if self.abort_restore(&table_id).await? {
                removed.push(table_name);
            }
        }
        Ok(removed)
    }
}

impl BackupEngine for SqliteEngine {
    fn create_backup(
        &self,
        account_id: &str,
        table_name: &str,
        backup_name: &str,
    ) -> BoxFuture<'_, Result<BackupDetails, StorageError>> {
        let engine = self.clone();
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        let backup_name = backup_name.to_owned();
        Box::pin(async move {
            // Admission: at most backup_server_slot_count backups run at once,
            // so a flood of CreateBackups cannot hold every pooled connection
            // (each holds its snapshot reader for its whole duration). The
            // permit travels with the detached task and is released when the
            // copy ends, whatever happened to the request.
            let permit = acquire_backup_permit(
                std::sync::Arc::clone(&engine.backup_server_slots),
                engine.backup_server_slot_count,
                BACKUP_SLOT_WAIT,
            )
            .await?;
            // Detach the operation from the request future. Once the CREATING
            // row commits, dropping an HTTP request must not drop the copy and
            // strand wire-visible state. A dropped JoinHandle leaves its Tokio
            // task running, so the copy reaches AVAILABLE or executes cleanup.
            let task = tokio::spawn(async move {
                let _permit = permit;
                // One id gives the ARN and the creation time, so the ARN, the
                // reported time, and the ListBackups cursor agree.
                let backup_id = extenddb_storage::util::new_backup_id();
                let backup_arn = extenddb_storage::util::backup_arn(
                    &engine.region,
                    &account_id,
                    &table_name,
                    &backup_id,
                );
                let created_at = extenddb_storage::util::backup_creation_date_time(&backup_id)
                    .ok_or_else(|| {
                        StorageError::Internal(format!("backup id {backup_id} has no timestamp"))
                    })?;
                let created = if engine.in_memory {
                    engine
                        .create_backup_single_connection(
                            &account_id,
                            &table_name,
                            &backup_name,
                            &backup_arn,
                            created_at,
                        )
                        .await
                } else {
                    engine
                        .create_backup_from_wal_snapshot(
                            &account_id,
                            &table_name,
                            &backup_name,
                            &backup_arn,
                            created_at,
                        )
                        .await
                };
                if let Err(error) = created {
                    if let Err(cleanup) = engine.remove_incomplete_backup(&backup_arn).await {
                        tracing::error!(
                            "backup {backup_arn} failed ({error}); could not remove its CREATING \
                             row: {cleanup}; the next startup will retry cleanup"
                        );
                    }
                    return Err(error);
                }
                created
            });
            task.await.map_err(|error| {
                StorageError::Internal(format!("SQLite backup task failed: {error}"))
            })?
        })
    }

    fn describe_backup(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            #[allow(clippy::type_complexity)]
            let row: Option<(String, String, String, String, i64, i64, String, String, String, String)> =
                sqlx::query_as(
                    "SELECT b.backup_name, b.backup_status, b.table_id, b.table_name, \
                     b.backup_size_bytes, b.item_count, b.key_schema, b.billing_mode, \
                     COALESCE(t.table_arn, \
                       'arn:aws:dynamodb:' || ? || ':' || b.account_id || ':table/' || b.table_name), \
                     b.created_at \
                     FROM backups b LEFT JOIN tables t ON t.table_id = b.table_id \
                     WHERE b.backup_arn = ? AND b.account_id = ? \
                     AND b.backup_status != 'DELETED'",
                )
                .bind(&self.region)
                .bind(&backup_arn)
                .bind(&account_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (name, status, table_id, table_name, size, count, ks, billing, table_arn, created) =
                row.ok_or_else(|| {
                    StorageError::Validation(format!("Backup not found: {backup_arn}"))
                })?;

            let key_schema: Vec<KeySchemaElement> =
                serde_json::from_str(&ks).map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(BackupDescription {
                backup_details: BackupDetails {
                    backup_arn: backup_arn.clone(),
                    backup_name: name,
                    backup_status: status,
                    backup_type: "USER".to_owned(),
                    backup_size_bytes: size,
                    backup_creation_date_time: ts_to_epoch(&created),
                },
                source_table_details: SourceTableDetails {
                    table_name,
                    table_id,
                    table_arn,
                    key_schema,
                    item_count: count,
                    table_size_bytes: size,
                    billing_mode: Some(billing),
                    table_creation_date_time: ts_to_epoch(&created),
                    // Filled by the engine from backup_restore_source for an
                    // AVAILABLE backup.
                    ..SourceTableDetails::default()
                },
                source_table_feature_details: None,
            })
        })
    }

    fn list_backups(
        &self,
        account_id: &str,
        table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.map(str::to_owned);
        Box::pin(async move {
            #[allow(clippy::type_complexity)]
            let rows: Vec<(String, String, String, String, i64, String, String, String)> =
                if let Some(tn) = table_name {
                    sqlx::query_as(
                        "SELECT b.backup_arn, b.backup_name, b.table_name, b.backup_status, \
                         b.backup_size_bytes, \
                         COALESCE(t.table_arn, 'arn:aws:dynamodb:' || ? || ':' || b.account_id || ':table/' || b.table_name), \
                         b.created_at, b.table_id FROM backups b LEFT JOIN tables t ON t.table_id = b.table_id \
                         WHERE b.account_id = ? AND b.table_name = ? AND b.backup_status != 'DELETED' \
                         ORDER BY b.created_at DESC",
                    )
                    .bind(&self.region)
                    .bind(&account_id)
                    .bind(tn)
                    .fetch_all(&self.pool)
                    .await
                } else {
                    sqlx::query_as(
                        "SELECT b.backup_arn, b.backup_name, b.table_name, b.backup_status, \
                         b.backup_size_bytes, \
                         COALESCE(t.table_arn, 'arn:aws:dynamodb:' || ? || ':' || b.account_id || ':table/' || b.table_name), \
                         b.created_at, b.table_id FROM backups b LEFT JOIN tables t ON t.table_id = b.table_id \
                         WHERE b.account_id = ? AND b.backup_status != 'DELETED' \
                         ORDER BY b.created_at DESC",
                    )
                    .bind(&self.region)
                    .bind(&account_id)
                    .fetch_all(&self.pool)
                    .await
                }
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(rows
                .into_iter()
                .map(
                    |(arn, name, tn, status, size, table_arn, created, table_id)| BackupSummary {
                        backup_arn: arn,
                        backup_name: name,
                        table_name: tn,
                        table_id,
                        table_arn,
                        backup_status: status,
                        backup_type: "USER".to_owned(),
                        backup_size_bytes: size,
                        backup_creation_date_time: ts_to_epoch(&created),
                    },
                )
                .collect())
        })
    }

    fn delete_backup(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            // Resolves account-scoped, so a backup owned by another account is
            // reported missing here and the writes below never run.
            let desc = self.describe_backup(&account_id, &backup_arn).await?;
            if desc.backup_details.backup_status != "AVAILABLE" {
                return Err(StorageError::Validation(format!(
                    "Backup not found: {backup_arn}"
                )));
            }

            // D1: every writer holds the engine write lock. The item delete
            // below is a bulk statement (one row per backed-up item), which is
            // exactly the slow-commit shape that can hold the SQLite file lock
            // past a concurrent locked writer's busy_timeout.
            let _writer = self.write_lock.lock().await;

            // One transaction, so a restore never sees the backup AVAILABLE
            // with its rows gone. The account predicate is repeated on every
            // write rather than relying on the lookup above, so the
            // statements are correct on their own terms.
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(db_err)?;
            // Refuse while a restore from this backup is still running, as the
            // service does. Under the write lock, which the restore also takes
            // to record itself, so the check cannot miss one.
            let restoring: Option<String> = sqlx::query_scalar(
                "SELECT t.table_name FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
                 WHERE r.source_backup_arn = ? AND t.table_status = 'CREATING' LIMIT 1",
            )
            .bind(&backup_arn)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_err)?;
            if let Some(table) = restoring {
                return Err(StorageError::BackupInUse(format!(
                    "Backup is being used to restore table {table}: {backup_arn}"
                )));
            }
            sqlx::query(
                "DELETE FROM backup_items WHERE backup_arn = ?1 AND EXISTS (\
                 SELECT 1 FROM backups b WHERE b.backup_arn = ?1 AND b.account_id = ?2)",
            )
            .bind(&backup_arn)
            .bind(&account_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query(
                "DELETE FROM backup_definitions WHERE backup_arn = ?1 AND EXISTS (\
                 SELECT 1 FROM backups b WHERE b.backup_arn = ?1 AND b.account_id = ?2)",
            )
            .bind(&backup_arn)
            .bind(&account_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query(
                "UPDATE backups SET backup_status = 'DELETED' \
                 WHERE backup_arn = ? AND account_id = ?",
            )
            .bind(&backup_arn)
            .bind(&account_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            tx.commit().await.map_err(db_err)?;
            Ok(BackupDescription {
                backup_details: BackupDetails {
                    backup_status: "DELETED".to_owned(),
                    ..desc.backup_details
                },
                source_table_details: desc.source_table_details,
                source_table_feature_details: None,
            })
        })
    }

    fn backup_restore_source(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupRestoreSource, StorageError>> {
        let account_id = account_id.to_owned();
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            #[allow(clippy::type_complexity)]
            let row: Option<(
                String,
                String,
                String,
                String,
                String,
                String,
                Option<String>,
                i64,
                i64,
                String,
                Option<String>,
            )> = sqlx::query_as(
                "SELECT b.table_name, b.table_id, \
                 COALESCE(t.table_arn, \
                   'arn:aws:dynamodb:' || ? || ':' || b.account_id || ':table/' || b.table_name), \
                 b.key_schema, b.attribute_definitions, b.billing_mode, \
                 b.provisioned_throughput, b.item_count, b.backup_size_bytes, b.created_at, \
                 d.definition \
                 FROM backups b \
                 LEFT JOIN tables t ON t.table_id = b.table_id \
                 LEFT JOIN backup_definitions d ON d.backup_arn = b.backup_arn \
                 WHERE b.backup_arn = ? AND b.account_id = ? AND b.backup_status = 'AVAILABLE'",
            )
            .bind(&self.region)
            .bind(&backup_arn)
            .bind(&account_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
            let (
                table_name,
                table_id,
                table_arn,
                ks,
                ad,
                billing_mode,
                row_throughput,
                item_count,
                size,
                created,
                definition,
            ) = row.ok_or_else(|| {
                StorageError::Validation(format!("Backup not found: {backup_arn}"))
            })?;
            // An unknown definition version is an error, never a legacy backup.
            let definition = definition
                .map(|d| {
                    BackupTableDefinition::from_json(
                        parse_json(&d, "table definition")?,
                        &backup_arn,
                    )
                })
                .transpose()?;
            let provisioned_throughput = match &definition {
                Some(d) => d.provisioned_throughput.clone(),
                None => {
                    let v: Option<serde_json::Value> = row_throughput
                        .as_deref()
                        .map(|s| parse_json(s, "backup throughput"))
                        .transpose()?;
                    throughput_from_catalog(v.as_ref())
                }
            };
            let created = ts_to_epoch(&created);
            Ok(BackupRestoreSource {
                backup_arn,
                backup_creation_date_time: created,
                source_table_name: table_name,
                source_table_arn: table_arn,
                source_table_id: table_id,
                key_schema: parse_json(&ks, "key schema")?,
                attribute_definitions: parse_json::<Vec<AttributeDefinition>>(
                    &ad,
                    "attribute definitions",
                )?,
                billing_mode,
                provisioned_throughput,
                item_count,
                table_size_bytes: size,
                // As DescribeBackup reports it.
                table_creation_date_time: created,
                definition,
                // This backend never recorded vector indexes outside the
                // definition, so a pre-0.0.4 backup has none to report.
                legacy_vector_index_count: 0,
            })
        })
    }

    fn restore_table_from_backup(
        &self,
        account_id: &str,
        request: RestoreRequest,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let engine = self.clone();
        let account_id = account_id.to_owned();
        Box::pin(async move {
            // The whole restore runs in a task that owns its inputs, so the
            // request future being dropped (a client timeout) stops neither the
            // registration nor the copy. The request waits only until the
            // CREATING target and its table_restores row have committed.
            let (registered_tx, registered_rx) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let RestoreRequest {
                    backup_arn,
                    source_table_arn,
                    restore_date_time,
                    input,
                } = request;
                // One write transaction: re-check the backup is AVAILABLE and
                // owned by the account, insert the CREATING target, record the
                // restore. DeleteBackup takes the same lock, so it sees either
                // no target or a CREATING target that names this backup.
                let registration = RestoreRegistration {
                    backup_arn: &backup_arn,
                    source_table_arn: &source_table_arn,
                    restore_date_time,
                };
                let registered = engine
                    .create_table_for_restore(&account_id, input, &registration)
                    .await
                    .map(|mut desc| {
                        desc.restore_summary = Some(extenddb_core::types::RestoreSummary {
                            source_backup_arn: Some(backup_arn.clone()),
                            source_table_arn: Some(source_table_arn.clone()),
                            restore_date_time,
                            restore_in_progress: true,
                        });
                        desc
                    });
                let desc = match registered {
                    Ok(desc) => desc,
                    Err(error) => {
                        let _ = registered_tx.send(Err(error));
                        return;
                    }
                };
                // A closed receiver means the request is gone; the copy goes on.
                let _ = registered_tx.send(Ok(desc.clone()));
                engine.finish_restore(&desc, &backup_arn).await;
            });
            registered_rx.await.map_err(|_| {
                StorageError::Internal(
                    "SQLite restore task ended before registering the restore".to_owned(),
                )
            })?
        })
    }

    fn describe_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE account_id = ? AND table_name = ?)",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            if !exists {
                return Err(StorageError::TableNotFound(table_name));
            }

            let pitr: Option<(bool,)> = sqlx::query_as(
                "SELECT pitr_enabled FROM continuous_backups WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            let enabled = pitr.is_some_and(|r| r.0);

            #[allow(clippy::cast_precision_loss)]
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as f64;

            Ok(ContinuousBackupsDescription {
                continuous_backups_status: "ENABLED".to_owned(),
                point_in_time_recovery_description: Some(PointInTimeRecoveryDescription {
                    point_in_time_recovery_status: if enabled { "ENABLED" } else { "DISABLED" }
                        .to_owned(),
                    earliest_restorable_date_time: enabled.then_some(now - 35.0 * 24.0 * 3600.0),
                    latest_restorable_date_time: enabled.then_some(now),
                }),
            })
        })
    }

    fn update_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
        pitr_enabled: bool,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE account_id = ? AND table_name = ?)",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            if !exists {
                return Err(StorageError::TableNotFound(table_name));
            }
            {
                // D1: every writer holds the engine write lock. Scoped so the
                // read-only describe below runs after release.
                let _writer = self.write_lock.lock().await;
                sqlx::query(
                    "INSERT INTO continuous_backups (account_id, table_name, pitr_enabled) \
                     VALUES (?, ?, ?) \
                     ON CONFLICT (account_id, table_name) DO UPDATE SET pitr_enabled = excluded.pitr_enabled",
                )
                .bind(&account_id)
                .bind(&table_name)
                .bind(pitr_enabled)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
            self.describe_continuous_backups(&account_id, &table_name)
                .await
        })
    }
}

#[cfg(test)]
mod restore_tests {
    use std::time::Duration;

    use extenddb_core::types::{Item, TableDescription, TableKeyInfo};
    use extenddb_storage::error::StorageError;
    use extenddb_storage::{BackupEngine, DataEngine, RestoreRequest, RestoreTableOverrides};
    use serde_json::json;

    use super::{acquire_backup_permit, backup_server_slot_count};
    use crate::SqliteEngine;
    use crate::create_table::RestoreRegistration;

    const ACCOUNT: &str = "000000000000";

    /// What the engine does before it calls `restore_table_from_backup`:
    /// resolve the backup, run the refusals, build the target's input.
    async fn restore_request(
        engine: &SqliteEngine,
        target: &str,
        backup_arn: &str,
    ) -> Result<RestoreRequest, StorageError> {
        let source = engine.backup_restore_source(ACCOUNT, backup_arn).await?;
        source.ensure_restorable()?;
        let input = source.restore_input(target, &RestoreTableOverrides::default());
        Ok(RestoreRequest {
            backup_arn: backup_arn.to_owned(),
            source_table_arn: source.source_table_arn.clone(),
            restore_date_time: source.backup_creation_date_time,
            input,
        })
    }

    /// The ARN of a backup that does not exist, with a well-formed id: `arn`
    /// with its last hex digit changed.
    fn flip_last_hex(arn: &str) -> String {
        let (head, last) = arn.split_at(arn.len() - 1);
        format!("{head}{}", if last == "0" { "1" } else { "0" })
    }

    /// Start a restore; returns the CREATING description without waiting for
    /// the copy.
    async fn start_restore(
        engine: &SqliteEngine,
        target: &str,
        backup_arn: &str,
    ) -> Result<TableDescription, StorageError> {
        let request = restore_request(engine, target, backup_arn).await?;
        engine.restore_table_from_backup(ACCOUNT, request).await
    }

    /// Poll the target's catalog row until its detached copy is done: `true`
    /// once ACTIVE, `false` once the row is gone (the copy failed and removed
    /// it). Panics if neither happens within 60 s.
    async fn wait_for_copy(engine: &SqliteEngine, table_id: &str) -> bool {
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let status: Option<String> =
                    sqlx::query_scalar("SELECT table_status FROM tables WHERE table_id = ?")
                        .bind(table_id)
                        .fetch_optional(&engine.pool)
                        .await
                        .expect("target status");
                match status.as_deref() {
                    Some("ACTIVE") => return true,
                    None => return false,
                    Some("CREATING") => tokio::time::sleep(Duration::from_millis(5)).await,
                    Some(other) => panic!("restore target in unexpected status {other}"),
                }
            }
        })
        .await
        .expect("the detached restore copy must finish within 60 s")
    }

    /// Restore and wait for the copy; an error if the copy removed the target.
    async fn restore(
        engine: &SqliteEngine,
        target: &str,
        backup_arn: &str,
    ) -> Result<TableDescription, StorageError> {
        let desc = start_restore(engine, target, backup_arn).await?;
        assert_eq!(
            desc.table_status,
            extenddb_core::types::TableStatus::Creating
        );
        if wait_for_copy(engine, &desc.table_id).await {
            Ok(desc)
        } else {
            Err(StorageError::Internal(format!(
                "restore into {target} failed and removed the target"
            )))
        }
    }

    async fn engine() -> SqliteEngine {
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::migrations::apply_for_tests(&engine.pool).await;
        sqlx::query("INSERT INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0') ON CONFLICT(key) DO UPDATE SET value = excluded.value")
            .execute(&engine.pool)
            .await
            .expect("delay");
        sqlx::query("INSERT INTO settings (key, value) VALUES ('index_propagation_delay_ms', '0') ON CONFLICT(key) DO UPDATE SET value = excluded.value")
            .execute(&engine.pool)
            .await
            .expect("propagation");
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
            .bind(ACCOUNT)
            .execute(&engine.pool)
            .await
            .expect("account");
        engine
    }

    /// A provisioned two-part-key table with one GSI and one LSI.
    async fn indexed_table(engine: &SqliteEngine, name: &str) -> String {
        let input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
            "TableName": name,
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "N"},
                {"AttributeName": "g", "AttributeType": "S"},
                {"AttributeName": "l", "AttributeType": "S"}
            ],
            "BillingMode": "PROVISIONED",
            "ProvisionedThroughput": {"ReadCapacityUnits": 7, "WriteCapacityUnits": 9},
            "GlobalSecondaryIndexes": [{
                "IndexName": "gi",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
                "ProvisionedThroughput": {"ReadCapacityUnits": 3, "WriteCapacityUnits": 4}
            }],
            "LocalSecondaryIndexes": [{
                "IndexName": "li",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "l", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }]
        }))
        .expect("input");
        let desc = engine
            .create_table_impl(ACCOUNT, input, false)
            .await
            .expect("create");
        let key_info = TableKeyInfo {
            table_name: name.to_owned(),
            account_id: ACCOUNT.to_owned(),
            table_id: desc.table_id.clone(),
            key_schema: desc.key_schema.clone(),
            base_key_schema: desc.key_schema.clone(),
            attribute_definitions: desc.attribute_definitions.clone(),
            ..Default::default()
        };
        for i in 0..30 {
            let mut item = json!({"pk": {"S": format!("p{}", i % 3)}, "sk": {"N": i.to_string()}});
            if i % 2 == 0 {
                item["g"] = json!({"S": format!("g{}", i % 4)});
            }
            if i % 3 == 0 {
                item["l"] = json!({"S": format!("l{i}")});
            }
            let item: Item = serde_json::from_value(item).expect("item");
            engine
                .put_item(
                    &key_info,
                    item,
                    false,
                    None,
                    &extenddb_core::expression::ExpressionMaps::default(),
                    None,
                )
                .await
                .expect("put");
        }
        desc.table_id
    }

    async fn count(engine: &SqliteEngine, table: &str) -> i64 {
        sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&engine.pool)
            .await
            .expect("count")
    }

    async fn index_tables(engine: &SqliteEngine, table_id: &str) -> Vec<(String, String)> {
        sqlx::query_as(
            "SELECT index_name, index_id FROM indexes WHERE table_id = ? ORDER BY index_name",
        )
        .bind(table_id)
        .fetch_all(&engine.pool)
        .await
        .expect("indexes")
    }

    #[tokio::test]
    async fn restore_recreates_indexes_and_throughput() {
        let engine = engine().await;
        let src = indexed_table(&engine, "src").await;
        let backup = engine
            .create_backup(ACCOUNT, "src", "bkp")
            .await
            .expect("backup");
        let desc = restore(&engine, "dst", &backup.backup_arn)
            .await
            .expect("restore");

        let (status, pt): (String, Option<String>) = sqlx::query_as(
            "SELECT table_status, provisioned_throughput FROM tables WHERE table_id = ?",
        )
        .bind(&desc.table_id)
        .fetch_one(&engine.pool)
        .await
        .expect("row");
        assert_eq!(status, "ACTIVE");
        let pt: serde_json::Value = serde_json::from_str(&pt.expect("throughput")).expect("json");
        assert_eq!(pt["ReadCapacityUnits"], 7);
        assert_eq!(pt["WriteCapacityUnits"], 9);

        // Same indexes, each holding the same number of rows as the source's.
        let src_idx = index_tables(&engine, &src).await;
        let dst_idx = index_tables(&engine, &desc.table_id).await;
        assert_eq!(
            src_idx.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
            vec!["gi", "li"]
        );
        assert_eq!(
            dst_idx.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>(),
            src_idx.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>()
        );
        for ((_, s), (_, d)) in src_idx.iter().zip(&dst_idx) {
            let (s, d) = (
                crate::data::index_table_name(s),
                crate::data::index_table_name(d),
            );
            assert_eq!(count(&engine, &d).await, count(&engine, &s).await);
            assert!(count(&engine, &d).await > 0);
        }
        assert_eq!(
            count(&engine, &crate::data::data_table_name(&desc.table_id)).await,
            30
        );
    }

    #[tokio::test]
    async fn startup_sweep_removes_an_abandoned_restore() {
        let engine = engine().await;
        indexed_table(&engine, "src").await;
        let backup = engine
            .create_backup(ACCOUNT, "src", "bkp")
            .await
            .expect("backup");
        let desc = restore(&engine, "dst", &backup.backup_arn)
            .await
            .expect("restore");
        let index_ids: Vec<String> = index_tables(&engine, &desc.table_id)
            .await
            .into_iter()
            .map(|(_, id)| id)
            .collect();

        // The state a process killed mid-copy leaves: the target CREATING with
        // no scheduled transition (its copy transaction rolled back).
        sqlx::query(
            "UPDATE tables SET table_status = 'CREATING', status_transition_at = NULL \
             WHERE table_id = ?",
        )
        .bind(&desc.table_id)
        .execute(&engine.pool)
        .await
        .expect("simulate crash");

        let removed = engine.sweep_abandoned_restores().await.expect("sweep");
        assert_eq!(removed, vec!["dst".to_owned()]);
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tables WHERE table_name = 'dst'")
            .fetch_one(&engine.pool)
            .await
            .expect("rows");
        assert_eq!(rows, 0);
        let mut leftover = vec![desc.table_id.clone()];
        leftover.extend(index_ids);
        for id in leftover {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)",
            )
            .bind(format!("_ddb_{id}"))
            .fetch_one(&engine.pool)
            .await
            .expect("exists");
            assert!(!exists, "data table _ddb_{id} left behind");
        }

        // A live table is never a candidate, and the name is free again.
        assert!(
            engine
                .sweep_abandoned_restores()
                .await
                .expect("sweep")
                .is_empty()
        );
        restore(&engine, "dst", &backup.backup_arn)
            .await
            .expect("restore again");
    }

    #[tokio::test]
    async fn startup_sweep_removes_an_incomplete_backup_and_its_rows() {
        let engine = engine().await;
        indexed_table(&engine, "src").await;
        let backup = engine
            .create_backup(ACCOUNT, "src", "bkp")
            .await
            .expect("backup");
        sqlx::query("UPDATE backups SET backup_status = 'CREATING' WHERE backup_arn = ?")
            .bind(&backup.backup_arn)
            .execute(&engine.pool)
            .await
            .expect("simulate crash");
        let listed = engine
            .list_backups(ACCOUNT, Some("src"))
            .await
            .expect("list CREATING backup");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].backup_status, "CREATING");
        let described = engine
            .describe_backup(ACCOUNT, &backup.backup_arn)
            .await
            .expect("describe CREATING backup");
        assert_eq!(described.backup_details.backup_status, "CREATING");
        engine
            .delete_backup(ACCOUNT, &backup.backup_arn)
            .await
            .expect_err("DeleteBackup only accepts AVAILABLE backups");

        let removed = engine
            .sweep_incomplete_backups()
            .await
            .expect("backup sweep");
        assert_eq!(removed, vec![backup.backup_arn.clone()]);
        let backup_rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM backups WHERE backup_arn = ?")
                .bind(&backup.backup_arn)
                .fetch_one(&engine.pool)
                .await
                .expect("backup rows");
        let item_rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM backup_items WHERE backup_arn = ?")
                .bind(&backup.backup_arn)
                .fetch_one(&engine.pool)
                .await
                .expect("item rows");
        let definition_rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM backup_definitions WHERE backup_arn = ?")
                .bind(&backup.backup_arn)
                .fetch_one(&engine.pool)
                .await
                .expect("definition rows");
        assert_eq!((backup_rows, item_rows, definition_rows), (0, 0, 0));
        assert!(
            engine
                .sweep_incomplete_backups()
                .await
                .expect("second sweep")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn failed_backup_copy_removes_its_creating_row() {
        let (engine, path) = file_engine().await;
        let source = plain_table(&engine, "source").await;
        let table = crate::data::data_table_name(&source.table_id);
        sqlx::query(&format!(
            "INSERT INTO {table} (pk, item_data) VALUES ('broken', 'not json')"
        ))
        .execute(&engine.pool)
        .await
        .expect("corrupt source row");

        engine
            .create_backup(ACCOUNT, "source", "broken")
            .await
            .expect_err("invalid source item must fail the backup");
        let leftovers: (i64, i64, i64) = sqlx::query_as(
            "SELECT \
             (SELECT COUNT(*) FROM backups WHERE backup_name = 'broken'), \
             (SELECT COUNT(*) FROM backup_definitions d \
              JOIN backups b ON b.backup_arn = d.backup_arn WHERE b.backup_name = 'broken'), \
             (SELECT COUNT(*) FROM backup_items i \
              JOIN backups b ON b.backup_arn = i.backup_arn WHERE b.backup_name = 'broken')",
        )
        .fetch_one(&engine.pool)
        .await
        .expect("leftovers");
        assert_eq!(leftovers, (0, 0, 0));
        close_file_engine(engine, &path).await;
    }

    #[tokio::test]
    async fn failed_copy_removes_the_target() {
        let engine = engine().await;
        indexed_table(&engine, "src").await;
        let backup = engine
            .create_backup(ACCOUNT, "src", "bkp")
            .await
            .expect("backup");
        // Corrupt one backup row so the copy fails part-way.
        sqlx::query(
            "UPDATE backup_items SET item_data = '{\"pk\": {\"S\": \"x\"}}' \
             WHERE id = (SELECT MAX(id) FROM backup_items WHERE backup_arn = ?)",
        )
        .bind(&backup.backup_arn)
        .execute(&engine.pool)
        .await
        .expect("corrupt");
        let before: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'")
                .fetch_one(&engine.pool)
                .await
                .expect("tables");
        // The registration succeeds; the detached copy fails part-way and
        // removes the target it created.
        let desc = start_restore(&engine, "dst", &backup.backup_arn)
            .await
            .expect("registration");
        assert!(
            !wait_for_copy(&engine, &desc.table_id).await,
            "an item without its sort key cannot restore"
        );
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tables WHERE table_name = 'dst'")
            .fetch_one(&engine.pool)
            .await
            .expect("rows");
        assert_eq!(rows, 0);
        let after: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'")
                .fetch_one(&engine.pool)
                .await
                .expect("tables");
        assert_eq!(
            after, before,
            "the target's data and index tables are dropped"
        );
    }

    #[tokio::test]
    async fn restore_intent_is_committed_with_target() {
        let engine = engine().await;
        indexed_table(&engine, "src").await;
        let backup = engine
            .create_backup(ACCOUNT, "src", "bkp")
            .await
            .expect("backup");
        let input = serde_json::from_value(json!({
            "TableName": "dst",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("input");

        // This is the state exposed between target creation and the first copy
        // batch. Both rows must become visible together, before restore gives
        // DeleteBackup any chance to acquire the write lock.
        let desc = engine
            .create_table_for_restore(
                ACCOUNT,
                input,
                &RestoreRegistration {
                    backup_arn: &backup.backup_arn,
                    source_table_arn: "arn:aws:dynamodb:us-east-1:000000000000:table/src",
                    restore_date_time: backup.backup_creation_date_time,
                },
            )
            .await
            .expect("create restore target");
        let mut tx = engine.pool.begin().await.expect("read transaction");
        let state: (bool, bool) = sqlx::query_as(
            "SELECT \
             EXISTS(SELECT 1 FROM tables WHERE table_id = ? AND table_status = 'CREATING'), \
             EXISTS(SELECT 1 FROM table_restores \
                    WHERE table_id = ? AND source_backup_arn = ?)",
        )
        .bind(&desc.table_id)
        .bind(&desc.table_id)
        .bind(&backup.backup_arn)
        .fetch_one(&mut *tx)
        .await
        .expect("restore state");
        tx.commit().await.expect("read commit");
        assert_eq!(state, (true, true));

        let err = engine
            .delete_backup(ACCOUNT, &backup.backup_arn)
            .await
            .expect_err("restore intent must block deletion");
        assert!(
            matches!(err, extenddb_storage::error::StorageError::BackupInUse(_)),
            "{err:?}"
        );

        assert!(engine.abort_restore(&desc.table_id).await.expect("abort"));
        let intents: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM table_restores WHERE source_backup_arn = ?")
                .bind(&backup.backup_arn)
                .fetch_one(&engine.pool)
                .await
                .expect("intent count");
        assert_eq!(intents, 0, "target cleanup must remove restore intent");
        engine
            .delete_backup(ACCOUNT, &backup.backup_arn)
            .await
            .expect("backup is deletable after abort");
    }

    async fn create(engine: &SqliteEngine, input: serde_json::Value) -> String {
        let input: extenddb_core::types::CreateTableInput =
            serde_json::from_value(input).expect("input");
        engine
            .create_table_impl(ACCOUNT, input, false)
            .await
            .expect("create")
            .table_id
    }

    #[tokio::test]
    async fn multipart_key_backup_is_refused() {
        let engine = engine().await;
        create(
            &engine,
            json!({
                "TableName": "m",
                "KeySchema": [
                    {"AttributeName": "a", "KeyType": "HASH"},
                    {"AttributeName": "b", "KeyType": "HASH"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "a", "AttributeType": "S"},
                    {"AttributeName": "b", "AttributeType": "S"}
                ],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        )
        .await;
        let backup = engine
            .create_backup(ACCOUNT, "m", "bkp")
            .await
            .expect("backup");
        // The refusal is the engine's (`ensure_restorable` in `restore`); this
        // checks the backend reports the backup's two-part key so it fires.
        let err = restore(&engine, "m2", &backup.backup_arn)
            .await
            .expect_err("refused");
        assert!(
            matches!(err, extenddb_storage::error::StorageError::Unsupported(_)),
            "{err:?}"
        );
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tables WHERE table_name = 'm2'")
            .fetch_one(&engine.pool)
            .await
            .expect("rows");
        assert_eq!(rows, 0);
    }

    /// SSE and the on-demand limits are carried by the restore; the table
    /// class is not (the service restores STANDARD, so the restored row has
    /// no class; the engine's `restore_input` drops it).
    #[tokio::test]
    async fn restore_keeps_sse_and_on_demand_limits_but_not_table_class() {
        let engine = engine().await;
        create(
            &engine,
            json!({
                "TableName": "c",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST",
                "TableClass": "STANDARD_INFREQUENT_ACCESS",
                "SSESpecification": {"Enabled": true, "SSEType": "KMS"},
                "OnDemandThroughput": {"MaxReadRequestUnits": 100, "MaxWriteRequestUnits": 50}
            }),
        )
        .await;
        let backup = engine
            .create_backup(ACCOUNT, "c", "bkp")
            .await
            .expect("backup");
        restore(&engine, "c2", &backup.backup_arn)
            .await
            .expect("restore");
        let read = |name: &'static str| {
            let pool = engine.pool.clone();
            async move {
                let row: (String, Option<String>, Option<String>, Option<String>) = sqlx::query_as(
                    "SELECT billing_mode, table_class, sse_specification, on_demand_throughput \
                     FROM tables WHERE table_name = ?",
                )
                .bind(name)
                .fetch_one(&pool)
                .await
                .expect("row");
                let json = |v: Option<String>| {
                    v.map(|s| serde_json::from_str::<serde_json::Value>(&s).expect("json"))
                };
                (row.0, row.1, json(row.2), json(row.3))
            }
        };
        let want = read("c").await;
        assert_eq!(want.1.as_deref(), Some("STANDARD_INFREQUENT_ACCESS"));
        assert!(want.2.is_some() && want.3.is_some(), "{want:?}");
        let got = read("c2").await;
        assert_eq!(got.1, None, "the restore does not carry the table class");
        assert_eq!((got.0, got.2, got.3), (want.0, want.2, want.3));
    }

    #[tokio::test]
    async fn restore_crosses_batch_boundaries_and_keeps_n_key_order() {
        let engine = engine().await;
        let src = indexed_table(&engine, "src").await;
        let (src_gsi_before,) = (index_tables(&engine, &src).await[0].1.clone(),);
        let base = crate::data::data_table_name(&src);
        // Add rows straight into the source, well past one read batch, with N
        // sort keys whose encoded order differs from their text order.
        let key_info = TableKeyInfo {
            table_name: "src".to_owned(),
            account_id: ACCOUNT.to_owned(),
            table_id: src.clone(),
            key_schema: serde_json::from_value(json!([
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ]))
            .expect("ks"),
            ..Default::default()
        };
        let mut key_info = key_info;
        key_info.base_key_schema = key_info.key_schema.clone();
        key_info.attribute_definitions = serde_json::from_value(json!([
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
            {"AttributeName": "g", "AttributeType": "S"},
            {"AttributeName": "l", "AttributeType": "S"}
        ]))
        .expect("ad");
        // More rows than one read batch (500), so the restore commits
        // several batches.
        for i in 0..700 {
            let n = if i % 2 == 0 {
                format!("-{i}.5")
            } else {
                format!("{}", i * 1000)
            };
            let item: Item = serde_json::from_value(json!({
                "pk": {"S": "bulk"}, "sk": {"N": n}, "g": {"S": "gbulk"}
            }))
            .expect("item");
            engine
                .put_item(
                    &key_info,
                    item,
                    false,
                    None,
                    &extenddb_core::expression::ExpressionMaps::default(),
                    None,
                )
                .await
                .expect("put");
        }
        let backup = engine
            .create_backup(ACCOUNT, "src", "bkp")
            .await
            .expect("backup");
        let desc = restore(&engine, "dst", &backup.backup_arn)
            .await
            .expect("restore");
        let dst = crate::data::data_table_name(&desc.table_id);
        assert_eq!(count(&engine, &dst).await, count(&engine, &base).await);
        assert_eq!(count(&engine, &dst).await, 730);
        // The physical rows are identical to the ones the write path made,
        // sort-key encoding included, so key order is preserved.
        let rows = |t: String| {
            let pool = engine.pool.clone();
            async move {
                sqlx::query_scalar::<_, String>(&format!(
                    "SELECT pk || '|' || sk_n || '|' || item_data FROM {t} ORDER BY pk, sk_n"
                ))
                .fetch_all(&pool)
                .await
                .expect("rows")
            }
        };
        assert_eq!(rows(dst).await, rows(base).await);
        let dst_gsi = index_tables(&engine, &desc.table_id).await[0].1.clone();
        let gsi_rows = |id: String| {
            let pool = engine.pool.clone();
            async move {
                sqlx::query_scalar::<_, String>(&format!(
                    "SELECT pk || '|' || base_pk || '|' || base_sk_n || '|' || item_data \
                     FROM {} ORDER BY 1",
                    crate::data::index_table_name(&id)
                ))
                .fetch_all(&pool)
                .await
                .expect("gsi rows")
            }
        };
        assert_eq!(gsi_rows(dst_gsi).await, gsi_rows(src_gsi_before).await);
    }

    /// A database with the schema catalog 0.0.3 shipped, holding a backup in
    /// the row shape 0.0.3 wrote (`pk` empty, `sk` NULL, no definition), is
    /// brought to the latest migration by the runner, as `extenddb migrate
    /// --yes` does; the old backup then restores as it did before: keys and
    /// items, no secondary indexes.
    #[tokio::test]
    async fn a_0_0_3_database_migrates_and_its_backups_restore() {
        let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("engine");
        sqlx::raw_sql(include_str!("../testdata/schema_0_0_3.sql"))
            .execute(&engine.pool)
            .await
            .expect("0.0.3 schema");
        sqlx::query("INSERT INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0') ON CONFLICT(key) DO UPDATE SET value = excluded.value")
            .execute(&engine.pool)
            .await
            .expect("delay");
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
            .bind(ACCOUNT)
            .execute(&engine.pool)
            .await
            .expect("account");
        let on_pool = crate::migrations::SqliteMigrations::on_pool(engine.pool.clone(), ":memory:")
            .expect("one connection");
        let runner = extenddb_storage::migrations::Runner::new(&on_pool, "test");
        assert!(
            matches!(
                runner.status().await.expect("status"),
                extenddb_storage::migrations::CatalogStatus::Behind { version: None, .. }
            ),
            "a 0.0.3 database has no ledger"
        );

        // A backup exactly as the 0.0.3 binary wrote one.
        let arn = format!("arn:aws:dynamodb:us-east-1:{ACCOUNT}:table/old/backup/1-00000000");
        sqlx::query(
            "INSERT INTO backups (backup_arn, backup_name, table_id, table_name, account_id, \
             backup_status, backup_size_bytes, item_count, key_schema, attribute_definitions, \
             billing_mode) VALUES (?, 'b', 'gone', 'old', ?, 'AVAILABLE', 0, 3, ?, ?, \
             'PROVISIONED')",
        )
        .bind(&arn)
        .bind(ACCOUNT)
        .bind(r#"[{"AttributeName":"pk","KeyType":"HASH"},{"AttributeName":"sk","KeyType":"RANGE"}]"#)
        .bind(r#"[{"AttributeName":"pk","AttributeType":"S"},{"AttributeName":"sk","AttributeType":"N"}]"#)
        .execute(&engine.pool)
        .await
        .expect("old backup row");
        for i in 0..3 {
            sqlx::query(
                "INSERT INTO backup_items (backup_arn, pk, sk, item_data) VALUES (?, '', NULL, ?)",
            )
            .bind(&arn)
            .bind(format!(
                r#"{{"pk":{{"S":"a"}},"sk":{{"N":"{i}"}},"v":{{"S":"x{i}"}}}}"#
            ))
            .execute(&engine.pool)
            .await
            .expect("old backup item");
        }

        // Migration 1 rebuilds backup_items and says how many rows it copied.
        let report = runner.migrate().await.expect("migrate");
        let baseline_notes = report.applied[0].notes.join("\n");
        assert!(
            baseline_notes.contains("copying 3 row(s)"),
            "{baseline_notes}"
        );
        assert!(
            matches!(
                runner.status().await.expect("status after"),
                extenddb_storage::migrations::CatalogStatus::Current { .. }
            ),
            "current after the run"
        );
        let id_is_primary_key: i64 = sqlx::query_scalar(
            "SELECT pk FROM pragma_table_info('backup_items') WHERE name = 'id'",
        )
        .fetch_one(&engine.pool)
        .await
        .expect("backup_items id");
        assert_eq!(id_is_primary_key, 1);
        let migrated_items: Vec<(i64, String)> =
            sqlx::query_as("SELECT id, item_data FROM backup_items ORDER BY id")
                .fetch_all(&engine.pool)
                .await
                .expect("migrated backup items");
        assert_eq!(
            migrated_items.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert!(migrated_items[0].1.contains("x0"));
        assert!(migrated_items[1].1.contains("x1"));
        assert!(migrated_items[2].1.contains("x2"));
        let desc = restore(&engine, "new", &arn)
            .await
            .expect("restore a 0.0.3 backup");
        assert!(index_tables(&engine, &desc.table_id).await.is_empty());
        assert_eq!(
            count(&engine, &crate::data::data_table_name(&desc.table_id)).await,
            3
        );
        let pt: String =
            sqlx::query_scalar("SELECT provisioned_throughput FROM tables WHERE table_id = ?")
                .bind(&desc.table_id)
                .fetch_one(&engine.pool)
                .await
                .expect("throughput");
        let pt: serde_json::Value = serde_json::from_str(&pt).expect("json");
        assert_eq!(
            (
                pt["ReadCapacityUnits"].as_i64(),
                pt["WriteCapacityUnits"].as_i64()
            ),
            (Some(5), Some(5))
        );
    }

    #[tokio::test]
    async fn batches_are_cut_by_stored_bytes() {
        let engine = engine().await;
        sqlx::query("CREATE TABLE t (item_data TEXT NOT NULL)")
            .execute(&engine.pool)
            .await
            .expect("table");
        // One row over the budget on its own, then small rows, then two rows
        // that together exceed it.
        let big = "x".repeat(super::COPY_BATCH_BYTES + 10);
        let half = "y".repeat(super::COPY_BATCH_BYTES / 2 + 10);
        let mut rows = vec![big.clone()];
        rows.extend((0..10).map(|i| format!("small-{i}")));
        rows.push(half.clone());
        rows.push(half.clone());
        for r in &rows {
            sqlx::query("INSERT INTO t (item_data) VALUES (?)")
                .bind(r)
                .execute(&engine.pool)
                .await
                .expect("insert");
        }
        let mut tx = engine.pool.begin().await.expect("tx");
        let mut last = 0;
        let mut seen: Vec<String> = Vec::new();
        let mut batches: Vec<usize> = Vec::new();
        loop {
            let batch = super::next_batch(&mut tx, "t", "rowid", None, last)
                .await
                .expect("batch");
            let Some(&(tail, _)) = batch.last() else {
                break;
            };
            let bytes: usize = batch.iter().map(|(_, d)| d.len()).sum();
            assert!(
                batch.len() == 1 || bytes <= super::COPY_BATCH_BYTES,
                "{bytes}"
            );
            batches.push(batch.len());
            seen.extend(batch.into_iter().map(|(_, d)| d));
            last = tail;
        }
        assert_eq!(seen, rows, "every row once, in order");
        // The oversized row alone; then small rows and one half; then the other half.
        assert_eq!(batches, vec![1, 11, 1]);
    }

    #[tokio::test]
    async fn delete_table_refuses_a_restore_in_progress() {
        let engine = engine().await;
        indexed_table(&engine, "src").await;
        let backup = engine
            .create_backup(ACCOUNT, "src", "bkp")
            .await
            .expect("backup");
        let desc = restore(&engine, "dst", &backup.backup_arn)
            .await
            .expect("restore");
        // Back to the state the target is in while its copy runs.
        sqlx::query(
            "UPDATE tables SET table_status = 'CREATING', status_transition_at = NULL \
             WHERE table_id = ?",
        )
        .bind(&desc.table_id)
        .execute(&engine.pool)
        .await
        .expect("in progress");
        let err = extenddb_storage::TableEngine::delete_table(
            &engine,
            ACCOUNT,
            extenddb_core::types::DeleteTableInput {
                table_name: "dst".to_owned(),
            },
        )
        .await
        .expect_err("refused");
        assert!(
            matches!(err, extenddb_storage::error::StorageError::IndexesInUse(_)),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn restore_summary_and_backup_in_use() {
        use extenddb_storage::TableEngine;
        let engine = engine().await;
        indexed_table(&engine, "src").await;
        let backup = engine
            .create_backup(ACCOUNT, "src", "bkp")
            .await
            .expect("backup");
        let desc = restore(&engine, "dst", &backup.backup_arn)
            .await
            .expect("restore");
        let describe = |name: &'static str| {
            let engine = &engine;
            async move {
                engine
                    .describe_table(
                        ACCOUNT,
                        extenddb_core::types::DescribeTableInput {
                            table_name: name.to_owned(),
                        },
                    )
                    .await
                    .expect("describe")
            }
        };
        let done = describe("dst").await.restore_summary.expect("summary");
        assert_eq!(
            done.source_backup_arn.as_deref(),
            Some(backup.backup_arn.as_str())
        );
        let src_arn = describe("src").await.table_arn;
        assert_eq!(done.source_table_arn.as_deref(), Some(src_arn.as_str()));
        // The backup's creation time, as the service reports it.
        assert_eq!(done.restore_date_time, backup.backup_creation_date_time);
        assert!(!done.restore_in_progress);
        assert!(done.restore_date_time > 0.0);
        assert!(describe("src").await.restore_summary.is_none());

        sqlx::query(
            "UPDATE tables SET table_status = 'CREATING', status_transition_at = NULL \
             WHERE table_id = ?",
        )
        .bind(&desc.table_id)
        .execute(&engine.pool)
        .await
        .expect("in progress");
        assert!(
            describe("dst")
                .await
                .restore_summary
                .expect("summary")
                .restore_in_progress
        );
        let err = engine
            .delete_backup(ACCOUNT, &backup.backup_arn)
            .await
            .expect_err("in use");
        assert!(
            matches!(err, extenddb_storage::error::StorageError::BackupInUse(_)),
            "{err:?}"
        );

        sqlx::query("UPDATE tables SET table_status = 'ACTIVE' WHERE table_id = ?")
            .bind(&desc.table_id)
            .execute(&engine.pool)
            .await
            .expect("finished");
        engine
            .delete_backup(ACCOUNT, &backup.backup_arn)
            .await
            .expect("deletable once the restore is done");
        let after = describe("dst").await.restore_summary.expect("summary");
        assert_eq!(
            after.source_backup_arn.as_deref(),
            Some(backup.backup_arn.as_str())
        );
    }

    async fn file_engine() -> (SqliteEngine, std::path::PathBuf) {
        file_engine_with_pool_size(4).await
    }

    async fn file_engine_with_pool_size(pool_size: u32) -> (SqliteEngine, std::path::PathBuf) {
        let target = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target")
            });
        assert!(target.is_dir(), "cargo target directory must exist");
        let path = target.join(format!("backup-test-{}.sqlite", uuid::Uuid::new_v4()));
        let engine = SqliteEngine::new(
            path.to_str().expect("UTF-8 path"),
            pool_size,
            "us-east-1",
            409_600,
        )
        .await
        .expect("file engine");
        crate::migrations::apply_for_tests(&engine.pool).await;
        sqlx::query("INSERT INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0') ON CONFLICT(key) DO UPDATE SET value = excluded.value")
            .execute(&engine.pool)
            .await
            .expect("delay");
        sqlx::query("INSERT INTO settings (key, value) VALUES ('index_propagation_delay_ms', '0') ON CONFLICT(key) DO UPDATE SET value = excluded.value")
            .execute(&engine.pool)
            .await
            .expect("propagation");
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
            .bind(ACCOUNT)
            .execute(&engine.pool)
            .await
            .expect("account");
        (engine, path)
    }

    async fn close_file_engine(engine: SqliteEngine, path: &std::path::Path) {
        engine.pool.close().await;
        drop(engine);
        for suffix in ["", "-wal", "-shm", ".lock"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    async fn plain_table(engine: &SqliteEngine, name: &str) -> TableKeyInfo {
        let table_id = create(
            engine,
            json!({
                "TableName": name,
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        )
        .await;
        let key_schema: Vec<extenddb_core::types::KeySchemaElement> =
            serde_json::from_value(json!([
                {"AttributeName": "pk", "KeyType": "HASH"}
            ]))
            .expect("key schema");
        TableKeyInfo {
            table_name: name.to_owned(),
            account_id: ACCOUNT.to_owned(),
            table_id,
            key_schema: key_schema.clone(),
            base_key_schema: key_schema,
            attribute_definitions: serde_json::from_value(json!([
                {"AttributeName": "pk", "AttributeType": "S"}
            ]))
            .expect("attribute definitions"),
            ..Default::default()
        }
    }

    async fn fill_table(engine: &SqliteEngine, key_info: &TableKeyInfo, count: i64) {
        let table = crate::data::data_table_name(&key_info.table_id);
        let sql = format!(
            "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i + 1 < ?) \
             INSERT INTO {table} (pk, item_data) \
             SELECT 'p' || i, json_object('pk', json_object('S', 'p' || i), \
                    'payload', json_object('S', ?)) FROM n"
        );
        let payload = "x".repeat(1024);
        let _writer = engine.write_lock.lock().await;
        let mut tx = engine
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .expect("write transaction");
        sqlx::query(&sql)
            .bind(count)
            .bind(payload)
            .execute(&mut *tx)
            .await
            .expect("fill table");
        tx.commit().await.expect("commit fill");
    }

    #[tokio::test]
    async fn backup_batches_do_not_stall_an_unrelated_writer() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, Mutex};
        use std::time::Instant;

        let (engine, path) = file_engine_with_pool_size(1).await;
        assert!(!engine.in_memory);
        assert_eq!(
            engine.pool.options().get_max_connections(),
            2,
            "file backups require concurrent WAL reader and batch writer connections"
        );
        let source = plain_table(&engine, "source").await;
        fill_table(&engine, &source, 5_000).await;
        let writer_key = plain_table(&engine, "writer").await;

        let stop = Arc::new(AtomicBool::new(false));
        let completions = Arc::new(Mutex::new(Vec::new()));
        let task_engine = engine.clone();
        let task_stop = Arc::clone(&stop);
        let task_completions = Arc::clone(&completions);
        let writer = tokio::spawn(async move {
            let mut i = 0_u64;
            while !task_stop.load(Ordering::Relaxed) {
                let item: Item = serde_json::from_value(json!({
                    "pk": {"S": format!("w{i}")}
                }))
                .expect("writer item");
                task_engine
                    .put_item(
                        &writer_key,
                        item,
                        false,
                        None,
                        &extenddb_core::expression::ExpressionMaps::default(),
                        None,
                    )
                    .await
                    .expect("writer put");
                task_completions
                    .lock()
                    .expect("completion lock")
                    .push(Instant::now());
                i += 1;
            }
        });
        while completions.lock().expect("completion lock").len() < 20 {
            tokio::task::yield_now().await;
        }

        let started = Instant::now();
        engine
            .create_backup(ACCOUNT, "source", "concurrent")
            .await
            .expect("backup");
        let finished = Instant::now();
        while completions
            .lock()
            .expect("completion lock")
            .last()
            .is_none_or(|t| *t <= finished)
        {
            tokio::task::yield_now().await;
        }
        stop.store(true, Ordering::Relaxed);
        writer.await.expect("writer task");

        {
            let times = completions.lock().expect("completion lock");
            assert!(
                times.iter().any(|t| *t > started && *t < finished),
                "the unrelated writer must make progress while the backup runs"
            );
            let backup_duration = finished.duration_since(started);
            let max_gap = times
                .windows(2)
                .filter(|pair| pair[0] <= finished && pair[1] >= started)
                .map(|pair| pair[1].duration_since(pair[0]))
                .max()
                .expect("writer gaps across backup");
            assert!(
                max_gap < backup_duration.mul_f64(0.75),
                "writer gap {max_gap:?} must be well below backup duration {backup_duration:?}"
            );
        }
        close_file_engine(engine, &path).await;
    }

    #[tokio::test]
    async fn backup_excludes_items_written_after_its_snapshot() {
        let (engine, path) = file_engine().await;
        let source = plain_table(&engine, "source").await;
        fill_table(&engine, &source, 5_000).await;

        let backup_engine = engine.clone();
        let backup = tokio::spawn(async move {
            backup_engine
                .create_backup(ACCOUNT, "source", "snapshot")
                .await
        });
        loop {
            let status: Option<String> = sqlx::query_scalar(
                "SELECT backup_status FROM backups WHERE backup_name = 'snapshot'",
            )
            .fetch_optional(&engine.pool)
            .await
            .expect("backup status");
            match status.as_deref() {
                Some("CREATING") => break,
                Some("AVAILABLE") => panic!("backup completed before the concurrent write"),
                None => tokio::task::yield_now().await,
                Some(other) => panic!("unexpected backup status {other}"),
            }
        }

        let late_item: Item = serde_json::from_value(json!({
            "pk": {"S": "late"}, "payload": {"S": "after snapshot"}
        }))
        .expect("late item");
        engine
            .put_item(
                &source,
                late_item,
                false,
                None,
                &extenddb_core::expression::ExpressionMaps::default(),
                None,
            )
            .await
            .expect("late put");
        assert!(
            !backup.is_finished(),
            "the source write must complete while backup copying continues"
        );
        let details = backup.await.expect("backup task").expect("backup");

        let backed_up: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM backup_items WHERE backup_arn = ?")
                .bind(&details.backup_arn)
                .fetch_one(&engine.pool)
                .await
                .expect("backup count");
        assert_eq!(backed_up, 5_000);
        let late_backed_up: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM backup_items \
             WHERE backup_arn = ? AND item_data LIKE '%after snapshot%')",
        )
        .bind(&details.backup_arn)
        .fetch_one(&engine.pool)
        .await
        .expect("late item lookup");
        assert!(!late_backed_up);
        let empty_keys: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM backup_items WHERE backup_arn = ? AND pk = ''",
        )
        .bind(&details.backup_arn)
        .fetch_one(&engine.pool)
        .await
        .expect("backup keys");
        assert_eq!(empty_keys, 0, "backup rows must retain their primary keys");
        close_file_engine(engine, &path).await;
    }

    #[tokio::test]
    async fn cancelled_create_backup_finishes_with_all_items() {
        let (engine, path) = file_engine_with_pool_size(2).await;
        let source = plain_table(&engine, "source").await;
        const SOURCE_ITEMS: i64 = 10_000;
        fill_table(&engine, &source, SOURCE_ITEMS).await;

        let request_engine = engine.clone();
        let request = tokio::spawn(async move {
            request_engine
                .create_backup(ACCOUNT, "source", "cancelled-request")
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let status: Option<String> = sqlx::query_scalar(
                    "SELECT backup_status FROM backups WHERE backup_name = 'cancelled-request'",
                )
                .fetch_optional(&engine.pool)
                .await
                .expect("backup status");
                match status.as_deref() {
                    Some("CREATING") => break,
                    Some(other) => panic!("unexpected backup status before cancellation: {other}"),
                    None => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .expect("backup must enter CREATING");

        // This drops the request future that is awaiting CreateBackup's inner
        // JoinHandle. The detached copy task must remain alive.
        request.abort();
        assert!(
            request
                .await
                .expect_err("request was cancelled")
                .is_cancelled()
        );

        let (backup_arn, item_count): (String, i64) =
            tokio::time::timeout(std::time::Duration::from_secs(30), async {
                loop {
                    let row: Option<(String, String, i64)> = sqlx::query_as(
                        "SELECT backup_arn, backup_status, item_count FROM backups \
                         WHERE backup_name = 'cancelled-request'",
                    )
                    .fetch_optional(&engine.pool)
                    .await
                    .expect("backup row");
                    match row {
                        Some((arn, status, count)) if status == "AVAILABLE" => break (arn, count),
                        Some((_, status, _)) if status == "CREATING" => {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        }
                        Some((_, status, _)) => panic!("unexpected final backup status: {status}"),
                        None => panic!("detached backup disappeared"),
                    }
                }
            })
            .await
            .expect("detached backup must leave CREATING");
        assert_eq!(item_count, SOURCE_ITEMS);
        let copied: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM backup_items WHERE backup_arn = ?")
                .bind(&backup_arn)
                .fetch_one(&engine.pool)
                .await
                .expect("copied item count");
        assert_eq!(copied, SOURCE_ITEMS);

        close_file_engine(engine, &path).await;
    }

    /// A table with a GSI and an LSI, filled straight through SQL with
    /// `count` items of about 1 KiB, each carrying both index keys. The
    /// source's own index tables stay empty; a backup reads only `item_data`.
    async fn bulk_indexed_table(engine: &SqliteEngine, name: &str, count: i64) -> String {
        let table_id = create(
            engine,
            json!({
                "TableName": name,
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"}
                ],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                    {"AttributeName": "g", "AttributeType": "S"},
                    {"AttributeName": "l", "AttributeType": "S"}
                ],
                "BillingMode": "PAY_PER_REQUEST",
                "GlobalSecondaryIndexes": [{
                    "IndexName": "gi",
                    "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                }],
                "LocalSecondaryIndexes": [{
                    "IndexName": "li",
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "l", "KeyType": "RANGE"}
                    ],
                    "Projection": {"ProjectionType": "KEYS_ONLY"}
                }]
            }),
        )
        .await;
        let table = crate::data::data_table_name(&table_id);
        let sql = format!(
            "WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i + 1 < ?) \
             INSERT INTO {table} (pk, sk_s, item_data) \
             SELECT 'p' || (i % 50), 's' || i, json_object(\
                 'pk', json_object('S', 'p' || (i % 50)), \
                 'sk', json_object('S', 's' || i), \
                 'g', json_object('S', 'g' || (i % 7)), \
                 'l', json_object('S', 'l' || i), \
                 'payload', json_object('S', ?)) FROM n"
        );
        let _writer = engine.write_lock.lock().await;
        sqlx::query(&sql)
            .bind(count)
            .bind("x".repeat(1024))
            .execute(&engine.pool)
            .await
            .expect("fill table");
        table_id
    }

    /// Rows in the target's base table and in each of its index tables.
    async fn target_counts(engine: &SqliteEngine, table_id: &str) -> (i64, Vec<(String, i64)>) {
        let base = count(engine, &crate::data::data_table_name(table_id)).await;
        let mut indexes = Vec::new();
        for (name, id) in index_tables(engine, table_id).await {
            indexes.push((
                name,
                count(engine, &crate::data::index_table_name(&id)).await,
            ));
        }
        (base, indexes)
    }

    async fn status_of(engine: &SqliteEngine, table_id: &str) -> Option<String> {
        sqlx::query_scalar("SELECT table_status FROM tables WHERE table_id = ?")
            .bind(table_id)
            .fetch_optional(&engine.pool)
            .await
            .expect("status")
    }

    /// Items in the bulk tests: more than four copy batches (500 items each).
    const BULK_ITEMS: i64 = 2_400;

    /// RestoreTableFromBackup returns once the target is registered, while
    /// the copy is still running, and the detached copy then fills the base
    /// table, the GSI, and the LSI across several batches and flips the
    /// target ACTIVE.
    #[tokio::test]
    async fn restore_returns_creating_and_the_detached_copy_finishes() {
        let (engine, path) = file_engine().await;
        bulk_indexed_table(&engine, "src", BULK_ITEMS).await;
        let backup = engine
            .create_backup(ACCOUNT, "src", "bkp")
            .await
            .expect("backup");
        assert!(
            BULK_ITEMS > i64::try_from(super::COPY_BATCH_ITEMS).expect("batch") * 2,
            "the copy must span at least two batches"
        );

        let desc = start_restore(&engine, "dst", &backup.backup_arn)
            .await
            .expect("restore");
        // The targeted assertion: at return the copy has not finished. A
        // restore that copied inside the request returns an ACTIVE, full table.
        assert_eq!(
            status_of(&engine, &desc.table_id).await.as_deref(),
            Some("CREATING"),
            "the restore must return before its copy completes"
        );
        let (at_return, _) = target_counts(&engine, &desc.table_id).await;
        assert!(at_return < BULK_ITEMS, "copied {at_return} before return");
        assert_eq!(
            desc.table_status,
            extenddb_core::types::TableStatus::Creating
        );
        let summary = desc.restore_summary.clone().expect("summary");
        assert!(summary.restore_in_progress);

        // Liveness: the copy moves and completes.
        assert!(
            wait_for_copy(&engine, &desc.table_id).await,
            "target removed"
        );
        let (base, indexes) = target_counts(&engine, &desc.table_id).await;
        assert_eq!(base, BULK_ITEMS);
        assert_eq!(
            indexes,
            vec![("gi".to_owned(), BULK_ITEMS), ("li".to_owned(), BULK_ITEMS)]
        );
        let item_count: i64 =
            sqlx::query_scalar("SELECT item_count FROM tables WHERE table_id = ?")
                .bind(&desc.table_id)
                .fetch_one(&engine.pool)
                .await
                .expect("item count");
        assert_eq!(item_count, BULK_ITEMS);
        close_file_engine(engine, &path).await;
    }

    /// The request future is dropped after its first poll, before it can
    /// return (a client that disconnects). The restore still registers,
    /// copies, and reaches ACTIVE with every item.
    #[tokio::test]
    async fn a_dropped_restore_request_still_reaches_active() {
        use futures::FutureExt;
        let (engine, path) = file_engine().await;
        bulk_indexed_table(&engine, "src", BULK_ITEMS).await;
        let backup = engine
            .create_backup(ACCOUNT, "src", "bkp")
            .await
            .expect("backup");
        let request = restore_request(&engine, "dst", &backup.backup_arn)
            .await
            .expect("request");

        let polled_once = engine
            .restore_table_from_backup(ACCOUNT, request)
            .now_or_never();
        assert!(
            polled_once.is_none(),
            "the restore cannot complete in one poll; this test needs it pending"
        );
        // The future is gone. The restore it started must appear and finish.
        let table_id: String = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let id: Option<String> =
                    sqlx::query_scalar("SELECT table_id FROM tables WHERE table_name = 'dst'")
                        .fetch_optional(&engine.pool)
                        .await
                        .expect("target");
                match id {
                    Some(id) => break id,
                    None => tokio::time::sleep(Duration::from_millis(5)).await,
                }
            }
        })
        .await
        .expect("the dropped request's restore must still create the target");
        assert!(
            wait_for_copy(&engine, &table_id).await,
            "the dropped request's restore must reach ACTIVE"
        );
        let (base, indexes) = target_counts(&engine, &table_id).await;
        assert_eq!(base, BULK_ITEMS);
        assert_eq!(
            indexes,
            vec![("gi".to_owned(), BULK_ITEMS), ("li".to_owned(), BULK_ITEMS)]
        );
        close_file_engine(engine, &path).await;
    }

    /// While the detached copy runs, DeleteBackup of the source is
    /// BackupInUse and DeleteTable of the target is refused; once the target
    /// is ACTIVE both succeed.
    #[tokio::test]
    async fn backup_and_target_are_protected_while_the_copy_runs() {
        use extenddb_storage::TableEngine;
        let (engine, path) = file_engine().await;
        bulk_indexed_table(&engine, "src", BULK_ITEMS).await;
        let backup = engine
            .create_backup(ACCOUNT, "src", "bkp")
            .await
            .expect("backup");
        let desc = start_restore(&engine, "dst", &backup.backup_arn)
            .await
            .expect("restore");

        let err = engine
            .delete_backup(ACCOUNT, &backup.backup_arn)
            .await
            .expect_err("the source backup is in use while the copy runs");
        assert!(matches!(err, StorageError::BackupInUse(_)), "{err:?}");
        let delete_target = || {
            engine.delete_table(
                ACCOUNT,
                extenddb_core::types::DeleteTableInput {
                    table_name: "dst".to_owned(),
                },
            )
        };
        let err = delete_target()
            .await
            .expect_err("the target cannot be deleted while it is restored");
        assert!(
            matches!(&err, StorageError::IndexesInUse(m) if m.contains("being restored")),
            "{err:?}"
        );
        // Both refusals were observed during the copy, not after it.
        assert_eq!(
            status_of(&engine, &desc.table_id).await.as_deref(),
            Some("CREATING")
        );

        assert!(
            wait_for_copy(&engine, &desc.table_id).await,
            "target removed"
        );
        assert_eq!(
            count(&engine, &crate::data::data_table_name(&desc.table_id)).await,
            BULK_ITEMS
        );
        engine
            .delete_backup(ACCOUNT, &backup.backup_arn)
            .await
            .expect("deletable once the restore is done");
        delete_target()
            .await
            .expect("the target is deletable once ACTIVE");
        close_file_engine(engine, &path).await;
    }

    /// A target registered but never copied (a process that exited between
    /// registration and copy) is removed by the startup sweep, its restore
    /// registration with it, which frees the source backup.
    #[tokio::test]
    async fn startup_sweep_removes_a_registered_target_that_was_never_copied() {
        let engine = engine().await;
        indexed_table(&engine, "src").await;
        let backup = engine
            .create_backup(ACCOUNT, "src", "bkp")
            .await
            .expect("backup");
        let request = restore_request(&engine, "dst", &backup.backup_arn)
            .await
            .expect("request");
        let desc = engine
            .create_table_for_restore(
                ACCOUNT,
                request.input,
                &RestoreRegistration {
                    backup_arn: &request.backup_arn,
                    source_table_arn: &request.source_table_arn,
                    restore_date_time: request.restore_date_time,
                },
            )
            .await
            .expect("register without a copy");
        let err = engine
            .delete_backup(ACCOUNT, &backup.backup_arn)
            .await
            .expect_err("registered restore holds the backup");
        assert!(matches!(err, StorageError::BackupInUse(_)), "{err:?}");

        let removed = engine.sweep_abandoned_restores().await.expect("sweep");
        assert_eq!(removed, vec!["dst".to_owned()]);
        assert_eq!(status_of(&engine, &desc.table_id).await, None);
        let registrations: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM table_restores WHERE table_id = ?")
                .bind(&desc.table_id)
                .fetch_one(&engine.pool)
                .await
                .expect("registrations");
        assert_eq!(registrations, 0);
        engine
            .delete_backup(ACCOUNT, &backup.backup_arn)
            .await
            .expect("the backup is free after the sweep");
    }

    /// table_restores records the source table ARN and the restore time the
    /// engine passed, and DescribeTable reports both unchanged.
    #[tokio::test]
    async fn the_registration_records_the_source_table_arn_and_restore_time() {
        use extenddb_storage::TableEngine;
        let engine = engine().await;
        indexed_table(&engine, "src").await;
        let backup = engine
            .create_backup(ACCOUNT, "src", "bkp")
            .await
            .expect("backup");
        let mut request = restore_request(&engine, "dst", &backup.backup_arn)
            .await
            .expect("request");
        // Distinct from both the backup's time and now, so only the passed
        // value can produce it.
        request.restore_date_time = 1_791_976_431.438;
        request.source_table_arn = "arn:aws:dynamodb:us-east-1:000000000000:table/elsewhere".into();
        let desc = engine
            .restore_table_from_backup(ACCOUNT, request)
            .await
            .expect("restore");
        assert!(wait_for_copy(&engine, &desc.table_id).await);

        let (arn, source_table_arn, at): (String, Option<String>, String) = sqlx::query_as(
            "SELECT source_backup_arn, source_table_arn, restore_date_time \
             FROM table_restores WHERE table_id = ?",
        )
        .bind(&desc.table_id)
        .fetch_one(&engine.pool)
        .await
        .expect("registration");
        assert_eq!(arn, backup.backup_arn);
        assert_eq!(
            source_table_arn.as_deref(),
            Some("arn:aws:dynamodb:us-east-1:000000000000:table/elsewhere")
        );
        assert_eq!(at, "2026-10-14T11:13:51.438000Z");

        let summary = engine
            .describe_table(
                ACCOUNT,
                extenddb_core::types::DescribeTableInput {
                    table_name: "dst".to_owned(),
                },
            )
            .await
            .expect("describe")
            .restore_summary
            .expect("summary");
        assert_eq!(summary.restore_date_time, 1_791_976_431.438);
        assert_eq!(
            summary.source_table_arn.as_deref(),
            Some("arn:aws:dynamodb:us-east-1:000000000000:table/elsewhere")
        );
    }

    /// Backup ids come from the shared helper (14-digit zero-padded epoch
    /// milliseconds, 8 hex), and the creation time CreateBackup,
    /// DescribeBackup, ListBackups, and backup_restore_source report is the
    /// id's own instant, to the millisecond. ListBackups carries the table id.
    #[tokio::test]
    async fn backup_ids_and_times_come_from_the_shared_helper() {
        let engine = engine().await;
        let src = indexed_table(&engine, "src").await;
        // A backup whose millisecond part is non-zero shows the time is not
        // truncated to the second; 999 in 1000 are, so a few tries suffice.
        let mut fractional = None;
        for i in 0..20 {
            let backup = engine
                .create_backup(ACCOUNT, "src", &format!("bkp{i}"))
                .await
                .expect("backup");
            let id = backup.backup_arn.rsplit('/').next().expect("id").to_owned();
            assert_eq!(id.len(), 23, "{id}");
            assert!(id.starts_with('0'), "zero-padded: {id}");
            let from_id = extenddb_storage::util::backup_creation_date_time(&backup.backup_arn)
                .expect("id in the shared shape");
            assert_eq!(backup.backup_creation_date_time, from_id);
            assert_eq!(
                backup.backup_arn,
                extenddb_storage::util::backup_arn("us-east-1", ACCOUNT, "src", &id)
            );
            let described = engine
                .describe_backup(ACCOUNT, &backup.backup_arn)
                .await
                .expect("describe");
            assert_eq!(described.backup_details.backup_creation_date_time, from_id);
            let source = engine
                .backup_restore_source(ACCOUNT, &backup.backup_arn)
                .await
                .expect("source");
            assert_eq!(source.backup_creation_date_time, from_id);
            if from_id.fract() != 0.0 {
                fractional = Some(backup.backup_arn);
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let fractional = fractional.expect("some backup with a non-zero millisecond");
        let listed = engine
            .list_backups(ACCOUNT, Some("src"))
            .await
            .expect("list");
        let summary = listed
            .iter()
            .find(|b| b.backup_arn == fractional)
            .expect("listed");
        assert_eq!(
            summary.backup_creation_date_time,
            extenddb_storage::util::backup_creation_date_time(&fractional).expect("time")
        );
        assert!(summary.backup_creation_date_time.fract() != 0.0);
        assert!(listed.iter().all(|b| b.table_id == src), "{listed:?}");
    }

    /// backup_restore_source reports the source table and the captured
    /// definition, including the TTL and stream settings DescribeBackup shows.
    #[tokio::test]
    async fn the_restore_source_carries_the_definition_ttl_and_stream() {
        use extenddb_storage::{MetadataEngine, TableEngine};
        let engine = engine().await;
        let src = indexed_table(&engine, "src").await;
        engine
            .update_ttl(ACCOUNT, "src", "expires", true)
            .await
            .expect("ttl");
        sqlx::query(
            "UPDATE tables SET stream_specification = \
             '{\"StreamEnabled\":true,\"StreamViewType\":\"NEW_IMAGE\"}' WHERE table_id = ?",
        )
        .bind(&src)
        .execute(&engine.pool)
        .await
        .expect("stream");
        let backup = engine
            .create_backup(ACCOUNT, "src", "bkp")
            .await
            .expect("backup");
        let source = engine
            .backup_restore_source(ACCOUNT, &backup.backup_arn)
            .await
            .expect("source");
        let table = engine
            .describe_table(
                ACCOUNT,
                extenddb_core::types::DescribeTableInput {
                    table_name: "src".to_owned(),
                },
            )
            .await
            .expect("describe");
        assert_eq!(source.source_table_name, "src");
        assert_eq!(source.source_table_id, src);
        assert_eq!(source.source_table_arn, table.table_arn);
        assert_eq!(source.item_count, 30);
        assert_eq!(source.billing_mode, "PROVISIONED");
        let pt = source.provisioned_throughput.clone().expect("throughput");
        assert_eq!((pt.read_capacity_units, pt.write_capacity_units), (7, 9));
        let definition = source.definition.clone().expect("definition");
        assert_eq!(definition.global_secondary_indexes.len(), 1);
        assert_eq!(definition.local_secondary_indexes.len(), 1);
        let ttl = definition.time_to_live.expect("ttl");
        assert_eq!(
            ttl.time_to_live_status,
            extenddb_core::types::TimeToLiveStatus::Enabled
        );
        assert_eq!(ttl.attribute_name.as_deref(), Some("expires"));
        let stream = definition.stream_specification.expect("stream");
        assert!(stream.stream_enabled);
        assert_eq!(source.legacy_vector_index_count, 0);

        // Not AVAILABLE, or another account's: not found.
        for (account, arn) in [
            ("111111111111", backup.backup_arn.as_str()),
            (
                ACCOUNT,
                "arn:aws:dynamodb:us-east-1:000000000000:table/src/backup/x",
            ),
        ] {
            let err = engine
                .backup_restore_source(account, arn)
                .await
                .expect_err("not found");
            assert!(
                matches!(&err, StorageError::Validation(m) if m.starts_with("Backup not found")),
                "{err:?}"
            );
        }
    }

    /// A configured pool_size the file backend raises is logged once, with
    /// the reason; one it keeps is not.
    #[tokio::test]
    async fn a_raised_pool_size_is_logged() {
        let (guard, logs) = crate::schema::log_capture::capture();
        let (engine, path) = file_engine_with_pool_size(1).await;
        close_file_engine(engine, &path).await;
        let (engine, path) = file_engine_with_pool_size(3).await;
        close_file_engine(engine, &path).await;
        drop(guard);
        let logs = logs.lock().expect("logs").clone();
        let raised: Vec<&String> = logs
            .iter()
            .filter(|l| l.contains("pool_size") && l.contains("raised"))
            .collect();
        assert_eq!(raised.len(), 1, "{logs:#?}");
        assert!(raised[0].starts_with("INFO"), "{raised:?}");
        assert!(raised[0].contains("pool_size 1 raised to 2"), "{raised:?}");
        assert!(raised[0].contains("pool_size >= 3"), "{raised:?}");
    }

    /// A flood of CreateBackups from one account on the default pool of 10
    /// does not starve another account's reads: at most
    /// `backup_server_slot_count` backups run at once (observed as CREATING
    /// rows), every read of the other account succeeds and none waits for
    /// the pool's acquire timeout, and every backup either completes or is
    /// refused with `LimitExceeded`. Before the bound, 16 concurrent backups
    /// each held a reader connection and the other account's reads answered
    /// an internal error after the 30 s acquire timeout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_backup_flood_does_not_starve_another_accounts_reads() {
        use extenddb_storage::TableEngine;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, Mutex};
        use std::time::Instant;

        const FLOOD: usize = 16;
        let (engine, path) = file_engine_with_pool_size(10).await;
        assert_eq!(engine.backup_server_slot_count, 4);
        let source = plain_table(&engine, "source").await;
        fill_table(&engine, &source, 3_000).await;
        const OTHER: &str = "111111111111";
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'other')")
            .bind(OTHER)
            .execute(&engine.pool)
            .await
            .expect("other account");
        let other_input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
            "TableName": "theirs",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("input");
        engine
            .create_table_impl(OTHER, other_input, false)
            .await
            .expect("other table");

        // The other account reads continuously; each read's latency and
        // outcome is recorded.
        let stop = Arc::new(AtomicBool::new(false));
        let reads: Arc<Mutex<Vec<Result<Duration, String>>>> = Arc::default();
        let reader = {
            let engine = engine.clone();
            let stop = Arc::clone(&stop);
            let reads = Arc::clone(&reads);
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let started = Instant::now();
                    let result = engine
                        .describe_table(
                            OTHER,
                            extenddb_core::types::DescribeTableInput {
                                table_name: "theirs".to_owned(),
                            },
                        )
                        .await
                        .map(|_| started.elapsed())
                        .map_err(|e| e.to_string());
                    reads.lock().expect("reads").push(result);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
        };
        // Sample how many backups are CREATING at once.
        let peak = Arc::new(Mutex::new(0_i64));
        let sampler = {
            let engine = engine.clone();
            let stop = Arc::clone(&stop);
            let peak = Arc::clone(&peak);
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    let creating: i64 = sqlx::query_scalar(
                        "SELECT COUNT(*) FROM backups WHERE backup_status = 'CREATING'",
                    )
                    .fetch_one(&engine.pool)
                    .await
                    .unwrap_or(0);
                    {
                        let mut peak = peak.lock().expect("peak");
                        *peak = (*peak).max(creating);
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
        };

        let started = Instant::now();
        let outcomes = futures::future::join_all((0..FLOOD).map(|i| {
            let engine = engine.clone();
            async move {
                engine
                    .create_backup(ACCOUNT, "source", &format!("flood{i}"))
                    .await
            }
        }))
        .await;
        let flood_took = started.elapsed();
        stop.store(true, Ordering::Relaxed);
        reader.await.expect("reader");
        sampler.await.expect("sampler");

        let mut completed = 0;
        for (i, outcome) in outcomes.iter().enumerate() {
            match outcome {
                Ok(_) => completed += 1,
                Err(StorageError::LimitExceeded(m)) => {
                    assert!(m.contains("on this server (at most 4"), "{m}");
                }
                Err(other) => panic!("backup {i}: {other}"),
            }
        }
        assert!(completed > 0, "some backups complete: {outcomes:?}");
        let peak = *peak.lock().expect("peak");
        assert!(
            (1..=4).contains(&peak),
            "at most backup_server_slot_count backups run at once, observed {peak}"
        );
        let reads = reads.lock().expect("reads").clone();
        assert!(reads.len() > 1, "the reader moved: {} reads", reads.len());
        let errors: Vec<&String> = reads.iter().filter_map(|r| r.as_ref().err()).collect();
        assert!(
            errors.is_empty(),
            "the other account's reads failed: {errors:?}"
        );
        let slowest = reads
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .max()
            .copied()
            .expect("a read");
        assert!(
            slowest < Duration::from_secs(5),
            "the other account's slowest read took {slowest:?} during a flood of {FLOOD} backups \
             ({completed} completed in {flood_took:?})"
        );
        close_file_engine(engine, &path).await;
    }

    /// While a target restored from a backup is CREATING, a second
    /// restore of that backup is `BackupInUse`, and a restore into that
    /// target's name is `TableBeingRestored` naming the target and its source
    /// backup; once the target is ACTIVE a restore of the backup is accepted
    /// and a restore into the name is `TableAlreadyExists`.
    #[tokio::test]
    async fn a_restore_is_refused_while_a_target_of_its_backup_is_creating() {
        let engine = engine().await;
        indexed_table(&engine, "src").await;
        let b1 = engine
            .create_backup(ACCOUNT, "src", "b1")
            .await
            .expect("backup 1");
        let b2 = engine
            .create_backup(ACCOUNT, "src", "b2")
            .await
            .expect("backup 2");
        let desc = restore(&engine, "dst", &b1.backup_arn)
            .await
            .expect("first restore");
        // Put the target back into the CREATING window its copy has.
        sqlx::query(
            "UPDATE tables SET table_status = 'CREATING', status_transition_at = NULL \
             WHERE table_id = ?",
        )
        .bind(&desc.table_id)
        .execute(&engine.pool)
        .await
        .expect("in progress");

        let second = start_restore(&engine, "dst2", &b1.backup_arn).await;
        assert!(
            matches!(&second, Err(StorageError::BackupInUse(m)) if m.contains("dst")),
            "a second restore of a backup with a CREATING target: {second:?}"
        );
        let none: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM tables WHERE account_id = ? AND table_name = 'dst2'",
        )
        .bind(ACCOUNT)
        .fetch_one(&engine.pool)
        .await
        .expect("count");
        assert_eq!(none, 0, "the refused restore created nothing");
        let into_target = start_restore(&engine, "dst", &b2.backup_arn).await;
        match into_target {
            Err(StorageError::TableBeingRestored {
                table_name,
                backup_arn,
            }) => {
                assert_eq!(table_name, "dst");
                assert_eq!(backup_arn, b1.backup_arn);
            }
            other => panic!("a restore into a CREATING restore target: {other:?}"),
        }
        // The same backup into the same name: the name is what is refused,
        // not the backup (the service answers TableInUseException).
        let same = start_restore(&engine, "dst", &b1.backup_arn).await;
        match same {
            Err(StorageError::TableBeingRestored {
                table_name,
                backup_arn,
            }) => {
                assert_eq!(table_name, "dst");
                assert_eq!(backup_arn, b1.backup_arn);
            }
            other => panic!("the same backup into its CREATING target: {other:?}"),
        }
        // The name is checked before the backup: a backup that does not
        // exist, restored into that name, is refused as the name too.
        let mut missing = restore_request(&engine, "dst", &b2.backup_arn)
            .await
            .expect("request");
        missing.backup_arn = flip_last_hex(&b2.backup_arn);
        match engine.restore_table_from_backup(ACCOUNT, missing).await {
            Err(StorageError::TableBeingRestored {
                table_name,
                backup_arn,
            }) => {
                assert_eq!(table_name, "dst");
                assert_eq!(backup_arn, b1.backup_arn);
            }
            other => panic!("a missing backup into a CREATING restore target: {other:?}"),
        }

        sqlx::query("UPDATE tables SET table_status = 'ACTIVE' WHERE table_id = ?")
            .bind(&desc.table_id)
            .execute(&engine.pool)
            .await
            .expect("finished");
        restore(&engine, "dst2", &b1.backup_arn)
            .await
            .expect("a restore once the first target is ACTIVE");
        let taken = start_restore(&engine, "dst", &b2.backup_arn).await;
        assert!(
            matches!(&taken, Err(StorageError::TableAlreadyExists(n)) if n == "dst"),
            "{taken:?}"
        );
    }

    /// The bound's arithmetic and its refusal, without a database.
    #[tokio::test]
    async fn backup_slots_follow_the_pool_and_refuse_after_the_wait() {
        assert_eq!(backup_server_slot_count(2), 1);
        assert_eq!(backup_server_slot_count(3), 1);
        assert_eq!(backup_server_slot_count(4), 1);
        assert_eq!(backup_server_slot_count(10), 4);
        assert_eq!(backup_server_slot_count(20), 9);
        assert_eq!(backup_server_slot_count(34), 16);
        assert_eq!(backup_server_slot_count(1_000), 16);
        let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let held = acquire_backup_permit(slots.clone(), 1, Duration::from_millis(50))
            .await
            .expect("first");
        match acquire_backup_permit(slots.clone(), 1, Duration::from_millis(50)).await {
            Err(StorageError::LimitExceeded(m)) => {
                assert!(
                    m.contains("backups are in progress on this server (at most 1"),
                    "{m}"
                );
            }
            other => panic!("a full bound refuses: {:?}", other.err()),
        }
        drop(held);
        let _freed = acquire_backup_permit(slots, 1, Duration::from_millis(50))
            .await
            .expect("freed");
    }
}
