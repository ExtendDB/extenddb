// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The catalog index on SQLite: `backup_index`, the restore job fields of
//! `table_restores`, `backup_maintenance` and `settings.catalog_id`.
//!
//! One process serves a database file (the serve lock enforces it), so a
//! job's claim is an in-process slot ([`JobSlots`]): a key held in a set for
//! the job's life and released by a drop guard, so a task that ends by any
//! route, a panic included, frees it. A restart frees every slot at once,
//! which is the proof of death this backend needs: a CREATING in-catalog
//! backup row whose slot no task holds is dead, and every RUNNING restore
//! job record of the stopped process is aged at startup so the reconciler's
//! first cycle finds it ([`SqliteEngine::expire_restore_job_heartbeats`]).
//!
//! Every write a job makes to its row goes through its claim and carries the
//! claim's `owner_generation`, so a row a successor took over matches no
//! write. The lock on a row for DeleteBackup and a restore's registration is
//! the engine write lock with a `BEGIN IMMEDIATE` transaction, as for every
//! other writer (design decision D1). Times are RFC 3339 text on the
//! database's clock (`strftime('now')`), and staleness is measured there.
//! A conditional write never has an unknown outcome here (one process, one
//! file: a statement either ran or errored), so `WriteOutcome::Unknown` is
//! never returned.
//!
//! The maintenance claim is the `backup_maintenance` row, taken under the
//! engine write lock: refused while a row with a heartbeat younger than the
//! stale window exists, taken over (replaced) from a dead holder, heartbeated
//! and released by generation. One process serves a file, so a holder is a
//! command run while the server is stopped.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use extenddb_core::backup_format::BackupArn;
use extenddb_storage::JobPermit;
use extenddb_storage::backup_catalog::{
    BackupCatalog, BackupKey, BackupRow, BackupStatus, DeleteStart, Heartbeat, JobClaim, JobRow,
    JobStatus, MaintenanceClaim, RestoreJobRow, RestoreKind, RestoreTargetKey, StatusFields,
    StoreIdentity, StoreKind, TimeBounds, WriteOutcome, new_generation,
};
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::sqlite_util::{format_epoch_seconds, timestamp_epoch_seconds};
use crate::store::SqliteEngine;

/// The database's clock as a catalog timestamp, millisecond precision.
pub(crate) const NOW: &str = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";

/// The in-process job slots of one engine: the key of every row a job of
/// this process owns. A key is held from before the row exists until the job
/// ends, so a row whose key is absent has no live owner.
#[derive(Debug, Default)]
pub(crate) struct JobSlots(Mutex<HashSet<String>>);

impl JobSlots {
    /// Take `key`, or `None` when a job of this process holds it.
    pub(crate) fn take(slots: &Arc<Self>, key: String) -> Option<JobSlot> {
        let mut held = slots
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        held.insert(key.clone()).then(|| JobSlot {
            slots: Arc::clone(slots),
            key,
        })
    }

    /// Whether a job of this process holds `key`.
    pub(crate) fn holds(&self, key: &str) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(key)
    }
}

/// A held slot; dropping it frees the key.
#[derive(Debug)]
pub(crate) struct JobSlot {
    slots: Arc<JobSlots>,
    key: String,
}

impl Drop for JobSlot {
    fn drop(&mut self) {
        self.slots
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.key);
    }
}

/// The slot key of a backup job's claim on a `backup_index` row.
fn backup_slot_key(backup_id: &str) -> String {
    format!("backup:{backup_id}")
}

/// The slot key of a legacy create's hold on its CREATING `backups` row.
pub(crate) fn legacy_backup_slot_key(backup_arn: &str) -> String {
    format!("legacy-backup:{backup_arn}")
}

/// An `owner_generation` as the catalog stores it (`INTEGER`); generations
/// are minted below `i64::MAX`.
pub(crate) fn generation_column(generation: u64) -> i64 {
    i64::try_from(generation).unwrap_or(i64::MAX)
}

/// The row a claim writes through.
#[derive(Debug, Clone)]
pub(crate) enum JobRecord {
    /// A `backup_index` row.
    Backup(BackupKey),
    /// The `table_restores` record of a restore target.
    Restore { table_id: String },
}

/// A job's handle on its record: the in-process slot, the generation, and
/// the engine the conditional writes go through. Held by a backup job on its
/// `backup_index` row and by a legacy restore on its job record.
pub(crate) struct SqliteJobClaim {
    engine: SqliteEngine,
    _slot: Option<JobSlot>,
    _permit: Option<JobPermit>,
    generation: u64,
    record: JobRecord,
}

impl SqliteJobClaim {
    /// A claim on a restore job record, born with the registration that
    /// writes `generation` on the row. The permit is the job's slot in the
    /// engine's `max_concurrent_jobs`, freed with the claim.
    pub(crate) fn for_restore(
        engine: &SqliteEngine,
        table_id: &str,
        generation: u64,
        permit: JobPermit,
    ) -> Self {
        Self {
            engine: engine.clone(),
            _slot: None,
            _permit: Some(permit),
            generation,
            record: JobRecord::Restore {
                table_id: table_id.to_owned(),
            },
        }
    }

    /// Heartbeat the record every `period` until a heartbeat is not
    /// confirmed; returns why. Never returns while the claim is confirmed.
    pub(crate) async fn heartbeat_loop(&self, period: Duration) -> StorageError {
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            match self.heartbeat().await {
                Ok(Heartbeat::Confirmed) => {}
                Ok(Heartbeat::Lost) => {
                    return StorageError::Internal(
                        "the job record no longer carries this claim's generation".to_owned(),
                    );
                }
                Err(e) => return e,
            }
        }
    }
}

impl JobClaim for SqliteJobClaim {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>> {
        Box::pin(async move {
            let _writer = self.engine.write_lock.lock().await;
            let affected = match &self.record {
                JobRecord::Backup(key) => {
                    sqlx::query(&format!(
                        "UPDATE backup_index SET heartbeat_at = {NOW} \
                         WHERE account_id = ? AND backup_id = ? AND owner_generation = ?"
                    ))
                    .bind(&key.account_id)
                    .bind(&key.backup_id)
                    .bind(generation_column(self.generation))
                    .execute(&self.engine.pool)
                    .await
                }
                JobRecord::Restore { table_id } => {
                    sqlx::query(&format!(
                        "UPDATE table_restores SET heartbeat_at = {NOW} \
                         WHERE table_id = ? AND status = 'RUNNING' AND owner_generation = ?"
                    ))
                    .bind(table_id)
                    .bind(generation_column(self.generation))
                    .execute(&self.engine.pool)
                    .await
                }
            }
            .map_err(db_err)?
            .rows_affected();
            Ok(if affected == 1 {
                Heartbeat::Confirmed
            } else {
                Heartbeat::Lost
            })
        })
    }

    fn set_status<'a>(
        &'a self,
        from: JobStatus,
        to: JobStatus,
        fields: &'a StatusFields,
    ) -> BoxFuture<'a, Result<WriteOutcome, StorageError>> {
        Box::pin(async move {
            let _writer = self.engine.write_lock.lock().await;
            let affected = match &self.record {
                JobRecord::Backup(key) => {
                    sqlx::query(&format!(
                        "UPDATE backup_index SET status = ?, \
                         size_bytes = COALESCE(?, size_bytes), \
                         item_count = COALESCE(?, item_count), \
                         failure_reason = COALESCE(?, failure_reason), \
                         completed_at = CASE WHEN ? THEN {NOW} ELSE completed_at END \
                         WHERE account_id = ? AND backup_id = ? AND owner_generation = ? \
                         AND status = ?"
                    ))
                    .bind(to.as_str())
                    .bind(fields.size_bytes)
                    .bind(fields.item_count)
                    .bind(fields.failure_reason.as_deref())
                    .bind(fields.completed)
                    .bind(&key.account_id)
                    .bind(&key.backup_id)
                    .bind(generation_column(self.generation))
                    .bind(from.as_str())
                    .execute(&self.engine.pool)
                    .await
                }
                JobRecord::Restore { table_id } => {
                    sqlx::query(&format!(
                        "UPDATE table_restores SET status = ?, \
                         items_applied = COALESCE(?, items_applied), \
                         failure_reason = COALESCE(?, failure_reason), \
                         completed_at = CASE WHEN ? THEN {NOW} ELSE completed_at END \
                         WHERE table_id = ? AND owner_generation = ? AND status = ?"
                    ))
                    .bind(to.as_str())
                    .bind(fields.item_count)
                    .bind(fields.failure_reason.as_deref())
                    .bind(fields.completed)
                    .bind(table_id)
                    .bind(generation_column(self.generation))
                    .bind(from.as_str())
                    .execute(&self.engine.pool)
                    .await
                }
            }
            .map_err(db_err)?
            .rows_affected();
            // One process and one file: a statement either ran or errored,
            // so no outcome is ever unknown.
            Ok(if affected == 1 {
                WriteOutcome::Matched
            } else {
                WriteOutcome::NotMatched
            })
        })
    }

    fn read(&self) -> BoxFuture<'_, Result<JobRow, StorageError>> {
        Box::pin(async move {
            let row: Option<(String, i64, String, Option<String>)> = match &self.record {
                JobRecord::Backup(key) => {
                    sqlx::query_as(
                        "SELECT status, owner_generation, heartbeat_at, failure_reason \
                         FROM backup_index WHERE account_id = ? AND backup_id = ?",
                    )
                    .bind(&key.account_id)
                    .bind(&key.backup_id)
                    .fetch_optional(&self.engine.pool)
                    .await
                }
                JobRecord::Restore { table_id } => {
                    sqlx::query_as(
                        "SELECT status, owner_generation, heartbeat_at, failure_reason \
                         FROM table_restores WHERE table_id = ?",
                    )
                    .bind(table_id)
                    .fetch_optional(&self.engine.pool)
                    .await
                }
            }
            .map_err(db_err)?;
            let (status, generation, heartbeat_at, failure_reason) =
                row.ok_or_else(|| match &self.record {
                    JobRecord::Backup(key) => not_found(&key.backup_arn),
                    JobRecord::Restore { table_id } => StorageError::Internal(format!(
                        "restore job record of table {table_id} is gone"
                    )),
                })?;
            Ok(JobRow {
                status: JobStatus::parse(&status)?,
                owner_generation: u64::try_from(generation).unwrap_or_default(),
                heartbeat_at: timestamp_epoch_seconds(&heartbeat_at),
                failure_reason,
            })
        })
    }
}

/// The name of a CREATING restore target registered from `backup_arn`, if
/// any: the one in-use rule DeleteBackup, a restore's registration and
/// `backup_in_use` share. Reads the target's status, not the job's.
pub(crate) async fn creating_restore_target<'e, E: sqlx::SqliteExecutor<'e>>(
    executor: E,
    backup_arn: &str,
) -> Result<Option<String>, StorageError> {
    sqlx::query_scalar(
        "SELECT t.table_name FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
         WHERE r.source_backup_arn = ? AND t.table_status = 'CREATING' LIMIT 1",
    )
    .bind(backup_arn)
    .fetch_optional(executor)
    .await
    .map_err(db_err)
}

/// The error for a backup a CREATING restore target still reads: the shared
/// text, [`extenddb_storage::backup_catalog::backup_in_use`].
pub(crate) fn backup_in_use(table: &str, backup_arn: &str) -> StorageError {
    extenddb_storage::backup_catalog::backup_in_use(table, backup_arn)
}

fn db_err(e: sqlx::Error) -> StorageError {
    StorageError::Internal(format!("Database error: {e}"))
}

fn not_found(backup_arn: &str) -> StorageError {
    extenddb_storage::backup_catalog::backup_not_found(backup_arn)
}

/// The columns of one `backup_index` row, in the order every read selects
/// them.
const ROW_COLUMNS: &str = "backup_id, account_id, table_name, backup_arn, backup_name, status, \
     store_kind, store_root, store_id, size_bytes, item_count, created_at, completed_at, \
     failure_reason, source_table_details_json, owner_generation, heartbeat_at";

/// One `backup_index` row as the driver reads it.
#[derive(sqlx::FromRow)]
struct RowTuple {
    backup_id: String,
    account_id: String,
    table_name: String,
    backup_arn: String,
    backup_name: String,
    status: String,
    store_kind: String,
    store_root: String,
    store_id: String,
    size_bytes: i64,
    item_count: i64,
    created_at: String,
    completed_at: Option<String>,
    failure_reason: Option<String>,
    source_table_details_json: String,
    owner_generation: i64,
    heartbeat_at: String,
}

fn row_from_tuple(t: RowTuple) -> Result<BackupRow, StorageError> {
    let RowTuple {
        backup_id,
        account_id,
        table_name,
        backup_arn,
        backup_name,
        status,
        store_kind,
        store_root,
        store_id,
        size_bytes,
        item_count,
        created_at,
        completed_at,
        failure_reason,
        source_table_details_json,
        owner_generation,
        heartbeat_at,
    } = t;
    let store_kind = StoreKind::parse(&store_kind).ok_or_else(|| {
        StorageError::Internal(format!(
            "backup {backup_arn} carries an unknown store kind {store_kind:?}"
        ))
    })?;
    let source_table_details_json =
        serde_json::from_str(&source_table_details_json).map_err(|e| {
            StorageError::Internal(format!(
                "backup {backup_arn} carries a source table that is not JSON: {e}"
            ))
        })?;
    Ok(BackupRow {
        backup_id,
        account_id,
        table_name,
        backup_arn,
        backup_name,
        status: BackupStatus::parse(&status)?,
        store: StoreIdentity {
            store_kind,
            store_root,
            store_id,
        },
        size_bytes,
        item_count,
        created_at: timestamp_epoch_seconds(&created_at),
        completed_at: completed_at.as_deref().map(timestamp_epoch_seconds),
        failure_reason,
        source_table_details_json,
        owner_generation: u64::try_from(owner_generation).unwrap_or_default(),
        heartbeat_at: timestamp_epoch_seconds(&heartbeat_at),
    })
}

/// The rows `read` fetches, as a stream: the read runs once, on the first
/// poll, and a failed read is one `Err` item.
fn rows_stream<'a, F>(read: F) -> BoxStream<'a, Result<BackupRow, StorageError>>
where
    F: std::future::Future<Output = Result<Vec<RowTuple>, StorageError>> + Send + 'a,
{
    use futures::StreamExt;
    Box::pin(
        futures::stream::once(async move {
            match read.await {
                Ok(rows) => rows.into_iter().map(row_from_tuple).collect::<Vec<_>>(),
                Err(e) => vec![Err(e)],
            }
        })
        .flat_map(futures::stream::iter),
    )
}

impl SqliteEngine {
    /// Every RUNNING restore job record at startup was owned by the process
    /// that stopped: one process serves a file, so a restart is proof of
    /// death. Set each record's heartbeat to the epoch, so the reconciler's
    /// first cycle fails it and removes its target without waiting the stale
    /// window. Returns the names of the targets whose record was aged (one
    /// already at the epoch is not reported again). Runs before the server
    /// takes requests, so no restore of this process can be in flight.
    pub(crate) async fn expire_restore_job_heartbeats(&self) -> Result<Vec<String>, StorageError> {
        let _writer = self.write_lock.lock().await;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_err)?;
        let names: Vec<String> = sqlx::query_scalar(
            "UPDATE table_restores SET heartbeat_at = '1970-01-01T00:00:00.000Z' \
             WHERE status = 'RUNNING' AND heartbeat_at > '1970-01-01T00:00:00.000Z' \
             RETURNING COALESCE((SELECT table_name FROM tables t \
             WHERE t.table_id = table_restores.table_id), table_id)",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok(names)
    }

    /// Remove the maintenance claim, whoever holds it: the server's startup
    /// step under the serve lock, where one process has the file and so no
    /// holder can be alive (a `backup purge` that died, or was killed, left
    /// the row with a heartbeat the jobs' check would otherwise honour for
    /// the whole stale window). Returns the holder's label when a row was
    /// removed.
    pub(crate) async fn clear_maintenance_claim(&self) -> Result<Option<String>, StorageError> {
        let _writer = self.write_lock.lock().await;
        sqlx::query_scalar("DELETE FROM backup_maintenance RETURNING holder")
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)
    }
}

impl BackupCatalog for SqliteEngine {
    fn catalog_id(&self) -> BoxFuture<'_, Result<String, StorageError>> {
        Box::pin(async move {
            let id: Option<String> =
                sqlx::query_scalar("SELECT value FROM settings WHERE key = 'catalog_id'")
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(db_err)?;
            id.ok_or_else(|| {
                StorageError::Internal(
                    "the catalog has no catalog_id; run extenddb migrate --yes".to_owned(),
                )
            })
        })
    }

    fn claim_backup<'a>(
        &'a self,
        row: &'a BackupRow,
    ) -> BoxFuture<'a, Result<Box<dyn JobClaim>, StorageError>> {
        Box::pin(async move {
            // The slot first, so no row ever exists unowned.
            let Some(slot) = JobSlots::take(&self.job_slots, backup_slot_key(&row.backup_id))
            else {
                return Err(StorageError::AlreadyExists(format!(
                    "a live job owns backup {}",
                    row.backup_arn
                )));
            };
            let generation = new_generation();
            let source_table = serde_json::to_string(&row.source_table_details_json)
                .map_err(|e| StorageError::Internal(format!("source table details: {e}")))?;
            let _writer = self.write_lock.lock().await;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(db_err)?;
            let inserted = sqlx::query(&format!(
                "INSERT OR IGNORE INTO backup_index (backup_id, account_id, table_name, \
                 backup_arn, backup_name, status, store_kind, store_root, store_id, size_bytes, \
                 item_count, created_at, source_table_details_json, owner_generation, \
                 heartbeat_at) \
                 VALUES (?, ?, ?, ?, ?, 'CREATING', ?, ?, ?, ?, ?, ?, ?, ?, {NOW})"
            ))
            .bind(&row.backup_id)
            .bind(&row.account_id)
            .bind(&row.table_name)
            .bind(&row.backup_arn)
            .bind(&row.backup_name)
            .bind(row.store.store_kind.as_str())
            .bind(&row.store.store_root)
            .bind(&row.store.store_id)
            .bind(row.size_bytes)
            .bind(row.item_count)
            .bind(format_epoch_seconds(row.created_at))
            .bind(&source_table)
            .bind(generation_column(generation))
            .execute(&mut *tx)
            .await
            .map_err(db_err)?
            .rows_affected();
            tx.commit().await.map_err(db_err)?;
            if inserted == 0 {
                drop(slot);
                return Err(StorageError::AlreadyExists(format!(
                    "backup {} already has a row",
                    row.backup_arn
                )));
            }
            Ok(Box::new(SqliteJobClaim {
                engine: self.clone(),
                _slot: Some(slot),
                _permit: None,
                generation,
                record: JobRecord::Backup(row.key()),
            }) as Box<dyn JobClaim>)
        })
    }

    fn list_backup_rows<'a>(
        &'a self,
        status: Option<BackupStatus>,
        store: &'a StoreIdentity,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        rows_stream(async move {
            sqlx::query_as(&format!(
                "SELECT {ROW_COLUMNS} FROM backup_index \
                 WHERE store_kind = ?1 AND store_root = ?2 AND store_id = ?3 \
                 AND (?4 IS NULL OR status = ?4) ORDER BY created_at, backup_arn"
            ))
            .bind(store.store_kind.as_str())
            .bind(&store.store_root)
            .bind(&store.store_id)
            .bind(status.map(BackupStatus::as_str))
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)
        })
    }

    fn list_account_backup_rows<'a>(
        &'a self,
        account_id: &'a str,
        table_name: Option<&'a str>,
        bounds: &'a TimeBounds,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        rows_stream(async move {
            // `created_at` is written by `format_epoch_seconds`, fixed width,
            // so the bounds compare as text in time order.
            sqlx::query_as(&format!(
                "SELECT {ROW_COLUMNS} FROM backup_index WHERE account_id = ?1 \
                 AND (?2 IS NULL OR table_name = ?2) \
                 AND (?3 IS NULL OR created_at >= ?3) \
                 AND (?4 IS NULL OR created_at < ?4) \
                 ORDER BY created_at, backup_arn"
            ))
            .bind(account_id)
            .bind(table_name)
            .bind(bounds.lower.map(format_epoch_seconds))
            .bind(bounds.upper.map(format_epoch_seconds))
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)
        })
    }

    fn read_backup_row<'a>(
        &'a self,
        arn: &'a BackupArn,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>> {
        Box::pin(async move {
            let row: Option<RowTuple> = sqlx::query_as(&format!(
                "SELECT {ROW_COLUMNS} FROM backup_index WHERE account_id = ? AND backup_id = ? \
                 AND table_name = ? AND backup_arn = ?"
            ))
            .bind(arn.account_id())
            .bind(arn.backup_id())
            .bind(arn.table_name())
            .bind(arn.to_arn_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
            row.map(row_from_tuple).transpose()
        })
    }

    fn start_backup_delete<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<DeleteStart, StorageError>> {
        Box::pin(async move {
            // Under the write lock, in one write transaction: a restore's
            // registration runs its in-use check the same way, so the two
            // order wholly one before the other.
            let _writer = self.write_lock.lock().await;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(db_err)?;
            let status: Option<String> = sqlx::query_scalar(
                "SELECT status FROM backup_index WHERE account_id = ? AND backup_id = ?",
            )
            .bind(&key.account_id)
            .bind(&key.backup_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_err)?;
            let status = BackupStatus::parse(&status.ok_or_else(|| not_found(&key.backup_arn))?)?;
            if status != BackupStatus::Available {
                return Ok(DeleteStart::Status(status));
            }
            if let Some(table) = creating_restore_target(&mut *tx, &key.backup_arn).await? {
                return Ok(DeleteStart::InUse(table));
            }
            sqlx::query(
                "UPDATE backup_index SET status = 'DELETING' \
                 WHERE account_id = ? AND backup_id = ? AND status = 'AVAILABLE'",
            )
            .bind(&key.account_id)
            .bind(&key.backup_id)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
            tx.commit().await.map_err(db_err)?;
            Ok(DeleteStart::Deleting)
        })
    }

    fn set_backup_deleted<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let _writer = self.write_lock.lock().await;
            let affected = sqlx::query(&format!(
                "UPDATE backup_index SET status = 'DELETED', completed_at = {NOW} \
                 WHERE account_id = ? AND backup_id = ? AND status = 'DELETING'"
            ))
            .bind(&key.account_id)
            .bind(&key.backup_id)
            .execute(&self.pool)
            .await
            .map_err(db_err)?
            .rows_affected();
            Ok(affected == 1)
        })
    }

    fn remove_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let _writer = self.write_lock.lock().await;
            let affected = sqlx::query(
                "DELETE FROM backup_index WHERE account_id = ? AND backup_id = ? \
                 AND status IN ('DELETED', 'FAILED')",
            )
            .bind(&key.account_id)
            .bind(&key.backup_id)
            .execute(&self.pool)
            .await
            .map_err(db_err)?
            .rows_affected();
            Ok(affected == 1)
        })
    }

    fn remove_dead_creating_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let _writer = self.write_lock.lock().await;
            let affected = sqlx::query(
                "DELETE FROM backup_index WHERE account_id = ? AND backup_id = ? \
                 AND status = 'CREATING' \
                 AND julianday(heartbeat_at) < julianday('now') - ? / 86400.0",
            )
            .bind(&key.account_id)
            .bind(&key.backup_id)
            .bind(stale_after.as_secs_f64())
            .execute(&self.pool)
            .await
            .map_err(db_err)?
            .rows_affected();
            Ok(affected == 1)
        })
    }

    fn backup_in_use<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<Option<String>, StorageError>> {
        Box::pin(async move { creating_restore_target(&self.pool, &key.backup_arn).await })
    }

    fn remove_restore_target<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            // The key must name a CREATING restore target with this table id;
            // a table re-created under the name has another id and is not
            // touched. `abort_restore` then claims it from CREATING in one
            // statement under the write lock, so it and the ACTIVE flip
            // exclude each other.
            let is_target: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables t JOIN table_restores r \
                 ON r.table_id = t.table_id WHERE t.table_id = ? AND t.account_id = ? \
                 AND t.table_name = ? AND t.table_status = 'CREATING')",
            )
            .bind(&key.table_id)
            .bind(&key.account_id)
            .bind(&key.table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(db_err)?;
            if !is_target {
                return Ok(false);
            }
            // Progress rows belong to a later release's restore path; a
            // catalog without that table has none to delete.
            let has_progress: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master \
                 WHERE type = 'table' AND name = 'restore_progress')",
            )
            .fetch_one(&self.pool)
            .await
            .map_err(db_err)?;
            if has_progress {
                let _writer = self.write_lock.lock().await;
                sqlx::query("DELETE FROM restore_progress WHERE table_id = ?")
                    .bind(&key.table_id)
                    .execute(&self.pool)
                    .await
                    .map_err(db_err)?;
            }
            self.abort_restore(&key.table_id).await
        })
    }

    fn list_dead_restore_jobs(
        &self,
        stale_after: Duration,
    ) -> BoxFuture<'_, Result<Vec<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            // Two arms, as on PostgreSQL: a RUNNING record whose heartbeat is
            // stale (a dead owner), and a FAILED record whose table is still
            // a CREATING restore target once the record is stale (the FAILED
            // write landed and the removal did not; `fail_restore_job` and
            // the copy's own failure path stamp `completed_at`, and a record
            // without one is aged by its heartbeat). The delay is what leaves
            // a task still on its own failure path to finish it. Both ages
            // are measured on the database's clock (`julianday('now')`).
            #[allow(clippy::type_complexity)]
            let rows: Vec<(
                String,
                String,
                String,
                String,
                String,
                String,
                i64,
                String,
            )> = sqlx::query_as(
                "SELECT r.table_id, t.account_id, t.table_name, r.source_backup_arn, \
                     r.kind, r.status, r.owner_generation, r.heartbeat_at \
                     FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
                     WHERE (r.status = 'RUNNING' \
                            AND julianday(r.heartbeat_at) < julianday('now') - ?1 / 86400.0) \
                        OR (r.status = 'FAILED' AND t.table_status = 'CREATING' \
                            AND julianday(COALESCE(r.completed_at, r.heartbeat_at)) \
                                < julianday('now') - ?1 / 86400.0) \
                     ORDER BY t.account_id, t.table_name",
            )
            .bind(stale_after.as_secs_f64())
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
            rows.into_iter()
                .map(
                    |(
                        table_id,
                        account_id,
                        table_name,
                        source_backup_arn,
                        kind,
                        status,
                        owner_generation,
                        heartbeat_at,
                    )| {
                        Ok(RestoreJobRow {
                            target: RestoreTargetKey {
                                account_id,
                                table_name,
                                table_id,
                            },
                            source_backup_arn,
                            kind: RestoreKind::parse(&kind)?,
                            status: JobStatus::parse(&status)?,
                            owner_generation: u64::try_from(owner_generation).unwrap_or_default(),
                            heartbeat_at: timestamp_epoch_seconds(&heartbeat_at),
                        })
                    },
                )
                .collect()
        })
    }

    fn fail_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let _writer = self.write_lock.lock().await;
            let affected = sqlx::query(&format!(
                "UPDATE table_restores SET status = 'FAILED', failure_reason = ?, \
                 completed_at = {NOW} \
                 WHERE table_id = ? AND status = 'RUNNING' AND EXISTS(\
                 SELECT 1 FROM tables t WHERE t.table_id = table_restores.table_id \
                 AND t.account_id = ? AND t.table_name = ?)"
            ))
            .bind(reason)
            .bind(&key.table_id)
            .bind(&key.account_id)
            .bind(&key.table_name)
            .execute(&self.pool)
            .await
            .map_err(db_err)?
            .rows_affected();
            Ok(affected == 1)
        })
    }

    fn read_store_ids<'a>(
        &'a self,
        store_kind: StoreKind,
        store_root: &'a str,
    ) -> BoxFuture<'a, Result<Vec<String>, StorageError>> {
        Box::pin(async move {
            sqlx::query_scalar(
                "SELECT DISTINCT store_id FROM backup_index \
                 WHERE store_kind = ? AND store_root = ? ORDER BY store_id",
            )
            .bind(store_kind.as_str())
            .bind(store_root)
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)
        })
    }

    fn maintenance_claim<'a>(
        &'a self,
        holder: &'a str,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<Box<dyn MaintenanceClaim>, StorageError>> {
        Box::pin(async move {
            // One process serves a file (the serve lock), so the row is the
            // claim: taken under the engine write lock, refused while a row
            // with a live heartbeat exists, taken over from a dead one.
            let _writer = self.write_lock.lock().await;
            let live: Option<String> = sqlx::query_scalar(
                "SELECT holder FROM backup_maintenance \
                 WHERE julianday(heartbeat_at) >= julianday('now') - ? / 86400.0",
            )
            .bind(stale_after.as_secs_f64())
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
            if let Some(current) = live {
                return Err(StorageError::AlreadyExists(format!(
                    "the backup maintenance claim is held by {current}"
                )));
            }
            let generation = new_generation();
            sqlx::query(&format!(
                "INSERT OR REPLACE INTO backup_maintenance \
                 (singleton, holder, owner_generation, claimed_at, heartbeat_at) \
                 VALUES (1, ?, ?, {NOW}, {NOW})"
            ))
            .bind(holder)
            .bind(generation_column(generation))
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
            Ok(Box::new(SqliteMaintenanceClaim {
                engine: self.clone(),
                generation,
            }) as Box<dyn MaintenanceClaim>)
        })
    }

    fn check_maintenance_claim(
        &self,
        stale_after: Duration,
    ) -> BoxFuture<'_, Result<bool, StorageError>> {
        Box::pin(async move {
            sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM backup_maintenance \
                 WHERE julianday(heartbeat_at) >= julianday('now') - ? / 86400.0)",
            )
            .bind(stale_after.as_secs_f64())
            .fetch_one(&self.pool)
            .await
            .map_err(db_err)
        })
    }
}

/// The holder's handle on the `backup_maintenance` row.
struct SqliteMaintenanceClaim {
    engine: SqliteEngine,
    generation: u64,
}

impl MaintenanceClaim for SqliteMaintenanceClaim {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>> {
        Box::pin(async move {
            let _writer = self.engine.write_lock.lock().await;
            let affected = sqlx::query(&format!(
                "UPDATE backup_maintenance SET heartbeat_at = {NOW} WHERE owner_generation = ?"
            ))
            .bind(generation_column(self.generation))
            .execute(&self.engine.pool)
            .await
            .map_err(db_err)?
            .rows_affected();
            Ok(if affected == 1 {
                Heartbeat::Confirmed
            } else {
                Heartbeat::Lost
            })
        })
    }

    fn release(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move {
            let _writer = self.engine.write_lock.lock().await;
            sqlx::query("DELETE FROM backup_maintenance WHERE owner_generation = ?")
                .bind(generation_column(self.generation))
                .execute(&self.engine.pool)
                .await
                .map_err(db_err)?;
            Ok(())
        })
    }
}

/// The `backup_catalog` module's unit tests that need no database.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_row_tuple_with_an_unknown_store_kind_status_or_source_table_is_internal() {
        let good = || RowTuple {
            backup_id: "01757500000123-0badcafe".to_owned(),
            account_id: "123456789012".to_owned(),
            table_name: "Music".to_owned(),
            backup_arn: "arn".to_owned(),
            backup_name: "b".to_owned(),
            status: "AVAILABLE".to_owned(),
            store_kind: "s3".to_owned(),
            store_root: "s3://bucket/prefix".to_owned(),
            store_id: "store-1".to_owned(),
            size_bytes: 1,
            item_count: 2,
            created_at: "2025-09-10T10:26:40.123000Z".to_owned(),
            completed_at: None,
            failure_reason: None,
            source_table_details_json: "{}".to_owned(),
            owner_generation: 7,
            heartbeat_at: "2025-09-10T10:26:41.000Z".to_owned(),
        };
        let row = row_from_tuple(good()).unwrap();
        assert_eq!(row.store.store_kind, StoreKind::S3);
        assert_eq!(row.owner_generation, 7);
        assert!((row.created_at - 1_757_500_000.123).abs() < 1e-6);
        assert_eq!(row.heartbeat_at, 1_757_500_001.0);
        let mut bad_kind = good();
        bad_kind.store_kind = "gcs".to_owned();
        assert!(matches!(
            row_from_tuple(bad_kind),
            Err(StorageError::Internal(_))
        ));
        let mut bad_status = good();
        bad_status.status = "ACTIVE".to_owned();
        assert!(matches!(
            row_from_tuple(bad_status),
            Err(StorageError::Internal(_))
        ));
        let mut bad_source = good();
        bad_source.source_table_details_json = "not json".to_owned();
        assert!(matches!(
            row_from_tuple(bad_source),
            Err(StorageError::Internal(_))
        ));
    }

    #[test]
    fn a_slot_is_held_once_and_freed_by_its_guard() {
        let slots = Arc::new(JobSlots::default());
        let held = JobSlots::take(&slots, "backup:1".to_owned()).expect("free");
        assert!(slots.holds("backup:1"));
        assert!(JobSlots::take(&slots, "backup:1".to_owned()).is_none());
        assert!(JobSlots::take(&slots, "backup:2".to_owned()).is_some());
        drop(held);
        assert!(!slots.holds("backup:1"));
        assert!(JobSlots::take(&slots, "backup:1".to_owned()).is_some());
    }

    #[test]
    fn a_generation_fits_the_integer_column() {
        assert_eq!(generation_column(7), 7);
        assert_eq!(generation_column(u64::MAX), i64::MAX);
        assert_ne!(
            generation_column(new_generation()),
            generation_column(extenddb_storage::backup_catalog::UNOWNED_GENERATION)
        );
    }
}
