// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The catalog index on SQLite: `backup_index`, the restore job fields of
//! `table_restores`, `backup_maintenance` and `settings.catalog_id`.
//!
//! One process serves a database file (the serve lock enforces it, on Unix;
//! on a platform where no lock is taken the server binds CreateBackup to the
//! in-catalog path, `extenddb_server::backup_store::in_catalog_create_only`,
//! since two servers on one file would take each other's rows for dead), so
//! a job's claim is an in-process slot ([`JobSlots`]): a key held in a set for
//! the job's life and released by a drop guard, so a task that ends by any
//! route, a panic included, frees it. A restart frees every slot at once,
//! which is the proof of death this backend needs: a CREATING row (a
//! `backup_index` row or an in-catalog `backups` row) whose slot no task
//! holds is dead, whatever its heartbeat says, so the reconciler's takeover
//! ([`BackupCatalog::claim_dead_backup`]) reads no stale window here, and
//! every RUNNING restore job record of the stopped process is aged at startup
//! so the reconciler's first cycle finds it
//! ([`SqliteEngine::expire_restore_job_heartbeats`]). A store-backed
//! restore's claim holds the slot of its target name
//! ([`restore_slot_key`]) the same way: the registration takes it before the
//! target exists, so two registrations into one name exclude each other in
//! this process, and a RUNNING `manifest` record whose slot no task holds is
//! taken over at once by [`BackupCatalog::claim_restore_dead`].
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
//! command run while the server is stopped. Such a command passes a zero
//! stale window, which the dead-owner listings and removals read as "every
//! owner is dead" and select by status alone ([`zero_window_clause`]), so a
//! heartbeat stamped ahead of the clock is not read as a live owner.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use extenddb_core::backup_format::BackupArn;
use extenddb_core::types::{CreateTableInput, RestoreSummary};
use extenddb_storage::JobPermit;
use extenddb_storage::backup_catalog::{
    BackupCatalog, BackupKey, BackupRow, BackupStatus, DeleteStart, ForgetRow, Heartbeat, JobClaim,
    JobRow, JobStatus, MaintenanceClaim, RestoreJobRow, RestoreKind, RestoreRegistered,
    RestoreRegistrationRow, RestoreTargetKey, StaleClock, StatusFields, StoreIdentity, StoreKind,
    TimeBounds, UNOWNED_GENERATION, WriteOutcome, new_generation,
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

/// The slot key of an in-catalog restore's hold on its job record, by
/// target table id. Held for the restore's life
/// ([`SqliteJobClaim::for_restore`]), so the reconciler never judges a
/// running restore of this process by its heartbeat, which queues behind
/// every writer holding the write lock.
pub(crate) fn legacy_restore_slot_key(table_id: &str) -> String {
    format!("legacy-restore:{table_id}")
}

/// The slot key of a store-backed restore's claim on its target name: taken
/// before the target exists, so two registrations into one name in this
/// process exclude each other before either reaches the catalog, and held
/// by the claim for the job's life.
pub(crate) fn restore_slot_key(account_id: &str, table_name: &str) -> String {
    format!("restore:{account_id}/{table_name}")
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
    /// engine's `max_concurrent_jobs`, freed with the claim. The in-process
    /// slot ([`legacy_restore_slot_key`]) is the SQLite rule of design 4(e): a
    /// RUNNING record whose slot a task of this process holds is alive
    /// whatever its heartbeat says, so a heartbeat delayed behind a long
    /// writer (an UpdateTable adding a GSI to a large table holds the write
    /// lock for minutes) cannot make the reconciler remove a live restore;
    /// one whose slot no task holds (a restart frees every slot) is judged by
    /// its heartbeat, which the restart ages to the epoch. The id is fresh,
    /// so the slot is free.
    pub(crate) fn for_restore(
        engine: &SqliteEngine,
        table_id: &str,
        generation: u64,
        permit: JobPermit,
    ) -> Self {
        Self {
            engine: engine.clone(),
            _slot: JobSlots::take(&engine.job_slots, legacy_restore_slot_key(table_id)),
            _permit: Some(permit),
            generation,
            record: JobRecord::Restore {
                table_id: table_id.to_owned(),
            },
        }
    }

    /// A claim on a store-backed restore's job record, born with the
    /// registration that writes `generation` on the row, holding the
    /// target name's slot ([`restore_slot_key`]) for the job's life. The
    /// engine's job holds the permit; none is held here.
    pub(crate) fn for_manifest_restore(
        engine: &SqliteEngine,
        table_id: &str,
        generation: u64,
        slot: JobSlot,
    ) -> Self {
        Self {
            engine: engine.clone(),
            _slot: Some(slot),
            _permit: None,
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

    /// The beat's `UPDATE`, run by a caller that holds the engine write
    /// lock.
    async fn beat_under_the_lock(&self) -> Result<Heartbeat, StorageError> {
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
    }
}

impl JobClaim for SqliteJobClaim {
    fn generation(&self) -> u64 {
        self.generation
    }

    /// The beat is an `UPDATE` under the engine write lock, so it queues
    /// behind every writer that holds or waits for that lock (item writes,
    /// a restore's copy batches, the GSI backfill, the TTL sweep, a table's
    /// drop) and returns only once they have; a busy writer delays the beat
    /// by as long as it holds the lock.
    fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>> {
        Box::pin(async move {
            let _writer = self.engine.write_lock.lock().await;
            self.beat_under_the_lock().await
        })
    }

    /// The bound starts once the beat holds the engine write lock: the wait
    /// for the lock is this process's own queue (a writer holding it past a
    /// heartbeat period, a GSI backfill say, delays the beat and does not end
    /// the job), and only the `UPDATE` itself is held to `bound`.
    fn heartbeat_within(
        &self,
        bound: Duration,
    ) -> BoxFuture<'_, Option<Result<Heartbeat, StorageError>>> {
        Box::pin(async move {
            let _writer = self.engine.write_lock.lock().await;
            tokio::time::timeout(bound, self.beat_under_the_lock())
                .await
                .ok()
        })
    }

    /// The wait for the engine write lock is outside the bound (above): a
    /// beat parked there confirms the claim for as long as it waits.
    fn heartbeat_parks_before_its_bound(&self) -> bool {
        true
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
                    let source_table = fields
                        .source_table_details_json
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()
                        .map_err(|e| {
                            StorageError::Internal(format!("source table details: {e}"))
                        })?;
                    sqlx::query(&format!(
                        "UPDATE backup_index SET status = ?, \
                         size_bytes = COALESCE(?, size_bytes), \
                         item_count = COALESCE(?, item_count), \
                         failure_reason = COALESCE(?, failure_reason), \
                         source_table_details_json = COALESCE(?, source_table_details_json), \
                         completed_at = CASE WHEN ? THEN {NOW} ELSE completed_at END \
                         WHERE account_id = ? AND backup_id = ? AND owner_generation = ? \
                         AND status = ?"
                    ))
                    .bind(to.as_str())
                    .bind(fields.size_bytes)
                    .bind(fields.item_count)
                    .bind(fields.failure_reason.as_deref())
                    .bind(source_table)
                    .bind(fields.completed)
                    .bind(&key.account_id)
                    .bind(&key.backup_id)
                    .bind(generation_column(self.generation))
                    .bind(from.as_str())
                    .execute(&self.engine.pool)
                    .await
                }
                JobRecord::Restore { table_id } => {
                    // The record's progress (`file_index`, `items_applied`)
                    // is `bulk_load`'s write; a status write leaves it.
                    sqlx::query(&format!(
                        "UPDATE table_restores SET status = ?, \
                         failure_reason = COALESCE(?, failure_reason), \
                         completed_at = CASE WHEN ? THEN {NOW} ELSE completed_at END \
                         WHERE table_id = ? AND owner_generation = ? AND status = ?"
                    ))
                    .bind(to.as_str())
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

/// The age test of a dead-owner listing or removal, for one bound
/// parameter (`window` in seconds): `column` older than the window on the
/// database's clock, or, for a zero window, nothing (the bound is still
/// consumed). A zero window is `backup purge`'s under the serve lock: one
/// process has the file, so every owner is dead whatever its heartbeat says,
/// a heartbeat stamped ahead of the clock (a backward clock step) included,
/// where `julianday(x) < julianday('now')` would count it live.
fn zero_window_clause(window: Duration, column: &str) -> String {
    if window.is_zero() {
        "? = 0.0".to_owned()
    } else {
        format!("julianday({column}) < julianday('now') - ? / 86400.0")
    }
}

/// The columns of one restore job record joined with its table, in the
/// order [`RestoreJobTuple`] reads them; every read of a record selects
/// them.
const RESTORE_ROW_COLUMNS: &str = "r.table_id, t.account_id, t.table_name, r.source_backup_arn, \
     r.kind, r.status, r.owner_generation, r.heartbeat_at, r.store_kind, r.store_root, \
     r.store_id, r.spool_host, r.file_index, r.items_applied, r.failure_reason, r.completed_at";

/// One restore job record as the driver reads it.
#[allow(clippy::type_complexity)]
type RestoreJobTuple = (
    String,
    String,
    String,
    String,
    String,
    String,
    i64,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<String>,
);

fn restore_job_from_tuple(t: RestoreJobTuple) -> Result<RestoreJobRow, StorageError> {
    let (
        table_id,
        account_id,
        table_name,
        source_backup_arn,
        kind,
        status,
        owner_generation,
        heartbeat_at,
        store_kind,
        store_root,
        store_id,
        spool_host,
        file_index,
        items_applied,
        failure_reason,
        completed_at,
    ) = t;
    let store = match (store_kind, store_root, store_id) {
        (Some(kind), Some(store_root), Some(store_id)) => Some(StoreIdentity {
            store_kind: StoreKind::parse(&kind).ok_or_else(|| {
                StorageError::Internal(format!(
                    "restore of {source_backup_arn} carries an unknown store kind {kind:?}"
                ))
            })?,
            store_root,
            store_id,
        }),
        _ => None,
    };
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
        store,
        spool_host,
        file_index: file_index.and_then(|i| u32::try_from(i).ok()),
        items_applied: items_applied.and_then(|i| u64::try_from(i).ok()),
        failure_reason,
        completed_at: completed_at.as_deref().map(timestamp_epoch_seconds),
    })
}

impl SqliteEngine {
    /// Every RUNNING restore job record at startup was owned by the process
    /// that stopped: one process serves a file, so a restart is proof of
    /// death. Set each record's heartbeat to the epoch, so the reconciler's
    /// first cycle acts on it (a store-backed record is taken over and
    /// resumed, a legacy one failed and its target removed) without waiting
    /// the stale window. Returns the name and kind of each target whose
    /// record was aged (one already at the epoch is not reported again).
    /// Runs before the server takes requests, so no restore of this process
    /// can be in flight.
    pub(crate) async fn expire_restore_job_heartbeats(
        &self,
    ) -> Result<Vec<(String, RestoreKind)>, StorageError> {
        let _writer = self.write_lock.lock().await;
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_err)?;
        let rows: Vec<(String, String)> = sqlx::query_as(
            "UPDATE table_restores SET heartbeat_at = '1970-01-01T00:00:00.000Z' \
             WHERE status = 'RUNNING' AND heartbeat_at > '1970-01-01T00:00:00.000Z' \
             RETURNING COALESCE((SELECT table_name FROM tables t \
             WHERE t.table_id = table_restores.table_id), table_id), kind",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        rows.into_iter()
            .map(|(name, kind)| Ok((name, RestoreKind::parse(&kind)?)))
            .collect()
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

    /// DeleteBackup's first step, and with `remove` the outright removal of
    /// `forget_available_backup_row`, as one write transaction under the
    /// write lock: read the status, run the in-use rule, then write DELETING
    /// or, with `remove`, delete the row (`Deleting` then means removed). A
    /// restore's registration runs its in-use check the same way, so it
    /// orders wholly before or after either. One body, so the two methods
    /// share the lock and the rule.
    async fn start_backup_delete_tx(
        &self,
        key: &BackupKey,
        remove: bool,
    ) -> Result<DeleteStart, StorageError> {
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
        let write = if remove {
            "DELETE FROM backup_index \
             WHERE account_id = ? AND backup_id = ? AND status = 'AVAILABLE'"
        } else {
            "UPDATE backup_index SET status = 'DELETING' \
             WHERE account_id = ? AND backup_id = ? AND status = 'AVAILABLE'"
        };
        sqlx::query(write)
            .bind(&key.account_id)
            .bind(&key.backup_id)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok(DeleteStart::Deleting)
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
            // OR IGNORE: typed values fail no NOT NULL or CHECK, so only a key conflict is ignored.
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

    fn insert_backup_row<'a>(
        &'a self,
        row: &'a BackupRow,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            // No slot: an unowned row has no job. A job that holds the id's
            // slot and has not inserted its row yet finds this one and is
            // refused, as it is by any row with the key.
            let source_table = serde_json::to_string(&row.source_table_details_json)
                .map_err(|e| StorageError::Internal(format!("source table details: {e}")))?;
            let _writer = self.write_lock.lock().await;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(db_err)?;
            // `ON CONFLICT DO NOTHING` covers the primary key (`account_id`,
            // `backup_id`) and the unique `backup_id`, and no other
            // constraint: a NOT NULL or CHECK violation is an error.
            let inserted = sqlx::query(&format!(
                "INSERT INTO backup_index (backup_id, account_id, table_name, \
                 backup_arn, backup_name, status, store_kind, store_root, store_id, size_bytes, \
                 item_count, created_at, completed_at, failure_reason, \
                 source_table_details_json, owner_generation, heartbeat_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, {NOW}, ?, ?, ?, {NOW}) \
                 ON CONFLICT DO NOTHING"
            ))
            .bind(&row.backup_id)
            .bind(&row.account_id)
            .bind(&row.table_name)
            .bind(&row.backup_arn)
            .bind(&row.backup_name)
            .bind(row.status.as_str())
            .bind(row.store.store_kind.as_str())
            .bind(&row.store.store_root)
            .bind(&row.store.store_id)
            .bind(row.size_bytes)
            .bind(row.item_count)
            .bind(format_epoch_seconds(row.created_at))
            .bind(&row.failure_reason)
            .bind(&source_table)
            .bind(generation_column(UNOWNED_GENERATION))
            .execute(&mut *tx)
            .await
            .map_err(db_err)?
            .rows_affected();
            tx.commit().await.map_err(db_err)?;
            Ok(inserted == 1)
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

    fn newest_available_backup_row<'a>(
        &'a self,
        store: &'a StoreIdentity,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>> {
        Box::pin(async move {
            let row: Option<RowTuple> = sqlx::query_as(&format!(
                "SELECT {ROW_COLUMNS} FROM backup_index \
                 WHERE store_kind = ? AND store_root = ? AND store_id = ? \
                 AND status = 'AVAILABLE' ORDER BY created_at DESC, backup_arn DESC LIMIT 1"
            ))
            .bind(store.store_kind.as_str())
            .bind(&store.store_root)
            .bind(&store.store_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
            row.map(row_from_tuple).transpose()
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
        Box::pin(self.start_backup_delete_tx(key, false))
    }

    fn forget_available_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<ForgetRow, StorageError>> {
        Box::pin(async move {
            Ok(match self.start_backup_delete_tx(key, true).await? {
                DeleteStart::Deleting => ForgetRow::Removed,
                DeleteStart::InUse(table) => ForgetRow::InUse(table),
                DeleteStart::Status(status) => ForgetRow::Status(status),
            })
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
            // A zero window selects by status alone (`zero_window_clause`):
            // the caller holds the serve lock and every owner is dead,
            // whatever the heartbeat says, a heartbeat ahead of the clock
            // (a backward clock step) included.
            let affected = sqlx::query(&format!(
                "DELETE FROM backup_index WHERE account_id = ? AND backup_id = ? \
                 AND status = 'CREATING' \
                 AND {}",
                zero_window_clause(stale_after, "heartbeat_at")
            ))
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

    fn claim_dead_backup<'a>(
        &'a self,
        key: &'a BackupKey,
        _stale_after: Duration,
    ) -> BoxFuture<'a, Result<Option<Box<dyn JobClaim>>, StorageError>> {
        Box::pin(async move {
            // The slot is the liveness test (module doc): a job of this
            // process that holds it is alive whatever its heartbeat says, and
            // a row without a holder is dead at once, so the stale window is
            // not read. Taking the slot is the test and the exclusion of a
            // second taker in one step.
            let Some(slot) = JobSlots::take(&self.job_slots, backup_slot_key(&key.backup_id))
            else {
                return Ok(None);
            };
            let generation = new_generation();
            let _writer = self.write_lock.lock().await;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(db_err)?;
            // `heartbeat_at` stays as the dead owner wrote it (trait doc).
            let taken = sqlx::query(
                "UPDATE backup_index SET owner_generation = ? \
                 WHERE account_id = ? AND backup_id = ? AND status = 'CREATING'",
            )
            .bind(generation_column(generation))
            .bind(&key.account_id)
            .bind(&key.backup_id)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?
            .rows_affected();
            tx.commit().await.map_err(db_err)?;
            if taken != 1 {
                drop(slot);
                return Ok(None);
            }
            Ok(Some(Box::new(SqliteJobClaim {
                engine: self.clone(),
                _slot: Some(slot),
                _permit: None,
                generation,
                record: JobRecord::Backup(key.clone()),
            }) as Box<dyn JobClaim>))
        })
    }

    fn list_backup_rows_older_than<'a>(
        &'a self,
        status: BackupStatus,
        store: &'a StoreIdentity,
        clock: StaleClock,
        age: Duration,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        let column = match clock {
            StaleClock::Heartbeat => "heartbeat_at",
            StaleClock::Completed => "completed_at",
        };
        rows_stream(async move {
            // On the database's clock, as `remove_dead_creating_backup_row`
            // measures; `julianday(NULL)` is NULL, so a row with
            // `completed_at` unset is never older than any age on that clock.
            // A zero window selects by status alone (`zero_window_clause`).
            sqlx::query_as(&format!(
                "SELECT {ROW_COLUMNS} FROM backup_index \
                 WHERE store_kind = ? AND store_root = ? AND store_id = ? AND status = ? \
                 AND {} \
                 ORDER BY created_at, backup_arn",
                zero_window_clause(age, column)
            ))
            .bind(store.store_kind.as_str())
            .bind(&store.store_root)
            .bind(&store.store_id)
            .bind(status.as_str())
            .bind(age.as_secs_f64())
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)
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
            // A store-backed restore's progress lives on the `table_restores`
            // row (`file_index`, `items_applied`); SQLite has
            // no `restore_progress` table (migration 7 is `NotApplicable`
            // here), so the record goes with the target below and there are
            // no rows to delete.
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
            // are measured on the database's clock (`julianday('now')`); a
            // zero window selects by status alone (`zero_window_clause`).
            let rows: Vec<RestoreJobTuple> = sqlx::query_as(&format!(
                "SELECT {RESTORE_ROW_COLUMNS} \
                 FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
                 WHERE (r.status = 'RUNNING' AND {}) \
                    OR (r.status = 'FAILED' AND t.table_status = 'CREATING' \
                        AND {}) \
                 ORDER BY t.account_id, t.table_name",
                zero_window_clause(stale_after, "r.heartbeat_at"),
                zero_window_clause(stale_after, "COALESCE(r.completed_at, r.heartbeat_at)"),
            ))
            .bind(stale_after.as_secs_f64())
            .bind(stale_after.as_secs_f64())
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
            // A RUNNING record whose in-process slot a task holds is skipped,
            // however old its heartbeat: one process serves the file, so the
            // slot is the liveness rule and the heartbeat only what a restart
            // ages (design 4(e)); an in-catalog restore holds its table id's
            // slot, a store-backed one its target name's. A row this build
            // cannot read (a kind or status no release writes) is logged and
            // skipped, as ListBackups skips a row it cannot read, so one such
            // row does not stop the cycle.
            Ok(rows
                .into_iter()
                .filter_map(|row| {
                    let (table_id, account_id, table_name, status) =
                        (row.0.clone(), row.1.clone(), row.2.clone(), row.5.clone());
                    if status == JobStatus::Running.as_str()
                        && (self.job_slots.holds(&legacy_restore_slot_key(&table_id))
                            || self
                                .job_slots
                                .holds(&restore_slot_key(&account_id, &table_name)))
                    {
                        return None;
                    }
                    match restore_job_from_tuple(row) {
                        Ok(job) => Some(job),
                        Err(e) => {
                            tracing::warn!(
                                "reconciler: restore job record of {table_name} (account \
                                 {account_id}) skipped: {e}"
                            );
                            None
                        }
                    }
                })
                .collect())
        })
    }

    fn list_running_restore_jobs(&self) -> BoxFuture<'_, Result<Vec<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            // By status alone: a record stamped in this statement's instant,
            // or ahead of the database's clock, is RUNNING too.
            let rows: Vec<RestoreJobTuple> = sqlx::query_as(&format!(
                "SELECT {RESTORE_ROW_COLUMNS} \
                 FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
                 WHERE r.status = 'RUNNING' ORDER BY t.account_id, t.table_name"
            ))
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
            rows.into_iter().map(restore_job_from_tuple).collect()
        })
    }

    fn claim_restore<'a>(
        &'a self,
        account_id: &'a str,
        input: CreateTableInput,
        registration: &'a RestoreRegistrationRow,
    ) -> BoxFuture<'a, Result<RestoreRegistered, StorageError>> {
        Box::pin(async move {
            let table_name = input.table_name.clone();
            // The name's slot first, before the target exists (module doc):
            // a slot already held is another store-backed restore into this
            // name in flight in this process. It is refused as the name is
            // taken: TableBeingRestored naming its source once its CREATING
            // row is visible, else as CreateTable would report it.
            let Some(slot) =
                JobSlots::take(&self.job_slots, restore_slot_key(account_id, &table_name))
            else {
                return Err(self.name_taken_error(account_id, &table_name).await);
            };
            let generation = new_generation();
            let registration = RestoreRegistrationRow {
                owner_generation: generation,
                ..registration.clone()
            };
            // The registration transaction (`BEGIN IMMEDIATE` under the
            // engine write lock): the `backup_index` row's status, the
            // in-use check, the CREATING target and its RUNNING record.
            let mut desc = match self
                .create_table_for_manifest_restore(account_id, input, &registration)
                .await
            {
                Ok(desc) => desc,
                Err(e) => {
                    drop(slot);
                    return Err(e);
                }
            };
            desc.restore_summary = Some(RestoreSummary {
                source_backup_arn: Some(registration.backup.backup_arn.clone()),
                source_table_arn: Some(registration.source_table_arn.clone()),
                restore_date_time: registration.restore_date_time,
                restore_in_progress: true,
            });
            let claim =
                SqliteJobClaim::for_manifest_restore(self, &desc.table_id, generation, slot);
            Ok((desc, Box::new(claim) as Box<dyn JobClaim>))
        })
    }

    fn claim_restore_dead<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
        _stale_after: Duration,
        spool_host: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Option<Box<dyn JobClaim>>, StorageError>> {
        Box::pin(async move {
            // The slot is the liveness test, as for `claim_dead_backup`: a
            // job of this process that holds the name's slot is alive
            // whatever its heartbeat says, and a record without a holder is
            // dead at once (a restart frees every slot, and ages every
            // RUNNING record so the reconciler lists it). Taking the slot is
            // the test and the exclusion of a second taker in one step.
            let Some(slot) = JobSlots::take(
                &self.job_slots,
                restore_slot_key(&key.account_id, &key.table_name),
            ) else {
                return Ok(None);
            };
            let generation = new_generation();
            let _writer = self.write_lock.lock().await;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(db_err)?;
            // The generation, the claimant's spool and a fresh heartbeat in
            // one write (the trait doc says why a restore record is refreshed
            // where a backup row is not; here the slot already excludes a
            // second taker of this process, and the beat keeps the record
            // out of the reconciler's dead listing). A legacy record is
            // never taken over: the reconciler fails and removes it.
            let taken = sqlx::query(&format!(
                "UPDATE table_restores SET owner_generation = ?, spool_host = ?, \
                 heartbeat_at = {NOW} \
                 WHERE table_id = ? AND status = 'RUNNING' AND kind = 'manifest' \
                 AND EXISTS(SELECT 1 FROM tables t WHERE t.table_id = table_restores.table_id \
                 AND t.account_id = ? AND t.table_name = ?)"
            ))
            .bind(generation_column(generation))
            .bind(spool_host)
            .bind(&key.table_id)
            .bind(&key.account_id)
            .bind(&key.table_name)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?
            .rows_affected();
            tx.commit().await.map_err(db_err)?;
            if taken != 1 {
                drop(slot);
                return Ok(None);
            }
            Ok(Some(Box::new(SqliteJobClaim::for_manifest_restore(
                self,
                &key.table_id,
                generation,
                slot,
            )) as Box<dyn JobClaim>))
        })
    }

    fn read_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<Option<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            let row: Option<RestoreJobTuple> = sqlx::query_as(&format!(
                "SELECT {RESTORE_ROW_COLUMNS} \
                 FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
                 WHERE r.table_id = ? AND t.account_id = ? AND t.table_name = ?"
            ))
            .bind(&key.table_id)
            .bind(&key.account_id)
            .bind(&key.table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
            row.map(restore_job_from_tuple).transpose()
        })
    }

    fn list_restore_jobs<'a>(
        &'a self,
        status: Option<JobStatus>,
    ) -> BoxFuture<'a, Result<Vec<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            let rows: Vec<RestoreJobTuple> = sqlx::query_as(&format!(
                "SELECT {RESTORE_ROW_COLUMNS} \
                 FROM table_restores r JOIN tables t ON t.table_id = r.table_id \
                 WHERE (?1 IS NULL OR r.status = ?1) \
                 ORDER BY t.account_id, t.table_name"
            ))
            .bind(status.map(JobStatus::as_str))
            .fetch_all(&self.pool)
            .await
            .map_err(db_err)?;
            rows.into_iter().map(restore_job_from_tuple).collect()
        })
    }

    /// Conditioned on RUNNING, on the listed `owner_generation` and on the
    /// listed `heartbeat_at` (compared as instants through `julianday`, so
    /// the text shape of the stored value does not matter), and written with
    /// a fresh generation (design 4(e)). Under the engine write lock, as every
    /// writer is.
    fn fail_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
        owner_generation: u64,
        heartbeat_at: f64,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let _writer = self.write_lock.lock().await;
            #[allow(clippy::cast_possible_truncation)] // epoch millis fit i64
            let listed_heartbeat =
                crate::sqlite_util::format_epoch_millis((heartbeat_at * 1000.0).round() as i64);
            let affected = sqlx::query(&format!(
                "UPDATE table_restores SET status = 'FAILED', failure_reason = ?, \
                 completed_at = {NOW}, owner_generation = ? \
                 WHERE table_id = ? AND status = 'RUNNING' AND owner_generation = ? \
                 AND julianday(heartbeat_at) = julianday(?) AND EXISTS(\
                 SELECT 1 FROM tables t WHERE t.table_id = table_restores.table_id \
                 AND t.account_id = ? AND t.table_name = ?)"
            ))
            .bind(reason)
            .bind(generation_column(new_generation()))
            .bind(&key.table_id)
            .bind(generation_column(owner_generation))
            .bind(listed_heartbeat)
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
            // with a live heartbeat exists, taken over from a dead one. An
            // engine that holds the serve lock (a command that opened the
            // file as `serve` does) is the only process on the file, so the
            // holder of any row is dead whatever its heartbeat says, as the
            // server's startup decides (`clear_maintenance_claim`): the row
            // is taken over.
            let _writer = self.write_lock.lock().await;
            let live: Option<String> = if self.serve_lock.is_some() {
                None
            } else {
                sqlx::query_scalar(
                    "SELECT holder FROM backup_maintenance \
                     WHERE julianday(heartbeat_at) >= julianday('now') - ? / 86400.0",
                )
                .bind(stale_after.as_secs_f64())
                .fetch_optional(&self.pool)
                .await
                .map_err(db_err)?
            };
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

/// The job under the engine write lock (the maintainer's blocking item on
/// PR 8): a writer that holds the lock past two heartbeat periods delays the
/// beat and the claim re-check and ends no job, and a put that comes while
/// the beat is parked on the lock is not refused.
#[cfg(test)]
mod write_lock_tests {
    use std::os::unix::fs::DirBuilderExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use extenddb_backup_store::{BackupStore, ByteStream, FilesystemStore, ObjectMeta, StoreError};
    use extenddb_core::backup_format::BackupArn;
    use extenddb_core::limits::LimitsConfig;
    use extenddb_engine::{
        BackupRuntime, ConfiguredStore, JobSettings, OperationContext, check_configured_store,
        dispatch,
    };
    use extenddb_storage::backup_catalog::{BackupCatalog, BackupStatus, StoreKind};
    use futures::future::BoxFuture;
    use futures::stream::BoxStream;
    use serde_json::json;

    use crate::store::SqliteEngine;

    const ACCOUNT: &str = "123456789012";

    /// A scratch directory under the cargo target directory, removed on
    /// drop with the database files beside it.
    struct Scratch(std::path::PathBuf);

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn call(
        ctx: &OperationContext,
        operation: &str,
        body: serde_json::Value,
    ) -> serde_json::Value {
        dispatch(operation, body, ctx, "127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("{operation}: {e:?}"))
            .body
    }

    /// A store whose first `put` waits `delay` before it starts, counting
    /// the puts started: the job's second data file then comes after the
    /// delay, while the test still holds the write lock.
    struct SlowFirstPut {
        inner: Arc<dyn BackupStore>,
        delay: Duration,
        puts_started: AtomicUsize,
    }

    impl BackupStore for SlowFirstPut {
        fn put<'a>(
            &'a self,
            key: &'a str,
            body: ByteStream,
        ) -> BoxFuture<'a, Result<ObjectMeta, StoreError>> {
            Box::pin(async move {
                if self.puts_started.fetch_add(1, Ordering::AcqRel) == 0 {
                    tokio::time::sleep(self.delay).await;
                }
                self.inner.put(key, body).await
            })
        }

        fn put_if_absent<'a>(
            &'a self,
            key: &'a str,
            body: ByteStream,
        ) -> BoxFuture<'a, Result<Option<ObjectMeta>, StoreError>> {
            self.inner.put_if_absent(key, body)
        }

        fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<ByteStream, StoreError>> {
            self.inner.get(key)
        }

        fn head<'a>(
            &'a self,
            key: &'a str,
        ) -> BoxFuture<'a, Result<Option<ObjectMeta>, StoreError>> {
            self.inner.head(key)
        }

        fn list<'a>(&'a self, prefix: &'a str) -> BoxStream<'a, Result<ObjectMeta, StoreError>> {
            self.inner.list(prefix)
        }

        fn delete_prefix<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, Result<u64, StoreError>> {
            self.inner.delete_prefix(prefix)
        }

        fn validate(&self) -> BoxFuture<'_, Result<(), StoreError>> {
            self.inner.validate()
        }
    }

    /// A file-backed engine with a healthy filesystem store under a scratch
    /// root, a runtime with `settings`, and a `Music` table of three items.
    struct Fixture {
        _scratch: Scratch,
        engine: SqliteEngine,
        root: std::path::PathBuf,
        runtime: Arc<BackupRuntime>,
        ctx: OperationContext,
    }

    async fn fixture(
        settings: JobSettings,
        wrap: impl FnOnce(Arc<dyn BackupStore>) -> Arc<dyn BackupStore>,
    ) -> Fixture {
        let target = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target")
            });
        let scratch = Scratch(target.join(format!("backup-write-lock-{}", uuid::Uuid::new_v4())));
        std::fs::create_dir_all(&scratch.0).expect("scratch directory");
        let db_path = scratch.0.join("catalog.sqlite");
        let root = scratch.0.join("backups");
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&root)
            .expect("root");
        let engine = SqliteEngine::new(
            db_path.to_str().expect("UTF-8 path"),
            4,
            "us-east-1",
            409_600,
        )
        .await
        .expect("file engine");
        crate::migrations::apply_for_tests(&engine.pool).await;
        for (key, value) in [
            ("control_plane_delay_seconds", "0"),
            ("index_propagation_delay_ms", "0"),
        ] {
            sqlx::query(
                "INSERT INTO settings (key, value) VALUES (?, ?) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            )
            .bind(key)
            .bind(value)
            .execute(&engine.pool)
            .await
            .expect("setting");
        }
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
            .bind(ACCOUNT)
            .execute(&engine.pool)
            .await
            .expect("account");
        let storage: Arc<dyn extenddb_storage::StorageEngine> = Arc::new(engine.clone());

        let store: Arc<dyn BackupStore> = Arc::new(
            FilesystemStore::open(&root)
                .await
                .expect("filesystem store"),
        );
        let configured = ConfiguredStore::checking(
            StoreKind::Filesystem,
            root.to_str().expect("UTF-8 path").to_owned(),
            wrap(store),
            None,
        );
        let outcome = check_configured_store(&configured, &*storage, "sqlite").await;
        assert!(outcome.is_ok(), "the store's first check: {outcome:?}");
        configured.record_check(outcome);
        let runtime = Arc::new(
            BackupRuntime::new(4)
                .with_job_settings(settings)
                .with_store(configured)
                .with_source("sqlite", "test"),
        );
        let ctx = OperationContext {
            storage: storage.clone(),
            backup: runtime.clone(),
            limits: Arc::new(LimitsConfig::default()),
            region: Arc::from("us-east-1"),
            account_id: Arc::from(ACCOUNT),
            import_paths: Arc::from(Vec::new()),
            export_paths: Arc::from(Vec::new()),
            authorized_key_info: Default::default(),
            auth_cache: extenddb_auth::AuthCacheRegistry::empty(),
            table_key_info_lookup: None,
        };
        call(
            &ctx,
            "CreateTable",
            json!({
                "TableName": "Music",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        )
        .await;
        for i in 0..3 {
            call(
                &ctx,
                "PutItem",
                json!({"TableName": "Music", "Item": {"pk": {"S": format!("k{i}")}}}),
            )
            .await;
        }
        Fixture {
            _scratch: scratch,
            engine,
            root,
            runtime,
            ctx,
        }
    }

    impl Fixture {
        /// CreateBackup of `Music`, answered CREATING; the backup's ARN.
        async fn create_backup(&self) -> BackupArn {
            let details = call(
                &self.ctx,
                "CreateBackup",
                json!({"TableName": "Music", "BackupName": "nightly"}),
            )
            .await;
            assert_eq!(
                details["BackupDetails"]["BackupStatus"], "CREATING",
                "{details}"
            );
            BackupArn::parse(
                details["BackupDetails"]["BackupArn"]
                    .as_str()
                    .expect("an ARN"),
            )
            .expect("a backup ARN")
        }

        /// Wait for the job to end, then read its row.
        async fn ended_row(&self, arn: &BackupArn) -> extenddb_storage::backup_catalog::BackupRow {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while self.runtime.running_jobs() > 0 {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the job did not end after the lock was released"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            self.engine
                .read_backup_row(arn)
                .await
                .expect("read the row")
                .expect("the row exists")
        }
    }

    /// With a heartbeat period of 300 ms, the test takes the engine write
    /// lock the moment CreateBackup answers CREATING and holds it for three
    /// periods. The timer's first beat and the job's re-check before the
    /// manifest queue behind it; their bound starts once they hold the lock
    /// (`heartbeat_within`), so both confirm when the lock is released and
    /// the backup ends AVAILABLE with its manifest in the store. With the
    /// bound over the whole call the first beat ends the job as a lost claim
    /// ("the heartbeat did not return within 0 s") and the row stays
    /// CREATING.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_writer_holding_the_lock_past_two_periods_delays_the_beat_and_ends_no_job() {
        let period = Duration::from_millis(300);
        let f = fixture(
            JobSettings {
                heartbeat_interval: period,
                ..JobSettings::default()
            },
            |store| store,
        )
        .await;
        let arn = f.create_backup().await;
        // The writer: the lock taken as the row exists, held for three
        // periods, past the first beat's tick and its whole-call bound.
        let held = f.engine.write_lock.lock().await;
        tokio::time::sleep(3 * period).await;
        assert_eq!(f.runtime.running_jobs(), 1, "the job waits on the lock");
        drop(held);

        let row = f.ended_row(&arn).await;
        assert_eq!(
            (row.status, row.failure_reason.clone()),
            (BackupStatus::Available, None),
            "{row:?}"
        );
        assert_eq!(row.item_count, 3);
        assert!(
            f.root
                .join(arn.store_prefix())
                .join("extenddb-manifest.json")
                .is_file(),
            "the manifest is in the store"
        );
        assert!(
            row.heartbeat_at > row.created_at,
            "a beat landed after the insert ({} > {})",
            row.heartbeat_at,
            row.created_at
        );
        f.engine.pool.close().await;
    }

    /// The maintainer's second-round case: a put that comes while the beat
    /// is parked on the lock. A data file per item (`data_file_target_bytes`
    /// of one byte) and a store whose first put takes four periods: the
    /// first data file's check passes on the claim's insert, the timer's
    /// first beat parks on the lock the test took as the row appeared, and
    /// the second data file's check comes four periods after the insert
    /// with no beat confirmed since. The check passes on the beat in flight
    /// (`confirms_the_claim`'s third clause), the second put starts under
    /// the hold, and once the lock is released the beat lands and the backup
    /// ends AVAILABLE with its three data files and its manifest. With the
    /// in-flight clause removed the second data file's check refuses the
    /// put ("no heartbeat confirmed the claim recently enough for data file
    /// 2") and the job stops: this cell then sees one put started and the
    /// row left CREATING, since no reconciler runs here (on a server the
    /// reconciler fails the row and deletes its prefix, which is what the
    /// maintainer measured at the third or fourth data file beside a GSI
    /// build).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_put_during_a_writers_hold_passes_on_the_beat_parked_behind_it() {
        let period = Duration::from_millis(300);
        let slow = Arc::new(std::sync::Mutex::new(None::<Arc<SlowFirstPut>>));
        let f = fixture(
            JobSettings {
                heartbeat_interval: period,
                data_file_target_bytes: 1,
                ..JobSettings::default()
            },
            |store| {
                let wrapped = Arc::new(SlowFirstPut {
                    inner: store,
                    delay: 4 * period,
                    puts_started: AtomicUsize::new(0),
                });
                *slow.lock().unwrap() = Some(Arc::clone(&wrapped));
                wrapped
            },
        )
        .await;
        let slow = slow.lock().unwrap().take().expect("the wrapped store");
        let arn = f.create_backup().await;
        // The writer: the lock taken as the row exists, held until the
        // second data file's put has started, which needs the pre-put check
        // to pass with the first beat parked behind this hold.
        let held = f.engine.write_lock.lock().await;
        let at_the_hold = f
            .engine
            .read_backup_row(&arn)
            .await
            .expect("read the row")
            .expect("the row exists");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while slow.puts_started.load(Ordering::Acquire) < 2 {
            if std::time::Instant::now() >= deadline || f.runtime.running_jobs() == 0 {
                drop(held);
                let row = f.ended_row(&arn).await;
                panic!(
                    "the second data file was not put under the hold: {} put(s) started, row {row:?}",
                    slow.puts_started.load(Ordering::Acquire)
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let parked = f
            .engine
            .read_backup_row(&arn)
            .await
            .expect("read the row")
            .expect("the row exists");
        assert_eq!(
            parked.heartbeat_at, at_the_hold.heartbeat_at,
            "no beat landed under the hold: the first beat is parked on the lock"
        );
        assert_eq!(parked.status, BackupStatus::Creating);
        drop(held);

        let row = f.ended_row(&arn).await;
        assert_eq!(
            (row.status, row.failure_reason.clone()),
            (BackupStatus::Available, None),
            "{row:?}"
        );
        assert_eq!(row.item_count, 3);
        let prefix = f.root.join(arn.store_prefix());
        for name in [
            "data/000001.json.gz",
            "data/000002.json.gz",
            "data/000003.json.gz",
            "extenddb-manifest.json",
        ] {
            assert!(prefix.join(name).is_file(), "{name} is in the store");
        }
        assert!(
            row.heartbeat_at > at_the_hold.heartbeat_at,
            "the parked beat landed once the lock was released ({} > {})",
            row.heartbeat_at,
            at_the_hold.heartbeat_at
        );
        f.engine.pool.close().await;
    }
}
