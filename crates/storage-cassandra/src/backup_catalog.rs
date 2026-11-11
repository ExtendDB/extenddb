// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The catalog index on Cassandra: the index rows in the reused
//! `backups_by_arn`, `backups_by_account` and `backups_by_table` tables, the
//! status-keyed `backup_index_by_status`, the restore job record on the
//! target's `tables` row, `backup_maintenance` and `settings.catalog_id`.
//!
//! An index row is a `backups_by_arn` row with `store_kind` set; a row without
//! it is an in-catalog backup of the legacy reader (`backup_engine.rs`), and
//! each path skips the other's rows. `backups_by_arn`, partitioned by
//! `(account_id, backup_arn)`, is authoritative and is written first by every
//! writer; the list rows and the `backup_index_by_status` row follow, and a
//! reader of those re-reads the authoritative row and skips an entry it no
//! longer matches, so a writer that dies between the writes leaves nothing a
//! reader believes. An index row's list rows exist for as long as the row
//! does, of whatever status (the legacy create writes its list rows only when
//! its row becomes AVAILABLE); `remove_backup_row` takes them with the row.
//!
//! The claim is the row: `claim_backup` inserts it `IF NOT EXISTS` with a
//! fresh `owner_generation` and `heartbeat_at = toTimestamp(now())`, every
//! later write by the job is a lightweight transaction `IF owner_generation =
//! <generation>`, and a heartbeat that does not apply means another owner has
//! the row. Times are the coordinator's clock; staleness is measured against
//! it (`coordinator_now_millis`), never an application host's. A dead
//! owner's row (`claim_dead_backup`) is taken over by an LWT on the
//! generation, the status and the heartbeat as read at `LOCAL_SERIAL`, the
//! heartbeat left as the owner wrote it. `sweep_restore_registrations`
//! removes, entry by entry under an LWT, the `active_restores` entries whose
//! target is no longer a CREATING restore target of the backup.
//!
//! DeleteBackup's lock is the lightweight transaction on the `backups_by_arn`
//! row, `IF backup_status = 'AVAILABLE' AND active_restores = null`, on the
//! same partition a restore registers on, so the two order wholly one before
//! the other, as the in-catalog paths do. The condition also carries the
//! store kind and root the delete read, as does `set_backup_deleted`'s
//! `IF backup_status = 'DELETING'`: each moves the row's status entry under
//! the root it read, so a row `rewrite_store` moved after the read fails the
//! condition, is read again, and gets its entry under the root it names.
//!
//! A status change moves the row's `backup_index_by_status` entry with a
//! plain batch after the lightweight transaction on the row. A process that
//! dies between the two, or a batch that fails (it is logged, not sent
//! again), leaves the row's only entry under the earlier status; every
//! reader of one status partition re-reads the row and skips such an entry,
//! one wasted read each, so `list_backup_rows` does not list that row until
//! its entry is repaired. `list_backup_rows_by_root` reads all five
//! partitions of the root and lists the row under its own status.
//! `remove_backup_row` deletes the entries under the status it read, so an
//! entry under an earlier status stays.
//! Three paths leave such entries without a crash or a failed batch:
//! - `insert_backup_row`'s row is AVAILABLE from its lightweight transaction
//!   on, so a DeleteBackup between that transaction and the insert's entry
//!   batch moves a status entry the batch has not written yet, and the batch
//!   then writes an AVAILABLE entry that no later write removes (an insert
//!   completed by a later sync has the same window).
//! - A deleter (`start_backup_delete`'s DELETING, `set_backup_deleted`'s
//!   DELETED) whose lightweight transaction lands before a `rewrite_store`
//!   of the row and whose entry batch lands after it writes the new status's
//!   entry under the old root, where no row stands behind it.
//! - A `rewrite_store` whose answer was lost and whose row changed status
//!   before the retry leaves the new root's entry under the earlier status.
//!
//! Every reader skips them. Plain sync's report of other stores verifies
//! the summary's counts against the rows on this backend
//! (`summary_counts_status_entries`, through `list_backup_rows_by_root`)
//! and names the counts no row of that status stands behind as status
//! entries, and `backup sync --forget-store <root>`, under the maintenance
//! claim, deletes the root's entries whose row is gone or names another
//! store, and repairs those whose row is of the root under another status
//! (`remove_orphan_status_entries`). The second case
//! cannot arise for the tombstone removal: `remove_backup_row`'s
//! transaction is conditioned on the store it read.
//!
//! `rewrite_store` (`backup sync --move-store`) is the one writer that
//! writes an entry before its row: per row it writes the new root's status
//! entry, then the lightweight transaction that rewrites the row's kind,
//! root and id (conditioned on the old kind and root and the status it
//! read), then deletes the old root's entry. With the row first, a stop
//! before the new entry would leave a moved row that no listing of the
//! configured store returns, so the reconciler, sync's row read and the
//! manifest probe would not see it. With the entry first, a stop before the
//! transaction leaves an entry under the new root whose row still names the
//! old root, which every reader skips, and the row still listed under the
//! old root, so the move runs again; a stop after it leaves the old root's
//! entry, which readers skip and `read_store_ids` of the old root counts,
//! until `--forget-store` of the old root deletes it. A transaction that does not
//! apply (another writer changed the row after the read) is followed by a
//! serial read of the row: one that already names the new store keeps the
//! new entry and counts as moved; one of the new root under another store
//! id with the status read (another move to that root, whose entry has the
//! new entry's key) has that entry written again with its own store id and
//! is not counted; and only otherwise is the new entry removed.
//!
//! A conditional write whose error is the driver's `Io` or `Timeout`, or the
//! server's `WriteTimeout` or `WriteFailure`, has an unknown outcome
//! (`WriteOutcome::Unknown`: the Paxos round may still commit); every other
//! error is `Err`.
//!
//! The maintenance claim is the one row of `backup_maintenance`, taken with
//! `INSERT ... IF NOT EXISTS`; when a row exists, one whose heartbeat is older
//! than the stale window on the coordinator's clock is taken over with
//! `IF owner_generation = <old>`, so two commands racing for a dead holder's
//! row see one win. An insert or a takeover whose answer was lost is settled
//! by a re-read at `LOCAL_SERIAL`: a row carrying the call's own generation is
//! its claim. Heartbeats and the
//! release are lightweight transactions on the generation. The two reads of
//! the claim row that decide whether another process's claim is live
//! (`check_maintenance_claim`, the row before a takeover) run at
//! `LOCAL_SERIAL`, so a heartbeat an LWT just wrote is seen; the RUNNING
//! restore records `list_dead_restore_jobs` scans are a range read, where a
//! serial level is a local quorum with no Paxos read, so the scan reads at
//! quorum and the coordinator clock after it.

use crate::cassandra_util::PolicySession as _;
use crate::cassandra_util::Stmt;
use std::time::Duration;

use cdrs_tokio::error::Error as DriverError;
use cdrs_tokio::frame::message_error::ErrorType;
use cdrs_tokio::query::QueryValues;
use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::rows::Row;
use cdrs_tokio::types::value::Value;
use extenddb_core::backup_format::BackupArn;
use extenddb_core::types::CreateTableInput;
use extenddb_storage::backup_catalog::{
    BackupCatalog, BackupKey, BackupRow, BackupStatus, DeleteStart, ForgetRow, Heartbeat, JobClaim,
    JobRow, JobStatus, MaintenanceClaim, RestoreJobRow, RestoreKind, RestoreRegistered,
    RestoreRegistrationRow, RestoreTargetKey, StaleClock, StatusEntrySweep, StatusFields,
    StoreIdentity, StoreKind, StoreRootCount, TimeBounds, UNOWNED_GENERATION, WriteOutcome,
    billing_mode_name, new_generation,
};
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::CassandraEngine;
use crate::engine::CassandraSession;

/// Attempts `start_backup_delete` and `forget_available_backup_row` make
/// when a restore registers or unregisters between their in-use check and
/// their write.
const DELETE_CLAIM_ATTEMPTS: usize = 3;

/// The condition of the DELETING write and of the outright removal: the row
/// is AVAILABLE, no restore is registered on it, and it still names the
/// store (kind and root) the writer read, bound after the key. With the
/// store in the condition, a row `rewrite_store` moved after the read is not
/// matched, and the re-read moves its status entry under the root it now
/// names, not the one read.
const DELETE_CLAIM_CONDITION: &str = "IF backup_status = 'AVAILABLE' AND active_restores = null \
     AND store_kind = ? AND store_root = ?";

/// The `owner_generation` column value of a generation: the column is a
/// signed 64-bit integer and `new_generation` never exceeds `i64::MAX`.
pub(crate) fn generation_column(generation: u64) -> i64 {
    i64::try_from(generation).unwrap_or(i64::MAX)
}

/// The generation a column holds; a negative value no release writes reads
/// as the unowned sentinel.
fn generation_from_column(column: Option<i64>) -> u64 {
    column
        .and_then(|g| u64::try_from(g).ok())
        .unwrap_or_default()
}

fn not_found(backup_arn: &str) -> StorageError {
    extenddb_storage::backup_catalog::backup_not_found(backup_arn)
}

fn millis_to_seconds(ms: i64) -> f64 {
    crate::cassandra_util::millis_to_seconds_f64(ms)
}

#[allow(clippy::cast_possible_truncation)]
fn seconds_to_millis(seconds: f64) -> i64 {
    (seconds * 1_000.0).round() as i64
}

/// The coordinator's clock, in epoch milliseconds: the clock every heartbeat
/// is stamped with, so a stale window is measured on it.
pub(crate) async fn coordinator_now_millis(engine: &CassandraEngine) -> Result<i64, StorageError> {
    let rows = crate::cassandra_util::system_read::<StorageError>(
        &engine.session,
        "SELECT toTimestamp(now()) AS now FROM system.local",
        cdrs_tokio::query_values!(),
        "coordinator clock",
    )
    .await?;
    let row = rows
        .first()
        .ok_or_else(|| StorageError::Internal("coordinator clock: no row".to_owned()))?;
    crate::cassandra_util::get_column(row, "now", "coordinator clock")
}

/// Refresh `heartbeat_at` of a `backups_by_arn` row on the coordinator's
/// clock, conditioned on the claim's generation; `false` when another owner
/// holds the row. Shared by the index claim and the in-catalog create.
#[doc(hidden)]
pub async fn heartbeat_backup_row(
    engine: &CassandraEngine,
    account_id: &str,
    backup_arn: &str,
    generation: u64,
) -> Result<bool, StorageError> {
    let query = format!(
        "UPDATE {}.backups_by_arn SET heartbeat_at = toTimestamp(now()) \
         WHERE account_id = ? AND backup_arn = ? IF owner_generation = ?",
        engine.catalog_keyspace()
    );
    let result = crate::cassandra_util::query_lwt(
        &engine.session,
        &query,
        cdrs_tokio::query_values!(account_id, backup_arn, generation_column(generation)),
    )
    .await?;
    crate::cassandra_util::lwt_applied(&result)
}

/// Whether a write's outcome cannot be told from its error: the statement
/// may have reached a replica before the answer was lost. A write timeout
/// or failure on a lightweight transaction is the Paxos round not completing
/// within the coordinator's view; the proposal may still commit.
fn outcome_unknown(error: &DriverError) -> bool {
    match error {
        DriverError::Io(_) | DriverError::Timeout(_) => true,
        DriverError::Server { body, .. } => matches!(
            body.ty,
            ErrorType::WriteTimeout(_) | ErrorType::WriteFailure(_)
        ),
        _ => false,
    }
}

/// Run a conditional write and report what it did.
pub(crate) async fn conditional_write(
    session: &CassandraSession,
    query: &str,
    values: QueryValues,
) -> Result<WriteOutcome, StorageError> {
    match session.run(Stmt::Lwt, query, values).await {
        Ok(result) => Ok(if crate::cassandra_util::lwt_applied(&result)? {
            WriteOutcome::Matched
        } else {
            WriteOutcome::NotMatched
        }),
        Err(error) if outcome_unknown(&error) => {
            tracing::warn!("conditional catalog write with an unknown outcome: {error}");
            Ok(WriteOutcome::Unknown)
        }
        Err(error) => Err(StorageError::Internal(format!("LWT query failed: {error}"))),
    }
}

/// The index row a `backups_by_arn` row carries, or `None` for an in-catalog
/// backup (no `store_kind`).
fn index_row_from_row(row: &Row) -> Result<Option<BackupRow>, StorageError> {
    let store_kind: Option<String> = row.get_by_name("store_kind").ok().flatten();
    let Some(store_kind) = store_kind else {
        return Ok(None);
    };
    let column = |name: &str| -> Result<String, StorageError> {
        crate::cassandra_util::get_column(row, name, "backup index row")
    };
    let optional = |name: &str| -> Option<String> { row.get_by_name(name).ok().flatten() };
    let backup_arn = column("backup_arn")?;
    let store_kind = StoreKind::parse(&store_kind).ok_or_else(|| {
        StorageError::Internal(format!(
            "backup {backup_arn} carries an unknown store kind {store_kind:?}"
        ))
    })?;
    let status: String = column("backup_status")?;
    let backup_id = match optional("backup_id") {
        Some(id) => id,
        None => BackupArn::parse(&backup_arn)
            .map(|arn| arn.backup_id().to_owned())
            .map_err(|e| {
                StorageError::Internal(format!("backup {backup_arn} carries no backup id: {e}"))
            })?,
    };
    let source_table_details_json = match optional("source_table_details_json") {
        Some(text) => serde_json::from_str(&text).map_err(|e| {
            StorageError::Internal(format!(
                "backup {backup_arn} carries a source table this build cannot read: {e}"
            ))
        })?,
        None => serde_json::Value::Null,
    };
    let created_at: i64 = crate::cassandra_util::get_column(row, "created_at", "backup index row")?;
    let completed_at: Option<i64> = row.get_by_name("completed_at").ok().flatten();
    let heartbeat_at: Option<i64> = row.get_by_name("heartbeat_at").ok().flatten();
    let owner_generation: Option<i64> = row.get_by_name("owner_generation").ok().flatten();
    Ok(Some(BackupRow {
        backup_id,
        account_id: column("account_id")?,
        table_name: column("table_name")?,
        backup_arn,
        backup_name: column("backup_name")?,
        status: BackupStatus::parse(&status)?,
        store: StoreIdentity {
            store_kind,
            store_root: column("store_root")?,
            store_id: column("store_id")?,
        },
        size_bytes: row
            .get_by_name("backup_size_bytes")
            .ok()
            .flatten()
            .unwrap_or(0),
        item_count: row.get_by_name("item_count").ok().flatten().unwrap_or(0),
        created_at: millis_to_seconds(created_at),
        completed_at: completed_at.map(millis_to_seconds),
        failure_reason: optional("failure_reason"),
        source_table_details_json,
        owner_generation: generation_from_column(owner_generation),
        heartbeat_at: heartbeat_at.map_or(0.0, millis_to_seconds),
    }))
}

/// Whether `read`, the row a serial read found, is the row
/// `insert_backup_row(row)` writes: unowned, with every column the insert
/// takes from `row` equal (`created_at` to the stored millisecond).
fn is_unowned_insert_of(read: &BackupRow, row: &BackupRow) -> bool {
    read.owner_generation == UNOWNED_GENERATION
        && read.backup_id == row.backup_id
        && read.table_name == row.table_name
        && read.backup_name == row.backup_name
        && read.status == row.status
        && read.store == row.store
        && read.size_bytes == row.size_bytes
        && read.item_count == row.item_count
        && seconds_to_millis(read.created_at) == seconds_to_millis(row.created_at)
        && read.failure_reason == row.failure_reason
        && read.source_table_details_json == row.source_table_details_json
}

/// The rows `read` fetches, as a stream: the read runs once, on the first
/// poll, and a failed read is one `Err` item.
fn rows_stream<'a, F>(read: F) -> BoxStream<'a, Result<BackupRow, StorageError>>
where
    F: std::future::Future<Output = Result<Vec<BackupRow>, StorageError>> + Send + 'a,
{
    use futures::StreamExt;
    Box::pin(
        futures::stream::once(async move {
            match read.await {
                Ok(rows) => rows.into_iter().map(Ok).collect::<Vec<_>>(),
                Err(e) => vec![Err(e)],
            }
        })
        .flat_map(futures::stream::iter),
    )
}

/// `(created_at, backup_arn)` ascending, the order every listing returns.
fn sort_listing(rows: &mut [BackupRow]) {
    rows.sort_by(|a, b| {
        a.created_at
            .partial_cmp(&b.created_at)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.backup_arn.cmp(&b.backup_arn))
    });
}

/// A backup job's handle on its `backups_by_arn` row.
struct CassandraBackupClaim {
    engine: CassandraEngine,
    key: BackupKey,
    store: StoreIdentity,
    generation: u64,
}

impl JobClaim for CassandraBackupClaim {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>> {
        Box::pin(async move {
            let confirmed = heartbeat_backup_row(
                &self.engine,
                &self.key.account_id,
                &self.key.backup_arn,
                self.generation,
            )
            .await?;
            Ok(if confirmed {
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
            let mut assignments = vec!["backup_status = ?".to_owned()];
            let mut values = vec![Value::from(to.as_str())];
            if let Some(size) = fields.size_bytes {
                assignments.push("backup_size_bytes = ?".to_owned());
                values.push(Value::from(size));
            }
            if let Some(count) = fields.item_count {
                assignments.push("item_count = ?".to_owned());
                values.push(Value::from(count));
            }
            if let Some(reason) = fields.failure_reason.as_deref() {
                assignments.push("failure_reason = ?".to_owned());
                values.push(Value::from(reason));
            }
            if fields.completed {
                assignments.push("completed_at = toTimestamp(now())".to_owned());
            }
            if let Some(source_table) = fields.source_table_details_json.as_ref() {
                assignments.push("source_table_details_json = ?".to_owned());
                values.push(Value::from(source_table.to_string()));
            }
            values.extend([
                Value::from(self.key.account_id.as_str()),
                Value::from(self.key.backup_arn.as_str()),
                Value::from(generation_column(self.generation)),
                Value::from(from.as_str()),
            ]);
            let query = format!(
                "UPDATE {}.backups_by_arn SET {} WHERE account_id = ? AND backup_arn = ? \
                 IF owner_generation = ? AND backup_status = ?",
                self.engine.catalog_keyspace(),
                assignments.join(", ")
            );
            let outcome = conditional_write(
                &self.engine.session,
                &query,
                QueryValues::SimpleValues(values),
            )
            .await?;
            if outcome == WriteOutcome::Matched {
                self.engine
                    .move_status_index_row(&self.key, &self.store, from.as_str(), to.as_str())
                    .await;
            }
            Ok(outcome)
        })
    }

    fn read(&self) -> BoxFuture<'_, Result<JobRow, StorageError>> {
        Box::pin(async move {
            let query = format!(
                "SELECT backup_status, owner_generation, heartbeat_at, failure_reason \
                 FROM {}.backups_by_arn WHERE account_id = ? AND backup_arn = ?",
                self.engine.catalog_keyspace()
            );
            let rows = crate::cassandra_util::query_rows::<StorageError>(
                &self.engine.session,
                &query,
                cdrs_tokio::query_values!(
                    self.key.account_id.as_str(),
                    self.key.backup_arn.as_str()
                ),
                "job row",
            )
            .await?;
            let row = rows
                .first()
                .ok_or_else(|| not_found(&self.key.backup_arn))?;
            let status: String =
                crate::cassandra_util::get_column(row, "backup_status", "job row")?;
            let generation: Option<i64> = row.get_by_name("owner_generation").ok().flatten();
            let heartbeat_at: Option<i64> = row.get_by_name("heartbeat_at").ok().flatten();
            Ok(JobRow {
                status: JobStatus::parse(&status)?,
                owner_generation: generation_from_column(generation),
                heartbeat_at: heartbeat_at.map_or(0.0, millis_to_seconds),
                failure_reason: row.get_by_name("failure_reason").ok().flatten(),
            })
        })
    }
}

impl CassandraEngine {
    /// The index row `backup_arn` names in `account_id`, if there is one; an
    /// in-catalog row is `None`.
    /// Remove the three index entries of a `backups_by_arn` row this process
    /// has deleted: its status partition, its account listing and its table
    /// listing, in one logged batch.
    async fn remove_index_entries(&self, row: &BackupRow) -> Result<(), StorageError> {
        let catalog = self.catalog_keyspace();
        let created_at = seconds_to_millis(row.created_at);
        if let Some(error) = crate::test_hooks::index_entry_removal_error(&row.backup_arn) {
            return Err(error);
        }
        let batch = crate::cassandra_util::batch()
            .add_query(
                format!(
                    "DELETE FROM {catalog}.backup_index_by_status \
                     WHERE store_root = ? AND backup_status = ? AND backup_arn = ?"
                ),
                cdrs_tokio::query_values!(
                    row.store.store_root.as_str(),
                    row.status.as_str(),
                    row.backup_arn.as_str()
                ),
            )
            .add_query(
                format!(
                    "DELETE FROM {catalog}.backups_by_account \
                     WHERE account_id = ? AND created_at = ? AND backup_arn = ?"
                ),
                cdrs_tokio::query_values!(
                    row.account_id.as_str(),
                    created_at,
                    row.backup_arn.as_str()
                ),
            )
            .add_query(
                format!(
                    "DELETE FROM {catalog}.backups_by_table \
                     WHERE account_id = ? AND table_name = ? AND created_at = ? AND backup_arn = ?"
                ),
                cdrs_tokio::query_values!(
                    row.account_id.as_str(),
                    row.table_name.as_str(),
                    created_at,
                    row.backup_arn.as_str()
                ),
            );
        self.session
            .run_batch(
                batch
                    .build()
                    .map_err(|e| StorageError::Internal(e.to_string()))?,
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Remove backup index rows: {e}")))?;
        Ok(())
    }

    /// Write the three index entries of a `backups_by_arn` row this process
    /// has just inserted, after it: its account listing, its table listing
    /// and its status partition under `status`, in one logged batch.
    async fn write_index_entries(
        &self,
        row: &BackupRow,
        table_id: &str,
        table_arn: &str,
        status: BackupStatus,
    ) -> Result<(), StorageError> {
        let catalog = self.catalog_keyspace();
        let created_at = seconds_to_millis(row.created_at);
        if let Some(error) = crate::test_hooks::index_entry_batch_error(&row.backup_arn) {
            return Err(error);
        }
        let batch = crate::cassandra_util::batch()
            .add_query(
                format!(
                    "INSERT INTO {catalog}.backups_by_account (account_id, created_at, \
                     backup_arn, backup_name, table_id, table_name, table_arn, \
                     backup_size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                ),
                cdrs_tokio::query_values!(
                    row.account_id.as_str(),
                    created_at,
                    row.backup_arn.as_str(),
                    row.backup_name.as_str(),
                    table_id,
                    row.table_name.as_str(),
                    table_arn,
                    row.size_bytes
                ),
            )
            .add_query(
                format!(
                    "INSERT INTO {catalog}.backups_by_table (account_id, table_name, \
                     created_at, backup_arn, backup_name, table_id, table_arn, \
                     backup_size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                ),
                cdrs_tokio::query_values!(
                    row.account_id.as_str(),
                    row.table_name.as_str(),
                    created_at,
                    row.backup_arn.as_str(),
                    row.backup_name.as_str(),
                    table_id,
                    table_arn,
                    row.size_bytes
                ),
            )
            .add_query(
                format!(
                    "INSERT INTO {catalog}.backup_index_by_status \
                     (store_root, backup_status, backup_arn, account_id, store_kind, store_id) \
                     VALUES (?, ?, ?, ?, ?, ?)"
                ),
                cdrs_tokio::query_values!(
                    row.store.store_root.as_str(),
                    status.as_str(),
                    row.backup_arn.as_str(),
                    row.account_id.as_str(),
                    row.store.store_kind.as_str(),
                    row.store.store_id.as_str()
                ),
            );
        self.session
            .run_batch(
                batch
                    .build()
                    .map_err(|e| StorageError::Internal(e.to_string()))?,
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Backup index rows: {e}")))?;
        Ok(())
    }

    /// [`Self::read_index_row`] at `LOCAL_SERIAL`, so a row a lightweight
    /// transaction committed, or one whose round is in progress, is seen.
    async fn read_index_row_serial(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> Result<Option<BackupRow>, StorageError> {
        let query = format!(
            "SELECT * FROM {}.backups_by_arn WHERE account_id = ? AND backup_arn = ?",
            self.catalog_keyspace()
        );
        let rows = crate::cassandra_util::query_rows_serial::<StorageError>(
            &self.session,
            &query,
            cdrs_tokio::query_values!(account_id, backup_arn),
            "backup index row",
        )
        .await?;
        rows.first()
            .map(index_row_from_row)
            .transpose()
            .map(|row| row.flatten().filter(|r| r.account_id == account_id))
    }

    async fn read_index_row(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> Result<Option<BackupRow>, StorageError> {
        let query = format!(
            "SELECT * FROM {}.backups_by_arn WHERE account_id = ? AND backup_arn = ?",
            self.catalog_keyspace()
        );
        let rows = crate::cassandra_util::query_rows::<StorageError>(
            &self.session,
            &query,
            cdrs_tokio::query_values!(account_id, backup_arn),
            "backup index row",
        )
        .await?;
        rows.first()
            .map(index_row_from_row)
            .transpose()
            .map(|row| row.flatten().filter(|r| r.account_id == account_id))
    }

    /// Move the `backup_index_by_status` entry of a row from one status
    /// partition to another, after the authoritative row changed. A failure
    /// is logged and the batch is not sent again: the row's entry stays
    /// under `from`, readers of one status partition re-read the row and
    /// skip it, `list_backup_rows_by_root` lists the row under its own
    /// status, and `remove_orphan_status_entries` of the root repairs it.
    async fn move_status_index_row(
        &self,
        key: &BackupKey,
        store: &StoreIdentity,
        from: &str,
        to: &str,
    ) {
        let catalog = self.catalog_keyspace();
        let batch = crate::cassandra_util::batch()
            .add_query(
                format!(
                    "DELETE FROM {catalog}.backup_index_by_status \
                     WHERE store_root = ? AND backup_status = ? AND backup_arn = ?"
                ),
                cdrs_tokio::query_values!(store.store_root.as_str(), from, key.backup_arn.as_str()),
            )
            .add_query(
                format!(
                    "INSERT INTO {catalog}.backup_index_by_status \
                     (store_root, backup_status, backup_arn, account_id, store_kind, store_id) \
                     VALUES (?, ?, ?, ?, ?, ?)"
                ),
                cdrs_tokio::query_values!(
                    store.store_root.as_str(),
                    to,
                    key.backup_arn.as_str(),
                    key.account_id.as_str(),
                    store.store_kind.as_str(),
                    store.store_id.as_str()
                ),
            );
        let sent = match batch.build() {
            Ok(batch) => self.session.run_batch(batch).await.map(|_| ()),
            Err(error) => Err(error),
        };
        if let Err(error) = sent {
            tracing::warn!(
                "backup {}: its status index entry could not be moved from {from} to {to} \
                 ({error}); readers verify against the row",
                key.backup_arn
            );
        }
    }

    /// DeleteBackup's first step, and with `remove` the outright removal of
    /// `forget_available_backup_row`, as one body: read the row, check its
    /// status, run the in-use rule, then one lightweight transaction under
    /// [`DELETE_CLAIM_CONDITION`] that writes DELETING or, with `remove`,
    /// deletes the row (`Deleting` then means removed); then the row's index
    /// entries. Every writer of `active_restores` is conditional, so the
    /// condition orders the write wholly before or after a registration; the
    /// condition also carries the store read, so a row `rewrite_store`
    /// moved between the read and the write is read again and its entry is
    /// moved under the root it names.
    ///
    /// A lost answer of the removal is settled by a serial read: no row
    /// means removed, a row means the loop runs again (a lost answer on
    /// every attempt is an error). A lost answer of the DELETING write is an
    /// error (another delete may have written the same status). After the
    /// removal a failed entry batch is logged: the row is gone, and every
    /// reader of an entry re-reads the row and skips it.
    async fn start_backup_delete_lwt(
        &self,
        key: &BackupKey,
        remove: bool,
    ) -> Result<DeleteStart, StorageError> {
        let catalog = self.catalog_keyspace();
        let write = if remove {
            format!(
                "DELETE FROM {catalog}.backups_by_arn WHERE account_id = ? AND backup_arn = ? \
                 {DELETE_CLAIM_CONDITION}"
            )
        } else {
            format!(
                "UPDATE {catalog}.backups_by_arn SET backup_status = 'DELETING' \
                 WHERE account_id = ? AND backup_arn = ? {DELETE_CLAIM_CONDITION}"
            )
        };
        let mut attempt = 0;
        loop {
            let stored = self
                .load_stored_backup(&key.account_id, &key.backup_arn)
                .await?
                .filter(|stored| !stored.is_in_catalog())
                .ok_or_else(|| not_found(&key.backup_arn))?;
            let status = BackupStatus::parse(&stored.backup_status)?;
            if status != BackupStatus::Available {
                return Ok(DeleteStart::Status(status));
            }
            // The in-use rule reads the target's status; a registration whose
            // target is no longer being restored is cleared here so it cannot
            // hold the row's lock.
            if let Some(target) = self.creating_restore_target(&stored).await? {
                return Ok(DeleteStart::InUse(target));
            }
            // The removal's entries are keyed by the row's columns, so the
            // row is read before it goes.
            let removed_row = if remove {
                Some(
                    self.read_index_row(&key.account_id, &key.backup_arn)
                        .await?
                        .ok_or_else(|| not_found(&key.backup_arn))?,
                )
            } else {
                None
            };
            crate::test_hooks::before_delete_claim(&key.backup_arn).await;
            let read_kind = stored.store_kind.as_deref().unwrap_or("");
            let read_root = stored.store_root.as_deref().unwrap_or("");
            let values = || {
                cdrs_tokio::query_values!(
                    key.account_id.as_str(),
                    key.backup_arn.as_str(),
                    read_kind,
                    read_root
                )
            };
            let fault = if remove {
                crate::test_hooks::forget_write_fault(&key.backup_arn)
            } else {
                None
            };
            let outcome = if fault == Some(crate::test_hooks::ProgressWriteFault::Lost) {
                WriteOutcome::Unknown
            } else {
                let outcome = conditional_write(&self.session, &write, values()).await?;
                if fault.is_some() {
                    WriteOutcome::Unknown
                } else {
                    outcome
                }
            };
            let applied = match outcome {
                WriteOutcome::Matched => true,
                WriteOutcome::NotMatched => false,
                WriteOutcome::Unknown if remove => self
                    .read_index_row_serial(&key.account_id, &key.backup_arn)
                    .await?
                    .is_none(),
                WriteOutcome::Unknown => {
                    return Err(StorageError::Internal(format!(
                        "the DELETING write of backup {} has an unknown outcome",
                        key.backup_arn
                    )));
                }
            };
            let lost = outcome == WriteOutcome::Unknown;
            if applied {
                if let Some(row) = removed_row {
                    if let Err(error) = self.remove_index_entries(&row).await {
                        tracing::warn!(
                            "backup {}: the row is removed and its list and status entries \
                             are not ({error}); readers verify against the row and skip them",
                            key.backup_arn
                        );
                    }
                } else {
                    crate::test_hooks::after_delete_claim(&key.backup_arn).await;
                    let store = StoreIdentity {
                        store_kind: StoreKind::parse(stored.store_kind.as_deref().unwrap_or(""))
                            .unwrap_or(StoreKind::Filesystem),
                        store_root: stored.store_root.clone().unwrap_or_default(),
                        store_id: stored.store_id.clone().unwrap_or_default(),
                    };
                    self.move_status_index_row(key, &store, "AVAILABLE", "DELETING")
                        .await;
                }
                return Ok(DeleteStart::Deleting);
            }
            attempt += 1;
            if attempt >= DELETE_CLAIM_ATTEMPTS {
                if lost {
                    return Err(StorageError::Internal(format!(
                        "the removal of backup {} has an unknown outcome on every attempt",
                        key.backup_arn
                    )));
                }
                // A registration or another writer (a move of the row to
                // another store included) landed between the check and the
                // write on every attempt.
                let stored = self
                    .load_stored_backup(&key.account_id, &key.backup_arn)
                    .await?
                    .ok_or_else(|| not_found(&key.backup_arn))?;
                let status = BackupStatus::parse(&stored.backup_status)?;
                if status != BackupStatus::Available {
                    return Ok(DeleteStart::Status(status));
                }
                let target = stored
                    .active_restores
                    .values()
                    .next()
                    .cloned()
                    .unwrap_or_default();
                return Ok(DeleteStart::InUse(target));
            }
        }
    }

    /// The ARNs the status index lists for `store` under `status`.
    async fn status_index_arns(
        &self,
        store: &StoreIdentity,
        status: BackupStatus,
    ) -> Result<Vec<(String, String)>, StorageError> {
        self.root_index_arns(
            store.store_kind,
            &store.store_root,
            Some(&store.store_id),
            status,
        )
        .await
    }

    /// The ARNs the status index lists under `store_root` and `status` whose
    /// entry carries `store_kind`, and `store_id` when given.
    async fn root_index_arns(
        &self,
        store_kind: StoreKind,
        store_root: &str,
        store_id: Option<&str>,
        status: BackupStatus,
    ) -> Result<Vec<(String, String)>, StorageError> {
        let query = format!(
            "SELECT backup_arn, account_id, store_kind, store_id FROM {}.backup_index_by_status \
             WHERE store_root = ? AND backup_status = ?",
            self.catalog_keyspace()
        );
        let rows = crate::cassandra_util::query_rows::<StorageError>(
            &self.session,
            &query,
            cdrs_tokio::query_values!(store_root, status.as_str()),
            "status index",
        )
        .await?;
        let mut arns = Vec::with_capacity(rows.len());
        for row in rows {
            let entry_kind: Option<String> = row.get_by_name("store_kind").ok().flatten();
            let entry_id: Option<String> = row.get_by_name("store_id").ok().flatten();
            if entry_kind.as_deref() != Some(store_kind.as_str())
                || store_id.is_some_and(|id| entry_id.as_deref() != Some(id))
            {
                continue;
            }
            let backup_arn: String =
                crate::cassandra_util::get_column(&row, "backup_arn", "status index")?;
            let account_id: String =
                crate::cassandra_util::get_column(&row, "account_id", "status index")?;
            arns.push((account_id, backup_arn));
        }
        Ok(arns)
    }

    /// Write one `backup_index_by_status` entry, or with `remove` delete it,
    /// at `LOCAL_QUORUM`: `rewrite_store`'s two entry writes.
    async fn write_status_entry(
        &self,
        key: &BackupKey,
        store: &StoreIdentity,
        status: BackupStatus,
        remove: bool,
    ) -> Result<(), StorageError> {
        let catalog = self.catalog_keyspace();
        if remove {
            crate::cassandra_util::execute::<StorageError>(
                &self.session,
                &format!(
                    "DELETE FROM {catalog}.backup_index_by_status \
                     WHERE store_root = ? AND backup_status = ? AND backup_arn = ?"
                ),
                cdrs_tokio::query_values!(
                    store.store_root.as_str(),
                    status.as_str(),
                    key.backup_arn.as_str()
                ),
                "remove a status entry",
            )
            .await?;
        } else {
            crate::cassandra_util::execute::<StorageError>(
                &self.session,
                &format!(
                    "INSERT INTO {catalog}.backup_index_by_status \
                     (store_root, backup_status, backup_arn, account_id, store_kind, store_id) \
                     VALUES (?, ?, ?, ?, ?, ?)"
                ),
                cdrs_tokio::query_values!(
                    store.store_root.as_str(),
                    status.as_str(),
                    key.backup_arn.as_str(),
                    key.account_id.as_str(),
                    store.store_kind.as_str(),
                    store.store_id.as_str()
                ),
                "write a status entry",
            )
            .await?;
        }
        Ok(())
    }

    /// `rewrite_store` for one key, in the order the module doc gives: the
    /// row read at `LOCAL_SERIAL` and skipped unless it is of the old kind
    /// and root and not CREATING; the new root's status entry; the
    /// lightweight transaction on the row, conditioned on the old kind and
    /// root and the status read; then the old root's entry deleted.
    /// `NotMatched` (another writer changed the row in between) reads the
    /// row again at `LOCAL_SERIAL`: a row that already carries `to` (kind,
    /// root and store id) counts as this call's write and keeps the new
    /// entry; a row of `to`'s root under another store id with the status
    /// read has its entry, which shares the new entry's key, written again
    /// with its own store; otherwise the new entry is removed; then the loop
    /// reads again, up to [`DELETE_CLAIM_ATTEMPTS`];
    /// a lost answer reads again, and a row that then carries `to` whole
    /// (kind, root and store id; `to`'s root under another store id is
    /// another store, left and not counted) is this call's write. `true`
    /// when this call rewrote the row.
    async fn rewrite_store_row(
        &self,
        from_kind: StoreKind,
        from_root: &str,
        key: &BackupKey,
        to: &StoreIdentity,
    ) -> Result<bool, StorageError> {
        let update = format!(
            "UPDATE {}.backups_by_arn SET store_kind = ?, store_root = ?, store_id = ? \
             WHERE account_id = ? AND backup_arn = ? \
             IF store_kind = ? AND store_root = ? AND backup_status = ?",
            self.catalog_keyspace()
        );
        let mut lost: Option<BackupStatus> = None;
        for _ in 0..DELETE_CLAIM_ATTEMPTS {
            let Some(row) = self
                .read_index_row_serial(&key.account_id, &key.backup_arn)
                .await?
            else {
                return Ok(false);
            };
            if let Some(status) = lost
                && row.store == *to
            {
                // The lost answer was this call's write.
                self.remove_old_status_entry(key, from_kind, from_root, status)
                    .await;
                return Ok(true);
            }
            if row.store.store_kind != from_kind
                || row.store.store_root != from_root
                || row.status == BackupStatus::Creating
            {
                return Ok(false);
            }
            // The new root's entry before the row: a stop after it leaves an
            // entry readers skip, and the row still listed under the old root.
            self.write_status_entry(key, to, row.status, false).await?;
            crate::test_hooks::rewrite_stop(
                &key.backup_arn,
                crate::test_hooks::RewriteStop::AfterNewEntry,
            )?;
            crate::test_hooks::before_rewrite_write(&key.backup_arn).await;
            let values = || {
                cdrs_tokio::query_values!(
                    to.store_kind.as_str(),
                    to.store_root.as_str(),
                    to.store_id.as_str(),
                    key.account_id.as_str(),
                    key.backup_arn.as_str(),
                    from_kind.as_str(),
                    from_root,
                    row.status.as_str()
                )
            };
            let fault = crate::test_hooks::rewrite_write_fault(&key.backup_arn);
            let outcome = if fault == Some(crate::test_hooks::ProgressWriteFault::Lost) {
                WriteOutcome::Unknown
            } else {
                let outcome = conditional_write(&self.session, &update, values()).await?;
                if fault.is_some() {
                    WriteOutcome::Unknown
                } else {
                    outcome
                }
            };
            match outcome {
                WriteOutcome::Matched => {
                    crate::test_hooks::rewrite_stop(
                        &key.backup_arn,
                        crate::test_hooks::RewriteStop::AfterRow,
                    )?;
                    self.remove_old_status_entry(key, from_kind, from_root, row.status)
                        .await;
                    return Ok(true);
                }
                WriteOutcome::NotMatched => {
                    lost = None;
                    // Read again before the new entry goes: a row that
                    // already carries `to` is this call's write (an earlier
                    // answer lost in transit whose proposal the retry's own
                    // Paxos prepare completed) or another move to the same store,
                    // and the new entry is its only correct one.
                    let now = self
                        .read_index_row_serial(&key.account_id, &key.backup_arn)
                        .await?;
                    if now.as_ref().is_some_and(|now| now.store == *to) {
                        self.remove_old_status_entry(key, from_kind, from_root, row.status)
                            .await;
                        return Ok(true);
                    }
                    // A row now of `to`'s root under another store id, with
                    // the status read (another move to that root), has its
                    // entry under the new entry's key: it is written again
                    // with the row's store rather than removed. Any other
                    // row has the new entry removed.
                    let entry = match &now {
                        Some(now)
                            if now.store.store_kind == to.store_kind
                                && now.store.store_root == to.store_root
                                && now.status == row.status =>
                        {
                            self.write_status_entry(key, &now.store, now.status, false)
                                .await
                        }
                        _ => self.write_status_entry(key, to, row.status, true).await,
                    };
                    if let Err(error) = entry {
                        tracing::warn!(
                            "backup {}: the status entry written under {} for a move that \
                             did not apply is left ({error}); readers verify against the row",
                            key.backup_arn,
                            to.store_root
                        );
                    }
                }
                WriteOutcome::Unknown => lost = Some(row.status),
            }
        }
        if lost.is_some() {
            return Err(StorageError::Internal(format!(
                "the store rewrite of backup {} has an unknown outcome on every attempt",
                key.backup_arn
            )));
        }
        // Another writer changed the row between the read and the write on
        // every attempt: left, and the caller's re-list names it.
        Ok(false)
    }

    /// Delete the old root's status entry of a row `rewrite_store` moved,
    /// under the status the move read. A failure is logged: readers skip
    /// the entry, and `read_store_ids` of the old root counts it until
    /// `--forget-store` of the old root deletes it.
    async fn remove_old_status_entry(
        &self,
        key: &BackupKey,
        from_kind: StoreKind,
        from_root: &str,
        status: BackupStatus,
    ) {
        let old = StoreIdentity {
            store_kind: from_kind,
            store_root: from_root.to_owned(),
            store_id: String::new(),
        };
        if let Err(error) = self.write_status_entry(key, &old, status, true).await {
            tracing::warn!(
                "backup {}: moved, and its status entry under {from_root} is left ({error}); \
                 readers verify against the row",
                key.backup_arn
            );
        }
    }

    /// Re-apply the target's `active_restores[table_id] = table_name` entry
    /// on the backup's row when it is absent, under the takeover of a dead
    /// manifest record (the trait doc of `claim_restore_dead`): one serial
    /// read of the map, then a lightweight transaction on the entry
    /// conditioned on the backup still AVAILABLE. A backup no longer
    /// AVAILABLE (a delete won while nothing held it) keeps no entry: the
    /// resumed load fails on the missing prefix and the target is removed.
    async fn reapply_restore_registration(
        &self,
        key: &RestoreTargetKey,
        backup_arn: &str,
    ) -> Result<(), StorageError> {
        use cdrs_tokio::types::AsRust;
        use cdrs_tokio::types::map::Map;
        let catalog = self.catalog_keyspace();
        let rows = crate::cassandra_util::query_rows_serial::<StorageError>(
            &self.session,
            &format!(
                "SELECT active_restores FROM {catalog}.backups_by_arn \
                 WHERE account_id = ? AND backup_arn = ?"
            ),
            cdrs_tokio::query_values!(key.account_id.as_str(), backup_arn),
            "restore registration",
        )
        .await?;
        let Some(row) = rows.first() else {
            return Ok(());
        };
        let map: Option<Map> = row.get_by_name("active_restores").ok().flatten();
        let entries: std::collections::HashMap<String, String> = match map {
            Some(map) => map
                .as_rust()
                .map_err(|e| StorageError::Internal(format!("Parse active_restores: {e}")))?
                .unwrap_or_default(),
            None => std::collections::HashMap::new(),
        };
        if entries.contains_key(&key.table_id) {
            return Ok(());
        }
        let applied = conditional_write(
            &self.session,
            &format!(
                "UPDATE {catalog}.backups_by_arn SET active_restores[?] = ? \
                 WHERE account_id = ? AND backup_arn = ? IF backup_status = 'AVAILABLE'"
            ),
            QueryValues::SimpleValues(vec![
                Value::from(key.table_id.as_str()),
                Value::from(key.table_name.as_str()),
                Value::from(key.account_id.as_str()),
                Value::from(backup_arn),
            ]),
        )
        .await?;
        match applied {
            WriteOutcome::Matched => tracing::warn!(
                "restore of table {}: the backup {backup_arn} did not list the target as in \
                 use (its owner died between the target's creation and the registration); the \
                 entry was re-applied by the takeover",
                key.table_name
            ),
            WriteOutcome::NotMatched => tracing::warn!(
                "restore of table {}: the backup {backup_arn} is no longer AVAILABLE and does \
                 not list the target; the resumed load fails on the missing prefix",
                key.table_name
            ),
            WriteOutcome::Unknown => tracing::warn!(
                "restore of table {}: re-applying the backup's in-use entry had no known \
                 outcome; the registration sweep removes a stale one",
                key.table_name
            ),
        }
        Ok(())
    }

    /// The `tables` row of a restore target, read at quorum.
    async fn restore_target_row(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<Option<RestoreTargetRow>, StorageError> {
        let query = format!(
            "SELECT table_id, table_status, restore_source_backup_arn, restore_status \
             FROM {}.tables WHERE account_id = ? AND table_name = ?",
            self.catalog_keyspace()
        );
        let rows = crate::cassandra_util::query_rows::<StorageError>(
            &self.session,
            &query,
            cdrs_tokio::query_values!(account_id, table_name),
            "restore target",
        )
        .await?;
        rows.first()
            .map(|row| {
                Ok(RestoreTargetRow {
                    table_id: crate::cassandra_util::get_column(row, "table_id", "restore target")?,
                    table_status: row.get_by_name("table_status").ok().flatten(),
                    source_backup_arn: row.get_by_name("restore_source_backup_arn").ok().flatten(),
                    job_status: row.get_by_name("restore_status").ok().flatten(),
                })
            })
            .transpose()
    }
}

/// The columns of a `tables` row `remove_restore_target` decides on.
struct RestoreTargetRow {
    table_id: String,
    table_status: Option<String>,
    source_backup_arn: Option<String>,
    job_status: Option<String>,
}

impl RestoreTargetRow {
    /// Whether this row is still the CREATING restore target the removal
    /// claims first.
    fn is_creating_target(&self) -> bool {
        self.table_status.as_deref() == Some("CREATING")
    }

    /// Whether this row is a target whose removal was claimed (CREATING to
    /// DELETING) and did not finish: DELETING with its registration and a
    /// FAILED record, the state `remove_restore_target` resumes from the
    /// drop. A DELETING row without a registration is a client's DeleteTable
    /// in flight, and one whose record is not FAILED (a restored table a
    /// client is deleting) belongs to that delete.
    fn is_claimed_for_removal(&self) -> bool {
        self.table_status.as_deref() == Some("DELETING")
            && self.source_backup_arn.is_some()
            && self.job_status.as_deref() == Some("FAILED")
    }
}

/// The `tables` columns one restore job record fills, in the order
/// [`restore_job_row_from`] reads them.
const RESTORE_JOB_COLUMNS: &str = "account_id, table_name, table_id, table_status, \
     restore_source_backup_arn, restore_kind, restore_status, restore_owner_generation, \
     restore_heartbeat_at, restore_store_kind, restore_store_root, restore_store_id, \
     restore_spool_host, restore_file_index, restore_items_applied, restore_failure_reason, \
     restore_completed_at";

/// One [`RestoreJobRow`] from a `tables` row that holds a restore record
/// (every member [`RESTORE_JOB_COLUMNS`] selects): the store the restore
/// reads, the claimant's spool, the recorded progress, the failure reason
/// and the completion time, beyond the six the reconciler's legacy and dead
/// arms read.
fn restore_job_row_from(row: &Row) -> Result<RestoreJobRow, StorageError> {
    let column = |name: &str| -> Result<String, StorageError> {
        crate::cassandra_util::get_column(row, name, "restore job")
    };
    let optional = |name: &str| -> Option<String> { row.get_by_name(name).ok().flatten() };
    let store = optional("restore_store_kind")
        .as_deref()
        .and_then(StoreKind::parse)
        .map(|store_kind| StoreIdentity {
            store_kind,
            store_root: optional("restore_store_root").unwrap_or_default(),
            store_id: optional("restore_store_id").unwrap_or_default(),
        });
    let generation: Option<i64> = row.get_by_name("restore_owner_generation").ok().flatten();
    let heartbeat_at: Option<i64> = row.get_by_name("restore_heartbeat_at").ok().flatten();
    let completed_at: Option<i64> = row.get_by_name("restore_completed_at").ok().flatten();
    let file_index: Option<i32> = row.get_by_name("restore_file_index").ok().flatten();
    let items_applied: Option<i64> = row.get_by_name("restore_items_applied").ok().flatten();
    Ok(RestoreJobRow {
        target: RestoreTargetKey {
            account_id: column("account_id")?,
            table_name: column("table_name")?,
            table_id: column("table_id")?,
        },
        source_backup_arn: column("restore_source_backup_arn")?,
        kind: RestoreKind::parse(&column("restore_kind")?)?,
        status: JobStatus::parse(&column("restore_status")?)?,
        owner_generation: generation_from_column(generation),
        heartbeat_at: heartbeat_at.map_or(0.0, millis_to_seconds),
        store,
        spool_host: optional("restore_spool_host"),
        file_index: file_index.and_then(|i| u32::try_from(i).ok()),
        items_applied: items_applied.and_then(|i| u64::try_from(i).ok()),
        failure_reason: optional("restore_failure_reason"),
        completed_at: completed_at.map(millis_to_seconds),
    })
}

/// A store-backed restore job's handle on its target's `tables` row. Unlike
/// the legacy copy's self-managed [`crate::backup_engine::JobHeartbeat`],
/// the engine drives this claim's heartbeat and status writes; every write
/// is a lightweight transaction conditioned on the restore generation and on
/// the record being RUNNING, and leaves `table_status` to
/// [`CassandraEngine::activate_restore_target_row`] (the ACTIVE flip) and to
/// `remove_restore_target` (the CREATING to DELETING step of a removal).
struct CassandraRestoreClaim {
    engine: CassandraEngine,
    key: RestoreTargetKey,
    generation: u64,
}

impl JobClaim for CassandraRestoreClaim {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>> {
        Box::pin(async move {
            let query = format!(
                "UPDATE {}.tables SET restore_heartbeat_at = toTimestamp(now()) \
                 WHERE account_id = ? AND table_name = ? \
                 IF table_id = ? AND restore_owner_generation = ? AND restore_status = 'RUNNING'",
                self.engine.catalog_keyspace()
            );
            let result = crate::cassandra_util::query_lwt(
                &self.engine.session,
                &query,
                cdrs_tokio::query_values!(
                    self.key.account_id.as_str(),
                    self.key.table_name.as_str(),
                    self.key.table_id.as_str(),
                    generation_column(self.generation)
                ),
            )
            .await?;
            Ok(if crate::cassandra_util::lwt_applied(&result)? {
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
            // The record only; `table_status` is left to the ACTIVE flip and
            // to the removal's CREATING to DELETING step (4(e)'s two-write
            // rule). A restore record has no size/item/source-table columns,
            // so those `StatusFields` are ignored.
            let mut assignments = vec!["restore_status = ?".to_owned()];
            let mut values = vec![Value::from(to.as_str())];
            if let Some(reason) = fields.failure_reason.as_deref() {
                assignments.push("restore_failure_reason = ?".to_owned());
                values.push(Value::from(reason));
            }
            if fields.completed {
                assignments.push("restore_completed_at = toTimestamp(now())".to_owned());
            }
            values.extend([
                Value::from(self.key.account_id.as_str()),
                Value::from(self.key.table_name.as_str()),
                Value::from(self.key.table_id.as_str()),
                Value::from(generation_column(self.generation)),
                Value::from(from.as_str()),
            ]);
            let query = format!(
                "UPDATE {}.tables SET {} WHERE account_id = ? AND table_name = ? \
                 IF table_id = ? AND restore_owner_generation = ? AND restore_status = ?",
                self.engine.catalog_keyspace(),
                assignments.join(", ")
            );
            conditional_write(
                &self.engine.session,
                &query,
                QueryValues::SimpleValues(values),
            )
            .await
        })
    }

    fn read(&self) -> BoxFuture<'_, Result<JobRow, StorageError>> {
        Box::pin(async move {
            let query = format!(
                "SELECT restore_status, restore_owner_generation, restore_heartbeat_at, \
                 restore_failure_reason FROM {}.tables WHERE account_id = ? AND table_name = ?",
                self.engine.catalog_keyspace()
            );
            let rows = crate::cassandra_util::query_rows::<StorageError>(
                &self.engine.session,
                &query,
                cdrs_tokio::query_values!(
                    self.key.account_id.as_str(),
                    self.key.table_name.as_str()
                ),
                "restore job row",
            )
            .await?;
            let row = rows.first().ok_or_else(|| {
                StorageError::TableNotFound(format!(
                    "restore target {} has no job record",
                    self.key.table_name
                ))
            })?;
            let status: Option<String> = row.get_by_name("restore_status").ok().flatten();
            let status = status.ok_or_else(|| {
                StorageError::TableNotFound(format!(
                    "restore target {} has no job record",
                    self.key.table_name
                ))
            })?;
            let generation: Option<i64> =
                row.get_by_name("restore_owner_generation").ok().flatten();
            let heartbeat_at: Option<i64> = row.get_by_name("restore_heartbeat_at").ok().flatten();
            Ok(JobRow {
                status: JobStatus::parse(&status)?,
                owner_generation: generation_from_column(generation),
                heartbeat_at: heartbeat_at.map_or(0.0, millis_to_seconds),
                failure_reason: row.get_by_name("restore_failure_reason").ok().flatten(),
            })
        })
    }
}

impl BackupCatalog for CassandraEngine {
    fn catalog_id(&self) -> BoxFuture<'_, Result<String, StorageError>> {
        Box::pin(async move {
            let query = format!(
                "SELECT value FROM {}.settings WHERE key = 'catalog_id'",
                self.catalog_keyspace()
            );
            let rows = crate::cassandra_util::query_rows::<StorageError>(
                &self.session,
                &query,
                cdrs_tokio::query_values!(),
                "catalog id",
            )
            .await?;
            let row = rows.first().ok_or_else(|| {
                StorageError::Internal(
                    "the catalog has no catalog_id; run extenddb migrate --yes".to_owned(),
                )
            })?;
            crate::cassandra_util::get_column(row, "value", "catalog id")
        })
    }

    fn claim_backup<'a>(
        &'a self,
        row: &'a BackupRow,
    ) -> BoxFuture<'a, Result<Box<dyn JobClaim>, StorageError>> {
        Box::pin(async move {
            Self::validate_account_id(&row.account_id)?;
            let source = row.source_table()?;
            let key_schema = serde_json::to_string(&source.key_schema)
                .map_err(|e| StorageError::Internal(format!("Serialize key schema: {e}")))?;
            let attribute_definitions = serde_json::to_string(&source.attribute_definitions)
                .map_err(|e| StorageError::Internal(format!("Serialize attributes: {e}")))?;
            let generation = new_generation();
            let catalog = self.catalog_keyspace();
            // The row is the claim: it exists only owned. The legacy columns
            // every `backups_by_arn` row carries are filled from the source
            // table, so the row describes itself to either reader of it.
            let insert = format!(
                "INSERT INTO {catalog}.backups_by_arn (account_id, backup_arn, backup_id, \
                 backup_name, table_id, table_name, table_arn, backup_status, backup_type, \
                 backup_size_bytes, item_count, key_schema, attribute_definitions, billing_mode, \
                 table_created_at, created_at, store_kind, store_root, store_id, \
                 source_table_details_json, owner_generation, heartbeat_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, 'CREATING', 'USER', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                 ?, ?, toTimestamp(now())) IF NOT EXISTS"
            );
            let inserted = crate::cassandra_util::query_lwt(
                &self.session,
                &insert,
                cdrs_tokio::query_values!(
                    row.account_id.as_str(),
                    row.backup_arn.as_str(),
                    row.backup_id.as_str(),
                    row.backup_name.as_str(),
                    source.table_id.as_str(),
                    row.table_name.as_str(),
                    source.table_arn.as_str(),
                    row.size_bytes,
                    row.item_count,
                    key_schema.as_str(),
                    attribute_definitions.as_str(),
                    billing_mode_name(source.billing_mode),
                    seconds_to_millis(source.table_creation_date_time),
                    seconds_to_millis(row.created_at),
                    row.store.store_kind.as_str(),
                    row.store.store_root.as_str(),
                    row.store.store_id.as_str(),
                    row.source_table_details_json.to_string().as_str(),
                    generation_column(generation)
                ),
            )
            .await?;
            if !crate::cassandra_util::lwt_applied(&inserted)? {
                return Err(StorageError::AlreadyExists(format!(
                    "backup {} already has a row",
                    row.backup_arn
                )));
            }
            // The index rows, after the authoritative row.
            self.write_index_entries(
                row,
                &source.table_id,
                &source.table_arn,
                BackupStatus::Creating,
            )
            .await?;
            Ok(Box::new(CassandraBackupClaim {
                engine: self.clone(),
                key: row.key(),
                store: row.store.clone(),
                generation,
            }) as Box<dyn JobClaim>)
        })
    }

    fn insert_backup_row<'a>(
        &'a self,
        row: &'a BackupRow,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            Self::validate_account_id(&row.account_id)?;
            let source = row.source_table()?;
            let key_schema = serde_json::to_string(&source.key_schema)
                .map_err(|e| StorageError::Internal(format!("Serialize key schema: {e}")))?;
            let attribute_definitions = serde_json::to_string(&source.attribute_definitions)
                .map_err(|e| StorageError::Internal(format!("Serialize attributes: {e}")))?;
            let source_table_details_json = row.source_table_details_json.to_string();
            let catalog = self.catalog_keyspace();
            // `claim_backup`'s insert, unowned and complete. The partition is
            // (`account_id`, `backup_arn`) and no table indexes the backup id,
            // so a row for the id under another ARN spelling does not stop it.
            let insert = format!(
                "INSERT INTO {catalog}.backups_by_arn (account_id, backup_arn, backup_id, \
                 backup_name, table_id, table_name, table_arn, backup_status, backup_type, \
                 backup_size_bytes, item_count, key_schema, attribute_definitions, billing_mode, \
                 table_created_at, created_at, completed_at, failure_reason, store_kind, \
                 store_root, store_id, source_table_details_json, owner_generation, heartbeat_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'USER', ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()), \
                 ?, ?, ?, ?, ?, ?, toTimestamp(now())) IF NOT EXISTS"
            );
            let values = || {
                cdrs_tokio::query_values!(
                    row.account_id.as_str(),
                    row.backup_arn.as_str(),
                    row.backup_id.as_str(),
                    row.backup_name.as_str(),
                    source.table_id.as_str(),
                    row.table_name.as_str(),
                    source.table_arn.as_str(),
                    row.status.as_str(),
                    row.size_bytes,
                    row.item_count,
                    key_schema.as_str(),
                    attribute_definitions.as_str(),
                    billing_mode_name(source.billing_mode),
                    seconds_to_millis(source.table_creation_date_time),
                    seconds_to_millis(row.created_at),
                    row.failure_reason.as_deref(),
                    row.store.store_kind.as_str(),
                    row.store.store_root.as_str(),
                    row.store.store_id.as_str(),
                    source_table_details_json.as_str(),
                    generation_column(UNOWNED_GENERATION)
                )
            };
            // A lost answer, and an insert that finds a row, are settled by a
            // serial read. The row as this call writes it counts as inserted:
            // another `insert_backup_row` of the same verified prefix writes
            // the same row, either call may count it, and one that stopped
            // (or whose batch failed) between the row and its index entries
            // left it with no entries, which this call then writes. Any other
            // row is one that existed. No row after a lost answer means the
            // proposal did not commit, so the write is sent once more, as
            // design 4(e) allows; no row after `NotMatched` is a legacy
            // in-catalog row at the ARN, which `read_index_row_serial` does
            // not return.
            let mut inserted = None;
            for _ in 0..2 {
                let fault = crate::test_hooks::insert_write_fault(&row.backup_arn);
                let outcome = if fault == Some(crate::test_hooks::ProgressWriteFault::Lost) {
                    WriteOutcome::Unknown
                } else {
                    let outcome = conditional_write(&self.session, &insert, values()).await?;
                    if fault.is_some() {
                        WriteOutcome::Unknown
                    } else {
                        outcome
                    }
                };
                let read = || self.read_index_row_serial(&row.account_id, &row.backup_arn);
                match outcome {
                    WriteOutcome::Matched => inserted = Some(true),
                    WriteOutcome::NotMatched => {
                        inserted = Some(
                            read()
                                .await?
                                .is_some_and(|read| is_unowned_insert_of(&read, row)),
                        );
                    }
                    WriteOutcome::Unknown => {
                        inserted = read().await?.map(|read| is_unowned_insert_of(&read, row));
                    }
                }
                if inserted.is_some() {
                    break;
                }
            }
            let Some(inserted) = inserted else {
                return Err(StorageError::Internal(format!(
                    "the insert of backup {} has an unknown outcome",
                    row.backup_arn
                )));
            };
            if inserted {
                // The index entries, after the authoritative row, the status
                // entry under the row's status. The batch is idempotent
                // upserts; a failed one is sent once more, and one that fails
                // twice is an error the next insert of the row repairs.
                if let Err(first) = self
                    .write_index_entries(row, &source.table_id, &source.table_arn, row.status)
                    .await
                {
                    tracing::warn!(
                        "backup {}: index entries not written, sending them once more: {first}",
                        row.backup_arn
                    );
                    self.write_index_entries(row, &source.table_id, &source.table_arn, row.status)
                        .await?;
                }
            }
            Ok(inserted)
        })
    }

    fn list_backup_rows<'a>(
        &'a self,
        status: Option<BackupStatus>,
        store: &'a StoreIdentity,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        rows_stream(async move {
            let statuses = match status {
                Some(status) => vec![status],
                None => vec![
                    BackupStatus::Creating,
                    BackupStatus::Available,
                    BackupStatus::Deleting,
                    BackupStatus::Deleted,
                    BackupStatus::Failed,
                ],
            };
            let mut rows = Vec::new();
            for status in statuses {
                for (account_id, backup_arn) in self.status_index_arns(store, status).await? {
                    // The entry is a hint: the authoritative row decides.
                    if let Some(row) = self.read_index_row(&account_id, &backup_arn).await?
                        && row.status == status
                        && row.store == *store
                    {
                        rows.push(row);
                    }
                }
            }
            sort_listing(&mut rows);
            Ok(rows)
        })
    }

    fn list_backup_rows_by_root<'a>(
        &'a self,
        store_kind: StoreKind,
        store_root: &'a str,
        status: Option<BackupStatus>,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        rows_stream(async move {
            // Every status partition of the root, whatever `status` is: the
            // entry is a hint and the authoritative row decides, so a row
            // whose entry is under an earlier status (a status change whose
            // entry batch failed, or whose writer died between its
            // transaction and its batch) is found and listed under its own
            // status. Each row is read once, however many entries it has.
            let mut seen = std::collections::HashSet::new();
            let mut rows = Vec::new();
            for entry_status in BackupStatus::ALL {
                for (account_id, backup_arn) in self
                    .root_index_arns(store_kind, store_root, None, entry_status)
                    .await?
                {
                    if !seen.insert((account_id.clone(), backup_arn.clone())) {
                        continue;
                    }
                    if let Some(row) = self.read_index_row(&account_id, &backup_arn).await?
                        && status.is_none_or(|status| row.status == status)
                        && row.store.store_kind == store_kind
                        && row.store.store_root == store_root
                    {
                        rows.push(row);
                    }
                }
            }
            sort_listing(&mut rows);
            Ok(rows)
        })
    }

    fn newest_available_backup_row<'a>(
        &'a self,
        store: &'a StoreIdentity,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>> {
        Box::pin(async move {
            // One partition read of the status index, then one row: the
            // entries are ordered by the instant minted into the backup id
            // (the row's `created_at` by the manifest rule), newest first,
            // and the first entry whose row confirms the status and the
            // store is the newest AVAILABLE row. An entry whose row moved
            // (a hint the deleter's batch has not yet moved) costs one more
            // read.
            let mut entries = self
                .status_index_arns(store, BackupStatus::Available)
                .await?;
            entries.sort_by(|(_, a), (_, b)| {
                let minted = |arn: &str| {
                    extenddb_core::backup_format::backup_creation_date_time(arn).unwrap_or(0.0)
                };
                minted(b)
                    .partial_cmp(&minted(a))
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| b.cmp(a))
            });
            for (account_id, backup_arn) in entries {
                if let Some(row) = self.read_index_row(&account_id, &backup_arn).await?
                    && row.status == BackupStatus::Available
                    && row.store == *store
                {
                    return Ok(Some(row));
                }
            }
            Ok(None)
        })
    }

    fn list_account_backup_rows<'a>(
        &'a self,
        account_id: &'a str,
        table_name: Option<&'a str>,
        bounds: &'a TimeBounds,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>> {
        rows_stream(async move {
            Self::validate_account_id(account_id)?;
            let catalog = self.catalog_keyspace();
            let mut query = match table_name {
                Some(_) => format!(
                    "SELECT backup_arn FROM {catalog}.backups_by_table \
                     WHERE account_id = ? AND table_name = ?"
                ),
                None => {
                    format!(
                        "SELECT backup_arn FROM {catalog}.backups_by_account WHERE account_id = ?"
                    )
                }
            };
            let mut values = vec![Value::from(account_id)];
            if let Some(table_name) = table_name {
                values.push(Value::from(table_name));
            }
            // The clustering range narrows the read; `contains` is the exact
            // rule (lower inclusive, upper exclusive) on the row's seconds.
            #[allow(clippy::cast_possible_truncation)]
            if let Some(lower) = bounds.lower {
                query.push_str(" AND created_at >= ?");
                values.push(Value::from((lower * 1_000.0).floor() as i64));
            }
            #[allow(clippy::cast_possible_truncation)]
            if let Some(upper) = bounds.upper {
                query.push_str(" AND created_at < ?");
                values.push(Value::from((upper * 1_000.0).ceil() as i64));
            }
            let listed = self
                .session
                .run(Stmt::Read, &query, QueryValues::SimpleValues(values))
                .await
                .map_err(|e| StorageError::Internal(format!("List backup index: {e}")))?
                .response_body()
                .map_err(|e| StorageError::Internal(format!("Parse backup index list: {e}")))?
                .into_rows()
                .unwrap_or_default();
            let mut rows = Vec::with_capacity(listed.len());
            for entry in listed {
                let backup_arn: String =
                    crate::cassandra_util::get_column(&entry, "backup_arn", "backup index list")?;
                // An in-catalog backup's entry reads as `None` here and is the
                // legacy reader's to list.
                if let Some(row) = self.read_index_row(account_id, &backup_arn).await?
                    && bounds.contains(row.created_at)
                {
                    rows.push(row);
                }
            }
            sort_listing(&mut rows);
            Ok(rows)
        })
    }

    fn read_backup_row<'a>(
        &'a self,
        arn: &'a BackupArn,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>> {
        Box::pin(async move {
            let row = self
                .read_index_row(arn.account_id(), &arn.to_arn_string())
                .await?;
            Ok(row.filter(|row| {
                row.backup_id == arn.backup_id()
                    && row.table_name == arn.table_name()
                    && row.account_id == arn.account_id()
            }))
        })
    }

    fn start_backup_delete<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<DeleteStart, StorageError>> {
        Box::pin(self.start_backup_delete_lwt(key, false))
    }

    fn forget_available_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<ForgetRow, StorageError>> {
        Box::pin(async move {
            Ok(match self.start_backup_delete_lwt(key, true).await? {
                DeleteStart::Deleting => ForgetRow::Removed,
                DeleteStart::InUse(table) => ForgetRow::InUse(table),
                DeleteStart::Status(status) => ForgetRow::Status(status),
            })
        })
    }

    fn rewrite_store<'a>(
        &'a self,
        from_kind: StoreKind,
        from_root: &'a str,
        keys: &'a [BackupKey],
        to: &'a StoreIdentity,
    ) -> BoxFuture<'a, Result<u64, StorageError>> {
        Box::pin(async move {
            let mut rewritten = 0;
            for key in keys {
                if self
                    .rewrite_store_row(from_kind, from_root, key, to)
                    .await?
                {
                    rewritten += 1;
                }
            }
            Ok(rewritten)
        })
    }

    /// Each key is a serial read, the new entry, the lightweight transaction
    /// and the old entry's delete, so the move calls with one key at a time.
    fn rewrite_store_runs_key_by_key(&self) -> bool {
        true
    }

    fn set_backup_deleted<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let Some(row) = self
                .read_index_row(&key.account_id, &key.backup_arn)
                .await?
            else {
                return Ok(false);
            };
            // The store read is in the condition, so a row `rewrite_store`
            // moved after the read is not matched: it is read once more at
            // `LOCAL_SERIAL` and, still DELETING under the root it now
            // names, written again with that root, and its status entry
            // moves there.
            let query = format!(
                "UPDATE {}.backups_by_arn SET backup_status = 'DELETED', \
                 completed_at = toTimestamp(now()) \
                 WHERE account_id = ? AND backup_arn = ? \
                 IF backup_status = 'DELETING' AND store_kind = ? AND store_root = ?",
                self.catalog_keyspace()
            );
            crate::test_hooks::before_deleted_write(&key.backup_arn).await;
            let mut row = row;
            for attempt in 0..2 {
                let result = crate::cassandra_util::query_lwt(
                    &self.session,
                    &query,
                    cdrs_tokio::query_values!(
                        key.account_id.as_str(),
                        key.backup_arn.as_str(),
                        row.store.store_kind.as_str(),
                        row.store.store_root.as_str()
                    ),
                )
                .await?;
                if crate::cassandra_util::lwt_applied(&result)? {
                    crate::test_hooks::after_deleted_write(&key.backup_arn).await;
                    self.move_status_index_row(key, &row.store, "DELETING", "DELETED")
                        .await;
                    return Ok(true);
                }
                if attempt > 0 {
                    break;
                }
                match self
                    .read_index_row_serial(&key.account_id, &key.backup_arn)
                    .await?
                {
                    Some(now)
                        if now.status == BackupStatus::Deleting
                            && (now.store.store_kind != row.store.store_kind
                                || now.store.store_root != row.store.store_root) =>
                    {
                        row = now;
                    }
                    _ => break,
                }
            }
            Ok(false)
        })
    }

    fn remove_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
        store_kind: StoreKind,
        store_root: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let Some(row) = self
                .read_index_row(&key.account_id, &key.backup_arn)
                .await?
            else {
                return Ok(false);
            };
            if !matches!(row.status, BackupStatus::Deleted | BackupStatus::Failed)
                || row.store.store_kind != store_kind
                || row.store.store_root != store_root
            {
                return Ok(false);
            }
            crate::test_hooks::before_tombstone_removal(&key.backup_arn).await;
            // The authoritative row first, conditioned on the tombstone
            // status and the store read, so a row `rewrite_store` moved
            // after the read is left (`NotMatched` answers `false`); then
            // its index entries.
            let delete = format!(
                "DELETE FROM {}.backups_by_arn WHERE account_id = ? AND backup_arn = ? \
                 IF backup_status = ? AND store_kind = ? AND store_root = ?",
                self.catalog_keyspace()
            );
            let result = crate::cassandra_util::query_lwt(
                &self.session,
                &delete,
                cdrs_tokio::query_values!(
                    key.account_id.as_str(),
                    key.backup_arn.as_str(),
                    row.status.as_str(),
                    store_kind.as_str(),
                    store_root
                ),
            )
            .await?;
            if !crate::cassandra_util::lwt_applied(&result)? {
                return Ok(false);
            }
            self.remove_index_entries(&row).await?;
            Ok(true)
        })
    }

    fn remove_dead_creating_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let Some(row) = self
                .read_index_row(&key.account_id, &key.backup_arn)
                .await?
            else {
                return Ok(false);
            };
            if row.status != BackupStatus::Creating {
                return Ok(false);
            }
            let now = coordinator_now_millis(self).await?;
            let stale_ms = i64::try_from(stale_after.as_millis()).unwrap_or(i64::MAX);
            let heartbeat_at = seconds_to_millis(row.heartbeat_at);
            if heartbeat_at >= now.saturating_sub(stale_ms) {
                return Ok(false);
            }
            // Conditioned on the status and the heartbeat read: a heartbeat
            // written since (the owner is alive after all) leaves the row.
            let delete = format!(
                "DELETE FROM {}.backups_by_arn WHERE account_id = ? AND backup_arn = ? \
                 IF backup_status = 'CREATING' AND heartbeat_at = ?",
                self.catalog_keyspace()
            );
            let result = crate::cassandra_util::query_lwt(
                &self.session,
                &delete,
                cdrs_tokio::query_values!(
                    key.account_id.as_str(),
                    key.backup_arn.as_str(),
                    heartbeat_at
                ),
            )
            .await?;
            if !crate::cassandra_util::lwt_applied(&result)? {
                return Ok(false);
            }
            self.remove_index_entries(&row).await?;
            Ok(true)
        })
    }

    fn claim_dead_backup<'a>(
        &'a self,
        key: &'a BackupKey,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<Option<Box<dyn JobClaim>>, StorageError>> {
        Box::pin(async move {
            // The owner is dead when the row's heartbeat is older than the
            // window on the coordinator's clock. The row is read at
            // LOCAL_SERIAL, so a heartbeat an LWT just wrote is seen, and the
            // takeover is conditioned on the generation, the status and the
            // heartbeat as read: a beat landing between the read and the
            // write (the owner alive after all), or another taker's
            // generation, leaves the row and answers `None`. The heartbeat is
            // not refreshed: the FAILED-prefix window counts from the dead
            // owner's last beat.
            let catalog = self.catalog_keyspace();
            let query = format!(
                "SELECT backup_status, owner_generation, heartbeat_at, store_kind, store_root, \
                 store_id FROM {catalog}.backups_by_arn WHERE account_id = ? AND backup_arn = ?"
            );
            let rows = crate::cassandra_util::query_rows_serial::<StorageError>(
                &self.session,
                &query,
                cdrs_tokio::query_values!(key.account_id.as_str(), key.backup_arn.as_str()),
                "dead backup row",
            )
            .await?;
            let Some(row) = rows.first() else {
                return Ok(None);
            };
            let status: Option<String> = row.get_by_name("backup_status").ok().flatten();
            if status.as_deref() != Some(BackupStatus::Creating.as_str()) {
                return Ok(None);
            }
            // An in-catalog row (no store) is the legacy duty's.
            let store_kind: Option<String> = row.get_by_name("store_kind").ok().flatten();
            let Some(store_kind) = store_kind.as_deref().and_then(StoreKind::parse) else {
                return Ok(None);
            };
            let store = StoreIdentity {
                store_kind,
                store_root: crate::cassandra_util::get_column(
                    row,
                    "store_root",
                    "dead backup row",
                )?,
                store_id: crate::cassandra_util::get_column(row, "store_id", "dead backup row")?,
            };
            let owner_generation: Option<i64> = row.get_by_name("owner_generation").ok().flatten();
            let heartbeat_at: Option<i64> = row.get_by_name("heartbeat_at").ok().flatten();
            let now = coordinator_now_millis(self).await?;
            let stale_ms = i64::try_from(stale_after.as_millis()).unwrap_or(i64::MAX);
            if heartbeat_at.is_some_and(|at| at >= now.saturating_sub(stale_ms)) {
                return Ok(None);
            }
            crate::test_hooks::before_takeover_write(&key.backup_arn).await;
            let generation = new_generation();
            let take_over = format!(
                "UPDATE {catalog}.backups_by_arn SET owner_generation = ? \
                 WHERE account_id = ? AND backup_arn = ? \
                 IF owner_generation = ? AND backup_status = 'CREATING' AND heartbeat_at = ?"
            );
            let taken = conditional_write(
                &self.session,
                &take_over,
                cdrs_tokio::query_values!(
                    generation_column(generation),
                    key.account_id.as_str(),
                    key.backup_arn.as_str(),
                    owner_generation,
                    heartbeat_at
                ),
            )
            .await?;
            let claimed = match taken {
                WriteOutcome::Matched => true,
                WriteOutcome::NotMatched => false,
                // A takeover whose answer was lost may have committed: a row
                // carrying this call's generation is this call's claim.
                WriteOutcome::Unknown => {
                    let rows = crate::cassandra_util::query_rows_serial::<StorageError>(
                        &self.session,
                        &format!(
                            "SELECT owner_generation FROM {catalog}.backups_by_arn \
                             WHERE account_id = ? AND backup_arn = ?"
                        ),
                        cdrs_tokio::query_values!(key.account_id.as_str(), key.backup_arn.as_str()),
                        "dead backup row",
                    )
                    .await?;
                    let current: Option<i64> = rows
                        .first()
                        .and_then(|r| r.get_by_name("owner_generation").ok().flatten());
                    current == Some(generation_column(generation))
                }
            };
            if !claimed {
                return Ok(None);
            }
            Ok(Some(Box::new(CassandraBackupClaim {
                engine: self.clone(),
                key: key.clone(),
                store,
                generation,
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
        rows_stream(async move {
            // The coordinator's clock is read once, before the status
            // partition is scanned, so a row's column is compared with the
            // clock that stamped it and never with this host's; a heartbeat
            // written while the rows are re-read is later than `now` and the
            // row is not listed. A row with no `completed_at` has no age on
            // that clock and is not listed.
            let now = coordinator_now_millis(self).await?;
            let age_ms = i64::try_from(age.as_millis()).unwrap_or(i64::MAX);
            let cutoff = now.saturating_sub(age_ms);
            let mut rows = Vec::new();
            for (account_id, backup_arn) in self.status_index_arns(store, status).await? {
                let Some(row) = self.read_index_row(&account_id, &backup_arn).await? else {
                    continue;
                };
                if row.status != status || row.store != *store {
                    continue;
                }
                let at = match clock {
                    StaleClock::Heartbeat => Some(seconds_to_millis(row.heartbeat_at)),
                    StaleClock::Completed => row.completed_at.map(seconds_to_millis),
                };
                if at.is_some_and(|at| at < cutoff) {
                    rows.push(row);
                }
            }
            sort_listing(&mut rows);
            Ok(rows)
        })
    }

    fn sweep_restore_registrations(&self) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        Box::pin(async move {
            // A registration is live while its target is a CREATING restore
            // target with the registered table id restoring from this backup
            // (the rule the in-use check applies); any other entry is one a
            // restore left behind, its removal lost with its owner, and is
            // removed by a lightweight transaction on the entry as read. The
            // map has no index, so the scan is a range read of `backups_by_arn`
            // at quorum; rows carrying an entry are few.
            use cdrs_tokio::types::AsRust;
            use cdrs_tokio::types::map::Map;
            let catalog = self.catalog_keyspace();
            let scan = format!(
                "SELECT account_id, backup_arn, active_restores FROM {catalog}.backups_by_arn"
            );
            let rows = crate::cassandra_util::query_rows::<StorageError>(
                &self.session,
                &scan,
                cdrs_tokio::query_values!(),
                "restore registrations",
            )
            .await?;
            let remove = format!(
                "DELETE active_restores[?] FROM {catalog}.backups_by_arn \
                 WHERE account_id = ? AND backup_arn = ? IF active_restores[?] = ?"
            );
            let mut swept = Vec::new();
            for row in rows {
                let map: Option<Map> = row.get_by_name("active_restores").ok().flatten();
                let Some(map) = map else {
                    continue;
                };
                let entries: std::collections::HashMap<String, String> = map
                    .as_rust()
                    .map_err(|e| StorageError::Internal(format!("Parse active_restores: {e}")))?
                    .unwrap_or_default();
                if entries.is_empty() {
                    continue;
                }
                let account_id: String =
                    crate::cassandra_util::get_column(&row, "account_id", "restore registrations")?;
                let backup_arn: String =
                    crate::cassandra_util::get_column(&row, "backup_arn", "restore registrations")?;
                let mut touched = false;
                for (table_id, target_name) in entries {
                    let live = self
                        .restore_target_row(&account_id, &target_name)
                        .await?
                        .is_some_and(|target| {
                            target.table_id == table_id
                                && target.is_creating_target()
                                && target.source_backup_arn.as_deref() == Some(backup_arn.as_str())
                        });
                    if live {
                        continue;
                    }
                    let removed = conditional_write(
                        &self.session,
                        &remove,
                        cdrs_tokio::query_values!(
                            table_id.as_str(),
                            account_id.as_str(),
                            backup_arn.as_str(),
                            table_id.as_str(),
                            target_name.as_str()
                        ),
                    )
                    .await?;
                    if removed == WriteOutcome::Matched {
                        tracing::info!(
                            "backup {backup_arn}: removed the restore registration of table \
                             {target_name} ({table_id}); the target is no longer being restored \
                             from it"
                        );
                        touched = true;
                    }
                }
                if touched {
                    swept.push(backup_arn);
                }
            }
            swept.sort();
            Ok(swept)
        })
    }

    fn backup_in_use<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<Option<String>, StorageError>> {
        Box::pin(async move {
            let Some(stored) = self
                .load_stored_backup(&key.account_id, &key.backup_arn)
                .await?
            else {
                return Ok(None);
            };
            // The backup's `active_restores` map names the targets whose
            // registration committed its second write. A store-backed
            // registration creates the target row (`tables` with
            // `restore_source_backup_arn`, CREATING) before it writes that
            // map entry, so a second restore's in-use check in that window
            // would miss the first and fall through to its own override
            // validation (`ValidationException` naming the override's index
            // where the other backends answer `BackupInUseException`). Read
            // the target rows too: a
            // CREATING restore target of this backup is in use whether or not
            // its `active_restores` entry has landed yet.
            if let Some(target) = self.creating_restore_target(&stored).await? {
                return Ok(Some(target));
            }
            self.creating_restore_target_of_backup(&key.account_id, &key.backup_arn)
                .await
        })
    }

    fn remove_restore_target<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            // The key must name a restore target with this table id; a table
            // re-created under the name has another id and is not touched.
            // The removal is two writes: a claim from CREATING to DELETING,
            // one lightweight transaction on the row the ACTIVE flip
            // conditions on, so the two exclude each other; then the drop.
            // A row the claim already moved to DELETING, whose drop did not
            // finish, is resumed from the drop (`is_claimed_for_removal`).
            let Some(row) = self
                .restore_target_row(&key.account_id, &key.table_name)
                .await?
            else {
                return Ok(false);
            };
            let Some(backup_arn) = row.source_backup_arn.clone() else {
                return Ok(false);
            };
            if row.table_id != key.table_id {
                return Ok(false);
            }
            if row.is_creating_target() {
                let claim = format!(
                    "UPDATE {}.tables SET table_status = 'DELETING' \
                     WHERE account_id = ? AND table_name = ? \
                     IF table_id = ? AND table_status = 'CREATING'",
                    self.catalog_keyspace()
                );
                let result = crate::cassandra_util::query_lwt(
                    &self.session,
                    &claim,
                    cdrs_tokio::query_values!(
                        key.account_id.as_str(),
                        key.table_name.as_str(),
                        key.table_id.as_str()
                    ),
                )
                .await?;
                if !crate::cassandra_util::lwt_applied(&result)? {
                    return Ok(false);
                }
            } else if !row.is_claimed_for_removal() {
                return Ok(false);
            }
            self.drop_restore_target(&key.account_id, &key.table_name, &key.table_id, &backup_arn)
                .await?;
            Ok(true)
        })
    }

    fn list_dead_restore_jobs(
        &self,
        stale_after: Duration,
    ) -> BoxFuture<'_, Result<Vec<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            // Two filtered scans over the tables (CQL takes one equality on a
            // non-key column per scan): RUNNING records, dead when their
            // heartbeat is older than the cutoff; and FAILED records whose
            // table is still a restore target the removal has not finished
            // (CREATING: the FAILED write landed and the removal did not;
            // DELETING with its registration: the removal's claim landed and
            // its drop did not), dead when their `restore_completed_at` (or,
            // where none was stamped, their heartbeat) is that old. Both
            // kinds are few. The clock is read after the scans, so no
            // heartbeat a scan returned is later than `now` (with a zero
            // window, every RUNNING record the scans saw is listed). A range
            // scan has no Paxos read (a serial level resolves to a local
            // quorum there), so it stays at quorum.
            let mut scanned = Vec::new();
            for status in ["RUNNING", "FAILED"] {
                let query = format!(
                    "SELECT {RESTORE_JOB_COLUMNS} FROM {}.tables WHERE restore_status = ? \
                     ALLOW FILTERING",
                    self.catalog_keyspace()
                );
                let rows = crate::cassandra_util::query_rows::<StorageError>(
                    &self.session,
                    &query,
                    cdrs_tokio::query_values!(status),
                    "dead restore jobs",
                )
                .await?;
                scanned.push((status, rows));
            }
            let now = coordinator_now_millis(self).await?;
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let cutoff = now - stale_after.as_millis() as i64;
            let mut dead = Vec::new();
            for (status, rows) in scanned {
                for row in rows {
                    let heartbeat_at: Option<i64> =
                        row.get_by_name("restore_heartbeat_at").ok().flatten();
                    let completed_at: Option<i64> =
                        row.get_by_name("restore_completed_at").ok().flatten();
                    // A RUNNING record always carries a heartbeat (the insert
                    // or the backfill wrote it); one without is as dead as the
                    // epoch.
                    let age_of = if status == "FAILED" {
                        let target = RestoreTargetRow {
                            table_id: crate::cassandra_util::get_column(
                                &row,
                                "table_id",
                                "dead restore jobs",
                            )?,
                            table_status: row.get_by_name("table_status").ok().flatten(),
                            source_backup_arn: row
                                .get_by_name("restore_source_backup_arn")
                                .ok()
                                .flatten(),
                            job_status: Some(status.to_owned()),
                        };
                        if !(target.is_creating_target() || target.is_claimed_for_removal()) {
                            continue;
                        }
                        completed_at.or(heartbeat_at)
                    } else {
                        heartbeat_at
                    };
                    if age_of.is_some_and(|at| at >= cutoff) {
                        continue;
                    }
                    // A row this build cannot read (a kind or status no
                    // release writes) is logged and skipped, as ListBackups
                    // skips a row it cannot read, so one such row does not
                    // stop the cycle.
                    match restore_job_row_from(&row) {
                        Ok(job) => dead.push(job),
                        Err(e) => {
                            tracing::warn!(
                                "reconciler: restore job record of {} (account {}) skipped: {e}",
                                row.get_by_name("table_name")
                                    .ok()
                                    .flatten()
                                    .unwrap_or_else(String::new),
                                row.get_by_name("account_id")
                                    .ok()
                                    .flatten()
                                    .unwrap_or_else(String::new)
                            );
                            continue;
                        }
                    }
                }
            }
            Ok(dead)
        })
    }

    fn list_running_restore_jobs(&self) -> BoxFuture<'_, Result<Vec<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            // The RUNNING scan of `list_dead_restore_jobs` with no cutoff: by
            // status alone, so a record stamped in the scan's millisecond, or
            // ahead of the coordinator's clock (skew between coordinators),
            // is listed too.
            let query = format!(
                "SELECT {RESTORE_JOB_COLUMNS} FROM {}.tables WHERE restore_status = 'RUNNING' \
                 ALLOW FILTERING",
                self.catalog_keyspace()
            );
            let rows = crate::cassandra_util::query_rows::<StorageError>(
                &self.session,
                &query,
                cdrs_tokio::query_values!(),
                "running restore jobs",
            )
            .await?;
            rows.iter().map(restore_job_row_from).collect()
        })
    }

    fn claim_restore<'a>(
        &'a self,
        account_id: &'a str,
        input: CreateTableInput,
        registration: &'a RestoreRegistrationRow,
    ) -> BoxFuture<'a, Result<RestoreRegistered, StorageError>> {
        Box::pin(async move {
            Self::validate_account_id(account_id)?;
            let (description, generation) = self
                .register_manifest_restore_target(account_id, input, registration)
                .await?;
            let key = RestoreTargetKey {
                account_id: account_id.to_owned(),
                table_name: description.table_name.clone(),
                table_id: description.table_id.clone(),
            };
            Ok((
                description,
                Box::new(CassandraRestoreClaim {
                    engine: self.clone(),
                    key,
                    generation,
                }) as Box<dyn JobClaim>,
            ))
        })
    }

    fn claim_restore_dead<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
        stale_after: Duration,
        spool_host: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Option<Box<dyn JobClaim>>, StorageError>> {
        Box::pin(async move {
            // The record is read at LOCAL_SERIAL, so a heartbeat an LWT just
            // wrote is seen; dead is `restore_heartbeat_at` older than the
            // window on the coordinator's clock. The takeover is conditioned
            // on the generation, RUNNING, the manifest kind and the heartbeat
            // as read: a beat landing between the read and the write (the
            // owner alive after all), another taker's generation, or a record
            // that left RUNNING leaves the row and answers `None`. The write
            // sets the generation, the claimant's `spool_host` (`None` clears
            // it) and `restore_heartbeat_at` to the coordinator's clock, so a
            // second taker's read finds the record live at once (the trait
            // doc says why a restore record is refreshed where a backup row
            // is not). A legacy record is not a manifest restore's to take.
            let catalog = self.catalog_keyspace();
            let read = format!(
                "SELECT table_id, restore_status, restore_kind, restore_owner_generation, \
                 restore_heartbeat_at, restore_source_backup_arn FROM {catalog}.tables \
                 WHERE account_id = ? AND table_name = ?"
            );
            let rows = crate::cassandra_util::query_rows_serial::<StorageError>(
                &self.session,
                &read,
                cdrs_tokio::query_values!(key.account_id.as_str(), key.table_name.as_str()),
                "dead restore job",
            )
            .await?;
            let Some(row) = rows.first() else {
                return Ok(None);
            };
            let table_id: Option<String> = row.get_by_name("table_id").ok().flatten();
            if table_id.as_deref() != Some(key.table_id.as_str()) {
                return Ok(None);
            }
            let status: Option<String> = row.get_by_name("restore_status").ok().flatten();
            if status.as_deref() != Some(JobStatus::Running.as_str()) {
                return Ok(None);
            }
            let owner_generation: Option<i64> =
                row.get_by_name("restore_owner_generation").ok().flatten();
            let heartbeat_at: Option<i64> = row.get_by_name("restore_heartbeat_at").ok().flatten();
            let source_backup_arn: Option<String> =
                row.get_by_name("restore_source_backup_arn").ok().flatten();
            let now = coordinator_now_millis(self).await?;
            let stale_ms = i64::try_from(stale_after.as_millis()).unwrap_or(i64::MAX);
            if heartbeat_at.is_some_and(|at| at >= now.saturating_sub(stale_ms)) {
                return Ok(None);
            }
            // A test may interpose a beat here, between the read and the
            // write, to prove the heartbeat condition refuses the takeover.
            crate::test_hooks::before_restore_takeover(&key.table_id).await;
            let generation = new_generation();
            let take_over = format!(
                "UPDATE {catalog}.tables SET restore_owner_generation = ?, \
                 restore_spool_host = ?, restore_heartbeat_at = toTimestamp(now()) \
                 WHERE account_id = ? AND table_name = ? \
                 IF restore_owner_generation = ? AND restore_status = 'RUNNING' \
                 AND restore_kind = 'manifest' AND restore_heartbeat_at = ?"
            );
            let taken = conditional_write(
                &self.session,
                &take_over,
                QueryValues::SimpleValues(vec![
                    Value::from(generation_column(generation)),
                    match spool_host {
                        Some(host) => Value::from(host),
                        None => Value::from(Option::<String>::None),
                    },
                    Value::from(key.account_id.as_str()),
                    Value::from(key.table_name.as_str()),
                    Value::from(owner_generation),
                    Value::from(heartbeat_at),
                ]),
            )
            .await?;
            let claimed = match taken {
                WriteOutcome::Matched => true,
                WriteOutcome::NotMatched => false,
                // A takeover whose answer was lost may have committed: a
                // record carrying this call's generation is this call's claim.
                WriteOutcome::Unknown => {
                    let rows = crate::cassandra_util::query_rows_serial::<StorageError>(
                        &self.session,
                        &format!(
                            "SELECT restore_owner_generation FROM {catalog}.tables \
                             WHERE account_id = ? AND table_name = ?"
                        ),
                        cdrs_tokio::query_values!(key.account_id.as_str(), key.table_name.as_str()),
                        "dead restore job",
                    )
                    .await?;
                    let current: Option<i64> = rows
                        .first()
                        .and_then(|r| r.get_by_name("restore_owner_generation").ok().flatten());
                    current == Some(generation_column(generation))
                }
            };
            if !claimed {
                return Ok(None);
            }
            // The backup's `active_restores` entry for the target, re-applied
            // when it is absent: the registration writes the target's row and
            // its DDL first and the entry second, so an owner that died
            // between the two left a resumable record whose backup nothing
            // held in use. Without the entry `start_backup_delete` would
            // succeed under the resumed load.
            if let Some(backup_arn) = source_backup_arn {
                self.reapply_restore_registration(key, &backup_arn).await?;
            }
            Ok(Some(Box::new(CassandraRestoreClaim {
                engine: self.clone(),
                key: key.clone(),
                generation,
            }) as Box<dyn JobClaim>))
        })
    }

    fn read_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<Option<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            let query = format!(
                "SELECT {RESTORE_JOB_COLUMNS} FROM {}.tables \
                 WHERE account_id = ? AND table_name = ?",
                self.catalog_keyspace()
            );
            let rows = crate::cassandra_util::query_rows::<StorageError>(
                &self.session,
                &query,
                cdrs_tokio::query_values!(key.account_id.as_str(), key.table_name.as_str()),
                "restore job",
            )
            .await?;
            let Some(row) = rows.first() else {
                return Ok(None);
            };
            // No record, or a table re-created under the name with another id,
            // is not this target's job.
            let table_id: Option<String> = row.get_by_name("table_id").ok().flatten();
            let status: Option<String> = row.get_by_name("restore_status").ok().flatten();
            if table_id.as_deref() != Some(key.table_id.as_str()) || status.is_none() {
                return Ok(None);
            }
            Ok(Some(restore_job_row_from(row)?))
        })
    }

    fn list_restore_jobs<'a>(
        &'a self,
        status: Option<JobStatus>,
    ) -> BoxFuture<'a, Result<Vec<RestoreJobRow>, StorageError>> {
        Box::pin(async move {
            // CQL filters one equality per scan, so a status filter is one
            // scan and "every record" is a scan per restore status.
            let statuses = match status {
                Some(status) => vec![status],
                None => vec![JobStatus::Running, JobStatus::Done, JobStatus::Failed],
            };
            let mut jobs = Vec::new();
            for status in statuses {
                let query = format!(
                    "SELECT {RESTORE_JOB_COLUMNS} FROM {}.tables WHERE restore_status = ? \
                     ALLOW FILTERING",
                    self.catalog_keyspace()
                );
                let rows = crate::cassandra_util::query_rows::<StorageError>(
                    &self.session,
                    &query,
                    cdrs_tokio::query_values!(status.as_str()),
                    "restore jobs",
                )
                .await?;
                for row in rows {
                    jobs.push(restore_job_row_from(&row)?);
                }
            }
            Ok(jobs)
        })
    }

    /// Conditioned on RUNNING, on the listed `owner_generation` and on the
    /// listed `restore_heartbeat_at` (a millisecond timestamp, rebuilt from
    /// the listed epoch seconds exactly), and written with a fresh generation
    /// (design 4(e)), in one lightweight transaction.
    fn fail_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
        owner_generation: u64,
        heartbeat_at: f64,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let query = format!(
                "UPDATE {}.tables SET restore_status = 'FAILED', restore_failure_reason = ?, \
                 restore_completed_at = toTimestamp(now()), restore_owner_generation = ? \
                 WHERE account_id = ? AND table_name = ? \
                 IF table_id = ? AND restore_status = 'RUNNING' AND restore_owner_generation = ? \
                 AND restore_heartbeat_at = ?",
                self.catalog_keyspace()
            );
            let result = crate::cassandra_util::query_lwt(
                &self.session,
                &query,
                cdrs_tokio::query_values!(
                    reason,
                    generation_column(new_generation()),
                    key.account_id.as_str(),
                    key.table_name.as_str(),
                    key.table_id.as_str(),
                    generation_column(owner_generation),
                    seconds_to_millis(heartbeat_at)
                ),
            )
            .await?;
            crate::cassandra_util::lwt_applied(&result)
        })
    }

    fn read_store_ids<'a>(
        &'a self,
        store_kind: StoreKind,
        store_root: &'a str,
    ) -> BoxFuture<'a, Result<Vec<String>, StorageError>> {
        Box::pin(async move {
            // `backup_index_by_status` is partitioned by (root, status), so a
            // root's rows are its five status partitions; the entry carries
            // the kind and id, which is all this read needs (an orphan entry
            // left by a crash names the store its row had or was being
            // moved to).
            let query = format!(
                "SELECT store_kind, store_id FROM {}.backup_index_by_status \
                 WHERE store_root = ? AND backup_status = ?",
                self.catalog_keyspace()
            );
            let mut ids = Vec::new();
            for status in [
                BackupStatus::Creating,
                BackupStatus::Available,
                BackupStatus::Deleting,
                BackupStatus::Deleted,
                BackupStatus::Failed,
            ] {
                let rows = crate::cassandra_util::query_rows::<StorageError>(
                    &self.session,
                    &query,
                    cdrs_tokio::query_values!(store_root, status.as_str()),
                    "status index",
                )
                .await?;
                for row in rows {
                    let kind: Option<String> = row.get_by_name("store_kind").ok().flatten();
                    let id: Option<String> = row.get_by_name("store_id").ok().flatten();
                    if kind.as_deref() == Some(store_kind.as_str())
                        && let Some(id) = id
                        && !ids.contains(&id)
                    {
                        ids.push(id);
                    }
                }
            }
            ids.sort();
            Ok(ids)
        })
    }

    fn summary_counts_status_entries(&self) -> bool {
        true
    }

    fn remove_orphan_status_entries<'a>(
        &'a self,
        store_kind: StoreKind,
        store_root: &'a str,
    ) -> BoxFuture<'a, Result<StatusEntrySweep, StorageError>> {
        Box::pin(async move {
            // The root's five status partitions; each entry's row is read at
            // `LOCAL_SERIAL` (a lightweight transaction in progress is
            // seen). An entry whose row is gone or names another store is
            // deleted. An entry whose row is of this root under another
            // status (a status change whose entry batch failed, or whose
            // writer died between its transaction and its batch) is
            // repaired: the entry under the row's status is written (an
            // idempotent upsert), then the stale one is deleted.
            let delete = format!(
                "DELETE FROM {}.backup_index_by_status \
                 WHERE store_root = ? AND backup_status = ? AND backup_arn = ?",
                self.catalog_keyspace()
            );
            let mut sweep = StatusEntrySweep::default();
            for status in BackupStatus::ALL {
                for (account_id, backup_arn) in self
                    .root_index_arns(store_kind, store_root, None, status)
                    .await?
                {
                    match self.read_index_row_serial(&account_id, &backup_arn).await? {
                        Some(row)
                            if row.store.store_kind == store_kind
                                && row.store.store_root == store_root =>
                        {
                            if row.status == status {
                                continue;
                            }
                            self.write_status_entry(&row.key(), &row.store, row.status, false)
                                .await?;
                            sweep.repaired += 1;
                        }
                        _ => sweep.removed += 1,
                    }
                    crate::cassandra_util::execute::<StorageError>(
                        &self.session,
                        &delete,
                        cdrs_tokio::query_values!(store_root, status.as_str(), backup_arn.as_str()),
                        "remove an orphan status entry",
                    )
                    .await?;
                }
            }
            Ok(sweep)
        })
    }

    fn summarize_store_roots(&self) -> BoxFuture<'_, Result<Vec<StoreRootCount>, StorageError>> {
        Box::pin(async move {
            // The status table's partitions are (root, status): a scan of
            // its partition keys, then each partition's entries counted by
            // the kind and id they carry. Hints, not verified against the
            // rows (the trait doc).
            let catalog = self.catalog_keyspace();
            let partitions = crate::cassandra_util::query_rows::<StorageError>(
                &self.session,
                &format!("SELECT DISTINCT store_root, backup_status FROM {catalog}.backup_index_by_status"),
                cdrs_tokio::query_values!(),
                "status index partitions",
            )
            .await?;
            let entries_query = format!(
                "SELECT store_kind, store_id FROM {catalog}.backup_index_by_status \
                 WHERE store_root = ? AND backup_status = ?"
            );
            let mut entries = Vec::new();
            for partition in partitions {
                let root: String =
                    crate::cassandra_util::get_column(&partition, "store_root", "status index")?;
                let status: String =
                    crate::cassandra_util::get_column(&partition, "backup_status", "status index")?;
                let status = BackupStatus::parse(&status)?;
                let rows = crate::cassandra_util::query_rows::<StorageError>(
                    &self.session,
                    &entries_query,
                    cdrs_tokio::query_values!(root.as_str(), status.as_str()),
                    "status index",
                )
                .await?;
                for row in rows {
                    let kind: Option<String> = row.get_by_name("store_kind").ok().flatten();
                    let id: Option<String> = row.get_by_name("store_id").ok().flatten();
                    let (Some(kind), Some(id)) = (kind, id) else {
                        continue;
                    };
                    let store_kind = StoreKind::parse(&kind).ok_or_else(|| {
                        StorageError::Internal(format!(
                            "a status entry of root {root:?} carries an unknown store kind \
                             {kind:?}"
                        ))
                    })?;
                    entries.push((
                        StoreIdentity {
                            store_kind,
                            store_root: root.clone(),
                            store_id: id,
                        },
                        status,
                    ));
                }
            }
            Ok(StoreRootCount::tally(entries))
        })
    }

    fn maintenance_claim<'a>(
        &'a self,
        holder: &'a str,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<Box<dyn MaintenanceClaim>, StorageError>> {
        Box::pin(async move {
            // The row is the claim: `INSERT ... IF NOT EXISTS` takes it when
            // there is none; otherwise the row is read, and one whose
            // heartbeat is older than `stale_after` on the coordinator's clock
            // is taken over with `IF owner_generation = <old>`, so two
            // commands racing for a dead holder's row see one win.
            let catalog = self.catalog_keyspace();
            let generation = new_generation();
            let insert = format!(
                "INSERT INTO {catalog}.backup_maintenance \
                 (claim, holder, owner_generation, claimed_at, heartbeat_at) \
                 VALUES (?, ?, ?, toTimestamp(now()), toTimestamp(now())) IF NOT EXISTS"
            );
            let inserted = conditional_write(
                &self.session,
                &insert,
                cdrs_tokio::query_values!(
                    MAINTENANCE_CLAIM_KEY,
                    holder,
                    generation_column(generation)
                ),
            )
            .await?;
            // A test's lost answer: the insert ran, the caller sees no result.
            let inserted = if crate::test_hooks::maintenance_insert_answer_lost() {
                WriteOutcome::Unknown
            } else {
                inserted
            };
            if inserted != WriteOutcome::Matched {
                let read = format!(
                    "SELECT holder, owner_generation, heartbeat_at FROM \
                     {catalog}.backup_maintenance WHERE claim = ?"
                );
                let rows = crate::cassandra_util::query_rows_serial::<StorageError>(
                    &self.session,
                    &read,
                    cdrs_tokio::query_values!(MAINTENANCE_CLAIM_KEY),
                    "maintenance claim",
                )
                .await?;
                let row = rows.first();
                let current_holder: Option<String> =
                    row.and_then(|r| r.get_by_name("holder").ok().flatten());
                let current_generation: Option<i64> =
                    row.and_then(|r| r.get_by_name("owner_generation").ok().flatten());
                // An insert whose answer was lost (`Unknown`) may have
                // committed: a row carrying this call's generation is this
                // call's claim.
                if inserted == WriteOutcome::Unknown
                    && current_generation == Some(generation_column(generation))
                {
                    return Ok(Box::new(CassandraMaintenanceClaim {
                        engine: self.clone(),
                        generation,
                    }) as Box<dyn MaintenanceClaim>);
                }
                let heartbeat_at: Option<i64> =
                    row.and_then(|r| r.get_by_name("heartbeat_at").ok().flatten());
                let now = coordinator_now_millis(self).await?;
                let stale_ms = i64::try_from(stale_after.as_millis()).unwrap_or(i64::MAX);
                let stale = heartbeat_at.is_none_or(|at| at < now.saturating_sub(stale_ms));
                let refused = || {
                    StorageError::AlreadyExists(format!(
                        "the backup maintenance claim is held by {}",
                        current_holder
                            .as_deref()
                            .unwrap_or("another maintenance command")
                    ))
                };
                if !stale {
                    return Err(refused());
                }
                let take_over = format!(
                    "UPDATE {catalog}.backup_maintenance SET holder = ?, owner_generation = ?, \
                     claimed_at = toTimestamp(now()), heartbeat_at = toTimestamp(now()) \
                     WHERE claim = ? IF owner_generation = ?"
                );
                // A test's pause between the read and the takeover, so two
                // takers both read the stale row before either writes.
                crate::test_hooks::before_maintenance_takeover().await;
                let taken = conditional_write(
                    &self.session,
                    &take_over,
                    cdrs_tokio::query_values!(
                        holder,
                        generation_column(generation),
                        MAINTENANCE_CLAIM_KEY,
                        current_generation
                    ),
                )
                .await?;
                match taken {
                    WriteOutcome::Matched => {}
                    WriteOutcome::NotMatched => return Err(refused()),
                    WriteOutcome::Unknown => {
                        // The takeover's answer was lost; the update may
                        // have committed. Re-read at `LOCAL_SERIAL`, as the
                        // insert path does: a row carrying this call's
                        // generation is this call's claim, and answering
                        // "held by" the old holder would leave a row with
                        // this generation and a fresh heartbeat that nobody
                        // renews, refusing every job until it goes stale.
                        let rows = crate::cassandra_util::query_rows_serial::<StorageError>(
                            &self.session,
                            &read,
                            cdrs_tokio::query_values!(MAINTENANCE_CLAIM_KEY),
                            "maintenance claim",
                        )
                        .await?;
                        let now_generation: Option<i64> = rows
                            .first()
                            .and_then(|r| r.get_by_name("owner_generation").ok().flatten());
                        if now_generation != Some(generation_column(generation)) {
                            return Err(refused());
                        }
                    }
                }
            }
            Ok(Box::new(CassandraMaintenanceClaim {
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
            let query = format!(
                "SELECT heartbeat_at FROM {}.backup_maintenance WHERE claim = ?",
                self.catalog_keyspace()
            );
            let rows = crate::cassandra_util::query_rows_serial::<StorageError>(
                &self.session,
                &query,
                cdrs_tokio::query_values!(MAINTENANCE_CLAIM_KEY),
                "maintenance claim",
            )
            .await?;
            let Some(row) = rows.first() else {
                return Ok(false);
            };
            let heartbeat_at: Option<i64> = row.get_by_name("heartbeat_at").ok().flatten();
            let Some(heartbeat_at) = heartbeat_at else {
                return Ok(false);
            };
            let now = coordinator_now_millis(self).await?;
            let stale_ms = i64::try_from(stale_after.as_millis()).unwrap_or(i64::MAX);
            Ok(heartbeat_at >= now.saturating_sub(stale_ms))
        })
    }
}

/// The partition key of the one maintenance claim row.
const MAINTENANCE_CLAIM_KEY: &str = "claim";

/// The holder's handle on the `backup_maintenance` row.
struct CassandraMaintenanceClaim {
    engine: CassandraEngine,
    generation: u64,
}

impl MaintenanceClaim for CassandraMaintenanceClaim {
    fn generation(&self) -> u64 {
        self.generation
    }

    fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>> {
        Box::pin(async move {
            let query = format!(
                "UPDATE {}.backup_maintenance SET heartbeat_at = toTimestamp(now()) \
                 WHERE claim = ? IF owner_generation = ?",
                self.engine.catalog_keyspace()
            );
            let result = crate::cassandra_util::query_lwt(
                &self.engine.session,
                &query,
                cdrs_tokio::query_values!(
                    MAINTENANCE_CLAIM_KEY,
                    generation_column(self.generation)
                ),
            )
            .await?;
            Ok(if crate::cassandra_util::lwt_applied(&result)? {
                Heartbeat::Confirmed
            } else {
                Heartbeat::Lost
            })
        })
    }

    fn release(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move {
            let query = format!(
                "DELETE FROM {}.backup_maintenance WHERE claim = ? IF owner_generation = ?",
                self.engine.catalog_keyspace()
            );
            // Not applied means another holder took the row over; it is
            // theirs to release.
            conditional_write(
                &self.engine.session,
                &query,
                cdrs_tokio::query_values!(
                    MAINTENANCE_CLAIM_KEY,
                    generation_column(self.generation)
                ),
            )
            .await?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use cdrs_tokio::consistency::Consistency;
    use cdrs_tokio::frame::message_error::{ErrorBody, WriteTimeoutError, WriteType};

    use super::*;

    fn server_error(ty: ErrorType) -> DriverError {
        DriverError::Server {
            body: ErrorBody {
                message: "synthetic".to_owned(),
                ty,
            },
            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9042),
        }
    }

    /// Only a lost answer leaves a write unknown: an I/O or driver timeout,
    /// or the server's write timeout on the Paxos round. A refused statement
    /// (syntax, invalid) is an error with a known outcome.
    #[test]
    fn only_a_lost_answer_leaves_a_conditional_write_unknown() {
        let cas_timeout = server_error(ErrorType::WriteTimeout(WriteTimeoutError {
            cl: Consistency::LocalSerial,
            received: 0,
            block_for: 1,
            write_type: WriteType::Cas,
            contentions: Some(1),
        }));
        assert!(outcome_unknown(&cas_timeout));
        assert!(outcome_unknown(&DriverError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ))));
        assert!(outcome_unknown(&DriverError::Timeout("waited".to_owned())));
        assert!(!outcome_unknown(&server_error(ErrorType::Invalid)));
        assert!(!outcome_unknown(&server_error(ErrorType::Syntax)));
        assert!(!outcome_unknown(&DriverError::General("x".to_owned())));
    }

    #[test]
    fn generations_round_trip_through_the_column_and_a_bad_value_is_unowned() {
        assert_eq!(generation_from_column(Some(generation_column(7))), 7);
        assert_eq!(generation_from_column(None), 0);
        assert_eq!(generation_from_column(Some(-1)), 0);
        assert_eq!(generation_column(u64::MAX), i64::MAX);
    }

    #[test]
    fn listings_sort_by_created_at_then_arn() {
        let row = |created_at: f64, arn: &str| BackupRow {
            backup_id: String::new(),
            account_id: String::new(),
            table_name: String::new(),
            backup_arn: arn.to_owned(),
            backup_name: String::new(),
            status: BackupStatus::Available,
            store: StoreIdentity {
                store_kind: StoreKind::Filesystem,
                store_root: String::new(),
                store_id: String::new(),
            },
            size_bytes: 0,
            item_count: 0,
            created_at,
            completed_at: None,
            failure_reason: None,
            source_table_details_json: serde_json::Value::Null,
            owner_generation: 0,
            heartbeat_at: 0.0,
        };
        let mut rows = vec![row(2.0, "b"), row(1.0, "z"), row(2.0, "a")];
        sort_listing(&mut rows);
        let arns: Vec<&str> = rows.iter().map(|r| r.backup_arn.as_str()).collect();
        assert_eq!(arns, ["z", "a", "b"]);
    }

    /// The settle of an insert whose answer was lost: the row read back is
    /// this insert only when it is unowned and every column the insert takes
    /// from the row is equal; the columns the insert sets itself
    /// (`completed_at`, `heartbeat_at`) and the ARN, which the read was
    /// keyed by, do not count.
    #[test]
    fn a_lost_insert_is_settled_only_by_the_row_it_writes() {
        let row = BackupRow {
            backup_id: "01757500000123-0badcafe".to_owned(),
            account_id: "123456789012".to_owned(),
            table_name: "Music".to_owned(),
            backup_arn:
                "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01757500000123-0badcafe"
                    .to_owned(),
            backup_name: "nightly".to_owned(),
            status: BackupStatus::Available,
            store: StoreIdentity {
                store_kind: StoreKind::Filesystem,
                store_root: "/backups".to_owned(),
                store_id: "store-1".to_owned(),
            },
            size_bytes: 1234,
            item_count: 5,
            created_at: 1_757_500_000.123,
            completed_at: None,
            failure_reason: None,
            source_table_details_json: serde_json::json!({ "table_name": "Music" }),
            owner_generation: 99,
            heartbeat_at: 0.0,
        };
        let read = BackupRow {
            owner_generation: UNOWNED_GENERATION,
            completed_at: Some(1_757_500_001.0),
            heartbeat_at: 1_757_500_001.0,
            created_at: 1_757_500_000.123_4,
            ..row.clone()
        };
        assert!(is_unowned_insert_of(&read, &row));
        let differing: [fn(&mut BackupRow); 11] = [
            |r| r.owner_generation = 7,
            |r| r.backup_id = "01757500000123-00000000".to_owned(),
            |r| r.table_name = "Other".to_owned(),
            |r| r.backup_name = "other".to_owned(),
            |r| r.status = BackupStatus::Creating,
            |r| r.store.store_id = "store-2".to_owned(),
            |r| r.size_bytes = 1,
            |r| r.item_count = 1,
            |r| r.created_at = 1_757_500_000.124,
            |r| r.failure_reason = Some("x".to_owned()),
            |r| r.source_table_details_json = serde_json::json!({ "table_name": "Other" }),
        ];
        for (i, change) in differing.iter().enumerate() {
            let mut other = read.clone();
            change(&mut other);
            assert!(!is_unowned_insert_of(&other, &row), "change {i}");
        }
    }
}
