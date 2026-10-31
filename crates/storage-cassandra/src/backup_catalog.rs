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
//! it (`coordinator_now_millis`), never an application host's.
//!
//! DeleteBackup's lock is the lightweight transaction on the `backups_by_arn`
//! row, `IF backup_status = 'AVAILABLE' AND active_restores = null`, on the
//! same partition a restore registers on, so the two order wholly one before
//! the other, as the in-catalog paths do.
//!
//! A status change moves the row's `backup_index_by_status` entry with a
//! plain batch after the lightweight transaction on the row. A process that
//! dies between the two leaves an entry whose status the row no longer
//! matches; every reader of the status table re-reads the row and skips such
//! an entry, and `remove_backup_row` deletes the entry under the status it
//! read, so the orphan costs a wasted read until then. The sweep that
//! removes orphan entries is `backup sync`'s, in a later release.
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
//! row see one win. An insert whose answer was lost is settled by a re-read:
//! a row carrying the call's own generation is its claim. Heartbeats and the
//! release are lightweight transactions on the generation. The two reads of
//! the claim row that decide whether another process's claim is live
//! (`check_maintenance_claim`, the row before a takeover) run at
//! `LOCAL_SERIAL`, so a heartbeat an LWT just wrote is seen; the RUNNING
//! restore records `list_dead_restore_jobs` scans are a range read, where a
//! serial level is a local quorum with no Paxos read, so the scan reads at
//! quorum and the coordinator clock after it.

use std::time::Duration;

use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::error::Error as DriverError;
use cdrs_tokio::frame::message_error::ErrorType;
use cdrs_tokio::query::{BatchQueryBuilder, QueryValues};
use cdrs_tokio::statement::StatementParamsBuilder;
use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::rows::Row;
use cdrs_tokio::types::value::Value;
use extenddb_core::backup_format::BackupArn;
use extenddb_storage::backup_catalog::{
    BackupCatalog, BackupKey, BackupRow, BackupStatus, DeleteStart, Heartbeat, JobClaim, JobRow,
    JobStatus, MaintenanceClaim, RestoreJobRow, RestoreKind, RestoreTargetKey, StatusFields,
    StoreIdentity, StoreKind, TimeBounds, WriteOutcome, billing_mode_name, new_generation,
};
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::CassandraEngine;
use crate::engine::CassandraSession;

/// Attempts `start_backup_delete` makes when a restore registers or
/// unregisters between its in-use check and the DELETING claim.
const DELETE_CLAIM_ATTEMPTS: usize = 3;

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
    let rows = crate::cassandra_util::query_rows::<StorageError>(
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
async fn conditional_write(
    session: &CassandraSession,
    query: &str,
    values: QueryValues,
) -> Result<WriteOutcome, StorageError> {
    let params = StatementParamsBuilder::new()
        .with_consistency(Consistency::LocalQuorum)
        .with_serial_consistency(Consistency::LocalSerial)
        .with_values(values)
        .build();
    match session.query_with_params(query, params).await {
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
            let rows = crate::cassandra_util::query_rows_quorum::<StorageError>(
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
        let batch = BatchQueryBuilder::new()
            .with_consistency(Consistency::LocalQuorum)
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
            .batch(
                batch
                    .build()
                    .map_err(|e| StorageError::Internal(e.to_string()))?,
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Remove backup index rows: {e}")))?;
        Ok(())
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
        let rows = crate::cassandra_util::query_rows_quorum::<StorageError>(
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
    /// is logged: a reader of the status index re-reads the authoritative
    /// row and skips an entry whose status it no longer matches.
    async fn move_status_index_row(
        &self,
        key: &BackupKey,
        store: &StoreIdentity,
        from: &str,
        to: &str,
    ) {
        let catalog = self.catalog_keyspace();
        let batch = BatchQueryBuilder::new()
            .with_consistency(Consistency::LocalQuorum)
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
            Ok(batch) => self.session.batch(batch).await.map(|_| ()),
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

    /// The ARNs the status index lists for `store` under `status`.
    async fn status_index_arns(
        &self,
        store: &StoreIdentity,
        status: BackupStatus,
    ) -> Result<Vec<(String, String)>, StorageError> {
        let query = format!(
            "SELECT backup_arn, account_id, store_kind, store_id FROM {}.backup_index_by_status \
             WHERE store_root = ? AND backup_status = ?",
            self.catalog_keyspace()
        );
        let rows = crate::cassandra_util::query_rows_quorum::<StorageError>(
            &self.session,
            &query,
            cdrs_tokio::query_values!(store.store_root.as_str(), status.as_str()),
            "status index",
        )
        .await?;
        let mut arns = Vec::with_capacity(rows.len());
        for row in rows {
            let store_kind: Option<String> = row.get_by_name("store_kind").ok().flatten();
            let store_id: Option<String> = row.get_by_name("store_id").ok().flatten();
            if store_kind.as_deref() != Some(store.store_kind.as_str())
                || store_id.as_deref() != Some(store.store_id.as_str())
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
        let rows = crate::cassandra_util::query_rows_quorum::<StorageError>(
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

impl BackupCatalog for CassandraEngine {
    fn catalog_id(&self) -> BoxFuture<'_, Result<String, StorageError>> {
        Box::pin(async move {
            let query = format!(
                "SELECT value FROM {}.settings WHERE key = 'catalog_id'",
                self.catalog_keyspace()
            );
            let rows = crate::cassandra_util::query_rows_quorum::<StorageError>(
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
            self.require_migrated_catalog().await?;
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
            let created_at = seconds_to_millis(row.created_at);
            let batch = BatchQueryBuilder::new()
                .with_consistency(Consistency::LocalQuorum)
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
                        source.table_id.as_str(),
                        row.table_name.as_str(),
                        source.table_arn.as_str(),
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
                        source.table_id.as_str(),
                        source.table_arn.as_str(),
                        row.size_bytes
                    ),
                )
                .add_query(
                    format!(
                        "INSERT INTO {catalog}.backup_index_by_status \
                         (store_root, backup_status, backup_arn, account_id, store_kind, store_id) \
                         VALUES (?, 'CREATING', ?, ?, ?, ?)"
                    ),
                    cdrs_tokio::query_values!(
                        row.store.store_root.as_str(),
                        row.backup_arn.as_str(),
                        row.account_id.as_str(),
                        row.store.store_kind.as_str(),
                        row.store.store_id.as_str()
                    ),
                );
            self.session
                .batch(
                    batch
                        .build()
                        .map_err(|e| StorageError::Internal(e.to_string()))?,
                )
                .await
                .map_err(|e| StorageError::Internal(format!("Backup index rows: {e}")))?;
            Ok(Box::new(CassandraBackupClaim {
                engine: self.clone(),
                key: row.key(),
                store: row.store.clone(),
                generation,
            }) as Box<dyn JobClaim>)
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
            let params = StatementParamsBuilder::new()
                .with_consistency(Consistency::LocalQuorum)
                .with_values(QueryValues::SimpleValues(values))
                .build();
            let listed = self
                .session
                .query_with_params(&query, params)
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
        Box::pin(async move {
            let mark_deleting = format!(
                "UPDATE {}.backups_by_arn SET backup_status = 'DELETING' \
                 WHERE account_id = ? AND backup_arn = ? \
                 IF backup_status = 'AVAILABLE' AND active_restores = null",
                self.catalog_keyspace()
            );
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
                // The in-use rule reads the target's status; a registration
                // whose target is no longer being restored is cleared here so
                // it cannot hold the row's lock.
                if let Some(target) = self.creating_restore_target(&stored).await? {
                    return Ok(DeleteStart::InUse(target));
                }
                crate::test_hooks::before_delete_claim(&key.backup_arn).await;
                let result = crate::cassandra_util::query_lwt(
                    &self.session,
                    &mark_deleting,
                    cdrs_tokio::query_values!(key.account_id.as_str(), key.backup_arn.as_str()),
                )
                .await?;
                if crate::cassandra_util::lwt_applied(&result)? {
                    let store = StoreIdentity {
                        store_kind: StoreKind::parse(stored.store_kind.as_deref().unwrap_or(""))
                            .unwrap_or(StoreKind::Filesystem),
                        store_root: stored.store_root.clone().unwrap_or_default(),
                        store_id: stored.store_id.clone().unwrap_or_default(),
                    };
                    self.move_status_index_row(key, &store, "AVAILABLE", "DELETING")
                        .await;
                    return Ok(DeleteStart::Deleting);
                }
                attempt += 1;
                if attempt >= DELETE_CLAIM_ATTEMPTS {
                    // A registration landed between the check and the claim
                    // on every attempt: the backup is in use by it.
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
        })
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
            let query = format!(
                "UPDATE {}.backups_by_arn SET backup_status = 'DELETED', \
                 completed_at = toTimestamp(now()) \
                 WHERE account_id = ? AND backup_arn = ? IF backup_status = 'DELETING'",
                self.catalog_keyspace()
            );
            let result = crate::cassandra_util::query_lwt(
                &self.session,
                &query,
                cdrs_tokio::query_values!(key.account_id.as_str(), key.backup_arn.as_str()),
            )
            .await?;
            let applied = crate::cassandra_util::lwt_applied(&result)?;
            if applied {
                self.move_status_index_row(key, &row.store, "DELETING", "DELETED")
                    .await;
            }
            Ok(applied)
        })
    }

    fn remove_backup_row<'a>(
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
            if !matches!(row.status, BackupStatus::Deleted | BackupStatus::Failed) {
                return Ok(false);
            }
            // The authoritative row first, conditioned on the tombstone
            // status read; then its index entries.
            let delete = format!(
                "DELETE FROM {}.backups_by_arn WHERE account_id = ? AND backup_arn = ? \
                 IF backup_status = ?",
                self.catalog_keyspace()
            );
            let result = crate::cassandra_util::query_lwt(
                &self.session,
                &delete,
                cdrs_tokio::query_values!(
                    key.account_id.as_str(),
                    key.backup_arn.as_str(),
                    row.status.as_str()
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
            self.creating_restore_target(&stored).await
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
            let columns = "account_id, table_name, table_id, table_status, \
                           restore_source_backup_arn, restore_kind, restore_status, \
                           restore_owner_generation, restore_heartbeat_at, restore_completed_at";
            let mut scanned = Vec::new();
            for status in ["RUNNING", "FAILED"] {
                let query = format!(
                    "SELECT {columns} FROM {}.tables WHERE restore_status = ? ALLOW FILTERING",
                    self.catalog_keyspace()
                );
                let rows = crate::cassandra_util::query_rows_quorum::<StorageError>(
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
                    let column = |name: &str| -> Result<String, StorageError> {
                        crate::cassandra_util::get_column(&row, name, "dead restore jobs")
                    };
                    // A RUNNING record always carries a heartbeat (the insert
                    // or the backfill wrote it); one without is as dead as the
                    // epoch.
                    let age_of = if status == "FAILED" {
                        let target = RestoreTargetRow {
                            table_id: column("table_id")?,
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
                    let kind = column("restore_kind")?;
                    let generation: Option<i64> =
                        row.get_by_name("restore_owner_generation").ok().flatten();
                    dead.push(RestoreJobRow {
                        target: RestoreTargetKey {
                            account_id: column("account_id")?,
                            table_name: column("table_name")?,
                            table_id: column("table_id")?,
                        },
                        source_backup_arn: column("restore_source_backup_arn")?,
                        kind: RestoreKind::parse(&kind)?,
                        status: JobStatus::parse(status)?,
                        owner_generation: generation_from_column(generation),
                        heartbeat_at: heartbeat_at.map_or(0.0, millis_to_seconds),
                    });
                }
            }
            Ok(dead)
        })
    }

    fn fail_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        Box::pin(async move {
            let query = format!(
                "UPDATE {}.tables SET restore_status = 'FAILED', restore_failure_reason = ?, \
                 restore_completed_at = toTimestamp(now()) \
                 WHERE account_id = ? AND table_name = ? \
                 IF table_id = ? AND restore_status = 'RUNNING'",
                self.catalog_keyspace()
            );
            let result = crate::cassandra_util::query_lwt(
                &self.session,
                &query,
                cdrs_tokio::query_values!(
                    reason,
                    key.account_id.as_str(),
                    key.table_name.as_str(),
                    key.table_id.as_str()
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
            // left by a crash names the same store its row did).
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
                let rows = crate::cassandra_util::query_rows_quorum::<StorageError>(
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
                if taken != WriteOutcome::Matched {
                    return Err(refused());
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
}
