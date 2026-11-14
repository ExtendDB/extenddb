// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The catalog index of backups and restore jobs, and the frozen reader of
//! the in-catalog backups earlier releases wrote.
//!
//! The engine (`crates/engine/src/backup.rs`) is backend-agnostic, and every
//! write to a catalog row (the claim, the heartbeat, the conditional status
//! writes, the store-scoped listings, the restore job record) has to be
//! written by the backend that holds the row. [`BackupCatalog`] is how the
//! engine reaches the index; [`JobClaim`] is the handle a job holds on its
//! row; [`LegacyBackupReader`] is the frozen path to backups stored in the
//! catalog itself (`backups` and `backup_items`), which stays until that
//! format is retired.
//!
//! Rows are addressed by [`BackupKey`] (account, backup id, ARN), built from
//! a parsed [`BackupArn`]; no method takes a bare backup id. A restore target
//! is addressed by [`RestoreTargetKey`] (account, name, table id), the table
//! id guarding against a table re-created under the name.
//!
//! Status mapping to the API ([`BackupStatus::reported`]): CREATING and
//! AVAILABLE as themselves; DELETING is reported as DELETED by DescribeBackup
//! and DeleteBackup and is not listed; DELETED and FAILED rows are tombstones
//! that are neither described nor listed.

use std::sync::Arc;
use std::time::Duration;

use extenddb_core::backup_format::{BackupArn, SourceTableSchema};
use extenddb_core::types::{
    BackupDescription, BackupDetails, BackupSummary, BillingMode, CreateTableInput,
    GlobalSecondaryIndexInfo, Item, LocalSecondaryIndexInfo, ProvisionedThroughput,
    SourceTableDetails, SourceTableFeatureDetails, TableDescription,
};
use futures::future::BoxFuture;
use futures::stream::BoxStream;

use crate::backup_definition::{BackupRestoreSource, RestoreRequest};
use crate::error::StorageError;

/// `BackupType` of every backup this server creates.
pub const BACKUP_TYPE_USER: &str = "USER";

/// The sentinel `owner_generation` the migration writes on a restore
/// registration that existed before the job record had one; no job ever
/// holds it.
pub const UNOWNED_GENERATION: u64 = 0;

/// A fresh `owner_generation`: random, never [`UNOWNED_GENERATION`], and
/// below `i64::MAX` so every backend's integer column holds it.
#[must_use]
pub fn new_generation() -> u64 {
    loop {
        let g = rand::random::<u64>() >> 1;
        if g != UNOWNED_GENERATION {
            return g;
        }
    }
}

/// Connections a `[storage.postgres] pool_size` must have for
/// `max_concurrent_jobs` jobs: twice the jobs, so a backend that admits at
/// most half its pool as open snapshots (#395) still admits every job the
/// permit allows, plus ten for requests. The caller takes the larger of this
/// and its own minimum pool size. A job's claim connection is outside the
/// pool and not counted here.
#[must_use]
pub fn pool_size_for_jobs(max_concurrent_jobs: usize) -> u32 {
    let jobs = u32::try_from(max_concurrent_jobs).unwrap_or(u32::MAX);
    jobs.saturating_mul(2).saturating_add(10)
}

/// The floor of a job's heartbeat period on every backend: the store-backed
/// jobs run at `max([backup] heartbeat_interval_secs, JOB_HEARTBEAT_INTERVAL)`
/// (`BackupConfig::effective_heartbeat_interval_secs`, which the server
/// hands the jobs), the in-catalog paths and the maintenance claim at the
/// constant. `BackupConfig::validate` floors `stale_job_after_secs` on this
/// period, so a configured interval shorter than it cannot make the
/// reconciler declare a live job dead between two heartbeats.
pub const JOB_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// The bound a job refusal names, so the text says which one was met
/// (an operator reading the engine's `max_concurrent_jobs` refusal and
/// SQLite's slot refusal, which differ in number, can tell them apart).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobBound<'a> {
    /// A counted bound of this instance: `name` (the config key or the
    /// backend's slot formula) admits `limit` jobs at once.
    Counted { name: &'a str, limit: usize },
    /// The database server refused the job's dedicated connection
    /// (PostgreSQL's `too_many_connections`): the bound is the server's own
    /// connection limit, which this process does not know.
    ServerConnections,
}

/// The text of the `LimitExceededException` a CreateBackup or
/// RestoreTableFromBackup answers when a bound on running jobs is met. One
/// implementation: the engine's `max_concurrent_jobs` permit, a backend's own
/// admission (SQLite's snapshot slots) and PostgreSQL's claim connection
/// answer the same wire refusal, each naming its bound.
#[must_use]
pub fn limit_exceeded_message(bound: JobBound<'_>) -> String {
    let detail = match bound {
        JobBound::Counted { name, limit } => format!("at most {limit} at once, the {name} bound"),
        JobBound::ServerConnections => {
            "the database server has no connection slot left for the job".to_owned()
        }
    };
    format!("Too many backup and restore jobs are running on this server ({detail}); retry later")
}

/// Where a backup's bytes live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoreKind {
    Filesystem,
    S3,
}

impl StoreKind {
    /// The value stored in the `store_kind` column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Filesystem => "filesystem",
            Self::S3 => "s3",
        }
    }

    /// The kind a `store_kind` column holds.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "filesystem" => Some(Self::Filesystem),
            "s3" => Some(Self::S3),
            _ => None,
        }
    }
}

impl std::fmt::Display for StoreKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The store a row belongs to: kind, the configured root at creation time,
/// and the id in the root's `extenddb-store.json`. Every duty that reads or
/// writes a backup row acts only on rows of the configured store.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StoreIdentity {
    pub store_kind: StoreKind,
    pub store_root: String,
    pub store_id: String,
}

/// What [`BackupCatalog::remove_orphan_status_entries`] did with a root's
/// status entries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatusEntrySweep {
    /// Entries deleted: their row is gone or names another store.
    pub removed: u64,
    /// Entries whose row is of the root under another status, rewritten
    /// under the row's status (the stale entry deleted).
    pub repaired: u64,
}

/// One line of [`BackupCatalog::summarize_store_roots`]: the catalog holds
/// `rows` rows of `status` under `store`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreRootCount {
    pub store: StoreIdentity,
    pub status: BackupStatus,
    pub rows: u64,
}

impl StoreRootCount {
    /// A count as a grouping query returns it: the `store_kind`,
    /// `store_root`, `store_id` and `status` columns and the count.
    ///
    /// # Errors
    ///
    /// `Internal` for a kind or status no release writes.
    pub fn from_columns(
        store_kind: &str,
        store_root: String,
        store_id: String,
        status: &str,
        rows: i64,
    ) -> Result<Self, StorageError> {
        let kind = StoreKind::parse(store_kind).ok_or_else(|| {
            StorageError::Internal(format!(
                "backup rows of root {store_root:?} carry an unknown store kind {store_kind:?}"
            ))
        })?;
        Ok(Self {
            store: StoreIdentity {
                store_kind: kind,
                store_root,
                store_id,
            },
            status: BackupStatus::parse(status)?,
            rows: u64::try_from(rows).unwrap_or_default(),
        })
    }

    /// The order [`BackupCatalog::summarize_store_roots`] returns on every
    /// backend: by kind, root and id, compared as bytes, then by status in
    /// [`BackupStatus::ALL`]'s order, whatever the database's collation.
    pub fn sort(counts: &mut [Self]) {
        counts.sort_by(|a, b| {
            fn key(c: &StoreRootCount) -> (&str, &[u8], &[u8], Option<usize>) {
                (
                    c.store.store_kind.as_str(),
                    c.store.store_root.as_bytes(),
                    c.store.store_id.as_bytes(),
                    BackupStatus::ALL.iter().position(|s| *s == c.status),
                )
            }
            key(a).cmp(&key(b))
        });
    }

    /// One count per (store, status) of `rows`, sorted: the tally a backend
    /// that cannot group in its query (Cassandra) makes of what it read.
    #[must_use]
    pub fn tally(rows: impl IntoIterator<Item = (StoreIdentity, BackupStatus)>) -> Vec<Self> {
        let mut counts: std::collections::HashMap<(StoreIdentity, BackupStatus), u64> =
            std::collections::HashMap::new();
        for key in rows {
            *counts.entry(key).or_default() += 1;
        }
        let mut counts: Vec<Self> = counts
            .into_iter()
            .map(|((store, status), rows)| Self {
                store,
                status,
                rows,
            })
            .collect();
        Self::sort(&mut counts);
        counts
    }
}

/// The status column of a backup row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BackupStatus {
    Creating,
    Available,
    Deleting,
    Deleted,
    Failed,
}

impl BackupStatus {
    /// Every status, in the order a row moves through them (CREATING first,
    /// FAILED last): the order a per-status listing or report uses.
    pub const ALL: [Self; 5] = [
        Self::Creating,
        Self::Available,
        Self::Deleting,
        Self::Deleted,
        Self::Failed,
    ];

    /// The value stored in the `status` column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Creating => "CREATING",
            Self::Available => "AVAILABLE",
            Self::Deleting => "DELETING",
            Self::Deleted => "DELETED",
            Self::Failed => "FAILED",
        }
    }

    /// The status a column holds.
    ///
    /// # Errors
    ///
    /// `Internal` for a value no release writes.
    pub fn parse(s: &str) -> Result<Self, StorageError> {
        match s {
            "CREATING" => Ok(Self::Creating),
            "AVAILABLE" => Ok(Self::Available),
            "DELETING" => Ok(Self::Deleting),
            "DELETED" => Ok(Self::Deleted),
            "FAILED" => Ok(Self::Failed),
            other => Err(StorageError::Internal(format!(
                "backup row carries an unknown status {other:?}"
            ))),
        }
    }

    /// The `BackupStatus` the API reports for a row of this status: DELETING
    /// is reported as DELETED, so a client retrying DeleteBackup sees the
    /// terminal status.
    #[must_use]
    pub const fn reported(self) -> &'static str {
        match self {
            Self::Deleting => "DELETED",
            other => other.as_str(),
        }
    }

    /// Whether DescribeBackup answers for a row of this status (CREATING,
    /// AVAILABLE, and DELETING as DELETED). DELETED and FAILED rows are
    /// `BackupNotFoundException`.
    #[must_use]
    pub const fn is_described(self) -> bool {
        matches!(self, Self::Creating | Self::Available | Self::Deleting)
    }

    /// Whether ListBackups lists a row of this status (CREATING and
    /// AVAILABLE only).
    #[must_use]
    pub const fn is_listed(self) -> bool {
        matches!(self, Self::Creating | Self::Available)
    }
}

impl std::fmt::Display for BackupStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One backup row's address: the account, the backup id and the ARN as the
/// row stores it. Built from a parsed [`BackupArn`] or from a row read.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BackupKey {
    pub account_id: String,
    pub backup_id: String,
    pub backup_arn: String,
}

impl BackupKey {
    /// The key of the row `arn` names.
    #[must_use]
    pub fn from_arn(arn: &BackupArn) -> Self {
        Self {
            account_id: arn.account_id().to_owned(),
            backup_id: arn.backup_id().to_owned(),
            backup_arn: arn.to_arn_string(),
        }
    }
}

/// One restore target's address. The table id guards against a table
/// re-created under the same name after the target was removed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RestoreTargetKey {
    pub account_id: String,
    pub table_name: String,
    pub table_id: String,
}

/// The `TimeRangeLowerBound` (inclusive) and `TimeRangeUpperBound`
/// (exclusive) of ListBackups, epoch seconds, applied to `created_at`.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct TimeBounds {
    pub lower: Option<f64>,
    pub upper: Option<f64>,
}

impl TimeBounds {
    /// Whether `created_at` (epoch seconds) is within the bounds.
    #[must_use]
    pub fn contains(&self, created_at: f64) -> bool {
        self.lower.is_none_or(|lower| created_at >= lower)
            && self.upper.is_none_or(|upper| created_at < upper)
    }
}

/// A row of `backup_index`. Times are epoch seconds; `created_at` is the
/// id's instant, so `BackupCreationDateTime`, the ARN and the ListBackups
/// cursor agree.
#[derive(Debug, Clone, PartialEq)]
pub struct BackupRow {
    pub backup_id: String,
    pub account_id: String,
    pub table_name: String,
    pub backup_arn: String,
    pub backup_name: String,
    pub status: BackupStatus,
    pub store: StoreIdentity,
    pub size_bytes: i64,
    pub item_count: i64,
    pub created_at: f64,
    pub completed_at: Option<f64>,
    pub failure_reason: Option<String>,
    /// The manifest's `source_table` object, verbatim
    /// ([`SourceTableSchema`]); the API's source-table shapes derive from it.
    pub source_table_details_json: serde_json::Value,
    pub owner_generation: u64,
    /// The last heartbeat by the owning job, on the database's clock.
    pub heartbeat_at: f64,
}

impl BackupRow {
    /// The row's address.
    #[must_use]
    pub fn key(&self) -> BackupKey {
        BackupKey {
            account_id: self.account_id.clone(),
            backup_id: self.backup_id.clone(),
            backup_arn: self.backup_arn.clone(),
        }
    }

    /// The source table the row recorded.
    ///
    /// # Errors
    ///
    /// `Internal` when `source_table_details_json` is not a manifest
    /// `source_table` object.
    pub fn source_table(&self) -> Result<SourceTableSchema, StorageError> {
        serde_json::from_value(self.source_table_details_json.clone()).map_err(|e| {
            StorageError::Internal(format!(
                "backup {} carries a source table this build cannot read: {e}",
                self.backup_arn
            ))
        })
    }

    /// `BackupDetails` as DescribeBackup and DeleteBackup report them, with
    /// the status mapped by [`BackupStatus::reported`].
    #[must_use]
    pub fn details(&self) -> BackupDetails {
        BackupDetails {
            backup_arn: self.backup_arn.clone(),
            backup_name: self.backup_name.clone(),
            backup_status: self.status.reported().to_owned(),
            backup_type: BACKUP_TYPE_USER.to_owned(),
            backup_size_bytes: self.size_bytes,
            backup_creation_date_time: self.created_at,
        }
    }

    /// The DescribeBackup description: the details, `SourceTableDetails` and
    /// `SourceTableFeatureDetails` derived from the recorded source table.
    /// `SourceTableDetails.ProvisionedThroughput` is always set (`{0, 0}` for
    /// an on-demand table, as the service reports it); a GSI's throughput is
    /// `{0, 0}` on an on-demand table too. `SourceTableFeatureDetails` is
    /// always set, `{}` for a source that recorded no index, stream, TTL or
    /// SSE (as the service reports it, measured on DynamoDB); the inner
    /// members the source recorded nothing for are omitted.
    ///
    /// # Errors
    ///
    /// As [`Self::source_table`].
    pub fn description(&self) -> Result<BackupDescription, StorageError> {
        let source = self.source_table()?;
        let zero = ProvisionedThroughput {
            read_capacity_units: 0,
            write_capacity_units: 0,
        };
        let on_demand = source.billing_mode == BillingMode::PayPerRequest;
        let provisioned_throughput = if on_demand {
            Some(zero.clone())
        } else {
            source
                .provisioned_throughput
                .clone()
                .or_else(|| Some(zero.clone()))
        };
        let features = SourceTableFeatureDetails {
            local_secondary_indexes: (!source.local_secondary_indexes.is_empty()).then(|| {
                source
                    .local_secondary_indexes
                    .iter()
                    .map(|i| LocalSecondaryIndexInfo {
                        index_name: i.index_name.clone(),
                        key_schema: i.key_schema.clone(),
                        projection: i.projection.clone(),
                    })
                    .collect()
            }),
            global_secondary_indexes: (!source.global_secondary_indexes.is_empty()).then(|| {
                source
                    .global_secondary_indexes
                    .iter()
                    .map(|i| GlobalSecondaryIndexInfo {
                        index_name: i.index_name.clone(),
                        key_schema: i.key_schema.clone(),
                        projection: i.projection.clone(),
                        provisioned_throughput: if on_demand {
                            Some(zero.clone())
                        } else {
                            i.provisioned_throughput.clone()
                        },
                    })
                    .collect()
            }),
            stream_description: source.stream_specification.clone(),
            time_to_live_description: source.time_to_live.clone(),
            sse_description: source.sse_description.clone(),
        };
        Ok(BackupDescription {
            backup_details: self.details(),
            source_table_details: SourceTableDetails {
                table_name: source.table_name.clone(),
                table_id: source.table_id.clone(),
                table_arn: source.table_arn.clone(),
                key_schema: source.key_schema.clone(),
                item_count: source.item_count,
                table_size_bytes: source.table_size_bytes,
                billing_mode: Some(billing_mode_name(source.billing_mode).to_owned()),
                table_creation_date_time: source.table_creation_date_time,
                provisioned_throughput,
                on_demand_throughput: source.on_demand_throughput.clone(),
            },
            source_table_feature_details: Some(features),
        })
    }

    /// The ListBackups summary. It needs only `table_id` and `table_arn` of
    /// the recorded source table, and reads those two members directly, so a
    /// row whose `source_table_details_json` a later build extended with a
    /// member this build requires still lists; DescribeBackup of the same
    /// row is the internal error [`Self::description`] reports.
    ///
    /// # Errors
    ///
    /// `Internal` when the recorded source carries no `table_id` or
    /// `table_arn` string.
    pub fn summary(&self) -> Result<BackupSummary, StorageError> {
        let member = |name: &str| -> Result<String, StorageError> {
            self.source_table_details_json
                .get(name)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| {
                    StorageError::Internal(format!(
                        "backup {} carries a source table without a {name} this build can read",
                        self.backup_arn
                    ))
                })
        };
        Ok(BackupSummary {
            backup_arn: self.backup_arn.clone(),
            backup_name: self.backup_name.clone(),
            table_name: self.table_name.clone(),
            table_id: member("table_id")?,
            table_arn: member("table_arn")?,
            backup_status: self.status.reported().to_owned(),
            backup_type: BACKUP_TYPE_USER.to_owned(),
            backup_size_bytes: self.size_bytes,
            backup_creation_date_time: self.created_at,
        })
    }
}

/// The wire name of a billing mode.
#[must_use]
pub const fn billing_mode_name(mode: BillingMode) -> &'static str {
    match mode {
        BillingMode::Provisioned => "PROVISIONED",
        BillingMode::PayPerRequest => "PAY_PER_REQUEST",
    }
}

/// What `start_backup_delete` found under the row's lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteStart {
    /// The row was AVAILABLE and is now DELETING.
    Deleting,
    /// A restore target registered from the backup is CREATING; its name.
    InUse(String),
    /// The row was not AVAILABLE; its status.
    Status(BackupStatus),
}

/// What [`BackupCatalog::forget_available_backup_row`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForgetRow {
    /// The row was AVAILABLE and is gone.
    Removed,
    /// A restore target registered from the backup is CREATING; its name.
    /// The row is left.
    InUse(String),
    /// The row was not AVAILABLE; its status. The row is left.
    Status(BackupStatus),
}

/// What a heartbeat found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Heartbeat {
    /// The row still carries the claim's generation and the claim is held.
    Confirmed,
    /// The row is gone, carries another generation, or the claim connection
    /// no longer holds the claim. The caller treats an `Err` the same way.
    Lost,
}

/// The status a job record moves through: CREATING, AVAILABLE and FAILED for
/// a backup row; RUNNING, DONE and FAILED for a restore job record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobStatus {
    Creating,
    Available,
    Running,
    Done,
    Failed,
}

impl JobStatus {
    /// The value stored in the record's status column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Creating => "CREATING",
            Self::Available => "AVAILABLE",
            Self::Running => "RUNNING",
            Self::Done => "DONE",
            Self::Failed => "FAILED",
        }
    }

    /// The status a column holds.
    ///
    /// # Errors
    ///
    /// `Internal` for a value no release writes.
    pub fn parse(s: &str) -> Result<Self, StorageError> {
        match s {
            "CREATING" => Ok(Self::Creating),
            "AVAILABLE" => Ok(Self::Available),
            "RUNNING" => Ok(Self::Running),
            "DONE" => Ok(Self::Done),
            "FAILED" => Ok(Self::Failed),
            other => Err(StorageError::Internal(format!(
                "job record carries an unknown status {other:?}"
            ))),
        }
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The fields a status write sets beside the status. A `None` leaves the
/// column as it is; `completed` stamps `completed_at` with the database's
/// `now()`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatusFields {
    pub size_bytes: Option<i64>,
    pub item_count: Option<i64>,
    pub failure_reason: Option<String>,
    /// The manifest's `source_table` object, which the AVAILABLE write of a
    /// backup row carries so DescribeBackup reports the counts the manifest
    /// has; a restore job record has no such column and ignores it.
    pub source_table_details_json: Option<serde_json::Value>,
    pub completed: bool,
}

/// The column an age in [`BackupCatalog::list_backup_rows_older_than`] is
/// measured on, against the database's own `now()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StaleClock {
    /// `heartbeat_at`: the owner's last sign of life.
    Heartbeat,
    /// `completed_at`: when the row reached its terminal status. A row with
    /// `completed_at` unset is never older than any age on this clock.
    Completed,
}

/// What a conditional write did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOutcome {
    /// Exactly one row matched the condition and was written.
    Matched,
    /// No row matched: the record is gone, carries another generation, or
    /// is not in the `from` status.
    NotMatched,
    /// The statement may or may not have been applied (the connection failed
    /// after it was sent). The caller re-reads the record ([`JobClaim::read`])
    /// rather than treating this as a failure.
    Unknown,
}

/// A job record as [`JobClaim::read`] returns it.
#[derive(Debug, Clone, PartialEq)]
pub struct JobRow {
    pub status: JobStatus,
    pub owner_generation: u64,
    /// Epoch seconds, on the database's clock.
    pub heartbeat_at: f64,
    pub failure_reason: Option<String>,
}

/// How a restore job reads its backup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RestoreKind {
    /// Through the [`LegacyBackupReader`]: an in-catalog backup.
    Legacy,
    /// From a manifest in the store.
    Manifest,
}

impl RestoreKind {
    /// The value stored in the `kind` column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Manifest => "manifest",
        }
    }

    /// The kind a column holds.
    ///
    /// # Errors
    ///
    /// `Internal` for a value no release writes.
    pub fn parse(s: &str) -> Result<Self, StorageError> {
        match s {
            "legacy" => Ok(Self::Legacy),
            "manifest" => Ok(Self::Manifest),
            other => Err(StorageError::Internal(format!(
                "restore job carries an unknown kind {other:?}"
            ))),
        }
    }
}

/// A restore job record: the row [`BackupCatalog::claim_restore`] writes
/// beside a CREATING target, as [`BackupCatalog::read_restore_job`] and
/// [`BackupCatalog::list_restore_jobs`] return it, and the one the
/// reconciler acts on (RUNNING with a dead owner, or FAILED with its target
/// still CREATING, [`BackupCatalog::list_dead_restore_jobs`]).
#[derive(Debug, Clone, PartialEq)]
pub struct RestoreJobRow {
    pub target: RestoreTargetKey,
    pub source_backup_arn: String,
    pub kind: RestoreKind,
    /// RUNNING, DONE or FAILED; the reconciler reads it to decide whether
    /// the FAILED write is still to be made.
    pub status: JobStatus,
    pub owner_generation: u64,
    /// Epoch seconds, on the database's clock.
    pub heartbeat_at: f64,
    /// The store the job reads its backup from; `None` on a legacy record,
    /// which reads the catalog.
    pub store: Option<StoreIdentity>,
    /// `<hostname>:<spool path>` of the instance that downloads the backup
    /// into its spool; `None` for a filesystem store, which is read in
    /// place, and on a legacy record. A takeover rewrites it to the
    /// claimant's.
    pub spool_host: Option<String>,
    /// The last data file whose progress the job recorded, as the backend
    /// records it (SQLite and Cassandra write it per batch, MongoDB per
    /// file; PostgreSQL keeps its progress in the data database and leaves
    /// this `None`).
    pub file_index: Option<u32>,
    /// Items of `file_index` applied so far, where the backend records
    /// mid-file progress (SQLite and Cassandra).
    pub items_applied: Option<u64>,
    pub failure_reason: Option<String>,
    /// Epoch seconds, on the database's clock; set by the DONE and FAILED
    /// writes.
    pub completed_at: Option<f64>,
}

impl RestoreJobRow {
    /// The row as the four backends' `list_dead_restore_jobs` built it
    /// before the restore job existed: the six members the reconciler's
    /// duties on legacy and dead records read, the rest `None`.
    #[must_use]
    pub fn reconciler_row(
        target: RestoreTargetKey,
        source_backup_arn: String,
        kind: RestoreKind,
        status: JobStatus,
        owner_generation: u64,
        heartbeat_at: f64,
    ) -> Self {
        Self {
            target,
            source_backup_arn,
            kind,
            status,
            owner_generation,
            heartbeat_at,
            store: None,
            spool_host: None,
            file_index: None,
            items_applied: None,
            failure_reason: None,
            completed_at: None,
        }
    }
}

/// What [`BackupCatalog::claim_restore`] returns: the CREATING target's
/// description with its `RestoreSummary`, and the claim on its record.
pub type RestoreRegistered = (TableDescription, Box<dyn JobClaim>);

/// What a restore's registration writes beside the target table it creates
/// (the design's 5.3 step 4): the backup the job reads, the provenance the
/// target's `RestoreSummary` reports, the store, the claimant's spool and
/// the claim's generation. The record is born RUNNING and `kind = manifest`.
#[derive(Debug, Clone, PartialEq)]
pub struct RestoreRegistrationRow {
    pub backup: BackupKey,
    /// Reported as `RestoreSummary.SourceTableArn`.
    pub source_table_arn: String,
    /// Reported as `RestoreSummary.RestoreDateTime`, epoch seconds: the
    /// backup's creation time, as the service reports it.
    pub restore_date_time: f64,
    pub store: StoreIdentity,
    /// `<hostname>:<spool path>` when the job downloads into a spool (an S3
    /// store); `None` for a filesystem store.
    pub spool_host: Option<String>,
    /// The claim's fencing token, written on the record with the target.
    pub owner_generation: u64,
}

/// The names of the pause points inside the backends' restore writes, one
/// spelling each: the backend passes its constant to
/// [`restore_pause_point`], and the kill-point harness names the same
/// constant in `EXTENDDB_BACKUP_PAUSE`. The engine's own kill points are its
/// `KillPoint` enum; these four live here because the backends cannot see
/// the engine.
///
/// SQLite's `bulk_load`: between two batches of one file, after the first
/// batch's transaction committed with its progress.
pub const RESTORE_SQLITE_MID_BATCH: &str = "restore_sqlite_mid_batch";
/// MongoDB's `bulk_load`: after the file's last `insert_many` returned,
/// before the record's `file_index` write.
pub const RESTORE_MONGODB_AFTER_INSERT_MANY: &str = "restore_mongodb_after_insert_many";
/// Cassandra's `bulk_load`: after a batch's writes, before its progress
/// lightweight transaction.
pub const RESTORE_CASSANDRA_BEFORE_PROGRESS: &str = "restore_cassandra_before_progress";
/// PostgreSQL's `finish_restore`: after the ACTIVE flip committed, before
/// the `restore_progress` rows are deleted.
pub const RESTORE_POSTGRES_BEFORE_PROGRESS_DELETE: &str = "restore_postgres_before_progress_delete";

/// A pause point inside a backend's `bulk_load` or its progress write, for
/// the binary-level kill-point harness: with `test-hooks` it pauses while
/// `EXTENDDB_BACKUP_PAUSE` names `name` (one of the constants above), until
/// the file `EXTENDDB_BACKUP_PAUSE_RELEASE` names exists, logging `restore
/// job reached kill point <name>` as the engine's own kill points do (one
/// harness drives both); without the feature it is a no-op.
#[cfg(feature = "test-hooks")]
pub async fn restore_pause_point(name: &str) {
    if std::env::var("EXTENDDB_BACKUP_PAUSE").ok().as_deref() != Some(name) {
        return;
    }
    let release = std::env::var("EXTENDDB_BACKUP_PAUSE_RELEASE").ok();
    tracing::warn!(
        "restore job reached kill point {name}; paused until {} exists",
        release
            .as_deref()
            .unwrap_or("no release file (the pause does not end)")
    );
    loop {
        if let Some(release) = &release
            && std::path::Path::new(release).exists()
        {
            tracing::warn!("restore job released from kill point {name}");
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// [`restore_pause_point`] without `test-hooks`: nothing.
#[cfg(not(feature = "test-hooks"))]
#[allow(clippy::unused_async)]
pub async fn restore_pause_point(_name: &str) {}

/// A legacy CREATING backup row whose create died.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbandonedLegacyBackup {
    pub account_id: String,
    pub backup_arn: String,
    /// Whether the backend removed the row, its items and its definition in
    /// the call that reported it: SQLite does, in one write under its engine
    /// write lock limited to CREATING rows whose slot no task holds, so a
    /// create that commits AVAILABLE and frees its slot between a read and
    /// the removal keeps its backup. The reconciler then only logs it. A
    /// backend that only reports (Cassandra) leaves `false`, and the
    /// reconciler removes the row through
    /// [`LegacyBackupReader::delete_fenced`].
    pub removed: bool,
}

/// One in-catalog backup, as [`LegacyBackupReader::list_all`] (CREATING,
/// AVAILABLE or DELETING) and [`LegacyBackupReader::list_deleted`]
/// (DELETED) list it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyBackupKey {
    pub account_id: String,
    pub backup_arn: String,
    pub status: BackupStatus,
}

/// An AVAILABLE in-catalog backup opened for copying, as
/// [`LegacyBackupReader::items`] answers it.
pub struct LegacyBackupItems {
    /// The backup's name, which its copy keeps.
    pub backup_name: String,
    /// The backup's recorded source, as
    /// [`LegacyBackupReader::restore_source`] answers it.
    pub source: BackupRestoreSource,
    /// What the backend can state about the source table's vector indexes.
    pub vector_state: crate::backup_definition::LegacyVectorState,
    /// Every item the backup stored, in the order the backend stored them
    /// (not key order), each as the legacy restore reads it: parsed from the
    /// stored item JSON, with no validation. The first `Err` ends the stream.
    /// Drop it inside a Tokio runtime (the sqlx backends hold a pooled
    /// connection until it ends).
    pub items: BoxStream<'static, Result<Item, StorageError>>,
}

impl std::fmt::Debug for LegacyBackupItems {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LegacyBackupItems")
            .field("backup_name", &self.backup_name)
            .field("source", &self.source)
            .field("vector_state", &self.vector_state)
            .finish_non_exhaustive()
    }
}

/// What [`LegacyBackupReader::replace_with_index_row`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Replaced {
    /// The index row was written and the in-catalog backup removed, in one
    /// step.
    Replaced,
    /// The index row was there already (an earlier run's, or a `backup sync`
    /// that adopted the copy), so only what was left of the in-catalog
    /// backup was removed: an AVAILABLE in-catalog backup (on MongoDB, a
    /// DELETING one too; on Cassandra, the item buckets of a conversion
    /// that stopped after its transaction). A DELETED or missing in-catalog
    /// backup is left as it is.
    AlreadyReplaced,
    /// The in-catalog backup is not AVAILABLE (missing, another account's,
    /// CREATING, DELETING or DELETED) and no index row of its id exists, or (on
    /// Cassandra, finishing a stopped conversion) the index row is no longer
    /// AVAILABLE; nothing was written but the item buckets' deletion. The
    /// text names what was found.
    NotAvailable(String),
    /// A restore target of the backup is CREATING; nothing was written. The
    /// text is the target's name.
    InUse(String),
    /// The write's condition failed at every attempt (Cassandra's
    /// conversion: a DeleteBackup or a restore's registration raced each
    /// one) and the in-catalog backup is still AVAILABLE; nothing was
    /// written. The text says so; the caller tries again later.
    NotApplied(String),
}

/// One of the `max_concurrent_jobs` slots of this instance, held by a backup
/// or restore job for its life. The engine takes it last among a request's
/// checks and hands it to the backend with the job; dropping it frees the
/// slot.
#[derive(Debug)]
pub struct JobPermit(#[allow(dead_code)] tokio::sync::OwnedSemaphorePermit);

impl JobPermit {
    /// Take a free slot of `slots`, or `None` when every slot is held, without
    /// waiting.
    #[must_use]
    pub fn try_acquire(slots: &Arc<tokio::sync::Semaphore>) -> Option<Self> {
        Arc::clone(slots).try_acquire_owned().ok().map(Self)
    }

    /// Take a slot of `slots`, waiting up to `wait` for one to be freed, or
    /// `None` when none was within that time. The request holds nothing while
    /// it waits: the engine takes the permit last among a request's checks,
    /// before the backend opens a snapshot or a transaction. A free slot is
    /// taken at once, whatever `wait` is.
    pub async fn acquire_within(
        slots: &Arc<tokio::sync::Semaphore>,
        wait: Duration,
    ) -> Option<Self> {
        tokio::time::timeout(wait, Arc::clone(slots).acquire_owned())
            .await
            .ok()
            .and_then(|permit| permit.ok().map(Self))
    }

    /// A permit of a slot nobody else can take: for a caller that runs a
    /// job outside the engine's slots (a tool, a backend's own tests).
    #[must_use]
    pub fn detached() -> Self {
        Self::try_acquire(&Arc::new(tokio::sync::Semaphore::new(1))).expect("a fresh slot")
    }
}

/// The text every backend and the engine answer for a backup a CREATING
/// restore target still reads (`BackupInUseException` on the wire): the
/// service's, which names the backup and not the table (measured on
/// DynamoDB: `Backup is being used to restore another table: <arn>`). One
/// implementation: the refusal is wire-visible and was spelled in each
/// backend before. The table's name goes to the log ([`backup_in_use`]).
#[must_use]
pub fn backup_in_use_message(backup_arn: &str) -> String {
    format!("Backup is being used to restore another table: {backup_arn}")
}

/// [`backup_in_use_message`] as the storage error the engine maps, with the
/// name of the CREATING target that holds the backup in use logged, since the
/// wire text does not carry it.
#[must_use]
pub fn backup_in_use(table: &str, backup_arn: &str) -> StorageError {
    tracing::info!("backup {backup_arn} is in use: a restore into {table} is still CREATING");
    StorageError::BackupInUse(backup_in_use_message(backup_arn))
}

/// The one signal of a missing backup every backend emits inside a
/// `Validation` error, which the engine turns into `BackupNotFoundException`
/// ([`is_backup_not_found`]).
pub const BACKUP_NOT_FOUND: &str = "Backup not found";

/// The error for a backup the catalog has no row for, or one of another
/// account, or (on the legacy reader) one that is not AVAILABLE.
#[must_use]
pub fn backup_not_found(backup_arn: &str) -> StorageError {
    StorageError::Validation(format!("{BACKUP_NOT_FOUND}: {backup_arn}"))
}

/// Whether a backend error says the backup is not there.
#[must_use]
pub fn is_backup_not_found(e: &StorageError) -> bool {
    matches!(e, StorageError::Validation(m) if m.contains(BACKUP_NOT_FOUND))
}

/// The catalog index of backups and restore jobs, implemented by each
/// backend. The engine reaches every row through it; the per-backend
/// mechanics (the lock on a row, the claim, the clock) are the backend's.
pub trait BackupCatalog: Send + Sync {
    /// The catalog's random id, written at `init` and by the migration that
    /// created the index for an existing catalog.
    fn catalog_id(&self) -> BoxFuture<'_, Result<String, StorageError>>;

    /// Insert `row` with status CREATING under a new claim, and return the
    /// claim. The claim precedes the insert or is the insert itself, so no
    /// CREATING row ever exists unowned (the AVAILABLE rows
    /// [`Self::insert_backup_row`] writes are unowned and have no job).
    ///
    /// # Errors
    ///
    /// `AlreadyExists` when a live owner holds the row's claim or the row
    /// exists.
    fn claim_backup<'a>(
        &'a self,
        row: &'a BackupRow,
    ) -> BoxFuture<'a, Result<Box<dyn JobClaim>, StorageError>>;

    /// Insert `row` unowned, if no row with its key exists: `backup sync`'s
    /// insert for a prefix it verified. The row is written as given (its
    /// status, sync's being AVAILABLE, its store, sizes, `created_at`,
    /// `failure_reason` and source table) except for three columns:
    /// `owner_generation` is [`UNOWNED_GENERATION`], and `completed_at` and
    /// `heartbeat_at` are the database's clock; the row's own values for
    /// those are ignored. No claim is taken: an unowned row has no job.
    /// Sync passes AVAILABLE; an unowned row of another status has no job to
    /// move it (on SQLite an unowned CREATING row has no slot, and the next
    /// cycle resolves it as dead).
    ///
    /// Returns `false`, writing nothing, when a row with the key exists, of
    /// whatever status or store, with one exception on Cassandra: a stored
    /// row equal to this one in every column the insert writes (unowned, the
    /// same store, sizes and source table) counts as inserted, and its list
    /// and status entries are written, which completes an insert that
    /// stopped between the row and its entries. On PostgreSQL, SQLite and
    /// MongoDB, which
    /// index `backup_id` alone as unique, a row with the backup id under
    /// another account also exists for this rule. Cassandra keys its rows
    /// by the account and the ARN and indexes no backup id, so there the
    /// rule is per ARN spelling: a row for the same id under another region
    /// or partition does not count. `backup sync` closes that gap itself:
    /// it inserts the row under this deployment's region's spelling of the
    /// manifest's ARN, and before the insert it reads the row under that
    /// spelling and the manifest's own and skips a backup the manifest's
    /// spelling, when it names another region, has a row for, so sync never
    /// gives an account two rows of one backup id under this deployment's or
    /// the manifest's spelling of the ARN on any backend (on Cassandra a row
    /// of another root under a third region's spelling is not read, and is
    /// not seen). A row under this deployment's spelling, the one sync
    /// inserts, is left to this method, which answers `false` for it or, on
    /// Cassandra, completes it under the exception above.
    ///
    /// # Errors
    ///
    /// The backend's error for a failed write. On Cassandra an answer lost
    /// in transit, and a `NotMatched` answer, is settled by a serial read of
    /// the row: this row counts as inserted and another as existing. No row
    /// after a lost answer means not written, and the write is sent once
    /// more; no row after `NotMatched` answers `false` (a legacy in-catalog
    /// backup holds the ARN, which the read does not return). An outcome
    /// still unknown after that is an error. A failed batch of the entries
    /// is sent once more before the error, and the next insert of the same
    /// row writes them.
    fn insert_backup_row<'a>(
        &'a self,
        row: &'a BackupRow,
    ) -> BoxFuture<'a, Result<bool, StorageError>>;

    /// Every row of `store` with `status`, or of every status for `None`.
    fn list_backup_rows<'a>(
        &'a self,
        status: Option<BackupStatus>,
        store: &'a StoreIdentity,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>>;

    /// Every row whose `store_kind` and `store_root` equal these, whatever
    /// its `store_id`, with `status` or of every status for `None`, in the
    /// (`created_at`, `backup_arn`) order [`Self::list_backup_rows`] uses:
    /// `backup sync --move-store` and `--forget-store`, which get a root and
    /// no id, and the report of the rows of other roots. `store_root`
    /// matches as stored, byte for byte: a root that differs by a trailing
    /// `/`, or an S3 root that differs by its `?endpoint=` suffix, is
    /// another root. The rows are read whole, as `list_backup_rows` reads
    /// them. On Cassandra the root's five status partitions are read
    /// whatever `status` is, each row behind an entry is re-read once, and
    /// it is kept when its kind and root still match (and its status, with
    /// `Some(status)`), whatever partition its entry is in: a row whose
    /// entry is under an earlier status (a status change whose entry batch
    /// failed, or whose writer died between the row and the batch) is
    /// listed under its own status. A row with no status entry at all (an
    /// insert that stopped before its entries) is not listed.
    fn list_backup_rows_by_root<'a>(
        &'a self,
        store_kind: StoreKind,
        store_root: &'a str,
        status: Option<BackupStatus>,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>>;

    /// The newest AVAILABLE row of `store` (the last in the
    /// (`created_at`, `backup_arn`) order [`Self::list_backup_rows`] lists
    /// in), or `None` when the store has none. The store's manifest probe
    /// reads it each reconciler cycle, so it reads one row and not the
    /// whole listing.
    fn newest_available_backup_row<'a>(
        &'a self,
        store: &'a StoreIdentity,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>>;

    /// The account's rows, of `table_name` when given, created within
    /// `bounds`, of every status: the caller decides which statuses are
    /// listed ([`BackupStatus::is_listed`]).
    fn list_account_backup_rows<'a>(
        &'a self,
        account_id: &'a str,
        table_name: Option<&'a str>,
        bounds: &'a TimeBounds,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>>;

    /// The row `arn` names, read by the ARN's account and backup id and
    /// returned only when the row's `account_id`, `table_name` and
    /// `backup_arn` equal the ARN's; `None` otherwise, of whatever status.
    fn read_backup_row<'a>(
        &'a self,
        arn: &'a BackupArn,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>>;

    /// The row holding `backup_id`, whatever its account, its ARN, its
    /// table name and its store. The backup id is unique in the index on
    /// its own (`idx_backup_index_backup_id` on PostgreSQL and SQLite, the
    /// `backup_id` index on MongoDB), so [`Self::insert_backup_row`]
    /// conflicts on it alone, another account's row included, and a store
    /// prefix (`account/table/id/`, which carries no region) whose id this
    /// answers for may hold that row's backup. `backup migrate-legacy`
    /// reads it before it deletes anything under a prefix. PostgreSQL,
    /// SQLite and MongoDB read it by the id. Cassandra can read only an
    /// ARN's own partition (no table of it indexes the backup id) and
    /// answers `None`; the caller reads the manifest under the prefix there
    /// instead.
    fn row_holding_backup_id<'a>(
        &'a self,
        backup_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<BackupRow>, StorageError>>;

    /// DeleteBackup's first step: under the row's lock, the in-use check and
    /// the AVAILABLE-to-DELETING write in one transaction.
    ///
    /// # Errors
    ///
    /// `Validation("Backup not found: <arn>")` when there is no such row.
    fn start_backup_delete<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<DeleteStart, StorageError>>;

    /// DELETING to DELETED, conditional on the row being DELETING; `false`
    /// when it was not. Besides the deletion's last step, `backup sync
    /// --forget-store` and `--forget-backup` remove a DELETING row with this
    /// write and then [`Self::remove_backup_row`], under the maintenance
    /// claim. No DELETED-prefix or tombstone duty acts on the row between
    /// the two, by the order of the listings: a duty that listed its rows
    /// before the claim was taken listed this one DELETING, not DELETED, and
    /// a duty that lists it DELETED did so after the claim was taken, and
    /// reads the claim held after its re-read of the row. A DELETING row of
    /// the configured store has its deletion finished first, by the forget
    /// itself or by a reconciler under the claim: its bytes were asked to be
    /// deleted. A DELETING row of another store keeps its bytes unless a
    /// server configured with that store finishes the deletion.
    fn set_backup_deleted<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>>;

    /// Remove a DELETED or FAILED tombstone that is still of `store_kind`
    /// and `store_root`, the store the caller read the row with; `false`
    /// when the row is not one, or names another store. The tombstone
    /// duty's write, and `backup sync --forget-store`'s and
    /// `--forget-backup`'s for a DELETED or FAILED row (and for a DELETING
    /// row after [`Self::set_backup_deleted`]). The store condition keeps a
    /// duty of a server still configured with an old root from removing a
    /// row `rewrite_store` moved after the duty read it: the configured
    /// store's own duties then finish that row (on Cassandra the lightweight
    /// transaction carries the condition, and a `NotMatched` answers
    /// `false`).
    fn remove_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
        store_kind: StoreKind,
        store_root: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>>;

    /// Remove an AVAILABLE row outright, leaving no DELETING or DELETED
    /// tombstone, so no deletion duty ever acts on the backup's objects:
    /// `backup sync --prune-missing`'s write for a row whose manifest the
    /// configured root does not hold, whose bytes may be under another
    /// host's root of the same path, and `--forget-backup`'s and
    /// `--forget-store`'s for an AVAILABLE row, whose bytes the operator
    /// keeps. Under the row's lock, as
    /// [`Self::start_backup_delete`] takes it, the status check and the
    /// in-use rule in one step with the removal: a row a CREATING restore
    /// target reads is left ([`ForgetRow::InUse`]), and so is a row another
    /// writer moved ([`ForgetRow::Status`]). The store is not touched.
    ///
    /// # Errors
    ///
    /// `Validation("Backup not found: <arn>")` when there is no such row.
    fn forget_available_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<ForgetRow, StorageError>>;

    /// `backup sync --move-store`'s write: each row of `keys` that is still
    /// of `from_kind` and `from_root` and is not CREATING gets `to`'s kind,
    /// root and id; every other column is left. Returns how many rows were
    /// rewritten. A key with no row, or whose row no longer matches (another
    /// writer moved it to another root, or it is CREATING: a job may still
    /// write its bytes under the old root), is left and not counted, so a
    /// second call with the same keys counts 0 once `to` is another root.
    /// A DELETING, DELETED or
    /// FAILED row is rewritten like an AVAILABLE one. No claim and no fence
    /// are taken: no object is touched.
    ///
    /// SQLite rewrites every key in one transaction, so a failed call
    /// rewrites none. PostgreSQL rewrites in batches of keys, each batch one
    /// statement in a transaction of its own (so no statement nears its
    /// bound whatever the key count), MongoDB in chunks of keys, each chunk
    /// one statement, and Cassandra key by key, so on those three a call
    /// that stops leaves the rows split between the two roots: every step
    /// is idempotent, and the caller re-lists the old root and runs again
    /// for what is left. Cassandra keeps no `backup_id` uniqueness, so the
    /// caller, not this method, refuses a move that would give an account
    /// two rows of one backup id.
    ///
    /// What a client that dies in the call (a killed command) leaves:
    /// - PostgreSQL: the batches before the one in flight, and of that one
    ///   nothing, unless its commit was already sent. Each batch's
    ///   statement runs in a transaction the client commits, under a
    ///   `lock_timeout` (a lock timeout rolls back and runs the batch
    ///   again, a bounded number of times) and a `statement_timeout`, so a
    ///   statement still waiting or running when the client dies commits
    ///   nothing: the server rolls it back once it finds the connection
    ///   closed, at the latest when those bounds end it, and no rewrite
    ///   lands after the maintenance claim's advisory lock went with the
    ///   client.
    /// - SQLite: nothing; the transaction is in the process and rolls back
    ///   with it.
    /// - MongoDB: the chunks before the one in flight are rewritten, and
    ///   the one in flight in whole or in part (each document's update is
    ///   atomic, and the server may finish the statement after the client
    ///   is gone). The claim is a heartbeat, live until it is stale, which
    ///   outlasts the statement.
    /// - Cassandra: the keys before the one in flight are rewritten, and
    ///   the one in flight is left at one of the points the module doc
    ///   names (its new entry written, or its row moved with the old entry
    ///   left); a lightweight transaction in flight may still commit. The
    ///   claim is a heartbeat, as on MongoDB.
    ///
    /// In each case the re-run of the move re-lists the old root and moves
    /// what is left.
    ///
    /// # Errors
    ///
    /// The backend's error for a failed read or write. On Cassandra an
    /// answer lost in transit is settled by a serial read of the row: a row
    /// that carries `to` whole (its kind, root and store id) counts as
    /// rewritten, a row of the old root is sent again (up to three attempts
    /// in all), a row of any other store (`to`'s root under another store
    /// id included) is left and not counted, and an outcome still unknown
    /// after the last attempt is an error.
    fn rewrite_store<'a>(
        &'a self,
        from_kind: StoreKind,
        from_root: &'a str,
        keys: &'a [BackupKey],
        to: &'a StoreIdentity,
    ) -> BoxFuture<'a, Result<u64, StorageError>>;

    /// Whether [`Self::rewrite_store`] writes key by key, several requests
    /// per key with nothing checked between keys (Cassandra), so a caller
    /// that must stop between keys (the move, once its maintenance claim
    /// reads lost) passes one key per call. `false` by default: the other
    /// backends rewrite a call's keys in one statement per batch or chunk.
    fn rewrite_store_runs_key_by_key(&self) -> bool {
        false
    }

    /// Remove a CREATING row whose owner is dead: `heartbeat_at` older than
    /// `stale_after` on the catalog's clock. `false` when the row is not
    /// CREATING, is gone, or has a live owner. The maintenance command's
    /// write for a CREATING row its wait saw no live owner for, after it has
    /// deleted the row's prefix; the row's own job, were it alive, would
    /// find its claim lost at its next heartbeat. Also `backup sync
    /// --forget-store`'s and `--forget-backup`'s write for a CREATING row,
    /// which delete no prefix: `false` with the row still CREATING is a live
    /// owner, and they refuse the row.
    fn remove_dead_creating_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<bool, StorageError>>;

    /// Take over a CREATING row whose owner is dead, under a fresh
    /// generation, and return the claim; the reconciler's way onto a row a
    /// job left behind. Dead is `heartbeat_at` older than `stale_after` on
    /// the database's clock (on PostgreSQL, once the owner's claim session
    /// has ended as well: the advisory lock is tried first, so a stopped
    /// owner whose session lives keeps its row), except on SQLite, where a
    /// CREATING row whose in-process slot no task holds is dead at once (a
    /// restart frees every slot, a job that panics drops its slot) and
    /// `stale_after` is not read.
    /// `heartbeat_at` is left as the dead owner wrote it, so the stale
    /// window of the FAILED-prefix duty counts from the owner's last beat
    /// and not from the takeover. `None` when the row is not CREATING, is
    /// gone, has a live owner, or is already claimed by another task; the
    /// old generation's writes through its [`JobClaim`] find
    /// [`WriteOutcome::NotMatched`] from then on.
    fn claim_dead_backup<'a>(
        &'a self,
        key: &'a BackupKey,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<Option<Box<dyn JobClaim>>, StorageError>>;

    /// The rows of `store` with `status` whose `clock` column is older than
    /// `age` on the database's clock: the maintenance command's test of a
    /// live owner (a CREATING or FAILED row it does not list under the
    /// window on [`StaleClock::Heartbeat`] has one), and the reconciler's
    /// listing for the FAILED-prefix delay (`Failed`,
    /// [`StaleClock::Heartbeat`]) and the tombstone retention (`Failed` or
    /// `Deleted`, [`StaleClock::Completed`]). The comparison is the
    /// backend's, in its own query, so the engine never compares a row's
    /// timestamp with a clock of its own.
    fn list_backup_rows_older_than<'a>(
        &'a self,
        status: BackupStatus,
        store: &'a StoreIdentity,
        clock: StaleClock,
        age: Duration,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>>;

    /// Remove the restore registrations a backend keeps beside its rows that
    /// name a target no longer a CREATING restore target of that backup, and
    /// return the ARNs of the backups touched. Only Cassandra keeps such a
    /// map (`active_restores`, maintained by lightweight transactions whose
    /// removal step can be lost with its owner); every other backend derives
    /// the in-use check from the target rows themselves and has nothing to
    /// sweep, which is what the default body says.
    fn sweep_restore_registrations(&self) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    /// The name of a CREATING restore target registered from the backup, if
    /// any: the rule under which DeleteBackup and a second restore of the
    /// backup are refused with `BackupInUseException`. Reads the target's
    /// status, not the job's.
    fn backup_in_use<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<Option<String>, StorageError>>;

    /// Remove a CREATING restore target, its job record and its progress
    /// rows, conditional on the table still being a CREATING restore target
    /// with this table id, so this and the ACTIVE flip exclude each other.
    /// `false` when the table is no longer one. Not DeleteTable, which
    /// refuses a restore target. Also resumes, from the drop, the removal of
    /// a FAILED record's target that an earlier call left DELETING with its
    /// registration (the state [`Self::list_dead_restore_jobs`] names).
    fn remove_restore_target<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>>;

    /// [`Self::remove_restore_target`] for the `extenddb backup
    /// remove-target` command, which runs with no claim of its own: a
    /// backend whose owner holds a lock that outlives its heartbeat
    /// (PostgreSQL's per-session advisory lock) takes that lock here first,
    /// so the removal and a reconciler's takeover of the same target exclude
    /// each other and a frozen owner whose session still holds the lock
    /// keeps its target; `false` when the lock is held. The default is
    /// [`Self::remove_restore_target`]: the job's own failure path and the
    /// reconciler's resume already hold the claim when they remove, so the
    /// lock lives only on this variant, and the other backends fence the
    /// takeover on the record's generation and heartbeat alone.
    fn remove_restore_target_exclusive<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        self.remove_restore_target(key)
    }

    /// The restore job records the reconciler acts on, two kinds: RUNNING
    /// records whose `heartbeat_at` is older than `stale_after` on the
    /// database's clock (the owner is dead by every backend's rule; the
    /// reconciler fails or resumes them), and FAILED records whose table is
    /// still a CREATING restore target (the FAILED write landed and the
    /// target's removal did not; the reconciler removes the target), listed
    /// once the record's `completed_at`, or its `heartbeat_at` where that is
    /// unset, is older than `stale_after`, so a job still on its own failure
    /// path is left to finish it. On a backend whose removal is more than one
    /// write (Cassandra: a claim CREATING to DELETING, then the drop), a
    /// FAILED record whose target is DELETING with its registration is
    /// returned once stale on the same rule, and
    /// [`Self::remove_restore_target`] resumes the removal; PostgreSQL and
    /// SQLite remove in one transaction and never leave that state, and
    /// MongoDB deletes the document first, so neither does it.
    fn list_dead_restore_jobs(
        &self,
        stale_after: Duration,
    ) -> BoxFuture<'_, Result<Vec<RestoreJobRow>, StorageError>>;

    /// Every RUNNING restore job record, by status alone, whatever its
    /// heartbeat says: the maintenance command's wait set, which must see a
    /// record stamped in the listing's own instant (or ahead of the
    /// listing's clock) as well as an old one. Not a liveness test.
    fn list_running_restore_jobs(&self) -> BoxFuture<'_, Result<Vec<RestoreJobRow>, StorageError>>;

    /// RUNNING to FAILED with `reason` on the target's job record, conditional
    /// on RUNNING and on the record still carrying the `owner_generation` and
    /// `heartbeat_at` the reconciler listed (design 4(e)): a heartbeat that
    /// lands after the listing changes `heartbeat_at`, so the write matches
    /// nothing and the live owner keeps its record. The write sets a fresh
    /// `owner_generation`, fencing the dead job's writes out. `false` when the
    /// record was not RUNNING under those values or is gone. The reconciler's
    /// write for a dead legacy restore, which is never resumed, and for a
    /// manifest restore it cannot resume (a store other than the configured
    /// one, a maintenance claim held).
    fn fail_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
        owner_generation: u64,
        heartbeat_at: f64,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>>;

    /// A store-backed restore's registration (the design's 5.3 step 4):
    /// create the target table CREATING with no scheduled transition and
    /// its RUNNING `kind = manifest` job record, `registration`'s store,
    /// `spool_host` and generation on it, under the
    /// lock on the backup's `backup_index` row that
    /// [`Self::start_backup_delete`] takes, re-checking there that the row
    /// is AVAILABLE and that no CREATING target of the backup exists. The
    /// catalog rows commit first and the target's data tables (on MongoDB
    /// its collections and index metadata) are created after the commit, in
    /// the order [`RestoreEngine::restore_target_ready`]'s doc states per
    /// backend, so a kill right after the commit leaves a RUNNING record
    /// over a target with no data table; the reconciler's resume checks
    /// `restore_target_ready` before it resumes a record. A
    /// DeleteBackup and a registration of one backup therefore order wholly
    /// one before the other. Returns the CREATING description, its
    /// `RestoreSummary` filled from `registration`, and the claim the job
    /// heartbeats and writes its status through; the record is born owned,
    /// as [`Self::claim_backup`]'s row is. The record's `heartbeat_at` is
    /// the database's clock as the backend stamps it at the insert: on
    /// PostgreSQL `now()` is the registration transaction's start, so a
    /// registration that waited on the `backup_index` row's lock, or whose
    /// data DDL after the commit took long, hands back a record whose
    /// heartbeat is that much older than its return; the engine's first beat,
    /// sent before the registration's reply, refreshes it.
    ///
    /// # Errors
    ///
    /// `Validation("Backup not found: <arn>")` ([`is_backup_not_found`])
    /// when the row is missing or not AVAILABLE; `BackupInUse` when a target
    /// of the backup is CREATING; `TableAlreadyExists` when the name is
    /// taken, or `TableBeingRestored` when it is a CREATING restore target;
    /// `LimitExceeded` when the backend admits no further job.
    fn claim_restore<'a>(
        &'a self,
        account_id: &'a str,
        input: CreateTableInput,
        registration: &'a RestoreRegistrationRow,
    ) -> BoxFuture<'a, Result<RestoreRegistered, StorageError>>;

    /// Take over a RUNNING manifest record whose owner is dead (the design's
    /// 4(e)): a fresh generation, the claimant's `spool_host` and
    /// `heartbeat_at` set to the database's clock, in one conditional write,
    /// so a second taker finds the record live at once. The refresh is where
    /// this takeover differs from [`Self::claim_dead_backup`]: the backup
    /// takeover leaves `heartbeat_at` because the FAILED-prefix duty's stale
    /// window counts from it, while nothing waits on a restore record's old
    /// heartbeat after a takeover (the FAILED-target duty ages on
    /// `completed_at`, which every FAILED write of a resume stamps, and the
    /// maintenance claim waits on RUNNING, not on the beat); without the
    /// refresh a second reconciler's cycle landing before the successor's
    /// first beat (one period after the takeover) would take the same record
    /// again. Dead is `heartbeat_at` older than `stale_after` on the
    /// database's clock; on PostgreSQL the owner's advisory lock on the
    /// target name is tried first, so a stopped owner whose session lives
    /// keeps its record, as [`Self::claim_dead_backup`] says; on SQLite the
    /// name's in-process slot is the test instead and `stale_after` is not
    /// read: a record whose slot no task of this process holds is dead at
    /// once (a restart frees every slot and ages every RUNNING record), and
    /// one whose slot a task holds is alive whatever its heartbeat says.
    /// `None` when the record is not RUNNING, is gone, or has a live owner;
    /// the old generation's writes find [`WriteOutcome::NotMatched`] from
    /// then on. On Cassandra the takeover also re-applies the target's
    /// `active_restores` entry on the backup's row when it is absent (a
    /// crash between the target's DDL and the registration), so the backup
    /// stays in use for the resumed load.
    fn claim_restore_dead<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
        stale_after: Duration,
        spool_host: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Option<Box<dyn JobClaim>>, StorageError>>;

    /// The job record of the target `key` names, of whatever status; `None`
    /// when the table is not a restore target with this table id.
    fn read_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
    ) -> BoxFuture<'a, Result<Option<RestoreJobRow>, StorageError>>;

    /// Every job record with `status`, or of every status for `None`: the
    /// reconciler's listing of RUNNING targets for its sweeps.
    fn list_restore_jobs<'a>(
        &'a self,
        status: Option<JobStatus>,
    ) -> BoxFuture<'a, Result<Vec<RestoreJobRow>, StorageError>>;

    /// The distinct `store_id`s the catalog's rows record for a root, of
    /// every status: the store identity check reads them before it knows an
    /// id. Empty when no row names the root.
    fn read_store_ids<'a>(
        &'a self,
        store_kind: StoreKind,
        store_root: &'a str,
    ) -> BoxFuture<'a, Result<Vec<String>, StorageError>>;

    /// How many rows the catalog holds per store (kind, root and id) and
    /// status, every store and status with at least one row, in
    /// [`StoreRootCount::sort`]'s order: plain `backup sync`'s report of the
    /// rows of roots other than the configured one, and the roots
    /// `--move-store` and `--forget-store` name when given one with no
    /// rows. Counts only; no row is read whole. On Cassandra the counts are
    /// of the status entries (`backup_index_by_status`, one partition per
    /// root and status), not verified against their rows, so an entry a
    /// stopped writer left behind counts as [`Self::read_store_ids`] counts
    /// it ([`Self::summary_counts_status_entries`]).
    fn summarize_store_roots(&self) -> BoxFuture<'_, Result<Vec<StoreRootCount>, StorageError>>;

    /// Whether [`Self::summarize_store_roots`] counts status entries rather
    /// than rows (Cassandra), so a caller that reports the counts verifies
    /// them against the rows' listing. `false` by default: the other
    /// backends count rows.
    fn summary_counts_status_entries(&self) -> bool {
        false
    }

    /// The sweep of the status entries under `store_root` whose entry
    /// names `store_kind`, each verified against its row as it is read:
    /// an entry whose row is gone or names another store is deleted, and
    /// one whose row is of the root under another status is written again
    /// under the row's status and then deleted (repaired). `backup sync
    /// --forget-store`'s sweep of the root it forgets, under the
    /// maintenance claim. Only Cassandra keeps status entries beside the
    /// rows (`backup_index_by_status`): a deleter whose entry batch lands
    /// after a `rewrite_store` of its row leaves an entry with no row of
    /// the root behind it, and a status change whose entry batch failed, or
    /// whose writer died between its row and its batch, leaves the row's
    /// entry under the earlier status. Every other backend has none, which
    /// is what the default body says.
    fn remove_orphan_status_entries<'a>(
        &'a self,
        store_kind: StoreKind,
        store_root: &'a str,
    ) -> BoxFuture<'a, Result<StatusEntrySweep, StorageError>> {
        let _ = (store_kind, store_root);
        Box::pin(async { Ok(StatusEntrySweep::default()) })
    }

    /// Take the catalog-level maintenance claim for `holder` (a label naming
    /// the command and the host it runs on, for the operator who finds it
    /// held). The claim is one record, `backup_maintenance`, born owned
    /// with a fresh generation and heartbeated through the handle; a holder
    /// whose heartbeat is older than `stale_after` on the catalog's clock is
    /// dead and its claim is taken over. PostgreSQL adds a proof of life on
    /// the holder's side: there the holder's session holds an advisory
    /// lock, so a holder whose session is open is alive whatever its
    /// heartbeat says, and the take also needs the record's heartbeat older
    /// than `stale_after`, as everywhere: a holder whose session ended
    /// (killed, or its connection reset) may still be running until its
    /// next beat finds the loss, so its claim is not taken while its
    /// heartbeat is young (the window [`check_maintenance_claim`] reads
    /// too). On SQLite a command that holds the serve lock takes the record
    /// whatever its age, and a server's startup under the serve lock removes
    /// it outright, since no other holder can be alive there.
    ///
    /// [`check_maintenance_claim`]: BackupCatalog::check_maintenance_claim
    ///
    /// # Errors
    ///
    /// `AlreadyExists` naming the holder when a live one holds the claim: a
    /// second maintenance command fails at once.
    fn maintenance_claim<'a>(
        &'a self,
        holder: &'a str,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<Box<dyn MaintenanceClaim>, StorageError>>;

    /// Whether the catalog-level maintenance claim is held by a live holder:
    /// a record whose heartbeat is younger than `stale_after` on the
    /// catalog's clock. Jobs check it and refuse while it is held; the
    /// reconciler runs only the duties the holder waits on. Where in the
    /// job's order the check runs depends on the job: the in-catalog create
    /// and restore of this release commit their row inside the backend and
    /// detach, so the engine checks after every other refusal and before the
    /// row exists (the engine's backup module states it); the store-backed
    /// job of a later release owns its row from the engine and checks after
    /// its own claim and row exist (the design's 5.1 step 2).
    fn check_maintenance_claim(
        &self,
        stale_after: Duration,
    ) -> BoxFuture<'_, Result<bool, StorageError>>;
}

/// The holder's handle on the catalog-level maintenance claim: the
/// heartbeat that keeps it, and the release that ends it. Dropping the handle
/// without `release` leaves the record for the stale rule to expire.
pub trait MaintenanceClaim: Send + Sync {
    /// The fencing token the claim was taken with.
    fn generation(&self) -> u64;

    /// Refresh `heartbeat_at` on the catalog's clock and confirm the claim is
    /// still this holder's. The caller treats an `Err` as [`Heartbeat::Lost`].
    fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>>;

    /// [`Self::heartbeat`] under `bound`: `None` when the beat did not
    /// return within it, which the holder treats as a lost claim. The
    /// default bounds the whole call; SQLite, whose beat queues behind its
    /// own process's engine write lock, measures the bound from the moment
    /// the beat holds that lock, as [`JobClaim::heartbeat_within`] does.
    fn heartbeat_within(
        &self,
        bound: Duration,
    ) -> BoxFuture<'_, Option<Result<Heartbeat, StorageError>>> {
        Box::pin(async move { tokio::time::timeout(bound, self.heartbeat()).await.ok() })
    }

    /// Whether [`Self::heartbeat_within`] can wait outside its bound before
    /// the bound starts, as [`JobClaim::heartbeat_parks_before_its_bound`]
    /// states it: `true` on SQLite only, where a beat parked on the write
    /// lock confirms the claim for as long as it waits.
    fn heartbeat_parks_before_its_bound(&self) -> bool {
        false
    }

    /// Remove the record, conditioned on the generation; a record another
    /// holder took over is left to it. Idempotent.
    fn release(&self) -> BoxFuture<'_, Result<(), StorageError>>;
}

/// A job's handle on its row: the claim that excludes a second owner and
/// lets a successor detect a dead one, and the writes the job makes through
/// it, each conditioned on the claim's generation.
pub trait JobClaim: Send + Sync {
    /// The fencing token the claim was taken with.
    fn generation(&self) -> u64;

    /// Refresh `heartbeat_at` on the database's clock and confirm the claim
    /// is still held. The caller treats an `Err` as [`Heartbeat::Lost`].
    fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>>;

    /// [`Self::heartbeat`] under `bound`: `None` when the beat did not
    /// return within it, which the caller treats as a lost claim. The default
    /// bounds the whole call. A backend whose beat queues behind a lock of
    /// its own process before it reaches the catalog (SQLite's engine write
    /// lock, held by a GSI backfill or a large write for as long as it
    /// takes) measures the bound from the moment the beat holds that lock,
    /// so a writer holding it past a period delays the beat and does not end
    /// the job: the queue is this process's, not a sign the claim is lost.
    fn heartbeat_within(
        &self,
        bound: Duration,
    ) -> BoxFuture<'_, Option<Result<Heartbeat, StorageError>>> {
        Box::pin(async move { tokio::time::timeout(bound, self.heartbeat()).await.ok() })
    }

    /// Whether [`Self::heartbeat_within`] can wait outside its bound before
    /// the bound starts (SQLite's beat queues behind the engine write lock
    /// of its own process, so the wait is this process's and never a sign of
    /// a takeover). The engine's pre-put check treats a beat in flight as
    /// confirming the claim for as long as it waits where this is `true`,
    /// and not at all where it is `false` (the default): there the bound
    /// covers the whole call, so the check's two-period rule already covers
    /// a beat in flight, and a process stopped and resumed (mid-beat, or
    /// between beats, whose missed tick starts a beat at once) would
    /// otherwise pass the check while that beat runs, though another owner
    /// may have taken the row over during the stop.
    fn heartbeat_parks_before_its_bound(&self) -> bool {
        false
    }

    /// Move the record from `from` to `to`, conditioned on the claim's
    /// generation and on the record being in `from`.
    fn set_status<'a>(
        &'a self,
        from: JobStatus,
        to: JobStatus,
        fields: &'a StatusFields,
    ) -> BoxFuture<'a, Result<WriteOutcome, StorageError>>;

    /// Re-read the record, after a write whose outcome is unknown.
    fn read(&self) -> BoxFuture<'_, Result<JobRow, StorageError>>;
}

/// The frozen path to backups stored in the catalog itself: the create,
/// describe, list, delete, restore-source and restore paths of the
/// in-catalog format, which each backend keeps as a copy of its own code for
/// reading and decoding that shape. The engine consults the catalog index
/// first and falls back to this reader; `create` is the CreateBackup path
/// while no `[backup.store]` is configured. `extenddb backup migrate-legacy`
/// reads a backup through [`Self::items`] (a frozen copy of each backend's
/// restore read), finds every backup through [`Self::list_all`] (and the
/// deleted ones through [`Self::list_deleted`]), and ends with
/// [`Self::replace_with_index_row`].
///
/// Two parts are not frozen. A restore registers through the restore job
/// fields (`kind = legacy`, the claim), so the reconciler finds a restore
/// whose owner died as it finds any other. And
/// [`Self::replace_with_index_row`] writes a catalog index row in the step
/// that removes the in-catalog backup.
pub trait LegacyBackupReader: Send + Sync {
    /// Create an in-catalog backup of a table, snapshotting all items. The
    /// permit is the job's slot, released when the create ends.
    fn create(
        &self,
        account_id: &str,
        table_name: &str,
        backup_name: &str,
        permit: JobPermit,
    ) -> BoxFuture<'_, Result<BackupDetails, StorageError>>;

    /// Describe a backup by ARN, scoped to the owning account.
    ///
    /// Backups belong to an account, so implementations must match on both
    /// `account_id` and `backup_arn` and report a missing backup when the ARN
    /// belongs to another account.
    fn describe(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>>;

    /// List the account's in-catalog backups, optionally only those of
    /// `table_name`, in any order. Each summary carries the source table's
    /// `TableId`.
    fn list(
        &self,
        account_id: &str,
        table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<BackupSummary>, StorageError>>;

    /// Delete a backup by ARN, scoped to the owning account; an ARN owned by
    /// another account is reported as missing. `BackupInUse` while a restore
    /// target of the backup is CREATING.
    fn delete(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>>;

    /// The backup's recorded source, for an AVAILABLE backup owned by
    /// `account_id`.
    ///
    /// The engine resolves the restore target and runs every refusal from
    /// this, and fills DescribeBackup's `SourceTableFeatureDetails` and
    /// `SourceTableDetails.ProvisionedThroughput` from it. `definition` is the
    /// stored [`crate::backup_definition::BackupTableDefinition`] (with
    /// `time_to_live` and `stream_specification` as captured at
    /// CreateBackup), or `None` for a backup recorded without one.
    ///
    /// # Errors
    ///
    /// `StorageError::Validation("Backup not found: <arn>")` if the backup is
    /// missing, owned by another account, or not AVAILABLE.
    /// `StorageError::Unsupported` for a stored definition this build cannot
    /// read.
    fn restore_source(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupRestoreSource, StorageError>>;

    /// Restore `request.input.table_name` from `request.backup_arn`.
    ///
    /// `request.input` is resolved and validated by the engine; create exactly
    /// that table, CREATING. In the same atomic step that re-checks the backup
    /// is still AVAILABLE and not in use, register the restore (the job
    /// record, `kind = legacy`, with the claim) so DescribeTable can report
    /// `RestoreSummary`, DeleteBackup refuses the source, and DeleteTable
    /// refuses the target while it is being restored. Then return the CREATING
    /// description at once. `permit` is the job's slot, released when the
    /// copy ends.
    ///
    /// The item copy runs in a detached task that owns its own clones, so
    /// the request future being dropped does not stop it. The task flips the
    /// target ACTIVE when the copy completes, or removes the target on
    /// failure.
    ///
    /// # Errors
    ///
    /// Returned synchronously, before the copy starts:
    /// `StorageError::Validation("Backup not found: <arn>")` if the backup is
    /// not AVAILABLE at registration; `StorageError::BackupInUse` if a
    /// target of it is CREATING; `StorageError::TableAlreadyExists(name)` if
    /// the name is taken; `StorageError::TableBeingRestored` if it is a
    /// CREATING restore target.
    fn restore(
        &self,
        account_id: &str,
        request: RestoreRequest,
        permit: JobPermit,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>>;

    /// In-catalog CREATING backup rows whose create is dead: on a backend
    /// whose legacy create commits a CREATING row before the copy, those
    /// whose heartbeat is older than `stale_after`, or that no running create
    /// holds. A backend whose legacy delete commits a DELETING row before its
    /// cleanup also reports the DELETING rows a stopped DeleteBackup left.
    /// The reconciler removes each through [`Self::delete_fenced`],
    /// unless the backend removed it in this call
    /// ([`AbandonedLegacyBackup::removed`]). Empty on a backend whose legacy
    /// create commits its row AVAILABLE with the copy.
    fn abandoned_creating_backups(
        &self,
        stale_after: Duration,
    ) -> BoxFuture<'_, Result<Vec<AbandonedLegacyBackup>, StorageError>>;

    /// Finish the deletion of a row [`Self::abandoned_creating_backups`]
    /// reported with `removed` false, a DELETING row whose owner is dead:
    /// either a CREATING row that call fenced to DELETING, or a stale
    /// DELETING row that a stopped DeleteBackup left. Remove the backup's
    /// items and definition and mark the row DELETED, as the second half of
    /// [`Self::delete`] does. `Ok` for a row that is DELETING, or already
    /// DELETED (a DeleteBackup that found it DELETING finished the cleanup);
    /// a row in any other status is left and reported missing.
    /// The reconciler counts the deletion and writes its audit line on `Ok`.
    /// [`Self::delete`] answers a caller that finds the row DELETING "not
    /// found", since that caller did not move the row there, and that answer
    /// stays for a client's DeleteBackup. A backend that reports no row with
    /// `removed` false is never asked and keeps the default, an error.
    fn delete_fenced<'a>(
        &'a self,
        _account_id: &'a str,
        backup_arn: &'a str,
    ) -> BoxFuture<'a, Result<(), StorageError>> {
        Box::pin(async move {
            Err(StorageError::Internal(format!(
                "{backup_arn}: this backend reports no fenced in-catalog backup"
            )))
        })
    }

    /// Every in-catalog backup that is not DELETED, of every account
    /// (including an account since deleted), ordered by creation time and
    /// then ARN. A frozen `delete` leaves a DELETED row, which is not a
    /// backup and is not listed. A row that does not decode is an error.
    fn list_all(&self) -> BoxFuture<'_, Result<Vec<LegacyBackupKey>, StorageError>>;

    /// Every DELETED in-catalog backup of every account that
    /// [`Self::replace_with_index_row`] did not replace: the rows a
    /// DeleteBackup of the legacy ARN left, ordered as [`Self::list_all`].
    /// `extenddb backup migrate-legacy --all` deletes what a stopped
    /// migration left under each one's store prefix.
    fn list_deleted(&self) -> BoxFuture<'_, Result<Vec<LegacyBackupKey>, StorageError>>;

    /// The status of the in-catalog backup `backup_arn` of `account_id`
    /// that [`Self::replace_with_index_row`] did not replace, DELETED
    /// included; `None` when there is none (never one, another account's,
    /// or one the replacement replaced). PostgreSQL and SQLite: the
    /// `backups` row, which the replacement removes. MongoDB: the backup
    /// document unless it carries the replacement's `migrated_to_store`
    /// marker. Cassandra: the `backups_by_arn` row while `store_kind` is
    /// null (the replacement converts the row into its index row).
    ///
    /// `backup sync` never adopts a prefix whose ARN this answers for, and
    /// `backup migrate-legacy` deletes the copy a stopped migration left
    /// under the prefix of one that is DELETED.
    fn holder_status<'a>(
        &'a self,
        account_id: &'a str,
        backup_arn: &'a str,
    ) -> BoxFuture<'a, Result<Option<BackupStatus>, StorageError>>;

    /// Finish a [`Self::replace_with_index_row`] that committed and stopped
    /// before its end, whatever the catalog index row now holds: on
    /// MongoDB a backup document DELETING with `migrated_to_store` has its
    /// item collection dropped and DELETED written (the frozen delete's
    /// second half), and the call answers `true`. Every other backend
    /// finishes its replacement in one step or through
    /// [`Self::replace_with_index_row`] itself, and answers `false`.
    fn finish_replacement<'a>(
        &'a self,
        account_id: &'a str,
        backup_arn: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>> {
        let _ = (account_id, backup_arn);
        Box::pin(async { Ok(false) })
    }

    /// Remove the DELETED in-catalog backup `backup_arn` of `account_id`
    /// (a DeleteBackup's tombstone, one [`Self::list_deleted`] lists), and
    /// nothing else: the call is conditioned on the backup being DELETED
    /// and in the catalog (not one [`Self::replace_with_index_row`]
    /// replaced), and answers whether it removed it. `backup migrate-legacy`
    /// calls it once the configured store lists nothing under the backup's
    /// prefix and no catalog row holds its id, so a DELETED in-catalog
    /// backup whose id carries an instant is one whose prefix is not yet
    /// confirmed clean. PostgreSQL and SQLite delete the `backups` row
    /// (its items and definition went with the DeleteBackup); MongoDB the
    /// backup document without the `migrated_to_store` marker; Cassandra
    /// the `backups_by_arn` row, with a lightweight transaction conditioned
    /// on DELETED and `store_kind = null`.
    fn remove_deleted<'a>(
        &'a self,
        account_id: &'a str,
        backup_arn: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>>;

    /// Open the AVAILABLE in-catalog backup `backup_arn` owned by
    /// `account_id` for copying: its name, its recorded source
    /// ([`Self::restore_source`]'s answer), what the backend can state about
    /// its vector indexes, and a stream of every item it stored.
    ///
    /// The stream is a copy of the backend's restore read, kept apart from
    /// it so either can change alone: PostgreSQL reads the items in one
    /// `REPEATABLE READ` catalog transaction, held until the stream ends or
    /// is dropped, whose first statement re-checks AVAILABLE; SQLite reads
    /// keyset batches, each in its own read transaction that re-checks
    /// AVAILABLE; MongoDB reads the backup's collection with a cursor; and
    /// Cassandra reads the item buckets in pages. On SQLite, MongoDB and
    /// Cassandra a DeleteBackup that lands while the stream runs ends it
    /// with an error or leaves it short; the caller compares the count with
    /// `source.item_count`.
    ///
    /// # Errors
    ///
    /// `StorageError::Validation("Backup not found: <arn>")` if the backup is
    /// missing, another account's, or not AVAILABLE.
    /// `StorageError::Unsupported` for a definition this build cannot read
    /// and, on MongoDB, for a backup taken before the `$out` copy, which has
    /// no item collection to read.
    fn items(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<LegacyBackupItems, StorageError>>;

    /// Replace the in-catalog backup `row.backup_arn` of `row.account_id` by
    /// `row`, the catalog index row of its copy in the store, in one step
    /// that DeleteBackup and a restore's registration order wholly before or
    /// after: the in-catalog backup is read AVAILABLE and locked, the in-use
    /// rule runs (a CREATING restore target of the backup), `row` is
    /// inserted as [`BackupCatalog::insert_backup_row`] inserts it (unowned,
    /// `completed_at` and `heartbeat_at` on the database's clock), and the
    /// in-catalog backup is removed so that no in-catalog row of the ARN is
    /// left that [`Self::holder_status`] answers for. PostgreSQL and SQLite
    /// do it in one transaction (SQLite under the engine write lock) that
    /// deletes the `backups` row outright, its items and definition with
    /// it; a DELETED `backups` row is then always a DeleteBackup's. MongoDB
    /// writes the index document and DELETING marked `migrated_to_store` in
    /// one session transaction and then drops the item collection and
    /// writes DELETED, as the frozen `delete` does; a stop after the commit
    /// leaves the marked DELETING document, which describe, list,
    /// `holder_status` and DeleteBackup leave out and which a later call or
    /// [`Self::finish_replacement`] finishes. Cassandra, whose in-catalog
    /// and index rows share `backups_by_arn`, converts the row in place with
    /// one lightweight transaction (`store_kind` set, so the in-catalog
    /// reader no longer reads it; a legacy restore's registration is
    /// conditioned on `store_kind = null` too), then writes its list and
    /// status entries and deletes the item buckets; no DELETED row is left
    /// there.
    ///
    /// [`Replaced::AlreadyReplaced`] when an index row with `row`'s account,
    /// ARN and store is there already: what is left of the in-catalog
    /// backup is removed as above when it is AVAILABLE (or, on MongoDB,
    /// DELETING), the in-use rule first, and nothing else is written; a
    /// DELETED or missing one is left as it is. On Cassandra that finishes a conversion that stopped
    /// after its transaction; its list and status entries are written only
    /// when a serial read finds the index row AVAILABLE, and otherwise (a
    /// DeleteBackup of the migrated backup since) the item buckets are
    /// deleted and the call answers [`Replaced::NotAvailable`]. An index row
    /// with the backup's id under another ARN spelling or another store is
    /// an `AlreadyExists` error, with nothing written, whatever the
    /// in-catalog backup's status (a DELETED one included): the caller
    /// keeps a copy that may be that row's prefix. Cassandra reads only
    /// the ARN's own partition, so it finds another store's row there and
    /// not a row of the id under another spelling (no table of it indexes
    /// the backup id); the engine reads the id's row whatever its ARN
    /// ([`BackupCatalog::row_holding_backup_id`]) and, on Cassandra, the
    /// manifest under the prefix, before it writes anything there.
    ///
    /// # Errors
    ///
    /// `StorageError::AlreadyExists` as above; the backend's errors
    /// otherwise, after which the in-catalog backup is as it was or (MongoDB
    /// after the commit, Cassandra after the conversion) the call can be
    /// repeated to finish it.
    fn replace_with_index_row<'a>(
        &'a self,
        row: &'a BackupRow,
    ) -> BoxFuture<'a, Result<Replaced, StorageError>>;
}

#[cfg(test)]
mod tests {
    use extenddb_core::types::{KeyType, ProjectionType};

    use super::*;

    /// The `source_table` object of the manifest fixture
    /// `crates/core/testdata/extenddb-manifest/v1.json`.
    fn fixture_source_table() -> serde_json::Value {
        let manifest: serde_json::Value = serde_json::from_str(include_str!(
            "../../core/testdata/extenddb-manifest/v1.json"
        ))
        .expect("fixture parses");
        manifest["source_table"].clone()
    }

    fn row(status: BackupStatus) -> BackupRow {
        BackupRow {
            backup_id: "01757500000123-0badcafe".to_owned(),
            account_id: "123456789012".to_owned(),
            table_name: "Music".to_owned(),
            backup_arn:
                "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01757500000123-0badcafe"
                    .to_owned(),
            backup_name: "music-nightly".to_owned(),
            status,
            store: StoreIdentity {
                store_kind: StoreKind::Filesystem,
                store_root: "/var/lib/extenddb/backups".to_owned(),
                store_id: "store-1".to_owned(),
            },
            size_bytes: 61828,
            item_count: 100_000,
            created_at: 1_757_500_000.123,
            completed_at: None,
            failure_reason: None,
            source_table_details_json: fixture_source_table(),
            owner_generation: 7,
            heartbeat_at: 1_757_500_001.0,
        }
    }

    #[test]
    fn a_description_derives_from_the_manifest_source_table() {
        let desc = row(BackupStatus::Available).description().unwrap();
        let d = &desc.backup_details;
        assert_eq!(d.backup_status, "AVAILABLE");
        assert_eq!(d.backup_type, "USER");
        assert_eq!(d.backup_size_bytes, 61828);
        assert_eq!(d.backup_creation_date_time, 1_757_500_000.123);
        let s = &desc.source_table_details;
        assert_eq!(s.table_name, "Music");
        assert_eq!(s.table_id, "12345a12-abcd-123a-ab12-1234abc12345");
        assert_eq!(
            s.table_arn,
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music"
        );
        assert_eq!(s.key_schema.len(), 2);
        assert_eq!(s.key_schema[1].key_type, KeyType::Range);
        assert_eq!(s.item_count, 100_000);
        assert_eq!(s.table_size_bytes, 123_456);
        assert_eq!(s.billing_mode.as_deref(), Some("PROVISIONED"));
        assert_eq!(s.table_creation_date_time, 1_757_400_000.0);
        assert_eq!(
            s.provisioned_throughput,
            Some(ProvisionedThroughput {
                read_capacity_units: 5,
                write_capacity_units: 5,
            })
        );
        assert_eq!(
            s.on_demand_throughput
                .as_ref()
                .and_then(|o| o.max_read_request_units),
            Some(100)
        );
        let f = desc.source_table_feature_details.expect("features");
        let gsi = &f.global_secondary_indexes.unwrap()[0];
        assert_eq!(gsi.index_name, "GenreIndex");
        assert_eq!(gsi.projection.projection_type, ProjectionType::Include);
        assert_eq!(
            gsi.provisioned_throughput,
            Some(ProvisionedThroughput {
                read_capacity_units: 1,
                write_capacity_units: 1,
            })
        );
        let lsi = &f.local_secondary_indexes.unwrap()[0];
        assert_eq!(lsi.index_name, "TitleIndex");
        assert_eq!(lsi.projection.projection_type, ProjectionType::KeysOnly);
        assert!(f.stream_description.unwrap().stream_enabled);
        assert_eq!(
            f.time_to_live_description
                .unwrap()
                .attribute_name
                .as_deref(),
            Some("expires_at")
        );
        let sse = f.sse_description.unwrap();
        assert_eq!(sse.status, "ENABLED");
        assert_eq!(sse.sse_type, Some(extenddb_core::types::SseType::KMS));
    }

    #[test]
    fn an_on_demand_source_reports_zero_throughput_on_the_table_and_its_indexes() {
        let mut row = row(BackupStatus::Available);
        row.source_table_details_json["billing_mode"] = "PAY_PER_REQUEST".into();
        let desc = row.description().unwrap();
        let zero = ProvisionedThroughput {
            read_capacity_units: 0,
            write_capacity_units: 0,
        };
        assert_eq!(
            desc.source_table_details.billing_mode.as_deref(),
            Some("PAY_PER_REQUEST")
        );
        assert_eq!(
            desc.source_table_details.provisioned_throughput,
            Some(zero.clone())
        );
        let f = desc.source_table_feature_details.unwrap();
        assert_eq!(
            f.global_secondary_indexes.unwrap()[0].provisioned_throughput,
            Some(zero)
        );
    }

    /// A source that recorded no index, stream, TTL or SSE still reports
    /// `SourceTableFeatureDetails`, as `{}` (the service's answer for such a
    /// table), with every inner member omitted.
    #[test]
    fn a_source_without_features_reports_empty_feature_details() {
        let mut row = row(BackupStatus::Available);
        let s = &mut row.source_table_details_json;
        s["global_secondary_indexes"] = serde_json::json!([]);
        s["local_secondary_indexes"] = serde_json::json!([]);
        s["stream_specification"] = serde_json::Value::Null;
        s["time_to_live"] = serde_json::Value::Null;
        s["sse_description"] = serde_json::Value::Null;
        let desc = row.description().unwrap();
        let features = desc
            .source_table_feature_details
            .expect("the member is always present");
        assert!(features.is_empty());
        assert_eq!(
            serde_json::to_value(&features).unwrap(),
            serde_json::json!({})
        );
    }

    #[test]
    fn a_summary_carries_the_source_table_id_and_arn() {
        let s = row(BackupStatus::Creating).summary().unwrap();
        assert_eq!(s.table_id, "12345a12-abcd-123a-ab12-1234abc12345");
        assert_eq!(
            s.table_arn,
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music"
        );
        assert_eq!(s.table_name, "Music");
        assert_eq!(s.backup_status, "CREATING");
        assert_eq!(s.backup_creation_date_time, 1_757_500_000.123);
    }

    #[test]
    fn deleting_is_reported_as_deleted_and_tombstones_are_hidden() {
        assert_eq!(
            row(BackupStatus::Deleting).details().backup_status,
            "DELETED"
        );
        assert!(BackupStatus::Deleting.is_described());
        assert!(!BackupStatus::Deleting.is_listed());
        for status in [BackupStatus::Deleted, BackupStatus::Failed] {
            assert!(!status.is_described(), "{status}");
            assert!(!status.is_listed(), "{status}");
        }
        for status in [BackupStatus::Creating, BackupStatus::Available] {
            assert!(status.is_described(), "{status}");
            assert!(status.is_listed(), "{status}");
            assert_eq!(status.reported(), status.as_str());
        }
    }

    #[test]
    fn a_source_table_this_build_cannot_read_is_internal() {
        let mut row = row(BackupStatus::Available);
        row.source_table_details_json = serde_json::json!({"table_name": "x"});
        assert!(matches!(row.description(), Err(StorageError::Internal(_))));
        assert!(matches!(row.summary(), Err(StorageError::Internal(_))));
    }

    /// A source table a later build extended with a member this build
    /// requires (here: every member but the two the summary reads is gone)
    /// still summarizes; only the description needs the whole object.
    #[test]
    fn a_summary_needs_only_the_source_table_id_and_arn() {
        let mut row = row(BackupStatus::Available);
        row.source_table_details_json = serde_json::json!({
            "table_id": "12345a12-abcd-123a-ab12-1234abc12345",
            "table_arn": "arn:aws:dynamodb:us-east-1:123456789012:table/Music",
            "member_this_build_does_not_know": true,
        });
        assert!(matches!(row.description(), Err(StorageError::Internal(_))));
        let s = row.summary().unwrap();
        assert_eq!(s.table_id, "12345a12-abcd-123a-ab12-1234abc12345");
        assert_eq!(
            s.table_arn,
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music"
        );
        row.source_table_details_json["table_arn"] = serde_json::json!(7);
        let e = row.summary().unwrap_err();
        assert!(e.to_string().contains("table_arn"), "{e}");
    }

    #[test]
    fn statuses_round_trip_through_their_column_values() {
        for status in [
            BackupStatus::Creating,
            BackupStatus::Available,
            BackupStatus::Deleting,
            BackupStatus::Deleted,
            BackupStatus::Failed,
        ] {
            assert_eq!(BackupStatus::parse(status.as_str()).unwrap(), status);
        }
        assert!(BackupStatus::parse("ACTIVE").is_err());
        for status in [
            JobStatus::Creating,
            JobStatus::Available,
            JobStatus::Running,
            JobStatus::Done,
            JobStatus::Failed,
        ] {
            assert_eq!(JobStatus::parse(status.as_str()).unwrap(), status);
        }
        for kind in [RestoreKind::Legacy, RestoreKind::Manifest] {
            assert_eq!(RestoreKind::parse(kind.as_str()).unwrap(), kind);
        }
        for kind in [StoreKind::Filesystem, StoreKind::S3] {
            assert_eq!(StoreKind::parse(kind.as_str()), Some(kind));
        }
        assert_eq!(StoreKind::parse("gcs"), None);
    }

    #[test]
    fn a_key_is_built_from_the_parsed_arn() {
        let arn = BackupArn::parse(
            "arn:aws:dynamodb:us-east-1:123456789012:table/Music/backup/01757500000123-0badcafe",
        )
        .unwrap();
        let key = BackupKey::from_arn(&arn);
        assert_eq!(key.account_id, "123456789012");
        assert_eq!(key.backup_id, "01757500000123-0badcafe");
        assert_eq!(key.backup_arn, arn.to_arn_string());
        assert_eq!(row(BackupStatus::Available).key(), key);
    }

    #[test]
    fn time_bounds_are_lower_inclusive_and_upper_exclusive() {
        let bounds = TimeBounds {
            lower: Some(10.0),
            upper: Some(20.0),
        };
        assert!(bounds.contains(10.0));
        assert!(bounds.contains(19.999));
        assert!(!bounds.contains(20.0));
        assert!(!bounds.contains(9.999));
        assert!(TimeBounds::default().contains(0.0));
    }

    #[test]
    fn the_pool_rule_is_twice_the_jobs_plus_ten() {
        assert_eq!(pool_size_for_jobs(4), 18);
        assert_eq!(pool_size_for_jobs(0), 10);
        assert_eq!(pool_size_for_jobs(16), 42);
    }

    #[tokio::test]
    async fn a_permit_is_taken_without_waiting_and_freed_on_drop() {
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let held = JobPermit::try_acquire(&slots).expect("a free slot");
        assert!(JobPermit::try_acquire(&slots).is_none());
        drop(held);
        assert!(JobPermit::try_acquire(&slots).is_some());
    }

    /// `acquire_within` takes a free slot at once whatever the wait, gives up
    /// once the wait passes with every slot held, and takes a slot freed
    /// during the wait.
    #[tokio::test]
    async fn a_permit_is_waited_for_within_the_bound() {
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let held = JobPermit::acquire_within(&slots, Duration::ZERO)
            .await
            .expect("a free slot is taken at once");
        assert!(
            JobPermit::acquire_within(&slots, Duration::from_millis(20))
                .await
                .is_none(),
            "every slot held past the bound"
        );
        let slots2 = Arc::clone(&slots);
        let waiter = tokio::spawn(async move {
            JobPermit::acquire_within(&slots2, Duration::from_secs(5)).await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(held);
        assert!(
            waiter.await.unwrap().is_some(),
            "the freed slot went to the waiter"
        );
    }

    /// A generation is never the unowned sentinel and always below 2^63, so
    /// every backend's signed integer column holds it unchanged and two
    /// claims never share a fencing token through saturation. Deterministic:
    /// 10,000 draws, each checked.
    #[test]
    fn generations_are_non_zero_and_fit_a_signed_integer_column() {
        for _ in 0..10_000 {
            let g = new_generation();
            assert_ne!(g, UNOWNED_GENERATION);
            assert!(g < 1u64 << 63, "{g} does not fit i64");
            assert_eq!(u64::try_from(i64::try_from(g).unwrap()).unwrap(), g);
        }
    }

    /// The one refusal text, by bound: a counted bound names its count and
    /// its name in one parenthesis; the server's connection slots name no
    /// count. Both carry the sentence a client matches on.
    #[test]
    fn limit_exceeded_message_names_the_bound() {
        assert_eq!(
            limit_exceeded_message(JobBound::Counted {
                name: "[backup] max_concurrent_jobs",
                limit: 4,
            }),
            "Too many backup and restore jobs are running on this server (at most 4 at once, \
             the [backup] max_concurrent_jobs bound); retry later"
        );
        assert_eq!(
            limit_exceeded_message(JobBound::ServerConnections),
            "Too many backup and restore jobs are running on this server (the database server \
             has no connection slot left for the job); retry later"
        );
    }

    /// The tally counts each (kind, root, id, status) apart, and the order
    /// is the bytes of kind, root and id, then the statuses in the order a
    /// row moves through them, not their names' order.
    #[test]
    fn the_root_tally_counts_each_store_and_status_apart_in_one_order() {
        let store = |kind: StoreKind, root: &str, id: &str| StoreIdentity {
            store_kind: kind,
            store_root: root.to_owned(),
            store_id: id.to_owned(),
        };
        let a1 = store(StoreKind::Filesystem, "/a", "id-1");
        let a2 = store(StoreKind::Filesystem, "/a", "id-2");
        let lower = store(StoreKind::Filesystem, "/a/b", "id-1");
        let s3 = store(StoreKind::S3, "s3://bucket/p", "id-1");
        let counts = StoreRootCount::tally([
            (s3.clone(), BackupStatus::Available),
            (a1.clone(), BackupStatus::Failed),
            (a1.clone(), BackupStatus::Available),
            (a2.clone(), BackupStatus::Available),
            (a1.clone(), BackupStatus::Available),
            (lower.clone(), BackupStatus::Creating),
            (a1.clone(), BackupStatus::Creating),
        ]);
        let line = |store: &StoreIdentity, status, rows| StoreRootCount {
            store: store.clone(),
            status,
            rows,
        };
        assert_eq!(
            counts,
            [
                line(&a1, BackupStatus::Creating, 1),
                line(&a1, BackupStatus::Available, 2),
                line(&a1, BackupStatus::Failed, 1),
                line(&a2, BackupStatus::Available, 1),
                line(&lower, BackupStatus::Creating, 1),
                line(&s3, BackupStatus::Available, 1),
            ]
        );
        assert!(StoreRootCount::tally([]).is_empty());
    }
}
