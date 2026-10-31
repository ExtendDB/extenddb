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
    BackupDescription, BackupDetails, BackupSummary, BillingMode, GlobalSecondaryIndexInfo,
    LocalSecondaryIndexInfo, ProvisionedThroughput, SourceTableDetails, SourceTableFeatureDetails,
    TableDescription,
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

/// How often a running job refreshes the heartbeat of the record it owns,
/// on every backend, until a later release wires `[backup]
/// heartbeat_interval_secs` to the jobs. `BackupConfig::validate` floors
/// `stale_job_after_secs` on this period, so a configured interval shorter
/// than it cannot make the reconciler declare a live job dead between two
/// heartbeats.
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
    /// `{0, 0}` on an on-demand table too. Feature members the source
    /// recorded nothing for are omitted.
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
            source_table_feature_details: (!features.is_empty()).then_some(features),
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
    pub completed: bool,
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

/// A restore job record the reconciler acts on: RUNNING with a dead owner,
/// or FAILED with its target still CREATING ([`BackupCatalog::list_dead_restore_jobs`]).
#[derive(Debug, Clone, PartialEq)]
pub struct RestoreJobRow {
    pub target: RestoreTargetKey,
    pub source_backup_arn: String,
    pub kind: RestoreKind,
    /// RUNNING or FAILED; the reconciler reads it to decide whether the
    /// FAILED write is still to be made.
    pub status: JobStatus,
    pub owner_generation: u64,
    /// Epoch seconds, on the database's clock.
    pub heartbeat_at: f64,
}

/// A legacy CREATING backup row whose create died.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbandonedLegacyBackup {
    pub account_id: String,
    pub backup_arn: String,
}

/// One of the `max_concurrent_jobs` slots of this instance, held by a backup
/// or restore job for its life. The engine takes it last among a request's
/// checks and hands it to the backend with the job; dropping it frees the
/// slot.
#[derive(Debug)]
pub struct JobPermit(#[allow(dead_code)] tokio::sync::OwnedSemaphorePermit);

impl JobPermit {
    /// Take a free slot of `slots`, or `None` when every slot is held. The
    /// request never waits: a queued job would hold a snapshot or a
    /// transaction while it waited.
    #[must_use]
    pub fn try_acquire(slots: &Arc<tokio::sync::Semaphore>) -> Option<Self> {
        Arc::clone(slots).try_acquire_owned().ok().map(Self)
    }

    /// A permit of a slot nobody else can take: for a caller that runs a
    /// job outside the engine's slots (a tool, a backend's own tests).
    #[must_use]
    pub fn detached() -> Self {
        Self::try_acquire(&Arc::new(tokio::sync::Semaphore::new(1))).expect("a fresh slot")
    }
}

/// The text every backend and the engine answer for a backup a CREATING
/// restore target still reads (`BackupInUseException` on the wire). One
/// implementation: the refusal is wire-visible and was spelled in each
/// backend before.
#[must_use]
pub fn backup_in_use_message(table: &str, backup_arn: &str) -> String {
    format!("Backup is being used to restore table {table}: {backup_arn}")
}

/// [`backup_in_use_message`] as the storage error the engine maps.
#[must_use]
pub fn backup_in_use(table: &str, backup_arn: &str) -> StorageError {
    StorageError::BackupInUse(backup_in_use_message(table, backup_arn))
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
    /// row ever exists unowned.
    ///
    /// # Errors
    ///
    /// `AlreadyExists` when a live owner holds the row's claim or the row
    /// exists.
    fn claim_backup<'a>(
        &'a self,
        row: &'a BackupRow,
    ) -> BoxFuture<'a, Result<Box<dyn JobClaim>, StorageError>>;

    /// Every row of `store` with `status`, or of every status for `None`.
    fn list_backup_rows<'a>(
        &'a self,
        status: Option<BackupStatus>,
        store: &'a StoreIdentity,
    ) -> BoxStream<'a, Result<BackupRow, StorageError>>;

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
    /// when it was not.
    fn set_backup_deleted<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>>;

    /// Remove a DELETED or FAILED tombstone; `false` when the row is not one.
    fn remove_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
    ) -> BoxFuture<'a, Result<bool, StorageError>>;

    /// Remove a CREATING row whose owner is dead: `heartbeat_at` older than
    /// `stale_after` on the catalog's clock. `false` when the row is not
    /// CREATING, is gone, or has a live owner. The maintenance command's
    /// write for a CREATING row its wait saw no live owner for, after it has
    /// deleted the row's prefix; the row's own job, were it alive, would
    /// find its claim lost at its next heartbeat.
    fn remove_dead_creating_backup_row<'a>(
        &'a self,
        key: &'a BackupKey,
        stale_after: Duration,
    ) -> BoxFuture<'a, Result<bool, StorageError>>;

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

    /// RUNNING to FAILED with `reason` on the target's job record, conditional
    /// on RUNNING; `false` when the record was not RUNNING or is gone. The
    /// reconciler's write for a dead legacy restore, which is never resumed.
    fn fail_restore_job<'a>(
        &'a self,
        key: &'a RestoreTargetKey,
        reason: &'a str,
    ) -> BoxFuture<'a, Result<bool, StorageError>>;

    /// The distinct `store_id`s the catalog's rows record for a root, of
    /// every status: the store identity check reads them before it knows an
    /// id. Empty when no row names the root.
    fn read_store_ids<'a>(
        &'a self,
        store_kind: StoreKind,
        store_root: &'a str,
    ) -> BoxFuture<'a, Result<Vec<String>, StorageError>>;

    /// Take the catalog-level maintenance claim for `holder` (a label naming
    /// the command and the host it runs on, for the operator who finds it
    /// held). The claim is one record, `backup_maintenance`, born owned
    /// with a fresh generation and heartbeated through the handle; a holder
    /// whose heartbeat is older than `stale_after` on the catalog's clock is
    /// dead and its claim is taken over. PostgreSQL is the exception to the
    /// liveness rule on the holder's side: there the holder's session holds
    /// an advisory lock, a holder whose session ended is dead whatever its
    /// heartbeat says, and `stale_after` is read by [`check_maintenance_claim`]
    /// alone; on SQLite a server's startup under the serve lock removes the
    /// record outright, since no holder can be alive there.
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
    /// catalog's clock. Jobs check it after their own claim and row exist and
    /// refuse while it is held; the reconciler runs only the duties the
    /// holder waits on.
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
/// while no `[backup.store]` is configured.
///
/// The one part that is not frozen is the restore's registration: a restore
/// registers through the restore job fields (`kind = legacy`, the claim), so
/// the reconciler finds a restore whose owner died as it finds any other.
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
    /// holds. The reconciler removes each through [`Self::delete`]. Empty on
    /// a backend whose legacy create commits its row AVAILABLE with the copy.
    fn abandoned_creating_backups(
        &self,
        stale_after: Duration,
    ) -> BoxFuture<'_, Result<Vec<AbandonedLegacyBackup>, StorageError>>;

    /// Describe continuous backups / PITR status for a table.
    fn describe_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<extenddb_core::types::ContinuousBackupsDescription, StorageError>>;

    /// Update continuous backups (enable/disable PITR).
    fn update_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
        pitr_enabled: bool,
    ) -> BoxFuture<'_, Result<extenddb_core::types::ContinuousBackupsDescription, StorageError>>;
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

    #[test]
    fn a_source_without_features_omits_the_feature_details() {
        let mut row = row(BackupStatus::Available);
        let s = &mut row.source_table_details_json;
        s["global_secondary_indexes"] = serde_json::json!([]);
        s["local_secondary_indexes"] = serde_json::json!([]);
        s["stream_specification"] = serde_json::Value::Null;
        s["time_to_live"] = serde_json::Value::Null;
        s["sse_description"] = serde_json::Value::Null;
        let desc = row.description().unwrap();
        assert!(desc.source_table_feature_details.is_none());
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
}
