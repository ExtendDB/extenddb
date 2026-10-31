// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The in-catalog backup format on `MongoDB`: its `LegacyBackupReader`.
//!
//! Backups are stored as one MongoDB collection per backup, plus a `backups`
//! metadata collection in the catalog. `CreateBackup` uses MongoDB's
//! server-side aggregation `$out` stage to clone the source data collection
//! into `_backup_<backup_id>` in the data database, with no per-item traffic
//! between the driver and the server. `RestoreTableFromBackup` uses the same
//! stage in reverse, in a task detached from the request. `DeleteBackup` drops
//! the collection.
//!
//! Backup metadata carries a `backup_id` UUID; the collection name is derived
//! from that id so the `backup_arn` (which contains slashes and colons) never
//! appears in a collection name.
//!
//! # The restore job record, DeleteBackup and the claim
//!
//! A restore registers through the job record on its target's `tables`
//! document (`restore_job`: `status`, `kind = legacy`, `owner_generation`,
//! `heartbeat_at`, [`crate::backup_catalog`]), beside the provenance fields
//! `DescribeTable` reports. The registration is one session transaction on
//! the catalog database: it `$inc`s `lock_seq` on the backup's `backups`
//! document while that document is AVAILABLE, checks that no CREATING target
//! is restoring the backup already, and inserts the CREATING target. The
//! legacy DeleteBackup takes the same lock in its own transaction before it
//! re-checks for a CREATING target and writes DELETING (on the delete's side
//! the DELETING write is itself a write to the document, so the `$inc` there
//! is what makes a refused delete, `Status` or `InUse`, conflict too), so
//! the two order wholly one before the other: a DeleteBackup that wins leaves the
//! registration a document that is not AVAILABLE (`Backup not found`, no
//! target), and a registration that wins makes the DeleteBackup `BackupInUse`.
//! From then on the CREATING target is what keeps DeleteBackup and other
//! restores of the backup out, until the target is ACTIVE, as the service
//! answers. Both in-use checks read at the transaction read concern
//! (`transaction_read_concern`, `snapshot` by default), which is
//! majority-committed data, while `DescribeTable` reads the catalog at
//! `local`: a client whose `DescribeTable` has just shown the target ACTIVE
//! can be answered `BackupInUse` by a RestoreTableFromBackup or DeleteBackup
//! issued within the majority-commit lag (the journal flush, and any
//! concurrent batch write still holding an earlier oplog slot), and the
//! retry succeeds. That lock replaces the claim markers earlier releases wrote on
//! the `backups` document (`restore_in_progress`, `delete_in_progress` and
//! their heartbeats): nothing writes or reads them now, and a marker a dead
//! process left no longer blocks the backup until a sweep clears it. Once
//! DeleteBackup has written DELETING the backup never reads AVAILABLE again:
//! the drop that follows may have run on the server even if the request
//! stops before its reply, and a later DeleteBackup of a DELETING backup
//! resumes the drop and the DELETED write instead of answering that the
//! backup is in use; `describe` and `list` leave DELETING out as they leave
//! DELETED out.
//!
//! The restore's claim is the job record: the target is inserted RUNNING
//! under a fresh `owner_generation`, and the copy task refreshes
//! `heartbeat_at` with the server's clock (`$$NOW`) at once and then every
//! [`JOB_HEARTBEAT_INTERVAL`], the one constant every backend's heartbeat
//! loop reads and the stale floor is computed from (`[backup]
//! heartbeat_interval_secs` reaches the backends in a later release); a
//! refresh that matches other than exactly one document, or fails, means the
//! claim is lost and the copy stops without touching the target (the FAILED
//! write and the removal, each conditioned on the generation, are what
//! follow); the `$out` already sent runs to its end on the server, a write
//! stage the server does not interrupt for a client's disconnect, and then
//! fails rather than recreate the data collection the removal dropped, since
//! `$out` refuses to replace a target whose indexes changed while it ran (see
//! `copy_restore`), so a lost heartbeat leaves no orphan collection. The
//! copy's hand-over to the GSI backfill worker marks the
//! record DONE in the same write, conditioned on the generation and on
//! RUNNING; a failure marks it FAILED under the same condition and removes
//! the target only when that matched. A target whose record is RUNNING with
//! a stale heartbeat has a dead owner: the reconciler
//! (`BackupCatalog::list_dead_restore_jobs`) fails it and removes it; a
//! FAILED record whose target is still CREATING (the removal did not land)
//! is listed once the record is stale, and the reconciler removes the target
//! with no second FAILED write. The removal deletes the target's `tables`
//! document first and then drops its data and index collections; once the
//! document is gone the target is removed (the name and the backup are
//! free, and no record is left for a later cycle), so a drop that fails
//! after it is logged with the collections' names and leaves them in the
//! data database for `backup sync` to sweep, as nothing in the catalog
//! points at them.

use std::time::{Duration, Instant};

use futures::TryStreamExt;
use futures::future::BoxFuture;
use mongodb::bson::{self, Bson, Document, doc};
use serde::Serialize;
use serde::de::DeserializeOwned;

use extenddb_core::types::{
    AttributeDefinition, BackupDescription, BackupDetails, BackupSummary,
    ContinuousBackupsDescription, GsiInput, KeySchemaElement, LsiInput,
    PointInTimeRecoveryDescription, Projection, ProvisionedThroughput, SourceTableDetails,
    StreamSpecification, TableDescription, TimeToLiveDescription, TimeToLiveStatus,
};
use extenddb_storage::backup_catalog::{
    AbandonedLegacyBackup, BackupCatalog, JOB_HEARTBEAT_INTERVAL, JobStatus, RestoreKind,
    RestoreTargetKey, new_generation,
};
use extenddb_storage::backup_definition::{BACKUP_DEFINITION_VERSION, BackupTableDefinition};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupRestoreSource, JobPermit, LegacyBackupReader, RestoreRequest, util};

use crate::MongoEngine;
use crate::backup_catalog::{
    LOCK_SEQ, RESTORE_JOB, RESTORE_JOB_COMPLETED_AT, RESTORE_JOB_FAILURE_REASON,
    RESTORE_JOB_GENERATION, RESTORE_JOB_HEARTBEAT, RESTORE_JOB_STATUS, backup_in_use,
    generation_field,
};
use crate::data::data_collection_name;
use crate::data_engine::{CommitOutcome, TRANSIENT_RETRY_ATTEMPTS, TxErr, backoff_sleep};

/// The definition version stored on a backup document written by this build.
/// Backup documents written before it carry the same fields without the
/// marker and are read as version 1.
const DEFINITION_VERSION_FIELD: &str = "definition_version";

/// How long a request waits for its detached task to register the CREATING
/// target before it answers `Internal`. The bound is what turns a connection
/// pool that has stopped handing out connections into a 500 instead of a
/// request that never returns (the driver does not enforce
/// `waitQueueTimeoutMS`, so a checkout waits for as long as every connection
/// stays checked out). The task goes on: a request that hit the bound may
/// leave a CREATING target that appears after its error, holds the backup in
/// use (`BackupInUseException` to DeleteBackup) and takes the name until the
/// copy ends, and DescribeTable is where a client finds it.
pub(crate) const RESTORE_REGISTRATION_TIMEOUT: Duration = Duration::from_secs(60);

/// Heartbeat timing of a restore's job record. Production uses the default,
/// [`JOB_HEARTBEAT_INTERVAL`]; tests shorten it so liveness can be observed
/// in seconds.
#[derive(Debug, Clone)]
pub(crate) struct BackupLiveness {
    pub(crate) heartbeat_interval: Duration,
    /// Test only: when set, a restore's task waits for one permit after the
    /// name check and before it registers its target, so a test can order a
    /// registration after a DeleteBackup of the same backup.
    #[cfg(test)]
    pub(crate) registration_gate: Option<std::sync::Arc<tokio::sync::Semaphore>>,
    /// Test only: when set, a restore's copy task waits for one permit after
    /// registering its target and before its base copy, so a test can observe
    /// the CREATING window deterministically.
    #[cfg(test)]
    pub(crate) copy_gate: Option<std::sync::Arc<tokio::sync::Semaphore>>,
    /// Test only: what a DeleteBackup does right after it dropped a backup's
    /// data collection.
    #[cfg(test)]
    pub(crate) after_backup_drop: Option<AfterBackupDrop>,
    /// Test only: when set, `remove_restore_target` treats the reply to the
    /// target's collection drops as lost (an error after the drops ran).
    #[cfg(test)]
    pub(crate) target_drop_fails: bool,
    /// Test only: how far this process's clock is behind the server's, applied
    /// to the one heartbeat written with the local clock (the target insert).
    #[cfg(test)]
    pub(crate) local_clock_behind: Duration,
    /// Test only: when set, the restore heartbeat fails at its next tick once
    /// the test has armed the cell (an injected error, as a network fault on
    /// the heartbeat's connection would be), and records that it did:
    /// [`HeartbeatFault::ARMED`] is the test's request,
    /// [`HeartbeatFault::INJECTED`] the loop's answer.
    #[cfg(test)]
    pub(crate) heartbeat_fault: Option<std::sync::Arc<std::sync::atomic::AtomicU8>>,
}

/// Test only: the two ways a backup drop can end after the server ran it.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AfterBackupDrop {
    /// The reply is an error (an ambiguous network or timeout error).
    Fail,
    /// The reply never arrives, so the caller's future is dropped while
    /// waiting for it (a client timeout).
    Hang,
}

/// Test only: the states of [`BackupLiveness::heartbeat_fault`].
#[cfg(test)]
pub(crate) struct HeartbeatFault;

#[cfg(test)]
impl HeartbeatFault {
    /// Nothing requested.
    pub(crate) const IDLE: u8 = 0;
    /// The next heartbeat tick fails.
    pub(crate) const ARMED: u8 = 1;
    /// A heartbeat failed as requested.
    pub(crate) const INJECTED: u8 = 2;
}

impl Default for BackupLiveness {
    fn default() -> Self {
        Self {
            heartbeat_interval: JOB_HEARTBEAT_INTERVAL,
            #[cfg(test)]
            registration_gate: None,
            #[cfg(test)]
            copy_gate: None,
            #[cfg(test)]
            after_backup_drop: None,
            #[cfg(test)]
            target_drop_fails: false,
            #[cfg(test)]
            local_clock_behind: Duration::ZERO,
            #[cfg(test)]
            heartbeat_fault: None,
        }
    }
}

/// Return the MongoDB collection name that holds items for a given backup.
///
/// The collection lives in the data database. The name is derived from the
/// backup's UUID so it is safe for MongoDB (no colons, slashes, or dots) and
/// bounded in length regardless of how long the source `backup_arn` is.
fn backup_collection_name(backup_id: &str) -> String {
    format!("_backup_{backup_id}")
}

impl MongoEngine {
    /// Copy collection `src` to `dst` in the data database with a server-side
    /// `$out` stage (no per-item traffic to the driver), for CreateBackup and
    /// for a restore's base copy. The stage writes with `w: majority`
    /// whatever the server default (see [`crate::majority_write_concern`]):
    /// the backup or target is published as AVAILABLE or handed over right
    /// after, and must not lose the copied items to a failover.
    async fn out_copy(&self, src: &str, dst: &str) -> Result<(), StorageError> {
        let out_cursor = self
            .data_db
            .collection::<Document>(src)
            .aggregate(vec![doc! { "$out": dst }])
            .write_concern(crate::majority_write_concern(self.client.write_concern()))
            .await
            .map_err(internal)?;
        // `$out` writes to the target collection and returns an empty cursor;
        // consume it so the stage has completed before the caller counts.
        let _drained: Vec<Document> = out_cursor.try_collect().await.map_err(internal)?;
        Ok(())
    }
}

#[allow(clippy::cast_precision_loss)]
fn now_epoch_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as f64
}

#[allow(clippy::cast_precision_loss)]
fn datetime_epoch_secs(dt: bson::DateTime) -> f64 {
    dt.timestamp_millis() as f64 / 1000.0
}

#[allow(clippy::cast_possible_truncation)]
fn epoch_secs_datetime(secs: f64) -> bson::DateTime {
    bson::DateTime::from_millis((secs * 1000.0).round() as i64)
}

fn internal(e: impl std::fmt::Display) -> StorageError {
    StorageError::Internal(e.to_string())
}

fn decode_optional<T: DeserializeOwned>(
    doc: &Document,
    key: &str,
) -> Result<Option<T>, StorageError> {
    match doc.get(key) {
        None | Some(Bson::Null) => Ok(None),
        Some(value) => bson::from_bson(value.clone())
            .map(Some)
            .map_err(|e| StorageError::Internal(format!("parse {key}: {e}"))),
    }
}

fn decode_required<T: DeserializeOwned>(doc: &Document, key: &str) -> Result<T, StorageError> {
    let value = doc
        .get(key)
        .ok_or_else(|| StorageError::Internal(format!("missing {key}")))?;
    bson::from_bson(value.clone()).map_err(|e| StorageError::Internal(format!("parse {key}: {e}")))
}

fn insert_non_empty_array<T: Serialize>(
    doc: &mut Document,
    key: &str,
    values: &[T],
) -> Result<(), StorageError> {
    if !values.is_empty() {
        let value = bson::to_bson(values)
            .map_err(|e| StorageError::Internal(format!("serialize {key}: {e}")))?;
        doc.insert(key, value);
    }
    Ok(())
}

fn restore_provisioned_throughput(
    billing_mode: &str,
    stored: Option<ProvisionedThroughput>,
) -> Option<ProvisionedThroughput> {
    if billing_mode == "PROVISIONED" {
        Some(stored.unwrap_or(ProvisionedThroughput {
            read_capacity_units: 5,
            write_capacity_units: 5,
        }))
    } else {
        None
    }
}

/// GSI throughput follows the table's billing mode the same way: a table
/// switched from PROVISIONED to PAY_PER_REQUEST keeps its indexes' old
/// capacities in the catalog, and restoring them would make the restored
/// on-demand table's indexes report capacity a fresh one does not.
fn restore_gsi_throughput(
    billing_mode: &str,
    gsis: Option<Vec<GsiInput>>,
) -> Option<Vec<GsiInput>> {
    if billing_mode == "PROVISIONED" {
        return gsis;
    }
    gsis.map(|gsis| {
        gsis.into_iter()
            .map(|mut gsi| {
                gsi.provisioned_throughput = None;

                gsi
            })
            .collect()
    })
}

/// The filter of a CREATING restore target registered from `backup_arn` in
/// `account_id`: the in-use rule, which reads the target's status, not the
/// job's, so it also covers the window between the job's DONE write and the
/// backfill worker's ACTIVE flip.
pub(crate) fn restoring_target_filter(account_id: &str, backup_arn: &str) -> Document {
    doc! {
        "_id.account_id": account_id,
        "restore_source_backup_arn": backup_arn,
        "table_status": "CREATING",
    }
}

/// `$expr` that is true when the first of `fields` that is set is older than
/// `stale_after` by the server clock; a document with none of them set is as
/// old as the epoch. The reconciler's two ages: a RUNNING record's heartbeat,
/// and a FAILED record's `completed_at` or, where none was stamped, its
/// heartbeat.
pub(crate) fn age_is_stale(fields: &[&str], stale_after: Duration) -> Document {
    let stale_ms = i64::try_from(stale_after.as_millis()).unwrap_or(i64::MAX);
    let age = fields.iter().rev().fold(
        Bson::DateTime(bson::DateTime::from_millis(0)),
        |fallback, field| Bson::Document(doc! { "$ifNull": [ format!("${field}"), fallback ] }),
    );
    doc! {
        "$lt": [
            age,
            { "$subtract": [ "$$NOW", stale_ms ] },
        ]
    }
}

/// What a restore's registration writes into the target's first catalog
/// document: the provenance `DescribeTable` reports and the job record.
pub(crate) struct RestoreRegistration<'a> {
    pub(crate) backup_arn: &'a str,
    pub(crate) source_table_arn: &'a str,
    pub(crate) restore_date_time: bson::DateTime,
    /// The claim's fencing token, written as `restore_job.owner_generation`.
    pub(crate) owner_generation: u64,
    /// The record's first heartbeat. An insert cannot use `$$NOW`, so this is
    /// the inserting process's clock; the copy task's first refresh replaces
    /// it with the server's as soon as the task starts.
    pub(crate) first_heartbeat: bson::DateTime,
    /// The session whose transaction the target's insert runs in and which
    /// the insert commits; `None` inserts and commits nothing beyond the
    /// document itself.
    pub(crate) session: Option<&'a mut mongodb::ClientSession>,
}

impl RestoreRegistration<'_> {
    pub(crate) fn insert_into(&self, table_doc: &mut Document) {
        table_doc.insert("restore_source_backup_arn", self.backup_arn);
        table_doc.insert("restore_source_table_arn", self.source_table_arn);
        table_doc.insert("restore_date_time", self.restore_date_time);
        table_doc.insert(
            RESTORE_JOB,
            doc! {
                "status": JobStatus::Running.as_str(),
                "kind": RestoreKind::Legacy.as_str(),
                "owner_generation": generation_field(self.owner_generation),
                "heartbeat_at": self.first_heartbeat,
            },
        );
    }
}

/// Why a legacy registration or DeleteBackup found no AVAILABLE backup
/// document to lock, from a re-read of the document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LockMiss {
    /// Missing, another account's, DELETED, or in a state no release writes.
    NotFound,
    /// A DeleteBackup has written DELETING and is dropping the data.
    Deleting,
}

fn lock_miss(doc: Option<&Document>) -> LockMiss {
    match doc.and_then(|d| d.get_str("backup_status").ok()) {
        Some("DELETING") => LockMiss::Deleting,
        _ => LockMiss::NotFound,
    }
}

impl MongoEngine {
    /// Drop a backup's data collection (missing is success). In test builds
    /// [`BackupLiveness::after_backup_drop`] can make it fail or hang after the
    /// drop ran.
    async fn drop_backup_collection(&self, backup_id: &str) -> Result<(), StorageError> {
        self.drop_collection_if_exists(&backup_collection_name(backup_id))
            .await?;
        #[cfg(test)]
        match self.backup_liveness.after_backup_drop {
            Some(AfterBackupDrop::Fail) => {
                return Err(StorageError::Internal(
                    "test: the reply to the backup drop was lost".to_owned(),
                ));
            }
            Some(AfterBackupDrop::Hang) => std::future::pending::<()>().await,
            None => {}
        }
        Ok(())
    }

    /// Drop a removed restore target's data and index collections and its
    /// index documents. Tries every step and returns the first error.
    pub(crate) async fn remove_target_collections(
        &self,
        table_id: &str,
    ) -> Result<(), StorageError> {
        let mut cleanup_error = None;
        if let Err(e) = self
            .drop_collection_if_exists(&data_collection_name(table_id))
            .await
        {
            cleanup_error = Some(e);
        }
        if let Err(e) = self.drop_index_collections_for_table(table_id).await {
            cleanup_error.get_or_insert(e);
        }
        self.gsi_cache_invalidate(table_id);
        cleanup_error.map_or(Ok(()), Err)
    }

    /// This process's clock, for the one heartbeat that cannot use the
    /// server's (the target insert). Test builds can set it behind.
    fn local_now(&self) -> bson::DateTime {
        let now = bson::DateTime::now();
        #[cfg(test)]
        let now = bson::DateTime::from_millis(
            now.timestamp_millis()
                - i64::try_from(self.backup_liveness.local_clock_behind.as_millis())
                    .unwrap_or(i64::MAX),
        );
        now
    }

    /// `TableBeingRestored` when `target_table_name` is a CREATING restore
    /// target, naming its source backup; `Ok(())` otherwise. Unfenced: the
    /// target insert is the fence. Checked first, so a restore into such a
    /// name is refused as the name (the service's TableInUseException) at
    /// once, whatever state the backup is in, as on the other backends.
    async fn refuse_a_name_being_restored(
        &self,
        account_id: &str,
        target_table_name: &str,
    ) -> Result<(), StorageError> {
        if let Some(target) = self
            .catalog_db
            .collection::<Document>("tables")
            .find_one(doc! {
                "_id": { "account_id": account_id, "table_name": target_table_name },
                "table_status": "CREATING",
                "restore_source_backup_arn": { "$exists": true },
            })
            .await
            .map_err(internal)?
            && let Ok(source) = target.get_str("restore_source_backup_arn")
        {
            return Err(StorageError::TableBeingRestored {
                table_name: target_table_name.to_owned(),
                backup_arn: source.to_owned(),
            });
        }
        Ok(())
    }

    /// Register a restore: one session transaction that locks the backup
    /// document (`$inc lock_seq` while AVAILABLE), checks that no CREATING
    /// target is restoring the backup, and inserts the CREATING target with
    /// its provenance and its RUNNING job record. The transaction commits
    /// right after the insert, before the target's collections and indexes
    /// are created; a transaction that lost a write conflict (a DeleteBackup
    /// or another registration of the backup) is re-run and sees the other's
    /// result. Returns the target's description and the backup's data
    /// collection id.
    ///
    /// Refusals: a missing, another account's, DELETED or DELETING backup is
    /// `Backup not found`; a CREATING target of the backup is `BackupInUse`
    /// naming it, or `TableBeingRestored` when that target is the name asked
    /// for; a taken name is what `create_table_for_restore` reports.
    async fn register_restore(
        &self,
        account_id: &str,
        request: &RestoreRequest,
        owner_generation: u64,
    ) -> Result<(TableDescription, String), StorageError> {
        let RestoreRequest {
            backup_arn,
            source_table_arn,
            restore_date_time,
            input,
        } = request;
        let target_table_name = input.table_name.as_str();
        let backups_coll = self.catalog_db.collection::<Document>("backups");
        let mut session = self.client.start_session().await.map_err(internal)?;
        let tx_options = self.write_transaction_options();
        for attempt in 0..TRANSIENT_RETRY_ATTEMPTS {
            session
                .start_transaction()
                .with_options(tx_options.clone())
                .await
                .map_err(internal)?;
            let body: Result<Result<(TableDescription, String), StorageError>, TxErr> = async {
                let locked = backups_coll
                    .find_one_and_update(
                        doc! {
                            "_id": backup_arn,
                            "account_id": account_id,
                            "backup_status": "AVAILABLE",
                        },
                        doc! { "$inc": { LOCK_SEQ: 1_i64 } },
                    )
                    .session(&mut session)
                    .await
                    .map_err(TxErr::from)?;
                let Some(backup_doc) = locked else {
                    // Missing, another account's, DELETED, or DELETING: the
                    // backup is not there to restore from (a DELETING one has
                    // its data dropped by the DeleteBackup that won).
                    return Ok(Err(StorageError::Validation(format!(
                        "Backup not found: {backup_arn}"
                    ))));
                };
                let backup_id =
                    backup_doc
                        .get_str("backup_id")
                        .map(str::to_owned)
                        .map_err(|_| {
                            TxErr::Fatal(StorageError::Internal(format!(
                                "backup metadata for {backup_arn} has no backup_id"
                            )))
                        })?;
                // Under the lock: a CREATING target of this backup belongs to a
                // restore that is still running (or one abandoned and not yet
                // removed, which blocks DeleteBackup the same way). When that
                // target is the name this restore asked for, the name is what
                // is refused (TableBeingRestored, the service's
                // TableInUseException), as on the other backends.
                if let Some(restoring) = self
                    .creating_restore_target(account_id, backup_arn, Some(&mut session))
                    .await
                    .map_err(TxErr::from)?
                {
                    return Ok(Err(if restoring == target_table_name {
                        StorageError::TableBeingRestored {
                            table_name: target_table_name.to_owned(),
                            backup_arn: backup_arn.clone(),
                        }
                    } else {
                        backup_in_use(&restoring, backup_arn)
                    }));
                }
                let mut registration = RestoreRegistration {
                    backup_arn,
                    source_table_arn,
                    restore_date_time: epoch_secs_datetime(*restore_date_time),
                    owner_generation,
                    first_heartbeat: self.local_now(),
                    session: Some(&mut session),
                };
                match self
                    .create_table_for_restore(account_id, input.clone(), &mut registration)
                    .await
                {
                    Ok(desc) => Ok(Ok((desc, backup_id))),
                    // The insert's transaction lost a write conflict and was
                    // aborted before anything outside it was created.
                    Err(StorageError::Transient(_)) => Err(TxErr::Transient),
                    Err(e) => {
                        // A creation that failed after its insert committed
                        // (a collection or index could not be created) left a
                        // registered target under this generation; a name
                        // refusal left nothing, and the removal matches
                        // nothing.
                        if let Err(cleanup) = self
                            .remove_target_of_generation(
                                account_id,
                                target_table_name,
                                owner_generation,
                            )
                            .await
                        {
                            tracing::error!(
                                "restore of {backup_arn} into {target_table_name} failed during \
                                 target creation ({e}), and cleanup failed: {cleanup}"
                            );
                        }
                        Ok(Err(e))
                    }
                }
            }
            .await;
            match body {
                // The insert committed its transaction before the target's
                // collections were created; nothing is left to commit.
                Ok(Ok(registered)) => {
                    let (mut desc, backup_id) = registered;
                    desc.restore_summary = Some(extenddb_core::types::RestoreSummary {
                        source_backup_arn: Some(backup_arn.clone()),
                        source_table_arn: Some(source_table_arn.clone()),
                        restore_date_time: *restore_date_time,
                        restore_in_progress: true,
                    });
                    return Ok((desc, backup_id));
                }
                Ok(Err(refusal)) => {
                    let _ = session.abort_transaction().await;
                    return Err(refusal);
                }
                Err(TxErr::Transient) => {
                    let _ = session.abort_transaction().await;
                    backoff_sleep(attempt).await;
                }
                Err(TxErr::Fatal(e)) => {
                    let _ = session.abort_transaction().await;
                    return Err(e);
                }
            }
        }
        Err(StorageError::Internal(format!(
            "the registration of the restore of {backup_arn} into {target_table_name} did not \
             commit after {TRANSIENT_RETRY_ATTEMPTS} conflicting attempts"
        )))
    }

    /// Remove the CREATING restore target `target_table_name` registered
    /// under `owner_generation`, with its collections; `Ok(false)` when there
    /// is none.
    async fn remove_target_of_generation(
        &self,
        account_id: &str,
        target_table_name: &str,
        owner_generation: u64,
    ) -> Result<bool, StorageError> {
        let removed = self
            .catalog_db
            .collection::<Document>("tables")
            .find_one_and_delete(doc! {
                "_id": { "account_id": account_id, "table_name": target_table_name },
                "table_status": "CREATING",
                RESTORE_JOB_GENERATION: generation_field(owner_generation),
            })
            .await
            .map_err(internal)?;
        let Some(removed) = removed else {
            return Ok(false);
        };
        let table_id = removed
            .get_str("table_id")
            .map_err(|_| StorageError::Internal("restore target missing table_id".to_owned()))?;
        self.remove_target_collections(table_id).await?;
        Ok(true)
    }

    /// The heartbeat of a running restore: refresh the job record's
    /// `heartbeat_at` with the server's clock at once and then every
    /// [`BackupLiveness::heartbeat_interval`], for as long as the record is
    /// RUNNING under `owner_generation`. Returns when a refresh matches other
    /// than exactly one document or fails: the claim is lost and the caller
    /// stops the copy.
    async fn restore_heartbeat_loop(
        &self,
        account_id: &str,
        target_table_name: &str,
        table_id: &str,
        owner_generation: u64,
    ) -> StorageError {
        let tables_coll = self.catalog_db.collection::<Document>("tables");
        let filter = doc! {
            "_id": { "account_id": account_id, "table_name": target_table_name },
            "table_id": table_id,
            RESTORE_JOB_STATUS: JobStatus::Running.as_str(),
            RESTORE_JOB_GENERATION: generation_field(owner_generation),
        };
        loop {
            #[cfg(test)]
            if let Some(cell) = &self.backup_liveness.heartbeat_fault
                && cell
                    .compare_exchange(
                        HeartbeatFault::ARMED,
                        HeartbeatFault::INJECTED,
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                    )
                    .is_ok()
            {
                return StorageError::Internal(
                    "restore heartbeat: injected connection error".to_owned(),
                );
            }
            match tables_coll
                .update_one(
                    filter.clone(),
                    vec![doc! { "$set": { RESTORE_JOB_HEARTBEAT: "$$NOW" } }],
                )
                .await
            {
                Ok(result) if result.matched_count == 1 => {}
                Ok(result) => {
                    return StorageError::Internal(format!(
                        "the restore job record no longer carries this claim ({} document(s) \
                         matched)",
                        result.matched_count
                    ));
                }
                Err(e) => return StorageError::Internal(format!("restore heartbeat: {e}")),
            }
            tokio::time::sleep(self.backup_liveness.heartbeat_interval).await;
        }
    }

    /// RUNNING to FAILED on the job record of the target, conditioned on
    /// `owner_generation`: the write a failing job makes before it removes
    /// its target. `Ok(false)` when the record is not RUNNING under that
    /// generation.
    async fn fail_restore_job_as(
        &self,
        account_id: &str,
        target_table_name: &str,
        table_id: &str,
        owner_generation: u64,
        reason: &str,
    ) -> Result<bool, StorageError> {
        let result = self
            .catalog_db
            .collection::<Document>("tables")
            .update_one(
                doc! {
                    "_id": { "account_id": account_id, "table_name": target_table_name },
                    "table_id": table_id,
                    RESTORE_JOB_STATUS: JobStatus::Running.as_str(),
                    RESTORE_JOB_GENERATION: generation_field(owner_generation),
                },
                vec![doc! { "$set": {
                    RESTORE_JOB_STATUS: { "$literal": JobStatus::Failed.as_str() },
                    RESTORE_JOB_FAILURE_REASON: { "$literal": reason },
                    RESTORE_JOB_COMPLETED_AT: "$$NOW",
                } }],
            )
            .await
            .map_err(internal)?;
        Ok(result.matched_count == 1)
    }

    /// The detached copy of a restore: copy the backup's items into the
    /// target with `$out` while the job record is heartbeated, then hand the
    /// target to the backfill worker, marking the record DONE in the same
    /// write. On failure the record is marked FAILED and the target removed,
    /// each conditioned on the generation. A lost heartbeat takes that path
    /// at once, dropping the copy future; the `$out` already sent is a write
    /// stage, which the server does not interrupt when its client
    /// disconnects, so it runs to its end there and then fails: the removal
    /// dropped the target's data collection, which the registration created
    /// with its indexes, and `$out` refuses to replace a target whose indexes
    /// changed while it ran, so it leaves no collection behind (MongoDB 7.0,
    /// error 125 "indexes of target collection changed during processing").
    /// Logs the outcome either way.
    async fn copy_restore(
        &self,
        account_id: &str,
        target_table_name: &str,
        backup_arn: &str,
        owner_generation: u64,
        backup_id: &str,
        table_id: &str,
    ) {
        let started = Instant::now();
        let copy = async {
            #[cfg(test)]
            if let Some(gate) = &self.backup_liveness.copy_gate {
                gate.acquire().await.map_err(internal)?.forget();
            }
            // The test-hook gate holds the restore after its CREATING index
            // metadata exists but before the base `$out` copy begins. This
            // lets the integration test force a worker tick through the
            // dangerous pre-copy window.
            self.wait_for_gsi_backfill_test_gate(target_table_name)
                .await?;

            // The backup collection has the source document shape, so this is
            // a direct server-side clone with no per-item transformation.
            let new_coll_name = data_collection_name(table_id);
            self.out_copy(&backup_collection_name(backup_id), &new_coll_name)
                .await?;

            let item_count = self
                .data_db
                .collection::<Document>(&new_coll_name)
                .count_documents(doc! {})
                .await
                .map_err(internal)?;

            // The base copy is complete, but restored secondary-index
            // collections are still empty. Mark the table pending restore
            // completion and leave its indexes CREATING so the shared worker
            // can backfill them in bounded, restartable batches, and mark the
            // job record DONE in the same write, conditioned on its generation
            // and on RUNNING. From here the worker owns the target: it
            // promotes each index and flips the table ACTIVE, conditioned on
            // the flag and on CREATING, and the reconciler leaves a DONE
            // record alone.
            let handed_over = self
                .catalog_db
                .collection::<Document>("tables")
                .update_one(
                    doc! {
                        "_id": { "account_id": account_id, "table_name": target_table_name },
                        "table_id": table_id,
                        "table_status": "CREATING",
                        RESTORE_JOB_STATUS: JobStatus::Running.as_str(),
                        RESTORE_JOB_GENERATION: generation_field(owner_generation),
                    },
                    vec![doc! { "$set": {
                        "item_count": { "$literal": i64::try_from(item_count).unwrap_or(i64::MAX) },
                        "restore_backfill_pending": { "$literal": true },
                        RESTORE_JOB_STATUS: { "$literal": JobStatus::Done.as_str() },
                        RESTORE_JOB_COMPLETED_AT: "$$NOW",
                    } }],
                )
                .await
                .map_err(internal)?;
            Ok::<_, StorageError>((item_count, handed_over.matched_count == 1))
        };
        let heartbeat =
            self.restore_heartbeat_loop(account_id, target_table_name, table_id, owner_generation);
        let outcome = tokio::select! {
            copied = copy => copied,
            lost = heartbeat => {
                tracing::error!(
                    "restore of {backup_arn} into {target_table_name} (account {account_id}): the \
                     job record's heartbeat failed ({lost}); stopping the copy"
                );
                Err(StorageError::Internal(format!("claim lost: {lost}")))
            }
        };

        match outcome {
            Ok((items, true)) => tracing::info!(
                "restore of {backup_arn} into {target_table_name}: copied {items} item(s) in \
                 {:.1}s; index backfill pending",
                started.elapsed().as_secs_f64()
            ),
            Ok((items, false)) => {
                // The target was removed while `$out` ran (the reconciler
                // judged it abandoned), or the record passed to another
                // claim. `$out` does not recreate the dropped data collection
                // (module doc); the drop here covers a removal that ran
                // between `$out`'s rename and the hand-over write, after
                // which the collection exists with no document naming it.
                let orphan = self
                    .catalog_db
                    .collection::<Document>("tables")
                    .find_one(doc! { "table_id": table_id })
                    .await
                    .map(|doc| doc.is_none());
                let dropped = match orphan {
                    Ok(true) => self.remove_target_collections(table_id).await,
                    Ok(false) => Ok(()),
                    Err(e) => Err(internal(e)),
                };
                tracing::error!(
                    "restore of {backup_arn} into {target_table_name} copied {items} item(s) but \
                     its target was removed meanwhile; the restore is lost (collection cleanup: \
                     {dropped:?})"
                );
            }
            Err(error) => {
                // The target must not stay behind: it is CREATING with no
                // scheduled transition, so nothing else would move it on, and
                // the name would stay taken. The job record is marked FAILED
                // first, conditioned on the generation and on RUNNING; only if
                // that matched is the target removed. A record another
                // generation owns, or a failed write, means the claim holder
                // owns the target, and nothing further is touched.
                let failed = self
                    .fail_restore_job_as(
                        account_id,
                        target_table_name,
                        table_id,
                        owner_generation,
                        &error.to_string(),
                    )
                    .await;
                match failed {
                    Ok(true) => {}
                    Ok(false) => {
                        tracing::warn!(
                            "restore of {backup_arn} into {target_table_name} ended with \
                             \"{error}\"; its job record was no longer RUNNING under this claim, \
                             so the target ({table_id}) was left to its owner"
                        );
                        return;
                    }
                    Err(write) => {
                        tracing::error!(
                            "restore of {backup_arn} into {target_table_name} failed ({error}), \
                             and its job record could not be marked FAILED ({write}); the \
                             reconciler removes the target once its heartbeat is stale"
                        );
                        return;
                    }
                }
                let key = RestoreTargetKey {
                    account_id: account_id.to_owned(),
                    table_name: target_table_name.to_owned(),
                    table_id: table_id.to_owned(),
                };
                match BackupCatalog::remove_restore_target(self, &key).await {
                    Ok(true) => tracing::error!(
                        "restore of {backup_arn} into {target_table_name} failed and the partial \
                         target was removed: {error}"
                    ),
                    Ok(false) => tracing::error!(
                        "restore of {backup_arn} into {target_table_name} failed after its target \
                         was already removed: {error}"
                    ),
                    Err(cleanup) => tracing::error!(
                        "restore of {backup_arn} into {target_table_name} failed ({error}), and \
                         physical cleanup also failed; the reconciler retries it: {cleanup}"
                    ),
                }
            }
        }
    }

    /// The destructive half of the legacy DeleteBackup, from a document that
    /// is DELETING: drop the backup's data collection, then write DELETED.
    /// The drop may have run on the server even if this future is dropped
    /// while waiting for its reply or the reply is an error; the document
    /// stays DELETING either way, and a later DeleteBackup resumes here.
    async fn finish_legacy_delete(
        &self,
        account_id: &str,
        backup_arn: &str,
        backup_doc: &Document,
    ) -> Result<BackupDescription, StorageError> {
        // A backup_id is absent on a pre-$out backup, which has no physical
        // collection to drop.
        if let Ok(backup_id) = backup_doc.get_str("backup_id") {
            self.drop_backup_collection(backup_id).await?;
        }
        self.catalog_db
            .collection::<Document>("backups")
            .update_one(
                doc! { "_id": backup_arn, "account_id": account_id, "backup_status": "DELETING" },
                doc! { "$set": { "backup_status": "DELETED" } },
            )
            .await
            .map_err(internal)?;
        let desc = description_from_doc(backup_doc, backup_arn)?;
        Ok(BackupDescription {
            backup_details: BackupDetails {
                backup_status: "DELETED".to_owned(),
                ..desc.backup_details
            },
            ..desc
        })
    }

    /// The legacy DeleteBackup's decision as one session transaction: lock
    /// the backup document (`$inc lock_seq` while AVAILABLE), re-check for a
    /// CREATING target, write DELETING. `Ok(Some(doc))` is the document as it
    /// was, now DELETING; `Ok(None)` means a concurrent DeleteBackup has
    /// written DELETING already and this one resumes its drop.
    async fn start_legacy_delete(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> Result<Option<Document>, StorageError> {
        let backups_coll = self.catalog_db.collection::<Document>("backups");
        let mut session = self.client.start_session().await.map_err(internal)?;
        let tx_options = self.write_transaction_options();
        for attempt in 0..TRANSIENT_RETRY_ATTEMPTS {
            session
                .start_transaction()
                .with_options(tx_options.clone())
                .await
                .map_err(internal)?;
            let body: Result<Result<Option<Document>, StorageError>, TxErr> = async {
                let locked = backups_coll
                    .find_one_and_update(
                        doc! {
                            "_id": backup_arn,
                            "account_id": account_id,
                            "backup_status": "AVAILABLE",
                        },
                        doc! { "$inc": { LOCK_SEQ: 1_i64 } },
                    )
                    .session(&mut session)
                    .await
                    .map_err(TxErr::from)?;
                let Some(backup_doc) = locked else {
                    let current = backups_coll
                        .find_one(doc! { "_id": backup_arn, "account_id": account_id })
                        .session(&mut session)
                        .await
                        .map_err(TxErr::from)?;
                    return Ok(match lock_miss(current.as_ref()) {
                        LockMiss::Deleting => Ok(None),
                        LockMiss::NotFound => Err(StorageError::Validation(format!(
                            "Backup not found: {backup_arn}"
                        ))),
                    });
                };
                // A restore can register its target between the unlocked
                // first check and this lock; from here new registrations
                // conflict with this transaction.
                if let Some(target) = self
                    .creating_restore_target(account_id, backup_arn, Some(&mut session))
                    .await
                    .map_err(TxErr::from)?
                {
                    return Ok(Err(backup_in_use(&target, backup_arn)));
                }
                backups_coll
                    .update_one(
                        doc! { "_id": backup_arn, "account_id": account_id, "backup_status": "AVAILABLE" },
                        doc! { "$set": { "backup_status": "DELETING" } },
                    )
                    .session(&mut session)
                    .await
                    .map_err(TxErr::from)?;
                Ok(Ok(Some(backup_doc)))
            }
            .await;
            match body {
                Ok(Ok(outcome)) => match self.commit_write_transaction(&mut session, None).await? {
                    CommitOutcome::Committed => return Ok(outcome),
                    CommitOutcome::RerunBody => backoff_sleep(attempt).await,
                },
                Ok(Err(refusal)) => {
                    let _ = session.abort_transaction().await;
                    return Err(refusal);
                }
                Err(TxErr::Transient) => {
                    let _ = session.abort_transaction().await;
                    backoff_sleep(attempt).await;
                }
                Err(TxErr::Fatal(e)) => {
                    let _ = session.abort_transaction().await;
                    return Err(e);
                }
            }
        }
        Err(StorageError::Internal(format!(
            "DeleteBackup of {backup_arn} did not commit after {TRANSIENT_RETRY_ATTEMPTS} \
             conflicting attempts"
        )))
    }
}

/// The table definition a backup document records, as a
/// [`BackupTableDefinition`]. MongoDB keeps the definition as fields of the
/// backup document rather than a separate row; a document written before
/// `definition_version` existed holds the same fields and reads as version 1,
/// with the 5/5 fallback for a PROVISIONED backup whose throughput was not
/// recorded (unchanged from earlier restores).
///
/// `VectorIndexNames` is always empty: this backend does not implement vector
/// indexes, so a MongoDB table never has one to record.
fn backup_definition_from_doc(
    doc: &Document,
    billing_mode: &str,
    backup_arn: &str,
) -> Result<BackupTableDefinition, StorageError> {
    fn json<T: Serialize>(value: &T) -> Result<serde_json::Value, StorageError> {
        serde_json::to_value(value).map_err(internal)
    }
    let version = match doc.get(DEFINITION_VERSION_FIELD) {
        None => serde_json::Value::from(BACKUP_DEFINITION_VERSION),
        Some(Bson::Int32(v)) => serde_json::Value::from(*v),
        Some(Bson::Int64(v)) => serde_json::Value::from(*v),
        Some(other) => serde_json::Value::String(other.to_string()),
    };
    let mut definition = serde_json::Map::new();
    definition.insert("Version".to_owned(), version);
    definition.insert("BillingMode".to_owned(), billing_mode.into());
    if let Some(throughput) = restore_provisioned_throughput(
        billing_mode,
        decode_optional(doc, "provisioned_throughput")?,
    ) {
        definition.insert("ProvisionedThroughput".to_owned(), json(&throughput)?);
    }
    let gsis = restore_gsi_throughput(
        billing_mode,
        decode_optional::<Vec<GsiInput>>(doc, "global_secondary_indexes")?,
    )
    .unwrap_or_default();
    definition.insert("GlobalSecondaryIndexes".to_owned(), json(&gsis)?);
    let lsis =
        decode_optional::<Vec<LsiInput>>(doc, "local_secondary_indexes")?.unwrap_or_default();
    definition.insert("LocalSecondaryIndexes".to_owned(), json(&lsis)?);
    if let Ok(table_class) = doc.get_str("table_class") {
        definition.insert("TableClass".to_owned(), table_class.into());
    }
    if let Some(sse) = decode_optional::<serde_json::Value>(doc, "sse_specification")? {
        definition.insert("SSESpecification".to_owned(), sse);
    }
    if let Some(on_demand) =
        decode_optional::<extenddb_core::types::OnDemandThroughput>(doc, "on_demand_throughput")?
    {
        definition.insert("OnDemandThroughput".to_owned(), json(&on_demand)?);
    }
    if let Some(ttl) = decode_optional::<TimeToLiveDescription>(doc, "time_to_live")? {
        definition.insert("TimeToLiveDescription".to_owned(), json(&ttl)?);
    }
    if let Some(stream) = decode_optional::<StreamSpecification>(doc, "stream_specification")? {
        definition.insert("StreamSpecification".to_owned(), json(&stream)?);
    }
    BackupTableDefinition::from_json(serde_json::Value::Object(definition), backup_arn)
}

fn backup_created_at(doc: &Document) -> f64 {
    doc.get_datetime("created_at")
        .map(|dt| datetime_epoch_secs(*dt))
        .unwrap_or(0.0)
}

fn restore_source_from_doc(
    doc: &Document,
    region: &str,
    account_id: &str,
    backup_arn: &str,
) -> Result<BackupRestoreSource, StorageError> {
    let billing_mode = doc
        .get_str("billing_mode")
        .unwrap_or("PAY_PER_REQUEST")
        .to_owned();
    let definition = backup_definition_from_doc(doc, &billing_mode, backup_arn)?;
    let created = backup_created_at(doc);
    let source_table_name = doc.get_str("table_name").unwrap_or_default().to_owned();
    let source_table_arn = match doc.get_str("table_arn") {
        Ok(arn) if !arn.is_empty() => arn.to_owned(),
        _ => util::table_arn(region, account_id, &source_table_name),
    };
    Ok(BackupRestoreSource {
        backup_arn: backup_arn.to_owned(),
        backup_creation_date_time: created,
        source_table_name,
        source_table_arn,
        source_table_id: doc.get_str("table_id").unwrap_or_default().to_owned(),
        key_schema: decode_required(doc, "key_schema")?,
        attribute_definitions: decode_required(doc, "attribute_definitions")?,
        billing_mode,
        provisioned_throughput: definition.provisioned_throughput.clone(),
        item_count: doc.get_i64("item_count").unwrap_or(0),
        table_size_bytes: doc.get_i64("backup_size_bytes").unwrap_or(0),
        table_creation_date_time: doc.get_f64("table_creation_date_time").unwrap_or(created),
        definition: Some(definition),
        // No vector indexes on this backend (see backup_definition_from_doc).
        legacy_vector_index_count: 0,
    })
}

/// The `DescribeBackup` description of a backup document, with the status as
/// stored. `SourceTableFeatureDetails` and the throughput are filled by the
/// engine from `restore_source` for an AVAILABLE backup.
fn description_from_doc(
    backup_doc: &Document,
    backup_arn: &str,
) -> Result<BackupDescription, StorageError> {
    let name = backup_doc
        .get_str("backup_name")
        .unwrap_or_default()
        .to_owned();
    let status = backup_doc
        .get_str("backup_status")
        .unwrap_or("AVAILABLE")
        .to_owned();
    let table_id = backup_doc
        .get_str("table_id")
        .unwrap_or_default()
        .to_owned();
    let table_name = backup_doc
        .get_str("table_name")
        .unwrap_or_default()
        .to_owned();
    let table_arn = backup_doc
        .get_str("table_arn")
        .unwrap_or_default()
        .to_owned();
    let size = backup_doc.get_i64("backup_size_bytes").unwrap_or(0);
    let count = backup_doc.get_i64("item_count").unwrap_or(0);
    let billing = backup_doc
        .get_str("billing_mode")
        .unwrap_or("PAY_PER_REQUEST")
        .to_owned();

    let created_at = backup_created_at(backup_doc);
    let table_created = backup_doc
        .get_f64("table_creation_date_time")
        .unwrap_or(created_at);
    let key_schema: Vec<KeySchemaElement> = decode_required(backup_doc, "key_schema")?;

    Ok(BackupDescription {
        backup_details: BackupDetails {
            backup_arn: backup_arn.to_owned(),
            backup_name: name,
            backup_status: status,
            backup_type: "USER".to_owned(),
            backup_size_bytes: size,
            backup_creation_date_time: created_at,
        },
        source_table_details: SourceTableDetails {
            table_name,
            table_id,
            table_arn,
            key_schema,
            item_count: count,
            table_size_bytes: size,
            billing_mode: Some(billing),
            table_creation_date_time: table_created,
            ..SourceTableDetails::default()
        },
        source_table_feature_details: None,
    })
}

/// The statuses of a backup document the legacy describe and list leave
/// out: a deleted backup and one whose DeleteBackup has started.
fn gone_statuses() -> Document {
    doc! { "$nin": ["DELETED", "DELETING"] }
}

impl LegacyBackupReader for MongoEngine {
    fn create(
        &self,
        account_id: &str,
        table_name: &str,
        backup_name: &str,
        permit: JobPermit,
    ) -> BoxFuture<'_, Result<BackupDetails, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        let backup_name = backup_name.to_string();
        Box::pin(async move {
            // The job's slot, held until the backup document is written.
            let _permit = permit;
            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let table_doc = tables_coll
                .find_one(doc! {
                    "_id": { "account_id": &account_id, "table_name": &table_name },
                    "table_status": "ACTIVE",
                })
                .await
                .map_err(internal)?
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

            let table_id = table_doc
                .get_str("table_id")
                .map_err(|_| StorageError::Internal("missing table_id".to_string()))?
                .to_owned();
            let table_arn = table_doc
                .get_str("table_arn")
                .unwrap_or_default()
                .to_owned();
            let key_schema_bson = table_doc
                .get_array("key_schema")
                .map_err(|_| StorageError::Internal("missing key_schema".to_string()))?
                .clone();
            let attr_defs_bson = table_doc
                .get_array("attribute_definitions")
                .map_err(|_| StorageError::Internal("missing attribute_definitions".to_string()))?
                .clone();
            let billing_mode = table_doc
                .get_str("billing_mode")
                .unwrap_or("PAY_PER_REQUEST")
                .to_owned();
            let table_size = table_doc.get_i64("table_size_bytes").unwrap_or(0);

            // Preserve TableClass / SSESpecification / OnDemandThroughput so
            // RestoreTableFromBackup can recreate the table with the same
            // configuration.
            let table_class_bson = table_doc.get("table_class").cloned().unwrap_or(Bson::Null);
            let sse_spec_bson = table_doc
                .get("sse_specification")
                .cloned()
                .unwrap_or(Bson::Null);
            let on_demand_bson = table_doc
                .get("on_demand_throughput")
                .cloned()
                .unwrap_or(Bson::Null);

            // For DescribeBackup's SourceTableFeatureDetails only; a restore
            // restores neither. TTL as DescribeTimeToLive reports it; the stream
            // specification as DescribeTable reports it (same lenient decode).
            let time_to_live = match table_doc.get_str("ttl_attribute") {
                Ok(attr) => TimeToLiveDescription {
                    time_to_live_status: TimeToLiveStatus::Enabled,
                    attribute_name: Some(attr.to_owned()),
                },
                Err(_) => TimeToLiveDescription {
                    time_to_live_status: TimeToLiveStatus::Disabled,
                    attribute_name: None,
                },
            };
            let stream_specification: Option<StreamSpecification> = table_doc
                .get("stream_specification")
                .filter(|b| b.as_null().is_none())
                .and_then(|b| bson::from_bson(b.clone()).ok());
            let table_created = table_doc
                .get_datetime("creation_date_time")
                .map(|dt| datetime_epoch_secs(*dt))
                .ok();

            // Preserve secondary-index definitions separately from the base
            // item snapshot. The base collection does not contain the index
            // metadata needed to recreate GSI/LSI collections on restore.
            let indexes_coll = self.catalog_db.collection::<Document>("indexes");
            let index_docs: Vec<Document> = indexes_coll
                .find(doc! { "_id.table_id": &table_id })
                .await
                .map_err(internal)?
                .try_collect()
                .await
                .map_err(internal)?;
            let attribute_definitions: Vec<AttributeDefinition> =
                bson::from_bson(Bson::Array(attr_defs_bson.clone())).map_err(|e| {
                    StorageError::Internal(format!("parse attribute_definitions: {e}"))
                })?;
            let mut global_secondary_indexes = Vec::new();
            let mut local_secondary_indexes = Vec::new();
            for index_doc in index_docs {
                let index_name = index_doc
                    .get_document("_id")
                    .and_then(|id| id.get_str("index_name"))
                    .map_err(|_| StorageError::Internal("missing index_name".to_string()))?
                    .to_owned();
                let key_schema: Vec<KeySchemaElement> = decode_required(&index_doc, "key_schema")?;
                let projection: Projection = decode_required(&index_doc, "projection")?;

                if matches!(
                    index_doc.get_str("index_type").unwrap_or("GSI"),
                    "GSI" | "LSI"
                ) {
                    for key in &key_schema {
                        if !attribute_definitions
                            .iter()
                            .any(|definition| definition.attribute_name == key.attribute_name)
                        {
                            return Err(StorageError::Validation(format!(
                                "Cannot create backup for table '{table_name}': index '{index_name}' references key attribute '{}' missing from attribute_definitions",
                                key.attribute_name
                            )));
                        }
                    }
                }

                match index_doc.get_str("index_type").unwrap_or("GSI") {
                    "GSI" => {
                        global_secondary_indexes.push(GsiInput {
                            index_name,
                            key_schema,
                            projection,
                            provisioned_throughput: decode_optional(
                                &index_doc,
                                "provisioned_throughput",
                            )?,
                        });
                    }
                    "LSI" => {
                        local_secondary_indexes.push(LsiInput {
                            index_name,
                            key_schema,
                            projection,
                        });
                    }
                    _ => {}
                }
            }
            let provisioned_throughput_bson = table_doc
                .get("provisioned_throughput")
                .cloned()
                .unwrap_or(Bson::Null);

            // The shared id: 14-digit zero-padded epoch milliseconds and an
            // 8-hex random suffix, so the ARN is not guessable from the time
            // alone. The creation time is the id's own instant, so the ARN, the
            // reported time, and the ListBackups cursor agree.
            let arn_id = util::new_backup_id();
            let backup_arn = util::backup_arn(&self.region, &account_id, &table_name, &arn_id);
            let created_at = util::backup_creation_date_time(&arn_id).ok_or_else(|| {
                StorageError::Internal(format!("backup id {arn_id} carries no creation time"))
            })?;
            // Names the physical collection; independent of the ARN.
            let backup_id = uuid::Uuid::new_v4().to_string();

            // Snapshot items from the data collection using a server-side
            // `$out` aggregation. Items are copied directly between
            // collections in MongoDB — no per-item traffic to the driver.
            let src_coll_name = data_collection_name(&table_id);
            let dst_coll_name = backup_collection_name(&backup_id);
            self.out_copy(&src_coll_name, &dst_coll_name).await?;

            let dst_coll = self.data_db.collection::<Document>(&dst_coll_name);
            let actual_count =
                i64::try_from(dst_coll.count_documents(doc! {}).await.map_err(internal)?)
                    .unwrap_or(i64::MAX);

            // Store backup metadata. `backup_id` is what maps to the
            // physical collection; `backup_arn` remains the caller-visible
            // handle and stays the `_id` for compatibility with existing
            // describe/list callers.
            let backups_coll = self.catalog_db.collection::<Document>("backups");
            let ttl_bson = bson::to_bson(&time_to_live).map_err(internal)?;
            let stream_bson = stream_specification
                .as_ref()
                .map(bson::to_bson)
                .transpose()
                .map_err(internal)?
                .unwrap_or(Bson::Null);
            let mut backup_meta = doc! {
                "_id": &backup_arn,
                "backup_id": &backup_id,
                "backup_name": &backup_name,
                "backup_status": "AVAILABLE",
                "backup_type": "USER",
                "table_id": &table_id,
                "table_name": &table_name,
                "table_arn": &table_arn,
                "account_id": &account_id,
                "backup_size_bytes": table_size,
                "item_count": actual_count,
                "key_schema": key_schema_bson,
                "attribute_definitions": attr_defs_bson,
                "billing_mode": &billing_mode,
                "provisioned_throughput": provisioned_throughput_bson,
                "created_at": epoch_secs_datetime(created_at),
                "table_creation_date_time": table_created.unwrap_or(created_at),
                "table_class": table_class_bson,
                "sse_specification": sse_spec_bson,
                "on_demand_throughput": on_demand_bson,
                "time_to_live": ttl_bson,
                "stream_specification": stream_bson,
            };
            backup_meta.insert(
                DEFINITION_VERSION_FIELD,
                i64::from(BACKUP_DEFINITION_VERSION),
            );

            insert_non_empty_array(
                &mut backup_meta,
                "global_secondary_indexes",
                &global_secondary_indexes,
            )?;
            insert_non_empty_array(
                &mut backup_meta,
                "local_secondary_indexes",
                &local_secondary_indexes,
            )?;

            backups_coll
                .insert_one(backup_meta)
                .await
                .map_err(internal)?;

            Ok(BackupDetails {
                backup_arn,
                backup_name,
                backup_status: "AVAILABLE".to_owned(),
                backup_type: "USER".to_owned(),
                backup_size_bytes: table_size,
                backup_creation_date_time: created_at,
            })
        })
    }

    fn describe(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = account_id.to_string();
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let backups_coll = self.catalog_db.collection::<Document>("backups");
            // Scope the lookup to the calling account so a backup ARN cannot be
            // read cross-account, and leave out a deleted backup and one being
            // deleted, so both read as BackupNotFoundException.
            let backup_doc = backups_coll
                .find_one(doc! {
                    "_id": &backup_arn,
                    "account_id": &account_id,
                    "backup_status": gone_statuses(),
                })
                .await
                .map_err(internal)?
                .ok_or_else(|| {
                    StorageError::Validation(format!("Backup not found: {backup_arn}"))
                })?;
            description_from_doc(&backup_doc, &backup_arn)
        })
    }

    fn list(
        &self,
        account_id: &str,
        table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.map(std::string::ToString::to_string);
        Box::pin(async move {
            let backups_coll = self.catalog_db.collection::<Document>("backups");

            let mut filter = doc! {
                "account_id": &account_id,
                "backup_status": gone_statuses(),
            };
            if let Some(tn) = &table_name {
                filter.insert("table_name", tn.as_str());
            }

            // The engine sorts and pages; this order is only for readability.
            let mut cursor = backups_coll
                .find(filter)
                .sort(doc! { "created_at": -1 })
                .await
                .map_err(internal)?;

            let mut results = Vec::new();
            while let Some(doc) = cursor.try_next().await.map_err(internal)? {
                results.push(BackupSummary {
                    backup_arn: doc.get_str("_id").unwrap_or_default().to_owned(),
                    backup_name: doc.get_str("backup_name").unwrap_or_default().to_owned(),
                    table_name: doc.get_str("table_name").unwrap_or_default().to_owned(),
                    table_id: doc.get_str("table_id").unwrap_or_default().to_owned(),
                    table_arn: doc.get_str("table_arn").unwrap_or_default().to_owned(),
                    backup_status: doc
                        .get_str("backup_status")
                        .unwrap_or("AVAILABLE")
                        .to_owned(),
                    backup_type: "USER".to_owned(),
                    backup_size_bytes: doc.get_i64("backup_size_bytes").unwrap_or(0),
                    backup_creation_date_time: backup_created_at(&doc),
                });
            }
            Ok(results)
        })
    }

    fn delete(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = account_id.to_string();
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let backups_coll = self.catalog_db.collection::<Document>("backups");
            let current = backups_coll
                .find_one(doc! { "_id": &backup_arn, "account_id": &account_id })
                .await
                .map_err(internal)?;
            let current = match current.as_ref().map(|d| d.get_str("backup_status")) {
                None | Some(Ok("DELETED")) => {
                    return Err(StorageError::Validation(format!(
                        "Backup not found: {backup_arn}"
                    )));
                }
                // A DeleteBackup that stopped after writing DELETING: resume
                // its drop and its DELETED write.
                Some(Ok("DELETING")) => {
                    let doc = current.expect("a status was read from a document");
                    return self
                        .finish_legacy_delete(&account_id, &backup_arn, &doc)
                        .await;
                }
                Some(_) => current.expect("a status was read from a document"),
            };

            // The in-use rule before the lock, so a backup a running restore
            // reads is refused without a transaction.
            if let Some(target) = self
                .creating_restore_target(&account_id, &backup_arn, None)
                .await
                .map_err(internal)?
            {
                return Err(backup_in_use(&target, &backup_arn));
            }

            // The decision under the lock: AVAILABLE and no CREATING target,
            // then DELETING, in one transaction a restore's registration
            // conflicts with.
            let backup_doc = match self.start_legacy_delete(&account_id, &backup_arn).await? {
                Some(doc) => doc,
                None => current,
            };
            self.finish_legacy_delete(&account_id, &backup_arn, &backup_doc)
                .await
        })
    }

    fn restore_source(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupRestoreSource, StorageError>> {
        let account_id = account_id.to_owned();
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            let doc = self
                .catalog_db
                .collection::<Document>("backups")
                .find_one(doc! {
                    "_id": &backup_arn,
                    "account_id": &account_id,
                    "backup_status": "AVAILABLE",
                })
                .await
                .map_err(internal)?
                .ok_or_else(|| {
                    StorageError::Validation(format!("Backup not found: {backup_arn}"))
                })?;
            restore_source_from_doc(&doc, &self.region, &account_id, &backup_arn)
        })
    }

    fn restore(
        &self,
        account_id: &str,
        request: RestoreRequest,
        permit: JobPermit,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let engine = self.clone();
        let account_id = account_id.to_owned();
        Box::pin(async move {
            // The whole restore, registration included, runs in a task that
            // owns its inputs and the job permit, so the request future being
            // dropped (a client timeout) stops neither the registration nor
            // the copy, and the permit is released when the copy ends. A
            // refused restore creates nothing. The request waits for the
            // CREATING target to exist, at most `RESTORE_REGISTRATION_TIMEOUT`
            // (its doc says what a request that hit the bound leaves behind).
            let (registered_tx, registered_rx) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let _permit = permit;
                let backup_arn = request.backup_arn.clone();
                let target_table_name = request.input.table_name.clone();
                // A refusal the request is no longer there to receive (its
                // client went away, or the registration bound fired) would
                // otherwise vanish, so it is logged. A refused restore removes
                // what it created; a cleanup that failed is logged on its own,
                // and the reconciler finishes it.
                let refuse = |registered: tokio::sync::oneshot::Sender<_>, e: StorageError| {
                    if let Err(Err(e)) = registered.send(Err(e)) {
                        tracing::warn!(
                            "restore of {backup_arn} into {target_table_name} (account \
                             {account_id}) was refused after its request ended: {e}"
                        );
                    }
                };
                // The name first, unfenced, as on the other backends: a
                // restore into a name that is a CREATING restore target is
                // refused as the name at once, whatever state the backup is in.
                if let Err(error) = engine
                    .refuse_a_name_being_restored(&account_id, &target_table_name)
                    .await
                {
                    refuse(registered_tx, error);
                    return;
                }
                #[cfg(test)]
                if let Some(gate) = &engine.backup_liveness.registration_gate {
                    match gate.acquire().await {
                        Ok(permit) => permit.forget(),
                        Err(e) => {
                            refuse(registered_tx, internal(e));
                            return;
                        }
                    }
                }
                let owner_generation = new_generation();
                let (desc, backup_id) = match engine
                    .register_restore(&account_id, &request, owner_generation)
                    .await
                {
                    Ok(registered) => registered,
                    Err(error) => {
                        refuse(registered_tx, error);
                        return;
                    }
                };
                let table_id = desc.table_id.clone();
                // A closed receiver means the request is gone; the copy goes on.
                let _ = registered_tx.send(Ok(desc));
                engine
                    .copy_restore(
                        &account_id,
                        &target_table_name,
                        &backup_arn,
                        owner_generation,
                        &backup_id,
                        &table_id,
                    )
                    .await;
            });
            match tokio::time::timeout(RESTORE_REGISTRATION_TIMEOUT, registered_rx).await {
                Ok(registered) => registered.map_err(|_| {
                    StorageError::Internal(
                        "MongoDB restore task ended before registering the restore".to_owned(),
                    )
                })?,
                Err(_) => Err(StorageError::Internal(format!(
                    "the restore was not registered within {}s; it goes on in the background \
                     and DescribeTable reports its target once it exists",
                    RESTORE_REGISTRATION_TIMEOUT.as_secs()
                ))),
            }
        })
    }

    /// None: this backend's create commits its backup document AVAILABLE
    /// after the copy and leaves no CREATING row behind.
    fn abandoned_creating_backups(
        &self,
        _stale_after: Duration,
    ) -> BoxFuture<'_, Result<Vec<AbandonedLegacyBackup>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn describe_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let exists = tables_coll
                .find_one(doc! { "_id": { "account_id": &account_id, "table_name": &table_name } })
                .await
                .map_err(internal)?;

            if exists.is_none() {
                return Err(StorageError::TableNotFound(table_name));
            }

            let cb_coll = self.catalog_db.collection::<Document>("continuous_backups");
            let pitr_doc = cb_coll
                .find_one(doc! { "account_id": &account_id, "table_name": &table_name })
                .await
                .map_err(internal)?;

            let pitr_enabled = pitr_doc
                .as_ref()
                .and_then(|d| d.get_bool("pitr_enabled").ok())
                .unwrap_or(false);

            let now_epoch = now_epoch_secs();

            Ok(ContinuousBackupsDescription {
                continuous_backups_status: "ENABLED".to_owned(),
                point_in_time_recovery_description: Some(PointInTimeRecoveryDescription {
                    point_in_time_recovery_status: if pitr_enabled {
                        "ENABLED".to_owned()
                    } else {
                        "DISABLED".to_owned()
                    },
                    earliest_restorable_date_time: if pitr_enabled {
                        Some(now_epoch - 35.0 * 24.0 * 3600.0)
                    } else {
                        None
                    },
                    latest_restorable_date_time: if pitr_enabled { Some(now_epoch) } else { None },
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
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let exists = tables_coll
                .find_one(doc! { "_id": { "account_id": &account_id, "table_name": &table_name } })
                .await
                .map_err(internal)?;

            if exists.is_none() {
                return Err(StorageError::TableNotFound(table_name.clone()));
            }

            let cb_coll = self.catalog_db.collection::<Document>("continuous_backups");
            cb_coll
                .update_one(
                    doc! { "account_id": &account_id, "table_name": &table_name },
                    doc! { "$set": {
                        "account_id": &account_id,
                        "table_name": &table_name,
                        "pitr_enabled": pitr_enabled,
                    }},
                )
                .upsert(true)
                .await
                .map_err(internal)?;

            self.describe_continuous_backups(&account_id, &table_name)
                .await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{insert_non_empty_array, restore_gsi_throughput, restore_provisioned_throughput};
    use extenddb_core::types::{GsiInput, ProvisionedThroughput};
    use mongodb::bson::Document;

    #[test]
    fn gsi_throughput_is_dropped_for_an_on_demand_table() {
        use extenddb_core::types::{
            GsiInput, KeySchemaElement, KeyType, Projection, ProjectionType,
        };
        let gsi = GsiInput {
            index_name: "g".to_owned(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gpk".to_owned(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: Some(ProvisionedThroughput {
                read_capacity_units: 7,
                write_capacity_units: 8,
            }),
        };
        let kept = restore_gsi_throughput("PROVISIONED", Some(vec![gsi.clone()])).expect("gsis");
        assert!(kept[0].provisioned_throughput.is_some());
        let dropped = restore_gsi_throughput("PAY_PER_REQUEST", Some(vec![gsi])).expect("gsis");
        assert!(dropped[0].provisioned_throughput.is_none());
        assert!(restore_gsi_throughput("PAY_PER_REQUEST", None).is_none());
    }

    #[test]
    fn backup_omits_empty_secondary_index_metadata() {
        let mut backup = Document::new();
        insert_non_empty_array::<GsiInput>(&mut backup, "global_secondary_indexes", &[])
            .expect("empty index metadata should be accepted");

        assert!(!backup.contains_key("global_secondary_indexes"));
    }

    #[test]
    fn restore_preserves_stored_provisioned_throughput() {
        let stored = ProvisionedThroughput {
            read_capacity_units: 7,
            write_capacity_units: 9,
        };
        assert_eq!(
            restore_provisioned_throughput("PROVISIONED", Some(stored.clone())),
            Some(stored)
        );
    }

    #[test]
    fn restore_uses_legacy_fallback_when_capacity_metadata_is_missing() {
        assert_eq!(
            restore_provisioned_throughput("PROVISIONED", None),
            Some(ProvisionedThroughput {
                read_capacity_units: 5,
                write_capacity_units: 5,
            })
        );
    }

    #[test]
    fn restore_drops_capacity_for_pay_per_request() {
        let stored = ProvisionedThroughput {
            read_capacity_units: 7,
            write_capacity_units: 9,
        };
        assert_eq!(
            restore_provisioned_throughput("PAY_PER_REQUEST", Some(stored)),
            None
        );
    }
}

#[cfg(test)]
mod definition_tests {
    use super::{
        LockMiss, RestoreRegistration, backup_definition_from_doc, description_from_doc,
        gone_statuses, lock_miss, restore_source_from_doc, restoring_target_filter,
    };
    use extenddb_core::types::{
        OnDemandThroughput, ProvisionedThroughput, StreamViewType, TimeToLiveStatus,
    };
    use extenddb_storage::error::StorageError;
    use mongodb::bson::{self, doc};

    const ARN: &str =
        "arn:aws:dynamodb:us-east-1:123456789012:table/src/backup/01791976431438-0000abcd";

    fn backup_doc() -> mongodb::bson::Document {
        doc! {
            "_id": ARN,
            "backup_id": "uuid",
            "table_id": "tid",
            "table_name": "src",
            "table_arn": "arn:aws:dynamodb:us-east-1:123456789012:table/src",
            "account_id": "123456789012",
            "backup_size_bytes": 77_i64,
            "item_count": 3_i64,
            "key_schema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
            "attribute_definitions": [
                { "AttributeName": "pk", "AttributeType": "S" },
                { "AttributeName": "g", "AttributeType": "S" },
            ],
            "billing_mode": "PROVISIONED",
            "provisioned_throughput": { "ReadCapacityUnits": 7_i64, "WriteCapacityUnits": 9_i64 },
            "created_at": bson::DateTime::from_millis(1_791_976_431_438),
            "table_creation_date_time": 1_791_000_000.25_f64,
            "table_class": "STANDARD_INFREQUENT_ACCESS",
            "sse_specification": { "Enabled": true },
            "on_demand_throughput": { "MaxReadRequestUnits": 10_i64, "MaxWriteRequestUnits": 11_i64 },
            "time_to_live": { "TimeToLiveStatus": "ENABLED", "AttributeName": "expires" },
            "stream_specification": { "StreamEnabled": true, "StreamViewType": "NEW_IMAGE" },
            "global_secondary_indexes": [{
                "IndexName": "g1",
                "KeySchema": [{ "AttributeName": "g", "KeyType": "HASH" }],
                "Projection": { "ProjectionType": "ALL" },
                "ProvisionedThroughput": { "ReadCapacityUnits": 3_i64, "WriteCapacityUnits": 4_i64 },
            }],
            "local_secondary_indexes": [{
                "IndexName": "l1",
                "KeySchema": [
                    { "AttributeName": "pk", "KeyType": "HASH" },
                    { "AttributeName": "g", "KeyType": "RANGE" },
                ],
                "Projection": { "ProjectionType": "KEYS_ONLY" },
            }],
            "definition_version": 1_i64,
        }
    }

    #[test]
    fn the_definition_is_read_from_the_backup_document() {
        let def = backup_definition_from_doc(&backup_doc(), "PROVISIONED", ARN).expect("def");
        assert_eq!(def.version, 1);
        assert_eq!(def.billing_mode, "PROVISIONED");
        assert_eq!(
            def.provisioned_throughput,
            Some(ProvisionedThroughput {
                read_capacity_units: 7,
                write_capacity_units: 9
            })
        );
        assert_eq!(def.global_secondary_indexes.len(), 1);
        assert_eq!(def.global_secondary_indexes[0].index_name, "g1");
        assert_eq!(
            def.global_secondary_indexes[0].provisioned_throughput,
            Some(ProvisionedThroughput {
                read_capacity_units: 3,
                write_capacity_units: 4
            })
        );
        assert_eq!(def.local_secondary_indexes.len(), 1);
        assert_eq!(def.local_secondary_indexes[0].index_name, "l1");
        assert_eq!(
            def.table_class.as_deref(),
            Some("STANDARD_INFREQUENT_ACCESS")
        );
        assert_eq!(
            def.sse_specification,
            Some(serde_json::json!({ "Enabled": true }))
        );
        assert_eq!(
            def.on_demand_throughput,
            Some(OnDemandThroughput {
                max_read_request_units: Some(10),
                max_write_request_units: Some(11),
            })
        );
        let ttl = def.time_to_live.expect("ttl");
        assert_eq!(ttl.time_to_live_status, TimeToLiveStatus::Enabled);
        assert_eq!(ttl.attribute_name.as_deref(), Some("expires"));
        let stream = def.stream_specification.expect("stream");
        assert!(stream.stream_enabled);
        assert_eq!(stream.stream_view_type, Some(StreamViewType::NewImage));
        assert!(def.vector_index_names.is_empty());
    }

    #[test]
    fn an_on_demand_definition_drops_recorded_throughput() {
        let mut d = backup_doc();
        d.insert("billing_mode", "PAY_PER_REQUEST");
        let def = backup_definition_from_doc(&d, "PAY_PER_REQUEST", ARN).expect("def");
        assert_eq!(def.provisioned_throughput, None);
        assert_eq!(def.global_secondary_indexes[0].provisioned_throughput, None);
    }

    #[test]
    fn a_document_from_before_definition_version_reads_as_version_1() {
        let mut d = backup_doc();
        for key in [
            "definition_version",
            "time_to_live",
            "stream_specification",
            "provisioned_throughput",
            "global_secondary_indexes",
            "local_secondary_indexes",
            "table_class",
            "sse_specification",
            "on_demand_throughput",
        ] {
            d.remove(key);
        }
        let def = backup_definition_from_doc(&d, "PROVISIONED", ARN).expect("def");
        assert_eq!(def.version, 1);
        // Unchanged legacy fallback for a PROVISIONED backup without throughput.
        assert_eq!(
            def.provisioned_throughput,
            Some(ProvisionedThroughput {
                read_capacity_units: 5,
                write_capacity_units: 5
            })
        );
        assert!(def.global_secondary_indexes.is_empty());
        assert!(def.time_to_live.is_none() && def.stream_specification.is_none());
    }

    #[test]
    fn an_unknown_definition_version_is_an_error() {
        let mut d = backup_doc();
        d.insert("definition_version", 2_i64);
        let err = backup_definition_from_doc(&d, "PROVISIONED", ARN).expect_err("refused");
        assert!(matches!(err, StorageError::Unsupported(_)), "{err:?}");
    }

    #[test]
    fn the_restore_source_reports_the_backup_document() {
        let source =
            restore_source_from_doc(&backup_doc(), "us-east-1", "123456789012", ARN).expect("src");
        assert_eq!(source.backup_arn, ARN);
        assert!((source.backup_creation_date_time - 1_791_976_431.438).abs() < 1e-9);
        assert_eq!(source.source_table_name, "src");
        assert_eq!(source.source_table_id, "tid");
        assert_eq!(
            source.source_table_arn,
            "arn:aws:dynamodb:us-east-1:123456789012:table/src"
        );
        assert_eq!(source.billing_mode, "PROVISIONED");
        assert_eq!(source.item_count, 3);
        assert_eq!(source.table_size_bytes, 77);
        assert!((source.table_creation_date_time - 1_791_000_000.25).abs() < 1e-9);
        assert_eq!(
            source.provisioned_throughput,
            source.definition.as_ref().unwrap().provisioned_throughput
        );
        assert_eq!(source.legacy_vector_index_count, 0);

        let mut d = backup_doc();
        d.insert("table_arn", "");
        let source = restore_source_from_doc(&d, "us-east-1", "123456789012", ARN).expect("src");
        assert_eq!(
            source.source_table_arn,
            "arn:aws:dynamodb:us-east-1:123456789012:table/src"
        );
    }

    /// The description reports the document's status as stored, so a
    /// resumed DeleteBackup can answer DELETED from the document it read.
    #[test]
    fn the_description_reports_the_documents_status_and_source_table() {
        let desc = description_from_doc(&backup_doc(), ARN).expect("description");
        assert_eq!(desc.backup_details.backup_arn, ARN);
        assert_eq!(desc.backup_details.backup_status, "AVAILABLE");
        assert_eq!(desc.backup_details.backup_size_bytes, 77);
        assert!((desc.backup_details.backup_creation_date_time - 1_791_976_431.438).abs() < 1e-9);
        assert_eq!(desc.source_table_details.table_name, "src");
        assert_eq!(desc.source_table_details.table_id, "tid");
        assert_eq!(desc.source_table_details.item_count, 3);
        assert_eq!(
            desc.source_table_details.billing_mode.as_deref(),
            Some("PROVISIONED")
        );
        assert!(desc.source_table_feature_details.is_none());
        let mut deleting = backup_doc();
        deleting.insert("backup_status", "DELETING");
        assert_eq!(
            description_from_doc(&deleting, ARN)
                .expect("description")
                .backup_details
                .backup_status,
            "DELETING"
        );
    }

    /// The legacy describe and list leave out a deleted backup and one whose
    /// DeleteBackup has started; a restore's registration and a DeleteBackup
    /// tell the two apart only to decide whether a delete is resumed.
    #[test]
    fn a_deleting_or_deleted_document_is_gone_and_only_deleting_resumes_a_delete() {
        let gone = gone_statuses();
        let excluded = gone.get_array("$nin").expect("$nin");
        assert_eq!(
            excluded,
            &[bson::Bson::from("DELETED"), bson::Bson::from("DELETING")]
        );
        assert_eq!(lock_miss(None), LockMiss::NotFound);
        let mut d = doc! { "_id": ARN, "backup_status": "AVAILABLE" };
        assert_eq!(lock_miss(Some(&d)), LockMiss::NotFound);
        d.insert("backup_status", "DELETED");
        assert_eq!(lock_miss(Some(&d)), LockMiss::NotFound);
        d.insert("backup_status", "DELETING");
        assert_eq!(lock_miss(Some(&d)), LockMiss::Deleting);
    }

    #[test]
    fn the_in_use_filter_is_account_scoped_and_reads_the_targets_status() {
        let filter = restoring_target_filter("123456789012", "backup");
        assert_eq!(filter.get_str("_id.account_id"), Ok("123456789012"));
        assert_eq!(filter.get_str("restore_source_backup_arn"), Ok("backup"));
        assert_eq!(filter.get_str("table_status"), Ok("CREATING"));
    }

    /// The registration writes the provenance DescribeTable reports and the
    /// job record RUNNING, as a legacy restore, under its generation.
    #[test]
    fn a_registration_writes_the_provenance_and_a_running_legacy_job_record() {
        let registration = RestoreRegistration {
            backup_arn: ARN,
            source_table_arn: "arn:aws:dynamodb:us-east-1:123456789012:table/src",
            restore_date_time: bson::DateTime::from_millis(1_791_976_431_438),
            owner_generation: 7,
            first_heartbeat: bson::DateTime::from_millis(1_791_976_500_000),
            session: None,
        };
        let mut table_doc = doc! { "table_status": "CREATING" };
        registration.insert_into(&mut table_doc);
        assert_eq!(table_doc.get_str("restore_source_backup_arn"), Ok(ARN));
        assert_eq!(
            table_doc.get_str("restore_source_table_arn"),
            Ok("arn:aws:dynamodb:us-east-1:123456789012:table/src")
        );
        assert_eq!(
            table_doc
                .get_datetime("restore_date_time")
                .map(|d| d.timestamp_millis()),
            Ok(1_791_976_431_438)
        );
        let job = table_doc.get_document("restore_job").expect("job record");
        assert_eq!(job.get_str("status"), Ok("RUNNING"));
        assert_eq!(job.get_str("kind"), Ok("legacy"));
        assert_eq!(job.get_i64("owner_generation"), Ok(7));
        assert_eq!(
            job.get_datetime("heartbeat_at")
                .map(|d| d.timestamp_millis()),
            Ok(1_791_976_500_000)
        );
        assert!(!table_doc.contains_key("restore_operation_id"));
        assert!(!table_doc.contains_key("restore_heartbeat_at"));
    }
}

/// The protocol model; see the module documentation.
#[cfg(test)]
#[path = "backup_protocol_model.rs"]
mod protocol_model;

#[cfg(test)]
#[path = "backup_live_tests.rs"]
mod live_tests;
