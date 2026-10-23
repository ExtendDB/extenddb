// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BackupEngine` implementation for `MongoDB`.
//!
//! Backups are stored as one MongoDB collection per backup, plus a `backups`
//! metadata collection in the catalog. `CreateBackup` uses MongoDB's
//! server-side aggregation `$out` stage to clone the source data collection
//! into `_backup_<backup_id>` in the data database — no per-item traffic
//! between the driver and the server. `RestoreTableFromBackup` uses the same
//! stage in reverse, in a task detached from the request. `DeleteBackup` drops
//! the collection.
//!
//! Backup metadata carries a `backup_id` UUID; the collection name is derived
//! from that id so the `backup_arn` (which contains slashes and colons) never
//! appears in a collection name.
//!
//! # Claims, restore targets, and liveness
//!
//! MongoDB has no transaction spanning the claim and the target's collections,
//! so a restore and a DeleteBackup exclude each other through markers:
//!
//! - a restore claims the backup document (`restore_in_progress`), checks that
//!   no CREATING target is restoring the backup already, inserts its CREATING
//!   target with provenance (`restore_source_backup_arn`,
//!   `restore_operation_id`), then releases the claim; from then on the
//!   CREATING target is what keeps DeleteBackup and other restores of the
//!   backup out (`BackupInUse` until the target is ACTIVE, as the service
//!   answers). A claim another restore released before inserting anything is
//!   retried for up to [`RESTORE_CLAIM_RETRY_FOR`]. The exclusion is bounded
//!   by the heartbeat: a restore that stalls between its claim and its insert
//!   for longer than [`BACKUP_HEARTBEAT_STALE_AFTER`] has its claim swept, a
//!   second restore of the backup can then claim and insert, and when the
//!   first resumes it inserts too (its release of the claim returns `false`
//!   and it goes on while the backup is AVAILABLE), leaving two CREATING
//!   targets of one backup that both finish. The other three backends have no
//!   such window: PostgreSQL and SQLite register a restore in one transaction,
//!   and Cassandra's registration is a lightweight transaction on the
//!   backup's row conditioned on no other registration;
//! - DeleteBackup claims the same document (`delete_in_progress`) under the
//!   inverse predicate, re-checks for a CREATING target, and only then drops
//!   the backup's data. Once the drop may have started its claim is never
//!   released; if DeleteBackup stops after that, the sweep finishes it.
//!
//! Every marker carries its owner's operation id and a heartbeat timestamp
//! (`<marker>_heartbeat` on a claim, `restore_heartbeat_at` on a target) that
//! a companion task refreshes at once and then every
//! [`BACKUP_HEARTBEAT_INTERVAL`] with the server's clock (`$$NOW`); a restore
//! also refreshes its target's right after inserting it, replacing the insert's
//! local-clock value. [`MongoEngine::sweep_stale_backup_claims`],
//! run at startup and periodically by the GSI backfill worker, removes only
//! markers whose heartbeat is older than [`BACKUP_HEARTBEAT_STALE_AFTER`], so
//! several processes can share one catalog without clearing each other's live
//! work. A restore target stops heartbeating once its base copy is done and
//! `restore_backfill_pending` is set; from then the backfill worker owns it and
//! finishes it after a restart, so the sweep never touches it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::TryStreamExt;
use futures::future::BoxFuture;
use mongodb::bson::{self, Bson, Document, doc};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use extenddb_core::types::{
    AttributeDefinition, BackupDescription, BackupDetails, BackupSummary,
    ContinuousBackupsDescription, GsiInput, KeySchemaElement, LsiInput,
    PointInTimeRecoveryDescription, Projection, ProvisionedThroughput, SourceTableDetails,
    StreamSpecification, TableDescription, TimeToLiveDescription, TimeToLiveStatus,
};
use extenddb_storage::backup_definition::{BACKUP_DEFINITION_VERSION, BackupTableDefinition};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupEngine, BackupRestoreSource, RestoreRequest, util};

use crate::MongoEngine;
use crate::data::data_collection_name;

/// How often a live restore or DeleteBackup refreshes its heartbeats.
pub(crate) const BACKUP_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
/// A claim or restore target whose heartbeat is older than this belongs to an
/// operation that stopped (its process died); the sweep removes it. Four
/// missed refreshes, so a slow catalog write does not make live work look dead.
pub(crate) const BACKUP_HEARTBEAT_STALE_AFTER: Duration = Duration::from_secs(60);

pub(crate) const RESTORE_CLAIM: &str = "restore_in_progress";
pub(crate) const RESTORE_CLAIM_HEARTBEAT: &str = "restore_in_progress_heartbeat";
pub(crate) const DELETE_CLAIM: &str = "delete_in_progress";
pub(crate) const DELETE_CLAIM_HEARTBEAT: &str = "delete_in_progress_heartbeat";
pub(crate) const TARGET_OPERATION: &str = "restore_operation_id";
pub(crate) const TARGET_HEARTBEAT: &str = "restore_heartbeat_at";

/// The definition version stored on a backup document written by this build.
/// Backup documents written before it carry the same fields without the
/// marker and are read as version 1.
const DEFINITION_VERSION_FIELD: &str = "definition_version";

/// Heartbeat timing for backup claims and restore targets. Production uses
/// the defaults; tests shorten both so liveness can be observed in seconds.
#[derive(Debug, Clone)]
pub(crate) struct BackupLiveness {
    pub(crate) heartbeat_interval: Duration,
    pub(crate) stale_after: Duration,
    /// How long a restore waits for its account slot and a server slot
    /// together before it is refused with `LimitExceeded`
    /// ([`RESTORE_SLOT_WAIT`] in production; tests shorten it).
    pub(crate) slot_wait: Duration,
    /// Test only: when set, a restore's copy task waits for one permit after
    /// registering its target and before its base copy, so a test can observe
    /// the CREATING window deterministically.
    #[cfg(test)]
    pub(crate) copy_gate: Option<std::sync::Arc<tokio::sync::Semaphore>>,
    /// Test only: what a DeleteBackup or sweep does right after it dropped a
    /// backup's data collection.
    #[cfg(test)]
    pub(crate) after_backup_drop: Option<AfterBackupDrop>,
    /// Test only: how far this process's clock is behind the server's, applied
    /// to the one heartbeat written with the local clock (the target insert).
    #[cfg(test)]
    pub(crate) local_clock_behind: Duration,
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

impl Default for BackupLiveness {
    fn default() -> Self {
        Self {
            heartbeat_interval: BACKUP_HEARTBEAT_INTERVAL,
            stale_after: BACKUP_HEARTBEAT_STALE_AFTER,
            slot_wait: RESTORE_SLOT_WAIT,
            #[cfg(test)]
            copy_gate: None,
            #[cfg(test)]
            after_backup_drop: None,
            #[cfg(test)]
            local_clock_behind: Duration::ZERO,
        }
    }
}

/// Restores one account may have in flight at once in this process. A
/// restore beyond the limit waits for one of its own account's slots, so one
/// account cannot hold every other account's restores back. Same figure as
/// the PostgreSQL backend.
const RESTORE_SLOTS_PER_ACCOUNT: usize = 4;

/// Upper limit on [`restore_server_slot_count`].
const RESTORE_SERVER_SLOTS_MAX: usize = 16;

/// How long a restore waits for its account slot and a server slot,
/// together, before it is refused with `LimitExceeded`.
pub(crate) const RESTORE_SLOT_WAIT: Duration = Duration::from_secs(30);

/// How long a request waits for its detached task to register the CREATING
/// target before it answers `Internal`. The wait for a slot
/// ([`RESTORE_SLOT_WAIT`]) runs inside the task, so it counts against this
/// bound too. The task goes on; the bound is what turns a connection pool
/// that has stopped handing out connections into a 500 instead of a request
/// that never returns (the driver enforces no checkout timeout, see
/// [`crate::effective_pool_size`]).
pub(crate) const RESTORE_REGISTRATION_TIMEOUT: Duration = Duration::from_secs(60);

/// Restores the whole process may have in flight at once, across accounts:
/// `min(16, pool_size / 2)`, at least 1, where `pool_size` is the driver pool
/// of the engine's client (its `maxPoolSize`, default 10).
///
/// A restore's detached task holds up to one pooled connection at a time
/// (the `$out` copy, a catalog write) and its heartbeat task takes one for
/// each refresh, so without a process-wide bound enough concurrent restores
/// can hold every pooled connection and every other request waits on a
/// checkout the driver never times out. Half the pool stays free for them.
/// A restore takes its account slot first, then one of these; the two waits
/// share one deadline of [`BackupLiveness::slot_wait`].
pub(crate) fn restore_server_slot_count(pool_size: u32) -> usize {
    usize::try_from(pool_size / 2)
        .unwrap_or(usize::MAX)
        .clamp(1, RESTORE_SERVER_SLOTS_MAX)
}

/// The two slots a running restore holds until its copy ends.
pub(crate) struct RestorePermits {
    _account: OwnedSemaphorePermit,
    _server: OwnedSemaphorePermit,
}

/// Take an account slot, then a server slot, waiting at most `wait` for
/// both together. Running out of time on either is `LimitExceeded`, with a
/// message naming the limit that was full.
async fn acquire_restore_permits(
    account: Arc<Semaphore>,
    server: Arc<Semaphore>,
    server_slots: usize,
    wait: Duration,
) -> Result<RestorePermits, StorageError> {
    let deadline = tokio::time::Instant::now() + wait;
    let closed =
        |e: tokio::sync::AcquireError| StorageError::Internal(format!("restore slot: {e}"));
    let account_permit = tokio::time::timeout_at(deadline, account.acquire_owned())
        .await
        .map_err(|_| {
            StorageError::LimitExceeded(format!(
                "Too many restores are in progress for this account (at most \
                 {RESTORE_SLOTS_PER_ACCOUNT} at once); retry later"
            ))
        })?
        .map_err(closed)?;
    let server_permit = tokio::time::timeout_at(deadline, server.acquire_owned())
        .await
        .map_err(|_| {
            StorageError::LimitExceeded(format!(
                "Too many restores are in progress on this server (at most {server_slots} at \
                 once); retry later"
            ))
        })?
        .map_err(closed)?;
    Ok(RestorePermits {
        _account: account_permit,
        _server: server_permit,
    })
}

/// Per-account restore slots of one engine (shared by its clones, so one
/// per server process). An entry nobody holds (no permit, no waiter) is
/// pruned the next time any account takes a slot, so the map does not grow
/// with every account that ever restored.
#[derive(Clone, Default)]
pub(crate) struct RestoreAccountSlots(Arc<std::sync::Mutex<HashMap<String, Arc<Semaphore>>>>);

impl RestoreAccountSlots {
    /// The slot semaphore for `account_id`.
    pub(crate) fn for_account(&self, account_id: &str) -> Arc<Semaphore> {
        let mut slots = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A permit and a waiter each hold a clone, so a count of one means
        // the account has nothing in flight.
        slots.retain(|_, s| Arc::strong_count(s) > 1);
        Arc::clone(
            slots
                .entry(account_id.to_owned())
                .or_insert_with(|| Arc::new(Semaphore::new(RESTORE_SLOTS_PER_ACCOUNT))),
        )
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

/// A restore first claims backup metadata, then transfers protection to the
/// provenance-bearing CREATING target. DeleteBackup claims the same metadata
/// before dropping data, so neither operation can pass the other between its
/// check and destructive action. The heartbeat fields do not take part in
/// either predicate: a stale claim still excludes the other operation until
/// the sweep removes it.
fn restore_claim_filter(account_id: &str, backup_arn: &str) -> Document {
    doc! {
        "_id": backup_arn,
        "account_id": account_id,
        "backup_status": "AVAILABLE",
        "restore_in_progress": { "$exists": false },
        "delete_in_progress": { "$exists": false },
    }
}

fn delete_claim_filter(account_id: &str, backup_arn: &str) -> Document {
    doc! {
        "_id": backup_arn,
        "account_id": account_id,
        "backup_status": "AVAILABLE",
        "restore_in_progress": { "$exists": false },
        "delete_in_progress": { "$exists": false },
    }
}

fn restoring_target_filter(account_id: &str, backup_arn: &str) -> Document {
    doc! {
        "_id.account_id": account_id,
        "restore_source_backup_arn": backup_arn,
        "table_status": "CREATING",
    }
}

/// How long a restore retries a backup claim held by another restore.
pub(crate) const RESTORE_CLAIM_RETRY_FOR: Duration = Duration::from_secs(2);

/// Why a restore's claim found no backup document to claim, from a re-read of
/// the document (`None`: no document for this ARN and account).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestoreClaimMiss {
    /// Missing, or not AVAILABLE.
    NotFound,
    /// AVAILABLE and claimed by a DeleteBackup.
    DeleteClaimed,
    /// AVAILABLE and claimed by another restore only.
    RestoreClaimed,
    /// AVAILABLE and unclaimed: the other claim was released meanwhile.
    Free,
}

fn restore_claim_miss(doc: Option<&Document>) -> RestoreClaimMiss {
    let Some(doc) = doc else {
        return RestoreClaimMiss::NotFound;
    };
    if doc.get_str("backup_status") != Ok("AVAILABLE") {
        RestoreClaimMiss::NotFound
    } else if doc.contains_key(DELETE_CLAIM) {
        RestoreClaimMiss::DeleteClaimed
    } else if doc.contains_key(RESTORE_CLAIM) {
        RestoreClaimMiss::RestoreClaimed
    } else {
        RestoreClaimMiss::Free
    }
}

/// The error for a DeleteBackup whose claim found no document to claim.
/// `restoring`: a CREATING restore target of the backup exists; `miss`: the
/// re-read backup document, classified. A backup another DeleteBackup has
/// claimed is in use (it reads AVAILABLE until that delete finishes), not
/// missing.
fn delete_claim_miss_error(
    restoring: bool,
    miss: RestoreClaimMiss,
    backup_arn: &str,
) -> StorageError {
    match miss {
        RestoreClaimMiss::DeleteClaimed => {
            StorageError::BackupInUse(format!("Backup is being deleted: {backup_arn}"))
        }
        RestoreClaimMiss::RestoreClaimed => StorageError::BackupInUse(format!(
            "Backup is being used to restore another table: {backup_arn}"
        )),
        RestoreClaimMiss::NotFound | RestoreClaimMiss::Free if restoring => {
            StorageError::BackupInUse(format!(
                "Backup is being used to restore another table: {backup_arn}"
            ))
        }
        RestoreClaimMiss::NotFound | RestoreClaimMiss::Free => {
            StorageError::Validation(format!("Backup not found: {backup_arn}"))
        }
    }
}

/// Install `field = operation_id` and its heartbeat (server clock) in one
/// update. A pipeline update, so `$$NOW` is the server's time and processes
/// with skewed clocks agree on staleness.
fn claim_update(field: &str, heartbeat_field: &str, operation_id: &str) -> Vec<Document> {
    let mut set = Document::new();
    set.insert(field, doc! { "$literal": operation_id });
    set.insert(heartbeat_field, "$$NOW");
    vec![doc! { "$set": set }]
}

/// `$expr` that is true when `heartbeat_field` is missing (a marker written
/// before heartbeats existed) or older than `stale_after` by the server clock.
fn heartbeat_is_stale(heartbeat_field: &str, stale_after: Duration) -> Document {
    let stale_ms = i64::try_from(stale_after.as_millis()).unwrap_or(i64::MAX);
    doc! {
        "$lt": [
            { "$ifNull": [ format!("${heartbeat_field}"), bson::DateTime::from_millis(0) ] },
            { "$subtract": [ "$$NOW", stale_ms ] },
        ]
    }
}

/// Filter for a stale claim of `field` on backup documents.
fn stale_claim_filter(field: &str, heartbeat_field: &str, stale_after: Duration) -> Document {
    let mut filter = Document::new();
    filter.insert(field, doc! { "$exists": true });
    filter.insert("$expr", heartbeat_is_stale(heartbeat_field, stale_after));
    filter
}

/// Filter for an abandoned restore target: CREATING, carrying restore
/// provenance, still before its base copy finished (no backfill flag; after
/// that the backfill worker owns it), and not heartbeating.
fn stale_target_filter(stale_after: Duration) -> Document {
    doc! {
        "table_status": "CREATING",
        "restore_source_backup_arn": { "$exists": true },
        "restore_backfill_pending": { "$exists": false },
        "$expr": heartbeat_is_stale(TARGET_HEARTBEAT, stale_after),
    }
}

fn claim_heartbeat_field(field: &'static str) -> &'static str {
    if field == RESTORE_CLAIM {
        RESTORE_CLAIM_HEARTBEAT
    } else {
        DELETE_CLAIM_HEARTBEAT
    }
}

/// Restore provenance written into the target's first catalog document.
pub(crate) struct RestoreProvenance<'a> {
    pub(crate) backup_arn: &'a str,
    pub(crate) source_table_arn: &'a str,
    pub(crate) restore_date_time: bson::DateTime,
    pub(crate) operation_id: &'a str,
    /// The target's first heartbeat. An insert cannot use `$$NOW`, so this is
    /// the inserting process's clock; `register_restore` overwrites it with
    /// the server's clock as soon as the insert returns.
    pub(crate) first_heartbeat: bson::DateTime,
    /// Notified once the target document is inserted, while the rest of the
    /// target (collections, indexes) is still being created.
    pub(crate) inserted: Option<&'a tokio::sync::Notify>,
}

impl RestoreProvenance<'_> {
    pub(crate) fn insert_into(&self, table_doc: &mut Document) {
        table_doc.insert("restore_source_backup_arn", self.backup_arn);
        table_doc.insert("restore_source_table_arn", self.source_table_arn);
        table_doc.insert("restore_date_time", self.restore_date_time);
        table_doc.insert(TARGET_OPERATION, self.operation_id);
        table_doc.insert(TARGET_HEARTBEAT, self.first_heartbeat);
    }
}

struct BackupMetadataClaim {
    backups: mongodb::Collection<Document>,
    account_id: String,
    backup_arn: String,
    field: &'static str,
    operation_id: String,
    armed: bool,
}

impl BackupMetadataClaim {
    fn new(
        backups: mongodb::Collection<Document>,
        account_id: &str,
        backup_arn: &str,
        field: &'static str,
        operation_id: String,
    ) -> Self {
        Self {
            backups,
            account_id: account_id.to_owned(),
            backup_arn: backup_arn.to_owned(),
            field,
            operation_id,
            armed: true,
        }
    }

    fn filter(&self) -> Document {
        let mut filter = doc! {
            "_id": &self.backup_arn,
            "account_id": &self.account_id,
        };
        filter.insert(self.field, &self.operation_id);
        filter
    }

    fn unset_update(field: &'static str) -> Document {
        let mut unset = Document::new();
        unset.insert(field, "");
        unset.insert(claim_heartbeat_field(field), "");
        doc! { "$unset": unset }
    }

    /// Release the claim. `Ok(false)` means it was no longer ours to release:
    /// a sweep removed it because its heartbeat had lapsed.
    async fn release(&mut self) -> Result<bool, StorageError> {
        if !self.armed {
            return Ok(true);
        }
        let result = self
            .backups
            .update_one(self.filter(), Self::unset_update(self.field))
            .await
            .map_err(internal)?;
        self.armed = false;
        Ok(result.matched_count == 1)
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for BackupMetadataClaim {
    /// Release a claim whose operation was dropped before its destructive
    /// step (a DeleteBackup request cancelled between its claim and its drop;
    /// DeleteBackup disarms the claim before dropping, so this never runs
    /// after the data may be gone). This runs only when a Tokio runtime is
    /// current and the spawned update can still reach the catalog; a process
    /// exit, a drop outside a runtime, or a failed update leaves the marker in
    /// place. The backstop for all of those is the heartbeat sweep
    /// ([`MongoEngine::sweep_stale_backup_claims`]): the companion heartbeat
    /// stops with the operation, and once the marker is older than
    /// [`BACKUP_HEARTBEAT_STALE_AFTER`] the sweep clears a restore claim and
    /// finishes the delete of a delete claim.
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let backups = self.backups.clone();
        let filter = self.filter();
        let update = Self::unset_update(self.field);
        let backup_arn = self.backup_arn.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let _cleanup = runtime.spawn(async move {
                if let Err(error) = backups.update_one(filter, update).await {
                    tracing::error!(
                        "could not clear cancelled backup metadata claim for {backup_arn}: {error}"
                    );
                }
            });
        }
    }
}

/// The companion heartbeat of one restore or DeleteBackup operation. Aborted
/// when dropped, so the heartbeat stops exactly when its owner does.
pub(crate) struct Heartbeat(tokio::task::JoinHandle<()>);

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The updates that refresh every marker `operation_id` owns on `backup_arn`
/// (either claim) and on the restore target, if any, as (collection, marker,
/// filter, pipeline update). Each matches only the owner's marker, so a
/// refresh never revives a marker a sweep already removed.
fn heartbeat_updates(
    operation_id: &str,
    account_id: &str,
    backup_arn: &str,
    target_table: Option<&str>,
) -> Vec<(&'static str, &'static str, Document, Vec<Document>)> {
    let mut updates = Vec::new();
    for (field, heartbeat_field) in [
        (RESTORE_CLAIM, RESTORE_CLAIM_HEARTBEAT),
        (DELETE_CLAIM, DELETE_CLAIM_HEARTBEAT),
    ] {
        let mut filter = doc! { "_id": backup_arn, "account_id": account_id };
        filter.insert(field, operation_id);
        let mut set = Document::new();
        set.insert(heartbeat_field, "$$NOW");
        updates.push(("backups", field, filter, vec![doc! { "$set": set }]));
    }
    if let Some(table_name) = target_table {
        let mut filter = doc! {
            "_id": { "account_id": account_id, "table_name": table_name },
            "table_status": "CREATING",
        };
        filter.insert(TARGET_OPERATION, operation_id);
        let mut set = Document::new();
        set.insert(TARGET_HEARTBEAT, "$$NOW");
        updates.push((
            "tables",
            TARGET_HEARTBEAT,
            filter,
            vec![doc! { "$set": set }],
        ));
    }
    updates
}

/// Run `run` on every item, also after one fails, and return every error.
async fn try_each<T, Fut>(items: Vec<T>, mut run: impl FnMut(T) -> Fut) -> Vec<StorageError>
where
    Fut: std::future::Future<Output = Result<(), StorageError>>,
{
    let mut errors = Vec::new();
    for item in items {
        if let Err(error) = run(item).await {
            errors.push(error);
        }
    }
    errors
}

/// Refresh every marker `operation_id` owns (see [`heartbeat_updates`]). A
/// failed update does not stop the others; every failure is returned.
async fn refresh_heartbeats(
    catalog: &mongodb::Database,
    operation_id: &str,
    account_id: &str,
    backup_arn: &str,
    target_table: Option<&str>,
) -> Vec<StorageError> {
    let updates = heartbeat_updates(operation_id, account_id, backup_arn, target_table);
    try_each(updates, |(collection, marker, filter, update)| {
        let collection_handle = catalog.collection::<Document>(collection);
        async move {
            collection_handle
                .update_one(filter, update)
                .await
                .map(|_| ())
                .map_err(|e| StorageError::Internal(format!("{collection}.{marker}: {e}")))
        }
    })
    .await
}

/// What one sweep removed, by name, for the log.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BackupSweep {
    /// Backup ARNs whose stale restore claim was cleared.
    pub restore_claims_cleared: Vec<String>,
    /// `<account>/<table>` of abandoned restore targets removed.
    pub restore_targets_removed: Vec<String>,
    /// Backup ARNs whose interrupted DeleteBackup the sweep finished.
    pub delete_claims_finished: Vec<String>,
    /// Backup ARNs whose stale delete claim the sweep took over but left held,
    /// because a restore target from the backup appeared after its first
    /// check; a later sweep finishes the delete.
    pub delete_claims_waiting: Vec<String>,
}

impl BackupSweep {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.restore_claims_cleared.is_empty()
            && self.restore_targets_removed.is_empty()
            && self.delete_claims_finished.is_empty()
            && self.delete_claims_waiting.is_empty()
    }
}

impl std::fmt::Display for BackupSweep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts = [
            (
                "cleared abandoned restore claim(s) on",
                &self.restore_claims_cleared,
            ),
            (
                "removed abandoned restore target(s)",
                &self.restore_targets_removed,
            ),
            (
                "finished interrupted DeleteBackup(s) of",
                &self.delete_claims_finished,
            ),
            (
                "left interrupted DeleteBackup(s) to a later sweep (a restore target exists) of",
                &self.delete_claims_waiting,
            ),
        ];
        let mut first = true;
        for (label, names) in parts {
            if names.is_empty() {
                continue;
            }
            if !first {
                f.write_str("; ")?;
            }
            first = false;
            write!(f, "{label} [{}]", names.join(", "))?;
        }
        Ok(())
    }
}

impl MongoEngine {
    /// Start the heartbeat of `operation_id`: at once and then every
    /// `heartbeat_interval` it refreshes the operation's claim on `backup_arn`
    /// and, for a restore, its CREATING target. Refreshes that find no marker
    /// do nothing; each failed update is logged.
    pub(crate) fn start_backup_heartbeat(
        &self,
        operation_id: &str,
        account_id: &str,
        backup_arn: &str,
        target_table: Option<&str>,
    ) -> Heartbeat {
        let catalog = self.catalog_db.clone();
        let interval = self.backup_liveness.heartbeat_interval;
        let operation_id = operation_id.to_owned();
        let account_id = account_id.to_owned();
        let backup_arn = backup_arn.to_owned();
        let target_table = target_table.map(str::to_owned);
        Heartbeat(tokio::spawn(async move {
            loop {
                for error in refresh_heartbeats(
                    &catalog,
                    &operation_id,
                    &account_id,
                    &backup_arn,
                    target_table.as_deref(),
                )
                .await
                {
                    tracing::warn!(
                        "backup heartbeat for operation {operation_id} on {backup_arn} failed: \
                         {error}"
                    );
                }
                tokio::time::sleep(interval).await;
            }
        }))
    }

    /// Remove the backup markers and restore targets whose owner stopped
    /// heartbeating. Safe while other processes share the catalog: every step
    /// re-checks staleness in the same statement that acts, so a marker whose
    /// owner refreshed it meanwhile is left alone. Run at startup and
    /// periodically by the GSI backfill worker.
    ///
    /// - A stale restore claim is removed. If its restore had already created
    ///   its target, the target carries its own heartbeat and is judged by it.
    /// - A stale CREATING restore target that has not finished its base copy
    ///   is removed completely: catalog document (which is its provenance), data
    ///   collection, and index collections and documents, so DeleteBackup of
    ///   the source and the target name are free again.
    /// - A stale delete claim whose backup has a CREATING restore target is
    ///   left as it is; a later sweep finishes the delete once the target is
    ///   ACTIVE or removed. Otherwise the claim is taken over (re-claimed under
    ///   a new operation id and heartbeat) and the delete is finished: the
    ///   caller already received an answer or died waiting for one, the
    ///   collection drop is idempotent, and leaving the backup AVAILABLE could
    ///   point at data the crash already dropped. The sweep never releases a
    ///   delete claim: the stalled owner may still drop the data.
    ///
    /// Targets are swept before delete claims, so a delete blocked only by a
    /// dead restore is finished in the same pass.
    pub async fn sweep_stale_backup_claims(&self) -> Result<BackupSweep, StorageError> {
        let stale_after = self.backup_liveness.stale_after;
        let backups_coll = self.catalog_db.collection::<Document>("backups");
        let tables_coll = self.catalog_db.collection::<Document>("tables");
        let mut sweep = BackupSweep::default();

        // 1. Stale restore claims.
        let stale: Vec<Document> = backups_coll
            .find(stale_claim_filter(
                RESTORE_CLAIM,
                RESTORE_CLAIM_HEARTBEAT,
                stale_after,
            ))
            .await
            .map_err(internal)?
            .try_collect()
            .await
            .map_err(internal)?;
        for meta in stale {
            let (Ok(arn), Some(owner)) = (meta.get_str("_id"), meta.get(RESTORE_CLAIM)) else {
                continue;
            };
            let mut filter = doc! { "_id": arn };
            filter.insert(RESTORE_CLAIM, owner.clone());
            filter.insert(
                "$expr",
                heartbeat_is_stale(RESTORE_CLAIM_HEARTBEAT, stale_after),
            );
            let result = backups_coll
                .update_one(filter, BackupMetadataClaim::unset_update(RESTORE_CLAIM))
                .await
                .map_err(internal)?;
            if result.modified_count == 1 {
                tracing::warn!(
                    "backup sweep: cleared the restore claim on {arn} (operation {owner}); its \
                     heartbeat stopped"
                );
                sweep.restore_claims_cleared.push(arn.to_owned());
            }
        }

        // 2. Abandoned restore targets.
        let stale: Vec<Document> = tables_coll
            .find(stale_target_filter(stale_after))
            .await
            .map_err(internal)?
            .try_collect()
            .await
            .map_err(internal)?;
        for target in stale {
            let (Ok(id), Ok(table_id)) = (target.get_document("_id"), target.get_str("table_id"))
            else {
                continue;
            };
            let mut filter = stale_target_filter(stale_after);
            filter.insert("_id", id.clone());
            filter.insert("table_id", table_id);
            let removed = tables_coll
                .find_one_and_delete(filter)
                .await
                .map_err(internal)?;
            let Some(removed) = removed else {
                continue;
            };
            let name = format!(
                "{}/{}",
                id.get_str("account_id").unwrap_or("?"),
                id.get_str("table_name").unwrap_or("?")
            );
            let source = removed
                .get_str("restore_source_backup_arn")
                .unwrap_or("?")
                .to_owned();
            match self.remove_target_collections(table_id).await {
                Ok(()) => tracing::warn!(
                    "backup sweep: removed abandoned restore target {name} (table_id \
                     {table_id}, restoring {source}); its restore stopped heartbeating before \
                     its copy finished"
                ),
                Err(error) => tracing::error!(
                    "backup sweep: removed abandoned restore target {name} (table_id \
                     {table_id}, restoring {source}) from the catalog, but dropping its \
                     collections failed: {error}"
                ),
            }
            sweep.restore_targets_removed.push(name);
        }

        // 3. Stale delete claims.
        let stale: Vec<Document> = backups_coll
            .find(stale_claim_filter(
                DELETE_CLAIM,
                DELETE_CLAIM_HEARTBEAT,
                stale_after,
            ))
            .await
            .map_err(internal)?
            .try_collect()
            .await
            .map_err(internal)?;
        for meta in stale {
            let (Ok(arn), Ok(account_id), Some(owner)) = (
                meta.get_str("_id"),
                meta.get_str("account_id"),
                meta.get(DELETE_CLAIM),
            ) else {
                continue;
            };
            // A CREATING restore target of this backup: leave the claim for a
            // later sweep, which finishes the delete once the target is
            // ACTIVE or removed. Nothing is written, so the next sweep looks
            // again.
            if tables_coll
                .find_one(restoring_target_filter(account_id, arn))
                .await
                .map_err(internal)?
                .is_some()
            {
                tracing::debug!(
                    "backup sweep: the stale DeleteBackup claim on {arn} waits for a running \
                     restore from it"
                );
                continue;
            }
            let sweeper = uuid::Uuid::new_v4().to_string();
            let mut filter = doc! { "_id": arn };
            filter.insert(DELETE_CLAIM, owner.clone());
            filter.insert(
                "$expr",
                heartbeat_is_stale(DELETE_CLAIM_HEARTBEAT, stale_after),
            );
            let taken = backups_coll
                .update_one(
                    filter,
                    claim_update(DELETE_CLAIM, DELETE_CLAIM_HEARTBEAT, &sweeper),
                )
                .await
                .map_err(internal)?;
            if taken.modified_count != 1 {
                continue;
            }
            // The sweep never releases a delete claim it took over: the
            // stalled owner may still be about to drop the data, and only a
            // held claim keeps the backup from reading as AVAILABLE once the
            // data is gone. Any early return below leaves the claim held by
            // `sweeper`; its heartbeat stops when this function returns, and a
            // later sweep takes it over again.
            let _heartbeat = self.start_backup_heartbeat(&sweeper, account_id, arn, None);
            let mut sweeper_filter = doc! { "_id": arn, "account_id": account_id };
            sweeper_filter.insert(DELETE_CLAIM, &sweeper);
            // A restore whose claim was swept while it stalled can insert its
            // target after the check above; it aborts that target when it sees
            // this claim, and a later sweep finishes the delete.
            if tables_coll
                .find_one(restoring_target_filter(account_id, arn))
                .await
                .map_err(internal)?
                .is_some()
            {
                tracing::warn!(
                    "backup sweep: took over the stale DeleteBackup claim on {arn}, but a \
                     restore target from it exists; a later sweep finishes the delete"
                );
                sweep.delete_claims_waiting.push(arn.to_owned());
                continue;
            }
            if let Ok(backup_id) = meta.get_str("backup_id") {
                self.drop_backup_collection(backup_id).await?;
            }
            backups_coll
                .update_one(
                    sweeper_filter,
                    doc! {
                        "$set": { "backup_status": "DELETED" },
                        "$unset": { DELETE_CLAIM: "", DELETE_CLAIM_HEARTBEAT: "" },
                    },
                )
                .await
                .map_err(internal)?;
            tracing::warn!(
                "backup sweep: finished the interrupted DeleteBackup of {arn} (operation {owner})"
            );
            sweep.delete_claims_finished.push(arn.to_owned());
        }
        Ok(sweep)
    }

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
    async fn remove_target_collections(&self, table_id: &str) -> Result<(), StorageError> {
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

    /// Remove a partial restore target without allowing its provenance to keep
    /// the source backup in use. The full predicate ensures cleanup can only
    /// claim the target created by this restore.
    async fn abort_backup_restore(
        &self,
        account_id: &str,
        target_table_name: &str,
        backup_arn: &str,
        restore_operation_id: &str,
    ) -> Result<bool, StorageError> {
        let removed = self
            .catalog_db
            .collection::<Document>("tables")
            .find_one_and_delete(doc! {
                "_id": { "account_id": account_id, "table_name": target_table_name },
                "restore_source_backup_arn": backup_arn,
                "restore_operation_id": restore_operation_id,
                "table_status": "CREATING",
                "status_transition_at": Bson::Null,
            })
            .await
            .map_err(internal)?;
        let Some(removed) = removed else {
            return Ok(false);
        };
        let table_id = removed
            .get_str("table_id")
            .map_err(|_| StorageError::Internal("restore target missing table_id".to_owned()))?;

        // The catalog row is already gone, so even a failed physical cleanup
        // cannot leave provenance that blocks DeleteBackup.
        self.remove_target_collections(table_id).await?;
        Ok(true)
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
    /// target insert is the fence. Checked before the restore takes a slot
    /// and again under the claim, so a restore into such a name is refused as
    /// the name (the service's TableInUseException) at once, whatever state
    /// the backup is in, as on the other backends.
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

    /// Phase one of a restore: install `restore_in_progress = operation_id`
    /// atomically with the AVAILABLE and account check, excluding
    /// DeleteBackup's claim, then check that no CREATING target is restoring
    /// this backup already. Returns the backup document.
    ///
    /// Only the claim holder inserts a target, and it releases the claim only
    /// after that insert, so the claim followed by the target check is what
    /// makes "one restore of a backup at a time" hold against a simultaneous
    /// request: the second either finds the claim held or finds the first's
    /// target. Both are `BackupInUse`, as the service answers while a restore
    /// from the backup is still CREATING. The guarantee holds for as long as
    /// the holder's heartbeat does: a holder stalled past the stale window
    /// between claim and insert loses the claim to the sweep and a second
    /// restore can be admitted (see the module doc). A claim held by
    /// DeleteBackup is `BackupInUse` too. Missing, another account's, or not
    /// AVAILABLE: `Backup not found`. A claim that was released between the
    /// claim attempt and the re-read (a restore that failed before its
    /// insert) is retried for up to [`RESTORE_CLAIM_RETRY_FOR`].
    async fn claim_backup_for_restore(
        &self,
        account_id: &str,
        backup_arn: &str,
        target_table_name: &str,
        operation_id: &str,
    ) -> Result<Document, StorageError> {
        let backups_coll = self.catalog_db.collection::<Document>("backups");
        // The name again, unfenced, under the claim: the state may have
        // changed while this restore waited for a slot. The target insert
        // stays the fence.
        self.refuse_a_name_being_restored(account_id, target_table_name)
            .await?;
        let started = Instant::now();
        let mut delay = Duration::from_millis(10);
        let doc = loop {
            if let Some(doc) = backups_coll
                .find_one_and_update(
                    restore_claim_filter(account_id, backup_arn),
                    claim_update(RESTORE_CLAIM, RESTORE_CLAIM_HEARTBEAT, operation_id),
                )
                .await
                .map_err(internal)?
            {
                break doc;
            }
            let current = backups_coll
                .find_one(doc! { "_id": backup_arn, "account_id": account_id })
                .await
                .map_err(internal)?;
            match restore_claim_miss(current.as_ref()) {
                RestoreClaimMiss::NotFound => {
                    return Err(StorageError::Validation(format!(
                        "Backup not found: {backup_arn}"
                    )));
                }
                RestoreClaimMiss::DeleteClaimed => {
                    return Err(StorageError::BackupInUse(format!(
                        "Backup is being deleted: {backup_arn}"
                    )));
                }
                RestoreClaimMiss::RestoreClaimed => {
                    return Err(StorageError::BackupInUse(format!(
                        "Backup is being used to restore another table: {backup_arn}"
                    )));
                }
                // Free: the other claim was released between the two reads.
                RestoreClaimMiss::Free => {}
            }
            if started.elapsed() + delay > RESTORE_CLAIM_RETRY_FOR {
                return Err(StorageError::BackupInUse(format!(
                    "Backup is being used by another restore that is starting; retry: \
                     {backup_arn}"
                )));
            }
            // Jitter, so restores that collided do not retry in step.
            let jitter_ms = u64::try_from(uuid::Uuid::new_v4().as_u128() % 10).unwrap_or(0);
            tokio::time::sleep(delay + Duration::from_millis(jitter_ms)).await;
            delay = (delay * 2).min(Duration::from_millis(200));
        };
        // Under the claim: a CREATING target of this backup belongs to a
        // restore that is still running (or one abandoned and not yet swept,
        // which blocks DeleteBackup the same way). When that target is the
        // name this restore asked for, the name is what is refused
        // (TableBeingRestored, the service's TableInUseException), as on the
        // other backends; a CREATING restore target of another backup under
        // the name is caught by the insert.
        let restoring = self
            .catalog_db
            .collection::<Document>("tables")
            .find_one(restoring_target_filter(account_id, backup_arn))
            .await;
        match restoring {
            Ok(None) => Ok(doc),
            Ok(Some(target)) => {
                let target_name = target
                    .get_document("_id")
                    .and_then(|id| id.get_str("table_name"))
                    .unwrap_or("?");
                let mut claim = BackupMetadataClaim::new(
                    backups_coll,
                    account_id,
                    backup_arn,
                    RESTORE_CLAIM,
                    operation_id.to_owned(),
                );
                claim.release().await?;
                if target_name == target_table_name {
                    return Err(StorageError::TableBeingRestored {
                        table_name: target_table_name.to_owned(),
                        backup_arn: backup_arn.to_owned(),
                    });
                }
                Err(StorageError::BackupInUse(format!(
                    "Backup is being used to restore table {target_name}: {backup_arn}"
                )))
            }
            Err(e) => {
                let mut claim = BackupMetadataClaim::new(
                    backups_coll,
                    account_id,
                    backup_arn,
                    RESTORE_CLAIM,
                    operation_id.to_owned(),
                );
                claim.release().await?;
                Err(internal(e))
            }
        }
    }

    /// Phase two of a restore, once the target document exists: replace the
    /// target's local-clock heartbeat with the server's, then release the
    /// backup claim. `Err` means the claim had been swept and the backup is
    /// gone or being deleted, so the caller must abort the target.
    async fn hand_claim_to_target(
        &self,
        restore_claim: &mut BackupMetadataClaim,
        account_id: &str,
        backup_arn: &str,
        target_table_name: &str,
        operation_id: &str,
    ) -> Result<(), StorageError> {
        // A process whose clock is behind the server's must not look stale to
        // another process's sweep before its first periodic refresh.
        for error in refresh_heartbeats(
            &self.catalog_db,
            operation_id,
            account_id,
            backup_arn,
            Some(target_table_name),
        )
        .await
        {
            tracing::warn!(
                "restore of {backup_arn} into {target_table_name}: first heartbeat refresh \
                 failed: {error}"
            );
        }
        match restore_claim.release().await? {
            true => Ok(()),
            // A sweep removed the claim because its heartbeat lapsed (this
            // process stalled for longer than the stale window). A DeleteBackup
            // may have claimed and dropped the backup in the gap. The target
            // now exists, so any later DeleteBackup sees it; one that started
            // earlier is visible as a delete claim or a DELETED status.
            false => match self
                .catalog_db
                .collection::<Document>("backups")
                .find_one(doc! {
                    "_id": backup_arn,
                    "account_id": account_id,
                    "backup_status": "AVAILABLE",
                    DELETE_CLAIM: { "$exists": false },
                })
                .await
                .map_err(internal)?
            {
                Some(_) => Ok(()),
                None => Err(StorageError::Validation(format!(
                    "Backup not found: {backup_arn}"
                ))),
            },
        }
    }

    /// Phase one and two of a restore: claim the backup, insert the CREATING
    /// target with provenance, release the claim. Returns the target's
    /// description and the backup's data collection id.
    async fn register_restore(
        &self,
        account_id: &str,
        request: RestoreRequest,
        operation_id: &str,
    ) -> Result<(TableDescription, String), StorageError> {
        let RestoreRequest {
            backup_arn,
            source_table_arn,
            restore_date_time,
            input,
        } = request;
        let target_table_name = input.table_name.clone();
        let backups_coll = self.catalog_db.collection::<Document>("backups");
        let backup_doc = self
            .claim_backup_for_restore(account_id, &backup_arn, &target_table_name, operation_id)
            .await?;
        let mut restore_claim = BackupMetadataClaim::new(
            backups_coll.clone(),
            account_id,
            &backup_arn,
            RESTORE_CLAIM,
            operation_id.to_owned(),
        );
        let Ok(backup_id) = backup_doc.get_str("backup_id").map(str::to_owned) else {
            let error = StorageError::Internal(format!(
                "backup metadata for {backup_arn} has no backup_id"
            ));
            if let Err(cleanup) = restore_claim.release().await {
                tracing::error!(
                    "restore of {backup_arn} failed ({error}) and its metadata claim could not \
                     be cleared: {cleanup}"
                );
            }
            return Err(error);
        };

        // Provenance is part of the initial CREATING document. The claim is
        // kept until that document exists, so protection overlaps and
        // DeleteBackup can observe neither an unclaimed backup nor an
        // ownerless target; it is released as soon as the document is
        // inserted, while the target's collections and indexes are still
        // being created.
        //
        // The creation and the hand-over are polled together until both have
        // finished. An earlier shape selected the hand-over and only then
        // awaited the rest of the creation; that left the creation's
        // in-flight command (its `createCollection`, issued right after the
        // insert) holding a pooled connection with its reply unread while
        // the hand-over waited for connections of its own. Enough restores in
        // that state at once held every pooled connection, and the driver's
        // checkout never times out: the process wedged (20 concurrent
        // restores of distinct backups on the default pool of 10).
        let inserted = tokio::sync::Notify::new();
        let provenance = RestoreProvenance {
            backup_arn: &backup_arn,
            source_table_arn: &source_table_arn,
            restore_date_time: epoch_secs_datetime(restore_date_time),
            operation_id,
            first_heartbeat: self.local_now(),
            inserted: Some(&inserted),
        };
        let (created, handed) = {
            let create = self.create_table_for_restore(account_id, input, &provenance);
            let hand = async {
                inserted.notified().await;
                self.hand_claim_to_target(
                    &mut restore_claim,
                    account_id,
                    &backup_arn,
                    &target_table_name,
                    operation_id,
                )
                .await
            };
            tokio::pin!(create);
            tokio::pin!(hand);
            let mut created: Option<Result<TableDescription, StorageError>> = None;
            let mut handed: Option<Result<(), StorageError>> = None;
            // A creation that failed may have failed before its insert, in
            // which case the hand-over never starts; a successful one always
            // notified, so its hand-over is awaited to the end.
            while created.is_none()
                || (handed.is_none() && created.as_ref().is_some_and(Result::is_ok))
            {
                tokio::select! {
                    result = &mut create, if created.is_none() => created = Some(result),
                    result = &mut hand, if handed.is_none() => handed = Some(result),
                }
            }
            (created.expect("creation polled to completion"), handed)
        };
        let mut desc = match created {
            Ok(desc) => desc,
            Err(error) => {
                if let Err(cleanup) = self
                    .abort_backup_restore(account_id, &target_table_name, &backup_arn, operation_id)
                    .await
                {
                    tracing::error!(
                        "restore of {backup_arn} into {target_table_name} failed during target \
                         creation ({error}), and cleanup failed: {cleanup}"
                    );
                }
                if handed.is_none()
                    && let Err(cleanup) = restore_claim.release().await
                {
                    tracing::error!(
                        "restore of {backup_arn} failed during target creation ({error}), and \
                         its metadata claim could not be cleared: {cleanup}"
                    );
                }
                return Err(error);
            }
        };
        let still_ours = handed.expect("a created target was notified and handed over");
        if let Err(error) = still_ours {
            if let Err(cleanup) = self
                .abort_backup_restore(account_id, &target_table_name, &backup_arn, operation_id)
                .await
            {
                tracing::error!(
                    "restore of {backup_arn} lost its metadata claim ({error}) and target cleanup \
                     failed: {cleanup}"
                );
            }
            return Err(error);
        }

        desc.restore_summary = Some(extenddb_core::types::RestoreSummary {
            source_backup_arn: Some(backup_arn),
            source_table_arn: Some(source_table_arn),
            restore_date_time,
            restore_in_progress: true,
        });
        Ok((desc, backup_id))
    }

    /// Phase three, in the detached task: copy the backup's items into the
    /// target with `$out`, then hand the target to the backfill worker. On
    /// failure the target is removed. Logs the outcome either way.
    async fn copy_restore(
        &self,
        account_id: &str,
        target_table_name: &str,
        backup_arn: &str,
        operation_id: &str,
        backup_id: &str,
        table_id: &str,
    ) {
        let started = Instant::now();
        let copied = async {
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
            // can backfill them in bounded, restartable batches. Dropping the
            // operation id and heartbeat hands the target to that worker; the
            // sweep no longer judges it.
            let mut filter = doc! {
                "_id": { "account_id": account_id, "table_name": target_table_name },
                "table_id": table_id,
                "table_status": "CREATING",
            };
            filter.insert(TARGET_OPERATION, operation_id);
            let handed_over = self
                .catalog_db
                .collection::<Document>("tables")
                .update_one(
                    filter,
                    doc! {
                        "$set": {
                            "item_count": i64::try_from(item_count).unwrap_or(i64::MAX),
                            "restore_backfill_pending": true,
                        },
                        "$unset": { TARGET_OPERATION: "", TARGET_HEARTBEAT: "" },
                    },
                )
                .await
                .map_err(internal)?;
            Ok::<_, StorageError>((item_count, handed_over.matched_count == 1))
        }
        .await;

        match copied {
            Ok((items, true)) => tracing::info!(
                "restore of {backup_arn} into {target_table_name}: copied {items} item(s) in \
                 {:.1}s; index backfill pending",
                started.elapsed().as_secs_f64()
            ),
            Ok((items, false)) => {
                // The target was removed while `$out` ran (a sweep judged it
                // abandoned). `$out` may have recreated its data collection.
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
            Err(error) => match self
                .abort_backup_restore(account_id, target_table_name, backup_arn, operation_id)
                .await
            {
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
                     physical cleanup also failed: {cleanup}"
                ),
            },
        }
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

impl BackupEngine for MongoEngine {
    fn create_backup(
        &self,
        account_id: &str,
        table_name: &str,
        backup_name: &str,
    ) -> BoxFuture<'_, Result<BackupDetails, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        let backup_name = backup_name.to_string();
        Box::pin(async move {
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
                            warm_throughput: None,
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

    fn describe_backup(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = account_id.to_string();
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let backups_coll = self.catalog_db.collection::<Document>("backups");
            // Scope the lookup to the calling account so a backup ARN cannot be
            // read cross-account, and exclude DELETED backups so a deleted
            // backup reads as BackupNotFoundException. Matches the postgres
            // backend.
            let backup_doc = backups_coll
                .find_one(doc! {
                    "_id": &backup_arn,
                    "account_id": &account_id,
                    "backup_status": { "$ne": "DELETED" },
                })
                .await
                .map_err(internal)?
                .ok_or_else(|| {
                    StorageError::Validation(format!("Backup not found: {backup_arn}"))
                })?;

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

            let created_at = backup_created_at(&backup_doc);
            let table_created = backup_doc
                .get_f64("table_creation_date_time")
                .unwrap_or(created_at);
            let key_schema: Vec<KeySchemaElement> = decode_required(&backup_doc, "key_schema")?;

            Ok(BackupDescription {
                backup_details: BackupDetails {
                    backup_arn: backup_arn.clone(),
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
        let account_id = account_id.to_string();
        let table_name = table_name.map(std::string::ToString::to_string);
        Box::pin(async move {
            let backups_coll = self.catalog_db.collection::<Document>("backups");

            let mut filter = doc! {
                "account_id": &account_id,
                "backup_status": { "$ne": "DELETED" },
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

    fn delete_backup(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = account_id.to_string();
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let desc = self.describe_backup(&account_id, &backup_arn).await?;
            let backups_coll = self.catalog_db.collection::<Document>("backups");
            let tables_coll = self.catalog_db.collection::<Document>("tables");

            // Fast-path the durable half of the two-phase restore claim.
            if let Some(target) = tables_coll
                .find_one(restoring_target_filter(&account_id, &backup_arn))
                .await
                .map_err(internal)?
            {
                let name = target
                    .get_document("_id")
                    .ok()
                    .and_then(|id| id.get_str("table_name").ok())
                    .unwrap_or("?");
                return Err(StorageError::BackupInUse(format!(
                    "Backup is being used to restore table {name}: {backup_arn}"
                )));
            }

            // Atomically claim deletion only when restore has not claimed the
            // metadata. Restore uses the inverse predicate, so once either
            // marker is installed the other operation cannot start. The
            // heartbeat runs from before the claim until this future ends
            // (completes or is dropped).
            let delete_operation_id = uuid::Uuid::new_v4().to_string();
            let _heartbeat =
                self.start_backup_heartbeat(&delete_operation_id, &account_id, &backup_arn, None);
            let meta = backups_coll
                .find_one_and_update(
                    delete_claim_filter(&account_id, &backup_arn),
                    claim_update(DELETE_CLAIM, DELETE_CLAIM_HEARTBEAT, &delete_operation_id),
                )
                .return_document(mongodb::options::ReturnDocument::Before)
                .await
                .map_err(internal)?;
            let Some(meta) = meta else {
                let restoring = tables_coll
                    .find_one(restoring_target_filter(&account_id, &backup_arn))
                    .await
                    .map_err(internal)?
                    .is_some();
                let reread = backups_coll
                    .find_one(doc! { "_id": &backup_arn, "account_id": &account_id })
                    .await
                    .map_err(internal)?;
                return Err(delete_claim_miss_error(
                    restoring,
                    restore_claim_miss(reread.as_ref()),
                    &backup_arn,
                ));
            };
            let mut delete_claim = BackupMetadataClaim::new(
                backups_coll.clone(),
                &account_id,
                &backup_arn,
                DELETE_CLAIM,
                delete_operation_id,
            );

            // A restore can transfer its marker to a target between the first
            // target read and our metadata claim. Recheck after claiming; new
            // restores are now excluded by delete_in_progress.
            if let Some(target) = tables_coll
                .find_one(restoring_target_filter(&account_id, &backup_arn))
                .await
                .map_err(internal)?
            {
                let name = target
                    .get_document("_id")
                    .ok()
                    .and_then(|id| id.get_str("table_name").ok())
                    .unwrap_or("?")
                    .to_owned();
                delete_claim.release().await?;
                return Err(StorageError::BackupInUse(format!(
                    "Backup is being used to restore table {name}: {backup_arn}"
                )));
            }

            // Drop the backup collection. If backup_id is absent (for an old
            // pre-$out backup), there is no physical collection to drop.
            //
            // From here on the claim is never released. The drop may have run
            // on the server even if this future is dropped while waiting for
            // its reply or the reply is an error, and releasing would leave an
            // AVAILABLE backup with no data, which a restore would copy as an
            // empty table. A claim left behind (an error below, or this future
            // dropped) stops heartbeating when this future ends, and the
            // stale-claim sweep finishes the delete.
            delete_claim.disarm();
            if let Ok(backup_id) = meta.get_str("backup_id") {
                self.drop_backup_collection(backup_id).await?;
            }

            backups_coll
                .update_one(
                    delete_claim.filter(),
                    doc! {
                        "$set": { "backup_status": "DELETED" },
                        "$unset": { DELETE_CLAIM: "", DELETE_CLAIM_HEARTBEAT: "" },
                    },
                )
                .await
                .map_err(internal)?;

            Ok(BackupDescription {
                backup_details: BackupDetails {
                    backup_status: "DELETED".to_owned(),
                    ..desc.backup_details
                },
                ..desc
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

    fn restore_table_from_backup(
        &self,
        account_id: &str,
        request: RestoreRequest,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let engine = self.clone();
        let account_id = account_id.to_owned();
        Box::pin(async move {
            // The whole restore, admission included, runs in a task that owns
            // its inputs, so the request future being dropped (a client
            // timeout) stops neither the wait for a slot, nor the
            // registration, nor the copy. At most RESTORE_SLOTS_PER_ACCOUNT
            // restores per account and restore_server_slot_count per process
            // run at once; the permits are released when the copy ends. A
            // restore refused for want of a slot creates nothing. The request
            // waits only until the CREATING target exists and the backup claim
            // has passed to it.
            let (registered_tx, registered_rx) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let backup_arn = request.backup_arn.clone();
                let target_table_name = request.input.table_name.clone();
                // A refusal the request is no longer there to receive (its
                // client went away, or the registration bound fired) would
                // otherwise vanish, so it is logged. A refused restore removes
                // what it created; a cleanup that failed is logged on its own,
                // and the heartbeat sweep finishes it.
                let refuse = |registered: tokio::sync::oneshot::Sender<_>, e: StorageError| {
                    if let Err(Err(e)) = registered.send(Err(e)) {
                        tracing::warn!(
                            "restore of {backup_arn} into {target_table_name} (account \
                             {account_id}) was refused after its request ended: {e}"
                        );
                    }
                };
                // The name before a slot is taken, so a client retrying its
                // own restore is answered at once instead of after the wait.
                if let Err(error) = engine
                    .refuse_a_name_being_restored(&account_id, &target_table_name)
                    .await
                {
                    refuse(registered_tx, error);
                    return;
                }
                let _permits = match acquire_restore_permits(
                    engine.restore_account_slots.for_account(&account_id),
                    Arc::clone(&engine.restore_server_slots),
                    engine.restore_server_slot_count,
                    engine.backup_liveness.slot_wait,
                )
                .await
                {
                    Ok(permits) => permits,
                    Err(error) => {
                        refuse(registered_tx, error);
                        return;
                    }
                };
                let operation_id = uuid::Uuid::new_v4().to_string();
                // Started before the claim so the claim and the target are
                // refreshed for as long as this task runs.
                let _heartbeat = engine.start_backup_heartbeat(
                    &operation_id,
                    &account_id,
                    &backup_arn,
                    Some(&target_table_name),
                );
                let (desc, backup_id) = match engine
                    .register_restore(&account_id, request, &operation_id)
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
                        &operation_id,
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
    use super::{
        RESTORE_SLOTS_PER_ACCOUNT, RestoreAccountSlots, delete_claim_filter,
        insert_non_empty_array, restore_claim_filter, restore_gsi_throughput,
        restore_provisioned_throughput, restoring_target_filter,
    };
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
            warm_throughput: None,
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

    #[test]
    fn restore_and_delete_claims_exclude_each_other() {
        let restore = restore_claim_filter("123456789012", "backup");
        let delete = delete_claim_filter("123456789012", "backup");
        for filter in [&restore, &delete] {
            assert_eq!(filter.get_str("backup_status"), Ok("AVAILABLE"));
            assert_eq!(
                filter
                    .get_document("restore_in_progress")
                    .and_then(|predicate| predicate.get_bool("$exists")),
                Ok(false)
            );
            assert_eq!(
                filter
                    .get_document("delete_in_progress")
                    .and_then(|predicate| predicate.get_bool("$exists")),
                Ok(false)
            );
        }
    }

    #[test]
    fn durable_restore_target_predicate_is_account_scoped_and_creating() {
        let filter = restoring_target_filter("123456789012", "backup");
        assert_eq!(filter.get_str("_id.account_id"), Ok("123456789012"));
        assert_eq!(filter.get_str("restore_source_backup_arn"), Ok("backup"));
        assert_eq!(filter.get_str("table_status"), Ok("CREATING"));
    }

    /// One engine's account slots are shared by its clones and by nothing
    /// else: a second engine in the same process (another test's fixture)
    /// has its own full allowance for the same account.
    #[test]
    fn account_restore_slots_belong_to_one_engine() {
        let slots = RestoreAccountSlots::default();
        let held = slots
            .for_account("123456789012")
            .try_acquire_owned()
            .expect("a free slot");
        assert_eq!(
            slots
                .clone()
                .for_account("123456789012")
                .available_permits(),
            RESTORE_SLOTS_PER_ACCOUNT - 1,
            "a clone sees the same account map"
        );
        assert_eq!(
            RestoreAccountSlots::default()
                .for_account("123456789012")
                .available_permits(),
            RESTORE_SLOTS_PER_ACCOUNT,
            "another engine has its own"
        );
        drop(held);
        // Nothing in flight for the account: its entry is pruned on the next
        // lookup of any account, and a fresh one has the full allowance.
        slots.for_account("other");
        assert_eq!(
            slots.0.lock().unwrap().len(),
            1,
            "the idle account's entry is gone"
        );
        assert_eq!(
            slots.for_account("123456789012").available_permits(),
            RESTORE_SLOTS_PER_ACCOUNT
        );
    }
}

#[cfg(test)]
mod definition_tests {
    use super::{
        BackupMetadataClaim, BackupSweep, DELETE_CLAIM, DELETE_CLAIM_HEARTBEAT, RESTORE_CLAIM,
        RESTORE_CLAIM_HEARTBEAT, RestoreClaimMiss, backup_definition_from_doc, claim_update,
        delete_claim_miss_error, heartbeat_updates, restore_claim_miss, restore_source_from_doc,
        stale_claim_filter, stale_target_filter, try_each,
    };
    use extenddb_core::types::{
        OnDemandThroughput, ProvisionedThroughput, StreamViewType, TimeToLiveStatus,
    };
    use extenddb_storage::error::StorageError;
    use mongodb::bson::{self, doc};
    use std::time::Duration;

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
    fn constant_keys_in_doc_macros_are_the_constant_values() {
        // The update documents use `CONST: value` keys; this pins that the
        // macro takes the constant's value, not its identifier.
        let update = BackupMetadataClaim::unset_update(DELETE_CLAIM);
        let unset = update.get_document("$unset").expect("$unset");
        assert!(unset.contains_key("delete_in_progress"));
        assert!(unset.contains_key("delete_in_progress_heartbeat"));
        let inline = doc! { DELETE_CLAIM_HEARTBEAT: "" };
        assert!(inline.contains_key("delete_in_progress_heartbeat"));
        let claim = claim_update(RESTORE_CLAIM, RESTORE_CLAIM_HEARTBEAT, "op");
        let set = claim[0].get_document("$set").expect("$set");
        assert_eq!(
            set.get_document("restore_in_progress")
                .and_then(|d| d.get_str("$literal")),
            Ok("op")
        );
        assert_eq!(set.get_str("restore_in_progress_heartbeat"), Ok("$$NOW"));
    }

    #[test]
    fn stale_filters_compare_the_heartbeat_with_the_server_clock() {
        let filter = stale_claim_filter(
            RESTORE_CLAIM,
            RESTORE_CLAIM_HEARTBEAT,
            Duration::from_secs(60),
        );
        assert_eq!(
            filter
                .get_document("restore_in_progress")
                .and_then(|d| d.get_bool("$exists")),
            Ok(true)
        );
        let expr = filter.get_document("$expr").expect("$expr").to_string();
        assert!(expr.contains("$restore_in_progress_heartbeat"), "{expr}");
        assert!(expr.contains("$$NOW") && expr.contains("60000"), "{expr}");
        let target = stale_target_filter(Duration::from_secs(60));
        assert_eq!(target.get_str("table_status"), Ok("CREATING"));
        assert_eq!(
            target
                .get_document("restore_backfill_pending")
                .and_then(|d| d.get_bool("$exists")),
            Ok(false)
        );
        assert!(
            target
                .get_document("$expr")
                .expect("$expr")
                .to_string()
                .contains("$restore_heartbeat_at")
        );
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

    /// A failed heartbeat update does not stop the others.
    #[tokio::test]
    async fn try_each_runs_every_item_after_a_failure() {
        let mut attempted = Vec::new();
        let errors = try_each(vec![1, 2, 3], |i| {
            attempted.push(i);
            async move {
                if i == 1 {
                    Err(StorageError::Internal(format!("update {i}")))
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert_eq!(attempted, [1, 2, 3]);
        assert_eq!(errors.len(), 1);
        assert!(matches!(&errors[0], StorageError::Internal(m) if m == "update 1"));
    }

    #[test]
    fn heartbeat_updates_cover_both_claims_and_the_target() {
        let updates = heartbeat_updates("op", "123456789012", ARN, Some("dst"));
        let names: Vec<_> = updates.iter().map(|u| (u.0, u.1)).collect();
        assert_eq!(
            names,
            [
                ("backups", "restore_in_progress"),
                ("backups", "delete_in_progress"),
                ("tables", "restore_heartbeat_at"),
            ]
        );
        assert_eq!(updates[0].2.get_str("restore_in_progress"), Ok("op"));
        assert_eq!(updates[2].2.get_str("restore_operation_id"), Ok("op"));
        assert_eq!(heartbeat_updates("op", "123456789012", ARN, None).len(), 2);
    }

    /// Why a restore claim missed decides between retrying, BackupInUse,
    /// and Backup not found.
    #[test]
    fn a_restore_claim_miss_is_classified_from_the_reread() {
        let available = doc! { "_id": ARN, "backup_status": "AVAILABLE" };
        assert_eq!(restore_claim_miss(None), RestoreClaimMiss::NotFound);
        let mut d = available.clone();
        d.insert("backup_status", "DELETED");
        assert_eq!(restore_claim_miss(Some(&d)), RestoreClaimMiss::NotFound);
        assert_eq!(
            restore_claim_miss(Some(&doc! { "_id": ARN })),
            RestoreClaimMiss::NotFound
        );
        assert_eq!(restore_claim_miss(Some(&available)), RestoreClaimMiss::Free);
        let mut d = available.clone();
        d.insert(RESTORE_CLAIM, "other");
        assert_eq!(
            restore_claim_miss(Some(&d)),
            RestoreClaimMiss::RestoreClaimed
        );
        d.insert(DELETE_CLAIM, "delete");
        assert_eq!(
            restore_claim_miss(Some(&d)),
            RestoreClaimMiss::DeleteClaimed
        );
    }

    /// R4B-1: a DeleteBackup whose claim misses because another DeleteBackup
    /// holds the backup answers BackupInUse, not Backup not found.
    #[test]
    fn a_delete_claim_miss_on_a_delete_claimed_backup_is_in_use() {
        let in_use = |e: StorageError| match e {
            StorageError::BackupInUse(m) => m,
            other => panic!("expected BackupInUse, got {other:?}"),
        };
        for restoring in [false, true] {
            let m = in_use(delete_claim_miss_error(
                restoring,
                RestoreClaimMiss::DeleteClaimed,
                ARN,
            ));
            assert!(m.starts_with("Backup is being deleted"), "{m}");
            let m = in_use(delete_claim_miss_error(
                restoring,
                RestoreClaimMiss::RestoreClaimed,
                ARN,
            ));
            assert!(
                m.starts_with("Backup is being used to restore another table"),
                "{m}"
            );
        }
        for miss in [RestoreClaimMiss::NotFound, RestoreClaimMiss::Free] {
            let m = in_use(delete_claim_miss_error(true, miss, ARN));
            assert!(
                m.starts_with("Backup is being used to restore another table"),
                "{m}"
            );
            assert!(matches!(
                delete_claim_miss_error(false, miss, ARN),
                StorageError::Validation(m) if m.starts_with("Backup not found")
            ));
        }
    }

    #[test]
    fn the_sweep_summary_names_what_it_removed() {
        assert!(BackupSweep::default().is_empty());
        assert_eq!(BackupSweep::default().to_string(), "");
        let sweep = BackupSweep {
            restore_claims_cleared: vec!["a".to_owned()],
            restore_targets_removed: vec!["acct/t1".to_owned(), "acct/t2".to_owned()],
            delete_claims_finished: vec![],
            delete_claims_waiting: vec!["b".to_owned()],
        };
        assert!(!sweep.is_empty());
        assert_eq!(
            sweep.to_string(),
            "cleared abandoned restore claim(s) on [a]; removed abandoned restore target(s) \
             [acct/t1, acct/t2]; left interrupted DeleteBackup(s) to a later sweep (a \
             restore target exists) of [b]"
        );
    }
}

/// The claim protocol model; see the module documentation.
#[cfg(test)]
#[path = "backup_protocol_model.rs"]
mod protocol_model;

#[cfg(test)]
#[path = "backup_live_tests.rs"]
mod live_tests;
