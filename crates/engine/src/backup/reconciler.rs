// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The reconciler: the periodic worker that resolves catalog rows whose
//! owner is dead, deletes what FAILED and DELETED rows leave in the store,
//! removes tombstones past their retention, and keeps the configured
//! store's health current.
//!
//! One cycle runs every `interval`. It first checks the catalog-level
//! maintenance claim; while a maintenance command holds it the reconciler
//! runs only the duties the command waits on or finishes itself (the
//! dead-owner duties, the store's check and the DELETING rows) and skips
//! the deleting duties it must not race (a FAILED row's prefix, a DELETED
//! row's objects, tombstone retention, the registrations sweep).
//!
//! The claim is read through `check_maintenance_claim(stale_job_after)`,
//! except in a cycle a maintenance command runs itself while it holds the
//! claim (`backup purge` on SQLite, with a zero dead-owner window): that
//! caller's state says so ([`ReconcilerState::for_maintenance_holder`]) and
//! the cycle takes the held branch without reading the claim. Read through
//! the clock, a zero window would see the command's own claim held only
//! when its heartbeat and the check land in the same instant, so the
//! branch would be decided by scheduler timing.
//!
//! The duties, in the order a cycle runs them:
//!
//! - A RUNNING restore job record whose `heartbeat_at` is older than
//!   `stale_job_after` (on the catalog's clock) has a dead owner. One of kind
//!   `legacy` is marked FAILED (`fail_restore_job`, conditioned on the
//!   `owner_generation` and `heartbeat_at` the listing read, so a heartbeat
//!   that lands after the listing keeps the record its owner's) and its
//!   target removed through
//!   `remove_restore_target`, never resumed: an in-catalog copy has no data
//!   files or progress to resume from. One of kind `manifest` is left for the
//!   duty that resumes it, and logged once per record for as long as this
//!   process runs: the set of logged records ([`ReconcilerState`]) belongs to
//!   the caller of [`run`] (`serve`), so a worker restart does not log a
//!   record again.
//! - A FAILED restore job record whose table is still a CREATING restore
//!   target, the FAILED-target duty: the FAILED write landed (the job's own,
//!   or an earlier cycle's) and the target's removal did not (an error
//!   between the two statements, or the process stopped between them). The
//!   backend lists it once the record is stale, and the target is removed
//!   through `remove_restore_target` with no second FAILED write.
//! - An in-catalog CREATING backup row whose create is dead (reported by the
//!   backend's `abandoned_creating_backups`) is removed through the reader's
//!   `delete`, which takes its items with it, unless the backend removed it
//!   in the call that reported it (`AbandonedLegacyBackup::removed`), when
//!   the reconciler only logs it.
//! - With a store configured: the store's check runs again
//!   ([`super::check_configured_store`]: `validate` while it has not passed,
//!   then the identity check and manifest probe) and sets the store's
//!   health, so a store whose checks run after the listener is up, or that
//!   failed at startup or later, is healthy once a check passes, and one
//!   that fails is refused from the next cycle on. The check runs under the
//!   store's bound ([`super::ConfiguredStore::check_timeout`]: 5 s for a
//!   filesystem root, 30 s for a bucket), so a store that has stopped
//!   answering costs each cycle that bound and nothing more: the duties
//!   above run every `interval` plus the bound, the check is recorded as a
//!   transport failure, and a filesystem check leaves one blocking thread
//!   parked on the mount until it answers, after which no check starts
//!   until that call returns (the runtime module's in-flight rule), so a
//!   mount that stays stalled parks one thread and not one per cycle. The
//!   check is also selected against the shutdown token, so a cycle that is
//!   checking when the server stops returns at once and the drain finishes.
//!   While the store is unhealthy the reconciler acts on no row of the
//!   store. While it is healthy, on the rows of the configured store:
//!   - Each DELETING row (a DeleteBackup whose deletion did not finish) is
//!     finished (`finish_backup_delete` in the engine's backup module): the
//!     manifest, then the prefix, then the row to DELETED.
//!   - Each CREATING row whose owner is dead is resolved. The backend's
//!     `claim_dead_backup` is the test and the takeover (a fresh generation
//!     fences the dead job's writes out; `heartbeat_at` is left as it was);
//!     a row it does not hand over has a live owner or is another's. Under
//!     the claim the store is probed for a complete manifest:
//!     `extenddb-manifest.json` and its sidecar present and agreeing, the
//!     manifest parsing with the row's ARN, and every data file it lists
//!     present under the prefix with the recorded size. A complete manifest
//!     makes the row AVAILABLE with the manifest's size, item count and
//!     `source_table`; anything else makes it FAILED with reason
//!     [`OWNER_DEAD_NO_MANIFEST`], and nothing is deleted here: the
//!     FAILED-prefix duty, which runs later in this cycle, deletes it once
//!     the row's heartbeat is older than `stale_job_after` on the database's
//!     clock: in this cycle on PostgreSQL, MongoDB and Cassandra, where a
//!     heartbeat that old is what made the owner dead (on PostgreSQL
//!     together with the owner's claim session gone, its advisory lock
//!     free); on SQLite, where the slot did, once the dead owner's last
//!     beat is that old. A store failure during the probe (not an absent
//!     object) leaves the row for the next cycle. The two writes are
//!     conditioned on the row being CREATING under the new generation
//!     (`job::failure::write_status`); one that matches no row (another
//!     reconciler took the row over in the gap, item 31) leaves the row to
//!     that owner.
//!   - Not while the maintenance claim is held: a FAILED row whose
//!     `heartbeat_at` is older than `stale_job_after` (the backend's
//!     `list_backup_rows_older_than` on the heartbeat clock) and whose
//!     prefix lists objects has its prefix deleted, retried each cycle while
//!     the listing shows objects; a DELETED row whose prefix lists objects
//!     has its prefix deleted; a FAILED or DELETED row whose prefix lists
//!     nothing and whose `completed_at` is older than `failed_row_retention`
//!     (the completed clock) is removed. Every age is the database's
//!     comparison; the engine holds no clock.
//! - Not while the maintenance claim is held, on every backend: the
//!   backend's `sweep_restore_registrations` (a no-op but on Cassandra).
//!
//! Every duty acts only on rows of the configured store (kind, root and
//! id), the legacy duty excepted. A cycle that fails logs and waits for the
//! next; each cycle records the worker metrics the other workers record
//! (`WorkerLastSuccess` and `WorkerCycleLatency` on success,
//! `WorkerErrorCount` on failure, under `QuerySource::BackupReconciler`);
//! a duty's store call that fails (a `get` or `list` of the probe or the
//! deleting duties, a `delete_prefix`; not an absent object, and not the
//! store's check, which sets the store's health instead) records
//! `BackupStoreErrors` under the call's name, as the job's calls do, so a
//! store that refuses one row's calls is counted though the cycle
//! completes; the worker stops at the shutdown token.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use extenddb_backup_store::{BackupStore, StoreError};
use extenddb_core::backup_format::{
    BackupArn, EXTENDDB_MANIFEST_FILE, ExtenddbManifest, MANIFEST_CHECKSUM_FILE,
    verify_manifest_checksum,
};
use extenddb_core::metrics::{MetricsCollector, QuerySource};
use extenddb_storage::backup_catalog::{
    BackupRow, BackupStatus, JobStatus, RestoreJobRow, RestoreKind, RestoreTargetKey, StaleClock,
    StatusFields, StoreIdentity,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupCatalog, CancellationToken, LegacyBackupReader, sleep_or_shutdown};
use futures::TryStreamExt;

use super::BackupRuntime;
use super::job::failure::{Written, write_status};

/// The two durations a reconciler reads from `[backup]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcilerSettings {
    /// `reconcile_interval_secs`.
    pub interval: Duration,
    /// `stale_job_after_secs`.
    pub stale_job_after: Duration,
}

/// The reason written on a restore job record whose owner died.
pub const OWNER_DEAD: &str = "owner dead: no heartbeat within stale_job_after_secs";

/// The reason written on a CREATING backup row whose owner died without a
/// complete manifest in the store.
pub const OWNER_DEAD_NO_MANIFEST: &str =
    "owner dead: no heartbeat within stale_job_after_secs; no complete manifest";

/// What one cycle did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CycleReport {
    /// Whether the maintenance claim was held during the cycle.
    pub maintenance_held: bool,
    /// Legacy restore targets failed and removed, by table name.
    pub legacy_restores_removed: Vec<String>,
    /// Targets of FAILED records removed (the FAILED-target duty), by table
    /// name.
    pub failed_targets_removed: Vec<String>,
    /// Targets a removal found no longer a CREATING restore target (whether
    /// this cycle wrote the FAILED or found it), left to their current owner,
    /// by table name.
    pub targets_left_to_owner: Vec<String>,
    /// Manifest restore jobs with a dead owner, left for their resumer.
    pub manifest_restores_seen: Vec<String>,
    /// Those of `manifest_restores_seen` this cycle logged: the ones no
    /// earlier cycle of this process had seen.
    pub manifest_restores_logged: Vec<String>,
    /// In-catalog CREATING backup rows removed, by ARN.
    pub legacy_backups_removed: Vec<String>,
    /// The configured store's health after this cycle's check; `None` with
    /// no store configured.
    pub store_healthy: Option<bool>,
    /// DELETING rows of the configured store moved to DELETED, by ARN.
    pub deletions_finished: Vec<String>,
    /// CREATING rows whose owner was dead, resolved to the status given, by
    /// ARN.
    pub creating_resolved: Vec<(String, BackupStatus)>,
    /// CREATING rows this cycle took over whose status write matched no row
    /// (another owner took them over first), left to that owner, by ARN.
    pub creating_left_to_owner: Vec<String>,
    /// FAILED rows past the stale window whose prefix was deleted, by ARN.
    pub failed_prefixes_deleted: Vec<String>,
    /// DELETED rows whose prefix still listed objects and was deleted, by
    /// ARN.
    pub deleted_prefixes_swept: Vec<String>,
    /// FAILED and DELETED rows past their retention with an empty prefix,
    /// removed, by ARN.
    pub tombstones_removed: Vec<String>,
    /// Backups whose restore registrations the backend swept, by ARN.
    pub registrations_swept: Vec<String>,
}

impl CycleReport {
    /// Whether the cycle changed a row or an object.
    #[must_use]
    pub fn acted(&self) -> bool {
        !(self.legacy_restores_removed.is_empty()
            && self.failed_targets_removed.is_empty()
            && self.legacy_backups_removed.is_empty()
            && self.deletions_finished.is_empty()
            && self.creating_resolved.is_empty()
            && self.failed_prefixes_deleted.is_empty()
            && self.deleted_prefixes_swept.is_empty()
            && self.tombstones_removed.is_empty()
            && self.registrations_swept.is_empty())
    }
}

/// What a reconciler carries from one cycle to the next: the manifest
/// records it has already logged, so a record it cannot act on is reported
/// once rather than every cycle (the caller of [`run`] owns it and hands the
/// same state to every worker it starts, so the set outlives a worker
/// restart: `serve` restarts its workers each time it takes the serving
/// registration again), the worker's shutdown token, which the
/// store's check is selected against, and, for a maintenance command's own
/// cycle, that its caller holds the maintenance claim.
#[derive(Debug, Default)]
pub struct ReconcilerState {
    logged_manifest_restores: HashSet<RestoreTargetKey>,
    shutdown: Option<CancellationToken>,
    /// `Some(true)` when the caller holds the maintenance claim: the cycle
    /// takes the held branch and does not read the claim. `None` reads it
    /// through `check_maintenance_claim`.
    maintenance_held: Option<bool>,
}

impl ReconcilerState {
    /// A state whose cycles stop waiting on the store's check when `shutdown`
    /// fires (the health is then left as the last check left it). A default
    /// state, which commands and tests use, waits the check's bound.
    #[must_use]
    pub fn with_shutdown(shutdown: CancellationToken) -> Self {
        Self {
            shutdown: Some(shutdown),
            ..Self::default()
        }
    }

    /// The state of a cycle that a maintenance command runs while it holds
    /// the maintenance claim: the cycle takes the held branch without
    /// reading the claim, whatever window it is given.
    #[must_use]
    pub fn for_maintenance_holder() -> Self {
        Self {
            maintenance_held: Some(true),
            ..Self::default()
        }
    }
}

/// Run cycles every `settings.interval` until `shutdown` fires. One cycle
/// runs at once at startup, so a restart resolves what the stopped process
/// left without waiting an interval. `backend` names the storage backend in
/// an identity file this worker has to rewrite. `state` is locked for each
/// cycle.
pub async fn run<S>(
    storage: Arc<S>,
    runtime: Arc<BackupRuntime>,
    settings: ReconcilerSettings,
    backend: String,
    metrics: Arc<MetricsCollector>,
    state: Arc<tokio::sync::Mutex<ReconcilerState>>,
    shutdown: CancellationToken,
) where
    S: BackupCatalog + LegacyBackupReader + ?Sized,
{
    loop {
        let started = std::time::Instant::now();
        let outcome = {
            let mut state = state.lock().await;
            // The state outlives this worker (it is `serve`'s); the token is
            // this worker's, so it is set here, each cycle.
            state.shutdown = Some(shutdown.clone());
            cycle(
                &*storage,
                &runtime,
                settings.stale_job_after,
                &backend,
                &mut state,
            )
            .await
        };
        match outcome {
            Ok(report) => {
                #[allow(clippy::cast_precision_loss)]
                let cycle_us = started.elapsed().as_micros() as f64;
                metrics.record_worker_success(QuerySource::BackupReconciler, cycle_us);
                if report.acted() {
                    tracing::info!(
                        "backup reconciler: failed and removed {} dead legacy restore(s), removed \
                         the target(s) of {} FAILED restore record(s) and {} abandoned in-catalog \
                         backup(s), finished {} deletion(s), resolved {} dead CREATING row(s), \
                         deleted {} FAILED prefix(es), swept {} DELETED prefix(es), removed {} \
                         tombstone(s), swept the registrations of {} backup(s)",
                        report.legacy_restores_removed.len(),
                        report.failed_targets_removed.len(),
                        report.legacy_backups_removed.len(),
                        report.deletions_finished.len(),
                        report.creating_resolved.len(),
                        report.failed_prefixes_deleted.len(),
                        report.deleted_prefixes_swept.len(),
                        report.tombstones_removed.len(),
                        report.registrations_swept.len()
                    );
                }
            }
            Err(e) => {
                metrics.record_worker_error(QuerySource::BackupReconciler);
                tracing::error!("backup reconciler cycle failed: {e}");
            }
        }
        if !sleep_or_shutdown(&shutdown, settings.interval).await {
            return;
        }
    }
}

/// One cycle: the maintenance check (or the caller's word that it holds
/// the claim, from `state`), then the duties. `stale_job_after` is the
/// dead-owner window; it is also the claim's stale window when the claim is
/// read.
///
/// # Errors
///
/// A catalog read that fails ends the cycle; a failed write on one row is
/// logged and the cycle goes on to the next row.
pub async fn cycle<S>(
    storage: &S,
    runtime: &BackupRuntime,
    stale_job_after: Duration,
    backend: &str,
    state: &mut ReconcilerState,
) -> Result<CycleReport, StorageError>
where
    S: BackupCatalog + LegacyBackupReader + ?Sized,
{
    let maintenance_held = match state.maintenance_held {
        Some(held) => held,
        None => storage.check_maintenance_claim(stale_job_after).await?,
    };
    let mut report = CycleReport {
        maintenance_held,
        ..CycleReport::default()
    };
    if report.maintenance_held {
        tracing::info!(
            "backup reconciler: a maintenance claim is held; running only the duties that \
             resolve rows whose owner is dead, the store's check and the DELETING rows"
        );
    }

    for job in storage.list_dead_restore_jobs(stale_job_after).await? {
        match (job.status, job.kind) {
            // The FAILED-target duty, whatever the record's kind: the job is
            // over and only its target is left.
            (JobStatus::Failed, _) => {
                match remove_restore_target(
                    storage,
                    &job,
                    "was FAILED when the reconciler reached it",
                )
                .await
                {
                    Removal::Removed => report
                        .failed_targets_removed
                        .push(job.target.table_name.clone()),
                    Removal::LeftToOwner => report
                        .targets_left_to_owner
                        .push(job.target.table_name.clone()),
                    Removal::Failed => {}
                }
            }
            (_, RestoreKind::Legacy) => match remove_dead_legacy_restore(storage, &job).await {
                Some(Removal::Removed) => report
                    .legacy_restores_removed
                    .push(job.target.table_name.clone()),
                Some(Removal::LeftToOwner) => report
                    .targets_left_to_owner
                    .push(job.target.table_name.clone()),
                Some(Removal::Failed) | None => {}
            },
            (_, RestoreKind::Manifest) => {
                if state.logged_manifest_restores.insert(job.target.clone()) {
                    tracing::info!(
                        "restore of {} into {} (account {}) has no live owner; a manifest \
                         restore is resumed by a later release's reconciler (reported once)",
                        job.source_backup_arn,
                        job.target.table_name,
                        job.target.account_id
                    );
                    report
                        .manifest_restores_logged
                        .push(job.target.table_name.clone());
                }
                report
                    .manifest_restores_seen
                    .push(job.target.table_name.clone());
            }
        }
    }

    for backup in storage.abandoned_creating_backups(stale_job_after).await? {
        if backup.removed {
            // The backend removed the row in the call that reported it
            // (SQLite: one fenced write under its lock).
            tracing::warn!(
                "removed in-catalog backup {} (account {}): its CreateBackup did not finish \
                 and no process owns it",
                backup.backup_arn,
                backup.account_id
            );
            report.legacy_backups_removed.push(backup.backup_arn);
            continue;
        }
        match storage.delete(&backup.account_id, &backup.backup_arn).await {
            Ok(_) => {
                tracing::warn!(
                    "removed in-catalog backup {} (account {}): its CreateBackup did not finish \
                     and no process owns it",
                    backup.backup_arn,
                    backup.account_id
                );
                report.legacy_backups_removed.push(backup.backup_arn);
            }
            Err(e) => tracing::error!(
                "could not remove the abandoned in-catalog backup {} (account {}): {e}",
                backup.backup_arn,
                backup.account_id
            ),
        }
    }

    if let Some(store) = runtime.configured_store() {
        let check = super::check_configured_store(store, storage, backend);
        let outcome = match &state.shutdown {
            Some(shutdown) => tokio::select! {
                outcome = check => Some(outcome),
                () = shutdown.cancelled() => None,
            },
            None => Some(check.await),
        };
        match outcome {
            Some(outcome) => {
                // The defaulted SQLite root's rule, applied to the first
                // check when the startup checks did not finish and left the
                // store checking: a failure with no rows for the root is the
                // warning the startup check would have given, and the store
                // is left unconfigured rather than unhealthy.
                if store.is_defaulted()
                    && store.health() == super::StoreHealth::Checking
                    && let Err(problem) = &outcome
                    && !matches!(problem, super::StoreProblem::Catalog(_))
                    && storage
                        .read_store_ids(store.kind, &store.root)
                        .await
                        .is_ok_and(|ids| ids.is_empty())
                {
                    tracing::warn!(
                        "backup store: the default root {} failed its first check ({problem}); \
                         the catalog has no backup rows for it, so the store is unconfigured and \
                         CreateBackup writes in-catalog backups. Fix the root (owned by this \
                         user, mode 0700, under directories others cannot write to, its \
                         extenddb-store.json naming this catalog), configure [backup.store] to \
                         choose another, or set kind = \"disabled\" to silence this",
                        store.root
                    );
                    store.set_aside();
                    return Ok(report);
                }
                store.record_check(outcome);
            }
            None => {
                tracing::info!(
                    "backup store ({}) check was stopped by shutdown; its health is left as the \
                     last check left it",
                    store.kind
                );
                report.store_healthy = Some(store.is_healthy());
                return Ok(report);
            }
        }
        report.store_healthy = Some(store.is_healthy());
        if let Some(identity) = store.identity().filter(|_| store.is_healthy()) {
            let calls = StoreCalls {
                store: &*store.store,
                metrics: runtime.metrics().map(|m| &**m),
            };
            report.deletions_finished = finish_deleting_rows(storage, store, &identity).await?;
            (report.creating_resolved, report.creating_left_to_owner) =
                resolve_dead_creating_rows(storage, calls, &identity, stale_job_after).await?;
            if !report.maintenance_held {
                let retention = runtime.job_settings().failed_row_retention;
                report.failed_prefixes_deleted =
                    delete_stale_failed_prefixes(storage, calls, &identity, stale_job_after)
                        .await?;
                report.deleted_prefixes_swept =
                    sweep_deleted_prefixes(storage, calls, &identity).await?;
                report.tombstones_removed =
                    remove_tombstones(storage, calls, &identity, retention).await?;
            }
        }
    }

    if !report.maintenance_held {
        report.registrations_swept = storage.sweep_restore_registrations().await?;
    }
    Ok(report)
}

/// The duty for CREATING rows of the configured store whose owner is dead
/// (module doc). Returns the rows resolved with their new status and the
/// rows whose write matched no row, left to their new owner; a row the
/// backend does not hand over is skipped, a row whose probe or write fails
/// is logged and left for the next cycle.
async fn resolve_dead_creating_rows<S: BackupCatalog + ?Sized>(
    storage: &S,
    store: StoreCalls<'_>,
    identity: &StoreIdentity,
    stale_job_after: Duration,
) -> Result<(Vec<(String, BackupStatus)>, Vec<String>), StorageError> {
    let rows: Vec<BackupRow> = storage
        .list_backup_rows(Some(BackupStatus::Creating), identity)
        .try_collect()
        .await?;
    let mut resolved = Vec::new();
    let mut left_to_owner = Vec::new();
    for row in rows {
        let claim = match storage.claim_dead_backup(&row.key(), stale_job_after).await {
            Ok(Some(claim)) => claim,
            Ok(None) => continue,
            Err(e) => {
                tracing::error!(
                    "could not test whether the owner of CREATING backup {} (account {}) is \
                     dead; the next cycle retries: {e}",
                    row.backup_arn,
                    row.account_id
                );
                continue;
            }
        };
        let arn = match BackupArn::parse(&row.backup_arn) {
            Ok(arn) => arn,
            Err(e) => {
                tracing::error!(
                    "CREATING row {} carries an ARN this build cannot parse; left as it is: {e}",
                    row.backup_arn
                );
                continue;
            }
        };
        let (to, fields) = match probe_manifest(store, &arn).await {
            Ok(Probe::Complete(fields)) => (JobStatus::Available, fields),
            Ok(Probe::Incomplete(why)) => {
                tracing::warn!(
                    "backup {} (account {}): its owner is dead and the store holds no complete \
                     manifest ({why}); the row is FAILED and its prefix is deleted once the row's \
                     heartbeat is stale",
                    row.backup_arn,
                    row.account_id
                );
                (
                    JobStatus::Failed,
                    StatusFields {
                        failure_reason: Some(OWNER_DEAD_NO_MANIFEST.to_owned()),
                        completed: true,
                        ..StatusFields::default()
                    },
                )
            }
            Err(e) => {
                tracing::error!(
                    "backup {} (account {}): its owner is dead and the store could not be \
                     probed for its manifest; the next cycle retries: {e}",
                    row.backup_arn,
                    row.account_id
                );
                continue;
            }
        };
        match write_status(&*claim, to, &fields).await {
            Written::Matched => {
                tracing::info!(
                    "backup {} (account {}): its owner was dead; the row is now {to}",
                    row.backup_arn,
                    row.account_id
                );
                let status = match to {
                    JobStatus::Available => BackupStatus::Available,
                    _ => BackupStatus::Failed,
                };
                resolved.push((row.backup_arn, status));
            }
            Written::NotMatched(why) => {
                tracing::info!(
                    "backup {} was no longer this cycle's to resolve ({why}); left to its owner",
                    row.backup_arn
                );
                left_to_owner.push(row.backup_arn);
            }
            Written::Error(e) => tracing::error!(
                "backup {} (account {}): the {to} write failed; the next cycle retries: {e}",
                row.backup_arn,
                row.account_id
            ),
        }
        drop(claim);
    }
    Ok((resolved, left_to_owner))
}

/// The configured store as the reconciler's duties call it: each failed
/// call (not an absent object) records `BackupStoreErrors` under the call's
/// name (`get`, `list` or `delete_prefix`), as the job records its own, so
/// a store that refuses the reconciler's calls moves the metric even when
/// every cycle completes.
#[derive(Clone, Copy)]
struct StoreCalls<'a> {
    store: &'a dyn BackupStore,
    metrics: Option<&'a MetricsCollector>,
}

impl StoreCalls<'_> {
    fn failed(self, op: &str, e: StoreError) -> StoreError {
        if let Some(metrics) = self.metrics {
            metrics.record_backup_store_error(op);
        }
        e
    }

    /// One object's bytes, `None` when absent.
    async fn read_object(self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let body = match self.store.get(key).await {
            Ok(body) => body,
            Err(StoreError::NotFound) => return Ok(None),
            Err(e) => return Err(self.failed("get", e)),
        };
        let bytes = body
            .try_fold(Vec::new(), |mut acc, chunk| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            })
            .await
            .map_err(|e| self.failed("get", e))?;
        Ok(Some(bytes))
    }

    /// The prefix's objects, key to size.
    async fn list_sizes(
        self,
        prefix: &str,
    ) -> Result<std::collections::HashMap<String, u64>, StoreError> {
        self.store
            .list(prefix)
            .map_ok(|meta| (meta.key, meta.size))
            .try_collect()
            .await
            .map_err(|e| self.failed("list", e))
    }

    /// Whether the prefix lists at least one object.
    async fn prefix_has_objects(self, prefix: &str) -> Result<bool, StoreError> {
        match self.store.list(prefix).try_next().await {
            Ok(first) => Ok(first.is_some()),
            Err(e) => Err(self.failed("list", e)),
        }
    }

    async fn delete_prefix(self, prefix: &str) -> Result<u64, StoreError> {
        self.store
            .delete_prefix(prefix)
            .await
            .map_err(|e| self.failed("delete_prefix", e))
    }
}

/// What the manifest probe found under a dead owner's prefix.
enum Probe {
    /// A complete, verified manifest: the AVAILABLE write's fields.
    Complete(StatusFields),
    /// No manifest, or one the reader refuses, or a data file missing or of
    /// another size; the text says which.
    Incomplete(String),
}

/// The manifest probe of the dead-owner duty (module doc). A store error
/// other than an absent object is the `Err`.
async fn probe_manifest(store: StoreCalls<'_>, arn: &BackupArn) -> Result<Probe, StoreError> {
    let prefix = arn.store_prefix();
    let manifest = match store
        .read_object(&format!("{prefix}{EXTENDDB_MANIFEST_FILE}"))
        .await?
    {
        Some(bytes) => bytes,
        None => return Ok(Probe::Incomplete("no manifest".to_owned())),
    };
    let sidecar = match store
        .read_object(&format!("{prefix}{MANIFEST_CHECKSUM_FILE}"))
        .await?
    {
        Some(bytes) => bytes,
        None => {
            return Ok(Probe::Incomplete(
                "a manifest without its sidecar".to_owned(),
            ));
        }
    };
    if let Err(e) = verify_manifest_checksum(&manifest, &sidecar) {
        return Ok(Probe::Incomplete(format!(
            "the sidecar does not match: {e}"
        )));
    }
    let manifest = match ExtenddbManifest::from_json_bytes(&manifest) {
        Ok(manifest) => manifest,
        Err(e) => return Ok(Probe::Incomplete(format!("the manifest is refused: {e}"))),
    };
    match manifest.backup_arn() {
        Ok(found) if found == *arn => {}
        Ok(found) => {
            return Ok(Probe::Incomplete(format!(
                "the manifest names {}, not the row's backup",
                found.to_arn_string()
            )));
        }
        Err(e) => {
            return Ok(Probe::Incomplete(format!(
                "the manifest's ARN is refused: {e}"
            )));
        }
    }
    let listed = store.list_sizes(&prefix).await?;
    for file in &manifest.data_files {
        match listed.get(&format!("{prefix}{}", file.key)) {
            Some(size) if *size == file.bytes => {}
            Some(size) => {
                return Ok(Probe::Incomplete(format!(
                    "data file {} has {size} bytes in the store and {} in the manifest",
                    file.key, file.bytes
                )));
            }
            None => {
                return Ok(Probe::Incomplete(format!(
                    "data file {} is not in the store",
                    file.key
                )));
            }
        }
    }
    let item_count = manifest
        .data_files
        .iter()
        .map(|f| f.item_count)
        .sum::<u64>();
    Ok(Probe::Complete(StatusFields {
        size_bytes: Some(manifest.backup_size_bytes),
        item_count: Some(i64::try_from(item_count).unwrap_or(i64::MAX)),
        failure_reason: None,
        source_table_details_json: serde_json::to_value(&manifest.source_table).ok(),
        completed: true,
    }))
}

/// The FAILED-prefix duty (module doc): FAILED rows whose heartbeat is older
/// than `stale_job_after`, their prefix deleted while it lists objects.
async fn delete_stale_failed_prefixes<S: BackupCatalog + ?Sized>(
    storage: &S,
    store: StoreCalls<'_>,
    identity: &StoreIdentity,
    stale_job_after: Duration,
) -> Result<Vec<String>, StorageError> {
    let rows: Vec<BackupRow> = storage
        .list_backup_rows_older_than(
            BackupStatus::Failed,
            identity,
            StaleClock::Heartbeat,
            stale_job_after,
        )
        .try_collect()
        .await?;
    let mut deleted = Vec::new();
    for row in rows {
        if delete_prefix_if_listed(store, &row, "FAILED").await {
            deleted.push(row.backup_arn);
        }
    }
    Ok(deleted)
}

/// The DELETED-prefix duty (module doc): DELETED rows whose prefix still
/// lists objects, the prefix deleted.
async fn sweep_deleted_prefixes<S: BackupCatalog + ?Sized>(
    storage: &S,
    store: StoreCalls<'_>,
    identity: &StoreIdentity,
) -> Result<Vec<String>, StorageError> {
    let rows: Vec<BackupRow> = storage
        .list_backup_rows(Some(BackupStatus::Deleted), identity)
        .try_collect()
        .await?;
    let mut swept = Vec::new();
    for row in rows {
        if delete_prefix_if_listed(store, &row, "DELETED").await {
            swept.push(row.backup_arn);
        }
    }
    Ok(swept)
}

/// Delete `row`'s prefix when it lists objects; returns whether a deletion
/// ran. A store failure is logged and left for the next cycle.
async fn delete_prefix_if_listed(store: StoreCalls<'_>, row: &BackupRow, status: &str) -> bool {
    let prefix = match BackupArn::parse(&row.backup_arn) {
        Ok(arn) => arn.store_prefix(),
        Err(e) => {
            tracing::error!(
                "{status} row {} carries an ARN this build cannot parse; left as it is: {e}",
                row.backup_arn
            );
            return false;
        }
    };
    match store.prefix_has_objects(&prefix).await {
        Ok(true) => {}
        Ok(false) => return false,
        Err(e) => {
            tracing::error!(
                "could not list the prefix of {status} backup {}; the next cycle retries: {e}",
                row.backup_arn
            );
            return false;
        }
    }
    match store.delete_prefix(&prefix).await {
        Ok(n) => {
            tracing::info!(
                "deleted {n} object(s) left under the prefix of {status} backup {} (account {})",
                row.backup_arn,
                row.account_id
            );
            true
        }
        Err(e) => {
            tracing::error!(
                "could not delete the prefix of {status} backup {} (account {}); the next cycle \
                 retries: {e}",
                row.backup_arn,
                row.account_id
            );
            false
        }
    }
}

/// The tombstone duty (module doc): FAILED and DELETED rows whose
/// `completed_at` is older than `retention` and whose prefix lists nothing
/// are removed.
async fn remove_tombstones<S: BackupCatalog + ?Sized>(
    storage: &S,
    store: StoreCalls<'_>,
    identity: &StoreIdentity,
    retention: Duration,
) -> Result<Vec<String>, StorageError> {
    let mut removed = Vec::new();
    for status in [BackupStatus::Failed, BackupStatus::Deleted] {
        let rows: Vec<BackupRow> = storage
            .list_backup_rows_older_than(status, identity, StaleClock::Completed, retention)
            .try_collect()
            .await?;
        for row in rows {
            let prefix = match BackupArn::parse(&row.backup_arn) {
                Ok(arn) => arn.store_prefix(),
                Err(e) => {
                    tracing::error!(
                        "{status} row {} carries an ARN this build cannot parse; left as it \
                         is: {e}",
                        row.backup_arn
                    );
                    continue;
                }
            };
            match store.prefix_has_objects(&prefix).await {
                Ok(false) => {}
                Ok(true) => continue,
                Err(e) => {
                    tracing::error!(
                        "could not list the prefix of {status} backup {} before removing its \
                         row; the next cycle retries: {e}",
                        row.backup_arn
                    );
                    continue;
                }
            }
            match storage.remove_backup_row(&row.key()).await {
                Ok(true) => {
                    tracing::info!(
                        "removed the {status} row of backup {} (account {}): its prefix is empty \
                         and its retention has passed",
                        row.backup_arn,
                        row.account_id
                    );
                    removed.push(row.backup_arn);
                }
                Ok(false) => {}
                Err(e) => tracing::error!(
                    "could not remove the {status} row of backup {} (account {}); the next cycle \
                     retries: {e}",
                    row.backup_arn,
                    row.account_id
                ),
            }
        }
    }
    Ok(removed)
}

/// The duty for DELETING rows of the configured store: finish each deletion.
/// Returns the ARNs moved to DELETED; a row whose deletion fails is logged
/// and left for the next cycle.
async fn finish_deleting_rows<S: BackupCatalog + ?Sized>(
    storage: &S,
    store: &super::ConfiguredStore,
    identity: &extenddb_storage::backup_catalog::StoreIdentity,
) -> Result<Vec<String>, StorageError> {
    let rows: Vec<_> = storage
        .list_backup_rows(Some(BackupStatus::Deleting), identity)
        .try_collect()
        .await?;
    let mut finished = Vec::new();
    for row in rows {
        let arn = match BackupArn::parse(&row.backup_arn) {
            Ok(arn) => arn,
            Err(e) => {
                tracing::error!(
                    "DELETING row {} carries an ARN this build cannot parse; left as it is: {e}",
                    row.backup_arn
                );
                continue;
            }
        };
        match super::finish_backup_delete(storage, &store.calls("deletion"), &arn).await {
            Ok(true) => {
                tracing::info!(
                    "finished the deletion of backup {} (account {})",
                    row.backup_arn,
                    row.account_id
                );
                finished.push(row.backup_arn);
            }
            Ok(false) => tracing::info!(
                "backup {} was no longer DELETING when its objects were gone; left to its owner",
                row.backup_arn
            ),
            Err(e) => tracing::error!(
                "could not finish the deletion of backup {} (account {}); the next cycle \
                 retries: {e}",
                row.backup_arn,
                row.account_id
            ),
        }
    }
    Ok(finished)
}

/// Mark a dead legacy restore FAILED and remove its target. The FAILED write
/// is conditioned on the `owner_generation` and `heartbeat_at` the listing
/// read (design 4(e)), so a heartbeat that landed after the listing, which
/// says the owner is alive after all, makes it match nothing. `None` when the
/// FAILED write did not match (the record is no longer RUNNING under the
/// listed claim and belongs to whoever holds it) or errored; otherwise the
/// removal's outcome.
async fn remove_dead_legacy_restore<S: BackupCatalog + ?Sized>(
    storage: &S,
    job: &RestoreJobRow,
) -> Option<Removal> {
    let target = &job.target;
    match storage
        .fail_restore_job(target, job.owner_generation, job.heartbeat_at, OWNER_DEAD)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            tracing::info!(
                "restore into {} (account {}) was no longer RUNNING under the listed claim \
                 when the reconciler reached it (its owner heartbeated or moved it); left to \
                 its owner",
                target.table_name,
                target.account_id
            );
            return None;
        }
        Err(e) => {
            tracing::error!(
                "could not mark the abandoned restore into {} (account {}) FAILED: {e}",
                target.table_name,
                target.account_id
            );
            return None;
        }
    }
    Some(remove_restore_target(storage, job, "was marked FAILED by the reconciler").await)
}

/// What [`remove_restore_target`] found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Removal {
    /// The CREATING target was removed.
    Removed,
    /// The target was no longer a CREATING restore target with this id; it
    /// belongs to whoever moved it and the record is left as it is.
    LeftToOwner,
    /// The removal errored; the FAILED record and the target wait for a
    /// later cycle.
    Failed,
}

/// Remove the CREATING target of a record that is FAILED (`how` says, for
/// the log, whether this cycle wrote the FAILED or found it). A target that
/// is no longer a CREATING restore target belongs to whoever moved it; a
/// removal that errors leaves the FAILED record and the target for a later
/// cycle, which lists the pair again once the record is stale.
async fn remove_restore_target<S: BackupCatalog + ?Sized>(
    storage: &S,
    job: &RestoreJobRow,
    how: &str,
) -> Removal {
    let target = &job.target;
    match storage.remove_restore_target(target).await {
        Ok(true) => {
            tracing::warn!(
                "removed abandoned restore target {} ({}) in account {}, restoring from {}: \
                 its restore did not finish and no process owns it",
                target.table_name,
                target.table_id,
                target.account_id,
                job.source_backup_arn
            );
            Removal::Removed
        }
        Ok(false) => {
            tracing::warn!(
                "the abandoned restore into {} (account {}) {how} but its target ({}) was no \
                 longer a CREATING restore target; left to its current owner",
                target.table_name,
                target.account_id,
                target.table_id
            );
            Removal::LeftToOwner
        }
        Err(e) => {
            tracing::error!(
                "the abandoned restore into {} (account {}) {how} but its target ({}) could \
                 not be removed yet; a later cycle retries once the FAILED record is older \
                 than stale_job_after_secs: {e}",
                target.table_name,
                target.account_id,
                target.table_id
            );
            Removal::Failed
        }
    }
}
