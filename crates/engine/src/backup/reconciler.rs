// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The reconciler: the periodic worker that resolves catalog rows whose
//! owner is dead.
//!
//! One cycle runs every `interval`. It first checks the catalog-level
//! maintenance claim; while a maintenance command holds it the reconciler
//! runs only the duties that resolve the states the command waits on, which
//! today are every duty it has, so the check is what decides when the other
//! duties are added. The duties, over the records `list_dead_restore_jobs`
//! returns:
//!
//! - A RUNNING restore job record whose `heartbeat_at` is older than
//!   `stale_job_after` (on the catalog's clock) has a dead owner. One of kind
//!   `legacy` is marked FAILED and its target removed through
//!   `remove_restore_target`, never resumed: an in-catalog copy has no data
//!   files or progress to resume from. One of kind `manifest` is left for the
//!   duty that resumes it, and logged once per record for as long as this
//!   process runs.
//! - A FAILED restore job record whose table is still a CREATING restore
//!   target, the FAILED-target duty: the FAILED write landed (the job's own,
//!   or an earlier cycle's) and the target's removal did not (an error
//!   between the two statements, or the process stopped between them). The
//!   backend lists it once the record is stale, and the target is removed
//!   through `remove_restore_target` with no second FAILED write.
//! - An in-catalog CREATING backup row whose create is dead (reported by the
//!   backend's `abandoned_creating_backups`) is removed through the reader's
//!   `delete`, which takes its items with it.
//!
//! A cycle that fails logs and waits for the next; each cycle records the
//! worker metrics the other workers record (`WorkerLastSuccess` and
//! `WorkerCycleLatency` on success, `WorkerErrorCount` on failure, under
//! `QuerySource::BackupReconciler`); the worker stops at the shutdown token.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use extenddb_core::metrics::{MetricsCollector, QuerySource};
use extenddb_storage::backup_catalog::{JobStatus, RestoreJobRow, RestoreKind, RestoreTargetKey};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupCatalog, CancellationToken, LegacyBackupReader, sleep_or_shutdown};

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
}

/// What a reconciler carries from one cycle to the next: the manifest
/// records it has already logged, so a record it cannot act on is reported
/// once rather than every cycle.
#[derive(Debug, Default)]
pub struct ReconcilerState {
    logged_manifest_restores: HashSet<RestoreTargetKey>,
}

/// Run cycles every `settings.interval` until `shutdown` fires. One cycle
/// runs at once at startup, so a restart resolves what the stopped process
/// left without waiting an interval.
pub async fn run<S>(
    storage: Arc<S>,
    settings: ReconcilerSettings,
    metrics: Arc<MetricsCollector>,
    shutdown: CancellationToken,
) where
    S: BackupCatalog + LegacyBackupReader + ?Sized,
{
    let mut state = ReconcilerState::default();
    loop {
        let started = std::time::Instant::now();
        match cycle(&*storage, settings.stale_job_after, &mut state).await {
            Ok(report) => {
                #[allow(clippy::cast_precision_loss)]
                let cycle_us = started.elapsed().as_micros() as f64;
                metrics.record_worker_success(QuerySource::BackupReconciler, cycle_us);
                if !report.legacy_restores_removed.is_empty()
                    || !report.failed_targets_removed.is_empty()
                    || !report.legacy_backups_removed.is_empty()
                {
                    tracing::info!(
                        "backup reconciler: removed {} abandoned restore target(s) and {} \
                         abandoned in-catalog backup(s)",
                        report.legacy_restores_removed.len() + report.failed_targets_removed.len(),
                        report.legacy_backups_removed.len()
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

/// One cycle: the maintenance check, then the duties.
///
/// # Errors
///
/// A catalog read that fails ends the cycle; a failed write on one row is
/// logged and the cycle goes on to the next row.
pub async fn cycle<S>(
    storage: &S,
    stale_job_after: Duration,
    state: &mut ReconcilerState,
) -> Result<CycleReport, StorageError>
where
    S: BackupCatalog + LegacyBackupReader + ?Sized,
{
    let mut report = CycleReport {
        maintenance_held: storage.check_maintenance_claim().await?,
        ..CycleReport::default()
    };
    if report.maintenance_held {
        tracing::info!(
            "backup reconciler: a maintenance claim is held; running only the duties that \
             resolve rows whose owner is dead"
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
    Ok(report)
}

/// Mark a dead legacy restore FAILED and remove its target. `None` when the
/// FAILED write did not match (the record is no longer RUNNING and belongs to
/// whoever moved it) or errored; otherwise the removal's outcome.
async fn remove_dead_legacy_restore<S: BackupCatalog + ?Sized>(
    storage: &S,
    job: &RestoreJobRow,
) -> Option<Removal> {
    let target = &job.target;
    match storage.fail_restore_job(target, OWNER_DEAD).await {
        Ok(true) => {}
        Ok(false) => {
            tracing::info!(
                "restore into {} (account {}) was no longer RUNNING when the reconciler \
                 reached it; left to its owner",
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
