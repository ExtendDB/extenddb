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
//! - With a store configured: the store's check runs again
//!   ([`super::check_configured_store`]: `validate` while it has not passed,
//!   then the identity check and manifest probe) and sets the store's
//!   health, so a store whose checks run after the listener is up, or that
//!   failed at startup or later, is healthy once a check passes, and one
//!   that fails is refused from the rest of this cycle on: every duty below
//!   that reads the store's health reads the health this cycle's check left.
//!   The check runs under the store's bound
//!   ([`super::ConfiguredStore::check_timeout`]: 5 s for a filesystem root,
//!   30 s for a bucket), so a store that has stopped answering costs each
//!   cycle that bound and nothing more: the duties run every `interval` plus
//!   the bound, the check is recorded as a transport failure, and a
//!   filesystem check leaves one blocking thread parked on the mount until
//!   it answers, after which no check starts until that call returns (the
//!   runtime module's in-flight rule), so a mount that stays stalled parks
//!   one thread and not one per cycle. The check is also selected against
//!   the shutdown token, so a cycle that is checking when the server stops
//!   returns at once with no further duty and the drain finishes.
//! - With the startup sync of an in-memory catalog pending and the store
//!   healthy after the check: the sync (`super::sync::startup_sync`),
//!   bounded by `super::sync::STARTUP_SYNC_TIMEOUT` (60 s) and selected
//!   against the shutdown token, so a store that stalls during it delays
//!   the duties below by at most the bound; one that stops before it has
//!   listed the whole root, reaches the bound, or skips a prefix for a store
//!   or catalog error stays pending for the next cycle.
//! - Over the records `list_dead_restore_jobs` returns: a RUNNING restore
//!   job record whose `heartbeat_at` is older than `stale_job_after` (on
//!   the catalog's clock) has a dead owner. One of kind `legacy` is marked
//!   FAILED (`fail_restore_job`, conditioned on the `owner_generation` and
//!   `heartbeat_at` the listing read, so a heartbeat that lands after the
//!   listing keeps the record its owner's) and its target removed through
//!   `remove_restore_target`, never resumed: an in-catalog copy has no data
//!   files or progress to resume from. One of kind `manifest` is resumed (`super::restore::resume`),
//!   in this order: once the server's shutdown has begun (the jobs' token is
//!   cancelled) the record is left as it is, with no takeover and no FAILED
//!   write, for the next instance's reconciler (a resume spawned on the way
//!   out would stop at its first check and leave the record with a fresh
//!   heartbeat, a second stale window for the next instance to wait); once
//!   this worker's own token has fired with the jobs' token intact (the
//!   server stopping its workers while it holds no serving registration) it
//!   is left the same way, for the next set's reconciler, since a resume is
//!   a job and the workers' stop would not stop it;
//!   while the maintenance claim is held it is failed and its target
//!   removed like a legacy one ([`OWNER_DEAD`]; a resume is a job, which
//!   the claim excludes), except a record with no recorded progress at all
//!   (no `file_index`, no progress row) whose target already holds items,
//!   which is failed [`super::restore::TARGET_NOT_EMPTY`] through the same
//!   unclaimed write and its target left for the operator, as below (the
//!   command's wait is satisfied either way: the record is no longer
//!   RUNNING); a record of a store other than the
//!   configured one (no store configured included) is failed with
//!   [`OTHER_STORE`] (configure that store, or copy its tree under the
//!   configured root and move the backup's rows to the configured store
//!   with `extenddb backup sync --move-store`, and
//!   restore again: the move re-points backup rows, never a restore
//!   record), its target removed by the FAILED-target duty
//!   next; while the configured store is unhealthy, or not yet checked,
//!   the record is skipped (one line per cycle, naming which); with no free
//!   job permit it is skipped to the next cycle; then the backend's
//!   `claim_restore_dead` is the test and the takeover (a fresh generation,
//!   this host's `spool_host` and a fresh `heartbeat_at`, so a second taker
//!   finds the record live at once), and a record it does not hand over has
//!   a live owner; the instant the takeover returned is the resumed job's
//!   first confirmed beat (the record's `heartbeat_at` was stamped inside
//!   the write, so the instant is at or after the beat it stands for by the
//!   write's own duration, and the reads that follow count against the
//!   heartbeat's window rather than widen it); the takeover after this
//!   instance's fifth of one record ([`RESTORE_TAKEOVERS`], counted in
//!   [`ReconcilerState`]) fails the record [`TAKEOVERS_EXHAUSTED`] and
//!   removes its target, since a record whose every resume ends RUNNING
//!   again (a read or the count that fails every time) would otherwise be
//!   retaken every stale window; under the claim, a record with no recorded progress at all
//!   (no `file_index`, no progress row) whose target already holds more
//!   items than the backup's row counts is FAILED
//!   [`super::restore::TARGET_NOT_EMPTY`] and its target left for the
//!   operator (`extenddb backup remove-target --account <account> <table>`),
//!   since a catalog restored
//!   from a dump can name a restore that finished and was written to since;
//!   a target holding no more than the total is a first data file applied
//!   in part before its progress was recorded (Cassandra), and the resume
//!   re-applies that file with upserts (`exceeds_backup_total`);
//!   otherwise the job is spawned through the runtime's tracker and runs
//!   from the recorded progress. A FAILED restore job record whose table is
//!   still a CREATING restore target, the FAILED-target duty: the FAILED
//!   write landed (the job's own, or an earlier cycle's) and the target's
//!   removal did not; the backend lists it once the record is stale, and the
//!   target is removed through `remove_restore_target` with no second
//!   FAILED write, except a record whose reason is `target_not_empty`,
//!   whose target is the operator's and is logged with the command.
//! - An in-catalog CREATING backup row whose create is dead (reported by the
//!   backend's `abandoned_creating_backups`) is removed through the reader's
//!   `delete`, which takes its items with it, unless the backend removed it
//!   in the call that reported it (`AbandonedLegacyBackup::removed`), when
//!   the reconciler only logs it.
//! - With a store configured: while the store is unhealthy the reconciler
//!   acts on no row of the store. While it is healthy, on the rows of the
//!   configured store:
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
//!   - Each deletion of a prefix and each removal of a tombstone, the
//!     DELETING rows' included, runs the deletion fence first
//!     ([`super::StoreFence`]): `extenddb-store.json` is re-read, and a
//!     file that names another catalog (a root adopted since this cycle's
//!     check) marks the store unhealthy with nothing deleted. The cycle
//!     re-tests the store's health after each store duty, so the duties
//!     after the refusal (the dead-owner duty's reads and writes among
//!     them) do not run on the root; the refusing duty's later rows are
//!     refused with no read of the file (the FAILED-prefix and
//!     DELETED-prefix duties still list each prefix first). The FAILED,
//!     DELETED and tombstone duties then read the row once more after the
//!     fence passed, skip a row that is gone or whose status or store
//!     changed since the duty listed it, and read the maintenance claim
//!     again, starting nothing while a command holds it. The claim is read
//!     once at the cycle's start, so a cycle that began just before a
//!     maintenance command took it may still be running its duties when
//!     the command removes a row (`backup sync --forget-backup`,
//!     `--forget-store`) or moves it (`--move-store`). A deletion or
//!     removal that read the claim free must end by its deadline,
//!     [`DUTY_DELETION_BOUND`] after that read was sent: a read answered
//!     after the deadline starts nothing, the prefix deletion checks the
//!     deadline inside the store before each removal (each request on S3,
//!     `delete_prefix_before`), and a call still running at it is dropped;
//!     the duty then stops for this cycle, the row left for the next. A
//!     command that removes FAILED or DELETED rows beside servers waits the
//!     bound and one store call after it takes the claim, so what a
//!     deletion duty can still do to a row such a command removes is the
//!     step in flight at the deadline: one removal on a filesystem root, one
//!     request on S3 (up to 1,000 keys for a `DeleteObjects`). The bound,
//!     like every lease in the design, assumes the server's process keeps
//!     running: one paused past the wait (SIGSTOP, a frozen cgroup, a VM
//!     pause) between its last check of the deadline and a call it has
//!     already sent runs that call when it resumes. The exception is a
//!     DELETING row of the configured store: its deletion is finished
//!     before its removal (by the command, or by `finish_deleting_rows`,
//!     which runs under the claim), the deletion its DeleteBackup asked
//!     for.
//! - Not while the maintenance claim is held, on every backend: the
//!   backend's `sweep_restore_registrations` (a no-op but on Cassandra);
//!   the progress-row sweep (`sweep_restore_progress` with the table ids of
//!   the RUNNING restore targets: PostgreSQL's `restore_progress` rows of
//!   any other table go, the other backends keep progress on the record and
//!   have nothing to sweep); and, with a spool configured, the spool sweep
//!   (`super::restore::spool::sweep_entries`: entries whose table id is
//!   not a RUNNING restore with this host's `spool_host`). Both sweeps read
//!   their candidates before the records: the progress-row sweep's
//!   candidates are the backend's, the spool's are listed
//!   (`super::restore::spool::list_entries`) before `list_restore_jobs`,
//!   so a restore whose registration commits between the two reads is
//!   never a candidate of either. A backend that cannot list the records
//!   yet logs once per cycle and the two sweeps are skipped.
//!
//! The worker runs in the server's restartable worker set: while the server
//! holds no serving registration (PostgreSQL, MongoDB and Cassandra) it is
//! stopped with the other workers and a new one started once the server
//! registers again, as `extenddb_server`'s `registration` module doc says;
//! a cycle that is running at the stop finishes or is aborted, and spawns
//! no resume after its token fired (above). Each start builds a fresh
//! [`ReconcilerState`], and its first cycle runs at once; no duty depends
//! on what an earlier set's cycles did. The backup and restore jobs,
//! resumes included, run on the jobs' token, a child of the server's
//! shutdown token, so the workers' stop does not stop them: a job that is
//! running when the registration is lost goes on writing the catalog and
//! the store until it ends or the server stops, beside a migration that
//! may already have taken the catalog (GitHub issue #755 is the gate).
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
    BackupRow, BackupStatus, JobStatus, RestoreJobRow, RestoreKind, StaleClock, StatusFields,
    StoreIdentity,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BackupCatalog, CancellationToken, JobPermit, LegacyBackupReader, RestoreEngine,
    sleep_or_shutdown,
};
use futures::TryStreamExt;

use super::job::JobEnv;
use super::job::failure::{Written, write_status};
use super::restore::{self, OTHER_STORE, TARGET_NOT_EMPTY};
use super::{BackupRuntime, ConfiguredStore, StoreFence};

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

/// How many times one instance resumes a dead store-backed restore record
/// before it fails the record instead: a resume that keeps ending with the
/// record RUNNING (a progress read or the final count that fails every time,
/// a load that stops `LeftRunning`) would otherwise be retaken every stale
/// window for as long as the server runs. The count is this instance's,
/// kept in [`ReconcilerState`]; a restart starts it over.
pub const RESTORE_TAKEOVERS: u32 = 5;

/// The reason written on a RUNNING store-backed restore record that this
/// instance took over [`RESTORE_TAKEOVERS`] times without the restore
/// ending; the target is removed as for [`OWNER_DEAD`].
pub const TAKEOVERS_EXHAUSTED: &str =
    "owner dead: taken over 5 times by one instance without the restore ending";

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
    /// Manifest restore jobs with a dead owner this cycle took over and
    /// spawned a resume of, by table name.
    pub manifest_restores_resumed: Vec<String>,
    /// Manifest restore jobs with a dead owner this cycle marked FAILED and
    /// left their target to the FAILED-target duty or the operator, by table
    /// name with the reason: another store, or `target_not_empty`.
    pub manifest_restores_failed: Vec<(String, String)>,
    /// Manifest restore jobs with a dead owner this cycle marked FAILED
    /// `OWNER_DEAD` under the maintenance claim and removed the target of,
    /// as a legacy record is, by table name.
    pub manifest_restores_failed_and_removed: Vec<String>,
    /// Manifest restore jobs with a dead owner skipped to the next cycle:
    /// no job permit was free, by table name.
    pub restores_skipped_no_permit: Vec<String>,
    /// Manifest restore jobs with a dead owner skipped because the
    /// configured store is unhealthy, by table name.
    pub restores_skipped_store_unhealthy: Vec<String>,
    /// FAILED `target_not_empty` records whose target is left for the
    /// operator, by table name.
    pub targets_left_for_operator: Vec<String>,
    /// Progress rows of tables that are not RUNNING restore targets removed
    /// (PostgreSQL's `restore_progress`).
    pub progress_rows_swept: u64,
    /// Spool entries of restores not RUNNING with this host's `spool_host`
    /// removed, by table id.
    pub spool_entries_swept: Vec<String>,
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
            && self.manifest_restores_resumed.is_empty()
            && self.manifest_restores_failed.is_empty()
            && self.manifest_restores_failed_and_removed.is_empty()
            && self.progress_rows_swept == 0
            && self.spool_entries_swept.is_empty()
            && self.legacy_backups_removed.is_empty()
            && self.deletions_finished.is_empty()
            && self.creating_resolved.is_empty()
            && self.failed_prefixes_deleted.is_empty()
            && self.deleted_prefixes_swept.is_empty()
            && self.tombstones_removed.is_empty()
            && self.registrations_swept.is_empty())
    }
}

/// What a reconciler carries from one cycle to the next: the worker's
/// shutdown token, which the store's check is selected against (the caller
/// of [`run`] owns the state and hands the same one to every worker it
/// starts, `serve` restarting its workers each time it takes the serving
/// registration again, and the token is set each cycle), and, for a
/// maintenance command's own cycle, that its caller holds the maintenance
/// claim.
#[derive(Debug, Default)]
pub struct ReconcilerState {
    shutdown: Option<CancellationToken>,
    /// `Some(true)` when the caller holds the maintenance claim: the cycle
    /// takes the held branch and does not read the claim. `None` reads it
    /// through `check_maintenance_claim`.
    maintenance_held: Option<bool>,
    /// Whether the cycle leaves the store's check out and reads the health
    /// as the caller recorded it: a command that writes nothing to the root
    /// (`backup sync --forget-backup`, `--forget-store`), whose own check
    /// writes no identity file.
    skip_store_check: bool,
    /// How many times this instance took over each store-backed restore
    /// record, by target table id, for the bound [`RESTORE_TAKEOVERS`]; an
    /// entry is removed when the record is failed by the bound.
    restore_takeovers: std::collections::HashMap<String, u32>,
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

    /// [`Self::for_maintenance_holder`] for a command that writes nothing
    /// to the root: the cycle runs no store check (which may write
    /// `extenddb-store.json` or a probe file), and the duties read the
    /// store's health as the command's own check recorded it.
    #[must_use]
    pub fn for_maintenance_holder_without_store_check() -> Self {
        Self {
            skip_store_check: true,
            ..Self::for_maintenance_holder()
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
    S: BackupCatalog + LegacyBackupReader + RestoreEngine + ?Sized + Send + Sync + 'static,
{
    loop {
        let started = std::time::Instant::now();
        let outcome = {
            let mut state = state.lock().await;
            // The state outlives this worker (it is `serve`'s); the token is
            // this worker's, so it is set here, each cycle.
            state.shutdown = Some(shutdown.clone());
            cycle(
                &storage,
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
                        "backup reconciler: failed and removed {} dead legacy restore(s), resumed \
                         {}, failed {} and failed and removed {} (under the maintenance claim) \
                         dead store-backed restore(s), removed the target(s) of {} FAILED restore \
                         record(s) and {} abandoned in-catalog backup(s), finished {} \
                         deletion(s), resolved {} dead CREATING row(s), deleted {} FAILED \
                         prefix(es), swept {} DELETED prefix(es), removed {} tombstone(s), swept \
                         the registrations of {} backup(s), {} progress row(s) and {} spool \
                         entry(ies)",
                        report.legacy_restores_removed.len(),
                        report.manifest_restores_resumed.len(),
                        report.manifest_restores_failed.len(),
                        report.manifest_restores_failed_and_removed.len(),
                        report.failed_targets_removed.len(),
                        report.legacy_backups_removed.len(),
                        report.deletions_finished.len(),
                        report.creating_resolved.len(),
                        report.failed_prefixes_deleted.len(),
                        report.deleted_prefixes_swept.len(),
                        report.tombstones_removed.len(),
                        report.registrations_swept.len(),
                        report.progress_rows_swept,
                        report.spool_entries_swept.len()
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
    storage: &Arc<S>,
    runtime: &BackupRuntime,
    stale_job_after: Duration,
    backend: &str,
    state: &mut ReconcilerState,
) -> Result<CycleReport, StorageError>
where
    S: BackupCatalog + LegacyBackupReader + RestoreEngine + ?Sized + Send + Sync + 'static,
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
        if state.maintenance_held.is_some() {
            tracing::info!(
                "backup reconciler: run by a maintenance command before its own work; running \
                 only the duties that resolve rows whose owner is dead, the store's check and \
                 the DELETING rows"
            );
        } else {
            tracing::info!(
                "backup reconciler: a maintenance claim is held; running only the duties that \
                 resolve rows whose owner is dead, the store's check and the DELETING rows"
            );
        }
    }

    // The store's check first, so every duty of this cycle that reads the
    // store's health reads what this cycle's check left.
    if let Some(store) = runtime.configured_store()
        && !state.skip_store_check
    {
        let check = super::check_configured_store(store, &**storage, backend);
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
        // The startup sync of an in-memory catalog, once its store is
        // healthy: an S3 store's first check is this cycle's, and a sync
        // the server's start did not finish is left pending for here
        // (`super::sync::startup_sync`).
        if store.startup_sync_pending() {
            let sync = super::sync::startup_sync(&**storage, runtime);
            match &state.shutdown {
                Some(shutdown) => tokio::select! {
                    _ = sync => {}
                    () = shutdown.cancelled() => return Ok(report),
                },
                None => {
                    sync.await;
                }
            }
        }
    }

    let mut store_not_ready_logged = false;
    for job in storage.list_dead_restore_jobs(stale_job_after).await? {
        match (job.status, job.kind) {
            // A `target_not_empty` target is the operator's.
            (JobStatus::Failed, _) if job.failure_reason.as_deref() == Some(TARGET_NOT_EMPTY) => {
                tracing::warn!(
                    "the restore into {} (account {}) from {} is FAILED {TARGET_NOT_EMPTY}: its \
                     target ({}) holds items no record of the restore accounts for and is left \
                     for the operator; inspect it at the storage level and remove it with \
                     extenddb backup remove-target --account {} {}",
                    job.target.table_name,
                    job.target.account_id,
                    job.source_backup_arn,
                    job.target.table_id,
                    job.target.account_id,
                    job.target.table_name
                );
                report
                    .targets_left_for_operator
                    .push(job.target.table_name.clone());
            }
            // The FAILED-target duty, whatever the record's kind: the job is
            // over and only its target is left.
            (JobStatus::Failed, _) => {
                match remove_restore_target(
                    &**storage,
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
            (_, RestoreKind::Legacy) => match remove_dead_legacy_restore(&**storage, &job).await {
                Some(Removal::Removed) => report
                    .legacy_restores_removed
                    .push(job.target.table_name.clone()),
                Some(Removal::LeftToOwner) => report
                    .targets_left_to_owner
                    .push(job.target.table_name.clone()),
                Some(Removal::Failed) | None => {}
            },
            (_, RestoreKind::Manifest) => {
                let name = job.target.table_name.clone();
                let stopping = state
                    .shutdown
                    .as_ref()
                    .is_some_and(CancellationToken::is_cancelled);
                match resume_dead_manifest_restore(
                    storage,
                    runtime,
                    stale_job_after,
                    &job,
                    report.maintenance_held,
                    stopping,
                    &mut state.restore_takeovers,
                )
                .await
                {
                    Resumed::Spawned => report.manifest_restores_resumed.push(name),
                    Resumed::Failed(reason) => report.manifest_restores_failed.push((name, reason)),
                    Resumed::FailedAndRemoved(removal) => match removal {
                        Removal::Removed => report.manifest_restores_failed_and_removed.push(name),
                        Removal::LeftToOwner => report.targets_left_to_owner.push(name),
                        Removal::Failed => {}
                    },
                    Resumed::NoPermit => report.restores_skipped_no_permit.push(name),
                    Resumed::StoreUnhealthy => {
                        if !store_not_ready_logged {
                            store_not_ready_logged = true;
                            let state =
                                match runtime.configured_store().map(ConfiguredStore::health) {
                                    Some(super::StoreHealth::Checking) => "not yet checked",
                                    _ => "unhealthy",
                                };
                            tracing::warn!(
                                "the configured backup store is {state}; dead store-backed \
                                 restores are not resumed this cycle"
                            );
                        }
                        report.restores_skipped_store_unhealthy.push(name);
                    }
                    Resumed::LeftToOwner | Resumed::Error => {}
                }
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

    if let Some(store) = runtime.configured_store()
        && let Some(identity) = store.identity().filter(|_| store.is_healthy())
    {
        let calls = StoreCalls {
            store: &*store.store,
            metrics: runtime.metrics().map(|m| &**m),
        };
        let fence = store.fence();
        // A fence refusal makes the store unhealthy (a root another catalog
        // adopted since this cycle's check): the store duties after it do
        // not run on the root, the dead-owner duty's reads included.
        let healthy = || store.is_healthy();
        report.deletions_finished =
            finish_deleting_rows(&**storage, store, &fence, &identity).await?;
        if healthy() {
            (report.creating_resolved, report.creating_left_to_owner) =
                resolve_dead_creating_rows(&**storage, calls, &identity, stale_job_after).await?;
        }
        if !report.maintenance_held && healthy() {
            let retention = runtime.job_settings().failed_row_retention;
            let duty = Duty {
                storage: &**storage,
                store: calls,
                fence: &fence,
                stale_job_after,
            };
            report.failed_prefixes_deleted =
                delete_stale_failed_prefixes(duty, &identity, stale_job_after).await?;
            if healthy() {
                report.deleted_prefixes_swept = sweep_deleted_prefixes(duty, &identity).await?;
            }
            if healthy() {
                report.tombstones_removed = remove_tombstones(duty, &identity, retention).await?;
            }
        }
    }

    if !report.maintenance_held {
        report.registrations_swept = storage.sweep_restore_registrations().await?;
        (report.progress_rows_swept, report.spool_entries_swept) =
            sweep_restore_leftovers(&**storage, runtime.configured_store()).await;
    }
    Ok(report)
}

/// What the manifest arm did with one dead RUNNING record.
enum Resumed {
    /// Taken over and the job spawned.
    Spawned,
    /// FAILED with the reason, the target left to the FAILED-target duty
    /// (another store) or to the operator (`target_not_empty`).
    Failed(String),
    /// FAILED `OWNER_DEAD` and the target removed (or found moved, or the
    /// removal failed), as a legacy record is, under the maintenance claim.
    FailedAndRemoved(Removal),
    /// No job permit was free; the next cycle retries.
    NoPermit,
    /// The configured store is unhealthy; the next cycle retries.
    StoreUnhealthy,
    /// The backend did not hand the record over: its owner is alive, or
    /// another reconciler took it; or the server is shutting down and the
    /// record is left for the next reconciler.
    LeftToOwner,
    /// A catalog or store call failed; logged, the next cycle retries.
    Error,
}

/// The manifest arm (module doc). `takeovers` counts this instance's
/// takeovers of each record for the bound [`RESTORE_TAKEOVERS`].
async fn resume_dead_manifest_restore<S>(
    storage: &Arc<S>,
    runtime: &BackupRuntime,
    stale_job_after: Duration,
    job: &RestoreJobRow,
    maintenance_held: bool,
    stopping: bool,
    takeovers: &mut std::collections::HashMap<String, u32>,
) -> Resumed
where
    S: BackupCatalog + RestoreEngine + ?Sized + Send + Sync + 'static,
{
    let target = &job.target;
    let what = format!(
        "the restore of {} into {} (account {})",
        job.source_backup_arn, target.table_name, target.account_id
    );
    // Once the server's shutdown has begun the jobs' token is cancelled: a
    // takeover now would spawn a resume that stops at its first check and
    // leaves the record with this instance's fresh heartbeat, so the next
    // instance waits a second stale window for it; a FAILED write on the
    // way out is no better. The record is left as it is for the next
    // reconciler to meet. The same holds once this reconciler's own token
    // fires while the jobs' token does not (`stopping`: the server stops its
    // workers while it holds no serving registration and starts a new set
    // once it holds one): a resume is a job, on the jobs' token, which the
    // workers' stop does not stop, so the record is left for the next set's
    // reconciler.
    if runtime.shutdown().is_cancelled() {
        tracing::info!(
            "{what} has no live owner and the server is shutting down; it is left for the next \
             reconciler"
        );
        return Resumed::LeftToOwner;
    }
    if stopping {
        tracing::info!(
            "{what} has no live owner and the reconciler is stopping; it is left for the next \
             reconciler"
        );
        return Resumed::LeftToOwner;
    }
    if maintenance_held {
        tracing::info!(
            "{what} has no live owner and a maintenance claim is held; it is failed rather than \
             resumed"
        );
        // The `target_not_empty` guard first, as on the takeover arm: a
        // record with no progress over a target that holds items is the
        // operator's, and `OWNER_DEAD` with the target's removal would drop
        // what the record does not account for.
        match unaccounted_items(&**storage, job).await {
            Ok(Some(count)) => {
                return match storage
                    .fail_restore_job(
                        target,
                        job.owner_generation,
                        job.heartbeat_at,
                        TARGET_NOT_EMPTY,
                    )
                    .await
                {
                    Ok(true) => {
                        tracing::error!(
                            "{what} recorded no progress and its target ({}) holds {count} \
                             item(s); the record is FAILED {TARGET_NOT_EMPTY} and the target is \
                             left for the operator: extenddb backup remove-target --account {} {}",
                            target.table_id,
                            target.account_id,
                            target.table_name
                        );
                        Resumed::Failed(TARGET_NOT_EMPTY.to_owned())
                    }
                    Ok(false) => Resumed::LeftToOwner,
                    Err(e) => {
                        tracing::error!(
                            "{what}: the FAILED write failed; the next cycle retries: {e}"
                        );
                        Resumed::Error
                    }
                };
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!(
                    "{what}: its progress or its target could not be read; the next cycle \
                     retries: {e}"
                );
                return Resumed::Error;
            }
        }
        return match remove_dead_legacy_restore(&**storage, job).await {
            Some(removal) => Resumed::FailedAndRemoved(removal),
            None => Resumed::LeftToOwner,
        };
    }
    let store = runtime.configured_store_arc();
    // An instance with no configured store does not own any store-backed
    // record: it leaves every one alone for the instance that holds its
    // store, rather than failing records it cannot read (an instance without
    // `[backup.store]` had failed every dead store-backed record it listed
    // `OTHER_STORE`). Only an instance that has a store, and whose store is
    // not the record's, fails the record OTHER_STORE.
    if store.is_none() {
        tracing::info!(
            "{what} has no live owner and this instance has no configured store; it is left for \
             the instance that holds the record's store"
        );
        return Resumed::LeftToOwner;
    }
    let of_this_store = job
        .store
        .as_ref()
        .is_some_and(|row_store| runtime.is_configured_store(row_store));
    if !of_this_store {
        tracing::error!(
            "{what} has no live owner and reads a store other than the configured one; the \
             record is FAILED and its target removed once the record is stale: {OTHER_STORE}"
        );
        // Conditioned on the claim the listing read, as the legacy arm's
        // write is: a heartbeat that landed since keeps the record its
        // owner's (`Ok(false)`).
        return match storage
            .fail_restore_job(target, job.owner_generation, job.heartbeat_at, OTHER_STORE)
            .await
        {
            Ok(true) => Resumed::Failed(OTHER_STORE.to_owned()),
            Ok(false) => Resumed::LeftToOwner,
            Err(e) => {
                tracing::error!("{what} could not be marked FAILED; the next cycle retries: {e}");
                Resumed::Error
            }
        };
    }
    let Some(store) = store.filter(|store| store.is_healthy()) else {
        return Resumed::StoreUnhealthy;
    };
    let env = match JobEnv::new(runtime, store) {
        Ok(env) => env,
        Err(_) => return Resumed::StoreUnhealthy,
    };
    let Some(permit) = JobPermit::try_acquire(runtime.slots()) else {
        tracing::info!("{what} has no live owner; no job permit is free, the next cycle retries");
        return Resumed::NoPermit;
    };
    let spool_host = env.spool.as_ref().map(|s| s.spool_host.clone());
    let claim = match storage
        .claim_restore_dead(target, stale_job_after, spool_host.as_deref())
        .await
    {
        Ok(Some(claim)) => claim,
        Ok(None) => return Resumed::LeftToOwner,
        Err(e) => {
            tracing::error!(
                "could not test whether the owner of {what} is dead; the next cycle retries: {e}"
            );
            return Resumed::Error;
        }
    };
    // The resumed job's first confirmed beat (module doc): the takeover
    // stamped `heartbeat_at` before it returned.
    let claimed_at = tokio::time::Instant::now();
    // The cycle read the maintenance claim once at its start; a command may
    // have taken it since. Re-read it now the record is this instance's, as
    // the request's `start` re-checks after its registration: held, the
    // resume is left for the maintenance arm of a later cycle rather than
    // run beside a command, and the record keeps this takeover's fresh
    // heartbeat.
    match storage.check_maintenance_claim(stale_job_after).await {
        Ok(false) => {}
        Ok(true) => {
            tracing::info!(
                "{what} was taken over but a maintenance claim is now held; the resume is left \
                 for the next cycle"
            );
            return Resumed::LeftToOwner;
        }
        Err(e) => {
            tracing::error!(
                "{what} was taken over but the maintenance claim could not be re-read; the next \
                 cycle retries: {e}"
            );
            return Resumed::Error;
        }
    }
    // The bound on this instance's takeovers of the record
    // (`RESTORE_TAKEOVERS`): the takeover after the fifth fails the record
    // and removes its target as the arm below does for a target with no
    // tables, since a record whose every resume ends RUNNING again would
    // otherwise be retaken every stale window for as long as the server runs.
    let taken = takeovers.entry(target.table_id.clone()).or_insert(0);
    *taken = taken.saturating_add(1);
    if *taken > RESTORE_TAKEOVERS {
        takeovers.remove(&target.table_id);
        let fields = StatusFields {
            failure_reason: Some(TAKEOVERS_EXHAUSTED.to_owned()),
            completed: true,
            ..StatusFields::default()
        };
        return match write_status(&*claim, JobStatus::Running, JobStatus::Failed, &fields).await {
            Written::Matched => {
                tracing::error!(
                    "{what} has no live owner and was taken over {RESTORE_TAKEOVERS} times by \
                     this instance without ending; the record is FAILED and the target ({}) \
                     removed",
                    target.table_id
                );
                Resumed::FailedAndRemoved(
                    remove_restore_target(&**storage, job, "was marked FAILED by the reconciler")
                        .await,
                )
            }
            Written::NotMatched(why) => {
                tracing::info!("{what} was no longer this cycle's ({why}); left to its owner");
                Resumed::LeftToOwner
            }
            Written::Error(e) => {
                tracing::error!("{what}: the FAILED write failed; the next cycle retries: {e}");
                Resumed::Error
            }
        };
    }
    // The progress under the claim: the record as it is now and, where the
    // backend keeps them apart, its progress rows.
    let (record, progress_rows) = match (
        storage.read_restore_job(target).await,
        storage.read_restore_progress(target).await,
    ) {
        (Ok(Some(record)), Ok(rows)) => (record, rows),
        (Ok(None), _) => {
            tracing::info!("{what} was gone when its progress was read; left to whoever moved it");
            return Resumed::LeftToOwner;
        }
        (Err(e), _) | (_, Err(e)) => {
            tracing::error!(
                "{what} was taken over but its progress could not be read; the next cycle \
                 retries once the record is stale again: {e}"
            );
            return Resumed::Error;
        }
    };
    // The target's data tables must exist: every backend commits the record
    // before the data DDL (the trait doc of `restore_target_ready`), so an
    // owner killed in between leaves a RUNNING record over a target with no
    // table, or on MongoDB with its index metadata missing. Such a record is
    // never resumable, and the count below would fail on the missing table
    // and leave the takeover's fresh heartbeat, so the record would be
    // retaken every stale window and never fail. It is failed and its target
    // removed instead, under the claim this takeover holds.
    match storage.restore_target_ready(target).await {
        Ok(true) => {}
        Ok(false) => {
            let fields = StatusFields {
                failure_reason: Some(OWNER_DEAD.to_owned()),
                completed: true,
                ..StatusFields::default()
            };
            return match write_status(&*claim, JobStatus::Running, JobStatus::Failed, &fields).await
            {
                Written::Matched => {
                    tracing::error!(
                        "{what} has no live owner and its target ({}) has no data tables (its \
                         owner died between the registration and the data DDL); the record is \
                         FAILED and the target removed",
                        target.table_id
                    );
                    Resumed::FailedAndRemoved(
                        remove_restore_target(
                            &**storage,
                            job,
                            "was marked FAILED by the reconciler",
                        )
                        .await,
                    )
                }
                Written::NotMatched(why) => {
                    tracing::info!("{what} was no longer this cycle's ({why}); left to its owner");
                    Resumed::LeftToOwner
                }
                Written::Error(e) => {
                    tracing::error!("{what}: the FAILED write failed; the next cycle retries: {e}");
                    Resumed::Error
                }
            };
        }
        Err(e) => {
            tracing::error!(
                "{what} was taken over but whether its target has data tables could not be read; \
                 the next cycle retries: {e}"
            );
            return Resumed::Error;
        }
    }
    if has_no_progress(&record, &progress_rows) {
        let count = match storage.count_restore_target(target).await {
            Ok(count) => count,
            Err(e) => {
                tracing::error!(
                    "{what} was taken over but its target could not be counted; the next cycle \
                     retries once the record is stale again: {e}"
                );
                return Resumed::Error;
            }
        };
        let unaccounted = match exceeds_backup_total(&**storage, job, count).await {
            Ok(unaccounted) => unaccounted,
            Err(e) => {
                tracing::error!(
                    "{what} was taken over but its backup's row could not be read; the next \
                     cycle retries once the record is stale again: {e}"
                );
                return Resumed::Error;
            }
        };
        if count > 0 && !unaccounted {
            tracing::info!(
                "{what} recorded no progress and its target ({}) holds {count} item(s), no more \
                 than the backup's total: a first data file applied in part before its progress \
                 was recorded; the resume re-applies it with upserts",
                target.table_id
            );
        }
        if unaccounted {
            let fields = StatusFields {
                failure_reason: Some(TARGET_NOT_EMPTY.to_owned()),
                completed: true,
                ..StatusFields::default()
            };
            return match write_status(&*claim, JobStatus::Running, JobStatus::Failed, &fields).await
            {
                Written::Matched => {
                    tracing::error!(
                        "{what} recorded no progress and its target ({}) holds {count} item(s); \
                         the record is FAILED {TARGET_NOT_EMPTY} and the target is left for the \
                         operator: extenddb backup remove-target --account {} {}",
                        target.table_id,
                        target.account_id,
                        target.table_name
                    );
                    Resumed::Failed(TARGET_NOT_EMPTY.to_owned())
                }
                Written::NotMatched(why) => {
                    tracing::info!("{what} was no longer this cycle's ({why}); left to its owner");
                    Resumed::LeftToOwner
                }
                Written::Error(e) => {
                    tracing::error!("{what}: the FAILED write failed; the next cycle retries: {e}");
                    Resumed::Error
                }
            };
        }
    }
    // The record's progress as recorded; the job's own start line names the
    // first file it opens.
    tracing::info!(
        "{what} has no live owner; taken over by this instance and resumed with {}",
        match (record.file_index, record.items_applied) {
            (Some(file), Some(items)) =>
                format!("{items} items of data file {file} recorded as applied"),
            (Some(file), None) => format!("data file {file} recorded as applied"),
            (None, _) if !progress_rows.is_empty() =>
                format!("{} data file(s) recorded as applied", progress_rows.len()),
            (None, _) => "nothing recorded as applied".to_owned(),
        }
    );
    let limits = (&**runtime.limits()).into();
    runtime.spawn_job(restore::resume(
        Arc::clone(storage),
        env,
        permit,
        claim,
        restore::DeadRecord {
            row: record,
            recorded_spool_host: job.spool_host.clone(),
            progress_rows,
            claimed_at,
        },
        limits,
    ));
    Resumed::Spawned
}

/// Whether a RUNNING record has no progress recorded at all: no
/// `file_index` on the record and no progress row beside it.
fn has_no_progress(record: &RestoreJobRow, progress_rows: &[(u32, u64)]) -> bool {
    record.file_index.is_none() && progress_rows.is_empty()
}

/// The `target_not_empty` test as the maintenance-held arm runs it, with
/// no claim on the record: the record and its progress rows read, and when
/// [`has_no_progress`] the target counted; `Some(count)` when the target
/// holds items no record accounts for ([`exceeds_backup_total`]), `None`
/// when progress is recorded, the target is empty or holds no more than the
/// backup's total, or the record is gone (the caller's write then finds it
/// so).
async fn unaccounted_items<S: BackupCatalog + RestoreEngine + ?Sized>(
    storage: &S,
    job: &RestoreJobRow,
) -> Result<Option<u64>, StorageError> {
    let target = &job.target;
    let Some(record) = storage.read_restore_job(target).await? else {
        return Ok(None);
    };
    let progress_rows = storage.read_restore_progress(target).await?;
    if !has_no_progress(&record, &progress_rows) {
        return Ok(None);
    }
    let count = storage.count_restore_target(target).await?;
    Ok(exceeds_backup_total(storage, job, count)
        .await?
        .then_some(count))
}

/// The `target_not_empty` rule's test: whether `count` items in a target
/// with no recorded progress are more than the backup holds (its row's
/// `item_count`). At or below the total they are a first data file applied
/// in part before its progress was recorded (Cassandra records a batch's
/// progress after the batch's writes, so a crash inside the first batch
/// leaves this state), and the resume re-applies the first file with
/// `reapplying` set, every backend's load being idempotent for a file
/// resumed that way. Above the total the target holds items the backup does
/// not account for (a catalog restored from a dump naming a restore that
/// finished and was written to since), the operator's. A count of zero is
/// never unaccounted; a backup row this catalog does not hold, or an ARN
/// this build cannot parse, leaves every item unaccounted.
async fn exceeds_backup_total<S: BackupCatalog + ?Sized>(
    storage: &S,
    job: &RestoreJobRow,
    count: u64,
) -> Result<bool, StorageError> {
    if count == 0 {
        return Ok(false);
    }
    let Ok(arn) = BackupArn::parse(&job.source_backup_arn) else {
        return Ok(true);
    };
    let total = storage
        .read_backup_row(&arn)
        .await?
        .map(|row| u64::try_from(row.item_count).unwrap_or(0));
    Ok(total.is_none_or(|total| count > total))
}

/// The progress-row and spool sweeps (module doc). The spool directory is
/// listed first, then the RUNNING records, and only entries of that listing
/// that no record of this host names are removed, so a restore whose
/// registration commits and whose entry opens between the two reads is
/// never a candidate (its entry is not in the listing, or its record is in
/// the records). A listing the backend cannot make is logged and skips
/// both.
async fn sweep_restore_leftovers<S: BackupCatalog + RestoreEngine + ?Sized>(
    storage: &S,
    store: Option<&ConfiguredStore>,
) -> (u64, Vec<String>) {
    let spool = store.and_then(|store| store.spool.as_ref());
    let entries = match spool.map(|spool| restore::spool::list_entries(&spool.path)) {
        None => Vec::new(),
        Some(Ok(entries)) => entries,
        Some(Err(e)) => {
            tracing::error!(
                "the restore spool {} could not be listed; the spool sweep is skipped this \
                 cycle: {e}",
                spool
                    .map(|s| s.path.display().to_string())
                    .unwrap_or_default()
            );
            Vec::new()
        }
    };
    let running = match storage.list_restore_jobs(Some(JobStatus::Running)).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(
                "the restore job records could not be listed; the progress-row and spool sweeps \
                 are skipped this cycle: {e}"
            );
            return (0, Vec::new());
        }
    };
    let running_ids: Vec<String> = running.iter().map(|r| r.target.table_id.clone()).collect();
    let progress_rows = match storage.sweep_restore_progress(&running_ids).await {
        Ok(n) => {
            if n > 0 {
                tracing::info!(
                    "removed {n} restore progress row(s) of tables that are not RUNNING restore \
                     targets"
                );
            }
            n
        }
        Err(e) => {
            tracing::error!(
                "the restore progress rows could not be swept; the next cycle retries: {e}"
            );
            0
        }
    };
    let Some(spool) = spool else {
        return (progress_rows, Vec::new());
    };
    let this_host: HashSet<String> = running
        .iter()
        .filter(|r| r.spool_host.as_deref() == Some(spool.spool_host.as_str()))
        .map(|r| r.target.table_id.clone())
        .collect();
    let swept = restore::spool::sweep_entries(entries, &this_host);
    (progress_rows, swept)
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
        match resolve_dead_creating_row(storage, store, &row, stale_job_after, OnIncomplete::Fail)
            .await
        {
            CreatingOutcome::Resolved(status) => resolved.push((row.backup_arn, status)),
            CreatingOutcome::LeftToOwner(_) => left_to_owner.push(row.backup_arn),
            CreatingOutcome::NotHandedOver
            | CreatingOutcome::Incomplete(_)
            | CreatingOutcome::Failed(_) => {}
        }
    }
    Ok((resolved, left_to_owner))
}

/// What [`resolve_dead_creating_row`] did with one CREATING row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum CreatingOutcome {
    /// The backend did not hand the row over: its owner is alive, or
    /// another taker has it.
    NotHandedOver,
    /// The row was written AVAILABLE (the store holds its complete
    /// manifest) or FAILED ([`OWNER_DEAD_NO_MANIFEST`]).
    Resolved(BackupStatus),
    /// The write matched no row: another owner took it over since the
    /// takeover (why).
    LeftToOwner(String),
    /// With [`OnIncomplete::Leave`], the store holds no complete manifest
    /// (why): no status was written, the row is CREATING under the
    /// takeover's generation with its heartbeat as it was.
    Incomplete(String),
    /// A catalog or store call failed, or the row's ARN does not parse
    /// (why); the row is left as it was and logged.
    Failed(String),
}

/// What [`resolve_dead_creating_row`] writes when the store holds no
/// complete manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OnIncomplete {
    /// FAILED ([`OWNER_DEAD_NO_MANIFEST`]): the store probed is the row's.
    Fail,
    /// Nothing ([`CreatingOutcome::Incomplete`]): the store probed holds a
    /// copy, and the row's own store may hold the complete backup.
    Leave,
}

/// One CREATING row whose owner may be dead: the backend's takeover
/// (`claim_dead_backup` with `stale_job_after`), the manifest probe of
/// `store` ([`probe_manifest`]), and the CREATING to AVAILABLE write under
/// the taken generation, or to FAILED when the probe finds no complete
/// manifest and `on_incomplete` is [`OnIncomplete::Fail`]. The dead-owner
/// duty runs it on the configured store's rows with `Fail`; `backup sync
/// --move-store` on the old root's rows with `Leave`, probing the
/// configured store, to which the copied prefix belongs: a copy without a
/// complete backup proves nothing about the old root's prefix (a copy's
/// order is arbitrary), so the move writes no FAILED.
pub(super) async fn resolve_dead_creating_row<S: BackupCatalog + ?Sized>(
    storage: &S,
    store: StoreCalls<'_>,
    row: &BackupRow,
    stale_job_after: Duration,
    on_incomplete: OnIncomplete,
) -> CreatingOutcome {
    let claim = match storage.claim_dead_backup(&row.key(), stale_job_after).await {
        Ok(Some(claim)) => claim,
        Ok(None) => return CreatingOutcome::NotHandedOver,
        Err(e) => {
            tracing::error!(
                "could not test whether the owner of CREATING backup {} (account {}) is dead; \
                 the next cycle retries: {e}",
                row.backup_arn,
                row.account_id
            );
            return CreatingOutcome::Failed(format!(
                "could not test whether its owner is dead: {e}"
            ));
        }
    };
    let arn = match BackupArn::parse(&row.backup_arn) {
        Ok(arn) => arn,
        Err(e) => {
            tracing::error!(
                "CREATING row {} carries an ARN this build cannot parse; left as it is: {e}",
                row.backup_arn
            );
            return CreatingOutcome::Failed(format!("the row's ARN does not parse: {e}"));
        }
    };
    let (to, fields) = match probe_manifest(store, &arn).await {
        Ok(Probe::Complete(fields)) => (JobStatus::Available, fields),
        Ok(Probe::Incomplete(why)) if on_incomplete == OnIncomplete::Leave => {
            drop(claim);
            return CreatingOutcome::Incomplete(why);
        }
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
                "backup {} (account {}): its owner is dead and the store could not be probed \
                 for its manifest; the next cycle retries: {e}",
                row.backup_arn,
                row.account_id
            );
            return CreatingOutcome::Failed(format!(
                "the store could not be probed for its manifest: {e}"
            ));
        }
    };
    let outcome = match write_status(&*claim, JobStatus::Creating, to, &fields).await {
        Written::Matched => {
            tracing::info!(
                "backup {} (account {}): its owner was dead; the row is now {to}",
                row.backup_arn,
                row.account_id
            );
            CreatingOutcome::Resolved(match to {
                JobStatus::Available => BackupStatus::Available,
                _ => BackupStatus::Failed,
            })
        }
        Written::NotMatched(why) => {
            tracing::info!(
                "backup {} was no longer this cycle's to resolve ({why}); left to its owner",
                row.backup_arn
            );
            CreatingOutcome::LeftToOwner(why)
        }
        Written::Error(e) => {
            tracing::error!(
                "backup {} (account {}): the {to} write failed; the next cycle retries: {e}",
                row.backup_arn,
                row.account_id
            );
            CreatingOutcome::Failed(format!("the {to} write failed: {e}"))
        }
    };
    drop(claim);
    outcome
}

/// The configured store as the reconciler's duties call it: each failed
/// call (not an absent object) records `BackupStoreErrors` under the call's
/// name (`get`, `list` or `delete_prefix`), as the job records its own, so
/// a store that refuses the reconciler's calls moves the metric even when
/// every cycle completes.
#[derive(Clone, Copy)]
pub(super) struct StoreCalls<'a> {
    pub(super) store: &'a dyn BackupStore,
    pub(super) metrics: Option<&'a MetricsCollector>,
}

impl StoreCalls<'_> {
    /// `e`, with `BackupStoreErrors` recorded under `op` when metrics are
    /// collected.
    pub(super) fn failed(self, op: &str, e: StoreError) -> StoreError {
        if let Some(metrics) = self.metrics {
            metrics.record_backup_store_error(op);
        }
        e
    }

    /// One object's bytes within `bound` ([`read_object`]).
    async fn read_object(self, key: &str, bound: u64) -> Result<ReadObject, StoreError> {
        read_object(self.store, key, bound)
            .await
            .map_err(|e| self.failed("get", e))
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

    /// The prefix deleted, no removal started at or after `deadline`
    /// ([`BackupStore::delete_prefix_before`]).
    async fn delete_prefix_before(
        self,
        prefix: &str,
        deadline: tokio::time::Instant,
    ) -> Result<extenddb_backup_store::PrefixDeletion, StoreError> {
        self.store
            .delete_prefix_before(prefix, deadline)
            .await
            .map_err(|e| self.failed("delete_prefix", e))
    }
}

/// What the manifest probe found under a dead owner's prefix.
pub(super) enum Probe {
    /// A complete, verified manifest: the AVAILABLE write's fields.
    Complete(StatusFields),
    /// No manifest, or one the reader refuses, or a data file missing or of
    /// another size; the text says which.
    Incomplete(String),
}

/// The manifest probe of the dead-owner duty (module doc): the checks of
/// [`verify_prefix`], with the manifest's ARN required to be the row's (its
/// region and partition included) before the prefix is listed. `backup
/// sync --move-store` probes each AVAILABLE row of the old root with it in
/// the configured store. A store error other than an absent object is the
/// `Err`.
pub(super) async fn probe_manifest(
    store: StoreCalls<'_>,
    arn: &BackupArn,
) -> Result<Probe, StoreError> {
    let prefix = arn.store_prefix();
    let manifest = match read_verified_manifest(store, &prefix).await? {
        Verified::Complete(manifest) => manifest,
        Verified::Incomplete(why) => return Ok(Probe::Incomplete(why)),
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
    if let Some(why) = missing_data_file(&manifest, &prefix, &listed) {
        return Ok(Probe::Incomplete(why));
    }
    Ok(Probe::Complete(StatusFields {
        size_bytes: Some(manifest.backup_size_bytes),
        item_count: Some(manifest_item_count(&manifest)),
        failure_reason: None,
        source_table_details_json: serde_json::to_value(&manifest.source_table).ok(),
        completed: true,
    }))
}

/// What [`verify_prefix`] found under a prefix.
pub(super) enum Verified {
    /// A manifest whose sidecar agrees, that the reader accepts, and whose
    /// every data file is listed under the prefix at its recorded size.
    Complete(Box<ExtenddbManifest>),
    /// No manifest, or one without its sidecar or not matching it, or one
    /// the reader refuses, or a data file missing or of another size; the
    /// text says which.
    Incomplete(String),
}

/// The checks a prefix must pass before anything reads it as a backup, in
/// order: `extenddb-manifest.json` read within [`MANIFEST_READ_BOUND`], its
/// sidecar within [`SIDECAR_READ_BOUND`], the sidecar's digest equal to the
/// manifest's, the manifest accepted by
/// [`ExtenddbManifest::from_json_bytes`] (which also checks that its ARN,
/// `source_table` and creation instant agree), then every `data_files` key
/// present under the prefix with its `bytes`. `listed` is the prefix's
/// objects (key to size) when the caller already listed them (sync, which
/// lists the whole root); `None` lists the prefix here. The manifest's ARN
/// is the caller's to compare: the dead-owner duty requires the row's ARN,
/// sync the listed path's components.
///
/// # Errors
///
/// A store error other than an absent object.
pub(super) async fn verify_prefix(
    store: StoreCalls<'_>,
    prefix: &str,
    listed: Option<&std::collections::HashMap<String, u64>>,
) -> Result<Verified, StoreError> {
    let manifest = match read_verified_manifest(store, prefix).await? {
        Verified::Complete(manifest) => manifest,
        incomplete @ Verified::Incomplete(_) => return Ok(incomplete),
    };
    let listed_here;
    let listed = match listed {
        Some(listed) => listed,
        None => {
            listed_here = store.list_sizes(prefix).await?;
            &listed_here
        }
    };
    Ok(match missing_data_file(&manifest, prefix, listed) {
        Some(why) => Verified::Incomplete(why),
        None => Verified::Complete(manifest),
    })
}

/// The first three checks of [`verify_prefix`]: the manifest and its
/// sidecar read within their bounds, the digest, the reader's acceptance.
async fn read_verified_manifest(
    store: StoreCalls<'_>,
    prefix: &str,
) -> Result<Verified, StoreError> {
    let manifest = match store
        .read_object(
            &format!("{prefix}{EXTENDDB_MANIFEST_FILE}"),
            MANIFEST_READ_BOUND,
        )
        .await?
    {
        ReadObject::Bytes(bytes) => bytes,
        ReadObject::Absent => return Ok(Verified::Incomplete("no manifest".to_owned())),
        ReadObject::Oversize => {
            return Ok(Verified::Incomplete(format!(
                "a manifest over {MANIFEST_READ_BOUND} bytes"
            )));
        }
    };
    let sidecar = match store
        .read_object(
            &format!("{prefix}{MANIFEST_CHECKSUM_FILE}"),
            SIDECAR_READ_BOUND,
        )
        .await?
    {
        ReadObject::Bytes(bytes) => bytes,
        ReadObject::Absent => {
            return Ok(Verified::Incomplete(
                "a manifest without its sidecar".to_owned(),
            ));
        }
        ReadObject::Oversize => {
            return Ok(Verified::Incomplete(format!(
                "a sidecar over {SIDECAR_READ_BOUND} bytes"
            )));
        }
    };
    if let Err(e) = verify_manifest_checksum(&manifest, &sidecar) {
        return Ok(Verified::Incomplete(format!(
            "the sidecar does not match: {e}"
        )));
    }
    Ok(match ExtenddbManifest::from_json_bytes(&manifest) {
        Ok(manifest) => Verified::Complete(Box::new(manifest)),
        Err(e) => Verified::Incomplete(format!("the manifest is refused: {e}")),
    })
}

/// The first `data_files` entry `listed` lacks or holds at another size, as
/// the reason a prefix is incomplete; `None` when every one is there.
fn missing_data_file(
    manifest: &ExtenddbManifest,
    prefix: &str,
    listed: &std::collections::HashMap<String, u64>,
) -> Option<String> {
    manifest
        .data_files
        .iter()
        .find_map(|file| match listed.get(&format!("{prefix}{}", file.key)) {
            Some(size) if *size == file.bytes => None,
            Some(size) => Some(format!(
                "data file {} has {size} bytes in the store and {} in the manifest",
                file.key, file.bytes
            )),
            None => Some(format!("data file {} is not in the store", file.key)),
        })
}

/// The manifest's item total, the sum over its data files.
pub(super) fn manifest_item_count(manifest: &ExtenddbManifest) -> i64 {
    let total = manifest
        .data_files
        .iter()
        .map(|f| f.item_count)
        .sum::<u64>();
    i64::try_from(total).unwrap_or(i64::MAX)
}

/// The most bytes a manifest read collects: a manifest this server wrote is
/// a few KiB per data file, so one over this names a prefix nothing this
/// server wrote.
pub(crate) const MANIFEST_READ_BOUND: u64 = 16 * 1024 * 1024;

/// The most bytes a sidecar read collects (a sidecar is one line: the
/// digest and the manifest's name).
pub(crate) const SIDECAR_READ_BOUND: u64 = 4 * 1024;

/// What a bounded read of one object found.
pub(crate) enum ReadObject {
    /// The object's bytes, within the bound.
    Bytes(Vec<u8>),
    /// No object under the key.
    Absent,
    /// The object has more bytes than the bound; the read stopped there and
    /// collected nothing.
    Oversize,
}

/// One object's bytes, collected only while they stay within `bound`
/// (the body is read in chunks and dropped once the count passes it). The
/// restore's request and resume read the manifest and sidecar through it
/// too, each with its own bound, so a prefix an attacker rewrote cannot make
/// a request hold an object of any size in memory.
pub(super) async fn read_object(
    store: &dyn BackupStore,
    key: &str,
    bound: u64,
) -> Result<ReadObject, StoreError> {
    let mut body = match store.get(key).await {
        Ok(body) => body,
        Err(StoreError::NotFound) => return Ok(ReadObject::Absent),
        Err(e) => return Err(e),
    };
    let mut bytes = Vec::new();
    while let Some(chunk) = body.try_next().await? {
        if (bytes.len() as u64).saturating_add(chunk.len() as u64) > bound {
            return Ok(ReadObject::Oversize);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(ReadObject::Bytes(bytes))
}

/// How long one FAILED-prefix or DELETED-prefix deletion, or one tombstone
/// removal, may run from the duty's read of the maintenance claim
/// ([`Duty::fenced`]), counted from before that read was sent: a read
/// answered after it starts nothing, the prefix deletion checks it inside
/// the store before each removal (the filesystem walk before each entry,
/// the S3 loop before each request: `delete_prefix_before`), and a call
/// still running at it is dropped. The duty then stops for this cycle, the
/// row left as it is for the next cycle to continue. It is the bound an S3
/// store's calls run under ([`super::S3_CHECK_TIMEOUT`], the larger of the
/// two kinds' bounds), so a deletion of an S3 prefix stopped at it is
/// dropped before the SDK starts a retry of a request that stalled
/// ([`extenddb_backup_store::S3_ATTEMPT_TIMEOUT`]). A maintenance command
/// that removes FAILED or DELETED rows beside servers waits this bound plus
/// one store call's after it takes the claim
/// ([`super::store_maintenance::FORGET_WAIT`]), so what remains of a
/// deletion a duty began before the claim when the command removes a row is
/// the step in flight at the deadline (one removal, or one S3 request).
/// The bound assumes the duty's process keeps running: one paused past the
/// wait between its last check and a call already sent is outside it, as
/// it is for every lease in the design.
pub const DUTY_DELETION_BOUND: Duration = super::S3_CHECK_TIMEOUT;

/// What the duties that delete a prefix or remove a tombstone act through:
/// the catalog, the store's calls, the deletion fence each runs right
/// before its deletion or removal, and the dead-owner window the
/// maintenance claim is read with.
struct Duty<'a, S: ?Sized> {
    storage: &'a S,
    store: StoreCalls<'a>,
    fence: &'a StoreFence,
    stale_job_after: Duration,
}

impl<S: ?Sized> Clone for Duty<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<S: ?Sized> Copy for Duty<'_, S> {}

/// What [`Duty::fenced`] decided for one row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fenced {
    /// Act, ending by this instant.
    Until(tokio::time::Instant),
    /// Act on nothing for this row; the duty goes on to the next.
    Skip,
    /// The claim's read answered after the deadline: the duty stops for
    /// this cycle.
    OutOfTime,
}

/// What one deletion or removal of a duty came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Acted {
    /// The prefix was deleted, or the row removed.
    Done,
    /// Nothing was done for this row; the duty goes on to the next.
    Skipped,
    /// The call did not end within [`DUTY_DELETION_BOUND`]: the duty stops
    /// for this cycle.
    OutOfTime,
}

impl<S: BackupCatalog + ?Sized> Duty<'_, S> {
    /// The fence before `row`'s deletion or removal, the row read once
    /// more, then the maintenance claim read again: the instant the call
    /// must end by ([`DUTY_DELETION_BOUND`] after the claim's read) when the
    /// fence passed, the row is still of the status and the store the duty
    /// listed it with, and no maintenance command holds the claim
    /// ([`Fenced::Until`]); [`Fenced::OutOfTime`] when the claim's read
    /// answered at or after that instant; [`Fenced::Skip`] otherwise, logged,
    /// acting on nothing. The cycle reads the claim once,
    /// at its start, so a duty that listed the row before a maintenance
    /// command took the claim may reach its row after the command has
    /// removed it (`backup sync --forget-backup`, `--forget-store`) or moved
    /// it to another store (`--move-store`): the re-read skips a row that is
    /// gone, of another status or of another store, and the claim's read
    /// after it starts no deletion once a command holds the claim. A
    /// deletion that read the claim free just before a command took it ends
    /// by the bound ([`DUTY_DELETION_BOUND`], counted from before the read
    /// was sent, so a read answered late starts nothing), and a command that
    /// removes FAILED or DELETED rows waits the bound and one store call
    /// before its first removal (`store_maintenance::FORGET_WAIT`). A failed
    /// read is left for the next cycle.
    async fn fenced(self, row: &BackupRow, status: &str, what: &str) -> Fenced {
        if let Err(problem) = self.fence.check_catalog(self.storage).await {
            tracing::error!(
                "did not {what} of {status} backup {} (account {}): {problem}",
                row.backup_arn,
                row.account_id
            );
            return Fenced::Skip;
        }
        let arn = match BackupArn::parse(&row.backup_arn) {
            Ok(arn) => arn,
            Err(e) => {
                tracing::error!(
                    "did not {what} of {status} backup {}: its ARN does not parse: {e}",
                    row.backup_arn
                );
                return Fenced::Skip;
            }
        };
        match self.storage.read_backup_row(&arn).await {
            Ok(Some(now)) if now.status == row.status && now.store == row.store => {}
            Ok(Some(now)) => {
                tracing::info!(
                    "did not {what} of {status} backup {} (account {}): the row is now {} in \
                     the {} store {} (store id {}); left to whoever changed it",
                    row.backup_arn,
                    row.account_id,
                    now.status,
                    now.store.store_kind,
                    now.store.store_root,
                    now.store.store_id
                );
                return Fenced::Skip;
            }
            Ok(None) => {
                tracing::info!(
                    "did not {what} of {status} backup {} (account {}): the row is gone (removed \
                     since the duty listed it); its objects are left",
                    row.backup_arn,
                    row.account_id
                );
                return Fenced::Skip;
            }
            Err(e) => {
                tracing::error!(
                    "did not {what} of {status} backup {} (account {}): the row could not be read \
                     again; the next cycle retries: {e}",
                    row.backup_arn,
                    row.account_id
                );
                return Fenced::Skip;
            }
        }
        // The bound counts from before the claim's read: a command that
        // takes the claim after the read was answered waits out the bound,
        // and a read answered after it (a slow catalog) acts on nothing.
        let deadline = tokio::time::Instant::now() + DUTY_DELETION_BOUND;
        match self
            .storage
            .check_maintenance_claim(self.stale_job_after)
            .await
        {
            Ok(false) if tokio::time::Instant::now() >= deadline => {
                tracing::warn!(
                    "did not {what} of {status} backup {} (account {}): the maintenance claim's \
                     read answered after the {} s bound; the duty stops for this cycle and the \
                     next cycle continues",
                    row.backup_arn,
                    row.account_id,
                    DUTY_DELETION_BOUND.as_secs()
                );
                Fenced::OutOfTime
            }
            Ok(false) => Fenced::Until(deadline),
            Ok(true) => {
                tracing::info!(
                    "did not {what} of {status} backup {} (account {}): a maintenance claim is \
                     held now; the next cycle without one acts on it",
                    row.backup_arn,
                    row.account_id
                );
                Fenced::Skip
            }
            Err(e) => {
                tracing::error!(
                    "did not {what} of {status} backup {} (account {}): the maintenance claim \
                     could not be read again; the next cycle retries: {e}",
                    row.backup_arn,
                    row.account_id
                );
                Fenced::Skip
            }
        }
    }
}

/// The FAILED-prefix duty (module doc): FAILED rows whose heartbeat is older
/// than `stale_job_after`, their prefix deleted while it lists objects.
async fn delete_stale_failed_prefixes<S: BackupCatalog + ?Sized>(
    duty: Duty<'_, S>,
    identity: &StoreIdentity,
    stale_job_after: Duration,
) -> Result<Vec<String>, StorageError> {
    let rows: Vec<BackupRow> = duty
        .storage
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
        match delete_prefix_if_listed(duty, &row, "FAILED").await {
            Acted::Done => deleted.push(row.backup_arn),
            Acted::Skipped => {}
            Acted::OutOfTime => break,
        }
    }
    Ok(deleted)
}

/// The DELETED-prefix duty (module doc): DELETED rows whose prefix still
/// lists objects, the prefix deleted.
async fn sweep_deleted_prefixes<S: BackupCatalog + ?Sized>(
    duty: Duty<'_, S>,
    identity: &StoreIdentity,
) -> Result<Vec<String>, StorageError> {
    let rows: Vec<BackupRow> = duty
        .storage
        .list_backup_rows(Some(BackupStatus::Deleted), identity)
        .try_collect()
        .await?;
    let mut swept = Vec::new();
    for row in rows {
        match delete_prefix_if_listed(duty, &row, "DELETED").await {
            Acted::Done => swept.push(row.backup_arn),
            Acted::Skipped => {}
            Acted::OutOfTime => break,
        }
    }
    Ok(swept)
}

/// Delete `row`'s prefix when it lists objects, once [`Duty::fenced`]
/// passed, within [`DUTY_DELETION_BOUND`]. A store failure or a refusal is
/// logged and left for the next cycle; a deletion that outlasts the bound
/// is dropped ([`Acted::OutOfTime`]).
async fn delete_prefix_if_listed<S: BackupCatalog + ?Sized>(
    duty: Duty<'_, S>,
    row: &BackupRow,
    status: &str,
) -> Acted {
    let store = duty.store;
    let prefix = match BackupArn::parse(&row.backup_arn) {
        Ok(arn) => arn.store_prefix(),
        Err(e) => {
            tracing::error!(
                "{status} row {} carries an ARN this build cannot parse; left as it is: {e}",
                row.backup_arn
            );
            return Acted::Skipped;
        }
    };
    match store.prefix_has_objects(&prefix).await {
        Ok(true) => {}
        Ok(false) => return Acted::Skipped,
        Err(e) => {
            tracing::error!(
                "could not list the prefix of {status} backup {}; the next cycle retries: {e}",
                row.backup_arn
            );
            return Acted::Skipped;
        }
    }
    let deadline = match duty.fenced(row, status, "delete the prefix").await {
        Fenced::Until(deadline) => deadline,
        Fenced::Skip => return Acted::Skipped,
        Fenced::OutOfTime => return Acted::OutOfTime,
    };
    // The store checks the deadline before each removal, so a call that
    // resumes past it (a paused process) starts no other; the timeout ends
    // a step that does not answer.
    match tokio::time::timeout_at(deadline, store.delete_prefix_before(&prefix, deadline)).await {
        Ok(Ok(done)) if done.finished => {
            tracing::info!(
                "deleted {} object(s) left under the prefix of {status} backup {} (account {})",
                done.deleted,
                row.backup_arn,
                row.account_id
            );
            Acted::Done
        }
        Ok(Ok(done)) => {
            tracing::warn!(
                "the deletion of the prefix of {status} backup {} (account {}) reached its {} s \
                 bound after {} object(s) and stopped; the duty stops for this cycle and the \
                 next cycle continues",
                row.backup_arn,
                row.account_id,
                DUTY_DELETION_BOUND.as_secs(),
                done.deleted
            );
            Acted::OutOfTime
        }
        Ok(Err(e)) => {
            tracing::error!(
                "could not delete the prefix of {status} backup {} (account {}); the next cycle \
                 retries: {e}",
                row.backup_arn,
                row.account_id
            );
            Acted::Skipped
        }
        Err(_) => {
            tracing::warn!(
                "the deletion of the prefix of {status} backup {} (account {}) did not end \
                 within {} s and was stopped; the duty stops for this cycle and the next cycle \
                 continues",
                row.backup_arn,
                row.account_id,
                DUTY_DELETION_BOUND.as_secs()
            );
            Acted::OutOfTime
        }
    }
}

/// The tombstone duty (module doc): FAILED and DELETED rows whose
/// `completed_at` is older than `retention` and whose prefix lists nothing
/// are removed, each removal within [`DUTY_DELETION_BOUND`] of the claim's
/// read; one that outlasts it stops the duty for this cycle.
async fn remove_tombstones<S: BackupCatalog + ?Sized>(
    duty: Duty<'_, S>,
    identity: &StoreIdentity,
    retention: Duration,
) -> Result<Vec<String>, StorageError> {
    let Duty { storage, store, .. } = duty;
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
            // The empty listing is a fact about this catalog's root only
            // while the root is this catalog's.
            let deadline = match duty
                .fenced(&row, &status.to_string(), "remove the row")
                .await
            {
                Fenced::Until(deadline) => deadline,
                Fenced::Skip => continue,
                Fenced::OutOfTime => return Ok(removed),
            };
            // Conditioned on the store the duty listed the row with: a row
            // `--move-store` moved since is the configured store's to
            // finish.
            let key = row.key();
            let removal =
                storage.remove_backup_row(&key, row.store.store_kind, &row.store.store_root);
            match tokio::time::timeout_at(deadline, removal).await {
                Ok(Ok(true)) => {
                    tracing::info!(
                        "removed the {status} row of backup {} (account {}): its prefix is empty \
                         and its retention has passed",
                        row.backup_arn,
                        row.account_id
                    );
                    removed.push(row.backup_arn);
                }
                Ok(Ok(false)) => {}
                Ok(Err(e)) => tracing::error!(
                    "could not remove the {status} row of backup {} (account {}); the next cycle \
                     retries: {e}",
                    row.backup_arn,
                    row.account_id
                ),
                Err(_) => {
                    tracing::warn!(
                        "the removal of the {status} row of backup {} (account {}) did not end \
                         within {} s and was stopped; the duty stops for this cycle and the next \
                         cycle continues",
                        row.backup_arn,
                        row.account_id,
                        DUTY_DELETION_BOUND.as_secs()
                    );
                    return Ok(removed);
                }
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
    fence: &StoreFence,
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
        match super::finish_backup_delete(storage, &store.calls("deletion"), fence, &arn).await {
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
