// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The holder's side of the catalog-level maintenance claim, and the steps a
//! maintenance command (`extenddb backup purge`) runs under it.
//!
//! [`HeldClaim`] is the handle the command keeps from the take to the
//! release, with the heartbeat running on a timer for the handle's whole
//! life, so every reader of the claim (`check_maintenance_claim` on every
//! backend measures the heartbeat's age, PostgreSQL the lock first) sees it
//! held through a wait and a deletion phase of any length. A heartbeat that
//! finds the claim lost (another holder took it over after this one looked
//! dead), or fails, is recorded for the command to stop at before its next
//! destructive step. Dropping the handle without `release` stops the timer
//! (the claim then expires by the stale rule, as a dead holder's does);
//! `release` stops it, waits for a heartbeat in flight, then releases.
//!
//! [`wait_for_quiet`] is the wait for the jobs the claim excludes from
//! starting to finish, and [`purge_row`] the deletion of one row's objects
//! and the row, each written against the catalog's trait so the engine's
//! tests run them against a double; the command prints what they return. An
//! [`Interrupt`] carries a signal the command received to both, so an
//! operator's Ctrl-C ends the wait at once and the deletion loop before its
//! next row, never inside one (the row's own steps are the idempotent
//! sequence a second deleter finishes).

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use extenddb_backup_store::BackupStore;
use extenddb_core::backup_format::{BackupArn, EXTENDDB_MANIFEST_FILE};
use extenddb_storage::backup_catalog::{
    BackupRow, BackupStatus, DeleteStart, Heartbeat, JobStatus, StoreIdentity, is_backup_not_found,
};
use extenddb_storage::{BackupCatalog, MaintenanceClaim};
use futures::TryStreamExt;
use tokio::sync::watch;
use tokio::time::Instant;

/// A maintenance claim with its heartbeat on a timer.
pub struct HeldClaim {
    claim: Arc<dyn MaintenanceClaim>,
    lost: Arc<Mutex<Option<String>>>,
    timer: tokio::task::JoinHandle<()>,
}

impl HeldClaim {
    /// Hold `claim`, heartbeating it every `interval` from now until
    /// [`HeldClaim::release`] or the drop (the caller passes the jobs'
    /// heartbeat period, `extenddb_storage::backup_catalog::JOB_HEARTBEAT_INTERVAL`,
    /// which every stale window the config accepts is at least three of).
    #[must_use]
    pub fn start(claim: Box<dyn MaintenanceClaim>, interval: Duration) -> Self {
        let claim: Arc<dyn MaintenanceClaim> = Arc::from(claim);
        let lost = Arc::new(Mutex::new(None));
        let timer = tokio::spawn({
            let claim = Arc::clone(&claim);
            let lost = Arc::clone(&lost);
            async move {
                let mut ticks = tokio::time::interval(interval);
                ticks.tick().await;
                loop {
                    ticks.tick().await;
                    let reason = match claim.heartbeat().await {
                        Ok(Heartbeat::Confirmed) => continue,
                        Ok(Heartbeat::Lost) => {
                            "the maintenance claim was lost (another holder took it over)"
                                .to_owned()
                        }
                        Err(e) => format!("the maintenance claim's heartbeat failed: {e}"),
                    };
                    *lost.lock().unwrap_or_else(PoisonError::into_inner) = Some(reason);
                    return;
                }
            }
        });
        Self { claim, lost, timer }
    }

    /// The claim's fencing token.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.claim.generation()
    }

    /// Why the claim is no longer held, once a heartbeat said so; `None`
    /// while every heartbeat has confirmed it.
    #[must_use]
    pub fn lost(&self) -> Option<String> {
        self.lost
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Stop the heartbeat, wait for one in flight to end, and release the
    /// claim.
    ///
    /// # Errors
    ///
    /// The backend's, when the release could not be written; the claim then
    /// expires by the stale rule.
    pub async fn release(mut self) -> Result<(), extenddb_storage::error::StorageError> {
        self.timer.abort();
        // The abort lands at the task's next await; a heartbeat already sent
        // finishes first, so the release never overlaps it on the claim's
        // connection. A cancelled task is the expected outcome.
        let _ = (&mut self.timer).await;
        self.claim.release().await
    }
}

impl Drop for HeldClaim {
    fn drop(&mut self) {
        self.timer.abort();
    }
}

/// A signal the command received (`SIGINT` or `SIGTERM`), for the wait and
/// the deletion loop to stop at: the wait at once, the loop before its next
/// row. The sender side is the command's signal listener; a test's is the
/// test.
#[derive(Clone)]
pub struct Interrupt {
    rx: watch::Receiver<Option<&'static str>>,
}

impl Interrupt {
    /// A pair: the sender a signal listener sets with the signal's name, and
    /// the handle the wait and the loop read.
    #[must_use]
    pub fn new() -> (watch::Sender<Option<&'static str>>, Self) {
        let (tx, rx) = watch::channel(None);
        (tx, Self { rx })
    }

    /// The signal's name once one was received.
    #[must_use]
    pub fn signalled(&self) -> Option<&'static str> {
        *self.rx.borrow()
    }

    /// Resolves with the signal's name once one is received; pending until
    /// then (forever, when the sender is gone and none was).
    pub async fn wait(&mut self) -> &'static str {
        loop {
            if let Some(name) = *self.rx.borrow_and_update() {
                return name;
            }
            if self.rx.changed().await.is_err() {
                return std::future::pending().await;
            }
        }
    }
}

/// How [`wait_for_quiet`] ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitOutcome {
    /// No job the claim excludes is running: the deletion may start.
    Quiet,
    /// The limit passed with these jobs still running (the operator's text,
    /// one per job).
    Busy(Vec<String>),
    /// A heartbeat found the claim lost or failed; the reason.
    Lost(String),
    /// The rows could not be read; the backend's error.
    CatalogError(String),
    /// The command received this signal.
    Interrupted(&'static str),
}

/// The durations of a [`wait_for_quiet`].
#[derive(Debug, Clone, Copy)]
pub struct Wait {
    /// A heartbeat within this of now (this host's clock) is a live owner;
    /// zero where every owner is known dead (SQLite under the serve lock).
    pub owner_window: Duration,
    /// How long to wait before giving up with the jobs still running.
    pub limit: Duration,
    /// How often the rows are re-read.
    pub poll: Duration,
}

/// Wait until no CREATING or FAILED backup row of `identity` has a live
/// owner and no restore job is RUNNING, re-reading every `wait.poll`, for
/// at most `wait.limit`; the claim lost, a catalog read failing, or a
/// signal ends it early. The jobs found running are reported through
/// `announce` the first time they are seen.
pub async fn wait_for_quiet<S: BackupCatalog + ?Sized>(
    storage: &S,
    identity: &StoreIdentity,
    claim: &HeldClaim,
    wait: Wait,
    interrupt: &mut Interrupt,
    announce: impl Fn(&[String]),
) -> WaitOutcome {
    let started = Instant::now();
    let mut announced = false;
    loop {
        if let Some(reason) = claim.lost() {
            return WaitOutcome::Lost(reason);
        }
        if let Some(signal) = interrupt.signalled() {
            return WaitOutcome::Interrupted(signal);
        }
        let remaining = match busy_jobs(storage, identity, wait.owner_window).await {
            Ok(remaining) => remaining,
            Err(e) => return WaitOutcome::CatalogError(e),
        };
        if remaining.is_empty() {
            return WaitOutcome::Quiet;
        }
        if started.elapsed() >= wait.limit {
            return WaitOutcome::Busy(remaining);
        }
        if !announced {
            announce(&remaining);
            announced = true;
        }
        tokio::select! {
            () = tokio::time::sleep(wait.poll) => {}
            signal = interrupt.wait() => return WaitOutcome::Interrupted(signal),
        }
    }
}

/// The jobs purge waits on, as text for the operator.
async fn busy_jobs<S: BackupCatalog + ?Sized>(
    storage: &S,
    identity: &StoreIdentity,
    stale_after: Duration,
) -> Result<Vec<String>, String> {
    let mut busy = Vec::new();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64());
    // A live owner heartbeats on the catalog's clock; this host's clock
    // stands in for it here, so a skew of up to the stale window makes the
    // wait longer, never shorter than the owner's death. A FAILED row whose
    // heartbeat is young may still be written to by a job that has not seen
    // its claim lost; its prefix waits for the window as the reconciler's
    // deletion of it does.
    for status in [BackupStatus::Creating, BackupStatus::Failed] {
        let rows: Vec<BackupRow> = storage
            .list_backup_rows(Some(status), identity)
            .try_collect()
            .await
            .map_err(|e| format!("cannot list the catalog's backup rows: {e}"))?;
        for row in rows {
            if row.heartbeat_at >= now - stale_after.as_secs_f64() {
                busy.push(format!(
                    "{} {} has a live owner (heartbeat within stale_job_after_secs)",
                    if status == BackupStatus::Creating {
                        "CreateBackup"
                    } else {
                        "the FAILED backup"
                    },
                    row.backup_arn
                ));
            }
        }
    }
    // Every RUNNING restore job: the dead-job listing with a zero window
    // lists every RUNNING record whose heartbeat is in the past, which is
    // all of them. It also lists the FAILED records whose target is still
    // CREATING (the FAILED-target duty's), which are the reconciler's to
    // finish, not jobs: nothing runs under one, and purge deletes no table,
    // so they are not waited on.
    let listed = storage
        .list_dead_restore_jobs(Duration::ZERO)
        .await
        .map_err(|e| format!("cannot list restore jobs: {e}"))?;
    for job in listed {
        if job.status != JobStatus::Running {
            continue;
        }
        busy.push(format!(
            "a restore into {} (account {}) is RUNNING",
            job.target.table_name, job.target.account_id
        ));
    }
    Ok(busy)
}

/// Delete one row's objects and the row: the manifest first, then the
/// prefix, then the row through the status machine to a tombstone and out.
/// The row is `row` as the command read it; the status another deleter may
/// have moved it to since is read from `start_backup_delete`'s answer, and
/// each state the row can be in when the command reaches it ends with no
/// row: AVAILABLE is moved to DELETING here; DELETING (another deleter is
/// mid-way) and DELETED (another finished) are finished here, the prefix
/// deletion being the same idempotent sequence on both sides; a row already
/// gone counts as deleted. A CREATING row's owner is dead (the wait saw no
/// live one on this host's clock): the row goes first, through the
/// dead-owner removal on the catalog's clock, and the prefix only once the
/// row is gone, so a clock skew that makes the catalog still see a young
/// heartbeat leaves the row and its objects together rather than a row over
/// a deleted prefix. A prefix left by a failure between the two steps holds
/// no manifest (a CREATING backup has none yet), so nothing adopts it.
///
/// # Errors
///
/// The reason the row was left, for the operator: a FAILED or CREATING
/// status another writer moved an AVAILABLE row to (neither follows
/// AVAILABLE; the row is left for a look), a restore reading the backup, a
/// CREATING row whose owner heartbeated after the wait, a store or catalog
/// call failing, or a prefix that still lists an object.
pub async fn purge_row<S: BackupCatalog + ?Sized>(
    storage: &S,
    store: &dyn BackupStore,
    row: &BackupRow,
    stale_after: Duration,
) -> Result<(), String> {
    let arn = BackupArn::parse(&row.backup_arn)
        .map_err(|e| format!("the row's ARN does not parse: {e}"))?;
    let key = row.key();
    if row.status == BackupStatus::Creating
        && !storage
            .remove_dead_creating_backup_row(&key, stale_after)
            .await
            .map_err(|e| format!("cannot remove the CREATING row: {e}"))?
    {
        return Err(
            "the CREATING row could not be removed (its owner heartbeated after the wait, or its \
             status changed); its objects are left with it"
                .to_owned(),
        );
    }
    // The status to finish from: the row's as read, or as another deleter
    // left it.
    let mut status = row.status;
    if status == BackupStatus::Available {
        match storage.start_backup_delete(&key).await {
            Ok(DeleteStart::Deleting | DeleteStart::Status(BackupStatus::Deleting)) => {
                status = BackupStatus::Deleting;
            }
            Ok(DeleteStart::Status(BackupStatus::Deleted)) => status = BackupStatus::Deleted,
            Ok(DeleteStart::InUse(table)) => {
                return Err(format!("a restore into {table} is reading it"));
            }
            Ok(DeleteStart::Status(other)) => {
                return Err(format!("its status changed to {other}"));
            }
            Err(e) if is_backup_not_found(&e) => return Ok(()),
            Err(e) => return Err(format!("cannot mark the row DELETING: {e}")),
        }
    }
    let prefix = arn.store_prefix();
    store
        .delete_prefix(&format!("{prefix}{EXTENDDB_MANIFEST_FILE}"))
        .await
        .map_err(|e| format!("cannot delete the manifest: {e}"))?;
    store
        .delete_prefix(&prefix)
        .await
        .map_err(|e| format!("cannot delete the prefix: {e}"))?;
    if let Some(left) = store
        .list(&prefix)
        .try_next()
        .await
        .map_err(|e| format!("cannot list the prefix after the deletion: {e}"))?
    {
        return Err(format!(
            "the prefix still lists {} after the deletion",
            left.key
        ));
    }
    if status == BackupStatus::Creating {
        return Ok(());
    }
    if status == BackupStatus::Deleting {
        storage
            .set_backup_deleted(&key)
            .await
            .map_err(|e| format!("cannot mark the row DELETED: {e}"))?;
    }
    if storage
        .remove_backup_row(&key)
        .await
        .map_err(|e| format!("cannot remove the row: {e}"))?
    {
        return Ok(());
    }
    // Not a tombstone: another deleter removed it, or a writer moved it.
    match storage
        .read_backup_row(&arn)
        .await
        .map_err(|e| format!("cannot re-read the row: {e}"))?
    {
        None => Ok(()),
        Some(current) => Err(format!(
            "the row could not be removed (it is {} now, not a tombstone)",
            current.status
        )),
    }
}

#[cfg(test)]
mod tests {
    use extenddb_storage::error::StorageError;
    use futures::future::BoxFuture;
    use tokio::time::Instant;

    use super::*;

    /// A claim whose heartbeats are timestamps on tokio's clock, with the age
    /// rule every backend's `check_maintenance_claim` applies.
    #[derive(Default)]
    struct FakeClaim {
        beats: Mutex<Vec<Instant>>,
        lost: Mutex<bool>,
        released: Mutex<bool>,
    }

    impl FakeClaim {
        /// `check_maintenance_claim(stale)`: held while the last heartbeat is
        /// within `stale`.
        fn held(&self, stale: Duration) -> bool {
            self.beats
                .lock()
                .unwrap()
                .last()
                .is_some_and(|at| at.elapsed() < stale)
        }
        fn beats(&self) -> usize {
            self.beats.lock().unwrap().len()
        }
    }

    impl MaintenanceClaim for FakeClaim {
        fn generation(&self) -> u64 {
            7
        }
        fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>> {
            let lost = *self.lost.lock().unwrap();
            if !lost {
                self.beats.lock().unwrap().push(Instant::now());
            }
            Box::pin(async move {
                Ok(if lost {
                    Heartbeat::Lost
                } else {
                    Heartbeat::Confirmed
                })
            })
        }
        fn release(&self) -> BoxFuture<'_, Result<(), StorageError>> {
            *self.released.lock().unwrap() = true;
            Box::pin(async { Ok(()) })
        }
    }

    /// The claim object handed to `HeldClaim`, forwarding to the test's own.
    struct ForwardingClaim(Arc<FakeClaim>);

    impl MaintenanceClaim for ForwardingClaim {
        fn generation(&self) -> u64 {
            self.0.generation()
        }
        fn heartbeat(&self) -> BoxFuture<'_, Result<Heartbeat, StorageError>> {
            self.0.heartbeat()
        }
        fn release(&self) -> BoxFuture<'_, Result<(), StorageError>> {
            self.0.release()
        }
    }

    fn held_over(fake: &Arc<FakeClaim>, interval: Duration) -> HeldClaim {
        fake.beats.lock().unwrap().push(Instant::now());
        HeldClaim::start(Box::new(ForwardingClaim(Arc::clone(fake))), interval)
    }

    /// On a paused clock (the timer and the samples share it, so a stall of
    /// the test process cannot read as a missed heartbeat): a held claim
    /// stays "held" to the jobs' check through a run of thirty stale
    /// windows with no heartbeat from the holder's own loop, a lost
    /// heartbeat is reported, and the release stops the timer.
    #[tokio::test(start_paused = true)]
    async fn the_heartbeat_timer_keeps_the_claim_held_through_a_long_run() {
        let fake = Arc::new(FakeClaim::default());
        let stale = Duration::from_secs(3);
        let held = held_over(&fake, Duration::from_secs(1));
        assert_eq!(held.generation(), 7);
        let started = Instant::now();
        let mut samples = 0;
        while started.elapsed() < Duration::from_secs(90) {
            assert!(
                fake.held(stale),
                "the claim read as free {:?} after the take",
                started.elapsed()
            );
            assert_eq!(held.lost(), None);
            samples += 1;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert!(samples >= 150, "{samples}");
        assert!(
            fake.beats() >= 85,
            "the timer heartbeated: {}",
            fake.beats()
        );
        // Taken over: the next heartbeat finds the claim lost.
        *fake.lost.lock().unwrap() = true;
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        assert!(
            held.lost().is_some_and(|reason| reason.contains("lost")),
            "{:?}",
            held.lost()
        );
        let beats = fake.beats();
        held.release().await.unwrap();
        assert!(*fake.released.lock().unwrap());
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(fake.beats(), beats, "no heartbeat after the release");
    }

    /// A handle dropped without `release` stops its timer: the claim gets no
    /// further heartbeat and expires by the stale rule, as a dead holder's
    /// does, rather than being kept alive by a detached task.
    #[tokio::test(start_paused = true)]
    async fn a_dropped_handle_stops_the_heartbeat() {
        let fake = Arc::new(FakeClaim::default());
        let held = held_over(&fake, Duration::from_secs(1));
        tokio::time::sleep(Duration::from_millis(3_500)).await;
        let before = fake.beats();
        assert!(before >= 3, "{before}");
        drop(held);
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(fake.beats(), before, "a dropped handle heartbeats no more");
        assert!(!*fake.released.lock().unwrap(), "a drop is not a release");
        assert!(
            !fake.held(Duration::from_secs(3)),
            "the claim expires by the stale rule"
        );
    }
}
