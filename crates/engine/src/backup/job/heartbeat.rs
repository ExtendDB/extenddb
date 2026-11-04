// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The job's heartbeat: a timer task that refreshes the claim every period
//! for as long as the job runs, independent of upload progress, and the
//! two things the job reads from it: the instant of the last confirmed beat
//! (checked before every put) and the signal that the claim is lost.
//!
//! One rule decides both: a beat that does not confirm the claim
//! ([`Heartbeat::Lost`] or an error) or that has not returned within one
//! period ends the timer and raises the lost signal, after which the job
//! makes no further store or catalog access. The pre-put check is the same
//! rule read the other way: the last confirmed beat is at most two periods
//! old (one for the timer to fire, one for the beat to return, the bound
//! the timer itself enforces), or a beat is in flight (the last paragraph),
//! and no loss has been raised. The claim's
//! insert stands for the first confirmed beat, and the two jobs hand its
//! instant in by their own convention: the backup job takes it before
//! `claim_backup` (the row's `heartbeat_at` is stamped inside the insert, so
//! the instant is at or before the beat and the maintenance check that runs
//! between the insert and the handle's start, one catalog query with no
//! bound of its own, counts against the two periods); the restore job takes
//! it after its first beat returned confirmed, a beat it sends itself right
//! after `claim_restore` returned (the record's `heartbeat_at` as the
//! registration stamped it can be old: on PostgreSQL `now()` is the
//! transaction's start, so a registration stalled on a lock commits a stale
//! beat; the first beat refreshes it on the catalog's clock), and after
//! `claim_restore_dead` returned for a resume (the takeover stamped
//! `heartbeat_at` inside that write). Either way the instant is at or after
//! the beat it stands for, by a write's own duration, and a slow
//! registration does not end the job at its first check. A job
//! whose first write comes within two periods of its instant needs no beat
//! of its own; `stale_job_after_secs` is at least three periods, so a row
//! that passes the check is never one a reconciler may take over.
//!
//! A process stopped and resumed (SIGSTOP, a paused VM) meets the same
//! rule. On PostgreSQL its claim session keeps the advisory lock while it
//! is stopped, so no takeover happens while the session lives. On resume a
//! job with no beat in flight at the stop runs on under its own generation
//! once its next beat succeeds, however long the stop was (a job frozen
//! 160 s finished AVAILABLE: the timer's next tick beats, the beat confirms,
//! and the puts that follow find a fresh confirmation); it stops as a lost
//! claim with no catalog write only when a beat in flight at the stop
//! outlives its one-period bound, or when a put comes before the next beat
//! and finds the last confirmed beat more than two periods old. In that
//! case its claim closes, the lock is freed and the reconciler resolves the
//! row (AVAILABLE from a complete manifest, FAILED otherwise) or resumes a
//! store-backed restore from its last committed batch. On MongoDB and
//! Cassandra the row may have been taken over during the stop: a put in
//! flight at the stop may still land (the reconciler's FAILED-prefix retry
//! deletes it), and the job's next beat and status write match no row.
//!
//! The one-period bound is the backend's to place
//! ([`JobClaim::heartbeat_within`]): on SQLite the beat is an `UPDATE`
//! under the engine write lock, and the bound starts once the beat holds
//! that lock, so a writer holding it past a period (a restore's write
//! batches, item writes, a GSI backfill, the TTL sweep, a table's drop)
//! delays the beat and does not end the job; the SQLite restore engine's
//! module doc gives the batch times the bench record measured. The re-check
//! before the manifest ([`HeartbeatHandle::recheck`]) is the same call, and
//! a confirmation it gets counts as a beat for the pre-put check.
//!
//! The pre-put check has a third clause for the same case: a beat that is
//! in flight and has not failed confirms the claim, however old the last
//! confirmed beat is. Without it a backup large enough to reach a put while
//! a writer holds the lock past two periods stopped at that put (`no
//! heartbeat confirmed the claim recently enough`), its beat parked on the
//! lock and the claim intact (the maintainer measured it twice at the third
//! or fourth data file of a 180,000-item table beside a GSI build). The
//! clause is safe because a beat that is in flight is one no backend has
//! answered: on SQLite the beat parks only on this process's own write
//! lock, one process serves the file (the serve lock; `backup purge`
//! refuses while a server runs on it) and the reconciler's liveness test is
//! the in-process slot the job holds (design 4(e)), so a parked beat cannot
//! coincide with a takeover, whatever `heartbeat_at` reads meanwhile; the
//! backend says so through [`JobClaim::heartbeat_parks_before_its_bound`],
//! and there the clause holds for as long as the beat waits. On PostgreSQL,
//! MongoDB and Cassandra no lock of this process sits in front of the beat
//! and `heartbeat_within` bounds the whole call, and there the clause holds
//! only within one period of the beat's start, the bound itself: a process
//! stopped mid-beat (SIGSTOP, a paused VM) and resumed keeps the beat in
//! flight until its timer runs again and raises the loss, a window of tens
//! of microseconds in which another owner may already hold the row, and a
//! clause that read the flight alone passed the checks in that window (the
//! maintainer measured 64 and 85 µs). A beat that returns Lost, fails or
//! outlives its bound raises the loss first, and the clause reads the loss
//! before the flight. A timer task that stops mid-beat (a panic in the
//! backend's beat) clears the flight on the way out, so the two-period
//! clause is what remains for it.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use extenddb_core::metrics::MetricsCollector;
use extenddb_storage::CancellationToken;
use extenddb_storage::JobClaim;
use extenddb_storage::backup_catalog::Heartbeat;
use extenddb_storage::error::StorageError;
use tokio::sync::watch;
use tokio::time::Instant;

/// The job's side of the heartbeat timer. Dropping it stops the timer.
pub(crate) struct HeartbeatHandle {
    claim: Arc<dyn JobClaim>,
    confirmed: Arc<watch::Sender<Instant>>,
    last_confirmed: watch::Receiver<Instant>,
    /// The instant the beat in flight (the timer's or the re-check's)
    /// started, while one is; the pre-put check's third clause (module doc).
    in_flight: Arc<Mutex<Option<Instant>>>,
    /// Whether the claim's beat can wait outside its bound
    /// ([`JobClaim::heartbeat_parks_before_its_bound`]): the third clause
    /// holds for as long as the beat is in flight where it can, and for one
    /// period from the beat's start where it cannot.
    parks: bool,
    lost: CancellationToken,
    reason: Arc<Mutex<Option<String>>>,
    period: Duration,
    timer: tokio::task::JoinHandle<()>,
}

/// Marks a beat in flight, with its start, for as long as it lives, and
/// clears the mark when dropped, by the beat's return or by its task's end.
struct InFlight<'a>(&'a Mutex<Option<Instant>>);

impl<'a> InFlight<'a> {
    fn mark(flag: &'a Mutex<Option<Instant>>) -> Self {
        *flag.lock().unwrap_or_else(PoisonError::into_inner) = Some(Instant::now());
        Self(flag)
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }
}

/// The third clause of the pre-put check (module doc): a beat in flight
/// since `since` confirms the claim for as long as it waits where the beat
/// parks before its bound, and within one `period` of its start otherwise.
fn in_flight_confirms(in_flight: &Mutex<Option<Instant>>, parks: bool, period: Duration) -> bool {
    match *in_flight.lock().unwrap_or_else(PoisonError::into_inner) {
        Some(since) => parks || since.elapsed() <= period,
        None => false,
    }
}

/// Why a beat did not confirm the claim.
fn beat_failure(outcome: Option<Result<Heartbeat, StorageError>>, bound: Duration) -> String {
    match outcome {
        Some(Ok(Heartbeat::Confirmed)) => unreachable!("a confirmed beat is not a failure"),
        Some(Ok(Heartbeat::Lost)) => {
            "the claim was lost: the row is gone or carries another generation".to_owned()
        }
        Some(Err(e)) => format!("the heartbeat failed: {e}"),
        None => format!("the heartbeat did not return within {} s", bound.as_secs()),
    }
}

/// What a confirmed beat records, if anything: a backup job records the
/// snapshot's age, a restore job records nothing per beat.
pub(crate) enum HeartbeatMetric {
    /// `BackupSnapshotAge`, seconds since `created_at`, into `metrics`.
    BackupSnapshotAge {
        metrics: Arc<MetricsCollector>,
        created_at: f64,
    },
    /// No per-beat metric.
    None,
}

impl HeartbeatHandle {
    /// Start the timer on `claim`, beating every `period` from one period
    /// after now; `claimed_at`, the instant of the claim's insert as the
    /// job's convention takes it (module doc), stands for the first
    /// confirmed beat. Each confirmed beat records `metric`.
    pub(crate) fn start(
        claim: Arc<dyn JobClaim>,
        period: Duration,
        metric: HeartbeatMetric,
        claimed_at: Instant,
    ) -> Self {
        let (tx, last_confirmed) = watch::channel(claimed_at);
        let confirmed = Arc::new(tx);
        let in_flight = Arc::new(Mutex::new(None));
        let parks = claim.heartbeat_parks_before_its_bound();
        let lost = CancellationToken::new();
        let reason = Arc::new(Mutex::new(None));
        let timer = tokio::spawn({
            let claim = Arc::clone(&claim);
            let confirmed = Arc::clone(&confirmed);
            let in_flight = Arc::clone(&in_flight);
            let lost = lost.clone();
            let reason = Arc::clone(&reason);
            async move {
                let mut ticks = tokio::time::interval(period);
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                ticks.tick().await;
                loop {
                    ticks.tick().await;
                    let outcome = {
                        let _beat = InFlight::mark(&in_flight);
                        claim.heartbeat_within(period).await
                    };
                    if let Some(Ok(Heartbeat::Confirmed)) = outcome {
                        let _ = confirmed.send(Instant::now());
                        if let HeartbeatMetric::BackupSnapshotAge {
                            metrics,
                            created_at,
                        } = &metric
                        {
                            metrics.record_backup_snapshot_age(seconds_since(*created_at));
                        }
                        continue;
                    }
                    let why = beat_failure(outcome, period);
                    *reason.lock().unwrap_or_else(PoisonError::into_inner) = Some(why.clone());
                    tracing::warn!(
                        "the job's heartbeat raised the loss of its claim; the job stops at its \
                         next check: {why}"
                    );
                    lost.cancel();
                    return;
                }
            }
        });
        Self {
            claim,
            confirmed,
            last_confirmed,
            in_flight,
            parks,
            lost,
            reason,
            period,
            timer,
        }
    }

    /// The signal that the claim is lost; select on `lost().cancelled()`.
    pub(crate) fn lost(&self) -> &CancellationToken {
        &self.lost
    }

    /// Why the claim is lost, once it is.
    pub(crate) fn reason(&self) -> String {
        self.reason
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .unwrap_or_else(|| "the claim is lost".to_owned())
    }

    /// Why the pre-put check failed, for the job's stop line: the timer's
    /// own reason when it raised the loss ([`Self::reason`]), else the
    /// second clause, that no beat confirmed the claim within two periods,
    /// naming `step`, what the job was about to do ("to load X", "for the
    /// DONE write").
    pub(crate) fn why_not_confirmed(&self, step: &str) -> String {
        if self.lost.is_cancelled() {
            self.reason()
        } else {
            format!("no heartbeat confirmed the claim recently enough {step}")
        }
    }

    /// The pre-put check (module doc): no loss raised, and either the last
    /// confirmed beat at most two periods old or a beat in flight (parked on
    /// SQLite's write lock behind a long writer; within one period of its
    /// start on the other backends). The second clause is what catches a
    /// timer task that stopped running without raising the loss; the third
    /// never outlives its beat.
    pub(crate) fn confirms_the_claim(&self) -> bool {
        self.claim_check().confirms_the_claim()
    }

    /// The pre-put check as a value a blocking thread can hold (the
    /// restore's decoder checks it at every chunk boundary): the same three
    /// clauses read from the same watch, token and flag.
    pub(crate) fn claim_check(&self) -> ClaimCheck {
        ClaimCheck {
            last_confirmed: self.last_confirmed.clone(),
            lost: self.lost.clone(),
            in_flight: Arc::clone(&self.in_flight),
            parks: self.parks,
            period: self.period,
        }
    }

    /// One beat outside the timer, under the period's bound (the claim
    /// re-check before the manifest): a confirmation counts as a beat for
    /// [`Self::confirms_the_claim`]; anything else is the reason the claim
    /// is lost, with the timer's own text.
    pub(crate) async fn recheck(&self) -> Result<(), String> {
        let outcome = {
            let _beat = InFlight::mark(&self.in_flight);
            self.claim.heartbeat_within(self.period).await
        };
        if let Some(Ok(Heartbeat::Confirmed)) = outcome {
            let _ = self.confirmed.send(Instant::now());
            return Ok(());
        }
        Err(beat_failure(outcome, self.period))
    }

    /// End the timer without raising the loss: a timer task that stopped
    /// running, for the test of the pre-put check's second clause and for
    /// the restore's fenced-out kill point, which lets a reconciler take the
    /// record over while the job pauses.
    pub(crate) fn stop_timer(&self) {
        self.timer.abort();
    }

    /// A handle that ends the timer as [`Self::stop_timer`] does, for a test
    /// that holds no reference to the job's handle.
    #[cfg(test)]
    pub(crate) fn timer_abort_handle(&self) -> tokio::task::AbortHandle {
        self.timer.abort_handle()
    }
}

/// [`HeartbeatHandle::confirms_the_claim`] detached from the handle.
#[derive(Clone)]
pub(crate) struct ClaimCheck {
    last_confirmed: watch::Receiver<Instant>,
    lost: CancellationToken,
    in_flight: Arc<Mutex<Option<Instant>>>,
    parks: bool,
    period: Duration,
}

impl ClaimCheck {
    /// No loss raised, and either the last confirmed beat at most two
    /// periods old or a beat in flight within its bound (module doc).
    pub(crate) fn confirms_the_claim(&self) -> bool {
        !self.lost.is_cancelled()
            && (self.last_confirmed.borrow().elapsed() <= 2 * self.period
                || in_flight_confirms(&self.in_flight, self.parks, self.period))
    }
}

impl Drop for HeartbeatHandle {
    fn drop(&mut self) {
        self.timer.abort();
    }
}

/// Seconds between `epoch_secs` and now, on the wall clock.
pub(crate) fn seconds_since(epoch_secs: f64) -> f64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default();
    (now - epoch_secs).max(0.0)
}
