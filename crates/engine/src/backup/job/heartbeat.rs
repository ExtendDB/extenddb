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
//! insert stands for the first confirmed beat: the check measures from the
//! instant the job took before `claim_backup`, so the maintenance check
//! that runs between the insert and the handle's start (one catalog query
//! with no bound of its own) counts against the two periods, and a job
//! whose first put comes within two periods of the insert needs no beat of
//! its own; `stale_job_after_secs` is at least three periods, so a row that
//! passes the check by the two-period rule is never one a reconciler may
//! take over. The third clause applies only on SQLite, where no reconciler
//! takes over a row whose job runs in its process (the last paragraph).
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
//! row (AVAILABLE from a complete manifest, FAILED otherwise). On MongoDB
//! and Cassandra the row may have been taken over during the stop: a put in
//! flight at the stop may still land (the reconciler's FAILED-prefix retry
//! deletes it), and the job's next beat and status write match no row.
//!
//! The one-period bound is the backend's to place
//! ([`JobClaim::heartbeat_within`]): on SQLite the beat is an `UPDATE`
//! under the engine write lock, and the bound starts once the beat holds
//! that lock, so a writer holding it past a period (a GSI backfill) delays
//! the beat and does not end the job. The re-check before the manifest
//! ([`HeartbeatHandle::recheck`]) is the same call, and a confirmation it
//! gets counts as a beat for the pre-put check.
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
//! and `heartbeat_within` bounds the whole call, so the two-period rule
//! already covers a beat in flight, and the clause does not apply: a beat
//! in flight there says nothing about the row. A process stopped mid-beat
//! (SIGSTOP, a paused VM) and resumed keeps that beat in flight until its
//! timer runs again and raises the loss, and a process stopped between
//! beats starts the missed tick's beat at once on resume; in either window
//! another owner may already hold the row, and a clause that read the
//! flight passed the checks there (the maintainer measured 64 and 85 µs
//! with the clause unbounded, and 5 to 6 ms after a stop between beats with
//! it bounded to one period of the beat's start). A beat that returns Lost,
//! fails or outlives its bound raises the loss first, and the clause reads
//! the loss before the flight. A timer task that stops mid-beat (a panic in
//! the backend's beat) clears the flight on the way out, so the two-period
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
    /// Whether a beat (the timer's or the re-check's) is in flight; the
    /// pre-put check's third clause (module doc).
    in_flight: Arc<Mutex<bool>>,
    /// Whether the claim's beat can wait outside its bound
    /// ([`JobClaim::heartbeat_parks_before_its_bound`]): the third clause
    /// applies only where it can.
    parks: bool,
    lost: CancellationToken,
    reason: Arc<Mutex<Option<String>>>,
    period: Duration,
    timer: tokio::task::JoinHandle<()>,
}

/// Marks a beat in flight for as long as it lives, and clears the mark
/// when dropped, by the beat's return or by its task's end.
struct InFlight<'a>(&'a Mutex<bool>);

impl<'a> InFlight<'a> {
    fn mark(flag: &'a Mutex<bool>) -> Self {
        *flag.lock().unwrap_or_else(PoisonError::into_inner) = true;
        Self(flag)
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        *self.0.lock().unwrap_or_else(PoisonError::into_inner) = false;
    }
}

/// The third clause of the pre-put check (module doc): where the beat
/// parks before its bound, a beat in flight confirms the claim for as long
/// as it waits; elsewhere the clause does not apply.
fn in_flight_confirms(in_flight: &Mutex<bool>, parks: bool) -> bool {
    parks && *in_flight.lock().unwrap_or_else(PoisonError::into_inner)
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

impl HeartbeatHandle {
    /// Start the timer on `claim`, beating every `period` from one period
    /// after now; `claimed_at`, the instant of the claim's insert, stands
    /// for the first confirmed beat. With `metrics`, each confirmed beat
    /// records the snapshot's age (`BackupSnapshotAge`), seconds since
    /// `created_at`.
    pub(crate) fn start(
        claim: Arc<dyn JobClaim>,
        period: Duration,
        metrics: Option<Arc<MetricsCollector>>,
        created_at: f64,
        claimed_at: Instant,
    ) -> Self {
        let (tx, last_confirmed) = watch::channel(claimed_at);
        let confirmed = Arc::new(tx);
        let in_flight = Arc::new(Mutex::new(false));
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
                        if let Some(metrics) = &metrics {
                            metrics.record_backup_snapshot_age(seconds_since(created_at));
                        }
                        continue;
                    }
                    *reason.lock().unwrap_or_else(PoisonError::into_inner) =
                        Some(beat_failure(outcome, period));
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

    /// The pre-put check (module doc): no loss raised, and either the last
    /// confirmed beat at most two periods old or, on SQLite only, a beat in
    /// flight (parked on the write lock behind a long writer). The second
    /// clause is what catches a timer task that stopped running without
    /// raising the loss; the third never outlives its beat.
    pub(crate) fn confirms_the_claim(&self) -> bool {
        !self.lost.is_cancelled()
            && (self.last_confirmed.borrow().elapsed() <= 2 * self.period
                || in_flight_confirms(&self.in_flight, self.parks))
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
    /// running, for the test of the pre-put check's second clause.
    #[cfg(test)]
    pub(crate) fn stop_timer(&self) {
        self.timer.abort();
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
