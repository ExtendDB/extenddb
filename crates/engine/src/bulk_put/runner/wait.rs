// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Waiting on the source while the lease is kept.
//!
//! The lease is renewed at a checkpoint, and checkpoints come between rows.
//! A source call can wait far longer than one lease: an S3 request that
//! gets no answer is retried for up to 120 seconds, and each attempt waits
//! up to 60 seconds for its answer. Without a renewal during the call the
//! lease would run out under the runner, the next write batch would drop
//! its staged rows and stop, and the next claim would read the same range
//! and meet the same fault, so the operation would never end while the
//! fault lasted.

use super::*;

/// What a wait on the source ended with.
pub(super) enum Waited<T> {
    /// The source answered.
    Answer(T),
    /// A boundary run during the wait ended the read: the lease was lost,
    /// the operation was cancelled, or its table or the caller's
    /// permission changed. The source call was dropped unanswered.
    Flow(Flow),
}

/// Await `call`, running a boundary while it waits so the lease does not
/// run out under the runner.
///
/// The boundary runs a third of the lease after the last renewal, and then
/// every third of the lease while the call still waits, but never before
/// the call has waited a sixth of the lease, so a short wait for the next
/// range never adds a checkpoint. It runs no later than a sixth of the
/// lease before the deadline. Every row staged at that point was read
/// before the call started, so the boundary writes them, checkpoints the
/// cursor after the last of them, and reads the cancel flag: a cancel is
/// seen during the wait.
pub(super) async fn keeping_lease<T>(
    ctx: &RunContext<'_>,
    st: &mut RunState,
    call: impl Future<Output = T>,
) -> Result<Waited<T>, RunnerError> {
    let lease = ctx.deps.config.lease;
    let started = Instant::now();
    let mut call = std::pin::pin!(call);
    loop {
        let Some(deadline) = st.lease_deadline else {
            return Ok(Waited::Answer(call.await));
        };
        let renew_at = renewal_instant(deadline, lease, started);
        // A server shutdown ends the wait at once: the boundary writes the
        // rows staged before the call, checkpoints, releases the lease and
        // stops the run (`boundary`).
        let shutdown = ctx.deps.shutdown.clone();
        tokio::select! {
            biased;
            answer = &mut call => return Ok(Waited::Answer(answer)),
            () = shutdown.cancelled() => {
                return boundary(ctx, st).await.map(Waited::Flow);
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(renew_at)) => {
                match boundary(ctx, st).await? {
                    Flow::NextRow => {}
                    flow => return Ok(Waited::Flow(flow)),
                }
            }
        }
    }
}

/// When a wait that started at `started` runs a boundary, for a lease of
/// `lease` that runs out at `deadline`.
pub(super) fn renewal_instant(deadline: Instant, lease: Duration, started: Instant) -> Instant {
    let sixth = lease / 6;
    let renewed = deadline.checked_sub(lease).unwrap_or(started);
    let due = (renewed + lease / 3).max(started + sixth);
    let latest = deadline.checked_sub(sixth).unwrap_or(deadline);
    due.min(latest)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEASE: Duration = Duration::from_secs(60);

    #[test]
    fn a_wait_after_a_fresh_renewal_runs_a_third_of_the_lease_after_it() {
        let renewed = Instant::now();
        let at = renewal_instant(renewed + LEASE, LEASE, renewed + Duration::from_secs(1));
        assert_eq!(at, renewed + Duration::from_secs(20));
    }

    #[test]
    fn a_wait_that_starts_late_waits_a_sixth_of_the_lease_first() {
        let renewed = Instant::now();
        let at = renewal_instant(renewed + LEASE, LEASE, renewed + Duration::from_secs(25));
        assert_eq!(at, renewed + Duration::from_secs(35));
    }

    #[test]
    fn a_wait_near_the_deadline_runs_a_sixth_of_the_lease_before_it() {
        let renewed = Instant::now();
        let at = renewal_instant(renewed + LEASE, LEASE, renewed + Duration::from_secs(45));
        assert_eq!(at, renewed + Duration::from_secs(50));
    }
}
