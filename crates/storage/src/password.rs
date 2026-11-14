// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! bcrypt on a blocking thread, bounded by one process-wide semaphore.
//!
//! bcrypt takes about 100 ms per call at the default cost, so it runs on
//! tokio's blocking pool. Hashing and verification share one semaphore so
//! that no more than [`MAX_CONCURRENT_BCRYPT`] bcrypt calls run at once in
//! this process: a burst of login requests cannot occupy more than that many
//! cores, and the sqlx blocking threads that share the pool are not starved.
//!
//! Every backend's password verification goes through [`verify_password`],
//! and the management API hashes through [`hash_password`], so the bound
//! holds whatever store a login is checked against. When a login names no
//! principal with a password, [`verify_stored_password`] checks the password
//! against a fixed hash, so the login waits for a slot and does the same
//! bcrypt work as a wrong password.
//!
//! A slot is held by the blocking call itself, not by the async caller. A
//! caller whose future is dropped while bcrypt runs (a client that closes
//! the connection mid-login) cannot release its slot early: the slot frees
//! when bcrypt returns.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{Semaphore, SemaphorePermit};

use crate::management_store::{OpError, OpResult};

/// Maximum bcrypt calls (hash or verify) running at once in this process.
pub const MAX_CONCURRENT_BCRYPT: usize = 4;

/// How long a verification waits for a bcrypt slot before giving up. A login
/// that waits longer than this is answered with an error rather than queued
/// without bound; it is not recorded as a failed attempt because no password
/// was checked.
pub const VERIFY_WAIT: Duration = Duration::from_secs(10);

/// The `OpError::Internal` message of a verification that found no free
/// bcrypt slot within [`VERIFY_WAIT`]. Callers match on it to log the
/// overload apart from storage errors.
pub const VERIFY_OVERLOADED: &str = "password verification is overloaded; try again later";

/// A bcrypt hash, at the default cost, of a random value that was discarded
/// once the hash was made. [`verify_stored_password`] checks a password
/// against it when a principal has no stored hash, so that a login for a name
/// that does not exist runs the same bcrypt work as a wrong password for a
/// name that does. That check refuses the login whatever bcrypt returns: a
/// password that matched this hash would still be refused, so knowing the
/// hash does not help a caller log in.
const NO_STORED_HASH: &str = "$2b$12$L4yNgP/xgOa4sckbcBNixutZwPfrRn3iJsfX1cMGumbupNRaepaEy";

static BCRYPT_SEMAPHORE: Semaphore = Semaphore::const_new(MAX_CONCURRENT_BCRYPT);

/// bcrypt calls running right now (on the blocking pool).
static RUNNING: AtomicUsize = AtomicUsize::new(0);

/// The most bcrypt calls ever observed running at once in this process.
static HIGH_WATER: AtomicUsize = AtomicUsize::new(0);

#[cfg(any(test, feature = "test-util"))]
tokio::task_local! {
    /// The bcrypt cost of each verification run inside the current
    /// [`count_verifications`] scope.
    static VERIFICATIONS: std::cell::RefCell<Vec<u32>>;
}

/// Run `future` and return its output with the bcrypt cost of each
/// verification it ran through [`verify_password`], in order. A verification
/// is recorded when bcrypt has checked the password against the hash, with
/// the cost read from the hash. A hash that bcrypt refuses with an error,
/// such as one that does not parse, records nothing. Verifications in tasks
/// that `future` spawns are not recorded, so tests that run bcrypt at the
/// same time do not change each other's records.
#[cfg(any(test, feature = "test-util"))]
pub async fn count_verifications<F: std::future::Future>(future: F) -> (F::Output, Vec<u32>) {
    VERIFICATIONS
        .scope(std::cell::RefCell::default(), async move {
            let output = future.await;
            (output, VERIFICATIONS.with(std::cell::RefCell::take))
        })
        .await
}

fn semaphore_closed() -> OpError {
    OpError::Internal("bcrypt semaphore closed".to_owned())
}

/// Whether `error` is the refusal [`verify_password`] returns when no bcrypt
/// slot became free within [`VERIFY_WAIT`].
#[must_use]
pub fn is_overloaded(error: &OpError) -> bool {
    matches!(error, OpError::Internal(msg) if msg == VERIFY_OVERLOADED)
}

/// Hash `password` with bcrypt at `cost` on a blocking thread, holding one
/// bcrypt slot until bcrypt returns.
///
/// Waits as long as it takes for a slot: the caller is an authenticated
/// admin creating or changing a password, and a queue is preferable to an
/// error.
///
/// # Errors
///
/// Returns `OpError::Internal` when bcrypt refuses the input or the blocking
/// task failed.
pub async fn hash_password(password: String, cost: u32) -> OpResult<String> {
    let permit = BCRYPT_SEMAPHORE
        .acquire()
        .await
        .map_err(|_| semaphore_closed())?;
    run_bcrypt(permit, move || bcrypt::hash(password, cost))
        .await?
        .map_err(|e| OpError::Internal(format!("bcrypt hash: {e}")))
}

/// Verify `password` against a bcrypt `hash` on a blocking thread, holding
/// one bcrypt slot until bcrypt returns.
///
/// Returns `Ok(false)` for a wrong password and for a hash bcrypt cannot
/// parse, matching how the stores treated a malformed hash before.
///
/// # Errors
///
/// Returns `OpError::Internal` with [`VERIFY_OVERLOADED`] when no bcrypt
/// slot became free within [`VERIFY_WAIT`], and `OpError::Internal` when the
/// blocking task failed. Callers treat both as a server error, not as a
/// failed login.
pub async fn verify_password(password: String, hash: String) -> OpResult<bool> {
    let Ok(acquired) = tokio::time::timeout(VERIFY_WAIT, BCRYPT_SEMAPHORE.acquire()).await else {
        tracing::warn!(
            "password verification waited {}s for a bcrypt slot; rejecting",
            VERIFY_WAIT.as_secs()
        );
        return Err(OpError::Internal(VERIFY_OVERLOADED.to_owned()));
    };
    let permit = acquired.map_err(|_| semaphore_closed())?;
    #[cfg(any(test, feature = "test-util"))]
    let cost = hash
        .parse::<bcrypt::HashParts>()
        .ok()
        .map(|parts| parts.get_cost());
    let checked = run_bcrypt(permit, move || bcrypt::verify(&password, &hash)).await?;
    #[cfg(any(test, feature = "test-util"))]
    if let (Some(cost), Ok(_)) = (cost, &checked) {
        let _ = VERIFICATIONS.try_with(|costs| costs.borrow_mut().push(cost));
    }
    Ok(checked.unwrap_or(false))
}

/// Verify `password` against a principal's stored bcrypt hash. `stored_hash`
/// is `None` when the principal does not exist or has no password.
///
/// Without a stored hash, the password is checked against a fixed hash at the
/// default cost and refused whatever bcrypt returns. That check waits for a
/// bcrypt slot and runs one bcrypt verification, as a check against a stored
/// hash does. A lookup that finds no principal therefore does the same bcrypt
/// work as a wrong password.
///
/// # Errors
///
/// As [`verify_password`], with or without a stored hash: a check for a
/// principal that does not exist fails with the same errors, including
/// [`VERIFY_OVERLOADED`], as a check for one that does.
pub async fn verify_stored_password(
    password: String,
    stored_hash: Option<String>,
) -> OpResult<bool> {
    match stored_hash {
        Some(hash) => verify_password(password, hash).await,
        None => {
            verify_password(password, NO_STORED_HASH.to_owned()).await?;
            Ok(false)
        }
    }
}

/// Run one bcrypt call on the blocking pool. The permit moves into the
/// blocking closure and is dropped there after the call returns, so the slot
/// stays taken for the whole call even if the caller's future is dropped
/// while it waits for the result.
async fn run_bcrypt<T: Send + 'static>(
    permit: SemaphorePermit<'static>,
    work: impl FnOnce() -> T + Send + 'static,
) -> OpResult<T> {
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let running = RUNNING.fetch_add(1, Ordering::SeqCst) + 1;
        HIGH_WATER.fetch_max(running, Ordering::SeqCst);
        let out = work();
        RUNNING.fetch_sub(1, Ordering::SeqCst);
        out
    })
    .await
    .map_err(|e| OpError::Internal(format!("bcrypt task: {e}")))
}

/// The most bcrypt calls observed running at once since the process started.
/// Never exceeds [`MAX_CONCURRENT_BCRYPT`].
#[must_use]
pub fn high_water_mark() -> usize {
    HIGH_WATER.load(Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;

    const COST: u32 = 4; // cheapest bcrypt cost; keeps the tests fast

    /// Tests that read `RUNNING` or the high-water mark take this lock, so
    /// their bcrypt calls are the only ones in flight in this process.
    static COUNTERS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Check the result of a wrong password queued behind other bcrypt calls:
    /// refused as wrong, or refused as overloaded when the host is slow
    /// enough that no slot freed within [`VERIFY_WAIT`].
    fn assert_wrong_or_overloaded(result: OpResult<bool>) {
        match result {
            Ok(verified) => assert!(!verified, "a wrong password verified"),
            Err(e) => assert!(is_overloaded(&e), "unexpected error: {e:?}"),
        }
    }

    /// Wait until `n` bcrypt calls are running on the blocking pool.
    async fn wait_until_running(n: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while RUNNING.load(Ordering::SeqCst) < n {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{n} bcrypt calls never started"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Start `MAX_CONCURRENT_BCRYPT` slow bcrypt calls through `start`, cancel
    /// their callers while bcrypt runs, then run `MAX_CONCURRENT_BCRYPT` fast
    /// verifications and return the peak number of bcrypt calls that ran at
    /// once. A slot released by the cancelled caller lets a fast call start
    /// beside a slow one still running, which pushes the peak past the bound.
    async fn peak_after_cancelling<F, Fut>(start: F) -> usize
    where
        F: Fn() -> Fut,
        Fut: std::future::Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let callers: Vec<_> = (0..MAX_CONCURRENT_BCRYPT)
            .map(|_| tokio::spawn(start()))
            .collect();
        wait_until_running(MAX_CONCURRENT_BCRYPT).await;
        for caller in &callers {
            caller.abort();
        }
        for caller in callers {
            let Err(cancelled) = caller.await else {
                panic!("the caller finished before it was cancelled");
            };
            assert!(cancelled.is_cancelled());
        }

        let fast_hash = bcrypt::hash("pw", COST).unwrap();
        let fresh: Vec<_> = (0..MAX_CONCURRENT_BCRYPT)
            .map(|_| {
                let hash = fast_hash.clone();
                tokio::spawn(async move { verify_password("wrong".to_owned(), hash).await })
            })
            .collect();
        for task in fresh {
            assert_wrong_or_overloaded(task.await.unwrap());
        }
        // The cancelled calls run to completion on the blocking pool.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while RUNNING.load(Ordering::SeqCst) > 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "bcrypt calls never finished"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        high_water_mark()
    }

    #[tokio::test]
    async fn verify_accepts_the_right_password_and_refuses_a_wrong_one() {
        let _counters = COUNTERS.lock().await;
        let hash = bcrypt::hash("correct horse", COST).unwrap();
        assert!(
            verify_password("correct horse".to_owned(), hash.clone())
                .await
                .unwrap()
        );
        assert!(
            !verify_password("battery staple".to_owned(), hash)
                .await
                .unwrap()
        );
    }

    #[test]
    fn the_fixed_hash_is_a_bcrypt_hash_at_the_default_cost() {
        let parts: bcrypt::HashParts = NO_STORED_HASH.parse().unwrap();
        assert_eq!(parts.get_cost(), bcrypt::DEFAULT_COST);
        // A hash bcrypt cannot decode fails before any work is done, which
        // would make a check without a stored hash return at once.
        assert!(bcrypt::verify("any password", NO_STORED_HASH).is_ok());
    }

    #[tokio::test]
    async fn a_check_without_a_stored_hash_runs_one_verification_and_refuses() {
        let _counters = COUNTERS.lock().await;
        let hash = bcrypt::hash("correct horse", COST).unwrap();
        let (wrong, wrong_costs) = count_verifications(verify_stored_password(
            "battery staple".to_owned(),
            Some(hash),
        ))
        .await;
        assert!(!wrong.unwrap(), "a wrong password verified");
        assert_eq!(wrong_costs, [COST]);
        let (absent, absent_costs) =
            count_verifications(verify_stored_password("battery staple".to_owned(), None)).await;
        assert!(!absent.unwrap(), "a principal with no stored hash verified");
        assert_eq!(
            absent_costs,
            [bcrypt::DEFAULT_COST],
            "a check without a stored hash must run one verification at the default cost"
        );
    }

    #[tokio::test]
    async fn a_check_without_a_stored_hash_waits_for_a_bcrypt_slot() {
        let _counters = COUNTERS.lock().await;
        let slots = u32::try_from(MAX_CONCURRENT_BCRYPT).unwrap();
        let held = BCRYPT_SEMAPHORE.acquire_many(slots).await.unwrap();
        let check = tokio::spawn(count_verifications(verify_stored_password(
            "pw".to_owned(),
            None,
        )));
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        // Every slot is held here, so the check cannot have run.
        assert!(!check.is_finished(), "the check finished without a slot");
        assert_eq!(RUNNING.load(Ordering::SeqCst), 0);
        drop(held);
        let (result, costs) = check.await.unwrap();
        assert!(!result.unwrap());
        assert_eq!(costs, [bcrypt::DEFAULT_COST]);
    }

    #[tokio::test]
    async fn a_hash_bcrypt_refuses_records_no_verification() {
        let _counters = COUNTERS.lock().await;
        // The second hash has bcrypt's three parts and cost 12, but its salt
        // is not bcrypt's base64, so bcrypt refuses it before hashing.
        let unreadable_salt = format!("$2b$12${}", "!".repeat(53));
        assert!(unreadable_salt.parse::<bcrypt::HashParts>().is_ok());
        for hash in ["not a bcrypt hash".to_owned(), unreadable_salt] {
            assert!(bcrypt::verify("x", &hash).is_err(), "{hash}");
            let (verified, costs) =
                count_verifications(verify_password("x".to_owned(), hash)).await;
            assert!(!verified.unwrap());
            assert!(costs.is_empty(), "a refused hash was recorded: {costs:?}");
        }
    }

    #[tokio::test]
    async fn a_malformed_hash_is_a_wrong_password_not_an_error() {
        let _counters = COUNTERS.lock().await;
        assert!(
            !verify_password("x".to_owned(), "not a bcrypt hash".to_owned())
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn hash_produces_a_hash_that_verifies() {
        let _counters = COUNTERS.lock().await;
        let hash = hash_password("correct horse".to_owned(), COST)
            .await
            .unwrap();
        assert!(
            verify_password("correct horse".to_owned(), hash)
                .await
                .unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_cancelled_verification_keeps_its_slot_until_bcrypt_returns() {
        let _counters = COUNTERS.lock().await;
        // Cost 12 keeps each cancelled call running long after its caller is
        // gone, so a fast call admitted early would overlap it.
        let slow_hash = bcrypt::hash("pw", 12).unwrap();
        let peak = peak_after_cancelling(|| {
            let hash = slow_hash.clone();
            async move { verify_password("wrong".to_owned(), hash).await }
        })
        .await;
        assert!(
            peak <= MAX_CONCURRENT_BCRYPT,
            "{peak} bcrypt calls ran at once after cancelled verifications; \
             the bound is {MAX_CONCURRENT_BCRYPT}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_cancelled_hash_keeps_its_slot_until_bcrypt_returns() {
        let _counters = COUNTERS.lock().await;
        let peak =
            peak_after_cancelling(|| async { hash_password("pw".to_owned(), 12).await }).await;
        assert!(
            peak <= MAX_CONCURRENT_BCRYPT,
            "{peak} bcrypt calls ran at once after cancelled hashes; \
             the bound is {MAX_CONCURRENT_BCRYPT}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_burst_of_verifications_never_runs_more_than_the_bound_at_once() {
        let _counters = COUNTERS.lock().await;
        // Cost 10 keeps each verify around 50 ms so the burst overlaps for
        // real; with 32 requests and 4 slots the burst takes 8 rounds.
        let hash = bcrypt::hash("pw", 10).unwrap();
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let hash = hash.clone();
            tasks.push(tokio::spawn(async move {
                verify_password("wrong".to_owned(), hash).await
            }));
        }
        for task in tasks {
            assert_wrong_or_overloaded(task.await.unwrap());
        }
        let peak = high_water_mark();
        assert!(peak >= 1, "no verification was observed running");
        assert!(
            peak <= MAX_CONCURRENT_BCRYPT,
            "{peak} verifications ran at once; the bound is {MAX_CONCURRENT_BCRYPT}"
        );
    }
}
