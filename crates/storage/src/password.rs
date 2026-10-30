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
//! so the bound holds whatever store a login is checked against.

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

static BCRYPT_SEMAPHORE: Semaphore = Semaphore::const_new(MAX_CONCURRENT_BCRYPT);

/// bcrypt calls running right now (on the blocking pool).
static RUNNING: AtomicUsize = AtomicUsize::new(0);

/// The most bcrypt calls ever observed running at once in this process.
static HIGH_WATER: AtomicUsize = AtomicUsize::new(0);

/// Acquire a bcrypt slot, waiting as long as it takes. Used for hashing,
/// where the caller is an authenticated admin creating or changing a
/// password and a queue is preferable to an error.
///
/// # Errors
///
/// Returns `OpError::Internal` only if the semaphore is closed, which a
/// `const_new` semaphore never is.
pub async fn acquire_permit() -> OpResult<SemaphorePermit<'static>> {
    BCRYPT_SEMAPHORE
        .acquire()
        .await
        .map_err(|_| OpError::Internal("bcrypt semaphore closed".to_owned()))
}

/// Verify `password` against a bcrypt `hash` on a blocking thread, holding
/// one bcrypt slot for the duration.
///
/// Returns `Ok(false)` for a wrong password and for a hash bcrypt cannot
/// parse, matching how the stores treated a malformed hash before.
///
/// # Errors
///
/// Returns `OpError::Internal` when no bcrypt slot became free within
/// [`VERIFY_WAIT`], or when the blocking task failed. Callers treat this as
/// a server error, not as a failed login.
pub async fn verify_password(password: String, hash: String) -> OpResult<bool> {
    let Ok(acquired) = tokio::time::timeout(VERIFY_WAIT, BCRYPT_SEMAPHORE.acquire()).await else {
        tracing::warn!(
            "password verification waited {}s for a bcrypt slot; rejecting",
            VERIFY_WAIT.as_secs()
        );
        return Err(OpError::Internal(
            "password verification is overloaded; try again later".to_owned(),
        ));
    };
    let _permit = acquired.map_err(|_| OpError::Internal("bcrypt semaphore closed".to_owned()))?;
    tokio::task::spawn_blocking(move || {
        let running = RUNNING.fetch_add(1, Ordering::SeqCst) + 1;
        HIGH_WATER.fetch_max(running, Ordering::SeqCst);
        let verified = bcrypt::verify(&password, &hash).unwrap_or(false);
        RUNNING.fetch_sub(1, Ordering::SeqCst);
        verified
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

    #[tokio::test]
    async fn verify_accepts_the_right_password_and_refuses_a_wrong_one() {
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

    #[tokio::test]
    async fn a_malformed_hash_is_a_wrong_password_not_an_error() {
        assert!(
            !verify_password("x".to_owned(), "not a bcrypt hash".to_owned())
                .await
                .unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_burst_of_verifications_never_runs_more_than_the_bound_at_once() {
        // Cost 10 keeps each verify around 50 ms so the burst overlaps for
        // real; with 32 requests and 4 slots the burst takes 8 rounds.
        let hash = bcrypt::hash("pw", 10).unwrap();
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let hash = hash.clone();
            tasks.push(tokio::spawn(async move {
                verify_password("wrong".to_owned(), hash).await.unwrap()
            }));
        }
        for task in tasks {
            assert!(!task.await.unwrap());
        }
        let peak = high_water_mark();
        assert!(peak >= 1, "no verification was observed running");
        assert!(
            peak <= MAX_CONCURRENT_BCRYPT,
            "{peak} verifications ran at once; the bound is {MAX_CONCURRENT_BCRYPT}"
        );
    }
}
