// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Async-safe password hashing and verification.
//!
//! bcrypt is CPU-intensive (~100ms per call at default cost). Calling it
//! directly from an async task blocks the tokio runtime. These helpers use
//! `spawn_blocking` to move the work to a dedicated thread pool.
//!
//! CB-8: A global semaphore limits concurrent bcrypt operations to prevent
//! denial-of-service via flooding the blocking thread pool (shared with sqlx).

use tokio::sync::Semaphore;

/// Maximum concurrent bcrypt operations. Limits CPU saturation from
/// concurrent login attempts without starving the sqlx blocking pool.
static BCRYPT_SEMAPHORE: Semaphore = Semaphore::const_new(4);

/// Hash a password with bcrypt on a blocking thread.
///
/// Takes `String` (not `&str`) because the closure must own the data
/// to move it into the `spawn_blocking` thread.
///
/// # Errors
///
/// Returns `bcrypt::BcryptError` if hashing fails or the blocking task panics.
pub async fn hash_password(password: String) -> Result<String, bcrypt::BcryptError> {
    // CB-8: Acquire semaphore permit before spawning blocking work.
    // `const_new` semaphore is never closed, so `acquire` cannot fail in practice.
    let Ok(_permit) = BCRYPT_SEMAPHORE.acquire().await else {
        tracing::error!("bcrypt semaphore closed unexpectedly");
        return Err(bcrypt::BcryptError::CostNotAllowed(0));
    };
    tokio::task::spawn_blocking(move || bcrypt::hash(password, bcrypt::DEFAULT_COST))
        .await
        .unwrap_or_else(|e| {
            tracing::error!("bcrypt blocking task failed: {e}");
            Err(bcrypt::BcryptError::CostNotAllowed(0))
        })
}

/// Verify a password against a stored bcrypt hash on a blocking thread.
///
/// The console login uses this so that it verifies the same hash it binds the
/// new session to. The management API's Basic auth verifies through the
/// `AdminStore` and `ManagementStore` implementations instead.
///
/// Returns `false` when the password does not match, when `hash` is not a
/// valid bcrypt hash, or when the blocking task fails.
pub async fn verify_password(password: String, hash: String) -> bool {
    tokio::task::spawn_blocking(move || bcrypt::verify(password, &hash).unwrap_or(false))
        .await
        .unwrap_or_else(|e| {
            tracing::error!("bcrypt blocking task failed: {e}");
            false
        })
}
