// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Async-safe password hashing.
//!
//! bcrypt is CPU-intensive (~100ms per call at default cost). Calling it
//! directly from an async task blocks the tokio runtime. This helper moves the
//! work to a blocking thread and takes a slot from the process-wide bcrypt
//! semaphore in [`extenddb_storage::password`], which it shares with password
//! verification in every storage backend, so hashing and verification together
//! never run more than that module's bound at once.

/// Hash a password with bcrypt on a blocking thread.
///
/// Takes `String` (not `&str`) because the closure must own the data
/// to move it into the `spawn_blocking` thread.
///
/// Waits as long as it takes for a bcrypt slot: the caller is an authenticated
/// admin creating or changing a password, and a queue is preferable to an
/// error.
///
/// # Errors
///
/// Returns `bcrypt::BcryptError` if hashing fails or the blocking task panics.
pub async fn hash_password(password: String) -> Result<String, bcrypt::BcryptError> {
    let Ok(_permit) = extenddb_storage::password::acquire_permit().await else {
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
