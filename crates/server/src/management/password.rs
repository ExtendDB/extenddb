// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Async-safe password hashing.
//!
//! bcrypt is CPU-intensive (~100ms per call at default cost). Calling it
//! directly from an async task blocks the tokio runtime. This helper hashes
//! through [`extenddb_storage::password::hash_password`], which runs bcrypt on
//! a blocking thread under a slot of the process-wide bcrypt semaphore it
//! shares with password verification in every storage backend, so hashing and
//! verification together never run more than that module's bound at once.

/// Hash a password with bcrypt on a blocking thread.
///
/// Takes `String` (not `&str`) because the closure must own the data
/// to move it into the `spawn_blocking` thread.
///
/// Waits as long as it takes for a bcrypt slot: the caller is an authenticated
/// admin creating or changing a password, and a queue is preferable to an
/// error. The slot is held until bcrypt returns, even if the request is
/// cancelled first.
///
/// # Errors
///
/// Returns `bcrypt::BcryptError` if hashing fails or the blocking task panics.
pub async fn hash_password(password: String) -> Result<String, bcrypt::BcryptError> {
    extenddb_storage::password::hash_password(password, bcrypt::DEFAULT_COST)
        .await
        .map_err(|e| {
            tracing::error!("bcrypt hash failed: {e:?}");
            bcrypt::BcryptError::CostNotAllowed(0)
        })
}
