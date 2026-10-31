// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Coordination hooks for this crate's integration tests (the
//! `direct_integration` binary), compiled only with the `test-hooks` feature.
//! Without the feature every hook is a no-op the compiler removes, so a
//! production build carries none of this. Each hook is keyed by the account
//! (or the backup ARN) a test owns, so the other tests running in the same
//! process are not affected.
//!
//! The hooks:
//!
//! * the heartbeat period of the jobs of one account, in place of
//!   [`extenddb_storage::backup_catalog::JOB_HEARTBEAT_INTERVAL`], so a test
//!   can watch a short create or restore beat;
//! * a count of heartbeats of one account's jobs that fail with an error
//!   before reaching the catalog, to exercise the consecutive-error rule of
//!   `JobHeartbeat`, and a read of how many are still to fire;
//! * a pause between the in-use read and the DELETING claim of a backup
//!   delete (the legacy `delete` and `start_backup_delete`), so a test can
//!   register a restore in that gap and see the claim's own
//!   `active_restores = null` condition refuse the delete.

use std::time::Duration;

use extenddb_storage::error::StorageError;

#[cfg(feature = "test-hooks")]
mod armed {
    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock, Mutex};
    use std::time::Duration;

    use tokio::sync::Notify;

    /// A pause a test arms at one point of one operation: the operation
    /// signals `reached` when it gets there and waits for `released`.
    pub struct Pause {
        reached: Notify,
        released: Notify,
    }

    impl Pause {
        pub(super) fn new() -> Self {
            Self {
                reached: Notify::new(),
                released: Notify::new(),
            }
        }

        /// Wait until the paused operation has reached the pause point.
        pub async fn reached(&self) {
            self.reached.notified().await;
        }

        /// Let the paused operation continue.
        pub fn release(&self) {
            self.released.notify_one();
        }

        pub(super) async fn hold(&self) {
            self.reached.notify_one();
            self.released.notified().await;
        }
    }

    #[derive(Default)]
    pub(super) struct Hooks {
        pub(super) heartbeat_interval: HashMap<String, Duration>,
        pub(super) heartbeat_errors: HashMap<String, usize>,
        pub(super) before_delete_claim: HashMap<String, Arc<Pause>>,
    }

    pub(super) static HOOKS: LazyLock<Mutex<Hooks>> = LazyLock::new(Mutex::default);

    pub(super) fn with_hooks<T>(f: impl FnOnce(&mut Hooks) -> T) -> T {
        let mut hooks = HOOKS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(&mut hooks)
    }
}

#[cfg(feature = "test-hooks")]
pub use armed::Pause;

/// Make the jobs of `account_id` heartbeat every `interval` instead of
/// `JOB_HEARTBEAT_INTERVAL`.
#[cfg(feature = "test-hooks")]
pub fn set_heartbeat_interval(account_id: &str, interval: Duration) {
    armed::with_hooks(|hooks| {
        hooks
            .heartbeat_interval
            .insert(account_id.to_owned(), interval);
    });
}

/// Make the next `count` heartbeats of the jobs of `account_id` fail with an
/// error before reaching the catalog.
#[cfg(feature = "test-hooks")]
pub fn fail_heartbeats(account_id: &str, count: usize) {
    armed::with_hooks(|hooks| {
        hooks.heartbeat_errors.insert(account_id.to_owned(), count);
    });
}

/// How many of the heartbeat errors [`fail_heartbeats`] armed for
/// `account_id` no beat has taken yet, so a test can see that the beats it
/// armed have fired.
#[cfg(feature = "test-hooks")]
pub fn pending_heartbeat_errors(account_id: &str) -> usize {
    armed::with_hooks(|hooks| hooks.heartbeat_errors.get(account_id).copied().unwrap_or(0))
}

/// Arm a pause in the next delete of `backup_arn`, between its in-use read
/// and its DELETING claim. The pause is taken once and then disarmed.
#[cfg(feature = "test-hooks")]
pub fn pause_before_delete_claim(backup_arn: &str) -> std::sync::Arc<Pause> {
    let pause = std::sync::Arc::new(Pause::new());
    armed::with_hooks(|hooks| {
        hooks
            .before_delete_claim
            .insert(backup_arn.to_owned(), std::sync::Arc::clone(&pause));
    });
    pause
}

/// The period the jobs of `account_id` heartbeat at.
pub(crate) fn heartbeat_interval(account_id: &str) -> Duration {
    #[cfg(feature = "test-hooks")]
    if let Some(interval) =
        armed::with_hooks(|hooks| hooks.heartbeat_interval.get(account_id).copied())
    {
        return interval;
    }
    let _ = account_id;
    extenddb_storage::backup_catalog::JOB_HEARTBEAT_INTERVAL
}

/// The error the next heartbeat of a job of `account_id` fails with, if a
/// test asked for one.
pub(crate) fn heartbeat_error(account_id: &str) -> Option<StorageError> {
    #[cfg(feature = "test-hooks")]
    {
        let injected =
            armed::with_hooks(|hooks| match hooks.heartbeat_errors.get_mut(account_id) {
                Some(left) if *left > 0 => {
                    *left -= 1;
                    true
                }
                _ => false,
            });
        if injected {
            return Some(StorageError::Internal(
                "test hook: heartbeat failed before reaching the catalog".to_owned(),
            ));
        }
    }
    let _ = account_id;
    None
}

/// Hold here if a test armed a pause for the delete of `backup_arn`.
pub(crate) async fn before_delete_claim(backup_arn: &str) {
    #[cfg(feature = "test-hooks")]
    if let Some(pause) = armed::with_hooks(|hooks| hooks.before_delete_claim.remove(backup_arn)) {
        pause.hold().await;
    }
    let _ = backup_arn;
}
