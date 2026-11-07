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
//!   `active_restores = null` condition refuse the delete;
//! * a pause in the reconciler's scan of abandoned in-catalog creates,
//!   between the read of a CREATING row and the fence that moves it to
//!   DELETING, so a test can heartbeat the row in that gap and see the
//!   fence's own `heartbeat_at = ?` condition leave it alone.
//! * a lost answer on the maintenance claim's insert, so a test can see the
//!   re-read settle a claim whose `INSERT ... IF NOT EXISTS` committed;
//! * a pause between a maintenance claim takeover's read and its write, so
//!   a test can hold two takers of one stale row there and see one win;
//! * a pause in `claim_dead_backup` between its `LOCAL_SERIAL` read and its
//!   takeover LWT, so a test can land the owner's beat in that gap and see
//!   the LWT's `heartbeat_at = <read>` condition refuse the takeover;
//! * a pause in a store-backed registration between the target's creation
//!   and its `active_restores` lightweight transaction, so a test can move
//!   the backup to DELETING in that gap and see the transaction's own
//!   `backup_status = 'AVAILABLE'` condition refuse the registration;
//! * a pause in `claim_restore_dead` between its read and its takeover
//!   lightweight transaction, and the batch size of a store-backed
//!   `bulk_load`;
//! * a fault on the progress lightweight transaction of a store-backed
//!   `bulk_load`, so a test can see the re-read that decides an unknown
//!   outcome;
//! * a pause in a store-backed `bulk_load` between a batch's claim check and
//!   its writes, so a test can let a successor write and delete in that gap
//!   and see the stalled batch's timestamp lose to the successor's state.

use std::time::Duration;

use extenddb_storage::error::StorageError;

/// How a test makes a progress lightweight transaction answer
/// `WriteOutcome::Unknown`: with the transaction run first (`Landed`: the
/// answer was lost on its way back) or not run at all (`Lost`: the proposal
/// never reached a replica). Each fault is taken by one write, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProgressWriteFault {
    Landed,
    Lost,
}

#[cfg(feature = "test-hooks")]
mod armed {
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, LazyLock, Mutex};
    use std::time::Duration;

    use tokio::sync::Notify;

    use super::ProgressWriteFault;

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
        pub(super) before_abandoned_fence: HashMap<String, Arc<Pause>>,
        pub(super) lose_maintenance_insert_answer: bool,
        pub(super) before_maintenance_takeover: Option<Arc<Pause>>,
        pub(super) before_takeover_write: HashMap<String, Arc<Pause>>,
        pub(super) before_restore_takeover: HashMap<String, Arc<Pause>>,
        pub(super) before_restore_registration: HashMap<String, Arc<Pause>>,
        pub(super) restore_batch_size: HashMap<String, usize>,
        pub(super) progress_write_faults: HashMap<String, VecDeque<ProgressWriteFault>>,
        pub(super) after_restore_claim_check: HashMap<String, Arc<Pause>>,
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

/// Arm a pause in the next abandoned-create scan that reaches the CREATING
/// row `backup_arn`, between the read of the row and the fence that moves it
/// to DELETING. The pause is taken once and then disarmed.
#[cfg(feature = "test-hooks")]
pub fn pause_before_abandoned_fence(backup_arn: &str) -> std::sync::Arc<Pause> {
    let pause = std::sync::Arc::new(Pause::new());
    armed::with_hooks(|hooks| {
        hooks
            .before_abandoned_fence
            .insert(backup_arn.to_owned(), std::sync::Arc::clone(&pause));
    });
    pause
}

/// Arm a pause in the next `claim_restore_dead` of the restore target
/// `table_id`, between its serial read (and staleness check) and its
/// takeover lightweight transaction. A test releases it after writing a
/// fresh heartbeat, to prove the takeover's `restore_heartbeat_at = <read>`
/// condition refuses a beat that lands in between. Taken once, then disarmed.
#[cfg(feature = "test-hooks")]
pub fn pause_before_restore_takeover(table_id: &str) -> std::sync::Arc<Pause> {
    let pause = std::sync::Arc::new(Pause::new());
    armed::with_hooks(|hooks| {
        hooks
            .before_restore_takeover
            .insert(table_id.to_owned(), std::sync::Arc::clone(&pause));
    });
    pause
}

/// Arm a pause in the next store-backed registration of a restore of
/// `backup_arn`, between the target's creation (its row and DDL) and the
/// `active_restores` lightweight transaction on the backup. A test moves the
/// backup to DELETING in the gap to prove the transaction's own
/// `backup_status = 'AVAILABLE'` condition, not the unfenced pre-read,
/// refuses the registration. Taken once, then disarmed.
#[cfg(feature = "test-hooks")]
pub fn pause_before_restore_registration(backup_arn: &str) -> std::sync::Arc<Pause> {
    let pause = std::sync::Arc::new(Pause::new());
    armed::with_hooks(|hooks| {
        hooks
            .before_restore_registration
            .insert(backup_arn.to_owned(), std::sync::Arc::clone(&pause));
    });
    pause
}

/// Arm a pause in the next `claim_dead_backup` of `backup_arn`, between its
/// read of the row and its takeover write. The pause is taken once and then
/// disarmed.
#[cfg(feature = "test-hooks")]
pub fn pause_before_takeover_write(backup_arn: &str) -> std::sync::Arc<Pause> {
    let pause = std::sync::Arc::new(Pause::new());
    armed::with_hooks(|hooks| {
        hooks
            .before_takeover_write
            .insert(backup_arn.to_owned(), std::sync::Arc::clone(&pause));
    });
    pause
}

/// Make the store-backed `bulk_load` of jobs of `account_id` flush and record
/// progress every `size` items instead of the production batch size, so a
/// test can cross a batch boundary without writing ten thousand items.
#[cfg(feature = "test-hooks")]
pub fn set_restore_batch_size(account_id: &str, size: usize) {
    armed::with_hooks(|hooks| {
        hooks.restore_batch_size.insert(account_id.to_owned(), size);
    });
}

/// Arm a pause in the next batch of a store-backed `bulk_load` into the
/// restore target `table_id`, between the batch's claim check (its
/// lightweight transaction on the record, run after the batch's write
/// timestamp was read) and the batch's writes. A test takes the record over
/// in the gap, lets the successor write and delete, then releases the pause
/// to prove the stalled batch's writes, at the earlier timestamp, change
/// nothing the successor wrote or removed. Taken once, then disarmed.
#[cfg(feature = "test-hooks")]
pub fn pause_after_restore_claim_check(table_id: &str) -> std::sync::Arc<Pause> {
    let pause = std::sync::Arc::new(Pause::new());
    armed::with_hooks(|hooks| {
        hooks
            .after_restore_claim_check
            .insert(table_id.to_owned(), std::sync::Arc::clone(&pause));
    });
    pause
}

/// Hold here if a test armed a pause after the claim check of a batch into
/// the restore target `table_id`.
pub(crate) async fn after_restore_claim_check(table_id: &str) {
    #[cfg(feature = "test-hooks")]
    if let Some(pause) = armed::with_hooks(|hooks| hooks.after_restore_claim_check.remove(table_id))
    {
        pause.hold().await;
    }
    let _ = table_id;
}

/// Make the next progress lightweight transactions of the store-backed
/// `bulk_load` of jobs of `account_id` answer `WriteOutcome::Unknown`, one
/// fault per write in the order given: [`ProgressWriteFault::Landed`] runs
/// the transaction and drops its answer, [`ProgressWriteFault::Lost`] drops
/// the transaction. Writes after the last fault run as usual.
#[cfg(feature = "test-hooks")]
pub fn fail_progress_writes(account_id: &str, faults: &[ProgressWriteFault]) {
    armed::with_hooks(|hooks| {
        hooks
            .progress_write_faults
            .insert(account_id.to_owned(), faults.iter().copied().collect());
    });
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

/// Hold here if a test armed a pause for the abandoned-create fence of
/// `backup_arn`.
pub(crate) async fn before_abandoned_fence(backup_arn: &str) {
    #[cfg(feature = "test-hooks")]
    if let Some(pause) = armed::with_hooks(|hooks| hooks.before_abandoned_fence.remove(backup_arn))
    {
        pause.hold().await;
    }
    let _ = backup_arn;
}

/// Hold here if a test armed a pause for the takeover of restore target
/// `table_id`.
pub(crate) async fn before_restore_takeover(table_id: &str) {
    #[cfg(feature = "test-hooks")]
    if let Some(pause) = armed::with_hooks(|hooks| hooks.before_restore_takeover.remove(table_id)) {
        pause.hold().await;
    }
    let _ = table_id;
}

/// Hold here if a test armed a pause for the store-backed registration of a
/// restore of `backup_arn`.
pub(crate) async fn before_restore_registration(backup_arn: &str) {
    #[cfg(feature = "test-hooks")]
    if let Some(pause) =
        armed::with_hooks(|hooks| hooks.before_restore_registration.remove(backup_arn))
    {
        pause.hold().await;
    }
    let _ = backup_arn;
}

/// Hold here if a test armed a pause for the takeover of `backup_arn`.
pub(crate) async fn before_takeover_write(backup_arn: &str) {
    #[cfg(feature = "test-hooks")]
    if let Some(pause) = armed::with_hooks(|hooks| hooks.before_takeover_write.remove(backup_arn)) {
        pause.hold().await;
    }
    let _ = backup_arn;
}

/// Make the next `maintenance_claim`'s `INSERT ... IF NOT EXISTS` lose its
/// answer: the statement runs and commits, and the caller sees
/// `WriteOutcome::Unknown`, as after a write timeout on the Paxos round. The
/// hook is taken once and then disarmed.
#[cfg(feature = "test-hooks")]
pub fn lose_next_maintenance_insert_answer() {
    armed::with_hooks(|hooks| hooks.lose_maintenance_insert_answer = true);
}

/// Arm a pause in every `maintenance_claim` takeover, between the read of
/// the stale row and the `UPDATE ... IF owner_generation = <old>`, so a test
/// can hold two takers there and release both: each signals `reached` and
/// waits for one `release`. Disarmed by [`clear_maintenance_takeover_pause`].
#[cfg(feature = "test-hooks")]
pub fn pause_before_maintenance_takeover() -> std::sync::Arc<Pause> {
    let pause = std::sync::Arc::new(Pause::new());
    armed::with_hooks(|hooks| {
        hooks.before_maintenance_takeover = Some(std::sync::Arc::clone(&pause));
    });
    pause
}

/// Disarm [`pause_before_maintenance_takeover`].
#[cfg(feature = "test-hooks")]
pub fn clear_maintenance_takeover_pause() {
    armed::with_hooks(|hooks| hooks.before_maintenance_takeover = None);
}

/// Hold here if a test armed the takeover pause.
pub(crate) async fn before_maintenance_takeover() {
    #[cfg(feature = "test-hooks")]
    if let Some(pause) = armed::with_hooks(|hooks| hooks.before_maintenance_takeover.clone()) {
        pause.hold().await;
    }
}

/// Whether a test asked for the maintenance claim insert's answer to be
/// lost ([`lose_next_maintenance_insert_answer`]); disarms the hook.
pub(crate) fn maintenance_insert_answer_lost() -> bool {
    #[cfg(feature = "test-hooks")]
    {
        armed::with_hooks(|hooks| std::mem::take(&mut hooks.lose_maintenance_insert_answer))
    }
    #[cfg(not(feature = "test-hooks"))]
    false
}

/// The number of items the store-backed `bulk_load` of jobs of `account_id`
/// applies before each progress write (the byte bound closes a batch
/// earlier). The production default is `RESTORE_BATCH_ITEMS`; a test may set
/// a small size with [`set_restore_batch_size`].
pub(crate) fn restore_batch_size(account_id: &str) -> usize {
    #[cfg(feature = "test-hooks")]
    if let Some(size) = armed::with_hooks(|hooks| hooks.restore_batch_size.get(account_id).copied())
    {
        return size;
    }
    let _ = account_id;
    crate::restore_engine::RESTORE_BATCH_ITEMS
}

/// The fault the next progress lightweight transaction of a job of
/// `account_id` takes, if a test armed one.
pub(crate) fn progress_write_fault(account_id: &str) -> Option<ProgressWriteFault> {
    #[cfg(feature = "test-hooks")]
    {
        armed::with_hooks(|hooks| {
            hooks
                .progress_write_faults
                .get_mut(account_id)
                .and_then(std::collections::VecDeque::pop_front)
        })
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        let _ = account_id;
        None
    }
}
