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
//!   and see the stalled batch's timestamp lose to the successor's state;
//! * a fault on the lightweight transaction of `insert_backup_row`, and an
//!   error on the logged batch that writes its index entries, so a test can
//!   see the serial re-read settle a lost answer and an insert complete the
//!   entries a stopped one left unwritten;
//! * a fault on the lightweight transaction of `forget_available_backup_row`,
//!   and an error on the logged batch that removes a row's index entries, so
//!   a test can see the serial re-read settle a lost removal and a removal
//!   whose entries stay answer that the row is removed;
//! * a fault on the lightweight transaction of `rewrite_store`, and a stop
//!   of one row's rewrite after its first write or after its transaction, so
//!   a test can see the serial re-read settle a lost rewrite and see where a
//!   stopped rewrite leaves the row listed;
//! * a pause in one row's `rewrite_store` between the new root's status
//!   entry and the lightweight transaction, so a test can delete or move
//!   the row in that gap and see the transaction's conditions and the
//!   serial re-read after it;
//! * a pause in `set_backup_deleted` between its read of the row and its
//!   lightweight transaction, so a test can move the row in that gap and see
//!   the transaction's store condition send it under the root it names.

use std::time::Duration;

use extenddb_storage::error::StorageError;

/// How a test makes a lightweight transaction (a restore's progress write,
/// the insert of `insert_backup_row` or the removal of
/// `forget_available_backup_row`) answer `WriteOutcome::Unknown`:
/// with the transaction run first (`Landed`: the answer was lost on its way
/// back) or not run at all (`Lost`: the proposal never reached a replica).
/// Each fault is taken by one write, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProgressWriteFault {
    Landed,
    Lost,
}

/// Where a test stops one row's `rewrite_store`, with an error, as a process
/// that dies there would: after the write that precedes the lightweight
/// transaction (the new root's status entry), or after the transaction,
/// before the old root's entry is deleted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RewriteStop {
    AfterNewEntry,
    AfterRow,
}

#[cfg(feature = "test-hooks")]
mod armed {
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, LazyLock, Mutex};
    use std::time::Duration;

    use tokio::sync::Notify;

    use super::{ProgressWriteFault, RewriteStop};

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
        pub(super) insert_write_faults: HashMap<String, VecDeque<ProgressWriteFault>>,
        pub(super) index_entry_batch_errors: HashMap<String, usize>,
        pub(super) forget_write_faults: HashMap<String, VecDeque<ProgressWriteFault>>,
        pub(super) index_entry_removal_errors: HashMap<String, usize>,
        pub(super) rewrite_write_faults: HashMap<String, VecDeque<ProgressWriteFault>>,
        pub(super) rewrite_stops: HashMap<String, RewriteStop>,
        pub(super) before_rewrite_write: HashMap<String, Arc<Pause>>,
        pub(super) before_deleted_write: HashMap<String, Arc<Pause>>,
        pub(super) after_delete_claim: HashMap<String, Arc<Pause>>,
        pub(super) after_deleted_write: HashMap<String, Arc<Pause>>,
        pub(super) before_tombstone_removal: HashMap<String, Arc<Pause>>,
        pub(super) convert_write_faults: HashMap<String, VecDeque<ProgressWriteFault>>,
        pub(super) convert_stops: std::collections::HashSet<String>,
        pub(super) before_convert: HashMap<String, Arc<Pause>>,
        pub(super) before_legacy_registration: HashMap<String, Arc<Pause>>,
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

/// Arm a pause in the next `rewrite_store` of the row of `backup_arn`,
/// between the new root's status entry and the lightweight transaction on
/// the row. The pause is taken once and then disarmed.
#[cfg(feature = "test-hooks")]
pub fn pause_before_rewrite_write(backup_arn: &str) -> std::sync::Arc<Pause> {
    let pause = std::sync::Arc::new(Pause::new());
    armed::with_hooks(|hooks| {
        hooks
            .before_rewrite_write
            .insert(backup_arn.to_owned(), std::sync::Arc::clone(&pause));
    });
    pause
}

/// Arm a pause in the next `set_backup_deleted` of `backup_arn`, between
/// its read of the row and its lightweight transaction. The pause is taken
/// once and then disarmed.
#[cfg(feature = "test-hooks")]
pub fn pause_before_deleted_write(backup_arn: &str) -> std::sync::Arc<Pause> {
    let pause = std::sync::Arc::new(Pause::new());
    armed::with_hooks(|hooks| {
        hooks
            .before_deleted_write
            .insert(backup_arn.to_owned(), std::sync::Arc::clone(&pause));
    });
    pause
}

/// Arm a pause in the next delete of `backup_arn`, after its applied
/// DELETING claim and before the batch that moves its status entry. The
/// pause is taken once and then disarmed.
#[cfg(feature = "test-hooks")]
pub fn pause_after_delete_claim(backup_arn: &str) -> std::sync::Arc<Pause> {
    let pause = std::sync::Arc::new(Pause::new());
    armed::with_hooks(|hooks| {
        hooks
            .after_delete_claim
            .insert(backup_arn.to_owned(), std::sync::Arc::clone(&pause));
    });
    pause
}

/// Arm a pause in the next `set_backup_deleted` of `backup_arn`, after its
/// applied lightweight transaction and before the batch that moves its
/// status entry. The pause is taken once and then disarmed.
#[cfg(feature = "test-hooks")]
pub fn pause_after_deleted_write(backup_arn: &str) -> std::sync::Arc<Pause> {
    let pause = std::sync::Arc::new(Pause::new());
    armed::with_hooks(|hooks| {
        hooks
            .after_deleted_write
            .insert(backup_arn.to_owned(), std::sync::Arc::clone(&pause));
    });
    pause
}

/// Arm a pause in the next `remove_backup_row` of `backup_arn`, between its
/// read of the row and its lightweight transaction. The pause is taken once
/// and then disarmed.
#[cfg(feature = "test-hooks")]
pub fn pause_before_tombstone_removal(backup_arn: &str) -> std::sync::Arc<Pause> {
    let pause = std::sync::Arc::new(Pause::new());
    armed::with_hooks(|hooks| {
        hooks
            .before_tombstone_removal
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

/// Make the next lightweight transactions of `insert_backup_row` for
/// `backup_arn` answer `WriteOutcome::Unknown`, one fault per write in the
/// order given, as [`fail_progress_writes`] does for progress writes. Writes
/// after the last fault run as usual.
#[cfg(feature = "test-hooks")]
pub fn fail_insert_writes(backup_arn: &str, faults: &[ProgressWriteFault]) {
    armed::with_hooks(|hooks| {
        hooks
            .insert_write_faults
            .insert(backup_arn.to_owned(), faults.iter().copied().collect());
    });
}

/// Make the next `count` logged batches that write the index entries of
/// `backup_arn` fail with an error before reaching the catalog.
#[cfg(feature = "test-hooks")]
pub fn fail_index_entry_batches(backup_arn: &str, count: usize) {
    armed::with_hooks(|hooks| {
        hooks
            .index_entry_batch_errors
            .insert(backup_arn.to_owned(), count);
    });
}

/// Make the next lightweight transactions of `forget_available_backup_row`
/// for `backup_arn` answer `WriteOutcome::Unknown`, one fault per write in
/// the order given, as [`fail_insert_writes`] does for the insert.
#[cfg(feature = "test-hooks")]
pub fn fail_forget_writes(backup_arn: &str, faults: &[ProgressWriteFault]) {
    armed::with_hooks(|hooks| {
        hooks
            .forget_write_faults
            .insert(backup_arn.to_owned(), faults.iter().copied().collect());
    });
}

/// Make the next `count` logged batches that remove the index entries of
/// `backup_arn` fail with an error before reaching the catalog.
#[cfg(feature = "test-hooks")]
pub fn fail_index_entry_removals(backup_arn: &str, count: usize) {
    armed::with_hooks(|hooks| {
        hooks
            .index_entry_removal_errors
            .insert(backup_arn.to_owned(), count);
    });
}

/// Make the next lightweight transactions of `rewrite_store` on the row of
/// `backup_arn` answer `WriteOutcome::Unknown`, one fault per write in the
/// order given, as [`fail_insert_writes`] does for the insert.
#[cfg(feature = "test-hooks")]
pub fn fail_rewrite_writes(backup_arn: &str, faults: &[ProgressWriteFault]) {
    armed::with_hooks(|hooks| {
        hooks
            .rewrite_write_faults
            .insert(backup_arn.to_owned(), faults.iter().copied().collect());
    });
}

/// Stop the next `rewrite_store` of the row of `backup_arn` at `at` with an
/// error, which ends the whole call. Taken once, then disarmed.
#[cfg(feature = "test-hooks")]
pub fn stop_rewrite(backup_arn: &str, at: RewriteStop) {
    armed::with_hooks(|hooks| {
        hooks.rewrite_stops.insert(backup_arn.to_owned(), at);
    });
}

/// Make the next lightweight transactions that convert the in-catalog row
/// of `backup_arn` into its index row (`replace_with_index_row`) answer
/// `WriteOutcome::Unknown`, one fault per write in the order given, as
/// [`fail_insert_writes`] does for the insert.
#[cfg(feature = "test-hooks")]
pub fn fail_convert_writes(backup_arn: &str, faults: &[ProgressWriteFault]) {
    armed::with_hooks(|hooks| {
        hooks
            .convert_write_faults
            .insert(backup_arn.to_owned(), faults.iter().copied().collect());
    });
}

/// Stop the next `replace_with_index_row` of `backup_arn` with an error
/// right after its row's conversion, before the list and status entries and
/// the item buckets, as a process that dies there would. Taken once.
#[cfg(feature = "test-hooks")]
pub fn stop_convert_after_row(backup_arn: &str) {
    armed::with_hooks(|hooks| {
        hooks.convert_stops.insert(backup_arn.to_owned());
    });
}

/// Arm a pause in the next `replace_with_index_row` of `backup_arn`, after
/// its read of the row and its in-use check and before the conversion's
/// lightweight transaction, so a test can land a restore's registration in
/// the gap. Taken once, then disarmed.
#[cfg(feature = "test-hooks")]
pub fn pause_before_convert(backup_arn: &str) -> std::sync::Arc<Pause> {
    let pause = std::sync::Arc::new(Pause::new());
    armed::with_hooks(|hooks| {
        hooks
            .before_convert
            .insert(backup_arn.to_owned(), std::sync::Arc::clone(&pause));
    });
    pause
}

/// Hold here if a test armed [`pause_before_convert`] for `backup_arn`.
pub(crate) async fn before_convert(backup_arn: &str) {
    #[cfg(feature = "test-hooks")]
    if let Some(pause) = armed::with_hooks(|hooks| hooks.before_convert.remove(backup_arn)) {
        pause.hold().await;
    }
    let _ = backup_arn;
}

/// Arm a pause in the next legacy restore of `backup_arn`, between its
/// target's creation and its `active_restores` lightweight transaction on
/// the in-catalog row, so a test can convert the row in the gap. Taken
/// once, then disarmed.
#[cfg(feature = "test-hooks")]
pub fn pause_before_legacy_restore_registration(backup_arn: &str) -> std::sync::Arc<Pause> {
    let pause = std::sync::Arc::new(Pause::new());
    armed::with_hooks(|hooks| {
        hooks
            .before_legacy_registration
            .insert(backup_arn.to_owned(), std::sync::Arc::clone(&pause));
    });
    pause
}

/// Hold here if a test armed [`pause_before_legacy_restore_registration`]
/// for `backup_arn`.
pub(crate) async fn before_legacy_restore_registration(backup_arn: &str) {
    #[cfg(feature = "test-hooks")]
    if let Some(pause) =
        armed::with_hooks(|hooks| hooks.before_legacy_registration.remove(backup_arn))
    {
        pause.hold().await;
    }
    let _ = backup_arn;
}

/// The fault the next conversion lightweight transaction of `backup_arn`
/// takes, if a test armed one.
pub(crate) fn convert_write_fault(backup_arn: &str) -> Option<ProgressWriteFault> {
    #[cfg(feature = "test-hooks")]
    {
        armed::with_hooks(|hooks| {
            hooks
                .convert_write_faults
                .get_mut(backup_arn)
                .and_then(std::collections::VecDeque::pop_front)
        })
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        let _ = backup_arn;
        None
    }
}

/// The error a conversion of `backup_arn` stops with after its row, if a
/// test armed [`stop_convert_after_row`].
pub(crate) fn convert_stop(backup_arn: &str) -> Result<(), StorageError> {
    #[cfg(feature = "test-hooks")]
    {
        if armed::with_hooks(|hooks| hooks.convert_stops.remove(backup_arn)) {
            return Err(StorageError::Internal(format!(
                "test hook: the conversion of {backup_arn} stopped after its row"
            )));
        }
    }
    let _ = backup_arn;
    Ok(())
}

/// The fault the next rewrite lightweight transaction of `backup_arn` takes,
/// if a test armed one.
pub(crate) fn rewrite_write_fault(backup_arn: &str) -> Option<ProgressWriteFault> {
    #[cfg(feature = "test-hooks")]
    {
        armed::with_hooks(|hooks| {
            hooks
                .rewrite_write_faults
                .get_mut(backup_arn)
                .and_then(std::collections::VecDeque::pop_front)
        })
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        let _ = backup_arn;
        None
    }
}

/// The error a rewrite of `backup_arn` that reached `at` stops with, if a
/// test armed [`stop_rewrite`] there.
pub(crate) fn rewrite_stop(backup_arn: &str, at: RewriteStop) -> Result<(), StorageError> {
    #[cfg(feature = "test-hooks")]
    {
        let armed_here = armed::with_hooks(|hooks| {
            if hooks.rewrite_stops.get(backup_arn) == Some(&at) {
                hooks.rewrite_stops.remove(backup_arn);
                true
            } else {
                false
            }
        });
        if armed_here {
            return Err(StorageError::Internal(format!(
                "test hook: the rewrite of {backup_arn} stopped at {at:?}"
            )));
        }
    }
    let _ = (backup_arn, at);
    Ok(())
}

/// The fault the next removal lightweight transaction of `backup_arn` takes,
/// if a test armed one.
pub(crate) fn forget_write_fault(backup_arn: &str) -> Option<ProgressWriteFault> {
    #[cfg(feature = "test-hooks")]
    {
        armed::with_hooks(|hooks| {
            hooks
                .forget_write_faults
                .get_mut(backup_arn)
                .and_then(std::collections::VecDeque::pop_front)
        })
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        let _ = backup_arn;
        None
    }
}

/// The error the next index-entry removal batch of `backup_arn` fails with,
/// if a test asked for one.
pub(crate) fn index_entry_removal_error(backup_arn: &str) -> Option<StorageError> {
    #[cfg(feature = "test-hooks")]
    {
        let injected =
            armed::with_hooks(
                |hooks| match hooks.index_entry_removal_errors.get_mut(backup_arn) {
                    Some(left) if *left > 0 => {
                        *left -= 1;
                        true
                    }
                    _ => false,
                },
            );
        if injected {
            return Some(StorageError::Internal(
                "test hook: index entry removal failed before reaching the catalog".to_owned(),
            ));
        }
    }
    let _ = backup_arn;
    None
}

/// The fault the next insert lightweight transaction of `backup_arn` takes,
/// if a test armed one.
pub(crate) fn insert_write_fault(backup_arn: &str) -> Option<ProgressWriteFault> {
    #[cfg(feature = "test-hooks")]
    {
        armed::with_hooks(|hooks| {
            hooks
                .insert_write_faults
                .get_mut(backup_arn)
                .and_then(std::collections::VecDeque::pop_front)
        })
    }
    #[cfg(not(feature = "test-hooks"))]
    {
        let _ = backup_arn;
        None
    }
}

/// The error the next index-entry batch of `backup_arn` fails with, if a
/// test asked for one.
pub(crate) fn index_entry_batch_error(backup_arn: &str) -> Option<StorageError> {
    #[cfg(feature = "test-hooks")]
    {
        let injected =
            armed::with_hooks(
                |hooks| match hooks.index_entry_batch_errors.get_mut(backup_arn) {
                    Some(left) if *left > 0 => {
                        *left -= 1;
                        true
                    }
                    _ => false,
                },
            );
        if injected {
            return Some(StorageError::Internal(
                "test hook: index entry batch failed before reaching the catalog".to_owned(),
            ));
        }
    }
    let _ = backup_arn;
    None
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

/// Hold here if a test armed a pause for the rewrite of `backup_arn`.
pub(crate) async fn before_rewrite_write(backup_arn: &str) {
    #[cfg(feature = "test-hooks")]
    if let Some(pause) = armed::with_hooks(|hooks| hooks.before_rewrite_write.remove(backup_arn)) {
        pause.hold().await;
    }
    let _ = backup_arn;
}

/// Hold here if a test armed a pause for the DELETED write of `backup_arn`.
pub(crate) async fn before_deleted_write(backup_arn: &str) {
    #[cfg(feature = "test-hooks")]
    if let Some(pause) = armed::with_hooks(|hooks| hooks.before_deleted_write.remove(backup_arn)) {
        pause.hold().await;
    }
    let _ = backup_arn;
}

/// Hold here if a test armed a pause after the DELETING claim of
/// `backup_arn`.
pub(crate) async fn after_delete_claim(backup_arn: &str) {
    #[cfg(feature = "test-hooks")]
    if let Some(pause) = armed::with_hooks(|hooks| hooks.after_delete_claim.remove(backup_arn)) {
        pause.hold().await;
    }
    let _ = backup_arn;
}

/// Hold here if a test armed a pause after the DELETED write of
/// `backup_arn`.
pub(crate) async fn after_deleted_write(backup_arn: &str) {
    #[cfg(feature = "test-hooks")]
    if let Some(pause) = armed::with_hooks(|hooks| hooks.after_deleted_write.remove(backup_arn)) {
        pause.hold().await;
    }
    let _ = backup_arn;
}

/// Hold here if a test armed a pause for the tombstone removal of
/// `backup_arn`.
pub(crate) async fn before_tombstone_removal(backup_arn: &str) {
    #[cfg(feature = "test-hooks")]
    if let Some(pause) =
        armed::with_hooks(|hooks| hooks.before_tombstone_removal.remove(backup_arn))
    {
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
