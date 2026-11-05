// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Waiting on the output store while the lease is kept and the cursor
//! stays where it is.
//!
//! The output check at the first report record, the report writes and the
//! manifest write each wait on the store for up to its 120-second bound.
//! None of them may run a boundary: the check runs before the checkpoint
//! that would persist the record it checks for, and the writes run after
//! the final checkpoint. So while one of them waits, the lease is renewed
//! by a checkpoint that writes back the progress the record already holds,
//! the store having no call that renews the lease alone.
//!
//! What else the renewal acts on depends on where the run is. The output
//! check at the first report record runs while the run still reads input,
//! so a cancel or a deleted table seen by its renewal ends the wait as at a
//! boundary ([`Watch::CancelAndTable`]). The report and manifest writes run
//! after the run decided its outcome at the final checkpoint; their
//! renewals end a wait only on a lost lease ([`Watch::LeaseOnly`]), so a
//! cancelled or failed run still writes every report and its manifest, and
//! a cancel that arrives during the writes is recorded by the terminal
//! transition.

use super::*;

/// What a renewal during a wait acts on besides a lost lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Watch {
    /// A cancel or a deleted table ends the wait too.
    CancelAndTable,
    /// Only a lost lease, or a renewal that cannot run, ends the wait.
    LeaseOnly,
}

/// What a wait on the output store ended with.
pub(super) enum InPlace<T> {
    /// The store answered.
    Answer(T),
    /// The lease passed to another process, or the record is gone; the
    /// runner stops with no further write.
    Lost,
    /// The operation was cancelled during the wait (`Watch::CancelAndTable`).
    Cancelled,
    /// The table was deleted or replaced during the wait
    /// (`Watch::CancelAndTable`).
    TableGone,
    /// The renewal could not read or write the record. Nothing can be
    /// decided about ownership, so the runner stops without a terminal
    /// transition, as after a failed checkpoint.
    Interrupted(StorageError),
}

/// Await `call`, renewing the lease without moving the cursor while it
/// waits. `deadline` is the local lease deadline, advanced by each renewal;
/// `None` awaits the call without renewing. The renewal runs at the same
/// instants a read's boundary does (`wait::renewal_instant`).
///
/// The call is polled while a renewal runs. The call may hold a storage
/// connection (a report page read, the lease re-read before a rename), and
/// a renewal that waited for the pool while the call was not polled could
/// wait for that connection forever.
pub(super) async fn keeping_lease_in_place<T>(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    deadline: &mut Option<Instant>,
    watch: Watch,
    call: impl Future<Output = T>,
) -> InPlace<T> {
    let lease = deps.config.lease;
    let started = Instant::now();
    let mut call = std::pin::pin!(call);
    loop {
        let Some(due) = *deadline else {
            return InPlace::Answer(call.await);
        };
        let renew_at = renewal_instant(due, lease, started);
        let renewal = async {
            tokio::time::sleep_until(tokio::time::Instant::from_std(renew_at)).await;
            renew_in_place(deps, record, watch).await
        };
        tokio::select! {
            biased;
            answer = &mut call => return InPlace::Answer(answer),
            renewed = renewal => match renewed {
                Ok(renewed_at) => *deadline = Some(renewed_at + lease),
                Err(stop) => return stop,
            },
        }
    }
}

/// One renewal: read the record, check the table binding when `watch`
/// asks for it, write the record's own progress back under the lease, then
/// act on the cancel flag the read saw when `watch` asks for it. Answers
/// the instant the renewal was sent, from which the new lease runs.
async fn renew_in_place<T>(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    watch: Watch,
) -> Result<Instant, InPlace<T>> {
    let account = &record.account_id;
    let operation = &record.operation_id;
    let current = match deps.storage.get_bulk_operation(account, operation).await {
        Ok(Some(current)) => current,
        Ok(None) => return Err(InPlace::Lost),
        Err(error) => return Err(InPlace::Interrupted(error)),
    };
    if watch == Watch::CancelAndTable {
        match table_binding_holds(deps, record).await {
            Ok(true) => {}
            Ok(false) => return Err(InPlace::TableGone),
            Err(error) => return Err(InPlace::Interrupted(error)),
        }
    }
    // Taken before the call, as at a boundary: the store grants the lease
    // from its own clock when the statement runs.
    let renewed_at = Instant::now();
    match deps
        .storage
        .checkpoint(
            account,
            operation,
            &deps.owner,
            deps.config.lease,
            unchanged_progress(&current),
        )
        .await
    {
        Err(error) => return Err(InPlace::Interrupted(error)),
        Ok(LeaseState::Lost) => return Err(InPlace::Lost),
        Ok(LeaseState::Held) => {}
    }
    tracing::info!(
        account_id = %account,
        operation_id = %operation,
        "lease renewed while the output store is waited on"
    );
    if watch == Watch::CancelAndTable && current.cancel_requested {
        return Err(InPlace::Cancelled);
    }
    Ok(renewed_at)
}

/// The progress the record holds, unchanged: every figure as stored, with
/// no report record and no file outcome to add. Written back by a holder
/// of the lease it changes nothing but the lease.
fn unchanged_progress(current: &BulkOperationRecord) -> Progress {
    Progress {
        cursor_file_index: current.cursor_file_index,
        cursor_row_index: current.cursor_row_index,
        processed_item_count: current.processed_item_count,
        failed_item_count: current.failed_item_count,
        processed_input_bytes: current.processed_input_bytes,
        report_record_count: current.report_record_count,
        report_bytes: current.report_bytes,
        usage: current.usage.clone(),
        records: Vec::new(),
        finished_files: Vec::new(),
        abandoned_files: Vec::new(),
    }
}
