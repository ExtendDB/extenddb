// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The claim loop that drives runnable operations through the runner, the
//! in-flight slot bookkeeping and its drop guard, and the no-store cancel
//! drain that finishes CANCELLING operations.

use super::*;

/// The two non-terminal statuses the cancel path reads and writes. Kept as
/// literals here, the same wire strings the store contract and the runner
/// use, so the no-store cancel drain does not reach into the runner.
const STATUS_CANCELLING: &str = "CANCELLING";
const STATUS_CANCELLED: &str = "CANCELLED";

/// The claim sequence for this process. The owner id written by a claim is
/// the process prefix plus this counter, one value per claim, so two
/// incarnations of one operation never share an owner string even inside
/// one process, and every lease-conditional write (the checkpoint, `finish`,
/// the lease re-read before a report rename) distinguishes them.
static CLAIM_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Runs in flight in this process, shared between the claim loop and the
/// slot guards of the spawned runs: per-account counts for the per-account
/// bound, and the operation ids for the claim exclusion.
#[derive(Default)]
struct InFlightState {
    per_account: HashMap<String, usize>,
    operations: HashSet<String>,
}

type InFlight = Arc<Mutex<InFlightState>>;

/// Releases one slot of its account and retires its operation id when
/// dropped, so a run frees both on success, error, and panic alike.
struct SlotGuard {
    in_flight: InFlight,
    account: String,
    operation: String,
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        let Ok(mut state) = self.in_flight.lock() else {
            return;
        };
        match state.per_account.get_mut(&self.account) {
            Some(n) if *n > 1 => *n -= 1,
            _ => {
                state.per_account.remove(&self.account);
            }
        }
        state.operations.remove(&self.operation);
    }
}

/// The claim loop: on every tick, fill the free slots one claim at a time.
///
/// Claims go one row at a time so the per-account bound holds inside a
/// single tick: each claim carries the exclusion lists rebuilt from the
/// bookkeeping the previous claim updated, so an account reaching its
/// share mid-tick is excluded from the very next claim, and a burst of one
/// account's operations cannot take every slot the way a single claim of
/// `free` rows could. An empty claim ends the tick.
///
/// Every claim also excludes the operation ids in flight in this process
/// and writes a fresh per-claim owner id (`run_one` receives it): without
/// both, a run that outlives its lease is re-claimed by its own process
/// one tick after expiry, the two incarnations share every lease guard,
/// and their report temp files share a name.
#[allow(clippy::too_many_arguments)] // one call site per entry point
pub(super) async fn claim_loop<F, Fut>(
    store: Arc<dyn BulkOperationStore>,
    concurrency: usize,
    per_account: usize,
    lease: Duration,
    interval: Duration,
    owner: String,
    metrics: Arc<MetricsCollector>,
    token: CancellationToken,
    run_one: F,
) where
    F: Fn(BulkOperationRecord, String) -> Fut,
    Fut: Future<Output = Result<(), RunnerError>> + Send + 'static,
{
    let in_flight: InFlight = Arc::new(Mutex::new(InFlightState::default()));
    // The runs this loop spawned. At shutdown the loop waits for them: each
    // run sees the same token, checkpoints at its next row and releases its
    // lease, so the next process resumes it at once from where it stopped.
    let mut runs: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    while tick(&token, interval).await {
        runs.retain(|run| !run.is_finished());
        let pass_start = Instant::now();
        let mut errored = false;
        loop {
            let (free, exclude_accounts, exclude_operations) = {
                let Ok(state) = in_flight.lock() else {
                    tracing::error!(
                        "bulk operation claim loop stopping: the in-flight state lock is \
                         poisoned"
                    );
                    metrics.record_worker_error(QuerySource::BulkClaim);
                    return;
                };
                let total: usize = state.per_account.values().sum();
                let exclude_accounts: Vec<String> = state
                    .per_account
                    .iter()
                    .filter(|(_, n)| **n >= per_account)
                    .map(|(account, _)| account.clone())
                    .collect();
                let exclude_operations: Vec<String> = state.operations.iter().cloned().collect();
                (
                    concurrency.saturating_sub(total),
                    exclude_accounts,
                    exclude_operations,
                )
            };
            if free == 0 {
                break;
            }
            let claim_owner = format!("{owner}-{}", CLAIM_SEQUENCE.fetch_add(1, Ordering::Relaxed));
            let claimed = match store
                .claim_runnable(
                    &claim_owner,
                    lease,
                    1,
                    &exclude_accounts,
                    &exclude_operations,
                )
                .await
            {
                Ok(rows) => rows,
                Err(StorageError::Validation(_)) => {
                    tracing::info!(
                        "Bulk operations are not supported on this storage backend; the \
                         bulk operation worker stops"
                    );
                    return;
                }
                Err(error) => {
                    tracing::warn!(error = %error, "bulk operation claim failed");
                    metrics.record_worker_error(QuerySource::BulkClaim);
                    errored = true;
                    break;
                }
            };
            let Some(record) = claimed.into_iter().next() else {
                break;
            };
            let account = record.account_id.clone();
            let operation = record.operation_id.clone();
            if let Ok(mut state) = in_flight.lock() {
                *state.per_account.entry(account.clone()).or_insert(0) += 1;
                state.operations.insert(operation.clone());
            }
            let guard = SlotGuard {
                in_flight: Arc::clone(&in_flight),
                account: account.clone(),
                operation: operation.clone(),
            };
            let fut = run_one(record, claim_owner);
            runs.push(tokio::spawn(async move {
                let _slot = guard;
                if let Err(error) = fut.await {
                    // The operation stays non-terminal; the next claim after
                    // its lease expires resumes it from the last committed
                    // checkpoint.
                    tracing::error!(
                        account_id = %account,
                        operation_id = %operation,
                        error = ?error,
                        "bulk operation run failed"
                    );
                }
            }));
        }
        // One completed claim pass: record a worker success unless a claim
        // failed this pass (which recorded a worker error above). This is the
        // liveness signal the other background workers publish; a stalled or
        // crashed claim loop stops advancing WorkerLastSuccess for BulkClaim.
        if !errored {
            metrics.record_worker_success(QuerySource::BulkClaim, elapsed_us(pass_start));
        }
    }
    // Wait up to `RUN_DRAIN` for the runs, inside the server's worker drain.
    // A run still running at the deadline is aborted, and the loop awaits it,
    // so no run outlives the loop. An aborted run keeps its lease until the
    // lease lapses, and a later claim resumes it from its last checkpoint.
    let deadline = tokio::time::Instant::now() + RUN_DRAIN;
    let mut aborted = Vec::new();
    for mut run in runs {
        if tokio::time::timeout_at(deadline, &mut run).await.is_err() {
            run.abort();
            aborted.push(run);
        }
    }
    for run in aborted {
        let _ = run.await;
    }
}

/// How long the claim loop waits at shutdown for its runs to checkpoint and
/// release their leases, inside the server's five-second worker drain. A
/// run at a row or waiting on its source stops within milliseconds. The
/// loop aborts a run still running when this passes, for example one inside
/// a store call; the run keeps its lease until the lease lapses, a later
/// claim resumes it from its last checkpoint, and its rows stay recorded in
/// the ledger.
pub(super) const RUN_DRAIN: Duration = Duration::from_secs(1);

/// Drive CANCELLING operations to CANCELLED when no `[bulk]` store is
/// configured, until `token` is cancelled.
///
/// A cancel reads no input, so it needs no store. Each tick claims runnable
/// rows one at a time and finishes the CANCELLING ones through
/// [`finish_cancelled`]. A row that is `IN_PROGRESS` with no cancel is
/// leased by the claim and then left non-terminal, because it cannot run
/// without a store; its lease lapses and, if it is cancelled later, a
/// subsequent claim finishes it. The claim is lease conditional and the
/// finish binds on the per-claim owner, so once a store is configured and
/// the process restarts, the normal worker resumes every operation this
/// loop did not finish from its last checkpoint.
pub(super) async fn cancel_only_loop(
    storage: Arc<dyn StorageEngine>,
    owner: String,
    lease: Duration,
    interval: Duration,
    token: CancellationToken,
) {
    while tick(&token, interval).await {
        loop {
            let claim_owner = format!("{owner}-{}", CLAIM_SEQUENCE.fetch_add(1, Ordering::Relaxed));
            let claimed = match storage
                .claim_runnable(&claim_owner, lease, 1, &[], &[])
                .await
            {
                Ok(rows) => rows,
                Err(StorageError::Validation(_)) => {
                    tracing::info!(
                        "Bulk operations are not supported on this storage backend; the \
                         bulk operation worker stops"
                    );
                    return;
                }
                Err(error) => {
                    tracing::warn!(error = %error, "bulk operation cancel claim failed");
                    break;
                }
            };
            let Some(record) = claimed.into_iter().next() else {
                break;
            };
            if record.status == STATUS_CANCELLING
                && let Err(error) = finish_cancelled(storage.as_ref(), &claim_owner, &record).await
            {
                tracing::warn!(
                    account_id = %record.account_id,
                    operation_id = %record.operation_id,
                    error = %error,
                    "bulk operation cancel finish failed"
                );
            }
        }
    }
}

/// Finish one CANCELLING operation as CANCELLED through the store's terminal
/// transition, the shape the runner writes when it drains a cancel: the end
/// time is now, the counters and usage are the floors the record already
/// carries, and no report location is set because there is no sink to render
/// one. On a held transition the operation's ledger rows are deleted, as the
/// runner does after a terminal transition.
async fn finish_cancelled(
    storage: &dyn StorageEngine,
    owner: &str,
    record: &BulkOperationRecord,
) -> Result<FinishState, StorageError> {
    // The store supplies this pair itself when it turns a CANCELLING row
    // into CANCELLED; sending it too keeps the record readable on its own.
    let outcome = Terminal {
        status: STATUS_CANCELLED.to_owned(),
        end_time_ms: epoch_millis(),
        failure_code: Some(extenddb_storage::CANCELLED_FAILURE_CODE.to_owned()),
        failure_message: Some(extenddb_storage::CANCELLED_FAILURE_MESSAGE.to_owned()),
        usage: record.usage.clone(),
    };
    let state = storage
        .finish(&record.account_id, &record.operation_id, owner, outcome)
        .await?;
    if matches!(state, FinishState::Held(_))
        && let Err(error) = storage
            .delete_bulk_row_outcomes(&record.account_id, &record.operation_id)
            .await
    {
        tracing::warn!(
            account_id = %record.account_id,
            operation_id = %record.operation_id,
            error = %error,
            "bulk row outcome ledger delete failed after a no-store cancel"
        );
    }
    Ok(state)
}
