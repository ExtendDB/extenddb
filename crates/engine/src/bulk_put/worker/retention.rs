// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The retention worker that purges terminal operation records past the
//! retention window, deleting each operation's ledger rows before its
//! record.

use super::*;

/// The retention tick.
const RETENTION_INTERVAL: Duration = Duration::from_secs(86_400);

/// Rows one retention pass lists and purges per chunk. Each chunk is its
/// own purge transaction, so the writer is released between chunks and a
/// large backlog drains over successive bounded transactions rather than
/// one unbounded one.
const RETENTION_CHUNK: usize = 1_000;

/// Purge terminal bulk operation records past the retention window, once
/// at startup and then daily, deleting each operation's ledger rows before
/// its record.
pub async fn bulk_operation_retention_worker(
    storage: Arc<dyn StorageEngine>,
    limits: Arc<LimitsConfig>,
    metrics: Arc<MetricsCollector>,
    token: CancellationToken,
) {
    // Floored at 1: a zero window makes the cutoff `now`, so the first pass
    // deletes every terminal record, including operations that finished
    // moments earlier. One day is the smallest window that keeps a record
    // past its own run. The floor is applied here, where the value is read,
    // with the warn the other bulk worker settings use.
    let retention_days = floored(
        limits.bulk_operation_retention_days,
        "bulk_operation_retention_days",
    );
    loop {
        if !purge_once(storage.as_ref(), retention_days, metrics.as_ref(), &token).await {
            return;
        }
        if !tick(&token, RETENTION_INTERVAL).await {
            return;
        }
    }
}

/// One retention pass. `false` stops the loop: the backend answered
/// `Validation`, meaning it does not carry bulk operations.
async fn purge_once(
    storage: &dyn StorageEngine,
    retention_days: u32,
    metrics: &MetricsCollector,
    token: &CancellationToken,
) -> bool {
    let pass_start = Instant::now();
    let cutoff =
        SystemTime::now() - Duration::from_secs(u64::from(retention_days).saturating_mul(86_400));
    // Drain in bounded chunks. Each chunk is a separately committed
    // purge transaction, so the writer (the SQLite engine write lock, a
    // PostgreSQL transaction) is released between chunks: a large backlog
    // after downtime or a shortened retention window cannot hold the
    // writer for the whole backlog in one transaction. A full pass drains
    // every eligible row over successive chunks; a chunk shorter than the
    // limit means the backlog is drained and the pass ends.
    loop {
        let pairs = match storage.list_purgeable_before(cutoff, RETENTION_CHUNK).await {
            Ok(pairs) => pairs,
            Err(StorageError::Validation(_)) => {
                tracing::info!(
                    "Bulk operations are not supported on this storage backend; the bulk \
                     operation retention worker stops"
                );
                return false;
            }
            Err(error) => {
                tracing::warn!(error = %error, "bulk operation retention list failed");
                metrics.record_worker_error(QuerySource::BulkRetention);
                return true;
            }
        };
        if pairs.is_empty() {
            metrics.record_worker_success(QuerySource::BulkRetention, elapsed_us(pass_start));
            return true;
        }
        let chunk_len = pairs.len();
        // Ledger rows die while their operation record still exists, never
        // after: the ledger lives in the data database, out of the catalog
        // purge's reach, and once the record is gone its rows could never
        // be addressed again. A pair is purged only once its ledger delete
        // succeeded; a failed delete keeps the record, so the next pass
        // lists the pair again, and a crash at any point between the two
        // steps does the same.
        let mut purgeable = Vec::with_capacity(chunk_len);
        for (account, operation) in pairs {
            match storage.delete_bulk_row_outcomes(&account, &operation).await {
                Ok(_deleted) => purgeable.push((account, operation)),
                Err(error) => tracing::warn!(
                    account_id = %account,
                    operation_id = %operation,
                    error = %error,
                    "bulk row outcome ledger delete failed during retention; the record is \
                     kept for the next pass"
                ),
            }
        }
        let purged = purgeable.len();
        if let Err(error) = storage.purge_operations(&purgeable).await {
            tracing::warn!(error = %error, "bulk operation retention purge failed");
            metrics.record_worker_error(QuerySource::BulkRetention);
            return true;
        }
        tracing::info!(purged, "bulk operation retention purge");
        // A short chunk means the backlog is drained. A full chunk means
        // more remains: continue draining this pass, but stop promptly if
        // the server is shutting down rather than hold up a drain.
        if chunk_len < RETENTION_CHUNK {
            metrics.record_worker_success(QuerySource::BulkRetention, elapsed_us(pass_start));
            return true;
        }
        if token.is_cancelled() {
            return false;
        }
    }
}
