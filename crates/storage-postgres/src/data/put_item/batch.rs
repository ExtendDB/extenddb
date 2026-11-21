// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Batched put implementation for the `PostgreSQL` backend.

use super::*;
use crate::data::bulk_row_outcomes::{LedgerRow, insert_outcomes_in_tx};
use crate::data::gsi_build::{missed_gsis, queue_repairs};

impl PostgresEngine {
    /// Implementation of `DataEngine::put_items_batch`: every row of the
    /// batch in one transaction.
    ///
    /// The batch first locks the operation's write-owner row and is refused
    /// unless its markers name that owner, then deletes the ledger rows the
    /// last checkpoint covers (`bulk_row_outcomes::fence_and_prune`). Each
    /// row is then written through the same path a single put uses, and its
    /// outcome, a failed condition included, is kept with what the row
    /// replaced. After the last row the outcomes go into the ledger in this
    /// transaction, one multi-row insert for up to 9,362 rows, so a batch
    /// of the runner's size costs one ledger statement instead of one a
    /// row. Any other error returns before commit, so nothing from the
    /// batch is visible.
    ///
    /// A batch takes its row locks in input order, so two batches on
    /// overlapping keys in different orders can each wait on a lock the
    /// other holds; PostgreSQL detects that and aborts one transaction with
    /// SQLSTATE 40P01. The aborted batch wrote nothing, so it is retried
    /// after a short backoff, up to `MAX_DEADLOCK_RETRIES` times. One row per
    /// transaction could not deadlock; this is the cost the batch pays for
    /// its throughput, kept inside the backend so an operation never fails
    /// for colliding with a sibling.
    ///
    /// The catalog is read once, with its stamp, and every attempt reuses
    /// that plan and that stamp. `gsi_build::fix_up_stale_write` takes the
    /// index set "the write read at `stamp`", so the two have to come from
    /// the same read: a fresh stamp beside the first read's index set would
    /// make a retry look fast while it maintains only the indexes the first
    /// read knew. Kept, the stamp includes the wait for PostgreSQL's
    /// `deadlock_timeout` (one second by default) on every retry, so a
    /// retried attempt re-reads the catalog before its commit and queues a
    /// repair for each GSI created since the read.
    pub(crate) async fn put_items_batch_impl(
        &self,
        key_info: &TableKeyInfo,
        rows: Vec<BatchRow>,
        stream: Option<&StreamCapture>,
    ) -> Result<Vec<BatchRowResult>, StorageError> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let plan = self.write_plan(key_info).await?;
        for row in &rows {
            Self::validate_index_keys(&plan, key_info, &row.item)?;
        }
        let mut attempt: u32 = 0;
        loop {
            match self
                .put_items_batch_once(&plan, key_info, &rows, stream)
                .await
            {
                Err(StorageError::Internal(message))
                    if is_deadlock(&message) && attempt < MAX_DEADLOCK_RETRIES =>
                {
                    attempt += 1;
                    tracing::debug!(attempt, "bulk batch deadlocked with a sibling; retrying");
                    tokio::time::sleep(deadlock_backoff(attempt)).await;
                }
                other => return other,
            }
        }
    }

    /// One attempt at the batch: one transaction, every row, the repair for
    /// GSIs the plan did not know, commit.
    async fn put_items_batch_once(
        &self,
        plan: &WritePlan,
        key_info: &TableKeyInfo,
        rows: &[BatchRow],
        stream: Option<&StreamCapture>,
    ) -> Result<Vec<BatchRowResult>, StorageError> {
        let mut tx = self.data_pool.begin().await.map_err(batch_tx_error)?;
        if let Some(marker) = rows.iter().find_map(|row| row.marker.as_ref()) {
            crate::data::bulk_row_outcomes::fence_and_prune(&mut tx, marker).await?;
        }
        let mut results = Vec::with_capacity(rows.len());
        let mut written_items: Vec<&Item> = Vec::with_capacity(rows.len());
        let mut ledger: Vec<LedgerRow<'_>> = Vec::with_capacity(rows.len());
        let mut async_enqueued: usize = 0;
        let default_maps = ExpressionMaps::default();

        for row in rows {
            let (condition, maps) = match row.condition.as_ref() {
                Some((expr, maps)) => (Some(expr), maps),
                None => (None, &default_maps),
            };
            match Self::put_row_in_tx(&mut tx, plan, key_info, &row.item, condition, maps, stream)
                .await
            {
                Ok(written) => {
                    if let Some(m) = row.marker.as_ref() {
                        ledger.push(LedgerRow::new(
                            m,
                            RowOutcome::Applied,
                            written.old.as_ref(),
                        )?);
                    }
                    async_enqueued += written.async_enqueued;
                    written_items.push(&row.item);
                    results.push(BatchRowResult::Written {
                        old: if row.return_old { written.old } else { None },
                    });
                }
                Err(StorageError::ConditionFailed(old)) => {
                    if let Some(m) = row.marker.as_ref() {
                        ledger.push(LedgerRow::new(
                            m,
                            RowOutcome::ConditionFailed,
                            old.as_ref(),
                        )?);
                    }
                    results.push(BatchRowResult::ConditionFailed { old });
                }
                Err(error) => return Err(error),
            }
        }

        insert_outcomes_in_tx(&mut tx, &ledger).await?;
        async_enqueued +=
            fix_up_stale_batch(&mut tx, &self.pool, plan, key_info, &written_items).await?;
        tx.commit().await.map_err(batch_tx_error)?;
        self.wake_index_workers(async_enqueued);
        Ok(results)
    }
}

/// `gsi_build::fix_up_stale_write` for a batch, called after its last row and
/// before its commit, so the repairs commit or roll back with the rows.
///
/// When the batch has run longer than `GRACE_WRITE` since the plan's catalog
/// read, the backfill of a GSI created in between may have passed its rows,
/// and the barrier may have passed before its first base-table write. One
/// catalog re-read finds every GSI the plan did not know, and a repair is
/// queued for each of them and each row the batch wrote. Rows that wrote
/// nothing (a failed condition) need none.
async fn fix_up_stale_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    catalog: &sqlx::PgPool,
    plan: &WritePlan,
    key_info: &TableKeyInfo,
    written: &[&Item],
) -> Result<usize, StorageError> {
    if written.is_empty() || plan.stamp.elapsed() <= GRACE_WRITE {
        return Ok(0);
    }
    let missed = missed_gsis(catalog, &key_info.table_id, &plan.indexes).await?;
    let mut queued = 0;
    for item in written {
        queued += queue_repairs(tx, key_info, &missed, item).await?;
    }
    Ok(queued)
}

/// Map an error from the batch's own transaction statements (`BEGIN`,
/// `COMMIT`). A pool that timed out, a dropped socket and a crashed worker
/// are `Transient`, which the bulk runner resumes after; a database error
/// keeps its SQLSTATE in the text, which the runner and `is_deadlock` read.
/// Before this mapping both arrived as bare `Internal` text and ended the
/// operation.
fn batch_tx_error(e: sqlx::Error) -> StorageError {
    match e {
        sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::WorkerCrashed => {
            StorageError::Transient(e.to_string())
        }
        other => crate::data::index::db_error(other),
    }
}

/// Retries of a bulk batch that PostgreSQL aborted as a deadlock victim. Each
/// retry starts a fresh transaction after `deadlock_backoff`; a batch still
/// deadlocking after this many is returned as the internal error it is.
const MAX_DEADLOCK_RETRIES: u32 = 8;

/// SQLSTATE 40P01 is `deadlock_detected`. The mapped error text carries the
/// code (see `sqlstate_message`), which is what is matched here.
fn is_deadlock(message: &str) -> bool {
    message.contains("SQLSTATE 40P01")
}

/// Backoff before a deadlock retry: 5 ms doubling, capped at 160 ms, plus up
/// to one base step of jitter so two colliding batches do not retry in step.
fn deadlock_backoff(attempt: u32) -> std::time::Duration {
    let base_ms = 5u64 << attempt.min(5);
    let jitter_ms = u64::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos()),
    ) % 5;
    std::time::Duration::from_millis(base_ms.min(160) + jitter_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pool_timeout_or_a_dropped_socket_on_the_batch_transaction_is_transient() {
        for error in [
            sqlx::Error::PoolTimedOut,
            sqlx::Error::Io(std::io::Error::from(std::io::ErrorKind::ConnectionReset)),
            sqlx::Error::WorkerCrashed,
        ] {
            assert!(
                matches!(batch_tx_error(error), StorageError::Transient(_)),
                "the runner resumes only after a Transient batch fault"
            );
        }
    }

    #[test]
    fn any_other_batch_transaction_error_stays_internal() {
        assert!(matches!(
            batch_tx_error(sqlx::Error::RowNotFound),
            StorageError::Internal(_)
        ));
        assert!(matches!(
            batch_tx_error(sqlx::Error::PoolClosed),
            StorageError::Internal(_)
        ));
    }
}
