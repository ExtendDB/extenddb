// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Round-robin over the tables a queue worker has ready rows for.
//!
//! A worker's partition holds rows from every table, in `id` order. Claiming the
//! lowest `id` across the whole partition applies one table's backlog before any
//! row a second table enqueued later: a bulk write of 50,000 items to one table
//! put a 21 row write to another table about 35 s behind it, when its propagation
//! delay was 2 s. The worker instead lists the tables with a ready row and claims
//! each table's oldest row in turn, so a table's latency depends on its own
//! backlog. Within one table the claims stay in `id` order, which is what vector
//! rows need; a secondary index row for an item whose base row a writer holds is
//! passed over for a while (see [`crate::gsi_queue`]).
//!
//! The claim loop itself ([`process_batch`]) and the per-batch table listing
//! ([`tables_with_rows`]) live here next to the rotation they drive; the enqueue
//! path, the worker loop and the apply stay in [`crate::gsi_queue`].

use extenddb_storage::error::StorageError;
use sqlx::PgPool;

use crate::gsi_queue::{
    BATCH_SIZE, ClaimedRow, GsiQueue, Parked, PendingApplyContext, ROW_LOCK_TIMEOUT,
    apply_claimed_row, decode_row, held_item, is_lock_timeout,
};

/// Claim and process up to `BATCH_SIZE` ready rows from this worker's partition.
/// Returns the number applied.
///
/// The batch first lists the tables with a ready row in the partition, then claims
/// each table's oldest ready row in turn:
///   1. `SELECT ... WHERE worker_partition = $1 AND table_id = $2 AND ready_at <= NOW() AND NOT (id = ANY($3)) ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`
///      (`$3` being the rows passed over, see `crate::gsi_queue::PARK_FOR`)
///   2. apply the snapshotted index updates (savepoint-guarded per index)
///   3. `DELETE` the claimed row
///   4. `COMMIT`
///
/// A crash or error before the `COMMIT` rolls all of it back: the row stays in
/// `gsi_pending` and is retried. A worker claims a table's rows in `id` order,
/// except that a secondary index row for an item whose base row is held is passed
/// over for a while (`PARK_FOR`), so a later row can apply before it. For those
/// rows the order does not matter: the apply writes the item's state at apply
/// time, whatever the row carried. Distinct partitions run concurrently. Only
/// rows whose `ready_at` has elapsed are eligible. A table that enqueued rows
/// after the listing is served from the next batch; a table whose claim finds
/// nothing (held, drained by this batch or by another front-end's worker, or left
/// with passed-over rows only) leaves the rotation.
///
/// Only applied rows count toward `BATCH_SIZE`. A row passed over stays on the
/// current table, and the next claim leaves its id out, so every turn of the loop
/// applies a row, takes a table out of the rotation, or passes one more row over.
///
/// `cursor` is the table this worker served last, carried from batch to batch.
/// The rotation begins after it, so consecutive batches continue around the ring
/// of listed tables instead of restarting at the lowest `table_id`: with more
/// than `BATCH_SIZE` tables listed, a batch that restarted would serve the same
/// first hundred every time and the rest would wait until those were empty.
pub(crate) async fn process_batch(
    worker_id: u64,
    q: &GsiQueue,
    cursor: &mut Option<String>,
    parked: &mut Parked,
) -> Result<usize, StorageError> {
    let mut processed = 0usize;
    let mut rotation = TableRotation::new(
        tables_with_rows(&q.data_pool, worker_id).await?,
        cursor.as_deref(),
    );

    while processed < BATCH_SIZE {
        let Some(table_id) = rotation.current().map(str::to_owned) else {
            break;
        };
        let skip = parked.ids();
        let mut tx = q
            .data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // The hold keeps a write from reaching an index whose backfill is still
        // running: the backfill holds an older snapshot of the same item, so applying
        // the newer write first would let the backfill overwrite it. Held per table,
        // which also keeps a secondary index row and a vector row for the same item
        // in order; the flip to ACTIVE deletes the hold and wakes the workers.
        // Checked in the claim statement, so it reads the row's snapshot: a hold
        // commits before the index it guards is published. Bound to the table rather
        // than correlated, so the planner evaluates it once and walks
        // `idx_gsi_pending_table_claim` in `id` order to the first ready row.
        let row: Option<ClaimedRow> = sqlx::query_as(
            "SELECT id, table_id, old_item, new_item, index_context FROM gsi_pending \
             WHERE worker_partition = $1 AND table_id = $2 AND ready_at <= NOW() \
             AND NOT (id = ANY($3)) \
             AND NOT EXISTS (SELECT 1 FROM vector_index_holds h WHERE h.table_id = $2) \
             ORDER BY id \
             LIMIT 1 \
             FOR UPDATE SKIP LOCKED",
        )
        .bind(worker_id as i32)
        .bind(&table_id)
        .bind(&skip)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let Some((id, table_id, old_json, new_json, ctx_json)) = row else {
            // This table has no ready row left for this worker; serve the others.
            let _ = tx.rollback().await;
            rotation.drained();
            continue;
        };

        let (old_item, new_item, context) = decode_row(old_json, new_json, ctx_json)?;

        // Secondary index rows take a bounded wait on the base row and are
        // passed over by item when it runs out; see `ROW_LOCK_TIMEOUT` and
        // `PARK_FOR`. Vector rows wait, as they always have.
        let mut held = None;
        if let PendingApplyContext::Gsi(c) = &context {
            if let Some(key_item) = new_item.as_ref().or(old_item.as_ref()) {
                let item = held_item(&table_id, key_item, &c.base_key_schema);
                if let Some(until) = parked.item_until(&item) {
                    // A row for an item already parked: give it back without
                    // waiting on the lock.
                    let _ = tx.rollback().await;
                    parked.park_row(id, until);
                    continue;
                }
                held = Some(item);
            }
            // Local to this transaction, so the claim itself (SKIP LOCKED,
            // never waits) is unaffected.
            sqlx::query(&format!("SET LOCAL lock_timeout = '{ROW_LOCK_TIMEOUT}'"))
                .execute(&mut *tx)
                .await
                .map_err(crate::data::index::db_error)?;
        }

        match apply_claimed_row(
            worker_id,
            &mut tx,
            id,
            &table_id,
            old_item.as_ref(),
            new_item.as_ref(),
            &context,
        )
        .await
        {
            Ok(()) => {}
            Err(e) if held.is_some() && is_lock_timeout(&e) => {
                // A writer is holding the base row. Give the claim back and pass
                // the item over for a while, so the rows behind it are not held
                // up for the whole hold; see `PARK_FOR`. Only a secondary index
                // row set the timeout; a vector row that meets one (a server-wide
                // `lock_timeout`) is an error like any other and is retried.
                let _ = tx.rollback().await;
                tracing::debug!(
                    "GSI worker {worker_id}: base row held by a writer, passing over id={id}"
                );
                if let Some(item) = held {
                    parked.park(id, item);
                }
                continue;
            }
            Err(e) => return Err(e),
        }

        sqlx::query("DELETE FROM gsi_pending WHERE id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        processed += 1;
        *cursor = Some(table_id);
        rotation.served();
    }

    Ok(processed)
}

/// The tables with a ready row in this worker's partition, in `table_id` order;
/// one query per batch. The recursive step lands on each table's lowest-`id` row
/// through `idx_gsi_pending_table_claim` (one probe per table, so the cost is the
/// number of tables, not the number of rows) and reads that row's `ready_at`. A
/// table is listed only when that row is ready: within a partition the enqueue
/// floors `ready_at` at the partition's current maximum, so a table's lowest `id`
/// carries its earliest `ready_at` and a table whose first row is inside its
/// delay has no ready row. Without this filter a partition holding N tables inside
/// their delay cost N claim transactions per wakeup, and every base write wakes
/// every worker. Two writers committing concurrently can land a later `id` with an
/// earlier `ready_at` (each read the floor before the other's commit); the table
/// is then listed when its first row is ready, at most one jitter later with one
/// delay in force. With `indexes.propagation_delay_ms` set above the default on
/// one index, the wait is bounded by the difference between the two rows' delays
/// instead (measured 9.5 s with a 10 s and a 0.5 s delay); nothing writes that
/// column today, so the shape is operator-made.
///
/// A hold is not applied here: the claim checks it on its own snapshot, and a
/// held table leaves the rotation after one statement.
async fn tables_with_rows(pool: &PgPool, worker_id: u64) -> Result<Vec<String>, StorageError> {
    sqlx::query_scalar(
        "WITH RECURSIVE t AS ( \
             (SELECT table_id, ready_at FROM gsi_pending WHERE worker_partition = $1 \
              ORDER BY table_id, id LIMIT 1) \
             UNION ALL \
             SELECT n.table_id, n.ready_at FROM t CROSS JOIN LATERAL ( \
                 SELECT table_id, ready_at FROM gsi_pending \
                 WHERE worker_partition = $1 AND table_id > t.table_id \
                 ORDER BY table_id, id LIMIT 1) n \
         ) \
         SELECT table_id FROM t WHERE ready_at <= NOW()",
    )
    .bind(worker_id as i32)
    .fetch_all(pool)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))
}

/// The tables a worker serves this batch, in turn.
pub(crate) struct TableRotation {
    tables: Vec<String>,
    next: usize,
}

impl TableRotation {
    /// `tables` in `table_id` order. The rotation begins at the first table after
    /// `after` (the table served last by the previous batch), wrapping to the first
    /// listed table when none follows it or when `after` is `None`.
    pub(crate) fn new(tables: Vec<String>, after: Option<&str>) -> Self {
        let next = match after {
            Some(last) => tables.iter().position(|t| t.as_str() > last).unwrap_or(0),
            None => 0,
        };
        Self { tables, next }
    }

    /// The table whose oldest ready row is claimed next, or `None` when every
    /// table listed at the start of the batch has been drained.
    pub(crate) fn current(&self) -> Option<&str> {
        if self.tables.is_empty() {
            None
        } else {
            Some(&self.tables[self.next % self.tables.len()])
        }
    }

    /// A row of the current table was applied: move to the next table.
    pub(crate) fn served(&mut self) {
        if !self.tables.is_empty() {
            self.next = (self.next + 1) % self.tables.len();
        }
    }

    /// The current table had no ready row left: drop it from this batch. The
    /// table that followed it becomes current.
    pub(crate) fn drained(&mut self) {
        if !self.tables.is_empty() {
            let i = self.next % self.tables.len();
            self.tables.remove(i);
            if !self.tables.is_empty() {
                self.next = i % self.tables.len();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TableRotation;

    fn rotation(names: &[&str]) -> TableRotation {
        TableRotation::new(names.iter().map(|s| (*s).to_owned()).collect(), None)
    }

    fn rotation_after(names: &[&str], after: &str) -> TableRotation {
        TableRotation::new(names.iter().map(|s| (*s).to_owned()).collect(), Some(after))
    }

    #[test]
    fn a_rotation_begins_after_the_table_served_last() {
        let r = rotation_after(&["a", "b", "c", "d"], "b");
        assert_eq!(r.current(), Some("c"));
    }

    #[test]
    fn a_cursor_at_or_past_the_last_table_wraps_to_the_first() {
        assert_eq!(rotation_after(&["a", "b", "c"], "c").current(), Some("a"));
        assert_eq!(rotation_after(&["a", "b", "c"], "z").current(), Some("a"));
        assert_eq!(rotation_after(&[], "a").current(), None);
    }

    #[test]
    fn a_cursor_naming_a_table_no_longer_listed_begins_at_its_successor() {
        assert_eq!(rotation_after(&["a", "c", "e"], "b").current(), Some("c"));
        assert_eq!(rotation_after(&["a", "c", "e"], "d").current(), Some("e"));
    }

    /// The shape behind the cursor: six tables listed, a batch of four claims.
    /// Each batch begins after the table the previous one served last, so every
    /// table is served within two consecutive batches; a rotation that restarted
    /// at the first table would serve a, b, c, d in every batch and e, f never.
    #[test]
    fn consecutive_batches_continue_around_the_ring() {
        let tables = ["a", "b", "c", "d", "e", "f"];
        let mut cursor: Option<String> = None;
        let mut batches = Vec::new();
        for _ in 0..3 {
            let mut r = TableRotation::new(
                tables.iter().map(|s| (*s).to_owned()).collect(),
                cursor.as_deref(),
            );
            let mut served = Vec::new();
            for _ in 0..4 {
                let t = r.current().unwrap().to_owned();
                cursor = Some(t.clone());
                served.push(t);
                r.served();
            }
            batches.push(served);
        }
        assert_eq!(batches[0], ["a", "b", "c", "d"]);
        assert_eq!(batches[1], ["e", "f", "a", "b"]);
        assert_eq!(batches[2], ["c", "d", "e", "f"]);
    }

    #[test]
    fn empty_rotation_has_no_current_table() {
        let mut r = rotation(&[]);
        assert_eq!(r.current(), None);
        r.served();
        r.drained();
        assert_eq!(r.current(), None);
    }

    #[test]
    fn serves_tables_in_turn_and_wraps() {
        let mut r = rotation(&["a", "b", "c"]);
        let mut seen = Vec::new();
        for _ in 0..7 {
            seen.push(r.current().unwrap().to_owned());
            r.served();
        }
        assert_eq!(seen, ["a", "b", "c", "a", "b", "c", "a"]);
    }

    #[test]
    fn a_drained_table_leaves_and_its_successor_is_current() {
        let mut r = rotation(&["a", "b", "c"]);
        r.served(); // current is b
        r.drained(); // b leaves, c is current
        assert_eq!(r.current(), Some("c"));
        r.served(); // wraps to a
        assert_eq!(r.current(), Some("a"));
        r.served();
        assert_eq!(r.current(), Some("c"));
    }

    #[test]
    fn draining_the_last_listed_table_wraps_to_the_first() {
        let mut r = rotation(&["a", "b"]);
        r.served(); // current is b
        r.drained(); // b leaves
        assert_eq!(r.current(), Some("a"));
        r.drained();
        assert_eq!(r.current(), None);
    }

    #[test]
    fn one_table_is_served_until_drained() {
        let mut r = rotation(&["only"]);
        for _ in 0..3 {
            assert_eq!(r.current(), Some("only"));
            r.served();
        }
        r.drained();
        assert_eq!(r.current(), None);
    }
}
