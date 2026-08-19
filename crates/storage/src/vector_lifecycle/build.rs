// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The build state machine: backfill orchestration, the `ACTIVE` flip, and the
//! rebuild used by crash recovery.

use std::future::Future;
use std::time::Duration;

use super::backfill::{BackfillOutcome, BatchOutcome};
use crate::error::StorageError;

/// Storage primitives one backend supplies for one vector index's build.
///
/// One value describes one index under construction: the implementor carries
/// the table id, index id, base key schema, and whatever connection handles its
/// batches need. The shared drivers ([`run_backfill`], [`complete_build`],
/// [`rebuild_index`]) own the ordering rules; the primitives own SQL,
/// transactions, and locking.
///
/// Build **ownership** is acquired by the backend before it spawns
/// [`complete_build`] (an in-process registry entry, or a cross-process
/// advisory lock) and released when the task ends, so it is not a primitive
/// here: the shared drivers never decide who owns a build, only what a build
/// does. Liveness renewal during a long backfill goes through
/// [`Self::heartbeat`].
pub trait VectorIndexBuild: Send {
    /// The backfill scan cursor. SQLite uses `rowid`; a backend without rowids
    /// uses keyset pagination over the full primary key. Opaque to the driver:
    /// it is threaded from one batch into the next and never inspected.
    type Cursor: Send;

    /// Run one transactional backfill batch: scan up to `limit` base rows after
    /// `cursor` (`None` means from the start), classify each with
    /// [`super::classify_backfill_row`], write the indexable ones, and commit.
    ///
    /// Batches commit independently so the base table stays writable while the
    /// index builds. Any locking a batch needs is acquired inside this call and
    /// released before it returns, so the inter-batch pause in [`run_backfill`]
    /// really does let a concurrent write proceed.
    fn backfill_batch(
        &mut self,
        cursor: Option<Self::Cursor>,
        limit: i64,
    ) -> impl Future<Output = Result<BatchOutcome<Self::Cursor>, StorageError>> + Send;

    /// Record that the scan is about to start: `CREATING` with
    /// `Backfilling: true`.
    ///
    /// Called by the backend's create path after the data table exists and
    /// before [`complete_build`] is spawned, and set outside the backfill
    /// transaction, otherwise no observer could see it: the whole point of the
    /// flag is to be readable while the scan is in progress.
    fn set_backfilling(&mut self) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Publish the index: `ACTIVE`, the `Backfilling` member cleared to absent
    /// (not `false`), and the poison-skip count recorded on the catalog row.
    ///
    /// The count lives only in the catalog (and the completion log line):
    /// DescribeTable parity forbids inventing a response field for it, so an
    /// operator diagnosing missing search results finds it by querying the
    /// catalog, not through the API.
    fn mark_active(
        &mut self,
        skipped: usize,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Drop and recreate the index's data table, and reload the index
    /// definition from the catalog (the request that created it is long gone).
    ///
    /// The recovery reset. Idempotent by construction: without the drop, a
    /// retry would duplicate every row it had already written before the
    /// crash, and a search would return the same item several times.
    fn reset_data_table(&mut self) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Wake whatever replays writes held while the index was `CREATING`.
    ///
    /// Writes that landed during the backfill were held by the propagation
    /// worker's claim gate. The index is `ACTIVE` now, so the worker must be
    /// woken rather than leaving them to sit until its next idle timeout.
    fn notify_active(&mut self);

    /// Renew build liveness, called between batches by [`run_backfill`].
    ///
    /// A single-process backend proves liveness by its in-process registry and
    /// needs nothing here (the default). A multi-process backend renews its
    /// heartbeat so peers can tell a slow build from a dead one.
    fn heartbeat(&mut self) -> impl Future<Output = Result<(), StorageError>> + Send {
        async { Ok(()) }
    }
}

/// Backfill the index in independently committed batches.
///
/// This is what lets the base table stay writable while an index builds, which is
/// how the service behaves: the table remains ACTIVE and accepts writes
/// throughout, and only the index reports CREATING. Holding one transaction for
/// the whole backfill would block every write until it finished.
///
/// Releasing the write path between batches is also what creates the ordering
/// hazard this design has to answer. A write landing mid-backfill is enqueued,
/// and if it were applied before the backfill wrote its older snapshot of the
/// same item, the index would converge on the stale generation. The propagation
/// worker therefore refuses to claim any row for a table whose vector index is
/// still CREATING, so those writes accumulate and are applied only after the
/// index flips to ACTIVE.
///
/// A crash part-way leaves the index in CREATING with some rows written, which
/// the backend's startup reconciler repairs by rebuilding ([`rebuild_index`]).
///
/// `batch_delay` pauses between batches, outside any lock, so a write can
/// actually proceed during the pause. Zero in production; a test sets it so a
/// write is guaranteed to land mid-backfill.
///
/// # Errors
/// Propagates the first batch or heartbeat failure; rows already committed by
/// earlier batches stay in place for the recovery rebuild to supersede.
pub async fn run_backfill<B: VectorIndexBuild>(
    ops: &mut B,
    batch_size: i64,
    batch_delay: Duration,
) -> Result<BackfillOutcome, StorageError> {
    let mut cursor: Option<B::Cursor> = None;
    let mut written = 0usize;
    let mut skipped = 0usize;
    loop {
        ops.heartbeat().await?;
        let outcome = ops.backfill_batch(cursor.take(), batch_size).await?;
        written += outcome.written;
        skipped += outcome.skipped;
        if outcome.fetched < batch_size {
            break;
        }
        let Some(next) = outcome.cursor else {
            break;
        };
        cursor = Some(next);
        if !batch_delay.is_zero() {
            tokio::time::sleep(batch_delay).await;
        }
    }
    Ok(BackfillOutcome { written, skipped })
}

/// The detached build task's body: backfill, then publish or leave `CREATING`.
///
/// Runs detached from the `UpdateTable` call, so the caller returns while the
/// index is still CREATING. The service behaves this way, and it is the whole
/// point: a table stays ACTIVE and writable throughout, and searches against
/// the index are refused until it is ACTIVE.
///
/// Not awaited by anyone, so failures cannot be returned. They are logged and
/// the index is deliberately LEFT in CREATING, which is the state the backend's
/// recovery repairs (its stuck-build sweep at runtime, its reconciler at
/// startup). Flipping to ACTIVE on error would publish a partially populated
/// index, and there is no failure state on the wire for an index to sit in.
pub async fn complete_build<B: VectorIndexBuild>(
    mut ops: B,
    index_name: &str,
    batch_size: i64,
    batch_delay: Duration,
) {
    match run_backfill(&mut ops, batch_size, batch_delay).await {
        Ok(outcome) => {
            match ops.mark_active(outcome.skipped).await {
                Ok(()) => {
                    tracing::info!(
                        index_name = %index_name,
                        vectors_indexed = outcome.written,
                        vectors_skipped = outcome.skipped,
                        "vector index backfill complete"
                    );
                    // Writes that landed during the backfill were held by the
                    // worker because this index was CREATING. It is ACTIVE
                    // now, so wake the worker rather than leaving them to sit
                    // until its next idle timeout.
                    ops.notify_active();
                }
                Err(e) => tracing::error!(
                    index_name = %index_name,
                    "vector index backfill finished but the ACTIVE flip failed, \
                     leaving it CREATING for startup reconciliation: {e}"
                ),
            }
        }
        Err(e) => tracing::error!(
            index_name = %index_name,
            "vector index backfill failed, leaving it CREATING for startup \
             reconciliation: {e}"
        ),
    }
}

/// Drop, recreate, backfill, and flip one vector index to `ACTIVE`.
///
/// The shared body of startup reconciliation and runtime stuck-build recovery,
/// factored so the two repairs cannot drift. The backfill runs on the batched
/// path, releasing the write path between batches, so a recovery on a large
/// table cannot become a write-availability outage. Recovery uses no batch
/// delay: the lever exists for tests, and recovery should finish as fast as
/// batching allows.
///
/// Returns the number of rows written, which is what distinguishes "backfilled
/// nothing because no item carries the vector" from "backfilled nothing because
/// the scan is broken".
///
/// Deliberately does NOT notify: the backend's recovery paths own when held
/// queue rows become claimable (startup reconciliation runs before the workers
/// exist; runtime recovery notifies once after a whole sweep).
///
/// # Errors
/// Unlike [`complete_build`], every failure propagates, including the terminal
/// flip: the caller is a repair loop with its own retry story, not a detached
/// task.
pub async fn rebuild_index<B: VectorIndexBuild>(
    ops: &mut B,
    batch_size: i64,
) -> Result<usize, StorageError> {
    ops.reset_data_table().await?;
    let outcome = run_backfill(ops, batch_size, Duration::ZERO).await?;
    ops.mark_active(outcome.skipped).await?;
    Ok(outcome.written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// A scripted backend: each entry is one batch's outcome, and every
    /// primitive call is recorded so the tests assert the driver's ordering
    /// decisions rather than its side effects.
    #[derive(Default)]
    struct Script {
        batches: Vec<Result<BatchOutcome<u32>, StorageError>>,
        flip_fails: bool,
        log: Vec<String>,
    }

    #[derive(Clone, Default)]
    struct MockBuild(Arc<Mutex<Script>>);

    impl MockBuild {
        fn log(&self) -> Vec<String> {
            self.0.lock().unwrap().log.clone()
        }
    }

    impl VectorIndexBuild for MockBuild {
        type Cursor = u32;

        async fn backfill_batch(
            &mut self,
            cursor: Option<u32>,
            limit: i64,
        ) -> Result<BatchOutcome<u32>, StorageError> {
            let mut s = self.0.lock().unwrap();
            s.log
                .push(format!("batch(cursor={cursor:?}, limit={limit})"));
            if s.batches.is_empty() {
                return Err(StorageError::Internal("script exhausted".to_owned()));
            }
            s.batches.remove(0)
        }

        async fn set_backfilling(&mut self) -> Result<(), StorageError> {
            self.0
                .lock()
                .unwrap()
                .log
                .push("set_backfilling".to_owned());
            Ok(())
        }

        async fn mark_active(&mut self, skipped: usize) -> Result<(), StorageError> {
            let mut s = self.0.lock().unwrap();
            s.log.push(format!("mark_active(skipped={skipped})"));
            if s.flip_fails {
                return Err(StorageError::Internal("flip failed".to_owned()));
            }
            Ok(())
        }

        async fn reset_data_table(&mut self) -> Result<(), StorageError> {
            self.0.lock().unwrap().log.push("reset".to_owned());
            Ok(())
        }

        fn notify_active(&mut self) {
            self.0.lock().unwrap().log.push("notify".to_owned());
        }
    }

    fn batch(
        written: usize,
        skipped: usize,
        fetched: i64,
        cursor: Option<u32>,
    ) -> BatchOutcome<u32> {
        BatchOutcome {
            written,
            skipped,
            fetched,
            cursor,
        }
    }

    /// The loop threads each batch's cursor into the next call, terminates on a
    /// short batch, and sums both counters across batches.
    #[tokio::test]
    async fn run_backfill_threads_the_cursor_and_stops_on_a_short_batch() {
        let mock = MockBuild::default();
        mock.0.lock().unwrap().batches = vec![
            Ok(batch(3, 0, 3, Some(30))),
            Ok(batch(2, 1, 3, Some(60))),
            Ok(batch(1, 0, 1, Some(70))),
        ];
        let mut ops = mock.clone();
        let outcome = run_backfill(&mut ops, 3, Duration::ZERO)
            .await
            .expect("backfill");
        assert_eq!(outcome.written, 6);
        assert_eq!(outcome.skipped, 1);
        assert_eq!(
            mock.log(),
            vec![
                "batch(cursor=None, limit=3)",
                "batch(cursor=Some(30), limit=3)",
                "batch(cursor=Some(60), limit=3)",
            ],
            "each batch resumes from the previous batch's cursor, and the short \
             third batch ends the scan"
        );
    }

    /// A successful build publishes with the summed skip count and only then
    /// wakes the held queue rows.
    #[tokio::test]
    async fn complete_build_marks_active_then_notifies() {
        let mock = MockBuild::default();
        mock.0.lock().unwrap().batches =
            vec![Ok(batch(2, 1, 2, Some(2))), Ok(batch(0, 1, 0, None))];
        complete_build(mock.clone(), "vidx", 2, Duration::ZERO).await;
        assert_eq!(
            mock.log(),
            vec![
                "batch(cursor=None, limit=2)",
                "batch(cursor=Some(2), limit=2)",
                "mark_active(skipped=2)",
                "notify",
            ],
            "publish carries the total skip count, and the wake follows the flip"
        );
    }

    /// A failed backfill leaves the index CREATING: no flip, no wake. That is
    /// the repair contract the reconciler and the stuck-build sweep rely on.
    #[tokio::test]
    async fn complete_build_leaves_a_failed_build_in_creating() {
        let mock = MockBuild::default();
        mock.0.lock().unwrap().batches = vec![Err(StorageError::Internal("scan died".to_owned()))];
        complete_build(mock.clone(), "vidx", 2, Duration::ZERO).await;
        assert_eq!(
            mock.log(),
            vec!["batch(cursor=None, limit=2)"],
            "neither mark_active nor notify may run after a failed backfill"
        );
    }

    /// A failed ACTIVE flip must not wake the queue: the index is still
    /// CREATING on disk, so a woken worker would find the hold still in place,
    /// and notifying would misrepresent the build as published.
    #[tokio::test]
    async fn complete_build_does_not_notify_when_the_flip_fails() {
        let mock = MockBuild::default();
        {
            let mut s = mock.0.lock().unwrap();
            s.batches = vec![Ok(batch(1, 0, 0, None))];
            s.flip_fails = true;
        }
        complete_build(mock.clone(), "vidx", 2, Duration::ZERO).await;
        assert_eq!(
            mock.log(),
            vec!["batch(cursor=None, limit=2)", "mark_active(skipped=0)"],
            "no notify after a failed flip"
        );
    }

    /// Recovery resets the data table BEFORE scanning (rebuild, not resume),
    /// flips at the end, and reports rows written. The flip error propagates,
    /// unlike the detached path.
    #[tokio::test]
    async fn rebuild_index_resets_first_and_propagates_a_flip_failure() {
        let mock = MockBuild::default();
        mock.0.lock().unwrap().batches = vec![Ok(batch(4, 1, 0, None))];
        let mut ops = mock.clone();
        let written = rebuild_index(&mut ops, 500).await.expect("rebuild");
        assert_eq!(written, 4);
        assert_eq!(
            mock.log(),
            vec![
                "reset",
                "batch(cursor=None, limit=500)",
                "mark_active(skipped=1)",
            ],
            "the reset precedes the scan, or already-written rows collide with \
             the backfill's plain INSERT"
        );

        let failing = MockBuild::default();
        {
            let mut s = failing.0.lock().unwrap();
            s.batches = vec![Ok(batch(1, 0, 0, None))];
            s.flip_fails = true;
        }
        let mut ops = failing.clone();
        let err = rebuild_index(&mut ops, 500)
            .await
            .expect_err("flip failure");
        assert!(
            matches!(err, StorageError::Internal(_)),
            "the repair loop must see the flip failure: {err:?}"
        );
    }
}
