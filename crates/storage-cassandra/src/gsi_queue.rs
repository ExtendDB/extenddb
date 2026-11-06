// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Persistent GSI propagation queue backed by the `gsi_pending` table.
//!
//! Rows are inserted inside the same logged batch as the base write (atomically
//! with the item mutation). Each row is self-describing: `index_context` carries
//! everything the worker needs to apply the update with zero catalog reads.
//!
//! Workers own one partition each (worker `i` → `worker_partition = i`), so all
//! updates for a given base item are applied in `(ready_at, id)` order by a
//! single worker — per-key FIFO without any row-level locking.

use extenddb_core::types::{AttributeDefinition, KeySchemaElement, Projection};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Notify;

/// Number of worker partitions. Fixed: changing while the queue is non-empty
/// would split a key's rows across workers.
pub(crate) const NUM_WORKERS: u64 = 4;

/// A single target index definition, snapshotted at enqueue time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GsiIndexDef {
    pub(crate) index_id: String,
    pub(crate) key_schema: Vec<KeySchemaElement>,
    pub(crate) projection: Projection,
}

/// Everything a worker needs to apply a pending GSI update without touching the
/// catalog. Serialized into `gsi_pending.index_context` at enqueue time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GsiApplyContext {
    pub(crate) base_key_schema: Vec<KeySchemaElement>,
    pub(crate) attribute_definitions: Vec<AttributeDefinition>,
    pub(crate) index: GsiIndexDef,
    /// Write timestamp for the index rows this entry produces: the base-row
    /// version the enqueuing write committed. Absent on entries written
    /// before the field existed; the worker then lets the coordinator stamp
    /// them, as it always did.
    #[serde(default)]
    pub(crate) index_stamp: Option<i64>,
}

/// Routes all updates for a given base item to one worker (FNV-1a, stable
/// across builds — not `std`'s hash which is not guaranteed stable).
pub(crate) fn partition_for(base_pk_text: &str) -> i32 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in base_pk_text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % NUM_WORKERS) as i32
}

/// The GSI worker that applies index updates for `item`, for tests that
/// need two writes handled by different workers.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
pub fn worker_for_item(
    item: &extenddb_core::types::Item,
    key_schema: &[extenddb_core::types::KeySchemaElement],
) -> Result<u64, extenddb_storage::error::StorageError> {
    let pk_text = extenddb_storage::util::composite_pk_to_text(item, key_schema)?;
    Ok(u64::try_from(partition_for(&pk_text)).unwrap_or_default())
}

/// Jitter a propagation delay: uniform in `[delay_ms/2 + 1, delay_ms]`.
/// Values <= 1 are returned unchanged.
pub(crate) fn jitter_delay_ms(delay_ms: u64) -> u64 {
    if delay_ms <= 1 {
        delay_ms
    } else {
        use rand::Rng;
        rand::rng().random_range(delay_ms / 2 + 1..=delay_ms)
    }
}

/// Handle for waking GSI workers after a write enqueues rows.
///
/// One `Notify` per worker partition, each with exactly one waiter: the
/// partition's worker. A wake uses `notify_one`, which stores a permit when
/// the worker is not waiting (busy applying rows), so the worker's next wait
/// returns at once instead of sleeping a full idle period with a due row in
/// the queue (#407).
pub struct GsiQueue {
    notify: [Notify; NUM_WORKERS as usize],
}

impl GsiQueue {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            notify: std::array::from_fn(|_| Notify::new()),
        })
    }

    /// Wake all workers after a write inserts into `gsi_pending`. A worker
    /// that is not waiting gets the wake on its next wait.
    pub fn notify_workers(&self) {
        for notify in &self.notify {
            notify.notify_one();
        }
    }

    /// Resolves when a write wakes `worker_id`, or at once if one already
    /// did since its last wait.
    pub(crate) async fn notified(&self, worker_id: u64) {
        // `worker_id` is always below NUM_WORKERS: the workers are spawned
        // for `0..NUM_WORKERS`.
        #[allow(clippy::cast_possible_truncation)]
        self.notify[worker_id as usize].notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::GsiQueue;
    use std::time::Duration;

    /// What a worker does when it finds no due row: wait for a wake, at most
    /// `bound`. `true` when woken.
    async fn worker_wait(queue: &GsiQueue, worker_id: u64, bound: Duration) -> bool {
        tokio::time::timeout(bound, queue.notified(worker_id))
            .await
            .is_ok()
    }

    /// A write can land while a worker is busy applying rows, not parked. Its
    /// wake must be kept for the worker's next wait, or the worker sleeps a
    /// full idle period with a due row in the queue (#407). Every partition's
    /// worker gets it.
    #[tokio::test]
    async fn a_wake_sent_while_no_worker_waits_is_kept() {
        let queue = GsiQueue::new();
        queue.notify_workers();
        for worker_id in 0..super::NUM_WORKERS {
            assert!(
                worker_wait(&queue, worker_id, Duration::from_millis(10)).await,
                "the wake sent before worker {worker_id} waited was lost"
            );
        }
    }

    /// A kept wake is used once: the worker's next wait after it sleeps.
    #[tokio::test]
    async fn a_kept_wake_is_used_once() {
        let queue = GsiQueue::new();
        queue.notify_workers();
        assert!(worker_wait(&queue, 0, Duration::from_millis(10)).await);
        assert!(
            !worker_wait(&queue, 0, Duration::from_millis(10)).await,
            "one wake woke the worker twice"
        );
    }
}
