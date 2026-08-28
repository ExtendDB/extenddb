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
pub struct GsiQueue {
    pub(crate) notify: Arc<Notify>,
}

impl GsiQueue {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            notify: Arc::new(Notify::new()),
        })
    }

    /// Wake all workers after a write inserts into `gsi_pending`.
    pub fn notify_workers(&self) {
        self.notify.notify_waiters();
    }
}
