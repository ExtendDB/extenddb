// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Persistent GSI update queue (D-4, D-5, D-6).
//!
//! Base table writes insert a row into `gsi_pending` within the same
//! transaction as the item mutation. Each row is **self-describing**: it
//! carries the base key schema, attribute definitions, and the target index
//! definitions captured at enqueue time (`GsiApplyContext`), so workers apply
//! updates with **zero catalog reads**. A GSI's key schema and projection are
//! immutable after creation, so the snapshot can never be stale relative to
//! the live index definition; if the index is dropped, the apply is skipped.
//!
//! Workers claim, apply, and delete each row inside a **single transaction**
//! (`SELECT ... FOR UPDATE SKIP LOCKED` → index writes → `DELETE` → `COMMIT`).
//! A crash or transient error before `COMMIT` rolls the whole unit back, so the
//! pending row reappears and is reprocessed — nothing is lost once enqueued.
//!
//! Rows are partitioned by a stable hash of the base table key, and each worker
//! owns one partition, so all updates to a given base item are applied in `id`
//! order by a single worker (**per-key FIFO**) while distinct keys propagate
//! concurrently.

use std::sync::Arc;

use extenddb_core::types::{AttributeDefinition, Item, KeySchemaElement, Projection};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::composite_pk_to_text;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::sync::Notify;

use crate::data::{
    all_sort_key_info, delete_index_row_multi, index_table_name, insert_index_row_multi,
    item_has_index_keys, project_item_for_index,
};

/// Number of worker tasks consuming from the persistent queue.
///
/// Fixed invariant: a row's `worker_partition` is `hash(base_key) % NUM_WORKERS`
/// and worker `i` consumes partition `i`, so all updates for a base item are
/// applied in `id` order by a single worker (per-key FIFO). Changing this while
/// the queue is non-empty could split a key's pending rows across workers; a
/// rolling restart drains in-flight rows first, so changing it between deploys
/// is safe.
const NUM_WORKERS: u64 = 4;

/// Maximum rows a single `process_batch` call claims before yielding. Each row
/// is processed in its own transaction, so this bounds work per wakeup without
/// holding a long-lived transaction.
const BATCH_SIZE: usize = 100;

/// Marker for PostgreSQL's "undefined_table" SQLSTATE (42P01) as embedded in
/// `StorageError` messages by `db_error`. Matching the full `SQLSTATE 42P01`
/// prefix (rather than the bare code) avoids false positives from item or
/// index data that happens to contain the substring `42P01`.
const PG_UNDEFINED_TABLE: &str = "SQLSTATE 42P01";

/// Stable partition for a base-table key (FNV-1a, mapped to a worker). Routes
/// all updates for one base item to a single worker for in-order apply. A local
/// hash (not `std`'s, which is not guaranteed stable across builds) keeps the
/// mapping fixed for a given key forever.
fn partition_for(base_pk_text: &str) -> i32 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325; // FNV-1a offset basis
    for byte in base_pk_text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3); // FNV prime
    }
    (hash % NUM_WORKERS) as i32
}

/// Check if a `StorageError` is caused by an undefined table (SQLSTATE 42P01).
///
/// This occurs when an index table is dropped (table deleted) while an async
/// GSI update is still queued. The pending row is consumed rather than retried.
fn is_undefined_table(err: &StorageError) -> bool {
    match err {
        StorageError::Internal(msg) => msg.contains(PG_UNDEFINED_TABLE),
        _ => false,
    }
}

/// A single target index definition, snapshotted at enqueue time.
///
/// Only the fields the worker needs to apply the update: the propagation delay
/// is not stored because it is already encoded in the row's `ready_at`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GsiIndexDef {
    pub(crate) index_id: String,
    pub(crate) key_schema: Vec<KeySchemaElement>,
    pub(crate) projection: Projection,
}

/// Everything a worker needs to apply a pending GSI update without touching the
/// catalog. Serialized into the `gsi_pending.index_context` column at enqueue.
///
/// Each `gsi_pending` row targets exactly **one** index. A base write that
/// touches several async GSIs enqueues one row per index, so each index keeps
/// its own propagation delay (encoded in the row's `ready_at`) and propagates
/// independently — mirroring the per-index queue items of the original
/// in-memory design. Rows for the same base item share a `worker_partition`
/// and are applied in `id` order, preserving per-key FIFO.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GsiApplyContext {
    pub(crate) base_key_schema: Vec<KeySchemaElement>,
    pub(crate) attribute_definitions: Vec<AttributeDefinition>,
    pub(crate) index: GsiIndexDef,
}

/// A row claimed from `gsi_pending`:
/// `(id, table_id, old_item, new_item, index_context)`.
type ClaimedRow = (
    i64,
    String,
    Option<serde_json::Value>,
    Option<serde_json::Value>,
    serde_json::Value,
);

/// Persistent GSI propagation queue backed by the `gsi_pending` table.
pub struct GsiQueue {
    data_pool: PgPool,
    notify: Arc<Notify>,
}

impl GsiQueue {
    /// Create the queue and spawn worker tasks.
    pub fn spawn(data_pool: PgPool) -> Arc<Self> {
        let q = Arc::new(Self {
            data_pool,
            notify: Arc::new(Notify::new()),
        });

        for worker_id in 0..NUM_WORKERS {
            let q = Arc::clone(&q);
            tokio::spawn(async move {
                worker(worker_id, q).await;
            });
        }

        q
    }

    /// Wake workers after a write inserts into `gsi_pending`.
    pub fn notify_workers(&self) {
        self.notify.notify_waiters();
    }
}

/// Compute a jittered propagation delay (milliseconds) for a single enqueue.
///
/// Restores the non-deterministic propagation the original in-memory queue had
/// — a fixed `NOW() + delay` is perfectly predictable, which is not how
/// DynamoDB's eventual consistency behaves — while keeping the configured delay
/// a **meaningful lower bound**: the result is uniformly distributed in
/// `[delay_ms / 2, delay_ms]`. (The original literally drew `1..=delay`, but
/// jittering all the way to ~0 makes the configured delay almost meaningless
/// and the behaviour impossible to assert on; clustering in the upper half
/// models a propagation latency that varies but does not vanish.) `delay_ms <=
/// 1` is returned unchanged. Callers only enqueue async indexes, so `delay_ms`
/// is always `>= 1` here.
fn jitter_delay_ms(delay_ms: u64) -> u64 {
    if delay_ms <= 1 {
        delay_ms
    } else {
        use rand::Rng;
        rand::rng().random_range(delay_ms / 2 + 1..=delay_ms)
    }
}

/// Insert a pending GSI update for a **single** index within an existing
/// transaction.
///
/// `delay_ms` is the index's effective propagation delay; a per-enqueue jitter
/// is applied (see [`jitter_delay_ms`]) so propagation is not perfectly
/// deterministic. `context` is the self-describing snapshot the worker uses to
/// apply the update without a catalog read.
///
/// `ready_at` is clamped to be **monotonically non-decreasing within the
/// worker partition**: `GREATEST(NOW() + jitter, MAX(ready_at) in partition)`.
/// The worker drains its partition in `id` order but only sees rows whose
/// `ready_at` has elapsed, so without this clamp a later write that drew a
/// smaller jitter could become eligible first and be applied before an earlier
/// write — leaving the index reflecting a stale value. The clamp guarantees a
/// row's `ready_at` never precedes that of a lower-`id` row in the same
/// partition, preserving per-key FIFO convergence while still adding jitter.
/// This matches the original design, where a partition's single consumer slept
/// sequentially and so could never reorder updates.
pub(crate) async fn enqueue_gsi_pending(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table_id: &str,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    delay_ms: u64,
    context: &GsiApplyContext,
) -> Result<(), StorageError> {
    let old_json = old_item
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let new_json = new_item
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let context_json =
        serde_json::to_value(context).map_err(|e| StorageError::Internal(e.to_string()))?;

    // Route all updates for a given base item to one worker (per-key FIFO).
    // The base key is immutable over an item's lifetime; `new_item` carries it
    // for puts/updates, `old_item` for deletes.
    let worker_partition = match new_item.or(old_item) {
        Some(item) => partition_for(&composite_pk_to_text(item, &context.base_key_schema)?),
        None => 0,
    };

    let delay_interval = jitter_delay_ms(delay_ms) as f64 / 1000.0;
    sqlx::query(
        "INSERT INTO gsi_pending \
         (table_id, worker_partition, old_item, new_item, index_context, ready_at) \
         VALUES ($1, $2, $3, $4, $5, GREATEST( \
             NOW() + make_interval(secs => $6), \
             COALESCE( \
                 (SELECT MAX(ready_at) FROM gsi_pending WHERE worker_partition = $2), \
                 NOW() \
             ) \
         ))",
    )
    .bind(table_id)
    .bind(worker_partition)
    .bind(old_json)
    .bind(new_json)
    .bind(context_json)
    .bind(delay_interval)
    .execute(&mut **tx)
    .await
    .map_err(|e| StorageError::Internal(e.to_string()))?;

    Ok(())
}

/// Worker loop. Drains ready rows in its partition, then sleeps until the next
/// row is due or a write notifies it — whichever is sooner.
async fn worker(worker_id: u64, q: Arc<GsiQueue>) {
    // Backstop so a missed notification or clock skew can never stall the
    // worker indefinitely when rows are (or will be) pending.
    const MAX_IDLE: std::time::Duration = std::time::Duration::from_secs(1);

    tracing::debug!("GSI worker {worker_id} started");

    loop {
        match process_batch(worker_id, &q).await {
            Ok(0) => {
                // Nothing ready in this partition. Sleep until the earliest
                // not-yet-due row becomes eligible (honoring the propagation
                // delay to the millisecond) or until a write wakes us.
                let wait = next_ready_wait(&q.data_pool, worker_id)
                    .await
                    .unwrap_or(MAX_IDLE)
                    .min(MAX_IDLE);
                tokio::time::timeout(wait, q.notify.notified()).await.ok();
            }
            Ok(_) => continue,
            Err(e) => {
                tracing::error!("GSI worker {worker_id}: batch error: {e}");
                tokio::time::sleep(MAX_IDLE).await;
            }
        }
    }
}

/// Time until the earliest not-yet-due row in this worker's partition becomes
/// eligible. `None` when the partition has no pending rows (caller waits its
/// backstop interval). A row already due maps to a near-zero wait so the loop
/// re-claims promptly.
async fn next_ready_wait(pool: &PgPool, worker_id: u64) -> Option<std::time::Duration> {
    // The ::float8 cast is load-bearing. Since PostgreSQL 14, EXTRACT returns
    // `numeric`, which sqlx refuses to decode into f64. Without the cast this
    // query fails on every partition that has a pending row, and the error was
    // swallowed into `None` below, indistinguishable from "no rows". The worker
    // then slept its full idle backstop instead of until `ready_at`, so every
    // asynchronous GSI propagation took ~1 s regardless of the configured
    // delay. Errors are logged now so a decode or connection failure can never
    // silently degrade propagation latency again.
    let secs: Option<f64> = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM (MIN(ready_at) - NOW()))::float8 FROM gsi_pending \
         WHERE worker_partition = $1",
    )
    .bind(worker_id as i32)
    .fetch_one(pool)
    .await
    .map_err(|e| tracing::error!("GSI worker {worker_id}: next_ready_wait failed: {e}"))
    .ok()
    .flatten();
    secs.map(|s| {
        if s <= 0.0 {
            std::time::Duration::from_millis(1)
        } else {
            std::time::Duration::from_secs_f64(s)
        }
    })
}

/// Claim and process up to `BATCH_SIZE` ready rows from this worker's partition.
/// Returns the number applied.
///
/// Each row is claimed, applied, and deleted inside a **single transaction**:
///   1. `SELECT ... WHERE worker_partition = $1 AND ready_at <= NOW() ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED`
///   2. apply the snapshotted index updates (savepoint-guarded per index)
///   3. `DELETE` the claimed row
///   4. `COMMIT`
///
/// A crash or error before the `COMMIT` rolls all of it back — the row stays in
/// `gsi_pending` and is retried. Each worker owns one partition and processes it
/// in `id` order, so updates to a given base item apply in order (per-key FIFO);
/// distinct partitions run concurrently. Only rows whose `ready_at` has elapsed
/// are eligible; the propagation delay is enforced by that timestamp.
async fn process_batch(worker_id: u64, q: &GsiQueue) -> Result<usize, StorageError> {
    let mut processed = 0usize;

    for _ in 0..BATCH_SIZE {
        let mut tx = q
            .data_pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row: Option<ClaimedRow> = sqlx::query_as(
            "SELECT id, table_id, old_item, new_item, index_context FROM gsi_pending \
             WHERE worker_partition = $1 AND ready_at <= NOW() \
             ORDER BY id \
             LIMIT 1 \
             FOR UPDATE SKIP LOCKED",
        )
        .bind(worker_id as i32)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let Some((id, table_id, old_json, new_json, ctx_json)) = row else {
            // No ready rows visible to this worker; end the batch.
            let _ = tx.rollback().await;
            break;
        };

        apply_claimed_row(
            worker_id, &mut tx, id, &table_id, old_json, new_json, ctx_json,
        )
        .await?;

        sqlx::query("DELETE FROM gsi_pending WHERE id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        processed += 1;
    }

    Ok(processed)
}

/// Apply the GSI update for one claimed row, within the caller's transaction.
///
/// The row's single target index is applied under a `SAVEPOINT`: if the index
/// table has been dropped (table deleted; SQLSTATE 42P01) the index is skipped
/// and the row is still consumed. Any other error propagates, rolling back the
/// whole transaction so the row is retried.
#[allow(clippy::too_many_arguments)]
async fn apply_claimed_row(
    worker_id: u64,
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: i64,
    table_id: &str,
    old_json: Option<serde_json::Value>,
    new_json: Option<serde_json::Value>,
    ctx_json: serde_json::Value,
) -> Result<(), StorageError> {
    let old_item: Option<Item> = old_json
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let new_item: Option<Item> = new_json
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let context: GsiApplyContext =
        serde_json::from_value(ctx_json).map_err(|e| StorageError::Internal(e.to_string()))?;

    // One index per row. Guard the apply with a savepoint so a dropped-index
    // race (42P01) can be recovered and the row still consumed; aborting the
    // subtransaction otherwise poisons the outer transaction and blocks the
    // subsequent DELETE.
    sqlx::query("SAVEPOINT gsi_apply")
        .execute(&mut **tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    match apply_index(
        tx,
        &context.index,
        old_item.as_ref(),
        new_item.as_ref(),
        &context.base_key_schema,
        &context.attribute_definitions,
    )
    .await
    {
        Ok(()) => {
            sqlx::query("RELEASE SAVEPOINT gsi_apply")
                .execute(&mut **tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }
        Err(e) if is_undefined_table(&e) => {
            // Index table dropped (table deleted) — recover the aborted
            // subtransaction and skip; the row is still consumed.
            sqlx::query("ROLLBACK TO SAVEPOINT gsi_apply")
                .execute(&mut **tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            tracing::debug!(
                "GSI worker {worker_id}: index {} gone, skipping id={id} table={table_id}",
                context.index.index_id
            );
        }
        Err(e) => return Err(e),
    }

    tracing::trace!("GSI worker {worker_id}: applied id={id} table={table_id}");
    Ok(())
}

/// Apply a single index's delete-old / insert-new within the transaction.
async fn apply_index(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    idx: &GsiIndexDef,
    old_item: Option<&Item>,
    new_item: Option<&Item>,
    base_key_schema: &[KeySchemaElement],
    attr_defs: &[AttributeDefinition],
) -> Result<(), StorageError> {
    let idx_table = index_table_name(&idx.index_id);
    let idx_sks = all_sort_key_info(&idx.key_schema, attr_defs);
    let base_sks = all_sort_key_info(base_key_schema, attr_defs);

    if let Some(old) = old_item
        && item_has_index_keys(old, &idx.key_schema)
    {
        delete_index_row_multi(tx, &idx_table, old, base_key_schema, &base_sks).await?;
    }

    if let Some(new) = new_item
        && item_has_index_keys(new, &idx.key_schema)
    {
        let projected =
            project_item_for_index(new, &idx.key_schema, base_key_schema, &idx.projection);
        insert_index_row_multi(
            tx,
            &idx_table,
            new,
            &projected,
            &idx.key_schema,
            base_key_schema,
            &idx_sks,
            &base_sks,
        )
        .await?;
    }

    Ok(())
}
