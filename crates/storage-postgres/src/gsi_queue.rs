// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Async GSI update queue (D-4, D-5, D-6).
//!
//! Base table writes commit independently. For GSIs with a non-zero
//! propagation delay, index updates are enqueued here and applied after a
//! random delay, simulating real DynamoDB eventual consistency.
//!
//! Each queue partition is consumed by a single worker task, guaranteeing
//! per-key FIFO ordering. Workers are event-driven via `Notify` and sleep
//! when the queue is empty.

use std::collections::VecDeque;
use std::sync::Arc;

use extenddb_core::types::{AttributeDefinition, Item, KeySchemaElement, Projection};
use extenddb_storage::error::StorageError;
use sqlx::PgPool;
use tokio::sync::{Mutex, Notify};

use crate::data::{
    all_sort_key_info, delete_index_row_multi, index_table_name, insert_index_row_multi,
    item_has_index_keys, project_item_for_index,
};

/// Number of queue partitions. Each partition has one consumer task.
const NUM_PARTITIONS: usize = 4;

/// PostgreSQL SQLSTATE code for "undefined_table" (relation does not exist).
const PG_UNDEFINED_TABLE: &str = "42P01";

/// Check if a `StorageError` is caused by an undefined table (SQLSTATE 42P01).
///
/// This occurs when a table is deleted while async GSI propagation is in flight.
fn is_undefined_table(err: &StorageError) -> bool {
    match err {
        StorageError::Internal(msg) => msg.contains(PG_UNDEFINED_TABLE),
        _ => false,
    }
}

/// A pending GSI update operation.
struct GsiUpdate {
    _account_id: String,
    table_name: String,
    _table_id: String,
    base_key_schema: Vec<KeySchemaElement>,
    attr_defs: Vec<AttributeDefinition>,
    index_name: String,
    index_id: String,
    index_key_schema: Vec<KeySchemaElement>,
    index_projection: Projection,
    old_item: Option<Item>,
    new_item: Option<Item>,
    delay_ms: u64,
}

/// Partitioned in-memory queue for async GSI updates.
pub struct GsiQueue {
    partitions: Vec<Arc<Partition>>,
}

struct Partition {
    queue: Mutex<VecDeque<GsiUpdate>>,
    notify: Notify,
}

impl GsiQueue {
    /// Create a new queue and spawn worker tasks.
    pub fn spawn(pool: PgPool) -> Arc<Self> {
        let mut partitions = Vec::with_capacity(NUM_PARTITIONS);
        for _ in 0..NUM_PARTITIONS {
            partitions.push(Arc::new(Partition {
                queue: Mutex::new(VecDeque::new()),
                notify: Notify::new(),
            }));
        }
        let q = Arc::new(Self { partitions });

        for (i, part) in q.partitions.iter().enumerate() {
            let part = Arc::clone(part);
            let pool = pool.clone();
            tokio::spawn(async move {
                worker(i, part, pool).await;
            });
        }

        q
    }

    /// Enqueue an async GSI update for a single index.
    ///
    /// The `pk_hash` determines which partition receives the update,
    /// guaranteeing per-key FIFO ordering.
    #[allow(clippy::too_many_arguments)]
    pub async fn enqueue(
        &self,
        pk_hash: u64,
        account_id: &str,
        table_name: &str,
        table_id: &str,
        base_key_schema: &[KeySchemaElement],
        attr_defs: &[AttributeDefinition],
        index_name: &str,
        index_id: &str,
        index_key_schema: &[KeySchemaElement],
        index_projection: &Projection,
        old_item: Option<&Item>,
        new_item: Option<&Item>,
        delay_ms: u64,
    ) {
        let update = GsiUpdate {
            _account_id: account_id.to_owned(),
            table_name: table_name.to_owned(),
            _table_id: table_id.to_owned(),
            base_key_schema: base_key_schema.to_vec(),
            attr_defs: attr_defs.to_vec(),
            index_name: index_name.to_owned(),
            index_id: index_id.to_owned(),
            index_key_schema: index_key_schema.to_vec(),
            index_projection: index_projection.clone(),
            old_item: old_item.cloned(),
            new_item: new_item.cloned(),
            delay_ms,
        };

        let partition_idx = (pk_hash as usize) % NUM_PARTITIONS;
        let part = &self.partitions[partition_idx];
        part.queue.lock().await.push_back(update);
        part.notify.notify_one();
    }
}

/// Worker loop for a single partition. Processes updates with their
/// configured delay, then sleeps until notified.
async fn worker(partition_id: usize, part: Arc<Partition>, pool: PgPool) {
    // Defensive sweep timeout — wake periodically even without notification.
    const SWEEP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

    tracing::debug!("GSI worker {partition_id} started");

    loop {
        // Drain the queue.
        loop {
            let update = {
                let mut q = part.queue.lock().await;
                q.pop_front()
            };
            let Some(update) = update else { break };

            // Apply the configured delay.
            if update.delay_ms > 0 {
                let delay = if update.delay_ms > 1 {
                    use rand::Rng;
                    let mut rng = rand::rng();
                    rng.random_range(1..=update.delay_ms)
                } else {
                    1
                };
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }

            if let Err(e) = apply_gsi_update(&pool, &update).await {
                // D-4: Table deletion races with async GSI propagation.
                // When the target relation no longer exists, this is a
                // normal condition — not an error. Match on PostgreSQL
                // SQLSTATE 42P01 (undefined_table).
                if is_undefined_table(&e) {
                    tracing::debug!(
                        "GSI worker {partition_id}: skipping update for deleted table {}.{}: {e}",
                        update.table_name,
                        update.index_name,
                    );
                } else {
                    tracing::error!(
                        "GSI worker {partition_id}: failed to apply index update for {}.{}: {e}",
                        update.table_name,
                        update.index_name,
                    );
                }
            }
        }

        // Sleep until notified or sweep timeout.
        tokio::time::timeout(SWEEP_TIMEOUT, part.notify.notified())
            .await
            .ok();
    }
}

/// Apply a single GSI update within a transaction.
async fn apply_gsi_update(pool: &PgPool, update: &GsiUpdate) -> Result<(), StorageError> {
    let idx_table = index_table_name(&update.index_id);
    let idx_sks = all_sort_key_info(&update.index_key_schema, &update.attr_defs);
    let base_sks = all_sort_key_info(&update.base_key_schema, &update.attr_defs);

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

    // Delete old index row if the old item had index keys.
    if let Some(ref old) = update.old_item
        && item_has_index_keys(old, &update.index_key_schema)
    {
        delete_index_row_multi(
            &mut tx,
            &idx_table,
            old,
            &update.base_key_schema,
            &update.attr_defs,
            &base_sks,
        )
        .await?;
    }

    // Insert new index row if the new item has index keys.
    if let Some(ref new) = update.new_item
        && item_has_index_keys(new, &update.index_key_schema)
    {
        let projected = project_item_for_index(
            new,
            &update.index_key_schema,
            &update.base_key_schema,
            &update.index_projection,
        );
        insert_index_row_multi(
            &mut tx,
            &idx_table,
            new,
            &projected,
            &update.index_key_schema,
            &update.base_key_schema,
            &update.attr_defs,
            &idx_sks,
            &base_sks,
        )
        .await?;
    }

    tx.commit()
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    Ok(())
}
