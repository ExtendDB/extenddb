// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra-specific background workers.

use std::sync::Arc;
use std::time::Duration;

use std::sync::atomic::AtomicU64;

use extenddb_storage::management_store::SettingsStore;

use crate::CassandraEngine;

/// Poll `gsi_propagation_delay_ms` from settings every 30 seconds and update
/// the in-memory atomic used by put_item/update_item/delete_item.
pub(crate) async fn poll_gsi_delay<S: SettingsStore + ?Sized>(
    store: Arc<S>,
    gsi_delay: Arc<AtomicU64>,
) {
    const POLL_INTERVAL: Duration = Duration::from_secs(30);

    loop {
        tokio::time::sleep(POLL_INTERVAL).await;

        match store.get_setting("gsi_propagation_delay_ms").await {
            Ok(Some(val)) => {
                if let Ok(ms) = val.parse::<u64>() {
                    gsi_delay.store(ms, std::sync::atomic::Ordering::Relaxed);
                }
            }
            Ok(None) => {
                gsi_delay.store(10, std::sync::atomic::Ordering::Relaxed);
            }
            Err(e) => {
                tracing::debug!("Failed to query gsi_propagation_delay_ms: {e:?}");
            }
        }
    }
}

pub(crate) async fn poll_control_plane_transitions<S: SettingsStore + ?Sized>(
    storage: Arc<CassandraEngine>,
    notify: Arc<tokio::sync::Notify>,
    settings: Arc<S>,
) {
    const ACTIVE_POLL: Duration = Duration::from_secs(1);
    const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
    const MARGIN_SECS: f64 = 5.0;

    loop {
        // Idle: wait for a wake signal or timeout (defensive sweep)
        let _ = tokio::time::timeout(IDLE_TIMEOUT, notify.notified()).await;

        // Read control_plane_delay_seconds from settings to compute active window
        let delay_secs = read_control_plane_delay(&*settings).await;
        let active_window = Duration::from_secs_f64(delay_secs + MARGIN_SECS);

        // Active: poll every second for active_window
        let deadline = tokio::time::Instant::now() + active_window;
        loop {
            match storage.process_control_plane_transitions().await {
                Ok(ref t) if t.is_empty() => {}
                Ok(transitions) => {
                    for (name, transition) in &transitions {
                        tracing::info!("Table '{name}': {transition}");
                    }
                }
                Err(e) => {
                    tracing::warn!("Control plane transition poll failed: {e}");
                    break;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(ACTIVE_POLL).await;
        }
    }
}

/// Background worker that detects and recovers stale transactions.
///
/// Scans every account keyspace for transactions older than `timeout` and
/// resumes COMMIT or executes ROLLBACK as appropriate.  Runs every
/// `scan_interval` seconds for the lifetime of the process.
pub(crate) async fn poll_transaction_recovery(
    engine: Arc<CassandraEngine>,
    timeout: Duration,
    scan_interval: Duration,
) {
    loop {
        tokio::time::sleep(scan_interval).await;

        let cutoff = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .saturating_sub(timeout)
            .as_millis() as i64;

        let keyspaces = match list_account_keyspaces(&engine).await {
            Ok(ks) => ks,
            Err(e) => {
                tracing::warn!("transaction_recovery: list keyspaces failed: {e}");
                continue;
            }
        };

        for keyspace in &keyspaces {
            let entries = match engine.scan_old_transactions(keyspace, cutoff).await {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!("transaction_recovery: scan {keyspace} failed: {e}");
                    continue;
                }
            };

            for entry in entries {
                tracing::info!(
                    "transaction_recovery: recovering txn {} (state={}) in {keyspace}",
                    entry.txn_id, entry.state
                );
                if let Err(e) = engine.recover_transaction(keyspace, entry.txn_id).await {
                    tracing::warn!(
                        "transaction_recovery: failed to recover txn {}: {e}",
                        entry.txn_id
                    );
                }
            }
        }
    }
}

/// List all account keyspaces for this engine (keyspaces matching `{prefix}_account_*`).
async fn list_account_keyspaces(engine: &CassandraEngine) -> Result<Vec<String>, extenddb_storage::error::StorageError> {
    let prefix = format!("{}_account_", engine.keyspace_prefix);
    let rows = engine
        .session
        .query("SELECT keyspace_name FROM system_schema.keyspaces")
        .await
        .map_err(|e| extenddb_storage::error::StorageError::Internal(format!("list keyspaces: {e}")))?
        .response_body()
        .map_err(|e| extenddb_storage::error::StorageError::Internal(format!("list keyspaces body: {e}")))?
        .into_rows()
        .unwrap_or_default();

    use cdrs_tokio::types::IntoRustByName as _;
    let mut keyspaces = Vec::new();
    for row in rows {
        let name: Result<String, _> = row.get_r_by_name("keyspace_name");
        if let Ok(name) = name {
            if name.starts_with(&prefix) {
                keyspaces.push(name);
            }
        }
    }
    Ok(keyspaces)
}

async fn read_control_plane_delay<S: SettingsStore + ?Sized>(store: &S) -> f64 {
    store
        .get_setting("control_plane_delay_seconds")
        .await
        .ok()
        .flatten()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|&v| v >= 0.0)
        .unwrap_or(0.25)
}

/// Handle that stops GSI workers when dropped.
pub struct GsiWorkerGuard {
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl Drop for GsiWorkerGuard {
    fn drop(&mut self) {
        self.shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Spawn GSI propagation workers — one per partition.
///
/// Returns a `GsiWorkerGuard`; workers stop when it is dropped.
pub fn spawn_gsi_workers(engine: Arc<CassandraEngine>) -> GsiWorkerGuard {
    let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
    for worker_id in 0..crate::gsi_queue::NUM_WORKERS {
        let engine = engine.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            gsi_worker(worker_id, engine, shutdown).await;
        });
    }
    GsiWorkerGuard { shutdown }
}

async fn gsi_worker(worker_id: u64, engine: Arc<CassandraEngine>, shutdown: Arc<std::sync::atomic::AtomicBool>) {
    const MAX_IDLE: Duration = Duration::from_secs(1);

    tracing::debug!("GSI worker {worker_id} started");

    loop {
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            tracing::debug!("GSI worker {worker_id} shutting down");
            return;
        }
        match gsi_process_batch(worker_id, &engine).await {
            Ok(0) => {
                // Nothing ready. Sleep until the next row is due or a write wakes us.
                let wait = gsi_next_ready_wait(worker_id, &engine)
                    .await
                    .unwrap_or(MAX_IDLE)
                    .min(MAX_IDLE);
                tokio::time::timeout(wait, engine.gsi_queue.notify.notified())
                    .await
                    .ok();
            }
            Ok(_) => continue,
            Err(e) => {
                tracing::error!("GSI worker {worker_id}: {e}");
                tokio::time::sleep(MAX_IDLE).await;
            }
        }
    }
}

/// Time until the earliest not-yet-due row in this partition becomes eligible.
async fn gsi_next_ready_wait(
    worker_id: u64,
    engine: &CassandraEngine,
) -> Option<Duration> {
    // We need to find the minimum ready_at across all account keyspaces.
    // For simplicity, use MAX_IDLE as the wait — the worker will re-check
    // promptly. A more precise implementation would query each keyspace.
    // TODO: query MIN(ready_at) per keyspace and return the smallest delta.
    let _ = (worker_id, engine);
    None
}

/// Claim and process up to 100 ready rows from this worker's partition across
/// all account keyspaces. Returns the total number applied.
async fn gsi_process_batch(
    worker_id: u64,
    engine: &CassandraEngine,
) -> Result<usize, extenddb_storage::error::StorageError> {
    use cdrs_tokio::query_values;
    use cdrs_tokio::types::IntoRustByName as _;
    use extenddb_core::types::Item;

    let keyspaces = list_account_keyspaces(engine).await?;
    let mut total = 0usize;

    for keyspace in &keyspaces {
        let query = format!(
            "SELECT worker_partition, ready_at, id, table_id, old_item, new_item, index_context \
             FROM {keyspace}.gsi_pending \
             WHERE worker_partition = ? AND ready_at <= toTimestamp(now()) \
             ORDER BY ready_at ASC, id ASC \
             LIMIT 100"
        );

        let rows = match crate::cassandra_util::query_rows::<extenddb_storage::error::StorageError>(
            &engine.session,
            &query,
            query_values!(worker_id as i32),
            "gsi_worker",
        )
        .await
        {
            Ok(rows) => rows,
            Err(ref e) if is_table_not_found(e) => {
                tracing::warn!("GSI worker {worker_id}: {keyspace}.gsi_pending not found, skipping keyspace");
                continue;
            }
            Err(e) => return Err(e),
        };

        for row in rows {
            let ready_at: i64 =
                crate::cassandra_util::get_column(&row, "ready_at", "gsi_worker")?;
            let id: uuid::Uuid =
                crate::cassandra_util::get_column(&row, "id", "gsi_worker")?;
            let table_id: String =
                crate::cassandra_util::get_column(&row, "table_id", "gsi_worker")?;
            let old_json: Option<String> = row.get_by_name("old_item").ok().flatten();
            let new_json: Option<String> = row.get_by_name("new_item").ok().flatten();
            let ctx_json: String =
                crate::cassandra_util::get_column(&row, "index_context", "gsi_worker")?;

            let old_item: Option<Item> = old_json
                .map(|s| serde_json::from_str(&s))
                .transpose()
                .map_err(|e| extenddb_storage::error::StorageError::Internal(e.to_string()))?;
            let new_item: Option<Item> = new_json
                .map(|s| serde_json::from_str(&s))
                .transpose()
                .map_err(|e| extenddb_storage::error::StorageError::Internal(e.to_string()))?;
            let context: crate::gsi_queue::GsiApplyContext =
                serde_json::from_str(&ctx_json)
                    .map_err(|e| extenddb_storage::error::StorageError::Internal(e.to_string()))?;

            // Apply the index update. If the index table is gone (table deleted),
            // log and skip — the row is still deleted below.
            let apply_result = gsi_apply_index(
                engine,
                keyspace,
                &table_id,
                &context,
                old_item.as_ref(),
                new_item.as_ref(),
            )
            .await;

            if let Err(ref e) = apply_result {
                if is_table_not_found(e) {
                    tracing::debug!(
                        "GSI worker {worker_id}: index {} gone, skipping (table={table_id})",
                        context.index.index_id
                    );
                } else {
                    tracing::warn!(
                        "GSI worker {worker_id}: apply failed for table={table_id} index={}: {e}",
                        context.index.index_id
                    );
                    continue;
                }
            }

            // Delete the processed row by full primary key.
            let delete_cql = format!(
                "DELETE FROM {keyspace}.gsi_pending \
                 WHERE worker_partition = ? AND ready_at = ? AND id = ?"
            );
            crate::cassandra_util::execute(
                &engine.session,
                &delete_cql,
                query_values!(worker_id as i32, ready_at, id),
                "gsi_worker_delete",
            )
            .await?;

            total += 1;
        }
    }

    Ok(total)
}

/// Apply a single GSI update (delete old row, insert new row) for one pending entry.
async fn gsi_apply_index(
    engine: &CassandraEngine,
    account_keyspace: &str,
    _table_id: &str,
    context: &crate::gsi_queue::GsiApplyContext,
    old_item: Option<&extenddb_core::types::Item>,
    new_item: Option<&extenddb_core::types::Item>,
) -> Result<(), extenddb_storage::error::StorageError> {
    use crate::data::ddl::all_sort_key_info;
    use crate::data::index::{
        delete_index_row_multi, insert_index_row_multi, item_has_index_keys, project_item_for_index,
    };
    use cdrs_tokio::consistency::Consistency;
    use cdrs_tokio::query::BatchQueryBuilder;

    let idx = &context.index;
    let idx_table = crate::data::ddl::index_table_name(&idx.index_id);
    let idx_sks = all_sort_key_info(&idx.key_schema, &context.attribute_definitions);
    let base_sks = all_sort_key_info(&context.base_key_schema, &context.attribute_definitions);

    let mut batch = BatchQueryBuilder::new().with_consistency(Consistency::LocalQuorum);

    if let Some(old) = old_item {
        if item_has_index_keys(old, &idx.key_schema) {
            delete_index_row_multi(
                &mut batch,
                account_keyspace,
                &idx_table,
                old,
                &idx.key_schema,
                &context.base_key_schema,
                &idx_sks,
                &base_sks,
            )?;
        }
    }

    if let Some(new) = new_item {
        if item_has_index_keys(new, &idx.key_schema) {
            let projected =
                project_item_for_index(new, &idx.key_schema, &context.base_key_schema, &idx.projection);
            insert_index_row_multi(
                &mut batch,
                account_keyspace,
                &idx_table,
                new,
                &projected,
                &idx.key_schema,
                &context.base_key_schema,
                &idx_sks,
                &base_sks,
            )?;
        }
    }

    // Only execute if there's something to do.
    let built = batch
        .build()
        .map_err(|e| extenddb_storage::error::StorageError::Internal(e.to_string()))?;
    if !built.request.queries.is_empty() {
        engine
            .session
            .batch(built)
            .await
            .map_err(|e| extenddb_storage::error::StorageError::Internal(format!("gsi_apply: {e}")))?;
    }

    Ok(())
}

/// Returns true if the error indicates the index table no longer exists.
fn is_table_not_found(err: &extenddb_storage::error::StorageError) -> bool {
    match err {
        extenddb_storage::error::StorageError::Internal(msg) => {
            msg.contains("unconfigured table") || msg.contains("does not exist")
        }
        _ => false,
    }
}
