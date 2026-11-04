// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra-specific background workers.

use std::sync::Arc;
use std::time::Duration;

use std::sync::atomic::AtomicU64;

use extenddb_storage::management_store::SettingsStore;
use extenddb_storage::{CancellationToken, sleep_or_shutdown};

use crate::CassandraEngine;

/// How long `spawn_workers` waits for the GSI delay setting before it spawns
/// the workers. A set starts after every registration again, while the
/// server admits nothing and acts on no loss, so the read is bounded; the
/// poller reads the setting again every 5 s.
pub(crate) const INITIAL_GSI_DELAY_READ: Duration = Duration::from_secs(2);

/// The GSI delay setting, `index_propagation_delay_ms` or else the legacy
/// `gsi_propagation_delay_ms`, read within `bound`: `None` when neither is
/// set to a number, a read fails, or the bound passes first.
pub(crate) async fn read_gsi_delay<S: SettingsStore + ?Sized>(
    store: &S,
    bound: Duration,
) -> Option<u64> {
    let read = async {
        let value = match store.get_setting("index_propagation_delay_ms").await {
            Ok(Some(v)) => Some(v),
            _ => store
                .get_setting("gsi_propagation_delay_ms")
                .await
                .ok()
                .flatten(),
        };
        value.and_then(|v| v.parse::<u64>().ok())
    };
    tokio::time::timeout(bound, read).await.ok().flatten()
}

/// Read `index_propagation_delay_ms` from settings every 5 seconds into
/// `gsi_delay`, until `shutdown` fires.
///
/// Client writes read the setting live
/// ([`CassandraEngine::index_propagation_delay`]); this keeps the cached
/// value current for the background TTL sweep, which uses it, and as the
/// fallback when a live read fails. The value is chosen the same way as the
/// live read: the canonical key, else the pre-rename one, else
/// [`crate::DEFAULT_INDEX_PROPAGATION_DELAY_MS`] when missing or unparseable.
pub(crate) async fn poll_gsi_delay<S: SettingsStore + ?Sized>(
    store: Arc<S>,
    gsi_delay: Arc<AtomicU64>,
    shutdown: CancellationToken,
) {
    const POLL_INTERVAL: Duration = Duration::from_secs(5);

    while sleep_or_shutdown(&shutdown, POLL_INTERVAL).await {
        let canonical = match store.get_setting("index_propagation_delay_ms").await {
            Ok(value) => value,
            Err(e) => {
                tracing::debug!("Failed to query index_propagation_delay_ms: {e:?}");
                continue;
            }
        };
        let legacy = if canonical.is_none() {
            match store.get_setting("gsi_propagation_delay_ms").await {
                Ok(value) => value,
                Err(e) => {
                    tracing::debug!("Failed to query gsi_propagation_delay_ms: {e:?}");
                    continue;
                }
            }
        } else {
            None
        };
        let ms =
            crate::engine::propagation_delay_from_settings(canonical.as_deref(), legacy.as_deref());
        gsi_delay.store(ms, std::sync::atomic::Ordering::Relaxed);
    }
}

pub(crate) async fn poll_control_plane_transitions<S: SettingsStore + ?Sized>(
    storage: Arc<CassandraEngine>,
    notify: Arc<tokio::sync::Notify>,
    settings: Arc<S>,
    shutdown: CancellationToken,
) {
    const ACTIVE_POLL: Duration = Duration::from_secs(1);
    const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
    const MARGIN_SECS: f64 = 5.0;

    loop {
        // Idle: wait for a wake signal or timeout (defensive sweep)
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = tokio::time::timeout(IDLE_TIMEOUT, notify.notified()) => {}
        }

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
            if !sleep_or_shutdown(&shutdown, ACTIVE_POLL).await {
                return;
            }
        }
    }
}

/// Background worker that detects and recovers stale transactions.
///
/// Scans every account keyspace for transactions older than `timeout` and
/// resumes COMMIT or executes ROLLBACK as appropriate.  Runs every
/// `scan_interval` seconds until `shutdown` fires, and stops before the next
/// keyspace once it has.
pub(crate) async fn poll_transaction_recovery(
    engine: Arc<CassandraEngine>,
    timeout: Duration,
    scan_interval: Duration,
    shutdown: CancellationToken,
) {
    while sleep_or_shutdown(&shutdown, scan_interval).await {
        #[allow(clippy::cast_possible_truncation)]
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
            if shutdown.is_cancelled() {
                return;
            }
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
                    entry.txn_id,
                    entry.state
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
pub(crate) async fn list_account_keyspaces(
    engine: &CassandraEngine,
) -> Result<Vec<String>, extenddb_storage::error::StorageError> {
    let prefix = format!("{}_account_", engine.keyspace_prefix);
    let rows = engine
        .session
        .query("SELECT keyspace_name FROM system_schema.keyspaces")
        .await
        .map_err(|e| {
            extenddb_storage::error::StorageError::Internal(format!("list keyspaces: {e}"))
        })?
        .response_body()
        .map_err(|e| {
            extenddb_storage::error::StorageError::Internal(format!("list keyspaces body: {e}"))
        })?
        .into_rows()
        .unwrap_or_default();

    use cdrs_tokio::types::IntoRustByName as _;
    let mut keyspaces = Vec::new();
    for row in rows {
        let name: Result<String, _> = row.get_r_by_name("keyspace_name");
        if let Ok(name) = name
            && name.starts_with(&prefix)
        {
            keyspaces.push(name);
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
        .unwrap_or(crate::DEFAULT_CONTROL_PLANE_DELAY_SECONDS)
}

/// Handle that stops GSI workers when dropped.
pub struct GsiWorkerGuard {
    shutdown: CancellationToken,
}

impl Drop for GsiWorkerGuard {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Spawn GSI propagation workers — one per partition.
///
/// Returns a `GsiWorkerGuard`; workers stop when it is dropped.
#[must_use]
pub fn spawn_gsi_workers(engine: Arc<CassandraEngine>) -> GsiWorkerGuard {
    let shutdown = CancellationToken::new();
    drop(spawn_gsi_workers_until(engine, &shutdown));
    GsiWorkerGuard { shutdown }
}

/// Spawn GSI propagation workers, one per partition, that return once
/// `shutdown` fires; their handles, for the server's drain.
pub(crate) fn spawn_gsi_workers_until(
    engine: Arc<CassandraEngine>,
    shutdown: &CancellationToken,
) -> Vec<tokio::task::JoinHandle<()>> {
    (0..crate::gsi_queue::NUM_WORKERS)
        .map(|worker_id| {
            let engine = engine.clone();
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                gsi_worker(worker_id, engine, shutdown).await;
            })
        })
        .collect()
}

async fn gsi_worker(worker_id: u64, engine: Arc<CassandraEngine>, shutdown: CancellationToken) {
    const MAX_IDLE: Duration = Duration::from_secs(1);

    tracing::debug!("GSI worker {worker_id} started");

    loop {
        if shutdown.is_cancelled() {
            tracing::debug!("GSI worker {worker_id} shutting down");
            return;
        }
        match gsi_process_batch(worker_id, &engine).await {
            Ok(batch) if batch.applied > 0 => continue,
            Ok(batch) => {
                // Nothing applied. Sleep until the earliest row not yet due
                // becomes due, or a write wakes us, at most MAX_IDLE (#407).
                let wait = batch.next_due_ms.map_or(MAX_IDLE, |due| {
                    let ms = due.saturating_sub(crate::cassandra_util::now_millis());
                    #[allow(clippy::cast_sign_loss)]
                    Duration::from_millis(ms.max(0) as u64)
                });
                tokio::select! {
                    () = shutdown.cancelled() => {}
                    _ = tokio::time::timeout(
                        wait.min(MAX_IDLE),
                        engine.gsi_queue.notified(worker_id),
                    ) => {}
                }
            }
            Err(e) => {
                tracing::error!("GSI worker {worker_id}: {e}");
                let _ = sleep_or_shutdown(&shutdown, MAX_IDLE).await;
            }
        }
    }
}

/// What one pass of a worker over its partition found.
struct GsiBatch {
    /// Rows applied (and deleted from the queue).
    applied: usize,
    /// `ready_at` (ms) of the earliest row seen that was not yet due, across
    /// all account keyspaces: when the worker has work again.
    next_due_ms: Option<i64>,
}

/// Apply the due rows of this worker's partition, up to 100 per account
/// keyspace, and find when the next row is due.
///
/// Each keyspace's partition is read in `(ready_at, id)` order without a
/// `ready_at` filter. Rows are applied until the first one not yet due; that
/// row's `ready_at` is the keyspace's next due time. One query per keyspace
/// gives both, where a separate `MIN(ready_at)` read would double the
/// worker's reads. "Due" is judged with this server's clock, the one
/// `enqueue_async_indexes` used to set `ready_at`.
async fn gsi_process_batch(
    worker_id: u64,
    engine: &CassandraEngine,
) -> Result<GsiBatch, extenddb_storage::error::StorageError> {
    use cdrs_tokio::query_values;
    use cdrs_tokio::types::IntoRustByName as _;
    use extenddb_core::types::Item;

    let keyspaces = list_account_keyspaces(engine).await?;
    let mut total = 0usize;
    let mut next_due_ms: Option<i64> = None;
    #[allow(clippy::cast_possible_truncation)]
    let worker_id_i32 = worker_id as i32;

    for keyspace in &keyspaces {
        let query = format!(
            "SELECT worker_partition, ready_at, id, table_id, old_item, new_item, index_context \
             FROM {keyspace}.gsi_pending \
             WHERE worker_partition = ? \
             ORDER BY ready_at ASC, id ASC \
             LIMIT 100"
        );

        let rows = match crate::cassandra_util::query_rows::<extenddb_storage::error::StorageError>(
            &engine.session,
            &query,
            query_values!(worker_id_i32),
            "gsi_worker",
        )
        .await
        {
            Ok(rows) => rows,
            Err(ref e) if is_table_not_found(e) => {
                tracing::warn!(
                    "GSI worker {worker_id}: {keyspace}.gsi_pending not found, skipping keyspace"
                );
                continue;
            }
            Err(e) => return Err(e),
        };

        let now_ms = crate::cassandra_util::now_millis();
        for row in rows {
            // A partition whose queued rows are all deleted still returns its
            // static `last_ready_at` as a row with no clustering key: no work.
            let ready_at: Option<i64> = row.get_by_name("ready_at").ok().flatten();
            let Some(ready_at) = ready_at else {
                continue;
            };
            if ready_at > now_ms {
                // Rows come in ready_at order: none after this one is due.
                next_due_ms = Some(next_due_ms.map_or(ready_at, |due| due.min(ready_at)));
                break;
            }
            let id: uuid::Uuid = crate::cassandra_util::get_column(&row, "id", "gsi_worker")?;
            let table_id: String =
                crate::cassandra_util::get_column(&row, "table_id", "gsi_worker")?;

            // Skip rows for tables that are currently being backfilled.
            if crate::propagation_hold::is_held(&engine.session, keyspace, &table_id).await? {
                continue;
            }

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
            let context: crate::gsi_queue::GsiApplyContext = serde_json::from_str(&ctx_json)
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
                query_values!(worker_id_i32, ready_at, id),
                "gsi_worker_delete",
            )
            .await?;

            total += 1;
        }
    }

    Ok(GsiBatch {
        applied: total,
        next_due_ms,
    })
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

    if let Some(old) = old_item
        && item_has_index_keys(old, &idx.key_schema)
    {
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

    if let Some(new) = new_item
        && item_has_index_keys(new, &idx.key_schema)
    {
        let projected = project_item_for_index(
            new,
            &idx.key_schema,
            &context.base_key_schema,
            &idx.projection,
        );
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

    // Only execute if there's something to do.
    let built = batch
        .build()
        .map_err(|e| extenddb_storage::error::StorageError::Internal(e.to_string()))?;
    if !built.request.queries.is_empty() {
        engine.session.batch(built).await.map_err(|e| {
            extenddb_storage::error::StorageError::Internal(format!("gsi_apply: {e}"))
        })?;
    }

    Ok(())
}

/// Background worker that detects and recovers stale GSI backfills.
///
/// Woken immediately when `apply_table_update` commits a new `CREATING` index,
/// and also sweeps periodically as a backstop. Treats a NULL `backfill_heartbeat`
/// (never started) and a heartbeat older than `stale_threshold` (crashed) the
/// same way: drop+recreate the data table and rerun the backfill. Once
/// `shutdown` fires it takes no further index, and it returns only after the
/// backfills it started have ended, so the server's stop covers them too.
pub(crate) async fn poll_gsi_backfill_recovery(
    engine: Arc<CassandraEngine>,
    notify: Arc<tokio::sync::Notify>,
    stale_threshold: Duration,
    scan_interval: Duration,
    shutdown: CancellationToken,
) {
    let mut backfills = tokio::task::JoinSet::new();
    'scan: loop {
        // Wait for an explicit wake or the periodic backstop timeout.
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = tokio::time::timeout(scan_interval, notify.notified()) => {}
        }
        // Collect the backfills that have ended.
        while backfills.try_join_next().is_some() {}

        let keyspaces = match list_account_keyspaces(&engine).await {
            Ok(ks) => ks,
            Err(e) => {
                tracing::warn!("gsi_recovery: list keyspaces failed: {e}");
                continue;
            }
        };

        for account_ks in &keyspaces {
            if shutdown.is_cancelled() {
                break 'scan;
            }
            if let Err(e) = recover_stale_backfills(
                &engine,
                account_ks,
                stale_threshold,
                &shutdown,
                &mut backfills,
            )
            .await
            {
                tracing::warn!("gsi_recovery: {account_ks}: {e}");
            }
        }
    }
    // A backfill stops at its next request once `shutdown` fires; one that
    // has finished its scan completes the activation and the hold's release.
    while backfills.join_next().await.is_some() {}
}

/// The GSI backfill recovery worker `serve` runs, with the stale threshold
/// and scan interval given, until `shutdown` fires; for tests.
#[cfg(any(test, feature = "test-util"))]
#[doc(hidden)]
#[must_use]
pub fn spawn_gsi_backfill_recovery_for_tests(
    engine: Arc<CassandraEngine>,
    stale_threshold: Duration,
    scan_interval: Duration,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let notify = engine.gsi_backfill_notify.clone();
    tokio::spawn(poll_gsi_backfill_recovery(
        engine,
        notify,
        stale_threshold,
        scan_interval,
        shutdown,
    ))
}

/// Rerun the stale backfills of `account_ks`, each in a task of
/// `backfills` that stops once `shutdown` fires.
async fn recover_stale_backfills(
    engine: &Arc<CassandraEngine>,
    account_ks: &str,
    stale_threshold: Duration,
    shutdown: &CancellationToken,
    backfills: &mut tokio::task::JoinSet<()>,
) -> Result<(), extenddb_storage::error::StorageError> {
    use cdrs_tokio::types::IntoRustByName as _;

    let catalog_ks = engine.catalog_keyspace();

    // Find all CREATING indexes whose heartbeat is stale (or NULL).
    let cutoff_ms = (chrono::Utc::now() - stale_threshold).timestamp_millis();

    // We need table_id, index_name, index_id, key_schema, projection, and the
    // base table's key_schema + attribute_definitions.  Fetch all CREATING
    // indexes from the catalog, then filter by heartbeat age.
    let rows = crate::cassandra_util::query_rows::<extenddb_storage::error::StorageError>(
        &engine.session,
        &format!(
            "SELECT table_id, index_name, index_id, key_schema, projection, \
             backfill_heartbeat \
             FROM {catalog_ks}.indexes \
             WHERE index_status = 'CREATING' ALLOW FILTERING"
        ),
        cdrs_tokio::query_values!(),
        "gsi_recovery_scan",
    )
    .await?;

    for row in rows {
        if shutdown.is_cancelled() {
            return Ok(());
        }
        let heartbeat_ms: Option<i64> = row.get_by_name("backfill_heartbeat").ok().flatten();
        let is_stale = heartbeat_ms.is_none_or(|hb| hb < cutoff_ms);
        if !is_stale {
            continue;
        }

        let table_id: String = crate::cassandra_util::get_column(&row, "table_id", "gsi_recovery")?;
        let index_name: String =
            crate::cassandra_util::get_column(&row, "index_name", "gsi_recovery")?;
        let index_id: String = crate::cassandra_util::get_column(&row, "index_id", "gsi_recovery")?;
        let ks_json: String =
            crate::cassandra_util::get_column(&row, "key_schema", "gsi_recovery")?;
        let proj_json: String =
            crate::cassandra_util::get_column(&row, "projection", "gsi_recovery")?;

        let index_key_schema: Vec<extenddb_core::types::KeySchemaElement> =
            match serde_json::from_str(&ks_json) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("gsi_recovery: bad key_schema for {index_name}: {e}");
                    continue;
                }
            };
        let projection: extenddb_core::types::Projection = match serde_json::from_str(&proj_json) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("gsi_recovery: bad projection for {index_name}: {e}");
                continue;
            }
        };

        // Fetch base table key_schema + attribute_definitions.
        let base_row =
            match crate::cassandra_util::query_optional::<extenddb_storage::error::StorageError>(
                &engine.session,
                &format!(
                    "SELECT key_schema, attribute_definitions \
                 FROM {catalog_ks}.tables WHERE table_id = ? ALLOW FILTERING"
                ),
                cdrs_tokio::query_values!(table_id.as_str()),
                "gsi_recovery_base",
            )
            .await
            {
                Ok(Some(r)) => r,
                Ok(None) => {
                    tracing::warn!(
                        "gsi_recovery: base table for index {index_name} not found, skipping"
                    );
                    continue;
                }
                Err(e) => {
                    tracing::warn!("gsi_recovery: fetch base table for {index_name}: {e}");
                    continue;
                }
            };

        let base_ks_json: String =
            crate::cassandra_util::get_column(&base_row, "key_schema", "gsi_recovery_base")?;
        let base_ad_json: String = crate::cassandra_util::get_column(
            &base_row,
            "attribute_definitions",
            "gsi_recovery_base",
        )?;
        let base_key_schema: Vec<extenddb_core::types::KeySchemaElement> =
            match serde_json::from_str(&base_ks_json) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("gsi_recovery: bad base key_schema for {index_name}: {e}");
                    continue;
                }
            };
        let base_attr_defs: Vec<extenddb_core::types::AttributeDefinition> =
            match serde_json::from_str(&base_ad_json) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("gsi_recovery: bad base attr_defs for {index_name}: {e}");
                    continue;
                }
            };

        tracing::info!(
            "gsi_recovery: recovering stale backfill for index '{index_name}' \
             (table_id={table_id}, index_id={index_id})"
        );

        // Drop and recreate the data table so we start from a clean slate.
        if let Err(e) = engine.drop_index_data_table(account_ks, &index_id).await {
            tracing::warn!("gsi_recovery: drop data table for {index_name}: {e}");
            continue;
        }
        if let Err(e) = engine
            .create_index_data_table(
                account_ks,
                &index_id,
                &index_key_schema,
                &base_attr_defs,
                &base_key_schema,
                &base_attr_defs,
            )
            .await
        {
            tracing::warn!("gsi_recovery: recreate data table for {index_name}: {e}");
            continue;
        }

        // Re-take the propagation hold (it may already be absent if the crash
        // happened after the hold was released but before ACTIVE was written).
        if let Err(e) = crate::propagation_hold::take_propagation_hold(
            &engine.session_arc(),
            account_ks,
            &table_id,
            &index_id,
        )
        .await
        {
            tracing::warn!("gsi_recovery: take hold for {index_name}: {e}");
            continue;
        }

        // Run the backfill in a task of its own so the recovery loop is not
        // blocked, and in `backfills` so the worker returns only after it.
        let session = engine.session_arc();
        let gsi_queue = Arc::clone(&engine.gsi_queue);
        let account_ks_owned = account_ks.to_owned();
        let catalog_ks_owned = catalog_ks.clone();
        let table_id_owned = table_id.clone();
        let index_id_owned = index_id.clone();
        let index_name_owned = index_name.clone();
        let shutdown = shutdown.clone();
        backfills.spawn(async move {
            // Once `shutdown` fires the backfill stops at its next request
            // and the task writes nothing more: the index stays CREATING
            // under its hold, its heartbeat goes stale, and a later pass runs
            // the backfill again, as after a crash.
            let Some(result) = shutdown
                .run_until_cancelled(crate::data::ddl::backfill_gsi(
                    &session,
                    &account_ks_owned,
                    &catalog_ks_owned,
                    &table_id_owned,
                    &index_id_owned,
                    &index_name_owned,
                    &index_key_schema,
                    &base_attr_defs,
                    &base_key_schema,
                    &base_attr_defs,
                    &projection,
                ))
                .await
            else {
                tracing::info!(
                    "gsi_recovery: backfill for '{index_name_owned}' stopped by the shutdown"
                );
                return;
            };

            if let Err(e) = result {
                tracing::error!(
                    "gsi_recovery: backfill for '{index_name_owned}' failed again: {e}"
                );
            } else {
                let flip = crate::cassandra_util::execute::<extenddb_storage::error::StorageError>(
                    &session,
                    &format!(
                        "UPDATE {catalog_ks_owned}.indexes \
                         SET index_status = 'ACTIVE' \
                         WHERE table_id = ? AND index_name = ?"
                    ),
                    cdrs_tokio::query_values!(table_id_owned.as_str(), index_name_owned.as_str()),
                    "gsi_recovery_activate",
                )
                .await;
                if let Err(e) = flip {
                    tracing::error!("gsi_recovery: failed to activate '{index_name_owned}': {e}");
                }
            }

            if let Err(e) = crate::propagation_hold::release_propagation_hold(
                &session,
                &account_ks_owned,
                &table_id_owned,
                &index_id_owned,
            )
            .await
            {
                tracing::error!(
                    "gsi_recovery: failed to release hold for '{index_name_owned}': {e}"
                );
            }

            gsi_queue.notify_workers();
        });
    }

    Ok(())
}

/// Returns true if the error indicates the index table no longer exists.
pub(crate) fn is_table_not_found(err: &extenddb_storage::error::StorageError) -> bool {
    match err {
        extenddb_storage::error::StorageError::Internal(msg) => {
            msg.contains("unconfigured table") || msg.contains("does not exist")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use extenddb_storage::management_store::{OpResult, SettingsStore};
    use futures::future::BoxFuture;
    use tokio::time::Instant;

    /// A settings store holding `rows`, or one whose reads never answer.
    struct Settings {
        rows: Vec<(&'static str, &'static str)>,
        hang: bool,
    }

    impl SettingsStore for Settings {
        fn get_setting(&self, key: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
            let value = self
                .rows
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| (*v).to_owned());
            let hang = self.hang;
            Box::pin(async move {
                if hang {
                    std::future::pending::<()>().await;
                }
                Ok(value)
            })
        }
        fn set_setting(&self, _key: &str, _value: &str) -> BoxFuture<'_, OpResult<()>> {
            Box::pin(async { Ok(()) })
        }
        fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    /// A read of the GSI delay that gets no answer ends at its bound, so a
    /// set started after a registration again spawns its workers.
    #[tokio::test(start_paused = true)]
    async fn a_read_of_the_gsi_delay_that_gets_no_answer_ends_at_its_bound() {
        let store = Settings {
            rows: Vec::new(),
            hang: true,
        };
        let started = Instant::now();
        let read = tokio::time::timeout(
            Duration::from_secs(60),
            super::read_gsi_delay(&store, super::INITIAL_GSI_DELAY_READ),
        )
        .await;
        assert_eq!(read, Ok(None), "the read was not bounded");
        assert_eq!(started.elapsed(), super::INITIAL_GSI_DELAY_READ);
    }

    /// The delay is read from `index_propagation_delay_ms`, else from the
    /// legacy `gsi_propagation_delay_ms`; a value that is not a number reads
    /// as none.
    #[tokio::test]
    async fn the_gsi_delay_is_read_from_the_setting_or_its_legacy_name() {
        let bound = super::INITIAL_GSI_DELAY_READ;
        let read = |rows| async move {
            super::read_gsi_delay(&Settings { rows, hang: false }, bound).await
        };
        assert_eq!(
            read(vec![
                ("index_propagation_delay_ms", "250"),
                ("gsi_propagation_delay_ms", "40")
            ])
            .await,
            Some(250)
        );
        assert_eq!(
            read(vec![("gsi_propagation_delay_ms", "40")]).await,
            Some(40)
        );
        assert_eq!(
            read(vec![("index_propagation_delay_ms", "soon")]).await,
            None
        );
        assert_eq!(read(Vec::new()).await, None);
    }
}
