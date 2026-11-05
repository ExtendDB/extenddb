// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL-specific background workers.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use extenddb_core::metrics::MetricsCollector;
use extenddb_storage::management_store::SettingsStore;
use extenddb_storage::sleep_or_shutdown as tick;
use extenddb_storage::{CancellationToken, DataEngine, MetadataEngine, StreamEngine};
use sqlx::PgPool;

use crate::PostgresEngine;
/// Read the propagation-delay setting through the settings store, preferring the
/// canonical key and falling back to the pre-rename one. See
/// `INDEX_PROPAGATION_DELAY_QUERY` for why the old name is still honoured.
async fn read_index_propagation_delay<S: SettingsStore + ?Sized>(
    settings: &S,
) -> extenddb_storage::management_store::OpResult<Option<String>> {
    if let Some(v) = settings
        .get_setting(extenddb_core::settings_keys::INDEX_PROPAGATION_DELAY_MS)
        .await?
    {
        return Ok(Some(v));
    }
    settings
        .get_setting(extenddb_core::settings_keys::LEGACY_GSI_PROPAGATION_DELAY_MS)
        .await
}

/// How often the control-plane poller retries while active.
pub(crate) const ACTIVE_POLL: Duration = Duration::from_secs(1);

/// The control-plane poller `serve` runs (`spawn_workers`).
///
/// Idle, it waits for a wake on `notify` (`CreateTable`, `DeleteTable`, a table
/// snapshot or in-catalog backup ending its read, startup recovery that left
/// a drop deferred) or for its 60 s idle timeout, and then runs an active
/// window of `control_plane_delay_seconds` + 5 s: a pass at once and one
/// every `active_poll` (one second in `serve`). The window lasts past its end
/// for as long as a `DELETING` row's drop is deferred because a session
/// holds a lock on the table's data tables
/// ([`crate::worker_store::ControlPlanePass::deferred_drops`]): only a
/// snapshot stream and the in-catalog backup wake this poller when they
/// release, and a plain transaction, `pg_dump` or a client read or write do
/// not, so going idle while a drop is deferred would leave the row
/// `DELETING` until the idle pass (up to 60 s after the holder is gone).
///
/// A pass probes each due `DELETING` table for holders and skips a held one
/// without waiting on it, and drops a free one under
/// `delete_table::SYNC_DROP_LOCK_TIMEOUT` (1 s), so a pass costs a probe per
/// held table rather than a lock wait per held table. Staying active
/// therefore bounds the time from a holder's release to its table's drop at
/// about `active_poll` (plus the pass in progress, which is short), whatever
/// the holder and however many other tables are held. A `CreateTable` that is
/// due is activated at the start of the next pass on the same cadence.
///
/// While active it waits on `notify` as well as on its timer, so a wake runs
/// the next pass at once rather than at the next poll, and restarts the
/// window (as a wake while idle starts one). A release that wakes the
/// poller therefore has the drop retried at once.
pub(crate) async fn poll_control_plane_transitions<S: SettingsStore + ?Sized>(
    storage: Arc<PostgresEngine>,
    notify: Arc<tokio::sync::Notify>,
    settings: Arc<S>,
    token: CancellationToken,
    active_poll: Duration,
) {
    const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
    const MARGIN_SECS: f64 = 5.0;

    let window = |delay_secs: f64| Duration::from_secs_f64(delay_secs + MARGIN_SECS);
    // `Some(deadline)` while active.
    let mut deadline: Option<tokio::time::Instant> = None;
    loop {
        if deadline.is_some() {
            let woken = tokio::select! {
                () = token.cancelled() => return,
                () = tokio::time::sleep(active_poll) => false,
                () = notify.notified() => true,
            };
            if woken {
                let delay_secs = read_control_plane_delay(&*settings).await;
                deadline = Some(tokio::time::Instant::now() + window(delay_secs));
            }
        } else {
            // Idle: a wake, the idle timeout (defensive sweep), or shutdown.
            tokio::select! {
                () = token.cancelled() => return,
                _ = tokio::time::timeout(IDLE_TIMEOUT, notify.notified()) => {}
            }
            let delay_secs = read_control_plane_delay(&*settings).await;
            deadline = Some(tokio::time::Instant::now() + window(delay_secs));
        }

        match storage.control_plane_pass().await {
            Ok(pass) => {
                for (name, transition) in &pass.transitions {
                    tracing::info!("Table '{name}': {transition}");
                }
                let past_window = deadline.is_some_and(|d| tokio::time::Instant::now() >= d);
                if pass.deferred_drops == 0 && past_window {
                    deadline = None;
                }
            }
            Err(e) => {
                tracing::warn!("Control plane transition poll failed: {e}");
                deadline = None;
            }
        }
    }
}

/// Startup recovery: one control-plane pass for the transitions a previous
/// server process left (a CREATING table past its transition time, a
/// DELETING row whose drop it never finished). When the pass had to defer a
/// drop because a session still holds the table (a reader that outlived the
/// restart: another server, `pg_dump`, a client transaction), the poller is
/// woken so it starts in its active loop and retries every `active_poll`,
/// rather than starting idle and leaving the row `DELETING` until its first
/// idle pass 60 s later. The wake is stored by the `Notify` until the poller
/// first waits.
pub(crate) async fn recover_control_plane_at_startup(engine: &PostgresEngine) {
    match engine.control_plane_pass().await {
        Ok(pass) => {
            for (name, transition) in &pass.transitions {
                tracing::info!("Recovered table '{name}': {transition}");
            }
            if pass.deferred_drops > 0 {
                tracing::info!(
                    deferred = pass.deferred_drops,
                    "DELETING table(s) still held by a session at startup; the control plane \
                     retries the drop while active"
                );
                engine.control_plane_notify.notify_one();
            }
        }
        Err(e) => tracing::error!("Failed to recover control plane transitions: {e}"),
    }
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

pub(crate) async fn table_size_refresh_worker(
    storage: Arc<PostgresEngine>,
    token: CancellationToken,
) {
    const REFRESH_INTERVAL: Duration = Duration::from_secs(300);

    while tick(&token, REFRESH_INTERVAL).await {
        let tables = match MetadataEngine::all_active_tables(&*storage).await {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("Size refresh worker: failed to list tables: {e}");
                continue;
            }
        };

        for (account_id, table_name) in &tables {
            if let Err(e) =
                MetadataEngine::refresh_table_size(&*storage, account_id, table_name).await
            {
                tracing::warn!("Size refresh worker: failed for {table_name}: {e}");
            }
        }
    }
}

pub(crate) async fn stream_record_cleanup_worker(
    storage: Arc<PostgresEngine>,
    metrics: Arc<MetricsCollector>,
    token: CancellationToken,
) {
    use extenddb_core::metrics::QuerySource;

    const CLEANUP_INTERVAL: Duration = Duration::from_secs(3600);
    const RETENTION_HOURS: i64 = 24;

    while tick(&token, CLEANUP_INTERVAL).await {
        let cycle_start = std::time::Instant::now();

        match StreamEngine::cleanup_expired_stream_records(&*storage, RETENTION_HOURS).await {
            Ok(0) => {
                #[allow(clippy::cast_precision_loss)]
                let cycle_us = cycle_start.elapsed().as_micros() as f64;
                metrics.record_worker_success(QuerySource::StreamCleanup, cycle_us);
            }
            Ok(n) => {
                tracing::info!("Stream cleanup worker: deleted {n} expired record(s)");
                #[allow(clippy::cast_precision_loss)]
                let cycle_us = cycle_start.elapsed().as_micros() as f64;
                metrics.record_worker_success(QuerySource::StreamCleanup, cycle_us);
            }
            Err(e) => {
                tracing::error!("Stream record cleanup failed: {e}");
                metrics.record_worker_error(QuerySource::StreamCleanup);
            }
        }
    }
}

pub(crate) async fn idempotency_token_cleanup_worker(
    storage: Arc<PostgresEngine>,
    metrics: Arc<MetricsCollector>,
    token: CancellationToken,
) {
    use extenddb_core::metrics::QuerySource;

    const CLEANUP_INTERVAL: Duration = Duration::from_secs(600);
    const MAX_AGE_SECONDS: i64 = 600;

    while tick(&token, CLEANUP_INTERVAL).await {
        let cycle_start = std::time::Instant::now();

        match DataEngine::cleanup_expired_idempotency_tokens(&*storage, MAX_AGE_SECONDS).await {
            Ok(0) => {
                #[allow(clippy::cast_precision_loss)]
                let cycle_us = cycle_start.elapsed().as_micros() as f64;
                metrics.record_worker_success(QuerySource::IdempotencyCleanup, cycle_us);
            }
            Ok(n) => {
                tracing::info!("Idempotency cleanup worker: deleted {n} expired token(s)");
                #[allow(clippy::cast_precision_loss)]
                let cycle_us = cycle_start.elapsed().as_micros() as f64;
                metrics.record_worker_success(QuerySource::IdempotencyCleanup, cycle_us);
            }
            Err(e) => {
                tracing::error!("Idempotency token cleanup failed: {e}");
                metrics.record_worker_error(QuerySource::IdempotencyCleanup);
            }
        }
    }
}

pub(crate) async fn poll_gsi_delay<S: SettingsStore + ?Sized>(
    store: Arc<S>,
    gsi_delay: Arc<AtomicU64>,
    token: CancellationToken,
) {
    const POLL_INTERVAL: Duration = Duration::from_secs(30);

    while tick(&token, POLL_INTERVAL).await {
        match read_index_propagation_delay(store.as_ref()).await {
            Ok(Some(val)) => {
                if let Ok(ms) = val.parse::<u64>() {
                    gsi_delay.store(ms, std::sync::atomic::Ordering::Relaxed);
                }
            }
            Ok(None) => {
                // Setting removed - revert to default
                gsi_delay.store(
                    crate::DEFAULT_INDEX_PROPAGATION_DELAY_MS,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            Err(e) => {
                tracing::debug!("Failed to query index_propagation_delay_ms: {e:?}");
            }
        }
    }
}

/// Rebuild vector index builds that have stopped making progress.
///
/// The advisory lock answers "is someone building this?", and nothing else answers
/// "is that someone alive?". A build that dies after its first batch leaves its
/// index CREATING with its queue hold in place, which stops the table's whole index
/// propagation, so without this the only exit is a restart.
///
/// Ownership is taken per index inside the rebuild, so this is safe to run on every
/// front-end: a healthy build holds its lock and renews its heartbeat, and is
/// therefore neither selected nor rebuildable.
pub(crate) async fn vector_stuck_build_worker(
    engine: Arc<crate::PostgresEngine>,
    token: CancellationToken,
) {
    // Long next to a heartbeat renewed every batch, so a slow batch cannot be
    // mistaken for a dead build. The cost of waiting is a paused queue for one
    // table; the cost of being wrong is rebuilding an index that was fine.
    const POLL_INTERVAL: Duration = Duration::from_secs(60);
    const STALE_AFTER: Duration = Duration::from_secs(300);

    while tick(&token, POLL_INTERVAL).await {
        match crate::data::vector_index::rebuild_stuck_vector_indexes(&engine, Some(STALE_AFTER))
            .await
        {
            Ok(0) => {}
            Ok(n) => tracing::warn!(
                rebuilt = n,
                "rebuilt vector index build(s) whose heartbeat had gone stale"
            ),
            Err(e) => tracing::error!("stuck vector build sweep failed: {e}"),
        }
    }
}

pub(crate) async fn pool_metrics_worker(
    catalog_pool: PgPool,
    data_pool: PgPool,
    metrics: Arc<MetricsCollector>,
    token: CancellationToken,
) {
    const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

    while tick(&token, SAMPLE_INTERVAL).await {
        let catalog_size = catalog_pool.size() as usize;
        let catalog_idle = catalog_pool.num_idle();
        let data_size = data_pool.size() as usize;
        let data_idle = data_pool.num_idle();

        // Combined pool stats (catalog + data)
        let total_active =
            (catalog_size.saturating_sub(catalog_idle)) + (data_size.saturating_sub(data_idle));
        let total_idle = catalog_idle + data_idle;

        #[allow(clippy::cast_possible_truncation)]
        metrics.record_pool_state(total_active as u32, total_idle as u32);
    }
}
