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

pub(crate) async fn poll_control_plane_transitions<S: SettingsStore + ?Sized>(
    storage: Arc<PostgresEngine>,
    notify: Arc<tokio::sync::Notify>,
    settings: Arc<S>,
    token: CancellationToken,
) {
    const ACTIVE_POLL: Duration = Duration::from_secs(1);
    const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
    const MARGIN_SECS: f64 = 5.0;

    loop {
        // Idle: wait for a wake signal, an idle timeout (defensive sweep), or
        // shutdown.
        let shutting_down = tokio::select! {
            () = token.cancelled() => true,
            _ = tokio::time::timeout(IDLE_TIMEOUT, notify.notified()) => false,
        };
        if shutting_down {
            return;
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
            if !tick(&token, ACTIVE_POLL).await {
                return;
            }
        }
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
        .unwrap_or(0.25)
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
        match store.get_setting("gsi_propagation_delay_ms").await {
            Ok(Some(val)) => {
                if let Ok(ms) = val.parse::<u64>() {
                    gsi_delay.store(ms, std::sync::atomic::Ordering::Relaxed);
                }
            }
            Ok(None) => {
                // Setting removed - revert to default
                gsi_delay.store(
                    crate::DEFAULT_GSI_PROPAGATION_DELAY_MS,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            Err(e) => {
                tracing::debug!("Failed to query gsi_propagation_delay_ms: {e:?}");
            }
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
