// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite-specific background workers.

use std::sync::Arc;
use std::time::Duration;

use extenddb_core::metrics::MetricsCollector;
use extenddb_storage::management_store::SettingsStore;
use extenddb_storage::{DataEngine, MetadataEngine, StreamEngine};

use crate::engine::SqliteEngine;

pub(crate) async fn poll_control_plane_transitions<S: SettingsStore + ?Sized>(
    storage: Arc<SqliteEngine>,
    notify: Arc<tokio::sync::Notify>,
    settings: Arc<S>,
) {
    const ACTIVE_POLL: Duration = Duration::from_secs(1);
    const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
    const MARGIN_SECS: f64 = 5.0;

    loop {
        let _ = tokio::time::timeout(IDLE_TIMEOUT, notify.notified()).await;

        let delay_secs = read_control_plane_delay(&*settings).await;
        let active_window = Duration::from_secs_f64(delay_secs + MARGIN_SECS);

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

pub(crate) async fn table_size_refresh_worker(storage: Arc<SqliteEngine>) {
    const REFRESH_INTERVAL: Duration = Duration::from_secs(300);

    loop {
        tokio::time::sleep(REFRESH_INTERVAL).await;

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
    storage: Arc<SqliteEngine>,
    metrics: Arc<MetricsCollector>,
) {
    use extenddb_core::metrics::QuerySource;

    const CLEANUP_INTERVAL: Duration = Duration::from_secs(3600);
    const RETENTION_HOURS: i64 = 24;

    loop {
        tokio::time::sleep(CLEANUP_INTERVAL).await;
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
    storage: Arc<SqliteEngine>,
    metrics: Arc<MetricsCollector>,
) {
    use extenddb_core::metrics::QuerySource;

    const CLEANUP_INTERVAL: Duration = Duration::from_secs(600);
    const MAX_AGE_SECONDS: i64 = 600;

    loop {
        tokio::time::sleep(CLEANUP_INTERVAL).await;
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
