// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Background workers spawned by `extenddb serve`.
//!
//! Each function runs as a `tokio::spawn`-ed task for the lifetime of the
//! server process. Workers handle log-level polling, control-plane transitions,
//! TTL cleanup, table size refresh, stream record expiry, idempotency token
//! cleanup, capacity warning, and metrics pruning.
//!
//! Workers are generic over storage traits so they are decoupled from the
//! concrete `PostgresEngine` / `PostgresCatalogStore` types.

use std::sync::Arc;

use extenddb_core::throttle::ThrottleManager;
use extenddb_storage::management_store::{MetricsStore, RateLimitStore, SettingsStore};
use tracing_subscriber::{EnvFilter, reload};

/// Poll the `log_level` and `sqlx_log_level` settings from the database
/// and reload the tracing filter when either changes (D-22, D-3).
/// The combined filter is `{log_level},sqlx={sqlx_log_level}`.
/// Falls back to `config_level` when `log_level` is absent from the DB.
/// Runs until the process exits.
pub(crate) async fn poll_log_level(
    store: Arc<dyn SettingsStore>,
    handle: reload::Handle<EnvFilter, tracing_subscriber::Registry>,
    config_level: String,
) {
    use std::time::Duration;

    const POLL_INTERVAL: Duration = Duration::from_secs(30);
    let mut current_level = config_level;
    let mut current_sqlx_level = String::from("warn");

    loop {
        tokio::time::sleep(POLL_INTERVAL).await;

        let (log_result, sqlx_result) = tokio::join!(
            store.get_setting("log_level"),
            store.get_setting("sqlx_log_level"),
        );

        let new_level = match log_result {
            Ok(Some(v)) => v,
            Ok(None) => current_level.clone(),
            Err(_) => {
                tracing::debug!("Failed to query log_level setting");
                continue;
            }
        };

        let new_sqlx_level = match sqlx_result {
            Ok(Some(v)) => v,
            Ok(None) => current_sqlx_level.clone(),
            Err(_) => {
                tracing::debug!("Failed to query sqlx_log_level setting");
                continue;
            }
        };

        if new_level == current_level && new_sqlx_level == current_sqlx_level {
            continue;
        }

        // D-3: Combined filter encodes both levels.
        let filter_str = format!("{new_level},sqlx={new_sqlx_level}");

        match EnvFilter::try_new(&filter_str) {
            Ok(new_filter) => {
                // H-4: Log at warn so the message is visible even when
                // switching to a more restrictive level (e.g. debug → error).
                if new_level != current_level {
                    tracing::warn!("Log level changing to '{new_level}' (from settings table)");
                }
                if new_sqlx_level != current_sqlx_level {
                    tracing::warn!(
                        "sqlx log level changing to '{new_sqlx_level}' (from settings table)"
                    );
                }
                if let Err(e) = handle.reload(new_filter) {
                    tracing::warn!("Failed to reload log filter: {e}");
                } else {
                    current_level = new_level;
                    current_sqlx_level = new_sqlx_level;
                }
            }
            Err(e) => {
                tracing::warn!("Invalid log filter '{filter_str}': {e}");
            }
        }
    }
}

/// Poll the `throttling_enabled` runtime setting and update the
/// `ThrottleManager` when it changes. This allows enabling/disabling
/// throttling at runtime via `extenddb settings set throttling_enabled true`.
pub(crate) async fn poll_throttling_enabled(
    store: Arc<dyn SettingsStore>,
    throttle: Arc<ThrottleManager>,
    config_enabled: bool,
) {
    use std::time::Duration;

    const POLL_INTERVAL: Duration = Duration::from_secs(30);
    let mut current = config_enabled;

    loop {
        tokio::time::sleep(POLL_INTERVAL).await;

        let new_enabled = match store.get_setting("throttling_enabled").await {
            Ok(Some(v)) => v == "true",
            Ok(None) => config_enabled,
            Err(_) => {
                tracing::debug!("Failed to query throttling_enabled setting");
                continue;
            }
        };

        if new_enabled != current {
            tracing::warn!(
                "Throttling {} (from settings table)",
                if new_enabled { "enabled" } else { "disabled" }
            );
            throttle.set_enabled(new_enabled);
            current = new_enabled;
        }
    }
}

/// Background worker that periodically logs a warning when requests use
/// approximate consumed capacity information.
///
/// Phase 11a: `ConsumedCapacity` returns plausible stubs, not real values.
/// This worker reads and resets the counter on a fixed interval and emits
/// a single log line summarizing usage since the last tick.
pub(crate) async fn capacity_warning_worker() {
    use extenddb_engine::capacity_helpers::CAPACITY_REQUEST_COUNT;
    use std::time::Duration;

    const WARNING_INTERVAL: Duration = Duration::from_secs(3600);

    loop {
        tokio::time::sleep(WARNING_INTERVAL).await;

        let count = CAPACITY_REQUEST_COUNT.swap(0, std::sync::atomic::Ordering::Relaxed);
        if count > 0 {
            tracing::warn!(
                "{count} request(s) used approximate consumed capacity information in the last {} seconds",
                WARNING_INTERVAL.as_secs(),
            );
        }
    }
}

/// Periodically prune metrics data points older than 1 day.
pub(crate) async fn metrics_prune_worker(metrics: Arc<extenddb_core::metrics::MetricsCollector>) {
    use extenddb_core::metrics::QuerySource;

    const PRUNE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);
    loop {
        tokio::time::sleep(PRUNE_INTERVAL).await;
        let cycle_start = std::time::Instant::now();
        metrics.prune();
        #[allow(clippy::cast_precision_loss)]
        let cycle_us = cycle_start.elapsed().as_micros() as f64;
        metrics.record_worker_success(QuerySource::MetricsPrune, cycle_us);
    }
}

/// Periodically flush in-memory metrics to the database.
///
/// Drains data points older than 60 seconds, aggregates them into 1-minute
/// buckets, and upserts via the `MetricsStore` trait. Also prunes DB rows
/// older than 24 hours.
pub(crate) async fn metrics_flush_worker(
    metrics: Arc<extenddb_core::metrics::MetricsCollector>,
    store: Arc<dyn MetricsStore>,
) {
    use extenddb_core::metrics::QuerySource;
    use extenddb_storage::management_store::MetricsRow;

    const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
    const RETENTION: std::time::Duration = std::time::Duration::from_secs(86400);
    loop {
        tokio::time::sleep(FLUSH_INTERVAL).await;
        let cycle_start = std::time::Instant::now();
        let buckets = metrics.drain(FLUSH_INTERVAL);
        if !buckets.is_empty() {
            let rows: Vec<MetricsRow> = buckets
                .iter()
                .map(|b| {
                    let secs = b
                        .bucket
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    #[allow(clippy::cast_possible_wrap)]
                    let bucket_ts = time::OffsetDateTime::from_unix_timestamp(secs as i64)
                        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
                    MetricsRow {
                        bucket: bucket_ts,
                        metric: b.metric.to_string(),
                        table_name: if b.table_name.is_empty() {
                            None
                        } else {
                            Some(b.table_name.clone())
                        },
                        index_name: if b.index_name.is_empty() {
                            None
                        } else {
                            Some(b.index_name.clone())
                        },
                        operation: if b.operation.is_empty() {
                            None
                        } else {
                            Some(b.operation.clone())
                        },
                        sum: b.sum,
                        count: i64::try_from(b.count).unwrap_or(i64::MAX),
                        min: b.min,
                        max: b.max,
                    }
                })
                .collect();
            // insert_metrics logs per-row failures internally and always returns Ok.
            let _ = store.insert_metrics(&rows).await;
        }
        // Prune old DB rows.
        let mut errored = false;
        if let Err(e) = store.prune_metrics(RETENTION).await {
            tracing::warn!("Failed to prune old metrics from DB: {e:?}");
            metrics.record_worker_error(QuerySource::MetricsFlush);
            errored = true;
        }
        if !errored {
            #[allow(clippy::cast_precision_loss)]
            let cycle_us = cycle_start.elapsed().as_micros() as f64;
            metrics.record_worker_success(QuerySource::MetricsFlush, cycle_us);
        }
    }
}

/// Background worker that deletes old login attempt records.
pub(crate) async fn login_attempt_cleanup_worker(store: Arc<dyn RateLimitStore>) {
    use std::time::Duration;

    const CLEANUP_INTERVAL: Duration = Duration::from_secs(3600);
    // Keep records for 24 hours for audit purposes.
    const MAX_AGE_SECONDS: i64 = 86400;

    loop {
        tokio::time::sleep(CLEANUP_INTERVAL).await;
        store.cleanup_old_attempts(MAX_AGE_SECONDS).await;
    }
}
