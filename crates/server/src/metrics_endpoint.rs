// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `/metrics` endpoint handler — `REQ-OBS-005`.
//!
//! Supports two modes:
//! 1. **In-memory** (fallback when no catalog store): queries the `MetricsCollector` directly.
//! 2. **Database** (default): queries via the `MetricsStore` trait.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::json;

use crate::AppState;

/// GET /metrics — JSON metrics endpoint.
///
/// S-1: `MetricsCollector::query()` holds `std::sync::RwLock` and iterates the
/// full map, so we run it on a blocking thread to avoid stalling the async runtime.
pub(crate) async fn metrics_endpoint(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    axum::extract::Query(params): axum::extract::Query<extenddb_core::metrics::MetricsQuery>,
) -> axum::response::Response {
    // `/metrics` returns per-table CloudWatch-style dimensions (TableName /
    // index), so it requires admin authentication. It uses admin HTTP Basic
    // auth, reusing the management API's mechanism; Prometheus scrapers configure
    // `basic_auth`. The web console dashboard does not call this route — it uses
    // the session-authed `/console/metrics-data` route instead.
    let Some(ref catalog_store) = state.catalog_store else {
        // No catalog store => no admin store to authenticate against. `/metrics`
        // requires admin auth, so an unauthenticatable request returns 401 — the
        // same response an unauthenticated request gets when a catalog IS
        // configured, giving a consistent response regardless of server config.
        // (Not 503: this is not a transient condition a retry would resolve.)
        return (
            StatusCode::UNAUTHORIZED,
            [(
                axum::http::header::WWW_AUTHENTICATE,
                "Basic realm=\"extenddb\"",
            )],
            "metrics requires admin authentication",
        )
            .into_response();
    };
    if let Err(resp) = crate::management::auth::authenticate_admin(
        &headers,
        catalog_store.as_ref(),
        catalog_store.as_ref(),
        None,
    )
    .await
    {
        return resp;
    }

    render_metrics(&state, params).await.into_response()
}

/// Build the metrics response. Callers MUST authenticate before invoking this
/// (top-level `/metrics` uses admin Basic auth; `/console/metrics-data` uses a
/// console session).
pub(crate) async fn render_metrics(
    state: &Arc<AppState>,
    params: extenddb_core::metrics::MetricsQuery,
) -> impl IntoResponse {
    use extenddb_core::metrics::MetricsResponse;

    // Always query from the database when available — all windows get
    // proper time-series buckets.  The in-memory path is a fallback only
    // when no catalog store exists (e.g. unit tests).
    if let Some(ref catalog_store) = state.catalog_store {
        match query_metrics_from_store(catalog_store.as_ref(), &params).await {
            Ok(mut response) => {
                // Enrich with in-memory latency segments (not persisted to DB).
                // Run on blocking thread — holds std::sync::RwLock (same pattern as query()).
                let window = params
                    .window
                    .unwrap_or(extenddb_core::metrics::TimeWindow::Last5Minutes);
                let seg_metrics = state.metrics.clone();
                response.segments =
                    tokio::task::spawn_blocking(move || seg_metrics.query_segments(window))
                        .await
                        .unwrap_or_default();
                return (
                    StatusCode::OK,
                    axum::Json(serde_json::to_value(response).unwrap_or_default()),
                );
            }
            Err(msg) => {
                let body = json!({
                    "__type": "ValidationException",
                    "message": msg,
                });
                return (StatusCode::BAD_REQUEST, axum::Json(body));
            }
        }
    }

    let metrics = state.metrics.clone();
    let window = params
        .window
        .unwrap_or(extenddb_core::metrics::TimeWindow::Last5Minutes);
    let metrics_for_segments = state.metrics.clone();
    let snapshots = match tokio::task::spawn_blocking(move || metrics.query(&params)).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("Metrics query task failed: {e}");
            Vec::new()
        }
    };
    let segments = tokio::task::spawn_blocking(move || metrics_for_segments.query_segments(window))
        .await
        .unwrap_or_default();
    let response = MetricsResponse {
        metrics: snapshots,
        buckets: Vec::new(),
        segments,
        source: "memory".to_owned(),
    };
    (
        StatusCode::OK,
        axum::Json(serde_json::to_value(response).unwrap_or_default()),
    )
}

/// Query historical metrics via the `MetricsStore` trait.
///
/// Aggregates rows into time-series buckets at the requested granularity
/// and also produces aggregate `MetricSnapshot`s for backward compatibility.
pub(crate) async fn query_metrics_from_store(
    store: &dyn extenddb_storage::management_store::MetricsStore,
    params: &extenddb_core::metrics::MetricsQuery,
) -> Result<extenddb_core::metrics::MetricsResponse, String> {
    use extenddb_core::metrics::{
        MetricsResponse, TimeWindow, auto_granularity, parse_granularity, window_duration_secs,
    };

    let now = time::OffsetDateTime::now_utc();

    // Resolve time range.
    let (start_ts, end_ts) = if let Some(ref start_str) = params.start {
        let start =
            time::OffsetDateTime::parse(start_str, &time::format_description::well_known::Rfc3339)
                .map_err(|_| format!("Invalid start time: {start_str}"))?;
        let end = params
            .end
            .as_ref()
            .map(|s| {
                time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                    .map_err(|_| format!("Invalid end time: {s}"))
            })
            .transpose()?
            .unwrap_or(now);
        (start, end)
    } else {
        let window = params.window.unwrap_or(TimeWindow::LastHour);
        match window_duration_secs(window) {
            Some(dur_secs) => {
                #[allow(clippy::cast_possible_wrap)]
                let start = now - time::Duration::seconds(dur_secs as i64);
                (start, now)
            }
            // AllTime: query from epoch.
            None => (time::OffsetDateTime::UNIX_EPOCH, now),
        }
    };

    // Resolve granularity.
    #[allow(clippy::cast_sign_loss)]
    let range_secs = (end_ts - start_ts).whole_seconds().unsigned_abs();
    let gran_secs = params
        .granularity
        .as_deref()
        .and_then(parse_granularity)
        .unwrap_or_else(|| auto_granularity(range_secs));

    let table_filter = params.table_name.as_deref().filter(|s| !s.is_empty());
    let metric_filter = params.metric.map(|m| m.to_string());

    let rows = store
        .query_metrics(start_ts, end_ts, table_filter, metric_filter.as_deref())
        .await
        .map_err(|e| {
            tracing::warn!("Failed to query metrics from store: {e:?}");
            "Internal error querying metrics".to_owned()
        })?;

    // For custom range queries (start/end provided), use `AllTime` as the
    // semantic window since the data covers an arbitrary range.
    let window = params.window.unwrap_or(TimeWindow::AllTime);
    let (snapshots, buckets) = aggregate_rows(&rows, gran_secs, window);

    Ok(MetricsResponse {
        metrics: snapshots,
        buckets,
        segments: Vec::new(),
        source: "database".to_owned(),
    })
}

/// Aggregate storage rows into time-series buckets and overall snapshots.
fn aggregate_rows(
    rows: &[extenddb_storage::management_store::MetricsRow],
    gran_secs: u64,
    window: extenddb_core::metrics::TimeWindow,
) -> (
    Vec<extenddb_core::metrics::MetricSnapshot>,
    Vec<extenddb_core::metrics::MetricsBucket>,
) {
    use extenddb_core::metrics::{Dimension, MetricName, MetricSnapshot, MetricsBucket};
    use std::collections::HashMap;

    // Key: (metric, table_name, index_name, operation)
    type DimKey = (String, String, String, String);

    // Aggregate snapshots (overall).
    let mut snap_map: HashMap<DimKey, (f64, u64, f64, f64)> = HashMap::new();
    // Time-series buckets: (dim_key, bucket_epoch) -> (sum, count, min, max).
    let mut bucket_map: HashMap<(DimKey, i64), (f64, u64, f64, f64)> = HashMap::new();

    for row in rows {
        // CloudWatch-style dimensions: metrics are keyed per table and per
        // index. The endpoint is authenticated (admin Basic auth), so per-table
        // dimensions are only returned to an authorized caller.
        let key = (
            row.metric.clone(),
            row.table_name.clone().unwrap_or_default(),
            row.index_name.clone().unwrap_or_default(),
            row.operation.clone().unwrap_or_default(),
        );

        // Overall aggregate.
        let snap =
            snap_map
                .entry(key.clone())
                .or_insert((0.0, 0, f64::INFINITY, f64::NEG_INFINITY));
        snap.0 += row.sum;
        #[allow(clippy::cast_sign_loss)]
        {
            snap.1 += row.count as u64;
        }
        snap.2 = snap.2.min(row.min);
        snap.3 = snap.3.max(row.max);

        // Time-series bucket.
        let epoch = row.bucket.unix_timestamp();
        #[allow(clippy::cast_possible_wrap)]
        let bucket_epoch = epoch - (epoch % gran_secs as i64);
        let b = bucket_map.entry((key, bucket_epoch)).or_insert((
            0.0,
            0,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ));
        b.0 += row.sum;
        #[allow(clippy::cast_sign_loss)]
        {
            b.1 += row.count as u64;
        }
        b.2 = b.2.min(row.min);
        b.3 = b.3.max(row.max);
    }

    let make_dims = |key: &DimKey| -> Vec<Dimension> {
        let mut dims = Vec::new();
        if !key.1.is_empty() {
            dims.push(Dimension::TableName(key.1.clone()));
        }
        if !key.2.is_empty() {
            dims.push(Dimension::GlobalSecondaryIndexName(key.2.clone()));
        }
        if !key.3.is_empty() {
            dims.push(Dimension::Operation(key.3.clone()));
        }
        dims
    };

    let parse_metric = |s: &str| -> MetricName {
        serde_json::from_value(serde_json::Value::String(s.to_owned())).unwrap_or_else(|_| {
            tracing::warn!("Unknown metric name in DB: {s}, falling back to SystemErrors");
            MetricName::SystemErrors
        })
    };

    let snapshots: Vec<MetricSnapshot> = snap_map
        .iter()
        .map(|(key, (sum, count, min, max))| MetricSnapshot {
            metric: parse_metric(&key.0),
            dimensions: make_dims(key),
            window,
            sum: *sum,
            count: *count,
            min: *min,
            max: *max,
            percentiles: None,
        })
        .collect();

    let mut buckets: Vec<MetricsBucket> = bucket_map
        .iter()
        .map(|((key, epoch), (sum, count, min, max))| {
            let ts = time::OffsetDateTime::from_unix_timestamp(*epoch)
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
            MetricsBucket {
                timestamp: ts
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_default(),
                metric: parse_metric(&key.0),
                dimensions: make_dims(key),
                sum: *sum,
                count: *count,
                min: *min,
                max: *max,
            }
        })
        .collect();
    buckets.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    (snapshots, buckets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_core::metrics::{Dimension, TimeWindow};
    use extenddb_storage::management_store::MetricsRow;

    /// The aggregation MUST retain per-table and per-index dimensions
    /// (CloudWatch parity) so authorized callers get full per-table
    /// observability; this test pins that they are not stripped.
    #[test]
    fn aggregate_rows_retains_table_and_index_dimensions() {
        let rows = vec![MetricsRow {
            bucket: time::OffsetDateTime::UNIX_EPOCH,
            metric: "SystemErrors".to_owned(),
            table_name: Some("MyTable".to_owned()),
            index_name: Some("MyIndex".to_owned()),
            operation: Some("GetItem".to_owned()),
            sum: 1.0,
            count: 1,
            min: 1.0,
            max: 1.0,
        }];

        let (snapshots, _buckets) = aggregate_rows(&rows, 60, TimeWindow::Last5Minutes);

        assert_eq!(snapshots.len(), 1, "expected one dimension group");
        let dims = &snapshots[0].dimensions;
        assert!(
            dims.iter()
                .any(|d| matches!(d, Dimension::TableName(t) if t.as_str() == "MyTable")),
            "TableName dimension must be retained, got {dims:?}"
        );
        assert!(
            dims.iter().any(|d| matches!(
                d,
                Dimension::GlobalSecondaryIndexName(i) if i.as_str() == "MyIndex"
            )),
            "GSI-name dimension must be retained, got {dims:?}"
        );
    }
}
