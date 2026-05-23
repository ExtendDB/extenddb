// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Auth/authz cache metrics endpoint.
//!
//! Exposes per-cache hit/miss/refresh counters and current entry counts.
//! Authenticated as admin so workload-fingerprinting telemetry is not
//! readable by anyone scraping the public API port.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;

use super::ManagementState;
use super::auth::authenticate_admin;

/// `GET /management/auth-cache-metrics` — JSON snapshot of cache counters.
///
/// Returns per-cache counters (hits, stale-hits, misses, refresh-success,
/// refresh-failure, refresh-skipped-inflight, refresh-dropped-epoch,
/// negative-hits, invalidations) and current entry counts.
pub async fn auth_cache_metrics(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    let mut out = serde_json::Map::new();

    if let Some(c) = &state.auth_cache.credential {
        out.insert(
            "credential".to_owned(),
            serde_json::to_value(snapshot_to_json(
                &c.metrics().snapshot(),
                c.entry_count(),
                c.is_pass_through(),
            ))
            .unwrap_or_default(),
        );
    }
    out.insert(
        "table_key_info".to_owned(),
        serde_json::to_value(snapshot_to_json(
            &state.table_key_info_cache.metrics().snapshot(),
            state.table_key_info_cache.entry_count(),
            state.table_key_info_cache.is_pass_through(),
        ))
        .unwrap_or_default(),
    );
    if let Some(authz_metrics) = state.authz_cache.metrics_snapshot() {
        // The authz block carries one entry per sub-cache plus a single
        // pass_through flag for the whole authz cache (sub-caches share
        // construction-time configuration).
        let mut authz_value =
            serde_json::to_value(authz_metrics).unwrap_or(serde_json::Value::Null);
        if let Some(obj) = authz_value.as_object_mut() {
            obj.insert(
                "pass_through".to_owned(),
                serde_json::Value::Bool(state.authz_cache.is_pass_through()),
            );
        }
        out.insert("authz".to_owned(), authz_value);
    }

    (StatusCode::OK, axum::Json(serde_json::Value::Object(out))).into_response()
}

fn snapshot_to_json(
    s: &extenddb_cache::SwrMetricsSnapshot,
    entry_count: u64,
    pass_through: bool,
) -> serde_json::Value {
    json!({
        "hits": s.hits,
        "stale_hits": s.stale_hits,
        "misses": s.misses,
        "negative_hits": s.negative_hits,
        "refresh_success": s.refresh_success,
        "refresh_failure": s.refresh_failure,
        "refresh_skipped_inflight": s.refresh_skipped_inflight,
        "refresh_dropped_epoch": s.refresh_dropped_epoch,
        "invalidations": s.invalidations,
        "entry_count": entry_count,
        // True when the cache is in kill-switch mode (auth.cache.enabled = false).
        // Distinguishes "cache disabled" from "cache cold" for operators.
        "pass_through": pass_through,
    })
}
