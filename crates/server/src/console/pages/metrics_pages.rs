// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Console metrics dashboard — P58 redesign.
//!
//! Renders a dashboard with line charts (D13), latency drill-down with
//! p50/avg/p99 and operation selector (D14), control/data plane split (D15),
//! availability chart (D16), admin vs user views (D17), and CSV/JSON
//! data export (D19). Charts use inline `<canvas>` — no external deps.
//!
//! HTML and JS content live in `metrics_content` to stay under the 500-line
//! file limit.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};

use crate::console::ConsoleState;
use crate::console::html;
use crate::management::CallerIdentity;

use super::{identity_label, metrics_content, require_session};

/// Data plane operations (reads + writes on items).
const DATA_OPS: &[&str] = &[
    "GetItem",
    "PutItem",
    "DeleteItem",
    "UpdateItem",
    "Query",
    "Scan",
    "BatchGetItem",
    "BatchWriteItem",
    "TransactGetItems",
    "TransactWriteItems",
];

/// GET /console/metrics — metrics dashboard.
pub async fn metrics_page(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(redirect) => return redirect,
    };

    let nav = html::nav_bar(&identity_label(&session.identity));
    let breadcrumb = html::breadcrumb(&[("Console", Some("/console")), ("Metrics", None)]);

    // D17: For non-admin users, inject their account_id so JS can filter.
    let account_filter = match &session.identity {
        CallerIdentity::IamUser { account_id, .. } => {
            format!("const accountFilter = '{}';", html::escape(account_id))
        }
        CallerIdentity::Admin(_) => "const accountFilter = null;".to_owned(),
    };

    // Build the data-ops list for JS to classify control vs data plane.
    let data_ops_js: String = DATA_OPS
        .iter()
        .map(|op| format!("'{op}'"))
        .collect::<Vec<_>>()
        .join(",");

    let content = format!(
        "{breadcrumb}\n{html}\n<script>\n\
         const DATA_OPS = new Set([{data_ops_js}]);\n\
         {account_filter}\n\
         {js}\n</script>",
        html = metrics_content::METRICS_HTML,
        js = metrics_content::METRICS_JS,
    );
    Html(html::layout_with_version_csrf(
        "Metrics",
        &nav,
        &content,
        Some(&state.version_info),
        &session.csrf_token,
    ))
    .into_response()
}

/// GET /console/metrics-data — session-authed JSON metrics for the dashboard.
///
/// Returns the same payload as the top-level `/metrics` (per-table
/// CloudWatch-style dimensions), but is gated by a console **session** instead
/// of admin Basic auth, so the browser dashboard can fetch it with its session
/// cookie. Both routes require authentication while preserving the per-table
/// dashboard.
pub async fn metrics_data(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<extenddb_core::metrics::MetricsQuery>,
) -> Response {
    // Require a valid console session (same gate as the dashboard page).
    if let Err(redirect) = require_session(&headers, &state).await {
        return redirect;
    }

    match crate::metrics_endpoint::query_metrics_from_store(state.catalog_store.as_ref(), &params)
        .await
    {
        Ok(mut response) => {
            // Enrich with in-memory latency segments (not persisted to DB) —
            // mirrors the top-level `/metrics` handler.
            let window = params
                .window
                .unwrap_or(extenddb_core::metrics::TimeWindow::Last5Minutes);
            let seg_metrics = state.metrics.clone();
            response.segments =
                tokio::task::spawn_blocking(move || seg_metrics.query_segments(window))
                    .await
                    .unwrap_or_default();
            axum::Json(serde_json::to_value(response).unwrap_or_default()).into_response()
        }
        Err(msg) => {
            let body = serde_json::json!({
                "__type": "ValidationException",
                "message": msg,
            });
            (StatusCode::BAD_REQUEST, axum::Json(body)).into_response()
        }
    }
}
