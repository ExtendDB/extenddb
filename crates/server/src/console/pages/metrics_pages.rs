// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Console metrics dashboard — P58 redesign.
//!
//! Renders a dashboard with line charts (D13), latency drill-down with
//! p50/avg/p99 and operation selector (D14), control/data plane split (D15),
//! availability chart (D16), and CSV/JSON data export (D19). Charts use
//! inline `<canvas>` and no external dependencies.
//!
//! The dashboard and its data feed are for admins only. The feed holds the
//! table names, index names, operations, request counts and latencies of
//! every account, each row under its `AccountId`.
//!
//! HTML and JS content live in `metrics_content` to stay under the 500-line
//! file limit.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use extenddb_storage::management_store::MetricsStore;

use crate::console::ConsoleState;
use crate::console::html;
use crate::management::CallerIdentity;

use super::{
    SessionData, identity_label, is_admin, metrics_content, require_admin, require_session,
};

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
///
/// Admin sessions only: an IAM-user session is redirected to the console
/// dashboard.
pub async fn metrics_page(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    match require_session(&headers, &state).await {
        Ok(session) => render_metrics_page(&session, &state.version_info),
        Err(redirect) => redirect,
    }
}

/// The dashboard page for `session`, or a redirect to the console dashboard
/// when the session is not an admin's.
fn render_metrics_page(session: &SessionData, version_info: &str) -> Response {
    if let Err(redirect) = require_admin(session) {
        return redirect;
    }

    let nav = html::nav_bar(&identity_label(&session.identity));
    let breadcrumb = html::breadcrumb(&[("Console", Some("/console")), ("Metrics", None)]);

    // Build the data-ops list for JS to classify control vs data plane.
    let data_ops_js: String = DATA_OPS
        .iter()
        .map(|op| format!("'{op}'"))
        .collect::<Vec<_>>()
        .join(",");

    let content = format!(
        "{breadcrumb}\n{html}\n<script>\n\
         const DATA_OPS = new Set([{data_ops_js}]);\n\
         {js}\n</script>",
        html = metrics_content::METRICS_HTML,
        js = metrics_content::METRICS_JS,
    );
    Html(html::layout_with_version_csrf(
        "Metrics",
        &nav,
        &content,
        Some(version_info),
        &session.csrf_token,
    ))
    .into_response()
}

/// GET /console/metrics-data: JSON metrics for the dashboard.
///
/// Returns the same payload as the top-level `/metrics`: per-table
/// CloudWatch-style dimensions for every account. It authenticates with a
/// console session cookie instead of admin Basic auth, so the browser
/// dashboard can fetch it, and like `/metrics` it serves admins only. An
/// IAM-user session gets HTTP 403 and no metric data.
pub async fn metrics_data(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    axum::extract::Query(params): axum::extract::Query<extenddb_core::metrics::MetricsQuery>,
) -> Response {
    match require_session(&headers, &state).await {
        Ok(session) => {
            metrics_json(
                &session.identity,
                state.catalog_store.as_ref(),
                &state.metrics,
                &params,
            )
            .await
        }
        Err(redirect) => redirect,
    }
}

/// The metrics payload for `identity`, or HTTP 403 when the caller is not an
/// admin. The refusal is returned before the store or the collector is read.
async fn metrics_json(
    identity: &CallerIdentity,
    store: &dyn MetricsStore,
    collector: &Arc<extenddb_core::metrics::MetricsCollector>,
    params: &extenddb_core::metrics::MetricsQuery,
) -> Response {
    if !is_admin(identity) {
        let body = serde_json::json!({
            "__type": "AccessDeniedException",
            "message": "Metrics are visible to admin users only",
        });
        return (StatusCode::FORBIDDEN, axum::Json(body)).into_response();
    }

    match crate::metrics_endpoint::query_metrics_from_store(store, params).await {
        Ok(mut response) => {
            // Add the in-memory latency segments, which are not persisted to
            // the database, as the top-level `/metrics` handler does.
            let window = params
                .window
                .unwrap_or(extenddb_core::metrics::TimeWindow::Last5Minutes);
            let seg_metrics = Arc::clone(collector);
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

#[cfg(test)]
#[path = "metrics_pages_tests.rs"]
mod tests;
