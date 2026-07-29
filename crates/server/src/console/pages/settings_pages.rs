// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Console settings page — read-only display of all configuration.
//!
//! Visible only to admin users. Shows static `.toml` configuration, runtime
//! settings from the database (with defaults for unset keys), and redacts
//! sensitive values (encryption keys, passwords, connection strings).

use std::fmt::Write;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};

use crate::console::ConsoleState;
use crate::console::html;
// Redaction patterns live in `extenddb-config` so the console and the static
// config display cannot drift apart.
use extenddb_config::should_redact;

use super::{identity_label, is_admin, require_session};

/// Known runtime settings with their default values.
/// These are the settings that can be changed via `extenddb settings set`.
const RUNTIME_DEFAULTS: &[(&str, &str)] = &[
    ("allow_credential_import", "false"),
    ("catalog_version", "—"),
    ("control_plane_delay_seconds", "0.25"),
    ("data_database_connection_string", "—"),
    ("data_database_name", "—"),
    ("gsi_propagation_delay_ms", "500"),
    ("log_level", "info"),
    ("sqlx_log_level", "warn"),
    ("throttling_enabled", "false"),
];

/// GET /console/settings — read-only settings display (admin only).
// Linear page-rendering function: auth check → build toml table → build runtime
// table → assemble HTML. Splitting would scatter the rendering logic.
#[allow(clippy::too_many_lines)]
pub async fn settings_page(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(redirect) => return redirect,
    };

    if !is_admin(&session.identity) {
        return (
            StatusCode::FORBIDDEN,
            "Settings are visible to admin users only",
        )
            .into_response();
    }

    let nav = html::nav_bar(&identity_label(&session.identity));
    let crumbs = html::breadcrumb(&[("Console", Some("/console")), ("Settings", None)]);

    // --- Static configuration from .toml ---
    let mut toml_table = String::from(
        "<h2>Static Configuration (toml)</h2>\
         <p style=\"font-size:0.85rem;color:#666\">Loaded at startup. Requires restart to change.</p>\
         <table><thead><tr><th>Key</th><th>Value</th></tr></thead><tbody>",
    );
    if state.config_entries.is_empty() {
        let _ = write!(
            toml_table,
            "<tr><td colspan=\"2\">No configuration loaded.</td></tr>"
        );
    } else {
        for (key, value) in &state.config_entries {
            let ek = html::escape(key);
            let ev = if should_redact(key) {
                "••••••••".to_owned()
            } else {
                html::escape(value)
            };
            let _ = write!(
                toml_table,
                "<tr><td><code>{ek}</code></td><td><code>{ev}</code></td></tr>",
            );
        }
    }
    toml_table.push_str("</tbody></table>");

    // --- Runtime settings from database, merged with defaults ---
    let db_rows: Vec<(String, String)> = match state.catalog_store.list_settings().await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Failed to query settings: {e:?}");
            Vec::new()
        }
    };

    let mut runtime_table = String::from(
        "<h2>Runtime Settings</h2>\
         <p style=\"font-size:0.85rem;color:#666\">\
         Change with <code>extenddb settings set &lt;key&gt; &lt;value&gt;</code>. \
         Takes effect without restart.</p>\
         <table><thead><tr><th>Key</th><th>Value</th><th>Source</th></tr></thead><tbody>",
    );

    // Build a map of DB values for quick lookup.
    let db_map: std::collections::HashMap<&str, &str> = db_rows
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    for &(key, default) in RUNTIME_DEFAULTS {
        let ek = html::escape(key);
        let (value, source) = if let Some(&db_val) = db_map.get(key) {
            (db_val.to_owned(), "database")
        } else {
            (default.to_owned(), "default")
        };
        let ev = if should_redact(key) {
            "••••••••".to_owned()
        } else {
            html::escape(&value)
        };
        let source_class = if source == "default" {
            " style=\"color:#999\""
        } else {
            ""
        };
        let _ = write!(
            runtime_table,
            "<tr><td><code>{ek}</code></td><td><code>{ev}</code></td>\
             <td{source_class}>{source}</td></tr>",
        );
    }

    // Show any DB settings not in RUNTIME_DEFAULTS (unexpected keys).
    for (key, value) in &db_rows {
        if RUNTIME_DEFAULTS.iter().any(|&(k, _)| k == key) {
            continue;
        }
        let ek = html::escape(key);
        let ev = if should_redact(key) {
            "••••••••".to_owned()
        } else {
            html::escape(value)
        };
        let _ = write!(
            runtime_table,
            "<tr><td><code>{ek}</code></td><td><code>{ev}</code></td>\
             <td>database</td></tr>",
        );
    }

    runtime_table.push_str("</tbody></table>");

    let content = format!(
        r#"{crumbs}
<h1>Settings</h1>
<div class="card">
{toml_table}
{runtime_table}
</div>"#
    );

    Html(html::layout_with_version_csrf(
        "Settings",
        &nav,
        &content,
        Some(&state.version_info),
        &session.csrf_token,
    ))
    .into_response()
}
