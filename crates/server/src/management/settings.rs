// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Settings management API endpoints.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use super::ManagementState;
use super::auth::authenticate_admin;
use super::ops_settings;

/// Keys whose values must be redacted in API responses.
const REDACTED_KEYS: &[&str] = &[
    "connection_string",
    "encryption_key",
    "password",
    "secret",
    "token",
];

fn should_redact(key: &str) -> bool {
    let lower = key.to_lowercase();
    REDACTED_KEYS.iter().any(|&pattern| lower.contains(pattern))
}

#[derive(Serialize)]
struct SettingEntry {
    key: String,
    value: String,
}

fn op_err_to_response(e: extenddb_storage::management_store::OpError) -> Response {
    super::ops::op_err_to_response(super::ops::OpError::from_storage(e))
}

/// GET /management/settings — list all settings (admin only).
pub async fn list_settings(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
) -> Response {
    if let Err(r) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return r;
    }

    match state.catalog_store.list_settings().await {
        Ok(rows) => {
            let entries: Vec<SettingEntry> = rows
                .into_iter()
                .map(|(key, value)| SettingEntry {
                    value: if should_redact(&key) {
                        "••••••••".to_owned()
                    } else {
                        value
                    },
                    key,
                })
                .collect();
            Json(entries).into_response()
        }
        Err(e) => op_err_to_response(e),
    }
}

/// GET /management/settings/{key} — get a single setting (admin only).
pub async fn get_setting(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path(key): Path<String>,
) -> Response {
    if let Err(r) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return r;
    }

    match state.catalog_store.get_setting(&key).await {
        Ok(Some(value)) => {
            let display_value = if should_redact(&key) {
                "••••••••".to_owned()
            } else {
                value
            };
            Json(SettingEntry {
                key,
                value: display_value,
            })
            .into_response()
        }
        Ok(None) => (axum::http::StatusCode::NOT_FOUND, "Setting not found").into_response(),
        Err(e) => op_err_to_response(e),
    }
}

#[derive(Deserialize)]
pub struct SetSettingRequest {
    value: String,
}

/// PUT /management/settings/{key} — set a setting (admin only).
pub async fn set_setting(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path(key): Path<String>,
    Json(body): Json<SetSettingRequest>,
) -> Response {
    if let Err(r) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return r;
    }

    match ops_settings::set_setting(
        &*state.catalog_store,
        state.setting_context,
        &key,
        &body.value,
    )
    .await
    {
        Ok(()) => {
            let display_value = if should_redact(&key) {
                "••••••••".to_owned()
            } else {
                body.value
            };
            Json(SettingEntry {
                key,
                value: display_value,
            })
            .into_response()
        }
        Err(e) => op_err_to_response(e),
    }
}
