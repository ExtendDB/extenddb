// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Settings management API endpoints.

use std::sync::Arc;

use axum::Json;
use axum::extract::{ConnectInfo, Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use super::ManagementState;
use super::auth::authenticate_admin;
use super::ops_settings;

// Redaction patterns and the mask live in one place, `extenddb_config`, so the
// API, the console and the `settings` CLI mask the same keys with the same text.
use extenddb_config::{MASK, should_redact};

#[derive(Serialize)]
struct SettingEntry {
    key: String,
    value: String,
}

/// The value a route returns for the row `key`: [`MASK`] for a key that
/// [`should_redact`] matches, with the stored value wiped first, so the route
/// keeps no copy of a secret such as the catalog key; else the value.
fn shown(key: &str, mut value: String) -> String {
    if should_redact(key) {
        zeroize::Zeroize::zeroize(&mut value);
        MASK.to_owned()
    } else {
        value
    }
}

fn op_err_to_response(e: extenddb_storage::management_store::OpError) -> Response {
    super::ops::op_err_to_response(super::ops::OpError::from_storage(e))
}

/// GET /management/settings — list all settings (admin only).
pub async fn list_settings(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
) -> Response {
    if let Err(r) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, addr).await
    {
        return r;
    }

    match state.catalog_store.list_settings().await {
        Ok(rows) => {
            let entries: Vec<SettingEntry> = rows
                .into_iter()
                .map(|(key, value)| SettingEntry {
                    value: shown(&key, value),
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
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    Path(key): Path<String>,
) -> Response {
    if let Err(r) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, addr).await
    {
        return r;
    }

    match state.catalog_store.get_setting(&key).await {
        Ok(Some(value)) => Json(SettingEntry {
            value: shown(&key, value),
            key,
        })
        .into_response(),
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
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    Path(key): Path<String>,
    Json(body): Json<SetSettingRequest>,
) -> Response {
    if let Err(r) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, addr).await
    {
        return r;
    }

    match ops_settings::set_setting(&*state.catalog_store, &key, &body.value).await {
        Ok(()) => Json(SettingEntry {
            value: shown(&key, body.value),
            key,
        })
        .into_response(),
        Err(e) => op_err_to_response(e),
    }
}

#[cfg(test)]
mod tests {
    use super::{MASK, shown};

    /// A row whose key has a secret pattern is masked, the key ring of a KEK
    /// deployment among them; any other value passes through.
    #[test]
    fn a_secret_row_is_masked_and_others_pass_through() {
        for (key, value, expected) in [
            (
                "encryption_key",
                "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
                MASK,
            ),
            ("encryption_key_ring", "{\"version\":1}", MASK),
            (
                "data_database_connection_string",
                "postgresql://extenddb:apppassword@127.0.0.1/extenddb",
                MASK,
            ),
            ("log_level", "info", "info"),
        ] {
            assert_eq!(shown(key, value.to_owned()), expected, "{key}");
        }
    }
}
