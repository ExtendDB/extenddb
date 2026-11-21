// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Account management endpoints.

use std::sync::Arc;

use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use super::ManagementState;
use super::auth::authenticate_admin;
use super::ops::op_err_to_response;

/// Generate a random 12-digit numeric account ID (matches AWS account ID format).
pub fn generate_account_id() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let id: u64 = rng.random_range(100_000_000_000..1_000_000_000_000);
    id.to_string()
}

#[derive(serde::Deserialize)]
pub struct CreateAccountRequest {
    /// Account ID (12-digit numeric string). Auto-generated if omitted.
    account_id: Option<String>,
    account_name: String,
}

#[derive(Serialize)]
struct AccountEntry {
    account_id: String,
    account_name: String,
    created_at: String,
}

/// `POST /management/accounts` — create a new account.
pub async fn create_account(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    axum::Json(body): axum::Json<CreateAccountRequest>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, addr).await
    {
        return e;
    }

    let account_id = body.account_id.unwrap_or_else(generate_account_id);

    // Validation stays in the server layer.
    if account_id.is_empty() {
        return op_err_to_response(super::ops::OpError::Validation(
            "account_id must not be empty".to_owned(),
        ));
    }
    if account_id.len() != 12 || !account_id.chars().all(|c| c.is_ascii_digit()) {
        return op_err_to_response(super::ops::OpError::Validation(
            "account_id must be a 12-digit numeric string".to_owned(),
        ));
    }
    if body.account_name.is_empty() {
        return op_err_to_response(super::ops::OpError::Validation(
            "account_name must not be empty".to_owned(),
        ));
    }

    match state
        .catalog_store
        .create_account(&account_id, &body.account_name)
        .await
    {
        Ok(()) => {
            // Create the account's bulk store directory so a caller can
            // place files without a first StartBulkPutItems. A failure is
            // logged and the request still succeeds: Start creates the
            // directory again when it is missing.
            if let Some(root) = state
                .bulk
                .as_ref()
                .and_then(|store| store.filesystem_root())
                .cloned()
            {
                let dir_account = account_id.clone();
                let created = tokio::task::spawn_blocking(move || {
                    extenddb_engine::bulk_put::store_root::ensure_account_dir(&root, &dir_account)
                })
                .await
                .unwrap_or_else(|join_error| Err(std::io::Error::other(join_error)));
                if let Err(e) = created {
                    tracing::warn!(
                        "Cannot create bulk store directory for account {account_id}: {e}"
                    );
                }
            }
            let resp =
                serde_json::json!({ "account_id": account_id, "message": "Account created" });
            (StatusCode::CREATED, axum::Json(resp)).into_response()
        }
        Err(e) => op_err_to_response(super::ops::OpError::from_storage(e)),
    }
}

/// `GET /management/accounts` — list all accounts.
pub async fn list_accounts(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, addr).await
    {
        return e;
    }

    match state.catalog_store.list_all_accounts_full().await {
        Ok(rows) => {
            let entries: Vec<AccountEntry> = rows
                .into_iter()
                .map(|(account_id, account_name, created_at)| AccountEntry {
                    account_id,
                    account_name,
                    created_at: created_at
                        .format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_default(),
                })
                .collect();
            axum::Json(entries).into_response()
        }
        Err(e) => {
            tracing::error!("Management API: list accounts failed: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// `DELETE /management/accounts/{id}` — delete an account.
pub async fn delete_account(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    Path(id): Path<String>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, addr).await
    {
        return e;
    }

    match state.catalog_store.delete_account(&id).await {
        Ok(()) => {
            // Cascade invalidation: drop every cached entry across every
            // authz subcache (and the credential cache) that belongs to
            // this account, so a recreate (same id) doesn't inherit the
            // deleted account's principals/policies.
            state.auth_cache.invalidate_account(&id).await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => op_err_to_response(super::ops::OpError::from_storage(e)),
    }
}
