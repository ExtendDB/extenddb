// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM user self-service endpoints.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use super::ManagementState;
use super::auth::{CallerIdentity, authenticate};
use super::ops::{OpError, op_err_to_response};

#[derive(Serialize)]
struct AccessKeyCreated {
    access_key_id: String,
    secret_access_key: String,
}

#[derive(Serialize)]
struct AccessKeyEntry {
    access_key_id: String,
    is_active: bool,
    created_at: String,
}

#[derive(Deserialize)]
pub struct ChangePasswordRequest {
    password: String,
}

#[derive(Deserialize)]
pub struct ImportAccessKeyRequest {
    access_key_id: String,
    secret_access_key: String,
}

#[allow(clippy::result_large_err)]
fn authorize_self_service(
    caller: &CallerIdentity,
    account_id: &str,
    user_name: &str,
) -> Result<(), Response> {
    match caller {
        CallerIdentity::Admin(_) => Ok(()),
        CallerIdentity::IamUser {
            account_id: caller_acct,
            user_name: caller_user,
        } => {
            if caller_acct == account_id && caller_user == user_name {
                Ok(())
            } else {
                Err((StatusCode::FORBIDDEN, "Access denied").into_response())
            }
        }
    }
}

/// `POST /management/accounts/{id}/users/{name}/access-keys` — create an access key.
pub async fn create_access_key(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
) -> Response {
    let caller =
        match authenticate(&headers, &*state.catalog_store, &*state.catalog_store, None).await {
            Ok(c) => c,
            Err(e) => return e,
        };
    if let Err(e) = authorize_self_service(&caller, &account_id, &user_name) {
        return e;
    }

    match state
        .catalog_store
        .create_access_key(&account_id, &user_name)
        .await
    {
        Ok(key) => {
            // Write-through invalidation: the new key may have a cached negative
            // (Ok(None)) entry from a prior lookup attempt. Drop it so the next
            // SigV4 verification picks up the credential immediately.
            state
                .auth_cache
                .invalidate_credential(&key.access_key_id)
                .await;
            (
                StatusCode::CREATED,
                axum::Json(AccessKeyCreated {
                    access_key_id: key.access_key_id,
                    secret_access_key: key.secret_access_key,
                }),
            )
                .into_response()
        }
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

/// `GET /management/accounts/{id}/users/{name}/access-keys` — list access keys.
pub async fn list_access_keys(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
) -> Response {
    let caller =
        match authenticate(&headers, &*state.catalog_store, &*state.catalog_store, None).await {
            Ok(c) => c,
            Err(e) => return e,
        };
    if let Err(e) = authorize_self_service(&caller, &account_id, &user_name) {
        return e;
    }

    match state
        .catalog_store
        .list_access_keys(&account_id, &user_name)
        .await
    {
        Ok(rows) => {
            let entries: Vec<AccessKeyEntry> = rows
                .into_iter()
                .map(|(access_key_id, is_active, created_at)| AccessKeyEntry {
                    access_key_id,
                    is_active,
                    created_at: created_at
                        .format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_default(),
                })
                .collect();
            axum::Json(entries).into_response()
        }
        Err(e) => {
            tracing::error!("Management API: list access keys failed: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// `DELETE /management/accounts/{id}/users/{name}/access-keys/{key_id}` — delete an access key.
pub async fn delete_access_key(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, user_name, key_id)): Path<(String, String, String)>,
) -> Response {
    let caller =
        match authenticate(&headers, &*state.catalog_store, &*state.catalog_store, None).await {
            Ok(c) => c,
            Err(e) => return e,
        };
    if let Err(e) = authorize_self_service(&caller, &account_id, &user_name) {
        return e;
    }

    match state
        .catalog_store
        .delete_access_key(&account_id, &user_name, &key_id)
        .await
    {
        Ok(()) => {
            // Write-through invalidation: ensure subsequent SigV4 verifications
            // for the deleted key see UnrecognizedClientException without TTL lag.
            state.auth_cache.invalidate_credential(&key_id).await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

/// `PUT /management/accounts/{id}/users/{name}/password` — change IAM user password.
pub async fn change_user_password(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
    axum::Json(body): axum::Json<ChangePasswordRequest>,
) -> Response {
    let caller =
        match authenticate(&headers, &*state.catalog_store, &*state.catalog_store, None).await {
            Ok(c) => c,
            Err(e) => return e,
        };
    if let Err(e) = authorize_self_service(&caller, &account_id, &user_name) {
        return e;
    }

    if body.password.is_empty() {
        return (StatusCode::BAD_REQUEST, "password must not be empty").into_response();
    }
    if body.password.len() > 72 {
        return (
            StatusCode::BAD_REQUEST,
            "password must not exceed 72 bytes (bcrypt limit)",
        )
            .into_response();
    }

    let hash = match super::password::hash_password(body.password.clone()).await {
        Ok(h) => h,
        Err(e) => {
            tracing::error!("Management API: bcrypt hash failed: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    match state
        .catalog_store
        .change_user_password(&account_id, &user_name, &hash)
        .await
    {
        // No cache invalidation: passwords are only used by the management
        // API's Basic auth path, which queries the catalog directly and
        // does not consult the auth cache. SigV4 (the cached path) uses
        // access keys, not passwords.
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

/// `POST /management/accounts/{id}/users/{name}/access-keys/import` — import an external access key.
pub async fn import_access_key(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
    axum::Json(body): axum::Json<ImportAccessKeyRequest>,
) -> Response {
    let caller =
        match authenticate(&headers, &*state.catalog_store, &*state.catalog_store, None).await {
            Ok(c) => c,
            Err(e) => return e,
        };
    if let Err(e) = authorize_self_service(&caller, &account_id, &user_name) {
        return e;
    }

    // Check runtime setting gate.
    let allowed: bool = match state
        .catalog_store
        .get_setting("allow_credential_import")
        .await
    {
        Ok(Some(v)) => v == "true",
        Ok(None) => true, // Default: allowed.
        Err(e) => {
            tracing::error!("Management API: check allow_credential_import failed: {e:?}");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    if !allowed {
        return (
            StatusCode::FORBIDDEN,
            "Credential import is disabled (allow_credential_import = false)",
        )
            .into_response();
    }

    // Validate access key ID format.
    if body.access_key_id.len() != 20 || !body.access_key_id.starts_with("AKIA") {
        return (
            StatusCode::BAD_REQUEST,
            "access_key_id must be 20 characters starting with AKIA",
        )
            .into_response();
    }
    if body.secret_access_key.len() != 40 {
        return (
            StatusCode::BAD_REQUEST,
            "secret_access_key must be 40 characters",
        )
            .into_response();
    }

    // Load encryption key and encrypt.
    // (Encryption is now handled by the store's import_access_key method.)

    match state
        .catalog_store
        .import_access_key(
            &account_id,
            &user_name,
            &body.access_key_id,
            &body.secret_access_key,
        )
        .await
    {
        Ok(()) => {
            // Drop any cached negative entry for this key.
            state
                .auth_cache
                .invalidate_credential(&body.access_key_id)
                .await;
            (StatusCode::CREATED, "Access key imported").into_response()
        }
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}
