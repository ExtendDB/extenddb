// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM user management endpoints (admin only).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use super::ManagementState;
use super::auth::authenticate_admin;
use super::ops::{OpError, op_err_to_response};

#[derive(Deserialize)]
pub struct CreateUserRequest {
    user_name: String,
    /// Console password (optional — if omitted, user has no console access).
    password: Option<String>,
}

#[derive(Serialize)]
struct UserEntry {
    account_id: String,
    user_name: String,
    user_arn: String,
    has_console_access: bool,
    created_at: String,
}

#[derive(Deserialize)]
pub struct TagRequest {
    tags: Vec<TagEntry>,
}

#[derive(Deserialize)]
pub struct UntagRequest {
    tag_keys: Vec<String>,
}

#[derive(Deserialize, Serialize)]
pub struct TagEntry {
    key: String,
    value: String,
}

/// `POST /management/accounts/{id}/users` — create an IAM user.
pub async fn create_user(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
    axum::Json(body): axum::Json<CreateUserRequest>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    // Validation stays in the server layer.
    if !super::is_valid_iam_name(&body.user_name) {
        return op_err_to_response(OpError::Validation(
            "user_name must be 1-128 characters: alphanumeric, hyphens, underscores, dots, plus, equals, at".to_owned(),
        ));
    }

    if let Some(ref pw) = body.password {
        if pw.is_empty() {
            return op_err_to_response(OpError::Validation(
                "password must not be empty".to_owned(),
            ));
        }
        if pw.len() > 72 {
            return op_err_to_response(OpError::Validation(
                "password must not exceed 72 bytes (bcrypt limit)".to_owned(),
            ));
        }
    }

    let password_hash = match body.password {
        Some(ref pw) => match super::password::hash_password(pw.clone()).await {
            Ok(h) => Some(h),
            Err(e) => {
                tracing::error!("create_user bcrypt hash: {e}");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        },
        None => None,
    };

    match state
        .catalog_store
        .create_user(&account_id, &body.user_name, password_hash.as_deref())
        .await
    {
        Ok(()) => (StatusCode::CREATED, "IAM user created").into_response(),
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

/// `GET /management/accounts/{id}/users` — list IAM users in an account.
pub async fn list_users(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    match state.catalog_store.list_users(&account_id).await {
        Ok(rows) => {
            let entries: Vec<UserEntry> = rows
                .into_iter()
                .map(
                    |(account_id, user_name, user_arn, has_pw, created_at)| UserEntry {
                        account_id,
                        user_name,
                        user_arn,
                        has_console_access: has_pw,
                        created_at: created_at
                            .format(&time::format_description::well_known::Rfc3339)
                            .unwrap_or_default(),
                    },
                )
                .collect();
            axum::Json(entries).into_response()
        }
        Err(e) => {
            tracing::error!("Management API: list IAM users failed: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// `DELETE /management/accounts/{id}/users/{name}` — delete an IAM user.
pub async fn delete_user(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    // Snapshot the user's access keys before deletion so we can invalidate
    // their cached credentials after the cascade delete succeeds. A failed
    // list shouldn't block deletion — fall back to TTL-based propagation
    // (auth.cache.ttl_seconds).
    //
    // The snapshot is racy by design: a CreateAccessKey concurrent with this
    // delete may produce a key not in `access_key_ids`. The defensive
    // `invalidate_principal_credentials` fanout below handles that case by
    // matching on (account_id, principal_name) over the cache values.
    let access_key_ids: Vec<String> = match state
        .catalog_store
        .list_access_keys(&account_id, &user_name)
        .await
    {
        Ok(rows) => rows.into_iter().map(|(id, _, _)| id).collect(),
        Err(e) => {
            tracing::warn!(
                "delete_user: list_access_keys failed before delete; cache invalidation will rely on TTL: {e:?}"
            );
            Vec::new()
        }
    };

    match state
        .catalog_store
        .delete_user(&account_id, &user_name)
        .await
    {
        Ok(()) => {
            // Cascade invalidations: every cached entry keyed by this user
            // must be dropped so a subsequent recreate (same name) doesn't
            // inherit the deleted user's policies/tags.
            for key_id in &access_key_ids {
                state.auth_cache.invalidate_credential(key_id).await;
            }
            // Defensive: any cached credential whose principal_name matches
            // the deleted user (e.g. a key not in our snapshot due to race
            // conditions) is also dropped.
            state
                .auth_cache
                .invalidate_principal_credentials(&account_id, &user_name)
                .await;
            state
                .auth_cache
                .invalidate_user_policies(&account_id, &user_name)
                .await;
            state
                .auth_cache
                .invalidate_user_group_policies(&account_id, &user_name)
                .await;
            state
                .auth_cache
                .invalidate_user_boundary(&account_id, &user_name)
                .await;
            state
                .auth_cache
                .invalidate_user_tags(&account_id, &user_name)
                .await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

/// `PUT /management/accounts/{id}/users/{name}/tags` — tag an IAM user.
pub async fn tag_user(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
    axum::Json(body): axum::Json<TagRequest>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    for tag in &body.tags {
        if tag.key.is_empty() || tag.key.len() > 128 {
            return (StatusCode::BAD_REQUEST, "tag key must be 1-128 characters").into_response();
        }
        if tag.value.len() > 256 {
            return (
                StatusCode::BAD_REQUEST,
                "tag value must not exceed 256 characters",
            )
                .into_response();
        }
    }

    let tags: Vec<(String, String)> = body
        .tags
        .iter()
        .map(|t| (t.key.clone(), t.value.clone()))
        .collect();

    match state
        .catalog_store
        .tag_user(&account_id, &user_name, &tags)
        .await
    {
        Ok(()) => {
            state
                .auth_cache
                .invalidate_user_tags(&account_id, &user_name)
                .await;
            tracing::warn!(
                target: "extenddb::audit::manage",
                "tag-user: account={}, user={}, keys=[{}]",
                account_id, user_name,
                body.tags.iter().map(|t| t.key.as_str()).collect::<Vec<_>>().join(","),
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

/// `DELETE /management/accounts/{id}/users/{name}/tags` — untag an IAM user.
pub async fn untag_user(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
    axum::Json(body): axum::Json<UntagRequest>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    match state
        .catalog_store
        .untag_user(&account_id, &user_name, &body.tag_keys)
        .await
    {
        Ok(()) => {
            state
                .auth_cache
                .invalidate_user_tags(&account_id, &user_name)
                .await;
            tracing::warn!(
                target: "extenddb::audit::manage",
                "untag-user: account={}, user={}, keys=[{}]",
                account_id, user_name,
                body.tag_keys.join(","),
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

/// `GET /management/accounts/{id}/users/{name}/tags` — list tags for an IAM user.
pub async fn list_user_tags(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    match state
        .catalog_store
        .list_user_tags(&account_id, &user_name)
        .await
    {
        Ok(rows) => {
            let entries: Vec<TagEntry> = rows
                .into_iter()
                .map(|(key, value)| TagEntry { key, value })
                .collect();
            axum::Json(entries).into_response()
        }
        Err(e) => {
            tracing::error!("Management API: list IAM user tags failed: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
