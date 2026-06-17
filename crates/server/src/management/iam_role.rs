// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM role management endpoints (admin only).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::ManagementState;
use super::auth::authenticate_admin;
use super::is_valid_iam_name;
use super::ops::{OpError, op_err_to_response};

#[derive(Deserialize)]
pub struct CreateRoleRequest {
    role_name: String,
    trust_policy: Value,
}

#[derive(Serialize)]
struct RoleEntry {
    account_id: String,
    role_name: String,
    role_arn: String,
    trust_policy: Value,
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

/// `POST /management/accounts/{id}/roles` — create an IAM role.
pub async fn create_role(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
    axum::Json(body): axum::Json<CreateRoleRequest>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    if !is_valid_iam_name(&body.role_name) {
        return op_err_to_response(OpError::Validation(
            "role_name must be 1-128 characters: alphanumeric, hyphens, underscores, dots, plus, equals, at".to_owned(),
        ));
    }

    if body.trust_policy.get("Version").is_none() || body.trust_policy.get("Statement").is_none() {
        return op_err_to_response(OpError::Validation(
            "Trust policy must contain Version and Statement".to_owned(),
        ));
    }

    match state
        .catalog_store
        .create_role(&account_id, &body.role_name, &body.trust_policy)
        .await
    {
        Ok(()) => (StatusCode::CREATED, "IAM role created").into_response(),
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

/// `GET /management/accounts/{id}/roles` — list IAM roles in an account.
pub async fn list_roles(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    match state.catalog_store.list_roles(&account_id).await {
        Ok(rows) => {
            let entries: Vec<RoleEntry> = rows
                .into_iter()
                .map(
                    |(account_id, role_name, role_arn, trust_policy, created_at)| RoleEntry {
                        account_id,
                        role_name,
                        role_arn,
                        trust_policy,
                        created_at: created_at
                            .format(&time::format_description::well_known::Rfc3339)
                            .unwrap_or_default(),
                    },
                )
                .collect();
            axum::Json(entries).into_response()
        }
        Err(e) => {
            tracing::error!("Management API: list IAM roles failed: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// `DELETE /management/accounts/{id}/roles/{name}` — delete an IAM role.
pub async fn delete_role(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, role_name)): Path<(String, String)>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    match state
        .catalog_store
        .delete_role(&account_id, &role_name)
        .await
    {
        Ok(()) => {
            // Cascade invalidations: every cached entry keyed by this role
            // must be dropped. ASIA* session credentials issued for this
            // role are dropped via `invalidate_principal_credentials`
            // (their `principal_name` is the role name); session-data
            // entries are dropped via the role-session predicate.
            state
                .auth_cache
                .invalidate_principal_credentials(&account_id, &role_name)
                .await;
            state
                .auth_cache
                .invalidate_role_policies(&account_id, &role_name)
                .await;
            state
                .auth_cache
                .invalidate_role_boundary(&account_id, &role_name)
                .await;
            state
                .auth_cache
                .invalidate_role_tags(&account_id, &role_name)
                .await;
            state
                .auth_cache
                .invalidate_role_sessions(&account_id, &role_name)
                .await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

/// `PUT /management/accounts/{id}/roles/{name}/tags` — tag an IAM role.
pub async fn tag_role(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, role_name)): Path<(String, String)>,
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
        .tag_role(&account_id, &role_name, &tags)
        .await
    {
        Ok(()) => {
            state
                .auth_cache
                .invalidate_role_tags(&account_id, &role_name)
                .await;
            tracing::warn!(
                target: "extenddb::audit::manage",
                "tag-role: account={}, role={}, keys=[{}]",
                account_id, role_name,
                body.tags.iter().map(|t| t.key.as_str()).collect::<Vec<_>>().join(","),
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

/// `DELETE /management/accounts/{id}/roles/{name}/tags` — untag an IAM role.
pub async fn untag_role(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, role_name)): Path<(String, String)>,
    axum::Json(body): axum::Json<UntagRequest>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    match state
        .catalog_store
        .untag_role(&account_id, &role_name, &body.tag_keys)
        .await
    {
        Ok(()) => {
            state
                .auth_cache
                .invalidate_role_tags(&account_id, &role_name)
                .await;
            tracing::warn!(
                target: "extenddb::audit::manage",
                "untag-role: account={}, role={}, keys=[{}]",
                account_id, role_name,
                body.tag_keys.join(","),
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

/// `GET /management/accounts/{id}/roles/{name}/tags` — list tags for an IAM role.
pub async fn list_role_tags(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, role_name)): Path<(String, String)>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    match state
        .catalog_store
        .list_role_tags(&account_id, &role_name)
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
            tracing::error!("Management API: list IAM role tags failed: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
