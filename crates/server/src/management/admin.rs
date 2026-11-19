// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Admin user management endpoints.

use std::sync::Arc;

use axum::extract::{ConnectInfo, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use super::auth::authenticate_admin;
use super::{ManagementState, is_valid_admin_name};

#[derive(Deserialize)]
pub struct CreateAdminRequest {
    admin_name: String,
    password: String,
}

#[derive(Serialize)]
struct AdminEntry {
    admin_name: String,
    created_at: String,
}

#[derive(Deserialize)]
pub struct ChangePasswordRequest {
    password: String,
}

/// `POST /management/admins` — create a new admin user.
pub async fn create_admin(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    axum::Json(body): axum::Json<CreateAdminRequest>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, addr).await
    {
        return e;
    }

    if body.admin_name.is_empty() {
        return (StatusCode::BAD_REQUEST, "admin_name must not be empty").into_response();
    }
    if !is_valid_admin_name(&body.admin_name) {
        return (
            StatusCode::BAD_REQUEST,
            "admin_name must be 1-64 characters, alphanumeric, hyphens, or underscores",
        )
            .into_response();
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
        .create_admin(&body.admin_name, &hash)
        .await
    {
        Ok(()) => (StatusCode::CREATED, "Admin user created").into_response(),
        Err(extenddb_storage::management_store::OpError::AlreadyExists(_)) => {
            (StatusCode::CONFLICT, "Admin user already exists").into_response()
        }
        Err(e) => {
            tracing::error!("Management API: create admin failed: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// `GET /management/admins` — list all admin users.
pub async fn list_admins(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, addr).await
    {
        return e;
    }

    match state.catalog_store.list_admins().await {
        Ok(entries) => {
            let entries: Vec<AdminEntry> = entries
                .into_iter()
                .map(|e| AdminEntry {
                    admin_name: e.admin_name,
                    created_at: e
                        .created_at
                        .format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_default(),
                })
                .collect();
            axum::Json(entries).into_response()
        }
        Err(e) => {
            tracing::error!("Management API: list admins failed: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// `DELETE /management/admins/{name}` — delete an admin user.
pub async fn delete_admin(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    Path(name): Path<String>,
) -> Response {
    let caller = match authenticate_admin(
        &headers,
        &*state.catalog_store,
        &*state.catalog_store,
        addr,
    )
    .await
    {
        Ok(c) => c,
        Err(e) => return e,
    };

    if caller == name {
        return (StatusCode::BAD_REQUEST, "Cannot delete yourself").into_response();
    }

    match state.catalog_store.delete_admin(&name).await {
        Ok(()) => {
            tracing::warn!(
                target: "extenddb::audit::manage",
                "delete-admin: admin_name={}, caller={}",
                name, caller,
            );
            StatusCode::NO_CONTENT.into_response()
        }
        Err(extenddb_storage::management_store::OpError::NotFound(_)) => {
            (StatusCode::NOT_FOUND, "Admin user not found").into_response()
        }
        Err(e) => {
            tracing::error!("Management API: delete admin failed: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// `PUT /management/admins/{name}/password` — change an admin user's password.
pub async fn change_admin_password(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    Path(name): Path<String>,
    axum::Json(body): axum::Json<ChangePasswordRequest>,
) -> Response {
    if let Err(e) =
        authenticate_admin(&headers, &*state.catalog_store, &*state.catalog_store, addr).await
    {
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
        .change_admin_password(&name, &hash)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(extenddb_storage::management_store::OpError::NotFound(_)) => {
            (StatusCode::NOT_FOUND, "Admin user not found").into_response()
        }
        Err(e) => {
            tracing::error!("Management API: change password failed: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
