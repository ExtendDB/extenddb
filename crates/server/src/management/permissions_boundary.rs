// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM permissions boundary endpoints (admin only).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::Value;

use super::ManagementState;
use super::auth::authenticate_admin;
use super::is_valid_iam_name;
use super::ops::{OpError, op_err_to_response};

pub async fn set_user_boundary(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
    axum::Json(document): axum::Json<Value>,
) -> Response {
    set_boundary(&state, &headers, &account_id, "user", &user_name, &document).await
}

pub async fn get_user_boundary(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
) -> Response {
    get_boundary(&state, &headers, &account_id, "user", &user_name).await
}

pub async fn delete_user_boundary(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
) -> Response {
    delete_boundary(&state, &headers, &account_id, "user", &user_name).await
}

pub async fn set_role_boundary(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, role_name)): Path<(String, String)>,
    axum::Json(document): axum::Json<Value>,
) -> Response {
    set_boundary(&state, &headers, &account_id, "role", &role_name, &document).await
}

pub async fn get_role_boundary(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, role_name)): Path<(String, String)>,
) -> Response {
    get_boundary(&state, &headers, &account_id, "role", &role_name).await
}

pub async fn delete_role_boundary(
    State(state): State<Arc<ManagementState>>,
    headers: HeaderMap,
    Path((account_id, role_name)): Path<(String, String)>,
) -> Response {
    delete_boundary(&state, &headers, &account_id, "role", &role_name).await
}

async fn set_boundary(
    state: &ManagementState,
    headers: &HeaderMap,
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
    document: &Value,
) -> Response {
    if let Err(e) =
        authenticate_admin(headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    if !is_valid_iam_name(principal_name) {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid principal name: 1-128 chars, alphanumeric and _-.+=@",
        )
            .into_response();
    }

    if document.get("Version").is_none() || document.get("Statement").is_none() {
        return (
            StatusCode::BAD_REQUEST,
            "Permissions boundary must contain Version and Statement",
        )
            .into_response();
    }

    let result = match principal_type {
        "user" => {
            state
                .catalog_store
                .set_user_boundary(account_id, principal_name, document)
                .await
        }
        "role" => {
            state
                .catalog_store
                .set_role_boundary(account_id, principal_name, document)
                .await
        }
        _ => unreachable!(),
    };

    match result {
        Ok(()) => {
            invalidate_boundary_cache(state, account_id, principal_type, principal_name).await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

async fn get_boundary(
    state: &ManagementState,
    headers: &HeaderMap,
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
) -> Response {
    if let Err(e) =
        authenticate_admin(headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    if !is_valid_iam_name(principal_name) {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid principal name: 1-128 chars, alphanumeric and _-.+=@",
        )
            .into_response();
    }

    let result = match principal_type {
        "user" => {
            state
                .catalog_store
                .get_user_boundary(account_id, principal_name)
                .await
        }
        "role" => {
            state
                .catalog_store
                .get_role_boundary(account_id, principal_name)
                .await
        }
        _ => unreachable!(),
    };

    match result {
        Ok(Some(doc)) => axum::Json(doc).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "Permissions boundary not set").into_response(),
        Err(e) => {
            tracing::error!("Management API: get permissions boundary failed: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn delete_boundary(
    state: &ManagementState,
    headers: &HeaderMap,
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
) -> Response {
    if let Err(e) =
        authenticate_admin(headers, &*state.catalog_store, &*state.catalog_store, None).await
    {
        return e;
    }

    if !is_valid_iam_name(principal_name) {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid principal name: 1-128 chars, alphanumeric and _-.+=@",
        )
            .into_response();
    }

    let result = match principal_type {
        "user" => {
            state
                .catalog_store
                .delete_user_boundary(account_id, principal_name)
                .await
        }
        "role" => {
            state
                .catalog_store
                .delete_role_boundary(account_id, principal_name)
                .await
        }
        _ => unreachable!(),
    };

    match result {
        Ok(()) => {
            invalidate_boundary_cache(state, account_id, principal_type, principal_name).await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => op_err_to_response(OpError::from_storage(e)),
    }
}

async fn invalidate_boundary_cache(
    state: &ManagementState,
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
) {
    match principal_type {
        "user" => {
            state
                .auth_cache
                .invalidate_user_boundary(account_id, principal_name)
                .await;
        }
        "role" => {
            state
                .auth_cache
                .invalidate_role_boundary(account_id, principal_name)
                .await;
        }
        _ => {}
    }
}
