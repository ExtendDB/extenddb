// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authentication helpers for the management API.
//!
//! Supports two authentication modes:
//! - Admin users: `admin_name:password` (Basic auth)
//! - IAM users: `account_id/user_name:password` (Basic auth, for self-service)

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;

/// Maximum allowed length for the Authorization header (8 KB).
const MAX_AUTH_HEADER_LEN: usize = 8 * 1024;

/// WWW-Authenticate header value for 401 responses (RFC 7235).
const WWW_AUTHENTICATE_BASIC: &str = "Basic realm=\"extenddb\"";

/// Identifies the authenticated caller of a management API request.
#[derive(Debug, Clone)]
pub enum CallerIdentity {
    /// An admin user (full management access).
    Admin(String),
    /// An IAM user (self-service access only).
    IamUser {
        account_id: String,
        user_name: String,
    },
}

/// Extract and verify credentials from the `Authorization: Basic ...` header.
///
/// Tries admin auth first (`name:password`). If the admin name is not found,
/// tries IAM user auth (`account_id/user_name:password`).
///
/// Returns the caller identity on success, or an error response on failure.
/// Enforces per-principal lockout and per-IP rate limiting via the storage backend.
#[allow(clippy::result_large_err)] // axum Response as rejection is this module's idiom
pub async fn authenticate(
    headers: &HeaderMap,
    store: &dyn extenddb_storage::CatalogStore,
    rate_limiter: &dyn extenddb_storage::management_store::RateLimitStore,
    source_ip: Option<&str>,
) -> Result<CallerIdentity, Response> {
    let header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
                "Missing Authorization header",
            )
                .into_response()
        })?;

    // S-2: Cap Authorization header length to prevent heap abuse.
    if header.len() > MAX_AUTH_HEADER_LEN {
        return Err((
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
            "Authorization header exceeds maximum allowed length",
        )
            .into_response());
    }

    let encoded = header.strip_prefix("Basic ").ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
            "Invalid Authorization scheme",
        )
            .into_response()
    })?;

    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| {
            (
                StatusCode::UNAUTHORIZED,
                [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
                "Invalid base64 in Authorization header",
            )
                .into_response()
        })?;

    let credentials = String::from_utf8(decoded).map_err(|_| {
        (
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
            "Invalid UTF-8 in credentials",
        )
            .into_response()
    })?;

    let (username, password) = credentials.split_once(':').ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
            "Invalid credentials format",
        )
            .into_response()
    })?;

    // Rate limit / lockout check before attempting authentication.
    if let Err(msg) =
        crate::rate_limit::check_login_allowed(rate_limiter, username, source_ip).await
    {
        return Err((StatusCode::TOO_MANY_REQUESTS, msg).into_response());
    }

    // Try admin auth first.
    let result = try_admin_auth(username, password, store).await;
    match result {
        Ok(Some(identity)) => {
            return Ok(identity);
        }
        Err(e) => {
            crate::rate_limit::record_failed_login(rate_limiter, username, source_ip).await;
            return Err(e);
        }
        Ok(None) => {} // Admin not found, try IAM.
    }

    // Try IAM user auth: username format is `account_id/user_name`.
    let result = try_iam_user_auth(username, password, store).await;
    match result {
        Ok(Some(identity)) => Ok(identity),
        Err(e) => {
            crate::rate_limit::record_failed_login(rate_limiter, username, source_ip).await;
            Err(e)
        }
        Ok(None) => {
            crate::rate_limit::record_failed_login(rate_limiter, username, source_ip).await;
            Err((
                StatusCode::UNAUTHORIZED,
                [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
                "Invalid credentials",
            )
                .into_response())
        }
    }
}

/// Authenticate as admin only. Returns error if caller is not an admin.
#[allow(clippy::result_large_err)] // axum Response as rejection is this module's idiom
pub async fn authenticate_admin(
    headers: &HeaderMap,
    store: &dyn extenddb_storage::CatalogStore,
    rate_limiter: &dyn extenddb_storage::management_store::RateLimitStore,
    source_ip: Option<&str>,
) -> Result<String, Response> {
    match authenticate(headers, store, rate_limiter, source_ip).await? {
        CallerIdentity::Admin(name) => Ok(name),
        CallerIdentity::IamUser { .. } => {
            Err((StatusCode::FORBIDDEN, "Admin access required").into_response())
        }
    }
}

#[allow(clippy::result_large_err)] // axum Response as rejection is this module's idiom
async fn try_admin_auth(
    username: &str,
    password: &str,
    store: &dyn extenddb_storage::CatalogStore,
) -> Result<Option<CallerIdentity>, Response> {
    let result = store
        .verify_admin_password(username, password)
        .await
        .map_err(|e| {
            tracing::error!("Management API: DB error during auth: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        })?;

    match result {
        Some(true) => Ok(Some(CallerIdentity::Admin(username.to_owned()))),
        Some(false) => {
            // Admin exists but password is wrong — don't fall through to IAM auth.
            Err((
                StatusCode::UNAUTHORIZED,
                [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
                "Invalid credentials",
            )
                .into_response())
        }
        None => Ok(None), // Admin not found, fall through to IAM auth.
    }
}

#[allow(clippy::similar_names)]
#[allow(clippy::result_large_err)] // axum Response as rejection is this module's idiom
async fn try_iam_user_auth(
    username: &str,
    password: &str,
    store: &dyn extenddb_storage::CatalogStore,
) -> Result<Option<CallerIdentity>, Response> {
    let Some((acct_id, uname)) = username.split_once('/') else {
        return Ok(None);
    };

    let valid = store
        .verify_iam_user_password(acct_id, uname, password)
        .await
        .map_err(|e| {
            tracing::error!("Management API: DB error during IAM user auth: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        })?;

    if valid {
        Ok(Some(CallerIdentity::IamUser {
            account_id: acct_id.to_owned(),
            user_name: uname.to_owned(),
        }))
    } else {
        Ok(None)
    }
}
