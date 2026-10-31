// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authentication helpers for the management API.
//!
//! Supports two authentication modes:
//! - Admin users: `admin_name:password` (Basic auth)
//! - IAM users: `account_id/user_name:password` (Basic auth, for self-service)

use std::net::SocketAddr;

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;

use crate::rate_limit::LoginRefusal;

/// Maximum allowed length for the Authorization header (8 KB).
const MAX_AUTH_HEADER_LEN: usize = 8 * 1024;

/// WWW-Authenticate header value for 401 responses (RFC 7235).
const WWW_AUTHENTICATE_BASIC: &str = "Basic realm=\"extenddb\"";

/// The 401 answered to a wrong password or an unknown principal.
fn invalid_credentials() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(axum::http::header::WWW_AUTHENTICATE, WWW_AUTHENTICATE_BASIC)],
        "Invalid credentials",
    )
        .into_response()
}

/// The answer to a login attempt the rate limiter did not admit.
fn refusal_response(refusal: LoginRefusal) -> Response {
    match refusal {
        LoginRefusal::TooManyAttempts(msg) => (StatusCode::TOO_MANY_REQUESTS, msg).into_response(),
        LoginRefusal::InvalidCredentials => invalid_credentials(),
        LoginRefusal::Internal => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

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
///
/// `peer` is the address of the connection the request arrived on. Failed
/// attempts are counted per principal and peer address and per peer address
/// (see [`crate::rate_limit`]); an attempt over either limit is answered 429
/// before any password is checked. A wrong password is recorded as a failure;
/// a storage error while checking it is answered 500 and recorded as nothing.
/// A principal longer than [`crate::rate_limit::MAX_PRINCIPAL_LEN`] bytes is
/// answered 401 and recorded as nothing.
#[allow(clippy::result_large_err)] // axum Response as rejection is this module's idiom
pub async fn authenticate(
    headers: &HeaderMap,
    store: &dyn extenddb_storage::CatalogStore,
    rate_limiter: &dyn extenddb_storage::management_store::RateLimitStore,
    peer: SocketAddr,
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

    // Reserve the attempt before any password is checked, so a burst cannot
    // run more checks than the limits allow.
    let attempt = crate::rate_limit::begin_login(rate_limiter, username, peer)
        .await
        .map_err(refusal_response)?;

    // Try admin auth first. A storage error returns 500 without recording a
    // failure: no password was checked, so nothing counts against the caller.
    match try_admin_auth(username, password, store).await? {
        AdminAuth::Authenticated(identity) => return Ok(identity),
        AdminAuth::WrongPassword => {
            attempt.record_failure(rate_limiter).await;
            return Err(invalid_credentials());
        }
        AdminAuth::NotFound => {} // Admin not found, try IAM.
    }

    // Try IAM user auth: username format is `account_id/user_name`.
    match try_iam_user_auth(username, password, store).await? {
        Some(identity) => Ok(identity),
        None => {
            attempt.record_failure(rate_limiter).await;
            Err(invalid_credentials())
        }
    }
}

/// Authenticate as admin only. Returns error if caller is not an admin.
#[allow(clippy::result_large_err)] // axum Response as rejection is this module's idiom
pub async fn authenticate_admin(
    headers: &HeaderMap,
    store: &dyn extenddb_storage::CatalogStore,
    rate_limiter: &dyn extenddb_storage::management_store::RateLimitStore,
    peer: SocketAddr,
) -> Result<String, Response> {
    match authenticate(headers, store, rate_limiter, peer).await? {
        CallerIdentity::Admin(name) => Ok(name),
        CallerIdentity::IamUser { .. } => {
            Err((StatusCode::FORBIDDEN, "Admin access required").into_response())
        }
    }
}

/// Outcome of checking a password against the admin store.
enum AdminAuth {
    Authenticated(CallerIdentity),
    /// The admin exists and the password is wrong; do not fall through to IAM.
    WrongPassword,
    /// No admin of that name; fall through to IAM user auth.
    NotFound,
}

/// Check `password` for admin `username`. `Err` is the 500 answered to a
/// storage error, including a timed-out wait for a bcrypt slot.
#[allow(clippy::result_large_err)] // axum Response as rejection is this module's idiom
async fn try_admin_auth(
    username: &str,
    password: &str,
    store: &dyn extenddb_storage::CatalogStore,
) -> Result<AdminAuth, Response> {
    let result = store
        .verify_admin_password(username, password)
        .await
        .map_err(|e| {
            tracing::error!("Management API: DB error during auth: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        })?;

    Ok(match result {
        Some(true) => AdminAuth::Authenticated(CallerIdentity::Admin(username.to_owned())),
        Some(false) => AdminAuth::WrongPassword,
        None => AdminAuth::NotFound,
    })
}

/// Check `password` for IAM user `account_id/user_name`. `Ok(None)` is a
/// wrong password or an unknown user; `Err` is the 500 answered to a storage
/// error.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::PRINCIPAL_LIMITED;

    #[test]
    fn a_refused_attempt_is_answered_by_its_reason() {
        let limited = refusal_response(LoginRefusal::TooManyAttempts(PRINCIPAL_LIMITED));
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);

        // The attempts store could not be read: a server error, not a limit.
        let internal = refusal_response(LoginRefusal::Internal);
        assert_eq!(internal.status(), StatusCode::INTERNAL_SERVER_ERROR);

        // A principal over the length limit gets the wrong-password answer.
        let too_long = refusal_response(LoginRefusal::InvalidCredentials);
        assert_eq!(too_long.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            too_long.headers()[axum::http::header::WWW_AUTHENTICATE],
            WWW_AUTHENTICATE_BASIC
        );
    }
}
