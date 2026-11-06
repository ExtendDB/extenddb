// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Console page handlers.
//!
//! Each handler queries the catalog DB directly and renders server-side HTML.
//! Authentication is via session cookies validated against the in-memory store
//! and, on every request, against the principal's password hash in the catalog.
//! All state-changing POST forms include a CSRF token validated on submission.

mod access_key_pages;
mod account_pages;
mod account_scope;
mod auth_pages;
mod cache_pages;
mod docs_page;
mod group_pages;
mod metrics_content;
mod metrics_pages;
mod policy_pages;
mod role_pages;
mod settings_pages;
mod user_pages;

pub use access_key_pages::{create_access_key, delete_access_key};
pub use account_pages::{
    account_detail, create_account, dashboard, delete_account, list_accounts, new_account_form,
};
pub use auth_pages::{login_page, login_submit, logout};
pub use cache_pages::{cache_page, invalidate_cache};
pub use docs_page::{docs_page, docs_pdf, docs_view};
pub use group_pages::{
    add_group_member, create_group, delete_group, group_detail, new_group_form, remove_group_member,
};
pub use metrics_pages::{metrics_data, metrics_page};
pub use policy_pages::{
    delete_group_policy, delete_role_policy, delete_user_policy, new_group_policy_form,
    new_role_policy_form, new_user_policy_form, put_group_policy, put_role_policy, put_user_policy,
};
pub use role_pages::{create_role, delete_role, new_role_form, role_detail};
pub use settings_pages::settings_page;
pub use user_pages::{create_user, delete_user, new_user_form, user_detail};

use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};

use crate::console::ConsoleState;
use crate::console::html;
use crate::console::session::SessionCheck;
use crate::management::CallerIdentity;

/// Session data returned by `require_session`: identity + CSRF token.
pub struct SessionData {
    pub identity: CallerIdentity,
    pub csrf_token: String,
}

/// Extract the session from the cookie header and check it against the
/// catalog. Returns the session data; a redirect to the login page when there
/// is no session, it expired, or its principal was deleted or had its
/// password changed; or 503 when the catalog cannot be read.
#[allow(clippy::result_large_err)]
async fn require_session(
    headers: &HeaderMap,
    state: &Arc<ConsoleState>,
) -> Result<SessionData, Response> {
    let Some(token) = extract_session_token(headers) else {
        return Err(Redirect::to("/console/login").into_response());
    };
    session_response(state.sessions.validate(&token, &*state.catalog_store).await)
}

/// The session data for a session that passed its check, or the response a
/// console request gets when it did not.
#[allow(clippy::result_large_err)]
fn session_response(check: SessionCheck) -> Result<SessionData, Response> {
    match check {
        SessionCheck::Valid {
            identity,
            csrf_token,
        } => Ok(SessionData {
            identity,
            csrf_token,
        }),
        SessionCheck::Invalid => Err(Redirect::to("/console/login").into_response()),
        SessionCheck::Unavailable => Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "The catalog is unavailable. Try again.",
        )
            .into_response()),
    }
}

/// Validate a CSRF token from a form submission against the session's token.
/// Uses constant-time comparison to prevent timing attacks.
fn validate_csrf(submitted: &str, expected: &str) -> bool {
    if submitted.len() != expected.len() {
        return false;
    }
    submitted
        .bytes()
        .zip(expected.bytes())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

/// Validate the `_csrf` field from a form body against the session CSRF token.
/// Returns `Err(Response)` with HTTP 403 if the token is missing or invalid.
#[allow(clippy::result_large_err)]
fn require_csrf(csrf_field: &str, session: &SessionData) -> Result<(), Response> {
    if validate_csrf(csrf_field, &session.csrf_token) {
        Ok(())
    } else {
        Err((axum::http::StatusCode::FORBIDDEN, "CSRF token mismatch").into_response())
    }
}

/// Form struct for POST handlers that only need CSRF validation (e.g. delete).
#[derive(serde::Deserialize)]
pub struct CsrfOnly {
    #[serde(rename = "_csrf", default)]
    pub csrf: String,
}

/// Extract the session token from the `Cookie` header.
fn extract_session_token(headers: &HeaderMap) -> Option<String> {
    let cookie_header = headers.get("cookie")?.to_str().ok()?;
    for part in cookie_header.split(';') {
        let part = part.trim();
        if let Some(value) = part.strip_prefix("extenddb_session=")
            && !value.is_empty()
        {
            return Some(value.to_owned());
        }
    }
    None
}

/// Describe a `CallerIdentity` for the nav bar: its display label, and
/// whether it is an admin, which decides the admin-only links.
fn identity_label(identity: &CallerIdentity) -> html::NavIdentity {
    let label = match identity {
        CallerIdentity::Admin(name) => format!("Admin: {name}"),
        CallerIdentity::IamUser {
            account_id,
            user_name,
        } => {
            format!("{account_id}/{user_name}")
        }
    };
    html::NavIdentity {
        label,
        is_admin: is_admin(identity),
    }
}

/// Check if the caller is an admin. Returns true for admins, false for IAM users.
fn is_admin(identity: &CallerIdentity) -> bool {
    matches!(identity, CallerIdentity::Admin(_))
}

/// Let an admin session through to an admin-only page: Metrics, Settings and
/// Cache. An IAM-user session gets a redirect to the console dashboard.
#[allow(clippy::result_large_err)]
fn require_admin(session: &SessionData) -> Result<(), Response> {
    if is_admin(&session.identity) {
        Ok(())
    } else {
        Err(Redirect::to("/console").into_response())
    }
}

/// Extract the user-facing message from a storage `OpError`.
fn op_error_message(e: extenddb_storage::management_store::OpError) -> String {
    use extenddb_storage::management_store::OpError;
    match e {
        OpError::Validation(m)
        | OpError::AlreadyExists(m)
        | OpError::HasDependents(m)
        | OpError::NotFound(m)
        | OpError::Internal(m) => m,
        // The driver's text stays out of the page.
        OpError::Database { .. } => "Database error".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::header::LOCATION;

    use super::*;

    #[test]
    fn a_passed_check_returns_the_session() {
        let Ok(session) = session_response(SessionCheck::Valid {
            identity: CallerIdentity::Admin("alice".to_owned()),
            csrf_token: "csrf".to_owned(),
        }) else {
            panic!("a session that passed its check was refused");
        };
        assert!(matches!(session.identity, CallerIdentity::Admin(ref n) if n == "alice"));
        assert_eq!(session.csrf_token, "csrf");
    }

    #[test]
    fn an_ended_session_gets_the_login_redirect() {
        let Err(response) = session_response(SessionCheck::Invalid) else {
            panic!("an ended session was accepted");
        };
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers().get(LOCATION).map(|v| v.as_bytes()),
            Some(&b"/console/login"[..])
        );
    }

    #[test]
    fn an_unreadable_catalog_gets_503() {
        let Err(response) = session_response(SessionCheck::Unavailable) else {
            panic!("a request was accepted while the catalog could not be read");
        };
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().get(LOCATION).is_none());
    }
}
