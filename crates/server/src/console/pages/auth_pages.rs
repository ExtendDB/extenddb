// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Login, logout, and session management pages.

use std::sync::Arc;

use axum::Form;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use serde::Deserialize;

use crate::console::ConsoleState;
use crate::console::html;
use crate::management::CallerIdentity;
use crate::rate_limit::{self, LoginRefusal};

use super::extract_session_token;

/// GET /console/login — render the login form.
pub async fn login_page(State(state): State<Arc<ConsoleState>>) -> Html<String> {
    Html(login_html(None, &state.listen_url))
}

/// POST /console/login — validate credentials and create a session.
#[derive(Deserialize)]
pub struct LoginForm {
    username: String,
    password: String,
}

pub async fn login_submit(
    State(state): State<Arc<ConsoleState>>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    Form(form): Form<LoginForm>,
) -> Response {
    // Reserve the attempt before any password is checked. Limits are per
    // username and peer address, so failures from elsewhere do not lock this
    // address out.
    let attempt = match rate_limit::begin_login(&*state.catalog_store, &form.username, addr).await {
        Ok(attempt) => attempt,
        Err(refusal) => return refusal_page(refusal, &state.listen_url),
    };

    // Try admin auth first. A storage error (including a timed-out wait for
    // a bcrypt slot) is answered 500 and recorded as nothing: no password
    // was checked.
    let identity =
        match try_admin_login(&form.username, &form.password, &*state.catalog_store).await {
            Err(()) => return internal_error(&state.listen_url),
            Ok(AdminLoginResult::Authenticated(id)) => Some(id),
            Ok(AdminLoginResult::WrongPassword) => None, // Admin exists, wrong password: do NOT fall through.
            Ok(AdminLoginResult::NotFound) => {
                // Admin not found: try IAM user auth.
                match try_iam_login(&form.username, &form.password, &*state.catalog_store).await {
                    Err(()) => return internal_error(&state.listen_url),
                    Ok(identity) => identity,
                }
            }
        };

    let Some(identity) = identity else {
        attempt.record_failure(&*state.catalog_store).await;
        return invalid_credentials(&state.listen_url);
    };

    let (token, _csrf) = state.sessions.create(identity).await;
    let cookie = format!(
        "extenddb_session={token}; Path=/console; HttpOnly; SameSite=Strict; Max-Age=28800"
    );

    (
        StatusCode::SEE_OTHER,
        [("location", "/console"), ("set-cookie", &cookie)],
    )
        .into_response()
}

/// POST /console/logout — destroy session and redirect to login.
pub async fn logout(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    if let Some(token) = extract_session_token(&headers) {
        state.sessions.remove(&token).await;
    }
    let cookie =
        "extenddb_session=; Path=/console; HttpOnly; SameSite=Strict; Max-Age=0".to_string();
    (
        StatusCode::SEE_OTHER,
        [("location", "/console/login"), ("set-cookie", &cookie)],
    )
        .into_response()
}

fn login_html(error: Option<&str>, listen_url: &str) -> String {
    let error_html = error.map(html::alert_error).unwrap_or_default();

    html::layout_full_with_url(
        "Login",
        r#"<nav><span class="brand">extenddb Console</span></nav>"#,
        &format!(
            r#"<div style="max-width:400px;margin:3rem auto">
<div class="card">
<h1>Login</h1>
{error_html}
<form method="post" action="/console/login">
<label for="username">Username</label>
<input id="username" name="username" type="text" required style="width:100%"
       placeholder="admin or account_id/user_name" autocomplete="username">
<label for="password">Password</label>
<input id="password" name="password" type="password" required style="width:100%"
       autocomplete="current-password">
<div style="margin-top:1rem">
<button class="btn btn-primary" type="submit" style="width:100%">Sign in</button>
</div>
</form>
</div>
<p style="text-align:center;margin-top:1rem;font-size:0.85rem;color:#666">
Admin users: enter your admin username.<br>
IAM users: enter <code>account_id/user_name</code>.
</p>
</div>"#
        ),
        None,
        None,
        Some(listen_url),
    )
}

/// Result of an admin login attempt — three-state to distinguish "not found"
/// (fall through to IAM) from "wrong password" (reject immediately).
enum AdminLoginResult {
    Authenticated(CallerIdentity),
    WrongPassword,
    NotFound,
}

/// The login page with a server error, answered when the store failed before
/// or while the password was checked. Nothing is recorded against the caller.
fn internal_error(listen_url: &str) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Html(login_html(
            Some("Internal error. Please try again later."),
            listen_url,
        )),
    )
        .into_response()
}

/// The login page answered to a wrong password or an unknown principal.
fn invalid_credentials(listen_url: &str) -> Response {
    (
        StatusCode::OK,
        Html(login_html(Some("Invalid credentials"), listen_url)),
    )
        .into_response()
}

/// The login page answered to an attempt the rate limiter did not admit.
fn refusal_page(refusal: LoginRefusal, listen_url: &str) -> Response {
    match refusal {
        LoginRefusal::TooManyAttempts(msg) => (
            StatusCode::TOO_MANY_REQUESTS,
            Html(login_html(Some(msg), listen_url)),
        )
            .into_response(),
        LoginRefusal::InvalidCredentials => invalid_credentials(listen_url),
        LoginRefusal::Internal => internal_error(listen_url),
    }
}

/// `Err(())` is a storage error; the caller answers 500 and records nothing.
async fn try_admin_login(
    username: &str,
    password: &str,
    store: &dyn extenddb_storage::management_store::AdminStore,
) -> Result<AdminLoginResult, ()> {
    let result = store
        .verify_admin_password(username, password)
        .await
        .map_err(|e| tracing::error!("Console login: DB error during admin auth: {e:?}"))?;
    Ok(match result {
        Some(true) => AdminLoginResult::Authenticated(CallerIdentity::Admin(username.to_owned())),
        Some(false) => AdminLoginResult::WrongPassword,
        None => AdminLoginResult::NotFound,
    })
}

/// `Ok(None)` is a wrong password or an unknown user; `Err(())` is a storage
/// error, which the caller answers 500 and records as nothing.
async fn try_iam_login(
    username: &str,
    password: &str,
    store: &dyn extenddb_storage::management_store::ManagementStore,
) -> Result<Option<CallerIdentity>, ()> {
    let Some((acct_id, uname)) = username.split_once('/') else {
        return Ok(None);
    };
    let ok = store
        .verify_iam_user_password(acct_id, uname, password)
        .await
        .map_err(|e| tracing::error!("Console login: DB error during IAM user auth: {e:?}"))?;
    Ok(ok.then(|| CallerIdentity::IamUser {
        account_id: acct_id.to_owned(),
        user_name: uname.to_owned(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::PRINCIPAL_LIMITED;

    const LISTEN_URL: &str = "https://127.0.0.1:8443";

    async fn body_text(response: Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn a_refused_attempt_is_answered_by_its_reason() {
        let limited = refusal_page(LoginRefusal::TooManyAttempts(PRINCIPAL_LIMITED), LISTEN_URL);
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(body_text(limited).await.contains(PRINCIPAL_LIMITED));

        // The attempts store could not be read: a server error, not a limit.
        let internal = refusal_page(LoginRefusal::Internal, LISTEN_URL);
        assert_eq!(internal.status(), StatusCode::INTERNAL_SERVER_ERROR);

        // A principal over the length limit gets the wrong-password page.
        let too_long = refusal_page(LoginRefusal::InvalidCredentials, LISTEN_URL);
        assert_eq!(too_long.status(), StatusCode::OK);
        assert!(body_text(too_long).await.contains("Invalid credentials"));
    }
}
