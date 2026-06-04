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
use crate::rate_limit;

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
    let source_ip = addr.ip().to_string();

    // Rate limit / lockout check before attempting authentication.
    if let Err(msg) =
        rate_limit::check_login_allowed(&*state.catalog_store, &form.username, Some(&source_ip))
            .await
    {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Html(login_html(Some(&msg), &state.listen_url)),
        )
            .into_response();
    }

    // Try admin auth first.
    let identity =
        match try_admin_login(&form.username, &form.password, &*state.catalog_store).await {
            AdminLoginResult::Authenticated(id) => Some(id),
            AdminLoginResult::WrongPassword => None, // Admin exists, wrong password — do NOT fall through.
            AdminLoginResult::NotFound => {
                // Admin not found — try IAM user auth.
                try_iam_login(&form.username, &form.password, &*state.catalog_store).await
            }
        };

    let Some(identity) = identity else {
        rate_limit::record_failed_login(&*state.catalog_store, &form.username, Some(&source_ip))
            .await;
        return (
            StatusCode::OK,
            Html(login_html(Some("Invalid credentials"), &state.listen_url)),
        )
            .into_response();
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

async fn try_admin_login(
    username: &str,
    password: &str,
    store: &dyn extenddb_storage::management_store::AdminStore,
) -> AdminLoginResult {
    let result = match store.verify_admin_password(username, password).await {
        Ok(r) => r,
        Err(_) => return AdminLoginResult::NotFound,
    };
    match result {
        Some(true) => AdminLoginResult::Authenticated(CallerIdentity::Admin(username.to_owned())),
        Some(false) => AdminLoginResult::WrongPassword,
        None => AdminLoginResult::NotFound,
    }
}

async fn try_iam_login(
    username: &str,
    password: &str,
    store: &dyn extenddb_storage::management_store::ManagementStore,
) -> Option<CallerIdentity> {
    let (acct_id, uname) = username.split_once('/')?;
    let ok = store
        .verify_iam_user_password(acct_id, uname, password)
        .await
        .ok()?;
    if ok {
        Some(CallerIdentity::IamUser {
            account_id: acct_id.to_owned(),
            user_name: uname.to_owned(),
        })
    } else {
        None
    }
}
