// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Account listing, creation, detail, and deletion pages.

use std::fmt::Write;
use std::sync::Arc;

use axum::Form;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{Html, IntoResponse, Redirect, Response};
use serde::Deserialize;

use crate::console::ConsoleState;
use crate::console::html;

use super::{CsrfOnly, identity_label, is_admin, op_error_message, require_csrf, require_session};

/// GET /console/ — dashboard showing account count and quick links.
pub async fn dashboard(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };

    let nav = html::nav_bar(&identity_label(&session.identity));

    let (account_count, admin_count) = state
        .catalog_store
        .dashboard_counts()
        .await
        .unwrap_or((0, 0));

    let content = format!(
        r#"<h1>Dashboard</h1>
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:1rem">
<div class="card">
<h2 style="margin:0">{account_count}</h2>
<p>Accounts</p>
<a href="/console/accounts">View all &rarr;</a>
</div>
<div class="card">
<h2 style="margin:0">{admin_count}</h2>
<p>Admin Users</p>
</div>
</div>"#
    );

    Html(html::layout_with_version_csrf(
        "Dashboard",
        &nav,
        &content,
        Some(&state.version_info),
        &session.csrf_token,
    ))
    .into_response()
}

/// GET /console/accounts — list all accounts.
pub async fn list_accounts(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };

    let nav = html::nav_bar(&identity_label(&session.identity));
    let crumbs = html::breadcrumb(&[("Dashboard", Some("/console")), ("Accounts", None)]);

    let rows: Vec<(String, String)> = match &session.identity {
        crate::management::CallerIdentity::Admin(_) => state
            .catalog_store
            .list_all_accounts()
            .await
            .unwrap_or_default(),
        crate::management::CallerIdentity::IamUser { account_id, .. } => state
            .catalog_store
            .list_accounts_for(account_id)
            .await
            .unwrap_or_default(),
    };

    let mut table = String::from(
        r#"<table><thead><tr><th>Account ID</th><th>Name</th><th></th></tr></thead><tbody>"#,
    );
    for (id, name) in &rows {
        let eid = html::escape(id);
        let ename = html::escape(name);
        let _ = write!(
            table,
            r#"<tr><td><a href="/console/accounts/{eid}">{eid}</a></td><td>{ename}</td><td></td></tr>"#
        );
    }
    table.push_str("</tbody></table>");

    let new_btn = if is_admin(&session.identity) {
        r#"<a href="/console/accounts/new" class="btn btn-primary" style="margin-bottom:1rem;display:inline-block">New Account</a>"#
    } else {
        ""
    };

    let content = format!("{crumbs}<h1>Accounts</h1>{new_btn}{table}");
    Html(html::layout_csrf(
        "Accounts",
        &nav,
        &content,
        &session.csrf_token,
    ))
    .into_response()
}

/// GET /console/accounts/new — form to create an account.
pub async fn new_account_form(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if !is_admin(&session.identity) {
        return Redirect::to("/console/accounts").into_response();
    }

    let nav = html::nav_bar(&identity_label(&session.identity));
    let crumbs = html::breadcrumb(&[
        ("Dashboard", Some("/console")),
        ("Accounts", Some("/console/accounts")),
        ("New", None),
    ]);

    let content = format!(
        r#"{crumbs}<h1>Create Account</h1>
<div class="card">
<form method="post" action="/console/accounts/new">
<label for="account_id">Account ID</label>
<input id="account_id" name="account_id" type="text" placeholder="123456789012 (auto-generated if empty)">
<label for="account_name">Account Name</label>
<input id="account_name" name="account_name" type="text" required placeholder="dev-team">
<div style="margin-top:1rem">
<button class="btn btn-primary" type="submit">Create</button>
<a href="/console/accounts" class="btn">Cancel</a>
</div>
</form>
</div>"#
    );

    Html(html::layout_csrf(
        "New Account",
        &nav,
        &content,
        &session.csrf_token,
    ))
    .into_response()
}

/// POST /console/accounts/new — create an account.
#[derive(Deserialize)]
pub struct CreateAccountForm {
    #[serde(rename = "_csrf", default)]
    csrf: String,
    #[serde(default)]
    account_id: String,
    account_name: String,
}

pub async fn create_account(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Form(form): Form<CreateAccountForm>,
) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if !is_admin(&session.identity) {
        return Redirect::to("/console/accounts").into_response();
    }

    if let Err(r) = require_csrf(&form.csrf, &session) {
        return r;
    }

    // D2: Auto-generate account ID if the user left the field empty.
    let account_id = if form.account_id.trim().is_empty() {
        crate::management::generate_account_id()
    } else {
        form.account_id.clone()
    };

    // Validate account ID format (must be 12-digit numeric string).
    if account_id.len() != 12 || !account_id.chars().all(|c| c.is_ascii_digit()) {
        let nav = html::nav_bar(&identity_label(&session.identity));
        let content = format!(
            "{}<h1>Create Account</h1>{}",
            html::breadcrumb(&[
                ("Dashboard", Some("/console")),
                ("Accounts", Some("/console/accounts")),
                ("New", None),
            ]),
            html::alert_error("account_id must be a 12-digit numeric string"),
        );
        return Html(html::layout_csrf(
            "New Account",
            &nav,
            &content,
            &session.csrf_token,
        ))
        .into_response();
    }

    let result = state
        .catalog_store
        .create_account(&account_id, &form.account_name)
        .await;

    match result {
        Ok(()) => Redirect::to(&format!("/console/accounts/{account_id}")).into_response(),
        Err(e) => {
            let nav = html::nav_bar(&identity_label(&session.identity));
            let msg = op_error_message(e);
            let content = format!(
                "{}<h1>Create Account</h1>{}",
                html::breadcrumb(&[
                    ("Dashboard", Some("/console")),
                    ("Accounts", Some("/console/accounts")),
                    ("New", None),
                ]),
                html::alert_error(&msg),
            );
            Html(html::layout_csrf(
                "New Account",
                &nav,
                &content,
                &session.csrf_token,
            ))
            .into_response()
        }
    }
}

/// GET /console/accounts/{id} — account detail with users, groups, roles.
pub async fn account_detail(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };

    let nav = html::nav_bar(&identity_label(&session.identity));
    let eid = html::escape(&account_id);
    let crumbs = html::breadcrumb(&[
        ("Dashboard", Some("/console")),
        ("Accounts", Some("/console/accounts")),
        (&account_id, None),
    ]);

    // Fetch account detail via catalog store trait.
    let detail = state
        .catalog_store
        .get_account_detail(&account_id)
        .await
        .unwrap_or(None);

    let Some(detail) = detail else {
        let content = format!("{crumbs}{}", html::alert_error("Account not found"));
        return Html(html::layout_csrf(
            "Account",
            &nav,
            &content,
            &session.csrf_token,
        ))
        .into_response();
    };

    let admin_actions = if is_admin(&session.identity) {
        format!(
            r#"<form class="inline" method="post" action="/console/accounts/{eid}/delete"
                onsubmit="return confirm('Delete account {eid}?')">
<button class="btn btn-danger btn-sm" type="submit">Delete Account</button>
</form>"#
        )
    } else {
        String::new()
    };

    let mut content = format!(
        r#"{crumbs}<h1>{} <small style="color:#666">({eid})</small></h1>
{admin_actions}
<h2>Users ({count})</h2>"#,
        html::escape(&detail.account_name),
        count = detail.users.len(),
    );

    if is_admin(&session.identity) {
        let _ = write!(
            content,
            r#"<a href="/console/accounts/{eid}/users/new" class="btn btn-primary btn-sm" style="margin-bottom:0.5rem;display:inline-block">New User</a>"#
        );
    }

    content.push_str(r#"<table><thead><tr><th>User Name</th></tr></thead><tbody>"#);
    for name in &detail.users {
        let en = html::escape(name);
        let _ = write!(
            content,
            r#"<tr><td><a href="/console/accounts/{eid}/users/{en}">{en}</a></td></tr>"#
        );
    }
    content.push_str("</tbody></table>");

    // Groups
    let _ = write!(content, "<h2>Groups ({})</h2>", detail.groups.len());
    if is_admin(&session.identity) {
        let _ = write!(
            content,
            r#"<a href="/console/accounts/{eid}/groups/new" class="btn btn-primary btn-sm" style="margin-bottom:0.5rem;display:inline-block">New Group</a>"#
        );
    }
    content.push_str(r#"<table><thead><tr><th>Group Name</th></tr></thead><tbody>"#);
    for name in &detail.groups {
        let en = html::escape(name);
        let _ = write!(
            content,
            r#"<tr><td><a href="/console/accounts/{eid}/groups/{en}">{en}</a></td></tr>"#
        );
    }
    content.push_str("</tbody></table>");

    // Roles
    let _ = write!(content, "<h2>Roles ({})</h2>", detail.roles.len());
    if is_admin(&session.identity) {
        let _ = write!(
            content,
            r#"<a href="/console/accounts/{eid}/roles/new" class="btn btn-primary btn-sm" style="margin-bottom:0.5rem;display:inline-block">New Role</a>"#
        );
    }
    content.push_str(r#"<table><thead><tr><th>Role Name</th></tr></thead><tbody>"#);
    for name in &detail.roles {
        let en = html::escape(name);
        let _ = write!(
            content,
            r#"<tr><td><a href="/console/accounts/{eid}/roles/{en}">{en}</a></td></tr>"#
        );
    }
    content.push_str("</tbody></table>");

    Html(html::layout_csrf(
        &format!("Account {account_id}"),
        &nav,
        &content,
        &session.csrf_token,
    ))
    .into_response()
}

/// POST /console/accounts/{id}/delete — delete an account.
pub async fn delete_account(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
    Form(form): Form<CsrfOnly>,
) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if !is_admin(&session.identity) {
        return Redirect::to("/console/accounts").into_response();
    }
    if let Err(r) = require_csrf(&form.csrf, &session) {
        return r;
    }

    match state.catalog_store.delete_account(&account_id).await {
        Ok(()) => {
            // Mirror management::account::delete_account: cascade invalidate
            // every cached entry across every authz subcache (and the
            // credential cache) that belongs to this account.
            state.auth_cache.invalidate_account(&account_id).await;
            Redirect::to("/console/accounts").into_response()
        }
        Err(e) => {
            let nav = html::nav_bar(&identity_label(&session.identity));
            let msg = op_error_message(e);
            let content = format!("<h1>Delete Account</h1>{}", html::alert_error(&msg));
            Html(html::layout_csrf(
                "Error",
                &nav,
                &content,
                &session.csrf_token,
            ))
            .into_response()
        }
    }
}
