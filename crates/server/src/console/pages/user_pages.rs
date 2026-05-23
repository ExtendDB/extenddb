// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM user pages: create, detail, and delete.

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

/// GET /console/accounts/{id}/users/new
pub async fn new_user_form(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if !is_admin(&session.identity) {
        return Redirect::to(&format!("/console/accounts/{account_id}")).into_response();
    }

    let nav = html::nav_bar(&identity_label(&session.identity));
    let eid = html::escape(&account_id);
    let crumbs = html::breadcrumb(&[
        ("Dashboard", Some("/console")),
        ("Accounts", Some("/console/accounts")),
        (&account_id, Some(&format!("/console/accounts/{eid}"))),
        ("New User", None),
    ]);

    let content = format!(
        r#"{crumbs}<h1>Create User</h1>
<div class="card">
<form method="post" action="/console/accounts/{eid}/users/new">
<label for="user_name">User Name</label>
<input id="user_name" name="user_name" type="text" required>
<label for="password">Console Password (optional)</label>
<input id="password" name="password" type="password" autocomplete="new-password">
<div style="margin-top:1rem">
<button class="btn btn-primary" type="submit">Create</button>
<a href="/console/accounts/{eid}" class="btn">Cancel</a>
</div>
</form>
</div>"#
    );

    Html(html::layout_csrf(
        "New User",
        &nav,
        &content,
        &session.csrf_token,
    ))
    .into_response()
}

/// POST /console/accounts/{id}/users/new
#[derive(Deserialize)]
pub struct CreateUserForm {
    #[serde(rename = "_csrf", default)]
    csrf: String,
    user_name: String,
    password: Option<String>,
}

pub async fn create_user(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
    Form(form): Form<CreateUserForm>,
) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if !is_admin(&session.identity) {
        return Redirect::to(&format!("/console/accounts/{account_id}")).into_response();
    }
    if let Err(r) = require_csrf(&form.csrf, &session) {
        return r;
    }

    // Validation stays in the server layer.
    if !crate::management::is_valid_iam_name(&form.user_name) {
        let nav = html::nav_bar(&identity_label(&session.identity));
        let content = format!(
            "<h1>Create User</h1>{}",
            html::alert_error(
                "user_name must be 1-128 characters: alphanumeric, hyphens, underscores, dots, plus, equals, at"
            )
        );
        return Html(html::layout_csrf(
            "New User",
            &nav,
            &content,
            &session.csrf_token,
        ))
        .into_response();
    }

    let pw = form.password.as_deref().filter(|p| !p.is_empty());

    if let Some(p) = pw {
        if p.len() > 72 {
            let nav = html::nav_bar(&identity_label(&session.identity));
            let content = format!(
                "<h1>Create User</h1>{}",
                html::alert_error("password must not exceed 72 bytes (bcrypt limit)")
            );
            return Html(html::layout_csrf(
                "New User",
                &nav,
                &content,
                &session.csrf_token,
            ))
            .into_response();
        }
    }

    let password_hash = match pw {
        Some(p) => match crate::management::password::hash_password(p.to_owned()).await {
            Ok(h) => Some(h),
            Err(e) => {
                tracing::error!("create_user bcrypt hash: {e}");
                let nav = html::nav_bar(&identity_label(&session.identity));
                let content = format!(
                    "<h1>Create User</h1>{}",
                    html::alert_error("Internal error")
                );
                return Html(html::layout_csrf(
                    "New User",
                    &nav,
                    &content,
                    &session.csrf_token,
                ))
                .into_response();
            }
        },
        None => None,
    };

    match state
        .catalog_store
        .create_user(&account_id, &form.user_name, password_hash.as_deref())
        .await
    {
        Ok(()) => Redirect::to(&format!(
            "/console/accounts/{}/users/{}",
            account_id, form.user_name
        ))
        .into_response(),
        Err(e) => {
            let nav = html::nav_bar(&identity_label(&session.identity));
            let content = format!(
                "<h1>Create User</h1>{}",
                html::alert_error(&op_error_message(e))
            );
            Html(html::layout_csrf(
                "New User",
                &nav,
                &content,
                &session.csrf_token,
            ))
            .into_response()
        }
    }
}

/// GET /console/accounts/{id}/users/{name}
pub async fn user_detail(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };

    let nav = html::nav_bar(&identity_label(&session.identity));
    let eid = html::escape(&account_id);
    let eun = html::escape(&user_name);
    let crumbs = html::breadcrumb(&[
        ("Dashboard", Some("/console")),
        ("Accounts", Some("/console/accounts")),
        (&account_id, Some(&format!("/console/accounts/{eid}"))),
        (&user_name, None),
    ]);

    // Fetch user detail via catalog store trait.
    let detail = state
        .catalog_store
        .get_user_detail(&account_id, &user_name)
        .await
        .unwrap_or(None);

    let Some(detail) = detail else {
        let content = format!("{crumbs}{}", html::alert_error("User not found"));
        return Html(html::layout_csrf(
            "User",
            &nav,
            &content,
            &session.csrf_token,
        ))
        .into_response();
    };

    let mut content = format!("{crumbs}<h1>User: {eun}</h1>");

    // Delete button (admin only).
    if is_admin(&session.identity) {
        let _ = write!(
            content,
            r#"<form class="inline" method="post" action="/console/accounts/{eid}/users/{eun}/delete"
                onsubmit="return confirm('Delete user {eun}?')">
<button class="btn btn-danger btn-sm" type="submit">Delete User</button>
</form>"#
        );
    }

    // Access keys section.
    let _ = write!(content, "<h2>Access Keys ({})</h2>", detail.keys.len());
    let _ = write!(
        content,
        r#"<form class="inline" method="post" action="/console/accounts/{eid}/users/{eun}/access-keys/new">
<button class="btn btn-primary btn-sm" type="submit">Create Access Key</button>
</form>"#
    );
    content.push_str(
        r#"<table><thead><tr><th>Access Key ID</th><th>Status</th><th></th></tr></thead><tbody>"#,
    );
    for (kid, is_active) in &detail.keys {
        let ekid = html::escape(kid);
        let status = if *is_active { "Active" } else { "Inactive" };
        let _ = write!(
            content,
            r#"<tr><td><code>{ekid}</code></td><td>{status}</td><td>
<form class="inline" method="post" action="/console/accounts/{eid}/users/{eun}/access-keys/{ekid}/delete"
      onsubmit="return confirm('Delete key {ekid}?')">
<button class="btn btn-danger btn-sm" type="submit">Delete</button>
</form></td></tr>"#
        );
    }
    content.push_str("</tbody></table>");

    // Policies section.
    let _ = write!(content, "<h2>Policies ({})</h2>", detail.policies.len());
    if is_admin(&session.identity) {
        let _ = write!(
            content,
            r#"<a href="/console/accounts/{eid}/users/{eun}/policies/new" class="btn btn-primary btn-sm" style="margin-bottom:0.5rem;display:inline-block">Add Policy</a>"#
        );
    }
    content.push_str(r#"<table><thead><tr><th>Policy Name</th><th></th></tr></thead><tbody>"#);
    for pname in &detail.policies {
        let ep = html::escape(pname);
        let delete_btn = if is_admin(&session.identity) {
            format!(
                r#"<form class="inline" method="post" action="/console/accounts/{eid}/users/{eun}/policies/{ep}/delete"
                      onsubmit="return confirm('Delete policy {ep}?')">
<button class="btn btn-danger btn-sm" type="submit">Delete</button>
</form>"#
            )
        } else {
            String::new()
        };
        let _ = write!(content, "<tr><td>{ep}</td><td>{delete_btn}</td></tr>");
    }
    content.push_str("</tbody></table>");

    // Groups section.
    let _ = write!(content, "<h2>Groups ({})</h2>", detail.groups.len());
    content.push_str(r#"<table><thead><tr><th>Group Name</th></tr></thead><tbody>"#);
    for gname in &detail.groups {
        let eg = html::escape(gname);
        let _ = write!(
            content,
            r#"<tr><td><a href="/console/accounts/{eid}/groups/{eg}">{eg}</a></td></tr>"#
        );
    }
    content.push_str("</tbody></table>");

    // Tags section.
    let _ = write!(content, "<h2>Tags ({})</h2>", detail.tags.len());
    content.push_str(r#"<table><thead><tr><th>Key</th><th>Value</th></tr></thead><tbody>"#);
    for (k, v) in &detail.tags {
        let _ = write!(
            content,
            "<tr><td>{}</td><td>{}</td></tr>",
            html::escape(k),
            html::escape(v)
        );
    }
    content.push_str("</tbody></table>");

    Html(html::layout_csrf(
        &format!("User {user_name}"),
        &nav,
        &content,
        &session.csrf_token,
    ))
    .into_response()
}

/// POST /console/accounts/{id}/users/{name}/delete
pub async fn delete_user(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
    Form(form): Form<CsrfOnly>,
) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if !is_admin(&session.identity) {
        return Redirect::to(&format!("/console/accounts/{account_id}")).into_response();
    }
    if let Err(r) = require_csrf(&form.csrf, &session) {
        return r;
    }

    // Mirror management::iam_user::delete_user: snapshot the user's access
    // keys BEFORE deletion so cache fanout can drop them after the cascade
    // succeeds. The defensive principal-fanout below covers any keys
    // created concurrently with this delete.
    let access_key_ids: Vec<String> = match state
        .catalog_store
        .list_access_keys(&account_id, &user_name)
        .await
    {
        Ok(rows) => rows.into_iter().map(|(id, _, _)| id).collect(),
        Err(e) => {
            tracing::warn!(
                "console delete_user: list_access_keys failed before delete; cache invalidation will rely on TTL: {e:?}"
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
            for key_id in &access_key_ids {
                state.auth_cache.invalidate_credential(key_id).await;
            }
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
            Redirect::to(&format!("/console/accounts/{account_id}")).into_response()
        }
        Err(e) => {
            let nav = html::nav_bar(&identity_label(&session.identity));
            let content = format!(
                "<h1>Delete User</h1>{}",
                html::alert_error(&op_error_message(e))
            );
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
