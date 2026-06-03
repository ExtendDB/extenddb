// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM role pages: create, detail, delete.

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

/// GET /console/accounts/{id}/roles/new
pub async fn new_role_form(
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
        ("New Role", None),
    ]);

    let content = format!(
        r#"{crumbs}<h1>Create Role</h1>
<div class="card">
<form method="post" action="/console/accounts/{eid}/roles/new">
<label for="role_name">Role Name</label>
<input id="role_name" name="role_name" type="text" required>
<label for="trust_policy">Trust Policy (JSON)</label>
<textarea id="trust_policy" name="trust_policy" required>{{
  "Version": "2012-10-17",
  "Statement": [{{
    "Effect": "Allow",
    "Principal": {{"AWS": "arn:aws:iam::*:user/*"}},
    "Action": "sts:AssumeRole"
  }}]
}}</textarea>
<div style="margin-top:1rem">
<button class="btn btn-primary" type="submit">Create</button>
<a href="/console/accounts/{eid}" class="btn">Cancel</a>
</div>
</form>
</div>"#
    );

    Html(html::layout_csrf(
        "New Role",
        &nav,
        &content,
        &session.csrf_token,
    ))
    .into_response()
}

/// POST /console/accounts/{id}/roles/new
#[derive(Deserialize)]
pub struct CreateRoleForm {
    #[serde(rename = "_csrf", default)]
    csrf: String,
    role_name: String,
    trust_policy: String,
}

pub async fn create_role(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
    Form(form): Form<CreateRoleForm>,
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

    // Validate trust policy is valid JSON.
    let tp: Result<serde_json::Value, _> = serde_json::from_str(&form.trust_policy);
    let Ok(tp) = tp else {
        let nav = html::nav_bar(&identity_label(&session.identity));
        let content = format!(
            "<h1>Create Role</h1>{}",
            html::alert_error("Trust policy is not valid JSON")
        );
        return Html(html::layout_csrf(
            "New Role",
            &nav,
            &content,
            &session.csrf_token,
        ))
        .into_response();
    };

    match state
        .catalog_store
        .create_role(&account_id, &form.role_name, &tp)
        .await
    {
        Ok(()) => Redirect::to(&format!(
            "/console/accounts/{}/roles/{}",
            account_id, form.role_name
        ))
        .into_response(),
        Err(e) => {
            let nav = html::nav_bar(&identity_label(&session.identity));
            let content = format!(
                "<h1>Create Role</h1>{}",
                html::alert_error(&op_error_message(e))
            );
            Html(html::layout_csrf(
                "New Role",
                &nav,
                &content,
                &session.csrf_token,
            ))
            .into_response()
        }
    }
}

/// GET /console/accounts/{id}/roles/{name}
pub async fn role_detail(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, role_name)): Path<(String, String)>,
) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };

    let nav = html::nav_bar(&identity_label(&session.identity));
    let eid = html::escape(&account_id);
    let ern = html::escape(&role_name);
    let crumbs = html::breadcrumb(&[
        ("Dashboard", Some("/console")),
        ("Accounts", Some("/console/accounts")),
        (&account_id, Some(&format!("/console/accounts/{eid}"))),
        (&role_name, None),
    ]);

    // Fetch role detail via catalog store trait.
    let detail = state
        .catalog_store
        .get_role_detail(&account_id, &role_name)
        .await
        .unwrap_or(None);

    let Some(detail) = detail else {
        let content = format!("{crumbs}{}", html::alert_error("Role not found"));
        return Html(html::layout_csrf(
            "Role",
            &nav,
            &content,
            &session.csrf_token,
        ))
        .into_response();
    };

    let mut content = format!("{crumbs}<h1>Role: {ern}</h1>");

    if is_admin(&session.identity) {
        let _ = write!(
            content,
            r#"<form class="inline" method="post" action="/console/accounts/{eid}/roles/{ern}/delete"
                onsubmit="return confirm('Delete role {ern}?')">
<button class="btn btn-danger btn-sm" type="submit">Delete Role</button>
</form>"#
        );
    }

    // Trust policy.
    let tp_pretty = serde_json::to_string_pretty(&detail.trust_policy).unwrap_or_default();
    let _ = write!(
        content,
        r#"<h2>Trust Policy</h2>
<div class="card"><pre style="white-space:pre-wrap;word-break:break-word">{}</pre></div>"#,
        html::escape(&tp_pretty)
    );

    // Policies section.
    let _ = write!(content, "<h2>Policies ({})</h2>", detail.policies.len());
    if is_admin(&session.identity) {
        let _ = write!(
            content,
            r#"<a href="/console/accounts/{eid}/roles/{ern}/policies/new" class="btn btn-primary btn-sm" style="margin-bottom:0.5rem;display:inline-block">Add Policy</a>"#
        );
    }
    content.push_str(r#"<table><thead><tr><th>Policy Name</th><th></th></tr></thead><tbody>"#);
    for pname in &detail.policies {
        let ep = html::escape(pname);
        let delete_btn = if is_admin(&session.identity) {
            format!(
                r#"<form class="inline" method="post" action="/console/accounts/{eid}/roles/{ern}/policies/{ep}/delete"
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
        &format!("Role {role_name}"),
        &nav,
        &content,
        &session.csrf_token,
    ))
    .into_response()
}

/// POST /console/accounts/{id}/roles/{name}/delete
pub async fn delete_role(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, role_name)): Path<(String, String)>,
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

    match state
        .catalog_store
        .delete_role(&account_id, &role_name)
        .await
    {
        Ok(()) => {
            // Mirror management::iam_role::delete_role: cascade invalidate
            // session credentials, role-level authz, and any cached
            // session_data entries for this role.
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
            Redirect::to(&format!("/console/accounts/{account_id}")).into_response()
        }
        Err(e) => {
            let nav = html::nav_bar(&identity_label(&session.identity));
            let content = format!(
                "<h1>Delete Role</h1>{}",
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
