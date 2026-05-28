// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Policy management pages: create and delete policies for users, groups, roles.

use std::sync::Arc;

use axum::Form;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{Html, IntoResponse, Redirect, Response};
use serde::Deserialize;

use crate::console::ConsoleState;
use crate::console::html;

use super::{CsrfOnly, identity_label, is_admin, op_error_message, require_csrf, require_session};

// ── User policies ──────────────────────────────────────────────────────

/// GET /console/accounts/{id}/users/{name}/policies/new
pub async fn new_user_policy_form(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
) -> Response {
    policy_form(&state, &headers, &account_id, "user", &user_name).await
}

/// POST /console/accounts/{id}/users/{name}/policies/new
pub async fn put_user_policy(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
    Form(form): Form<PolicyForm>,
) -> Response {
    let redirect = format!("/console/accounts/{account_id}/users/{user_name}");
    put_policy(
        &state,
        &headers,
        &account_id,
        "user",
        &user_name,
        form,
        &redirect,
    )
    .await
}

/// POST /console/accounts/{id}/users/{name}/policies/{policy}/delete
pub async fn delete_user_policy(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, user_name, policy_name)): Path<(String, String, String)>,
    Form(form): Form<CsrfOnly>,
) -> Response {
    let redirect = format!("/console/accounts/{account_id}/users/{user_name}");
    delete_policy(
        &state,
        &headers,
        &account_id,
        "user",
        &user_name,
        &policy_name,
        &redirect,
        &form.csrf,
    )
    .await
}

// ── Group policies ─────────────────────────────────────────────────────

/// GET /console/accounts/{id}/groups/{name}/policies/new
pub async fn new_group_policy_form(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, group_name)): Path<(String, String)>,
) -> Response {
    policy_form(&state, &headers, &account_id, "group", &group_name).await
}

/// POST /console/accounts/{id}/groups/{name}/policies/new
pub async fn put_group_policy(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, group_name)): Path<(String, String)>,
    Form(form): Form<PolicyForm>,
) -> Response {
    let redirect = format!("/console/accounts/{account_id}/groups/{group_name}");
    put_policy(
        &state,
        &headers,
        &account_id,
        "group",
        &group_name,
        form,
        &redirect,
    )
    .await
}

/// POST /console/accounts/{id}/groups/{name}/policies/{policy}/delete
pub async fn delete_group_policy(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, group_name, policy_name)): Path<(String, String, String)>,
    Form(form): Form<CsrfOnly>,
) -> Response {
    let redirect = format!("/console/accounts/{account_id}/groups/{group_name}");
    delete_policy(
        &state,
        &headers,
        &account_id,
        "group",
        &group_name,
        &policy_name,
        &redirect,
        &form.csrf,
    )
    .await
}

// ── Role policies ──────────────────────────────────────────────────────

/// GET /console/accounts/{id}/roles/{name}/policies/new
pub async fn new_role_policy_form(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, role_name)): Path<(String, String)>,
) -> Response {
    policy_form(&state, &headers, &account_id, "role", &role_name).await
}

/// POST /console/accounts/{id}/roles/{name}/policies/new
pub async fn put_role_policy(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, role_name)): Path<(String, String)>,
    Form(form): Form<PolicyForm>,
) -> Response {
    let redirect = format!("/console/accounts/{account_id}/roles/{role_name}");
    put_policy(
        &state,
        &headers,
        &account_id,
        "role",
        &role_name,
        form,
        &redirect,
    )
    .await
}

/// POST /console/accounts/{id}/roles/{name}/policies/{policy}/delete
pub async fn delete_role_policy(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, role_name, policy_name)): Path<(String, String, String)>,
    Form(form): Form<CsrfOnly>,
) -> Response {
    let redirect = format!("/console/accounts/{account_id}/roles/{role_name}");
    delete_policy(
        &state,
        &headers,
        &account_id,
        "role",
        &role_name,
        &policy_name,
        &redirect,
        &form.csrf,
    )
    .await
}

// ── Shared helpers ─────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct PolicyForm {
    #[serde(rename = "_csrf", default)]
    csrf: String,
    policy_name: String,
    policy_document: String,
}

/// Render the "add policy" form for any entity type.
async fn policy_form(
    state: &Arc<ConsoleState>,
    headers: &HeaderMap,
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
) -> Response {
    let session = match require_session(headers, state).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if !is_admin(&session.identity) {
        return Redirect::to("/console").into_response();
    }

    let nav = html::nav_bar(&identity_label(&session.identity));
    let eid = html::escape(account_id);
    let een = html::escape(principal_name);
    let eet = html::escape(principal_type);

    let back_url = format!("/console/accounts/{eid}/{eet}s/{een}");
    let action_url = format!("/console/accounts/{eid}/{eet}s/{een}/policies/new");

    let crumbs = html::breadcrumb(&[
        ("Dashboard", Some("/console")),
        ("Accounts", Some("/console/accounts")),
        (account_id, Some(&format!("/console/accounts/{eid}"))),
        (principal_name, Some(&back_url)),
        ("New Policy", None),
    ]);

    let content = format!(
        r#"{crumbs}<h1>Add Policy to {eet} "{een}"</h1>
<div class="card">
<form method="post" action="{action_url}">
<label for="policy_name">Policy Name</label>
<input id="policy_name" name="policy_name" type="text" required>
<label for="policy_document">Policy Document (JSON)</label>
<textarea id="policy_document" name="policy_document" required>{{
  "Version": "2012-10-17",
  "Statement": [{{
    "Effect": "Allow",
    "Action": "dynamodb:*",
    "Resource": "*"
  }}]
}}</textarea>
<div style="margin-top:1rem">
<button class="btn btn-primary" type="submit">Save Policy</button>
<a href="{back_url}" class="btn">Cancel</a>
</div>
</form>
</div>"#
    );

    Html(html::layout_csrf(
        "New Policy",
        &nav,
        &content,
        &session.csrf_token,
    ))
    .into_response()
}

/// Insert or replace a policy for any entity type.
async fn put_policy(
    state: &Arc<ConsoleState>,
    headers: &HeaderMap,
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
    form: PolicyForm,
    redirect_url: &str,
) -> Response {
    let session = match require_session(headers, state).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if !is_admin(&session.identity) {
        return Redirect::to("/console").into_response();
    }
    if let Err(r) = require_csrf(&form.csrf, &session) {
        return r;
    }

    // Validate JSON.
    let doc: serde_json::Value = match serde_json::from_str(&form.policy_document) {
        Ok(v) => v,
        Err(_) => {
            let nav = html::nav_bar(&identity_label(&session.identity));
            let content = format!(
                "<h1>Add Policy</h1>{}",
                html::alert_error("Policy document is not valid JSON")
            );
            return Html(html::layout_csrf(
                "New Policy",
                &nav,
                &content,
                &session.csrf_token,
            ))
            .into_response();
        }
    };

    match state
        .catalog_store
        .put_policy(
            account_id,
            principal_type,
            principal_name,
            &form.policy_name,
            &doc,
        )
        .await
    {
        Ok(()) => {
            invalidate_policy_caches(state, account_id, principal_type, principal_name).await;
            Redirect::to(redirect_url).into_response()
        }
        Err(e) => {
            let nav = html::nav_bar(&identity_label(&session.identity));
            let content = format!(
                "<h1>Add Policy</h1>{}",
                html::alert_error(&op_error_message(e))
            );
            Html(html::layout_csrf(
                "New Policy",
                &nav,
                &content,
                &session.csrf_token,
            ))
            .into_response()
        }
    }
}

/// Delete a policy for any entity type.
#[allow(clippy::too_many_arguments)]
async fn delete_policy(
    state: &Arc<ConsoleState>,
    headers: &HeaderMap,
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
    policy_name: &str,
    redirect_url: &str,
    csrf_token: &str,
) -> Response {
    let session = match require_session(headers, state).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if !is_admin(&session.identity) {
        return Redirect::to("/console").into_response();
    }
    if let Err(r) = require_csrf(csrf_token, &session) {
        return r;
    }

    match state
        .catalog_store
        .delete_policy(account_id, principal_type, principal_name, policy_name)
        .await
    {
        Ok(()) => {
            invalidate_policy_caches(state, account_id, principal_type, principal_name).await;
            Redirect::to(redirect_url).into_response()
        }
        Err(e) => {
            let nav = html::nav_bar(&identity_label(&session.identity));
            let content = format!(
                "<h1>Delete Policy</h1>{}",
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

/// Drop the cache entries affected by a policy mutation on the given
/// principal. Mirrors `management::iam_policy::invalidate_policy_caches`.
async fn invalidate_policy_caches(
    state: &Arc<ConsoleState>,
    account_id: &str,
    principal_type: &str,
    principal_name: &str,
) {
    match principal_type {
        "user" => {
            state
                .auth_cache
                .invalidate_user_policies(account_id, principal_name)
                .await;
        }
        "role" => {
            state
                .auth_cache
                .invalidate_role_policies(account_id, principal_name)
                .await;
        }
        "group" => {
            let members = match state
                .catalog_store
                .get_group_detail(account_id, principal_name)
                .await
            {
                Ok(Some(detail)) => detail.members,
                Ok(None) => Vec::new(),
                Err(e) => {
                    tracing::warn!(
                        "console invalidate_policy_caches: get_group_detail failed for \
                         {principal_name}: {e:?}; members' cached group policies will age out at TTL"
                    );
                    Vec::new()
                }
            };
            state
                .auth_cache
                .invalidate_users_group_policies(account_id, &members)
                .await;
        }
        _ => {}
    }
}
