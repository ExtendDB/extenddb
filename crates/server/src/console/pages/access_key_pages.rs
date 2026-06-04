// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Access key management pages: create and delete access keys for IAM users.

use std::sync::Arc;

use axum::Form;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{Html, IntoResponse, Redirect, Response};

use crate::console::ConsoleState;
use crate::console::html;

use super::{CsrfOnly, identity_label, op_error_message, require_csrf, require_session};

use crate::management::CallerIdentity;

/// POST /console/accounts/{id}/users/{name}/access-keys/new
pub async fn create_access_key(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, user_name)): Path<(String, String)>,
    Form(form): Form<CsrfOnly>,
) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };

    // Allow self-service (same IAM user) or admin.
    let allowed = match &session.identity {
        CallerIdentity::Admin(_) => true,
        CallerIdentity::IamUser {
            account_id: a,
            user_name: u,
        } => a == &account_id && u == &user_name,
    };
    if !allowed {
        return Redirect::to("/console").into_response();
    }
    if let Err(r) = require_csrf(&form.csrf, &session) {
        return r;
    }

    let nav = html::nav_bar(&identity_label(&session.identity));
    let eid = html::escape(&account_id);
    let eun = html::escape(&user_name);

    let crumbs = html::breadcrumb(&[
        ("Dashboard", Some("/console")),
        ("Accounts", Some("/console/accounts")),
        (&account_id, Some(&format!("/console/accounts/{eid}"))),
        (
            &user_name,
            Some(&format!("/console/accounts/{eid}/users/{eun}")),
        ),
        ("New Access Key", None),
    ]);

    match state
        .catalog_store
        .create_access_key(&account_id, &user_name)
        .await
    {
        Ok(key) => {
            // Drop any negative cache entry for this newly-issued key id —
            // mirrors management::iam_user_self::create_access_key.
            state
                .auth_cache
                .invalidate_credential(&key.access_key_id)
                .await;
            let content = format!(
                r#"{crumbs}<h1>Access Key Created</h1>
{}
<div class="card">
<p><strong>Access Key ID:</strong></p>
<div class="secret-box">{}</div>
<p><strong>Secret Access Key:</strong></p>
<div class="secret-box">{}</div>
<p style="margin-top:0.5rem;color:#991b1b;font-weight:600">
Save the secret key now. It cannot be retrieved later.
</p>
</div>
<a href="/console/accounts/{eid}/users/{eun}" class="btn btn-primary">Back to User</a>"#,
                html::alert_success("Access key created successfully"),
                html::escape(&key.access_key_id),
                html::escape(&key.secret_access_key),
            );
            Html(html::layout_csrf(
                "Access Key Created",
                &nav,
                &content,
                &session.csrf_token,
            ))
            .into_response()
        }
        Err(e) => {
            let content = format!(
                "{crumbs}<h1>Error</h1>{}",
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

/// POST /console/accounts/{id}/users/{name}/access-keys/{key_id}/delete
pub async fn delete_access_key(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, user_name, key_id)): Path<(String, String, String)>,
    Form(form): Form<CsrfOnly>,
) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };

    // Allow self-service (same IAM user) or admin.
    let allowed = match &session.identity {
        CallerIdentity::Admin(_) => true,
        CallerIdentity::IamUser {
            account_id: a,
            user_name: u,
        } => a == &account_id && u == &user_name,
    };
    if !allowed {
        return Redirect::to("/console").into_response();
    }
    if let Err(r) = require_csrf(&form.csrf, &session) {
        return r;
    }

    match state
        .catalog_store
        .delete_access_key(&account_id, &user_name, &key_id)
        .await
    {
        Ok(()) => {
            // Mirror management::iam_user_self::delete_access_key.
            state.auth_cache.invalidate_credential(&key_id).await;
            Redirect::to(&format!("/console/accounts/{account_id}/users/{user_name}"))
                .into_response()
        }
        Err(e) => {
            let nav = html::nav_bar(&identity_label(&session.identity));
            let content = format!("<h1>Error</h1>{}", html::alert_error(&op_error_message(e)));
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
