// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Console cache page — admin-only break-glass invalidation form.
//!
//! Mirrors `POST /management/cache/invalidate` (see
//! `docs/design/12-auth-authz-cache.md` §6.1). Both paths delegate to
//! `crate::management::cache_invalidate::apply` so behavior stays
//! identical regardless of how the operator triggers it.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use serde::Deserialize;

use crate::console::ConsoleState;
use crate::console::html;
use crate::management::cache_invalidate::{
    self, InvalidateRequest, Scope, Selectors, apply as apply_invalidation,
};

use super::{identity_label, is_admin, require_csrf, require_session};

/// Confirmation token the operator must type for `scope=all`. Mirrors
/// the CLI's `--yes` requirement; intentionally distinctive so the
/// operator can't pass it by muscle memory.
const ALL_CONFIRMATION_TOKEN: &str = "INVALIDATE";

/// `GET /console/cache` — page with the invalidation form (admin-only).
pub async fn cache_page(State(state): State<Arc<ConsoleState>>, headers: HeaderMap) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(redirect) => return redirect,
    };
    if !is_admin(&session.identity) {
        return (
            StatusCode::FORBIDDEN,
            "Cache controls are visible to admin users only",
        )
            .into_response();
    }

    let nav = html::nav_bar(&identity_label(&session.identity));
    let crumbs = html::breadcrumb(&[("Console", Some("/console")), ("Cache", None)]);

    let content = format!(
        r#"{crumbs}
<h1>Cache</h1>
<div class="card">
<h2>Manual invalidation</h2>
<p style="font-size:0.85rem;color:#666">
Drops cached entries on this instance. Complements the automatic write-through
hooks; reach for this when off-instance changes have not yet expired or when a
test needs a deterministic flush.
See the <a href="/console/docs/12-auth-authz-cache">design doc</a> for scope
semantics.
</p>
<form method="post" action="/console/cache/invalidate">
<label for="scope">Scope</label>
<select id="scope" name="scope" required>
<option value="user">user — drop everything cached about an IAM user</option>
<option value="role">role — drop everything cached about an IAM role</option>
<option value="account">account — sweep one account across every cache</option>
<option value="credential">credential — drop one access key</option>
<option value="group_members">group_members — fan out to a list of users</option>
<option value="table_key_info">table_key_info — drop one table's key info</option>
<option value="resource_tags">resource_tags — drop one ARN's resource tags</option>
<option value="all">all — flush every cache (requires confirmation)</option>
</select>

<label for="account_id">account_id <span style="color:#999;font-size:0.85rem">(account, user, role, group_members, table_key_info)</span></label>
<input id="account_id" name="account_id" type="text" autocomplete="off">

<label for="user_name">user_name <span style="color:#999;font-size:0.85rem">(user)</span></label>
<input id="user_name" name="user_name" type="text" autocomplete="off">

<label for="role_name">role_name <span style="color:#999;font-size:0.85rem">(role)</span></label>
<input id="role_name" name="role_name" type="text" autocomplete="off">

<label for="user_names">user_names <span style="color:#999;font-size:0.85rem">(group_members, comma-separated)</span></label>
<input id="user_names" name="user_names" type="text" autocomplete="off" placeholder="alice, bob, charlie">

<label for="access_key_id">access_key_id <span style="color:#999;font-size:0.85rem">(credential)</span></label>
<input id="access_key_id" name="access_key_id" type="text" autocomplete="off">

<label for="table_name">table_name <span style="color:#999;font-size:0.85rem">(table_key_info)</span></label>
<input id="table_name" name="table_name" type="text" autocomplete="off">

<label for="arn">arn <span style="color:#999;font-size:0.85rem">(resource_tags)</span></label>
<input id="arn" name="arn" type="text" autocomplete="off">

<label for="confirm">Confirmation <span style="color:#999;font-size:0.85rem">(scope=all only — type "{ALL_CONFIRMATION_TOKEN}")</span></label>
<input id="confirm" name="confirm" type="text" autocomplete="off">

<div style="margin-top:1rem">
<button class="btn btn-primary" type="submit">Invalidate</button>
<a href="/console" class="btn">Cancel</a>
</div>
</form>
</div>"#
    );

    Html(html::layout_csrf(
        "Cache",
        &nav,
        &content,
        &session.csrf_token,
    ))
    .into_response()
}

/// Form payload for `POST /console/cache/invalidate`. All selector
/// fields are optional; the shared `apply` helper validates the
/// scope-specific subset.
#[derive(Debug, Deserialize)]
pub struct InvalidateForm {
    #[serde(rename = "_csrf", default)]
    pub csrf: String,
    pub scope: String,
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub user_name: String,
    #[serde(default)]
    pub role_name: String,
    #[serde(default)]
    pub user_names: String,
    #[serde(default)]
    pub access_key_id: String,
    #[serde(default)]
    pub table_name: String,
    #[serde(default)]
    pub arn: String,
    #[serde(default)]
    pub confirm: String,
}

/// `POST /console/cache/invalidate` — handle the form submission.
pub async fn invalidate_cache(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    axum::Form(form): axum::Form<InvalidateForm>,
) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if !is_admin(&session.identity) {
        return (
            StatusCode::FORBIDDEN,
            "Cache controls are visible to admin users only",
        )
            .into_response();
    }
    if let Err(r) = require_csrf(&form.csrf, &session) {
        return r;
    }

    let admin_name = match &session.identity {
        crate::management::CallerIdentity::Admin(n) => n.clone(),
        crate::management::CallerIdentity::IamUser { .. } => unreachable!("checked by is_admin"),
    };

    let scope = match parse_scope(&form.scope) {
        Ok(s) => s,
        Err(msg) => return render_error(&session, &msg),
    };

    // The console form-confirmation pattern: scope=all requires the
    // operator to type the literal token in the confirm field.
    let confirm_ok = scope != Scope::All || form.confirm == ALL_CONFIRMATION_TOKEN;
    if scope == Scope::All && !confirm_ok {
        return render_error(
            &session,
            &format!(
                r#"scope "all" requires typing "{ALL_CONFIRMATION_TOKEN}" in the confirmation field"#
            ),
        );
    }

    let user_names = if form.user_names.trim().is_empty() {
        None
    } else {
        Some(
            form.user_names
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>(),
        )
    };
    let selectors = Selectors {
        account_id: optional(&form.account_id),
        user_name: optional(&form.user_name),
        role_name: optional(&form.role_name),
        user_names,
        access_key_id: optional(&form.access_key_id),
        table_name: optional(&form.table_name),
        arn: optional(&form.arn),
        confirm: Some(confirm_ok),
    };

    let request = InvalidateRequest { scope, selectors };

    match apply_invalidation(
        &state.auth_cache,
        &state.authz_cache,
        &state.table_key_info_cache,
        request,
        &admin_name,
    )
    .await
    {
        Ok(resp) => render_success(&session, &resp),
        Err(msg) => render_error(&session, &msg),
    }
}

fn parse_scope(s: &str) -> Result<Scope, String> {
    // Console form sends snake_case values matching the API enum.
    Ok(match s {
        "all" => Scope::All,
        "account" => Scope::Account,
        "credential" => Scope::Credential,
        "user" => Scope::User,
        "role" => Scope::Role,
        "group_members" => Scope::GroupMembers,
        "table_key_info" => Scope::TableKeyInfo,
        "resource_tags" => Scope::ResourceTags,
        other => return Err(format!("unknown scope: {other}")),
    })
}

fn optional(s: &str) -> Option<String> {
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(t.to_owned())
    }
}

fn render_success(
    session: &super::SessionData,
    resp: &cache_invalidate::InvalidateResponse,
) -> Response {
    let nav = html::nav_bar(&identity_label(&session.identity));
    let crumbs = html::breadcrumb(&[
        ("Console", Some("/console")),
        ("Cache", Some("/console/cache")),
        ("Invalidated", None),
    ]);
    let invalidated_html: String = resp
        .invalidated
        .iter()
        .map(|s| format!("<li><code>{}</code></li>", html::escape(s)))
        .collect();
    let content = format!(
        r#"{crumbs}
<h1>Cache invalidated</h1>
<div class="card">
<p>Scope: <code>{scope:?}</code></p>
<p>Subcaches touched:</p>
<ul>{invalidated_html}</ul>
<p style="margin-top:1rem"><a class="btn" href="/console/cache">Back</a></p>
</div>"#,
        scope = resp.scope,
    );
    Html(html::layout_csrf(
        "Cache invalidated",
        &nav,
        &content,
        &session.csrf_token,
    ))
    .into_response()
}

fn render_error(session: &super::SessionData, msg: &str) -> Response {
    let nav = html::nav_bar(&identity_label(&session.identity));
    let crumbs = html::breadcrumb(&[
        ("Console", Some("/console")),
        ("Cache", Some("/console/cache")),
        ("Error", None),
    ]);
    let content = format!(
        r#"{crumbs}
<h1>Cache</h1>
{alert}
<p style="margin-top:1rem"><a class="btn" href="/console/cache">Back</a></p>"#,
        alert = html::alert_error(msg),
    );
    Html(html::layout_csrf(
        "Cache",
        &nav,
        &content,
        &session.csrf_token,
    ))
    .into_response()
}
