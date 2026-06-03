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

<div class="sel" data-scopes="account user role group_members table_key_info">
<label for="account_id">account_id</label>
<input id="account_id" name="account_id" type="text" autocomplete="off">
</div>

<div class="sel" data-scopes="user">
<label for="user_name">user_name</label>
<input id="user_name" name="user_name" type="text" autocomplete="off">
</div>

<div class="sel" data-scopes="role">
<label for="role_name">role_name</label>
<input id="role_name" name="role_name" type="text" autocomplete="off">
</div>

<div class="sel" data-scopes="group_members">
<label for="user_names">user_names <span style="color:#999;font-size:0.85rem">(comma-separated)</span></label>
<input id="user_names" name="user_names" type="text" autocomplete="off" placeholder="alice, bob, charlie">
</div>

<div class="sel" data-scopes="credential">
<label for="access_key_id">access_key_id</label>
<input id="access_key_id" name="access_key_id" type="text" autocomplete="off">
</div>

<div class="sel" data-scopes="table_key_info">
<label for="table_name">table_name</label>
<input id="table_name" name="table_name" type="text" autocomplete="off">
</div>

<div class="sel" data-scopes="resource_tags">
<label for="arn">arn</label>
<input id="arn" name="arn" type="text" autocomplete="off">
</div>

<div class="sel" data-scopes="all">
<label for="confirm">Confirmation <span style="color:#999;font-size:0.85rem">(type "{ALL_CONFIRMATION_TOKEN}")</span></label>
<input id="confirm" name="confirm" type="text" autocomplete="off">
</div>

<div style="margin-top:1rem">
<button class="btn btn-primary" type="submit">Invalidate</button>
<a href="/console" class="btn">Cancel</a>
</div>
</form>
<script>
// Progressive disclosure: hide selector fields that don't apply to the
// chosen scope. Submitted values for hidden fields are ignored by the
// server (the shared `apply` helper validates per-scope), so this is
// purely UX. The page works without JS — every field is visible.
(function() {{
  var sel = document.getElementById('scope');
  var rows = document.querySelectorAll('.sel');
  function apply() {{
    var s = sel.value;
    rows.forEach(function(row) {{
      var scopes = row.getAttribute('data-scopes').split(' ');
      row.style.display = scopes.indexOf(s) >= 0 ? '' : 'none';
    }});
  }}
  sel.addEventListener('change', apply);
  apply();
}})();
</script>
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

    match apply_invalidation(&state.auth_cache, request, &admin_name).await {
        Ok(resp) => render_success(&session, &resp),
        Err(msg) => render_error(&session, &msg),
    }
}

fn parse_scope(s: &str) -> Result<Scope, String> {
    // Drive parsing through Scope's snake_case Deserialize impl so the
    // form, the API JSON, the CLI, and the design doc table all stay in
    // sync from one source. Adding a scope variant Just Works.
    serde_json::from_value(serde_json::Value::String(s.to_owned()))
        .map_err(|_| format!("unknown scope: {s}"))
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
<p>Scope: <code>{scope}</code></p>
<p>Subcaches touched:</p>
<ul>{invalidated_html}</ul>
<p style="margin-top:1rem"><a class="btn" href="/console/cache">Back</a></p>
</div>"#,
        scope = resp.scope.as_str(),
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
