// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! IAM group pages: create, detail, delete, member management.

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

/// GET /console/accounts/{id}/groups/new
pub async fn new_group_form(
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
        ("New Group", None),
    ]);

    let content = format!(
        r#"{crumbs}<h1>Create Group</h1>
<div class="card">
<form method="post" action="/console/accounts/{eid}/groups/new">
<label for="group_name">Group Name</label>
<input id="group_name" name="group_name" type="text" required>
<div style="margin-top:1rem">
<button class="btn btn-primary" type="submit">Create</button>
<a href="/console/accounts/{eid}" class="btn">Cancel</a>
</div>
</form>
</div>"#
    );

    Html(html::layout_csrf(
        "New Group",
        &nav,
        &content,
        &session.csrf_token,
    ))
    .into_response()
}

/// POST /console/accounts/{id}/groups/new
#[derive(Deserialize)]
pub struct CreateGroupForm {
    #[serde(rename = "_csrf", default)]
    csrf: String,
    group_name: String,
}

pub async fn create_group(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path(account_id): Path<String>,
    Form(form): Form<CreateGroupForm>,
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
        .create_group(&account_id, &form.group_name)
        .await
    {
        Ok(()) => Redirect::to(&format!(
            "/console/accounts/{}/groups/{}",
            account_id, form.group_name
        ))
        .into_response(),
        Err(e) => {
            let nav = html::nav_bar(&identity_label(&session.identity));
            let content = format!(
                "<h1>Create Group</h1>{}",
                html::alert_error(&op_error_message(e))
            );
            Html(html::layout_csrf(
                "New Group",
                &nav,
                &content,
                &session.csrf_token,
            ))
            .into_response()
        }
    }
}

/// GET /console/accounts/{id}/groups/{name}
pub async fn group_detail(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, group_name)): Path<(String, String)>,
) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };

    let nav = html::nav_bar(&identity_label(&session.identity));
    let eid = html::escape(&account_id);
    let egn = html::escape(&group_name);
    let crumbs = html::breadcrumb(&[
        ("Dashboard", Some("/console")),
        ("Accounts", Some("/console/accounts")),
        (&account_id, Some(&format!("/console/accounts/{eid}"))),
        (&group_name, None),
    ]);

    // Fetch group detail via catalog store trait.
    let detail = state
        .catalog_store
        .get_group_detail(&account_id, &group_name)
        .await
        .unwrap_or(None);

    let Some(detail) = detail else {
        let content = format!("{crumbs}{}", html::alert_error("Group not found"));
        return Html(html::layout_csrf(
            "Group",
            &nav,
            &content,
            &session.csrf_token,
        ))
        .into_response();
    };

    let mut content = format!("{crumbs}<h1>Group: {egn}</h1>");

    if is_admin(&session.identity) {
        let _ = write!(
            content,
            r#"<form class="inline" method="post" action="/console/accounts/{eid}/groups/{egn}/delete"
                onsubmit="return confirm('Delete group {egn}?')">
<button class="btn btn-danger btn-sm" type="submit">Delete Group</button>
</form>"#
        );
    }

    // Members section.
    let _ = write!(content, "<h2>Members ({})</h2>", detail.members.len());

    if is_admin(&session.identity) && !detail.all_users.is_empty() {
        let _ = write!(
            content,
            r#"<form class="inline" method="post" action="/console/accounts/{eid}/groups/{egn}/members/add" style="margin-bottom:0.5rem">
<select name="user_name">"#
        );
        for u in &detail.all_users {
            let eu = html::escape(u);
            let _ = write!(content, r#"<option value="{eu}">{eu}</option>"#);
        }
        content.push_str(
            r#"</select> <button class="btn btn-primary btn-sm" type="submit">Add Member</button></form>"#,
        );
    }

    content.push_str(r#"<table><thead><tr><th>User Name</th><th></th></tr></thead><tbody>"#);
    for uname in &detail.members {
        let eu = html::escape(uname);
        let remove_btn = if is_admin(&session.identity) {
            format!(
                r#"<form class="inline" method="post" action="/console/accounts/{eid}/groups/{egn}/members/{eu}/remove">
<button class="btn btn-danger btn-sm" type="submit">Remove</button>
</form>"#
            )
        } else {
            String::new()
        };
        let _ = write!(
            content,
            r#"<tr><td><a href="/console/accounts/{eid}/users/{eu}">{eu}</a></td><td>{remove_btn}</td></tr>"#
        );
    }
    content.push_str("</tbody></table>");

    // Policies section.
    let _ = write!(content, "<h2>Policies ({})</h2>", detail.policies.len());
    if is_admin(&session.identity) {
        let _ = write!(
            content,
            r#"<a href="/console/accounts/{eid}/groups/{egn}/policies/new" class="btn btn-primary btn-sm" style="margin-bottom:0.5rem;display:inline-block">Add Policy</a>"#
        );
    }
    content.push_str(r#"<table><thead><tr><th>Policy Name</th><th></th></tr></thead><tbody>"#);
    for pname in &detail.policies {
        let ep = html::escape(pname);
        let delete_btn = if is_admin(&session.identity) {
            format!(
                r#"<form class="inline" method="post" action="/console/accounts/{eid}/groups/{egn}/policies/{ep}/delete"
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

    Html(html::layout_csrf(
        &format!("Group {group_name}"),
        &nav,
        &content,
        &session.csrf_token,
    ))
    .into_response()
}

/// POST /console/accounts/{id}/groups/{name}/delete
pub async fn delete_group(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, group_name)): Path<(String, String)>,
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

    // Mirror management::iam_group::delete_group: snapshot members BEFORE
    // delete so we can drop their cached user_group_policies after the
    // FK cascade removes membership rows.
    let members: Vec<String> = match state
        .catalog_store
        .get_group_detail(&account_id, &group_name)
        .await
    {
        Ok(Some(detail)) => detail.members,
        Ok(None) => Vec::new(),
        Err(e) => {
            tracing::warn!(
                "console delete_group: get_group_detail failed before delete; member \
                 cache invalidation will rely on TTL: {e:?}"
            );
            Vec::new()
        }
    };

    match state
        .catalog_store
        .delete_group(&account_id, &group_name)
        .await
    {
        Ok(()) => {
            state
                .auth_cache
                .invalidate_users_group_policies(&account_id, &members)
                .await;
            Redirect::to(&format!("/console/accounts/{account_id}")).into_response()
        }
        Err(e) => {
            let nav = html::nav_bar(&identity_label(&session.identity));
            let content = format!(
                "<h1>Delete Group</h1>{}",
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

/// POST /console/accounts/{id}/groups/{name}/members/add
#[derive(Deserialize)]
pub struct AddMemberForm {
    #[serde(rename = "_csrf", default)]
    csrf: String,
    user_name: String,
}

pub async fn add_group_member(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, group_name)): Path<(String, String)>,
    Form(form): Form<AddMemberForm>,
) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if !is_admin(&session.identity) {
        return Redirect::to(&format!(
            "/console/accounts/{account_id}/groups/{group_name}"
        ))
        .into_response();
    }
    if let Err(r) = require_csrf(&form.csrf, &session) {
        return r;
    }

    let redirect = format!("/console/accounts/{account_id}/groups/{group_name}");

    match state
        .catalog_store
        .add_group_member(&account_id, &group_name, &form.user_name)
        .await
    {
        Ok(()) => {
            // Mirror management::iam_group::add_member: the user's
            // group_policies cache flattens membership at fetch time, so
            // adding the user changes their effective policy set.
            state
                .auth_cache
                .invalidate_user_group_policies(&account_id, &form.user_name)
                .await;
            Redirect::to(&redirect).into_response()
        }
        Err(e) => {
            let nav = html::nav_bar(&identity_label(&session.identity));
            let content = format!(
                "<h1>Add Member</h1>{}",
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

/// POST /console/accounts/{id}/groups/{name}/members/{user}/remove
pub async fn remove_group_member(
    State(state): State<Arc<ConsoleState>>,
    headers: HeaderMap,
    Path((account_id, group_name, user_name)): Path<(String, String, String)>,
    Form(form): Form<CsrfOnly>,
) -> Response {
    let session = match require_session(&headers, &state).await {
        Ok(s) => s,
        Err(r) => return r,
    };
    if !is_admin(&session.identity) {
        return Redirect::to(&format!(
            "/console/accounts/{account_id}/groups/{group_name}"
        ))
        .into_response();
    }
    if let Err(r) = require_csrf(&form.csrf, &session) {
        return r;
    }

    let redirect = format!("/console/accounts/{account_id}/groups/{group_name}");

    match state
        .catalog_store
        .remove_group_member(&account_id, &group_name, &user_name)
        .await
    {
        Ok(()) => {
            state
                .auth_cache
                .invalidate_user_group_policies(&account_id, &user_name)
                .await;
            Redirect::to(&redirect).into_response()
        }
        Err(e) => {
            let nav = html::nav_bar(&identity_label(&session.identity));
            let content = format!(
                "<h1>Remove Member</h1>{}",
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
