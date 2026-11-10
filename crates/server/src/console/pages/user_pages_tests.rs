// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the console user page: an IAM-user session sees its own access
//! keys and no link to a page it may not open; an admin session sees the
//! whole page.

use axum::http::StatusCode;
use axum::response::Response;
use extenddb_storage::management_store::UserDetail;

use super::{render_user_page, user_breadcrumb};
use crate::console::pages::SessionData;
use crate::management::CallerIdentity;

const ACCOUNT: &str = "444455556666";
const USER: &str = "bob";
/// Names no page would show by chance, so a test can look for them.
const KEY_ID: &str = "AKIAEXAMPLE000000731";
const POLICY: &str = "policy-7f3e1c";
const GROUP: &str = "group-9a42d0";
const TAG_KEY: &str = "tag-key-51b8";
const TAG_VALUE: &str = "tag-value-c06e";

fn session(identity: CallerIdentity) -> SessionData {
    SessionData {
        identity,
        csrf_token: "csrf-token".to_owned(),
    }
}

fn admin() -> CallerIdentity {
    CallerIdentity::Admin("admin".to_owned())
}

fn iam_user() -> CallerIdentity {
    CallerIdentity::IamUser {
        account_id: ACCOUNT.to_owned(),
        user_name: USER.to_owned(),
    }
}

fn detail() -> UserDetail {
    UserDetail {
        keys: vec![(KEY_ID.to_owned(), true)],
        policies: vec![POLICY.to_owned()],
        tags: vec![(TAG_KEY.to_owned(), TAG_VALUE.to_owned())],
        groups: vec![GROUP.to_owned()],
    }
}

async fn body_text(response: Response) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    String::from_utf8(bytes.to_vec()).expect("UTF-8 response body")
}

/// Every link target (`href="..."`) in `page`.
fn links(page: &str) -> Vec<&str> {
    page.split("href=\"")
        .skip(1)
        .filter_map(|rest| rest.split('"').next())
        .collect()
}

#[tokio::test]
async fn iam_user_page_shows_its_access_keys_and_the_forms_that_change_them() {
    let response = render_user_page(&session(iam_user()), ACCOUNT, USER, Some(&detail()));

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_text(response).await;
    assert!(body.contains("<h1>User: bob</h1>"), "{body}");
    assert!(body.contains("<h2>Access Keys (1)</h2>"), "{body}");
    assert!(body.contains(&format!("<code>{KEY_ID}</code>")), "{body}");
    assert!(
        body.contains(r#"action="/console/accounts/444455556666/users/bob/access-keys/new""#),
        "{body}"
    );
    assert!(
        body.contains(&format!(
            r#"action="/console/accounts/444455556666/users/bob/access-keys/{KEY_ID}/delete""#
        )),
        "{body}"
    );
}

#[tokio::test]
async fn iam_user_page_leaves_out_policies_groups_and_tags() {
    let response = render_user_page(&session(iam_user()), ACCOUNT, USER, Some(&detail()));

    let body = body_text(response).await;
    for absent in [
        POLICY,
        GROUP,
        TAG_KEY,
        TAG_VALUE,
        "<h2>Policies",
        "<h2>Groups",
        "<h2>Tags",
        "Delete User",
        "/policies/",
    ] {
        assert!(!body.contains(absent), "the page shows {absent:?}: {body}");
    }
}

#[tokio::test]
async fn iam_user_page_links_to_no_account_page() {
    // Every page under /console/accounts other than its own user page
    // redirects an IAM-user session, and the page is not linked to itself.
    let response = render_user_page(&session(iam_user()), ACCOUNT, USER, Some(&detail()));

    let body = body_text(response).await;
    let to_accounts: Vec<&str> = links(&body)
        .into_iter()
        .filter(|link| link.starts_with("/console/accounts"))
        .collect();
    assert!(to_accounts.is_empty(), "{to_accounts:?}: {body}");
    assert!(
        body.contains(r#"<a href="/console">Dashboard</a>"#),
        "{body}"
    );
}

#[tokio::test]
async fn admin_page_shows_the_whole_user() {
    let response = render_user_page(&session(admin()), ACCOUNT, USER, Some(&detail()));

    assert_eq!(response.status(), StatusCode::OK);
    let body = body_text(response).await;
    for shown in [
        KEY_ID,
        POLICY,
        TAG_KEY,
        TAG_VALUE,
        "<h2>Policies (1)</h2>",
        "<h2>Groups (1)</h2>",
        "<h2>Tags (1)</h2>",
        "Delete User",
        r#"href="/console/accounts/444455556666/users/bob/policies/new""#,
        r#"href="/console/accounts/444455556666/groups/group-9a42d0""#,
        r#"action="/console/accounts/444455556666/users/bob/policies/policy-7f3e1c/delete""#,
        r#"<a href="/console/accounts">Accounts</a> &rsaquo; <a href="/console/accounts/444455556666">444455556666</a> &rsaquo; bob</div>"#,
    ] {
        assert!(body.contains(shown), "the page lacks {shown:?}: {body}");
    }
}

#[tokio::test]
async fn a_missing_user_gets_the_not_found_page() {
    let response = render_user_page(&session(admin()), ACCOUNT, "nobody", None);

    assert_eq!(response.status(), StatusCode::OK);
    assert!(body_text(response).await.contains("User not found"));
}

#[test]
fn iam_user_breadcrumb_holds_the_dashboard_and_its_own_user_page() {
    assert_eq!(
        user_breadcrumb(&iam_user(), ACCOUNT, USER, None),
        r#"<div class="breadcrumb"><a href="/console">Dashboard</a> &rsaquo; bob</div>"#
    );
    assert_eq!(
        user_breadcrumb(&iam_user(), ACCOUNT, USER, Some("New Access Key")),
        concat!(
            r#"<div class="breadcrumb"><a href="/console">Dashboard</a> &rsaquo; "#,
            r#"<a href="/console/accounts/444455556666/users/bob">bob</a> &rsaquo; New Access Key</div>"#,
        )
    );
}

#[test]
fn admin_breadcrumb_runs_through_the_account_list_and_the_account() {
    assert_eq!(
        user_breadcrumb(&admin(), ACCOUNT, USER, Some("New Access Key")),
        concat!(
            r#"<div class="breadcrumb"><a href="/console">Dashboard</a> &rsaquo; "#,
            r#"<a href="/console/accounts">Accounts</a> &rsaquo; "#,
            r#"<a href="/console/accounts/444455556666">444455556666</a> &rsaquo; "#,
            r#"<a href="/console/accounts/444455556666/users/bob">bob</a> &rsaquo; New Access Key</div>"#,
        )
    );
}
