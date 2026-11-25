// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests that the handler of a page an IAM-user session may not open
//! redirects the session before it calls the catalog store, other than for
//! the session check.
//!
//! The handlers run against a store that holds no data and counts every
//! call to it. Every request first reads the password hash of the session's
//! principal, to check that the session is still valid. The store answers
//! that read with the hash the session logged in with, and counts it apart
//! from the other calls. A refused request makes that one read and no other
//! call. An admin request and the IAM user's own user page each make one
//! other call, which shows that the store counts the calls the handlers make.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::header::{COOKIE, LOCATION};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::Response;

use crate::console::ConsoleState;
use crate::console::pages::{account_detail, group_detail, role_detail, user_detail};
use crate::console::session::CredentialStamp;
use crate::console::test_catalog::{CountingStore, console_state, session_hash};
use crate::management::CallerIdentity;

const ACCOUNT: &str = "444455556666";
/// The name of the user `iam_user` logs in as.
const OWN_USER: &str = "bob";

fn admin() -> CallerIdentity {
    CallerIdentity::Admin("admin".to_owned())
}

fn iam_user() -> CallerIdentity {
    CallerIdentity::IamUser {
        account_id: ACCOUNT.to_owned(),
        user_name: OWN_USER.to_owned(),
    }
}

/// A console page of the account `ACCOUNT`.
#[derive(Clone, Copy, Debug)]
enum Page {
    Account,
    Group,
    Role,
    User(&'static str),
}

/// A console whose catalog is a new `CountingStore`, the store, and the
/// cookie header of a session that `identity` holds in the console.
async fn console(identity: CallerIdentity) -> (Arc<ConsoleState>, Arc<CountingStore>, HeaderMap) {
    let store = Arc::new(CountingStore::default());
    let state = console_state(store.clone());
    let stamp = CredentialStamp::of(&session_hash());
    let (token, _csrf) = state.sessions.create(identity, stamp).await;
    let mut headers = HeaderMap::new();
    let cookie = HeaderValue::from_str(&format!("extenddb_session={token}")).expect("cookie");
    headers.insert(COOKIE, cookie);
    (state, store, headers)
}

/// Run the handler of `page` for the session in `headers`.
async fn open(state: &Arc<ConsoleState>, headers: &HeaderMap, page: Page) -> Response {
    let (state, headers) = (State(Arc::clone(state)), headers.clone());
    let account = ACCOUNT.to_owned();
    match page {
        Page::Account => account_detail(state, headers, Path(account)).await,
        Page::Group => group_detail(state, headers, Path((account, "devs".to_owned()))).await,
        Page::Role => role_detail(state, headers, Path((account, "deployer".to_owned()))).await,
        Page::User(name) => user_detail(state, headers, Path((account, name.to_owned()))).await,
    }
}

/// `page` redirects an IAM-user session to the console home page, and its
/// handler makes no call to the store other than the hash read of the
/// session check.
async fn assert_refused_before_the_store_is_called(page: Page) {
    let (state, store, headers) = console(iam_user()).await;

    let response = open(&state, &headers, page).await;

    assert_eq!(response.status(), StatusCode::SEE_OTHER, "{page:?}");
    let location = response
        .headers()
        .get(LOCATION)
        .and_then(|v| v.to_str().ok());
    assert_eq!(location, Some("/console"), "{page:?}");
    assert_eq!(
        store.calls(),
        0,
        "the handler of {page:?} called the store for a refused IAM-user session"
    );
    assert_eq!(
        store.hash_reads(),
        1,
        "the request for {page:?} read a password hash other than once, in the session check"
    );
}

#[tokio::test]
async fn iam_user_is_refused_the_account_page_before_the_store_is_called() {
    assert_refused_before_the_store_is_called(Page::Account).await;
}

#[tokio::test]
async fn iam_user_is_refused_a_group_page_before_the_store_is_called() {
    assert_refused_before_the_store_is_called(Page::Group).await;
}

#[tokio::test]
async fn iam_user_is_refused_a_role_page_before_the_store_is_called() {
    assert_refused_before_the_store_is_called(Page::Role).await;
}

#[tokio::test]
async fn iam_user_is_refused_another_users_page_before_the_store_is_called() {
    assert_refused_before_the_store_is_called(Page::User("alice")).await;
}

#[tokio::test]
async fn iam_user_own_page_calls_the_store_once() {
    let (state, store, headers) = console(iam_user()).await;

    let response = open(&state, &headers, Page::User(OWN_USER)).await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(store.calls(), 1);
}

#[tokio::test]
async fn admin_page_calls_the_store_once() {
    for page in [Page::Account, Page::Group, Page::Role, Page::User("alice")] {
        let (state, store, headers) = console(admin()).await;

        let response = open(&state, &headers, page).await;

        assert_eq!(response.status(), StatusCode::OK, "{page:?}");
        assert_eq!(store.calls(), 1, "{page:?}");
    }
}
