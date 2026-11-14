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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::header::{COOKIE, LOCATION};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::Response;
use extenddb_core::metrics::MetricsCollector;
use extenddb_storage::CatalogStore;
use extenddb_storage::authorization_store::{AuthorizationStore, SessionData as RoleSession};
use extenddb_storage::management_store::{
    AccessKeyCreated, AccountDetail, AdminEntry, AdminStore, GroupDetail, GroupListEntry,
    ManagementStore, MetricsFilter, MetricsRow, MetricsStore, OpError, OpResult, PasswordHashStore,
    RateLimitStore, RoleDetail, RoleListEntry, SettingsStore, UserDetail, UserListEntry,
};
use futures::future::BoxFuture;
use serde_json::Value;
use time::OffsetDateTime;

use crate::console::ConsoleState;
use crate::console::pages::{account_detail, group_detail, role_detail, user_detail};
use crate::console::session::{CredentialStamp, SessionStore};
use crate::management::CallerIdentity;

const ACCOUNT: &str = "444455556666";
/// The name of the user `iam_user` logs in as.
const OWN_USER: &str = "bob";

/// A catalog store that holds no data and counts every call to it.
#[derive(Default)]
struct CountingStore {
    calls: AtomicUsize,
    /// Reads of a password hash, which the session check makes on every
    /// request.
    hash_reads: AtomicUsize,
}

impl CountingStore {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn hash_reads(&self) -> usize {
        self.hash_reads.load(Ordering::SeqCst)
    }

    /// Count a password hash read and answer it with [`session_hash`].
    fn hash_read(&self) -> BoxFuture<'_, OpResult<Option<String>>> {
        self.hash_reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(Some(session_hash())) })
    }

    /// Count a call and answer it with an error. Each page handler renders
    /// an error from the store as its "not found" page.
    fn call<T: Send + 'static>(&self) -> BoxFuture<'_, OpResult<T>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(OpError::Internal("the test store holds no data".to_owned())) })
    }
}

/// Implements each listed method of a store trait as a counted call.
/// `name(argument types) -> output;` stands for
/// `fn name(&self, ...) -> BoxFuture<'_, OpResult<output>>`.
macro_rules! counted_calls {
    ($($name:ident($($arg:ty),*) -> $out:ty;)*) => {
        $(
            fn $name(&self, $(_: $arg),*) -> BoxFuture<'_, OpResult<$out>> {
                self.call()
            }
        )*
    };
}

impl ManagementStore for CountingStore {
    counted_calls! {
        create_account(&str, &str) -> ();
        delete_account(&str) -> ();
        list_all_accounts() -> Vec<(String, String)>;
        default_account_id() -> Option<String>;
        list_all_accounts_full() -> Vec<(String, String, OffsetDateTime)>;
        list_accounts_for(&str) -> Vec<(String, String)>;
        get_account_detail(&str) -> Option<AccountDetail>;
        dashboard_counts() -> (i64, i64);
        create_user(&str, &str, Option<&str>) -> ();
        delete_user(&str, &str) -> ();
        list_users(&str) -> Vec<UserListEntry>;
        get_user_detail(&str, &str) -> Option<UserDetail>;
        verify_iam_user_password(&str, &str, &str) -> bool;
        change_user_password(&str, &str, &str) -> ();
        tag_user(&str, &str, &[(String, String)]) -> ();
        untag_user(&str, &str, &[String]) -> ();
        list_user_tags(&str, &str) -> Vec<(String, String)>;
        create_group(&str, &str) -> ();
        delete_group(&str, &str) -> ();
        list_groups(&str) -> Vec<GroupListEntry>;
        get_group_detail(&str, &str) -> Option<GroupDetail>;
        add_group_member(&str, &str, &str) -> ();
        remove_group_member(&str, &str, &str) -> ();
        create_role(&str, &str, &Value) -> ();
        delete_role(&str, &str) -> ();
        list_roles(&str) -> Vec<RoleListEntry>;
        get_role_detail(&str, &str) -> Option<RoleDetail>;
        get_role_trust_policy(&str, &str) -> Option<Value>;
        tag_role(&str, &str, &[(String, String)]) -> ();
        untag_role(&str, &str, &[String]) -> ();
        list_role_tags(&str, &str) -> Vec<(String, String)>;
        put_policy(&str, &str, &str, &str, &Value) -> ();
        delete_policy(&str, &str, &str, &str) -> ();
        list_policies(&str, &str, &str) -> Vec<(String, Value, OffsetDateTime)>;
        set_user_boundary(&str, &str, &Value) -> ();
        get_user_boundary(&str, &str) -> Option<Value>;
        delete_user_boundary(&str, &str) -> ();
        set_role_boundary(&str, &str, &Value) -> ();
        get_role_boundary(&str, &str) -> Option<Value>;
        delete_role_boundary(&str, &str) -> ();
        create_access_key(&str, &str) -> AccessKeyCreated;
        delete_access_key(&str, &str, &str) -> ();
        list_access_keys(&str, &str) -> Vec<(String, bool, OffsetDateTime)>;
        import_access_key(&str, &str, &str, &str) -> ();
        store_session(
            &str, &str, &[u8], &str, &str, &str, &Option<Value>, &Option<Value>, OffsetDateTime
        ) -> ();
        fetch_caller_tags(&str, &str) -> Vec<(String, String)>;
    }
}

impl AdminStore for CountingStore {
    counted_calls! {
        create_admin(&str, &str) -> ();
        list_admins() -> Vec<AdminEntry>;
        delete_admin(&str) -> ();
        change_admin_password(&str, &str) -> ();
        verify_admin_password(&str, &str) -> Option<bool>;
    }
}

impl SettingsStore for CountingStore {
    counted_calls! {
        get_setting(&str) -> Option<String>;
        set_setting(&str, &str) -> ();
        list_settings() -> Vec<(String, String)>;
    }
}

impl MetricsStore for CountingStore {
    counted_calls! {
        insert_metrics(&[MetricsRow]) -> ();
        query_metrics(OffsetDateTime, OffsetDateTime, &MetricsFilter) -> Vec<MetricsRow>;
        prune_metrics(Duration) -> ();
    }
}

impl RateLimitStore for CountingStore {
    counted_calls! {
        count_principal_failures(&str, &str, i64) -> i64;
        count_ip_failures(&str, i64) -> i64;
    }

    fn record_failed_login(&self, _: &str, _: Option<&str>) -> BoxFuture<'_, ()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {})
    }

    fn cleanup_old_attempts(&self, _: i64) -> BoxFuture<'_, ()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {})
    }
}

impl AuthorizationStore for CountingStore {
    counted_calls! {
        fetch_user_policies(&str, &str) -> Vec<String>;
        fetch_user_group_policies(&str, &str) -> Vec<(String, String)>;
        fetch_user_boundary(&str, &str) -> Option<String>;
        fetch_role_policies(&str, &str) -> Vec<String>;
        fetch_role_boundary(&str, &str) -> Option<String>;
        fetch_session_data(&str, &str, &str) -> Option<RoleSession>;
        fetch_user_tags(&str, &str) -> Vec<(String, String)>;
        fetch_role_tags(&str, &str) -> Vec<(String, String)>;
        fetch_resource_tags(&str) -> Vec<(String, String)>;
        fetch_user_created_at(&str, &str) -> Option<String>;
        fetch_role_created_at(&str, &str) -> Option<String>;
    }
}

impl PasswordHashStore for CountingStore {
    fn get_admin_password_hash(&self, _: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        self.hash_read()
    }

    fn get_user_password_hash(&self, _: &str, _: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        self.hash_read()
    }
}

impl CatalogStore for CountingStore {
    fn cached_encryption_key(&self) -> Option<String> {
        None
    }
}

/// The password hash every test session logs in with. It is made at run
/// time, so that no literal reaches the session stamp.
fn session_hash() -> String {
    format!("session-hash-{}", std::process::id())
}

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
    let state = Arc::new(ConsoleState {
        sessions: SessionStore::new(),
        version_info: Arc::from("test-version"),
        listen_url: "https://127.0.0.1:8000".to_owned(),
        config_entries: Vec::new(),
        catalog_store: store.clone(),
        docs_store: None,
        auth_cache: extenddb_auth::AuthCacheRegistry::empty(),
        metrics: Arc::new(MetricsCollector::new()),
        max_policy_document_bytes: extenddb_core::limits::DEFAULT_MAX_POLICY_DOCUMENT_BYTES,
        secure_cookies: false,
    });
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
