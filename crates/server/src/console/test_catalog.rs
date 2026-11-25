// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A catalog store for console tests that holds no data and counts every call
//! to it, and a console whose catalog is one.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use extenddb_core::metrics::MetricsCollector;
use extenddb_storage::CatalogStore;
use extenddb_storage::authorization_store::{AuthorizationStore, SessionData as RoleSession};
use extenddb_storage::management_store::{
    AccessKeyCreated, AccountDetail, AdminEntry, AdminStore, GroupDetail, GroupListEntry,
    ManagementStore, MetricsFilter, MetricsRow, MetricsStore, OpError, OpResult, PasswordHashStore,
    RateLimitStore, RoleDetail, RoleListEntry, SecretPage, SecretWrite, SettingsStore, UserDetail,
    UserListEntry,
};
use extenddb_storage::secret_keys::SecretTable;
use futures::future::BoxFuture;
use serde_json::Value;
use time::OffsetDateTime;

use crate::console::ConsoleState;
use crate::console::session::SessionStore;

/// A catalog store that holds no data and counts every call to it.
#[derive(Default)]
pub(crate) struct CountingStore {
    calls: AtomicUsize,
    /// Reads of a password hash, which the session check makes on every
    /// request.
    hash_reads: AtomicUsize,
    /// What a password hash read returns.
    hash: StoredHash,
}

/// What [`CountingStore`] answers when the session check reads the password
/// hash of the session's principal.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) enum StoredHash {
    /// The hash the session logged in with, [`session_hash`].
    #[default]
    LoginHash,
    /// No hash: the principal was deleted.
    Missing,
    /// Another hash: the password changed after the login.
    Changed,
    /// An error: the catalog cannot be read.
    Unreadable,
}

impl CountingStore {
    /// A store whose password hash reads return `hash`.
    pub(crate) fn answering(hash: StoredHash) -> Self {
        Self {
            hash,
            ..Self::default()
        }
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    pub(crate) fn hash_reads(&self) -> usize {
        self.hash_reads.load(Ordering::SeqCst)
    }

    /// Count a password hash read and answer it as [`StoredHash`] says.
    fn hash_read(&self) -> BoxFuture<'_, OpResult<Option<String>>> {
        self.hash_reads.fetch_add(1, Ordering::SeqCst);
        let answer = match self.hash {
            StoredHash::LoginHash => Ok(Some(session_hash())),
            StoredHash::Missing => Ok(None),
            StoredHash::Changed => Ok(Some(format!("{}-changed", session_hash()))),
            StoredHash::Unreadable => Err(OpError::Internal("the test catalog is down".to_owned())),
        };
        Box::pin(async move { answer })
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
        read_secret_page(SecretTable, Option<&str>, usize) -> SecretPage;
        write_secrets(SecretTable, &[SecretWrite]) -> ();
        delete_secrets(SecretTable, &[String]) -> ();
        delete_setting(&str) -> ();
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

impl CatalogStore for CountingStore {}

/// The password hash every test session logs in with. It is made at run
/// time, so that no literal reaches the session stamp.
pub(crate) fn session_hash() -> String {
    format!("session-hash-{}", std::process::id())
}

/// A console with no tabs whose catalog is `store`.
pub(crate) fn console_state(store: Arc<CountingStore>) -> Arc<ConsoleState> {
    Arc::new(ConsoleState {
        sessions: SessionStore::new(),
        tabs: Arc::from(Vec::new()),
        version_info: Arc::from("test-version"),
        listen_url: "https://127.0.0.1:8000".to_owned(),
        config_entries: Vec::new(),
        catalog_store: store,
        docs_store: None,
        auth_cache: extenddb_auth::AuthCacheRegistry::empty(),
        metrics: Arc::new(MetricsCollector::new()),
        secure_cookies: false,
    })
}
