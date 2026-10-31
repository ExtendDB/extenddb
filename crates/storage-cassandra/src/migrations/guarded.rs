// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The management store migration 2's shared body runs through under the
//! migration lock.
//!
//! The runner bounds every request it sends itself by
//! [`REQUEST_TIMEOUT`](super::connect::REQUEST_TIMEOUT) and checks the lock's
//! deadline before each one. A code body's requests go through
//! [`CassandraCatalogStore`], whose session carries a connection timeout and
//! no request timeout, so a request that never gets an answer was cut only
//! by the lease: no progress for the stall, then the deadline, 35 to 65 s
//! after the last answer. This store wraps every delegated call: the lease
//! is checked first, so a holder paused past its deadline sends nothing
//! more; the call waits at most the request timeout for its answer; and a
//! call that completes is progress, as a request of the runner's is, so the
//! renewal keeps the lock through a scan that lasts longer than the stall.

use std::time::Duration;

use extenddb_storage::management_store::{
    AccessKeyCreated, AccountDetail, GroupDetail, GroupListEntry, ManagementStore, OpError,
    OpResult, RoleDetail, RoleListEntry, SessionPolicyEntry, UserDetail, UserListEntry,
};
use futures::future::BoxFuture;

use super::connect::REQUEST_TIMEOUT;
use super::locks::LeaseHandle;
use crate::catalog_store::CassandraCatalogStore;

/// [`CassandraCatalogStore`] with the lease checked before every call, the
/// request timeout on every call, and progress recorded when a call
/// completes.
pub(crate) struct LeasedManagementStore {
    inner: CassandraCatalogStore,
    lease: Option<LeaseHandle>,
    /// The bound on one call: the request timeout, or a shorter one in tests.
    bound: Duration,
}

impl LeasedManagementStore {
    pub(crate) fn new(inner: CassandraCatalogStore, lease: Option<LeaseHandle>) -> Self {
        Self {
            inner,
            lease,
            bound: REQUEST_TIMEOUT,
        }
    }

    /// Stop the policy writes of migration 2's body when the test hook's
    /// request budget is spent.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn stop_after(&mut self, left: std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        self.inner.stop_after(left);
    }

    fn lease_ok(&self) -> OpResult<()> {
        match &self.lease {
            Some(lease) => lease.check().map_err(|e| OpError::Internal(e.to_string())),
            None => Ok(()),
        }
    }

    /// Record that a call completed, as the runner does after each of its
    /// requests, so the renewal task keeps the lock while a scan runs.
    fn progressed(&self) {
        if let Some(lease) = &self.lease {
            lease.progress();
        }
    }

    /// A call that had no answer within the bound.
    fn no_answer(&self, what: &str) -> OpError {
        OpError::Internal(format!(
            "{what}: no answer within {} s",
            self.bound.as_secs()
        ))
    }
}

/// Delegate each method to the inner store after checking the lease, wait
/// at most the bound for its answer, and record progress when the call
/// completes.
macro_rules! checked {
    ($( fn $name:ident(&self $(, $arg:ident: $ty:ty)* $(,)?) -> $ret:ty; )+) => {
        $(
            fn $name(&self $(, $arg: $ty)*) -> $ret {
                if let Err(e) = self.lease_ok() {
                    return Box::pin(async move { Err(e) });
                }
                let call = self.inner.$name($($arg),*);
                Box::pin(async move {
                    let answer = tokio::time::timeout(self.bound, call).await;
                    self.progressed();
                    answer.unwrap_or_else(|_| Err(self.no_answer(stringify!($name))))
                })
            }
        )+
    };
}

impl ManagementStore for LeasedManagementStore {
    checked! {
        fn list_all_accounts(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;
        fn create_account(&self, account_id: &str, account_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn delete_account(&self, account_id: &str) -> BoxFuture<'_, OpResult<()>>;
        fn default_account_id(&self) -> BoxFuture<'_, OpResult<Option<String>>>;
        fn list_all_accounts_full(&self) -> BoxFuture<'_, OpResult<Vec<(String, String, time::OffsetDateTime)>>>;
        fn list_accounts_for(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;
        fn get_account_detail(&self, account_id: &str) -> BoxFuture<'_, OpResult<Option<AccountDetail>>>;
        fn dashboard_counts(&self) -> BoxFuture<'_, OpResult<(i64, i64)>>;
        fn create_user(&self, account_id: &str, user_name: &str, password_hash: Option<&str>) -> BoxFuture<'_, OpResult<()>>;
        fn delete_user(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn list_users(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<UserListEntry>>>;
        fn get_user_detail(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<Option<UserDetail>>>;
        fn verify_iam_user_password(&self, account_id: &str, user_name: &str, password: &str) -> BoxFuture<'_, OpResult<bool>>;
        fn change_user_password(&self, account_id: &str, user_name: &str, password_hash: &str) -> BoxFuture<'_, OpResult<()>>;
        fn tag_user(&self, account_id: &str, user_name: &str, tags: &[(String, String)]) -> BoxFuture<'_, OpResult<()>>;
        fn untag_user(&self, account_id: &str, user_name: &str, tag_keys: &[String]) -> BoxFuture<'_, OpResult<()>>;
        fn list_user_tags(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;
        fn create_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn delete_group(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn list_groups(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<GroupListEntry>>>;
        fn get_group_detail(&self, account_id: &str, group_name: &str) -> BoxFuture<'_, OpResult<Option<GroupDetail>>>;
        fn add_group_member(&self, account_id: &str, group_name: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn remove_group_member(&self, account_id: &str, group_name: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn create_role(&self, account_id: &str, role_name: &str, trust_policy: &serde_json::Value) -> BoxFuture<'_, OpResult<()>>;
        fn delete_role(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn list_roles(&self, account_id: &str) -> BoxFuture<'_, OpResult<Vec<RoleListEntry>>>;
        fn get_role_detail(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<Option<RoleDetail>>>;
        fn get_role_trust_policy(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>>;
        fn tag_role(&self, account_id: &str, role_name: &str, tags: &[(String, String)]) -> BoxFuture<'_, OpResult<()>>;
        fn untag_role(&self, account_id: &str, role_name: &str, tag_keys: &[String]) -> BoxFuture<'_, OpResult<()>>;
        fn list_role_tags(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;
        fn put_policy(&self, account_id: &str, principal_type: &str, principal_name: &str, policy_name: &str, document: &serde_json::Value) -> BoxFuture<'_, OpResult<()>>;
        fn delete_policy(&self, account_id: &str, principal_type: &str, principal_name: &str, policy_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn list_policies(&self, account_id: &str, principal_type: &str, principal_name: &str) -> BoxFuture<'_, OpResult<Vec<(String, serde_json::Value, time::OffsetDateTime)>>>;
        fn set_user_boundary(&self, account_id: &str, user_name: &str, document: &serde_json::Value) -> BoxFuture<'_, OpResult<()>>;
        fn get_user_boundary(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>>;
        fn delete_user_boundary(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn set_role_boundary(&self, account_id: &str, role_name: &str, document: &serde_json::Value) -> BoxFuture<'_, OpResult<()>>;
        fn get_role_boundary(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<Option<serde_json::Value>>>;
        fn delete_role_boundary(&self, account_id: &str, role_name: &str) -> BoxFuture<'_, OpResult<()>>;
        fn create_access_key(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<AccessKeyCreated>>;
        fn delete_access_key(&self, account_id: &str, user_name: &str, key_id: &str) -> BoxFuture<'_, OpResult<()>>;
        fn list_access_keys(&self, account_id: &str, user_name: &str) -> BoxFuture<'_, OpResult<Vec<(String, bool, time::OffsetDateTime)>>>;
        fn import_access_key(&self, account_id: &str, user_name: &str, access_key_id: &str, secret_access_key: &str) -> BoxFuture<'_, OpResult<()>>;
        fn store_session(&self, session_token: &str, access_key_id: &str, secret_key_encrypted: &[u8], account_id: &str, role_name: &str, session_name: &str, session_tags: &Option<serde_json::Value>, session_policy: &Option<serde_json::Value>, expires_at: time::OffsetDateTime) -> BoxFuture<'_, OpResult<()>>;
        fn list_session_policies(&self) -> BoxFuture<'_, OpResult<Vec<SessionPolicyEntry>>>;
        fn update_session_policy(&self, session_token: &str, document: &serde_json::Value) -> BoxFuture<'_, OpResult<()>>;
        fn fetch_caller_tags(&self, account_id: &str, resource: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>>;
    }
}
