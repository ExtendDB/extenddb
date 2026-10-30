// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The management store migration 2's shared body runs through on MongoDB.
//!
//! Every call checks the migration lock's lease first, so a holder that was
//! paused past its deadline sends no further request, as the runner checks
//! before each of its own, and waits at most
//! [`REQUEST_TIMEOUT`](super::REQUEST_TIMEOUT) for its answer, as each of
//! the runner's requests does. A call that completes is progress, as a
//! request of the runner's is, so the renewal task keeps the lock through a
//! scan that lasts longer than the idle limit.
//!
//! `list_all_accounts` reads every document in `accounts`, including the
//! default account a released `init` wrote with only `_id`, which the
//! store's own listing skips because it has no `account_id` field; its
//! `_id` is the id its principals would carry.

use std::sync::Arc;

use bson::{Document, doc};
use extenddb_storage::management_store::{
    AccessKeyCreated, AccountDetail, GroupDetail, GroupListEntry, ManagementStore, OpError,
    OpResult, RoleDetail, RoleListEntry, SessionPolicyEntry, UserDetail, UserListEntry,
};
use futures::future::BoxFuture;

use super::REQUEST_TIMEOUT;
use super::locks::Lease;
use crate::MongoCatalogStore;

/// [`MongoCatalogStore`] with the lease checked before every call, a timeout
/// on every call, and progress recorded when a call completes.
pub(crate) struct LeasedManagementStore {
    inner: MongoCatalogStore,
    accounts: mongodb::Collection<Document>,
    lease: Option<Arc<Lease>>,
    #[cfg(feature = "test-util")]
    stop_after: Option<Arc<std::sync::atomic::AtomicUsize>>,
}

/// Whether the test hook's request budget is spent; takes one request.
#[cfg(feature = "test-util")]
pub(crate) fn stopped(left: Option<&std::sync::atomic::AtomicUsize>) -> bool {
    use std::sync::atomic::Ordering;
    // A compare-exchange loop: `fetch_update` is deprecated on current
    // stable and its replacement does not exist on 1.88.
    let Some(left) = left else {
        return false;
    };
    let mut n = left.load(Ordering::SeqCst);
    loop {
        let Some(next) = n.checked_sub(1) else {
            return true;
        };
        match left.compare_exchange(n, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return false,
            Err(actual) => n = actual,
        }
    }
}

impl LeasedManagementStore {
    pub(crate) fn new(inner: MongoCatalogStore, lease: Option<Arc<Lease>>) -> Self {
        let accounts = inner.catalog_db().collection::<Document>("accounts");
        Self {
            inner,
            accounts,
            lease,
            #[cfg(feature = "test-util")]
            stop_after: None,
        }
    }

    #[cfg(feature = "test-util")]
    pub(crate) fn stop_after(&mut self, left: Arc<std::sync::atomic::AtomicUsize>) {
        self.stop_after = Some(left);
    }

    fn lease_ok(&self) -> OpResult<()> {
        #[cfg(feature = "test-util")]
        if stopped(self.stop_after.as_deref()) {
            return Err(OpError::Internal("stopped by the test".to_owned()));
        }
        match &self.lease {
            Some(lease) => lease.check().map_err(|e| OpError::Internal(e.to_string())),
            None => Ok(()),
        }
    }

    /// Record that a call completed, as `MongoStores::request` does after
    /// each request, so the renewal task keeps the lock while a scan runs.
    fn progressed(&self) {
        if let Some(lease) = &self.lease {
            lease.progressed();
        }
    }
}

/// Delegate each method to the inner store after checking the lease, wait
/// at most [`REQUEST_TIMEOUT`] for its answer, and record progress when the
/// call completes.
macro_rules! checked {
    ($( fn $name:ident(&self $(, $arg:ident: $ty:ty)* $(,)?) -> $ret:ty; )+) => {
        $(
            fn $name(&self $(, $arg: $ty)*) -> $ret {
                if let Err(e) = self.lease_ok() {
                    return Box::pin(async move { Err(e) });
                }
                let call = self.inner.$name($($arg),*);
                Box::pin(async move {
                    let answer = tokio::time::timeout(REQUEST_TIMEOUT, call).await;
                    self.progressed();
                    answer.unwrap_or_else(|_| Err(no_answer(stringify!($name))))
                })
            }
        )+
    };
}

/// A call that had no answer within [`REQUEST_TIMEOUT`].
fn no_answer(what: &str) -> OpError {
    OpError::Internal(format!(
        "{what}: no answer within {} s",
        REQUEST_TIMEOUT.as_secs()
    ))
}

impl ManagementStore for LeasedManagementStore {
    fn list_all_accounts(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async move {
            use futures::TryStreamExt;
            self.lease_ok()?;
            let read = async {
                let cursor = self.accounts.find(doc! {}).sort(doc! { "_id": 1 }).await?;
                cursor.try_collect::<Vec<Document>>().await
            };
            let answer = tokio::time::timeout(REQUEST_TIMEOUT, read).await;
            self.progressed();
            let docs = match answer {
                Ok(Ok(docs)) => docs,
                Ok(Err(e)) => return Err(OpError::Internal(format!("list accounts: {e}"))),
                Err(_) => return Err(no_answer("list accounts")),
            };
            Ok(docs
                .iter()
                .filter_map(|d| {
                    let id = d.get_str("account_id").or_else(|_| d.get_str("_id")).ok()?;
                    Some((
                        id.to_owned(),
                        d.get_str("account_name").unwrap_or_default().to_owned(),
                    ))
                })
                .collect())
        })
    }

    checked! {
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

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use extenddb_storage::management_store::{ManagementStore, OpError};

    use super::super::REQUEST_TIMEOUT;
    use super::super::locks::tests::silent_server;
    use super::LeasedManagementStore;
    use crate::MongoCatalogStore;

    /// A delegated call to a database that stopped answering fails at the
    /// request timeout, not when the driver gives up on the connection.
    #[tokio::test]
    async fn a_delegated_call_with_no_answer_fails_at_the_request_timeout() {
        let (address, accepting) = silent_server().await;
        let mut options = mongodb::options::ClientOptions::parse(format!(
            "mongodb://{address}/?directConnection=true"
        ))
        .await
        .expect("options");
        options.connect_timeout = Some(Duration::from_secs(60));
        options.server_selection_timeout = Some(Duration::from_secs(60));
        let client = mongodb::Client::with_options(options).expect("client");
        let store = LeasedManagementStore::new(
            MongoCatalogStore::on_database(client.clone(), client.database("silent")),
            None,
        );
        let started = Instant::now();
        let answer = tokio::time::timeout(
            REQUEST_TIMEOUT + Duration::from_secs(5),
            store.list_policies("000000000000", "user", "alice"),
        )
        .await
        .expect("an answer within the request timeout");
        let elapsed = started.elapsed();
        let Err(OpError::Internal(message)) = answer else {
            panic!("{answer:?}");
        };
        assert!(
            message.contains("no answer within 10 s"),
            "{message} after {elapsed:?}"
        );
        assert!(elapsed >= REQUEST_TIMEOUT, "{elapsed:?}");
        accepting.abort();
    }
}
