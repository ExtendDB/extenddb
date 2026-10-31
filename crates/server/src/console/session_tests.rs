// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for console session validation against the catalog.

use std::collections::HashMap;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use extenddb_storage::management_store::{OpError, OpResult, PasswordHashStore};
use futures::future::BoxFuture;

use super::{CredentialStamp, SESSION_TTL, SessionCheck, SessionStore};
use crate::management::CallerIdentity;

const ACCOUNT: &str = "111111111111";

/// Catalog holding password hashes for admins and IAM users. With `failing`
/// set, every read returns an error.
#[derive(Default)]
struct FakeCatalog {
    admins: StdMutex<HashMap<String, String>>,
    users: StdMutex<HashMap<(String, String), String>>,
    failing: AtomicBool,
}

impl FakeCatalog {
    fn set_admin(&self, name: &str, hash: &str) {
        self.admins
            .lock()
            .unwrap()
            .insert(name.to_owned(), hash.to_owned());
    }

    fn delete_admin(&self, name: &str) {
        self.admins.lock().unwrap().remove(name);
    }

    fn set_user(&self, user: &str, hash: &str) {
        self.users
            .lock()
            .unwrap()
            .insert((ACCOUNT.to_owned(), user.to_owned()), hash.to_owned());
    }

    fn delete_user(&self, user: &str) {
        self.users
            .lock()
            .unwrap()
            .remove(&(ACCOUNT.to_owned(), user.to_owned()));
    }

    fn answer(&self, hash: Option<String>) -> OpResult<Option<String>> {
        if self.failing.load(Ordering::SeqCst) {
            Err(OpError::Internal("Database error".to_owned()))
        } else {
            Ok(hash)
        }
    }
}

impl PasswordHashStore for FakeCatalog {
    fn get_admin_password_hash(&self, admin_name: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let hash = self.admins.lock().unwrap().get(admin_name).cloned();
        let answer = self.answer(hash);
        Box::pin(async move { answer })
    }

    fn get_user_password_hash(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let key = (account_id.to_owned(), user_name.to_owned());
        let hash = self.users.lock().unwrap().get(&key).cloned();
        let answer = self.answer(hash);
        Box::pin(async move { answer })
    }
}

/// Log an admin in: record its hash in the catalog and create a session
/// bound to that hash.
async fn admin_session(
    store: &SessionStore,
    catalog: &FakeCatalog,
    name: &str,
    hash: &str,
) -> String {
    catalog.set_admin(name, hash);
    let identity = CallerIdentity::Admin(name.to_owned());
    store.create(identity, CredentialStamp::of(hash)).await.0
}

async fn user_session(
    store: &SessionStore,
    catalog: &FakeCatalog,
    user: &str,
    hash: &str,
) -> String {
    catalog.set_user(user, hash);
    let identity = CallerIdentity::IamUser {
        account_id: ACCOUNT.to_owned(),
        user_name: user.to_owned(),
    };
    store.create(identity, CredentialStamp::of(hash)).await.0
}

async fn admin_name_of(store: &SessionStore, catalog: &FakeCatalog, token: &str) -> Option<String> {
    match store.validate(token, catalog).await {
        SessionCheck::Valid {
            identity: CallerIdentity::Admin(name),
            ..
        } => Some(name),
        _ => None,
    }
}

async fn is_invalid(store: &SessionStore, catalog: &FakeCatalog, token: &str) -> bool {
    matches!(store.validate(token, catalog).await, SessionCheck::Invalid)
}

async fn is_valid(store: &SessionStore, catalog: &FakeCatalog, token: &str) -> bool {
    matches!(
        store.validate(token, catalog).await,
        SessionCheck::Valid { .. }
    )
}

#[tokio::test]
async fn deleted_admin_session_is_refused_and_other_admins_keep_theirs() {
    let store = SessionStore::new();
    let catalog = FakeCatalog::default();
    let alice = admin_session(&store, &catalog, "alice", "hash-alice").await;
    let bob = admin_session(&store, &catalog, "bob", "hash-bob").await;
    assert_eq!(
        admin_name_of(&store, &catalog, &alice).await.as_deref(),
        Some("alice")
    );

    catalog.delete_admin("alice");

    assert!(is_invalid(&store, &catalog, &alice).await);
    assert_eq!(
        admin_name_of(&store, &catalog, &bob).await.as_deref(),
        Some("bob")
    );
}

#[tokio::test]
async fn ended_session_is_removed_from_the_store() {
    let store = SessionStore::new();
    let catalog = FakeCatalog::default();
    let alice = admin_session(&store, &catalog, "alice", "hash-alice").await;

    catalog.delete_admin("alice");
    assert!(is_invalid(&store, &catalog, &alice).await);

    // Restoring the same hash does not bring the session back.
    catalog.set_admin("alice", "hash-alice");
    assert!(is_invalid(&store, &catalog, &alice).await);
}

#[tokio::test]
async fn admin_password_change_ends_the_old_session_only() {
    let store = SessionStore::new();
    let catalog = FakeCatalog::default();
    let old = admin_session(&store, &catalog, "alice", "hash-1").await;

    catalog.set_admin("alice", "hash-2");
    assert!(is_invalid(&store, &catalog, &old).await);

    let new = store
        .create(
            CallerIdentity::Admin("alice".to_owned()),
            CredentialStamp::of("hash-2"),
        )
        .await
        .0;
    assert!(is_valid(&store, &catalog, &new).await);
}

#[tokio::test]
async fn iam_user_session_ends_on_password_change_and_delete() {
    let store = SessionStore::new();
    let catalog = FakeCatalog::default();
    let first = user_session(&store, &catalog, "carol", "hash-1").await;
    assert!(is_valid(&store, &catalog, &first).await);

    catalog.set_user("carol", "hash-2");
    assert!(is_invalid(&store, &catalog, &first).await);

    let second = user_session(&store, &catalog, "carol", "hash-2").await;
    assert!(is_valid(&store, &catalog, &second).await);
    catalog.delete_user("carol");
    assert!(is_invalid(&store, &catalog, &second).await);
}

#[tokio::test]
async fn catalog_error_refuses_the_request_and_keeps_the_session() {
    let store = SessionStore::new();
    let catalog = FakeCatalog::default();
    let alice = admin_session(&store, &catalog, "alice", "hash-alice").await;

    catalog.failing.store(true, Ordering::SeqCst);
    assert!(matches!(
        store.validate(&alice, &catalog).await,
        SessionCheck::Unavailable
    ));

    catalog.failing.store(false, Ordering::SeqCst);
    assert!(is_valid(&store, &catalog, &alice).await);
}

/// Move a session's last-active time `by` into the past.
async fn backdate(store: &SessionStore, token: &str, by: Duration) -> Instant {
    let then = Instant::now()
        .checked_sub(by)
        .expect("the monotonic clock is past the backdate");
    store
        .sessions
        .lock()
        .await
        .get_mut(token)
        .expect("session exists")
        .last_active = then;
    then
}

async fn last_active(store: &SessionStore, token: &str) -> Instant {
    store.sessions.lock().await[token].last_active
}

#[tokio::test]
async fn only_a_request_that_passes_the_check_counts_as_activity() {
    let store = SessionStore::new();
    let catalog = FakeCatalog::default();
    let alice = admin_session(&store, &catalog, "alice", "hash-alice").await;
    let then = backdate(&store, &alice, Duration::from_secs(60)).await;

    catalog.failing.store(true, Ordering::SeqCst);
    assert!(matches!(
        store.validate(&alice, &catalog).await,
        SessionCheck::Unavailable
    ));
    assert_eq!(last_active(&store, &alice).await, then);

    catalog.failing.store(false, Ordering::SeqCst);
    assert!(is_valid(&store, &catalog, &alice).await);
    assert!(last_active(&store, &alice).await > then + Duration::from_secs(59));
}

#[test]
fn sessions_last_eight_hours_without_activity() {
    assert_eq!(SESSION_TTL, Duration::from_secs(8 * 3600));
    assert_eq!(SessionStore::new().ttl, SESSION_TTL);
}

#[tokio::test]
async fn a_session_idle_longer_than_the_ttl_ends_and_stays_ended() {
    let store = SessionStore::with_ttl(Duration::from_secs(60));
    let catalog = FakeCatalog::default();
    let alice = admin_session(&store, &catalog, "alice", "hash-alice").await;
    let bob = admin_session(&store, &catalog, "bob", "hash-bob").await;
    backdate(&store, &alice, Duration::from_secs(61)).await;
    backdate(&store, &bob, Duration::from_secs(50)).await;

    assert!(is_invalid(&store, &catalog, &alice).await);
    assert!(!store.sessions.lock().await.contains_key(&alice));
    // A later request with the same token does not bring the session back.
    assert!(is_invalid(&store, &catalog, &alice).await);

    assert!(is_valid(&store, &catalog, &bob).await);
}

#[tokio::test]
async fn unknown_token_is_invalid() {
    let store = SessionStore::new();
    let catalog = FakeCatalog::default();
    assert!(is_invalid(&store, &catalog, "no-such-token").await);
}
