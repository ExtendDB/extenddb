// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! How a server reads the `search_inequality_filters` setting: per server,
//! kept for a short interval, bounded in time, and a fault answered as a
//! retryable error.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::search_filter::EnabledAccounts;
use extenddb_storage::StorageEngine;
use extenddb_storage::management_store::{OpError, OpResult, SettingsStore};
use extenddb_storage_sqlite::SqliteEngine;
use futures::future::BoxFuture;

use super::{
    READ_TIMEOUT, REFRESH, Settings, enabled_accounts, install_search_filter_settings,
    search_filter_settings_changed,
};

/// How a read of the store answers.
#[derive(Clone)]
enum Answer {
    Value(Option<String>),
    Fail,
    Hang,
}

/// A settings store with one answer for every read, counting the reads.
struct Store {
    answer: Mutex<Answer>,
    reads: AtomicUsize,
}

impl Store {
    fn new(answer: Answer) -> Arc<Self> {
        Arc::new(Self {
            answer: Mutex::new(answer),
            reads: AtomicUsize::new(0),
        })
    }

    fn value(value: &str) -> Arc<Self> {
        Self::new(Answer::Value(Some(value.to_owned())))
    }

    fn set(&self, answer: Answer) {
        *self.answer.lock().unwrap() = answer;
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

impl SettingsStore for Store {
    fn get_setting(&self, _key: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let answer = self.answer.lock().unwrap().clone();
        Box::pin(async move {
            match answer {
                Answer::Value(v) => Ok(v),
                Answer::Fail => Err(OpError::Internal("connection reset".to_owned())),
                Answer::Hang => std::future::pending().await,
            }
        })
    }

    fn set_setting(&self, _key: &str, _value: &str) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

async fn storage() -> Arc<dyn StorageEngine> {
    Arc::new(
        SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
            .await
            .expect("an in-memory database opens"),
    )
}

fn is_unavailable(answer: &Result<EnabledAccounts, DynamoDbError>) -> bool {
    matches!(answer, Err(DynamoDbError::ServiceUnavailable(m))
        if m == "Service is temporarily unavailable")
}

#[tokio::test]
async fn a_server_without_a_store_enables_no_account() {
    let storage = storage().await;
    assert_eq!(
        enabled_accounts(&storage).await.unwrap(),
        EnabledAccounts::Off
    );
}

#[tokio::test]
async fn each_server_reads_its_own_store() {
    let (first, second, third) = (storage().await, storage().await, storage().await);
    install_search_filter_settings(&first, Store::value("all"));
    install_search_filter_settings(&second, Store::value("111111111111"));
    assert_eq!(
        enabled_accounts(&first).await.unwrap(),
        EnabledAccounts::All
    );
    assert_eq!(
        enabled_accounts(&second).await.unwrap(),
        EnabledAccounts::parse("111111111111").unwrap()
    );
    assert_eq!(
        enabled_accounts(&third).await.unwrap(),
        EnabledAccounts::Off
    );
    // installing again replaces the server's store
    install_search_filter_settings(&first, Store::value("off"));
    assert_eq!(
        enabled_accounts(&first).await.unwrap(),
        EnabledAccounts::Off
    );
}

#[tokio::test]
async fn a_failed_read_is_a_retryable_fault() {
    let store = Store::new(Answer::Fail);
    let settings = Settings::new(store.clone());
    assert!(is_unavailable(&settings.enabled_accounts().await));
}

#[tokio::test(start_paused = true)]
async fn a_read_that_does_not_finish_is_a_retryable_fault() {
    let store = Store::new(Answer::Hang);
    let settings = Settings::new(store.clone());
    let started = tokio::time::Instant::now();
    assert!(is_unavailable(&settings.enabled_accounts().await));
    assert_eq!(started.elapsed(), READ_TIMEOUT);
}

#[tokio::test(start_paused = true)]
async fn a_read_is_kept_for_the_refresh_interval() {
    let store = Store::value("all");
    let settings = Settings::new(store.clone());
    for _ in 0..5 {
        assert_eq!(
            settings.enabled_accounts().await.unwrap(),
            EnabledAccounts::All
        );
    }
    assert_eq!(store.reads(), 1);

    store.set(Answer::Value(Some("off".to_owned())));
    tokio::time::advance(REFRESH / 2).await;
    assert_eq!(
        settings.enabled_accounts().await.unwrap(),
        EnabledAccounts::All
    );
    tokio::time::advance(REFRESH / 2).await;
    assert_eq!(
        settings.enabled_accounts().await.unwrap(),
        EnabledAccounts::Off
    );
    assert_eq!(store.reads(), 2);

    // a fault is kept as briefly, then the next read recovers
    store.set(Answer::Fail);
    tokio::time::advance(REFRESH).await;
    assert!(is_unavailable(&settings.enabled_accounts().await));
    store.set(Answer::Value(None));
    assert!(is_unavailable(&settings.enabled_accounts().await));
    tokio::time::advance(REFRESH).await;
    assert_eq!(
        settings.enabled_accounts().await.unwrap(),
        EnabledAccounts::Off
    );
    assert_eq!(store.reads(), 4);
}

#[tokio::test(start_paused = true)]
async fn concurrent_requests_share_one_read() {
    let store = Store::value("all");
    let settings = Arc::new(Settings::new(store.clone()));
    let tasks: Vec<_> = (0..16)
        .map(|_| {
            let settings = Arc::clone(&settings);
            tokio::spawn(async move { settings.enabled_accounts().await })
        })
        .collect();
    for task in tasks {
        assert_eq!(task.await.unwrap().unwrap(), EnabledAccounts::All);
    }
    assert_eq!(store.reads(), 1);
}

#[tokio::test]
async fn a_change_through_the_management_api_applies_at_once() {
    let storage = storage().await;
    let store = Store::value("off");
    install_search_filter_settings(&storage, store.clone());
    assert_eq!(
        enabled_accounts(&storage).await.unwrap(),
        EnabledAccounts::Off
    );
    store.set(Answer::Value(Some("all".to_owned())));
    search_filter_settings_changed();
    assert_eq!(
        enabled_accounts(&storage).await.unwrap(),
        EnabledAccounts::All
    );
}
