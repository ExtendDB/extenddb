// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! How a server reads the `search_inequality_filters` setting: per server,
//! kept for a short interval, bounded in time, one read at a time, and a
//! fault answered as a retryable error.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use extenddb_core::error::DynamoDbError;
use extenddb_core::expression::search_filter::EnabledAccounts;
use extenddb_storage::StorageEngine;
use extenddb_storage::management_store::{OpError, OpResult, SettingsStore};
use extenddb_storage_sqlite::SqliteEngine;
use futures::future::BoxFuture;
use tokio::sync::Notify;

use super::{
    READ_TIMEOUT, REFRESH, SERVERS, Settings, enabled_accounts, install_search_filter_settings,
    same, search_filter_settings_changed,
};

/// How a read of the store answers.
#[derive(Clone)]
enum Answer {
    Value(Option<String>),
    /// The value, after a delay that lets concurrent readers interleave.
    Slow(String),
    Fail,
    Hang,
    /// The value, once the gate is opened.
    Gated(Arc<Notify>, String),
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
                Answer::Slow(v) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    Ok(Some(v))
                }
                Answer::Fail => Err(OpError::Internal("connection reset".to_owned())),
                Answer::Hang => std::future::pending().await,
                Answer::Gated(gate, v) => {
                    gate.notified().await;
                    Ok(Some(v))
                }
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
async fn a_server_without_a_store_enables_every_account() {
    let storage = storage().await;
    assert_eq!(
        enabled_accounts(&storage).await.unwrap(),
        EnabledAccounts::All
    );
}

#[tokio::test]
async fn an_absent_or_empty_value_enables_every_account() {
    for answer in [Answer::Value(None), Answer::Value(Some(String::new()))] {
        let settings = Settings::new(Store::new(answer));
        assert_eq!(
            settings.enabled_accounts().await.unwrap(),
            EnabledAccounts::All
        );
    }
}

#[tokio::test]
async fn off_and_a_list_still_restrict_the_grammar() {
    let settings = Settings::new(Store::value("off"));
    assert_eq!(
        settings.enabled_accounts().await.unwrap(),
        EnabledAccounts::Off
    );
    let settings = Settings::new(Store::value("111111111111"));
    let listed = settings.enabled_accounts().await.unwrap();
    assert!(listed.enabled_for("111111111111"));
    assert!(!listed.enabled_for("222222222222"));
}

/// A malformed value enables no account and is logged once per change.
#[test]
fn a_malformed_value_is_logged_once_and_enables_no_account() {
    let (accounts, logged) = Settings::parse(Some("on"), None);
    assert_eq!(accounts, EnabledAccounts::Off);
    assert_eq!(
        logged.as_deref(),
        Some(
            "search_inequality_filters = \"on\" enables no account: must be empty, 'off', \
             'all', or a comma-separated list of 12-digit account IDs"
        )
    );
    assert_eq!(
        Settings::parse(Some("on"), Some("on")),
        (EnabledAccounts::Off, None)
    );
    assert!(Settings::parse(Some("on"), Some("off")).1.is_some());
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
        EnabledAccounts::All
    );
    // installing again replaces the server's store
    install_search_filter_settings(&first, Store::value("off"));
    assert_eq!(
        enabled_accounts(&first).await.unwrap(),
        EnabledAccounts::Off
    );
}

/// Before any read has answered, a read that fails is a retryable fault.
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
    // the test's own deadline, so a read without a bound fails here
    let answer = tokio::time::timeout(READ_TIMEOUT * 2, settings.enabled_accounts())
        .await
        .expect("the read is bounded");
    assert!(is_unavailable(&answer));
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

    // a fault keeps the last value read as briefly, then the next read
    // recovers
    store.set(Answer::Fail);
    tokio::time::advance(REFRESH).await;
    assert_eq!(
        settings.enabled_accounts().await.unwrap(),
        EnabledAccounts::Off
    );
    store.set(Answer::Value(None));
    assert_eq!(
        settings.enabled_accounts().await.unwrap(),
        EnabledAccounts::Off
    );
    tokio::time::advance(REFRESH).await;
    assert_eq!(
        settings.enabled_accounts().await.unwrap(),
        EnabledAccounts::All
    );
    assert_eq!(store.reads(), 4);
}

#[tokio::test(start_paused = true)]
async fn concurrent_requests_share_one_read() {
    let store = Store::new(Answer::Slow("all".to_owned()));
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

/// A read that does not end is sent once: later requests wait on it rather
/// than sending reads of their own, each of which would hold a catalog
/// connection while it waits.
#[tokio::test(start_paused = true)]
async fn a_read_that_does_not_end_is_sent_once() {
    let store = Store::new(Answer::Hang);
    let settings = Arc::new(Settings::new(store.clone()));
    for _ in 0..20 {
        let tasks: Vec<_> = (0..4)
            .map(|_| {
                let settings = Arc::clone(&settings);
                tokio::spawn(async move { settings.enabled_accounts().await })
            })
            .collect();
        for task in tasks {
            assert!(is_unavailable(&task.await.unwrap()));
        }
        tokio::time::advance(REFRESH).await;
    }
    assert_eq!(store.reads(), 1, "reads sent while the first had not ended");
}

/// When the read that outlasted the timeout ends, the next request uses its
/// answer.
#[tokio::test(start_paused = true)]
async fn a_read_that_ends_late_answers_the_next_request() {
    let gate = Arc::new(Notify::new());
    let store = Store::new(Answer::Gated(Arc::clone(&gate), "all".to_owned()));
    let settings = Settings::new(store.clone());
    assert!(is_unavailable(&settings.enabled_accounts().await));
    tokio::time::advance(REFRESH).await;
    gate.notify_one();
    let started = tokio::time::Instant::now();
    assert_eq!(
        settings.enabled_accounts().await.unwrap(),
        EnabledAccounts::All
    );
    assert!(started.elapsed() < READ_TIMEOUT);
    assert_eq!(store.reads(), 1);
}

/// A read sent before a change through the management API is not waited on
/// after it: the next request sends a read of the changed value.
#[tokio::test(start_paused = true)]
async fn a_change_applies_while_an_earlier_read_has_not_ended() {
    let store = Store::new(Answer::Hang);
    let settings = Settings::new(store.clone());
    assert!(is_unavailable(&settings.enabled_accounts().await));
    store.set(Answer::Value(Some("all".to_owned())));
    settings.changed();
    assert_eq!(
        settings.enabled_accounts().await.unwrap(),
        EnabledAccounts::All
    );
    assert_eq!(store.reads(), 2);
}

/// A read that fails, after an earlier read answered, uses the value of the
/// earlier read, a change through the management API notwithstanding.
#[tokio::test(start_paused = true)]
async fn a_failed_read_uses_the_last_value_read() {
    let store = Store::value("off");
    let settings = Settings::new(store.clone());
    assert_eq!(
        settings.enabled_accounts().await.unwrap(),
        EnabledAccounts::Off
    );
    store.set(Answer::Fail);
    tokio::time::advance(REFRESH).await;
    assert_eq!(
        settings.enabled_accounts().await.unwrap(),
        EnabledAccounts::Off
    );
    settings.changed();
    assert_eq!(
        settings.enabled_accounts().await.unwrap(),
        EnabledAccounts::Off
    );
    assert_eq!(store.reads(), 3);
}

/// A read that does not end, after an earlier read answered, is waited on
/// once for READ_TIMEOUT. Later requests use the earlier value at once while
/// it has not ended, and send no read of their own.
#[tokio::test(start_paused = true)]
async fn a_read_that_does_not_end_uses_the_last_value_read() {
    let store = Store::value("off");
    let settings = Settings::new(store.clone());
    assert_eq!(
        settings.enabled_accounts().await.unwrap(),
        EnabledAccounts::Off
    );
    store.set(Answer::Hang);
    tokio::time::advance(REFRESH).await;
    let started = tokio::time::Instant::now();
    // the test's own deadline, so a read without a bound fails here
    let answer = tokio::time::timeout(READ_TIMEOUT * 2, settings.enabled_accounts())
        .await
        .expect("the read is bounded");
    assert_eq!(answer.unwrap(), EnabledAccounts::Off);
    assert_eq!(started.elapsed(), READ_TIMEOUT);
    for _ in 0..5 {
        tokio::time::advance(REFRESH).await;
        let started = tokio::time::Instant::now();
        assert_eq!(
            settings.enabled_accounts().await.unwrap(),
            EnabledAccounts::Off
        );
        assert_eq!(started.elapsed(), std::time::Duration::ZERO);
    }
    assert_eq!(store.reads(), 2);
}

/// A request does not wait on a read another request is waiting on when an
/// earlier read answered: it uses the earlier value, and the waiting request
/// gets the new one.
#[tokio::test(start_paused = true)]
async fn a_request_does_not_wait_on_a_read_in_flight_once_a_value_was_read() {
    let store = Store::value("off");
    let settings = Arc::new(Settings::new(store.clone()));
    assert_eq!(
        settings.enabled_accounts().await.unwrap(),
        EnabledAccounts::Off
    );
    let gate = Arc::new(Notify::new());
    store.set(Answer::Gated(Arc::clone(&gate), "all".to_owned()));
    tokio::time::advance(REFRESH).await;
    let waiting = {
        let settings = Arc::clone(&settings);
        tokio::spawn(async move { settings.enabled_accounts().await })
    };
    while store.reads() < 2 {
        tokio::task::yield_now().await;
    }
    let started = tokio::time::Instant::now();
    // the test's own deadline, so a request that waits on the read fails here
    let answer = tokio::time::timeout(READ_TIMEOUT / 2, settings.enabled_accounts())
        .await
        .expect("the request does not wait on the read");
    assert_eq!(answer.unwrap(), EnabledAccounts::Off);
    assert_eq!(started.elapsed(), std::time::Duration::ZERO);
    gate.notify_one();
    assert_eq!(waiting.await.unwrap().unwrap(), EnabledAccounts::All);
    assert_eq!(store.reads(), 2);
}

/// Poison `lock` with a thread that panics while holding it.
fn poison<T: Send>(lock: &Mutex<T>) {
    std::thread::scope(|s| {
        let held = s.spawn(|| {
            let _held = lock.lock();
            panic!("poison the lock");
        });
        assert!(held.join().is_err());
    });
    assert!(lock.is_poisoned());
}

/// A thread that panicked while holding the server list or a server's kept
/// read leaves the setting readable.
#[tokio::test]
async fn a_poisoned_lock_leaves_the_setting_readable() {
    let storage = storage().await;
    let store = Store::value("all");
    install_search_filter_settings(&storage, store.clone());
    let settings = SERVERS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .iter()
        .find(|(engine, _)| same(engine, &storage))
        .map(|(_, settings)| Arc::clone(settings))
        .expect("the server is installed");
    poison(&SERVERS);
    poison(&settings.kept);
    poison(&settings.changes);
    assert_eq!(
        enabled_accounts(&storage).await.unwrap(),
        EnabledAccounts::All
    );
    assert_eq!(
        enabled_accounts(&storage).await.unwrap(),
        EnabledAccounts::All
    );
    assert_eq!(store.reads(), 1, "the kept read is used");
    store.set(Answer::Value(Some("off".to_owned())));
    search_filter_settings_changed();
    assert_eq!(
        enabled_accounts(&storage).await.unwrap(),
        EnabledAccounts::Off
    );
    assert_eq!(store.reads(), 2);
}

/// The warnings this thread's subscriber receives.
#[derive(Clone, Default)]
struct Warnings(Arc<Mutex<Vec<String>>>);

struct Message<'a>(&'a mut String);

impl tracing::field::Visit for Message<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            *self.0 = format!("{value:?}");
        }
    }
}

impl tracing::Subscriber for Warnings {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::WARN
    }
    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut message = String::new();
        event.record(&mut Message(&mut message));
        self.0.lock().unwrap().push(message);
    }
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
}

/// A server that reads a malformed value logs it once, and again after the
/// value changes to another malformed one.
#[tokio::test(start_paused = true)]
async fn a_server_logs_a_malformed_value_once_per_change() {
    let warnings = Warnings::default();
    let _guard = tracing::subscriber::set_default(warnings.clone());
    let store = Store::value("on");
    let settings = Settings::new(store.clone());
    for _ in 0..3 {
        assert_eq!(
            settings.enabled_accounts().await.unwrap(),
            EnabledAccounts::Off
        );
        tokio::time::advance(REFRESH).await;
    }
    store.set(Answer::Value(Some("true".to_owned())));
    tokio::time::advance(REFRESH).await;
    assert_eq!(
        settings.enabled_accounts().await.unwrap(),
        EnabledAccounts::Off
    );
    assert_eq!(store.reads(), 4);
    let reason = "must be empty, 'off', 'all', or a comma-separated list of 12-digit account IDs";
    assert_eq!(
        *warnings.0.lock().unwrap(),
        [
            format!("search_inequality_filters = \"on\" enables no account: {reason}"),
            format!("search_inequality_filters = \"true\" enables no account: {reason}"),
        ]
    );
}
