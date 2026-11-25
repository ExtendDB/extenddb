// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The setting through a poisoned lock, and the log line of a malformed
//! value.

use super::*;

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

/// A thread that panicked while holding the server list, a server's kept
/// read, its change count or its last value read leaves the setting
/// readable, and a failed read still answers the last value read.
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
    poison(&settings.last);
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
    store.set(Answer::Fail);
    search_filter_settings_changed();
    assert_eq!(
        enabled_accounts(&storage).await.unwrap(),
        EnabledAccounts::Off,
        "a failed read answers the last value read"
    );
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
