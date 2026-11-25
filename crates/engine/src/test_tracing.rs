// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The process's global `tracing` subscriber for the tests that read log
//! events through a subscriber set as their thread's default.

/// The process's global `tracing` subscriber while the tests run:
/// interested in every callsite and enabled for none. `tracing` caches each
/// callsite's interest the first time any thread reaches it; while exactly
/// one scoped subscriber is registered in the process, that cache is
/// computed from the default of the thread that got there first. A test
/// without a subscriber running beside a test with one on another thread
/// would so cache a callsite as "never", and the test with the subscriber
/// would then see no event from it (tokio-rs/tracing#2743). With this
/// global default the cache reads "sometimes" instead, and each event asks
/// the thread's own default.
struct InterestedInEveryCallsite;

impl tracing::Subscriber for InterestedInEveryCallsite {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        false
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

/// Install [`InterestedInEveryCallsite`] as the process's global default,
/// once. A test calls this before it sets its own subscriber as its thread's
/// default.
pub(crate) fn interest_in_every_callsite() {
    static GLOBAL: std::sync::Once = std::sync::Once::new();
    GLOBAL.call_once(|| {
        // Another global may already be set by a test outside these
        // modules; then its interest rule applies and this one is moot.
        let _ = tracing::subscriber::set_global_default(InterestedInEveryCallsite);
    });
}
