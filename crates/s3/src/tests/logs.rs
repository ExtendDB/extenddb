// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A subscriber that keeps the events logged on the current thread, for
//! the tests that assert a log line's level, target and fields.

use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

/// One logged event: its level, its target and its fields, `message`
/// included.
#[derive(Clone, Debug)]
pub(super) struct Logged {
    pub(super) level: Level,
    pub(super) target: String,
    fields: Vec<(String, String)>,
}

impl Logged {
    pub(super) fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(field, _)| field == name)
            .map(|(_, value)| value.as_str())
    }

    pub(super) fn message(&self) -> &str {
        self.field("message").unwrap_or_default()
    }
}

/// The events logged while the guard [`Capture::install`] returns lives.
#[derive(Clone, Default)]
pub(super) struct Capture(Arc<Mutex<Vec<Logged>>>);

impl Capture {
    /// Install on the current thread until the guard drops. A test that
    /// uses it runs on a current-thread runtime, so the store's calls log
    /// on this thread.
    pub(super) fn install() -> (Self, tracing::dispatcher::DefaultGuard) {
        let capture = Self::default();
        let guard = tracing::dispatcher::set_default(&tracing::Dispatch::new(capture.clone()));
        (capture, guard)
    }

    /// The events this crate logged, the SDK's own left out.
    pub(super) fn ours(&self) -> Vec<Logged> {
        self.events()
            .into_iter()
            .filter(|event| event.target.starts_with("extenddb_s3"))
            .collect()
    }

    pub(super) fn events(&self) -> Vec<Logged> {
        self.0.lock().unwrap().clone()
    }
}

struct Fields<'a>(&'a mut Vec<(String, String)>);

impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.push((field.name().to_owned(), format!("{value:?}")));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push((field.name().to_owned(), value.to_owned()));
    }
}

impl Subscriber for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = Vec::new();
        event.record(&mut Fields(&mut fields));
        self.0.lock().unwrap().push(Logged {
            level: *event.metadata().level(),
            target: event.metadata().target().to_owned(),
            fields,
        });
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}
