// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A subscriber that keeps the events logged on the current thread with
//! their level, target and message, for the tests that pin which handler
//! a log line is filed under.

use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Metadata, Subscriber};

/// One logged event.
#[derive(Clone, Debug)]
pub(super) struct Logged {
    pub(super) level: Level,
    pub(super) target: String,
    pub(super) message: String,
}

/// The events logged while the guard [`Capture::install`] returns lives.
#[derive(Clone, Default)]
pub(super) struct Capture(Arc<Mutex<Vec<Logged>>>);

impl Capture {
    /// Install on the current thread until the guard drops. A test that
    /// uses it runs on the default current-thread runtime, so the handler
    /// logs on this thread.
    pub(super) fn install() -> (Self, tracing::dispatcher::DefaultGuard) {
        let capture = Self::default();
        let guard = tracing::dispatcher::set_default(&tracing::Dispatch::new(capture.clone()));
        (capture, guard)
    }

    /// The ERROR events whose message contains `needle`.
    pub(super) fn errors_containing(&self, needle: &str) -> Vec<Logged> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.level == Level::ERROR && e.message.contains(needle))
            .cloned()
            .collect()
    }
}

struct Message<'a>(&'a mut String);

impl Visit for Message<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            *self.0 = format!("{value:?}");
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            value.clone_into(self.0);
        }
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
        let mut message = String::new();
        event.record(&mut Message(&mut message));
        self.0.lock().unwrap().push(Logged {
            level: *event.metadata().level(),
            target: event.metadata().target().to_owned(),
            message,
        });
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}
