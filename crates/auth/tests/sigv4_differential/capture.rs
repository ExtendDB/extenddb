// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The canonical request `aws-sigv4` builds.
//!
//! `aws-sigv4` keeps its canonical request private and records it in a
//! trace-level event, in the field `canonical_request`. The subscriber here
//! keeps that field's text while a closure runs on the current thread.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

/// Run `sign` and return its result with the canonical request `aws-sigv4`
/// recorded while it ran, if it recorded one.
pub fn canonical_request<T>(sign: impl FnOnce() -> T) -> (T, Option<String>) {
    let recorded = Arc::new(Mutex::new(None));
    let recorder = Recorder {
        canonical_request: Arc::clone(&recorded),
    };
    let output = tracing::subscriber::with_default(recorder, sign);
    let text = recorded
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take();
    (output, text)
}

/// Keeps the last `canonical_request` field an `aws-sigv4` event carries.
struct Recorder {
    canonical_request: Arc<Mutex<Option<String>>>,
}

impl Subscriber for Recorder {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target().starts_with("aws_sigv4")
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut field = CanonicalRequestField(None);
        event.record(&mut field);
        if let Some(text) = field.0 {
            *self
                .canonical_request
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(text);
        }
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

/// The text of an event's `canonical_request` field.
struct CanonicalRequestField(Option<String>);

impl Visit for CanonicalRequestField {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "canonical_request" {
            // The field is recorded with `%`, so its Debug output is the
            // Display text of the canonical request.
            self.0 = Some(format!("{value:?}"));
        }
    }
}
