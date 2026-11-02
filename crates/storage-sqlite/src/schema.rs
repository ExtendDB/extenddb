// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Catalog schema helpers.
//!
//! The catalog schema itself is created and upgraded by the migrations in
//! `migrations/` (migration 1 is `migrations/0001_baseline.sql`). Table and
//! column names are kept identical to PostgreSQL so the catalog and
//! management stores stay portable across backends. Per-DynamoDB-table data
//! tables (`_ddb_<table_id>`) are created by `create_table`, as in the
//! PostgreSQL backend.

use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::SqlitePool;

/// Return whether a table with the given name exists in the catalog.
pub async fn table_exists(pool: &SqlitePool, name: &str) -> OpResult<bool> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("table_exists({name}): {e}")))?;
    Ok(exists)
}

/// Collect the `tracing` events emitted on the current thread, for tests
/// that assert an operator-facing log line. Each entry is `"<LEVEL> <message>"`.
///
/// One process-wide subscriber, installed on first use, forwards each event
/// to the buffer of the thread that emitted it, if that thread is capturing.
/// A global subscriber rather than `set_default`: callsite interest is cached
/// process-wide, and with per-thread subscribers a test on another thread
/// could leave a callsite cached as disabled.
#[cfg(test)]
pub(crate) mod log_capture {
    use std::cell::RefCell;
    use std::sync::{Arc, Mutex};

    type Lines = Arc<Mutex<Vec<String>>>;

    thread_local! {
        static CAPTURING: RefCell<Option<Lines>> = const { RefCell::new(None) };
    }

    struct Forward;

    struct Message(String);

    impl tracing::field::Visit for Message {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0 = format!("{value:?}");
            }
        }
    }

    impl tracing::Subscriber for Forward {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            CAPTURING.with(|c| {
                if let Some(lines) = c.borrow().as_ref() {
                    let mut message = Message(String::new());
                    event.record(&mut message);
                    if let Ok(mut lines) = lines.lock() {
                        lines.push(format!("{} {}", event.metadata().level(), message.0));
                    }
                }
            });
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// Stops capturing on drop.
    pub(crate) struct Guard;

    impl Drop for Guard {
        fn drop(&mut self) {
            CAPTURING.with(|c| c.borrow_mut().take());
        }
    }

    /// Capture this thread's events until the guard is dropped. Use it from a
    /// current-thread runtime (the `#[tokio::test]` default).
    pub(crate) fn capture() -> (Guard, Lines) {
        static INSTALL: std::sync::Once = std::sync::Once::new();
        INSTALL.call_once(|| {
            tracing::subscriber::set_global_default(Forward)
                .expect("no other global subscriber in this test binary");
        });
        let lines = Lines::default();
        CAPTURING.with(|c| *c.borrow_mut() = Some(Arc::clone(&lines)));
        (Guard, lines)
    }
}
