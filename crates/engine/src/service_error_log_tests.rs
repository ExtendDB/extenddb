// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The second half of the `StorageError::Service` contract: a backend-classified
//! error is not logged as a storage fault, while an unclassified one still is.
//! One subscriber counts error-level events around each engine mapping site
//! that has its own catch-all.

use std::sync::{Arc, Mutex};

use extenddb_core::error::DynamoDbError;
use extenddb_storage::error::StorageError;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

/// Counts error-level events.
#[derive(Default)]
struct Errors(Arc<Mutex<usize>>);

impl<S: Subscriber> Layer<S> for Errors {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        if *event.metadata().level() == Level::ERROR {
            *self.0.lock().unwrap() += 1;
        }
    }
}

/// Runs `f` under a fresh subscriber and returns how many error events it logged.
fn error_events_during(f: impl FnOnce()) -> usize {
    let errors = Errors::default();
    let count = Arc::clone(&errors.0);
    let subscriber = tracing_subscriber::registry().with(errors);
    let _guard = tracing::subscriber::set_default(subscriber);
    f();
    *count.lock().unwrap()
}

fn service() -> StorageError {
    StorageError::Service(DynamoDbError::ServiceUnavailable(
        "retry shortly".to_owned(),
    ))
}

fn internal() -> StorageError {
    StorageError::Internal("driver text".to_owned())
}

type Site = (&'static str, fn(StorageError) -> DynamoDbError);

/// Every engine function that maps a `StorageError` and owns a catch-all arm.
fn sites() -> Vec<Site> {
    vec![
        (
            "create_table::storage_err_to_dynamo",
            crate::create_table::storage_err_to_dynamo,
        ),
        (
            "backup::storage_err_to_dynamo",
            crate::backup::storage_err_to_dynamo,
        ),
        ("update_table::update_table_err_to_dynamo", |e| {
            crate::update_table::update_table_err_to_dynamo(e, "t")
        }),
        (
            "streams::storage_to_dynamo",
            crate::streams::storage_to_dynamo,
        ),
        ("ttl::storage_to_dynamo", crate::ttl::storage_to_dynamo),
        ("sanitize_storage_error", crate::sanitize_storage_error),
    ]
}

#[test]
fn a_service_error_is_not_logged_as_a_storage_fault_at_any_site() {
    for (name, map) in sites() {
        let logged = error_events_during(|| {
            let err = map(service());
            assert!(
                matches!(err, DynamoDbError::ServiceUnavailable(_)),
                "{name}: expected ServiceUnavailable, got {err:?}"
            );
        });
        assert_eq!(logged, 0, "{name} logged a Service error as a fault");
    }
}

// Control: the same sites still log an unclassified failure once.
#[test]
fn an_internal_error_is_logged_once_as_a_storage_fault_at_every_site() {
    for (name, map) in sites() {
        let logged = error_events_during(|| {
            let err = map(internal());
            assert!(
                matches!(err, DynamoDbError::InternalServerError(_)),
                "{name}: expected InternalServerError, got {err:?}"
            );
        });
        assert_eq!(
            logged, 1,
            "{name} logged {logged} fault events for an Internal error"
        );
    }
}
