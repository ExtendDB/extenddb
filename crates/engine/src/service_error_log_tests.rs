// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The `StorageError::Service` contract at the engine functions that map a
//! `StorageError` to a client error: the client gets the error class and the
//! message the backend chose, never the item of a conditional check error, and
//! the error is not logged as a storage fault, while an unclassified error
//! still is. The three mappings that only delegate to these and are private to
//! their modules (the create-backup, restore and shard lookup mappings) are
//! checked there with [`rows`].

use std::sync::{Arc, Mutex};

use extenddb_core::error::{DynamoDbError, error_document};
use extenddb_core::types::{AttributeValue, Item, ReturnValuesOnConditionCheckFailure};
use extenddb_storage::error::StorageError;
use serde_json::{Value, json};
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

/// Runs `f` with a fresh subscriber as this thread's default and returns how
/// many error events it logged. The global default of `test_tracing` keeps
/// `tracing` from caching a callsite that another thread met first as
/// "never", which would hide its events from this subscriber.
fn error_events_during(f: impl FnOnce()) -> usize {
    crate::test_tracing::interest_in_every_callsite();
    let errors = Errors::default();
    let count = Arc::clone(&errors.0);
    let subscriber = tracing_subscriber::registry().with(errors);
    let _guard = tracing::subscriber::set_default(subscriber);
    f();
    *count.lock().unwrap()
}

/// Backend-classified errors, each with a message unlike any the engine
/// writes, and the body the client gets for each.
pub(crate) fn rows() -> Vec<(DynamoDbError, Value)> {
    let mut stored = Item::new();
    stored.insert("pk".to_owned(), AttributeValue::S("stored".to_owned()));
    let body = |class: &str, message: &str| {
        json!({
            "__type": format!("com.amazonaws.dynamodb.v20120810#{class}"),
            "message": message,
        })
    };
    let unavailable = "The database is temporarily unavailable, retry shortly.";
    let internal = "The server encountered an internal error trying to fulfill the request.";
    let capacity = "The level of configured provisioned throughput for the table was exceeded.";
    let limit = "Too many operations are in progress for this account.";
    let condition = "The stored item does not match the condition.";
    vec![
        (
            DynamoDbError::ServiceUnavailable(unavailable.to_owned()),
            body("ServiceUnavailable", unavailable),
        ),
        (
            DynamoDbError::InternalServerError(internal.to_owned()),
            body("InternalServerError", internal),
        ),
        (
            DynamoDbError::provisioned_throughput_exceeded(capacity, vec![]),
            body("ProvisionedThroughputExceededException", capacity),
        ),
        (
            DynamoDbError::LimitExceededException(limit.to_owned()),
            body("LimitExceededException", limit),
        ),
        // The stored item stays out of the body: only a request that set
        // ReturnValuesOnConditionCheckFailure to ALL_OLD gets it.
        (
            DynamoDbError::ConditionalCheckFailedException(condition.to_owned(), Some(stored)),
            body("ConditionalCheckFailedException", condition),
        ),
    ]
}

type Site = (&'static str, fn(StorageError) -> DynamoDbError);

/// The engine functions that map a `StorageError` to a client error and are
/// reachable from here: the six that log a storage fault (five end in a
/// catch-all; the data path mapping names every variant) and the conditional
/// check mapping, which maps the engine's own `ConditionFailed` and, under
/// `ALL_OLD`, a backend's conditional check error itself, and delegates the
/// rest to the data path mapping.
fn sites() -> Vec<Site> {
    vec![
        (
            "create_table::storage_err_to_dynamo",
            crate::create_table::storage_err_to_dynamo,
        ),
        ("create_table::storage_err_to_dynamo_with_ccf", |e| {
            crate::create_table::storage_err_to_dynamo_with_ccf(
                e,
                ReturnValuesOnConditionCheckFailure::None,
            )
        }),
        (
            "backup_requests::storage_err_to_dynamo",
            crate::backup_requests::storage_err_to_dynamo,
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
fn a_service_error_reaches_the_client_as_classified_and_is_not_logged_at_any_site() {
    for (name, map) in sites() {
        for (error, body) in rows() {
            let mut got = Value::Null;
            let logged = error_events_during(|| {
                got = error_document(&map(StorageError::Service(error.clone())));
            });
            assert_eq!(got, body, "{name}: {error:?}");
            assert_eq!(logged, 0, "{name} logged {error:?} as a storage fault");
        }
    }
}

// Control: the same sites still redact an unclassified failure behind the
// fixed text and log it once.
#[test]
fn an_internal_error_is_redacted_and_logged_once_at_every_site() {
    for (name, map) in sites() {
        let mut got = Value::Null;
        let logged = error_events_during(|| {
            got = error_document(&map(StorageError::Internal("driver text".to_owned())));
        });
        assert_eq!(
            got,
            json!({
                "__type": "com.amazonaws.dynamodb.v20120810#InternalServerError",
                "message": "Internal server error",
            }),
            "{name}"
        );
        assert_eq!(
            logged, 1,
            "{name} logged {logged} fault events for an Internal error"
        );
    }
}

/// An event reaches the counting subscriber when another thread, without a
/// subscriber of its own, met its callsite first while the counting
/// subscriber was the only thread default in the process. The callsite is
/// this test's own, so no other test has met it before.
#[test]
fn an_event_from_a_callsite_another_thread_met_first_is_counted() {
    fn fault() {
        tracing::error!("a fault that two threads log");
    }
    let logged = error_events_during(|| {
        std::thread::spawn(fault).join().unwrap();
        fault();
    });
    assert_eq!(logged, 1);
}
