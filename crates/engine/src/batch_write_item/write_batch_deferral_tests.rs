// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `write_batch` against a `DataEngine` double: what a write that cannot be
//! applied now leaves behind, and the answer when no request could be applied.

use serde_json::json;

use super::scripted::{Scripted, answered, found, put, rejected, run, run_body};
use super::*;

fn transient() -> StorageError {
    StorageError::Transient("lost every attempt".to_owned())
}

fn conflict() -> StorageError {
    StorageError::TransactionConflict("held".to_owned())
}

/// A failed request does not stop the ones after it. The service answered 200
/// with only the held item's request unprocessed and the other request applied
/// whichever came first (the neighbouring test has the held item second).
#[tokio::test]
async fn a_failed_first_request_does_not_stop_the_rest() {
    let data = Scripted::new(vec![Err(conflict()), Ok(())]);
    let items = json!({"t": [put("held"), put("side")]});

    let out = answered(run(&data, items, &["t"], found(&["t"])).await);

    assert_eq!(out.body["UnprocessedItems"], json!({"t": [put("held")]}));
    assert_eq!(data.writes(), ["t/held", "t/side"]);
}

/// UnprocessedItems holds exactly the requests that failed, per table, and the
/// consumed capacity counts the requests that applied.
#[tokio::test]
async fn unprocessed_items_hold_only_the_failed_requests_and_capacity_the_applied_ones() {
    let data = Scripted::new(vec![Ok(()), Err(transient()), Err(conflict()), Ok(())]);
    let body = json!({
        "RequestItems": {"t1": [put("a"), put("b")], "t2": [put("c"), put("d")]},
        "ReturnConsumedCapacity": "TOTAL",
    });

    let out = answered(run_body(&data, body, &["t1", "t2"], found(&["t1", "t2"])).await);

    assert_eq!(
        out.body["UnprocessedItems"],
        json!({"t1": [put("b")], "t2": [put("c")]})
    );
    assert_eq!(data.writes(), ["t1/a", "t1/b", "t2/c", "t2/d"]);
    let mut units: Vec<(String, f64)> = out.body["ConsumedCapacity"]
        .as_array()
        .expect("consumed capacity was requested")
        .iter()
        .map(|c| {
            (
                c["TableName"].as_str().unwrap().to_owned(),
                c["CapacityUnits"].as_f64().unwrap(),
            )
        })
        .collect();
    units.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(units, [("t1".to_owned(), 1.0), ("t2".to_owned(), 1.0)]);
    assert!((out.metrics.write_capacity_units - 2.0).abs() < f64::EPSILON);
}

/// A batch of which no request applied is an error, not a 200 whose client
/// replays it at full speed. Every request is tried first. The service
/// answered 500 InternalServerError for such a batch; this server answers 503,
/// which the SDKs retry with backoff.
#[tokio::test]
async fn a_batch_where_nothing_applied_answers_503_after_trying_every_request() {
    for failure in [transient(), conflict()] {
        let data = Scripted::new(vec![Err(failure.clone()), Err(failure.clone())]);
        let items = json!({"t": [put("a"), put("b")]});

        let err = rejected(run(&data, items, &["t"], found(&["t"])).await);

        assert!(
            matches!(err, DynamoDbError::ServiceUnavailable(_)),
            "{failure:?} gave {err:?}"
        );
        assert_eq!(err.status_code(), 503);
        assert_eq!(data.writes(), ["t/a", "t/b"], "{failure:?}");
    }
}

/// A connection error ends the attempt: the requests after it would wait on
/// the same unreachable backend, so they are left for the client.
#[tokio::test]
async fn a_connection_error_leaves_the_rest_of_the_batch_unattempted() {
    let down = StorageError::Connection("refused".to_owned());
    let data = Scripted::new(vec![Ok(()), Err(down)]);
    let items = json!({"t1": [put("a"), put("b"), put("c")], "t2": [put("d")]});

    let out = answered(run(&data, items, &["t1", "t2"], found(&["t1", "t2"])).await);

    assert_eq!(
        out.body["UnprocessedItems"],
        json!({"t1": [put("b"), put("c")], "t2": [put("d")]})
    );
    assert_eq!(data.writes(), ["t1/a", "t1/b"]);
}

/// A connection error with nothing applied answers 503 after one attempt.
#[tokio::test]
async fn a_connection_error_with_nothing_applied_answers_503_after_one_attempt() {
    let down = StorageError::Connection("refused".to_owned());
    let data = Scripted::new(vec![Err(down)]);
    let items = json!({"t": [put("a"), put("b")]});

    let err = rejected(run(&data, items, &["t"], found(&["t"])).await);

    assert_eq!(err.status_code(), 503, "{err:?}");
    assert_eq!(data.writes(), ["t/a"]);
}

/// An error a replay would not clear fails the batch with 500, also after an
/// earlier request was left unprocessed.
#[tokio::test]
async fn an_internal_error_after_a_failed_request_fails_the_batch() {
    let broken = StorageError::Internal("corrupt row".to_owned());
    let data = Scripted::new(vec![Err(transient()), Err(broken)]);
    let items = json!({"t": [put("a"), put("b")]});

    let err = rejected(run(&data, items, &["t"], found(&["t"])).await);

    assert!(
        matches!(err, DynamoDbError::InternalServerError(_)),
        "{err:?}"
    );
    assert_eq!(err.status_code(), 500);
    assert_eq!(data.writes(), ["t/a", "t/b"]);
}
