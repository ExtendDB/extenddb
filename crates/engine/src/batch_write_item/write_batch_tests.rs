// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `write_batch` against a `DataEngine` double: what is checked before the
//! first write.

use serde_json::json;

use super::scripted::{
    Scripted, answered, found, put, put_with_wrong_key_type, rejected, run, table,
};
use super::*;

/// The first write cannot be applied now and the second request is invalid.
/// The `BatchWriteItem` reference has the service reject the whole batch for
/// the invalid request, so the batch is rejected here too: a deferral never
/// returns an unchecked request in UnprocessedItems with a 200.
#[tokio::test]
async fn an_invalid_request_after_a_deferred_write_rejects_the_batch() {
    let data = Scripted::new(vec![Err(StorageError::Transient("lost".to_owned()))]);
    let items = json!({"t": [put("a"), put_with_wrong_key_type()]});

    let err = rejected(run(&data, items, &["t"], found(&["t"])).await);

    assert!(
        matches!(err, DynamoDbError::ValidationException(_)),
        "{err:?}"
    );
    assert_eq!(err.status_code(), 400);
    assert!(data.writes().is_empty(), "wrote {:?}", data.writes());
}

/// An invalid request in a later table rejects the batch before the first
/// table's requests are written.
#[tokio::test]
async fn an_invalid_request_in_a_later_table_rejects_the_batch_with_nothing_written() {
    let data = Scripted::new(vec![]);
    let items = json!({"t1": [put("a")], "t2": [put_with_wrong_key_type()]});

    let err = rejected(run(&data, items, &["t1", "t2"], found(&["t1", "t2"])).await);

    assert!(
        matches!(err, DynamoDbError::ValidationException(_)),
        "{err:?}"
    );
    assert!(data.writes().is_empty(), "wrote {:?}", data.writes());
}

/// A table that does not exist rejects the batch with nothing written, also
/// when it is not the first table.
#[tokio::test]
async fn a_missing_later_table_rejects_the_batch_with_nothing_written() {
    let data = Scripted::new(vec![]);
    let items = json!({"t1": [put("a")], "t2": [put("b")]});
    let lookups = vec![
        Ok(table("t1")),
        Err(StorageError::TableNotFound("t2".to_owned())),
    ];

    let err = rejected(run(&data, items, &["t1", "t2"], lookups).await);

    assert!(
        matches!(err, DynamoDbError::ResourceNotFoundException(_)),
        "{err:?}"
    );
    assert!(data.writes().is_empty(), "wrote {:?}", data.writes());
}

/// Duplicate keys in a later table reject the batch with nothing written.
#[tokio::test]
async fn duplicate_keys_in_a_later_table_reject_the_batch_with_nothing_written() {
    let data = Scripted::new(vec![]);
    let items = json!({"t1": [put("a")], "t2": [put("b"), put("b")]});

    let err = rejected(run(&data, items, &["t1", "t2"], found(&["t1", "t2"])).await);

    assert!(
        matches!(err, DynamoDbError::ValidationException(_)),
        "{err:?}"
    );
    assert!(data.writes().is_empty(), "wrote {:?}", data.writes());
}

/// A valid batch writes every request with its own table's key info, in the
/// order the tables and requests are listed.
#[tokio::test]
async fn a_valid_batch_writes_each_request_with_its_own_table() {
    let data = Scripted::new(vec![]);
    let items = json!({"t1": [put("a"), put("b")], "t2": [put("c")]});

    let out = answered(run(&data, items, &["t1", "t2"], found(&["t1", "t2"])).await);

    assert_eq!(out.body["UnprocessedItems"], json!({}));
    assert_eq!(data.writes(), ["t1/a", "t1/b", "t2/c"]);
}

/// The Cassandra backend raises `TransactionConflict` from a put when a
/// transaction or an expiration claim holds the item (the storage tests in
/// `ttl_integration.rs` pin that error). The service leaves such a request in
/// UnprocessedItems and keeps the request that applied: a batch of two
/// requests racing a TransactWriteItems on one of the items answered 200 with
/// the held item's request unprocessed and the other applied, in either
/// order (us-east-1, 2026-10-27, two samples of 640 raced batches). No
/// BatchWriteItem call answered TransactionConflictException.
#[tokio::test]
async fn a_request_that_lost_to_a_transaction_is_left_unprocessed() {
    let held = StorageError::TransactionConflict("held".to_owned());
    let data = Scripted::new(vec![Ok(()), Err(held)]);
    let items = json!({"t": [put("side"), put("held")]});

    let out = answered(run(&data, items, &["t"], found(&["t"])).await);

    assert_eq!(out.body["UnprocessedItems"], json!({"t": [put("held")]}));
    assert_eq!(data.writes(), ["t/side", "t/held"]);
}
