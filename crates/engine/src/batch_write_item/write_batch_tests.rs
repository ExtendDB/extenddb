// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `write_batch` against a `DataEngine` double: what is checked before the
//! first write.

use extenddb_core::types::{
    AttributeDefinition, IndexInfo, IndexType, KeyType, Projection, ProjectionType,
    ScalarAttributeType, VectorIndexKeyInfo,
};
use serde_json::{Value, json};

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

/// The Cassandra backend raises `TransactionConflict` from a put on a table
/// with TTL enabled when a transaction or an expiration claim holds the item
/// (the storage tests in `ttl_integration.rs` pin that error); without TTL the
/// put returns `TransactionCanceled`, which is not deferred. The service
/// leaves such a request in UnprocessedItems and keeps the request that
/// applied: a batch of two requests racing a TransactWriteItems on one of the
/// items answered 200 with the held item's request unprocessed and the other
/// applied, in either order (us-east-1, 2026-10-27, two samples of 640 raced
/// batches). No BatchWriteItem call answered TransactionConflictException.
#[tokio::test]
async fn a_request_that_lost_to_a_transaction_is_left_unprocessed() {
    let held = StorageError::TransactionConflict("held".to_owned());
    let data = Scripted::new(vec![Ok(()), Err(held)]);
    let items = json!({"t": [put("side"), put("held")]});

    let out = answered(run(&data, items, &["t"], found(&["t"])).await);

    assert_eq!(out.body["UnprocessedItems"], json!({"t": [put("held")]}));
    assert_eq!(data.writes(), ["t/side", "t/held"]);
}

// --- each per-request check runs before the first write ----------------------

/// A table whose items may carry a vector of three components in `vec`.
fn vector_table(name: &str) -> TableKeyInfo {
    TableKeyInfo {
        vector_indexes: vec![VectorIndexKeyInfo {
            index_name: "v".to_owned(),
            dimensions: 3,
            vector_attribute_name: "vec".to_owned(),
            search_schema: vec![],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
        }],
        ..table(name)
    }
}

/// A valid put followed by `faulty`, a request that fails exactly one
/// per-request check. The batch must be rejected with a 400 that carries the
/// message of that check, and the double must have recorded no write: a check
/// that ran after the write of the put ahead of it would leave `t/a` written.
async fn rejected_before_any_write(table: TableKeyInfo, faulty: Value, expected: &str) {
    let data = Scripted::new(vec![]);
    let items = json!({"t": [put("a"), faulty]});

    let err = rejected(run(&data, items, &["t"], vec![Ok(table)]).await);

    assert_eq!(err.status_code(), 400, "{err:?}");
    let DynamoDbError::ValidationException(message) = &err else {
        panic!("the batch must fail validation, got {err:?}");
    };
    assert!(message.contains(expected), "{message}");
    assert!(data.writes().is_empty(), "wrote {:?}", data.writes());
}

/// 401 KB is past the item limit of 400 KB (409,600 bytes).
#[tokio::test]
async fn an_item_over_the_size_limit_rejects_the_batch_before_any_write() {
    let item = json!({"pk": {"S": "b"}, "pad": {"S": "x".repeat(401 * 1024)}});

    rejected_before_any_write(
        table("t"),
        json!({"PutRequest": {"Item": item}}),
        "Item size has exceeded the maximum allowed size",
    )
    .await;
}

/// 40 levels of maps are past the limit of 32 nesting levels.
#[tokio::test]
async fn an_item_nested_past_the_depth_limit_rejects_the_batch_before_any_write() {
    let mut deep = json!({"S": "leaf"});
    for _ in 0..40 {
        deep = json!({"M": {"a": deep}});
    }
    let item = json!({"pk": {"S": "b"}, "deep": deep});

    rejected_before_any_write(
        table("t"),
        json!({"PutRequest": {"Item": item}}),
        "Nesting Levels have exceeded supported limits",
    )
    .await;
}

/// A partition key of 3,000 bytes is past the limit of 2,048 bytes.
#[tokio::test]
async fn a_key_over_the_size_limit_rejects_the_batch_before_any_write() {
    let item = json!({"pk": {"S": "k".repeat(3000)}});

    rejected_before_any_write(
        table("t"),
        json!({"PutRequest": {"Item": item}}),
        "Size of hashkey has exceeded",
    )
    .await;
}

/// A vector of two components in an index declared with three.
#[tokio::test]
async fn a_vector_of_the_wrong_dimension_rejects_the_batch_before_any_write() {
    let item = json!({"pk": {"S": "b"}, "vec": {"L": [{"N": "1"}, {"N": "2"}]}});

    rejected_before_any_write(
        vector_table("t"),
        json!({"PutRequest": {"Item": item}}),
        "Invalid size for parameter vec",
    )
    .await;
}

/// A delete whose key carries an attribute that is not part of the key.
#[tokio::test]
async fn a_delete_with_an_extra_key_attribute_rejects_the_batch_before_any_write() {
    let key = json!({"pk": {"S": "b"}, "extra": {"S": "x"}});

    rejected_before_any_write(
        table("t"),
        json!({"DeleteRequest": {"Key": key}}),
        "The provided key element does not match the schema",
    )
    .await;
}

// --- secondary index keys ----------------------------------------------------

fn key_element(attribute_name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: attribute_name.to_owned(),
        key_type,
    }
}

fn index(index_name: &str, index_type: IndexType, key_schema: Vec<KeySchemaElement>) -> IndexInfo {
    IndexInfo {
        index_name: index_name.to_owned(),
        index_id: format!("id-{index_name}"),
        index_type,
        key_schema,
        projection: Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        },
    }
}

/// A table with a global secondary index `by-g` keyed by a string `g`, and a
/// local secondary index `by-l` whose sort key is a string `l`.
fn indexed_table(name: &str) -> TableKeyInfo {
    let mut table = table(name);
    for attribute_name in ["g", "l"] {
        table.attribute_definitions.push(AttributeDefinition {
            attribute_name: attribute_name.to_owned(),
            attribute_type: ScalarAttributeType::S,
        });
    }
    table.global_secondary_indexes = vec![index(
        "by-g",
        IndexType::Gsi,
        vec![key_element("g", KeyType::Hash)],
    )];
    table.local_secondary_indexes = vec![index(
        "by-l",
        IndexType::Lsi,
        vec![
            key_element("pk", KeyType::Hash),
            key_element("l", KeyType::Range),
        ],
    )];
    table.has_lsi = true;
    table
}

/// The storage layer also checks the index keys, inside `put_item`, so a
/// check left to it alone would write `t/a` before the 400. The service
/// answered this message and applied nothing (us-east-1, 2026-11-10).
#[tokio::test]
async fn a_global_secondary_index_key_of_the_wrong_type_rejects_the_batch_before_any_write() {
    let item = json!({"pk": {"S": "b"}, "g": {"N": "1"}});

    rejected_before_any_write(
        indexed_table("t"),
        json!({"PutRequest": {"Item": item}}),
        "One or more parameter values were invalid: Type mismatch for Index Key g Expected: S \
         Actual: N IndexName: by-g",
    )
    .await;
}

/// The service answered this message and applied nothing (us-east-1,
/// 2026-11-10).
#[tokio::test]
async fn an_empty_global_secondary_index_key_rejects_the_batch_before_any_write() {
    let item = json!({"pk": {"S": "b"}, "g": {"S": ""}});

    rejected_before_any_write(
        indexed_table("t"),
        json!({"PutRequest": {"Item": item}}),
        "One or more parameter values are not valid. A value specified for a secondary index \
         key is not supported. The AttributeValue for a key attribute cannot contain an empty \
         string value. IndexName: by-g, IndexKey: g",
    )
    .await;
}

/// A local secondary index key goes through the same check. The service's
/// answer to this batch was not measured, so the test pins only that the
/// batch is refused with a 400 before any write.
#[tokio::test]
async fn a_local_secondary_index_key_of_the_wrong_type_rejects_the_batch_before_any_write() {
    let data = Scripted::new(vec![]);
    let faulty = json!({"PutRequest": {"Item": {"pk": {"S": "b"}, "l": {"N": "1"}}}});
    let items = json!({"t": [put("a"), faulty]});

    let err = rejected(run(&data, items, &["t"], vec![Ok(indexed_table("t"))]).await);

    assert!(
        matches!(err, DynamoDbError::ValidationException(_)),
        "{err:?}"
    );
    assert_eq!(err.status_code(), 400);
    assert!(data.writes().is_empty(), "wrote {:?}", data.writes());
}

/// Index keys of the declared type, and an item without them, are written.
#[tokio::test]
async fn valid_secondary_index_keys_are_written() {
    let data = Scripted::new(vec![]);
    let with_keys = json!({"PutRequest": {"Item": {
        "pk": {"S": "b"}, "g": {"S": "x"}, "l": {"S": "y"}
    }}});
    let items = json!({"t": [put("a"), with_keys]});

    let out = answered(run(&data, items, &["t"], vec![Ok(indexed_table("t"))]).await);

    assert_eq!(out.body["UnprocessedItems"], json!({}));
    assert_eq!(data.writes(), ["t/a", "t/b"]);
}
