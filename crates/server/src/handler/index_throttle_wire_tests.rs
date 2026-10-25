// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The server's refusal on a provisioned global secondary index's own
//! capacity, driven through `handle_request` against an in-memory engine: the
//! two measured bodies and the metric records they leave, the index ARN they
//! name, the local secondary index served from the table's buckets, the index
//! deleted by `UpdateTable`, the index set kept across a table throughput
//! update, the index read that does not consult the table's read bucket, and
//! the registration of the indexes on the first request after a restart.

use std::sync::Arc;

use extenddb_core::error::{
    INDEX_WRITE_PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE, PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
};
use extenddb_core::metrics::{Dimension, MetricName, MetricsQuery, TimeWindow};
use serde_json::{Value, json};

use super::throttle_wire_tests::{arn, build_state, call, create_table, item};
use crate::AppState;

const GSI: &str = "by-g";
const LSI: &str = "by-l";

/// A provisioned table with a sort key, one global secondary index at
/// `index_rcu` and `index_wcu`, and one local secondary index.
async fn create_indexed_table(
    state: &Arc<AppState>,
    table: &str,
    table_rcu: i64,
    table_wcu: i64,
    index_rcu: i64,
    index_wcu: i64,
) {
    create_table(
        state,
        table,
        json!({
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "g", "AttributeType": "S"},
                {"AttributeName": "l", "AttributeType": "S"},
            ],
            "ProvisionedThroughput": {
                "ReadCapacityUnits": table_rcu,
                "WriteCapacityUnits": table_wcu,
            },
            "GlobalSecondaryIndexes": [{
                "IndexName": GSI,
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": index_rcu,
                    "WriteCapacityUnits": index_wcu,
                },
            }],
            "LocalSecondaryIndexes": [{
                "IndexName": LSI,
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "l", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }],
        }),
    )
    .await;
}

fn index_arn(account_id: &str, table: &str, index: &str) -> String {
    format!("{}/index/{index}", arn(account_id, table))
}

fn query(table: &str, index: &str, key: &str) -> Value {
    json!({
        "TableName": table,
        "IndexName": index,
        "KeyConditionExpression": format!("{key} = :v"),
        "ExpressionAttributeValues": {":v": {"S": "k"}},
    })
}

/// The `(metric, dimensions, sum)` throttle records the collector holds for
/// `table` in the last minute, sorted, for an assertion on what a refusal
/// recorded.
fn throttle_records(state: &AppState, table: &str) -> Vec<(MetricName, Vec<Dimension>, f64)> {
    let mut records: Vec<_> = state
        .metrics
        .query(&MetricsQuery {
            table_name: Some(table.to_owned()),
            window: Some(TimeWindow::LastMinute),
            ..MetricsQuery::default()
        })
        .into_iter()
        .filter(|s| {
            matches!(
                s.metric,
                MetricName::ReadThrottleEvents
                    | MetricName::WriteThrottleEvents
                    | MetricName::ThrottledRequests
            )
        })
        .map(|s| (s.metric, s.dimensions, s.sum))
        .collect();
    records.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    records
}

fn indexed_item() -> Value {
    let mut it = item(16);
    it["sk"] = json!({"S": "s"});
    it["g"] = json!({"S": "k"});
    it["l"] = json!({"S": "k"});
    it
}

/// A `Query` naming the index past its read capacity is refused with the
/// provisioned class, the table message and one reason naming the index ARN,
/// the same string `DescribeTable` returns as the index's `IndexArn`.
#[tokio::test]
async fn index_read_throttle_names_the_index_arn_with_the_table_message() {
    let (state, account_id) = build_state().await;
    let table = "index-reads";
    create_indexed_table(&state, table, 1000, 1000, 1, 1).await;
    let (_, described) = call(&state, "DescribeTable", json!({"TableName": table})).await;
    let described_arn = described["Table"]["GlobalSecondaryIndexes"][0]["IndexArn"]
        .as_str()
        .expect("IndexArn")
        .to_owned();
    assert_eq!(described_arn, index_arn(&account_id, table, GSI));
    // One second of burst at 1 RCU; drained directly so the test does not
    // depend on timing.
    state
        .throttle
        .consume_with_partition(&account_id, table, 2.0, 0.0, None, Some(GSI));
    let (status, body) = call(&state, "Query", query(table, GSI, "g")).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body,
        json!({
            "__type": "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException",
            "message": PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
            "ThrottlingReasons": [{
                "reason": "IndexReadProvisionedThroughputExceeded",
                "resource": described_arn,
            }],
        })
    );
    // Scan naming the index is refused the same way.
    let (status, body) = call(
        &state,
        "Scan",
        json!({"TableName": table, "IndexName": GSI}),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body["ThrottlingReasons"][0]["reason"],
        "IndexReadProvisionedThroughputExceeded"
    );
    // The throttle event carries the index as its dimension; the request
    // count carries the table and operation only.
    let t = |op: &str| {
        vec![
            Dimension::TableName(table.to_owned()),
            Dimension::Operation(op.to_owned()),
        ]
    };
    let g = |op: &str| {
        vec![
            Dimension::TableName(table.to_owned()),
            Dimension::GlobalSecondaryIndexName(GSI.to_owned()),
            Dimension::Operation(op.to_owned()),
        ]
    };
    let mut expected = vec![
        (MetricName::ReadThrottleEvents, g("Query"), 1.0),
        (MetricName::ReadThrottleEvents, g("Scan"), 1.0),
        (MetricName::ThrottledRequests, t("Query"), 1.0),
        (MetricName::ThrottledRequests, t("Scan"), 1.0),
    ];
    expected.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(throttle_records(&state, table), expected);
}

/// A write to the base table held back by the index's write capacity, while
/// the table's own capacity is large, carries the index message (no trailing
/// period) and the index ARN.
#[tokio::test]
async fn index_write_throttle_names_the_index_message_and_arn() {
    let (state, account_id) = build_state().await;
    let table = "index-writes";
    create_indexed_table(&state, table, 1000, 1000, 1, 1).await;
    // A write is charged to the table and to every index; the table at 1000
    // WCU keeps capacity while the index's one second of burst is gone.
    state
        .throttle
        .consume_with_partition(&account_id, table, 0.0, 2.0, None, None);
    // The segment throttles a request with a top-level `TableName`; a
    // `BatchWriteItem` or `TransactWriteItems` names its tables per item and
    // is not checked here, as before this change.
    for operation in ["PutItem", "UpdateItem", "DeleteItem"] {
        let body = match operation {
            "PutItem" => json!({"TableName": table, "Item": indexed_item()}),
            "UpdateItem" => json!({
                "TableName": table,
                "Key": {"pk": {"S": "k"}, "sk": {"S": "s"}},
                "UpdateExpression": "SET v = :v",
                "ExpressionAttributeValues": {":v": {"S": "x"}},
            }),
            _ => json!({
                "TableName": table,
                "Key": {"pk": {"S": "k"}, "sk": {"S": "s"}},
            }),
        };
        let (status, got) = call(&state, operation, body).await;
        assert_eq!(status, 400, "{operation}: {got}");
        assert_eq!(
            got,
            json!({
                "__type": "com.amazonaws.dynamodb.v20120810#ProvisionedThroughputExceededException",
                "message": INDEX_WRITE_PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE,
                "ThrottlingReasons": [{
                    "reason": "IndexWriteProvisionedThroughputExceeded",
                    "resource": index_arn(&account_id, table, GSI),
                }],
            }),
            "{operation}"
        );
    }
    assert!(
        !INDEX_WRITE_PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE.ends_with('.'),
        "the index write message carries no trailing period"
    );
    let records = throttle_records(&state, table);
    assert_eq!(records.len(), 6, "{records:?}");
    for (metric, dimensions, sum) in &records {
        assert_eq!(*sum, 1.0, "{metric:?} {dimensions:?}");
        let has_index = dimensions
            .iter()
            .any(|d| matches!(d, Dimension::GlobalSecondaryIndexName(n) if n == GSI));
        match metric {
            MetricName::WriteThrottleEvents => assert!(has_index, "{dimensions:?}"),
            MetricName::ThrottledRequests => assert!(!has_index, "{dimensions:?}"),
            other => panic!("unexpected {other:?}"),
        }
    }
}

/// A local secondary index shares the table's capacity: a `Query` naming it
/// past the table's read capacity carries the table body and the table ARN.
#[tokio::test]
async fn lsi_read_throttle_is_the_table_body() {
    let (state, account_id) = build_state().await;
    let table = "lsi-reads";
    create_indexed_table(&state, table, 1, 1000, 1000, 1000).await;
    state.throttle.consume(&account_id, table, 2.0, 0.0);
    let (status, body) = call(&state, "Query", query(table, LSI, "pk")).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body["ThrottlingReasons"],
        json!([{
            "reason": "TableReadProvisionedThroughputExceeded",
            "resource": arn(&account_id, table),
        }])
    );
}

/// `UpdateTable` deleting the index replaces the registered set, so the write
/// the index was holding back is served.
#[tokio::test]
async fn deleting_the_index_stops_its_write_refusals() {
    let (state, account_id) = build_state().await;
    let table = "index-deleted";
    create_indexed_table(&state, table, 1000, 1000, 1, 1).await;
    state
        .throttle
        .consume_with_partition(&account_id, table, 0.0, 2.0, None, None);
    let put = json!({"TableName": table, "Item": indexed_item()});
    let (status, body) = call(&state, "PutItem", put.clone()).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body["ThrottlingReasons"][0]["reason"],
        "IndexWriteProvisionedThroughputExceeded"
    );
    let (status, body) = call(
        &state,
        "UpdateTable",
        json!({
            "TableName": table,
            "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": GSI}}],
        }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let (status, body) = call(&state, "PutItem", put).await;
    assert_eq!(status, 200, "{body}");
}

/// An index read is served from the index's read bucket and does not consult
/// the table's: with the table's read bucket drained, a `Scan` of the base
/// table is refused and a `Query` on the index is served.
#[tokio::test]
async fn index_read_does_not_consult_the_table_read_bucket() {
    let (state, account_id) = build_state().await;
    let table = "index-apart";
    create_indexed_table(&state, table, 1, 1000, 1000, 1000).await;
    state.throttle.consume(&account_id, table, 2.0, 0.0);
    let (status, body) = call(&state, "Scan", json!({"TableName": table})).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body["ThrottlingReasons"][0]["reason"],
        "TableReadProvisionedThroughputExceeded"
    );
    let (status, body) = call(&state, "Query", query(table, GSI, "g")).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["Count"], 0);
}

/// An `UpdateTable` that changes only the table's throughput re-sends the
/// index set from its response, so a drained index keeps holding writes back.
#[tokio::test]
async fn a_table_throughput_update_keeps_the_index_buckets() {
    let (state, account_id) = build_state().await;
    let table = "index-kept";
    create_indexed_table(&state, table, 1000, 1000, 1, 1).await;
    state
        .throttle
        .consume_with_partition(&account_id, table, 0.0, 2.0, None, None);
    let (status, body) = call(
        &state,
        "UpdateTable",
        json!({
            "TableName": table,
            "ProvisionedThroughput": {"ReadCapacityUnits": 1000, "WriteCapacityUnits": 1001},
        }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["TableDescription"]["GlobalSecondaryIndexes"][0]["IndexName"], GSI,
        "{body}"
    );
    let (status, body) = call(
        &state,
        "PutItem",
        json!({"TableName": table, "Item": indexed_item()}),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body["ThrottlingReasons"][0]["reason"],
        "IndexWriteProvisionedThroughputExceeded"
    );
}

/// Buckets live in memory, so after a restart the first data request on a
/// table registers it from its description, indexes included. Dropping the
/// registration stands in for the restart: the next index read registers the
/// index again, and draining it then refuses with the index body.
#[tokio::test]
async fn the_lazy_registration_path_registers_the_indexes() {
    let (state, account_id) = build_state().await;
    let table = "index-lazy";
    create_indexed_table(&state, table, 1000, 1000, 1, 1).await;
    state.throttle.remove_table(&account_id, table);
    assert!(!state.throttle.is_registered(&account_id, table));
    let (status, body) = call(&state, "Query", query(table, GSI, "g")).await;
    assert_eq!(status, 200, "{body}");
    assert!(state.throttle.is_registered(&account_id, table));
    state
        .throttle
        .consume_with_partition(&account_id, table, 2.0, 0.0, None, Some(GSI));
    let (status, body) = call(&state, "Query", query(table, GSI, "g")).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        body["ThrottlingReasons"],
        json!([{
            "reason": "IndexReadProvisionedThroughputExceeded",
            "resource": index_arn(&account_id, table, GSI),
        }])
    );
}
