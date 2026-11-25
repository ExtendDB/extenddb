// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The server's refusal on a provisioned global secondary index's own
//! capacity, driven through `handle_request` against an in-memory engine: the
//! two measured bodies and the metric records they leave, the index ARN they
//! name, the write charged to an index only when its item carries the index
//! key, the local secondary index served from the table's buckets, the index
//! deleted by `UpdateTable`, the index raised by `UpdateTable`, the index set
//! kept across a table throughput update, the index read that does not
//! consult the table's read bucket, and the registration of the indexes on
//! the first request after a restart.

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

/// The units a test drains a 1-unit bucket by: a debt of 999 seconds at that
/// rate, so no delay in a run refills it before the request under test, and
/// far less than one second of the account's aggregate, which a read drain
/// is charged to as well.
const DRAIN: f64 = 1_000.0;

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

/// The dimensions of a throttle record: the account, the table, the index
/// when there is one, and the operation.
fn dims(account: &str, table: &str, index: Option<&str>, op: &str) -> Vec<Dimension> {
    let mut dims = vec![
        Dimension::AccountId(account.to_owned()),
        Dimension::TableName(table.to_owned()),
    ];
    dims.extend(index.map(|i| Dimension::GlobalSecondaryIndexName(i.to_owned())));
    dims.push(Dimension::Operation(op.to_owned()));
    dims
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
    // Drained directly, by far more than the index's one second of burst at
    // 1 RCU (see `DRAIN`).
    state
        .throttle
        .consume_with_partition(&account_id, table, DRAIN, 0.0, None, Some(GSI));
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
    // count carries the account, the table and the operation, and no index.
    let mut expected = Vec::new();
    for op in ["Query", "Scan"] {
        let index = dims(&account_id, table, Some(GSI), op);
        expected.push((MetricName::ReadThrottleEvents, index, 1.0));
        let table_only = dims(&account_id, table, None, op);
        expected.push((MetricName::ThrottledRequests, table_only, 1.0));
    }
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
    // The index's write bucket drained directly (see `DRAIN`); the table at
    // 1000 WCU keeps capacity.
    drain_index_writes(&state, &account_id, table);
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
    // Each refusal is a throttle event of the index and of the table: the
    // service publishes WriteThrottleEvents for both series on an index write
    // refusal. The request count carries no index.
    let mut expected = Vec::new();
    for op in ["PutItem", "UpdateItem", "DeleteItem"] {
        let index = dims(&account_id, table, Some(GSI), op);
        let table_only = dims(&account_id, table, None, op);
        expected.push((MetricName::WriteThrottleEvents, index, 1.0));
        expected.push((MetricName::WriteThrottleEvents, table_only.clone(), 1.0));
        expected.push((MetricName::ThrottledRequests, table_only, 1.0));
    }
    expected.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(throttle_records(&state, table), expected);
}

/// A write is charged to an index only when its item carries the index's
/// key, as the service charges it. Each row: an item and the reason of the
/// first refusal. On a table at 3 WCU with the index at 1 WCU, 2 KB items
/// without the index key are served until the table's capacity is gone and
/// then refused with the table reason; the same items with the key drain the
/// index on the first write and the second is refused with the index reason.
#[tokio::test]
async fn a_write_is_charged_to_an_index_only_when_its_item_carries_the_index_key() {
    let (state, _) = build_state().await;
    let mut without_key = item(2_000);
    without_key["sk"] = json!({"S": "s"});
    let mut with_key = without_key.clone();
    with_key["g"] = json!({"S": "k"});
    let rows = [
        (
            "no-index-key",
            without_key,
            2,
            "TableWriteProvisionedThroughputExceeded",
        ),
        (
            "index-key",
            with_key,
            1,
            "IndexWriteProvisionedThroughputExceeded",
        ),
    ];
    for (table, item, served, reason) in rows {
        create_indexed_table(&state, table, 1000, 3, 1000, 1).await;
        let put = json!({"TableName": table, "Item": item});
        for _ in 0..served {
            let (status, body) = call(&state, "PutItem", put.clone()).await;
            assert_eq!(status, 200, "{table}: {body}");
        }
        let (status, body) = call(&state, "PutItem", put).await;
        assert_eq!(status, 400, "{table}: {body}");
        assert_eq!(body["ThrottlingReasons"][0]["reason"], reason, "{table}");
    }
}

/// Drain the index's write bucket by `DRAIN`, as a served write that carries
/// the index key is charged.
fn drain_index_writes(state: &AppState, account_id: &str, table: &str) {
    let writes = [(GSI.to_owned(), DRAIN)].into_iter().collect();
    state
        .throttle
        .consume_index_writes(account_id, table, &writes);
}

/// A local secondary index shares the table's capacity: a `Query` naming it
/// past the table's read capacity carries the table body and the table ARN.
#[tokio::test]
async fn lsi_read_throttle_is_the_table_body() {
    let (state, account_id) = build_state().await;
    let table = "lsi-reads";
    create_indexed_table(&state, table, 1, 1000, 1000, 1000).await;
    state.throttle.consume(&account_id, table, DRAIN, 0.0);
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
    drain_index_writes(&state, &account_id, table);
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

/// `UpdateTable` raising the index's `ProvisionedThroughput` is the way out
/// the refusal names: the response and `DescribeTable` show the new units,
/// and the index's buckets take the new rate from the response, so the write
/// the index was holding back is served at once. The index is drained by
/// writes of new items that carry its key (one unit each, one second of
/// burst at 1 WCU). At the old rate the debt of about one unit takes a second
/// to repay; the retries below stop after 200 ms.
#[tokio::test]
async fn raising_the_index_throughput_ends_its_write_refusals() {
    let (state, _) = build_state().await;
    let table = "index-raised";
    create_indexed_table(&state, table, 1000, 1000, 1, 1).await;
    let put = |n: usize| {
        let mut it = indexed_item();
        it["pk"] = json!({"S": format!("k{n}")});
        json!({"TableName": table, "Item": it})
    };
    let mut refused = None;
    for n in 0..10 {
        let (status, body) = call(&state, "PutItem", put(n)).await;
        if status != 200 {
            refused = Some(body);
            break;
        }
    }
    let refused = refused.expect("the index at 1 WCU refuses within ten writes");
    assert_eq!(
        refused["ThrottlingReasons"][0]["reason"], "IndexWriteProvisionedThroughputExceeded",
        "{refused}"
    );
    let raised = json!({"ReadCapacityUnits": 5, "WriteCapacityUnits": 1000});
    let (status, body) = call(
        &state,
        "UpdateTable",
        json!({
            "TableName": table,
            "GlobalSecondaryIndexUpdates": [{"Update": {
                "IndexName": GSI,
                "ProvisionedThroughput": raised,
            }}],
        }),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    let units = |description: &Value| {
        let pt = &description["GlobalSecondaryIndexes"][0]["ProvisionedThroughput"];
        (
            pt["ReadCapacityUnits"].clone(),
            pt["WriteCapacityUnits"].clone(),
        )
    };
    assert_eq!(
        units(&body["TableDescription"]),
        (json!(5), json!(1000)),
        "{body}"
    );
    let (_, described) = call(&state, "DescribeTable", json!({"TableName": table})).await;
    assert_eq!(
        units(&described["Table"]),
        (json!(5), json!(1000)),
        "{described}"
    );
    let mut last = Value::Null;
    for _ in 0..20 {
        let (status, body) = call(&state, "PutItem", put(10)).await;
        if status == 200 {
            return;
        }
        last = body;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the write is still refused 200 ms after the raise: {last}");
}

/// An index read is served from the index's read bucket and does not consult
/// the table's: with the table's read bucket drained, a `Scan` of the base
/// table is refused and a `Query` on the index is served.
#[tokio::test]
async fn index_read_does_not_consult_the_table_read_bucket() {
    let (state, account_id) = build_state().await;
    let table = "index-apart";
    create_indexed_table(&state, table, 1, 1000, 1000, 1000).await;
    state.throttle.consume(&account_id, table, DRAIN, 0.0);
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
    drain_index_writes(&state, &account_id, table);
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
        .consume_with_partition(&account_id, table, DRAIN, 0.0, None, Some(GSI));
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
