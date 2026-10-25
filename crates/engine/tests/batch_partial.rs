// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Partial success on `BatchGetItem` and `BatchWriteItem` when a backend
//! throttles some entries.
//!
//! Expected behaviour was measured against Amazon DynamoDB on 2026-10-21
//! (1,070 raw responses on a 1 RCU / 1 WCU table) and 2026-10-20 (an
//! on-demand table): a throttled entry moves to `UnprocessedKeys` or
//! `UnprocessedItems` with the request fields echoed (`ConsistentRead` and
//! `ProjectionExpression` measured), served and unprocessed sets are disjoint
//! and complete, `ConsumedCapacity` counts served entries only, and an empty
//! unprocessed map is present as `{}`. A batch whose every entry was throttled
//! fails with the throttle error per the API Reference; that case and the
//! echo of `ExpressionAttributeNames` and `AttributesToGet` were not reached
//! in the samples. The order of the unprocessed lists, which the samples
//! returned in neither request nor sorted order, is not asserted. The storage
//! double throttles a chosen set of partition key values and records which
//! writes it applied.

#[path = "batch_partial/failing_store.rs"]
mod failing_store;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::{Value, json};

use extenddb_auth::AuthCacheRegistry;
use extenddb_core::error::{DynamoDbError, PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE};
use extenddb_core::limits::LimitsConfig;
use extenddb_engine::{
    DispatchResult, OperationContext, handle_batch_get_item, handle_batch_write_item,
};

use failing_store::*;

fn ctx(store: Arc<FailingStore>) -> OperationContext {
    OperationContext {
        storage: store,
        limits: Arc::new(LimitsConfig::default()),
        region: Arc::from(REGION),
        account_id: Arc::from(ACCOUNT),
        import_paths: Arc::from(Vec::<Arc<PathBuf>>::new()),
        export_paths: Arc::from(Vec::<Arc<PathBuf>>::new()),
        authorized_key_info: extenddb_engine::AuthorizedKeyInfo::new(),
        auth_cache: AuthCacheRegistry::empty(),
        table_key_info_lookup: None,
    }
}

fn keys_json(pks: &[&str]) -> Value {
    Value::Array(pks.iter().map(|p| json!({"pk": {"S": p}})).collect())
}

/// Partition key values of a JSON list of keys or items.
fn pks(list: &Value) -> BTreeSet<String> {
    list.as_array()
        .expect("list")
        .iter()
        .map(|k| k["pk"]["S"].as_str().expect("pk").to_owned())
        .collect()
}

/// The error of a handler result, or a panic naming the body that came back.
fn err_of(result: Result<DispatchResult, DynamoDbError>) -> DynamoDbError {
    match result {
        Err(e) => e,
        Ok(ok) => panic!("expected an error, got a response: {}", ok.body),
    }
}

fn assert_throttle(err: &DynamoDbError, reason: &str) {
    assert_eq!(err.status_code(), 400);
    assert_eq!(err.error_type(), "ProvisionedThroughputExceededException");
    assert_eq!(err.message(), PROVISIONED_THROUGHPUT_EXCEEDED_MESSAGE);
    let reasons = err.throttling_reasons().expect("reasons");
    assert_eq!(reasons.len(), 1);
    assert_eq!(reasons[0].reason, reason);
}

// ---------------------------------------------------------------------------
// BatchGetItem
// ---------------------------------------------------------------------------

/// Measured 2026-10-21: served items in `Responses`, the throttled keys under
/// `UnprocessedKeys[<table>]` beside the request's `ConsistentRead` and
/// `ProjectionExpression`, sets disjoint and complete, `ConsumedCapacity` for
/// the served items only. `ExpressionAttributeNames` is echoed per the API
/// Reference; the sample sent none.
#[tokio::test]
async fn batch_get_partial_echoes_request_fields_and_charges_served_only() {
    let store = Arc::new(
        FailingStore::throttling()
            .with_items(&["k1", "k2", "k3", "k4", "k5"])
            .failing_on(&["k2", "k4"]),
    );
    let body = json!({
        "RequestItems": {
            "T": {
                "Keys": keys_json(&["k1", "k2", "k3", "k4", "k5"]),
                "ConsistentRead": true,
                "ProjectionExpression": "#p",
                "ExpressionAttributeNames": {"#p": "pk"}
            }
        },
        "ReturnConsumedCapacity": "TOTAL"
    });

    let result = handle_batch_get_item(body, &ctx(store))
        .await
        .expect("partial success is a 200");

    let served = pks(&result.body["Responses"]["T"]);
    assert_eq!(served, ["k1", "k3", "k5"].map(str::to_owned).into());
    for it in result.body["Responses"]["T"].as_array().expect("items") {
        assert!(it.get("v").is_none(), "projection applied to served items");
    }

    let unprocessed = &result.body["UnprocessedKeys"]["T"];
    assert_eq!(
        pks(&unprocessed["Keys"]),
        ["k2", "k4"].map(str::to_owned).into()
    );
    assert_eq!(unprocessed["ConsistentRead"], json!(true));
    assert_eq!(unprocessed["ProjectionExpression"], json!("#p"));
    assert_eq!(unprocessed["ExpressionAttributeNames"], json!({"#p": "pk"}));
    assert!(unprocessed.get("AttributesToGet").is_none());
    assert_eq!(
        unprocessed.as_object().expect("map").len(),
        4,
        "Keys plus the three request fields that were sent"
    );

    // Disjoint and complete.
    let unread = pks(&unprocessed["Keys"]);
    assert!(served.is_disjoint(&unread));
    let all: BTreeSet<String> = served.union(&unread).cloned().collect();
    assert_eq!(
        all,
        ["k1", "k2", "k3", "k4", "k5"].map(str::to_owned).into()
    );

    // Three consistent reads under 4 KB: 1 unit each, nothing for k2 and k4.
    assert_eq!(
        result.body["ConsumedCapacity"],
        json!([{"TableName": "T", "CapacityUnits": 3.0}])
    );
    assert_eq!(result.metrics.read_capacity_units, 3.0);
    assert_eq!(result.metrics.returned_item_count, 3);
}

/// Two tables: one throttled on every key, the other fully served. The
/// throttled table lists all its keys under `UnprocessedKeys`; the served
/// table is absent from it.
#[tokio::test]
async fn batch_get_one_table_throttled_beside_a_served_table() {
    let store = Arc::new(
        FailingStore::throttling()
            .with_items(&["t1", "t2", "u1", "u2"])
            .failing_on(&["t1", "t2"]),
    );
    let body = json!({
        "RequestItems": {
            "T": {"Keys": keys_json(&["t1", "t2"])},
            "U": {"Keys": keys_json(&["u1", "u2"]), "ConsistentRead": false}
        },
        "ReturnConsumedCapacity": "TOTAL"
    });

    let result = handle_batch_get_item(body, &ctx(store))
        .await
        .expect("at least one key served is a 200");

    let unprocessed = result.body["UnprocessedKeys"].as_object().expect("map");
    assert_eq!(unprocessed.len(), 1);
    assert_eq!(
        pks(&unprocessed["T"]["Keys"]),
        ["t1", "t2"].map(str::to_owned).into()
    );
    assert!(unprocessed["T"].get("ConsistentRead").is_none());
    // The fully throttled table keeps an empty list under Responses (the
    // branch's own contract; the service's shape for this case is unmeasured).
    let responses = result.body["Responses"].as_object().expect("map");
    assert_eq!(responses.len(), 2);
    assert_eq!(responses["T"], json!([]));
    assert_eq!(
        pks(&result.body["Responses"]["U"]),
        ["u1", "u2"].map(str::to_owned).into()
    );
    // Eventually consistent reads under 4 KB: 0.5 each; nothing for T.
    assert_eq!(
        result.body["ConsumedCapacity"],
        json!([{"TableName": "U", "CapacityUnits": 1.0}])
    );
    assert_eq!(result.metrics.read_capacity_units, 1.0);
}

/// Every key of every table throttled: the call fails with the throttle
/// error (API Reference; documented, not measured).
#[tokio::test]
async fn batch_get_every_key_throttled_fails_with_the_throttle_error() {
    let store = Arc::new(
        FailingStore::throttling()
            .with_items(&["t1", "u1"])
            .failing_on(&["t1", "u1"]),
    );
    let body = json!({
        "RequestItems": {
            "T": {"Keys": keys_json(&["t1"])},
            "U": {"Keys": keys_json(&["u1"])}
        }
    });

    let err = err_of(handle_batch_get_item(body, &ctx(store)).await);
    assert_throttle(&err, "TableReadProvisionedThroughputExceeded");
}

/// The boundary of the all-throttled rule: with one key served and every
/// other throttled the call is a 200 carrying that one item, the rest
/// unprocessed, and capacity for the one.
#[tokio::test]
async fn batch_get_one_served_key_is_a_partial_success() {
    let store = Arc::new(
        FailingStore::throttling()
            .with_items(&["k1", "k2", "k3"])
            .failing_on(&["k1", "k2"]),
    );
    let body = json!({
        "RequestItems": {"T": {"Keys": keys_json(&["k1", "k2", "k3"]), "ConsistentRead": true}},
        "ReturnConsumedCapacity": "TOTAL"
    });
    let result = handle_batch_get_item(body, &ctx(store))
        .await
        .expect("one key served is a 200");
    assert_eq!(
        pks(&result.body["Responses"]["T"]),
        ["k3".to_owned()].into()
    );
    assert_eq!(
        pks(&result.body["UnprocessedKeys"]["T"]["Keys"]),
        ["k1", "k2"].map(str::to_owned).into()
    );
    assert_eq!(
        result.body["ConsumedCapacity"],
        json!([{"TableName": "T", "CapacityUnits": 1.0}])
    );
}

/// `AttributesToGet` is echoed beside the unprocessed keys (API Reference;
/// the sample sent `ProjectionExpression` instead).
#[tokio::test]
async fn batch_get_partial_echoes_attributes_to_get() {
    let store = Arc::new(
        FailingStore::throttling()
            .with_items(&["k1", "k2"])
            .failing_on(&["k2"]),
    );
    let body = json!({
        "RequestItems": {"T": {"Keys": keys_json(&["k1", "k2"]), "AttributesToGet": ["pk"]}}
    });
    let result = handle_batch_get_item(body, &ctx(store))
        .await
        .expect("one key served is a 200");
    let t = result.body["UnprocessedKeys"]["T"]
        .as_object()
        .expect("map");
    assert_eq!(t.len(), 2, "{t:?}");
    assert_eq!(t["AttributesToGet"], json!(["pk"]));
    assert_eq!(pks(&t["Keys"]), ["k2".to_owned()].into());
}

/// Capacity for a served item follows its size: a consistent read of an item
/// over 4 KB costs two units (the measured 16.0 for 16 reads of 4 KB items
/// sits at the one-unit tier; this pins the tier boundary the engine applies).
#[tokio::test]
async fn batch_get_partial_charges_served_items_by_size() {
    let store = Arc::new(
        FailingStore::throttling()
            .with_items(&["k1"])
            .with_large_item("k2")
            .failing_on(&["k3"])
            .with_items(&["k3"]),
    );
    let body = json!({
        "RequestItems": {"T": {"Keys": keys_json(&["k1", "k2", "k3"]), "ConsistentRead": true}},
        "ReturnConsumedCapacity": "TOTAL"
    });
    let result = handle_batch_get_item(body, &ctx(store))
        .await
        .expect("two keys served is a 200");
    assert_eq!(
        result.body["ConsumedCapacity"],
        json!([{"TableName": "T", "CapacityUnits": 3.0}])
    );
    assert_eq!(result.metrics.read_capacity_units, 3.0);
}

/// Nothing throttled: `UnprocessedKeys` is present as `{}` (26 of 26
/// measured).
#[tokio::test]
async fn batch_get_nothing_throttled_serializes_empty_unprocessed_keys() {
    let store = Arc::new(FailingStore::throttling().with_items(&["k1"]));
    let body = json!({
        "RequestItems": {"T": {"Keys": keys_json(&["k1", "k2"])}}
    });

    let result = handle_batch_get_item(body, &ctx(store)).await.expect("200");
    let serialized = serde_json::to_string(&result.body).expect("serialize");
    assert!(
        serialized.contains(r#""UnprocessedKeys":{}"#),
        "body: {serialized}"
    );
    assert_eq!(
        pks(&result.body["Responses"]["T"]),
        ["k1"].map(str::to_owned).into()
    );
}

/// A storage `Service` error that is not a throttle still fails the request.
#[tokio::test]
async fn batch_get_non_throttle_service_error_fails_the_request() {
    let store = Arc::new(
        FailingStore::new(DynamoDbError::InternalServerError(
            "Internal server error".to_owned(),
        ))
        .with_items(&["k1", "k2"])
        .failing_on(&["k2"]),
    );
    let body = json!({
        "RequestItems": {"T": {"Keys": keys_json(&["k1", "k2"])}}
    });

    let err = err_of(handle_batch_get_item(body, &ctx(store)).await);
    assert_eq!(err.status_code(), 500);
    assert_eq!(err.error_type(), "InternalServerError");
}

// ---------------------------------------------------------------------------
// BatchWriteItem
// ---------------------------------------------------------------------------

/// Puts and deletes, some throttled: 200, the throttled entries echoed as
/// sent under `UnprocessedItems[<table>]`, `ConsumedCapacity` and
/// `ItemCollectionMetrics` for the applied entries only, and the backend saw
/// exactly the applied writes.
#[tokio::test]
async fn batch_write_partial_echoes_entries_and_charges_applied_only() {
    let store = Arc::new(FailingStore::throttling_writes().failing_on(&["p2", "d2"]));
    let body = json!({
        "RequestItems": {
            "T": [
                {"PutRequest": {"Item": {"pk": {"S": "p1"}, "v": {"N": "1"}}}},
                {"PutRequest": {"Item": {"pk": {"S": "p2"}, "v": {"N": "2"}}}},
                {"DeleteRequest": {"Key": {"pk": {"S": "d1"}}}},
                {"DeleteRequest": {"Key": {"pk": {"S": "d2"}}}}
            ]
        },
        "ReturnConsumedCapacity": "TOTAL",
        "ReturnItemCollectionMetrics": "SIZE"
    });

    let result = handle_batch_write_item(body, &ctx(Arc::clone(&store)))
        .await
        .expect("partial success is a 200");

    let unprocessed = result.body["UnprocessedItems"]["T"]
        .as_array()
        .expect("list");
    assert_eq!(unprocessed.len(), 2);
    assert!(unprocessed.contains(&json!({
        "PutRequest": {"Item": {"pk": {"S": "p2"}, "v": {"N": "2"}}}
    })));
    assert!(unprocessed.contains(&json!({
        "DeleteRequest": {"Key": {"pk": {"S": "d2"}}}
    })));
    assert_eq!(
        result.body["UnprocessedItems"]
            .as_object()
            .expect("map")
            .len(),
        1
    );

    // Two applied writes under 1 KB: 1 unit each.
    assert_eq!(
        result.body["ConsumedCapacity"],
        json!([{"TableName": "T", "CapacityUnits": 2.0}])
    );
    assert_eq!(result.metrics.write_capacity_units, 2.0);

    let icm: BTreeSet<String> = result.body["ItemCollectionMetrics"]["T"]
        .as_array()
        .expect("metrics list")
        .iter()
        .map(|m| {
            m["ItemCollectionKey"]["pk"]["S"]
                .as_str()
                .expect("pk")
                .to_owned()
        })
        .collect();
    assert_eq!(icm, ["p1", "d1"].map(str::to_owned).into());

    assert_eq!(
        store.applied(),
        vec![
            Applied::Put("p1".to_owned()),
            Applied::Delete("d1".to_owned())
        ]
    );
}

/// Every entry throttled: the call fails with the throttle error and nothing
/// was applied.
#[tokio::test]
async fn batch_write_every_entry_throttled_fails_with_the_throttle_error() {
    let store = Arc::new(FailingStore::throttling_writes().failing_on(&["p1", "d1"]));
    let body = json!({
        "RequestItems": {
            "T": [{"PutRequest": {"Item": {"pk": {"S": "p1"}}}}],
            "U": [{"DeleteRequest": {"Key": {"pk": {"S": "d1"}}}}]
        }
    });

    let err = err_of(handle_batch_write_item(body, &ctx(Arc::clone(&store))).await);
    assert_throttle(&err, "TableWriteProvisionedThroughputExceeded");
    assert!(store.applied().is_empty());
}

/// The boundary of the all-throttled rule on writes: one entry applied and
/// the rest throttled is a 200 with that one write applied and the others
/// echoed under `UnprocessedItems`.
#[tokio::test]
async fn batch_write_one_applied_entry_is_a_partial_success() {
    let store = Arc::new(FailingStore::throttling_writes().failing_on(&["p1", "p2"]));
    let body = json!({
        "RequestItems": {
            "T": [
                {"PutRequest": {"Item": {"pk": {"S": "p1"}}}},
                {"PutRequest": {"Item": {"pk": {"S": "p2"}}}},
                {"PutRequest": {"Item": {"pk": {"S": "p3"}}}}
            ]
        },
        "ReturnConsumedCapacity": "TOTAL"
    });
    let result = handle_batch_write_item(body, &ctx(Arc::clone(&store)))
        .await
        .expect("one entry applied is a 200");
    assert_eq!(store.applied(), vec![Applied::Put("p3".to_owned())]);
    let unprocessed = result.body["UnprocessedItems"]["T"]
        .as_array()
        .expect("list");
    assert_eq!(unprocessed.len(), 2);
    assert_eq!(
        result.body["ConsumedCapacity"],
        json!([{"TableName": "T", "CapacityUnits": 1.0}])
    );
}

/// Nothing throttled: `UnprocessedItems` is present as `{}` (13 of 13 fully
/// processed responses measured on 2026-10-21 carried it).
#[tokio::test]
async fn batch_write_nothing_throttled_serializes_empty_unprocessed_items() {
    let store = Arc::new(FailingStore::throttling());
    let body = json!({
        "RequestItems": {
            "T": [{"PutRequest": {"Item": {"pk": {"S": "p1"}}}}]
        }
    });

    let result = handle_batch_write_item(body, &ctx(store))
        .await
        .expect("200");
    let serialized = serde_json::to_string(&result.body).expect("serialize");
    assert!(
        serialized.contains(r#""UnprocessedItems":{}"#),
        "body: {serialized}"
    );
    assert!(
        !serialized.contains("ItemCollectionMetrics"),
        "no SIZE requested, the field is absent: {serialized}"
    );
}

/// A fully processed batch that asked for `ReturnItemCollectionMetrics: SIZE`
/// on a table without a local secondary index carries
/// `"ItemCollectionMetrics": {}`, the field present and empty (measured 13 of
/// 13 on 2026-10-21).
#[tokio::test]
async fn batch_write_size_requested_without_collections_serializes_empty_metrics() {
    let store = Arc::new(FailingStore::throttling());
    let body = json!({
        "RequestItems": {
            "U": [{"PutRequest": {"Item": {"pk": {"S": "p1"}}}}]
        },
        "ReturnItemCollectionMetrics": "SIZE"
    });

    let result = handle_batch_write_item(body, &ctx(store))
        .await
        .expect("200");
    let serialized = serde_json::to_string(&result.body).expect("serialize");
    assert!(
        serialized.contains(r#""ItemCollectionMetrics":{}"#),
        "body: {serialized}"
    );
}

/// A storage `Service` error that is not a throttle still fails the request.
#[tokio::test]
async fn batch_write_non_throttle_service_error_fails_the_request() {
    let store = Arc::new(
        FailingStore::new(DynamoDbError::InternalServerError(
            "Internal server error".to_owned(),
        ))
        .failing_on(&["p2"]),
    );
    let body = json!({
        "RequestItems": {
            "T": [
                {"PutRequest": {"Item": {"pk": {"S": "p1"}}}},
                {"PutRequest": {"Item": {"pk": {"S": "p2"}}}}
            ]
        }
    });

    let err = err_of(handle_batch_write_item(body, &ctx(store)).await);
    assert_eq!(err.status_code(), 500);
    assert_eq!(err.error_type(), "InternalServerError");
}
