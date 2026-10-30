// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for stream records written by `TransactWriteItems`.

use std::collections::HashMap;
use std::sync::Arc;

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::expression::{Expr, ExpressionMaps, PathElement, UpdateAction};
use extenddb_core::types::{
    AttributeValue, Item, ReturnValuesOnConditionCheckFailure, StreamViewType,
};
use extenddb_storage::{DataEngine, StreamCapture, TransactWriteOp};
use extenddb_storage_cassandra::CassandraEngine;

use crate::helpers::{TestTable, setup_engine};

pub(crate) fn capture(view_type: StreamViewType) -> StreamCapture {
    StreamCapture {
        view_type,
        user_identity: None,
        region: Arc::from("us-east-1"),
    }
}

pub(crate) fn item_with(id: &str, val: &str) -> Item {
    let mut item = Item::new();
    item.insert("id".to_owned(), AttributeValue::S(id.to_owned()));
    item.insert("val".to_owned(), AttributeValue::S(val.to_owned()));
    item
}

pub(crate) fn key_for(id: &str) -> Item {
    let mut key = Item::new();
    key.insert("id".to_owned(), AttributeValue::S(id.to_owned()));
    key
}

/// `attribute_not_exists(id)`, the condition used to cancel a transaction on a
/// key that does exist.
fn attribute_not_exists_id() -> Expr {
    Expr::Function {
        name: "attribute_not_exists".to_owned(),
        args: vec![Expr::Path(vec![PathElement::Attribute("id".to_owned())])],
    }
}

/// `attribute_exists(id)`, a condition that holds for a key that is present.
fn attribute_exists_id() -> Expr {
    Expr::Function {
        name: "attribute_exists".to_owned(),
        args: vec![Expr::Path(vec![PathElement::Attribute("id".to_owned())])],
    }
}

/// `SET val = :after`, with the map that resolves the placeholder.
pub(crate) fn set_val_action(value: &str) -> (Vec<UpdateAction>, ExpressionMaps) {
    let mut values = HashMap::new();
    values.insert("after".to_owned(), AttributeValue::S(value.to_owned()));
    let actions = vec![UpdateAction::Set {
        path: vec![PathElement::Attribute("val".to_owned())],
        value: Expr::Placeholder("after".to_owned()),
    }];
    (actions, ExpressionMaps::new(HashMap::new(), values))
}

/// All stream records for the given partition keys, as `(sequence_number,
/// event_name, record_data)` sorted by sequence number.
///
/// The shards come from `stream_util::assign_shard_id`, the function the write
/// paths mint the record's shard with, so a change in shard naming fails the
/// assertions below rather than returning no records at all.
pub(crate) async fn records_for_keys(
    engine: &CassandraEngine,
    account_id: &str,
    table_id: &str,
    keys: &[&str],
) -> Vec<(String, String, serde_json::Value)> {
    let keyspace = format!("extenddb_ttl_test_account_{account_id}");
    let mut shard_ids: Vec<String> = keys
        .iter()
        .map(|key| extenddb_storage_cassandra::stream_util::assign_shard_id(key, table_id))
        .collect();
    shard_ids.sort();
    shard_ids.dedup();

    let mut rows = Vec::new();
    for shard_id in shard_ids {
        let query = format!(
            "SELECT sequence_number, event_name, record_data FROM {keyspace}.stream_records \
             WHERE shard_id = ?"
        );
        let result = engine
            .session_arc()
            .query_with_values(&query, cdrs_tokio::query_values!(shard_id.as_str()))
            .await
            .unwrap();
        let body = result.response_body().unwrap();
        for row in body.into_rows().unwrap_or_default() {
            let sequence_number: String = row.get_r_by_name("sequence_number").unwrap();
            let event_name: String = row.get_r_by_name("event_name").unwrap();
            let data: String = row.get_r_by_name("record_data").unwrap();
            rows.push((
                sequence_number,
                event_name,
                serde_json::from_str(&data).unwrap(),
            ));
        }
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

/// A transaction carrying a Put, an Update and a Delete on a streamed table
/// records one record per item mutation, in request order, with the event names
/// and images the single-item writes produce.
#[tokio::test]
async fn test_transact_write_records_one_stream_record_per_mutation() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnStreamAllVerbs", false).await;
    one_stream_record_per_mutation(&engine, &table, item_with, key_for).await;
}

/// The scenario of the test above on a table of any key shape. `item` and `key`
/// build the item and the key for an id, with whatever sort key or index
/// attribute the table needs. Returns the records the transaction wrote.
pub(crate) async fn one_stream_record_per_mutation(
    engine: &CassandraEngine,
    table: &TestTable,
    item: impl Fn(&str, &str) -> Item,
    key: impl Fn(&str) -> Item,
) -> Vec<(String, String, serde_json::Value)> {
    let maps = ExpressionMaps::default();

    // Seed the keys the Update and the Delete act on, without a capture, so the
    // only records in the table are the transaction's.
    for (id, val) in [("upd-1", "before"), ("del-1", "doomed")] {
        engine
            .put_item(&table.key_info, item(id, val), false, None, &maps, None)
            .await
            .unwrap();
    }

    let put_item = item("put-1", "created");
    let update_key = key("upd-1");
    let delete_key = key("del-1");
    let (actions, update_maps) = set_val_action("after");
    let view = capture(StreamViewType::NewAndOldImages);

    let ops = vec![
        TransactWriteOp::Put {
            key_info: &table.key_info,
            item: &put_item,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(view.clone()),
        },
        TransactWriteOp::Update {
            key_info: &table.key_info,
            key: &update_key,
            actions: &actions,
            condition: None,
            maps: &update_maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(view.clone()),
        },
        TransactWriteOp::Delete {
            key_info: &table.key_info,
            key: &delete_key,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(view.clone()),
        },
    ];

    engine.transact_write_items(&ops, None).await.unwrap();

    let records = records_for_keys(
        engine,
        &table.key_info.account_id,
        &table.key_info.table_id,
        &["put-1", "upd-1", "del-1"],
    )
    .await;

    assert_eq!(
        records.len(),
        3,
        "expected one record per item mutation, got {records:?}"
    );

    let events: Vec<&str> = records.iter().map(|(_, event, _)| event.as_str()).collect();
    assert_eq!(events, vec!["Insert", "Modify", "Remove"]);

    let (_, _, insert) = &records[0];
    assert_eq!(insert["eventName"], "INSERT");
    assert_eq!(insert["dynamodb"]["Keys"]["id"]["S"], "put-1");
    assert_eq!(insert["dynamodb"]["NewImage"]["val"]["S"], "created");
    assert!(insert["dynamodb"].get("OldImage").is_none());

    let (_, _, modify) = &records[1];
    assert_eq!(modify["eventName"], "MODIFY");
    assert_eq!(modify["dynamodb"]["Keys"]["id"]["S"], "upd-1");
    assert_eq!(modify["dynamodb"]["OldImage"]["val"]["S"], "before");
    assert_eq!(modify["dynamodb"]["NewImage"]["val"]["S"], "after");

    let (_, _, remove) = &records[2];
    assert_eq!(remove["eventName"], "REMOVE");
    assert_eq!(remove["dynamodb"]["Keys"]["id"]["S"], "del-1");
    assert_eq!(remove["dynamodb"]["OldImage"]["val"]["S"], "doomed");
    assert!(remove["dynamodb"].get("NewImage").is_none());

    for (_, _, record) in &records {
        assert_eq!(record["eventSource"], "aws:dynamodb");
        assert_eq!(record["awsRegion"], "us-east-1");
        assert_eq!(
            record["dynamodb"]["StreamViewType"], "NEW_AND_OLD_IMAGES",
            "record carries the capture's view type"
        );
    }
    records
}

/// Two mutations on the same streamed table record two records, ordered as the
/// request lists them.
///
/// This pins ExtendDB's own behaviour on every backend. The service makes no
/// such promise: one `TransactWriteItems` of a Put to `k5` and a Put to `k7`
/// that share a shard came back `k7`, `k5` (measured 2026-10-25), so a
/// consumer can rely on the order of one key's records and not on the order of
/// a transaction's.
#[tokio::test]
async fn test_transact_write_records_two_mutations_in_request_order() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnStreamTwoPuts", false).await;
    let maps = ExpressionMaps::default();
    let view = capture(StreamViewType::NewAndOldImages);

    let first = item_with("ord-a", "first");
    let second = item_with("ord-b", "second");
    let ops = vec![
        TransactWriteOp::Put {
            key_info: &table.key_info,
            item: &first,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(view.clone()),
        },
        TransactWriteOp::Put {
            key_info: &table.key_info,
            item: &second,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(view.clone()),
        },
    ];

    engine.transact_write_items(&ops, None).await.unwrap();

    let records = records_for_keys(
        &engine,
        &table.key_info.account_id,
        &table.key_info.table_id,
        &["ord-a", "ord-b"],
    )
    .await;

    assert_eq!(records.len(), 2, "got {records:?}");
    // The records are sorted by sequence number, so the keys in this order are
    // the request's order.
    assert_eq!(records[0].2["dynamodb"]["Keys"]["id"]["S"], "ord-a");
    assert_eq!(records[1].2["dynamodb"]["Keys"]["id"]["S"], "ord-b");
}

/// A transaction spanning a streamed and an unstreamed table records the
/// streamed table's mutation only.
#[tokio::test]
async fn test_transact_write_records_streamed_table_only() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account_id = crate::helpers::unique_test_account();
    let streamed = TestTable::with_account(&engine, &account_id, "TxnStreamOn", false).await;
    let plain = TestTable::with_account(&engine, &account_id, "TxnStreamOff", false).await;
    let maps = ExpressionMaps::default();

    let streamed_item = item_with("on-1", "recorded");
    let plain_item = item_with("off-1", "silent");
    let ops = vec![
        TransactWriteOp::Put {
            key_info: &streamed.key_info,
            item: &streamed_item,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(capture(StreamViewType::NewAndOldImages)),
        },
        TransactWriteOp::Put {
            key_info: &plain.key_info,
            item: &plain_item,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
    ];

    engine.transact_write_items(&ops, None).await.unwrap();

    let streamed_records =
        records_for_keys(&engine, &account_id, &streamed.key_info.table_id, &["on-1"]).await;
    assert_eq!(streamed_records.len(), 1, "got {streamed_records:?}");
    assert_eq!(streamed_records[0].2["dynamodb"]["Keys"]["id"]["S"], "on-1");

    let plain_records =
        records_for_keys(&engine, &account_id, &plain.key_info.table_id, &["off-1"]).await;
    assert!(
        plain_records.is_empty(),
        "unstreamed table recorded {plain_records:?}"
    );
}

/// A `KEYS_ONLY` stream records the keys and neither image.
#[tokio::test]
async fn test_transact_write_keys_only_view_omits_images() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnStreamKeysOnly", false).await;
    let maps = ExpressionMaps::default();

    let item = item_with("keys-1", "hidden");
    let ops = vec![TransactWriteOp::Put {
        key_info: &table.key_info,
        item: &item,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: Some(capture(StreamViewType::KeysOnly)),
    }];

    engine.transact_write_items(&ops, None).await.unwrap();

    let records = records_for_keys(
        &engine,
        &table.key_info.account_id,
        &table.key_info.table_id,
        &["keys-1"],
    )
    .await;
    assert_eq!(records.len(), 1, "got {records:?}");
    assert_eq!(records[0].2["dynamodb"]["Keys"]["id"]["S"], "keys-1");
    assert!(records[0].2["dynamodb"].get("NewImage").is_none());
    assert!(records[0].2["dynamodb"].get("OldImage").is_none());
    assert_eq!(records[0].2["dynamodb"]["StreamViewType"], "KEYS_ONLY");
}

/// A `ConditionCheck` records nothing, while the Put beside it records.
#[tokio::test]
async fn test_transact_write_condition_check_records_nothing() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnStreamCheck", false).await;
    let maps = ExpressionMaps::default();

    engine
        .put_item(
            &table.key_info,
            item_with("chk-1", "present"),
            false,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();

    let check_key = key_for("chk-1");
    let put_item = item_with("chk-2", "written");
    let exists = attribute_exists_id();
    let ops = vec![
        TransactWriteOp::ConditionCheck {
            key_info: &table.key_info,
            key: &check_key,
            condition: &exists,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        },
        TransactWriteOp::Put {
            key_info: &table.key_info,
            item: &put_item,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(capture(StreamViewType::NewAndOldImages)),
        },
    ];

    engine.transact_write_items(&ops, None).await.unwrap();

    let records = records_for_keys(
        &engine,
        &table.key_info.account_id,
        &table.key_info.table_id,
        &["chk-1", "chk-2"],
    )
    .await;
    assert_eq!(records.len(), 1, "got {records:?}");
    assert_eq!(records[0].2["dynamodb"]["Keys"]["id"]["S"], "chk-2");
}

/// A transaction cancelled by a condition failure records nothing for any of
/// its items, including the ones whose conditions held.
#[tokio::test]
async fn test_cancelled_transaction_records_nothing() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnStreamCancelled", false).await;
    let maps = ExpressionMaps::default();

    engine
        .put_item(
            &table.key_info,
            item_with("taken", "present"),
            false,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();

    let free_item = item_with("free", "would-be-written");
    let taken_item = item_with("taken", "would-overwrite");
    let condition = attribute_not_exists_id();
    let view = capture(StreamViewType::NewAndOldImages);

    let ops = vec![
        TransactWriteOp::Put {
            key_info: &table.key_info,
            item: &free_item,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(view.clone()),
        },
        // attribute_not_exists(id) against a key that exists: cancels the
        // transaction.
        TransactWriteOp::Put {
            key_info: &table.key_info,
            item: &taken_item,
            condition: Some(&condition),
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(view.clone()),
        },
    ];

    let result = engine.transact_write_items(&ops, None).await;
    assert!(
        matches!(
            result,
            Err(extenddb_storage::error::StorageError::TransactionCanceled(
                _
            ))
        ),
        "expected cancellation, got {result:?}"
    );

    let records = records_for_keys(
        &engine,
        &table.key_info.account_id,
        &table.key_info.table_id,
        &["free", "taken"],
    )
    .await;
    assert!(
        records.is_empty(),
        "a cancelled transaction recorded {records:?}"
    );

    // The uncancelled op's item is not written either.
    assert!(
        engine
            .get_item(&table.key_info, &key_for("free"))
            .await
            .unwrap()
            .is_none(),
        "cancelled transaction wrote an item"
    );
}

/// A transacted Update of a key that does not exist creates the item and
/// records INSERT with no old image, as `UpdateItem` does.
#[tokio::test]
async fn test_transact_write_update_of_absent_key_records_insert() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnStreamUpdInsert", false).await;

    let key = key_for("upd-new");
    let (actions, update_maps) = set_val_action("created-by-update");
    let ops = vec![TransactWriteOp::Update {
        key_info: &table.key_info,
        key: &key,
        actions: &actions,
        condition: None,
        maps: &update_maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: Some(capture(StreamViewType::NewAndOldImages)),
    }];

    engine.transact_write_items(&ops, None).await.unwrap();

    let records = records_for_keys(
        &engine,
        &table.key_info.account_id,
        &table.key_info.table_id,
        &["upd-new"],
    )
    .await;
    assert_eq!(records.len(), 1, "got {records:?}");
    assert_eq!(records[0].1, "Insert");
    assert_eq!(records[0].2["eventName"], "INSERT");
    assert_eq!(
        records[0].2["dynamodb"]["NewImage"]["val"]["S"],
        "created-by-update"
    );
    assert!(records[0].2["dynamodb"].get("OldImage").is_none());
}

/// A transacted delete of a key that was already absent records nothing, as the
/// single-item delete does.
#[tokio::test]
async fn test_transact_write_delete_of_absent_key_records_nothing() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnStreamDeleteGhost", false).await;
    let maps = ExpressionMaps::default();

    let ghost = key_for("ghost");
    let ops = vec![TransactWriteOp::Delete {
        key_info: &table.key_info,
        key: &ghost,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: Some(capture(StreamViewType::NewAndOldImages)),
    }];

    engine.transact_write_items(&ops, None).await.unwrap();

    let records = records_for_keys(
        &engine,
        &table.key_info.account_id,
        &table.key_info.table_id,
        &["ghost"],
    )
    .await;
    assert!(records.is_empty(), "got {records:?}");
}
