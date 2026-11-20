// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for the old image a transacted mutation records,
//! and for a transacted record against the single-item record of the same verb.
//!
//! Helpers are shared with `transact_write_stream_records.rs`, which covers the
//! event names, the view types and the ops that record nothing.

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeValue, Item, ReturnValuesOnConditionCheckFailure, StreamViewType,
};
use extenddb_storage::{DataEngine, TransactWriteOp};
use extenddb_storage_cassandra::CassandraEngine;

use crate::helpers::{TestTable, setup_engine, unique_test_account};
use crate::transact_write_stream_records::{
    capture, item_with, key_for, records_for_keys, set_val_action,
};

/// One transacted Put of `item`, with a `NEW_AND_OLD_IMAGES` capture.
async fn transacted_put(engine: &CassandraEngine, table: &TestTable, item: &Item) {
    let maps = ExpressionMaps::default();
    let ops = vec![TransactWriteOp::Put {
        key_info: &table.key_info,
        item,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: Some(capture(StreamViewType::NewAndOldImages)),
    }];
    engine.transact_write_items(&ops, None).await.unwrap();
}

/// Write a row shaped like the one a PREPARE plants for an absent key:
/// `created_to_prepare` set and `item_data` holding a placeholder rather than a
/// committed item, with no `last_committed_txn_timestamp`. The transaction under
/// test then PREPAREs this row like any other, which sets `prepared_txn_id`.
///
/// A commit-time read of `item_data` alone cannot tell this row from a row
/// holding a committed item, and PREPARE's own read cannot tell it either. The
/// record's old image is decided from the flag and the commit timestamp
/// instead.
async fn plant_prepare_created_row(engine: &CassandraEngine, table: &TestTable, item: &Item) {
    let keyspace = format!("extenddb_ttl_test_account_{}", table.key_info.account_id);
    let data_table = format!("items_{}", table.key_info.table_id.replace('-', "_"));
    let AttributeValue::S(pk) = item.get("id").unwrap() else {
        panic!("the test tables key on a string `id`");
    };
    let item_data = serde_json::to_string(item).unwrap();
    let query = format!(
        "INSERT INTO {keyspace}.{data_table} (pk, item_data, created_to_prepare) \
         VALUES (?, ?, true)"
    );
    engine
        .session_arc()
        .query_with_values(
            &query,
            cdrs_tokio::query_values!(pk.as_str(), item_data.as_str()),
        )
        .await
        .unwrap();
}

/// A transacted Put over a key that already holds an item records MODIFY with
/// the stored item as `OldImage`, as an overwriting `PutItem` does.
#[tokio::test]
async fn test_transact_write_put_over_existing_records_modify() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnStreamPutOverwrite", false).await;
    let maps = ExpressionMaps::default();

    engine
        .put_item(
            &table.key_info,
            item_with("ovr-1", "before"),
            false,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();

    transacted_put(&engine, &table, &item_with("ovr-1", "after")).await;

    let records = records_for_keys(
        &engine,
        &table.key_info.account_id,
        &table.key_info.table_id,
        &["ovr-1"],
    )
    .await;
    assert_eq!(records.len(), 1, "got {records:?}");
    assert_eq!(records[0].1, "Modify");
    assert_eq!(records[0].2["eventName"], "MODIFY");
    assert_eq!(records[0].2["dynamodb"]["OldImage"]["val"]["S"], "before");
    assert_eq!(records[0].2["dynamodb"]["NewImage"]["val"]["S"], "after");
}

/// A transacted Put over a row that only exists because a PREPARE planted it
/// records INSERT: that row holds a placeholder, not a committed item.
#[tokio::test]
async fn test_transact_write_put_over_prepare_created_row_records_insert() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnStreamPrepareMarker", false).await;

    plant_prepare_created_row(&engine, &table, &item_with("mark-1", "placeholder")).await;

    transacted_put(&engine, &table, &item_with("mark-1", "committed")).await;

    let records = records_for_keys(
        &engine,
        &table.key_info.account_id,
        &table.key_info.table_id,
        &["mark-1"],
    )
    .await;
    assert_eq!(records.len(), 1, "got {records:?}");
    assert_eq!(records[0].1, "Insert");
    assert_eq!(records[0].2["eventName"], "INSERT");
    assert!(
        records[0].2["dynamodb"].get("OldImage").is_none(),
        "a planted row is not an old image: {records:?}"
    );
    assert_eq!(
        records[0].2["dynamodb"]["NewImage"]["val"]["S"],
        "committed"
    );
}

/// A second transacted Put of a key an earlier transaction created records
/// MODIFY: committing clears `created_to_prepare`, so the row is a committed
/// item from then on.
#[tokio::test]
async fn test_transact_write_put_over_transacted_key_records_modify() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnStreamPutTwice", false).await;

    transacted_put(&engine, &table, &item_with("twice-1", "first")).await;
    transacted_put(&engine, &table, &item_with("twice-1", "second")).await;

    let records = records_for_keys(
        &engine,
        &table.key_info.account_id,
        &table.key_info.table_id,
        &["twice-1"],
    )
    .await;
    assert_eq!(records.len(), 2, "got {records:?}");
    let events: Vec<&str> = records.iter().map(|(_, event, _)| event.as_str()).collect();
    assert_eq!(events, vec!["Insert", "Modify"]);
    assert_eq!(records[1].2["dynamodb"]["OldImage"]["val"]["S"], "first");
    assert_eq!(records[1].2["dynamodb"]["NewImage"]["val"]["S"], "second");
}

/// The one record for `key`, as `record_data`.
fn record_for(records: &[(String, String, serde_json::Value)], key: &str) -> serde_json::Value {
    let matching: Vec<&serde_json::Value> = records
        .iter()
        .filter(|(_, _, data)| data["dynamodb"]["Keys"]["id"]["S"] == key)
        .map(|(_, _, data)| data)
        .collect();
    assert_eq!(matching.len(), 1, "one record for {key}: {matching:?}");
    matching[0].clone()
}

/// A record without the fields that identify the write rather than describe it.
fn without_identity(record: &serde_json::Value) -> serde_json::Value {
    let mut record = record.clone();
    let top = record.as_object_mut().unwrap();
    top.remove("eventID");
    let data = top.get_mut("dynamodb").unwrap().as_object_mut().unwrap();
    data.remove("SequenceNumber");
    data.remove("ApproximateCreationDateTime");
    record
}

/// A transacted Put, Update and Delete record what the equivalent `PutItem`,
/// `UpdateItem` and `DeleteItem` record: the same fields with the same values,
/// once the per-write identity fields are removed.
///
/// Two tables in one account carry the same keys and the same items, so the
/// records differ in nothing but which path wrote them.
#[tokio::test]
async fn test_transacted_records_match_the_single_item_records() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account_id = unique_test_account();
    let solo = TestTable::with_account(&engine, &account_id, "TxnStreamSoloPath", false).await;
    let txn = TestTable::with_account(&engine, &account_id, "TxnStreamTxnPath", false).await;
    let maps = ExpressionMaps::default();
    let view = capture(StreamViewType::NewAndOldImages);

    // Seed the keys the update and the delete act on, on both tables, without a
    // capture, so the only records are the ones under comparison.
    for table in [&solo, &txn] {
        for (id, val) in [("k-upd", "before"), ("k-del", "doomed")] {
            engine
                .put_item(
                    &table.key_info,
                    item_with(id, val),
                    false,
                    None,
                    &maps,
                    None,
                )
                .await
                .unwrap();
        }
    }

    let created = item_with("k-put", "created");
    let (actions, update_maps) = set_val_action("after");

    engine
        .put_item(
            &solo.key_info,
            created.clone(),
            false,
            None,
            &maps,
            Some(&view),
        )
        .await
        .unwrap();
    engine
        .update_item(
            &solo.key_info,
            &key_for("k-upd"),
            &actions,
            false,
            false,
            None,
            &update_maps,
            Some(&view),
        )
        .await
        .unwrap();
    engine
        .delete_item(
            &solo.key_info,
            &key_for("k-del"),
            false,
            None,
            &maps,
            Some(&view),
        )
        .await
        .unwrap();

    let update_key = key_for("k-upd");
    let delete_key = key_for("k-del");
    let ops = vec![
        TransactWriteOp::Put {
            key_info: &txn.key_info,
            item: &created,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(view.clone()),
        },
        TransactWriteOp::Update {
            key_info: &txn.key_info,
            key: &update_key,
            actions: &actions,
            condition: None,
            maps: &update_maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(view.clone()),
        },
        TransactWriteOp::Delete {
            key_info: &txn.key_info,
            key: &delete_key,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(view.clone()),
        },
    ];
    engine.transact_write_items(&ops, None).await.unwrap();

    let keys = ["k-put", "k-upd", "k-del"];
    let solo_records = records_for_keys(&engine, &account_id, &solo.key_info.table_id, &keys).await;
    let txn_records = records_for_keys(&engine, &account_id, &txn.key_info.table_id, &keys).await;
    assert_eq!(solo_records.len(), 3, "got {solo_records:?}");
    assert_eq!(txn_records.len(), 3, "got {txn_records:?}");

    for key in keys {
        let single = record_for(&solo_records, key);
        let transacted = record_for(&txn_records, key);
        assert_eq!(
            without_identity(&single),
            without_identity(&transacted),
            "the records for {key} differ"
        );
    }
}

/// Puts the row into the state a release before this branch leaves after a
/// transacted commit: committed `item_data`, `prepared_txn_id` null,
/// `last_committed_txn_timestamp` set, and `created_to_prepare` still true,
/// since that release's commit statement did not clear the flag. The single
/// `UPDATE` here is the only difference between that statement and this
/// branch's, so the row is what an upgraded database holds.
async fn leave_flag_set_as_an_earlier_release_did(
    engine: &CassandraEngine,
    table: &TestTable,
    id: &str,
) {
    let keyspace = format!("extenddb_ttl_test_account_{}", table.key_info.account_id);
    let data_table = format!("items_{}", table.key_info.table_id.replace('-', "_"));
    let query =
        format!("UPDATE {keyspace}.{data_table} SET created_to_prepare = true WHERE pk = ?");
    engine
        .session_arc()
        .query_with_values(&query, cdrs_tokio::query_values!(id))
        .await
        .unwrap();
}

/// A transacted Delete of an item committed by an earlier release records
/// REMOVE: the row carries the flag and a commit timestamp, and the timestamp
/// says it is a committed item, not a placeholder.
#[tokio::test]
async fn test_transact_write_delete_of_an_item_committed_before_the_flag_was_cleared_records_remove()
 {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnStreamLegacyDelete", false).await;
    let maps = ExpressionMaps::default();

    transacted_put(&engine, &table, &item_with("leg-del", "committed")).await;
    leave_flag_set_as_an_earlier_release_did(&engine, &table, "leg-del").await;

    let key = key_for("leg-del");
    let ops = vec![TransactWriteOp::Delete {
        key_info: &table.key_info,
        key: &key,
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
        &["leg-del"],
    )
    .await;
    let names: Vec<&str> = records.iter().map(|r| r.1.as_str()).collect();
    assert_eq!(
        names,
        vec!["Insert", "Remove"],
        "a transacted delete of a committed item records REMOVE: {records:?}"
    );
}

/// A transacted Put over an item committed by an earlier release records
/// MODIFY with the committed item as `OldImage`.
#[tokio::test]
async fn test_transact_write_put_over_an_item_committed_before_the_flag_was_cleared_records_modify()
{
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnStreamLegacyPut", false).await;

    transacted_put(&engine, &table, &item_with("leg-put", "first")).await;
    leave_flag_set_as_an_earlier_release_did(&engine, &table, "leg-put").await;

    transacted_put(&engine, &table, &item_with("leg-put", "second")).await;

    let records = records_for_keys(
        &engine,
        &table.key_info.account_id,
        &table.key_info.table_id,
        &["leg-put"],
    )
    .await;
    let names: Vec<&str> = records.iter().map(|r| r.1.as_str()).collect();
    assert_eq!(
        names,
        vec!["Insert", "Modify"],
        "a transacted put over a committed item records MODIFY: {records:?}"
    );
    assert_eq!(
        records[1].2["dynamodb"]["OldImage"]["val"]["S"], "first",
        "the committed item is the old image: {records:?}"
    );
}
