// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for the batches that carry a transacted mutation's
//! index rows and stream record.
//!
//! Cassandra fails a LOGGED BATCH over several partitions once its data passes
//! `batch_size_fail_threshold`, 50 KiB by default, and the batch runs after the
//! base row's lightweight transaction has committed. A rejection there leaves
//! the item written, writes neither the index rows nor the record, and answers
//! with an error. The record holds the new and the old image of the item, and
//! each index row or GSI queue row holds the item again, so a write of a few
//! tens of kilobytes reaches the limit. These tests run on the default
//! threshold.

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, CreateTableInput, GsiInput, Item, KeySchemaElement,
    KeyType, Projection, ProjectionType, ReturnValuesOnConditionCheckFailure, ScalarAttributeType,
    StreamViewType, TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TableEngine, TransactWriteOp};
use extenddb_storage_cassandra::CassandraEngine;

use crate::helpers::{
    DropKeyspacesOnDrop, TestTable, ensure_test_account, setup_engine, unique_test_account,
};
use crate::transact_write_stream_records::{
    capture, item_with, key_for, records_for_keys, set_val_action,
};

/// An item of about `bytes` bytes. `blob` carries the bulk, `status` and
/// `owner` are the keys of the test tables' indexes, and `val` tells two
/// versions of one item apart.
pub(crate) fn big_item(id: &str, bytes: usize, val: &str) -> Item {
    let mut item = item_with(id, val);
    item.insert("status".to_owned(), AttributeValue::S("s".to_owned()));
    item.insert("owner".to_owned(), AttributeValue::S("o".to_owned()));
    item.insert("blob".to_owned(), AttributeValue::S("x".repeat(bytes)));
    item
}

/// Write a big item without a capture, so it leaves no record.
pub(crate) async fn seed(
    engine: &CassandraEngine,
    key_info: &TableKeyInfo,
    id: &str,
    bytes: usize,
    val: &str,
) {
    let maps = ExpressionMaps::default();
    let item = big_item(id, bytes, val);
    engine
        .put_item(key_info, item, false, None, &maps, None)
        .await
        .unwrap();
}

/// One transacted Put of a big item with a `NEW_AND_OLD_IMAGES` capture.
pub(crate) async fn transacted_put(
    engine: &CassandraEngine,
    key_info: &TableKeyInfo,
    id: &str,
    bytes: usize,
    val: &str,
) -> Result<(), StorageError> {
    let maps = ExpressionMaps::default();
    let item = big_item(id, bytes, val);
    let ops = vec![TransactWriteOp::Put {
        key_info,
        item: &item,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: Some(capture(StreamViewType::NewAndOldImages)),
    }];
    engine.transact_write_items(&ops, None).await
}

/// The records of `keys` as `(sequence_number, event_name, record_data)`.
pub(crate) async fn records_of(
    engine: &CassandraEngine,
    key_info: &TableKeyInfo,
    keys: &[&str],
) -> Vec<(String, String, serde_json::Value)> {
    records_for_keys(engine, &key_info.account_id, &key_info.table_id, keys).await
}

/// The event names recorded for `key`, in sequence order.
pub(crate) fn events_for(
    records: &[(String, String, serde_json::Value)],
    key: &str,
) -> Vec<String> {
    records
        .iter()
        .filter(|(_, _, data)| data["dynamodb"]["Keys"]["id"]["S"] == key)
        .map(|(_, event, _)| event.clone())
        .collect()
}

/// How many transactions the account's ledger still holds. A transaction that
/// committed in full deletes its entry.
pub(crate) async fn ledger_entries(engine: &CassandraEngine, key_info: &TableKeyInfo) -> usize {
    let keyspace = format!("extenddb_ttl_test_account_{}", key_info.account_id);
    engine
        .scan_old_transactions(&keyspace, i64::MAX)
        .await
        .unwrap()
        .len()
}

/// A table keyed on a string `id` with two GSIs, on `status` and `owner`, that
/// each project every attribute. The guard drops the account's keyspace when
/// the test ends.
async fn two_gsi_table(engine: &CassandraEngine) -> (TableKeyInfo, DropKeyspacesOnDrop) {
    let account_id = unique_test_account();
    ensure_test_account(engine, &account_id).await.unwrap();

    let string_attribute = |name: &str| AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type: ScalarAttributeType::S,
    };
    let hash_key = |name: &str| {
        vec![KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type: KeyType::Hash,
        }]
    };
    let gsi = |index: &str, attribute: &str| GsiInput {
        index_name: index.to_owned(),
        key_schema: hash_key(attribute),
        projection: Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    };
    let attribute_definitions = vec![
        string_attribute("id"),
        string_attribute("status"),
        string_attribute("owner"),
    ];
    let input = CreateTableInput {
        table_name: "TxnStreamTwoIndexes".to_owned(),
        key_schema: hash_key("id"),
        attribute_definitions: attribute_definitions.clone(),
        global_secondary_indexes: Some(vec![
            gsi("StatusIndex", "status"),
            gsi("OwnerIndex", "owner"),
        ]),
        ..Default::default()
    };
    let description = engine.create_table(&account_id, input).await.unwrap();

    let key_info = TableKeyInfo {
        table_name: "TxnStreamTwoIndexes".to_owned(),
        account_id: account_id.clone(),
        table_id: description.table_id,
        key_schema: hash_key("id"),
        base_key_schema: hash_key("id"),
        attribute_definitions,
        ..Default::default()
    };
    let keyspace = format!("extenddb_ttl_test_account_{account_id}");
    (key_info, DropKeyspacesOnDrop(vec![keyspace]))
}

/// A transacted Put that overwrites a 24 KB item on a table with a GSI and a
/// `NEW_AND_OLD_IMAGES` stream commits. The record carries both 24 KB images and
/// the index entry carries the item again, which together are over what one
/// batch may hold.
#[tokio::test]
async fn test_transact_write_put_over_a_24kb_item_on_an_indexed_table_records_modify() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::with_gsi(&engine, "TxnStreamBigPut", "StatusIndex", "status").await;
    let key_info = &table.key_info;
    seed(&engine, key_info, "big-1", 24_000, "before").await;

    let result = transacted_put(&engine, key_info, "big-1", 24_000, "after").await;
    assert!(
        result.is_ok(),
        "a transacted overwrite of a 24 KB item failed: {result:?}"
    );

    let records = records_of(&engine, key_info, &["big-1"]).await;
    assert_eq!(events_for(&records, "big-1"), vec!["Modify"]);
    let data = &records[0].2["dynamodb"];
    assert_eq!(data["OldImage"]["val"]["S"], "before");
    assert_eq!(data["NewImage"]["val"]["S"], "after");
    assert_eq!(
        ledger_entries(&engine, key_info).await,
        0,
        "a committed transaction leaves no ledger entry"
    );
}

/// A transacted Update of a 20 KB item on a table with a GSI commits and
/// records MODIFY with both images. The Update arm hands the index sync the old
/// item as well, so its index entries are larger than a Put's.
#[tokio::test]
async fn test_transact_write_update_of_a_20kb_item_on_an_indexed_table_records_modify() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::with_gsi(&engine, "TxnStreamBigUpdate", "StatusIndex", "status").await;
    let key_info = &table.key_info;
    seed(&engine, key_info, "upd-big", 20_000, "before").await;

    let key = key_for("upd-big");
    let (actions, update_maps) = set_val_action("after");
    let ops = vec![TransactWriteOp::Update {
        key_info,
        key: &key,
        actions: &actions,
        condition: None,
        maps: &update_maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: Some(capture(StreamViewType::NewAndOldImages)),
    }];
    let result = engine.transact_write_items(&ops, None).await;
    assert!(
        result.is_ok(),
        "a transacted update of a 20 KB item failed: {result:?}"
    );

    let records = records_of(&engine, key_info, &["upd-big"]).await;
    assert_eq!(events_for(&records, "upd-big"), vec!["Modify"]);
    let data = &records[0].2["dynamodb"];
    assert_eq!(data["OldImage"]["val"]["S"], "before");
    assert_eq!(data["NewImage"]["val"]["S"], "after");
    assert_eq!(ledger_entries(&engine, key_info).await, 0);
}

/// Every index adds a copy of the item to the batch, so a table with two GSIs
/// reaches the limit at a smaller item than a table with one. An 18 KB insert
/// has a 19 KB record and two 19 KB index entries: the index entries alone fit
/// in one batch, and the record on top of them does not.
#[tokio::test]
async fn test_transact_write_insert_of_an_18kb_item_on_a_two_index_table_records_insert() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (key_info, _keyspace_guard) = two_gsi_table(&engine).await;

    let result = transacted_put(&engine, &key_info, "two-1", 18_000, "new").await;
    assert!(
        result.is_ok(),
        "a transacted insert of an 18 KB item failed: {result:?}"
    );

    let records = records_of(&engine, &key_info, &["two-1"]).await;
    assert_eq!(events_for(&records, "two-1"), vec!["Insert"]);
    assert_eq!(ledger_entries(&engine, &key_info).await, 0);
}
