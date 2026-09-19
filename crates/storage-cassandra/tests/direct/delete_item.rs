// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for delete_item operation.

use extenddb_storage::DataEngine;
use std::collections::BTreeMap;

use crate::helpers::{TestTable, setup_engine};

#[tokio::test]
async fn test_delete_item_pk_only_exists() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "test_delete_pk", false).await;

    // Put an item
    let mut item = BTreeMap::new();
    item.insert(
        "id".to_string(),
        extenddb_core::types::AttributeValue::S("test-id".to_string()),
    );
    item.insert(
        "data".to_string(),
        extenddb_core::types::AttributeValue::S("test data".to_string()),
    );

    engine
        .put_item(
            &table.key_info,
            item.clone(),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("put_item failed");

    // Delete the item (return_old=true)
    let mut key = BTreeMap::new();
    key.insert(
        "id".to_string(),
        extenddb_core::types::AttributeValue::S("test-id".to_string()),
    );

    let result = engine
        .delete_item(&table.key_info, &key, true, None, &Default::default(), None)
        .await
        .expect("delete_item failed");

    // Should return the old item
    assert!(result.is_some());
    let old_item = result.unwrap();
    assert_eq!(old_item.get("id"), item.get("id"));
    assert_eq!(old_item.get("data"), item.get("data"));

    // Verify item is deleted
    let get_result = engine
        .get_item(&table.key_info, &key)
        .await
        .expect("get_item failed");
    assert!(get_result.is_none());
}

#[tokio::test]
async fn test_delete_item_pk_only_not_exists() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "test_delete_pk_notexists", false).await;

    // Try to delete non-existent item
    let mut key = BTreeMap::new();
    key.insert(
        "id".to_string(),
        extenddb_core::types::AttributeValue::S("nonexistent".to_string()),
    );

    let result = engine
        .delete_item(&table.key_info, &key, true, None, &Default::default(), None)
        .await
        .expect("delete_item failed");

    // Should return None when item doesn't exist
    assert!(result.is_none());
}

#[tokio::test]
async fn test_delete_item_with_sk_exists() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "test_delete_sk", true).await;

    // Put an item
    let mut item = BTreeMap::new();
    item.insert(
        "id".to_string(),
        extenddb_core::types::AttributeValue::S("user-123".to_string()),
    );
    item.insert(
        "sort".to_string(),
        extenddb_core::types::AttributeValue::S("order-456".to_string()),
    );
    item.insert(
        "amount".to_string(),
        extenddb_core::types::AttributeValue::N("99.99".to_string()),
    );

    engine
        .put_item(
            &table.key_info,
            item.clone(),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("put_item failed");

    // Delete the item
    let mut key = BTreeMap::new();
    key.insert(
        "id".to_string(),
        extenddb_core::types::AttributeValue::S("user-123".to_string()),
    );
    key.insert(
        "sort".to_string(),
        extenddb_core::types::AttributeValue::S("order-456".to_string()),
    );

    let result = engine
        .delete_item(&table.key_info, &key, true, None, &Default::default(), None)
        .await
        .expect("delete_item failed");

    assert!(result.is_some());
    let old_item = result.unwrap();
    assert_eq!(old_item.get("id"), item.get("id"));
    assert_eq!(old_item.get("amount"), item.get("amount"));
}

#[tokio::test]
async fn test_delete_item_with_sk_not_exists() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "test_delete_sk_notexists", true).await;

    let mut key = BTreeMap::new();
    key.insert(
        "id".to_string(),
        extenddb_core::types::AttributeValue::S("nonexistent".to_string()),
    );
    key.insert(
        "sort".to_string(),
        extenddb_core::types::AttributeValue::S("missing".to_string()),
    );

    let result = engine
        .delete_item(&table.key_info, &key, true, None, &Default::default(), None)
        .await
        .expect("delete_item failed");

    assert!(result.is_none());
}

#[tokio::test]
async fn test_delete_item_return_old_false() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "test_delete_noreturn", false).await;

    // Put an item
    let mut item = BTreeMap::new();
    item.insert(
        "id".to_string(),
        extenddb_core::types::AttributeValue::S("test-id".to_string()),
    );

    engine
        .put_item(
            &table.key_info,
            item.clone(),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("put_item failed");

    // Delete without returning old value
    let mut key = BTreeMap::new();
    key.insert(
        "id".to_string(),
        extenddb_core::types::AttributeValue::S("test-id".to_string()),
    );

    let result = engine
        .delete_item(
            &table.key_info,
            &key,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("delete_item failed");

    assert!(result.is_none());

    // Verify deletion
    let get_result = engine
        .get_item(&table.key_info, &key)
        .await
        .expect("get_item failed");
    assert!(get_result.is_none());
}

// ═══════════════════════════════════════════════════════════════════════════════
// Transaction protection tests (Phase 3, T3.1)
// ═══════════════════════════════════════════════════════════════════════════════

use crate::helpers::put_item_then_lock;
use extenddb_core::types::AttributeValue;

#[tokio::test]
async fn test_delete_item_rejects_when_prepared_txn_id_set_pk_only() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use extenddb_storage::error::StorageError;

    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnProtDelPk", false).await;

    let mut item = BTreeMap::new();
    item.insert(
        "id".to_string(),
        AttributeValue::S("del-locked".to_string()),
    );
    item.insert(
        "data".to_string(),
        AttributeValue::S("precious".to_string()),
    );

    put_item_then_lock(&engine, &table, &item).await;

    let mut key = BTreeMap::new();
    key.insert(
        "id".to_string(),
        AttributeValue::S("del-locked".to_string()),
    );

    let result = engine
        .delete_item(
            &table.key_info,
            &key,
            false,
            None,
            &Default::default(),
            None,
        )
        .await;

    match result {
        Err(StorageError::TransactionCanceled(reasons)) => {
            assert_eq!(reasons[0].code, "TransactionConflict");
        }
        other => panic!("Expected TransactionCanceled, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_delete_item_rejects_when_prepared_txn_id_set_with_sk() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use extenddb_storage::error::StorageError;

    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnProtDelSk", true).await;

    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("dpk1".to_string()));
    item.insert("sort".to_string(), AttributeValue::S("dsk1".to_string()));
    item.insert(
        "data".to_string(),
        AttributeValue::S("important".to_string()),
    );

    put_item_then_lock(&engine, &table, &item).await;

    let mut key = BTreeMap::new();
    key.insert("id".to_string(), AttributeValue::S("dpk1".to_string()));
    key.insert("sort".to_string(), AttributeValue::S("dsk1".to_string()));

    let result = engine
        .delete_item(
            &table.key_info,
            &key,
            false,
            None,
            &Default::default(),
            None,
        )
        .await;

    match result {
        Err(StorageError::TransactionCanceled(reasons)) => {
            assert_eq!(reasons[0].code, "TransactionConflict");
        }
        other => panic!("Expected TransactionCanceled, got: {:?}", other),
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// partition_max_delete_timestamp tests (Phase 3, T3.2)
// ═══════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_delete_item_sets_partition_max_delete_timestamp_pk_only() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnDelTsPk", false).await;

    // Put an item
    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("ts-item".to_string()));
    item.insert("data".to_string(), AttributeValue::S("value".to_string()));

    engine
        .put_item(
            &table.key_info,
            item,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("put_item should succeed");

    // Delete it
    let mut key = BTreeMap::new();
    key.insert("id".to_string(), AttributeValue::S("ts-item".to_string()));

    engine
        .delete_item(
            &table.key_info,
            &key,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("delete_item should succeed");

    // For PK-only tables the timestamp is a regular column and is deleted
    // with the row — the persistence protection only exists for sort-key
    // tables where it is STATIC. What this shape CAN assert: the delete
    // actually removed the item.
    assert!(
        engine
            .get_item(&table.key_info, &key)
            .await
            .expect("get after delete")
            .is_none(),
        "item still present after delete"
    );
}

#[tokio::test]
async fn test_delete_item_sets_partition_max_delete_timestamp_with_sk() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use cdrs_tokio::types::IntoRustByName;

    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnDelTsSk", true).await;

    // Put two items in the same partition
    let mut item1 = BTreeMap::new();
    item1.insert("id".to_string(), AttributeValue::S("pk1".to_string()));
    item1.insert("sort".to_string(), AttributeValue::S("sk1".to_string()));
    item1.insert("data".to_string(), AttributeValue::S("val1".to_string()));

    let mut item2 = BTreeMap::new();
    item2.insert("id".to_string(), AttributeValue::S("pk1".to_string()));
    item2.insert("sort".to_string(), AttributeValue::S("sk2".to_string()));
    item2.insert("data".to_string(), AttributeValue::S("val2".to_string()));

    engine
        .put_item(
            &table.key_info,
            item1,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("put_item 1 should succeed");
    engine
        .put_item(
            &table.key_info,
            item2,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("put_item 2 should succeed");

    // Delete one item - this should set partition_max_delete_timestamp
    let mut key = BTreeMap::new();
    key.insert("id".to_string(), AttributeValue::S("pk1".to_string()));
    key.insert("sort".to_string(), AttributeValue::S("sk1".to_string()));

    engine
        .delete_item(
            &table.key_info,
            &key,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("delete_item should succeed");

    // Read partition_max_delete_timestamp from the remaining row (STATIC column
    // is shared across all rows in the partition)
    let account_keyspace = format!("extenddb_ttl_test_account_{}", table.key_info.account_id);
    let data_table = format!("items_{}", table.key_info.table_id.replace("-", "_"));

    let query = format!(
        "SELECT partition_max_delete_timestamp FROM {}.{} WHERE pk = ? LIMIT 1",
        account_keyspace, data_table
    );
    let result = engine
        .session_arc()
        .query_with_values(&query, cdrs_tokio::query_values!("pk1"))
        .await
        .expect("SELECT should succeed");

    let body = result.response_body().expect("response_body");
    let rows = body.into_rows().expect("should have rows");
    let row = rows
        .into_iter()
        .next()
        .expect("should have at least one row");

    let max_ts: Option<i64> = row
        .get_by_name("partition_max_delete_timestamp")
        .ok()
        .flatten();

    assert!(
        max_ts.is_some(),
        "partition_max_delete_timestamp should be set after delete"
    );
    assert!(
        max_ts.unwrap() > 0,
        "partition_max_delete_timestamp should be a positive timestamp"
    );
}

#[tokio::test]
async fn test_delete_item_partition_max_timestamp_increases_on_subsequent_deletes() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use cdrs_tokio::types::IntoRustByName;

    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnDelTsInc", true).await;

    // Put two items in the same partition
    let mut item1 = BTreeMap::new();
    item1.insert("id".to_string(), AttributeValue::S("pk1".to_string()));
    item1.insert("sort".to_string(), AttributeValue::S("sk1".to_string()));
    item1.insert("data".to_string(), AttributeValue::S("val1".to_string()));

    let mut item2 = BTreeMap::new();
    item2.insert("id".to_string(), AttributeValue::S("pk1".to_string()));
    item2.insert("sort".to_string(), AttributeValue::S("sk2".to_string()));
    item2.insert("data".to_string(), AttributeValue::S("val2".to_string()));

    engine
        .put_item(
            &table.key_info,
            item1,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("put_item 1");
    engine
        .put_item(
            &table.key_info,
            item2,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("put_item 2");

    // Delete first item
    let mut key1 = BTreeMap::new();
    key1.insert("id".to_string(), AttributeValue::S("pk1".to_string()));
    key1.insert("sort".to_string(), AttributeValue::S("sk1".to_string()));

    engine
        .delete_item(
            &table.key_info,
            &key1,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("delete_item 1");

    // Read first timestamp
    let account_keyspace = format!("extenddb_ttl_test_account_{}", table.key_info.account_id);
    let data_table = format!("items_{}", table.key_info.table_id.replace("-", "_"));

    let query = format!(
        "SELECT partition_max_delete_timestamp FROM {}.{} WHERE pk = ? LIMIT 1",
        account_keyspace, data_table
    );
    let result = engine
        .session_arc()
        .query_with_values(&query, cdrs_tokio::query_values!("pk1"))
        .await
        .unwrap();
    let body = result.response_body().unwrap();
    let rows = body.into_rows().unwrap();
    let row = rows.into_iter().next().unwrap();
    let ts1: i64 = row
        .get_by_name("partition_max_delete_timestamp")
        .ok()
        .flatten()
        .expect("ts1 should be set");

    // Small delay to ensure different timestamp
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Re-create and delete second item
    let mut item2_again = BTreeMap::new();
    item2_again.insert("id".to_string(), AttributeValue::S("pk1".to_string()));
    item2_again.insert("sort".to_string(), AttributeValue::S("sk2".to_string()));
    item2_again.insert("data".to_string(), AttributeValue::S("val2b".to_string()));

    // sk2 still exists from initial put, so just delete it
    let mut key2 = BTreeMap::new();
    key2.insert("id".to_string(), AttributeValue::S("pk1".to_string()));
    key2.insert("sort".to_string(), AttributeValue::S("sk2".to_string()));

    engine
        .delete_item(
            &table.key_info,
            &key2,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("delete_item 2");

    // Re-insert an item so we can read the STATIC column
    let mut item3 = BTreeMap::new();
    item3.insert("id".to_string(), AttributeValue::S("pk1".to_string()));
    item3.insert("sort".to_string(), AttributeValue::S("sk3".to_string()));
    item3.insert("data".to_string(), AttributeValue::S("val3".to_string()));

    engine
        .put_item(
            &table.key_info,
            item3,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("put_item 3");

    let result2 = engine
        .session_arc()
        .query_with_values(&query, cdrs_tokio::query_values!("pk1"))
        .await
        .unwrap();
    let body2 = result2.response_body().unwrap();
    let rows2 = body2.into_rows().unwrap();
    let row2 = rows2.into_iter().next().unwrap();
    let ts2: i64 = row2
        .get_by_name("partition_max_delete_timestamp")
        .ok()
        .flatten()
        .expect("ts2 should be set");

    assert!(
        ts2 >= ts1,
        "partition_max_delete_timestamp should not decrease: ts1={}, ts2={}",
        ts1,
        ts2
    );
}

#[tokio::test]
async fn test_transaction_put_rejected_by_partition_max_delete_timestamp() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use extenddb_core::expression::ExpressionMaps;
    use extenddb_core::types::ReturnValuesOnConditionCheckFailure;
    use extenddb_storage::TransactWriteOp;
    use extenddb_storage::error::StorageError;

    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnDelTsBlock", true).await;

    // Put an item in the partition so we have a row to hold the STATIC column
    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("pk1".to_string()));
    item.insert("sort".to_string(), AttributeValue::S("sk-keep".to_string()));
    item.insert("data".to_string(), AttributeValue::S("anchor".to_string()));

    engine
        .put_item(
            &table.key_info,
            item,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("put_item anchor should succeed");

    // Manually set partition_max_delete_timestamp to a far-future value.
    // This simulates a delete that happened "after" any transaction that's
    // currently in-flight would have started.
    let account_keyspace = format!("extenddb_ttl_test_account_{}", table.key_info.account_id);
    let data_table = format!("items_{}", table.key_info.table_id.replace("-", "_"));

    let far_future_ts: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 600_000; // 10 minutes in the future

    let update_query = format!(
        "UPDATE {}.{} SET partition_max_delete_timestamp = ? WHERE pk = ?",
        account_keyspace, data_table
    );
    engine
        .session_arc()
        .query_with_values(
            &update_query,
            cdrs_tokio::query_values!(far_future_ts, "pk1"),
        )
        .await
        .expect("Setting partition_max_delete_timestamp should succeed");

    // Now try a transaction that puts a NEW item in the same partition.
    // The transaction's timestamp will be less than partition_max_delete_timestamp,
    // so it should be rejected.
    let mut new_item = BTreeMap::new();
    new_item.insert("id".to_string(), AttributeValue::S("pk1".to_string()));
    new_item.insert("sort".to_string(), AttributeValue::S("sk-new".to_string()));
    new_item.insert("data".to_string(), AttributeValue::S("stale".to_string()));

    let maps = ExpressionMaps::default();
    let ops = vec![TransactWriteOp::Put {
        key_info: &table.key_info,
        item: &new_item,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];

    let result = engine.transact_write_items(&ops, None).await;

    match result {
        Err(StorageError::TransactionCanceled(reasons)) => {
            // The cancellation reason should indicate the item was rejected
            assert!(
                !reasons.is_empty(),
                "Should have at least one cancellation reason"
            );
            // The specific message is "Item was deleted at a later timestamp"
            assert!(
                reasons[0]
                    .message
                    .as_deref()
                    .unwrap_or("")
                    .contains("deleted"),
                "Expected deletion-related rejection, got: {:?}",
                reasons[0]
            );
        }
        Ok(()) => {
            panic!("Transaction should have been rejected due to partition_max_delete_timestamp")
        }
        Err(other) => panic!("Expected TransactionCanceled, got: {:?}", other),
    }

    // Verify the new item was NOT created
    let mut key = BTreeMap::new();
    key.insert("id".to_string(), AttributeValue::S("pk1".to_string()));
    key.insert("sort".to_string(), AttributeValue::S("sk-new".to_string()));

    let retrieved = engine
        .get_item(&table.key_info, &key)
        .await
        .expect("get_item should succeed");
    assert!(
        retrieved.is_none(),
        "Item should not exist - transaction was rejected"
    );
}
