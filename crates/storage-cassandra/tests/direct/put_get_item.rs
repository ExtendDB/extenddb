// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for put_item and get_item operations.

use extenddb_core::types::{AttributeValue, ScalarAttributeType};
use extenddb_storage::DataEngine;
use std::collections::BTreeMap;

use crate::helpers::{TestTable, setup_engine};

#[tokio::test]
async fn test_put_and_get_item_pk_only() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TestPkOnlyTable", false).await;

    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("test-id-1".to_string()));
    item.insert(
        "name".to_string(),
        AttributeValue::S("Test Item".to_string()),
    );
    item.insert("count".to_string(), AttributeValue::N("42".to_string()));

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
        .expect("Put failed");

    let mut key = BTreeMap::new();
    key.insert("id".to_string(), AttributeValue::S("test-id-1".to_string()));

    let retrieved = engine
        .get_item(&table.key_info, &key)
        .await
        .expect("Get failed")
        .expect("Item should exist");

    assert_eq!(retrieved.get("id"), item.get("id"));
    assert_eq!(retrieved.get("name"), item.get("name"));
    assert_eq!(retrieved.get("count"), item.get("count"));
}

#[tokio::test]
async fn test_put_and_get_item_with_string_sk() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TestStringSkTable", true).await;

    let mut item = BTreeMap::new();
    item.insert(
        "id".to_string(),
        AttributeValue::S("partition1".to_string()),
    );
    item.insert("sort".to_string(), AttributeValue::S("sort1".to_string()));
    item.insert(
        "data".to_string(),
        AttributeValue::S("test data".to_string()),
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
        .expect("Put failed");

    let mut key = BTreeMap::new();
    key.insert(
        "id".to_string(),
        AttributeValue::S("partition1".to_string()),
    );
    key.insert("sort".to_string(), AttributeValue::S("sort1".to_string()));

    let retrieved = engine
        .get_item(&table.key_info, &key)
        .await
        .expect("Get failed")
        .expect("Item should exist");

    assert_eq!(
        retrieved.get("data"),
        Some(&AttributeValue::S("test data".to_string()))
    );
}

#[tokio::test]
async fn test_put_and_get_item_with_number_sk() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table =
        TestTable::with_sort_key_type(&engine, "TestNumberSkTable", ScalarAttributeType::N).await;

    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("user-1".to_string()));
    item.insert("sort".to_string(), AttributeValue::N("100".to_string()));
    item.insert("value".to_string(), AttributeValue::S("data".to_string()));

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
        .expect("Put failed");

    let mut key = BTreeMap::new();
    key.insert("id".to_string(), AttributeValue::S("user-1".to_string()));
    key.insert("sort".to_string(), AttributeValue::N("100".to_string()));

    let retrieved = engine
        .get_item(&table.key_info, &key)
        .await
        .expect("Get failed")
        .expect("Item should exist");

    assert_eq!(retrieved.get("value"), item.get("value"));
}

#[tokio::test]
async fn test_put_and_get_item_with_decimal_sk() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    // Regression test for the numeric-sort-key decimal support (Technical
    // Debt #1). DynamoDB's N type is an arbitrary-precision decimal; the
    // `sk_n` column is now `decimal` and N values bind as a real Cassandra
    // decimal, so fractional/high-precision sort keys must round-trip exactly.
    let engine = setup_engine().await;
    let table =
        TestTable::with_sort_key_type(&engine, "TestDecimalSkTable", ScalarAttributeType::N).await;

    // A range of values that a varint column or string binding could not
    // represent: fractions, negatives, and a high-precision value.
    let decimal_keys = [
        "123.456",
        "0.0000000001",
        "-42.5",
        "3.14159265358979323846",
        "1000000000000.000001",
    ];

    for (i, sk) in decimal_keys.iter().enumerate() {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), AttributeValue::S("dec-pk".to_string()));
        item.insert("sort".to_string(), AttributeValue::N((*sk).to_string()));
        item.insert(
            "value".to_string(),
            AttributeValue::S(format!("payload-{i}")),
        );

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
            .expect("put_item with decimal sort key should succeed");
    }

    // Each decimal sort key must fetch its exact item back.
    for (i, sk) in decimal_keys.iter().enumerate() {
        let mut key = BTreeMap::new();
        key.insert("id".to_string(), AttributeValue::S("dec-pk".to_string()));
        key.insert("sort".to_string(), AttributeValue::N((*sk).to_string()));

        let retrieved = engine
            .get_item(&table.key_info, &key)
            .await
            .expect("get_item should succeed")
            .unwrap_or_else(|| panic!("item with decimal sort key {sk} should exist"));

        assert_eq!(
            retrieved.get("value"),
            Some(&AttributeValue::S(format!("payload-{i}"))),
            "decimal sort key {sk} did not round-trip to the correct item"
        );
        // The sort key attribute itself is stored in item_data and must be
        // byte-identical to what was written (no precision loss).
        assert_eq!(
            retrieved.get("sort"),
            Some(&AttributeValue::N((*sk).to_string())),
            "decimal sort key {sk} lost precision on round-trip"
        );
    }
}

#[tokio::test]
async fn test_put_and_get_item_with_binary_sk() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table =
        TestTable::with_sort_key_type(&engine, "TestBinarySkTable", ScalarAttributeType::B).await;

    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("record-1".to_string()));
    item.insert("sort".to_string(), AttributeValue::B(vec![1, 2, 3, 4]));
    item.insert(
        "info".to_string(),
        AttributeValue::S("binary key test".to_string()),
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
        .expect("Put failed");

    let mut key = BTreeMap::new();
    key.insert("id".to_string(), AttributeValue::S("record-1".to_string()));
    key.insert("sort".to_string(), AttributeValue::B(vec![1, 2, 3, 4]));

    let retrieved = engine
        .get_item(&table.key_info, &key)
        .await
        .expect("Get failed")
        .expect("Item should exist");

    assert_eq!(retrieved.get("info"), item.get("info"));
}

#[tokio::test]
async fn test_put_item_update_existing() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TestUpdateTable", false).await;

    let mut item = BTreeMap::new();
    item.insert(
        "id".to_string(),
        AttributeValue::S("update-test".to_string()),
    );
    item.insert("value".to_string(), AttributeValue::N("1".to_string()));

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
        .expect("First put failed");

    item.insert("value".to_string(), AttributeValue::N("2".to_string()));

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
        .expect("Second put failed");

    let mut key = BTreeMap::new();
    key.insert(
        "id".to_string(),
        AttributeValue::S("update-test".to_string()),
    );

    let retrieved = engine
        .get_item(&table.key_info, &key)
        .await
        .expect("Get failed")
        .expect("Item should exist");

    assert_eq!(
        retrieved.get("value"),
        Some(&AttributeValue::N("2".to_string()))
    );
}

#[tokio::test]
async fn test_get_item_not_found() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TestNotFoundTable", false).await;

    let mut key = BTreeMap::new();
    key.insert(
        "id".to_string(),
        AttributeValue::S("nonexistent".to_string()),
    );

    let result = engine
        .get_item(&table.key_info, &key)
        .await
        .expect("Get should succeed");

    assert!(result.is_none());
}

#[tokio::test]
async fn test_put_item_with_return_old() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TestReturnOldTable", false).await;

    let mut item = BTreeMap::new();
    item.insert(
        "id".to_string(),
        AttributeValue::S("return-test".to_string()),
    );
    item.insert("version".to_string(), AttributeValue::N("1".to_string()));

    let first_result = engine
        .put_item(
            &table.key_info,
            item.clone(),
            true,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("First put failed");

    assert!(first_result.is_none());

    item.insert("version".to_string(), AttributeValue::N("2".to_string()));

    let second_result = engine
        .put_item(
            &table.key_info,
            item.clone(),
            true,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("Second put failed");

    assert!(second_result.is_some());
    let old = second_result.unwrap();
    assert_eq!(
        old.get("version"),
        Some(&AttributeValue::N("1".to_string()))
    );
}

#[tokio::test]
async fn test_put_item_with_sync_gsi() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use crate::helpers::TestTable;

    let engine = setup_engine().await;
    let table = TestTable::with_gsi(&engine, "TestGSITable", "TestGSI", "gsi_pk").await;

    // The in-tree default GSI propagation is asynchronous (10ms via the GSI
    // queue, drained by a worker that does not run in these tests). This test
    // is about the SYNCHRONOUS write path, so pin the index's delay to 0 —
    // the same knob the async/sync routing reads in production.
    let pin_sync = format!(
        "UPDATE extenddb_ttl_test_catalog.indexes SET propagation_delay_ms = 0 \
         WHERE table_id = '{}' AND index_name = 'TestGSI'",
        table.key_info.table_id
    );
    engine
        .session_arc()
        .query(pin_sync)
        .await
        .expect("pin GSI to synchronous propagation");

    // Put an item with the GSI key
    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("item-1".to_string()));
    item.insert(
        "gsi_pk".to_string(),
        AttributeValue::S("gsi-value-1".to_string()),
    );
    item.insert(
        "data".to_string(),
        AttributeValue::S("test data".to_string()),
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
        .expect("put_item with GSI failed");

    // Verify item exists in base table
    let mut key = BTreeMap::new();
    key.insert("id".to_string(), AttributeValue::S("item-1".to_string()));

    let retrieved = engine
        .get_item(&table.key_info, &key)
        .await
        .expect("get_item failed")
        .expect("Item should exist");

    assert_eq!(retrieved.get("gsi_pk"), item.get("gsi_pk"));
    assert_eq!(retrieved.get("data"), item.get("data"));

    // Verify the index row exists via the public index-scan path (the write
    // is synchronous, so the row must be visible immediately).
    let (index_rows, _) = engine
        .scan(&table.key_info, None, None, None, None, Some("TestGSI"))
        .await
        .expect("index scan failed");
    assert!(
        index_rows.iter().any(|row| {
            row.get("gsi_pk") == Some(&AttributeValue::S("gsi-value-1".to_string()))
                && row.get("id") == Some(&AttributeValue::S("item-1".to_string()))
        }),
        "synchronously-written GSI row missing from index scan: {index_rows:?}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════
// Transaction protection tests (Phase 3, T3.1)
// ═══════════════════════════════════════════════════════════════════════════════

use crate::helpers::put_item_then_lock;

#[tokio::test]
async fn test_put_item_rejects_when_prepared_txn_id_set_pk_only() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use extenddb_storage::error::StorageError;

    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnProtPutPk", false).await;

    let mut item = BTreeMap::new();
    item.insert(
        "id".to_string(),
        AttributeValue::S("locked-item".to_string()),
    );
    item.insert("value".to_string(), AttributeValue::N("1".to_string()));

    put_item_then_lock(&engine, &table, &item).await;

    // Try to overwrite the locked item
    let mut new_item = BTreeMap::new();
    new_item.insert(
        "id".to_string(),
        AttributeValue::S("locked-item".to_string()),
    );
    new_item.insert("value".to_string(), AttributeValue::N("2".to_string()));

    let result = engine
        .put_item(
            &table.key_info,
            new_item,
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
async fn test_put_item_rejects_when_prepared_txn_id_set_with_sk() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use extenddb_storage::error::StorageError;

    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnProtPutSk", true).await;

    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("pk1".to_string()));
    item.insert("sort".to_string(), AttributeValue::S("sk1".to_string()));
    item.insert("value".to_string(), AttributeValue::N("1".to_string()));

    put_item_then_lock(&engine, &table, &item).await;

    // Try to overwrite the locked item
    let mut new_item = BTreeMap::new();
    new_item.insert("id".to_string(), AttributeValue::S("pk1".to_string()));
    new_item.insert("sort".to_string(), AttributeValue::S("sk1".to_string()));
    new_item.insert("value".to_string(), AttributeValue::N("99".to_string()));

    let result = engine
        .put_item(
            &table.key_info,
            new_item,
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
async fn test_put_item_succeeds_when_prepared_txn_id_is_null() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnProtPutOk", false).await;

    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("unlocked".to_string()));
    item.insert("value".to_string(), AttributeValue::N("1".to_string()));

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
        .expect("First put should succeed");

    // Overwrite should also succeed (no transaction lock)
    let mut item2 = BTreeMap::new();
    item2.insert("id".to_string(), AttributeValue::S("unlocked".to_string()));
    item2.insert("value".to_string(), AttributeValue::N("2".to_string()));

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
        .expect("Overwrite of unlocked item should succeed");
}

#[tokio::test]
async fn test_update_item_rejects_when_prepared_txn_id_set_pk_only() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use extenddb_core::expression::{Expr, ExpressionMaps, PathElement, UpdateAction};
    use extenddb_storage::error::StorageError;

    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnProtUpdPk", false).await;

    let mut item = BTreeMap::new();
    item.insert(
        "id".to_string(),
        AttributeValue::S("upd-locked".to_string()),
    );
    item.insert("counter".to_string(), AttributeValue::N("10".to_string()));

    put_item_then_lock(&engine, &table, &item).await;

    let mut key = BTreeMap::new();
    key.insert(
        "id".to_string(),
        AttributeValue::S("upd-locked".to_string()),
    );

    let actions = vec![UpdateAction::Set {
        path: vec![PathElement::Attribute("counter".to_string())],
        value: Expr::Placeholder("val".to_string()),
    }];

    let mut values = std::collections::HashMap::new();
    values.insert("val".to_string(), AttributeValue::N("20".to_string()));
    let maps = ExpressionMaps::new(std::collections::HashMap::new(), values);

    let result = engine
        .update_item(
            &table.key_info,
            &key,
            &actions,
            false,
            false,
            None,
            &maps,
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
async fn test_update_item_rejects_when_prepared_txn_id_set_with_sk() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use extenddb_core::expression::{Expr, ExpressionMaps, PathElement, UpdateAction};
    use extenddb_storage::error::StorageError;

    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnProtUpdSk", true).await;

    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("upk1".to_string()));
    item.insert("sort".to_string(), AttributeValue::S("usk1".to_string()));
    item.insert("counter".to_string(), AttributeValue::N("5".to_string()));

    put_item_then_lock(&engine, &table, &item).await;

    let mut key = BTreeMap::new();
    key.insert("id".to_string(), AttributeValue::S("upk1".to_string()));
    key.insert("sort".to_string(), AttributeValue::S("usk1".to_string()));

    let actions = vec![UpdateAction::Set {
        path: vec![PathElement::Attribute("counter".to_string())],
        value: Expr::Placeholder("val".to_string()),
    }];

    let mut values = std::collections::HashMap::new();
    values.insert("val".to_string(), AttributeValue::N("99".to_string()));
    let maps = ExpressionMaps::new(std::collections::HashMap::new(), values);

    let result = engine
        .update_item(
            &table.key_info,
            &key,
            &actions,
            false,
            false,
            None,
            &maps,
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
