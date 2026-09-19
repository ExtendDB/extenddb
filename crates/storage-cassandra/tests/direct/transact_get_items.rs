// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for TransactGetItems.

use extenddb_core::types::AttributeValue;
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TransactGetOp};
use std::collections::BTreeMap;

use crate::helpers::{TestTable, setup_engine};

#[tokio::test]
async fn test_transact_get_items_single_item() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TransactGetSingleTable", false).await;

    // Put an item
    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("item-1".to_string()));
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
        .expect("Put item failed");

    // TransactGetItems with single item
    let mut key = BTreeMap::new();
    key.insert("id".to_string(), AttributeValue::S("item-1".to_string()));

    let ops = vec![TransactGetOp {
        key_info: &table.key_info,
        key: &key,
    }];

    let results = engine
        .transact_get_items(&ops)
        .await
        .expect("TransactGetItems failed");

    assert_eq!(results.len(), 1);
    let retrieved = results[0].as_ref().expect("Item should exist");
    assert_eq!(retrieved.get("id"), item.get("id"));
    assert_eq!(retrieved.get("name"), item.get("name"));
    assert_eq!(retrieved.get("count"), item.get("count"));
}

#[tokio::test]
async fn test_transact_get_items_multiple_items() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TransactGetMultiTable", false).await;

    // Put multiple items
    for i in 1..=5 {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), AttributeValue::S(format!("item-{}", i)));
        item.insert("value".to_string(), AttributeValue::N(i.to_string()));

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
            .expect("Put item failed");
    }

    // TransactGetItems with multiple items
    let ops: Vec<TransactGetOp> = (1..=5)
        .map(|i| {
            let mut key = BTreeMap::new();
            key.insert("id".to_string(), AttributeValue::S(format!("item-{}", i)));
            TransactGetOp {
                key_info: &table.key_info,
                key: Box::leak(Box::new(key)),
            }
        })
        .collect();

    let results = engine
        .transact_get_items(&ops)
        .await
        .expect("TransactGetItems failed");

    assert_eq!(results.len(), 5);
    for (i, result) in results.iter().enumerate() {
        let item = result.as_ref().expect("Item should exist");
        let expected_id = format!("item-{}", i + 1);
        assert_eq!(
            item.get("id"),
            Some(&AttributeValue::S(expected_id.clone()))
        );
        assert_eq!(
            item.get("value"),
            Some(&AttributeValue::N((i + 1).to_string()))
        );
    }
}

#[tokio::test]
async fn test_transact_get_items_nonexistent_item() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TransactGetNonexistentTable", false).await;

    // Put one item
    let mut item1 = BTreeMap::new();
    item1.insert("id".to_string(), AttributeValue::S("exists".to_string()));
    item1.insert(
        "data".to_string(),
        AttributeValue::S("some data".to_string()),
    );

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
        .unwrap();

    // TransactGetItems with mix of existent and non-existent
    let mut key1 = BTreeMap::new();
    key1.insert("id".to_string(), AttributeValue::S("exists".to_string()));

    let mut key2 = BTreeMap::new();
    key2.insert(
        "id".to_string(),
        AttributeValue::S("does-not-exist".to_string()),
    );

    let ops = vec![
        TransactGetOp {
            key_info: &table.key_info,
            key: &key1,
        },
        TransactGetOp {
            key_info: &table.key_info,
            key: &key2,
        },
    ];

    let results = engine
        .transact_get_items(&ops)
        .await
        .expect("TransactGetItems failed");

    assert_eq!(results.len(), 2);
    assert!(results[0].is_some(), "First item should exist");
    assert!(results[1].is_none(), "Second item should not exist");
}

#[tokio::test]
async fn test_transact_get_items_with_sort_key() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TransactGetWithSKTable", true).await;

    // Put items with sort keys
    for i in 1..=3 {
        let mut item = BTreeMap::new();
        item.insert(
            "id".to_string(),
            AttributeValue::S("partition-1".to_string()),
        );
        item.insert("sort".to_string(), AttributeValue::S(format!("sort-{}", i)));
        item.insert("data".to_string(), AttributeValue::N(i.to_string()));

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
            .unwrap();
    }

    // TransactGetItems with composite keys
    let ops: Vec<TransactGetOp> = (1..=3)
        .map(|i| {
            let mut key = BTreeMap::new();
            key.insert(
                "id".to_string(),
                AttributeValue::S("partition-1".to_string()),
            );
            key.insert("sort".to_string(), AttributeValue::S(format!("sort-{}", i)));
            TransactGetOp {
                key_info: &table.key_info,
                key: Box::leak(Box::new(key)),
            }
        })
        .collect();

    let results = engine
        .transact_get_items(&ops)
        .await
        .expect("TransactGetItems failed");

    assert_eq!(results.len(), 3);
    for (i, result) in results.iter().enumerate() {
        let item = result.as_ref().expect("Item should exist");
        assert_eq!(
            item.get("sort"),
            Some(&AttributeValue::S(format!("sort-{}", i + 1)))
        );
        assert_eq!(
            item.get("data"),
            Some(&AttributeValue::N((i + 1).to_string()))
        );
    }
}

#[tokio::test]
async fn test_transact_get_items_cross_table() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;

    // Create both tables in the same account
    let account_id = crate::helpers::unique_test_account();
    let table1 =
        TestTable::with_account(&engine, &account_id, "TransactGetCrossTable1", false).await;
    let table2 =
        TestTable::with_account(&engine, &account_id, "TransactGetCrossTable2", false).await;

    // Put items in different tables
    let mut item1 = BTreeMap::new();
    item1.insert(
        "id".to_string(),
        AttributeValue::S("table1-item".to_string()),
    );
    item1.insert(
        "source".to_string(),
        AttributeValue::S("table1".to_string()),
    );

    let mut item2 = BTreeMap::new();
    item2.insert(
        "id".to_string(),
        AttributeValue::S("table2-item".to_string()),
    );
    item2.insert(
        "source".to_string(),
        AttributeValue::S("table2".to_string()),
    );

    engine
        .put_item(
            &table1.key_info,
            item1,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .unwrap();

    engine
        .put_item(
            &table2.key_info,
            item2,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .unwrap();

    // TransactGetItems across tables
    let mut key1 = BTreeMap::new();
    key1.insert(
        "id".to_string(),
        AttributeValue::S("table1-item".to_string()),
    );

    let mut key2 = BTreeMap::new();
    key2.insert(
        "id".to_string(),
        AttributeValue::S("table2-item".to_string()),
    );

    let ops = vec![
        TransactGetOp {
            key_info: &table1.key_info,
            key: &key1,
        },
        TransactGetOp {
            key_info: &table2.key_info,
            key: &key2,
        },
    ];

    let results = engine
        .transact_get_items(&ops)
        .await
        .expect("TransactGetItems should succeed across tables in same account");

    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].as_ref().unwrap().get("source"),
        Some(&AttributeValue::S("table1".to_string()))
    );
    assert_eq!(
        results[1].as_ref().unwrap().get("source"),
        Some(&AttributeValue::S("table2".to_string()))
    );
}

#[tokio::test]
async fn test_transact_get_items_validation_error() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TransactGetValidationTable", true).await;

    // Create a key with wrong type (should have both id and sort)
    let mut bad_key = BTreeMap::new();
    bad_key.insert("id".to_string(), AttributeValue::S("test".to_string()));
    // Missing sort - validation should fail

    let ops = vec![TransactGetOp {
        key_info: &table.key_info,
        key: &bad_key,
    }];

    let result = engine.transact_get_items(&ops).await;

    // Should get TransactionCanceled with validation error
    match result {
        Err(StorageError::TransactionCanceled(reasons)) => {
            assert_eq!(reasons.len(), 1);
            assert_eq!(reasons[0].code, "ValidationError");
        }
        _ => panic!("Expected TransactionCanceled with ValidationError"),
    }
}

#[tokio::test]
async fn test_transact_get_items_max_items() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TransactGetMaxItemsTable", false).await;

    // Put 100 items (DynamoDB limit)
    for i in 1..=100 {
        let mut item = BTreeMap::new();
        item.insert(
            "id".to_string(),
            AttributeValue::S(format!("item-{:03}", i)),
        );
        item.insert("index".to_string(), AttributeValue::N(i.to_string()));

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
            .unwrap();
    }

    // TransactGetItems with 100 items (max allowed)
    let ops: Vec<TransactGetOp> = (1..=100)
        .map(|i| {
            let mut key = BTreeMap::new();
            key.insert(
                "id".to_string(),
                AttributeValue::S(format!("item-{:03}", i)),
            );
            TransactGetOp {
                key_info: &table.key_info,
                key: Box::leak(Box::new(key)),
            }
        })
        .collect();

    let results = engine
        .transact_get_items(&ops)
        .await
        .expect("TransactGetItems with 100 items should succeed");

    assert_eq!(results.len(), 100);
    for result in &results {
        assert!(result.is_some(), "All items should exist");
    }
}

#[tokio::test]
async fn test_transact_get_items_empty() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;

    // TransactGetItems with empty ops list
    let ops: Vec<TransactGetOp> = vec![];

    let results = engine
        .transact_get_items(&ops)
        .await
        .expect("Empty TransactGetItems should succeed");

    assert_eq!(results.len(), 0);
}
