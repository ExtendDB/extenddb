// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for TransactWriteItems (two-phase commit).

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{AttributeValue, Item, ReturnValuesOnConditionCheckFailure};
use extenddb_storage::{DataEngine, IdempotencyKey, TransactWriteOp};

use crate::helpers::{TestTable, setup_engine};

#[tokio::test]
async fn test_transact_write_put_simple() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnPutSimple", false).await;

    // Create an item to put
    let mut item = Item::new();
    item.insert("id".to_string(), AttributeValue::S("test_key".to_string()));
    item.insert("value".to_string(), AttributeValue::N("42".to_string()));

    let maps = ExpressionMaps::default();

    // Create transaction with single Put operation
    let ops = vec![TransactWriteOp::Put {
        key_info: &table.key_info,
        item: &item,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];

    let result = engine.transact_write_items(&ops, None).await;

    // Should succeed
    assert!(
        result.is_ok(),
        "Transaction should succeed: {:?}",
        result.err()
    );

    // Verify item was written
    let mut key = Item::new();
    key.insert("id".to_string(), AttributeValue::S("test_key".to_string()));

    let retrieved = engine
        .get_item(&table.key_info, &key)
        .await
        .expect("get_item should succeed");

    assert!(retrieved.is_some(), "Item should exist after commit");
    let retrieved_item = retrieved.unwrap();
    assert_eq!(
        retrieved_item.get("value"),
        Some(&AttributeValue::N("42".to_string())),
        "Item value should match"
    );
}

#[tokio::test]
async fn test_transact_write_put_and_delete() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnPutDel", false).await;

    let maps = ExpressionMaps::default();

    // First, put an item using regular put_item
    let mut item1 = Item::new();
    item1.insert("id".to_string(), AttributeValue::S("key1".to_string()));
    item1.insert(
        "value".to_string(),
        AttributeValue::S("original".to_string()),
    );

    engine
        .put_item(&table.key_info, item1.clone(), false, None, &maps, None)
        .await
        .expect("Initial put_item should succeed");

    // Now create a transaction that:
    // 1. Puts a new item (key2)
    // 2. Deletes the existing item (key1)
    let mut item2 = Item::new();
    item2.insert("id".to_string(), AttributeValue::S("key2".to_string()));
    item2.insert("value".to_string(), AttributeValue::S("new".to_string()));

    let mut key1 = Item::new();
    key1.insert("id".to_string(), AttributeValue::S("key1".to_string()));

    let ops = vec![
        TransactWriteOp::Put {
            key_info: &table.key_info,
            item: &item2,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
        TransactWriteOp::Delete {
            key_info: &table.key_info,
            key: &key1,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
    ];

    let result = engine.transact_write_items(&ops, None).await;
    assert!(
        result.is_ok(),
        "Transaction should succeed: {:?}",
        result.err()
    );

    // Verify key1 was deleted
    let retrieved1 = engine
        .get_item(&table.key_info, &key1)
        .await
        .expect("get_item should succeed");
    assert!(retrieved1.is_none(), "key1 should be deleted");

    // Verify key2 was written
    let mut key2 = Item::new();
    key2.insert("id".to_string(), AttributeValue::S("key2".to_string()));

    let retrieved2 = engine
        .get_item(&table.key_info, &key2)
        .await
        .expect("get_item should succeed");
    assert!(retrieved2.is_some(), "key2 should exist");
    assert_eq!(
        retrieved2.unwrap().get("value"),
        Some(&AttributeValue::S("new".to_string())),
        "key2 value should match"
    );
}

#[tokio::test]
async fn test_transact_write_rollback_on_condition_failure() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use std::collections::HashMap;

    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnRollback", false).await;

    // First, put an item with value=10
    let mut item1 = Item::new();
    item1.insert("id".to_string(), AttributeValue::S("key1".to_string()));
    item1.insert("value".to_string(), AttributeValue::N("10".to_string()));

    let maps = ExpressionMaps::default();

    engine
        .put_item(&table.key_info, item1.clone(), false, None, &maps, None)
        .await
        .expect("Initial put_item should succeed");

    // Create a transaction with a condition that will fail
    // Condition: value = 999 (which is false, so transaction will rollback)
    use extenddb_core::expression::{CompareOp, Expr, PathElement};

    // Create expression maps with the comparison value
    let mut values_map = HashMap::new();
    values_map.insert("val1".to_string(), AttributeValue::N("999".to_string()));
    let maps_with_condition = ExpressionMaps::new(HashMap::new(), values_map);

    let condition = Expr::Compare {
        left: Box::new(Expr::Path(vec![PathElement::Attribute(
            "value".to_string(),
        )])),
        op: CompareOp::Eq,
        right: Box::new(Expr::Placeholder("val1".to_string())),
    };

    // Put a new item (key2)
    let mut item2 = Item::new();
    item2.insert("id".to_string(), AttributeValue::S("key2".to_string()));
    item2.insert("value".to_string(), AttributeValue::S("new".to_string()));

    let ops = vec![
        TransactWriteOp::Put {
            key_info: &table.key_info,
            item: &item2,
            condition: None,
            maps: &maps_with_condition,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
        TransactWriteOp::ConditionCheck {
            key_info: &table.key_info,
            key: &item1,
            condition: &condition,
            maps: &maps_with_condition,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        },
    ];

    // Execute the transaction - should fail with TransactionCanceled
    let result = engine.transact_write_items(&ops, None).await;
    assert!(
        result.is_err(),
        "Transaction should fail due to condition check"
    );

    // Verify key2 was NOT written (transaction was rolled back)
    let mut key2 = Item::new();
    key2.insert("id".to_string(), AttributeValue::S("key2".to_string()));

    let retrieved2 = engine
        .get_item(&table.key_info, &key2)
        .await
        .expect("get_item should succeed");
    assert!(
        retrieved2.is_none(),
        "key2 should not exist (transaction rolled back)"
    );

    // Verify key1 still exists unchanged
    let mut key1 = Item::new();
    key1.insert("id".to_string(), AttributeValue::S("key1".to_string()));

    let retrieved1 = engine
        .get_item(&table.key_info, &key1)
        .await
        .expect("get_item should succeed");
    assert!(retrieved1.is_some(), "key1 should still exist");
    assert_eq!(
        retrieved1.unwrap().get("value"),
        Some(&AttributeValue::N("10".to_string())),
        "key1 should be unchanged"
    );
}

#[tokio::test]
async fn test_transact_write_rollback_update_existing_item() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    // Rollback must restore an existing item's prepared_txn_id to null without
    // deleting the row (created_to_prepare=false path).
    use extenddb_core::expression::{CompareOp, Expr, PathElement, UpdateAction};
    use std::collections::HashMap;

    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnRollbackUpdate", false).await;

    // Write an existing item
    let mut existing = Item::new();
    existing.insert("id".to_string(), AttributeValue::S("upd_key".to_string()));
    existing.insert("value".to_string(), AttributeValue::N("100".to_string()));
    let maps_empty = ExpressionMaps::default();
    engine
        .put_item(
            &table.key_info,
            existing.clone(),
            false,
            None,
            &maps_empty,
            None,
        )
        .await
        .expect("setup put_item");

    let mut key = Item::new();
    key.insert("id".to_string(), AttributeValue::S("upd_key".to_string()));

    // Update sets value = :new_val (999)
    let mut values_map = HashMap::new();
    values_map.insert("new_val".to_string(), AttributeValue::N("999".to_string()));
    // ConditionCheck on a non-existent item forces rollback
    values_map.insert("v".to_string(), AttributeValue::S("x".to_string()));
    let maps_with_vals = ExpressionMaps::new(HashMap::new(), values_map);

    let set_action = UpdateAction::Set {
        path: vec![PathElement::Attribute("value".to_string())],
        value: Expr::Placeholder("new_val".to_string()),
    };

    let mut other_key = Item::new();
    other_key.insert(
        "id".to_string(),
        AttributeValue::S("no_such_key".to_string()),
    );
    let failing_condition = Expr::Compare {
        left: Box::new(Expr::Path(vec![PathElement::Attribute(
            "value".to_string(),
        )])),
        op: CompareOp::Eq,
        right: Box::new(Expr::Placeholder("v".to_string())),
    };

    let actions = [set_action];
    let ops = vec![
        TransactWriteOp::Update {
            key_info: &table.key_info,
            key: &key,
            actions: &actions,
            condition: None,
            maps: &maps_with_vals,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
        TransactWriteOp::ConditionCheck {
            key_info: &table.key_info,
            key: &other_key,
            condition: &failing_condition,
            maps: &maps_with_vals,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        },
    ];

    let result = engine.transact_write_items(&ops, None).await;
    assert!(result.is_err(), "Transaction should be cancelled");

    // Item must still exist with original value and be unlocked
    let retrieved = engine
        .get_item(&table.key_info, &key)
        .await
        .expect("get_item");
    assert!(retrieved.is_some(), "item must still exist after rollback");
    assert_eq!(
        retrieved.unwrap().get("value"),
        Some(&AttributeValue::N("100".to_string())),
        "item value must be unchanged after rollback"
    );
}

#[tokio::test]
async fn test_transact_write_rollback_delete_existing_item() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    // Rollback of a Delete must leave the existing item intact and unlocked.
    use extenddb_core::expression::{CompareOp, Expr, PathElement};
    use std::collections::HashMap;

    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnRollbackDelete", false).await;
    let maps = ExpressionMaps::default();

    // Write an existing item
    let mut existing = Item::new();
    existing.insert("id".to_string(), AttributeValue::S("del_key".to_string()));
    existing.insert(
        "value".to_string(),
        AttributeValue::S("keep_me".to_string()),
    );
    engine
        .put_item(&table.key_info, existing.clone(), false, None, &maps, None)
        .await
        .expect("setup put_item");

    let mut key = Item::new();
    key.insert("id".to_string(), AttributeValue::S("del_key".to_string()));

    // ConditionCheck on a non-existent item - will fail, forcing rollback
    let mut other_key = Item::new();
    other_key.insert(
        "id".to_string(),
        AttributeValue::S("no_such_key".to_string()),
    );

    let mut values_map = HashMap::new();
    values_map.insert("v".to_string(), AttributeValue::S("x".to_string()));
    let failing_maps = ExpressionMaps::new(HashMap::new(), values_map);

    let failing_condition = Expr::Compare {
        left: Box::new(Expr::Path(vec![PathElement::Attribute(
            "value".to_string(),
        )])),
        op: CompareOp::Eq,
        right: Box::new(Expr::Placeholder("v".to_string())),
    };

    let ops = vec![
        TransactWriteOp::Delete {
            key_info: &table.key_info,
            key: &key,
            condition: None,
            maps: &maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
        TransactWriteOp::ConditionCheck {
            key_info: &table.key_info,
            key: &other_key,
            condition: &failing_condition,
            maps: &failing_maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        },
    ];

    let result = engine.transact_write_items(&ops, None).await;
    assert!(result.is_err(), "Transaction should be cancelled");

    // Item must still exist and be readable (not locked)
    let retrieved = engine
        .get_item(&table.key_info, &key)
        .await
        .expect("get_item");
    assert!(retrieved.is_some(), "item must still exist after rollback");
    assert_eq!(
        retrieved.unwrap().get("value"),
        Some(&AttributeValue::S("keep_me".to_string())),
        "item value must be unchanged after rollback"
    );

    // Verify item is no longer locked (a subsequent write should succeed)
    engine
        .put_item(&table.key_info, existing.clone(), false, None, &maps, None)
        .await
        .expect("put_item after rollback should succeed (item not locked)");
}

#[tokio::test]
async fn test_idempotency_token_replay() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnIdempotencyReplay", false).await;
    let token = format!("tok-replay-{}", uuid::Uuid::new_v4().simple());

    let mut item = Item::new();
    item.insert("id".to_string(), AttributeValue::S("idem_key".to_string()));
    let maps = ExpressionMaps::default();
    let ops = vec![TransactWriteOp::Put {
        key_info: &table.key_info,
        item: &item,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];

    engine
        .transact_write_items(
            &ops,
            Some(IdempotencyKey {
                account_id: &table.key_info.account_id,
                token: &token,
                fingerprint: "fp-123",
            }),
        )
        .await
        .expect("First call should succeed");

    let result = engine
        .transact_write_items(
            &ops,
            Some(IdempotencyKey {
                account_id: &table.key_info.account_id,
                token: &token,
                fingerprint: "fp-123",
            }),
        )
        .await;
    assert!(
        matches!(
            result,
            Err(extenddb_storage::error::StorageError::IdempotentReplay)
        ),
        "Expected IdempotentReplay, got: {:?}",
        result
    );
}

#[tokio::test]
async fn test_idempotency_token_mismatch() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "TxnIdempotencyMismatch", false).await;
    let token = format!("tok-mismatch-{}", uuid::Uuid::new_v4().simple());

    let mut item = Item::new();
    item.insert("id".to_string(), AttributeValue::S("idem_key2".to_string()));
    let maps = ExpressionMaps::default();
    let ops = vec![TransactWriteOp::Put {
        key_info: &table.key_info,
        item: &item,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];

    engine
        .transact_write_items(
            &ops,
            Some(IdempotencyKey {
                account_id: &table.key_info.account_id,
                token: &token,
                fingerprint: "fp-aaa",
            }),
        )
        .await
        .expect("First call should succeed");

    let result = engine
        .transact_write_items(
            &ops,
            Some(IdempotencyKey {
                account_id: &table.key_info.account_id,
                token: &token,
                fingerprint: "fp-bbb",
            }),
        )
        .await;
    assert!(
        matches!(
            result,
            Err(extenddb_storage::error::StorageError::IdempotentMismatch)
        ),
        "Expected IdempotentMismatch, got: {:?}",
        result
    );
}

#[tokio::test]
async fn test_idempotency_token_is_scoped_to_account() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table_a = TestTable::new(&engine, "TxnIdempotencyAccountA", false).await;
    let table_b = TestTable::new(&engine, "TxnIdempotencyAccountB", false).await;
    let token = format!("shared-token-{}", uuid::Uuid::new_v4().simple());
    let maps = ExpressionMaps::default();

    let mut item_a = Item::new();
    item_a.insert("id".to_owned(), AttributeValue::S("account-a".to_owned()));
    let ops_a = vec![TransactWriteOp::Put {
        key_info: &table_a.key_info,
        item: &item_a,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    engine
        .transact_write_items(
            &ops_a,
            Some(IdempotencyKey {
                account_id: &table_a.key_info.account_id,
                token: &token,
                fingerprint: "same-fingerprint",
            }),
        )
        .await
        .expect("first account should reserve the token");

    let mut item_b = Item::new();
    item_b.insert("id".to_owned(), AttributeValue::S("account-b".to_owned()));
    let ops_b = vec![TransactWriteOp::Put {
        key_info: &table_b.key_info,
        item: &item_b,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    engine
        .transact_write_items(
            &ops_b,
            Some(IdempotencyKey {
                account_id: &table_b.key_info.account_id,
                token: &token,
                fingerprint: "same-fingerprint",
            }),
        )
        .await
        .expect("the same token in another account must not replay");
}

/// A Put of `{id}` with `token`, whose fingerprint differs from every other
/// request in `test_idempotency_token_after_a_key_fault`.
async fn put_with_token(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    key_info: &extenddb_core::types::TableKeyInfo,
    id: &str,
    token: &str,
) -> Result<(), extenddb_storage::error::StorageError> {
    let mut item = Item::new();
    item.insert("id".to_owned(), AttributeValue::S(id.to_owned()));
    let maps = ExpressionMaps::default();
    let ops = [TransactWriteOp::Put {
        key_info,
        item: &item,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    let fingerprint = format!("fp-put-{id}");
    engine
        .transact_write_items(
            &ops,
            Some(IdempotencyKey {
                account_id: &key_info.account_id,
                token,
                fingerprint: &fingerprint,
            }),
        )
        .await
}

/// A token beside a key fault, as the service keeps it (measured
/// 2026-11-01): a fault of the request-level checks (a wrong-typed table key)
/// leaves the token unused, so another request with it commits. A fault found
/// after them (an empty index-key literal, a failed condition) leaves it used:
/// the same request with it answers that fault again, and another request
/// with it answers the mismatch and writes nothing.
#[tokio::test]
async fn test_idempotency_token_after_a_key_fault() {
    use extenddb_core::expression::{self, Expr, PathElement};
    use extenddb_storage::TableEngine;
    use extenddb_storage::error::StorageError;
    use std::collections::HashMap;

    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::with_gsi(&engine, "TxnTokenKeyFault", "GkIndex", "gk").await;
    // The index is in the key info only once the table is active.
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    engine
        .process_control_plane_transitions()
        .await
        .expect("process table transitions");
    let key_info = engine
        .table_key_info(&table.key_info.account_id, "TxnTokenKeyFault")
        .await
        .expect("key info");
    assert_eq!(key_info.global_secondary_indexes.len(), 1, "{key_info:?}");
    let present = |id: &'static str| {
        let engine = &engine;
        let key_info = &key_info;
        async move {
            let key = Item::from([("id".to_owned(), AttributeValue::S(id.to_owned()))]);
            engine
                .get_item(key_info, &key)
                .await
                .expect("get_item")
                .is_some()
        }
    };
    let token = |name: &str| format!("tok-{name}-{}", uuid::Uuid::new_v4().simple());
    let plain = ExpressionMaps::default();

    // A wrong-typed table key: answered by the request-level checks, and the
    // token stays unused.
    let bad_key = Item::from([("id".to_owned(), AttributeValue::N("1".to_owned()))]);
    let t1 = token("key");
    let ops = [TransactWriteOp::Put {
        key_info: &key_info,
        item: &bad_key,
        condition: None,
        maps: &plain,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    let key_fault = IdempotencyKey {
        account_id: &key_info.account_id,
        token: &t1,
        fingerprint: "fp-key",
    };
    let result = engine.transact_write_items(&ops, Some(key_fault)).await;
    assert!(
        matches!(&result, Err(StorageError::TransactionCanceled(r)) if r[0].code == "ValidationError"),
        "{result:?}"
    );
    put_with_token(&engine, &key_info, "after-key", &t1)
        .await
        .expect("the token is unused after a request-level fault");
    assert!(present("after-key").await);

    // An empty index-key literal: found after the token is checked, so the
    // token stays used.
    let empty = ExpressionMaps::new(
        HashMap::new(),
        HashMap::from([("e".to_owned(), AttributeValue::S(String::new()))]),
    );
    let tokens = expression::tokenize("SET gk = :e").expect("tokenize");
    let actions = expression::parse_update(&tokens).expect("parse");
    let key_u = Item::from([("id".to_owned(), AttributeValue::S("u".to_owned()))]);
    let t2 = token("empty");
    let ops = [TransactWriteOp::Update {
        key_info: &key_info,
        key: &key_u,
        actions: &actions,
        condition: None,
        maps: &empty,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    let empty_fault = IdempotencyKey {
        account_id: &key_info.account_id,
        token: &t2,
        fingerprint: "fp-empty",
    };
    for send in ["first", "again"] {
        let result = engine.transact_write_items(&ops, Some(empty_fault)).await;
        assert!(
            matches!(&result, Err(StorageError::Validation(m)) if m.ends_with("cannot contain an empty string value.")),
            "{send}: {result:?}"
        );
    }
    let result = put_with_token(&engine, &key_info, "after-empty", &t2).await;
    assert!(
        matches!(result, Err(StorageError::IdempotentMismatch)),
        "{result:?}"
    );
    assert!(!present("after-empty").await);

    // A failed condition: the same.
    engine
        .put_item(
            &key_info,
            Item::from([("id".to_owned(), AttributeValue::S("c1".to_owned()))]),
            false,
            None,
            &plain,
            None,
        )
        .await
        .expect("seed c1");
    let c1 = Item::from([("id".to_owned(), AttributeValue::S("c1".to_owned()))]);
    let not_exists = Expr::Function {
        name: "attribute_not_exists".to_owned(),
        args: vec![Expr::Path(vec![PathElement::Attribute("id".to_owned())])],
    };
    let t3 = token("ccf");
    let ops = [TransactWriteOp::Put {
        key_info: &key_info,
        item: &c1,
        condition: Some(&not_exists),
        maps: &plain,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    let ccf = IdempotencyKey {
        account_id: &key_info.account_id,
        token: &t3,
        fingerprint: "fp-ccf",
    };
    for send in ["first", "again"] {
        let result = engine.transact_write_items(&ops, Some(ccf)).await;
        assert!(
            matches!(&result, Err(StorageError::TransactionCanceled(r)) if r[0].code == "ConditionalCheckFailed"),
            "{send}: {result:?}"
        );
    }
    let result = put_with_token(&engine, &key_info, "after-ccf", &t3).await;
    assert!(
        matches!(result, Err(StorageError::IdempotentMismatch)),
        "{result:?}"
    );
    assert!(!present("after-ccf").await);
}
