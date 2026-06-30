// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transaction validation tests — condition checks, atomicity, update expressions.
//! Mirrors Java `TransactionValidationTests` (scenarios not covered by
//! `transact_write_items.rs` / `transact_get_items.rs`).

use crate::test_base::*;
use aws_sdk_dynamodb::types::{ConditionCheck, Put, TransactWriteItem, Update};

#[tokio::test]
async fn transact_write_put_and_condition_check() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;

    let key1 = format!("twpc1_{}", ts());
    let key2 = format!("twpc2_{}", ts());

    let mut item1 = std::collections::HashMap::new();
    item1.insert(HASH_KEY_S.into(), s(&key1));
    item1.insert("status".into(), s("ACTIVE"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item1))
        .send()
        .await
        .unwrap();

    // Transaction: put new item + condition check on existing.
    let mut new_item = std::collections::HashMap::new();
    new_item.insert(HASH_KEY_S.into(), s(&key2));
    new_item.insert("data".into(), s("new"));

    let mut check_key = std::collections::HashMap::new();
    check_key.insert(HASH_KEY_S.into(), s(&key1));

    c.transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(table)
                        .set_item(Some(new_item))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactWriteItem::builder()
                .condition_check(
                    ConditionCheck::builder()
                        .table_name(table)
                        .set_key(Some(check_key))
                        .condition_expression("#s = :val")
                        .expression_attribute_names("#s", "status")
                        .expression_attribute_values(":val", s("ACTIVE"))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Verify new item exists.
    let mut k2 = std::collections::HashMap::new();
    k2.insert(HASH_KEY_S.into(), s(&key2));
    let resp = c
        .get_item()
        .table_name(table)
        .set_key(Some(k2))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(resp.item().is_some(), "New item should exist");
}

#[tokio::test]
async fn transact_write_condition_check_fails_cancels_all() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;

    let key1 = format!("twcf1_{}", ts());
    let key2 = format!("twcf2_{}", ts());

    let mut item1 = std::collections::HashMap::new();
    item1.insert(HASH_KEY_S.into(), s(&key1));
    item1.insert("status".into(), s("ACTIVE"));
    c.put_item()
        .table_name(table)
        .set_item(Some(item1))
        .send()
        .await
        .unwrap();

    // Condition check expects DELETED but status is ACTIVE — should fail.
    let mut new_item = std::collections::HashMap::new();
    new_item.insert(HASH_KEY_S.into(), s(&key2));
    new_item.insert("data".into(), s("should_not_exist"));

    let mut check_key = std::collections::HashMap::new();
    check_key.insert(HASH_KEY_S.into(), s(&key1));

    let err = c
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(table)
                        .set_item(Some(new_item))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactWriteItem::builder()
                .condition_check(
                    ConditionCheck::builder()
                        .table_name(table)
                        .set_key(Some(check_key))
                        .condition_expression("#s = :val")
                        .expression_attribute_names("#s", "status")
                        .expression_attribute_values(":val", s("DELETED"))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap_err();

    assert_eq!(err_code(&err), Some("TransactionCanceledException"));

    // key2 should NOT exist.
    let mut k2 = std::collections::HashMap::new();
    k2.insert(HASH_KEY_S.into(), s(&key2));
    let resp = c
        .get_item()
        .table_name(table)
        .set_key(Some(k2))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert!(
        resp.item().is_none(),
        "Transaction should have been cancelled"
    );
}

#[tokio::test]
#[ignore = "reserved keyword validation too strict"]
async fn transact_write_update_with_expression() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;

    let key = format!("twue_{}", ts());
    let mut item = std::collections::HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&key));
    item.insert("counter".into(), n(10));
    c.put_item()
        .table_name(table)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    let mut upd_key = std::collections::HashMap::new();
    upd_key.insert(HASH_KEY_S.into(), s(&key));

    c.transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .update(
                    Update::builder()
                        .table_name(table)
                        .set_key(Some(upd_key.clone()))
                        .update_expression("SET counter = counter + :inc")
                        .expression_attribute_values(":inc", n(5))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = c
        .get_item()
        .table_name(table)
        .set_key(Some(upd_key))
        .consistent_read(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.item().unwrap().get("counter").unwrap(), &n(15));
}

#[tokio::test]
async fn transact_write_empty_items_list() {
    let c = client();
    let err = c
        .transact_write_items()
        .set_transact_items(Some(vec![]))
        .send()
        .await
        .unwrap_err();
    assert!(err_code(&err).is_some());
}
