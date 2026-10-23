// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transaction-API primary-key size validation.
//!
//! An oversized hash key (> 2048 bytes) or range key (> 1024 bytes) in any
//! transaction sub-op must cancel the transaction with a per-item
//! `ValidationError` cancellation reason (`TransactionCanceledException`),
//! matching real DynamoDB. This covers TransactGetItems (Get) and each
//! TransactWriteItems sub-op (Put / Delete / Update / ConditionCheck).
//!
//! An EMPTY key value, by contrast, remains a top-level `ValidationException`
//! (covered in `transact_key_validation`); this file exercises the size path.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    ConditionCheck, Delete, Get, Put, TransactGetItem, TransactWriteItem, Update,
};
use std::collections::HashMap;

fn oversized_hash() -> String {
    "a".repeat(2049)
}

fn assert_cancelled_validation(err_code_opt: Option<&str>, msg: &str) {
    assert_eq!(
        err_code_opt,
        Some("TransactionCanceledException"),
        "expected TransactionCanceledException, got: {msg}"
    );
    assert!(
        msg.contains("ValidationError"),
        "expected a ValidationError cancellation reason, got: {msg}"
    );
}

#[tokio::test]
async fn transact_write_put_oversized_hash_key_cancels() {
    let c = client();
    let t = tables().await;
    let mut item: HashMap<String, _> = HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&oversized_hash()));
    let err = c
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(&t.simple_key_string)
                        .set_item(Some(item))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect_err("oversized hash key must cancel the transaction");
    assert_cancelled_validation(err_code(&err), &err_msg(&err));
}

#[tokio::test]
async fn transact_write_delete_oversized_hash_key_cancels() {
    let c = client();
    let t = tables().await;
    let mut key: HashMap<String, _> = HashMap::new();
    key.insert(HASH_KEY_S.into(), s(&oversized_hash()));
    let err = c
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .delete(
                    Delete::builder()
                        .table_name(&t.simple_key_string)
                        .set_key(Some(key))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect_err("oversized hash key must cancel the transaction");
    assert_cancelled_validation(err_code(&err), &err_msg(&err));
}

#[tokio::test]
async fn transact_write_update_oversized_hash_key_cancels() {
    let c = client();
    let t = tables().await;
    let mut key: HashMap<String, _> = HashMap::new();
    key.insert(HASH_KEY_S.into(), s(&oversized_hash()));
    let err = c
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .update(
                    Update::builder()
                        .table_name(&t.simple_key_string)
                        .set_key(Some(key))
                        .update_expression("SET #d = :v")
                        .expression_attribute_names("#d", "data")
                        .expression_attribute_values(":v", s("x"))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect_err("oversized hash key must cancel the transaction");
    assert_cancelled_validation(err_code(&err), &err_msg(&err));
}

#[tokio::test]
async fn transact_write_condition_check_oversized_hash_key_cancels() {
    let c = client();
    let t = tables().await;
    let mut key: HashMap<String, _> = HashMap::new();
    key.insert(HASH_KEY_S.into(), s(&oversized_hash()));
    let err = c
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .condition_check(
                    ConditionCheck::builder()
                        .table_name(&t.simple_key_string)
                        .set_key(Some(key))
                        .condition_expression("attribute_exists(#h)")
                        .expression_attribute_names("#h", HASH_KEY_S)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect_err("oversized hash key must cancel the transaction");
    assert_cancelled_validation(err_code(&err), &err_msg(&err));
}

#[tokio::test]
async fn transact_get_oversized_hash_key_cancels() {
    let c = client();
    let t = tables().await;
    let mut key: HashMap<String, _> = HashMap::new();
    key.insert(HASH_KEY_S.into(), s(&oversized_hash()));
    let err = c
        .transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(&t.simple_key_string)
                        .set_key(Some(key))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect_err("oversized hash key must cancel the transaction");
    assert_cancelled_validation(err_code(&err), &err_msg(&err));
}

#[tokio::test]
async fn transact_get_valid_key_succeeds() {
    // Sanity: a normal-sized key is not rejected by the size check.
    let c = client();
    let t = tables().await;
    let mut key: HashMap<String, _> = HashMap::new();
    key.insert(HASH_KEY_S.into(), s(&format!("tks_{}", ts())));
    c.transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(&t.simple_key_string)
                        .set_key(Some(key))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect("valid key must be accepted");
}
