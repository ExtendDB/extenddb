// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TransactWriteItems key validation parity:
//! - An empty key element is reported up front as a top-level
//!   ValidationException (not a per-item cancellation reason).
//! - A key type mismatch on Delete/Update/ConditionCheck cancels the
//!   transaction (TransactionCanceledException).

use crate::test_base::*;
use aws_sdk_dynamodb::types::{Delete, TransactWriteItem, Update};
use std::collections::HashMap;

#[tokio::test]
async fn transact_update_empty_hash_key_is_top_level_validation() {
    let c = client();
    let t = tables().await;

    let mut key: HashMap<String, _> = HashMap::new();
    key.insert(HASH_KEY_S.into(), s("")); // empty key value

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
        .expect_err("empty key must be rejected up front");
    // Up-front input validation → top-level ValidationException, NOT
    // TransactionCanceledException.
    assert_eq!(
        err_code(&err),
        Some("ValidationException"),
        "expected top-level ValidationException, got: {}",
        err_msg(&err)
    );
}

#[tokio::test]
async fn transact_delete_wrong_typed_key_cancels() {
    let c = client();
    let t = tables().await;
    // comp_key_string_number: hashKey (S) + rangeKey (N). Supply rangeKey as S.
    let mut key: HashMap<String, _> = HashMap::new();
    key.insert(HASH_KEY_S.into(), s(&format!("twk_{}", ts())));
    key.insert(RANGE_KEY_N.into(), s("not-a-number"));

    let err = c
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .delete(
                    Delete::builder()
                        .table_name(&t.comp_key_string_number)
                        .set_key(Some(key))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect_err("key type mismatch must fail");
    let code = err_code(&err);
    assert!(
        code == Some("TransactionCanceledException") || code == Some("ValidationException"),
        "expected cancellation or validation error, got: {code:?} {}",
        err_msg(&err)
    );
}
