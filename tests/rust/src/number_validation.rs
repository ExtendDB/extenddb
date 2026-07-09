// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Number (`N`) attribute validation: whitespace and bare-dot inputs must be
//! rejected with a ValidationException, matching real DynamoDB. Covers the
//! parity fix that stopped trimming/accepting malformed numeric values.

use crate::test_base::*;
use std::collections::HashMap;

async fn put_with_raw_number(table: &str, raw_n: &str) -> Result<(), String> {
    let c = client();
    let mut item: HashMap<String, _> = HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&format!("num_{}", ts())));
    item.insert(
        "amount".into(),
        aws_sdk_dynamodb::types::AttributeValue::N(raw_n.to_string()),
    );
    c.put_item()
        .table_name(table)
        .set_item(Some(item))
        .send()
        .await
        .map(|_| ())
        .map_err(|e| format!("{}:{}", err_code(&e).unwrap_or("None"), err_msg(&e)))
}

#[tokio::test]
async fn put_item_rejects_number_with_leading_whitespace() {
    let t = tables().await;
    let err = put_with_raw_number(&t.simple_key_string, " 5")
        .await
        .expect_err("leading-whitespace number must be rejected");
    assert!(err.starts_with("ValidationException"), "got: {err}");
}

#[tokio::test]
async fn put_item_rejects_number_with_trailing_whitespace() {
    let t = tables().await;
    let err = put_with_raw_number(&t.simple_key_string, "5 ")
        .await
        .expect_err("trailing-whitespace number must be rejected");
    assert!(err.starts_with("ValidationException"), "got: {err}");
}

#[tokio::test]
async fn put_item_rejects_bare_dot_number() {
    let t = tables().await;
    let err = put_with_raw_number(&t.simple_key_string, ".")
        .await
        .expect_err("bare-dot number must be rejected");
    assert!(err.starts_with("ValidationException"), "got: {err}");
    assert!(
        err.contains("numeric value"),
        "expected 'cannot be converted to a numeric value', got: {err}"
    );
}

#[tokio::test]
async fn put_item_accepts_valid_number() {
    let t = tables().await;
    put_with_raw_number(&t.simple_key_string, "42.5")
        .await
        .expect("valid number must be accepted");
}
