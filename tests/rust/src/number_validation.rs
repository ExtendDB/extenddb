// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Number (`N`) attribute validation: malformed values (whitespace, a bare
//! dot, an exponent outside i64) and out-of-range magnitudes must be rejected
//! with a ValidationException, as DynamoDB does.

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

// DynamoDB's answers below were measured with PutItem on 2026-10-27, each value
// twice. It prefixes each message with "1 validation error detected: ", which
// ExtendDB does not, so the checks match on the message body.

#[tokio::test]
async fn put_item_exponent_near_i32_limits_is_overflow_or_underflow() {
    let t = tables().await;
    for (n, want) in [
        ("10E2147483647", "Number overflow."),
        ("12E2147483647", "Number overflow."),
        ("1E2147483648", "Number overflow."),
        ("1.5E-2147483648", "Number underflow."),
        ("1E-2147483649", "Number underflow."),
    ] {
        let err = put_with_raw_number(&t.simple_key_string, n)
            .await
            .expect_err("out-of-range number must be rejected");
        assert!(
            err.starts_with("ValidationException") && err.contains(want),
            "{n}: got {err}"
        );
    }
}

#[tokio::test]
async fn put_item_exponent_outside_i64_is_malformed() {
    let t = tables().await;
    let n = "1E9223372036854775808";
    let err = put_with_raw_number(&t.simple_key_string, n)
        .await
        .expect_err("exponent outside i64 must be rejected");
    assert!(
        err.starts_with("ValidationException")
            && err.contains(&format!("cannot be converted to a numeric value: {n}")),
        "got {err}"
    );
}

#[tokio::test]
async fn put_item_zero_with_exponent_outside_i32_is_rejected() {
    let t = tables().await;
    for (n, want) in [
        ("0E2147483648", "Number overflow."),
        ("0E-9223372036854775808", "Number underflow."),
    ] {
        let err = put_with_raw_number(&t.simple_key_string, n)
            .await
            .expect_err("zero with an out-of-range exponent must be rejected");
        assert!(
            err.starts_with("ValidationException") && err.contains(want),
            "{n}: got {err}"
        );
    }
    for n in ["0E125", "0.0E126", "0E-130"] {
        put_with_raw_number(&t.simple_key_string, n)
            .await
            .unwrap_or_else(|e| panic!("{n} must be accepted: {e}"));
    }
}
