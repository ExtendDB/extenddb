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

// DynamoDB's answers below were measured with PutItem in us-east-1 on
// 2026-11-09, each value twice with the same answer both times, except the
// values marked with another date. The checks match on the full message body,
// which DynamoDB prefixes with "1 validation error detected: " and ExtendDB
// does not.
const OVERFLOW_MESSAGE: &str =
    "Number overflow. Attempting to store a number with magnitude larger than supported range";
const UNDERFLOW_MESSAGE: &str =
    "Number underflow. Attempting to store a number with magnitude smaller than supported range";
const TOO_MANY_DIGITS_MESSAGE: &str =
    "Attempting to store more than 38 significant digits in a Number";

#[tokio::test]
async fn put_item_zero_with_exponent_at_i64_max_is_underflow() {
    let t = tables().await;
    for (n, want) in [
        ("0E9223372036854775807", UNDERFLOW_MESSAGE),
        // Measured on 2026-10-27.
        ("0E9223372036854775806", OVERFLOW_MESSAGE),
        ("0.0E9223372036854775807", OVERFLOW_MESSAGE),
    ] {
        let err = put_with_raw_number(&t.simple_key_string, n)
            .await
            .expect_err("zero with an exponent at the i64 limit must be rejected");
        assert!(
            err.starts_with("ValidationException") && err.contains(want),
            "{n}: got {err}"
        );
    }
}

#[tokio::test]
async fn put_item_magnitude_at_i64_max_is_overflow_before_digit_count() {
    let t = tables().await;
    for (n, want) in [
        (
            "1234567890123456789012345678901234567890E9223372036854775768",
            OVERFLOW_MESSAGE,
        ),
        (
            "1234567890123456789012345678901234567890E9223372036854775767",
            TOO_MANY_DIGITS_MESSAGE,
        ),
        // The order of magnitude at i64::MAX + 1 and at i64::MAX + 39.
        // Measured on 2026-11-10, each value sent twice.
        (
            "1234567890123456789012345678901234567890E9223372036854775769",
            OVERFLOW_MESSAGE,
        ),
        (
            "1234567890123456789012345678901234567890E9223372036854775807",
            OVERFLOW_MESSAGE,
        ),
    ] {
        let err = put_with_raw_number(&t.simple_key_string, n)
            .await
            .expect_err("forty digits with an out-of-range magnitude must be rejected");
        assert!(
            err.starts_with("ValidationException") && err.contains(want),
            "{n}: got {err}"
        );
    }
}

#[tokio::test]
async fn put_item_magnitude_below_i64_min_is_underflow_before_digit_count() {
    let t = tables().await;
    // More than 38 digits with the exponent of the normalized form `0.d * 10^n`
    // below i64::MIN are an underflow before the digit count. Each fraction
    // zero before the digits lowers that exponent by one: under the smallest
    // exponent, one zero puts forty digits at i64::MIN - 1, two at i64::MIN - 2
    // and forty at i64::MIN - 40. At i64::MIN, and at i64::MIN + 40 with the
    // digits before the point, the digit count is reported. The answers are
    // the same with 39 digits and with a minus sign. Measured on 2026-11-10,
    // each value sent twice with the same answer both times.
    let forty_zeros = "0".repeat(40);
    let forty_below =
        format!(".{forty_zeros}1234567890123456789012345678901234567891E-9223372036854775808");
    for (n, want) in [
        (
            ".01234567890123456789012345678901234567891E-9223372036854775808",
            UNDERFLOW_MESSAGE,
        ),
        (
            ".001234567890123456789012345678901234567891E-9223372036854775807",
            UNDERFLOW_MESSAGE,
        ),
        (
            ".001234567890123456789012345678901234567891E-9223372036854775808",
            UNDERFLOW_MESSAGE,
        ),
        (forty_below.as_str(), UNDERFLOW_MESSAGE),
        (
            ".0123456789012345678901234567890123456789E-9223372036854775808",
            UNDERFLOW_MESSAGE,
        ),
        (
            "-.01234567890123456789012345678901234567891E-9223372036854775808",
            UNDERFLOW_MESSAGE,
        ),
        (
            ".1234567890123456789012345678901234567891E-9223372036854775808",
            TOO_MANY_DIGITS_MESSAGE,
        ),
        (
            ".01234567890123456789012345678901234567891E-9223372036854775807",
            TOO_MANY_DIGITS_MESSAGE,
        ),
        (
            ".001234567890123456789012345678901234567891E-9223372036854775806",
            TOO_MANY_DIGITS_MESSAGE,
        ),
        (
            "1234567890123456789012345678901234567891E-9223372036854775808",
            TOO_MANY_DIGITS_MESSAGE,
        ),
        (
            ".123456789012345678901234567890123456789E-9223372036854775808",
            TOO_MANY_DIGITS_MESSAGE,
        ),
        (
            "-.1234567890123456789012345678901234567891E-9223372036854775808",
            TOO_MANY_DIGITS_MESSAGE,
        ),
    ] {
        let err = put_with_raw_number(&t.simple_key_string, n)
            .await
            .expect_err("more than 38 digits with an out-of-range magnitude must be rejected");
        assert!(
            err.starts_with("ValidationException") && err.contains(want),
            "{n}: got {err}"
        );
    }
}
