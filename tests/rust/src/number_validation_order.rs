// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The order of the #618 number checks against another fault in the same
//! request. Each test sends a request holding a malformed number and one
//! other fault, and expects the fault DynamoDB reported first. Every
//! expectation is a row of the measured service answers (us-east-1,
//! 2026-10-27, each request sent twice with the same answer both times),
//! named in the test's comment.
//!
//! The tests check that the answer contains the expected message rather than
//! equals it: DynamoDB puts `1 validation error detected: ` on the PutItem
//! item and mixed-parameter messages, and ExtendDB does not, which is a known
//! difference these tests leave alone.

use aws_sdk_dynamodb::types::{
    ConditionCheck, ExpectedAttributeValue, Get, KeysAndAttributes, TransactGetItem,
};

use crate::number_validation_all_ops::{
    batch_write, delete_of, n_raw, number_key, put_request, refused, string_key, transact_write,
    txn_check, txn_delete, Key, NUMERIC,
};
use crate::test_base::*;

const MIXED: &str = "Can not use both expression and non-expression parameters in the same request";

/// Assert a `ValidationException` whose message contains `text`.
fn assert_refused_with(err: &str, text: &str) {
    assert!(
        err.starts_with("ValidationException:") && err.contains(text),
        "expected a ValidationException containing {text:?}, got {err}"
    );
}

fn missing_table() -> String {
    format!("Absent618{}", ts())
}

fn not_exists() -> ExpectedAttributeValue {
    ExpectedAttributeValue::builder().exists(false).build()
}

/// An item of `simple_key_string` holding a malformed number.
fn item_with_a_malformed_number(name: &str) -> Key {
    let mut item = string_key(name);
    item.insert("n".to_string(), n_raw("abc"));
    item
}

#[tokio::test]
async fn get_item_reports_the_key_number_ahead_of_an_unused_name() {
    // "GetItem EAN without projection + key pk abc": the number.
    let t = tables().await;
    let result = client()
        .get_item()
        .table_name(&t.comp_key_number_string)
        .set_key(Some(number_key("abc")))
        .expression_attribute_names("#u", "u")
        .send()
        .await;
    assert_refused_with(&refused(result), NUMERIC);
}

#[tokio::test]
async fn delete_item_reports_the_key_number_ahead_of_the_mixed_parameter_error() {
    // "DeleteItem Expected + ConditionExpression + key pk abc": the number.
    let t = tables().await;
    let result = client()
        .delete_item()
        .table_name(&t.comp_key_number_string)
        .set_key(Some(number_key("abc")))
        .expected("a", not_exists())
        .condition_expression("attribute_not_exists(b)")
        .send()
        .await;
    assert_refused_with(&refused(result), NUMERIC);
}

#[tokio::test]
async fn delete_item_reports_the_mixed_parameter_error_ahead_of_value_numbers() {
    // "DeleteItem Expected + CE + EAV abc": the mixed-parameter error.
    let t = tables().await;
    let result = client()
        .delete_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(string_key("o618")))
        .expected("a", not_exists())
        .condition_expression("attribute_not_exists(b) OR b = :v")
        .expression_attribute_values(":v", n_raw("abc"))
        .send()
        .await;
    assert_refused_with(&refused(result), MIXED);
}

#[tokio::test]
async fn update_item_reports_the_key_number_ahead_of_the_mixed_parameter_error() {
    // "UpdateExpression + Expected + key pk abc" and "ConditionExpression +
    // Expected + key pk abc": the number on both.
    let t = tables().await;
    let result = client()
        .update_item()
        .table_name(&t.comp_key_number_string)
        .set_key(Some(number_key("abc")))
        .update_expression("SET a = :s")
        .expression_attribute_values(":s", s("x"))
        .expected("x", not_exists())
        .send()
        .await;
    assert_refused_with(&refused(result), NUMERIC);
}

#[tokio::test]
async fn put_item_reports_the_item_number_ahead_of_an_absent_table() {
    // "PutItem absent table + item n abc": the number.
    let result = client()
        .put_item()
        .table_name(missing_table())
        .set_item(Some(item_with_a_malformed_number("o618")))
        .send()
        .await;
    assert_refused_with(&refused(result), NUMERIC);
}

#[tokio::test]
async fn put_item_reports_the_item_number_ahead_of_the_mixed_parameter_error() {
    // "PutItem Expected + CE + item n abc": the number.
    let t = tables().await;
    let result = client()
        .put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item_with_a_malformed_number("o618")))
        .expected("a", not_exists())
        .condition_expression("attribute_not_exists(b)")
        .send()
        .await;
    assert_refused_with(&refused(result), NUMERIC);
}

#[tokio::test]
async fn put_item_reports_the_mixed_parameter_error_ahead_of_value_numbers() {
    // "PutItem Expected + ConditionExpression + EAV abc": the mixed-parameter
    // error.
    let t = tables().await;
    let result = client()
        .put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(string_key("o618")))
        .expected("a", not_exists())
        .condition_expression("attribute_not_exists(b) OR b = :v")
        .expression_attribute_values(":v", n_raw("abc"))
        .send()
        .await;
    assert_refused_with(&refused(result), MIXED);
}

#[tokio::test]
async fn batch_get_item_reports_a_key_number_ahead_of_an_absent_table() {
    // "BatchGetItem absent-table", one table with the key pk abc: the number.
    // One table, so the answer does not depend on the order of RequestItems.
    let keys = KeysAndAttributes::builder()
        .keys(number_key("abc"))
        .build()
        .expect("keys");
    let result = client()
        .batch_get_item()
        .request_items(missing_table(), keys)
        .send()
        .await;
    assert_refused_with(&refused(result), NUMERIC);
}

#[tokio::test]
async fn batch_write_item_reports_a_put_number_ahead_of_an_absent_table() {
    // "BatchWriteItem put absent-table", one table with a put holding n abc:
    // the number.
    let put = put_request(item_with_a_malformed_number("o618"));
    let err = batch_write(&missing_table(), vec![put]).await;
    assert_refused_with(&err.unwrap_err(), NUMERIC);
}

#[tokio::test]
async fn transact_get_items_reports_an_earlier_key_number_ahead_of_a_later_projection() {
    // "TransactGetItems get1 key pk abc + get2 projection syntax": the number.
    let t = tables().await;
    let get = |key: Key, projection: Option<&str>| {
        let get = Get::builder()
            .table_name(&t.comp_key_number_string)
            .set_key(Some(key))
            .set_projection_expression(projection.map(str::to_string));
        TransactGetItem::builder()
            .get(get.build().expect("get"))
            .build()
    };
    let result = client()
        .transact_get_items()
        .transact_items(get(number_key("abc"), None))
        .transact_items(get(number_key("2"), Some(",,b")))
        .send()
        .await;
    assert_refused_with(&refused(result), NUMERIC);
}

#[tokio::test]
async fn transact_write_items_reports_an_earlier_key_number_ahead_of_a_later_condition_syntax() {
    // "TransactWriteItems item1 key pk abc + item2 condition syntax": the
    // number.
    let t = tables().await;
    let first = delete_of(&t.comp_key_number_string, number_key("abc"));
    let second = ConditionCheck::builder()
        .table_name(&t.simple_key_string)
        .set_key(Some(string_key("o618")))
        .condition_expression("a = = :w")
        .expression_attribute_values(":w", s("x"))
        .build()
        .expect("check");
    let err = transact_write(vec![txn_delete(first), txn_check(second)]).await;
    assert_refused_with(&err.unwrap_err(), NUMERIC);
}
