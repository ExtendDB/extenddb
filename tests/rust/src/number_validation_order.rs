// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The order of the #618 number checks against another fault in the same
//! request. Each test sends a request holding a malformed number and one
//! other fault, and expects the fault DynamoDB reported first. Every
//! expectation is a row of the measured service answers (us-east-1, each
//! request sent twice with the same answer both times), named in the test's
//! comment: 2026-10-27, and 2026-11-11 for the rows with an attribute name
//! over the size limit. For an item that holds such a name and a malformed
//! number, the service gave either answer to the same request, and those
//! tests accept either.
//!
//! The tests check that the answer contains the expected message rather than
//! equals it: DynamoDB puts `1 validation error detected: ` on the PutItem
//! item and mixed-parameter messages, and ExtendDB does not, which is a known
//! difference these tests leave alone.

use aws_sdk_dynamodb::types::{
    ConditionCheck, ExpectedAttributeValue, Get, KeysAndAttributes, Put, TransactGetItem,
    TransactWriteItem,
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

// ---- an attribute name over the size limit (measured 2026-11-11) ----

/// The size refusal for a 65,536-byte attribute name in an item.
const LONG_NAME: &str = "Attribute name is too large, must be less than 65536 bytes";
/// The answer for `1E2147483648`.
const OVERFLOW: &str =
    "Number overflow. Attempting to store a number with magnitude larger than supported range";
/// A 37-character `ClientRequestToken`, one over the limit.
const TOKEN_TEXT: &str =
    "failed to satisfy constraint: Member must have length less than or equal to 36";

/// An attribute name of 65,536 bytes, one over the limit.
fn long_name() -> String {
    "a".repeat(65_536)
}

/// An item of `simple_key_string` holding a 65,536-byte attribute name and,
/// when `number` is set, the number `1E2147483648`.
fn item_with_a_long_name(name: &str, number: bool) -> Key {
    let mut item = string_key(name);
    item.insert(long_name(), s("x"));
    if number {
        item.insert("n".to_string(), n_raw("1E2147483648"));
    }
    item
}

/// An item of `simple_key_string` holding the number `1E2147483648`.
fn item_with_an_overflow(name: &str) -> Key {
    let mut item = string_key(name);
    item.insert("n".to_string(), n_raw("1E2147483648"));
    item
}

/// Assert a `ValidationException` whose message ends with the name refusal
/// or with the overflow, the two answers the service gave.
fn assert_name_or_overflow(err: &str) {
    assert!(
        err.starts_with("ValidationException:")
            && (err.ends_with(LONG_NAME) || err.ends_with(OVERFLOW)),
        "expected the name refusal or the overflow, got {}",
        &err[..err.len().min(300)]
    );
}

/// Assert the refusal of a 37-character `ClientRequestToken`. The member name
/// in the service's message was `ClientRequestToken` on 2026-11-11 and
/// `clientRequestToken` on 2026-10-27, so the check ignores its case.
pub(crate) fn assert_token_refused(err: &str) {
    let lower = err.to_ascii_lowercase();
    assert!(
        err.starts_with("ValidationException:")
            && lower.contains("value at 'clientrequesttoken'")
            && err.contains(TOKEN_TEXT),
        "expected the token's constraint message, got {}",
        &err[..err.len().min(300)]
    );
}

#[tokio::test]
async fn put_batch_and_transact_items_answer_a_long_name_or_the_number() {
    // "PutItem item {pk, LONG, n}": the name on 6 of 8 sends and the
    // overflow on 2; "{pk, n, LONG}": the overflow on 2 of 2. The same for
    // BatchWriteItem (6 and 2 of 8; 2 of 2) and for a TransactWriteItems Put
    // (8 and 4 of 12; the name on 2 of 8 and the overflow on 6). The SDK does
    // not keep the attribute order, so each answer is accepted. ExtendDB
    // reports the name: its size check runs first.
    let t = tables().await;
    let result = client()
        .put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item_with_a_long_name("o618", true)))
        .send()
        .await;
    assert_name_or_overflow(&refused(result));

    let put = put_request(item_with_a_long_name("o618", true));
    let err = batch_write(&t.simple_key_string, vec![put]).await;
    assert_name_or_overflow(&err.unwrap_err());

    let put = Put::builder()
        .table_name(&t.simple_key_string)
        .set_item(Some(item_with_a_long_name("o618", true)))
        .build()
        .expect("put");
    let err = transact_write(vec![TransactWriteItem::builder().put(put).build()]).await;
    assert_name_or_overflow(&err.unwrap_err());
}

#[tokio::test]
async fn delete_and_update_item_report_a_long_name_ahead_of_value_numbers() {
    // "DeleteItem, #n = LONG in ExpressionAttributeNames, :v in
    // ExpressionAttributeValues" and "UpdateItem SET #n = :v, same maps": the
    // name for key #n, 4 of 4 each, with either map first in the body.
    let t = tables().await;
    let want = format!("ValidationException:1 validation error detected: {LONG_NAME} for key #n");
    let result = client()
        .delete_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(string_key("o618")))
        .condition_expression("#n = :v")
        .expression_attribute_names("#n", long_name())
        .expression_attribute_values(":v", n_raw("1E2147483648"))
        .send()
        .await;
    assert_eq!(refused(result), want);

    let result = client()
        .update_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(string_key("o618")))
        .update_expression("SET #n = :v")
        .expression_attribute_names("#n", long_name())
        .expression_attribute_values(":v", n_raw("1E2147483648"))
        .send()
        .await;
    assert_eq!(refused(result), want);
}

#[tokio::test]
async fn transact_write_items_reports_a_long_token_ahead_of_a_long_name_and_a_number() {
    // "TransactWriteItems ClientRequestToken of 37 characters, Put item {pk,
    // LONG}" and "..., Put item {pk, n}": the token, 2 of 2 each.
    let t = tables().await;
    for item in [
        item_with_a_long_name("o618", false),
        item_with_an_overflow("o618"),
    ] {
        let put = Put::builder()
            .table_name(&t.simple_key_string)
            .set_item(Some(item))
            .build()
            .expect("put");
        let result = client()
            .transact_write_items()
            .transact_items(TransactWriteItem::builder().put(put).build())
            .client_request_token("x".repeat(37))
            .send()
            .await;
        assert_token_refused(&refused(result));
    }
}
