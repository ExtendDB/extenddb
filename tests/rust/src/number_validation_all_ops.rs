// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Number (`N`) validation on the operations that did not read a number at
//! all (#618): the keys of `GetItem`, `DeleteItem`, `UpdateItem`,
//! `BatchGetItem` and `TransactGetItems`, the items and keys of
//! `BatchWriteItem` and `TransactWriteItems`, and the
//! `ExpressionAttributeValues` of `PutItem`, `DeleteItem` and
//! `TransactWriteItems`.
//!
//! Every message below was measured against DynamoDB on 2026-10-27
//! (us-east-1), each request sent twice with the same answer both times.
//! `PutItem`, `UpdateItem` and `DeleteItem` start the message with
//! "1 validation error detected: " and the other operations do not.
//! `TransactWriteItems` reports a value in `ExpressionAttributeValues` as
//! "ExpressionAttributeValues contains invalid value: <message> for key :v".
//! More tests of the order of these checks against other faults in the same
//! request are in `number_validation_order`, which shares the helpers below.

use std::collections::HashMap;

use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use aws_sdk_dynamodb::types::{
    AttributeValue, ConditionCheck, Delete, DeleteRequest, Get, KeysAndAttributes, Put, PutRequest,
    TransactGetItem, TransactWriteItem, Update, WriteRequest,
};
use aws_smithy_runtime_api::client::result::SdkError;

use crate::test_base::*;

pub(crate) type Key = HashMap<String, AttributeValue>;

pub(crate) const NUMERIC: &str = "The parameter cannot be converted to a numeric value: abc";
const OVERFLOW: &str =
    "Number overflow. Attempting to store a number with magnitude larger than supported range";
/// A number whose magnitude is over the supported range.
const OVER_RANGE: &str = "1E126";
/// The prefix `PutItem`, `UpdateItem` and `DeleteItem` put on a message.
const PREFIX: &str = "1 validation error detected: ";
const CONDITION: &str = "attribute_not_exists(amount) OR amount = :v";

/// What `TransactWriteItems` answers for a value in `ExpressionAttributeValues`.
fn in_values(message: &str) -> String {
    format!("ExpressionAttributeValues contains invalid value: {message} for key :v")
}

pub(crate) fn n_raw(value: &str) -> AttributeValue {
    AttributeValue::N(value.to_string())
}

/// A key of `comp_key_number_string`, whose hash key is an `N`.
pub(crate) fn number_key(value: &str) -> Key {
    HashMap::from([
        (HASH_KEY_N.to_string(), n_raw(value)),
        (RANGE_KEY_S.to_string(), s("x")),
    ])
}

/// A key of `simple_key_string`, whose hash key is an `S`.
pub(crate) fn string_key(name: &str) -> Key {
    HashMap::from([(HASH_KEY_S.to_string(), s(&format!("{name}_{}", ts())))])
}

/// An error as "Code:message".
fn describe<E, R>(err: &SdkError<E, R>) -> String
where
    E: ProvideErrorMetadata,
{
    format!("{}:{}", err_code(err).unwrap_or("None"), err_msg(err))
}

/// The error a request failed with; panics when it succeeded.
pub(crate) fn refused<T, E, R>(result: Result<T, SdkError<E, R>>) -> String
where
    E: ProvideErrorMetadata,
{
    match result {
        Ok(_) => panic!("the request must be refused"),
        Err(err) => describe(&err),
    }
}

fn assert_refused(err: &str, message: &str) {
    assert_eq!(err, format!("ValidationException:{message}"));
}

pub(crate) fn put_request(item: Key) -> WriteRequest {
    let put = PutRequest::builder().set_item(Some(item)).build();
    WriteRequest::builder()
        .put_request(put.expect("put"))
        .build()
}

fn delete_request(key: Key) -> WriteRequest {
    let delete = DeleteRequest::builder().set_key(Some(key)).build();
    WriteRequest::builder()
        .delete_request(delete.expect("delete"))
        .build()
}

pub(crate) async fn batch_write(table: &str, requests: Vec<WriteRequest>) -> Result<(), String> {
    let result = client()
        .batch_write_item()
        .request_items(table, requests)
        .send()
        .await;
    result.map(|_| ()).map_err(|e| describe(&e))
}

pub(crate) async fn transact_write(items: Vec<TransactWriteItem>) -> Result<(), String> {
    let result = client()
        .transact_write_items()
        .set_transact_items(Some(items))
        .send()
        .await;
    result.map(|_| ()).map_err(|e| describe(&e))
}

fn txn_put(put: Put) -> TransactWriteItem {
    TransactWriteItem::builder().put(put).build()
}

pub(crate) fn txn_delete(delete: Delete) -> TransactWriteItem {
    TransactWriteItem::builder().delete(delete).build()
}

fn txn_update(update: Update) -> TransactWriteItem {
    TransactWriteItem::builder().update(update).build()
}

pub(crate) fn txn_check(check: ConditionCheck) -> TransactWriteItem {
    TransactWriteItem::builder().condition_check(check).build()
}

fn put_of(table: &str, item: Key) -> Put {
    let put = Put::builder().table_name(table).set_item(Some(item));
    put.build().expect("put")
}

pub(crate) fn delete_of(table: &str, key: Key) -> Delete {
    let delete = Delete::builder().table_name(table).set_key(Some(key));
    delete.build().expect("delete")
}

// ---- keys ----

#[tokio::test]
async fn get_item_rejects_a_malformed_number_in_the_key() {
    let t = tables().await;
    let result = client()
        .get_item()
        .table_name(&t.comp_key_number_string)
        .set_key(Some(number_key("abc")))
        .send()
        .await;
    assert_refused(&refused(result), NUMERIC);
}

#[tokio::test]
async fn delete_item_rejects_a_malformed_number_in_the_key() {
    let t = tables().await;
    let result = client()
        .delete_item()
        .table_name(&t.comp_key_number_string)
        .set_key(Some(number_key("abc")))
        .send()
        .await;
    assert_refused(&refused(result), &format!("{PREFIX}{NUMERIC}"));
}

#[tokio::test]
async fn update_item_rejects_a_malformed_number_in_the_key() {
    let t = tables().await;
    let result = client()
        .update_item()
        .table_name(&t.comp_key_number_string)
        .set_key(Some(number_key("abc")))
        .update_expression("SET a = :v")
        .expression_attribute_values(":v", s("ok"))
        .send()
        .await;
    assert_refused(&refused(result), &format!("{PREFIX}{NUMERIC}"));
}

#[tokio::test]
async fn batch_get_item_rejects_a_malformed_number_in_a_key() {
    let t = tables().await;
    let keys = KeysAndAttributes::builder()
        .keys(number_key("abc"))
        .build()
        .expect("keys");
    let result = client()
        .batch_get_item()
        .request_items(&t.comp_key_number_string, keys)
        .send()
        .await;
    assert_refused(&refused(result), NUMERIC);
}

#[tokio::test]
async fn transact_get_items_rejects_a_malformed_number_in_a_key() {
    let t = tables().await;
    let get = Get::builder()
        .table_name(&t.comp_key_number_string)
        .set_key(Some(number_key("abc")))
        .build()
        .expect("get");
    let result = client()
        .transact_get_items()
        .transact_items(TransactGetItem::builder().get(get).build())
        .send()
        .await;
    assert_refused(&refused(result), NUMERIC);
}

#[tokio::test]
async fn batch_write_item_rejects_a_malformed_number_in_a_delete_key() {
    let t = tables().await;
    let err = batch_write(
        &t.comp_key_number_string,
        vec![delete_request(number_key("abc"))],
    )
    .await;
    assert_refused(&err.unwrap_err(), NUMERIC);
}

#[tokio::test]
async fn transact_write_items_rejects_a_malformed_number_in_a_delete_key() {
    let t = tables().await;
    let delete = delete_of(&t.comp_key_number_string, number_key("abc"));
    let err = transact_write(vec![txn_delete(delete)]).await;
    assert_refused(&err.unwrap_err(), NUMERIC);
}

#[tokio::test]
async fn transact_write_items_rejects_a_malformed_number_in_an_update_key() {
    let t = tables().await;
    let update = Update::builder()
        .table_name(&t.comp_key_number_string)
        .set_key(Some(number_key("abc")))
        .update_expression("SET a = :v")
        .expression_attribute_values(":v", s("ok"))
        .build()
        .expect("update");
    let err = transact_write(vec![txn_update(update)]).await;
    assert_refused(&err.unwrap_err(), NUMERIC);
}

#[tokio::test]
async fn transact_write_items_rejects_a_malformed_number_in_a_condition_check_key() {
    let t = tables().await;
    let check = ConditionCheck::builder()
        .table_name(&t.comp_key_number_string)
        .set_key(Some(number_key("abc")))
        .condition_expression("attribute_not_exists(a)")
        .build()
        .expect("check");
    let err = transact_write(vec![txn_check(check)]).await;
    assert_refused(&err.unwrap_err(), NUMERIC);
}

// ---- items ----

#[tokio::test]
async fn batch_write_item_rejects_a_malformed_number_in_a_put_item() {
    let t = tables().await;
    let mut item = string_key("b618");
    item.insert("amount".to_string(), n_raw("abc"));
    let err = batch_write(&t.simple_key_string, vec![put_request(item)]).await;
    assert_refused(&err.unwrap_err(), NUMERIC);
}

#[tokio::test]
async fn batch_write_item_rejects_a_number_set_member_out_of_range() {
    let t = tables().await;
    let mut item = string_key("b618");
    let members = vec!["1".to_string(), "1E126".to_string()];
    item.insert("amounts".to_string(), AttributeValue::Ns(members));
    let err = batch_write(&t.simple_key_string, vec![put_request(item)]).await;
    assert_refused(&err.unwrap_err(), OVERFLOW);
}

#[tokio::test]
async fn batch_write_item_rejects_a_malformed_number_sort_key() {
    // Answered InternalServerError before #618.
    let t = tables().await;
    let item = HashMap::from([
        (HASH_KEY_S.to_string(), s("b618")),
        (RANGE_KEY_N.to_string(), n_raw("abc")),
    ]);
    let err = batch_write(&t.comp_key_string_number, vec![put_request(item)]).await;
    assert_refused(&err.unwrap_err(), NUMERIC);
}

#[tokio::test]
async fn transact_write_items_rejects_a_malformed_number_in_a_put_item() {
    let t = tables().await;
    let mut item = string_key("t618");
    item.insert("amount".to_string(), n_raw("abc"));
    let err = transact_write(vec![txn_put(put_of(&t.simple_key_string, item))]).await;
    assert_refused(&err.unwrap_err(), NUMERIC);
}

#[tokio::test]
async fn transact_write_items_rejects_a_malformed_number_sort_key() {
    // Answered InternalServerError before #618.
    let t = tables().await;
    let item = HashMap::from([
        (HASH_KEY_S.to_string(), s("t618")),
        (RANGE_KEY_N.to_string(), n_raw("abc")),
    ]);
    let err = transact_write(vec![txn_put(put_of(&t.comp_key_string_number, item))]).await;
    assert_refused(&err.unwrap_err(), NUMERIC);
}

// ---- ExpressionAttributeValues ----

#[tokio::test]
async fn put_item_rejects_a_condition_value_out_of_range() {
    let t = tables().await;
    let result = client()
        .put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(string_key("p618")))
        .condition_expression(CONDITION)
        .expression_attribute_values(":v", n_raw(OVER_RANGE))
        .send()
        .await;
    assert_refused(&refused(result), &format!("{PREFIX}{OVERFLOW}"));
}

#[tokio::test]
async fn delete_item_rejects_a_condition_value_out_of_range() {
    let t = tables().await;
    let result = client()
        .delete_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(string_key("d618")))
        .condition_expression(CONDITION)
        .expression_attribute_values(":v", n_raw(OVER_RANGE))
        .send()
        .await;
    assert_refused(&refused(result), &format!("{PREFIX}{OVERFLOW}"));
}

#[tokio::test]
async fn transact_write_items_rejects_a_condition_check_value_out_of_range() {
    let t = tables().await;
    let check = ConditionCheck::builder()
        .table_name(&t.simple_key_string)
        .set_key(Some(string_key("t618")))
        .condition_expression(CONDITION)
        .expression_attribute_values(":v", n_raw(OVER_RANGE))
        .build()
        .expect("check");
    let err = transact_write(vec![txn_check(check)]).await;
    assert_refused(&err.unwrap_err(), &in_values(OVERFLOW));
}

#[tokio::test]
async fn transact_write_items_rejects_a_put_condition_value_out_of_range() {
    let t = tables().await;
    let put = Put::builder()
        .table_name(&t.simple_key_string)
        .set_item(Some(string_key("t618")))
        .condition_expression(CONDITION)
        .expression_attribute_values(":v", n_raw(OVER_RANGE))
        .build()
        .expect("put");
    let err = transact_write(vec![txn_put(put)]).await;
    assert_refused(&err.unwrap_err(), &in_values(OVERFLOW));
}

#[tokio::test]
async fn transact_write_items_rejects_an_update_value_out_of_range() {
    let t = tables().await;
    let update = Update::builder()
        .table_name(&t.simple_key_string)
        .set_key(Some(string_key("t618")))
        .update_expression("SET amount = :v")
        .expression_attribute_values(":v", n_raw(OVER_RANGE))
        .build()
        .expect("update");
    let err = transact_write(vec![txn_update(update)]).await;
    assert_refused(&err.unwrap_err(), &in_values(OVERFLOW));
}

#[tokio::test]
async fn transact_write_items_rejects_a_delete_condition_value_out_of_range() {
    let t = tables().await;
    let delete = Delete::builder()
        .table_name(&t.simple_key_string)
        .set_key(Some(string_key("t618")))
        .condition_expression(CONDITION)
        .expression_attribute_values(":v", n_raw(OVER_RANGE))
        .build()
        .expect("delete");
    let err = transact_write(vec![txn_delete(delete)]).await;
    assert_refused(&err.unwrap_err(), &in_values(OVERFLOW));
}

// ---- behaviour around the checks ----

#[tokio::test]
async fn batch_write_item_writes_nothing_when_a_later_put_is_refused() {
    let t = tables().await;
    let good = string_key("b618_good");
    let mut bad = string_key("b618_bad");
    bad.insert("amount".to_string(), n_raw("abc"));
    let requests = vec![put_request(good.clone()), put_request(bad)];
    let err = batch_write(&t.simple_key_string, requests).await;
    assert_refused(&err.unwrap_err(), NUMERIC);

    let stored = client()
        .get_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(good))
        .consistent_read(true)
        .send()
        .await
        .expect("GetItem");
    assert!(
        stored.item().is_none(),
        "the earlier put must not be written"
    );
}

#[tokio::test]
async fn transact_write_items_reports_an_earlier_actions_unused_value_first() {
    // Measured 2026-10-27: an unused value in the first action is reported
    // ahead of a malformed number in the second.
    let t = tables().await;
    let check = ConditionCheck::builder()
        .table_name(&t.simple_key_string)
        .set_key(Some(string_key("t618")))
        .condition_expression("attribute_not_exists(amount)")
        .expression_attribute_values(":w", s("x"))
        .build()
        .expect("check");
    let delete = delete_of(&t.comp_key_number_string, number_key("abc"));
    let err = transact_write(vec![txn_check(check), txn_delete(delete)]).await;
    assert_refused(
        &err.unwrap_err(),
        "Value provided in ExpressionAttributeValues unused in expressions: keys: {:w}",
    );
}

#[tokio::test]
async fn transact_write_items_reports_a_long_client_request_token_first() {
    // Measured 2026-10-27: a 37-character token is reported ahead of a
    // malformed key number.
    let t = tables().await;
    let delete = delete_of(&t.comp_key_number_string, number_key("abc"));
    let result = client()
        .transact_write_items()
        .transact_items(txn_delete(delete))
        .client_request_token("t".repeat(37))
        .send()
        .await;
    let err = refused(result);
    assert!(
        err.starts_with("ValidationException:")
            && err.contains("at 'clientRequestToken' failed to satisfy constraint"),
        "expected the token's constraint message, got: {err}"
    );
}

#[tokio::test]
async fn transact_get_items_reports_an_unused_name_ahead_of_the_key_number() {
    // Measured 2026-10-27: the item's projection checks come first.
    let t = tables().await;
    let get = Get::builder()
        .table_name(&t.comp_key_number_string)
        .set_key(Some(number_key("abc")))
        .projection_expression("a")
        .expression_attribute_names("#u", "u")
        .build()
        .expect("get");
    let result = client()
        .transact_get_items()
        .transact_items(TransactGetItem::builder().get(get).build())
        .send()
        .await;
    assert_refused(
        &refused(result),
        "Value provided in ExpressionAttributeNames unused in expressions: keys: {#u}",
    );
}

#[tokio::test]
async fn batch_get_item_reports_duplicate_keys_ahead_of_their_number() {
    // Measured 2026-10-27.
    let t = tables().await;
    let keys = KeysAndAttributes::builder()
        .keys(number_key("abc"))
        .keys(number_key("abc"))
        .build()
        .expect("keys");
    let result = client()
        .batch_get_item()
        .request_items(&t.comp_key_number_string, keys)
        .send()
        .await;
    assert_refused(
        &refused(result),
        "Provided list of item keys contains duplicates",
    );
}

#[tokio::test]
async fn a_number_over_the_range_is_refused_in_get_and_delete_keys() {
    // The GetItem key, a BatchWriteItem delete key and a TransactWriteItems
    // delete key.
    let t = tables().await;
    let table = &t.comp_key_number_string;
    let get = client()
        .get_item()
        .table_name(table)
        .set_key(Some(number_key(OVER_RANGE)))
        .send()
        .await;
    assert_refused(&refused(get), OVERFLOW);

    let err = batch_write(table, vec![delete_request(number_key(OVER_RANGE))]).await;
    assert_refused(&err.unwrap_err(), OVERFLOW);

    let err = transact_write(vec![txn_delete(delete_of(table, number_key(OVER_RANGE)))]).await;
    assert_refused(&err.unwrap_err(), OVERFLOW);
}

#[tokio::test]
async fn a_zero_inside_the_range_is_stored_as_zero_on_batch_and_transact_writes() {
    // Measured 2026-10-27: `0.0E126` and `-0E-130` are stored as 0.
    let t = tables().await;
    for (writer, value) in [("batch", "0.0E126"), ("transact", "-0E-130")] {
        let key = string_key("z618");
        let mut item = key.clone();
        item.insert("amount".to_string(), n_raw(value));
        let outcome = if writer == "batch" {
            batch_write(&t.simple_key_string, vec![put_request(item)]).await
        } else {
            transact_write(vec![txn_put(put_of(&t.simple_key_string, item))]).await
        };
        outcome.expect("a zero inside the range is accepted");

        let stored = client()
            .get_item()
            .table_name(&t.simple_key_string)
            .set_key(Some(key))
            .consistent_read(true)
            .send()
            .await
            .expect("GetItem");
        let amount = stored.item().and_then(|item| item.get("amount"));
        assert_eq!(amount, Some(&n_raw("0")), "{writer} write of {value}");
    }
}
