// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The result of `+`, `-` and a number `ADD` in an `UpdateExpression`, on
//! `UpdateItem` and a `TransactWriteItems` `Update`: how a result in range is
//! spelled, and the answer for one that is not.
//!
//! Every row was measured against DynamoDB on 2026-11-03 (us-east-1) on both
//! operations, each request sent twice with the same answer both times. The
//! two `1.5 + 1.5` rows were measured on `UpdateItem` only.

use std::collections::HashMap;

use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
use aws_sdk_dynamodb::types::{AttributeValue, ReturnValue, TransactWriteItem, Update};

use crate::test_base::*;

const OVERFLOW: &str =
    "Number overflow. Attempting to store a number with magnitude larger than supported range";

fn number(value: &str) -> AttributeValue {
    AttributeValue::N(value.to_string())
}

/// Store an item whose `n` holds `stored` and return its key.
async fn seed(table: &str, stored: &str) -> HashMap<String, AttributeValue> {
    let mut item = create_item(table);
    item.insert("n".into(), number(stored));
    client()
        .put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    get_key(table, &item)
}

async fn stored_n(table: &str, key: HashMap<String, AttributeValue>) -> Option<AttributeValue> {
    client()
        .get_item()
        .table_name(table)
        .set_key(Some(key))
        .consistent_read(true)
        .send()
        .await
        .unwrap()
        .item()
        .and_then(|item| item.get("n").cloned())
}

fn transact_update(
    table: &str,
    key: HashMap<String, AttributeValue>,
    expression: &str,
    value: &str,
) -> TransactWriteItem {
    TransactWriteItem::builder()
        .update(
            Update::builder()
                .table_name(table)
                .set_key(Some(key))
                .update_expression(expression)
                .expression_attribute_values(":v", number(value))
                .build()
                .unwrap(),
        )
        .build()
}

#[tokio::test]
async fn arithmetic_result_is_stored_as_the_service_spells_it() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let min = format!("0.{}1", "0".repeat(129));
    let twice_max_magnitude = format!("2{}", "0".repeat(125));
    let rows = [
        ("2.25", "SET n = n - :v", "0.25", "2"),
        ("2.25", "ADD n :v", "-0.25", "2"),
        ("1.5", "SET n = n - :v", "1.5", "0"),
        ("2E-130", "SET n = n - :v", "1E-130", min.as_str()),
        (
            "1E125",
            "SET n = n + :v",
            "1E125",
            twice_max_magnitude.as_str(),
        ),
    ];
    for (stored, expression, value, expected) in rows {
        let key = seed(table, stored).await;
        let resp = c
            .update_item()
            .table_name(table)
            .set_key(Some(key.clone()))
            .update_expression(expression)
            .expression_attribute_values(":v", number(value))
            .return_values(ReturnValue::UpdatedNew)
            .send()
            .await
            .unwrap_or_else(|e| panic!("UpdateItem `{expression}`, n = {stored}: {}", err_msg(&e)));
        let returned = resp.attributes().and_then(|a| a.get("n")).cloned();
        assert_eq!(
            returned,
            Some(number(expected)),
            "UpdateItem `{expression}`, n = {stored}"
        );
        assert_eq!(
            stored_n(table, key).await,
            Some(number(expected)),
            "UpdateItem `{expression}`"
        );

        let key = seed(table, stored).await;
        c.transact_write_items()
            .transact_items(transact_update(table, key.clone(), expression, value))
            .send()
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "TransactWriteItems `{expression}`, n = {stored}: {}",
                    err_msg(&e)
                )
            });
        assert_eq!(
            stored_n(table, key).await,
            Some(number(expected)),
            "TransactWriteItems `{expression}`"
        );
    }
    // Measured on UpdateItem only.
    for expression in ["SET n = n + :v", "ADD n :v"] {
        let key = seed(table, "1.5").await;
        c.update_item()
            .table_name(table)
            .set_key(Some(key.clone()))
            .update_expression(expression)
            .expression_attribute_values(":v", number("1.5"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            stored_n(table, key).await,
            Some(number("3")),
            "`{expression}`"
        );
    }
}

/// A result outside the range or with more than 38 digits is a
/// `Number overflow` with no prefix, a result below the range included. On
/// `TransactWriteItems` it is the cancellation reason.
#[tokio::test]
async fn arithmetic_result_out_of_range_is_an_overflow() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let rows = [
        ("5E125", "SET n = n + :v", "5E125"),
        ("5E125", "ADD n :v", "5E125"),
        ("1E125", "SET n = n + :v", "1"),
        ("1.1E-130", "SET n = n - :v", "1E-130"),
        ("1.1E-130", "ADD n :v", "-1E-130"),
        ("-1.1E-130", "SET n = n + :v", "1E-130"),
    ];
    for (stored, expression, value) in rows {
        let key = seed(table, stored).await;
        let before = stored_n(table, key.clone()).await;
        let err = c
            .update_item()
            .table_name(table)
            .set_key(Some(key.clone()))
            .update_expression(expression)
            .expression_attribute_values(":v", number(value))
            .send()
            .await
            .expect_err("an out-of-range result must be refused");
        assert_eq!(
            err_code(&err),
            Some("ValidationException"),
            "`{expression}`, n = {stored}"
        );
        assert_eq!(
            err_msg(&err),
            OVERFLOW,
            "UpdateItem `{expression}`, n = {stored}"
        );
        assert_eq!(
            stored_n(table, key.clone()).await,
            before,
            "the item keeps its value"
        );

        let err = c
            .transact_write_items()
            .transact_items(transact_update(table, key.clone(), expression, value))
            .send()
            .await
            .expect_err("an out-of-range result must cancel the transaction");
        let message = err_msg(&err);
        let TransactWriteItemsError::TransactionCanceledException(tce) = err.into_service_error()
        else {
            panic!(
                "expected TransactionCanceledException for `{expression}`, n = {stored}: {message}"
            );
        };
        let reasons: Vec<(Option<&str>, Option<&str>)> = tce
            .cancellation_reasons()
            .iter()
            .map(|r| (r.code(), r.message()))
            .collect();
        assert_eq!(
            reasons,
            [(Some("ValidationError"), Some(OVERFLOW))],
            "`{expression}`, n = {stored}"
        );
        assert_eq!(
            stored_n(table, key).await,
            before,
            "the item keeps its value"
        );
    }
}
