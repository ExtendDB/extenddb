// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! UpdateItem number-value validation. Malformed, out-of-range, and invalid
//! number-set values supplied via UpdateExpression (ExpressionAttributeValues)
//! or the legacy AttributeUpdates map must be rejected with a
//! ValidationException, matching real DynamoDB. Values referenced only in an
//! UpdateExpression were previously stored without number validation.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{AttributeAction, AttributeValue, AttributeValueUpdate};
use std::collections::HashMap;

async fn seed(table: &str) -> HashMap<String, AttributeValue> {
    let c = client();
    let item = create_item(table);
    c.put_item()
        .table_name(table)
        .set_item(Some(item.clone()))
        .send()
        .await
        .unwrap();
    get_key(table, &item)
}

#[tokio::test]
async fn update_item_rejects_malformed_number_in_expression() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let key = seed(table).await;
    let e = c
        .update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET bad = :v")
        .expression_attribute_values(":v", AttributeValue::N("12e".into()))
        .send()
        .await
        .expect_err("malformed number must be rejected");
    assert_eq!(err_code(&e), Some("ValidationException"), "{}", err_msg(&e));
    assert!(
        err_msg(&e).contains("ExpressionAttributeValues contains invalid value")
            && err_msg(&e).contains("cannot be converted to a numeric value: 12e"),
        "got: {}",
        err_msg(&e)
    );
}

#[tokio::test]
async fn update_item_rejects_number_overflow_in_expression() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let key = seed(table).await;
    let big = format!("1{}", "0".repeat(200));
    let e = c
        .update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET bad = :v")
        .expression_attribute_values(":v", AttributeValue::N(big))
        .send()
        .await
        .expect_err("out-of-range number must be rejected");
    assert_eq!(err_code(&e), Some("ValidationException"), "{}", err_msg(&e));
    assert!(
        err_msg(&e).contains("Number overflow"),
        "got: {}",
        err_msg(&e)
    );
}

#[tokio::test]
async fn update_item_rejects_invalid_number_set_member_in_expression() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let key = seed(table).await;
    let e = c
        .update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET bad = :v")
        .expression_attribute_values(":v", AttributeValue::Ns(vec!["1".into(), "abc".into()]))
        .send()
        .await
        .expect_err("invalid number-set member must be rejected");
    assert_eq!(err_code(&e), Some("ValidationException"), "{}", err_msg(&e));
    assert!(
        err_msg(&e).contains("cannot be converted to a numeric value: abc"),
        "got: {}",
        err_msg(&e)
    );
}

#[tokio::test]
async fn update_item_rejects_bad_number_in_attribute_updates() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let key = seed(table).await;
    let e = c
        .update_item()
        .table_name(table)
        .set_key(Some(key))
        .attribute_updates(
            "bad",
            AttributeValueUpdate::builder()
                .value(AttributeValue::N("not_a_num".into()))
                .action(AttributeAction::Put)
                .build(),
        )
        .send()
        .await
        .expect_err("bad AttributeUpdates number must be rejected");
    assert_eq!(err_code(&e), Some("ValidationException"), "{}", err_msg(&e));
    assert!(
        err_msg(&e).contains("cannot be converted to a numeric value: not_a_num"),
        "got: {}",
        err_msg(&e)
    );
}

#[tokio::test]
async fn update_item_accepts_valid_number_in_expression() {
    let c = client();
    let t = tables().await;
    let table = &t.simple_key_string;
    let key = seed(table).await;
    c.update_item()
        .table_name(table)
        .set_key(Some(key))
        .update_expression("SET good = :v")
        .expression_attribute_values(":v", AttributeValue::N("42".into()))
        .send()
        .await
        .expect("valid number must be accepted");
}
