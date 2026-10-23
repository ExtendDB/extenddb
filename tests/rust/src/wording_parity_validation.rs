// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Validation message/behavior parity for a few request-validation cases that
//! previously diverged from real DynamoDB (verified against us-east-1):
//!
//! - PutItem/DeleteItem `ReturnValues`: a valid-but-disallowed enum value
//!   (e.g. UPDATED_OLD) → "ReturnValues can only be ALL_OLD or NONE"; a
//!   non-enum value (e.g. GARBAGE) → the generic constraint error listing the
//!   full enum set.
//! - Query/Scan `Select` + `ProjectionExpression`: the rejection carries the
//!   "1 validation error detected: " prefix.
//! - Query/Scan `Select=ALL_ATTRIBUTES` on a non-ALL GSI is rejected.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ReturnValue, ScalarAttributeType, Select,
};
use std::collections::HashMap;

#[tokio::test]
async fn put_item_disallowed_return_values_message() {
    let c = client();
    let t = tables().await;
    let mut item: HashMap<String, _> = HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&format!("rv_{}", ts())));
    let err = c
        .put_item()
        .table_name(&t.simple_key_string)
        .set_item(Some(item))
        .return_values(ReturnValue::UpdatedOld) // valid enum, not allowed for Put
        .send()
        .await
        .expect_err("UPDATED_OLD not allowed for PutItem");
    assert_eq!(
        err_code(&err),
        Some("ValidationException"),
        "{}",
        err_msg(&err)
    );
    assert_eq!(err_msg(&err), "ReturnValues can only be ALL_OLD or NONE");
}

#[tokio::test]
async fn delete_item_disallowed_return_values_message() {
    let c = client();
    let t = tables().await;
    let mut key: HashMap<String, _> = HashMap::new();
    key.insert(HASH_KEY_S.into(), s(&format!("rv_{}", ts())));
    let err = c
        .delete_item()
        .table_name(&t.simple_key_string)
        .set_key(Some(key))
        .return_values(ReturnValue::UpdatedOld)
        .send()
        .await
        .expect_err("UPDATED_OLD not allowed for DeleteItem");
    assert_eq!(err_msg(&err), "ReturnValues can only be ALL_OLD or NONE");
}

#[tokio::test]
async fn query_count_with_projection_has_validation_prefix() {
    let c = client();
    let t = tables().await;
    let err = c
        .query()
        .table_name(&t.simple_key_string)
        .key_condition_expression("#h = :p")
        .expression_attribute_names("#h", HASH_KEY_S)
        .expression_attribute_values(":p", s("x"))
        .select(Select::Count)
        .projection_expression("#h")
        .send()
        .await
        .expect_err("COUNT with ProjectionExpression is rejected");
    assert_eq!(
        err_msg(&err),
        "1 validation error detected: Cannot specify the ProjectionExpression \
         when choosing to get only the Count"
    );
}

#[tokio::test]
async fn scan_count_with_projection_no_prefix() {
    // Scan (unlike Query) does NOT carry the "1 validation error detected: "
    // prefix on this rejection — matches real DynamoDB.
    let c = client();
    let t = tables().await;
    let err = c
        .scan()
        .table_name(&t.simple_key_string)
        .select(Select::Count)
        .projection_expression("#h")
        .expression_attribute_names("#h", HASH_KEY_S)
        .send()
        .await
        .expect_err("COUNT with ProjectionExpression is rejected");
    assert_eq!(
        err_msg(&err),
        "Cannot specify the ProjectionExpression when choosing to get only the Count"
    );
}

#[tokio::test]
async fn scan_all_attributes_on_non_all_gsi_rejected() {
    let c = client();
    let name = format!("WordingGsi_{}", ts());
    c.create_table()
        .table_name(&name)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("g")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("g_index")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("g")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::KeysOnly)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create table with KEYS_ONLY gsi");
    wait_for_active(c, &name).await;

    let scan_err = c
        .scan()
        .table_name(&name)
        .index_name("g_index")
        .select(Select::AllAttributes)
        .send()
        .await
        .expect_err("ALL_ATTRIBUTES on a KEYS_ONLY GSI must be rejected");
    assert_eq!(
        err_code(&scan_err),
        Some("ValidationException"),
        "{}",
        err_msg(&scan_err)
    );
    assert!(
        err_msg(&scan_err).contains(
            "Select type ALL_ATTRIBUTES is not supported for global secondary index g_index \
             because its projection type is not ALL"
        ),
        "got: {}",
        err_msg(&scan_err)
    );

    let query_err = c
        .query()
        .table_name(&name)
        .index_name("g_index")
        .key_condition_expression("g = :v")
        .expression_attribute_values(":v", s("x"))
        .select(Select::AllAttributes)
        .send()
        .await
        .expect_err("ALL_ATTRIBUTES on a KEYS_ONLY GSI must be rejected");
    assert!(
        err_msg(&query_err).contains("Select type ALL_ATTRIBUTES is not supported"),
        "got: {}",
        err_msg(&query_err)
    );

    let _ = c.delete_table().table_name(&name).send().await;
}
