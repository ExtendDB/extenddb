// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Secondary-index key attribute validation on writes. A write whose GSI key
//! attribute has the wrong type, or is an empty string, must be rejected with a
//! ValidationException, matching real DynamoDB.

use crate::test_base::*;
use std::collections::HashMap;

#[tokio::test]
async fn put_item_rejects_wrong_typed_index_key() {
    let c = client();
    let t = tables().await;
    // simple_key_string_gsi has a GSI on gsiHashKey (type S). Supplying a
    // numeric value for it is a type mismatch on the index key.
    let mut item: HashMap<String, _> = HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&format!("idxk_{}", ts())));
    item.insert(GSI_HASH_KEY.into(), n(123));
    item.insert(GSI_RANGE_KEY.into(), s("ok"));

    let err = c
        .put_item()
        .table_name(&t.simple_key_string_gsi)
        .set_item(Some(item))
        .send()
        .await
        .expect_err("wrong-typed index key must be rejected");
    assert_eq!(
        err_code(&err),
        Some("ValidationException"),
        "{}",
        err_msg(&err)
    );
    let m = err_msg(&err);
    assert!(
        m.contains("Index Key") && m.contains("IndexName"),
        "expected index-key type-mismatch message, got: {m}"
    );
}

#[tokio::test]
async fn put_item_rejects_empty_index_key() {
    let c = client();
    let t = tables().await;
    let mut item: HashMap<String, _> = HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&format!("idxe_{}", ts())));
    item.insert(GSI_HASH_KEY.into(), s("")); // empty string index key
    item.insert(GSI_RANGE_KEY.into(), s("ok"));

    let err = c
        .put_item()
        .table_name(&t.simple_key_string_gsi)
        .set_item(Some(item))
        .send()
        .await
        .expect_err("empty index key must be rejected");
    assert_eq!(
        err_code(&err),
        Some("ValidationException"),
        "{}",
        err_msg(&err)
    );
}

#[tokio::test]
async fn put_item_accepts_valid_index_key() {
    let c = client();
    let t = tables().await;
    let mut item: HashMap<String, _> = HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&format!("idxok_{}", ts())));
    item.insert(GSI_HASH_KEY.into(), s("h"));
    item.insert(GSI_RANGE_KEY.into(), s("r"));
    c.put_item()
        .table_name(&t.simple_key_string_gsi)
        .set_item(Some(item))
        .send()
        .await
        .expect("valid index key must be accepted");
}

#[tokio::test]
async fn put_item_rejects_empty_binary_index_key_reports_binary() {
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement,
        KeyType, Projection, ProjectionType, ScalarAttributeType,
    };
    let c = client();
    let name = format!("BinGsiEmpty{}", ts());
    c.create_table()
        .table_name(&name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("gb")
                .attribute_type(ScalarAttributeType::B)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("gsib")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("gb")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
    wait_for_active(c, &name).await;

    // Empty BINARY value on the GSI key must be reported as an "empty binary
    // value" (matching real DynamoDB), not "empty string value".
    let mut item: HashMap<String, AttributeValue> = HashMap::new();
    item.insert("pk".into(), s("a"));
    item.insert(
        "gb".into(),
        AttributeValue::B(aws_smithy_types::Blob::new(Vec::<u8>::new())),
    );
    let err = c
        .put_item()
        .table_name(&name)
        .set_item(Some(item))
        .send()
        .await
        .expect_err("empty binary index key must be rejected");
    assert_eq!(
        err_code(&err),
        Some("ValidationException"),
        "{}",
        err_msg(&err)
    );
    let m = err_msg(&err);
    assert!(
        m.contains("empty binary value"),
        "expected type-correct 'empty binary value' message, got: {m}"
    );
}

/// Control for the rejection above: a normal update that leaves the index key
/// valid must still succeed.
#[tokio::test]
async fn update_item_accepts_setting_an_index_key_to_a_valid_value() {
    let c = client();
    let t = tables().await;
    let key_value = format!("idxupdok_{}", ts());

    let mut item: HashMap<String, _> = HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&key_value));
    item.insert(GSI_HASH_KEY.into(), s("h"));
    item.insert(GSI_RANGE_KEY.into(), s("r"));
    c.put_item()
        .table_name(&t.simple_key_string_gsi)
        .set_item(Some(item))
        .send()
        .await
        .expect("seed item must be accepted");

    c.update_item()
        .table_name(&t.simple_key_string_gsi)
        .key(HASH_KEY_S, s(&key_value))
        .update_expression("SET #g = :v")
        .expression_attribute_names("#g", GSI_HASH_KEY)
        .expression_attribute_values(":v", s("h2"))
        .send()
        .await
        .expect("a valid index-key update must be accepted");
}

/// An update expression that sets a secondary-index key to an empty value must
/// be rejected, the same as writing that value with PutItem. Before this was
/// enforced, UpdateItem accepted the write and stored an unindexable key.
#[tokio::test]
async fn update_item_rejects_setting_an_index_key_to_an_empty_value() {
    let c = client();
    let t = tables().await;
    let key_value = format!("idxupde_{}", ts());

    // Seed a row with a valid index key so the update targets an existing item.
    let mut item: HashMap<String, _> = HashMap::new();
    item.insert(HASH_KEY_S.into(), s(&key_value));
    item.insert(GSI_HASH_KEY.into(), s("h"));
    item.insert(GSI_RANGE_KEY.into(), s("r"));
    c.put_item()
        .table_name(&t.simple_key_string_gsi)
        .set_item(Some(item))
        .send()
        .await
        .expect("seed item must be accepted");

    let err = c
        .update_item()
        .table_name(&t.simple_key_string_gsi)
        .key(HASH_KEY_S, s(&key_value))
        .update_expression("SET #g = :empty")
        .expression_attribute_names("#g", GSI_HASH_KEY)
        .expression_attribute_values(":empty", s(""))
        .send()
        .await
        .expect_err("setting an index key to an empty value must be rejected");
    assert_eq!(
        err_code(&err),
        Some("ValidationException"),
        "{}",
        err_msg(&err)
    );
    let m = err_msg(&err);
    assert!(
        m.contains("update expression attempted to update a secondary index key"),
        "expected the update-expression index-key message, got: {m}"
    );
}
