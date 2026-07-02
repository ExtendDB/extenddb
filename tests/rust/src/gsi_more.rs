// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! GSI integration tests — projection types and composite key tables.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType,
};

#[tokio::test]
async fn create_table_with_gsi_keys_only_projection() {
    let c = client();
    let table_name = format!("GsiKeysOnly_{}", ts());

    let gsi = GlobalSecondaryIndex::builder()
        .index_name("keys_only_gsi")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("gsiKey")
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
        .unwrap();

    c.create_table()
        .table_name(&table_name)
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(HASH_KEY_S)
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(HASH_KEY_S)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("gsiKey")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(gsi)
        .send()
        .await
        .unwrap();

    wait_for_active(c, &table_name).await;

    let mut item = std::collections::HashMap::new();
    item.insert(HASH_KEY_S.into(), s("pk1"));
    item.insert("gsiKey".into(), s("gk1"));
    item.insert("extra".into(), s("should_not_appear"));
    c.put_item()
        .table_name(&table_name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let resp = c
        .query()
        .table_name(&table_name)
        .index_name("keys_only_gsi")
        .key_condition_expression("gsiKey = :v")
        .expression_attribute_values(":v", s("gk1"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 1);
    let result = &resp.items()[0];
    assert!(result.contains_key(HASH_KEY_S));
    assert!(result.contains_key("gsiKey"));
    assert!(
        !result.contains_key("extra"),
        "KEYS_ONLY projection should not include non-key attributes"
    );
}

#[tokio::test]
async fn create_table_with_gsi_include_projection() {
    let c = client();
    let table_name = format!("GsiInclude_{}", ts());

    let gsi = GlobalSecondaryIndex::builder()
        .index_name("include_gsi")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("gsiKey")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .projection(
            Projection::builder()
                .projection_type(ProjectionType::Include)
                .non_key_attributes("included_attr")
                .build(),
        )
        .build()
        .unwrap();

    c.create_table()
        .table_name(&table_name)
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(HASH_KEY_S)
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(HASH_KEY_S)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("gsiKey")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(gsi)
        .send()
        .await
        .unwrap();

    wait_for_active(c, &table_name).await;

    let mut item = std::collections::HashMap::new();
    item.insert(HASH_KEY_S.into(), s("pk1"));
    item.insert("gsiKey".into(), s("gk1"));
    item.insert("included_attr".into(), s("visible"));
    item.insert("excluded_attr".into(), s("hidden"));
    c.put_item()
        .table_name(&table_name)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let resp = c
        .query()
        .table_name(&table_name)
        .index_name("include_gsi")
        .key_condition_expression("gsiKey = :v")
        .expression_attribute_values(":v", s("gk1"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 1);
    let result = &resp.items()[0];
    assert!(result.contains_key("included_attr"));
    assert!(
        !result.contains_key("excluded_attr"),
        "INCLUDE projection should not include non-projected attributes"
    );
}

#[tokio::test]
async fn query_gsi_composite_key_table() {
    let c = client();
    let t = tables().await;
    let table = &t.comp_key_string_string_gsi;
    let gsi_hash = format!("gsi_comp_{}", ts());

    for i in 0..3 {
        let mut item = create_item(table);
        item.insert(GSI_HASH_KEY.into(), s(&gsi_hash));
        item.insert(GSI_RANGE_KEY.into(), s(&format!("r_{i}")));
        c.put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let resp = c
        .query()
        .table_name(table)
        .index_name(GSI_NAME)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", GSI_HASH_KEY)
        .expression_attribute_values(":hv", s(&gsi_hash))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 3);
}
