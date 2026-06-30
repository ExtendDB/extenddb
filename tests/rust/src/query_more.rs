// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Additional query and scan tests — mirrors remaining Java `QueryScanTests` scenarios.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType, Select,
};

#[tokio::test]
async fn query_with_select_count() {
    let c = client();
    let t = tables().await;
    let prefix = format!("qsc_{}", ts());

    for i in 0..3 {
        c.put_item()
            .table_name(&t.comp_key_string_number)
            .item(HASH_KEY_S, s(&prefix))
            .item(RANGE_KEY_N, n(i))
            .item("data", s(&format!("val_{i}")))
            .send()
            .await
            .unwrap();
    }

    let resp = c
        .query()
        .table_name(&t.comp_key_string_number)
        .key_condition_expression("#pk = :pk")
        .expression_attribute_names("#pk", HASH_KEY_S)
        .expression_attribute_values(":pk", s(&prefix))
        .select(Select::Count)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 3);
    // With SELECT COUNT, items should be empty
    assert!(resp.items().is_empty());
}

#[tokio::test]
async fn query_with_consistent_read() {
    let c = client();
    let t = tables().await;
    let prefix = format!("qcr_{}", ts());

    c.put_item()
        .table_name(&t.comp_key_string_number)
        .item(HASH_KEY_S, s(&prefix))
        .item(RANGE_KEY_N, n(1))
        .item("data", s("consistent"))
        .send()
        .await
        .unwrap();

    let resp = c
        .query()
        .table_name(&t.comp_key_string_number)
        .key_condition_expression("#pk = :pk")
        .expression_attribute_names("#pk", HASH_KEY_S)
        .expression_attribute_values(":pk", s(&prefix))
        .consistent_read(true)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 1);
    assert_eq!(
        resp.items()[0].get("data").unwrap().as_s().unwrap(),
        "consistent"
    );
}

#[tokio::test]
async fn query_on_gsi() {
    let c = client();
    let t = tables().await;
    let gsi_hash = format!("gsi_qog_{}", ts());
    let gsi_range = format!("gsir_qog_{}", ts());

    let mut item = create_item(&t.simple_key_string_gsi);
    item.insert(GSI_HASH_KEY.into(), s(&gsi_hash));
    item.insert(GSI_RANGE_KEY.into(), s(&gsi_range));

    c.put_item()
        .table_name(&t.simple_key_string_gsi)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    // Allow GSI propagation
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let resp = c
        .query()
        .table_name(&t.simple_key_string_gsi)
        .index_name(GSI_NAME)
        .key_condition_expression("#gk = :gk")
        .expression_attribute_names("#gk", GSI_HASH_KEY)
        .expression_attribute_values(":gk", s(&gsi_hash))
        .send()
        .await
        .unwrap();

    assert!(resp.count() >= 1);
}

#[tokio::test]
async fn query_on_non_existent_index() {
    let c = client();
    let t = tables().await;

    let err = c
        .query()
        .table_name(&t.simple_key_string)
        .index_name("non_existent_index")
        .key_condition_expression("#pk = :pk")
        .expression_attribute_names("#pk", HASH_KEY_S)
        .expression_attribute_values(":pk", s("test"))
        .send()
        .await
        .unwrap_err();
    assert!(err_code(&err).is_some());
}

#[tokio::test]
async fn scan_with_select_count() {
    let c = client();
    let table = format!("ScanCount_{}", ts());
    c.create_table()
        .table_name(&table)
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
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
    wait_for_active(c, &table).await;

    for i in 0..4 {
        c.put_item()
            .table_name(&table)
            .item("pk", s(&format!("item_{i}")))
            .send()
            .await
            .unwrap();
    }

    let resp = c
        .scan()
        .table_name(&table)
        .select(Select::Count)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 4);
    assert!(resp.items().is_empty());

}

#[tokio::test]
async fn scan_with_consistent_read() {
    let c = client();
    let t = tables().await;

    let resp = c
        .scan()
        .table_name(&t.simple_key_string)
        .consistent_read(true)
        .send()
        .await
        .unwrap();

    // Just verify it doesn't error — consistent read on scan is valid
    assert!(resp.count() >= 0);
}
