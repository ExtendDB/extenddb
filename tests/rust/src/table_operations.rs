// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Table operations tests — mirrors `TableOperationsTests.java` (21 tests).

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, GlobalSecondaryIndex, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType,
};

// ========== CREATE TABLE TESTS ==========

#[tokio::test]
async fn create_simple_hash_key_table() {
    let c = client();
    let name = format!("test_create_hash_{}", ts());
    let result = c
        .create_table()
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
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let desc = result.table_description().unwrap();
    assert_eq!(desc.table_name().unwrap(), name);

    wait_for_active(c, &name).await;
    let _ = c.delete_table().table_name(&name).send().await;
}

#[tokio::test]
async fn create_hash_range_table() {
    let c = client();
    let name = format!("test_create_hr_{}", ts());
    let result = c
        .create_table()
        .table_name(&name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("sk")
                .key_type(KeyType::Range)
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
                .attribute_name("sk")
                .attribute_type(ScalarAttributeType::N)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let desc = result.table_description().unwrap();
    assert_eq!(desc.key_schema().len(), 2);

    wait_for_active(c, &name).await;
    let _ = c.delete_table().table_name(&name).send().await;
}

#[tokio::test]
async fn create_table_with_gsi() {
    let c = client();
    let name = format!("test_create_gsi_{}", ts());
    let result = c
        .create_table()
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
                .attribute_name("gsi_pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("test_gsi")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("gsi_pk")
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

    let desc = result.table_description().unwrap();
    let gsis = desc.global_secondary_indexes();
    assert_eq!(gsis.len(), 1);
    assert_eq!(gsis[0].index_name().unwrap(), "test_gsi");

    wait_for_active(c, &name).await;
    let _ = c.delete_table().table_name(&name).send().await;
}

#[tokio::test]
async fn create_duplicate_table() {
    let c = client();
    let t = tables().await;
    let err = c
        .create_table()
        .table_name(&t.simple_key_string)
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
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await;

    assert!(err.is_err());
    let e = err.unwrap_err();
    assert_eq!(
        err_code(&e),
        Some("ResourceInUseException"),
        "Expected ResourceInUseException, got: {e:?}"
    );
}

#[tokio::test]
async fn create_table_with_invalid_name() {
    let c = client();
    let err = c
        .create_table()
        .table_name("")
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
        .await;

    assert!(err.is_err());
}

#[tokio::test]
async fn create_table_with_all_key_types() {
    let c = client();
    let suffixes = ["_s_", "_n_", "_b_"];
    let types = [
        ScalarAttributeType::S,
        ScalarAttributeType::N,
        ScalarAttributeType::B,
    ];
    let mut created = Vec::new();

    for (i, attr_type) in types.iter().enumerate() {
        let name = format!("test_keytype{}{}", suffixes[i], ts());
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
                    .attribute_type(attr_type.clone())
                    .build()
                    .unwrap(),
            )
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await
            .unwrap();
        created.push(name);
    }

    assert_eq!(created.len(), 3);

    for name in &created {
        wait_for_active(c, name).await;
        let _ = c.delete_table().table_name(name).send().await;
    }
}

#[tokio::test]
async fn create_table_with_max_length_name() {
    let c = client();
    let name = "a".repeat(255);

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
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    wait_for_active(c, &name).await;
    let desc = c.describe_table().table_name(&name).send().await.unwrap();
    assert_eq!(desc.table().unwrap().table_name().unwrap(), name);

    let _ = c.delete_table().table_name(&name).send().await;
}

// ========== DESCRIBE TABLE TESTS ==========

#[tokio::test]
async fn describe_existing_table() {
    let c = client();
    let t = tables().await;
    let result = c
        .describe_table()
        .table_name(&t.simple_key_string)
        .send()
        .await
        .unwrap();

    let table = result.table().unwrap();
    assert_eq!(table.table_name().unwrap(), t.simple_key_string);
    assert!(!table.key_schema().is_empty());
}

#[tokio::test]
async fn describe_table_with_gsi() {
    let c = client();
    let t = tables().await;
    let result = c
        .describe_table()
        .table_name(&t.simple_key_string_gsi)
        .send()
        .await
        .unwrap();

    let table = result.table().unwrap();
    let gsis = table.global_secondary_indexes();
    assert_eq!(gsis.len(), 1);
    assert_eq!(gsis[0].index_name().unwrap(), GSI_NAME);
}

#[tokio::test]
async fn describe_non_existent_table() {
    let c = client();
    let err = c
        .describe_table()
        .table_name(format!("nonExistentTable_{}", ts()))
        .send()
        .await;

    assert!(err.is_err());
    let e = err.unwrap_err();
    assert_eq!(
        err_code(&e),
        Some("ResourceNotFoundException"),
        "Expected ResourceNotFoundException, got: {e:?}"
    );
}

#[tokio::test]
async fn describe_table_key_schema() {
    let c = client();
    let t = tables().await;
    let result = c
        .describe_table()
        .table_name(&t.comp_key_string_number)
        .send()
        .await
        .unwrap();

    let key_schema = result.table().unwrap().key_schema();
    assert_eq!(key_schema.len(), 2);

    let has_hash = key_schema.iter().any(|k| k.key_type() == &KeyType::Hash);
    let has_range = key_schema.iter().any(|k| k.key_type() == &KeyType::Range);
    assert!(has_hash, "Should have HASH key");
    assert!(has_range, "Should have RANGE key");
}

// ========== LIST TABLES TESTS ==========

#[tokio::test]
async fn list_tables_basic() {
    let c = client();
    let t = tables().await;

    // ListTables returns at most 100 names per page; the suite can create more
    // than that, so paginate until we find the shared table or exhaust pages.
    let mut found = false;
    let mut start: Option<String> = None;
    loop {
        let mut req = c.list_tables();
        if let Some(s) = &start {
            req = req.exclusive_start_table_name(s);
        }
        let resp = req.send().await.unwrap();
        if resp.table_names().contains(&t.simple_key_string) {
            found = true;
            break;
        }
        match resp.last_evaluated_table_name() {
            Some(n) => start = Some(n.to_string()),
            None => break,
        }
    }
    assert!(found, "Should contain {}", t.simple_key_string);
}

#[tokio::test]
async fn list_tables_with_limit() {
    let c = client();
    let _ = tables().await;
    let result = c.list_tables().limit(2).send().await.unwrap();

    assert!(result.table_names().len() <= 2);
}

#[tokio::test]
async fn list_tables_pagination() {
    let c = client();
    let _ = tables().await;
    let result = c.list_tables().limit(1).send().await.unwrap();

    assert_eq!(result.table_names().len(), 1);

    if let Some(last) = result.last_evaluated_table_name() {
        let result2 = c
            .list_tables()
            .limit(1)
            .exclusive_start_table_name(last)
            .send()
            .await
            .unwrap();
        assert!(!result2.table_names().is_empty());
        assert_ne!(result.table_names()[0], result2.table_names()[0]);
    }
}
