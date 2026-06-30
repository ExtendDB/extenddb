// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! LSI (Local Secondary Index) pagination tests — regression for issue #145.
//!
//! Verifies that ExclusiveStartKey pagination works correctly when multiple
//! items share the same index sort key value.
//!
//! Tables are shared at module level to avoid redundant CreateTable calls.
//! Each test uses a unique partition key to isolate its data.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    LocalSecondaryIndex, Projection, ProjectionType, ScalarAttributeType,
};
use std::collections::HashMap;
use tokio::sync::OnceCell as AsyncOnceCell;

const LSI_NAME: &str = "TaskTypeLSI";
const LSI_SK_ATTR: &str = "taskType";

// ---- Module-level shared tables (created once, reused by all tests) ----

static LSI_TABLE: AsyncOnceCell<String> = AsyncOnceCell::const_new();
static GSI_HASH_ONLY_TABLE: AsyncOnceCell<String> = AsyncOnceCell::const_new();

/// Get or create the shared LSI table.
async fn lsi_table() -> &'static str {
    LSI_TABLE
        .get_or_init(|| async {
            let name = format!("LSI_{}", ts());
            create_lsi_table_impl(&name).await;
            name
        })
        .await
}

/// Get or create the shared hash-only GSI table.
async fn gsi_hash_only_table() -> &'static str {
    GSI_HASH_ONLY_TABLE
        .get_or_init(|| async {
            let name = format!("GSIHashOnly_{}", ts());
            create_hash_only_gsi_table_impl(&name).await;
            name
        })
        .await
}

/// Create a table with an LSI for pagination tests.
async fn create_lsi_table_impl(name: &str) {
    let c = client();
    let attr_defs = vec![
        AttributeDefinition::builder()
            .attribute_name(HASH_KEY_S)
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap(),
        AttributeDefinition::builder()
            .attribute_name(RANGE_KEY_S)
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap(),
        AttributeDefinition::builder()
            .attribute_name(LSI_SK_ATTR)
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap(),
    ];

    let key_schema = vec![
        KeySchemaElement::builder()
            .attribute_name(HASH_KEY_S)
            .key_type(KeyType::Hash)
            .build()
            .unwrap(),
        KeySchemaElement::builder()
            .attribute_name(RANGE_KEY_S)
            .key_type(KeyType::Range)
            .build()
            .unwrap(),
    ];

    let lsi = LocalSecondaryIndex::builder()
        .index_name(LSI_NAME)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(HASH_KEY_S)
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name(LSI_SK_ATTR)
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        )
        .projection(
            Projection::builder()
                .projection_type(ProjectionType::All)
                .build(),
        )
        .build()
        .unwrap();

    match c
        .create_table()
        .table_name(name)
        .billing_mode(BillingMode::PayPerRequest)
        .set_key_schema(Some(key_schema))
        .set_attribute_definitions(Some(attr_defs))
        .local_secondary_indexes(lsi)
        .send()
        .await
    {
        Ok(_) => {}
        Err(e) => {
            if err_code(&e) != Some("ResourceInUseException") {
                panic!("Failed to create LSI table {name}: {e:?}");
            }
        }
    }
    wait_for_active(&c, name).await;
}

/// Create a table with a hash-only GSI for pagination tests.
async fn create_hash_only_gsi_table_impl(name: &str) {
    let c = client();
    let attr_defs = vec![
        AttributeDefinition::builder()
            .attribute_name("instanceId")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap(),
        AttributeDefinition::builder()
            .attribute_name("nodeStatus")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap(),
    ];

    let key_schema = vec![KeySchemaElement::builder()
        .attribute_name("instanceId")
        .key_type(KeyType::Hash)
        .build()
        .unwrap()];

    let gsi = aws_sdk_dynamodb::types::GlobalSecondaryIndex::builder()
        .index_name("StatusGSI")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("nodeStatus")
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
        .unwrap();

    match c
        .create_table()
        .table_name(name)
        .billing_mode(BillingMode::PayPerRequest)
        .set_key_schema(Some(key_schema))
        .set_attribute_definitions(Some(attr_defs))
        .global_secondary_indexes(gsi)
        .send()
        .await
    {
        Ok(_) => {}
        Err(e) => {
            if err_code(&e) != Some("ResourceInUseException") {
                panic!("Failed to create GSI table {name}: {e:?}");
            }
        }
    }
    wait_for_active(&c, name).await;
}

// ---- Helpers ----

/// Seed items that share the same LSI sort key value, using a unique PK.
async fn seed_lsi_items(table: &str, pk: &str, count: usize) {
    let c = client();
    for i in 1..=count {
        let mut item = HashMap::new();
        item.insert(HASH_KEY_S.into(), s(pk));
        item.insert(RANGE_KEY_S.into(), s(&format!("task{i}")));
        item.insert(LSI_SK_ATTR.into(), s("EXPORT"));
        item.insert("data".into(), s(&format!("payload-{i}")));
        c.put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }
}

/// Seed items for hash-only GSI tests, using a unique prefix for isolation.
async fn seed_gsi_items(table: &str, prefix: &str, count: usize) {
    let c = client();
    for i in 1..=count {
        let mut item = HashMap::new();
        item.insert("instanceId".into(), s(&format!("{prefix}-{i}")));
        item.insert("nodeStatus".into(), s(prefix));
        item.insert("data".into(), s(&format!("payload-{i}")));
        c.put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }
}

// ---- LSI tests (share LSI_TABLE) ----

#[tokio::test]
async fn lsi_pagination_duplicate_sort_keys() {
    let table = lsi_table().await;
    let pk = format!("lsi_pag_{}", ts());
    seed_lsi_items(table, &pk, 5).await;

    let c = client();
    let mut all_items: Vec<HashMap<String, AttributeValue>> = Vec::new();
    let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

    loop {
        let mut req = c
            .query()
            .table_name(table)
            .index_name(LSI_NAME)
            .key_condition_expression("#pk = :pk")
            .expression_attribute_names("#pk", HASH_KEY_S)
            .expression_attribute_values(":pk", s(&pk))
            .limit(2);

        if let Some(ref lek) = exclusive_start_key {
            req = req.set_exclusive_start_key(Some(lek.clone()));
        }

        let resp = req.send().await.unwrap();
        all_items.extend(resp.items().to_vec());

        match resp.last_evaluated_key() {
            Some(lek) => exclusive_start_key = Some(lek.to_owned()),
            None => break,
        }
    }

    assert_eq!(
        all_items.len(),
        5,
        "All 5 items must be returned through pagination with duplicate LSI sort keys"
    );

    let mut sks: Vec<String> = all_items
        .iter()
        .map(|item| match item.get(RANGE_KEY_S).unwrap() {
            AttributeValue::S(v) => v.clone(),
            _ => panic!("unexpected type"),
        })
        .collect();
    sks.sort();
    assert_eq!(sks, vec!["task1", "task2", "task3", "task4", "task5"]);
}

#[tokio::test]
async fn lsi_pagination_reverse_order() {
    let table = lsi_table().await;
    let pk = format!("lsi_rev_{}", ts());
    seed_lsi_items(table, &pk, 5).await;

    let c = client();
    let mut all_items: Vec<HashMap<String, AttributeValue>> = Vec::new();
    let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

    loop {
        let mut req = c
            .query()
            .table_name(table)
            .index_name(LSI_NAME)
            .key_condition_expression("#pk = :pk")
            .expression_attribute_names("#pk", HASH_KEY_S)
            .expression_attribute_values(":pk", s(&pk))
            .scan_index_forward(false)
            .limit(2);

        if let Some(ref lek) = exclusive_start_key {
            req = req.set_exclusive_start_key(Some(lek.clone()));
        }

        let resp = req.send().await.unwrap();
        all_items.extend(resp.items().to_vec());

        match resp.last_evaluated_key() {
            Some(lek) => exclusive_start_key = Some(lek.to_owned()),
            None => break,
        }
    }

    assert_eq!(all_items.len(), 5);

    // With ScanIndexForward=false and equal index sort keys, items should be
    // sub-sorted by base table sort key in descending order.
    let sks: Vec<String> = all_items
        .iter()
        .map(|item| match item.get(RANGE_KEY_S).unwrap() {
            AttributeValue::S(v) => v.clone(),
            _ => panic!("unexpected type"),
        })
        .collect();
    assert_eq!(sks, vec!["task5", "task4", "task3", "task2", "task1"]);
}

#[tokio::test]
async fn lsi_page_two_returns_items() {
    let table = lsi_table().await;
    let pk = format!("lsi_p2_{}", ts());
    seed_lsi_items(table, &pk, 5).await;

    let c = client();

    // Page 1
    let resp1 = c
        .query()
        .table_name(table)
        .index_name(LSI_NAME)
        .key_condition_expression("#pk = :pk")
        .expression_attribute_names("#pk", HASH_KEY_S)
        .expression_attribute_values(":pk", s(&pk))
        .limit(2)
        .send()
        .await
        .unwrap();

    assert_eq!(resp1.items().len(), 2);
    let lek = resp1
        .last_evaluated_key()
        .expect("Page 1 must have LastEvaluatedKey")
        .to_owned();

    // Page 2 — core regression test for issue #145
    let resp2 = c
        .query()
        .table_name(table)
        .index_name(LSI_NAME)
        .key_condition_expression("#pk = :pk")
        .expression_attribute_names("#pk", HASH_KEY_S)
        .expression_attribute_values(":pk", s(&pk))
        .set_exclusive_start_key(Some(lek))
        .limit(2)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp2.items().len(),
        2,
        "Page 2 must return items when index sort keys are duplicated"
    );
}

#[tokio::test]
async fn lsi_last_evaluated_key_contains_all_keys() {
    let table = lsi_table().await;
    let pk = format!("lsi_lek_{}", ts());
    seed_lsi_items(table, &pk, 5).await;

    let c = client();

    let resp = c
        .query()
        .table_name(table)
        .index_name(LSI_NAME)
        .key_condition_expression("#pk = :pk")
        .expression_attribute_names("#pk", HASH_KEY_S)
        .expression_attribute_values(":pk", s(&pk))
        .limit(2)
        .send()
        .await
        .unwrap();

    let lek = resp
        .last_evaluated_key()
        .expect("Must have LastEvaluatedKey");

    assert!(
        lek.contains_key(HASH_KEY_S),
        "LEK must contain partition key"
    );
    assert!(
        lek.contains_key(RANGE_KEY_S),
        "LEK must contain base table sort key"
    );
    assert!(
        lek.contains_key(LSI_SK_ATTR),
        "LEK must contain index sort key"
    );
}

// ---- Hash-only GSI pagination tests (share GSI_HASH_ONLY_TABLE) ----

#[tokio::test]
async fn gsi_hash_only_pagination() {
    let table = gsi_hash_only_table().await;
    let prefix = format!("pag_{}", ts());
    seed_gsi_items(table, &prefix, 10).await;

    // Allow GSI propagation
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let c = client();
    let mut all_items: Vec<HashMap<String, AttributeValue>> = Vec::new();
    let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

    loop {
        let mut req = c
            .query()
            .table_name(table)
            .index_name("StatusGSI")
            .key_condition_expression("nodeStatus = :s")
            .expression_attribute_values(":s", s(&prefix))
            .limit(3);

        if let Some(ref lek) = exclusive_start_key {
            req = req.set_exclusive_start_key(Some(lek.clone()));
        }

        let resp = req.send().await.unwrap();
        all_items.extend(resp.items().to_vec());

        match resp.last_evaluated_key() {
            Some(lek) => exclusive_start_key = Some(lek.to_owned()),
            None => break,
        }
    }

    assert_eq!(
        all_items.len(),
        10,
        "All 10 items must be returned through hash-only GSI pagination"
    );

    let mut ids: Vec<String> = all_items
        .iter()
        .map(|item| match item.get("instanceId").unwrap() {
            AttributeValue::S(v) => v.clone(),
            _ => panic!("unexpected type"),
        })
        .collect();
    ids.sort();
    let mut expected: Vec<String> = (1..=10).map(|i| format!("{prefix}-{i}")).collect();
    expected.sort();
    assert_eq!(ids, expected);
}

#[tokio::test]
async fn gsi_hash_only_page_two_returns_items() {
    let table = gsi_hash_only_table().await;
    let prefix = format!("p2_{}", ts());
    seed_gsi_items(table, &prefix, 10).await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let c = client();

    let resp1 = c
        .query()
        .table_name(table)
        .index_name("StatusGSI")
        .key_condition_expression("nodeStatus = :s")
        .expression_attribute_values(":s", s(&prefix))
        .limit(3)
        .send()
        .await
        .unwrap();

    assert_eq!(resp1.items().len(), 3);
    let lek = resp1
        .last_evaluated_key()
        .expect("Page 1 must have LastEvaluatedKey")
        .to_owned();

    let resp2 = c
        .query()
        .table_name(table)
        .index_name("StatusGSI")
        .key_condition_expression("nodeStatus = :s")
        .expression_attribute_values(":s", s(&prefix))
        .set_exclusive_start_key(Some(lek))
        .limit(3)
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp2.items().len(),
        3,
        "Page 2 must return items for hash-only GSI pagination"
    );
}

#[tokio::test]
async fn gsi_hash_only_no_duplicates() {
    let table = gsi_hash_only_table().await;
    let prefix = format!("nodup_{}", ts());
    seed_gsi_items(table, &prefix, 10).await;

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let c = client();
    let mut all_ids: Vec<String> = Vec::new();
    let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> = None;

    loop {
        let mut req = c
            .query()
            .table_name(table)
            .index_name("StatusGSI")
            .key_condition_expression("nodeStatus = :s")
            .expression_attribute_values(":s", s(&prefix))
            .limit(2);

        if let Some(ref lek) = exclusive_start_key {
            req = req.set_exclusive_start_key(Some(lek.clone()));
        }

        let resp = req.send().await.unwrap();
        for item in resp.items() {
            if let Some(AttributeValue::S(id)) = item.get("instanceId") {
                all_ids.push(id.clone());
            }
        }

        match resp.last_evaluated_key() {
            Some(lek) => exclusive_start_key = Some(lek.to_owned()),
            None => break,
        }
    }

    let unique_count = all_ids
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len();
    assert_eq!(
        all_ids.len(),
        unique_count,
        "No duplicate items across pages"
    );
    assert_eq!(all_ids.len(), 10);
}
