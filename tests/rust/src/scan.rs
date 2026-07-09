// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Scan integration tests.

use crate::test_base::*;

/// Create a dedicated table and seed it with items. Returns (table_name, item_count).
async fn create_and_seed(prefix: &str, count: usize) -> (String, usize) {
    let c = client();
    let table = format!("{prefix}_{}", ts());
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
    };
    c.create_table()
        .table_name(&table)
        .billing_mode(BillingMode::PayPerRequest)
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
        .send()
        .await
        .unwrap();
    wait_for_active(c, &table).await;

    for i in 0..count {
        let mut item = std::collections::HashMap::new();
        item.insert(HASH_KEY_S.into(), s(&format!("key_{i}")));
        item.insert("idx".into(), n(i as i64));
        item.insert("parity".into(), s(if i % 2 == 0 { "even" } else { "odd" }));
        c.put_item()
            .table_name(&table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }
    (table, count)
}

#[tokio::test]
async fn scan_all_items() {
    let (table, count) = create_and_seed("ScanAll", 10).await;
    let c = client();

    let resp = c.scan().table_name(&table).send().await.unwrap();
    assert_eq!(resp.count() as usize, count);
}

#[tokio::test]
async fn scan_with_limit() {
    let (table, _) = create_and_seed("ScanLim", 10).await;
    let c = client();

    let resp = c.scan().table_name(&table).limit(3).send().await.unwrap();
    assert_eq!(resp.items().len(), 3);
    assert!(
        resp.last_evaluated_key().is_some(),
        "Should have pagination token"
    );
}

#[tokio::test]
async fn scan_pagination() {
    let (table, count) = create_and_seed("ScanPag", 7).await;
    let c = client();

    let mut total = 0;
    let mut lek = None;
    loop {
        let mut req = c.scan().table_name(&table).limit(3);
        if let Some(k) = lek {
            req = req.set_exclusive_start_key(Some(k));
        }
        let resp = req.send().await.unwrap();
        total += resp.items().len();
        lek = resp.last_evaluated_key().map(|m| m.to_owned());
        if lek.is_none() {
            break;
        }
    }
    assert_eq!(total, count);
}

#[tokio::test]
async fn scan_with_filter_expression() {
    let (table, _) = create_and_seed("ScanFilt", 10).await;
    let c = client();

    let resp = c
        .scan()
        .table_name(&table)
        .filter_expression("parity = :p")
        .expression_attribute_values(":p", s("even"))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 5);
}

#[tokio::test]
async fn scan_with_projection_expression() {
    let (table, _) = create_and_seed("ScanProj", 5).await;
    let c = client();

    let resp = c
        .scan()
        .table_name(&table)
        .projection_expression("#h, idx")
        .expression_attribute_names("#h", HASH_KEY_S)
        .send()
        .await
        .unwrap();

    for item in resp.items() {
        assert!(item.get(HASH_KEY_S).is_some());
        assert!(item.get("idx").is_some());
        assert!(
            item.get("parity").is_none(),
            "Projection should exclude 'parity'"
        );
    }
}

#[tokio::test]
async fn scan_empty_table() {
    let c = client();
    let table = format!("ScanEmpty_{}", ts());
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
    };
    c.create_table()
        .table_name(&table)
        .billing_mode(BillingMode::PayPerRequest)
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
        .send()
        .await
        .unwrap();
    wait_for_active(c, &table).await;

    let resp = c.scan().table_name(&table).send().await.unwrap();
    assert_eq!(resp.count(), 0);
    assert!(resp.items().is_empty());
}

#[tokio::test]
async fn scan_non_existent_table() {
    let c = client();
    let err = c
        .scan()
        .table_name("NonExistentTable_scan")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err_code(&err), Some("ResourceNotFoundException"));
}

#[tokio::test]
async fn scan_consistent_read() {
    let (table, count) = create_and_seed("ScanCR", 5).await;
    let c = client();

    let resp = c
        .scan()
        .table_name(&table)
        .consistent_read(true)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count() as usize, count);
}

#[tokio::test]
async fn scan_with_numeric_filter() {
    let (table, _) = create_and_seed("ScanNum", 20).await;
    let c = client();
    let resp = c
        .scan()
        .table_name(&table)
        .filter_expression("idx >= :min")
        .expression_attribute_values(":min", n(15))
        .send()
        .await
        .unwrap();
    // Items with idx >= 15: items 15-19
    assert_eq!(resp.count(), 5);
}

#[tokio::test]
async fn scan_with_expression_attribute_names() {
    let (table, _) = create_and_seed("ScanEAN", 10).await;
    let c = client();
    let resp = c
        .scan()
        .table_name(&table)
        .filter_expression("#p = :val")
        .expression_attribute_names("#p", "parity")
        .expression_attribute_values(":val", s("even"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.count(), 5);
}

#[tokio::test]
async fn scan_with_multiple_filters() {
    let (table, _) = create_and_seed("ScanMulti", 20).await;
    let c = client();
    let resp = c
        .scan()
        .table_name(&table)
        .filter_expression("parity = :p AND idx > :min")
        .expression_attribute_values(":p", s("even"))
        .expression_attribute_values(":min", n(10))
        .send()
        .await
        .unwrap();
    // Even items with idx > 10: items 12, 14, 16, 18
    assert_eq!(resp.count(), 4);
}

/// Scan filter `contains` over a Binary (B) attribute performs a contiguous
/// byte-substring match, matching DynamoDB. Regression for the previously
/// missing (B, B) arm which returned no matches for any binary operand.
#[tokio::test]
async fn scan_filter_contains_binary() {
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
    };
    let c = client();
    let table = format!("ScanBinContains_{}", ts());
    c.create_table()
        .table_name(&table)
        .billing_mode(BillingMode::PayPerRequest)
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
        .send()
        .await
        .unwrap();
    wait_for_active(c, &table).await;

    // i1 = b"hello", i2 = b"world"
    for (k, v) in [("i1", "hello"), ("i2", "world")] {
        let mut item = std::collections::HashMap::new();
        item.insert(HASH_KEY_S.into(), s(k));
        item.insert("blob".into(), b(v));
        c.put_item()
            .table_name(&table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    // contains(blob, b"ell") -> only i1 ("hello" contains "ell")
    let resp = c
        .scan()
        .table_name(&table)
        .filter_expression("contains(#b, :sub)")
        .expression_attribute_names("#b", "blob")
        .expression_attribute_values(":sub", b("ell"))
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = resp
        .items()
        .iter()
        .map(|i| i.get(HASH_KEY_S).unwrap().as_s().unwrap().as_str())
        .collect();
    assert_eq!(keys, vec!["i1"], "CONTAINS b\"ell\" should match only i1");

    // NOT_CONTAINS(blob, b"ell") -> only i2
    let resp = c
        .scan()
        .table_name(&table)
        .filter_expression("NOT contains(#b, :sub)")
        .expression_attribute_names("#b", "blob")
        .expression_attribute_values(":sub", b("ell"))
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = resp
        .items()
        .iter()
        .map(|i| i.get(HASH_KEY_S).unwrap().as_s().unwrap().as_str())
        .collect();
    assert_eq!(keys, vec!["i2"], "NOT contains b\"ell\" should match only i2");
}
