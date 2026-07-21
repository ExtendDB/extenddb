// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Binary (B) sort-key ordering and range-condition tests.
//!
//! DynamoDB orders Binary values by **unsigned lexicographic byte order**
//! (each byte treated as an unsigned value, compared left to right; a shorter
//! run of equal leading bytes sorts first). This is distinct from orderings
//! that compare length first. These tests pin the byte-order contract for
//! Query result order and for `BETWEEN` range conditions on a binary sort key,
//! a path not otherwise covered by the suite.

use crate::test_base::*;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use aws_smithy_types::Blob;

/// Build a Binary `AttributeValue` from raw bytes.
fn bb(bytes: &[u8]) -> AttributeValue {
    AttributeValue::B(Blob::new(bytes.to_vec()))
}

/// Create a `pk` (S, HASH) + `sk` (B, RANGE) table for binary sort-key tests.
async fn create_binary_sk_table(c: &aws_sdk_dynamodb::Client, table: &str) {
    c.create_table()
        .table_name(table)
        .billing_mode(BillingMode::PayPerRequest)
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
                .attribute_type(ScalarAttributeType::B)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    wait_for_active(c, table).await;
}

async fn seed(c: &aws_sdk_dynamodb::Client, table: &str, pk: &str, sks: &[&[u8]]) {
    for sk in sks {
        let mut item = std::collections::HashMap::new();
        item.insert("pk".to_string(), s(pk));
        item.insert("sk".to_string(), bb(sk));
        c.put_item()
            .table_name(table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }
}

/// Extract the `sk` binary values from a Query response, in returned order.
fn sk_bytes(items: &[std::collections::HashMap<String, AttributeValue>]) -> Vec<Vec<u8>> {
    items
        .iter()
        .map(|i| i.get("sk").unwrap().as_b().unwrap().as_ref().to_vec())
        .collect()
}

#[tokio::test]
async fn query_binary_sort_key_orders_by_unsigned_bytes() {
    let c = client();
    let table = format!("BinSkOrder_{}", ts());
    create_binary_sk_table(c, &table).await;

    let pk = "p";
    // Insert scrambled. Unsigned byte order is:
    //   [0x01,0x00] < [0x01,0xFF] < [0x02]
    // A length-first ordering (e.g. BSON BinData) would instead yield
    //   [0x02] < [0x01,0x00] < [0x01,0xFF].
    seed(c, &table, pk, &[&[0x02], &[0x01, 0xFF], &[0x01, 0x00]]).await;

    // Ascending (ScanIndexForward = true, the default).
    let resp = c
        .query()
        .table_name(&table)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", "pk")
        .expression_attribute_values(":hv", s(pk))
        .send()
        .await
        .unwrap();
    assert_eq!(
        sk_bytes(resp.items()),
        vec![vec![0x01, 0x00], vec![0x01, 0xFF], vec![0x02]],
        "binary sort keys must be returned in unsigned lexicographic byte order"
    );

    // Descending must be the exact reverse.
    let resp_desc = c
        .query()
        .table_name(&table)
        .key_condition_expression("#h = :hv")
        .expression_attribute_names("#h", "pk")
        .expression_attribute_values(":hv", s(pk))
        .scan_index_forward(false)
        .send()
        .await
        .unwrap();
    assert_eq!(
        sk_bytes(resp_desc.items()),
        vec![vec![0x02], vec![0x01, 0xFF], vec![0x01, 0x00]],
        "descending scan must reverse the unsigned byte order"
    );

    let _ = c.delete_table().table_name(&table).send().await;
}

#[tokio::test]
async fn query_binary_sort_key_between_uses_byte_order() {
    let c = client();
    let table = format!("BinSkBetween_{}", ts());
    create_binary_sk_table(c, &table).await;

    let pk = "p";
    seed(c, &table, pk, &[&[0x01, 0x00], &[0x01, 0xFF], &[0x02]]).await;

    // BETWEEN [0x01,0x00] AND [0x01,0xFF] (inclusive) must select the two
    // 0x01-prefixed keys and exclude [0x02] (0x02 > 0x01 in the first byte).
    // A length-first ordering would include [0x02] (length 1) and mis-scope
    // the range.
    let resp = c
        .query()
        .table_name(&table)
        .key_condition_expression("#h = :hv AND #r BETWEEN :lo AND :hi")
        .expression_attribute_names("#h", "pk")
        .expression_attribute_names("#r", "sk")
        .expression_attribute_values(":hv", s(pk))
        .expression_attribute_values(":lo", bb(&[0x01, 0x00]))
        .expression_attribute_values(":hi", bb(&[0x01, 0xFF]))
        .send()
        .await
        .unwrap();
    assert_eq!(
        sk_bytes(resp.items()),
        vec![vec![0x01, 0x00], vec![0x01, 0xFF]],
        "BETWEEN on a binary sort key must use unsigned byte order and exclude [0x02]"
    );

    let _ = c.delete_table().table_name(&table).send().await;
}
