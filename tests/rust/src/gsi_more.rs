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

    c.delete_table()
        .table_name(&table_name)
        .send()
        .await
        .unwrap();
    wait_for_deleted(c, &table_name).await;
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

    c.delete_table()
        .table_name(&table_name)
        .send()
        .await
        .unwrap();
    wait_for_deleted(c, &table_name).await;
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

/// Regression: a hash-only GSI on a composite-key base table. Many items share
/// the GSI hash and the base partition key, differing only by the base sort
/// key, so a continuation must carry the base sort key to disambiguate.
///
/// Previously the pagination resume bound the base sort key without the base
/// partition key, diverging from the generated SQL (which binds both), causing
/// a parameter/bind-count mismatch and a 500 on the second page. This walks
/// every item across pages and asserts each is returned exactly once.
#[tokio::test]
async fn hash_only_gsi_pagination_on_composite_base_table() {
    use aws_sdk_dynamodb::types::{AttributeValue, BillingMode};
    use std::collections::{BTreeSet, HashMap};
    use std::time::Duration;

    let c = client();
    let table = format!("HashOnlyGsiPage_{}", ts());
    let gh = format!("gho_{}", ts());

    let gsi = GlobalSecondaryIndex::builder()
        .index_name("gho")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("gh")
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

    c.create_table()
        .table_name(&table)
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
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("gh")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(gsi)
        .send()
        .await
        .unwrap();
    wait_for_active(c, &table).await;

    let count = 5;
    for i in 0..count {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), s("p"));
        item.insert("sk".to_string(), s(&format!("s{i}")));
        item.insert("gh".to_string(), s(&gh));
        c.put_item()
            .table_name(&table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }
    // GSI is eventually consistent — allow projection to settle.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut total = 0usize;
    let mut pages = 0usize;
    let mut esk: Option<HashMap<String, AttributeValue>> = None;

    loop {
        let mut req = c
            .query()
            .table_name(&table)
            .index_name("gho")
            .key_condition_expression("#gh = :gh")
            .expression_attribute_names("#gh", "gh")
            .expression_attribute_values(":gh", s(&gh))
            .limit(2);
        if let Some(ref lek) = esk {
            req = req.set_exclusive_start_key(Some(lek.clone()));
        }

        let resp = req.send().await.unwrap();
        for it in resp.items() {
            if let Some(AttributeValue::S(sk)) = it.get("sk") {
                seen.insert(sk.clone());
                total += 1;
            }
        }
        pages += 1;
        match resp.last_evaluated_key() {
            Some(lek) => esk = Some(lek.to_owned()),
            None => break,
        }
        assert!(pages < 10, "pagination did not terminate");
    }

    assert_eq!(seen.len(), count, "every item must be seen exactly once");
    assert_eq!(
        total, count,
        "no item may be returned twice across a page boundary"
    );
    assert!(pages > 1, "Limit 2 over 5 items must force real pagination");

    let _ = c.delete_table().table_name(&table).send().await;
}
