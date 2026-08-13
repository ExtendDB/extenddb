// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Regression tests for issue #259: adding a GSI to a populated composite-key
//! table must not disturb the base table's own key metadata.
//!
//! `UpdateTable` carries only the attribute definitions the request needs, so the
//! backends must merge them into the stored set. Replacing the set dropped the
//! base table's `pk`/`sk` definitions, `sk_info` then found no definition for the
//! sort key, and `GetItem` silently degraded to a partition-only lookup that
//! returned a different item under the same partition key with HTTP 200.
//!
//! The merge behaviour is measured against real DynamoDB (us-east-1, 2026-08-13):
//! a table created with `[pk, sk]` whose `UpdateTable` supplied only `[f01, f02]`
//! reported all four definitions in the next `DescribeTable`.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, CreateGlobalSecondaryIndexAction, GlobalSecondaryIndexUpdate, IndexStatus,
    KeySchemaElement, KeyType, Projection, ProjectionType, ScalarAttributeType,
};
use std::collections::HashMap;
use std::time::Duration;

/// Items per partition. More than one is what makes a partition-only read
/// observably wrong; a handful keeps the test fast while still failing on the
/// pre-fix code, where every read but one returned the first physical row.
const ITEM_COUNT: usize = 8;

/// Shared by every item, so the GSI's hash key alone never narrows to one row.
const GSI_HASH_VALUE: &str = "gpk-shared";

/// Poll DescribeTable until the named index reports ACTIVE (up to 60s).
async fn wait_for_index_active(c: &aws_sdk_dynamodb::Client, table: &str, index_name: &str) {
    for _ in 0..60 {
        if let Ok(resp) = c.describe_table().table_name(table).send().await {
            if let Some(t) = resp.table() {
                let active = t.global_secondary_indexes().iter().any(|i| {
                    i.index_name() == Some(index_name)
                        && i.index_status() == Some(&IndexStatus::Active)
                });
                if active {
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    panic!("Index {index_name} on {table} did not become ACTIVE within 60s");
}

/// Create a composite-key table and populate one partition with `ITEM_COUNT`
/// items whose sort keys are distinct, then add a GSI keyed on two non-key
/// attributes supplying only those attributes' definitions.
async fn table_with_gsi_added_after_population(label: &str) -> (String, String) {
    let c = client();
    let table = format!("Gsi259_{label}_{}", ts());

    c.create_table()
        .table_name(&table)
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
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
    wait_for_active(c, &table).await;

    for i in 0..ITEM_COUNT {
        let mut item = HashMap::new();
        item.insert("pk".into(), s("shared"));
        item.insert("sk".into(), s(&format!("sk-{i:05}")));
        // Every item shares one GSI hash key so a query that matched on the hash
        // key alone would return all of them. That is what makes the sort-key
        // assertion below discriminating: an index whose sort key was not
        // resolved behaves as hash-only and returns ITEM_COUNT rows, not one.
        item.insert("f01".into(), s(GSI_HASH_VALUE));
        item.insert("f02".into(), s(&format!("gsk-{i:05}")));
        item.insert("payload".into(), s(&format!("payload-{i:05}")));
        c.put_item()
            .table_name(&table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    // Only the new index's key attributes are supplied, exactly as the SDK and
    // real DynamoDB expect. The stored set must end up as the union.
    c.update_table()
        .table_name(&table)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("f01")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("f02")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_index_updates(
            GlobalSecondaryIndexUpdate::builder()
                .create(
                    CreateGlobalSecondaryIndexAction::builder()
                        .index_name("gsi259")
                        .key_schema(
                            KeySchemaElement::builder()
                                .attribute_name("f01")
                                .key_type(KeyType::Hash)
                                .build()
                                .unwrap(),
                        )
                        .key_schema(
                            KeySchemaElement::builder()
                                .attribute_name("f02")
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
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    wait_for_index_active(c, &table, "gsi259").await;

    (table, "gsi259".to_owned())
}

/// Same drill on a NUMERIC sort key. The merge is type-agnostic, but the
/// `extenddb migrate` repair recovers the lost type from the data table's PRIMARY
/// KEY column, and every table carries all three typed columns (`sk_s`, `sk_n`,
/// `sk_b`), so a numeric sort key is the case that catches a recovery keying off
/// column existence instead of key membership.
#[tokio::test]
async fn adding_a_gsi_must_not_change_get_item_on_a_numeric_sort_key() {
    let c = client();
    let table = format!("Gsi259N_{}", ts());

    c.create_table()
        .table_name(&table)
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
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
    wait_for_active(c, &table).await;

    for i in 0..ITEM_COUNT {
        let mut item = HashMap::new();
        item.insert("pk".into(), s("shared"));
        item.insert("sk".into(), n(i64::try_from(i).unwrap()));
        item.insert("f01".into(), s(GSI_HASH_VALUE));
        item.insert("f02".into(), s(&format!("gsk-{i:05}")));
        c.put_item()
            .table_name(&table)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    c.update_table()
        .table_name(&table)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("f01")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("f02")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_index_updates(
            GlobalSecondaryIndexUpdate::builder()
                .create(
                    CreateGlobalSecondaryIndexAction::builder()
                        .index_name("gsi259n")
                        .key_schema(
                            KeySchemaElement::builder()
                                .attribute_name("f01")
                                .key_type(KeyType::Hash)
                                .build()
                                .unwrap(),
                        )
                        .key_schema(
                            KeySchemaElement::builder()
                                .attribute_name("f02")
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
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    wait_for_index_active(c, &table, "gsi259n").await;

    for i in 0..ITEM_COUNT {
        let mut key = HashMap::new();
        key.insert("pk".into(), s("shared"));
        key.insert("sk".into(), n(i64::try_from(i).unwrap()));
        let resp = c
            .get_item()
            .table_name(&table)
            .set_key(Some(key))
            .consistent_read(true)
            .send()
            .await
            .unwrap();
        let item = resp
            .item()
            .unwrap_or_else(|| panic!("GetItem for numeric sort key {i} returned no item"));
        assert_eq!(
            item.get("sk").unwrap(),
            &n(i64::try_from(i).unwrap()),
            "GetItem returned a different numeric sort key than the one requested ({i})"
        );
    }
}

/// #259: after the GSI is ACTIVE, every strongly consistent base-table GetItem
/// must still return the item its sort key names. Before the fix this returned
/// `sk-00000` for every request.
#[tokio::test]
async fn adding_a_gsi_must_not_change_base_table_get_item() {
    let c = client();
    let (table, _index) = table_with_gsi_added_after_population("get").await;

    for i in 0..ITEM_COUNT {
        let requested = format!("sk-{i:05}");
        let mut key = HashMap::new();
        key.insert("pk".into(), s("shared"));
        key.insert("sk".into(), s(&requested));

        let resp = c
            .get_item()
            .table_name(&table)
            .set_key(Some(key))
            .consistent_read(true)
            .send()
            .await
            .unwrap();

        let item = resp
            .item()
            .unwrap_or_else(|| panic!("GetItem for {requested} returned no item"));
        assert_eq!(
            item.get("sk").unwrap(),
            &s(&requested),
            "GetItem returned a different sort key than the one requested ({requested}); \
             the base table's sort key metadata was lost when the GSI was added"
        );
        assert_eq!(
            item.get("payload").unwrap(),
            &s(&format!("payload-{i:05}")),
            "GetItem returned another item's payload for {requested}"
        );
    }
}

/// The stored attribute definitions must be the union of the base table's and the
/// request's, which is what DescribeTable reports on real DynamoDB.
#[tokio::test]
async fn adding_a_gsi_merges_attribute_definitions() {
    let c = client();
    let (table, _index) = table_with_gsi_added_after_population("merge").await;

    let resp = c.describe_table().table_name(&table).send().await.unwrap();
    let mut names: Vec<&str> = resp
        .table()
        .unwrap()
        .attribute_definitions()
        .iter()
        .map(aws_sdk_dynamodb::types::AttributeDefinition::attribute_name)
        .collect();
    names.sort_unstable();

    assert_eq!(
        names,
        vec!["f01", "f02", "pk", "sk"],
        "UpdateTable must merge the request's attribute definitions into the stored set, \
         not replace it"
    );
}

/// The created index must be usable through its own sort key. The index DDL is
/// built from the merged definitions, so a backend that passed only the request's
/// subset (or, on MongoDB, never persisted them at all) would resolve the index's
/// sort key to nothing and behave as hash-only.
#[tokio::test]
async fn a_gsi_created_by_update_table_is_queryable_by_its_sort_key() {
    let c = client();
    let (table, index) = table_with_gsi_added_after_population("query").await;

    let resp = c
        .query()
        .table_name(&table)
        .index_name(&index)
        .key_condition_expression("#h = :h AND #r = :r")
        .expression_attribute_names("#h", "f01")
        .expression_attribute_names("#r", "f02")
        .expression_attribute_values(":h", s(GSI_HASH_VALUE))
        .expression_attribute_values(":r", s("gsk-00003"))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.count(),
        1,
        "the GSI must apply its sort key: all {ITEM_COUNT} items share the hash key, so an \
         index whose sort key was not resolved returns every one of them"
    );
    assert_eq!(resp.items()[0].get("sk").unwrap(), &s("sk-00003"));

    // The index is fully populated: the hash key alone reaches every item.
    let all = c
        .query()
        .table_name(&table)
        .index_name(&index)
        .key_condition_expression("#h = :h")
        .expression_attribute_names("#h", "f01")
        .expression_attribute_values(":h", s(GSI_HASH_VALUE))
        .send()
        .await
        .unwrap();
    assert_eq!(all.count(), i32::try_from(ITEM_COUNT).unwrap());
}
