// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Adding a GSI to a table that has no sort key.
//!
//! The backfill that copies existing rows into a new index reads the base table
//! ordered by its key columns. Naming the sort-key columns unconditionally makes
//! that query fail with `column "sk_s" does not exist` on a hash-only base
//! table, and because the backfill runs inside the index creation the whole
//! `UpdateTable` is rolled back and the caller sees `InternalServerError`. A
//! partition-key-only table is the simplest table DynamoDB allows, so this
//! covers the add end to end: the call succeeds, the index reaches ACTIVE, and a
//! row written before the index existed is visible through it afterwards.

use crate::test_base::*;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, CreateGlobalSecondaryIndexAction, GlobalSecondaryIndexUpdate,
    KeySchemaElement, KeyType, Projection, ProjectionType, ScalarAttributeType,
};
use std::time::Duration;

#[tokio::test]
async fn gsi_can_be_added_to_a_table_with_no_sort_key() {
    let c = client();
    let table = format!("GsiHashOnly{}", ts());

    c.create_table()
        .table_name(&table)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
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
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .expect("hash-only table creation must succeed");
    wait_for_active(c, &table).await;

    // Written before the index exists, so it can only appear through the index
    // if the backfill ran. Without it the query below would pass on an index
    // that copied nothing.
    c.put_item()
        .table_name(&table)
        .item("pk", s("item-1"))
        .item("gsiKey", s("gsi-value"))
        .send()
        .await
        .expect("seed write must succeed");

    c.update_table()
        .table_name(&table)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("gsiKey")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_index_updates(
            GlobalSecondaryIndexUpdate::builder()
                .create(
                    CreateGlobalSecondaryIndexAction::builder()
                        .index_name("hashOnlyIdx")
                        .key_schema(
                            KeySchemaElement::builder()
                                .attribute_name("gsiKey")
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
                .build(),
        )
        .send()
        .await
        .unwrap_or_else(|e| {
            panic!(
                "adding a GSI to a hash-only table must succeed: {}",
                err_msg(&e)
            )
        });
    wait_for_active(c, &table).await;

    // The index must exist on the table description, not merely have been
    // accepted by UpdateTable.
    let described = c
        .describe_table()
        .table_name(&table)
        .send()
        .await
        .expect("describe must succeed");
    let gsis = described
        .table()
        .expect("table description")
        .global_secondary_indexes();
    assert!(
        gsis.iter().any(|g| g.index_name() == Some("hashOnlyIdx")),
        "the new index must appear on the table description, got: {:?}",
        gsis.iter().map(|g| g.index_name()).collect::<Vec<_>>()
    );

    // The pre-existing row must be readable through the index, which is what
    // proves the backfill query ran rather than silently copying nothing. The
    // index is eventually consistent, so poll rather than read once.
    let mut count = 0;
    for _ in 0..30 {
        count = c
            .query()
            .table_name(&table)
            .index_name("hashOnlyIdx")
            .key_condition_expression("gsiKey = :v")
            .expression_attribute_values(":v", s("gsi-value"))
            .send()
            .await
            .expect("query on the new index must succeed")
            .count();
        if count >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(
        count, 1,
        "the row written before the index existed must be backfilled into it"
    );

    c.delete_table().table_name(&table).send().await.ok();
}
