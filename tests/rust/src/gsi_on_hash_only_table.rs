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

const GSI_BACKFILL_TEST_GATE: &str = "gsi_backfill_test_gate";

fn management_client() -> (reqwest::Client, String, String, String) {
    let endpoint = std::env::var("EXTENDDB_TEST_ENDPOINT")
        .expect("MongoDB GSI race test requires EXTENDDB_TEST_ENDPOINT");
    let user = std::env::var("EXTENDDB_ADMIN_USER").unwrap_or_else(|_| "admin".to_owned());
    let password = std::env::var("EXTENDDB_ADMIN_PASSWORD")
        .expect("MongoDB GSI race test requires EXTENDDB_ADMIN_PASSWORD");
    let http = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("management HTTP client build");
    (
        http,
        format!("{}/management", endpoint.trim_end_matches('/')),
        user,
        password,
    )
}

async fn set_backfill_gate(value: &str) {
    let (http, base, user, password) = management_client();
    let response = http
        .put(format!("{base}/settings/{GSI_BACKFILL_TEST_GATE}"))
        .basic_auth(user, Some(password))
        .json(&serde_json::json!({ "value": value }))
        .send()
        .await
        .expect("set GSI backfill gate request");
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    assert!(status.is_success(), "setting GSI backfill gate failed: {status}: {body}");
}

async fn wait_for_backfill_gate(value: &str) -> bool {
    let (http, base, user, password) = management_client();
    for _ in 0..120 {
        let response = http
            .get(format!("{base}/settings/{GSI_BACKFILL_TEST_GATE}"))
            .basic_auth(&user, Some(&password))
            .send()
            .await;
        if let Ok(response) = response {
            if response.status().is_success() {
                if let Ok(body) = response.json::<serde_json::Value>().await {
                    if body.get("value").and_then(serde_json::Value::as_str) == Some(value) {
                        return true;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

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

/// A DeleteItem that commits after backfill reads an item but before it writes
/// the index must not be followed by a stale GSI upsert. The management gate
/// makes that ordering deterministic rather than relying on timing.
#[tokio::test]
async fn deleting_item_during_gsi_backfill_does_not_leave_stale_index_entry() {
    if std::env::var("EXTENDDB_TEST_MONGODB_CONTAINER").is_err() {
        // This test uses the MongoDB-only management gate and is not valid
        // against PostgreSQL or real DynamoDB.
        return;
    }

    let c = client();
    let table = format!("GsiBackfillDelete{}", ts());

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
        .expect("table creation must succeed");
    wait_for_active(c, &table).await;

    c.put_item()
        .table_name(&table)
        .item("pk", s("item-1"))
        .item("gsiKey", s("stale-value"))
        .send()
        .await
        .expect("seed write must succeed");

    // Arm the gate before creating the index. Backfill will publish `paused`
    // after reading its base batch and wait for `release` before any index
    // write or base-document guard.
    set_backfill_gate("armed").await;

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
                        .index_name("deleteDuringBackfill")
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
        .expect("GSI creation must succeed");

    if !wait_for_backfill_gate("paused").await {
        // Release first so a diagnostic failure cannot leave the background
        // worker blocked for the remainder of the suite.
        set_backfill_gate("release").await;
        panic!("backfill did not reach its deterministic pause");
    }

    // The base item is deleted after the backfill snapshot read, while no
    // index row has been written yet.
    let delete_result = c
        .delete_item()
        .table_name(&table)
        .key("pk", s("item-1"))
        .send()
        .await;

    // Always release the worker after the API operation so a failed assertion
    // cannot leave the server's background worker paused.
    set_backfill_gate("release").await;
    delete_result.expect("DeleteItem during GSI backfill must succeed");

    let item = c
        .get_item()
        .table_name(&table)
        .key("pk", s("item-1"))
        .send()
        .await
        .expect("GetItem must succeed after deletion")
        .item;
    assert!(item.is_none(), "the deleted base item must remain absent");

    let mut index_active = false;
    for _ in 0..120 {
        let desc = c
            .describe_table()
            .table_name(&table)
            .send()
            .await
            .expect("describe must succeed");
        index_active = desc
            .table()
            .and_then(|t| t.global_secondary_indexes().iter().find(|i| {
                i.index_name() == Some("deleteDuringBackfill")
                    && i.index_status()
                        == Some(&aws_sdk_dynamodb::types::IndexStatus::Active)
            }))
            .is_some();
        if index_active {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(index_active, "the new GSI did not become ACTIVE in time");

    let old_count = c
        .query()
        .table_name(&table)
        .index_name("deleteDuringBackfill")
        .key_condition_expression("gsiKey = :v")
        .expression_attribute_values(":v", s("stale-value"))
        .send()
        .await
        .expect("GSI query must succeed")
        .count();
    assert_eq!(old_count, 0, "the deleted item must not survive in the GSI");

    c.delete_table().table_name(&table).send().await.ok();
}
