// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A put that carries key info read before an UpdateTable added a GSI is
//! checked against the definitions read with the indexes, on the single
//! path and on the batch path, so a wrong-typed key of the new index is
//! refused rather than written.

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{AttributeValue, CreateTableInput, Item, UpdateTableInput};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BatchRow, DataEngine, TableEngine};
use serde_json::json;

use crate::SqliteEngine;

const ACCOUNT: &str = "000000000000";

fn item(pk: &str, g: AttributeValue) -> Item {
    let mut item = Item::new();
    item.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
    item.insert("g".to_owned(), g);
    item
}

fn row(item: Item) -> BatchRow {
    BatchRow {
        item,
        condition: None,
        marker: None,
        return_old: false,
    }
}

#[tokio::test]
async fn a_write_with_key_info_older_than_a_new_gsi_is_validated_against_its_definitions() {
    let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
        .await
        .expect("engine");
    crate::migrations::apply_for_tests(&engine.pool).await;
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
        .bind(ACCOUNT)
        .execute(&engine.pool)
        .await
        .expect("account");
    let input: CreateTableInput = serde_json::from_value(json!({
        "TableName": "t",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("input");
    engine
        .create_table_impl(ACCOUNT, input, false)
        .await
        .expect("create table");
    sqlx::query("UPDATE tables SET table_status = 'ACTIVE' WHERE table_name = 't'")
        .execute(&engine.pool)
        .await
        .expect("activate the table");
    // Read before the GSI exists, as the engine's cached key info is until
    // UpdateTable returns: it does not know `g`.
    let stale = engine.table_key_info(ACCOUNT, "t").await.expect("key info");
    let update: UpdateTableInput = serde_json::from_value(json!({
        "TableName": "t",
        "AttributeDefinitions": [{"AttributeName": "g", "AttributeType": "S"}],
        "GlobalSecondaryIndexUpdates": [{"Create": {
            "IndexName": "gidx",
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }}]
    }))
    .expect("update input");
    engine
        .update_table_impl(ACCOUNT, update)
        .await
        .expect("add the GSI");
    let wrong = || AttributeValue::N("7".to_owned());

    let put = engine
        .put_item(
            &stale,
            item("bad", wrong()),
            false,
            None,
            &ExpressionMaps::default(),
            None,
            None,
        )
        .await;
    assert!(
        matches!(put, Err(StorageError::Validation(_))),
        "a put with a wrong-typed index key: {put:?}"
    );
    let batch = engine
        .put_items_batch(
            &stale,
            vec![
                row(item("ok", AttributeValue::S("fine".to_owned()))),
                row(item("bad", wrong())),
            ],
            None,
        )
        .await;
    assert!(
        matches!(batch, Err(StorageError::Validation(_))),
        "a batch row with a wrong-typed index key: {batch:?}"
    );
    let (table_id,): (String,) =
        sqlx::query_as("SELECT table_id FROM tables WHERE table_name = 't'")
            .fetch_one(&engine.pool)
            .await
            .expect("table id");
    let written: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM \"_ddb_{table_id}\""))
        .fetch_one(&engine.pool)
        .await
        .expect("count the base rows");
    assert_eq!(written, 0, "the refused put and batch wrote nothing");
}
