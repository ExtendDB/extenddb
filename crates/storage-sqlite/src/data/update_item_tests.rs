// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_item` on a key that holds no item (issue #478).

use std::collections::HashMap;

use extenddb_core::expression::{ExpressionMaps, UpdateAction, parse_update, tokenize};
use extenddb_core::types::{AttributeValue, CreateTableInput, Item, TableKeyInfo};
use extenddb_storage::DataEngine;
use serde_json::json;

use crate::SqliteEngine;

const ACCOUNT: &str = "000000000000";

async fn engine() -> SqliteEngine {
    let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
        .await
        .expect("engine");
    crate::schema::apply(&engine.pool).await.expect("schema");
    sqlx::query("UPDATE settings SET value = '0' WHERE key = 'control_plane_delay_seconds'")
        .execute(&engine.pool)
        .await
        .expect("delay");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
        .bind(ACCOUNT)
        .execute(&engine.pool)
        .await
        .expect("account");
    engine
}

async fn hash_table(engine: &SqliteEngine) -> TableKeyInfo {
    let input: CreateTableInput = serde_json::from_value(json!({
        "TableName": "t",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST"
    }))
    .expect("input");
    let desc = engine
        .create_table_impl(ACCOUNT, input, false)
        .await
        .expect("create");
    TableKeyInfo {
        table_name: "t".to_owned(),
        account_id: ACCOUNT.to_owned(),
        table_id: desc.table_id.clone(),
        key_schema: desc.key_schema.clone(),
        base_key_schema: desc.key_schema.clone(),
        attribute_definitions: desc.attribute_definitions.clone(),
        ..Default::default()
    }
}

fn key(name: &str) -> Item {
    let mut key = Item::new();
    key.insert("pk".to_owned(), AttributeValue::S(name.to_owned()));
    key
}

fn actions(expr: &str) -> Vec<UpdateAction> {
    parse_update(&tokenize(expr).expect("tokens")).expect("actions")
}

fn maps_with_value(value: AttributeValue) -> ExpressionMaps {
    let mut values = HashMap::new();
    values.insert("v".to_owned(), value);
    ExpressionMaps::new(HashMap::new(), values)
}

async fn update(
    engine: &SqliteEngine,
    key_info: &TableKeyInfo,
    key: &Item,
    expr: &str,
    maps: &ExpressionMaps,
) -> (Option<Item>, Option<Item>) {
    engine
        .update_item(key_info, key, &actions(expr), true, true, None, maps, None)
        .await
        .expect("update")
}

#[tokio::test]
async fn removal_only_update_on_a_missing_key_writes_nothing() {
    let engine = engine().await;
    let key_info = hash_table(&engine).await;
    let maps = maps_with_value(AttributeValue::SS(["x".to_owned()].into_iter().collect()));

    for expr in [
        "REMOVE a",
        "REMOVE a, b",
        "REMOVE a.b[0]",
        "DELETE s :v",
        "REMOVE a DELETE s :v",
    ] {
        let k = key(expr);
        let (old, new) = update(&engine, &key_info, &k, expr, &maps).await;
        assert_eq!((old, new), (None, None), "{expr}: no image to return");
        let stored = engine.get_item(&key_info, &k).await.expect("get");
        assert_eq!(stored, None, "{expr}: no item may be created");
    }
}

#[tokio::test]
async fn set_add_and_mixed_updates_on_a_missing_key_still_create() {
    let engine = engine().await;
    let key_info = hash_table(&engine).await;
    let maps = maps_with_value(AttributeValue::N("1".to_owned()));

    for expr in ["SET a = :v", "ADD n :v", "REMOVE a SET b = :v"] {
        let k = key(expr);
        let (old, new) = update(&engine, &key_info, &k, expr, &maps).await;
        assert_eq!(old, None, "{expr}");
        let new = new.expect("created image");
        assert_eq!(new.len(), 2, "{expr}: key plus one attribute, got {new:?}");
        let stored = engine.get_item(&key_info, &k).await.expect("get");
        assert_eq!(stored, Some(new), "{expr}");
    }

    // No actions at all: the key-only item is created, as the service does.
    let k = key("no-actions");
    let (_, new) = engine
        .update_item(&key_info, &k, &[], true, true, None, &maps, None)
        .await
        .expect("update");
    assert_eq!(new, Some(k.clone()));
    assert_eq!(engine.get_item(&key_info, &k).await.expect("get"), Some(k));
}

#[tokio::test]
async fn removal_on_an_existing_item_still_removes() {
    let engine = engine().await;
    let key_info = hash_table(&engine).await;
    let maps = ExpressionMaps::default();
    let k = key("existing");
    let mut item = k.clone();
    item.insert("a".to_owned(), AttributeValue::S("1".to_owned()));
    engine
        .put_item(&key_info, item.clone(), false, None, &maps, None)
        .await
        .expect("put");

    let (old, new) = update(&engine, &key_info, &k, "REMOVE a", &maps).await;
    assert_eq!(old, Some(item));
    assert_eq!(new, Some(k.clone()));
    assert_eq!(engine.get_item(&key_info, &k).await.expect("get"), Some(k));
}
