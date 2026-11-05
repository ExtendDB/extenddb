// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_item` on a key that holds no item (issue #478), and a literal
//! for a secondary-index key that the cached key info does not list yet
//! (issue #410).

use std::collections::HashMap;

use extenddb_core::expression::{ExpressionMaps, UpdateAction, parse_update, tokenize};
use extenddb_core::types::{
    AttributeValue, CreateTableInput, Item, TableKeyInfo, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TableEngine};
use serde_json::json;

use crate::SqliteEngine;

const ACCOUNT: &str = "000000000000";

async fn engine() -> SqliteEngine {
    let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
        .await
        .expect("engine");
    crate::migrations::apply_for_tests(&engine.pool).await;
    // Zero control-plane delay: the table is ACTIVE at create time, since no
    // transition poller runs inside a unit test. The migrations write no
    // settings row for the delay, so the row is inserted here.
    sqlx::query(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0')",
    )
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
        .put_item(&key_info, item.clone(), false, None, &maps, None, None)
        .await
        .expect("put");

    let (old, new) = update(&engine, &key_info, &k, "REMOVE a", &maps).await;
    assert_eq!(old, Some(item));
    assert_eq!(new, Some(k.clone()));
    assert_eq!(engine.get_item(&key_info, &k).await.expect("get"), Some(k));
}

/// UpdateItem carrying the key info cached before an UpdateTable added a GSI,
/// as a write does that read the cache before the UpdateTable returned and
/// took the write lock after the build. A literal of the wrong type for the
/// new index key gets the service's per-index wording, the answer once the
/// cache lists the index, not the wording for a value computed from the item.
#[tokio::test]
async fn a_wrong_typed_literal_for_a_gsi_the_cached_key_info_lacks_names_the_index() {
    let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 409_600)
        .await
        .expect("engine");
    crate::migrations::apply_for_tests(&engine.pool).await;
    let account = "000000000000";
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'default')")
        .bind(account)
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
        .create_table_impl(account, input, false)
        .await
        .expect("create table");
    sqlx::query("UPDATE tables SET table_status = 'ACTIVE' WHERE table_name = 't'")
        .execute(&engine.pool)
        .await
        .expect("activate the table");
    let cached = engine.table_key_info(account, "t").await.expect("key info");
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
        .update_table_impl(account, update)
        .await
        .expect("add the GSI");

    let key: Item = serde_json::from_value(json!({"pk": {"S": "a"}})).expect("key");
    let actions = parse_update(&tokenize("SET g = :v").expect("tokenize")).expect("parse");
    let maps = ExpressionMaps::new(
        std::collections::HashMap::new(),
        std::collections::HashMap::from([("v".to_owned(), AttributeValue::N("9".to_owned()))]),
    );
    let result = engine
        .update_item(&cached, &key, &actions, false, false, None, &maps, None)
        .await;
    let Err(StorageError::Validation(msg)) = result else {
        panic!("a wrong-typed index key was not a ValidationException: {result:?}");
    };
    assert_eq!(
        msg,
        "One or more parameter values were invalid: Type mismatch for Index Key g \
         Expected: S Actual: N IndexName: gidx"
    );
    let stored = engine.get_item(&cached, &key).await.expect("get the item");
    assert!(stored.is_none(), "the refused update wrote nothing");
}
