// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB tests for tables whose key attribute names are not plain
//! field names.
//!
//! The native UpdateItem upsert writes each key attribute as the path
//! `item_data.<name>`, and MongoDB reads a dot in a path as a step into a
//! sub-document. A table with a key attribute named `a.b` or `s.k` must take
//! the session path instead, which stores the field `"a.b"` the way PutItem
//! does (see `data_engine_update_create.rs`).
//!
//! The tests read the replica set URI from `EXTENDDB_TEST_MONGODB_URI` and are
//! `#[ignore]`d, like the tests in the parent module.

use std::collections::HashMap;

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, UpdateAction};
use extenddb_core::types::{AttributeValue, Item, KeyType, ScalarAttributeType, TableKeyInfo};

use super::{def, expected_document, finish, key, maps, path, s, scratch_engine, stored, with};
use crate::MongoEngine;

/// Key info for a table with a string hash key named `hash` and, when given,
/// a string range key named `range`. The GSI cache is set to "no indexes", so
/// an unconditional UpdateItem would take the native fast path if the key
/// names allowed it.
fn named_table(
    engine: &MongoEngine,
    table_id: &str,
    hash: &str,
    range: Option<&str>,
) -> TableKeyInfo {
    engine.gsi_cache_set(table_id, false);
    let mut key_schema = vec![key(hash, KeyType::Hash)];
    let mut attribute_definitions = vec![def(hash, ScalarAttributeType::S)];
    if let Some(range) = range {
        key_schema.push(key(range, KeyType::Range));
        attribute_definitions.push(def(range, ScalarAttributeType::S));
    }
    TableKeyInfo {
        table_id: table_id.to_owned(),
        table_name: table_id.to_owned(),
        key_schema: key_schema.clone(),
        base_key_schema: key_schema,
        attribute_definitions,
        ..Default::default()
    }
}

fn named_key(hash: &str, range: Option<&str>) -> Item {
    let mut key = Item::new();
    key.insert(hash.to_owned(), s("u1"));
    if let Some(range) = range {
        key.insert(range.to_owned(), s("s1"));
    }
    key
}

/// `SET <name> = :v`.
fn set(name: &str) -> Vec<UpdateAction> {
    vec![UpdateAction::Set {
        path: path(name),
        value: Expr::Placeholder(":v".to_owned()),
    }]
}

/// The key condition `<hash> = :p` and the map that supplies `:p`.
fn partition_condition(hash: &str) -> (KeyCondition, ExpressionMaps) {
    let condition = KeyCondition {
        pk_path: path(hash),
        pk_value: Expr::Placeholder(":p".to_owned()),
        extra_pk_conditions: Vec::new(),
        sk_condition: None,
        extra_sk_conditions: Vec::new(),
    };
    let values = HashMap::from([(":p".to_owned(), s("u1"))]);
    (condition, ExpressionMaps::new(HashMap::new(), values))
}

/// An UpdateItem that creates an item on a table with these key names
/// answers Ok, returns the item with its key under ALL_NEW and stores the
/// document PutItem stores. GetItem, Query and Scan return the item. An
/// UpdateItem that sets an attribute named `prefix`, which is a path prefix
/// of a key name, succeeds on the created item, and PutItem and DeleteItem
/// then read it as any other item.
async fn creation_matches_put_item(
    engine: &MongoEngine,
    table_id: &str,
    hash: &str,
    range: Option<&str>,
    prefix: &str,
) {
    let key_info = named_table(engine, table_id, hash, range);
    let key = named_key(hash, range);
    let created = with(key.clone(), "w", s("upd"));

    let (old, new) = engine
        .update_item_impl(
            &key_info,
            &key,
            &set("w"),
            false,
            true,
            None,
            &maps(s("upd")),
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("{table_id}: creating update: {e:?}"));
    assert_eq!(old, None, "{table_id}: no old image on creation");
    assert_eq!(
        new,
        Some(created.clone()),
        "{table_id}: ALL_NEW has the key"
    );
    assert_eq!(
        stored(engine, &key_info, &key).await,
        expected_document(&created, &key_info, 1),
        "{table_id}: stored document"
    );
    assert_eq!(
        engine
            .get_item_impl(&key_info, &key)
            .await
            .unwrap_or_else(|e| panic!("{table_id}: get: {e:?}")),
        Some(created.clone()),
        "{table_id}: GetItem returns the created item"
    );

    let (condition, query_maps) = partition_condition(hash);
    let (query, _) = engine
        .query_impl(&key_info, &condition, &query_maps, true, None, None, None)
        .await
        .unwrap_or_else(|e| panic!("{table_id}: query: {e:?}"));
    assert_eq!(query, vec![created.clone()], "{table_id}: Query");
    let (scan, _) = engine
        .scan_impl(&key_info, None, None, None, None, None)
        .await
        .unwrap_or_else(|e| panic!("{table_id}: scan: {e:?}"));
    assert_eq!(scan, vec![created.clone()], "{table_id}: Scan");

    let updated = with(created, prefix, s("p"));
    let (_, new) = engine
        .update_item_impl(
            &key_info,
            &key,
            &set(prefix),
            false,
            true,
            None,
            &maps(s("p")),
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("{table_id}: SET {prefix} on the created item: {e:?}"));
    assert_eq!(new, Some(updated.clone()), "{table_id}: SET {prefix}");
    assert_eq!(
        stored(engine, &key_info, &key).await,
        expected_document(&updated, &key_info, 2),
        "{table_id}: stored document after SET {prefix}"
    );

    let put = with(key.clone(), "w", s("put"));
    let old = engine
        .put_item_impl(
            &key_info,
            put.clone(),
            true,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("{table_id}: put: {e:?}"));
    assert_eq!(
        old,
        Some(updated),
        "{table_id}: PutItem returns the old item"
    );
    let deleted = engine
        .delete_item_impl(
            &key_info,
            &key,
            true,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("{table_id}: delete: {e:?}"));
    assert_eq!(
        deleted,
        Some(put),
        "{table_id}: DeleteItem returns the item"
    );
}

/// A hash key named `a.b`: the creating UpdateItem would have stored
/// `item_data: { a: { b: ... } }`, which no read parses.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_update_item_creates_an_item_when_the_hash_key_name_has_a_dot() {
    let engine = scratch_engine().await;
    creation_matches_put_item(&engine, "dot-hash", "a.b", None, "a").await;
    finish(engine).await;
}

/// A range key named `s.k`, with a plain hash key.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_update_item_creates_an_item_when_the_range_key_name_has_a_dot() {
    let engine = scratch_engine().await;
    creation_matches_put_item(&engine, "dot-range", "pk", Some("s.k"), "s").await;
    finish(engine).await;
}

/// An UpdateItem on an item that already exists may set an attribute named
/// like a path prefix of a key name (`a` for the key `a.b`). Through the
/// native upsert the `$set` of `item_data.a` and the `$setOnInsert` of
/// `item_data.a.b` conflict, and MongoDB refuses the update before it looks
/// for the document, so this worked on main and must keep working.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_update_item_may_set_an_attribute_named_like_a_prefix_of_a_key_name() {
    let engine = scratch_engine().await;
    let cases: [(&str, Option<&str>, &str); 2] = [("a.b", None, "a"), ("pk", Some("s.k"), "s")];
    for (hash, range, prefix) in cases {
        let table_id = format!("prefix-{prefix}");
        let key_info = named_table(&engine, &table_id, hash, range);
        let key = named_key(hash, range);
        let existing = with(key.clone(), "w", s("put"));
        engine
            .put_item_impl(
                &key_info,
                existing.clone(),
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{table_id}: put: {e:?}"));

        let (_, new) = engine
            .update_item_impl(
                &key_info,
                &key,
                &set(prefix),
                false,
                true,
                None,
                &maps(AttributeValue::S("p".to_owned())),
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("{table_id}: SET {prefix} on a stored item: {e:?}"));
        let updated = with(existing, prefix, s("p"));
        assert_eq!(new, Some(updated.clone()), "{table_id}: ALL_NEW");
        assert_eq!(
            stored(&engine, &key_info, &key).await,
            expected_document(&updated, &key_info, 1),
            "{table_id}: stored document"
        );
    }
    finish(engine).await;
}
