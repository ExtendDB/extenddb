// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB tests for the document an UpdateItem writes when it creates an
//! item (issue #436), on the native fast path and on the transaction path.
//!
//! The tests read the replica set URI from `EXTENDDB_TEST_MONGODB_URI` and are
//! `#[ignore]`d, like the tests in the parent module.

use bson::{Document, doc};
use extenddb_core::expression::{Expr, ExpressionMaps, UpdateAction};
use extenddb_core::types::{AttributeValue, Item};
use extenddb_storage::error::StorageError;

use super::{
    expected_document, finish, item_key, maps, n, path, s, scratch_engine, stored, table, with,
};
use crate::data::{data_collection_name, document_to_item};

/// An UpdateItem that creates the item, on the native fast path (SET with a
/// placeholder, REMOVE) and on the transaction path (numeric ADD), stores the
/// document `item_to_document` builds, plus `_v` 1: `_id` as the key text and
/// the key attributes in `item_data`. For SET and ADD, ALL_NEW returns the
/// item with its key, and a PutItem on the key replaces the item and returns
/// it as the old image. For REMOVE the test pins the stored document and that
/// a PutItem on the key succeeds. The service creates no item for a
/// REMOVE-only update and its ALL_NEW answer for one is not measured, so the
/// REMOVE case asserts neither the ALL_NEW answer nor the old image.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_update_item_that_creates_an_item_stores_its_key_like_put_item() {
    let engine = scratch_engine().await;
    let cases: [(&str, Vec<UpdateAction>, AttributeValue, Item); 3] = [
        (
            "set",
            vec![UpdateAction::Set {
                path: path("w"),
                value: Expr::Placeholder(":v".to_owned()),
            }],
            s("upd"),
            with(Item::new(), "w", s("upd")),
        ),
        (
            "remove",
            vec![UpdateAction::Remove { path: path("w") }],
            s("unused"),
            Item::new(),
        ),
        (
            "add",
            vec![UpdateAction::Add {
                path: path("c"),
                value: Expr::Placeholder(":v".to_owned()),
            }],
            n("5"),
            with(Item::new(), "c", n("5")),
        ),
    ];
    for range in [false, true] {
        for (name, actions, value, extra) in &cases {
            let table_id = format!("{name}-{}", if range { "range" } else { "hash" });
            let key_info = table(&engine, &table_id, range);
            let key = item_key(range);
            let mut want = key.clone();
            want.extend(extra.clone());

            let (old, new) = engine
                .update_item_impl(
                    &key_info,
                    &key,
                    actions,
                    false,
                    true,
                    None,
                    &maps(value.clone()),
                    None,
                )
                .await
                .unwrap_or_else(|e| panic!("{table_id}: update: {e:?}"));
            assert_eq!(old, None, "{table_id}: no old image on creation");
            if *name != "remove" {
                assert_eq!(
                    new,
                    Some(want.clone()),
                    "{table_id}: ALL_NEW carries the key"
                );
            }
            assert_eq!(
                stored(&engine, &key_info, &key).await,
                expected_document(&want, &key_info, 1),
                "{table_id}: stored document"
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
                .unwrap_or_else(|e| panic!("{table_id}: put after update: {e:?}"));
            if *name != "remove" {
                assert_eq!(old, Some(want), "{table_id}: put returns the created item");
            }
            let after = stored(&engine, &key_info, &key).await;
            assert_eq!(
                document_to_item(&after).expect("item"),
                put,
                "{table_id}: put replaced the item"
            );
        }
    }
    finish(engine).await;
}

/// Sixty-four UpdateItem calls creating the same item at once, on the fast
/// path, from eight runtime threads: every call succeeds, the stored
/// document has the shape PutItem writes, and `_v` counts every call, so
/// each one applied to the one document after losing the create race.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_concurrent_creating_updates_on_one_key_all_apply() {
    const WRITERS: i64 = 64;
    let engine = std::sync::Arc::new(scratch_engine().await);
    for range in [false, true] {
        let table_id = format!("race-{}", if range { "range" } else { "hash" });
        let key_info = std::sync::Arc::new(table(&engine, &table_id, range));
        let key = item_key(range);
        let mut tasks = Vec::new();
        for i in 0..WRITERS {
            let engine = engine.clone();
            let key_info = key_info.clone();
            let key = key.clone();
            tasks.push(tokio::spawn(async move {
                let actions = vec![UpdateAction::Set {
                    path: path("w"),
                    value: Expr::Placeholder(":v".to_owned()),
                }];
                engine
                    .update_item_impl(
                        &key_info,
                        &key,
                        &actions,
                        false,
                        true,
                        None,
                        &maps(s(&format!("w{i}"))),
                        None,
                    )
                    .await
            }));
        }
        for task in tasks {
            let (old, new) = task
                .await
                .expect("task")
                .unwrap_or_else(|e| panic!("{table_id}: a creating update failed: {e:?}"));
            assert_eq!(old, None, "{table_id}");
            let new = new.expect("ALL_NEW");
            for (name, value) in &key {
                assert_eq!(
                    new.get(name),
                    Some(value),
                    "{table_id}: ALL_NEW has the key"
                );
            }
        }
        let doc = stored(&engine, &key_info, &key).await;
        let item = document_to_item(&doc).expect("item");
        let w = item.get("w").cloned().expect("w was set");
        let mut want = key.clone();
        want.insert("w".to_owned(), w);
        assert_eq!(
            doc,
            expected_document(&want, &key_info, WRITERS),
            "{table_id}"
        );
    }
    let engine = std::sync::Arc::try_unwrap(engine)
        .ok()
        .expect("all tasks done");
    finish(engine).await;
}

/// A creating upsert whose insert always collides on `_id` (another document
/// holds the key text as its `_id` under a different `pk`) exhausts the
/// retry budget and answers `TransactionConflict`, which the server maps to
/// `TransactionConflictException`, not an internal error: the duplicate-key
/// command error `findAndModify` returns is recognised.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_creating_update_that_always_loses_on_id_answers_transaction_conflict() {
    let engine = scratch_engine().await;
    let key_info = table(&engine, "collide-hash", false);
    let key = item_key(false);
    engine
        .data_db
        .collection::<Document>(&data_collection_name(&key_info.table_id))
        .insert_one(doc! { "_id": "u1", "pk": "other", "item_data": { "pk": { "S": "other" } } })
        .await
        .expect("colliding document");
    let actions = vec![UpdateAction::Set {
        path: path("w"),
        value: Expr::Placeholder(":v".to_owned()),
    }];
    let err = engine
        .update_item_impl(
            &key_info,
            &key,
            &actions,
            false,
            true,
            None,
            &maps(s("upd")),
            None,
        )
        .await
        .expect_err("the insert cannot succeed");
    assert!(
        matches!(err, StorageError::TransactionConflict(_)),
        "expected TransactionConflict, got {err:?}"
    );
    finish(engine).await;
}
