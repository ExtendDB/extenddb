// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB tests for UpdateItem calls that create an item on the
//! transaction path. A numeric ADD takes it: the call finds no item inside a
//! snapshot transaction and inserts with a `replace_one` upsert.
//!
//! The tests read the replica set URI from `EXTENDDB_TEST_MONGODB_URI` and are
//! `#[ignore]`d, like the tests in the parent module.

use std::sync::Arc;

use bson::{Document, doc};
use extenddb_core::expression::{Expr, UpdateAction};
use extenddb_storage::error::StorageError;

use super::{
    expected_document, finish, item_key, maps, n, path, scratch_engine, stored, table, with,
};
use crate::data::data_collection_name;

fn add_one() -> Vec<UpdateAction> {
    vec![UpdateAction::Add {
        path: path("c"),
        value: Expr::Placeholder(":v".to_owned()),
    }]
}

/// Eight numeric ADD calls creating the same item at once all apply. Each
/// finds no item, and the creators that lose to the first insert get a write
/// conflict from MongoDB, which the engine retries: the retry reads the
/// winner's item and adds to it. The stored `c` is the sum and `_v` counts
/// every call.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_concurrent_numeric_adds_that_create_one_item_all_apply() {
    const WRITERS: i64 = 8;
    let engine = Arc::new(scratch_engine().await);
    let key_info = Arc::new(table(&engine, "race-add", false));
    let key = item_key(false);
    let mut tasks = Vec::new();
    for _ in 0..WRITERS {
        let engine = engine.clone();
        let key_info = key_info.clone();
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            engine
                .update_item_impl(
                    &key_info,
                    &key,
                    &add_one(),
                    false,
                    true,
                    None,
                    &maps(n("1")),
                    None,
                )
                .await
        }));
    }
    for task in tasks {
        task.await
            .expect("task")
            .unwrap_or_else(|e| panic!("a creating ADD failed: {e:?}"));
    }
    let want = with(key.clone(), "c", n(&WRITERS.to_string()));
    assert_eq!(
        stored(&engine, &key_info, &key).await,
        expected_document(&want, &key_info, WRITERS)
    );
    let engine = Arc::try_unwrap(engine).ok().expect("all tasks done");
    finish(engine).await;
}

/// A creating numeric ADD whose `_id` is already held by a document with a
/// different `pk` fails with the duplicate-key error, and the call does not
/// run again. The other document is visible in the transaction's snapshot,
/// so every rerun would meet it again; a creator that loses a race for the
/// key is a write conflict instead, which the test above covers.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_creating_add_that_collides_on_id_answers_the_duplicate_key_error() {
    let engine = scratch_engine().await;
    let key_info = table(&engine, "collide-add", false);
    let key = item_key(false);
    engine
        .data_db
        .collection::<Document>(&data_collection_name(&key_info.table_id))
        .insert_one(doc! { "_id": "u1", "pk": "other", "item_data": { "pk": { "S": "other" } } })
        .await
        .expect("colliding document");

    let err = engine
        .update_item_impl(
            &key_info,
            &key,
            &add_one(),
            false,
            true,
            None,
            &maps(n("1")),
            None,
        )
        .await
        .expect_err("the insert cannot succeed");
    match err {
        StorageError::Internal(message) => assert!(
            message.contains("E11000 duplicate key error"),
            "expected the server's duplicate-key error, got {message}"
        ),
        other => panic!("expected Internal with the duplicate-key error, got {other:?}"),
    }
    finish(engine).await;
}
