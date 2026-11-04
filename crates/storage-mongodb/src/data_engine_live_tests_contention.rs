// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB tests for writes that lose to concurrent writers on one item:
//! puts on one key of a table with a GSI, and the two bounds that end a
//! contended write, the engine's contention deadline and the attempt budget.
//!
//! Each test sets the engine's deadline, so a test decides which bound a
//! contended write reaches instead of racing 1,000 attempts against a wall
//! clock. The tests read the replica set URI from `EXTENDDB_TEST_MONGODB_URI`
//! and are `#[ignore]`d, like the tests in the parent module.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bson::{Document, doc};
use extenddb_core::expression::{Expr, ExpressionMaps, PathElement, UpdateAction};
use extenddb_core::types::{
    AttributeValue, Item, KeySchemaElement, KeyType, Projection, ProjectionType,
    ReturnValuesOnConditionCheckFailure, TableKeyInfo,
};
use extenddb_storage::error::StorageError;

use super::super::{CONTENTION_DEADLINE, OwnedTransactWriteOp};
use super::{finish, scratch_engine, table};
use crate::MongoEngine;
use crate::data::{data_collection_name, document_to_item, item_to_document};

/// An engine on fresh databases with its contention deadline set to
/// `contention_deadline`. A deadline below the time the attempt budget takes
/// makes the deadline the bound a contended write reaches; one far above it
/// makes the attempt budget the bound.
async fn engine_with_deadline(contention_deadline: Duration) -> MongoEngine {
    MongoEngine {
        contention_deadline,
        ..scratch_engine().await
    }
}

/// Key info for a table with a string hash key `pk`, with the GSI cache set
/// so that every write takes the transaction path. The catalog has no index
/// rows unless a test inserts one, so the writes write no index entries.
fn gsi_path_table(engine: &MongoEngine, table_id: &str) -> TableKeyInfo {
    let key_info = table(engine, table_id, false);
    engine.gsi_cache_set(table_id, true);
    key_info
}

fn item_with(pk: &str, w: &str) -> Item {
    let mut item = Item::new();
    item.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
    item.insert("w".to_owned(), AttributeValue::S(w.to_owned()));
    item
}

/// The key of `item_with(pk, ...)`.
fn hash_key(pk: &str) -> Item {
    let mut key = Item::new();
    key.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
    key
}

fn stored_document(item: &Item, key_info: &TableKeyInfo) -> Document {
    item_to_document(item, &key_info.key_schema, &key_info.attribute_definitions)
        .expect("test item converts to a document")
}

/// `SET w = :w` with its placeholder, for an UpdateItem under contention.
fn set_w(value: &str) -> (Vec<UpdateAction>, ExpressionMaps) {
    let actions = vec![UpdateAction::Set {
        path: vec![PathElement::Attribute("w".to_owned())],
        value: Expr::Placeholder(":w".to_owned()),
    }];
    let maps = ExpressionMaps::new(
        std::collections::HashMap::new(),
        std::collections::HashMap::from([(":w".to_owned(), AttributeValue::S(value.to_owned()))]),
    );
    (actions, maps)
}

/// Hold `pk` of the table inside a transaction that is never committed, so
/// every attempt of a contended write on that key conflicts. The caller
/// aborts the returned session when it is done.
async fn hold_item_in_a_transaction(
    engine: &MongoEngine,
    key_info: &TableKeyInfo,
    pk: &str,
) -> mongodb::ClientSession {
    let coll = engine
        .data_db
        .collection::<Document>(&data_collection_name(&key_info.table_id));
    coll.insert_one(stored_document(&item_with(pk, "held"), key_info))
        .await
        .expect("insert the item to hold");
    let mut holder = engine.client.start_session().await.expect("session");
    holder
        .start_transaction()
        .with_options(engine.write_transaction_options())
        .await
        .expect("start the holding transaction");
    coll.replace_one(
        doc! { "pk": pk },
        stored_document(&item_with(pk, "rewritten"), key_info),
    )
    .session(&mut holder)
    .await
    .expect("write the item inside the held transaction");
    holder
}

/// The item stored under `pk`, read outside any transaction.
async fn stored_item(engine: &MongoEngine, key_info: &TableKeyInfo, pk: &str) -> Option<Item> {
    engine
        .data_db
        .collection::<Document>(&data_collection_name(&key_info.table_id))
        .find_one(doc! { "pk": pk })
        .await
        .expect("read")
        .map(|d| document_to_item(&d).expect("stored document converts to an item"))
}

/// Concurrent unconditional puts on one key of a table with a GSI all commit:
/// 32 writers put the same key 8 times each, every put returns Ok, and the
/// index holds the final item's entry only. On the service every one of these
/// puts answers 200; the backend reaches that through its retry budget.
///
/// The table has a GSI row in the catalog, so `sync_indexes_in_session`
/// writes an index entry per put and keeps the GSI cache at `true`. Without
/// the row the first committed put flips the cache to `false`, the other
/// writers take the sessionless path, and a sessionless findAndModify on a
/// document an open transaction has written waits for that transaction; with
/// 32 writers starting transactions without pause the run stalled for
/// minutes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_concurrent_unconditional_puts_on_one_key_of_a_gsi_table_all_commit() {
    const WRITERS: usize = 32;
    const PUTS: usize = 8;
    let engine = Arc::new(engine_with_deadline(CONTENTION_DEADLINE).await);
    let key_info = Arc::new(gsi_path_table(&engine, "hot-key"));
    let gsi_key_schema = vec![KeySchemaElement {
        attribute_name: "gk".to_owned(),
        key_type: KeyType::Hash,
    }];
    let projection = Projection {
        projection_type: ProjectionType::All,
        non_key_attributes: None,
    };
    engine
        .catalog_db
        .collection::<Document>("indexes")
        .insert_one(doc! {
            "_id": { "table_id": "hot-key", "index_name": "gk-index" },
            "index_id": "hot-key-gk-index",
            "index_type": "GSI",
            "key_schema": bson::to_bson(&gsi_key_schema).expect("key schema"),
            "projection": bson::to_bson(&projection).expect("projection"),
            "index_status": "ACTIVE",
        })
        .await
        .expect("insert the GSI catalog row");
    let start = Arc::new(tokio::sync::Barrier::new(WRITERS));
    let mut tasks = Vec::with_capacity(WRITERS);
    for w in 0..WRITERS {
        let (engine, key_info, start) = (engine.clone(), key_info.clone(), start.clone());
        tasks.push(tokio::spawn(async move {
            let maps = ExpressionMaps::default();
            start.wait().await;
            let mut failures = Vec::new();
            for n in 0..PUTS {
                let mut item = item_with("k1", &format!("w{w}-{n}"));
                item.insert("gk".to_owned(), AttributeValue::S(format!("g-w{w}-{n}")));
                if let Err(e) = engine
                    .put_item_impl(&key_info, item, false, None, &maps, None)
                    .await
                {
                    failures.push(format!("{e:?}"));
                }
            }
            failures
        }));
    }
    let mut failures = Vec::new();
    for task in tasks {
        failures.extend(task.await.expect("writer task"));
    }
    assert!(
        failures.is_empty(),
        "{} of {} puts failed: {:?}",
        failures.len(),
        WRITERS * PUTS,
        &failures[..failures.len().min(3)]
    );
    let coll = engine
        .data_db
        .collection::<Document>(&data_collection_name("hot-key"));
    assert_eq!(coll.count_documents(doc! {}).await.expect("count"), 1);
    let stored = stored_item(&engine, &key_info, "k1")
        .await
        .expect("the item");
    let idx_coll = engine
        .data_db
        .collection::<Document>(&data_collection_name("hot-key-gk-index"));
    use futures::TryStreamExt as _;
    let entries: Vec<Document> = idx_coll
        .find(doc! {})
        .await
        .expect("read the index")
        .try_collect()
        .await
        .expect("collect the index");
    assert_eq!(entries.len(), 1, "index entries: {entries:?}");
    let entry = document_to_item(&entries[0]).expect("index entry converts to an item");
    assert_eq!(
        entry.get("gk"),
        stored.get("gk"),
        "index entry {entry:?} for {stored:?}"
    );
    let engine = Arc::try_unwrap(engine).ok().expect("all writers done");
    finish(engine).await;
}

/// Each single-item write that reaches the engine's contention deadline gives
/// up with the transient error the engine answers as HTTP 503
/// `ServiceUnavailable`, and the message names the operation and the deadline
/// that passed.
///
/// The deadline is 2 s here, below the 9.5 to 12.7 s the 1,000 attempts take
/// against a document a transaction holds, so the deadline is the bound each
/// of the three writes reaches, with no race between the two. The attempt
/// budget is the bound in
/// `live_unconditional_put_that_spends_its_attempt_budget_gives_up_with_a_transient_error`.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_single_item_write_that_reaches_the_deadline_answers_a_transient_error() {
    let deadline = Duration::from_secs(2);
    let engine = engine_with_deadline(deadline).await;
    let maps = ExpressionMaps::default();
    let key_info = gsi_path_table(&engine, "deadline-key");
    let mut holder = hold_item_in_a_transaction(&engine, &key_info, "k1").await;
    let key = hash_key("k1");
    let (actions, update_maps) = set_w("this-update");

    for operation in ["PutItem", "DeleteItem", "UpdateItem"] {
        let started = Instant::now();
        let res = match operation {
            "PutItem" => engine
                .put_item_impl(
                    &key_info,
                    item_with("k1", "this-put"),
                    false,
                    None,
                    &maps,
                    None,
                )
                .await
                .map(|_| ()),
            "DeleteItem" => engine
                .delete_item_impl(&key_info, &key, false, None, &maps, None)
                .await
                .map(|_| ()),
            _ => engine
                .update_item_impl(
                    &key_info,
                    &key,
                    &actions,
                    false,
                    false,
                    None,
                    &update_maps,
                    None,
                )
                .await
                .map(|_| ()),
        };
        let elapsed = started.elapsed();
        match res {
            Err(StorageError::Transient(msg)) => {
                assert_eq!(
                    msg,
                    format!(
                        "{operation}: the write lost to concurrent writers for 2 seconds; \
                         retry the request"
                    )
                );
                println!("{operation} gave up on the deadline in {elapsed:?}: {msg}");
            }
            other => panic!("{operation}: want the transient deadline error, got: {other:?}"),
        }
        assert!(
            elapsed >= deadline,
            "{operation} answered in {elapsed:?}, before the deadline"
        );
        assert!(
            elapsed < deadline + Duration::from_secs(5),
            "{operation} took {elapsed:?}, so a bound other than the deadline ended it"
        );
    }

    let _ = holder.abort_transaction().await;
    // No give-up wrote anything: the held item is as it was inserted.
    assert_eq!(
        stored_item(&engine, &key_info, "k1").await,
        Some(item_with("k1", "held"))
    );
    finish(engine).await;
}

/// A write inside a request clock scope measures the deadline from the
/// scope's start, which is how the write requests of one `BatchWriteItem`
/// share one deadline.
///
/// The scope here started 5 s ago and the deadline is 2 s, so a put of the
/// held item makes its first attempt and gives up before a second one,
/// instead of retrying for 2 s from its own start. A put of a free item in the
/// same scope commits, because the first attempt of a write always runs.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_write_in_a_request_scope_measures_the_deadline_from_the_scope_start() {
    let deadline = Duration::from_secs(2);
    let engine = engine_with_deadline(deadline).await;
    let maps = ExpressionMaps::default();
    let key_info = gsi_path_table(&engine, "deadline-scope");
    let mut holder = hold_item_in_a_transaction(&engine, &key_info, "k1").await;
    let request_started = Instant::now()
        .checked_sub(Duration::from_secs(5))
        .expect("the process clock reaches five seconds back");

    let started = Instant::now();
    let (held, free) = extenddb_storage::request_clock::scope(request_started, async {
        let held = engine
            .put_item_impl(
                &key_info,
                item_with("k1", "this-put"),
                false,
                None,
                &maps,
                None,
            )
            .await;
        let free = engine
            .put_item_impl(
                &key_info,
                item_with("k2", "this-put"),
                false,
                None,
                &maps,
                None,
            )
            .await;
        (held, free)
    })
    .await;
    let elapsed = started.elapsed();
    let _ = holder.abort_transaction().await;

    match held {
        Err(StorageError::Transient(msg)) => assert_eq!(
            msg,
            "PutItem: the write lost to concurrent writers for 2 seconds; retry the request"
        ),
        other => panic!("want the transient deadline error, got: {other:?}"),
    }
    assert!(free.is_ok(), "the put of the free item: {free:?}");
    assert!(
        elapsed < Duration::from_secs(1),
        "the two puts took {elapsed:?}, so the held put measured a deadline of its own"
    );
    assert_eq!(
        stored_item(&engine, &key_info, "k1").await,
        Some(item_with("k1", "held"))
    );
    assert_eq!(
        stored_item(&engine, &key_info, "k2").await,
        Some(item_with("k2", "this-put"))
    );
    finish(engine).await;
}

/// A `TransactWriteItems` that reaches the deadline answers a canceled
/// transaction with a `TransactionConflict` reason per item, which is what
/// live DynamoDB answers a transaction that loses to contention (107 of 160
/// calls at 16 writers on one key). It does not answer the HTTP 503 the
/// single-item writes answer.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_a_transaction_that_reaches_the_deadline_answers_a_canceled_transaction() {
    let deadline = Duration::from_secs(2);
    let engine = engine_with_deadline(deadline).await;
    let key_info = gsi_path_table(&engine, "deadline-transact");
    let mut holder = hold_item_in_a_transaction(&engine, &key_info, "k1").await;

    // Two operations, one on the held key and one on a free key, so the
    // transaction conflicts and the answer carries one reason per operation
    // rather than one for the contended item.
    let ops = vec![
        OwnedTransactWriteOp::Put {
            key_info: key_info.clone(),
            item: item_with("k1", "this-transaction"),
            condition: None,
            maps: ExpressionMaps::default(),
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
        OwnedTransactWriteOp::Put {
            key_info: key_info.clone(),
            item: item_with("k2", "this-transaction"),
            condition: None,
            maps: ExpressionMaps::default(),
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
    ];

    let started = Instant::now();
    let res = engine.transact_write_items_impl(&ops, None).await;
    let elapsed = started.elapsed();
    let _ = holder.abort_transaction().await;

    match res {
        Err(StorageError::TransactionCanceled(reasons)) => {
            assert_eq!(reasons.len(), ops.len(), "one reason per operation");
            for reason in &reasons {
                assert_eq!(reason.code, "TransactionConflict");
                assert_eq!(
                    reason.message.as_deref(),
                    Some("Transaction is ongoing for the item")
                );
            }
            println!("the transaction was canceled in {elapsed:?}: {reasons:?}");
        }
        other => panic!("want a canceled transaction, got: {other:?}"),
    }
    assert!(
        elapsed >= deadline,
        "the transaction answered in {elapsed:?}, before the deadline"
    );
    assert!(
        elapsed < deadline + Duration::from_secs(5),
        "took {elapsed:?}, so a bound other than the deadline ended it"
    );

    // Neither operation of the canceled transaction was applied.
    assert_eq!(
        stored_item(&engine, &key_info, "k1").await,
        Some(item_with("k1", "held"))
    );
    assert_eq!(
        stored_item(&engine, &key_info, "k2").await,
        None,
        "the free key of the canceled transaction must not be written"
    );
    finish(engine).await;
}

/// An unconditional PutItem that spends its whole attempt budget on
/// contention gives up with the transient error naming the attempts, which
/// the engine answers as HTTP 503.
///
/// The deadline is set far above the time the budget takes (9.5 to 12.7 s
/// against a document a transaction holds, over nine runs on a shared host),
/// so the attempt budget is the bound this test reaches. With the production
/// deadline of 10 s either bound can fire first, which is why the deadline is
/// the test's input rather than its race.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_unconditional_put_that_spends_its_attempt_budget_gives_up_with_a_transient_error() {
    let engine = engine_with_deadline(Duration::from_secs(600)).await;
    let maps = ExpressionMaps::default();
    let key_info = gsi_path_table(&engine, "held-key");
    let mut holder = hold_item_in_a_transaction(&engine, &key_info, "k1").await;

    let started = Instant::now();
    let res = engine
        .put_item_impl(
            &key_info,
            item_with("k1", "this-put"),
            false,
            None,
            &maps,
            None,
        )
        .await;
    let elapsed = started.elapsed();
    let _ = holder.abort_transaction().await;

    match res {
        Err(StorageError::Transient(msg)) => {
            // The budget is written out rather than read from
            // `TRANSIENT_RETRY_ATTEMPTS`, so a change to the constant fails
            // here instead of changing what the test expects.
            assert_eq!(
                msg,
                "PutItem: the write lost 1000 attempts to concurrent writers; retry the request"
            );
            println!("the put spent its attempt budget in {elapsed:?}: {msg}");
        }
        other => panic!("want the transient spent-budget error, got: {other:?}"),
    }
    // The loop made those attempts. `backoff_sleep` sleeps a uniform draw
    // from 0 to 12.8 ms before all but the first eight attempts, so the
    // sleeps of 1,000 attempts alone come to about 6.4 s (standard deviation
    // about 0.12 s), before the 1,000 lock waits. A budget of 50 attempts
    // sleeps about 0.3 s, and the create-race budget is also 50, so a loop
    // that one of those ended answers well before this floor.
    assert!(
        elapsed >= Duration::from_secs(5),
        "the put gave up in {elapsed:?}, too soon for 1,000 attempts"
    );
    // The 1,000 attempts take 9.5 to 12.7 s on a shared host, and the sleeps
    // alone come to 12.8 s at worst, so the upper bound is loose: what it
    // rules out is a budget that no longer ends the loop at all.
    assert!(elapsed < Duration::from_secs(120), "took {elapsed:?}");
    assert_eq!(
        stored_item(&engine, &key_info, "k1").await,
        Some(item_with("k1", "held"))
    );
    finish(engine).await;
}
