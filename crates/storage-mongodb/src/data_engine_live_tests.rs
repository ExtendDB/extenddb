// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB tests for the shape of the document an UpdateItem writes when
//! it creates an item (issue #436), and for the startup repair of documents an
//! earlier build stored without their key.
//!
//! They read the replica set URI from `EXTENDDB_TEST_MONGODB_URI` and are
//! `#[ignore]`d so `cargo test --lib` passes without one; run them with
//! `cargo test -p extenddb-storage-mongodb --lib -- --ignored live_`. Each test
//! uses its own catalog and data databases and drops them at the end.

use std::collections::HashMap;

use bson::{Document, doc};
use extenddb_core::expression::{Expr, ExpressionMaps, PathElement, UpdateAction};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, KeyType, ScalarAttributeType,
    TableKeyInfo,
};

use extenddb_storage::error::StorageError;

use super::key_repair::KeyRepairSummary;
use crate::MongoEngine;
use crate::data::{data_collection_name, document_to_item, item_to_document, pk_filter};

fn uri() -> String {
    std::env::var("EXTENDDB_TEST_MONGODB_URI").expect(
        "set EXTENDDB_TEST_MONGODB_URI to a MongoDB replica set, e.g. \
         mongodb://127.0.0.1:27741/?directConnection=true",
    )
}

/// An engine on fresh catalog and data databases.
async fn scratch_engine() -> MongoEngine {
    let base = MongoEngine::new(
        &uri(),
        "us-east-1",
        None,
        mongodb::options::ReadConcern::snapshot(),
    )
    .await
    .expect("engine");
    let tag = uuid::Uuid::new_v4().simple().to_string();
    MongoEngine {
        catalog_db: base.client.database(&format!("k{}_cat", &tag[..12])),
        data_db: base.client.database(&format!("k{}_data", &tag[..12])),
        ..base
    }
}

async fn finish(engine: MongoEngine) {
    engine.catalog_db.drop().await.expect("drop catalog");
    engine.data_db.drop().await.expect("drop data");
}

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

fn def(name: &str, attribute_type: ScalarAttributeType) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type,
    }
}

/// Key info for a table with a string hash key `pk` and, with `range`, a
/// string sort key `sk`. The GSI cache is set to "no indexes" so an
/// unconditional UpdateItem takes the native fast path, as it does on a
/// served table without indexes once the first write has filled the cache.
fn table(engine: &MongoEngine, table_id: &str, range: bool) -> TableKeyInfo {
    engine.gsi_cache_set(table_id, false);
    let mut key_schema = vec![key("pk", KeyType::Hash)];
    let mut attribute_definitions = vec![def("pk", ScalarAttributeType::S)];
    if range {
        key_schema.push(key("sk", KeyType::Range));
        attribute_definitions.push(def("sk", ScalarAttributeType::S));
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

fn item_key(range: bool) -> Item {
    let mut key = Item::new();
    key.insert("pk".to_owned(), AttributeValue::S("u1".to_owned()));
    if range {
        key.insert("sk".to_owned(), AttributeValue::S("s1".to_owned()));
    }
    key
}

fn with(mut item: Item, name: &str, value: AttributeValue) -> Item {
    item.insert(name.to_owned(), value);
    item
}

fn s(v: &str) -> AttributeValue {
    AttributeValue::S(v.to_owned())
}

fn n(v: &str) -> AttributeValue {
    AttributeValue::N(v.to_owned())
}

fn path(name: &str) -> Vec<PathElement> {
    vec![PathElement::Attribute(name.to_owned())]
}

fn maps(value: AttributeValue) -> ExpressionMaps {
    let mut values = HashMap::new();
    values.insert(":v".to_owned(), value);
    ExpressionMaps::new(HashMap::new(), values)
}

async fn stored(engine: &MongoEngine, key_info: &TableKeyInfo, key: &Item) -> Document {
    let coll = engine
        .data_db
        .collection::<Document>(&data_collection_name(&key_info.table_id));
    let filter =
        pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions).expect("filter");
    coll.find_one(filter)
        .await
        .expect("read back")
        .expect("the item's document exists")
}

/// The document `item_to_document` writes for `item`, with version `v`.
fn expected_document(item: &Item, key_info: &TableKeyInfo, v: i64) -> Document {
    let mut doc = item_to_document(item, &key_info.key_schema, &key_info.attribute_definitions)
        .expect("document");
    doc.insert("_v", v);
    doc
}

/// An UpdateItem that creates the item, on the native fast path (SET with a
/// placeholder, REMOVE) and on the transaction path (numeric ADD), stores the
/// same document PutItem would store: `_id` as the key text, the key
/// attributes in `item_data`, version 1. ALL_NEW returns the item with its
/// key. A PutItem on the key then replaces the item and returns it as the old
/// image.
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
            assert_eq!(
                new,
                Some(want.clone()),
                "{table_id}: ALL_NEW carries the key"
            );
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
            assert_eq!(old, Some(want), "{table_id}: put returns the created item");
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

/// Store a document the way the fast path did before the fix: an upsert whose
/// filter is the key and whose update sets one attribute, or, when `w` is
/// `None`, removes one. MongoDB copies the filter's `pk` and `sk_*` fields,
/// generates an ObjectId `_id`, and writes no key attribute into `item_data`.
/// The `None` form is main's REMOVE-only creating update: `$unset`
/// contributes nothing to the inserted document, so it has no `item_data` at
/// all.
async fn store_without_key(
    engine: &MongoEngine,
    key_info: &TableKeyInfo,
    key: &Item,
    w: Option<&str>,
) {
    let coll = engine
        .data_db
        .collection::<Document>(&data_collection_name(&key_info.table_id));
    let filter =
        pk_filter(key, &key_info.key_schema, &key_info.attribute_definitions).expect("filter");
    let update = match w {
        Some(w) => doc! { "$set": { "item_data.w": { "S": w } }, "$inc": { "_v": 1_i64 } },
        None => doc! { "$unset": { "item_data.w": "" }, "$inc": { "_v": 1_i64 } },
    };
    coll.find_one_and_update(filter, update)
        .upsert(true)
        .await
        .expect("upsert");
}

/// The repair rewrites a document stored without its key into the document
/// PutItem would have written, keeping the attributes and the version. The
/// item then reads with its key and a PutItem on it succeeds. Documents with
/// their key are left alone, and a second run finds nothing.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_repair_rewrites_documents_stored_without_their_key() {
    let engine = scratch_engine().await;
    for range in [false, true] {
        let table_id = format!("repair-{}", if range { "range" } else { "hash" });
        let key_info = table(&engine, &table_id, range);
        let key = item_key(range);
        store_without_key(&engine, &key_info, &key, Some("upd")).await;
        store_without_key(&engine, &key_info, &key, Some("upd2")).await;
        let bad = stored(&engine, &key_info, &key).await;
        assert!(
            bad.get_object_id("_id").is_ok(),
            "{table_id}: the pre-fix shape has an ObjectId _id"
        );
        assert!(
            !document_to_item(&bad).expect("item").contains_key("pk"),
            "{table_id}: and no key"
        );

        // A healthy item in the same collection is not touched.
        let other = with(with(item_key(range), "w", s("fine")), "pk", s("u2"));
        engine
            .put_item_impl(
                &key_info,
                other.clone(),
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .expect("put the healthy item");

        let mut summary = KeyRepairSummary::default();
        engine
            .repair_table(
                &table_id,
                &key_info.key_schema,
                &key_info.attribute_definitions,
                &mut summary,
            )
            .await
            .expect("repair");
        assert_eq!(
            summary,
            KeyRepairSummary {
                tables_scanned: 0,
                documents_repaired: 1,
                documents_failed: 0,
            },
            "{table_id}"
        );

        let want = with(key.clone(), "w", s("upd2"));
        assert_eq!(
            stored(&engine, &key_info, &key).await,
            expected_document(&want, &key_info, 2),
            "{table_id}: repaired document"
        );
        assert_eq!(
            document_to_item(&stored(&engine, &key_info, &other).await).expect("item"),
            other,
            "{table_id}: the healthy item is unchanged"
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
            .unwrap_or_else(|e| panic!("{table_id}: put after repair: {e:?}"));
        assert_eq!(old, Some(want), "{table_id}: put returns the repaired item");

        let mut again = KeyRepairSummary::default();
        engine
            .repair_table(
                &table_id,
                &key_info.key_schema,
                &key_info.attribute_definitions,
                &mut again,
            )
            .await
            .expect("second repair");
        assert!(
            again.is_clean(),
            "{table_id}: nothing left to repair: {again}"
        );
    }
    finish(engine).await;
}

/// A pre-fix document with no `item_data` at all, which is what main's fast
/// path stored for a REMOVE-only creating UpdateItem, reads as an internal
/// error before the repair and as the key-only item after it. The repaired
/// document is the one the fixed fast path writes for the same request, and a
/// PutItem on the key then succeeds.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_repair_rewrites_a_document_stored_with_no_item_data() {
    let engine = scratch_engine().await;
    for range in [false, true] {
        let table_id = format!("repair-noitem-{}", if range { "range" } else { "hash" });
        let key_info = table(&engine, &table_id, range);
        let key = item_key(range);
        store_without_key(&engine, &key_info, &key, None).await;

        let bad = stored(&engine, &key_info, &key).await;
        assert!(
            bad.get_object_id("_id").is_ok(),
            "{table_id}: the pre-fix shape has an ObjectId _id"
        );
        assert!(
            !bad.contains_key("item_data"),
            "{table_id}: a REMOVE-only creating update stored no item_data: {bad:?}"
        );
        assert!(
            engine.get_item_impl(&key_info, &key).await.is_err(),
            "{table_id}: the key reads as an error until the repair runs"
        );

        let mut summary = KeyRepairSummary::default();
        engine
            .repair_table(
                &table_id,
                &key_info.key_schema,
                &key_info.attribute_definitions,
                &mut summary,
            )
            .await
            .expect("repair");
        assert_eq!(
            summary,
            KeyRepairSummary {
                tables_scanned: 0,
                documents_repaired: 1,
                documents_failed: 0,
            },
            "{table_id}"
        );

        assert_eq!(
            stored(&engine, &key_info, &key).await,
            expected_document(&key, &key_info, 1),
            "{table_id}: repaired into the key-only item"
        );
        assert_eq!(
            engine.get_item_impl(&key_info, &key).await.expect("get"),
            Some(key.clone()),
            "{table_id}: the key reads back"
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
            .unwrap_or_else(|e| panic!("{table_id}: put after repair: {e:?}"));
        assert_eq!(
            old,
            Some(key.clone()),
            "{table_id}: put returns the repaired key-only item"
        );
    }
    finish(engine).await;
}

/// Insert `count` documents in the pre-fix shape directly, one per key
/// `u0..u<count>`, with no key attribute in `item_data`. The upsert path in
/// `store_without_key` is one round trip per document, which is too slow for
/// the counts the batched repair needs.
async fn store_many_without_key(engine: &MongoEngine, key_info: &TableKeyInfo, count: usize) {
    let coll = engine
        .data_db
        .collection::<Document>(&data_collection_name(&key_info.table_id));
    let docs: Vec<Document> = (0..count)
        .map(|i| {
            doc! {
                "_id": bson::oid::ObjectId::new(),
                "pk": format!("u{i}"),
                "item_data": { "w": { "S": format!("value-{i}") } },
                "_v": 1_i64,
            }
        })
        .collect();
    coll.insert_many(docs).await.expect("plant");
}

/// A repair pass that spans more than one batch rebuilds every document. The
/// batch is 500 documents, so 1,100 crosses two boundaries and ends with a
/// partial batch. Every key reads back with its value, none is duplicated,
/// and a second pass finds nothing.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_repair_spans_more_than_one_batch() {
    const COUNT: usize = 1_100;
    let engine = scratch_engine().await;
    let key_info = table(&engine, "repair-batches", false);
    store_many_without_key(&engine, &key_info, COUNT).await;

    let mut summary = KeyRepairSummary::default();
    engine
        .repair_table(
            &key_info.table_id,
            &key_info.key_schema,
            &key_info.attribute_definitions,
            &mut summary,
        )
        .await
        .expect("repair");
    assert_eq!(
        summary,
        KeyRepairSummary {
            tables_scanned: 0,
            documents_repaired: COUNT as u64,
            documents_failed: 0,
        }
    );

    let coll = engine
        .data_db
        .collection::<Document>(&data_collection_name(&key_info.table_id));
    assert_eq!(
        coll.count_documents(doc! {}).await.expect("count"),
        COUNT as u64,
        "no document was duplicated or lost"
    );
    assert_eq!(
        coll.count_documents(doc! { "_id": { "$type": "objectId" } })
            .await
            .expect("count"),
        0,
        "every document carries its key text as _id"
    );
    for i in [0_usize, 499, 500, 999, 1_000, COUNT - 1] {
        let mut key = Item::new();
        key.insert("pk".to_owned(), s(&format!("u{i}")));
        assert_eq!(
            engine.get_item_impl(&key_info, &key).await.expect("get"),
            Some(with(key.clone(), "w", s(&format!("value-{i}")))),
            "key u{i} reads back"
        );
    }

    let mut again = KeyRepairSummary::default();
    engine
        .repair_table(
            &key_info.table_id,
            &key_info.key_schema,
            &key_info.attribute_definitions,
            &mut again,
        )
        .await
        .expect("second repair");
    assert!(again.is_clean(), "nothing left to repair: {again}");
    finish(engine).await;
}

/// The whole-catalog entry point finds the table through its catalog row.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_repair_reads_the_key_schema_from_the_catalog() {
    let engine = scratch_engine().await;
    let key_info = table(&engine, "catalog-hash", false);
    engine
        .catalog_db
        .collection::<Document>("tables")
        .insert_one(doc! {
            "_id": { "account_id": "123456789012", "table_name": "t" },
            "table_id": "catalog-hash",
            "table_status": "ACTIVE",
            "key_schema": bson::to_bson(&key_info.key_schema).expect("bson"),
            "attribute_definitions": bson::to_bson(&key_info.attribute_definitions).expect("bson"),
        })
        .await
        .expect("catalog row");
    let key = item_key(false);
    store_without_key(&engine, &key_info, &key, Some("upd")).await;

    let summary = engine
        .repair_items_stored_without_key()
        .await
        .expect("repair");
    assert_eq!(
        summary,
        KeyRepairSummary {
            tables_scanned: 1,
            documents_repaired: 1,
            documents_failed: 0,
        }
    );
    assert_eq!(
        document_to_item(&stored(&engine, &key_info, &key).await).expect("item"),
        with(key, "w", s("upd"))
    );
    finish(engine).await;
}
