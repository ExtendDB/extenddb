// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB tests for the pass that finds the documents to repair: how it
//! reads each table, how it goes on after a table it cannot read, and the task
//! that `serve` starts to run it.
//!
//! The tests read the replica set URI from `EXTENDDB_TEST_MONGODB_URI` and are
//! `#[ignore]`d, like the tests in the parent module.

use std::sync::Arc;

use bson::{Bson, Document, doc};
use extenddb_core::types::TableKeyInfo;
use futures::TryStreamExt;

use super::{
    KeyRepairSummary, expected_document, finish, item_key, s, scratch_engine,
    store_many_without_key, store_without_key, stored, table, with,
};
use crate::data::{data_collection_name, document_to_item};
use crate::data_engine::key_repair::spawn_key_repair;

/// A catalog `tables` row for `table_id` with the key schema of `key_info`.
fn catalog_row(table_name: &str, table_id: &str, key_info: &TableKeyInfo) -> Document {
    doc! {
        "_id": { "account_id": "123456789012", "table_name": table_name },
        "table_id": table_id,
        "table_status": "ACTIVE",
        "key_schema": bson::to_bson(&key_info.key_schema).expect("bson"),
        "attribute_definitions": bson::to_bson(&key_info.attribute_definitions).expect("bson"),
    }
}

/// An integer field of a profiler entry, which MongoDB writes as int32 or
/// int64.
fn count(entry: &Document, field: &str) -> i64 {
    match entry.get(field) {
        Some(Bson::Int32(n)) => i64::from(*n),
        Some(Bson::Int64(n)) => *n,
        other => panic!("{field} in {entry:?} is {other:?}, not an integer"),
    }
}

/// A scan that stops on an error does not stop the pass. The first catalog
/// row carries a table id with a `$`, which MongoDB refuses in a collection
/// name, so the scan of its data collection fails. The second row is an
/// ordinary table with one document stored without its key. The pass counts
/// the first table as stopped by an error, repairs the second, and returns Ok.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_repair_goes_on_to_the_next_table_when_a_scan_fails() {
    let engine = scratch_engine().await;
    let key_info = table(&engine, "pass-good", false);
    let key = item_key(false);
    let tables = engine.catalog_db.collection::<Document>("tables");
    tables
        .insert_one(catalog_row("unreadable", "pass-bad$id", &key_info))
        .await
        .expect("catalog row of the table that cannot be scanned");
    tables
        .insert_one(catalog_row("readable", &key_info.table_id, &key_info))
        .await
        .expect("catalog row of the readable table");
    store_without_key(&engine, &key_info, &key, Some("upd")).await;

    let summary = engine
        .repair_items_stored_without_key()
        .await
        .expect("a table that cannot be scanned does not fail the pass");
    assert_eq!(
        summary,
        KeyRepairSummary {
            tables_scanned: 2,
            tables_failed: 1,
            documents_repaired: 1,
            documents_failed: 0,
        }
    );
    assert!(!summary.is_clean(), "a stopped scan is not a clean pass");
    assert_eq!(
        document_to_item(&stored(&engine, &key_info, &key).await).expect("item"),
        with(key, "w", s("upd")),
        "the table after the failing one was repaired"
    );
    finish(engine).await;
}

/// The scan reads `_id` only. With the projection the `_id` index answers
/// it, and the server's profiler records that the find returned every
/// document it found and examined none; without the projection it examines
/// each one to return its fields.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_repair_scan_is_answered_from_the_id_index() {
    const DOCUMENTS: usize = 20;
    let engine = scratch_engine().await;
    let key_info = table(&engine, "pass-scan", false);
    store_many_without_key(&engine, &key_info, DOCUMENTS).await;

    engine
        .data_db
        .run_command(doc! { "profile": 2 })
        .await
        .expect("profiler on");
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
    engine
        .data_db
        .run_command(doc! { "profile": 0 })
        .await
        .expect("profiler off");
    assert_eq!(summary.documents_repaired, DOCUMENTS as u64);

    let namespace = format!(
        "{}.{}",
        engine.data_db.name(),
        data_collection_name(&key_info.table_id)
    );
    let queries: Vec<Document> = engine
        .data_db
        .collection::<Document>("system.profile")
        .find(doc! { "ns": &namespace, "op": "query" })
        .await
        .expect("profile entries")
        .try_collect()
        .await
        .expect("profile entries");
    // The scan filters on the `_id` type; the batch transaction's own read
    // filters on a list of ids.
    let scans: Vec<&Document> = queries
        .iter()
        .filter(|entry| {
            entry
                .get_document("command")
                .and_then(|command| command.get_document("filter"))
                .and_then(|filter| filter.get_document("_id"))
                .is_ok_and(|id| id.contains_key("$type"))
        })
        .collect();
    assert_eq!(scans.len(), 1, "one scan of {namespace}: {queries:?}");
    let scan = scans[0];
    assert_eq!(count(scan, "nreturned"), DOCUMENTS as i64, "{scan:?}");
    assert_eq!(
        count(scan, "docsExamined"),
        0,
        "the scan is answered from the _id index: {scan:?}"
    );
    finish(engine).await;
}

/// `spawn_key_repair`, the task `serve` starts (`lib.rs`), runs the whole
/// pass: with a catalog table whose collection holds a document stored
/// without its key, the document is rebuilt by the time the task finishes,
/// and the task does finish. The other tests call `repair_table` and
/// `repair_items_stored_without_key` directly.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_the_spawned_key_repair_rewrites_documents_stored_without_their_key() {
    let engine = scratch_engine().await;
    let key_info = table(&engine, "pass-spawned", false);
    let key = item_key(false);
    engine
        .catalog_db
        .collection::<Document>("tables")
        .insert_one(catalog_row("spawned", &key_info.table_id, &key_info))
        .await
        .expect("catalog row");
    store_without_key(&engine, &key_info, &key, Some("upd")).await;
    assert!(
        stored(&engine, &key_info, &key)
            .await
            .get_object_id("_id")
            .is_ok(),
        "planted in the shape the pre-fix upsert stored"
    );

    let engine = Arc::new(engine);
    spawn_key_repair(engine.clone())
        .await
        .expect("the repair task finishes");

    let want = with(key.clone(), "w", s("upd"));
    assert_eq!(
        stored(&engine, &key_info, &key).await,
        expected_document(&want, &key_info, 1),
        "the task repaired the document"
    );
    let engine = Arc::try_unwrap(engine)
        .ok()
        .expect("the finished task dropped its engine");
    finish(engine).await;
}
