// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB tests for an unconditional PutItem whose insert hits the
//! `_id_` unique index: the put's filter on the key finds no item, and
//! another document already holds the item's `_id`. On the sessionless path
//! that is how a put that loses the create race to another put fails. On the
//! transaction path a put that loses the race to another put gets a write
//! conflict instead, and the duplicate-key arm meets only a document that
//! holds the `_id` without matching the key filter, which is the document
//! these tests insert.
//!
//! The tests read the replica set URI from `EXTENDDB_TEST_MONGODB_URI` and are
//! `#[ignore]`d, like the tests in the parent module.

use bson::{Bson, Document, doc};
use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{Item, TableKeyInfo};
use extenddb_storage::error::StorageError;

use super::super::TRANSIENT_RETRY_ATTEMPTS;
use super::{finish, item_key, s, scratch_engine, table, with};
use crate::MongoEngine;
use crate::data::{data_collection_name, document_to_item, item_to_document};

/// Key info for a table with a string hash key `pk`. The GSI cache is set so
/// that an unconditional put takes the sessionless path (`has_gsi` false) or
/// the transaction path (`has_gsi` true). The catalog has no index rows, so
/// the transaction path writes no index entries.
fn hash_table(engine: &MongoEngine, table_id: &str, has_gsi: bool) -> TableKeyInfo {
    let key_info = table(engine, table_id, false);
    engine.gsi_cache_set(table_id, has_gsi);
    key_info
}

/// The item at the key `item_key` builds, with attribute `w` set to `w`.
fn item_with_w(w: &str) -> Item {
    with(item_key(false), "w", s(w))
}

/// The document a put of `item` stores.
fn stored_document(item: &Item, key_info: &TableKeyInfo) -> Document {
    item_to_document(item, &key_info.key_schema, &key_info.attribute_definitions)
        .expect("test item converts to a document")
}

/// The `_id` of the document a put of `item` stores.
fn document_id(item: &Item, key_info: &TableKeyInfo) -> Bson {
    stored_document(item, key_info)
        .get("_id")
        .cloned()
        .expect("document has an _id")
}

/// An unconditional PutItem whose insert keeps colliding on `_id` retries for
/// the whole retry budget, then gives up with the create race error, on the
/// sessionless path and on the transaction path.
///
/// A document that holds the item's `_id` under a different `pk` makes every
/// attempt collide: the put's filter on `pk` finds no item, and its insert
/// raises the duplicate-key error. The obstructing document stays as it was.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_unconditional_put_retries_a_lost_create_race_and_then_gives_up() {
    let engine = scratch_engine().await;
    let maps = ExpressionMaps::default();
    for (table_id, has_gsi) in [("sessionless-put", false), ("transaction-put", true)] {
        let key_info = hash_table(&engine, table_id, has_gsi);
        let item = item_with_w("new");
        let id = document_id(&item, &key_info);
        let coll = engine
            .data_db
            .collection::<Document>(&data_collection_name(table_id));
        coll.insert_one(doc! { "_id": id.clone(), "pk": "another-key", "item_data": {} })
            .await
            .expect("insert the obstructing document");

        let res = engine
            .put_item_impl(&key_info, item, true, None, &maps, None)
            .await;

        let want = format!("could not create the item after {TRANSIENT_RETRY_ATTEMPTS} attempts");
        match res {
            Err(StorageError::Internal(msg)) => assert!(
                msg.contains(&want),
                "{table_id}: want the create race error, got: {msg}"
            ),
            other => panic!("{table_id}: want the create race error, got: {other:?}"),
        }
        assert_eq!(
            coll.count_documents(doc! {}).await.expect("count"),
            1,
            "{table_id}: the put wrote a document"
        );
        let left = coll
            .find_one(doc! { "_id": id })
            .await
            .expect("read back")
            .expect("obstructing document is still there");
        assert_eq!(left.get_str("pk").expect("pk"), "another-key", "{table_id}");
    }
    finish(engine).await;
}

/// What happens to the obstructing document while the put retries.
#[derive(Clone, Copy, Debug)]
enum Obstruction {
    /// It is replaced, under the same `_id`, with the item of another put
    /// that created the key.
    BecomesTheOtherPutsItem,
    /// It is deleted, as a DeleteItem removes the item another put created.
    IsDeleted,
}

/// An unconditional PutItem whose first attempts collide on `_id` succeeds on
/// a later attempt, on the sessionless path and on the transaction path.
///
/// The put starts while a document holds the item's `_id` under another
/// `pk`, so its first attempts collide. 100 ms later another task either
/// replaces that document with the item of another put on the same key, or
/// deletes it. The put's attempts back off between collisions and together
/// take longer than 100 ms, so a later attempt either replaces the other
/// put's item and returns it as the old image, or inserts and returns no old
/// image. In both cases the stored document is this put's item.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_unconditional_put_that_lost_a_create_race_succeeds_on_a_later_attempt() {
    let engine = scratch_engine().await;
    let maps = ExpressionMaps::default();
    for (path, has_gsi) in [("sessionless", false), ("transaction", true)] {
        for change in [Obstruction::BecomesTheOtherPutsItem, Obstruction::IsDeleted] {
            let table_id = format!("{path}-{change:?}");
            let key_info = hash_table(&engine, &table_id, has_gsi);
            let item = item_with_w("this-put");
            let other_put = item_with_w("other-put");
            let other_put_doc = stored_document(&other_put, &key_info);
            let id = document_id(&other_put, &key_info);
            let coll = engine
                .data_db
                .collection::<Document>(&data_collection_name(&table_id));
            coll.insert_one(doc! { "_id": id.clone(), "pk": "another-key", "item_data": {} })
                .await
                .expect("insert the obstructing document");

            let put = engine.put_item_impl(&key_info, item.clone(), true, None, &maps, None);
            let clear = async {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                match change {
                    Obstruction::BecomesTheOtherPutsItem => {
                        coll.replace_one(doc! { "_id": id.clone() }, other_put_doc.clone())
                            .await
                            .expect("replace the obstructing document");
                    }
                    Obstruction::IsDeleted => {
                        coll.delete_one(doc! { "_id": id.clone() })
                            .await
                            .expect("delete the obstructing document");
                    }
                }
            };
            let (res, ()) = tokio::join!(put, clear);

            let old = match res {
                Ok(old) => old,
                Err(e) => panic!("{table_id}: want the put to succeed, got: {e:?}"),
            };
            let want_old = match change {
                Obstruction::BecomesTheOtherPutsItem => Some(other_put),
                Obstruction::IsDeleted => None,
            };
            assert_eq!(old, want_old, "{table_id}: old image");
            assert_eq!(
                coll.count_documents(doc! {}).await.expect("count"),
                1,
                "{table_id}: documents in the collection"
            );
            let stored = coll
                .find_one(doc! { "_id": id })
                .await
                .expect("read back")
                .expect("the put's document is stored");
            assert_eq!(
                document_to_item(&stored).expect("stored document converts to an item"),
                item,
                "{table_id}: stored item"
            );
        }
    }
    finish(engine).await;
}
