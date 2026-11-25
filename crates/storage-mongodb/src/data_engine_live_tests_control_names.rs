// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB test for the field names of a document that a release before
//! the escape of U+0000 wrote. Such a document carries no `names_escaped`
//! mark and holds its names as written, so they read back unchanged: `c\x01`
//! and `c\x01\x02` stay two attributes, and an UpdateItem keeps both, on the
//! session path, which reads the item and writes it back, and on the native
//! path.
//!
//! The test reads the replica set URI from `EXTENDDB_TEST_MONGODB_URI` and is
//! `#[ignore]`d, like the tests in the parent module.

use bson::{Document, doc};
use extenddb_core::expression::{Expr, UpdateAction};

use super::{finish, item_key, maps, path, s, scratch_engine, stored, table, with};
use crate::data::{NAMES_ESCAPED, data_collection_name, document_to_item};

#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_names_an_earlier_release_stored_read_back_and_survive_an_update() {
    let engine = scratch_engine().await;
    let key_info = table(&engine, "legacy-names", false);
    let key = item_key(false);
    engine
        .data_db
        .collection::<Document>(&data_collection_name(&key_info.table_id))
        .insert_one(doc! {
            "_id": "u1",
            "pk": "u1",
            "item_data": {
                "pk": { "S": "u1" },
                "c\u{1}": { "S": "one" },
                "c\u{1}\u{2}": { "S": "two" },
            },
        })
        .await
        .expect("store the document as an earlier release wrote it");
    let written = with(
        with(key.clone(), "c\u{1}", s("one")),
        "c\u{1}\u{2}",
        s("two"),
    );
    assert_eq!(
        engine.get_item_impl(&key_info, &key).await.expect("get"),
        Some(written.clone())
    );

    // ALL_OLD takes the session path; the other update takes the native one.
    let mut want = written;
    for (name, return_old) in [("z", true), ("y", false)] {
        let actions = vec![UpdateAction::Set {
            path: path(name),
            value: Expr::Placeholder(":v".to_owned()),
        }];
        let (old, _) = engine
            .update_item_impl(
                &key_info,
                &key,
                &actions,
                return_old,
                false,
                None,
                &maps(s(name)),
                None,
            )
            .await
            .unwrap_or_else(|e| panic!("update {name}: {e:?}"));
        if return_old {
            assert_eq!(old.as_ref(), Some(&want), "the old image of {name}");
        }
        want = with(want, name, s(name));
        let doc = stored(&engine, &key_info, &key).await;
        assert_eq!(doc.get(NAMES_ESCAPED), None, "after {name}");
        assert_eq!(document_to_item(&doc).expect("item"), want, "after {name}");
    }
    assert_eq!(
        engine.get_item_impl(&key_info, &key).await.expect("get"),
        Some(want)
    );
    finish(engine).await;
}
