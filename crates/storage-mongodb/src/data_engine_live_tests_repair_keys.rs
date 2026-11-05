// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB tests for the startup repair of documents stored without their
//! key, for key shapes the tests in the parent module do not cover.
//!
//! The tests read the replica set URI from `EXTENDDB_TEST_MONGODB_URI` and are
//! `#[ignore]`d, like the tests in the parent module.

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{AttributeValue, Item, KeyType, ScalarAttributeType, TableKeyInfo};

use super::{
    KeyRepairSummary, def, finish, key, s, scratch_engine, store_without_key, stored, with,
};
use crate::MongoEngine;
use crate::data::{document_to_item, item_to_document};

/// Key info for a table with a string hash key `pk` and a numeric sort key
/// `sk`, with the GSI cache set to "no indexes".
fn numeric_range_table(engine: &MongoEngine, table_id: &str) -> TableKeyInfo {
    engine.gsi_cache_set(table_id, false);
    let key_schema = vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)];
    TableKeyInfo {
        table_id: table_id.to_owned(),
        table_name: table_id.to_owned(),
        key_schema: key_schema.clone(),
        base_key_schema: key_schema,
        attribute_definitions: vec![
            def("pk", ScalarAttributeType::S),
            def("sk", ScalarAttributeType::N),
        ],
        ..Default::default()
    }
}

/// The key `pk = "u1"`, `sk = <number>`, with the number in the plain decimal
/// form a request carries.
fn number_key(number: &str) -> Item {
    let mut key = Item::new();
    key.insert("pk".to_owned(), s("u1"));
    key.insert("sk".to_owned(), AttributeValue::N(number.to_owned()));
    key
}

/// A numeric sort key below 1e-6, which `Decimal128` displays in exponent
/// form (`1E-7`), is repaired into the `_id` that PutItem builds from the
/// plain decimal. A PutItem on each repaired key then succeeds and returns
/// the repaired item as the old image; built on the exponent form, the repair
/// would leave an `_id` that PutItem cannot replace. The values at and above
/// 1e-6 are repaired the same way and show the other side of the switch.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_repair_keeps_a_small_numeric_sort_key_writable() {
    const NUMBERS: [&str; 4] = ["0.0000001", "-0.00000012345", "0.000001", "12.5"];
    let engine = scratch_engine().await;
    let key_info = numeric_range_table(&engine, "repair-small-numbers");
    for number in NUMBERS {
        store_without_key(&engine, &key_info, &number_key(number), Some("upd")).await;
    }

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
            tables_failed: 0,
            documents_repaired: NUMBERS.len() as u64,
            documents_failed: 0,
        }
    );

    for number in NUMBERS {
        let key = number_key(number);
        let repaired = with(key.clone(), "w", s("upd"));
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
            .unwrap_or_else(|e| panic!("{number}: PutItem on the repaired key: {e:?}"));
        assert_eq!(
            old,
            Some(repaired),
            "{number}: PutItem returns the repaired item"
        );

        let after = stored(&engine, &key_info, &key).await;
        let want = item_to_document(&put, &key_info.key_schema, &key_info.attribute_definitions)
            .expect("document");
        assert_eq!(
            after.get("_id"),
            want.get("_id"),
            "{number}: the _id is the one PutItem builds"
        );
        assert_eq!(
            document_to_item(&after).expect("item"),
            put,
            "{number}: PutItem replaced the item"
        );
    }
    finish(engine).await;
}
