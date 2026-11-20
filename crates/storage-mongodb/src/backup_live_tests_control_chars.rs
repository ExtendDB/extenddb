// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB test of an in-catalog backup of an item whose attribute names
//! hold U+0000 and U+0001.
//!
//! The data path stores each field name of `item_data` through the escape of
//! `extenddb_storage::util::escape_json_keys`, and an in-catalog backup is a
//! `$out` copy of the table's collection, so the copy holds the escaped
//! names. `items`, which `extenddb backup migrate-legacy` reads, must return
//! the names as written. The test reads the replica set URI from
//! `EXTENDDB_TEST_MONGODB_URI` and is `#[ignore]`d, like the tests in the
//! parent module.

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeValue, BillingMode, CreateTableInput, Item, KeyType, ScalarAttributeType,
};
use extenddb_storage::{DataEngine, JobPermit, LegacyBackupReader, TableEngine};
use futures::{FutureExt, StreamExt};

use super::{ACCOUNT, Fixture, attr, key};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_legacy_items_return_names_that_hold_u0000_and_u0001_as_written() {
    let fixture = Fixture::new().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let fx = &fixture;
        fx.engine
            .create_table(
                ACCOUNT,
                CreateTableInput {
                    table_name: "names".to_owned(),
                    key_schema: vec![key("pk", KeyType::Hash)],
                    attribute_definitions: vec![attr("pk", ScalarAttributeType::S)],
                    billing_mode: Some(BillingMode::PayPerRequest),
                    ..Default::default()
                },
            )
            .await
            .expect("create the table");
        let key_info = fx
            .engine
            .table_key_info(ACCOUNT, "names")
            .await
            .expect("key info");
        let written: Item = [("pk", "p"), ("c\u{1}", "one"), ("n\u{0}", "two")]
            .into_iter()
            .map(|(name, value)| (name.to_owned(), AttributeValue::S(value.to_owned())))
            .collect();
        let maps = ExpressionMaps::default();
        fx.engine
            .put_item(&key_info, written.clone(), false, None, &maps, None)
            .await
            .expect("put");
        let arn = fx
            .engine
            .create(ACCOUNT, "names", "b", JobPermit::detached())
            .await
            .expect("backup")
            .backup_arn;
        let read: Vec<Item> = fx
            .engine
            .items(ACCOUNT, &arn)
            .await
            .expect("items")
            .items
            .map(|item| item.expect("an item"))
            .collect()
            .await;
        assert_eq!(read, vec![written]);
    })
    .catch_unwind()
    .await;
    fixture.finish_after(outcome).await;
}
