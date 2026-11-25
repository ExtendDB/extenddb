// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Live MongoDB tests of DeleteTable's tags (issue #604): the table's tags
//! stay readable for as long as its catalog document says DELETING, and go
//! with the document.

use bson::doc;
use extenddb_core::types::{
    CreateTableInput, DeleteTableInput, KeyType, ScalarAttributeType, TableStatus, Tag,
};
use extenddb_storage::{MetadataEngine, TableEngine};

use super::{def, finish, key, scratch_engine};

const ACCOUNT: &str = "111111111111";

/// A DeleteTable that stops after the collection drops, while the document
/// says DELETING, leaves the tags in place; DeleteTable run again removes the
/// document and the tags together, and a table created again under the name
/// has none. The stop is made by turning `stream_shards` into a view, so the
/// stream cleanup's `delete_many` on it fails.
#[tokio::test]
#[ignore = "needs a MongoDB replica set in EXTENDDB_TEST_MONGODB_URI"]
async fn live_delete_table_keeps_tags_while_deleting() {
    let engine = scratch_engine().await;
    let tags = vec![Tag {
        key: "team".to_owned(),
        value: "a".to_owned(),
    }];
    let input = || CreateTableInput {
        table_name: "t".to_owned(),
        key_schema: vec![key("pk", KeyType::Hash)],
        attribute_definitions: vec![def("pk", ScalarAttributeType::S)],
        ..Default::default()
    };
    let desc = engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                tags: Some(tags.clone()),
                ..input()
            },
        )
        .await
        .expect("create");
    let arn = desc.table_arn.clone();
    let delete = || {
        engine.delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: "t".to_owned(),
            },
        )
    };

    let shards = engine.data_db.collection::<bson::Document>("stream_shards");
    shards.drop().await.expect("drop stream_shards");
    engine
        .data_db
        .run_command(doc! { "create": "stream_shards", "viewOn": "none", "pipeline": [] })
        .await
        .expect("stream_shards view");
    delete()
        .await
        .expect_err("the stream cleanup fails on a view");
    let deleting = engine
        .describe_table_impl(ACCOUNT, "t")
        .await
        .expect("the document is still there");
    assert_eq!(deleting.table_status, TableStatus::Deleting);
    assert_eq!(
        engine.list_tags(&arn).await.expect("list"),
        tags,
        "the tags are readable while the table is DELETING"
    );

    shards.drop().await.expect("drop the view");
    delete().await.expect("delete");
    assert!(engine.describe_table_impl(ACCOUNT, "t").await.is_err());
    assert_eq!(engine.list_tags(&arn).await.expect("list"), vec![]);
    engine.create_table(ACCOUNT, input()).await.expect("again");
    assert_eq!(
        engine.list_tags(&arn).await.expect("list"),
        vec![],
        "the table created again does not inherit the tags"
    );
    finish(engine).await;
}
