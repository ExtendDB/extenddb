// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `UpdateTimeToLive`, the TTL index build, and the engine's tag and TTL
//! handlers while the table document is `DELETING`. The replica set and the
//! fixture come from `deleting_window_tests.rs`.

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::TimeToLiveStatus;
use serde_json::json;

use super::*;

impl Fixture {
    /// Drop `TABLE`'s data collection, as a delete call does after it marks
    /// the document `DELETING` and before it removes the document, and return
    /// the collection's name.
    async fn drop_data_collection(&self) -> String {
        let doc = self
            .engine
            .catalog_db
            .collection::<Document>("tables")
            .find_one(doc! { "_id": { "account_id": ACCOUNT, "table_name": TABLE } })
            .await
            .expect("read the table document")
            .expect("the table document");
        let name = crate::data::data_collection_name(doc.get_str("table_id").expect("table_id"));
        self.engine
            .data_db
            .collection::<Document>(&name)
            .drop()
            .await
            .expect("drop the data collection");
        name
    }

    async fn data_collections(&self) -> Vec<String> {
        self.engine
            .data_db
            .list_collection_names()
            .await
            .expect("list collections")
    }
}

/// UpdateTimeToLive during the window writes the catalog document, the way the
/// service runs its ordinary TTL rules there (measured 2026-10-25), and the
/// other non-ACTIVE statuses keep their refusal.
#[tokio::test]
async fn update_ttl_during_the_window_is_served() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.open_window().await;

    f.engine
        .update_ttl(ACCOUNT, TABLE, "ttl", true)
        .await
        .expect("UpdateTimeToLive on a DELETING table");
    let desc = f
        .engine
        .describe_ttl(ACCOUNT, TABLE)
        .await
        .expect("the TTL description");
    assert_eq!(desc.time_to_live_status, TimeToLiveStatus::Enabled);
    assert_eq!(desc.attribute_name.as_deref(), Some("ttl"));
    assert_eq!(
        f.status().await.as_deref(),
        Some("DELETING"),
        "the write must not change the status"
    );

    f.engine
        .update_ttl(ACCOUNT, TABLE, "ttl", false)
        .await
        .expect("the disable direction during the window");
    let desc = f
        .engine
        .describe_ttl(ACCOUNT, TABLE)
        .await
        .expect("the TTL description");
    assert_eq!(desc.time_to_live_status, TimeToLiveStatus::Disabled);

    f.set_status("CREATING").await;
    let err = f
        .engine
        .update_ttl(ACCOUNT, TABLE, "ttl", true)
        .await
        .expect_err("UpdateTimeToLive on a CREATING table");
    assert!(
        matches!(&err, StorageError::TableNotActive(name) if name == TABLE),
        "expected TableNotActive({TABLE:?}), got {err:?}"
    );

    f.set_status("DELETING").await;
    f.finish().await;
}

/// A TTL index build that reaches a `DELETING` document after the delete call
/// dropped the data collection creates no collection: the build reads an
/// `ACTIVE` document only, so it answers `TableNotFound`. MongoDB would
/// otherwise create the collection again to hold the index, and nothing would
/// remove it once the catalog document is gone.
#[tokio::test]
async fn a_ttl_index_build_during_the_window_creates_no_collection() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.open_window().await;
    let data_coll = f.drop_data_collection().await;

    f.engine
        .update_ttl(ACCOUNT, TABLE, "ttl", true)
        .await
        .expect("UpdateTimeToLive on a DELETING table");
    let built = f.engine.create_ttl_index(ACCOUNT, TABLE, "ttl").await;
    let collections = f.data_collections().await;
    assert!(
        !collections.contains(&data_coll),
        "{data_coll} was created again: {collections:?}"
    );
    assert!(
        matches!(&built, Err(StorageError::TableNotFound(name)) if name == TABLE),
        "expected TableNotFound({TABLE:?}), got {built:?}"
    );

    f.finish().await;
}

/// The engine's tag and TTL handlers against a `DELETING` document, through a
/// `MongoEngine`. The key-info read reports such a document as not found on
/// this backend, so the answers below depend on the handlers reading the
/// status first. The tag rows match the service (measured 2026-10-25 for the
/// table ARN, 2026-10-26 for the stream ARN), as do the `DescribeTimeToLive`
/// rows for `DELETING` and `CREATING`. The enable runs after the data
/// collection is dropped, as the delete call drops it before it removes the
/// document, and leaves no collection behind.
#[tokio::test]
async fn engine_tag_and_ttl_handlers_during_the_window() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.open_window().await;
    let ctx = extenddb_engine::OperationContext {
        storage: Arc::new(f.engine.clone()),
        backup: Arc::new(extenddb_engine::BackupRuntime::new(1)),
        limits: Arc::new(extenddb_core::limits::LimitsConfig::default()),
        region: Arc::from("us-east-1"),
        account_id: Arc::from(ACCOUNT),
        import_paths: Arc::from(Vec::new()),
        export_paths: Arc::from(Vec::new()),
        authorized_key_info: extenddb_engine::AuthorizedKeyInfo::default(),
        auth_cache: extenddb_auth::AuthCacheRegistry::empty(),
        table_key_info_lookup: None,
        item_authorizer: None,
    };
    let arn = format!("arn:aws:dynamodb:us-east-1:{ACCOUNT}:table/{TABLE}");
    // The tag path does not look the stream up, so any label of the
    // service's shape names the table's stream.
    let stream_arn = format!("{arn}/stream/2026-10-26T00:13:40.188");
    let in_use = format!(
        "Attempt to change a resource which is still in use: Table is being deleted: {TABLE}"
    );
    // A tag written before the delete.
    f.engine
        .tag_resource(
            &arn,
            &[Tag {
                key: "env".to_owned(),
                value: "test".to_owned(),
            }],
        )
        .await
        .expect("seed a tag");

    let tag = extenddb_engine::handle_tag_resource(
        json!({"ResourceArn": arn, "Tags": [{"Key": "added", "Value": "in-window"}]}),
        &ctx,
    )
    .await;
    assert!(
        matches!(&tag, Err(DynamoDbError::ResourceInUseException(m)) if *m == in_use),
        "TagResource: {tag:?}"
    );
    let untag = extenddb_engine::handle_untag_resource(
        json!({"ResourceArn": arn, "TagKeys": ["env"]}),
        &ctx,
    )
    .await;
    assert!(
        matches!(&untag, Err(DynamoDbError::ResourceInUseException(m)) if *m == in_use),
        "UntagResource: {untag:?}"
    );
    let listed = extenddb_engine::handle_list_tags_of_resource(json!({"ResourceArn": arn}), &ctx)
        .await
        .expect("ListTagsOfResource is served");
    assert_eq!(listed["Tags"], json!([{"Key": "env", "Value": "test"}]));

    extenddb_engine::handle_tag_resource(
        json!({"ResourceArn": stream_arn, "Tags": [{"Key": "sk", "Value": "sv"}]}),
        &ctx,
    )
    .await
    .expect("TagResource on the stream ARN is served");
    let listed =
        extenddb_engine::handle_list_tags_of_resource(json!({"ResourceArn": stream_arn}), &ctx)
            .await
            .expect("ListTagsOfResource on the stream ARN is served");
    assert_eq!(listed["Tags"], json!([{"Key": "sk", "Value": "sv"}]));
    extenddb_engine::handle_untag_resource(
        json!({"ResourceArn": stream_arn, "TagKeys": ["sk"]}),
        &ctx,
    )
    .await
    .expect("UntagResource on the stream ARN is served");

    let described =
        extenddb_engine::handle_describe_time_to_live(json!({"TableName": TABLE}), &ctx).await;
    assert!(
        matches!(&described, Err(DynamoDbError::ValidationException(m))
            if m == "Cannot describe time to live while table is in DELETING state: \
                     Current table state is DELETING"),
        "DescribeTimeToLive: {described:?}"
    );

    let data_coll = f.drop_data_collection().await;
    let updated = extenddb_engine::handle_update_time_to_live(
        json!({"TableName": TABLE,
               "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "ttl"}}),
        &ctx,
    )
    .await
    .expect("UpdateTimeToLive is served");
    assert_eq!(
        updated["TimeToLiveSpecification"],
        json!({"Enabled": true, "AttributeName": "ttl"})
    );
    let collections = f.data_collections().await;
    assert!(
        !collections.contains(&data_coll),
        "{data_coll} was created again: {collections:?}"
    );
    let ttl = f
        .engine
        .describe_ttl(ACCOUNT, TABLE)
        .await
        .expect("the TTL description");
    assert_eq!(
        ttl.attribute_name.as_deref(),
        Some("ttl"),
        "the catalog write landed"
    );
    assert_eq!(f.status().await.as_deref(), Some("DELETING"));

    f.set_status("CREATING").await;
    let described =
        extenddb_engine::handle_describe_time_to_live(json!({"TableName": TABLE}), &ctx).await;
    assert!(
        matches!(&described, Err(DynamoDbError::ValidationException(m))
            if m == "Cannot describe time to live while table is in CREATING state: \
                     Current table state is CREATING"),
        "DescribeTimeToLive on CREATING: {described:?}"
    );

    drop(ctx);
    f.finish().await;
}
