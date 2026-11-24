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
    /// Create `TABLE` with a stream and set its document to `DELETING`.
    /// Returns the stream's ARN.
    async fn open_window_with_stream(&self) -> String {
        let input: CreateTableInput = serde_json::from_value(json!({
            "TableName": TABLE,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"},
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("create input");
        let desc = self
            .engine
            .create_table(ACCOUNT, input)
            .await
            .expect("create the table");
        self.set_status("DELETING").await;
        desc.latest_stream_arn.expect("the table's stream ARN")
    }

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
/// service runs its ordinary TTL rules there (measured 2026-10-25). A CREATING
/// table keeps `TableNotActive`, ExtendDB's current answer: the service
/// answers `ResourceNotFoundException` for a CREATING table (measured
/// 2026-11-20).
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

/// A TTL index build answers `TableNotActive` for a `DELETING` document before
/// `create_index`, so a build that reaches the document after the delete call
/// dropped the data collection creates no collection. MongoDB would otherwise
/// create the collection again to hold the index, and nothing would remove it
/// once the catalog document is gone. A document in another status builds the
/// index as before: an UPDATING one here.
#[tokio::test]
async fn a_ttl_index_build_refuses_a_deleting_document_only() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    f.open_window().await;

    f.set_status("UPDATING").await;
    f.engine
        .create_ttl_index(ACCOUNT, TABLE, "ttl")
        .await
        .expect("the build on an UPDATING document");
    let doc = f
        .engine
        .catalog_db
        .collection::<Document>("tables")
        .find_one(doc! { "_id": { "account_id": ACCOUNT, "table_name": TABLE } })
        .await
        .expect("read the table document")
        .expect("the table document");
    let indexes = f
        .engine
        .data_db
        .collection::<Document>(&crate::data::data_collection_name(
            doc.get_str("table_id").expect("table_id"),
        ))
        .list_index_names()
        .await
        .expect("list the data collection's indexes");
    assert!(
        indexes.contains(&"idx_ttl_ttl".to_owned()),
        "no TTL index on the UPDATING table: {indexes:?}"
    );

    f.set_status("DELETING").await;
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
        matches!(&built, Err(StorageError::TableNotActive(name)) if name == TABLE),
        "expected TableNotActive({TABLE:?}), got {built:?}"
    );

    f.finish().await;
}

/// The engine's tag and TTL handlers against a `DELETING` document, through a
/// `MongoEngine`. The key-info read reports such a document as not found on
/// this backend, so the answers below depend on the handlers reading the
/// status first. The tag rows match the service: for the table ARN its first
/// answers after `DeleteTable` returns (measured 2026-10-25), for the stream
/// ARN its answers in the window (measured 2026-10-26), and for a stream label
/// that names no stream its `ResourceNotFoundException` (measured 2026-11-20).
/// The `DescribeTimeToLive` rows for `DELETING` and `CREATING` match it too.
/// The enable runs after the data collection is dropped, as the delete call
/// drops it before it removes the document, and leaves no collection behind.
#[tokio::test]
async fn engine_tag_and_ttl_handlers_during_the_window() {
    let Some(f) = Fixture::new().await else {
        return;
    };
    let stream_arn = f.open_window_with_stream().await;
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
    // A label of the service's shape that names no stream of the table.
    let made_up = format!("{arn}/stream/2000-01-01T00:00:00.000");
    let in_use = format!(
        "Attempt to change a resource which is still in use: Table is being deleted: {TABLE}"
    );
    let not_found = format!("Requested resource not found: ResourceArn: {made_up} not found");
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

    let tag = extenddb_engine::handle_tag_resource(
        json!({"ResourceArn": made_up, "Tags": [{"Key": "fk", "Value": "fv"}]}),
        &ctx,
    )
    .await;
    assert!(
        matches!(&tag, Err(DynamoDbError::ResourceNotFoundException(m)) if *m == not_found),
        "TagResource on a made-up stream label: {tag:?}"
    );
    let listed =
        extenddb_engine::handle_list_tags_of_resource(json!({"ResourceArn": made_up}), &ctx).await;
    assert!(
        matches!(&listed, Err(DynamoDbError::ResourceNotFoundException(m)) if *m == not_found),
        "ListTagsOfResource on a made-up stream label: {listed:?}"
    );
    assert_eq!(
        f.engine
            .list_tags(&made_up)
            .await
            .expect("read the made-up label's tag rows"),
        Vec::<Tag>::new(),
        "no tag row is stored for a made-up stream label"
    );

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
