// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Disabling and re-enabling a table's stream on SQLite, against the rows
//! measured on the service on 2026-10-24.

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    DescribeStreamInput, DescribeTableInput, StreamDescription, StreamSpecification, StreamStatus,
    StreamViewType, TableDescription,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, StreamCapture, StreamEngine, TableEngine};

use crate::store::SqliteEngine;

const ACCOUNT: &str = "123456789012";

async fn engine() -> SqliteEngine {
    let engine = SqliteEngine::new(":memory:", 1, "us-east-1", 400 * 1024)
        .await
        .unwrap();
    crate::schema::apply(&engine.pool).await.unwrap();
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES (?, 'stream-test')")
        .bind(ACCOUNT)
        .execute(&engine.pool)
        .await
        .unwrap();
    engine
}

async fn create(engine: &SqliteEngine, name: &str, stream: bool) {
    let mut input = serde_json::json!({
        "TableName": name,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST",
    });
    if stream {
        input["StreamSpecification"] =
            serde_json::json!({"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"});
    }
    engine
        .create_table(ACCOUNT, serde_json::from_value(input).unwrap())
        .await
        .unwrap();
    activate(engine, name).await;
}

/// Finish the control-plane transition the worker would run.
async fn activate(engine: &SqliteEngine, name: &str) {
    sqlx::query("UPDATE tables SET table_status = 'ACTIVE' WHERE table_name = ?")
        .bind(name)
        .execute(&engine.pool)
        .await
        .unwrap();
}

async fn set_stream(
    engine: &SqliteEngine,
    name: &str,
    spec: serde_json::Value,
) -> Result<TableDescription, StorageError> {
    let input = serde_json::json!({"TableName": name, "StreamSpecification": spec});
    let out = engine
        .update_table(ACCOUNT, serde_json::from_value(input).unwrap())
        .await;
    activate(engine, name).await;
    out
}

async fn describe_table(engine: &SqliteEngine, name: &str) -> TableDescription {
    engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: name.to_owned(),
            },
        )
        .await
        .unwrap()
}

async fn describe_stream(engine: &SqliteEngine, arn: &str) -> StreamDescription {
    engine
        .describe_stream(
            ACCOUNT,
            &DescribeStreamInput {
                stream_arn: arn.to_owned(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap()
}

/// Put an item through the data path with the stream capture the engine
/// builds from `view_type`, as a caller holding stale key info would.
async fn put(engine: &SqliteEngine, name: &str, pk: &str, view_type: StreamViewType) {
    let key_info = engine.table_key_info(ACCOUNT, name).await.unwrap();
    let item = serde_json::from_value(serde_json::json!({"pk": {"S": pk}})).unwrap();
    let capture = StreamCapture {
        view_type,
        user_identity: None,
        region: "us-east-1".into(),
    };
    engine
        .put_item(
            &key_info,
            item,
            false,
            None,
            &ExpressionMaps::default(),
            Some(&capture),
        )
        .await
        .unwrap();
}

async fn records_in(engine: &SqliteEngine, shard: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT sequence_number FROM stream_records WHERE shard_id = ? ORDER BY sequence_number",
    )
    .bind(shard)
    .fetch_all(&engine.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn disable_closes_every_shard_and_re_enable_creates_a_new_stream() {
    let engine = engine().await;
    create(&engine, "t482", true).await;
    for pk in ["k0", "k1", "k2", "k3", "k4", "k5"] {
        put(&engine, "t482", pk, StreamViewType::NewAndOldImages).await;
    }
    let enabled = describe_table(&engine, "t482").await;
    let old_arn = enabled.latest_stream_arn.clone().unwrap();
    let before = describe_stream(&engine, &old_arn).await;
    assert!(matches!(before.stream_status, StreamStatus::Enabled));
    assert!(
        before
            .shards
            .iter()
            .all(|s| s.sequence_number_range.ending_sequence_number.is_none())
    );

    let disabled = set_stream(&engine, "t482", serde_json::json!({"StreamEnabled": false}))
        .await
        .unwrap();
    // The backend keeps `{StreamEnabled: false}` with the closed stream's
    // view type; the engine omits it from every response.
    assert_eq!(
        disabled.stream_specification,
        Some(StreamSpecification {
            stream_enabled: false,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        })
    );
    let mut rendered = disabled.clone();
    rendered.prepare_for_response();
    assert!(rendered.stream_specification.is_none());
    assert_eq!(
        disabled.latest_stream_arn.as_deref(),
        Some(old_arn.as_str())
    );
    let mut table = describe_table(&engine, "t482").await;
    table.prepare_for_response();
    assert!(table.stream_specification.is_none());
    assert_eq!(table.latest_stream_label, enabled.latest_stream_label);

    let closed = describe_stream(&engine, &old_arn).await;
    assert!(matches!(closed.stream_status, StreamStatus::Disabled));
    assert_eq!(closed.stream_view_type, StreamViewType::NewAndOldImages);
    assert_eq!(closed.shards.len(), 4);
    for shard in &closed.shards {
        let range = &shard.sequence_number_range;
        let last = records_in(&engine, &shard.shard_id).await.pop();
        let expected = last.unwrap_or_else(|| range.starting_sequence_number.clone());
        assert_eq!(
            range.ending_sequence_number.as_deref(),
            Some(expected.as_str())
        );
    }

    // A write still carrying the enabled view type records nothing.
    put(&engine, "t482", "stale", StreamViewType::NewAndOldImages).await;
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM stream_records")
        .fetch_one(&engine.pool)
        .await
        .unwrap();
    assert_eq!(total, 6);

    let reenabled = set_stream(
        &engine,
        "t482",
        serde_json::json!({"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}),
    )
    .await
    .unwrap();
    let new_arn = reenabled.latest_stream_arn.clone().unwrap();
    assert_ne!(new_arn, old_arn);
    assert!(reenabled.latest_stream_label > enabled.latest_stream_label);
    let new = describe_stream(&engine, &new_arn).await;
    assert!(matches!(new.stream_status, StreamStatus::Enabled));
    assert_eq!(new.stream_view_type, StreamViewType::KeysOnly);
    assert!(
        new.shards
            .iter()
            .all(|s| !closed.shards.iter().any(|o| o.shard_id == s.shard_id))
    );

    put(&engine, "t482", "after", StreamViewType::KeysOnly).await;
    let old_again = describe_stream(&engine, &old_arn).await;
    assert!(matches!(old_again.stream_status, StreamStatus::Disabled));
    assert_eq!(old_again.stream_view_type, StreamViewType::NewAndOldImages);
    for (a, b) in old_again.shards.iter().zip(&closed.shards) {
        assert_eq!(a.shard_id, b.shard_id);
        assert_eq!(
            a.sequence_number_range.ending_sequence_number,
            b.sequence_number_range.ending_sequence_number
        );
        assert_eq!(records_in(&engine, &a.shard_id).await.len(), {
            records_in(&engine, &b.shard_id).await.len()
        });
    }
    let mut on_new = 0;
    for s in &new.shards {
        on_new += records_in(&engine, &s.shard_id).await.len();
    }
    assert_eq!(on_new, 1);

    let (listed, _) = engine
        .list_streams(ACCOUNT, Some("t482"), 100, None)
        .await
        .unwrap();
    let arns: Vec<&str> = listed.iter().map(|s| s.stream_arn.as_str()).collect();
    assert_eq!(arns, [new_arn.as_str(), old_arn.as_str()]);

    // Each ARN accepts only its own shards.
    engine
        .validate_shard(ACCOUNT, &old_arn, &closed.shards[0].shard_id)
        .await
        .unwrap();
    for (arn, shard) in [
        (&old_arn, &new.shards[0].shard_id),
        (&new_arn, &closed.shards[0].shard_id),
    ] {
        match engine.validate_shard(ACCOUNT, arn, shard).await {
            Err(StorageError::TableNotFound(m)) => {
                assert_eq!(m, "Requested resource not found: Shard does not exist");
            }
            other => panic!("{other:?}"),
        }
    }
    engine.pool.close().await;
}

#[tokio::test]
async fn enable_while_enabled_and_disable_without_a_stream_are_refused() {
    let engine = engine().await;
    create(&engine, "t482", true).await;
    create(&engine, "nostream", false).await;
    for view in ["NEW_AND_OLD_IMAGES", "KEYS_ONLY"] {
        let spec = serde_json::json!({"StreamEnabled": true, "StreamViewType": view});
        match set_stream(&engine, "t482", spec).await {
            Err(StorageError::Validation(m)) => {
                assert_eq!(m, "Table already has an enabled stream: TableName: t482");
            }
            other => panic!("{other:?}"),
        }
    }
    match set_stream(
        &engine,
        "nostream",
        serde_json::json!({"StreamEnabled": false}),
    )
    .await
    {
        Err(StorageError::Validation(m)) => {
            assert_eq!(m, "Table has no stream to disable: TableName: nostream");
        }
        other => panic!("{other:?}"),
    }
    set_stream(&engine, "t482", serde_json::json!({"StreamEnabled": false}))
        .await
        .unwrap();
    match set_stream(&engine, "t482", serde_json::json!({"StreamEnabled": false})).await {
        Err(StorageError::Validation(m)) => {
            assert_eq!(m, "Table has no stream to disable: TableName: t482");
        }
        other => panic!("{other:?}"),
    }
    // A refused request changes nothing: one stream, four shards.
    let shards: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM stream_shards")
        .fetch_one(&engine.pool)
        .await
        .unwrap();
    assert_eq!(shards, 4);
    engine.pool.close().await;
}

#[tokio::test]
async fn a_stream_from_before_labelled_shard_ids_closes_and_is_replaced() {
    let engine = engine().await;
    create(&engine, "legacy", true).await;
    // Rewrite the shards in the shape earlier builds created.
    sqlx::query(
        "UPDATE stream_shards SET shard_id = 'shardId-' || table_id || '-' || \
         printf('%016d', CAST(substr(shard_id, -2) AS INTEGER))",
    )
    .execute(&engine.pool)
    .await
    .unwrap();
    let arn = describe_table(&engine, "legacy")
        .await
        .latest_stream_arn
        .unwrap();
    put(&engine, "legacy", "k", StreamViewType::NewAndOldImages).await;
    let open = describe_stream(&engine, &arn).await;
    assert!(matches!(open.stream_status, StreamStatus::Enabled));
    assert_eq!(open.stream_view_type, StreamViewType::NewAndOldImages);
    assert!(open.shards.iter().all(|s| {
        s.shard_id
            .rsplit('-')
            .next()
            .is_some_and(|suffix| suffix.starts_with("0000000000000"))
    }));

    set_stream(
        &engine,
        "legacy",
        serde_json::json!({"StreamEnabled": false}),
    )
    .await
    .unwrap();
    let closed = describe_stream(&engine, &arn).await;
    assert!(matches!(closed.stream_status, StreamStatus::Disabled));
    assert!(
        closed
            .shards
            .iter()
            .all(|s| s.sequence_number_range.ending_sequence_number.is_some())
    );
    let re = set_stream(
        &engine,
        "legacy",
        serde_json::json!({"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}),
    )
    .await
    .unwrap();
    let new = describe_stream(&engine, &re.latest_stream_arn.unwrap()).await;
    assert!(matches!(new.stream_status, StreamStatus::Enabled));
    assert_eq!(new.shards.len(), 4);
    engine.pool.close().await;
}
