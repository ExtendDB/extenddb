// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Streams an earlier release left behind: the name-based shards of a
//! stream created before v0.1.14, and a disabled stream whose shards stayed
//! open.
//!
//! Needs `EXTENDDB_TEST_PG_CONNECTION_STRING` (host-only, e.g.
//! `postgres://user:pass@127.0.0.1:5432`); skips otherwise. Each test builds
//! its own scratch database and drops it afterwards.

#[path = "common/stream_disable_fixture.rs"]
mod fixture;

use fixture::*;

/// A stream-enabled table created under v0.1.13 or earlier has four shard
/// rows named `shardId-{table_name}-{index:016}`. Its stream stays
/// describable, readable and listed after the upgrade, keeps its view type
/// once disabled, and is replaced by a labelled stream on re-enable.
#[tokio::test]
async fn a_stream_from_before_v0_1_14_survives_the_upgrade() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    s.engine
        .create_table(ACCOUNT, table_input("legacy", false))
        .await
        .expect("create");
    activate(&s, "legacy").await;
    let table_id = describe_table(&s, "legacy").await.table_id;
    // What a pre-v0.1.14 release wrote: the label and specification on the
    // catalog row, four name-based open shards in the data database.
    let label = "2026-10-20T10:00:00.000";
    sqlx::query(
        "UPDATE tables SET stream_label = $1, stream_specification = $2 \
         WHERE account_id = $3 AND table_name = 'legacy'",
    )
    .bind(label)
    .bind(serde_json::json!({"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}))
    .bind(ACCOUNT)
    .execute(&s.db)
    .await
    .expect("seed the catalog row");
    for i in 0..4 {
        sqlx::query(
            "INSERT INTO stream_shards (shard_id, table_id, starting_sequence_number) \
             VALUES ($1, $2, $3)",
        )
        .bind(format!("shardId-legacy-{i:016}"))
        .bind(&table_id)
        .bind(format!("{:021}", 0))
        .execute(&s.db)
        .await
        .expect("seed a legacy shard");
    }
    let arn = describe_table(&s, "legacy")
        .await
        .latest_stream_arn
        .expect("the seeded stream arn");
    assert!(arn.ends_with(label));

    put(&s, "legacy", "k", StreamViewType::NewImage).await;
    let open = describe_stream(&s, &arn).await;
    assert!(matches!(open.stream_status, StreamStatus::Enabled));
    assert_eq!(open.stream_view_type, StreamViewType::NewImage);
    let ids: Vec<&str> = open.shards.iter().map(|x| x.shard_id.as_str()).collect();
    assert_eq!(
        ids,
        (0..4)
            .map(|i| format!("shardId-legacy-{i:016}"))
            .collect::<Vec<_>>()
    );
    s.engine
        .validate_shard(ACCOUNT, &arn, "shardId-legacy-0000000000000001")
        .await
        .expect("a legacy shard belongs to the stream");
    assert_eq!(record_count(&s).await, 1);
    let (listed, _) = s
        .engine
        .list_streams(ACCOUNT, Some("legacy"), 100, None)
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].stream_arn, arn);

    set_stream(&s, "legacy", serde_json::json!({"StreamEnabled": false}))
        .await
        .expect("disable");
    let closed = describe_stream(&s, &arn).await;
    assert!(matches!(closed.stream_status, StreamStatus::Disabled));
    assert_eq!(
        closed.stream_view_type,
        StreamViewType::NewImage,
        "a disabled legacy stream keeps its view type"
    );
    assert!(
        closed
            .shards
            .iter()
            .all(|x| x.sequence_number_range.ending_sequence_number.is_some())
    );
    // The backend keeps the disabled specification (with the view type) and
    // the engine omits it from every response.
    let stored = describe_table(&s, "legacy").await.stream_specification;
    assert_eq!(
        stored,
        Some(StreamSpecification {
            stream_enabled: false,
            stream_view_type: Some(StreamViewType::NewImage),
        })
    );
    let mut rendered = describe_table(&s, "legacy").await;
    rendered.prepare_for_response();
    assert!(rendered.stream_specification.is_none());
    put(&s, "legacy", "dark", StreamViewType::NewImage).await;
    assert_eq!(
        record_count(&s).await,
        1,
        "a write after the disable records nothing"
    );

    let re = set_stream(
        &s,
        "legacy",
        serde_json::json!({"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}),
    )
    .await
    .expect("re-enable");
    let new_arn = re.latest_stream_arn.expect("new arn");
    assert_ne!(new_arn, arn);
    let new = describe_stream(&s, &new_arn).await;
    assert!(matches!(new.stream_status, StreamStatus::Enabled));
    assert_eq!(new.shards.len(), 4);
    assert!(new.shards.iter().all(|x| x.shard_id.len() == 61));
    // The enable closed the four legacy rows: none is still open.
    let open_legacy: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM stream_shards \
         WHERE shard_id LIKE 'shardId-legacy-%' AND ending_sequence_number IS NULL",
    )
    .fetch_one(&s.db)
    .await
    .expect("count open legacy shards");
    assert_eq!(
        open_legacy, 0,
        "the enable closes the legacy stream's shards"
    );
    s.cleanup().await;
}

/// A stream disabled by a release that stored `{StreamEnabled: false}` and
/// left its four name-based shards open (the same state a close that failed
/// after its catalog write leaves): writes still record into it. A disable is
/// refused, as on the service, and closes the open shards first, so the next
/// write records nothing.
#[tokio::test]
async fn a_disable_refused_on_a_disabled_stream_closes_the_shards_it_left_open() {
    if base_conn().is_none() {
        eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    s.engine
        .create_table(ACCOUNT, table_input("base", false))
        .await
        .expect("create");
    activate(&s, "base").await;
    let table_id = describe_table(&s, "base").await.table_id;
    let label = "2026-10-20T10:00:00.000";
    sqlx::query(
        "UPDATE tables SET stream_label = $1, stream_specification = $2 \
         WHERE account_id = $3 AND table_name = 'base'",
    )
    .bind(label)
    .bind(serde_json::json!({"StreamEnabled": false}))
    .bind(ACCOUNT)
    .execute(&s.db)
    .await
    .expect("seed the catalog row");
    for i in 0..4 {
        sqlx::query(
            "INSERT INTO stream_shards (shard_id, table_id, starting_sequence_number) \
             VALUES ($1, $2, $3)",
        )
        .bind(format!("shardId-base-{i:016}"))
        .bind(&table_id)
        .bind(format!("{:021}", 0))
        .execute(&s.db)
        .await
        .expect("seed an open shard");
    }
    let open_shards = || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM stream_shards WHERE ending_sequence_number IS NULL",
        )
        .fetch_one(&s.db)
        .await
        .expect("count open shards")
    };
    let arn = describe_table(&s, "base")
        .await
        .latest_stream_arn
        .expect("the seeded stream arn");
    assert!(matches!(
        describe_stream(&s, &arn).await.stream_status,
        StreamStatus::Disabled
    ));
    put(&s, "base", "dark", StreamViewType::KeysOnly).await;
    assert_eq!(record_count(&s).await, 1, "the open shards still record");

    match set_stream(&s, "base", serde_json::json!({"StreamEnabled": false})).await {
        Err(StorageError::Validation(m)) => {
            assert_eq!(m, "Table has no stream to disable: TableName: base");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        open_shards().await,
        0,
        "the refused disable closed the shards"
    );
    let closed = describe_stream(&s, &arn).await;
    assert!(matches!(closed.stream_status, StreamStatus::Disabled));
    for shard in &closed.shards {
        let expected = last_record(&s, &shard.shard_id)
            .await
            .unwrap_or_else(|| shard.sequence_number_range.starting_sequence_number.clone());
        assert_eq!(
            shard
                .sequence_number_range
                .ending_sequence_number
                .as_deref(),
            Some(expected.as_str())
        );
    }
    put(&s, "base", "after", StreamViewType::KeysOnly).await;
    assert_eq!(
        record_count(&s).await,
        1,
        "a write after the close records nothing"
    );
    // The stored specification is untouched, and a second refusal has nothing
    // left to close.
    assert_eq!(
        describe_table(&s, "base").await.stream_specification,
        Some(StreamSpecification {
            stream_enabled: false,
            stream_view_type: None,
        })
    );
    match set_stream(&s, "base", serde_json::json!({"StreamEnabled": false})).await {
        Err(StorageError::Validation(m)) => {
            assert_eq!(m, "Table has no stream to disable: TableName: base");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(open_shards().await, 0);
    s.cleanup().await;
}
