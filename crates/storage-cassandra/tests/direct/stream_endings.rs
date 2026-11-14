// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A closed stream's ending numbers against the next stream's start on
//! Cassandra, where every stream of a table keeps its records in the same
//! partition per shard index.

use extenddb_core::types::{
    AttributeDefinition, CreateTableInput, KeySchemaElement, KeyType, ScalarAttributeType,
    StreamSpecification, StreamViewType,
};
use extenddb_storage::TableEngine;

use crate::helpers::setup_engine;
use crate::stream_disable::{activate, describe_stream, describe_table, put, read_all, set_stream};

/// The closed stream's last record came from a server whose clock is ahead of
/// this one: the new stream starts above that ending number, not at this
/// server's clock, so the record stays in one stream.
#[tokio::test]
async fn a_new_stream_starts_above_the_closed_streams_ending_numbers() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = crate::helpers::unique_test_account();
    crate::helpers::ensure_test_account(&engine, &account)
        .await
        .unwrap();
    let name = "StreamSkew";
    engine
        .create_table(
            &account,
            CreateTableInput {
                table_name: name.to_owned(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "id".to_owned(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "id".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                }],
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::KeysOnly),
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    activate(&engine).await;
    let old_arn = describe_table(&engine, &account, name)
        .await
        .latest_stream_arn
        .unwrap();
    set_stream(
        &engine,
        &account,
        name,
        serde_json::json!({"StreamEnabled": false}),
    )
    .await
    .unwrap();
    let closed = describe_stream(&engine, &account, &old_arn).await;
    // An ending number an hour ahead of this server's clock, as a record from
    // a server that far ahead would leave it.
    let ahead = format!(
        "{:013}{:06}{:04}",
        chrono::Utc::now().timestamp_millis() + 3_600_000,
        0,
        1
    );
    let account_ks = engine.account_keyspace(&account);
    let update = format!(
        "UPDATE {account_ks}.stream_shards SET ending_sequence_number = ? WHERE shard_id = ?"
    );
    for shard in &closed.shards {
        engine
            .session_arc()
            .query_with_values(
                &update,
                cdrs_tokio::query_values!(ahead.as_str(), shard.shard_id.as_str()),
            )
            .await
            .unwrap();
    }

    let reenabled = set_stream(
        &engine,
        &account,
        name,
        serde_json::json!({"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}),
    )
    .await
    .unwrap();
    let new = describe_stream(&engine, &account, &reenabled.latest_stream_arn.unwrap()).await;
    assert_eq!(new.shards.len(), 4);
    for shard in &new.shards {
        let start = &shard.sequence_number_range.starting_sequence_number;
        assert_eq!(start.len(), ahead.len());
        assert!(
            start.as_str() > ahead.as_str(),
            "start {start} is not above the closed stream's ending number {ahead}"
        );
    }
}

/// A disable whose close read the old stream's shards as open and writes their
/// ending numbers only after an overlapping enable closed them, started the
/// new stream and the new stream took records: the old shards keep the ending
/// numbers the first close wrote, below the new stream's start.
#[tokio::test]
async fn a_late_close_keeps_the_ending_numbers_the_first_close_wrote() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = crate::helpers::unique_test_account();
    crate::helpers::ensure_test_account(&engine, &account)
        .await
        .unwrap();
    let name = "StreamLateClose";
    engine
        .create_table(
            &account,
            CreateTableInput {
                table_name: name.to_owned(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "id".to_owned(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "id".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                }],
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::KeysOnly),
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    activate(&engine).await;
    let table = describe_table(&engine, &account, name).await;
    let old_arn = table.latest_stream_arn.clone().unwrap();
    for i in 0..8 {
        put(
            &engine,
            &account,
            name,
            &format!("a{i}"),
            StreamViewType::KeysOnly,
        )
        .await;
    }
    // The rows a disable reads before it closes: every shard open.
    let read_open: Vec<extenddb_storage::stream_lineage::ShardRow> =
        describe_stream(&engine, &account, &old_arn)
            .await
            .shards
            .into_iter()
            .map(|s| extenddb_storage::stream_lineage::ShardRow {
                table_id: table.table_id.clone(),
                shard_id: s.shard_id,
                parent_shard_id: s.parent_shard_id,
                starting_sequence_number: s.sequence_number_range.starting_sequence_number,
                ending_sequence_number: None,
            })
            .collect();
    assert_eq!(read_open.len(), 4);

    set_stream(
        &engine,
        &account,
        name,
        serde_json::json!({"StreamEnabled": false}),
    )
    .await
    .unwrap();
    let closed = describe_stream(&engine, &account, &old_arn).await;
    let reenabled = set_stream(
        &engine,
        &account,
        name,
        serde_json::json!({"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}),
    )
    .await
    .unwrap();
    let new_arn = reenabled.latest_stream_arn.unwrap();
    for i in 0..8 {
        put(
            &engine,
            &account,
            name,
            &format!("b{i}"),
            StreamViewType::KeysOnly,
        )
        .await;
    }
    let new = describe_stream(&engine, &account, &new_arn).await;
    let mut new_records = 0;
    for shard in &new.shards {
        new_records += read_all(&engine, &account, &new_arn, &shard.shard_id)
            .await
            .len();
    }
    assert_eq!(new_records, 8, "the new stream holds the eight new records");

    // The late close of the old stream's label.
    engine
        .close_shards_as_read(
            &engine.account_keyspace(&account),
            &table.table_id,
            read_open,
        )
        .await
        .unwrap();
    let after = describe_stream(&engine, &account, &old_arn).await;
    for (a, b) in after.shards.iter().zip(&closed.shards) {
        assert_eq!(a.shard_id, b.shard_id);
        assert_eq!(
            a.sequence_number_range.ending_sequence_number,
            b.sequence_number_range.ending_sequence_number,
            "the late close moved shard {}'s ending number",
            a.shard_id
        );
    }
    let start = &new.shards[0].sequence_number_range.starting_sequence_number;
    for shard in &after.shards {
        let end = shard
            .sequence_number_range
            .ending_sequence_number
            .as_deref()
            .unwrap();
        assert!(
            end < start.as_str(),
            "ending {end} is not below the new start {start}"
        );
    }
}
