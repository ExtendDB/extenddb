// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Disabling a table's stream closes its shards, and enabling it again
//! creates a new stream, against the rows measured on the service on
//! 2026-10-24. Records stay in one partition per shard index; each stream's
//! shard reads only its own range of it.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use extenddb_core::types::{
    AttributeDefinition, AttributeValue, CreateTableInput, DescribeStreamInput, DescribeTableInput,
    KeySchemaElement, KeyType, ScalarAttributeType, StreamDescription, StreamSpecification,
    StreamStatus, StreamViewType, TableDescription,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, StreamCapture, StreamEngine, TableEngine};
use extenddb_storage_cassandra::CassandraEngine;

use crate::helpers::setup_engine;

async fn activate(engine: &CassandraEngine) {
    tokio::time::sleep(Duration::from_millis(350)).await;
    engine
        .process_control_plane_transitions()
        .await
        .expect("process table transitions");
}

async fn describe_table(engine: &CassandraEngine, account: &str, name: &str) -> TableDescription {
    engine
        .describe_table(
            account,
            DescribeTableInput {
                table_name: name.to_owned(),
            },
        )
        .await
        .unwrap()
}

async fn describe_stream(engine: &CassandraEngine, account: &str, arn: &str) -> StreamDescription {
    engine
        .describe_stream(
            account,
            &DescribeStreamInput {
                stream_arn: arn.to_owned(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap()
}

async fn set_stream(
    engine: &CassandraEngine,
    account: &str,
    name: &str,
    spec: serde_json::Value,
) -> Result<TableDescription, StorageError> {
    let input = serde_json::json!({"TableName": name, "StreamSpecification": spec});
    let out = engine
        .update_table(account, serde_json::from_value(input).unwrap())
        .await;
    activate(engine).await;
    out
}

async fn put(engine: &CassandraEngine, account: &str, name: &str, pk: &str, view: StreamViewType) {
    let key_info = engine.table_key_info(account, name).await.unwrap();
    let mut item = BTreeMap::new();
    item.insert("id".to_owned(), AttributeValue::S(pk.to_owned()));
    let capture = StreamCapture {
        view_type: view,
        user_identity: None,
        region: Arc::from("us-east-1"),
    };
    engine
        .put_item(
            &key_info,
            item,
            false,
            None,
            &Default::default(),
            Some(&capture),
        )
        .await
        .unwrap();
}

/// Every record a shard yields, from its start.
async fn read_all(engine: &CassandraEngine, account: &str, shard: &str) -> Vec<String> {
    let (records, _) = engine
        .get_stream_records(account, shard, None, 1000)
        .await
        .unwrap();
    records
        .into_iter()
        .map(|r| r.dynamodb.sequence_number)
        .collect()
}

#[tokio::test]
async fn disable_closes_every_shard_and_re_enable_creates_a_new_stream() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = crate::helpers::unique_test_account();
    crate::helpers::ensure_test_account(&engine, &account)
        .await
        .unwrap();
    let name = "StreamDisable";
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
                    stream_view_type: Some(StreamViewType::NewAndOldImages),
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    activate(&engine).await;
    for pk in ["k0", "k1", "k2"] {
        put(&engine, &account, name, pk, StreamViewType::NewAndOldImages).await;
    }
    let enabled = describe_table(&engine, &account, name).await;
    let old_arn = enabled.latest_stream_arn.clone().unwrap();

    let disabled = set_stream(
        &engine,
        &account,
        name,
        serde_json::json!({"StreamEnabled": false}),
    )
    .await
    .unwrap();
    let mut rendered = disabled.clone();
    rendered.prepare_for_response();
    assert!(rendered.stream_specification.is_none());
    assert_eq!(
        disabled.latest_stream_arn.as_deref(),
        Some(old_arn.as_str())
    );
    let mut table = describe_table(&engine, &account, name).await;
    table.prepare_for_response();
    assert!(table.stream_specification.is_none());

    let closed = describe_stream(&engine, &account, &old_arn).await;
    assert!(matches!(closed.stream_status, StreamStatus::Disabled));
    assert_eq!(closed.stream_view_type, StreamViewType::NewAndOldImages);
    let mut before = 0;
    for shard in &closed.shards {
        let range = &shard.sequence_number_range;
        let records = read_all(&engine, &account, &shard.shard_id).await;
        before += records.len();
        let expected = records
            .last()
            .cloned()
            .unwrap_or_else(|| range.starting_sequence_number.clone());
        assert_eq!(
            range.ending_sequence_number.as_deref(),
            Some(expected.as_str())
        );
    }
    assert_eq!(before, 3);

    match set_stream(
        &engine,
        &account,
        name,
        serde_json::json!({"StreamEnabled": false}),
    )
    .await
    {
        Err(StorageError::Validation(m)) => {
            assert_eq!(
                m,
                format!("Table has no stream to disable: TableName: {name}")
            );
        }
        other => panic!("{other:?}"),
    }

    let reenabled = set_stream(
        &engine,
        &account,
        name,
        serde_json::json!({"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"}),
    )
    .await
    .unwrap();
    let new_arn = reenabled.latest_stream_arn.clone().unwrap();
    assert_ne!(new_arn, old_arn);
    let new = describe_stream(&engine, &account, &new_arn).await;
    assert!(matches!(new.stream_status, StreamStatus::Enabled));
    assert_eq!(new.stream_view_type, StreamViewType::KeysOnly);
    assert!(
        new.shards
            .iter()
            .all(|n| !closed.shards.iter().any(|o| o.shard_id == n.shard_id))
    );
    match set_stream(
        &engine,
        &account,
        name,
        serde_json::json!({"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}),
    )
    .await
    {
        Err(StorageError::Validation(m)) => {
            assert_eq!(
                m,
                format!("Table already has an enabled stream: TableName: {name}")
            );
        }
        other => panic!("{other:?}"),
    }

    // A write after the re-enable lands in the shared partition, and only the
    // new stream's shard yields it.
    put(&engine, &account, name, "after", StreamViewType::KeysOnly).await;
    let mut old_after = 0;
    for shard in &closed.shards {
        old_after += read_all(&engine, &account, &shard.shard_id).await.len();
    }
    assert_eq!(old_after, 3);
    let mut on_new = 0;
    for shard in &new.shards {
        on_new += read_all(&engine, &account, &shard.shard_id).await.len();
    }
    assert_eq!(on_new, 1);
    // LATEST on a closed shard resolves to its last record, not to the new
    // stream's.
    for shard in &closed.shards {
        let latest = engine
            .latest_sequence_number(&shard.shard_id)
            .await
            .unwrap();
        let end = shard.sequence_number_range.ending_sequence_number.clone();
        if latest.is_some() {
            assert_eq!(latest, end);
        }
    }

    let (listed, _) = engine
        .list_streams(&account, Some(name), 100, None)
        .await
        .unwrap();
    let arns: Vec<&str> = listed.iter().map(|s| s.stream_arn.as_str()).collect();
    assert_eq!(arns, [new_arn.as_str(), old_arn.as_str()]);
    match engine
        .validate_shard(&account, &old_arn, &new.shards[0].shard_id)
        .await
    {
        Err(StorageError::TableNotFound(m)) => {
            assert_eq!(m, "Requested resource not found: Shard does not exist");
        }
        other => panic!("{other:?}"),
    }
}

/// Of two stream changes fired at the same instant, one is applied and the
/// other refused (the conditional catalog write); the table ends with one
/// enabled stream of four open shards and every other shard closed.
#[tokio::test]
async fn overlapping_stream_changes_apply_exactly_one() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = crate::helpers::unique_test_account();
    crate::helpers::ensure_test_account(&engine, &account)
        .await
        .unwrap();
    let name = "StreamOverlap";
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
    let input = |spec: serde_json::Value| -> extenddb_core::types::UpdateTableInput {
        serde_json::from_value(serde_json::json!({"TableName": name, "StreamSpecification": spec}))
            .unwrap()
    };
    let disable = serde_json::json!({"StreamEnabled": false});
    let enable = serde_json::json!({"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"});
    for round in 0..6 {
        let (a, b) = match round % 3 {
            0 => (disable.clone(), disable.clone()),
            1 => (enable.clone(), disable.clone()),
            _ => (disable.clone(), enable.clone()),
        };
        let (ra, rb) = tokio::join!(
            engine.update_table(&account, input(a)),
            engine.update_table(&account, input(b)),
        );
        activate(&engine).await;
        let ok = [&ra, &rb].iter().filter(|r| r.is_ok()).count();
        for r in [&ra, &rb] {
            if let Err(e) = r {
                assert!(
                    matches!(
                        e,
                        StorageError::Validation(_) | StorageError::TableNotActive(_)
                    ),
                    "{e:?}"
                );
            }
        }
        assert_eq!(
            ok, 1,
            "round {round}: exactly one change applies: {ra:?} {rb:?}"
        );

        // Audit: one enabled stream with four open shards at most, every
        // other shard closed.
        let table = describe_table(&engine, &account, name).await;
        let (listed, _) = engine
            .list_streams(&account, Some(name), 100, None)
            .await
            .unwrap();
        let mut enabled = 0;
        for s in &listed {
            let d = describe_stream(&engine, &account, &s.stream_arn).await;
            let open = d
                .shards
                .iter()
                .filter(|x| x.sequence_number_range.ending_sequence_number.is_none())
                .count();
            if matches!(d.stream_status, StreamStatus::Enabled) {
                enabled += 1;
                assert_eq!((d.shards.len(), open), (4, 4), "{}", s.stream_arn);
                assert_eq!(
                    table.latest_stream_arn.as_deref(),
                    Some(s.stream_arn.as_str())
                );
            } else {
                assert_eq!(open, 0, "a disabled stream has every shard closed");
            }
        }
        let spec_enabled = table
            .stream_specification
            .as_ref()
            .is_some_and(|s| s.stream_enabled);
        assert_eq!(enabled, usize::from(spec_enabled));
        // Back to enabled for the next round.
        let _ = engine.update_table(&account, input(enable.clone())).await;
        activate(&engine).await;
    }
}
