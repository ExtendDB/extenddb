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

pub(crate) async fn activate(engine: &CassandraEngine) {
    tokio::time::sleep(Duration::from_millis(350)).await;
    engine
        .process_control_plane_transitions()
        .await
        .expect("process table transitions");
}

pub(crate) async fn describe_table(
    engine: &CassandraEngine,
    account: &str,
    name: &str,
) -> TableDescription {
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

pub(crate) async fn describe_stream(
    engine: &CassandraEngine,
    account: &str,
    arn: &str,
) -> StreamDescription {
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

pub(crate) async fn set_stream(
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

pub(crate) async fn put(
    engine: &CassandraEngine,
    account: &str,
    name: &str,
    pk: &str,
    view: StreamViewType,
) {
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

/// Every record a shard of the stream `arn` yields, from its start.
pub(crate) async fn read_all(
    engine: &CassandraEngine,
    account: &str,
    arn: &str,
    shard: &str,
) -> Vec<String> {
    let (records, _) = engine
        .get_stream_records(account, arn, shard, None, 1000)
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
        let records = read_all(&engine, &account, &old_arn, &shard.shard_id).await;
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
        old_after += read_all(&engine, &account, &old_arn, &shard.shard_id)
            .await
            .len();
    }
    assert_eq!(old_after, 3);
    let mut on_new = 0;
    for shard in &new.shards {
        on_new += read_all(&engine, &account, &new_arn, &shard.shard_id)
            .await
            .len();
    }
    assert_eq!(on_new, 1);
    // A token pairing one stream's ARN with the other stream's shard id names
    // a shard the stream does not have.
    for (arn, shard) in [
        (&new_arn, &closed.shards[0].shard_id),
        (&old_arn, &new.shards[0].shard_id),
    ] {
        match engine
            .get_stream_records(&account, arn, shard, None, 1000)
            .await
        {
            Err(StorageError::TableNotFound(m)) => {
                assert_eq!(m, extenddb_storage::util::STREAM_DOES_NOT_EXIST);
            }
            other => panic!("{other:?}"),
        }
    }
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

/// Two stream changes fired at the same instant on a table whose stream is
/// enabled apply as some order of the two would. Of two disables, one applies
/// and the other is refused: the conditional catalog write refuses the one
/// that finds the stream fields changed, or `stream_change` refuses the one
/// that reads the stream as disabled. Of an enable and a disable, the enable
/// is refused by `stream_change` when it reads the stream as enabled, and
/// the disable applies; when the disable commits before the enable reads the
/// table, both apply in that order (#1192). The table ends with one enabled
/// stream of four open shards at most and every other shard closed.
#[tokio::test]
async fn overlapping_stream_changes_apply_in_one_order() {
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
    let stream_enabled = |t: &TableDescription| {
        t.stream_specification
            .as_ref()
            .is_some_and(|s| s.stream_enabled)
    };
    for round in 0..6 {
        // The two changes, and which of them is an enable, if one is.
        let (a, b, enable_at) = match round % 3 {
            0 => (disable.clone(), disable.clone(), None),
            1 => (enable.clone(), disable.clone(), Some(0)),
            _ => (disable.clone(), enable.clone(), Some(1)),
        };
        let before = describe_table(&engine, &account, name).await;
        assert!(
            stream_enabled(&before),
            "round {round} starts with the stream enabled: {before:?}"
        );
        let (streams_before, _) = engine
            .list_streams(&account, Some(name), 100, None)
            .await
            .unwrap();
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
        let table = describe_table(&engine, &account, name).await;
        match (enable_at, ok) {
            (None, 1) => assert!(!stream_enabled(&table), "round {round}: {table:?}"),
            // The stream was enabled, so an enable that applies without the
            // disable before it applied over an enabled stream.
            (Some(enable_at), 1) => {
                assert!(
                    [&ra, &rb][enable_at].is_err(),
                    "round {round}: the enable applied over an enabled stream: {ra:?} {rb:?}"
                );
                assert!(!stream_enabled(&table), "round {round}: {table:?}");
            }
            // The disable, then the enable: the old stream is closed and the
            // enable started one new stream.
            (Some(_), 2) => {
                assert!(
                    stream_enabled(&table),
                    "round {round}: both applied, so the enable ran last: {table:?}"
                );
                assert_ne!(
                    table.latest_stream_label, before.latest_stream_label,
                    "round {round}: both applied, so the enable started a new stream"
                );
                let (streams_after, _) = engine
                    .list_streams(&account, Some(name), 100, None)
                    .await
                    .unwrap();
                assert_eq!(
                    streams_after.len(),
                    streams_before.len() + 1,
                    "round {round}: both applied, so exactly one new stream"
                );
                let old_arn = before.latest_stream_arn.as_deref().unwrap();
                let old = describe_stream(&engine, &account, old_arn).await;
                assert!(
                    !matches!(old.stream_status, StreamStatus::Enabled),
                    "round {round}: the disable closed the old stream: {old:?}"
                );
            }
            _ => panic!("round {round}: {ok} changes applied: {ra:?} {rb:?}"),
        }

        // Audit: one enabled stream with four open shards at most, every
        // other shard closed.
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

/// The catalog write of a stream enable on a table created without a stream
/// binds null for the specification and the label it read. Cassandra meets a
/// condition that compares a column with null on a row that does not exist,
/// and an UPDATE writes the row. The write is also conditional on `table_id`,
/// so for a table deleted after the read it does not apply and writes no row,
/// while on the table that exists it applies.
#[tokio::test]
async fn a_stream_enable_write_does_not_apply_to_a_missing_row() {
    use cdrs_tokio::types::value::Value;

    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let account = crate::helpers::unique_test_account();
    crate::helpers::ensure_test_account(&engine, &account)
        .await
        .unwrap();
    let set = [
        (
            "stream_specification",
            Value::from(r#"{"StreamEnabled":true,"StreamViewType":"KEYS_ONLY"}"#),
        ),
        ("stream_label", Value::from("2026-11-09T18:00:00.000")),
    ];
    let rows_named = |name: &'static str| {
        let engine = &engine;
        let account = account.clone();
        async move {
            extenddb_storage_cassandra::cassandra_util::query_rows::<StorageError>(
                &engine.session_arc(),
                &format!(
                    "SELECT table_name FROM {}.tables WHERE account_id = ? AND table_name = ?",
                    engine.catalog_keyspace()
                ),
                cdrs_tokio::query_values!(account.as_str(), name),
                "tables row",
            )
            .await
            .unwrap()
            .len()
        }
    };

    let missing = "StreamEnableMissingRow";
    assert_eq!(rows_named(missing).await, 0);
    let applied = engine
        .write_tables_row(
            &account,
            missing,
            &set,
            "550e8400-e29b-41d4-a716-446655440000",
            None,
            Some((None, None)),
        )
        .await
        .unwrap();
    assert!(!applied, "the write applied to a row that does not exist");
    assert_eq!(
        rows_named(missing).await,
        0,
        "the write left a row for a table that does not exist"
    );

    // The same write on a table created without a stream applies.
    let name = "StreamEnableNoStream";
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
                ..Default::default()
            },
        )
        .await
        .unwrap();
    activate(&engine).await;
    let table_id = describe_table(&engine, &account, name).await.table_id;
    let applied = engine
        .write_tables_row(&account, name, &set, &table_id, None, Some((None, None)))
        .await
        .unwrap();
    assert!(applied, "the write did not apply to the table that exists");
}
