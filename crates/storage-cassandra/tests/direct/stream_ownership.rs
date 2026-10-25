// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `get_stream_records` reads a shard only under the stream its iterator names.
//!
//! The catalog read is keyed on the caller's account and the table name the
//! ARN carries, the shard read on that table's id, and the stream label is
//! decided in Rust from the table's current label. Each test here removes one
//! of those from the picture (same label on two tables, same name in two
//! accounts, a new label on the same shards) so that one predicate alone
//! decides the answer.

use std::collections::BTreeMap;
use std::sync::Arc;

use extenddb_core::types::{AttributeValue, StreamViewType, TableKeyInfo};
use extenddb_storage::error::StorageError;
use extenddb_storage::util::stream_arn;
use extenddb_storage::{DataEngine, StreamCapture, StreamEngine, StreamRecordsResult};
use extenddb_storage_cassandra::CassandraEngine;

use crate::helpers::setup_engine;
use crate::streams::{setup_stream_table, shard_for};

const REGION: &str = "us-east-1";
const LABEL: &str = "2026-01-01T00:00:00.000";
const LATER_LABEL: &str = "2026-02-02T00:00:00.000";

/// Pin a table's stream label so two tables can share one.
async fn relabel(engine: &CassandraEngine, key_info: &TableKeyInfo, label: &str) {
    let catalog_keyspace = engine.catalog_keyspace();
    let query = format!(
        "UPDATE {catalog_keyspace}.tables SET stream_label = ? \
         WHERE account_id = ? AND table_name = ?"
    );
    engine
        .session_arc()
        .query_with_values(
            &query,
            cdrs_tokio::query_values!(
                label,
                key_info.account_id.as_str(),
                key_info.table_name.as_str()
            ),
        )
        .await
        .expect("relabel");
}

/// A second stream-enabled table in an account `setup_stream_table` created.
async fn stream_table_in(
    engine: &CassandraEngine,
    account_id: &str,
    table_name: &str,
) -> TableKeyInfo {
    use extenddb_core::types::{
        AttributeDefinition, CreateTableInput, KeySchemaElement, KeyType, ScalarAttributeType,
        StreamSpecification,
    };
    use extenddb_storage::TableEngine;
    let key_schema = vec![KeySchemaElement {
        attribute_name: "id".to_string(),
        key_type: KeyType::Hash,
    }];
    let attribute_definitions = vec![AttributeDefinition {
        attribute_name: "id".to_string(),
        attribute_type: ScalarAttributeType::S,
    }];
    let desc = engine
        .create_table(
            account_id,
            CreateTableInput {
                table_name: table_name.to_string(),
                key_schema: key_schema.clone(),
                attribute_definitions: attribute_definitions.clone(),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::KeysOnly),
                }),
                ..Default::default()
            },
        )
        .await
        .expect("create the second table");
    TableKeyInfo {
        vector_indexes: Vec::new(),
        table_name: table_name.to_string(),
        account_id: account_id.to_string(),
        table_id: desc.table_id,
        key_schema: key_schema.clone(),
        base_key_schema: key_schema,
        attribute_definitions,
        has_lsi: false,
        global_secondary_indexes: Vec::new(),
        local_secondary_indexes: Vec::new(),
        stream_specification: desc.stream_specification,
    }
}

/// One item written under `pk`, which lands in `shard_for(pk, table_id)`.
async fn put(engine: &CassandraEngine, key_info: &TableKeyInfo, pk: &str, val: u32) {
    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S(pk.to_string()));
    item.insert("val".to_string(), AttributeValue::N(val.to_string()));
    let capture = StreamCapture {
        view_type: StreamViewType::KeysOnly,
        user_identity: None,
        region: Arc::from(REGION),
    };
    engine
        .put_item(
            key_info,
            item,
            false,
            None,
            &Default::default(),
            Some(&capture),
        )
        .await
        .expect("put_item");
}

fn arn_of(key_info: &TableKeyInfo, label: &str) -> String {
    stream_arn(REGION, &key_info.account_id, &key_info.table_name, label)
}

fn assert_stream_not_found(result: StreamRecordsResult, arn: &str) {
    let StorageError::TableNotFound(expected) = extenddb_storage::util::stream_not_found(arn)
    else {
        unreachable!("stream_not_found is a TableNotFound error")
    };
    match result {
        Err(StorageError::TableNotFound(msg)) => assert_eq!(msg, expected),
        other => panic!("expected the stream not-found error, got {other:?}"),
    }
}

/// Two tables of one account share a stream label: a shard of table `b`
/// under table `a`'s ARN is refused, so the table name alone decides.
#[tokio::test]
async fn test_another_tables_shard_with_the_same_label_is_not_found() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (a, _) = setup_stream_table(&engine, "OwnA").await;
    let b = stream_table_in(&engine, &a.account_id, "OwnB").await;
    relabel(&engine, &a, LABEL).await;
    relabel(&engine, &b, LABEL).await;
    put(&engine, &a, "pk-a", 1).await;
    put(&engine, &b, "pk-b", 1).await;
    let shard_a = shard_for("pk-a", &a.table_id);
    let shard_b = shard_for("pk-b", &b.table_id);
    let arn_a = arn_of(&a, LABEL);
    let arn_b = arn_of(&b, LABEL);

    // Both directions, so whichever table a lookup without the name would
    // pick, one of them reads a shard that is not its own.
    assert_stream_not_found(
        engine
            .get_stream_records(&a.account_id, &arn_a, &shard_b, None, 10)
            .await,
        &arn_a,
    );
    assert_stream_not_found(
        engine
            .get_stream_records(&a.account_id, &arn_b, &shard_a, None, 10)
            .await,
        &arn_b,
    );
    assert_stream_not_found(
        engine
            .get_stream_records(
                &a.account_id,
                &arn_a,
                "shardId-forged-000000000000",
                None,
                10,
            )
            .await,
        &arn_a,
    );
    // The control: table A's own shard reads under A's ARN.
    let (records, _) = engine
        .get_stream_records(&a.account_id, &arn_a, &shard_a, None, 10)
        .await
        .expect("own shard reads");
    assert_eq!(records.len(), 1);
}

/// Two accounts own a table of the same name and label: the caller's ARN
/// resolves in the caller's account only, so the other account's shard is
/// refused under either spelling of the account in the ARN.
#[tokio::test]
async fn test_another_accounts_same_named_stream_is_not_found() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    // `setup_stream_table` creates a fresh account per call, so these two
    // tables of one name belong to two accounts.
    let (own, _) = setup_stream_table(&engine, "SameName").await;
    let (other, _) = setup_stream_table(&engine, "SameName").await;
    assert_ne!(own.account_id, other.account_id);
    relabel(&engine, &own, LABEL).await;
    relabel(&engine, &other, LABEL).await;
    put(&engine, &own, "pk-o", 1).await;
    put(&engine, &other, "pk-o", 1).await;
    let own_shard = shard_for("pk-o", &own.table_id);
    let other_shard = shard_for("pk-o", &other.table_id);

    // From both callers, so whichever table a lookup without the account
    // would pick, one caller reads the other account's shard.
    for (caller, foreign_shard, own_shard) in [
        (&own, &other_shard, &own_shard),
        (&other, &own_shard, &other_shard),
    ] {
        for arn in [
            stream_arn(REGION, &own.account_id, "SameName", LABEL),
            stream_arn(REGION, &other.account_id, "SameName", LABEL),
        ] {
            assert_stream_not_found(
                engine
                    .get_stream_records(&caller.account_id, &arn, foreign_shard, None, 10)
                    .await,
                &arn,
            );
            let (records, _) = engine
                .get_stream_records(&caller.account_id, &arn, own_shard, None, 10)
                .await
                .expect("own shard reads");
            assert_eq!(records.len(), 1);
        }
    }
}

/// The table takes a new stream label while keeping its shard rows: the old
/// ARN's iterator is refused and the new ARN reads every record.
#[tokio::test]
async fn test_an_earlier_label_on_the_same_shards_is_not_found() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let (t, _) = setup_stream_table(&engine, "Relabel").await;
    relabel(&engine, &t, LABEL).await;
    put(&engine, &t, "pk-r", 1).await;
    let shard = shard_for("pk-r", &t.table_id);
    let old_arn = arn_of(&t, LABEL);
    engine
        .get_stream_records(&t.account_id, &old_arn, &shard, None, 10)
        .await
        .expect("current label reads");

    relabel(&engine, &t, LATER_LABEL).await;
    put(&engine, &t, "pk-r", 2).await;
    assert_stream_not_found(
        engine
            .get_stream_records(&t.account_id, &old_arn, &shard, None, 10)
            .await,
        &old_arn,
    );
    let (records, _) = engine
        .get_stream_records(&t.account_id, &arn_of(&t, LATER_LABEL), &shard, None, 10)
        .await
        .expect("new label reads");
    assert_eq!(records.len(), 2);
}
