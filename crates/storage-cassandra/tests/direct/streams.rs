// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Direct integration tests for stream record injection into write batches.

use std::collections::BTreeMap;
use std::sync::Arc;

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::types::{AttributeValue, StreamViewType};
use extenddb_storage::{DataEngine, StreamCapture};
use extenddb_storage_cassandra::CassandraEngine;

use crate::helpers::{TestTable, setup_engine};

fn capture(view_type: StreamViewType) -> StreamCapture {
    StreamCapture {
        view_type,
        user_identity: None,
        region: Arc::from("us-east-1"),
    }
}

/// Create a table with streams enabled and return its key_info + stream_label.
async fn setup_stream_table(
    engine: &CassandraEngine,
    table_name: &str,
) -> (extenddb_core::types::TableKeyInfo, String) {
    use extenddb_core::types::{
        AttributeDefinition, CreateTableInput, KeySchemaElement, KeyType, ScalarAttributeType,
        StreamSpecification, TableKeyInfo,
    };
    use extenddb_storage::TableEngine;

    let account_id = crate::helpers::unique_test_account();
    crate::helpers::ensure_test_account(engine, &account_id)
        .await
        .unwrap();

    let key_schema = vec![KeySchemaElement {
        attribute_name: "id".to_string(),
        key_type: KeyType::Hash,
    }];
    let attribute_definitions = vec![AttributeDefinition {
        attribute_name: "id".to_string(),
        attribute_type: ScalarAttributeType::S,
    }];

    let input = CreateTableInput {
        vector_indexes: None,
        table_throughput_mode: None,
        table_name: table_name.to_string(),
        key_schema: key_schema.clone(),
        attribute_definitions: attribute_definitions.clone(),
        stream_specification: Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        }),
        local_secondary_indexes: None,
        global_secondary_indexes: None,
        billing_mode: None,
        provisioned_throughput: None,
        on_demand_throughput: None,
        sse_specification: None,
        tags: None,
        table_class: None,
        deletion_protection_enabled: None,
    };

    let desc = engine.create_table(&account_id, input).await.unwrap();

    // Fetch stream_label from catalog
    let catalog_keyspace = engine.catalog_keyspace();
    let query = format!(
        "SELECT stream_label FROM {catalog_keyspace}.tables WHERE account_id = ? AND table_name = ?"
    );
    use cdrs_tokio::types::IntoRustByName;
    let result = engine
        .session_arc()
        .query_with_values(
            &query,
            cdrs_tokio::query_values!(account_id.as_str(), table_name),
        )
        .await
        .unwrap();
    let stream_label: String = result
        .response_body()
        .unwrap()
        .into_rows()
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .get_r_by_name("stream_label")
        .unwrap();

    let key_info = TableKeyInfo {
        vector_indexes: Vec::new(),
        table_name: table_name.to_string(),
        account_id,
        table_id: desc.table_id,
        key_schema: key_schema.clone(),
        base_key_schema: key_schema,
        attribute_definitions,
        has_lsi: false,
        global_secondary_indexes: Vec::new(),
        local_secondary_indexes: Vec::new(),
        stream_specification: desc.stream_specification,
    };

    (key_info, stream_label)
}

/// Query stream_records for a given shard and return (event_name, record_data) rows.
async fn fetch_stream_records(
    engine: &CassandraEngine,
    account_id: &str,
    shard_id: &str,
) -> Vec<(String, String)> {
    let keyspace = format!("extenddb_ttl_test_account_{}", account_id);
    let query = format!(
        "SELECT event_name, record_data FROM {}.stream_records WHERE shard_id = ?",
        keyspace
    );
    let result = engine
        .session_arc()
        .query_with_values(&query, cdrs_tokio::query_values!(shard_id))
        .await
        .unwrap();
    let body = result.response_body().unwrap();
    body.into_rows()
        .unwrap_or_default()
        .into_iter()
        .map(|row| {
            let event: String = row.get_r_by_name("event_name").unwrap();
            let data: String = row.get_r_by_name("record_data").unwrap();
            (event, data)
        })
        .collect()
}

fn shard_for(pk: &str, table_id: &str) -> String {
    extenddb_storage_cassandra::stream_util::assign_shard_id(pk, table_id)
}

#[tokio::test]
async fn test_put_item_insert_writes_stream_record() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "StreamPutInsert", false).await;

    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("pk-1".to_string()));
    item.insert("val".to_string(), AttributeValue::S("hello".to_string()));

    engine
        .put_item(
            &table.key_info,
            item,
            false,
            None,
            &Default::default(),
            Some(&capture(StreamViewType::NewAndOldImages)),
        )
        .await
        .unwrap();

    let shard_id = shard_for("pk-1", &table.key_info.table_id);
    let records = fetch_stream_records(&engine, &table.key_info.account_id, &shard_id).await;

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].0, "Insert");
    let data: serde_json::Value = serde_json::from_str(&records[0].1).unwrap();
    assert_eq!(data["eventName"], "INSERT");
    assert!(data["dynamodb"]["NewImage"].is_object());
    assert!(data["dynamodb"].get("OldImage").is_none());
}

#[tokio::test]
async fn test_put_item_overwrite_writes_modify_stream_record() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "StreamPutModify", false).await;

    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("pk-2".to_string()));
    item.insert("val".to_string(), AttributeValue::S("v1".to_string()));

    // First write — no stream capture
    engine
        .put_item(
            &table.key_info,
            item.clone(),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .unwrap();

    // Second write — with stream capture
    item.insert("val".to_string(), AttributeValue::S("v2".to_string()));
    engine
        .put_item(
            &table.key_info,
            item,
            false,
            None,
            &Default::default(),
            Some(&capture(StreamViewType::NewAndOldImages)),
        )
        .await
        .unwrap();

    let shard_id = shard_for("pk-2", &table.key_info.table_id);
    let records = fetch_stream_records(&engine, &table.key_info.account_id, &shard_id).await;

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].0, "Modify");
    let data: serde_json::Value = serde_json::from_str(&records[0].1).unwrap();
    assert_eq!(data["eventName"], "MODIFY");
    assert!(data["dynamodb"]["OldImage"].is_object());
    assert!(data["dynamodb"]["NewImage"].is_object());
}

#[tokio::test]
async fn test_delete_item_writes_remove_stream_record() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "StreamDelete", false).await;

    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("pk-3".to_string()));
    item.insert(
        "val".to_string(),
        AttributeValue::S("to-delete".to_string()),
    );

    engine
        .put_item(
            &table.key_info,
            item,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .unwrap();

    let mut key = BTreeMap::new();
    key.insert("id".to_string(), AttributeValue::S("pk-3".to_string()));

    engine
        .delete_item(
            &table.key_info,
            &key,
            false,
            None,
            &Default::default(),
            Some(&capture(StreamViewType::OldImage)),
        )
        .await
        .unwrap();

    let shard_id = shard_for("pk-3", &table.key_info.table_id);
    let records = fetch_stream_records(&engine, &table.key_info.account_id, &shard_id).await;

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].0, "Remove");
    let data: serde_json::Value = serde_json::from_str(&records[0].1).unwrap();
    assert_eq!(data["eventName"], "REMOVE");
    assert!(data["dynamodb"]["OldImage"].is_object());
}

#[tokio::test]
async fn test_delete_nonexistent_item_writes_no_stream_record() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "StreamDeleteMissing", false).await;

    let mut key = BTreeMap::new();
    key.insert("id".to_string(), AttributeValue::S("pk-ghost".to_string()));

    engine
        .delete_item(
            &table.key_info,
            &key,
            false,
            None,
            &Default::default(),
            Some(&capture(StreamViewType::OldImage)),
        )
        .await
        .unwrap();

    let shard_id = shard_for("pk-ghost", &table.key_info.table_id);
    let records = fetch_stream_records(&engine, &table.key_info.account_id, &shard_id).await;
    assert!(records.is_empty());
}

#[tokio::test]
async fn test_keys_only_view_type_omits_images() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "StreamKeysOnly", false).await;

    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("pk-keys".to_string()));
    item.insert("val".to_string(), AttributeValue::S("secret".to_string()));

    engine
        .put_item(
            &table.key_info,
            item,
            false,
            None,
            &Default::default(),
            Some(&capture(StreamViewType::KeysOnly)),
        )
        .await
        .unwrap();

    let shard_id = shard_for("pk-keys", &table.key_info.table_id);
    let records = fetch_stream_records(&engine, &table.key_info.account_id, &shard_id).await;

    assert_eq!(records.len(), 1);
    let data: serde_json::Value = serde_json::from_str(&records[0].1).unwrap();
    assert!(data["dynamodb"].get("NewImage").is_none());
    assert!(data["dynamodb"].get("OldImage").is_none());
    assert!(data["dynamodb"]["Keys"].is_object());
}

#[tokio::test]
async fn test_same_pk_records_land_in_same_shard_with_ordered_sequences() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let engine = setup_engine().await;
    let table = TestTable::new(&engine, "StreamShardConsistency", false).await;

    for i in 0..3u32 {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), AttributeValue::S("same-pk".to_string()));
        item.insert("val".to_string(), AttributeValue::N(i.to_string()));
        engine
            .put_item(
                &table.key_info,
                item,
                false,
                None,
                &Default::default(),
                Some(&capture(StreamViewType::NewImage)),
            )
            .await
            .unwrap();
    }

    let shard_id = shard_for("same-pk", &table.key_info.table_id);
    let records = fetch_stream_records(&engine, &table.key_info.account_id, &shard_id).await;
    assert_eq!(records.len(), 3);

    let seqs: Vec<String> = records
        .iter()
        .map(|(_, data)| {
            let v: serde_json::Value = serde_json::from_str(data).unwrap();
            v["dynamodb"]["SequenceNumber"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    let sorted = {
        let mut s = seqs.clone();
        s.sort();
        s
    };
    assert_eq!(seqs, sorted, "sequence numbers must be in ascending order");
}

// --- Read-side tests ---

#[tokio::test]
async fn test_validate_shard_ok() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use extenddb_storage::StreamEngine;

    let engine = setup_engine().await;
    let (key_info, stream_label) = setup_stream_table(&engine, "ValidateShardOk").await;
    let arn = extenddb_storage::util::stream_arn(
        "us-east-1",
        &key_info.account_id,
        &key_info.table_name,
        &stream_label,
    );
    let shard_id = shard_for("any-pk", &key_info.table_id);

    engine
        .validate_shard(&key_info.account_id, &arn, &shard_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_validate_shard_wrong_arn_fails() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use extenddb_storage::StreamEngine;
    use extenddb_storage::error::StorageError;

    let engine = setup_engine().await;
    let (key_info, _) = setup_stream_table(&engine, "ValidateShardBadArn").await;
    let bad_arn = extenddb_storage::util::stream_arn(
        "us-east-1",
        &key_info.account_id,
        &key_info.table_name,
        "1970-01-01T00:00:00",
    );
    let shard_id = shard_for("any-pk", &key_info.table_id);

    let err = engine
        .validate_shard(&key_info.account_id, &bad_arn, &shard_id)
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::TableNotFound(_)));
}

#[tokio::test]
async fn test_latest_sequence_number_empty_shard() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use extenddb_storage::StreamEngine;

    let engine = setup_engine().await;
    let (key_info, _) = setup_stream_table(&engine, "LatestSeqEmpty").await;
    let shard_id = shard_for("any-pk", &key_info.table_id);

    let seq = engine.latest_sequence_number(&shard_id).await.unwrap();
    assert!(seq.is_none());
}

#[tokio::test]
async fn test_latest_sequence_number_after_write() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use extenddb_storage::StreamEngine;

    let engine = setup_engine().await;
    let (key_info, _) = setup_stream_table(&engine, "LatestSeqAfterWrite").await;

    let mut item = BTreeMap::new();
    item.insert("id".to_string(), AttributeValue::S("pk-seq".to_string()));
    engine
        .put_item(
            &key_info,
            item,
            false,
            None,
            &Default::default(),
            Some(&capture(StreamViewType::NewImage)),
        )
        .await
        .unwrap();

    let shard_id = shard_for("pk-seq", &key_info.table_id);
    let seq = engine.latest_sequence_number(&shard_id).await.unwrap();
    assert!(seq.is_some());
    assert_eq!(seq.unwrap().len(), 23);
}

#[tokio::test]
async fn test_get_stream_records_returns_written_records() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use extenddb_storage::StreamEngine;

    let engine = setup_engine().await;
    let (key_info, _) = setup_stream_table(&engine, "GetStreamRecords").await;

    for i in 0..3u32 {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), AttributeValue::S("pk-read".to_string()));
        item.insert("val".to_string(), AttributeValue::N(i.to_string()));
        engine
            .put_item(
                &key_info,
                item,
                false,
                None,
                &Default::default(),
                Some(&capture(StreamViewType::NewImage)),
            )
            .await
            .unwrap();
    }

    let shard_id = shard_for("pk-read", &key_info.table_id);
    let (records, last_seq) = engine
        .get_stream_records(&key_info.account_id, &shard_id, None, 10)
        .await
        .unwrap();

    assert_eq!(records.len(), 3);
    assert!(last_seq.is_some());
    // All records are for the same PK
    for r in &records {
        assert!(r.dynamodb.keys.contains_key("id"));
    }
}

#[tokio::test]
async fn test_get_stream_records_after_sequence_paginates() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use extenddb_storage::StreamEngine;

    let engine = setup_engine().await;
    let (key_info, _) = setup_stream_table(&engine, "GetStreamRecordsPaginate").await;

    for i in 0..4u32 {
        let mut item = BTreeMap::new();
        item.insert("id".to_string(), AttributeValue::S("pk-page".to_string()));
        item.insert("val".to_string(), AttributeValue::N(i.to_string()));
        engine
            .put_item(
                &key_info,
                item,
                false,
                None,
                &Default::default(),
                Some(&capture(StreamViewType::NewImage)),
            )
            .await
            .unwrap();
    }

    let shard_id = shard_for("pk-page", &key_info.table_id);
    let (first_page, last_seq) = engine
        .get_stream_records(&key_info.account_id, &shard_id, None, 2)
        .await
        .unwrap();
    assert_eq!(first_page.len(), 2);

    let (second_page, _) = engine
        .get_stream_records(&key_info.account_id, &shard_id, last_seq.as_deref(), 10)
        .await
        .unwrap();
    assert_eq!(second_page.len(), 2);

    // No overlap
    let first_seqs: Vec<_> = first_page
        .iter()
        .map(|r| &r.dynamodb.sequence_number)
        .collect();
    let second_seqs: Vec<_> = second_page
        .iter()
        .map(|r| &r.dynamodb.sequence_number)
        .collect();
    assert!(first_seqs.iter().all(|s| !second_seqs.contains(s)));
}

#[tokio::test]
async fn test_describe_stream_returns_shards() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use extenddb_core::types::DescribeStreamInput;
    use extenddb_storage::StreamEngine;

    let engine = setup_engine().await;
    let (key_info, stream_label) = setup_stream_table(&engine, "DescribeStream").await;
    let arn = extenddb_storage::util::stream_arn(
        "us-east-1",
        &key_info.account_id,
        &key_info.table_name,
        &stream_label,
    );

    let desc = engine
        .describe_stream(
            &key_info.account_id,
            &DescribeStreamInput {
                stream_arn: arn.clone(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(desc.stream_arn, arn);
    assert_eq!(desc.table_name, key_info.table_name);
    assert_eq!(desc.shards.len(), 4); // SHARDS_PER_STREAM
    assert!(desc.last_evaluated_shard_id.is_none());
}

#[tokio::test]
async fn test_list_streams_includes_stream_enabled_table() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    use extenddb_storage::StreamEngine;

    let engine = setup_engine().await;
    let (key_info, stream_label) = setup_stream_table(&engine, "ListStreams").await;
    let expected_arn = extenddb_storage::util::stream_arn(
        "us-east-1",
        &key_info.account_id,
        &key_info.table_name,
        &stream_label,
    );

    let (streams, _) = engine
        .list_streams(&key_info.account_id, None, 100, None)
        .await
        .unwrap();

    assert!(streams.iter().any(|s| s.stream_arn == expected_arn));
}
