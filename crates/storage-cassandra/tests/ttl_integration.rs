// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for Cassandra TTL against a live Cassandra.
//!
//! Requires Cassandra on 127.0.0.1:9042. There is no Cassandra CI workflow, so
//! these are run locally.

#[path = "common/mod.rs"]
mod helpers;

use extenddb_storage::MetadataEngine;

use crate::helpers::setup_engine;

async fn activate_tables(engine: &extenddb_storage_cassandra::CassandraEngine) {
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    engine
        .process_control_plane_transitions()
        .await
        .expect("process table transitions");
}

async fn ttl_generation(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    account_id: &str,
    table_name: &str,
) -> uuid::Uuid {
    use cdrs_tokio::types::IntoRustByName;
    let query = format!(
        "SELECT ttl_generation FROM {}.tables WHERE account_id = ? AND table_name = ?",
        engine.catalog_keyspace()
    );
    engine
        .session_arc()
        .query_with_values(&query, cdrs_tokio::query_values!(account_id, table_name))
        .await
        .unwrap()
        .response_body()
        .unwrap()
        .into_rows()
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .get_r_by_name("ttl_generation")
        .unwrap()
}

async fn ttl_bucket_count(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    account_id: &str,
    table_id: &str,
) -> usize {
    let query = format!(
        "SELECT generation, bucket, shard FROM {}.ttl_expiration_buckets WHERE table_id = ?",
        engine.account_keyspace(account_id)
    );
    engine
        .session_arc()
        .query_with_values(&query, cdrs_tokio::query_values!(table_id))
        .await
        .unwrap()
        .response_body()
        .unwrap()
        .into_rows()
        .unwrap_or_default()
        .len()
}
async fn ttl_outbox_count(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    account_id: &str,
) -> usize {
    let keyspace = engine.account_keyspace(account_id);
    let mut count = 0usize;
    for partition in 0..64 {
        let query =
            format!("SELECT id FROM {keyspace}.ttl_reconcile_pending WHERE worker_partition = ?");
        count += engine
            .session_arc()
            .query_with_values(&query, cdrs_tokio::query_values!(partition))
            .await
            .unwrap()
            .response_body()
            .unwrap()
            .into_rows()
            .unwrap_or_default()
            .len();
    }
    count
}

#[tokio::test]
async fn test_ttl_metadata_enable_disable_and_listing() {
    use extenddb_core::types::{AttributeValue, TimeToLiveStatus};

    use extenddb_storage::DataEngine;
    use extenddb_storage::error::StorageError;

    let engine = setup_engine().await;
    let table = crate::helpers::TestTable::new(&engine, "TtlMetadata", false).await;
    activate_tables(&engine).await;

    let disabled = engine
        .describe_ttl(&table.key_info.account_id, &table.key_info.table_name)
        .await
        .expect("describe disabled TTL");
    assert_eq!(disabled.time_to_live_status, TimeToLiveStatus::Disabled);
    assert!(disabled.attribute_name.is_none());

    engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            true,
        )
        .await
        .expect("enable TTL metadata");
    engine
        .create_ttl_index(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
        )
        .await
        .expect("backfill TTL queue");
    let first_generation = ttl_generation(
        &engine,
        &table.key_info.account_id,
        &table.key_info.table_name,
    )
    .await;
    let mut queued_item = std::collections::BTreeMap::new();
    queued_item.insert("id".to_owned(), AttributeValue::S("queued".to_owned()));
    queued_item.insert(
        "expires_at".to_owned(),
        AttributeValue::N(
            (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3_600)
                .to_string(),
        ),
    );
    engine
        .put_item(
            &table.key_info,
            queued_item,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("populate TTL queue");
    assert!(
        ttl_bucket_count(
            &engine,
            &table.key_info.account_id,
            &table.key_info.table_id
        )
        .await
            > 0
    );

    let enabled = engine
        .describe_ttl(&table.key_info.account_id, &table.key_info.table_name)
        .await
        .expect("describe enabled TTL");
    assert_eq!(enabled.time_to_live_status, TimeToLiveStatus::Enabled);
    assert_eq!(enabled.attribute_name.as_deref(), Some("expires_at"));
    assert!(
        engine
            .tables_with_ttl(&table.key_info.account_id)
            .await
            .expect("list account TTL tables")
            .iter()
            .any(
                |(name, attribute)| name == &table.key_info.table_name && attribute == "expires_at"
            )
    );
    assert!(
        engine
            .all_tables_with_ttl_index_ready()
            .await
            .expect("list ready TTL tables")
            .iter()
            .any(
                |(account, name, attribute)| account == &table.key_info.account_id
                    && name == &table.key_info.table_name
                    && attribute == "expires_at"
            )
    );

    let sweep_owner = engine
        .acquire_current_ttl_sweep_lease(&table.key_info.account_id, &table.key_info.table_name)
        .await
        .expect("acquire TTL sweep lease")
        .expect("TTL sweep lease applied");
    let blocked_disable = engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            false,
        )
        .await;
    assert!(matches!(
        blocked_disable,
        Err(StorageError::IndexesInUse(_))
    ));
    engine
        .release_ttl_sweep_lease(
            &table.key_info.account_id,
            &table.key_info.table_name,
            sweep_owner,
        )
        .await
        .expect("release TTL sweep lease");

    engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            false,
        )
        .await
        .expect("disable TTL metadata");
    engine
        .drop_ttl_index(&table.key_info.account_id, &table.key_info.table_name)
        .await
        .expect("clear TTL queue");
    assert_eq!(
        ttl_bucket_count(
            &engine,
            &table.key_info.account_id,
            &table.key_info.table_id
        )
        .await,
        0
    );
    let disabled_again = engine
        .describe_ttl(&table.key_info.account_id, &table.key_info.table_name)
        .await
        .expect("describe disabled TTL again");
    assert_eq!(
        disabled_again.time_to_live_status,
        TimeToLiveStatus::Disabled
    );

    engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            true,
        )
        .await
        .expect("re-enable TTL metadata");
    engine
        .create_ttl_index(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
        )
        .await
        .expect("backfill re-enabled TTL queue");
    let second_generation = ttl_generation(
        &engine,
        &table.key_info.account_id,
        &table.key_info.table_name,
    )
    .await;
    assert_ne!(first_generation, second_generation);
}

#[tokio::test]
async fn test_ttl_queue_sweep_and_stale_candidate_protection() {
    use std::sync::Arc;

    use extenddb_core::metrics::MetricsCollector;
    use extenddb_core::types::AttributeValue;
    use extenddb_storage::DataEngine;

    let engine = Arc::new(setup_engine().await);
    let table = crate::helpers::TestTable::new(&engine, "TtlSweep", false).await;
    activate_tables(&engine).await;
    engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            true,
        )
        .await
        .expect("enable TTL");
    engine
        .create_ttl_index(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
        )
        .await
        .expect("prepare TTL queue");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let make_item = |id: &str, ttl: Option<u64>| {
        let mut item = std::collections::BTreeMap::new();
        item.insert("id".to_owned(), AttributeValue::S(id.to_owned()));
        if let Some(ttl) = ttl {
            item.insert("expires_at".to_owned(), AttributeValue::N(ttl.to_string()));
        }
        item
    };

    for item in [
        make_item("expired", Some(now - 10)),
        make_item("future", Some(now + 3_600)),
        make_item("missing", None),
    ] {
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
            .expect("put TTL test item");
    }

    // Replacing an expired timestamp with a future timestamp removes the old
    // queue entry in the same logged batch, so the item must survive the sweep.
    engine
        .put_item(
            &table.key_info,
            make_item("moved", Some(now - 10)),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("put initially expired item");
    engine
        .put_item(
            &table.key_info,
            make_item("moved", Some(now + 3_600)),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("move TTL into future");

    assert!(ttl_outbox_count(&engine, &table.key_info.account_id).await > 0);
    extenddb_storage_cassandra::ttl_worker::reconcile_pending_once(&engine, 1_000)
        .await
        .expect("drain TTL reconciliation outbox");
    assert_eq!(
        ttl_outbox_count(&engine, &table.key_info.account_id).await,
        0
    );

    let candidates = engine
        .find_expired_items_indexed(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            100,
        )
        .await
        .expect("find expired items");
    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].get("id"),
        Some(&AttributeValue::S("expired".to_owned()))
    );

    extenddb_storage_cassandra::ttl_worker::sweep_once(&engine, &MetricsCollector::new()).await;

    for id in ["expired", "future", "missing", "moved"] {
        let mut key = std::collections::BTreeMap::new();
        key.insert("id".to_owned(), AttributeValue::S(id.to_owned()));
        let current = engine
            .get_item(&table.key_info, &key)
            .await
            .expect("get item after TTL sweep");
        if id == "expired" {
            assert!(current.is_none(), "expired item should be deleted");
        } else {
            assert!(current.is_some(), "{id} should survive TTL sweep");
        }
    }
}

#[tokio::test]
async fn test_ttl_sweep_emits_service_remove_stream_record() {
    use std::sync::Arc;

    use cdrs_tokio::types::IntoRustByName;
    use extenddb_core::metrics::MetricsCollector;
    use extenddb_core::types::{
        AttributeDefinition, AttributeValue, CreateTableInput, KeySchemaElement, KeyType,
        ScalarAttributeType, StreamSpecification, StreamViewType,
    };
    use extenddb_storage::{DataEngine, TableEngine};

    let engine = Arc::new(setup_engine().await);
    let account = crate::helpers::TestAccount::new(&engine, "extenddb_ttl_test").await;
    let table_name = "TtlStream";
    engine
        .create_table(
            &account.account_id,
            CreateTableInput {
                table_name: table_name.to_owned(),
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
                local_secondary_indexes: None,
                global_secondary_indexes: None,
                vector_indexes: None,
                billing_mode: None,
                provisioned_throughput: None,
                on_demand_throughput: None,
                sse_specification: None,
                tags: None,
                table_class: None,
                deletion_protection_enabled: None,
            },
        )
        .await
        .expect("create stream-enabled TTL table");
    activate_tables(&engine).await;
    let key_info = engine
        .table_key_info(&account.account_id, table_name)
        .await
        .expect("stream TTL table key info");
    engine
        .update_ttl(&account.account_id, table_name, "expires_at", true)
        .await
        .expect("enable TTL");
    engine
        .create_ttl_index(&account.account_id, table_name, "expires_at")
        .await
        .expect("prepare TTL queue");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut item = std::collections::BTreeMap::new();
    item.insert(
        "id".to_owned(),
        AttributeValue::S("ttl-stream-item".to_owned()),
    );
    item.insert(
        "expires_at".to_owned(),
        AttributeValue::N((now - 10).to_string()),
    );
    item.insert("value".to_owned(), AttributeValue::S("old".to_owned()));
    engine
        .put_item(&key_info, item, false, None, &Default::default(), None)
        .await
        .expect("put expired stream item");

    let account_keyspace = engine.account_keyspace(&account.account_id);
    engine
        .session_arc()
        .query(format!("DROP TABLE {account_keyspace}.stream_records"))
        .await
        .expect("drop stream table for TTL retry test");

    let metrics = MetricsCollector::new();
    extenddb_storage_cassandra::ttl_worker::sweep_once(&engine, &metrics).await;
    let mut key = std::collections::BTreeMap::new();
    key.insert(
        "id".to_owned(),
        AttributeValue::S("ttl-stream-item".to_owned()),
    );
    assert!(
        engine
            .get_item(&key_info, &key)
            .await
            .expect("read item after failed TTL effects")
            .is_some()
    );

    engine
        .session_arc()
        .query(format!(
            "CREATE TABLE {account_keyspace}.stream_records (\
             shard_id text, sequence_number text, table_id text, event_name text, \
             record_data text, created_at timestamp, \
             PRIMARY KEY ((shard_id), sequence_number)) \
             WITH CLUSTERING ORDER BY (sequence_number ASC)"
        ))
        .await
        .expect("recreate stream table for TTL retry");
    tokio::join!(
        extenddb_storage_cassandra::ttl_worker::sweep_once(&engine, &metrics),
        extenddb_storage_cassandra::ttl_worker::sweep_once(&engine, &metrics)
    );
    extenddb_storage_cassandra::ttl_worker::sweep_once(&engine, &metrics).await;
    let shard_id = extenddb_storage_cassandra::stream_util::assign_shard_id(
        "ttl-stream-item",
        &key_info.table_id,
    );
    let query = format!(
        "SELECT event_name, record_data FROM {account_keyspace}.stream_records WHERE shard_id = ?"
    );
    let rows = engine
        .session_arc()
        .query_with_values(&query, cdrs_tokio::query_values!(shard_id.as_str()))
        .await
        .expect("query TTL stream records")
        .response_body()
        .expect("parse TTL stream response")
        .into_rows()
        .unwrap_or_default();
    let remove_records: Vec<String> = rows
        .into_iter()
        .filter_map(|row| {
            let event: String = row.get_r_by_name("event_name").ok()?;
            if event == "Remove" {
                row.get_r_by_name("record_data").ok()
            } else {
                None
            }
        })
        .collect();
    assert_eq!(remove_records.len(), 1);
    let record: serde_json::Value =
        serde_json::from_str(&remove_records[0]).expect("parse TTL stream record JSON");
    assert_eq!(record["userIdentity"]["Type"], "Service");
    assert_eq!(
        record["userIdentity"]["PrincipalId"],
        "dynamodb.amazonaws.com"
    );
    assert_eq!(record["dynamodb"]["OldImage"]["id"]["S"], "ttl-stream-item");

    // A worker that published the record but never completed the exact base
    // delete would satisfy everything above.
    let mut key = extenddb_core::types::Item::new();
    key.insert(
        "id".to_owned(),
        extenddb_core::types::AttributeValue::S("ttl-stream-item".to_owned()),
    );
    assert!(
        extenddb_storage::DataEngine::get_item(engine.as_ref(), &key_info, &key)
            .await
            .unwrap()
            .is_none(),
        "the retried sweep must also complete the base-row delete"
    );
}

#[tokio::test]
async fn test_transactional_write_reconciles_ttl_queue() {
    use extenddb_core::expression::ExpressionMaps;
    use extenddb_core::metrics::MetricsCollector;
    use extenddb_core::types::{AttributeValue, Item, ReturnValuesOnConditionCheckFailure};
    use extenddb_storage::{DataEngine, TransactWriteOp};

    let engine = setup_engine().await;
    let table = crate::helpers::TestTable::new(&engine, "TtlTransaction", false).await;
    activate_tables(&engine).await;
    engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            true,
        )
        .await
        .expect("enable transaction TTL");
    engine
        .create_ttl_index(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
        )
        .await
        .expect("prepare transaction TTL queue");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut item = Item::new();
    item.insert("id".to_owned(), AttributeValue::S("txn-ttl".to_owned()));
    item.insert(
        "expires_at".to_owned(),
        AttributeValue::N((now - 10).to_string()),
    );
    let maps = ExpressionMaps::default();
    engine
        .transact_write_items(
            &[TransactWriteOp::Put {
                key_info: &table.key_info,
                item: &item,
                condition: None,
                maps: &maps,
                return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                stream: None,
            }],
            None,
        )
        .await
        .expect("transactional TTL put");

    assert!(
        engine
            .all_tables_with_ttl_index_ready()
            .await
            .expect("list reconciled TTL queues")
            .iter()
            .any(|(account, name, _)| account == &table.key_info.account_id
                && name == &table.key_info.table_name)
    );
    extenddb_storage_cassandra::ttl_worker::sweep_once(&engine, &MetricsCollector::new()).await;

    let mut key = Item::new();
    key.insert("id".to_owned(), AttributeValue::S("txn-ttl".to_owned()));
    assert!(
        engine
            .get_item(&table.key_info, &key)
            .await
            .expect("read transactional TTL item")
            .is_none()
    );
}

#[tokio::test]
async fn test_ttl_claim_serializes_delayed_writer() {
    use extenddb_core::types::{AttributeValue, Item};
    use extenddb_storage::DataEngine;
    use extenddb_storage::error::StorageError;

    let engine = setup_engine().await;
    let table = crate::helpers::TestTable::new(&engine, "TtlClaimRace", false).await;
    activate_tables(&engine).await;
    engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            true,
        )
        .await
        .unwrap();
    engine
        .create_ttl_index(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
        )
        .await
        .unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut old = Item::new();
    old.insert("id".to_owned(), AttributeValue::S("race".to_owned()));
    old.insert(
        "expires_at".to_owned(),
        AttributeValue::N((now - 10).to_string()),
    );
    engine
        .put_item(
            &table.key_info,
            old.clone(),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .unwrap();
    let mut key = Item::new();
    key.insert("id".to_owned(), AttributeValue::S("race".to_owned()));

    let first_claim = engine
        .acquire_ttl_mutation_claim(&table.key_info, &key, Some(&old))
        .await
        .unwrap();
    let second_claim = engine
        .acquire_ttl_mutation_claim(&table.key_info, &key, Some(&old))
        .await;
    assert!(matches!(
        second_claim,
        Err(StorageError::TransactionConflict(_))
    ));

    let mut renewed = old.clone();
    renewed.insert(
        "expires_at".to_owned(),
        AttributeValue::N((now + 3_600).to_string()),
    );
    let blocked_write = engine
        .put_item(
            &table.key_info,
            renewed.clone(),
            false,
            None,
            &Default::default(),
            None,
        )
        .await;
    // The write retries the claim on a jittered backoff before giving up, and
    // surfaces DynamoDB's canonical single-item conflict error rather than
    // TransactionCanceledException, which PutItem does not have.
    assert!(matches!(
        blocked_write,
        Err(StorageError::TransactionConflict(_))
    ));

    engine
        .release_ttl_mutation_claim(&table.key_info, &key, first_claim)
        .await;
    engine
        .put_item(
            &table.key_info,
            renewed.clone(),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("renewed write succeeds after claim release");

    let mut follow_up = renewed.clone();
    follow_up.insert("value".to_owned(), AttributeValue::S("second".to_owned()));
    engine
        .put_item(
            &table.key_info,
            follow_up.clone(),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("successful put releases its TTL claim immediately");
    engine
        .delete_item(
            &table.key_info,
            &key,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("delete succeeds after immediate put follow-up");
    assert!(
        engine
            .get_item(&table.key_info, &key)
            .await
            .expect("read after claimed delete")
            .is_none(),
        "successful delete must not leave a claim-only row"
    );
    engine
        .put_item(
            &table.key_info,
            follow_up,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("successful delete releases its exact TTL claim immediately");
}

/// Two ordinary writers contending for the same key on a TTL-enabled table must
/// both succeed: the base-row claim is an internal serialisation device, not a
/// client-visible failure mode. DynamoDB has no conflict error for concurrent
/// unconditional `PutItem`s, so losing the claim has to be retried internally
/// against a freshly read image rather than surfaced.
#[tokio::test]
async fn test_concurrent_ordinary_writes_on_ttl_table_both_succeed() {
    use extenddb_core::types::{AttributeValue, Item};
    use extenddb_storage::DataEngine;

    let engine = std::sync::Arc::new(setup_engine().await);
    let table = crate::helpers::TestTable::new(&engine, "TtlConcurrentPut", false).await;
    activate_tables(&engine).await;
    engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            true,
        )
        .await
        .unwrap();
    engine
        .create_ttl_index(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
        )
        .await
        .unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let write = |value: &str| {
        let engine = engine.clone();
        let key_info = table.key_info.clone();
        let value = value.to_owned();
        async move {
            let mut item = Item::new();
            item.insert("id".to_owned(), AttributeValue::S("contended".to_owned()));
            item.insert(
                "expires_at".to_owned(),
                AttributeValue::N((now + 3_600).to_string()),
            );
            item.insert("value".to_owned(), AttributeValue::S(value));
            engine
                .put_item(&key_info, item, false, None, &Default::default(), None)
                .await
        }
    };

    let (first, second) = tokio::join!(write("a"), write("b"));
    first.expect("first concurrent write succeeds");
    second.expect("second concurrent write succeeds");

    // Sustained contention, so the collision is hit on the pre-read as well as
    // on the claim LWT. Every writer must still succeed.
    let mut wave = Vec::new();
    for index in 0..8 {
        wave.push(write(&format!("wave-{index}")));
    }
    for (index, result) in futures::future::join_all(wave)
        .await
        .into_iter()
        .enumerate()
    {
        result.unwrap_or_else(|error| {
            panic!("contended write {index} must not surface a conflict: {error}")
        });
    }

    let mut key = Item::new();
    key.insert("id".to_owned(), AttributeValue::S("contended".to_owned()));
    let stored = engine
        .get_item(&table.key_info, &key)
        .await
        .expect("read after contended writes")
        .expect("one of the writes is durable");
    let expected: Vec<String> = std::iter::once("a".to_owned())
        .chain(std::iter::once("b".to_owned()))
        .chain((0..8).map(|index| format!("wave-{index}")))
        .collect();
    let stored_value = match stored.get("value") {
        Some(AttributeValue::S(value)) => value.clone(),
        other => panic!("expected a string value, got {other:?}"),
    };
    assert!(
        expected.contains(&stored_value),
        "the durable value must be one an actual writer wrote, got {stored_value:?}"
    );
    assert!(
        stored.contains_key("expires_at"),
        "the winning write must have kept the TTL attribute"
    );
}

/// Manufacture a durable queue row in `state` owning `work_id`, and put the
/// matching exact claim on the base row, as a crashed worker would leave them.
/// Returns the queue row's clustering key.
async fn forge_ttl_work(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    key_info: &extenddb_core::types::TableKeyInfo,
    old_item: &extenddb_core::types::Item,
    state: &str,
    work_id: uuid::Uuid,
) -> (uuid::Uuid, i64, i32, i64, String, String) {
    use cdrs_tokio::types::IntoRustByName;

    let generation = ttl_generation(engine, &key_info.account_id, &key_info.table_name).await;
    let keyspace = engine.account_keyspace(&key_info.account_id);
    let bucket_row = engine
        .session_arc()
        .query_with_values(
            &format!(
                "SELECT bucket, shard FROM {keyspace}.ttl_expiration_buckets \
                 WHERE table_id = ? AND generation = ?"
            ),
            cdrs_tokio::query_values!(key_info.table_id.as_str(), generation),
        )
        .await
        .unwrap()
        .response_body()
        .unwrap()
        .into_rows()
        .unwrap_or_default()
        .into_iter()
        .next()
        .expect("TTL bucket for active generation");
    let bucket: i64 = bucket_row.get_r_by_name("bucket").unwrap();
    let shard: i32 = bucket_row.get_r_by_name("shard").unwrap();

    let queue_row = engine
        .session_arc()
        .query_with_values(
            &format!(
                "SELECT expires_at, key_hash, key_data FROM {keyspace}.ttl_expirations \
                 WHERE table_id = ? AND generation = ? AND bucket = ? AND shard = ?"
            ),
            cdrs_tokio::query_values!(key_info.table_id.as_str(), generation, bucket, shard),
        )
        .await
        .unwrap()
        .response_body()
        .unwrap()
        .into_rows()
        .unwrap_or_default()
        .into_iter()
        .next()
        .expect("pending TTL entry");
    let expires_at: i64 = queue_row.get_r_by_name("expires_at").unwrap();
    let key_hash: String = queue_row.get_r_by_name("key_hash").unwrap();
    let key_data: String = queue_row.get_r_by_name("key_data").unwrap();

    let work_data = serde_json::json!({
        "old_item": old_item,
        "delete_timestamp_ms": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        "stream": null
    })
    .to_string();
    engine
        .session_arc()
        .query_with_values(
            &format!(
                "UPDATE {keyspace}.ttl_expirations SET state = ?, work_id = ?, work_data = ? \
                 WHERE table_id = ? AND generation = ? AND bucket = ? AND shard = ? \
                 AND expires_at = ? AND key_hash = ? AND key_data = ?"
            ),
            cdrs_tokio::query_values!(
                state,
                work_id,
                work_data.as_str(),
                key_info.table_id.as_str(),
                generation,
                bucket,
                shard,
                expires_at,
                key_hash.as_str(),
                key_data.as_str()
            ),
        )
        .await
        .unwrap();

    let data_table = format!("items_{}", key_info.table_id.replace('-', "_"));
    let pk = extenddb_storage::util::composite_pk_to_text(old_item, &key_info.key_schema).unwrap();
    engine
        .session_arc()
        .query_with_values(
            &format!("UPDATE {keyspace}.{data_table} SET prepared_txn_id = ? WHERE pk = ?"),
            cdrs_tokio::query_values!(work_id, pk.as_str()),
        )
        .await
        .unwrap();

    (generation, bucket, shard, expires_at, key_hash, key_data)
}

async fn base_row_owner(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    key_info: &extenddb_core::types::TableKeyInfo,
    item: &extenddb_core::types::Item,
) -> Option<uuid::Uuid> {
    use cdrs_tokio::types::IntoRustByName;
    let keyspace = engine.account_keyspace(&key_info.account_id);
    let data_table = format!("items_{}", key_info.table_id.replace('-', "_"));
    let pk = extenddb_storage::util::composite_pk_to_text(item, &key_info.key_schema).unwrap();
    engine
        .session_arc()
        .query_with_values(
            &format!("SELECT prepared_txn_id FROM {keyspace}.{data_table} WHERE pk = ?"),
            cdrs_tokio::query_values!(pk.as_str()),
        )
        .await
        .unwrap()
        .response_body()
        .unwrap()
        .into_rows()
        .unwrap_or_default()
        .into_iter()
        .next()
        .and_then(|row| row.get_by_name("prepared_txn_id").ok().flatten())
}

async fn ttl_queue_row_count(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    account_id: &str,
    table_id: &str,
    generation: uuid::Uuid,
    bucket: i64,
    shard: i32,
) -> usize {
    let keyspace = engine.account_keyspace(account_id);
    engine
        .session_arc()
        .query_with_values(
            &format!(
                "SELECT key_hash FROM {keyspace}.ttl_expirations WHERE table_id = ? \
                 AND generation = ? AND bucket = ? AND shard = ?"
            ),
            cdrs_tokio::query_values!(table_id, generation, bucket, shard),
        )
        .await
        .unwrap()
        .response_body()
        .unwrap()
        .into_rows()
        .unwrap_or_default()
        .len()
}

/// Set up a TTL-enabled table holding one already-expired item.
async fn ttl_table_with_expired_item(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    table_name: &str,
    age_seconds: i64,
) -> (crate::helpers::TestTable, extenddb_core::types::Item) {
    use extenddb_core::types::{AttributeValue, Item};
    use extenddb_storage::DataEngine;

    let table = crate::helpers::TestTable::new(engine, table_name, false).await;
    activate_tables(engine).await;
    engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            true,
        )
        .await
        .unwrap();
    engine
        .create_ttl_index(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
        )
        .await
        .unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let mut item = Item::new();
    item.insert("id".to_owned(), AttributeValue::S("drain".to_owned()));
    item.insert(
        "expires_at".to_owned(),
        AttributeValue::N((now - age_seconds).to_string()),
    );
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
    (table, item)
}

/// Retiring a generation must not delete a CLAIMED row and walk away: that would
/// strand the base-row claim it owns. Nothing externally visible has happened
/// yet, so the claim is released and the work abandoned — disabling TTL stops
/// the deletion rather than completing it.
#[tokio::test]
async fn test_disable_drains_claimed_work_and_releases_its_claim() {
    use extenddb_storage::DataEngine;

    let engine = setup_engine().await;
    let (table, item) = ttl_table_with_expired_item(&engine, "TtlDrainClaimed", 10).await;

    let work_id = uuid::Uuid::new_v4();
    let (generation, bucket, shard, ..) =
        forge_ttl_work(&engine, &table.key_info, &item, "CLAIMED", work_id).await;
    assert_eq!(
        base_row_owner(&engine, &table.key_info, &item).await,
        Some(work_id),
        "precondition: the forged claim is held"
    );

    engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            false,
        )
        .await
        .expect("disable succeeds even with claimed work in flight");

    assert_eq!(
        base_row_owner(&engine, &table.key_info, &item).await,
        None,
        "draining a CLAIMED row must release the base-row claim it owned"
    );
    assert_eq!(
        ttl_queue_row_count(
            &engine,
            &table.key_info.account_id,
            &table.key_info.table_id,
            generation,
            bucket,
            shard
        )
        .await,
        0,
        "the abandoned queue row must be removed"
    );

    let mut key = extenddb_core::types::Item::new();
    key.insert(
        "id".to_owned(),
        extenddb_core::types::AttributeValue::S("drain".to_owned()),
    );
    assert!(
        engine
            .get_item(&table.key_info, &key)
            .await
            .unwrap()
            .is_some(),
        "disabling TTL must stop the deletion, not complete it"
    );
}

/// The one phase that must go forward. At EFFECTS_APPLIED the index rows are
/// already deleted and the REMOVE record already published, so abandoning the
/// work would leave a live item with a missing index. Cleanup completes the base
/// delete instead.
#[tokio::test]
async fn test_disable_completes_effects_applied_work() {
    use extenddb_storage::DataEngine;

    let engine = setup_engine().await;
    let (table, item) = ttl_table_with_expired_item(&engine, "TtlDrainEffects", 10).await;

    let work_id = uuid::Uuid::new_v4();
    let (generation, bucket, shard, ..) =
        forge_ttl_work(&engine, &table.key_info, &item, "EFFECTS_APPLIED", work_id).await;

    engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            false,
        )
        .await
        .expect("disable succeeds with effects-applied work in flight");

    let mut key = extenddb_core::types::Item::new();
    key.insert(
        "id".to_owned(),
        extenddb_core::types::AttributeValue::S("drain".to_owned()),
    );
    assert!(
        engine
            .get_item(&table.key_info, &key)
            .await
            .unwrap()
            .is_none(),
        "EFFECTS_APPLIED work must complete its base delete, not be abandoned"
    );
    assert_eq!(
        ttl_queue_row_count(
            &engine,
            &table.key_info.account_id,
            &table.key_info.table_id,
            generation,
            bucket,
            shard
        )
        .await,
        0,
        "the completed queue row must be removed"
    );
}

/// A bucket registration is retired once its day is fully past and its partition
/// is confirmed empty at quorum, so per-cycle sweep fan-out stops growing with
/// the age of the table.
#[tokio::test]
async fn test_drained_past_bucket_registration_is_retired() {
    use extenddb_core::metrics::MetricsCollector;

    let engine = setup_engine().await;
    // Two days old, so the entry lands in a bucket strictly before today's.
    let (table, _item) =
        ttl_table_with_expired_item(&engine, "TtlBucketRetire", 2 * 86_400 + 60).await;

    let metrics = MetricsCollector::new();
    assert!(
        ttl_bucket_count(
            &engine,
            &table.key_info.account_id,
            &table.key_info.table_id
        )
        .await
            > 0,
        "precondition: the past bucket is registered"
    );

    // First sweep deletes the item; the second observes the drained partition.
    extenddb_storage_cassandra::ttl_worker::sweep_once(&engine, &metrics).await;
    extenddb_storage_cassandra::ttl_worker::sweep_once(&engine, &metrics).await;

    // Retirement is only correct if the partition really drained. Assert the
    // item and its queue row are gone first, so a sweep that retired the
    // registration while leaving work behind fails here rather than passing.
    let mut key = extenddb_core::types::Item::new();
    key.insert(
        "id".to_owned(),
        extenddb_core::types::AttributeValue::S("drain".to_owned()),
    );
    assert!(
        extenddb_storage::DataEngine::get_item(&engine, &table.key_info, &key)
            .await
            .unwrap()
            .is_none(),
        "the expired item must have been deleted by the sweep"
    );
    assert_eq!(
        ttl_bucket_count(
            &engine,
            &table.key_info.account_id,
            &table.key_info.table_id
        )
        .await,
        0,
        "a drained past bucket's registration must be retired"
    );
}

#[tokio::test]
async fn test_ttl_sweep_removes_synchronous_gsi_entry() {
    use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, PathElement};
    use extenddb_core::metrics::MetricsCollector;
    use extenddb_core::types::AttributeValue;
    use extenddb_storage::DataEngine;

    let engine = setup_engine().await;
    let table =
        crate::helpers::TestTable::with_gsi(&engine, "TtlGsiCleanup", "StatusIndex", "status")
            .await;
    activate_tables(&engine).await;
    engine
        .session_arc()
        .query_with_values(
            &format!(
                "UPDATE {}.indexes SET propagation_delay_ms = 0 \
                 WHERE table_id = ? AND index_name = ?",
                engine.catalog_keyspace()
            ),
            cdrs_tokio::query_values!(table.key_info.table_id.as_str(), "StatusIndex"),
        )
        .await
        .unwrap();
    engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            true,
        )
        .await
        .unwrap();
    engine
        .create_ttl_index(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
        )
        .await
        .unwrap();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut item = std::collections::BTreeMap::new();
    item.insert("id".to_owned(), AttributeValue::S("ttl-gsi".to_owned()));
    item.insert("status".to_owned(), AttributeValue::S("expired".to_owned()));
    item.insert(
        "expires_at".to_owned(),
        AttributeValue::N((now - 10).to_string()),
    );
    engine
        .put_item(
            &table.key_info,
            item,
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .unwrap();

    let condition = KeyCondition {
        pk_path: vec![PathElement::Attribute("status".to_owned())],
        pk_value: Expr::Placeholder(":status".to_owned()),
        sk_condition: None,
        extra_pk_conditions: Vec::new(),
        extra_sk_conditions: Vec::new(),
    };
    let mut maps = ExpressionMaps::default();
    maps.values.insert(
        ":status".to_owned(),
        AttributeValue::S("expired".to_owned()),
    );
    assert_eq!(
        engine
            .query(
                &table.key_info,
                &condition,
                &maps,
                true,
                None,
                None,
                Some("StatusIndex"),
            )
            .await
            .unwrap()
            .0
            .len(),
        1
    );

    extenddb_storage_cassandra::ttl_worker::sweep_once(&engine, &MetricsCollector::new()).await;
    assert!(
        engine
            .query(
                &table.key_info,
                &condition,
                &maps,
                true,
                None,
                None,
                Some("StatusIndex"),
            )
            .await
            .unwrap()
            .0
            .is_empty()
    );
}

#[tokio::test]
async fn test_ttl_enable_rejects_asynchronous_gsi() {
    use extenddb_storage::error::StorageError;

    let engine = setup_engine().await;
    let table =
        crate::helpers::TestTable::with_gsi(&engine, "TtlAsyncGsi", "StatusIndex", "status").await;
    activate_tables(&engine).await;
    engine
        .session_arc()
        .query_with_values(
            &format!(
                "UPDATE {}.indexes SET propagation_delay_ms = 1000 \
                 WHERE table_id = ? AND index_name = ?",
                engine.catalog_keyspace()
            ),
            cdrs_tokio::query_values!(table.key_info.table_id.as_str(), "StatusIndex"),
        )
        .await
        .unwrap();

    let result = engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            true,
        )
        .await;
    assert!(
        matches!(result, Err(StorageError::Validation(message)) if message.contains("asynchronously propagated GSIs"))
    );
}

#[tokio::test]
async fn test_ttl_reconciles_same_expiry_after_queue_only_claim() {
    use cdrs_tokio::types::IntoRustByName;
    use extenddb_core::metrics::MetricsCollector;
    use extenddb_core::types::{AttributeValue, Item};
    use extenddb_storage::DataEngine;

    let engine = setup_engine().await;
    let table = crate::helpers::TestTable::new(&engine, "TtlQueueClaimRace", false).await;
    activate_tables(&engine).await;
    engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            true,
        )
        .await
        .unwrap();
    engine
        .create_ttl_index(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
        )
        .await
        .unwrap();

    let expires_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 10;
    let mut old = Item::new();
    old.insert("id".to_owned(), AttributeValue::S("queue-race".to_owned()));
    old.insert(
        "expires_at".to_owned(),
        AttributeValue::N(expires_at.to_string()),
    );
    old.insert("value".to_owned(), AttributeValue::S("old".to_owned()));
    engine
        .put_item(
            &table.key_info,
            old.clone(),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .unwrap();

    let generation = ttl_generation(
        &engine,
        &table.key_info.account_id,
        &table.key_info.table_name,
    )
    .await;
    let keyspace = engine.account_keyspace(&table.key_info.account_id);
    let bucket_rows = engine
        .session_arc()
        .query_with_values(
            &format!(
                "SELECT generation, bucket, shard FROM {keyspace}.ttl_expiration_buckets \
                 WHERE table_id = ?"
            ),
            cdrs_tokio::query_values!(table.key_info.table_id.as_str()),
        )
        .await
        .unwrap()
        .response_body()
        .unwrap()
        .into_rows()
        .unwrap_or_default();
    let bucket_row = bucket_rows
        .into_iter()
        .find(|row| {
            let row_generation: uuid::Uuid = row.get_r_by_name("generation").unwrap();
            row_generation == generation
        })
        .expect("TTL bucket for active generation");
    let bucket: i64 = bucket_row.get_r_by_name("bucket").unwrap();
    let shard: i32 = bucket_row.get_r_by_name("shard").unwrap();
    let queue_row = engine
        .session_arc()
        .query_with_values(
            &format!(
                "SELECT expires_at, key_hash, key_data FROM {keyspace}.ttl_expirations \
                 WHERE table_id = ? AND generation = ? AND bucket = ? AND shard = ?"
            ),
            cdrs_tokio::query_values!(table.key_info.table_id.as_str(), generation, bucket, shard),
        )
        .await
        .unwrap()
        .response_body()
        .unwrap()
        .into_rows()
        .unwrap_or_default()
        .into_iter()
        .next()
        .expect("pending TTL entry");
    let queued_expiry: i64 = queue_row.get_r_by_name("expires_at").unwrap();
    let key_hash: String = queue_row.get_r_by_name("key_hash").unwrap();
    let key_data: String = queue_row.get_r_by_name("key_data").unwrap();
    let work_id = uuid::Uuid::new_v4();
    let work_data = serde_json::json!({
        "old_item": old,
        "delete_timestamp_ms": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        "stream": null
    })
    .to_string();

    let mut updated = old.clone();
    updated.insert("value".to_owned(), AttributeValue::S("new".to_owned()));
    engine
        .put_item(
            &table.key_info,
            updated,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("same-expiry update commits and records durable reconciliation work");
    assert!(ttl_outbox_count(&engine, &table.key_info.account_id).await > 0);

    // Reproduce the race endpoint: the ordinary batch committed with its older
    // request timestamp, while a queue-only claim with old work data is newer.
    engine
        .session_arc()
        .query_with_values(
            &format!(
                "UPDATE {keyspace}.ttl_expirations SET state = 'CLAIMED', work_id = ?, \
                 work_data = ? WHERE table_id = ? AND generation = ? AND bucket = ? \
                 AND shard = ? AND expires_at = ? AND key_hash = ? AND key_data = ?"
            ),
            cdrs_tokio::query_values!(
                work_id,
                work_data.as_str(),
                table.key_info.table_id.as_str(),
                generation,
                bucket,
                shard,
                queued_expiry,
                key_hash.as_str(),
                key_data.as_str()
            ),
        )
        .await
        .unwrap();

    extenddb_storage_cassandra::ttl_worker::reconcile_pending_once(&engine, 1_000)
        .await
        .expect("conflicting outbox reconciliation is retryable");
    assert!(
        ttl_outbox_count(&engine, &table.key_info.account_id).await > 0,
        "outbox must remain while old claimed queue work owns the key"
    );

    extenddb_storage_cassandra::ttl_worker::sweep_once(&engine, &MetricsCollector::new()).await;
    extenddb_storage_cassandra::ttl_worker::reconcile_pending_once(&engine, 1_000)
        .await
        .expect("reconcile current image after stale work retires");
    assert_eq!(
        ttl_outbox_count(&engine, &table.key_info.account_id).await,
        0
    );
    extenddb_storage_cassandra::ttl_worker::sweep_once(&engine, &MetricsCollector::new()).await;

    let mut key = Item::new();
    key.insert("id".to_owned(), AttributeValue::S("queue-race".to_owned()));
    assert!(
        engine
            .get_item(&table.key_info, &key)
            .await
            .expect("read eventually expired same-TTL update")
            .is_none()
    );

    // Reproduce a stale old-timestamp delete leaving only the newer permanent
    // TTL work owner. Recovery must release that exact owner before retiring work.
    engine
        .session_arc()
        .query_with_values(
            &format!(
                "INSERT INTO {keyspace}.ttl_expirations \
                 (table_id, generation, bucket, shard, expires_at, key_hash, key_data, \
                  state, work_id, work_data) VALUES (?, ?, ?, ?, ?, ?, ?, 'CLAIMED', ?, ?)"
            ),
            cdrs_tokio::query_values!(
                table.key_info.table_id.as_str(),
                generation,
                bucket,
                shard,
                queued_expiry,
                key_hash.as_str(),
                key_data.as_str(),
                work_id,
                work_data.as_str()
            ),
        )
        .await
        .unwrap();
    let data_table = format!("items_{}", table.key_info.table_id.replace('-', "_"));
    let stored_pk =
        extenddb_storage::util::composite_pk_to_text(&key, &table.key_info.key_schema).unwrap();
    engine
        .session_arc()
        .query_with_values(
            &format!(
                "UPDATE {keyspace}.{data_table} SET prepared_txn_id = ?, \
                 prepared_txn_timestamp = ? WHERE pk = ?"
            ),
            cdrs_tokio::query_values!(
                work_id,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
                stored_pk.as_str()
            ),
        )
        .await
        .unwrap();

    extenddb_storage_cassandra::ttl_worker::sweep_once(&engine, &MetricsCollector::new()).await;
    let mut recreated = key.clone();
    recreated.insert(
        "expires_at".to_owned(),
        AttributeValue::N(
            (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3_600)
                .to_string(),
        ),
    );
    engine
        .put_item(
            &table.key_info,
            recreated.clone(),
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .expect("worker releases orphaned exact work owner before retiring queue work");
    // Prove the recreation is durable and unclaimed, not merely accepted.
    let stored = engine
        .get_item(&table.key_info, &key)
        .await
        .unwrap()
        .expect("the recreated item must be durable");
    assert_eq!(stored.get("expires_at"), recreated.get("expires_at"));
    assert_eq!(
        base_row_owner(&engine, &table.key_info, &recreated).await,
        None,
        "the orphaned exact owner must have been released"
    );
}

#[tokio::test]
async fn test_ttl_update_recreates_logically_absent_item() {
    use extenddb_core::expression::{Expr, ExpressionMaps, PathElement, UpdateAction};
    use extenddb_core::types::{AttributeValue, Item};
    use extenddb_storage::DataEngine;

    let engine = setup_engine().await;
    let table = crate::helpers::TestTable::new(&engine, "TtlUpdateAbsent", false).await;
    activate_tables(&engine).await;
    engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            true,
        )
        .await
        .unwrap();
    engine
        .create_ttl_index(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
        )
        .await
        .unwrap();

    let mut key = Item::new();
    key.insert("id".to_owned(), AttributeValue::S("absent".to_owned()));
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3_600;
    let actions = vec![
        UpdateAction::Set {
            path: vec![PathElement::Attribute("value".to_owned())],
            value: Expr::Placeholder("value".to_owned()),
        },
        UpdateAction::Set {
            path: vec![PathElement::Attribute("expires_at".to_owned())],
            value: Expr::Placeholder("expires".to_owned()),
        },
    ];
    let maps = ExpressionMaps::new(
        std::collections::HashMap::new(),
        std::collections::HashMap::from([
            ("value".to_owned(), AttributeValue::S("first".to_owned())),
            ("expires".to_owned(), AttributeValue::N(future.to_string())),
        ]),
    );
    engine
        .update_item(
            &table.key_info,
            &key,
            &actions,
            false,
            false,
            None,
            &maps,
            None,
        )
        .await
        .expect("TTL UpdateItem creates an absent row through an exact claim");
    // Without this the test would pass on a no-op first update followed by a
    // no-op delete, which is the failure it exists to catch.
    let created = engine
        .get_item(&table.key_info, &key)
        .await
        .unwrap()
        .expect("the first update must have created the item");
    assert_eq!(
        created.get("value"),
        Some(&AttributeValue::S("first".to_owned()))
    );
    engine
        .delete_item(
            &table.key_info,
            &key,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .unwrap();
    assert!(
        engine
            .get_item(&table.key_info, &key)
            .await
            .unwrap()
            .is_none(),
        "the delete must have removed it before the recreation is exercised"
    );

    let recreate_maps = ExpressionMaps::new(
        std::collections::HashMap::new(),
        std::collections::HashMap::from([
            ("value".to_owned(), AttributeValue::S("second".to_owned())),
            ("expires".to_owned(), AttributeValue::N(future.to_string())),
        ]),
    );
    engine
        .update_item(
            &table.key_info,
            &key,
            &actions,
            false,
            false,
            None,
            &recreate_maps,
            None,
        )
        .await
        .expect("TTL UpdateItem recreates a row that retains partition metadata");
    let item = engine
        .get_item(&table.key_info, &key)
        .await
        .unwrap()
        .expect("recreated item exists");
    assert_eq!(
        item.get("value"),
        Some(&AttributeValue::S("second".to_owned()))
    );
}

#[tokio::test]
async fn test_conditional_put_recreates_metadata_only_row() {
    use extenddb_core::expression::{Expr, PathElement};
    use extenddb_core::types::{AttributeValue, Item};
    use extenddb_storage::DataEngine;

    let engine = setup_engine().await;
    let table = crate::helpers::TestTable::new(&engine, "ConditionalPutMetadata", false).await;
    activate_tables(&engine).await;
    let mut item = Item::new();
    item.insert("id".to_owned(), AttributeValue::S("key".to_owned()));
    item.insert("value".to_owned(), AttributeValue::S("old".to_owned()));
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
    let mut key = Item::new();
    key.insert("id".to_owned(), AttributeValue::S("key".to_owned()));
    engine
        .delete_item(
            &table.key_info,
            &key,
            false,
            None,
            &Default::default(),
            None,
        )
        .await
        .unwrap();

    item.insert("value".to_owned(), AttributeValue::S("new".to_owned()));
    let condition = Expr::Function {
        name: "attribute_not_exists".to_owned(),
        args: vec![Expr::Path(vec![PathElement::Attribute("id".to_owned())])],
    };
    engine
        .put_item(
            &table.key_info,
            item,
            false,
            Some(&condition),
            &Default::default(),
            None,
        )
        .await
        .expect("conditional put treats metadata-only row as absent");
    let stored = engine
        .get_item(&table.key_info, &key)
        .await
        .unwrap()
        .expect("the conditional put must be durable");
    assert_eq!(
        stored.get("value"),
        Some(&AttributeValue::S("new".to_owned())),
        "a surviving stale row would satisfy a bare is_some() check"
    );
}

#[tokio::test]
async fn test_non_ttl_put_does_not_enqueue_ttl_reconciliation() {
    use extenddb_core::types::{AttributeValue, Item};
    use extenddb_storage::DataEngine;

    let engine = setup_engine().await;
    let table = crate::helpers::TestTable::new(&engine, "NonTtlPutFastPath", false).await;
    activate_tables(&engine).await;
    let mut item = Item::new();
    item.insert("id".to_owned(), AttributeValue::S("key".to_owned()));
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
    assert_eq!(
        ttl_outbox_count(&engine, &table.key_info.account_id).await,
        0,
        "non-TTL PutItem must not create permanent TTL journal traffic"
    );
}

#[tokio::test]
async fn test_ttl_enabled_table_rejects_new_async_gsi() {
    use extenddb_core::types::{
        AttributeDefinition, CreateGsiAction, GlobalSecondaryIndexUpdate, KeySchemaElement,
        KeyType, Projection, ProjectionType, ScalarAttributeType, UpdateTableInput,
    };
    use extenddb_storage::TableEngine;
    use extenddb_storage::error::StorageError;

    let engine = setup_engine().await;
    let table = crate::helpers::TestTable::new(&engine, "TtlRejectLaterGsi", false).await;
    activate_tables(&engine).await;
    engine
        .update_ttl(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
            true,
        )
        .await
        .unwrap();
    engine
        .create_ttl_index(
            &table.key_info.account_id,
            &table.key_info.table_name,
            "expires_at",
        )
        .await
        .unwrap();

    let result = engine
        .update_table(
            &table.key_info.account_id,
            UpdateTableInput {
                table_name: table.key_info.table_name.clone(),
                billing_mode: None,
                provisioned_throughput: None,
                deletion_protection_enabled: None,
                global_secondary_index_updates: Some(vec![GlobalSecondaryIndexUpdate {
                    create: Some(CreateGsiAction {
                        index_name: "StatusIndex".to_owned(),
                        key_schema: vec![KeySchemaElement {
                            attribute_name: "status".to_owned(),
                            key_type: KeyType::Hash,
                        }],
                        projection: Projection {
                            projection_type: ProjectionType::All,
                            non_key_attributes: None,
                        },
                        provisioned_throughput: None,
                    }),
                    update: None,
                    delete: None,
                }]),
                attribute_definitions: Some(vec![
                    AttributeDefinition {
                        attribute_name: "id".to_owned(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "status".to_owned(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ]),
                stream_specification: None,
                table_class: None,
                on_demand_throughput: None,
                vector_index_updates: None,
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(StorageError::Validation(message))
            if message.contains("asynchronously propagated GSI")
    ));
}
