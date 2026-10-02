// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for GSI/LSI index operations.

use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType,
};
use extenddb_storage_cassandra::CassandraEngine;

use crate::helpers::{ensure_test_account, test_config, unique_test_account, unique_test_id};

#[tokio::test]
async fn test_create_and_drop_index_table() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let config = test_config();
    let engine = CassandraEngine::new(&config, "us-east-1").await.unwrap();
    let account_id = unique_test_account();
    let index_id = unique_test_id();

    ensure_test_account(&engine, &account_id).await.unwrap();

    let account_keyspace = engine.account_keyspace(&account_id);

    // Base table key schema: pk (HASH), sk (RANGE)
    let base_key_schema = vec![
        KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "sk".to_owned(),
            key_type: KeyType::Range,
        },
    ];

    let base_attr_defs = vec![
        AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "sk".to_owned(),
            attribute_type: ScalarAttributeType::N,
        },
    ];

    // Index key schema: gsi_pk (HASH), gsi_sk (RANGE)
    let index_key_schema = vec![
        KeySchemaElement {
            attribute_name: "gsi_pk".to_owned(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "gsi_sk".to_owned(),
            key_type: KeyType::Range,
        },
    ];

    let index_attr_defs = vec![
        AttributeDefinition {
            attribute_name: "gsi_pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "gsi_sk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
    ];

    // Create index table
    engine
        .create_index_data_table(
            &account_keyspace,
            &index_id,
            &index_key_schema,
            &index_attr_defs,
            &base_key_schema,
            &base_attr_defs,
        )
        .await
        .unwrap();

    // Verify table exists by querying system schema
    let table_name = format!("index_{}", index_id.replace("-", "_"));
    let query =
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?"
            .to_string();
    let result = engine
        .session()
        .query_with_values(
            &query,
            cdrs_tokio::query_values!(account_keyspace.as_str(), table_name.as_str()),
        )
        .await
        .unwrap();

    let body = result.response_body().unwrap();
    let rows = body.into_rows().unwrap();
    assert_eq!(rows.len(), 1);

    // Drop index table
    engine
        .drop_index_data_table(&account_keyspace, &index_id)
        .await
        .unwrap();

    // Verify table is gone
    let result = engine
        .session()
        .query_with_values(
            &query,
            cdrs_tokio::query_values!(account_keyspace.as_str(), table_name.as_str()),
        )
        .await
        .unwrap();

    let body = result.response_body().unwrap();
    let rows = body.into_rows().unwrap_or_default();
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_fetch_indexes_for_table() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let config = test_config();
    let engine = CassandraEngine::new(&config, "us-east-1").await.unwrap();
    let account_id = unique_test_account();
    let table_id = unique_test_id();

    ensure_test_account(&engine, &account_id).await.unwrap();

    let catalog_keyspace = engine.catalog_keyspace();

    // Insert test index metadata into catalog
    let index_id = unique_test_id();
    let key_schema = serde_json::json!([
        {"AttributeName": "gsi_pk", "KeyType": "HASH"},
        {"AttributeName": "gsi_sk", "KeyType": "RANGE"}
    ])
    .to_string();

    let projection = serde_json::json!({
        "ProjectionType": "ALL"
    })
    .to_string();

    let insert_query = format!(
        "INSERT INTO {}.indexes (table_id, index_name, index_id, index_type, key_schema, projection, index_status, propagation_delay_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        catalog_keyspace
    );

    engine
        .session()
        .query_with_values(
            &insert_query,
            cdrs_tokio::query_values!(
                table_id.as_str(),
                "test_gsi",
                index_id.as_str(),
                "GSI",
                key_schema.as_str(),
                projection.as_str(),
                "ACTIVE",
                1000
            ),
        )
        .await
        .unwrap();

    // Fetch indexes
    let indexes = extenddb_storage_cassandra::data::index::fetch_indexes_for_table(
        &table_id,
        &engine.session_arc(),
        &catalog_keyspace,
    )
    .await
    .unwrap();

    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].index_name, "test_gsi");
    assert_eq!(indexes[0].index_id, index_id);
    assert_eq!(indexes[0].index_type, "GSI");
    assert_eq!(indexes[0].key_schema.len(), 2);
    assert_eq!(indexes[0].propagation_delay_ms, Some(1000));
    assert_eq!(indexes[0].projection.projection_type, ProjectionType::All);

    // Cleanup
    let delete_query = format!(
        "DELETE FROM {}.indexes WHERE table_id = ? AND index_name = ?",
        catalog_keyspace
    );
    engine
        .session()
        .query_with_values(
            &delete_query,
            cdrs_tokio::query_values!(table_id.as_str(), "test_gsi"),
        )
        .await
        .ok();
}

#[tokio::test]
async fn test_index_table_primary_key_structure() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let config = test_config();
    let engine = CassandraEngine::new(&config, "us-east-1").await.unwrap();
    let account_id = unique_test_account();
    let index_id = unique_test_id();

    ensure_test_account(&engine, &account_id).await.unwrap();

    let account_keyspace = engine.account_keyspace(&account_id);

    // Base table: pk (S), sk (N)
    let base_key_schema = vec![
        KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "sk".to_owned(),
            key_type: KeyType::Range,
        },
    ];

    let base_attr_defs = vec![
        AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "sk".to_owned(),
            attribute_type: ScalarAttributeType::N,
        },
    ];

    // GSI: gsi_pk (S), gsi_sk (S)
    let index_key_schema = vec![
        KeySchemaElement {
            attribute_name: "gsi_pk".to_owned(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "gsi_sk".to_owned(),
            key_type: KeyType::Range,
        },
    ];

    let index_attr_defs = vec![
        AttributeDefinition {
            attribute_name: "gsi_pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "gsi_sk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
    ];

    engine
        .create_index_data_table(
            &account_keyspace,
            &index_id,
            &index_key_schema,
            &index_attr_defs,
            &base_key_schema,
            &base_attr_defs,
        )
        .await
        .unwrap();

    let table_name = format!("index_{}", index_id.replace("-", "_"));

    // Query system schema to verify PRIMARY KEY structure
    let query = "SELECT column_name, kind, position FROM system_schema.columns \
         WHERE keyspace_name = ? AND table_name = ?"
        .to_string();

    let result = engine
        .session()
        .query_with_values(
            &query,
            cdrs_tokio::query_values!(account_keyspace.as_str(), table_name.as_str()),
        )
        .await
        .unwrap();

    let body = result.response_body().unwrap();
    let mut rows = body.into_rows().unwrap();

    // Sort by position in application (can't ORDER BY non-clustering column)
    rows.sort_by_key(|row| {
        use cdrs_tokio::types::IntoRustByName;
        let pos: i32 = row.get_r_by_name("position").unwrap_or(0);
        pos
    });

    // Verify PRIMARY KEY order: (pk) as partition key, then sk_s, base_pk, base_sk_n as clustering
    let mut partition_keys = Vec::new();
    let mut clustering_keys = Vec::new();

    for row in rows {
        use cdrs_tokio::types::IntoRustByName;
        let col_name: String = row.get_r_by_name("column_name").unwrap();
        let kind: String = row.get_r_by_name("kind").unwrap();

        match kind.as_str() {
            "partition_key" => partition_keys.push(col_name),
            "clustering" => clustering_keys.push(col_name),
            _ => {}
        }
    }

    // Verify structure
    assert_eq!(partition_keys, vec!["pk"]);
    assert_eq!(
        clustering_keys,
        vec!["sk_s", "base_pk", "base_sk_n"],
        "Clustering keys should be: index SK (sk_s), then base keys (base_pk, base_sk_n)"
    );

    // Cleanup
    engine
        .drop_index_data_table(&account_keyspace, &index_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_sync_indexes_insert_and_delete() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let config = test_config();
    let engine = CassandraEngine::new(&config, "us-east-1").await.unwrap();
    let account_id = unique_test_account();
    let _table_id = unique_test_id();
    let index_id = unique_test_id();

    ensure_test_account(&engine, &account_id).await.unwrap();

    let account_keyspace = engine.account_keyspace(&account_id);

    // Base table: pk (S), sk (N)
    let base_key_schema = vec![
        KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "sk".to_owned(),
            key_type: KeyType::Range,
        },
    ];

    let base_attr_defs = vec![
        AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "sk".to_owned(),
            attribute_type: ScalarAttributeType::N,
        },
        AttributeDefinition {
            attribute_name: "gsi_pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
    ];

    // GSI: gsi_pk (S) with sync delay
    let index_key_schema = vec![KeySchemaElement {
        attribute_name: "gsi_pk".to_owned(),
        key_type: KeyType::Hash,
    }];

    let index_attr_defs = vec![AttributeDefinition {
        attribute_name: "gsi_pk".to_owned(),
        attribute_type: ScalarAttributeType::S,
    }];

    // Create index table
    engine
        .create_index_data_table(
            &account_keyspace,
            &index_id,
            &index_key_schema,
            &index_attr_defs,
            &base_key_schema,
            &base_attr_defs,
        )
        .await
        .unwrap();

    // Create an index metadata entry
    let indexes = vec![extenddb_storage_cassandra::data::index::IndexMeta {
        index_name: "test_gsi".to_owned(),
        index_id: index_id.clone(),
        index_type: "GSI".to_owned(),
        key_schema: index_key_schema.clone(),
        projection: Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        },
        propagation_delay_ms: Some(0), // Sync
    }];

    // Create a test item
    let mut item = Item::new();
    item.insert("pk".to_owned(), AttributeValue::S("test_pk".to_owned()));
    item.insert("sk".to_owned(), AttributeValue::N("123".to_owned()));
    item.insert(
        "gsi_pk".to_owned(),
        AttributeValue::S("gsi_value".to_owned()),
    );
    item.insert("data".to_owned(), AttributeValue::S("some_data".to_owned()));

    // Test sync_indexes for INSERT
    let mut batch = cdrs_tokio::query::BatchQueryBuilder::new();
    extenddb_storage_cassandra::data::index::sync_indexes(
        &mut batch,
        &account_keyspace,
        &base_key_schema,
        &base_attr_defs,
        &indexes,
        None,
        Some(&item),
        1000,
    )
    .unwrap();

    let built = batch.build().unwrap();
    assert_eq!(
        built.request.queries.len(),
        1,
        "Expected 1 INSERT statement"
    );

    // Execute the batch
    engine.session().batch(built).await.unwrap();

    // Verify the row was inserted
    let idx_table = format!("index_{}", index_id.replace("-", "_"));
    let query = format!(
        "SELECT item_data FROM {}.{} WHERE pk = 'gsi_value' AND base_pk = 'test_pk' AND base_sk_n = 123",
        account_keyspace, idx_table
    );
    println!("SELECT query: {}", query);
    let result = engine.session().query(&query).await.unwrap();
    let body = result.response_body().unwrap();
    let rows = body.into_rows().unwrap();
    assert_eq!(rows.len(), 1);

    // Test sync_indexes for DELETE
    let mut batch = cdrs_tokio::query::BatchQueryBuilder::new();
    extenddb_storage_cassandra::data::index::sync_indexes(
        &mut batch,
        &account_keyspace,
        &base_key_schema,
        &base_attr_defs,
        &indexes,
        Some(&item),
        None,
        1000,
    )
    .unwrap();

    let built = batch.build().unwrap();
    assert_eq!(
        built.request.queries.len(),
        1,
        "Expected 1 DELETE statement"
    );

    // Execute the batch
    engine.session().batch(built).await.unwrap();

    // Verify the row was deleted
    let result = engine.session().query(&query).await.unwrap();
    let body = result.response_body().unwrap();
    let rows = body.into_rows().unwrap_or_default();
    assert_eq!(rows.len(), 0);

    // Cleanup
    engine
        .drop_index_data_table(&account_keyspace, &index_id)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_sync_indexes_skips_async_gsi() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let config = test_config();
    let engine = CassandraEngine::new(&config, "us-east-1").await.unwrap();
    let account_id = unique_test_account();
    let _table_id = unique_test_id();

    ensure_test_account(&engine, &account_id).await.unwrap();

    let account_keyspace = engine.account_keyspace(&account_id);

    let base_key_schema = vec![KeySchemaElement {
        attribute_name: "pk".to_owned(),
        key_type: KeyType::Hash,
    }];

    let base_attr_defs = vec![
        AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "gsi_pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
    ];

    // Async GSI with delay > 0
    let indexes = vec![extenddb_storage_cassandra::data::index::IndexMeta {
        index_name: "async_gsi".to_owned(),
        index_id: unique_test_id(),
        index_type: "GSI".to_owned(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gsi_pk".to_owned(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: ProjectionType::KeysOnly,
            non_key_attributes: None,
        },
        propagation_delay_ms: Some(1000), // Async
    }];

    let mut item = Item::new();
    item.insert("pk".to_owned(), AttributeValue::S("test".to_owned()));
    item.insert("gsi_pk".to_owned(), AttributeValue::S("value".to_owned()));

    // sync_indexes should NOT add any statements for async GSI
    let mut batch = cdrs_tokio::query::BatchQueryBuilder::new();
    extenddb_storage_cassandra::data::index::sync_indexes(
        &mut batch,
        &account_keyspace,
        &base_key_schema,
        &base_attr_defs,
        &indexes,
        None,
        Some(&item),
        500, // System default < GSI delay
    )
    .unwrap();

    let built = batch.build().unwrap();
    assert_eq!(
        built.request.queries.len(),
        0,
        "Async GSI should not generate sync statements"
    );
}

// ── Async GSI queue integration tests ────────────────────────────────────────

use extenddb_core::expression::ExpressionMaps;
use extenddb_storage::DataEngine as _;
use std::sync::Arc;

/// Set the propagation delay for a GSI in the catalog.
async fn set_gsi_delay(engine: &CassandraEngine, table_id: &str, gsi_name: &str, delay_ms: i32) {
    let catalog_keyspace = engine.catalog_keyspace();
    let cql = format!(
        "UPDATE {catalog_keyspace}.indexes SET propagation_delay_ms = ? \
         WHERE table_id = ? AND index_name = ?"
    );
    engine
        .session()
        .query_with_values(
            &cql,
            cdrs_tokio::query_values!(delay_ms, table_id, gsi_name),
        )
        .await
        .expect("set_gsi_delay");
}

/// Count rows in `gsi_pending` for a given account keyspace.
async fn gsi_pending_count(engine: &CassandraEngine, account_keyspace: &str) -> usize {
    // Filter out static-column-only rows (ready_at=null) which persist after
    // all clustering rows are deleted but last_ready_at remains set.
    let cql = format!("SELECT id FROM {account_keyspace}.gsi_pending");
    engine
        .session()
        .query(&cql)
        .await
        .ok()
        .and_then(|f| f.response_body().ok())
        .and_then(|b| b.into_rows())
        .map(|rows| {
            use cdrs_tokio::types::IntoRustByName as _;
            rows.iter()
                .filter(|row| {
                    let id: Result<uuid::Uuid, _> = row.get_r_by_name("id");
                    id.is_ok()
                })
                .count()
        })
        .unwrap_or(0)
}

/// Query a GSI and return the count of matching items.
async fn gsi_query_count(
    engine: &CassandraEngine,
    table: &crate::helpers::TestTable,
    gsi_name: &str,
    gsi_pk_attr: &str,
    gsi_pk_value: &str,
) -> usize {
    use extenddb_core::expression::{Expr, KeyCondition, PathElement};
    let key_condition = KeyCondition {
        pk_path: vec![PathElement::Attribute(gsi_pk_attr.to_string())],
        pk_value: Expr::Placeholder(":v".to_string()),
        sk_condition: None,
        extra_pk_conditions: vec![],
        extra_sk_conditions: vec![],
    };
    let mut maps = ExpressionMaps::default();
    maps.values.insert(
        ":v".to_string(),
        AttributeValue::S(gsi_pk_value.to_string()),
    );
    engine
        .query(
            &table.key_info,
            &key_condition,
            &maps,
            true,
            None,
            None,
            Some(gsi_name),
        )
        .await
        .map(|(items, _)| items.len())
        .unwrap_or(0)
}

#[tokio::test]
async fn test_async_gsi_enqueues_row_atomically() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    // A put_item with an async GSI must write a gsi_pending row in the same
    // batch as the base write — visible immediately, before the worker runs.
    let config = test_config();
    let engine = CassandraEngine::new(&config, "us-east-1").await.unwrap();
    let table =
        crate::helpers::TestTable::with_gsi(&engine, "AsyncGsiEnqueueTable", "GsiIdx", "gpk").await;

    // Long delay: worker will not apply before we check.
    set_gsi_delay(&engine, &table.key_info.table_id, "GsiIdx", 30_000).await;

    let account_keyspace = engine.account_keyspace(&table.key_info.account_id);
    let maps = ExpressionMaps::default();

    let mut item = Item::new();
    item.insert("id".to_string(), AttributeValue::S("x".to_string()));
    item.insert("gpk".to_string(), AttributeValue::S("g1".to_string()));

    engine
        .put_item(&table.key_info, item, false, None, &maps, None)
        .await
        .expect("put_item");

    // Row must be in gsi_pending immediately (same batch as base write).
    let pending = gsi_pending_count(&engine, &account_keyspace).await;
    assert_eq!(
        pending, 1,
        "gsi_pending row not written atomically with base write"
    );

    // GSI must not yet be visible (worker hasn't run).
    let visible = gsi_query_count(&engine, &table, "GsiIdx", "gpk", "g1").await;
    assert_eq!(visible, 0, "GSI entry visible before worker ran");
}

#[tokio::test]
async fn test_async_gsi_worker_convergence() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    // Successive writes to the same base item must converge to the latest GSI
    // entry — no stale entries from earlier writes.
    let config = test_config();
    let engine = Arc::new(CassandraEngine::new(&config, "us-east-1").await.unwrap());
    let table =
        crate::helpers::TestTable::with_gsi(&engine, "AsyncGsiConvergeTable", "GsiIdx", "gpk")
            .await;

    // Short delay so the worker drains quickly.
    set_gsi_delay(&engine, &table.key_info.table_id, "GsiIdx", 50).await;

    let account_keyspace = engine.account_keyspace(&table.key_info.account_id);
    let maps = ExpressionMaps::default();

    // Write the same item 5 times, changing its GSI key each time.
    let values = ["v0", "v1", "v2", "v3", "v4"];
    for v in &values {
        let mut item = Item::new();
        item.insert("id".to_string(), AttributeValue::S("X".to_string()));
        item.insert("gpk".to_string(), AttributeValue::S(v.to_string()));
        engine
            .put_item(&table.key_info, item, false, None, &maps, None)
            .await
            .expect("put_item");
    }

    // Spawn workers and wait for the queue to drain.
    // Guard is held until end of scope — workers stop when it drops.
    let _worker_guard = extenddb_storage_cassandra::workers::spawn_gsi_workers(engine.clone());

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let count = gsi_pending_count(&engine, &account_keyspace).await;
        println!("gsi_pending count: {count}");
        if count == 0 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            // Print what's actually in the table before panicking
            let cql = format!(
                "SELECT worker_partition, ready_at, toTimestamp(now()) as now, table_id FROM {account_keyspace}.gsi_pending"
            );
            if let Ok(frame) = engine.session().query(&cql).await
                && let Ok(body) = frame.response_body()
                && let Some(rows) = body.into_rows()
            {
                use cdrs_tokio::types::IntoRustByName as _;
                for row in &rows {
                    let wp: i32 = row.get_r_by_name("worker_partition").unwrap_or(-1);
                    let ready_at: i64 = row.get_r_by_name("ready_at").unwrap_or(0);
                    let now: i64 = row.get_r_by_name("now").unwrap_or(0);
                    let tid: String = row.get_r_by_name("table_id").unwrap_or_default();
                    println!(
                        "  row: partition={wp} ready_at={ready_at} now={now} diff={}ms table={tid}",
                        now - ready_at
                    );
                }
            }
            panic!("gsi_pending did not drain within timeout");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Only the latest GSI key should be present; all earlier ones must be gone.
    let latest = values.last().unwrap();
    assert_eq!(
        gsi_query_count(&engine, &table, "GsiIdx", "gpk", latest).await,
        1,
        "latest GSI entry missing after convergence"
    );
    for stale in &values[..values.len() - 1] {
        assert_eq!(
            gsi_query_count(&engine, &table, "GsiIdx", "gpk", stale).await,
            0,
            "stale GSI entry for {stale} survived — updates applied out of order"
        );
    }
}

#[tokio::test]
async fn test_async_gsi_worker_skips_dropped_index() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    // If the index table is gone (table-deletion race), the worker must consume
    // the row (skip + delete) rather than retrying forever.
    let config = test_config();
    let engine = Arc::new(CassandraEngine::new(&config, "us-east-1").await.unwrap());
    let table =
        crate::helpers::TestTable::with_gsi(&engine, "AsyncGsiDroppedTable", "GsiIdx", "gpk").await;

    set_gsi_delay(&engine, &table.key_info.table_id, "GsiIdx", 50).await;

    let account_keyspace = engine.account_keyspace(&table.key_info.account_id);
    let maps = ExpressionMaps::default();

    // Enqueue a few rows.
    for i in 0..3u32 {
        let mut item = Item::new();
        item.insert("id".to_string(), AttributeValue::S(format!("pk{i}")));
        item.insert("gpk".to_string(), AttributeValue::S(format!("g{i}")));
        engine
            .put_item(&table.key_info, item, false, None, &maps, None)
            .await
            .expect("put_item");
    }
    assert_eq!(gsi_pending_count(&engine, &account_keyspace).await, 3);

    // Look up the index_id and drop the index table.
    let catalog_keyspace = engine.catalog_keyspace();
    let cql = format!(
        "SELECT index_id FROM {catalog_keyspace}.indexes WHERE table_id = ? AND index_name = ?"
    );
    let session = engine.session_arc();
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows::<
        extenddb_storage::error::StorageError,
    >(
        &session,
        &cql,
        cdrs_tokio::query_values!(table.key_info.table_id.as_str(), "GsiIdx"),
        "test_dropped_index",
    )
    .await
    .unwrap();
    let index_id: String = extenddb_storage_cassandra::cassandra_util::get_column::<
        String,
        extenddb_storage::error::StorageError,
    >(&rows[0], "index_id", "test_dropped_index")
    .unwrap();

    let drop_cql = format!(
        "DROP TABLE IF EXISTS {account_keyspace}.{}",
        extenddb_storage_cassandra::data::ddl::index_table_name(&index_id)
    );
    engine.session().query(&drop_cql).await.unwrap();

    // Spawn workers — they must drain the queue without looping.
    let _worker_guard = extenddb_storage_cassandra::workers::spawn_gsi_workers(engine.clone());

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if gsi_pending_count(&engine, &account_keyspace).await == 0 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("gsi_pending did not drain after index table was dropped");
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
