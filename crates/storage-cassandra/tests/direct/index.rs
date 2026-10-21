// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for GSI/LSI index operations.

use extenddb_core::types::{
    AttributeDefinition, AttributeValue, Item, KeySchemaElement, KeyType, Projection,
    ProjectionType, ScalarAttributeType,
};
use extenddb_storage_cassandra::CassandraEngine;

use crate::helpers::{
    ensure_test_account, setup_engine, test_config, unique_test_account, unique_test_id,
};

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

// ---------------------------------------------------------------------------
// Index rows follow the base item: the same-row rule and version stamps.
// ---------------------------------------------------------------------------

/// Base pk (S) / sk (N) with one GSI on `gsi_pk`, for direct `sync_indexes` tests.
struct SyncIndexFixture {
    engine: CassandraEngine,
    account_keyspace: String,
    index_id: String,
    base_key_schema: Vec<KeySchemaElement>,
    base_attr_defs: Vec<AttributeDefinition>,
    indexes: Vec<extenddb_storage_cassandra::data::index::IndexMeta>,
}

impl SyncIndexFixture {
    async fn new() -> Self {
        let config = test_config();
        let engine = CassandraEngine::new(&config, "us-east-1").await.unwrap();
        let account_id = unique_test_account();
        let index_id = unique_test_id();
        ensure_test_account(&engine, &account_id).await.unwrap();
        let account_keyspace = engine.account_keyspace(&account_id);

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
        let index_key_schema = vec![KeySchemaElement {
            attribute_name: "gsi_pk".to_owned(),
            key_type: KeyType::Hash,
        }];
        engine
            .create_index_data_table(
                &account_keyspace,
                &index_id,
                &index_key_schema,
                &[AttributeDefinition {
                    attribute_name: "gsi_pk".to_owned(),
                    attribute_type: ScalarAttributeType::S,
                }],
                &base_key_schema,
                &base_attr_defs,
            )
            .await
            .unwrap();
        let indexes = vec![extenddb_storage_cassandra::data::index::IndexMeta {
            index_name: "sync_gsi".to_owned(),
            index_id: index_id.clone(),
            index_type: "GSI".to_owned(),
            key_schema: index_key_schema,
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            propagation_delay_ms: Some(0),
        }];
        Self {
            engine,
            account_keyspace,
            index_id,
            base_key_schema,
            base_attr_defs,
            indexes,
        }
    }

    fn item(gsi_pk: &str, data: &str) -> Item {
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S("p".to_owned()));
        item.insert("sk".to_owned(), AttributeValue::N("1".to_owned()));
        item.insert("gsi_pk".to_owned(), AttributeValue::S(gsi_pk.to_owned()));
        item.insert("data".to_owned(), AttributeValue::S(data.to_owned()));
        item
    }

    /// The batch `sync_indexes` builds for old -> new, stamped at `stamp`.
    fn batch(
        &self,
        old: Option<&Item>,
        new: Option<&Item>,
        stamp: Option<i64>,
    ) -> cdrs_tokio::query::BatchQueryBuilder {
        let mut batch = cdrs_tokio::query::BatchQueryBuilder::new();
        if let Some(stamp) = stamp {
            batch = batch.with_timestamp(stamp);
        }
        extenddb_storage_cassandra::data::index::sync_indexes(
            &mut batch,
            &self.account_keyspace,
            &self.base_key_schema,
            &self.base_attr_defs,
            &self.indexes,
            old,
            new,
            1000,
        )
        .unwrap();
        batch
    }

    async fn apply(&self, batch: cdrs_tokio::query::BatchQueryBuilder) {
        self.engine
            .session()
            .batch(batch.build().unwrap())
            .await
            .unwrap();
    }

    /// `data` of the index row for `gsi_pk`, if the row exists.
    async fn index_row_data(&self, gsi_pk: &str) -> Option<String> {
        use cdrs_tokio::types::IntoRustByName as _;
        let idx_table = format!("index_{}", self.index_id.replace('-', "_"));
        let cql = format!(
            "SELECT item_data FROM {}.{} WHERE pk = ? AND base_pk = ? AND base_sk_n = 1",
            self.account_keyspace, idx_table
        );
        let result = self
            .engine
            .session()
            .query_with_values(&cql, cdrs_tokio::query_values!(gsi_pk, "p"))
            .await
            .unwrap();
        let rows = result
            .response_body()
            .unwrap()
            .into_rows()
            .unwrap_or_default();
        let row = rows.into_iter().next()?;
        let json: String = row.get_r_by_name("item_data").unwrap();
        let image: Item = serde_json::from_str(&json).unwrap();
        match image.get("data") {
            Some(AttributeValue::S(data)) => Some(data.clone()),
            other => panic!("unexpected data attribute {other:?}"),
        }
    }

    async fn drop(self) {
        self.engine
            .drop_index_data_table(&self.account_keyspace, &self.index_id)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_sync_indexes_rewrites_an_unchanged_index_row_with_one_statement() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    // Every statement in a batch carries the batch's timestamp and a tombstone
    // wins a tie, so a DELETE and an INSERT of one row in one batch would
    // leave the row deleted. An unchanged index row is rewritten by the
    // INSERT alone; a changed one still gets the DELETE of the old row.
    let f = SyncIndexFixture::new().await;
    let v1 = SyncIndexFixture::item("g", "v1");
    let v2 = SyncIndexFixture::item("g", "v2");
    let moved = SyncIndexFixture::item("h", "v3");

    let same_row = f.batch(Some(&v1), Some(&v2), None).build().unwrap();
    assert_eq!(
        same_row.request.queries.len(),
        1,
        "unchanged row: INSERT only"
    );

    let changed_row = f.batch(Some(&v2), Some(&moved), None).build().unwrap();
    assert_eq!(
        changed_row.request.queries.len(),
        2,
        "moved row: DELETE and INSERT"
    );

    f.apply(f.batch(None, Some(&v1), Some(1_000))).await;
    f.apply(f.batch(Some(&v1), Some(&v2), Some(2_000))).await;
    assert_eq!(f.index_row_data("g").await.as_deref(), Some("v2"));

    f.apply(f.batch(Some(&v2), Some(&moved), Some(3_000))).await;
    assert_eq!(f.index_row_data("g").await, None);
    assert_eq!(f.index_row_data("h").await.as_deref(), Some("v3"));
    f.drop().await;
}

#[tokio::test]
async fn test_version_stamps_order_index_mutations_whatever_their_arrival_order() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    // Two writers commit base versions v1 (gsi_pk = a) then v2 (gsi_pk = b),
    // but their index batches reach Cassandra in the other order. The v2
    // batch deletes the row for a and inserts b; the late v1 batch inserts a.
    // With the batches stamped by version, a's late insert loses to the
    // tombstone the newer version left, so the index agrees with the base
    // item: only b.
    let f = SyncIndexFixture::new().await;
    let v1 = SyncIndexFixture::item("a", "v1");
    let v2 = SyncIndexFixture::item("b", "v2");
    let stamp_v1 = extenddb_storage_cassandra::data::version::next_row_version(None);
    let stamp_v2 = extenddb_storage_cassandra::data::version::next_row_version(Some(stamp_v1));
    assert!(stamp_v2 > stamp_v1);

    f.apply(f.batch(Some(&v1), Some(&v2), Some(stamp_v2))).await;
    f.apply(f.batch(None, Some(&v1), Some(stamp_v1))).await;

    assert_eq!(
        f.index_row_data("a").await,
        None,
        "the older version's row came back"
    );
    assert_eq!(f.index_row_data("b").await.as_deref(), Some("v2"));

    // The same two batches with no stamps resolve by arrival order and leave
    // both rows: this is the defect the stamps remove.
    f.apply(f.batch(Some(&v2), None, Some(stamp_v2 + 1))).await;
    f.apply(f.batch(Some(&v1), None, Some(stamp_v2 + 1))).await;
    f.apply(f.batch(Some(&v1), Some(&v2), None)).await;
    f.apply(f.batch(None, Some(&v1), None)).await;
    assert_eq!(f.index_row_data("a").await.as_deref(), Some("v1"));
    assert_eq!(f.index_row_data("b").await.as_deref(), Some("v2"));
    f.drop().await;
}

#[tokio::test]
async fn test_async_gsi_worker_keeps_the_row_across_a_same_key_update() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    // The worker applies each pending entry as delete-old plus insert-new in
    // one batch. For an update that keeps the GSI key, that pair addresses one
    // row, and the worker must rewrite it rather than delete it.
    let config = test_config();
    let engine = Arc::new(CassandraEngine::new(&config, "us-east-1").await.unwrap());
    let table =
        crate::helpers::TestTable::with_gsi(&engine, "AsyncGsiSameKeyTable", "GsiIdx", "gpk").await;
    set_gsi_delay(&engine, &table.key_info.table_id, "GsiIdx", 50).await;
    let account_keyspace = engine.account_keyspace(&table.key_info.account_id);

    let mut v1 = Item::new();
    v1.insert("id".to_string(), AttributeValue::S("X".to_string()));
    v1.insert("gpk".to_string(), AttributeValue::S("same".to_string()));
    v1.insert("data".to_string(), AttributeValue::S("v1".to_string()));
    let mut v2 = v1.clone();
    v2.insert("data".to_string(), AttributeValue::S("v2".to_string()));
    let maps = ExpressionMaps::default();
    engine
        .put_item(&table.key_info, v1, false, None, &maps, None)
        .await
        .expect("put v1");
    engine
        .put_item(&table.key_info, v2, false, None, &maps, None)
        .await
        .expect("put v2");
    assert_eq!(gsi_pending_count(&engine, &account_keyspace).await, 2);

    let _worker_guard = extenddb_storage_cassandra::workers::spawn_gsi_workers(engine.clone());
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    while gsi_pending_count(&engine, &account_keyspace).await != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "gsi_pending did not drain within timeout"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    assert_eq!(
        gsi_query_count(&engine, &table, "GsiIdx", "gpk", "same").await,
        1,
        "the GSI row was deleted by the same-key update"
    );
}

// ---------------------------------------------------------------------------
// Every write path stamps its index batch with the version it stored.
// ---------------------------------------------------------------------------

/// `(version, WRITETIME(item_data))` of the base row, and `WRITETIME(item_data)`
/// of the LSI row for `priority`, for the item `(id, sort)` on a `with_lsi` table.
async fn stamps(
    engine: &CassandraEngine,
    table: &crate::helpers::TestTable,
    lsi_name: &str,
    id: &str,
    sort: i64,
    priority: i64,
) -> (Option<i64>, Option<i64>) {
    use cdrs_tokio::types::IntoRustByName as _;
    let keyspace = engine.account_keyspace(&table.key_info.account_id);
    let table_id = &table.key_info.table_id;
    let data_table = format!("items_{}", table_id.replace('-', "_"));
    let cql = format!("SELECT version FROM {keyspace}.{data_table} WHERE pk = ? AND sk_n = ?");
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows::<
        extenddb_storage::error::StorageError,
    >(
        &engine.session_arc(),
        &cql,
        cdrs_tokio::query_values!(id, sort),
        "stamps base",
    )
    .await
    .unwrap();
    let version: Option<i64> = rows
        .into_iter()
        .next()
        .and_then(|row| row.get_by_name("version").ok().flatten());

    let catalog = engine.catalog_keyspace();
    let cql =
        format!("SELECT index_id FROM {catalog}.indexes WHERE table_id = ? AND index_name = ?");
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows::<
        extenddb_storage::error::StorageError,
    >(
        &engine.session_arc(),
        &cql,
        cdrs_tokio::query_values!(table_id.as_str(), lsi_name),
        "stamps index id",
    )
    .await
    .unwrap();
    let index_id: String = rows[0].get_r_by_name("index_id").unwrap();
    let idx_table = extenddb_storage_cassandra::data::ddl::index_table_name(&index_id);
    // LSI row key: pk = id, sk_n = priority, base_pk = id, base_sk_n = sort.
    let cql = format!(
        "SELECT WRITETIME(item_data) AS wt FROM {keyspace}.{idx_table} \
         WHERE pk = ? AND sk_n = ? AND base_pk = ? AND base_sk_n = ?"
    );
    let rows = extenddb_storage_cassandra::cassandra_util::query_rows::<
        extenddb_storage::error::StorageError,
    >(
        &engine.session_arc(),
        &cql,
        cdrs_tokio::query_values!(id, priority, id, sort),
        "stamps index",
    )
    .await
    .unwrap();
    let index_wt: Option<i64> = rows
        .into_iter()
        .next()
        .and_then(|row| row.get_by_name("wt").ok().flatten());
    (version, index_wt)
}

fn lsi_item(id: &str, sort: i64, priority: i64, data: &str) -> Item {
    let mut item = Item::new();
    item.insert("id".to_string(), AttributeValue::S(id.to_string()));
    item.insert("sort".to_string(), AttributeValue::N(sort.to_string()));
    item.insert(
        "priority".to_string(),
        AttributeValue::N(priority.to_string()),
    );
    item.insert("data".to_string(), AttributeValue::S(data.to_string()));
    item
}

#[tokio::test]
async fn test_every_write_path_stamps_the_index_row_with_the_stored_version() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    // The guard behind issue #414 is the stamp each call site puts on its index
    // batch: the version the base-row LWT stored. On one node the batches
    // arrive in commit order anyway, so the ordering itself cannot be observed
    // over the wire; what can be observed is the stamp. For each write path
    // the LSI row's write timestamp must equal the base row's version.
    use extenddb_core::expression::{Expr, PathElement, UpdateAction};
    use extenddb_core::types::ReturnValuesOnConditionCheckFailure;
    use extenddb_storage::TransactWriteOp;

    let engine = Arc::new(setup_engine().await);
    let table = crate::helpers::TestTable::with_lsi(
        &engine,
        "StampedWritesTable",
        "LsiPriority",
        "priority",
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    engine.process_control_plane_transitions().await.unwrap();
    let maps = ExpressionMaps::default();

    let check = |path: &'static str, got: (Option<i64>, Option<i64>)| {
        let (version, index_wt) = got;
        assert!(version.is_some(), "{path}: base row has no version");
        assert_eq!(
            index_wt, version,
            "{path}: index row timestamp differs from the stored version"
        );
        version.unwrap()
    };

    // put_item creating the item
    engine
        .put_item(
            &table.key_info,
            lsi_item("a", 1, 10, "v1"),
            false,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();
    let v1 = check(
        "put_item create",
        stamps(&engine, &table, "LsiPriority", "a", 1, 10).await,
    );

    // put_item with attribute_not_exists on the key (the IF NOT EXISTS fast path)
    let not_exists = Expr::Function {
        name: "attribute_not_exists".to_string(),
        args: vec![Expr::Path(vec![PathElement::Attribute("id".to_string())])],
    };
    engine
        .put_item(
            &table.key_info,
            lsi_item("b", 1, 10, "v1"),
            false,
            Some(&not_exists),
            &maps,
            None,
        )
        .await
        .unwrap();
    check(
        "put_item if not exists",
        stamps(&engine, &table, "LsiPriority", "b", 1, 10).await,
    );

    // put_item over an existing item (occ_write), same index key
    engine
        .put_item(
            &table.key_info,
            lsi_item("a", 1, 10, "v2"),
            false,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();
    let v2 = check(
        "put_item overwrite",
        stamps(&engine, &table, "LsiPriority", "a", 1, 10).await,
    );
    assert!(v2 > v1);

    // update_item moving the index key
    let mut key = Item::new();
    key.insert("id".to_string(), AttributeValue::S("a".to_string()));
    key.insert("sort".to_string(), AttributeValue::N("1".to_string()));
    let actions = vec![UpdateAction::Set {
        path: vec![PathElement::Attribute("priority".to_string())],
        value: Expr::Placeholder("p".to_string()),
    }];
    let mut values = std::collections::HashMap::new();
    values.insert("p".to_string(), AttributeValue::N("20".to_string()));
    let update_maps = ExpressionMaps::new(std::collections::HashMap::new(), values);
    engine
        .update_item(
            &table.key_info,
            &key,
            &actions,
            false,
            false,
            None,
            &update_maps,
            None,
        )
        .await
        .unwrap();
    let v3 = check(
        "update_item",
        stamps(&engine, &table, "LsiPriority", "a", 1, 20).await,
    );
    assert!(v3 > v2);
    assert_eq!(
        stamps(&engine, &table, "LsiPriority", "a", 1, 10).await.1,
        None,
        "update_item left the old index row"
    );

    // transactional Put over the existing item
    let put = lsi_item("a", 1, 30, "v4");
    let ops = vec![TransactWriteOp::Put {
        key_info: &table.key_info,
        item: &put,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    engine.transact_write_items(&ops, None).await.unwrap();
    let v4 = check(
        "transaction put",
        stamps(&engine, &table, "LsiPriority", "a", 1, 30).await,
    );
    assert!(v4 > v3);
    assert_eq!(
        stamps(&engine, &table, "LsiPriority", "a", 1, 20).await.1,
        None
    );

    // transactional Update
    values = std::collections::HashMap::new();
    values.insert("p".to_string(), AttributeValue::N("40".to_string()));
    let update_maps = ExpressionMaps::new(std::collections::HashMap::new(), values);
    let ops = vec![TransactWriteOp::Update {
        key_info: &table.key_info,
        key: &key,
        actions: &actions,
        condition: None,
        maps: &update_maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    engine.transact_write_items(&ops, None).await.unwrap();
    let v5 = check(
        "transaction update",
        stamps(&engine, &table, "LsiPriority", "a", 1, 40).await,
    );
    assert!(v5 > v4);

    // delete_item: the tombstone's timestamp is not readable, so observe that
    // the index row is gone and that a create landing afterwards is stamped
    // above the deleted row's version (the create takes the clock, which has
    // moved past the version the delete read).
    engine
        .delete_item(&table.key_info, &key, false, None, &maps, None)
        .await
        .unwrap();
    assert_eq!(
        stamps(&engine, &table, "LsiPriority", "a", 1, 40).await,
        (None, None)
    );
    engine
        .put_item(
            &table.key_info,
            lsi_item("a", 1, 50, "v6"),
            false,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();
    let v6 = check(
        "put_item after delete",
        stamps(&engine, &table, "LsiPriority", "a", 1, 50).await,
    );
    assert!(v6 > v5);

    // transactional Delete
    let ops = vec![TransactWriteOp::Delete {
        key_info: &table.key_info,
        key: &key,
        condition: None,
        maps: &maps,
        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        stream: None,
    }];
    engine.transact_write_items(&ops, None).await.unwrap();
    assert_eq!(
        stamps(&engine, &table, "LsiPriority", "a", 1, 50).await,
        (None, None)
    );
}
