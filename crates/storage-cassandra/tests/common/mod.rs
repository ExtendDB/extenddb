// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

//! Shared test helpers.

use extenddb_core::types::{
    AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType, TableKeyInfo,
};
use extenddb_storage::TableEngine;
use extenddb_storage_cassandra::{
    CassandraCatalogStore, CassandraEngine, CassandraSession, CassandraStorageConfig,
};
use std::sync::Arc;

/// Keyspaces queued for async drop between tests.
static PENDING_DROPS: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

fn queue_keyspace_drop(keyspace: String) {
    if let Ok(mut q) = PENDING_DROPS.lock() {
        q.push(keyspace);
    }
}

/// Drain all pending keyspace drops. Called at the start of each test via setup_engine().
pub async fn flush_pending_drops(session: &Arc<CassandraSession>) {
    let keyspaces: Vec<String> = PENDING_DROPS
        .lock()
        .map(|mut q| q.drain(..).collect())
        .unwrap_or_default();
    for ks in keyspaces {
        let _ = session.query(format!("DROP KEYSPACE IF EXISTS {ks}")).await;
    }
}
/// Returns a standard test configuration for Cassandra.
pub fn test_config() -> CassandraStorageConfig {
    let mut config = CassandraStorageConfig {
        contact_points: vec!["127.0.0.1:9042".to_string()],
        username: Some("cassandra".to_string()),
        password: Some("cassandra".to_string()),
        keyspace_prefix: "extenddb_ttl_test".to_string(),
        datacenter: "datacenter1".to_string(),
        replication_factor: 1,
        max_connections: 5,
        cached_connection_string: None,
        instance_id: None,
    };
    config.ensure_cached_connection_string();
    config
}

/// Generates a unique test identifier.
pub fn unique_test_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Generates a unique test account ID (12-digit format).
pub fn unique_test_account() -> String {
    format!("{:012}", rand::random::<u64>() % 1_000_000_000_000)
}

/// Ensures account exists in catalog and creates keyspace.
pub async fn ensure_test_account(
    engine: &CassandraEngine,
    account_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let account_keyspace = format!("extenddb_ttl_test_account_{}", account_id);
    let catalog_keyspace = "extenddb_ttl_test_catalog";

    // Create and migrate the catalog keyspace if it doesn't exist yet.
    if !engine.keyspace_exists(catalog_keyspace).await? {
        engine.create_keyspace(catalog_keyspace).await?;
    }
    ensure_keyspace_rf(engine, catalog_keyspace).await?;
    // Always run migrations — idempotent, guards against concurrent creation races.
    extenddb_storage_cassandra::migrations::run_catalog_migrations(
        &engine.session_arc(),
        catalog_keyspace,
    )
    .await
    .map_err(|e| format!("Failed to run catalog migrations: {:?}", e))?;

    if !engine.keyspace_exists(&account_keyspace).await? {
        engine.create_keyspace(&account_keyspace).await?;
        ensure_keyspace_rf(engine, &account_keyspace).await?;
    } else {
        ensure_keyspace_rf(engine, &account_keyspace).await?;
    }
    // Always run migrations — idempotent, guards against concurrent creation races.
    extenddb_storage_cassandra::migrations::run_data_migrations(
        &engine.session_arc(),
        &account_keyspace,
    )
    .await
    .map_err(|e| format!("Failed to run migrations: {:?}", e))?;

    let query = format!(
        "INSERT INTO {}.accounts (account_id, account_name, created_at) VALUES (?, ?, toTimestamp(now())) IF NOT EXISTS",
        catalog_keyspace
    );
    let _ = engine
        .session_arc()
        .query_with_values(
            &query,
            cdrs_tokio::query_values!(account_id, "Test Account"),
        )
        .await;

    Ok(())
}

/// RAII wrapper for test accounts - drops keyspace on cleanup.
pub struct TestAccount {
    session: Arc<CassandraSession>,
    keyspace_prefix: String,
    pub account_id: String,
}

impl TestAccount {
    pub async fn new(engine: &CassandraEngine, keyspace_prefix: &str) -> Self {
        let account_id = unique_test_account();
        ensure_test_account(engine, &account_id).await.unwrap();
        Self {
            session: engine.session_arc(),
            keyspace_prefix: keyspace_prefix.to_string(),
            account_id,
        }
    }
}

impl Drop for TestAccount {
    fn drop(&mut self) {
        let keyspace = format!("{}_account_{}", self.keyspace_prefix, self.account_id);
        queue_keyspace_drop(keyspace);
    }
}

/// RAII wrapper for test tables - deletes table on cleanup.
pub struct TestTable {
    session: Arc<CassandraSession>,
    owns_keyspace: bool,
    pub key_info: TableKeyInfo,
}

impl TestTable {
    pub async fn new(engine: &CassandraEngine, table_name: &str, has_sort_key: bool) -> Self {
        Self::with_account_and_schema(
            engine,
            &unique_test_account(),
            table_name,
            if has_sort_key {
                Some(ScalarAttributeType::S)
            } else {
                None
            },
            true,
        )
        .await
    }

    pub async fn with_sort_key_type(
        engine: &CassandraEngine,
        table_name: &str,
        sort_key_type: ScalarAttributeType,
    ) -> Self {
        Self::with_account_and_schema(
            engine,
            &unique_test_account(),
            table_name,
            Some(sort_key_type),
            true,
        )
        .await
    }

    pub async fn with_account(
        engine: &CassandraEngine,
        account_id: &str,
        table_name: &str,
        has_sort_key: bool,
    ) -> Self {
        let sort_key_type = if has_sort_key {
            Some(ScalarAttributeType::S)
        } else {
            None
        };
        Self::with_account_and_schema(engine, account_id, table_name, sort_key_type, false).await
    }

    pub async fn with_account_and_schema(
        engine: &CassandraEngine,
        account_id: &str,
        table_name: &str,
        sort_key_type: Option<ScalarAttributeType>,
        owns_keyspace: bool,
    ) -> Self {
        ensure_test_account(engine, account_id).await.unwrap();

        let (key_schema, attribute_definitions) = if let Some(sk_type) = sort_key_type {
            (
                vec![
                    KeySchemaElement {
                        attribute_name: "id".to_string(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sort".to_string(),
                        key_type: KeyType::Range,
                    },
                ],
                vec![
                    AttributeDefinition {
                        attribute_name: "id".to_string(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sort".to_string(),
                        attribute_type: sk_type,
                    },
                ],
            )
        } else {
            (
                vec![KeySchemaElement {
                    attribute_name: "id".to_string(),
                    key_type: KeyType::Hash,
                }],
                vec![AttributeDefinition {
                    attribute_name: "id".to_string(),
                    attribute_type: ScalarAttributeType::S,
                }],
            )
        };

        let input = extenddb_core::types::CreateTableInput {
            table_name: table_name.to_string(),
            key_schema: key_schema.clone(),
            attribute_definitions: attribute_definitions.clone(),
            local_secondary_indexes: None,
            global_secondary_indexes: None,
            vector_indexes: None,
            billing_mode: None,
            table_throughput_mode: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            stream_specification: None,
            sse_specification: None,
            tags: None,
            table_class: None,
            deletion_protection_enabled: None,
        };

        let table_desc = engine.create_table(account_id, input).await.unwrap();

        let key_info = TableKeyInfo {
            table_name: table_name.to_string(),
            account_id: account_id.to_string(),
            table_id: table_desc.table_id,
            key_schema: key_schema.clone(),
            base_key_schema: key_schema,
            attribute_definitions,
            has_lsi: false,
            global_secondary_indexes: Vec::new(),
            local_secondary_indexes: Vec::new(),
            vector_indexes: Vec::new(),
            stream_specification: None,
        };

        Self {
            session: engine.session_arc(),
            key_info,
            owns_keyspace,
        }
    }

    /// Create a test table with a GSI (synchronous by default with propagation_delay_ms=0).
    pub async fn with_gsi(
        engine: &CassandraEngine,
        table_name: &str,
        gsi_name: &str,
        gsi_pk_attr: &str,
    ) -> Self {
        use extenddb_core::types::GsiInput;

        let account_id = unique_test_account();
        ensure_test_account(engine, &account_id).await.unwrap();

        let key_schema = vec![KeySchemaElement {
            attribute_name: "id".to_string(),
            key_type: KeyType::Hash,
        }];

        let attribute_definitions = vec![
            AttributeDefinition {
                attribute_name: "id".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: gsi_pk_attr.to_string(),
                attribute_type: ScalarAttributeType::S,
            },
        ];

        let gsi = GsiInput {
            index_name: gsi_name.to_string(),
            key_schema: vec![KeySchemaElement {
                attribute_name: gsi_pk_attr.to_string(),
                key_type: KeyType::Hash,
            }],
            projection: extenddb_core::types::Projection {
                projection_type: extenddb_core::types::ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        };

        let input = extenddb_core::types::CreateTableInput {
            table_name: table_name.to_string(),
            key_schema: key_schema.clone(),
            attribute_definitions: attribute_definitions.clone(),
            local_secondary_indexes: None,
            global_secondary_indexes: Some(vec![gsi]),
            vector_indexes: None,
            billing_mode: None,
            table_throughput_mode: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            stream_specification: None,
            sse_specification: None,
            tags: None,
            table_class: None,
            deletion_protection_enabled: None,
        };

        let table_desc = engine.create_table(&account_id, input).await.unwrap();

        let key_info = TableKeyInfo {
            table_name: table_name.to_string(),
            account_id: account_id.to_string(),
            table_id: table_desc.table_id,
            key_schema: key_schema.clone(),
            base_key_schema: key_schema,
            attribute_definitions,
            has_lsi: false,
            global_secondary_indexes: Vec::new(),
            local_secondary_indexes: Vec::new(),
            vector_indexes: Vec::new(),
            stream_specification: None,
        };

        Self {
            session: engine.session_arc(),
            key_info,
            owns_keyspace: true,
        }
    }

    /// Create a test table with a GSI that has both partition and sort key.
    pub async fn with_gsi_with_sk(
        engine: &CassandraEngine,
        table_name: &str,
        gsi_name: &str,
        gsi_pk_attr: &str,
        gsi_sk_attr: &str,
    ) -> Self {
        use extenddb_core::types::GsiInput;

        let account_id = unique_test_account();
        ensure_test_account(engine, &account_id).await.unwrap();

        let key_schema = vec![KeySchemaElement {
            attribute_name: "id".to_string(),
            key_type: KeyType::Hash,
        }];

        let attribute_definitions = vec![
            AttributeDefinition {
                attribute_name: "id".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: gsi_pk_attr.to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: gsi_sk_attr.to_string(),
                attribute_type: ScalarAttributeType::N,
            },
        ];

        let gsi = GsiInput {
            index_name: gsi_name.to_string(),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: gsi_pk_attr.to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: gsi_sk_attr.to_string(),
                    key_type: KeyType::Range,
                },
            ],
            projection: extenddb_core::types::Projection {
                projection_type: extenddb_core::types::ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        };

        let input = extenddb_core::types::CreateTableInput {
            table_name: table_name.to_string(),
            key_schema: key_schema.clone(),
            attribute_definitions: attribute_definitions.clone(),
            local_secondary_indexes: None,
            global_secondary_indexes: Some(vec![gsi]),
            vector_indexes: None,
            billing_mode: None,
            table_throughput_mode: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            stream_specification: None,
            sse_specification: None,
            tags: None,
            table_class: None,
            deletion_protection_enabled: None,
        };

        let table_desc = engine.create_table(&account_id, input).await.unwrap();

        let key_info = TableKeyInfo {
            table_name: table_name.to_string(),
            account_id: account_id.to_string(),
            table_id: table_desc.table_id,
            key_schema: key_schema.clone(),
            base_key_schema: key_schema,
            attribute_definitions,
            has_lsi: false,
            global_secondary_indexes: Vec::new(),
            local_secondary_indexes: Vec::new(),
            vector_indexes: Vec::new(),
            stream_specification: None,
        };

        Self {
            session: engine.session_arc(),
            key_info,
            owns_keyspace: true,
        }
    }

    /// Create a test table with an LSI (shares PK with base table, different SK).
    pub async fn with_lsi(
        engine: &CassandraEngine,
        table_name: &str,
        lsi_name: &str,
        lsi_sk_attr: &str,
    ) -> Self {
        use extenddb_core::types::LsiInput;

        let account_id = unique_test_account();
        ensure_test_account(engine, &account_id).await.unwrap();

        let key_schema = vec![
            KeySchemaElement {
                attribute_name: "id".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sort".to_string(),
                key_type: KeyType::Range,
            },
        ];

        let attribute_definitions = vec![
            AttributeDefinition {
                attribute_name: "id".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sort".to_string(),
                attribute_type: ScalarAttributeType::N,
            },
            AttributeDefinition {
                attribute_name: lsi_sk_attr.to_string(),
                attribute_type: ScalarAttributeType::N,
            },
        ];

        let lsi = LsiInput {
            index_name: lsi_name.to_string(),
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: "id".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: lsi_sk_attr.to_string(),
                    key_type: KeyType::Range,
                },
            ],
            projection: extenddb_core::types::Projection {
                projection_type: extenddb_core::types::ProjectionType::All,
                non_key_attributes: None,
            },
        };

        let input = extenddb_core::types::CreateTableInput {
            table_name: table_name.to_string(),
            key_schema: key_schema.clone(),
            attribute_definitions: attribute_definitions.clone(),
            local_secondary_indexes: Some(vec![lsi]),
            global_secondary_indexes: None,
            vector_indexes: None,
            billing_mode: None,
            table_throughput_mode: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            stream_specification: None,
            sse_specification: None,
            tags: None,
            table_class: None,
            deletion_protection_enabled: None,
        };

        let table_desc = engine.create_table(&account_id, input).await.unwrap();

        let key_info = TableKeyInfo {
            table_name: table_name.to_string(),
            account_id: account_id.to_string(),
            table_id: table_desc.table_id,
            key_schema: key_schema.clone(),
            base_key_schema: key_schema,
            attribute_definitions,
            has_lsi: true,
            global_secondary_indexes: Vec::new(),
            local_secondary_indexes: Vec::new(),
            vector_indexes: Vec::new(),
            stream_specification: None,
        };

        Self {
            session: engine.session_arc(),
            key_info,
            owns_keyspace: true,
        }
    }
}

impl Drop for TestTable {
    fn drop(&mut self) {
        if self.owns_keyspace {
            let account_keyspace =
                format!("extenddb_ttl_test_account_{}", self.key_info.account_id);
            queue_keyspace_drop(account_keyspace);
        }
        // owns_keyspace=false: individual table drops are skipped; the keyspace
        // persists but is isolated by unique account_id and cleaned up next run.
    }
}

/// Deprecated: Use TestAccount or ensure_test_account instead.
/// Returns a test account ID, ensuring the account keyspace exists.
/// Also ensures the account is registered in the catalog.
pub async fn test_account_id(
    engine: &extenddb_storage_cassandra::CassandraEngine,
) -> Result<String, Box<dyn std::error::Error>> {
    let account_id = "999999999999";
    ensure_test_account(engine, account_id).await?;
    Ok(account_id.to_string())
}

/// Ensures a keyspace has RF=1 for single-node testing.
pub async fn ensure_keyspace_rf(
    engine: &extenddb_storage_cassandra::CassandraEngine,
    keyspace: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let cql = format!(
        "ALTER KEYSPACE {} WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter1': 1}}",
        keyspace
    );
    engine.session_arc().query(cql).await?;
    Ok(())
}

/// Sets up a CassandraEngine with standard test configuration.
/// Ensures the catalog keyspace exists and is migrated.
pub async fn setup_engine() -> CassandraEngine {
    let config = test_config();
    let engine = CassandraEngine::new(&config, "us-east-1").await.unwrap();

    // Drop keyspaces queued by the previous test's Drop impls.
    flush_pending_drops(&engine.session_arc()).await;

    let catalog_keyspace = format!("{}_catalog", config.keyspace_prefix);
    if !engine.keyspace_exists(&catalog_keyspace).await.unwrap() {
        engine.create_keyspace(&catalog_keyspace).await.unwrap();
    }
    ensure_keyspace_rf(&engine, &catalog_keyspace)
        .await
        .unwrap();
    // Always run migrations — idempotent, guards against concurrent creation races.
    extenddb_storage_cassandra::migrations::run_catalog_migrations(
        &engine.session_arc(),
        &catalog_keyspace,
    )
    .await
    .unwrap();

    engine
}

/// Creates a CassandraCatalogStore from engine for tests.
pub fn create_catalog_store(
    engine: &CassandraEngine,
    config: &CassandraStorageConfig,
) -> CassandraCatalogStore {
    CassandraCatalogStore::new(
        engine.session_arc(),
        config.keyspace_prefix.clone(),
        config.datacenter.clone(),
        config.replication_factor,
    )
}

/// Helper: insert an item then manually set its `prepared_txn_id` via raw CQL,
/// simulating a transaction that has prepared (locked) the item.
///
/// The item must have a string `"id"` partition key. If the table has a sort
/// key, the item must have a string `"sort"` attribute.
pub async fn put_item_then_lock(
    engine: &CassandraEngine,
    table: &TestTable,
    item: &std::collections::BTreeMap<String, extenddb_core::types::AttributeValue>,
) {
    use extenddb_storage::DataEngine;

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
        .expect("Initial put_item should succeed");

    let account_keyspace = format!("extenddb_ttl_test_account_{}", table.key_info.account_id);
    let data_table = format!("items_{}", table.key_info.table_id.replace("-", "_"));
    let fake_txn_id = uuid::Uuid::new_v4();

    let pk = match item.get("id").unwrap() {
        extenddb_core::types::AttributeValue::S(s) => s.as_str(),
        _ => panic!("Expected string partition key 'id'"),
    };

    let has_sk = table.key_info.key_schema.len() > 1;

    if has_sk {
        let sk = match item.get("sort").unwrap() {
            extenddb_core::types::AttributeValue::S(s) => s.clone(),
            _ => panic!("Expected string sort key 'sort'"),
        };
        let query = format!(
            "UPDATE {}.{} SET prepared_txn_id = ? WHERE pk = ? AND sk_s = ? \
             IF prepared_txn_id = null",
            account_keyspace, data_table
        );
        engine
            .session_arc()
            .query_with_values(
                &query,
                cdrs_tokio::query_values!(
                    cdrs_tokio::types::value::Bytes::new(fake_txn_id.as_bytes().to_vec()),
                    pk,
                    sk.as_str()
                ),
            )
            .await
            .expect("Setting prepared_txn_id should succeed");
    } else {
        let query = format!(
            "UPDATE {}.{} SET prepared_txn_id = ? WHERE pk = ? IF prepared_txn_id = null",
            account_keyspace, data_table
        );
        engine
            .session_arc()
            .query_with_values(
                &query,
                cdrs_tokio::query_values!(
                    cdrs_tokio::types::value::Bytes::new(fake_txn_id.as_bytes().to_vec()),
                    pk
                ),
            )
            .await
            .expect("Setting prepared_txn_id should succeed");
    }
}

/// True when the suite should be skipped because no Cassandra is reachable.
///
/// The workspace `cargo test` job in test.yml runs with no services, so these
/// suites probe 127.0.0.1:9042 and skip themselves when nothing is listening.
/// The dedicated integration-cassandra workflow provides a service container
/// and sets `EXTENDDB_TEST_CASSANDRA=required`, under which this never skips:
/// if Cassandra is down there, setup fails loudly rather than the suite
/// silently passing with zero tests executed (the failure mode integration.yml
/// documents for batch_transact_authz).
pub fn skip_without_cassandra() -> bool {
    if std::env::var("EXTENDDB_TEST_CASSANDRA").as_deref() == Ok("required") {
        return false;
    }
    let reachable = std::net::TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], 9042)),
        std::time::Duration::from_secs(2),
    )
    .is_ok();
    if !reachable {
        eprintln!(
            "skipping: no Cassandra at 127.0.0.1:9042 (set EXTENDDB_TEST_CASSANDRA=required to fail instead)"
        );
    }
    !reachable
}
