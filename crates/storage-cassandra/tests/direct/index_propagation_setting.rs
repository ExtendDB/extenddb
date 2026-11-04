// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `index_propagation_delay_ms` setting applies to the next write (#407).
//!
//! SQLite and PostgreSQL read the setting live on each write to a table with
//! secondary indexes, so `extenddb settings set index_propagation_delay_ms 0`
//! makes the next write synchronous. These tests change the setting through
//! the catalog store, the way `settings set` does from another process, and
//! check that the next write follows it.
//!
//! Each test runs on its own keyspace prefix, so the catalog `settings` table
//! it changes is its own, and other tests running at the same time keep the
//! default.

use extenddb_core::expression::{Expr, ExpressionMaps, KeyCondition, PathElement};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, CreateTableInput, GsiInput, Item, KeySchemaElement,
    KeyType, Projection, ProjectionType, ScalarAttributeType, TableKeyInfo,
};
use extenddb_storage::management_store::SettingsStore;
use extenddb_storage::{DataEngine as _, TableEngine as _};
use extenddb_storage_cassandra::{CassandraCatalogStore, CassandraEngine};

const GSI: &str = "GsiIdx";
const GSI_PK: &str = "gpk";

/// An engine on a keyspace prefix of its own, with a migrated catalog, one
/// migrated account, and a table with one GSI that sets no delay of its own,
/// so the GSI follows the system setting.
struct Fixture {
    engine: std::sync::Arc<CassandraEngine>,
    settings: CassandraCatalogStore,
    key_info: TableKeyInfo,
    account_keyspace: String,
    _drop: crate::helpers::DropKeyspacesOnDrop,
}

async fn fixture(tag: &str) -> Fixture {
    let mut config = crate::helpers::test_config();
    config.keyspace_prefix = format!("ips{tag}_{}", &crate::helpers::unique_test_id()[..10]);
    config.cached_connection_string = None;
    config.ensure_cached_connection_string();
    let engine = std::sync::Arc::new(CassandraEngine::new(&config, "us-east-1").await.unwrap());
    let session = engine.session_arc();
    let account_id = crate::helpers::unique_test_account();
    let catalog_ks = engine.catalog_keyspace();
    let account_keyspace = engine.account_keyspace(&account_id);
    let drop =
        crate::helpers::DropKeyspacesOnDrop(vec![catalog_ks.clone(), account_keyspace.clone()]);
    for ks in [&catalog_ks, &account_keyspace] {
        engine.create_keyspace(ks).await.unwrap();
        crate::helpers::ensure_keyspace_rf(&engine, ks)
            .await
            .unwrap();
    }
    extenddb_storage_cassandra::migrations::migrate_catalog_keyspace(
        session.clone(),
        &config.keyspace_prefix,
        "test",
        std::time::Duration::from_secs(120),
    )
    .await
    .unwrap();
    extenddb_storage_cassandra::migrations::migrate_account(
        session.clone(),
        &config.keyspace_prefix,
        &account_keyspace,
        "test",
    )
    .await
    .unwrap();
    session
        .query_with_values(
            format!(
                "INSERT INTO {catalog_ks}.accounts (account_id, account_name, created_at) \
                 VALUES (?, ?, toTimestamp(now()))"
            ),
            cdrs_tokio::query_values!(account_id.as_str(), "Propagation Setting Test"),
        )
        .await
        .unwrap();

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
            attribute_name: GSI_PK.to_string(),
            attribute_type: ScalarAttributeType::S,
        },
    ];
    let input = CreateTableInput {
        table_name: "PropagationSetting".to_string(),
        key_schema: key_schema.clone(),
        attribute_definitions: attribute_definitions.clone(),
        local_secondary_indexes: None,
        global_secondary_indexes: Some(vec![GsiInput {
            index_name: GSI.to_string(),
            key_schema: vec![KeySchemaElement {
                attribute_name: GSI_PK.to_string(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]),
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
    let table = engine.create_table(&account_id, input).await.unwrap();
    let key_info = TableKeyInfo {
        table_name: "PropagationSetting".to_string(),
        account_id,
        table_id: table.table_id,
        key_schema: key_schema.clone(),
        base_key_schema: key_schema,
        attribute_definitions,
        has_lsi: false,
        global_secondary_indexes: Vec::new(),
        local_secondary_indexes: Vec::new(),
        vector_indexes: Vec::new(),
        stream_specification: None,
    };
    let settings = crate::helpers::create_catalog_store(&engine, &config);
    Fixture {
        engine,
        settings,
        key_info,
        account_keyspace,
        _drop: drop,
    }
}

impl Fixture {
    async fn put(&self, id: &str, gsi_pk: &str) {
        let mut item = Item::new();
        item.insert("id".to_string(), AttributeValue::S(id.to_string()));
        item.insert(GSI_PK.to_string(), AttributeValue::S(gsi_pk.to_string()));
        self.engine
            .put_item(
                &self.key_info,
                item,
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .expect("put_item");
    }

    /// One GSI query, no polling: items the index holds right now.
    async fn gsi_count(&self, gsi_pk: &str) -> usize {
        let key_condition = KeyCondition {
            pk_path: vec![PathElement::Attribute(GSI_PK.to_string())],
            pk_value: Expr::Placeholder(":v".to_string()),
            sk_condition: None,
            extra_pk_conditions: vec![],
            extra_sk_conditions: vec![],
        };
        let mut maps = ExpressionMaps::default();
        maps.values
            .insert(":v".to_string(), AttributeValue::S(gsi_pk.to_string()));
        self.engine
            .query(
                &self.key_info,
                &key_condition,
                &maps,
                true,
                None,
                None,
                Some(GSI),
            )
            .await
            .map(|(items, _)| items.len())
            .expect("query GSI")
    }

    /// Queued `gsi_pending` entries, not counting a partition's static-only
    /// row (`last_ready_at` with no clustering row).
    async fn pending(&self) -> usize {
        use cdrs_tokio::types::IntoRustByName as _;
        let rows = self
            .engine
            .session()
            .query(&format!(
                "SELECT id FROM {}.gsi_pending",
                self.account_keyspace
            ))
            .await
            .unwrap()
            .response_body()
            .unwrap()
            .into_rows()
            .unwrap_or_default();
        rows.iter()
            .filter(|row| {
                let id: Result<uuid::Uuid, _> = row.get_r_by_name("id");
                id.is_ok()
            })
            .count()
    }
}

/// `index_propagation_delay_ms = 0`, set after the engine started, makes the
/// next write synchronous: the GSI holds the item at once and nothing is
/// queued.
#[tokio::test]
async fn setting_zero_makes_the_next_write_synchronous() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let f = fixture("zero").await;
    f.settings
        .set_setting("index_propagation_delay_ms", "0")
        .await
        .unwrap();

    f.put("a", "g-zero").await;

    assert_eq!(
        f.pending().await,
        0,
        "the write was queued for asynchronous propagation; the setting of 0 was not applied"
    );
    assert_eq!(
        f.gsi_count("g-zero").await,
        1,
        "the GSI does not hold the item"
    );
}

/// A change from 0 back to a delay applies to the next write too: the first
/// write is synchronous, the second is queued.
#[tokio::test]
async fn setting_change_applies_to_the_next_write() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let f = fixture("chg").await;
    f.settings
        .set_setting("index_propagation_delay_ms", "0")
        .await
        .unwrap();
    f.put("a", "g-sync").await;
    assert_eq!(
        f.pending().await,
        0,
        "the first write was queued; the setting of 0 was not applied"
    );

    f.settings
        .set_setting("index_propagation_delay_ms", "60000")
        .await
        .unwrap();
    f.put("b", "g-async").await;

    assert_eq!(
        f.pending().await,
        1,
        "the second write was not queued; the setting of 60000 was not applied"
    );
    assert_eq!(f.gsi_count("g-sync").await, 1);
    assert_eq!(f.gsi_count("g-async").await, 0);
}

/// A catalog that holds the value only under the pre-rename name
/// `gsi_propagation_delay_ms` is honoured, as on SQLite and PostgreSQL.
#[tokio::test]
async fn legacy_setting_name_is_honoured() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    let f = fixture("old").await;
    f.settings
        .set_setting("gsi_propagation_delay_ms", "0")
        .await
        .unwrap();

    f.put("a", "g-old").await;

    assert_eq!(
        f.pending().await,
        0,
        "the write was queued; the legacy setting of 0 was not applied"
    );
    assert_eq!(f.gsi_count("g-old").await, 1);
}

/// With the GSI workers running, a write under a 50 ms delay reaches the GSI
/// within a small multiple of that delay, not after the workers' 1 s idle
/// sleep (#407). The bar, 500 ms, is the one
/// `tests/test_gsi_async.py::test_gsi_configured_delay_range` uses on a
/// served build.
///
/// The keyspace prefix is this test's own, so only this engine's workers
/// can apply the row: a worker of another test, polling once a second,
/// cannot make this pass by chance.
#[tokio::test]
async fn delayed_write_reaches_the_gsi_within_its_delay() {
    if crate::helpers::skip_without_cassandra() {
        return;
    }
    const DELAY_MS: &str = "50";
    const BAR: std::time::Duration = std::time::Duration::from_millis(500);
    const GIVE_UP: std::time::Duration = std::time::Duration::from_secs(5);

    let f = fixture("lat").await;
    f.settings
        .set_setting("index_propagation_delay_ms", DELAY_MS)
        .await
        .unwrap();
    let _workers = extenddb_storage_cassandra::workers::spawn_gsi_workers(f.engine.clone());
    // Let the workers reach their first idle sleep, as on a running server.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let mut observed = Vec::new();
    for i in 0..3 {
        let gsi_pk = format!("g-lat-{i}");
        let started = std::time::Instant::now();
        f.put(&format!("item-{i}"), &gsi_pk).await;
        while f.gsi_count(&gsi_pk).await == 0 {
            assert!(
                started.elapsed() < GIVE_UP,
                "write {i} did not reach the GSI within {GIVE_UP:?}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        observed.push(started.elapsed());
    }
    eprintln!("delay {DELAY_MS} ms; write-to-GSI times: {observed:?}");
    assert!(
        observed.iter().all(|t| *t < BAR),
        "with a {DELAY_MS} ms delay, writes reached the GSI after {observed:?}; \
         expected each under {BAR:?}"
    );
}
