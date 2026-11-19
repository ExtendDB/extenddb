// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! End-to-end integration test suite for Google Cloud Bigtable storage backend.
//!
//! Validates:
//! - Catalog bootstrapping, migrations, and table lifecycle.
//! - Guarded single-row CRUD operations (`PutItem`, `GetItem`, `UpdateItem`, `DeleteItem`) with expression parsing and arithmetic.
//! - Deterministic OCC-derived millisecond-aligned cell timestamps (`occ_version * 1_000`) and concurrent OCC/conditional serialization.
//! - 2PC multi-row transactions (`TransactWriteItems`), atomic rollback, idempotency replay/mismatch, and concurrent `TransactGetItems` read-through-log rule.
//! - Fenced single-item stream emission + simulated mid-flight crash roll-forward recovery without duplicates.
//! - Decimal number key encoding, exact numerical sorting, forward/reverse range scans, and parallel scan segment validation.
//! - GSI shadow table operations, FDB escape-encoded sort keys, range queries, update propagation, and deletion cleanup.
//! - TTL index maintenance, background sweep with live-item expiry verification, and fenced TTL service stream emission (`dynamodb.amazonaws.com`).
//!
//! These tests run when `BIGTABLE_EMULATOR_HOST` is set, and skip gracefully
//! otherwise unless `EXTENDDB_TEST_BIGTABLE_REQUIRED` is set to a non-empty,
//! non-`"0"` value (in which case missing or unreachable emulator endpoints
//! fail the test).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use extenddb_core::expression::{
    Expr, ExpressionMaps, KeyCondition, UpdateAction, parse_condition, parse_key_condition,
    parse_update, tokenize,
};
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DescribeStreamInput,
    DescribeTableInput, GsiInput, Item, KeySchemaElement, KeyType, ListTablesInput, Projection,
    ProjectionType, ReturnValuesOnConditionCheckFailure, ScalarAttributeType, StreamEventName,
    StreamSpecification, StreamViewType, TableStatus, TimeToLiveStatus,
};
use extenddb_storage::bootstrapper::Bootstrapper;
use extenddb_storage::error::StorageError;
use extenddb_storage::migrations::{
    CatalogMigrations, CatalogStatus, MigrationId, RunKind, Target,
};
use extenddb_storage::{
    DataEngine, IdempotencyKey, MetadataEngine, StreamCapture, StreamEngine, TableEngine,
    TransactGetOp, TransactWriteOp,
};
use extenddb_storage_bigtable::data::admin::AdminClient;
use extenddb_storage_bigtable::migrations::BigtableCatalogMigrations;
use extenddb_storage_bigtable::transact::ensure_txn_log_table;
use extenddb_storage_bigtable::ttl_worker::{ensure_ttl_index_table, sweep_once};
use extenddb_storage_bigtable::{
    BigtableBootstrapper, BigtableClient, BigtableEngine, BigtableStorageConfig,
};
use googleapis_tonic_google_bigtable_v2::google::bigtable::v2::{
    MutateRowRequest, Mutation, ReadRowsRequest, RowSet, mutation,
};
use uuid::Uuid;

struct TestContext {
    config: BigtableStorageConfig,
    client: Arc<BigtableClient>,
    engine: Arc<BigtableEngine>,
    account_id: String,
}

fn unique_name(prefix: &str) -> String {
    format!(
        "{}_{}",
        prefix,
        &Uuid::new_v4().to_string().replace('-', "")[..12]
    )
}

fn parse_cond(expr_str: &str) -> Expr {
    let tokens = tokenize(expr_str).expect("tokenize condition");
    parse_condition(&tokens).expect("parse condition")
}

fn parse_upd(expr_str: &str) -> Vec<UpdateAction> {
    let tokens = tokenize(expr_str).expect("tokenize update");
    parse_update(&tokens).expect("parse update")
}

fn parse_kc(expr_str: &str) -> KeyCondition {
    let tokens = tokenize(expr_str).expect("tokenize key condition");
    parse_key_condition(&tokens).expect("parse key condition")
}

fn is_required_flag_enabled(raw: Option<&str>) -> bool {
    raw.is_some_and(|v| {
        let trimmed = v.trim();
        !trimmed.is_empty() && trimmed != "0"
    })
}

fn is_emulator_required() -> bool {
    is_required_flag_enabled(
        std::env::var("EXTENDDB_TEST_BIGTABLE_REQUIRED")
            .ok()
            .as_deref(),
    )
}

async fn resolve_reachable_emulator_host(
    config_for_probe: &BigtableStorageConfig,
) -> Option<(String, Arc<BigtableClient>)> {
    let required = is_emulator_required();
    let host = match std::env::var("BIGTABLE_EMULATOR_HOST") {
        Ok(h) if !h.trim().is_empty() => h,
        _ => {
            assert!(
                !required,
                "BIGTABLE_EMULATOR_HOST is not set, and EXTENDDB_TEST_BIGTABLE_REQUIRED requires a live Bigtable emulator"
            );
            return None;
        }
    };

    let mut cfg = config_for_probe.clone();
    cfg.emulator_host = Some(host.clone());
    cfg.dev_mode = true;

    let client = match BigtableClient::connect(&cfg).await {
        Ok(c) => Arc::new(c),
        Err(e) => {
            assert!(
                !required,
                "Bigtable emulator at {host} connect failed while EXTENDDB_TEST_BIGTABLE_REQUIRED is set: {e}"
            );
            eprintln!("Skipping Bigtable emulator integration test (connect failed): {e}");
            return None;
        }
    };
    let mut admin = match AdminClient::connect(&client).await {
        Ok(a) => a,
        Err(e) => {
            assert!(
                !required,
                "Bigtable emulator at {host} admin connect failed while EXTENDDB_TEST_BIGTABLE_REQUIRED is set: {e}"
            );
            eprintln!("Skipping Bigtable emulator integration test (admin connect failed): {e}");
            return None;
        }
    };
    if let Err(e) = admin.list_tables().await {
        assert!(
            !required,
            "Bigtable emulator at {host} list_tables probe failed while EXTENDDB_TEST_BIGTABLE_REQUIRED is set: {e}"
        );
        eprintln!("Skipping Bigtable emulator integration test (list_tables probe failed): {e}");
        return None;
    }

    Some((host, client))
}

async fn setup_emulator_context() -> Option<TestContext> {
    let suffix = &Uuid::new_v4().to_string().replace('-', "")[..8];
    let project_id =
        std::env::var("BIGTABLE_PROJECT_ID").unwrap_or_else(|_| format!("test-proj-{suffix}"));
    let instance_id =
        std::env::var("BIGTABLE_INSTANCE_ID").unwrap_or_else(|_| format!("test-inst-{suffix}"));

    let mut config = BigtableStorageConfig {
        project_id,
        instance_id,
        dev_mode: true,
        ..BigtableStorageConfig::default()
    };

    let (host, client) = resolve_reachable_emulator_host(&config).await?;
    config.emulator_host = Some(host);

    let bootstrapper = BigtableBootstrapper::new(config.clone());
    bootstrapper
        .create_catalog_db()
        .await
        .expect("create_catalog_db");
    bootstrapper.create_data_db().await.expect("create_data_db");

    let target = Target::at(config.connection_string());
    let lock = BigtableCatalogMigrations
        .lock(&target, Duration::from_secs(10))
        .await
        .expect("lock migrations");
    BigtableCatalogMigrations
        .run(&lock, env!("CARGO_PKG_VERSION"), RunKind::Create)
        .await
        .expect("run baseline migrations");
    BigtableCatalogMigrations.release(lock).await;

    bootstrapper
        .bootstrap_encryption_key()
        .await
        .expect("bootstrap_encryption_key");
    bootstrapper
        .bootstrap_default_account()
        .await
        .expect("bootstrap_default_account");
    bootstrapper
        .bootstrap_admin_user(None, None)
        .await
        .expect("bootstrap_admin_user");

    ensure_txn_log_table(&client)
        .await
        .expect("ensure_txn_log_table");
    ensure_ttl_index_table(&client)
        .await
        .expect("ensure_ttl_index_table");

    let engine = Arc::new(BigtableEngine::new(
        client.clone(),
        client.clone(),
        config.intent_timeout_secs,
    ));
    let account_id = "123456789012".to_string();

    Some(TestContext {
        config,
        client,
        engine,
        account_id,
    })
}

// -----------------------------------------------------------------------------
// Test 1: Bootstrapping, Migrations & Table Lifecycle
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_bootstrapping_and_table_lifecycle() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let bootstrapper = BigtableBootstrapper::new(ctx.config.clone());
    assert!(bootstrapper.has_admin_user().await.unwrap());

    #[cfg(feature = "test-hooks")]
    {
        assert!(bootstrapper.is_catalog_initialized().await.unwrap());
        let latest_expected = MigrationId::LATEST.number().to_string();
        assert_eq!(
            bootstrapper.read_catalog_version().await.unwrap(),
            Some(latest_expected)
        );
    }

    let mig_status = BigtableCatalogMigrations
        .status(&Target::at(ctx.config.connection_string()))
        .await
        .expect("migration status");
    assert_eq!(
        mig_status,
        CatalogStatus::Current {
            version: MigrationId::LATEST.number()
        }
    );

    let table_name = unique_name("tbl_lifecycle");
    let input = CreateTableInput {
        table_name: table_name.clone(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        billing_mode: Some(BillingMode::PayPerRequest),
        deletion_protection_enabled: Some(false),
        ..CreateTableInput::default()
    };

    let desc = ctx
        .engine
        .create_table(&ctx.account_id, input)
        .await
        .expect("create_table failed");
    assert_eq!(desc.table_name, table_name);
    assert_eq!(desc.table_status, TableStatus::Active);

    let described = ctx
        .engine
        .describe_table(
            &ctx.account_id,
            DescribeTableInput {
                table_name: table_name.clone(),
            },
        )
        .await
        .expect("describe_table failed");
    assert_eq!(described.table_name, table_name);
    assert_eq!(described.key_schema.len(), 2);

    let list = ctx
        .engine
        .list_tables(
            &ctx.account_id,
            ListTablesInput {
                limit: None,
                exclusive_start_table_name: None,
            },
        )
        .await
        .expect("list_tables failed");
    assert!(list.table_names.contains(&table_name));

    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .expect("table_key_info failed");
    assert_eq!(key_info.table_name, table_name);
    assert_eq!(key_info.key_schema.len(), 2);
}

// -----------------------------------------------------------------------------
// Test 2: Guarded Single-Row CRUD Operations
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_guarded_single_row_crud() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_crud");
    let input = CreateTableInput {
        table_name: table_name.clone(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        billing_mode: Some(BillingMode::PayPerRequest),
        deletion_protection_enabled: Some(false),
        ..CreateTableInput::default()
    };

    ctx.engine
        .create_table(&ctx.account_id, input)
        .await
        .expect("create_table failed");
    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .expect("table_key_info failed");

    let mut item: Item = BTreeMap::new();
    item.insert("pk".to_string(), AttributeValue::S("user#1".to_string()));
    item.insert("sk".to_string(), AttributeValue::S("profile".to_string()));
    item.insert("name".to_string(), AttributeValue::S("Alice".to_string()));
    item.insert("age".to_string(), AttributeValue::N("30".to_string()));
    item.insert("counter".to_string(), AttributeValue::N("100".to_string()));
    item.insert("active".to_string(), AttributeValue::Bool(true));
    item.insert(
        "tags".to_string(),
        AttributeValue::SS(BTreeSet::from(["admin".to_string(), "eng".to_string()])),
    );

    let empty_maps = ExpressionMaps::default();
    let old = ctx
        .engine
        .put_item(&key_info, item.clone(), false, None, &empty_maps, None)
        .await
        .expect("put_item failed");
    assert!(old.is_none());

    let mut key: Item = BTreeMap::new();
    key.insert("pk".to_string(), AttributeValue::S("user#1".to_string()));
    key.insert("sk".to_string(), AttributeValue::S("profile".to_string()));

    let fetched = ctx
        .engine
        .get_item(&key_info, &key)
        .await
        .expect("get_item failed")
        .expect("item not found");
    assert_eq!(
        fetched.get("name"),
        Some(&AttributeValue::S("Alice".to_string()))
    );
    assert_eq!(
        fetched.get("age"),
        Some(&AttributeValue::N("30".to_string()))
    );
    assert_eq!(
        fetched.get("counter"),
        Some(&AttributeValue::N("100".to_string()))
    );
    assert_eq!(fetched.get("active"), Some(&AttributeValue::Bool(true)));

    let cond = parse_cond("attribute_not_exists(pk)");
    let ccf_result = ctx
        .engine
        .put_item(
            &key_info,
            item.clone(),
            false,
            Some(&cond),
            &empty_maps,
            None,
        )
        .await;
    assert!(matches!(ccf_result, Err(StorageError::ConditionFailed(_))));

    let mut update_maps = ExpressionMaps::default();
    update_maps
        .values
        .insert("new_age".to_string(), AttributeValue::N("31".to_string()));
    update_maps.values.insert(
        "email".to_string(),
        AttributeValue::S("alice@example.com".to_string()),
    );
    update_maps
        .values
        .insert("inc".to_string(), AttributeValue::N("25".to_string()));
    update_maps.values.insert(
        "expected_age".to_string(),
        AttributeValue::N("30".to_string()),
    );

    let update_actions =
        parse_upd("SET age = :new_age, email = :email, counter = counter + :inc REMOVE active");
    let update_cond = parse_cond("age = :expected_age");

    let (old_upd, new_upd) = ctx
        .engine
        .update_item(
            &key_info,
            &key,
            &update_actions,
            true,
            true,
            Some(&update_cond),
            &update_maps,
            None,
        )
        .await
        .expect("guarded update_item failed");

    assert_eq!(
        old_upd.unwrap().get("age"),
        Some(&AttributeValue::N("30".to_string()))
    );
    let new_upd = new_upd.expect("expected new item");
    assert_eq!(
        new_upd.get("age"),
        Some(&AttributeValue::N("31".to_string()))
    );
    assert_eq!(
        new_upd.get("counter"),
        Some(&AttributeValue::N("125".to_string()))
    );
    assert!(!new_upd.contains_key("active"));

    let failing_cond = parse_cond("age = :expected_age");
    let upd_fail = ctx
        .engine
        .update_item(
            &key_info,
            &key,
            &update_actions,
            false,
            false,
            Some(&failing_cond),
            &update_maps,
            None,
        )
        .await;
    assert!(matches!(upd_fail, Err(StorageError::ConditionFailed(_))));

    let del_fail = ctx
        .engine
        .delete_item(
            &key_info,
            &key,
            false,
            Some(&failing_cond),
            &update_maps,
            None,
        )
        .await;
    assert!(matches!(del_fail, Err(StorageError::ConditionFailed(_))));

    let del_ok = ctx
        .engine
        .delete_item(&key_info, &key, true, None, &empty_maps, None)
        .await
        .expect("delete_item failed");
    assert!(del_ok.is_some());

    let post_delete = ctx
        .engine
        .get_item(&key_info, &key)
        .await
        .expect("get_item failed");
    assert!(post_delete.is_none());

    // REMOVE-only UpdateItem on a missing item must write nothing and return (None, None).
    let remove_only = parse_upd("REMOVE active");
    let (ro_old, ro_new) = ctx
        .engine
        .update_item(
            &key_info,
            &key,
            &remove_only,
            true,
            true,
            None,
            &empty_maps,
            None,
        )
        .await
        .expect("remove-only update on missing item");
    assert!(ro_old.is_none());
    assert!(ro_new.is_none());
    assert!(
        ctx.engine
            .get_item(&key_info, &key)
            .await
            .unwrap()
            .is_none()
    );
}

// -----------------------------------------------------------------------------
// Test 3: 2PC Multi-Row Transactions, Rollback & Idempotency
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_transact_write_items_and_rollback() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let tbl1_name = unique_name("tbl_tx_orders");
    let tbl2_name = unique_name("tbl_tx_inventory");

    let input1 = CreateTableInput {
        table_name: tbl1_name.clone(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "order_id".to_string(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "order_id".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        billing_mode: Some(BillingMode::PayPerRequest),
        deletion_protection_enabled: Some(false),
        ..CreateTableInput::default()
    };
    ctx.engine
        .create_table(&ctx.account_id, input1)
        .await
        .unwrap();
    let key_info1 = ctx
        .engine
        .table_key_info(&ctx.account_id, &tbl1_name)
        .await
        .unwrap();

    let input2 = CreateTableInput {
        table_name: tbl2_name.clone(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "item_id".to_string(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "item_id".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        billing_mode: Some(BillingMode::PayPerRequest),
        deletion_protection_enabled: Some(false),
        ..CreateTableInput::default()
    };
    ctx.engine
        .create_table(&ctx.account_id, input2)
        .await
        .unwrap();
    let key_info2 = ctx
        .engine
        .table_key_info(&ctx.account_id, &tbl2_name)
        .await
        .unwrap();

    let empty_maps = ExpressionMaps::default();

    let mut order_item: Item = BTreeMap::new();
    order_item.insert(
        "order_id".to_string(),
        AttributeValue::S("ord#1001".to_string()),
    );
    order_item.insert("total".to_string(), AttributeValue::N("250".to_string()));

    let mut inv_item: Item = BTreeMap::new();
    inv_item.insert(
        "item_id".to_string(),
        AttributeValue::S("prod#50".to_string()),
    );
    inv_item.insert("stock".to_string(), AttributeValue::N("99".to_string()));

    let ops = vec![
        TransactWriteOp::Put {
            key_info: &key_info1,
            item: &order_item,
            condition: None,
            maps: &empty_maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
        TransactWriteOp::Put {
            key_info: &key_info2,
            item: &inv_item,
            condition: None,
            maps: &empty_maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
    ];

    let token_str = format!("tok-{}", Uuid::new_v4());
    let token = IdempotencyKey {
        account_id: &ctx.account_id,
        token: &token_str,
        fingerprint: "fp-ord1001",
    };

    ctx.engine
        .transact_write_items(&ops, Some(token))
        .await
        .expect("transact_write_items across distinct tables failed");

    let mut ord_key: Item = BTreeMap::new();
    ord_key.insert(
        "order_id".to_string(),
        AttributeValue::S("ord#1001".to_string()),
    );
    assert!(
        ctx.engine
            .get_item(&key_info1, &ord_key)
            .await
            .unwrap()
            .is_some()
    );

    let mut inv_key: Item = BTreeMap::new();
    inv_key.insert(
        "item_id".to_string(),
        AttributeValue::S("prod#50".to_string()),
    );
    assert!(
        ctx.engine
            .get_item(&key_info2, &inv_key)
            .await
            .unwrap()
            .is_some()
    );

    let replay_token = IdempotencyKey {
        account_id: &ctx.account_id,
        token: &token_str,
        fingerprint: "fp-ord1001",
    };
    assert!(matches!(
        ctx.engine
            .transact_write_items(&[], Some(replay_token))
            .await,
        Err(StorageError::IdempotentReplay)
    ));

    let mismatch_token = IdempotencyKey {
        account_id: &ctx.account_id,
        token: &token_str,
        fingerprint: "fp-mismatch",
    };
    assert!(matches!(
        ctx.engine
            .transact_write_items(&[], Some(mismatch_token))
            .await,
        Err(StorageError::IdempotentMismatch)
    ));

    let get_op1 = TransactGetOp {
        key_info: &key_info1,
        key: &ord_key,
    };
    let get_op2 = TransactGetOp {
        key_info: &key_info2,
        key: &inv_key,
    };
    let fetched_items = ctx
        .engine
        .transact_get_items(&[get_op1, get_op2])
        .await
        .unwrap();
    assert_eq!(fetched_items.len(), 2);
    assert!(fetched_items[0].is_some());
    assert!(fetched_items[1].is_some());

    let mut order_item2: Item = BTreeMap::new();
    order_item2.insert(
        "order_id".to_string(),
        AttributeValue::S("ord#1002".to_string()),
    );
    order_item2.insert("total".to_string(), AttributeValue::N("500".to_string()));

    let mut missing_inv_key: Item = BTreeMap::new();
    missing_inv_key.insert(
        "item_id".to_string(),
        AttributeValue::S("prod#9999".to_string()),
    );
    let fail_cond = parse_cond("attribute_exists(item_id)");

    let rollback_ops = vec![
        TransactWriteOp::Put {
            key_info: &key_info1,
            item: &order_item2,
            condition: None,
            maps: &empty_maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: None,
        },
        TransactWriteOp::ConditionCheck {
            key_info: &key_info2,
            key: &missing_inv_key,
            condition: &fail_cond,
            maps: &empty_maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        },
    ];

    assert!(
        ctx.engine
            .transact_write_items(&rollback_ops, None)
            .await
            .is_err()
    );

    let mut ord2_key: Item = BTreeMap::new();
    ord2_key.insert(
        "order_id".to_string(),
        AttributeValue::S("ord#1002".to_string()),
    );
    assert!(
        ctx.engine
            .get_item(&key_info1, &ord2_key)
            .await
            .unwrap()
            .is_none()
    );
}

// -----------------------------------------------------------------------------
// Test 4: Decimal Number Key Encoding, Reverse Query & Parallel Scan Pagination
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_decimal_number_keys_and_range_scan() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_decimal_keys");
    let input = CreateTableInput {
        table_name: table_name.clone(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "sensor_id".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "reading".to_string(),
                key_type: KeyType::Range,
            },
        ],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "sensor_id".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "reading".to_string(),
                attribute_type: ScalarAttributeType::N,
            },
        ],
        billing_mode: Some(BillingMode::PayPerRequest),
        deletion_protection_enabled: Some(false),
        ..CreateTableInput::default()
    };

    ctx.engine
        .create_table(&ctx.account_id, input)
        .await
        .unwrap();
    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();

    let readings = vec![
        "-1000.5", "-100.5", "-50.25", "-1.0", "0", "0.005", "0.5", "1.0", "10.5", "100.0",
        "999.99", "1000.5",
    ];

    let empty_maps = ExpressionMaps::default();
    for r in &readings {
        let mut item: Item = BTreeMap::new();
        item.insert(
            "sensor_id".to_string(),
            AttributeValue::S("sensor#1".to_string()),
        );
        item.insert("reading".to_string(), AttributeValue::N((*r).to_string()));
        item.insert("status".to_string(), AttributeValue::S("OK".to_string()));
        ctx.engine
            .put_item(&key_info, item, false, None, &empty_maps, None)
            .await
            .unwrap();
    }

    let mut query_maps = ExpressionMaps::default();
    query_maps
        .values
        .insert("sid".to_string(), AttributeValue::S("sensor#1".to_string()));
    query_maps
        .values
        .insert("lo".to_string(), AttributeValue::N("-50.25".to_string()));
    query_maps
        .values
        .insert("hi".to_string(), AttributeValue::N("10.5".to_string()));

    let kc = parse_kc("sensor_id = :sid AND reading BETWEEN :lo AND :hi");

    let (items, lek_all) = ctx
        .engine
        .query(&key_info, &kc, &query_maps, true, None, None, None)
        .await
        .expect("query failed");
    assert!(lek_all.is_none());

    let expected = vec!["-50.25", "-1.0", "0", "0.005", "0.5", "1.0", "10.5"];
    let actual: Vec<String> = items
        .iter()
        .map(|item| match item.get("reading").unwrap() {
            AttributeValue::N(s) => s.clone(),
            _ => panic!("expected N attribute"),
        })
        .collect();
    assert_eq!(actual, expected, "readings must match numerical sort order");

    // Reverse query (both full and paginated)
    let (rev_items, rev_lek) = ctx
        .engine
        .query(&key_info, &kc, &query_maps, false, None, None, None)
        .await
        .expect("reverse query failed");
    assert!(rev_lek.is_none());

    let mut rev_expected = expected.clone();
    rev_expected.reverse();
    let rev_actual: Vec<String> = rev_items
        .iter()
        .map(|item| match item.get("reading").unwrap() {
            AttributeValue::N(s) => s.clone(),
            _ => panic!("expected N attribute"),
        })
        .collect();
    assert_eq!(
        rev_actual, rev_expected,
        "reverse scan must match reversed numerical order"
    );

    // Reverse pagination check (3 + 4 = 7, with LastEvaluatedKey omitted on exact boundary!)
    let (rev_p1, rev_k1) = ctx
        .engine
        .query(&key_info, &kc, &query_maps, false, Some(3), None, None)
        .await
        .expect("reverse page 1");
    assert_eq!(rev_p1.len(), 3);
    assert!(rev_k1.is_some());
    let (rev_p2, rev_k2) = ctx
        .engine
        .query(
            &key_info,
            &kc,
            &query_maps,
            false,
            Some(4),
            rev_k1.as_ref(),
            None,
        )
        .await
        .expect("reverse page 2");
    assert_eq!(rev_p2.len(), 4);
    assert!(
        rev_k2.is_none(),
        "LastEvaluatedKey must be None when exact limit reaches end of range"
    );

    // Scan table with limit pagination (6 + 6 = 12)
    let (scanned_first, last_key) = ctx
        .engine
        .scan(&key_info, Some(6), None, None, None, None)
        .await
        .expect("scan first page");
    assert_eq!(scanned_first.len(), 6);
    assert!(last_key.is_some());

    let (scanned_second, second_lek) = ctx
        .engine
        .scan(&key_info, Some(6), last_key.as_ref(), None, None, None)
        .await
        .expect("scan second page");
    assert_eq!(scanned_second.len(), 6);
    assert!(second_lek.is_none());

    // Inverted BETWEEN (:lo > :hi) must return empty results cleanly in both
    // forward and reverse directions without sending an invalid RowRange RPC.
    let mut inv_maps = ExpressionMaps::default();
    inv_maps
        .values
        .insert("sid".to_string(), AttributeValue::S("sensor#1".to_string()));
    inv_maps
        .values
        .insert("lo".to_string(), AttributeValue::N("50.0".to_string()));
    inv_maps
        .values
        .insert("hi".to_string(), AttributeValue::N("-50.0".to_string()));
    for forward in [true, false] {
        let (inv_items, inv_lek) = ctx
            .engine
            .query(&key_info, &kc, &inv_maps, forward, Some(5), None, None)
            .await
            .expect("inverted BETWEEN query must not fail");
        assert!(inv_items.is_empty());
        assert!(inv_lek.is_none());
    }
}

// -----------------------------------------------------------------------------
// Test 5: GSI Shadow Table Operations & Queries
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_gsi_shadow_table_operations_and_query() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_gsi_test");
    let index_name = "DepartmentSalaryIndex";

    let gsi = GsiInput {
        index_name: index_name.to_string(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "department".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "salary".to_string(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        },
        provisioned_throughput: None,
        warm_throughput: None,
    };

    let input = CreateTableInput {
        table_name: table_name.clone(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "emp_id".to_string(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "emp_id".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "department".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "salary".to_string(),
                attribute_type: ScalarAttributeType::N,
            },
        ],
        billing_mode: Some(BillingMode::PayPerRequest),
        global_secondary_indexes: Some(vec![gsi]),
        deletion_protection_enabled: Some(false),
        ..CreateTableInput::default()
    };

    ctx.engine
        .create_table(&ctx.account_id, input)
        .await
        .unwrap();
    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();

    let empty_maps = ExpressionMaps::default();

    for (id, dept, sal, name) in [
        ("emp#1", "Engineering", "120000", "Alice"),
        ("emp#2", "Engineering", "150000", "Bob"),
        ("emp#3", "Sales", "90000", "Carol"),
    ] {
        let mut emp: Item = BTreeMap::new();
        emp.insert("emp_id".to_string(), AttributeValue::S(id.to_string()));
        emp.insert(
            "department".to_string(),
            AttributeValue::S(dept.to_string()),
        );
        emp.insert("salary".to_string(), AttributeValue::N(sal.to_string()));
        emp.insert("name".to_string(), AttributeValue::S(name.to_string()));
        ctx.engine
            .put_item(&key_info, emp, false, None, &empty_maps, None)
            .await
            .unwrap();
    }

    let mut gsi_maps = ExpressionMaps::default();
    gsi_maps.values.insert(
        "dept".to_string(),
        AttributeValue::S("Engineering".to_string()),
    );
    let gsi_kc = parse_kc("department = :dept");

    let (gsi_items, _) = ctx
        .engine
        .query(
            &key_info,
            &gsi_kc,
            &gsi_maps,
            true,
            None,
            None,
            Some(index_name),
        )
        .await
        .expect("GSI query failed");

    assert_eq!(gsi_items.len(), 2);
    assert_eq!(
        gsi_items[0].get("emp_id"),
        Some(&AttributeValue::S("emp#1".to_string()))
    );
    assert_eq!(
        gsi_items[1].get("emp_id"),
        Some(&AttributeValue::S("emp#2".to_string()))
    );

    let mut upd_key: Item = BTreeMap::new();
    upd_key.insert("emp_id".to_string(), AttributeValue::S("emp#1".to_string()));

    let mut upd_maps = ExpressionMaps::default();
    upd_maps.values.insert(
        "new_dept".to_string(),
        AttributeValue::S("Management".to_string()),
    );
    upd_maps.values.insert(
        "new_salary".to_string(),
        AttributeValue::N("175000".to_string()),
    );
    let upd_actions = parse_upd("SET department = :new_dept, salary = :new_salary");

    ctx.engine
        .update_item(
            &key_info,
            &upd_key,
            &upd_actions,
            false,
            false,
            None,
            &upd_maps,
            None,
        )
        .await
        .expect("update emp1 failed");

    let (gsi_items_after, _) = ctx
        .engine
        .query(
            &key_info,
            &gsi_kc,
            &gsi_maps,
            true,
            None,
            None,
            Some(index_name),
        )
        .await
        .unwrap();
    assert_eq!(gsi_items_after.len(), 1);
    assert_eq!(
        gsi_items_after[0].get("emp_id"),
        Some(&AttributeValue::S("emp#2".to_string()))
    );

    let mut mgmt_maps = ExpressionMaps::default();
    mgmt_maps.values.insert(
        "dept".to_string(),
        AttributeValue::S("Management".to_string()),
    );
    let (mgmt_items, _) = ctx
        .engine
        .query(
            &key_info,
            &gsi_kc,
            &mgmt_maps,
            true,
            None,
            None,
            Some(index_name),
        )
        .await
        .unwrap();
    assert_eq!(mgmt_items.len(), 1);
    assert_eq!(
        mgmt_items[0].get("emp_id"),
        Some(&AttributeValue::S("emp#1".to_string()))
    );

    let mut del_key: Item = BTreeMap::new();
    del_key.insert("emp_id".to_string(), AttributeValue::S("emp#2".to_string()));
    ctx.engine
        .delete_item(&key_info, &del_key, false, None, &empty_maps, None)
        .await
        .expect("delete emp2 failed");

    let (gsi_items_after_del, _) = ctx
        .engine
        .query(
            &key_info,
            &gsi_kc,
            &gsi_maps,
            true,
            None,
            None,
            Some(index_name),
        )
        .await
        .unwrap();
    assert_eq!(gsi_items_after_del.len(), 0);

    // GSI Sort Key Ordering & BeginsWith test (FDB escape encoding)
    let str_gsi_table = unique_name("tbl_str_gsi");
    let str_gsi_name = "CategoryCodeIndex";
    let str_gsi = GsiInput {
        index_name: str_gsi_name.to_string(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "category".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "code".to_string(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: ProjectionType::All,
            non_key_attributes: None,
        },
        provisioned_throughput: None,
        warm_throughput: None,
    };
    let str_input = CreateTableInput {
        table_name: str_gsi_table.clone(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "id".to_string(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "id".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "category".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "code".to_string(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        billing_mode: Some(BillingMode::PayPerRequest),
        global_secondary_indexes: Some(vec![str_gsi]),
        deletion_protection_enabled: Some(false),
        ..CreateTableInput::default()
    };
    ctx.engine
        .create_table(&ctx.account_id, str_input)
        .await
        .unwrap();
    let str_key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &str_gsi_table)
        .await
        .unwrap();

    for (id_val, code_val) in [
        ("id#3", "B"),
        ("id#1", "A"),
        ("id#2", "AB"),
        ("id#4", "ABC"),
    ] {
        let mut it: Item = BTreeMap::new();
        it.insert("id".to_string(), AttributeValue::S(id_val.to_string()));
        it.insert(
            "category".to_string(),
            AttributeValue::S("cat1".to_string()),
        );
        it.insert("code".to_string(), AttributeValue::S(code_val.to_string()));
        ctx.engine
            .put_item(&str_key_info, it, false, None, &empty_maps, None)
            .await
            .unwrap();
    }

    let mut cat_maps = ExpressionMaps::default();
    cat_maps
        .values
        .insert("cat".to_string(), AttributeValue::S("cat1".to_string()));
    let cat_kc = parse_kc("category = :cat");
    let (all_items, _) = ctx
        .engine
        .query(
            &str_key_info,
            &cat_kc,
            &cat_maps,
            true,
            None,
            None,
            Some(str_gsi_name),
        )
        .await
        .unwrap();
    let codes: Vec<&str> = all_items
        .iter()
        .map(|it| match it.get("code").unwrap() {
            AttributeValue::S(s) => s.as_str(),
            _ => panic!(),
        })
        .collect();
    assert_eq!(codes, vec!["A", "AB", "ABC", "B"]);

    let mut bw_maps = ExpressionMaps::default();
    bw_maps
        .values
        .insert("cat".to_string(), AttributeValue::S("cat1".to_string()));
    bw_maps
        .values
        .insert("pfx".to_string(), AttributeValue::S("AB".to_string()));
    let bw_kc = parse_kc("category = :cat AND begins_with(code, :pfx)");
    let (bw_items, _) = ctx
        .engine
        .query(
            &str_key_info,
            &bw_kc,
            &bw_maps,
            true,
            None,
            None,
            Some(str_gsi_name),
        )
        .await
        .unwrap();
    let bw_codes: Vec<&str> = bw_items
        .iter()
        .map(|it| match it.get("code").unwrap() {
            AttributeValue::S(s) => s.as_str(),
            _ => panic!(),
        })
        .collect();
    assert_eq!(bw_codes, vec!["AB", "ABC"]);

    // Verify background GSI reconciler sweep succeeds cleanly against composite shadow keys
    extenddb_storage_bigtable::gsi_worker::sweep_once(&ctx.engine)
        .await
        .expect("GSI reconciler sweep_once");

    // Secondary-index key type/emptiness & missing-item REMOVE/DELETE validation
    let mut bad_gsi_item: Item = BTreeMap::new();
    bad_gsi_item.insert(
        "emp_id".to_string(),
        AttributeValue::S("emp#bad".to_string()),
    );
    bad_gsi_item.insert(
        "department".to_string(),
        AttributeValue::S("Engineering".to_string()),
    );
    bad_gsi_item.insert(
        "salary".to_string(),
        AttributeValue::S("not-a-number".to_string()),
    );
    assert!(
        matches!(
            ctx.engine
                .put_item(&key_info, bad_gsi_item, false, None, &empty_maps, None)
                .await,
            Err(StorageError::Validation(_))
        ),
        "PutItem with wrong GSI key scalar type must fail with Validation"
    );

    let mut bad_upd_maps = ExpressionMaps::default();
    bad_upd_maps
        .values
        .insert("empty_str".to_string(), AttributeValue::S(String::new()));
    let bad_upd_act = parse_upd("SET department = :empty_str");
    let mut emp3_key: Item = BTreeMap::new();
    emp3_key.insert("emp_id".to_string(), AttributeValue::S("emp#3".to_string()));
    assert!(
        matches!(
            ctx.engine
                .update_item(
                    &key_info,
                    &emp3_key,
                    &bad_upd_act,
                    false,
                    false,
                    None,
                    &bad_upd_maps,
                    None,
                )
                .await,
            Err(StorageError::Validation(_))
        ),
        "UpdateItem setting GSI key to empty string must fail with Validation"
    );

    // Invalid REMOVE/DELETE expression on a non-existent item must fail validation
    // BEFORE the `removes_only` missing-item no-op short-circuit (#478).
    let mut missing_emp_key: Item = BTreeMap::new();
    missing_emp_key.insert(
        "emp_id".to_string(),
        AttributeValue::S("emp#missing".to_string()),
    );
    let bad_del_act = parse_upd("DELETE tags :unbound_set_val");
    assert!(
        matches!(
            ctx.engine
                .update_item(
                    &key_info,
                    &missing_emp_key,
                    &bad_del_act,
                    false,
                    false,
                    None,
                    &empty_maps,
                    None,
                )
                .await,
            Err(StorageError::Validation(_))
        ),
        "Invalid DELETE operand on missing item must return Validation (#478)"
    );
}

// -----------------------------------------------------------------------------
// Test 6: TransactGetItems Read-Through-Log Rule
// -----------------------------------------------------------------------------
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_transact_get_items_read_through_log_rule() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_tget_rule");
    let input = CreateTableInput {
        table_name: table_name.clone(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        billing_mode: Some(BillingMode::PayPerRequest),
        deletion_protection_enabled: Some(false),
        ..CreateTableInput::default()
    };
    ctx.engine
        .create_table(&ctx.account_id, input)
        .await
        .unwrap();
    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let empty_maps = ExpressionMaps::default();

    let mut item1: Item = BTreeMap::new();
    item1.insert("pk".to_string(), AttributeValue::S("row1".to_string()));
    item1.insert("val".to_string(), AttributeValue::S("initial1".to_string()));
    ctx.engine
        .put_item(&key_info, item1.clone(), false, None, &empty_maps, None)
        .await
        .unwrap();

    let mut item2: Item = BTreeMap::new();
    item2.insert("pk".to_string(), AttributeValue::S("row2".to_string()));
    item2.insert("val".to_string(), AttributeValue::S("initial2".to_string()));
    ctx.engine
        .put_item(&key_info, item2.clone(), false, None, &empty_maps, None)
        .await
        .unwrap();

    let data_table = ctx.engine.data_table_for(&key_info).await.unwrap();
    let enc_key1 = extenddb_storage_bigtable::data::encoding::row_key::encode_key(
        &item1,
        &key_info.key_schema,
    )
    .unwrap();
    let enc_key2 = extenddb_storage_bigtable::data::encoding::row_key::encode_key(
        &item2,
        &key_info.key_schema,
    )
    .unwrap();

    let coord = extenddb_storage_bigtable::transact::TxnCoordinator::new(
        ctx.engine.client_ref(),
        Duration::from_secs(60),
    );

    // Case A: Active intent on row1 with txn in COMMITTED state
    let txn_id_committed = "txn-test-committed-123";
    let part1 = extenddb_storage_bigtable::transact::ParticipantRow {
        data_table: data_table.clone(),
        row_key: enc_key1.clone(),
    };
    let (_, started_at_committed) = coord
        .open(txn_id_committed, std::slice::from_ref(&part1))
        .await
        .unwrap();
    coord.place_intent(txn_id_committed, &part1).await.unwrap();

    let mut committed_item1 = item1.clone();
    committed_item1.insert(
        "val".to_string(),
        AttributeValue::S("committed_update".to_string()),
    );
    let mut_payload = vec![extenddb_storage_bigtable::transact::ParticipantMutation {
        participant: part1.clone(),
        payload: extenddb_storage_bigtable::transact::TxnOpPayload::Put {
            item: committed_item1,
        },
        expected_version: Some(1),
        row_existed: true,
        new_version: Some(2),
    }];
    coord
        .commit(txn_id_committed, started_at_committed, &mut_payload, None)
        .await
        .unwrap();

    // Case B: Active intent on row2 with txn in PENDING state
    let txn_id_pending = "txn-test-pending-456";
    let part2 = extenddb_storage_bigtable::transact::ParticipantRow {
        data_table,
        row_key: enc_key2,
    };
    let _ = coord
        .open(txn_id_pending, std::slice::from_ref(&part2))
        .await
        .unwrap();
    coord.place_intent(txn_id_pending, &part2).await.unwrap();

    let mut k1: Item = BTreeMap::new();
    k1.insert("pk".to_string(), AttributeValue::S("row1".to_string()));
    let mut k2: Item = BTreeMap::new();
    k2.insert("pk".to_string(), AttributeValue::S("row2".to_string()));

    let op1 = TransactGetOp {
        key_info: &key_info,
        key: &k1,
    };
    let op2 = TransactGetOp {
        key_info: &key_info,
        key: &k2,
    };

    let results = ctx
        .engine
        .transact_get_items(&[op1, op2])
        .await
        .expect("transact_get_items must succeed");

    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].as_ref().and_then(|it| it.get("val")),
        Some(&AttributeValue::S("committed_update".to_string())),
        "TransactGetItems must serve logged post-image for COMMITTED transaction"
    );
    assert_eq!(
        results[1].as_ref().and_then(|it| it.get("val")),
        Some(&AttributeValue::S("initial2".to_string())),
        "TransactGetItems must serve current d value for PENDING transaction"
    );

    let _ = coord.clear_intent(txn_id_committed, &part1).await;
    let _ = coord.drop(txn_id_committed).await;
    let _ = coord.clear_intent(txn_id_pending, &part2).await;
    let _ = coord.drop(txn_id_pending).await;
}

// -----------------------------------------------------------------------------
// Test 7: TTL Index Maintenance & Sweep
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_ttl_index_operations() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_ttl");
    let input = CreateTableInput {
        table_name: table_name.clone(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "session_id".to_string(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "session_id".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        billing_mode: Some(BillingMode::PayPerRequest),
        deletion_protection_enabled: Some(false),
        ..CreateTableInput::default()
    };

    ctx.engine
        .create_table(&ctx.account_id, input)
        .await
        .unwrap();

    ctx.engine
        .update_ttl(&ctx.account_id, &table_name, "expires_at", true)
        .await
        .expect("update_ttl failed");

    let ttl_desc = ctx
        .engine
        .describe_ttl(&ctx.account_id, &table_name)
        .await
        .expect("describe_ttl failed");
    assert_eq!(ttl_desc.time_to_live_status, TimeToLiveStatus::Enabled);
    assert_eq!(ttl_desc.attribute_name, Some("expires_at".to_string()));

    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let empty_maps = ExpressionMaps::default();

    ensure_ttl_index_table(&ctx.client)
        .await
        .expect("ensure TTL table");

    let mut expired_item: Item = BTreeMap::new();
    expired_item.insert(
        "session_id".to_string(),
        AttributeValue::S("sess#expired".to_string()),
    );
    expired_item.insert(
        "expires_at".to_string(),
        AttributeValue::N("1000".to_string()),
    );
    expired_item.insert(
        "data".to_string(),
        AttributeValue::S("temp_token".to_string()),
    );
    ctx.engine
        .put_item(&key_info, expired_item, false, None, &empty_maps, None)
        .await
        .unwrap();

    let mut valid_item: Item = BTreeMap::new();
    valid_item.insert(
        "session_id".to_string(),
        AttributeValue::S("sess#active".to_string()),
    );
    valid_item.insert(
        "expires_at".to_string(),
        AttributeValue::N("9999999999".to_string()),
    );
    valid_item.insert(
        "data".to_string(),
        AttributeValue::S("permanent_token".to_string()),
    );
    ctx.engine
        .put_item(&key_info, valid_item, false, None, &empty_maps, None)
        .await
        .unwrap();

    let mut perm_item: Item = BTreeMap::new();
    perm_item.insert(
        "session_id".to_string(),
        AttributeValue::S("sess#permanent".to_string()),
    );
    perm_item.insert(
        "data".to_string(),
        AttributeValue::S("no_ttl_token".to_string()),
    );
    ctx.engine
        .put_item(&key_info, perm_item, false, None, &empty_maps, None)
        .await
        .unwrap();

    sweep_once(&ctx.engine).await.expect("sweep_once failed");

    let mut exp_key: Item = BTreeMap::new();
    exp_key.insert(
        "session_id".to_string(),
        AttributeValue::S("sess#expired".to_string()),
    );
    assert!(
        ctx.engine
            .get_item(&key_info, &exp_key)
            .await
            .unwrap()
            .is_none(),
        "expired session must be deleted by TTL worker sweep"
    );

    let mut act_key: Item = BTreeMap::new();
    act_key.insert(
        "session_id".to_string(),
        AttributeValue::S("sess#active".to_string()),
    );
    assert!(
        ctx.engine
            .get_item(&key_info, &act_key)
            .await
            .unwrap()
            .is_some()
    );

    let mut perm_key: Item = BTreeMap::new();
    perm_key.insert(
        "session_id".to_string(),
        AttributeValue::S("sess#permanent".to_string()),
    );
    assert!(
        ctx.engine
            .get_item(&key_info, &perm_key)
            .await
            .unwrap()
            .is_some()
    );
}

// -----------------------------------------------------------------------------
// Test 8: Deterministic OCC-Derived Cell Timestamps & Concurrent Contention
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_occ_deterministic_cell_timestamps_and_contention() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_occ_ts");
    let input = CreateTableInput {
        table_name: table_name.clone(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        billing_mode: Some(BillingMode::PayPerRequest),
        deletion_protection_enabled: Some(false),
        ..CreateTableInput::default()
    };
    ctx.engine
        .create_table(&ctx.account_id, input)
        .await
        .unwrap();
    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let data_table = ctx.engine.data_table_for(&key_info).await.unwrap();
    let empty_maps = ExpressionMaps::default();

    // 1. Sub-millisecond sequential updates must stamp all cells in a write with
    //    the exact OCC-derived timestamp `m:v * 1000` (millisecond-aligned) and
    //    strictly advance timestamps across rapid writes.
    let mut item: Item = BTreeMap::new();
    item.insert("pk".to_string(), AttributeValue::S("k_ts".to_string()));
    item.insert("a".to_string(), AttributeValue::S("v1".to_string()));
    item.insert("b".to_string(), AttributeValue::N("0".to_string()));
    ctx.engine
        .put_item(&key_info, item.clone(), false, None, &empty_maps, None)
        .await
        .unwrap();

    let mut key: Item = BTreeMap::new();
    key.insert("pk".to_string(), AttributeValue::S("k_ts".to_string()));
    let encoded_rk =
        extenddb_storage_bigtable::data::encoding::row_key::encode_key(&key, &key_info.key_schema)
            .unwrap();

    let read_cell_timestamps = || {
        let client = ctx.client.clone();
        let full_table = client.full_table_name(&data_table);
        let rk = encoded_rk.clone();
        async move {
            let mut data = client.data();
            let resp = data
                .read_rows(ReadRowsRequest {
                    table_name: full_table,
                    rows_limit: 1,
                    rows: Some(RowSet {
                        row_keys: vec![rk],
                        row_ranges: vec![],
                    }),
                    ..ReadRowsRequest::default()
                })
                .await
                .unwrap();
            let (_, cells) = resp.into_iter().next().expect("row present");
            let mut version_val = None;
            let mut cell_ts = Vec::new();
            for c in cells {
                if c.family_name == "m" && c.qualifier == b"v" && version_val.is_none() {
                    version_val =
                        extenddb_storage_bigtable::data::item_ops::decode_version(&c.value);
                    cell_ts.push(c.timestamp_micros);
                } else if c.family_name == "d" {
                    cell_ts.push(c.timestamp_micros);
                }
            }
            (version_val.expect("m:v present"), cell_ts)
        }
    };

    let (v1, ts_list_1) = read_cell_timestamps().await;
    let expected_ts1 = (v1 * 1_000) as i64;
    assert_eq!(expected_ts1 % 1_000, 0);
    for ts in &ts_list_1 {
        assert_eq!(
            *ts, expected_ts1,
            "every cell of write 1 must share timestamp m:v * 1000"
        );
    }

    // Immediate back-to-back update without sleeping
    let mut upd_maps = ExpressionMaps::default();
    upd_maps
        .values
        .insert("one".to_string(), AttributeValue::N("1".to_string()));
    upd_maps
        .values
        .insert("v2".to_string(), AttributeValue::S("v2".to_string()));
    let upd_actions = parse_upd("SET a = :v2, b = b + :one");
    ctx.engine
        .update_item(
            &key_info,
            &key,
            &upd_actions,
            false,
            false,
            None,
            &upd_maps,
            None,
        )
        .await
        .unwrap();

    let (v2, ts_list_2) = read_cell_timestamps().await;
    assert!(v2 > v1, "OCC version must strictly advance: {v2} > {v1}");
    let expected_ts2 = (v2 * 1_000) as i64;
    for ts in &ts_list_2 {
        assert_eq!(
            *ts, expected_ts2,
            "every cell of write 2 must share timestamp m:v * 1000"
        );
    }

    // 1b. DeleteItem must clear family `d` (so GetItem & Scan omit the row) while
    //     preserving a strictly higher `m:v` tombstone version, and rapid re-insert
    //     must strictly advance `m:v` again without timestamp regression.
    ctx.engine
        .delete_item(&key_info, &key, false, None, &empty_maps, None)
        .await
        .expect("delete k_ts");
    assert!(
        ctx.engine
            .get_item(&key_info, &key)
            .await
            .unwrap()
            .is_none()
    );
    let (scan_after_del, _) = ctx
        .engine
        .scan(&key_info, None, None, None, None, None)
        .await
        .unwrap();
    assert!(
        scan_after_del.is_empty(),
        "version-only tombstone must never surface in Scan"
    );
    let (v_del, ts_list_del) = read_cell_timestamps().await;
    assert!(
        v_del > v2,
        "tombstone OCC version must advance across DeleteItem: {v_del} > {v2}"
    );
    assert_eq!(
        ts_list_del,
        vec![(v_del * 1_000) as i64],
        "tombstone row must have m:v cell only (zero d:* cells) at v_del * 1000"
    );

    // Re-insert `k_ts` with `b = 1` so step 2's 10 concurrent +1 increments start from 1.
    let mut reinsert_item: Item = BTreeMap::new();
    reinsert_item.insert("pk".to_string(), AttributeValue::S("k_ts".to_string()));
    reinsert_item.insert("a".to_string(), AttributeValue::S("v2".to_string()));
    reinsert_item.insert("b".to_string(), AttributeValue::N("1".to_string()));
    ctx.engine
        .put_item(&key_info, reinsert_item, false, None, &empty_maps, None)
        .await
        .expect("re-put k_ts");
    let (v_reput, ts_list_reput) = read_cell_timestamps().await;
    assert!(
        v_reput > v_del,
        "re-inserted OCC version must advance beyond tombstone: {v_reput} > {v_del}"
    );
    for ts in &ts_list_reput {
        assert_eq!(*ts, (v_reput * 1_000) as i64);
    }

    // 2. Concurrent arithmetic UpdateItem contention: 10 tasks increment b by 1.
    let mut handles = Vec::new();
    for _ in 0..10 {
        let eng = ctx.engine.clone();
        let ki = key_info.clone();
        let k = key.clone();
        handles.push(tokio::spawn(async move {
            let mut m = ExpressionMaps::default();
            m.values
                .insert("one".to_string(), AttributeValue::N("1".to_string()));
            let act = parse_upd("SET b = b + :one");
            eng.update_item(&ki, &k, &act, false, false, None, &m, None)
                .await
        }));
    }
    for h in handles {
        h.await
            .unwrap()
            .expect("concurrent UpdateItem must serialize via OCC retry");
    }
    let final_item = ctx
        .engine
        .get_item(&key_info, &key)
        .await
        .unwrap()
        .expect("final item");
    assert_eq!(
        final_item.get("b"),
        Some(&AttributeValue::N("11".to_string())),
        "1 initial + 10 concurrent increments must equal 11 with zero lost updates"
    );

    // 3. Concurrent conditional PutItem (`attribute_not_exists(pk)`): exactly 1 wins,
    //    all 7 losers fail with `StorageError::ConditionFailed`.
    let mut race_handles = Vec::new();
    for i in 0..8 {
        let eng = ctx.engine.clone();
        let ki = key_info.clone();
        race_handles.push(tokio::spawn(async move {
            let mut it = BTreeMap::new();
            it.insert("pk".to_string(), AttributeValue::S("race_pk".to_string()));
            it.insert("winner".to_string(), AttributeValue::S(format!("task-{i}")));
            let cond = parse_cond("attribute_not_exists(pk)");
            let maps = ExpressionMaps::default();
            eng.put_item(&ki, it, false, Some(&cond), &maps, None).await
        }));
    }
    let mut wins = 0;
    let mut cond_failures = 0;
    for h in race_handles {
        match h.await.unwrap() {
            Ok(_) => wins += 1,
            Err(StorageError::ConditionFailed(_)) => cond_failures += 1,
            Err(other) => panic!("unexpected error in conditional race: {other:?}"),
        }
    }
    assert_eq!(wins, 1, "exactly one conditional PutItem must succeed");
    assert_eq!(
        cond_failures, 7,
        "all 7 losing conditional PutItems must return ConditionFailed"
    );
}

async fn read_all_stream_records(
    engine: &BigtableEngine,
    account_id: &str,
    stream_arn: &str,
    sdesc: &extenddb_core::types::StreamDescription,
) -> Vec<extenddb_core::types::StreamRecord> {
    let mut all = Vec::new();
    for s in &sdesc.shards {
        let (recs, _) = engine
            .get_stream_records(account_id, stream_arn, &s.shard_id, None, 100)
            .await
            .expect("get_stream_records across shard");
        all.extend(recs);
    }
    all.sort_by(|a, b| a.dynamodb.sequence_number.cmp(&b.dynamodb.sequence_number));
    all
}

// -----------------------------------------------------------------------------
// Test 9: Fenced Single-Item Streams & Mid-Flight Crash Recovery
// -----------------------------------------------------------------------------
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_fenced_single_item_streams_and_crash_recovery() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_fenced_stream");
    let input = CreateTableInput {
        table_name: table_name.clone(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        billing_mode: Some(BillingMode::PayPerRequest),
        stream_specification: Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        }),
        deletion_protection_enabled: Some(false),
        ..CreateTableInput::default()
    };

    let desc = ctx
        .engine
        .create_table(&ctx.account_id, input)
        .await
        .expect("create stream table");
    let stream_arn = desc.latest_stream_arn.clone().expect("stream ARN");
    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let empty_maps = ExpressionMaps::default();

    let sdesc = ctx
        .engine
        .describe_stream(
            &ctx.account_id,
            &DescribeStreamInput {
                stream_arn: stream_arn.clone(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .expect("describe_stream");
    assert_eq!(
        sdesc.shards.len(),
        extenddb_storage::stream_lineage::SHARDS_PER_STREAM as usize,
        "stream must provision SHARDS_PER_STREAM (4) open shards"
    );
    for sh in &sdesc.shards {
        ctx.engine
            .validate_shard(&ctx.account_id, &stream_arn, &sh.shard_id)
            .await
            .expect("validate_shard");
    }
    let s1_shard_id = ctx
        .engine
        .assign_shard(&ctx.account_id, &table_name, "s1")
        .await
        .expect("assign_shard s1");
    let crash_shard_id = ctx
        .engine
        .assign_shard(&ctx.account_id, &table_name, "s_crash")
        .await
        .expect("assign_shard s_crash");

    // Cross-account stream read must fail with TableNotFound ("Stream does not exist")
    assert!(matches!(
        ctx.engine
            .get_stream_records("999999999999", &stream_arn, &s1_shard_id, None, 10)
            .await,
        Err(StorageError::TableNotFound(_))
    ));

    // 1. Single-item PutItem -> Modify UpdateItem -> DeleteItem with StreamCapture
    let capture = StreamCapture {
        view_type: StreamViewType::NewAndOldImages,
        user_identity: None,
        region: Arc::from("us-east-1"),
    };
    let mut item: Item = BTreeMap::new();
    item.insert("pk".to_string(), AttributeValue::S("s1".to_string()));
    item.insert("val".to_string(), AttributeValue::S("v1".to_string()));
    ctx.engine
        .put_item(&key_info, item, false, None, &empty_maps, Some(&capture))
        .await
        .unwrap();

    let mut k1: Item = BTreeMap::new();
    k1.insert("pk".to_string(), AttributeValue::S("s1".to_string()));
    let mut upd_maps = ExpressionMaps::default();
    upd_maps
        .values
        .insert("v2".to_string(), AttributeValue::S("v2".to_string()));
    let upd_act = parse_upd("SET val = :v2");
    ctx.engine
        .update_item(
            &key_info,
            &k1,
            &upd_act,
            false,
            false,
            None,
            &upd_maps,
            Some(&capture),
        )
        .await
        .unwrap();

    ctx.engine
        .delete_item(&key_info, &k1, false, None, &empty_maps, Some(&capture))
        .await
        .unwrap();

    // 1b. ImportTable bulk-import suppression contract (`stream: None`):
    //     writing with `stream: None` on a stream-enabled table must persist the item
    //     WITHOUT emitting any stream event on ANY shard.
    let mut bulk_item: Item = BTreeMap::new();
    bulk_item.insert("pk".to_string(), AttributeValue::S("s_bulk".to_string()));
    bulk_item.insert("val".to_string(), AttributeValue::S("imported".to_string()));
    ctx.engine
        .put_item(&key_info, bulk_item, false, None, &empty_maps, None)
        .await
        .expect("bulk import put_item with stream: None");
    let mut bulk_key: Item = BTreeMap::new();
    bulk_key.insert("pk".to_string(), AttributeValue::S("s_bulk".to_string()));
    assert!(
        ctx.engine
            .get_item(&key_info, &bulk_key)
            .await
            .unwrap()
            .is_some()
    );

    let all_before_crash =
        read_all_stream_records(&ctx.engine, &ctx.account_id, &stream_arn, &sdesc).await;
    assert_eq!(
        all_before_crash.len(),
        3,
        "only the 3 StreamCapture writes (not stream: None bulk import) may emit records across all 4 shards"
    );

    let (records, s1_next_seq) = ctx
        .engine
        .get_stream_records(&ctx.account_id, &stream_arn, &s1_shard_id, None, 10)
        .await
        .expect("get_stream_records on s1 assigned shard");
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].event_name, StreamEventName::Insert);
    assert_eq!(records[1].event_name, StreamEventName::Modify);
    assert_eq!(records[2].event_name, StreamEventName::Remove);
    for r in &records {
        assert_eq!(
            r.dynamodb.sequence_number.len(),
            ctx.engine.sequence_number_width()
        );
        assert!(r.dynamodb.sequence_number.parse::<u128>().is_ok());
    }
    assert!(records[0].dynamodb.sequence_number < records[1].dynamodb.sequence_number);
    assert!(records[1].dynamodb.sequence_number < records[2].dynamodb.sequence_number);

    let (_, crash_before_seq) = ctx
        .engine
        .get_stream_records(&ctx.account_id, &stream_arn, &crash_shard_id, None, 10)
        .await
        .expect("get_stream_records before crash");
    let crash_start_seq = if crash_shard_id == s1_shard_id {
        s1_next_seq
    } else {
        crash_before_seq
    };

    // 2. Simulated crash right after coordinator `COMMITTED` before base-row or
    //    stream-record Phase 2 apply:
    let mut crashed_item: Item = BTreeMap::new();
    crashed_item.insert("pk".to_string(), AttributeValue::S("s_crash".to_string()));
    crashed_item.insert(
        "val".to_string(),
        AttributeValue::S("recovered_post_image".to_string()),
    );
    let txn_id = ctx
        .engine
        .debug_fenced_put_commit_only(&key_info, crashed_item.clone(), Some(&capture))
        .await
        .expect("debug_fenced_put_commit_only");

    let mut crash_key: Item = BTreeMap::new();
    crash_key.insert("pk".to_string(), AttributeValue::S("s_crash".to_string()));

    // Before sweeper runs, base table `d` does NOT yet have the item, BUT
    // `TransactGetItems` MUST already serve it via read-through-log!
    assert!(
        ctx.engine
            .get_item(&key_info, &crash_key)
            .await
            .unwrap()
            .is_none(),
        "raw GetItem before roll-forward sees unapplied base row"
    );
    let tget = ctx
        .engine
        .transact_get_items(&[TransactGetOp {
            key_info: &key_info,
            key: &crash_key,
        }])
        .await
        .unwrap();
    assert_eq!(
        tget[0].as_ref().and_then(|i| i.get("val")),
        Some(&AttributeValue::S("recovered_post_image".to_string())),
        "TransactGetItems must serve committed post-image from coordinator log"
    );

    // Snapshot coordinator state before sweep so we can also test idempotent re-execution
    let coord = extenddb_storage_bigtable::transact::TxnCoordinator::new(
        ctx.engine.client_ref(),
        Duration::from_secs(60),
    );
    let saved_state = coord
        .get_state(&txn_id)
        .await
        .unwrap()
        .expect("committed state in txn log");
    assert_eq!(saved_state.state, "COMMITTED");

    // Run 2PC sweeper recovery
    extenddb_storage_bigtable::sweeper::sweep_with_cutoff_override(&ctx.engine, Some(i64::MAX))
        .await
        .expect("sweeper roll-forward");

    // Both base row AND stream record must now be present!
    let recovered_base = ctx
        .engine
        .get_item(&key_info, &crash_key)
        .await
        .unwrap()
        .expect("sweeper must apply base row");
    assert_eq!(
        recovered_base.get("val"),
        Some(&AttributeValue::S("recovered_post_image".to_string()))
    );

    let (post_sweep_records, _) = ctx
        .engine
        .get_stream_records(
            &ctx.account_id,
            &stream_arn,
            &crash_shard_id,
            crash_start_seq.as_deref(),
            10,
        )
        .await
        .expect("get_stream_records after recovery");
    assert_eq!(
        post_sweep_records.len(),
        1,
        "sweeper must emit the committed stream record on s_crash's assigned shard"
    );
    assert_eq!(post_sweep_records[0].event_name, StreamEventName::Insert);

    // Re-running roll_forward with the saved state must be 100% idempotent (no duplicate stream record)
    ctx.engine
        .roll_forward(&txn_id, &saved_state)
        .await
        .expect("idempotent second roll_forward");
    let (after_replay_records, _) = ctx
        .engine
        .get_stream_records(
            &ctx.account_id,
            &stream_arn,
            &crash_shard_id,
            crash_start_seq.as_deref(),
            10,
        )
        .await
        .unwrap();
    assert_eq!(
        after_replay_records.len(),
        1,
        "idempotent roll_forward must not duplicate stream records"
    );

    let total_after_recovery =
        read_all_stream_records(&ctx.engine, &ctx.account_id, &stream_arn, &sdesc).await;
    assert_eq!(total_after_recovery.len(), 4);

    // 3. Simulated crash BEFORE coordinator `COMMITTED` (`PENDING` + active intent):
    //    sweeper must roll back cleanly, clear intent, drop coordinator row, and emit
    //    ZERO phantom stream records or base-row writes.
    let mut aborted_item: Item = BTreeMap::new();
    aborted_item.insert(
        "pk".to_string(),
        AttributeValue::S("s_precommit_crash".to_string()),
    );
    aborted_item.insert(
        "val".to_string(),
        AttributeValue::S("never_committed".to_string()),
    );
    let aborted_txn_id = ctx
        .engine
        .debug_fenced_put_open_intent_only(&key_info, &aborted_item)
        .await
        .expect("debug_fenced_put_open_intent_only");
    assert_eq!(
        coord
            .get_state(&aborted_txn_id)
            .await
            .unwrap()
            .expect("pending state")
            .state,
        "PENDING"
    );

    extenddb_storage_bigtable::sweeper::sweep_with_cutoff_override(&ctx.engine, Some(i64::MAX))
        .await
        .expect("sweeper rollback pre-commit crash");

    assert!(
        coord.get_state(&aborted_txn_id).await.unwrap().is_none(),
        "sweeper must drop PENDING coordinator row on rollback"
    );
    let mut aborted_key: Item = BTreeMap::new();
    aborted_key.insert(
        "pk".to_string(),
        AttributeValue::S("s_precommit_crash".to_string()),
    );
    assert!(
        ctx.engine
            .get_item(&key_info, &aborted_key)
            .await
            .unwrap()
            .is_none(),
        "pre-commit crash must never materialize base row"
    );
    let total_after_rollback =
        read_all_stream_records(&ctx.engine, &ctx.account_id, &stream_arn, &sdesc).await;
    assert_eq!(
        total_after_rollback.len(),
        4,
        "pre-commit crash must emit zero phantom stream records across all shards"
    );
    // Subsequent normal PutItem on `s_precommit_crash` must succeed (intent cleared).
    ctx.engine
        .put_item(
            &key_info,
            aborted_item,
            false,
            None,
            &empty_maps,
            Some(&capture),
        )
        .await
        .expect("subsequent PutItem after intent rollback must succeed");
}

// -----------------------------------------------------------------------------
// Test 10: TTL Sweeper Fenced Stream Emission, Decimal/Zero/Negative TTL & Metrics
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_ttl_sweeper_fenced_stream_emission_and_stale_index_guard() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_ttl_stream");
    let input = CreateTableInput {
        table_name: table_name.clone(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: ScalarAttributeType::S,
        }],
        billing_mode: Some(BillingMode::PayPerRequest),
        stream_specification: Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        }),
        deletion_protection_enabled: Some(false),
        ..CreateTableInput::default()
    };

    let desc = ctx
        .engine
        .create_table(&ctx.account_id, input)
        .await
        .unwrap();
    let stream_arn = desc.latest_stream_arn.unwrap();
    ctx.engine
        .update_ttl(&ctx.account_id, &table_name, "expires_at", true)
        .await
        .unwrap();
    ensure_ttl_index_table(&ctx.client).await.unwrap();

    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let empty_maps = ExpressionMaps::default();
    let capture = StreamCapture {
        view_type: StreamViewType::NewAndOldImages,
        user_identity: None,
        region: Arc::from("us-east-1"),
    };

    // 1. Insert genuinely expired integer, fractional, and scientific TTL items,
    //    plus zero (`"0"`) and negative (`"-42"`) TTL items (which DynamoDB never expires).
    for (pk, exp_val) in [
        ("sess#expired", "1000"),
        ("sess#fractional", "1000.75"),
        ("sess#scientific", "1.5e3"),
        ("sess#zero_never_exp", "0"),
        ("sess#neg_never_exp", "-42"),
    ] {
        let mut it: Item = BTreeMap::new();
        it.insert("pk".to_string(), AttributeValue::S(pk.to_string()));
        it.insert(
            "expires_at".to_string(),
            AttributeValue::N(exp_val.to_string()),
        );
        ctx.engine
            .put_item(&key_info, it, false, None, &empty_maps, Some(&capture))
            .await
            .unwrap();
    }

    // 2. Insert extended item (`sess#extended`, expires_at = 9999999999) AND
    //    inject a stale orphan TTL index entry (`expiry = 900`) pointing at it,
    //    plus a stale orphan TTL index entry pointing at `"sess#zero_never_exp"`.
    let mut extended: Item = BTreeMap::new();
    extended.insert(
        "pk".to_string(),
        AttributeValue::S("sess#extended".to_string()),
    );
    extended.insert(
        "expires_at".to_string(),
        AttributeValue::N("9999999999".to_string()),
    );
    ctx.engine
        .put_item(
            &key_info,
            extended.clone(),
            false,
            None,
            &empty_maps,
            Some(&capture),
        )
        .await
        .unwrap();

    for pk_for_stale in ["sess#extended", "sess#zero_never_exp"] {
        let mut k = Item::new();
        k.insert(
            "pk".to_string(),
            AttributeValue::S(pk_for_stale.to_string()),
        );
        let base_rk = extenddb_storage_bigtable::data::encoding::row_key::encode_key(
            &k,
            &key_info.key_schema,
        )
        .unwrap();
        let stale_ttl_key = extenddb_storage_bigtable::data::encoding::ttl_key::encode_ttl_key(
            &ctx.account_id,
            &table_name,
            &base_rk,
            900,
        );
        ctx.client
            .data()
            .mutate_row(MutateRowRequest {
                table_name: ctx.client.full_table_name(
                    extenddb_storage_bigtable::data::encoding::ttl_key::TTL_INDEX_TABLE,
                ),
                row_key: stale_ttl_key,
                mutations: vec![Mutation {
                    mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
                        family_name: "d".to_string(),
                        column_qualifier: vec![],
                        timestamp_micros:
                            extenddb_storage_bigtable::data::item_ops::now_millis_micros(),
                        value: vec![],
                    })),
                }],
                ..MutateRowRequest::default()
            })
            .await
            .unwrap();
    }

    // 3. Sweep expired items with live `MetricsCollector`
    let metrics = extenddb_core::metrics::MetricsCollector::new();
    extenddb_storage_bigtable::ttl_worker::sweep_once_with_metrics(&ctx.engine, Some(&metrics))
        .await
        .expect("TTL sweep_once_with_metrics");

    for deleted_pk in ["sess#expired", "sess#fractional", "sess#scientific"] {
        let mut k_exp: Item = BTreeMap::new();
        k_exp.insert("pk".to_string(), AttributeValue::S(deleted_pk.to_string()));
        assert!(
            ctx.engine
                .get_item(&key_info, &k_exp)
                .await
                .unwrap()
                .is_none(),
            "TTL sweeper must evict {deleted_pk}"
        );
    }

    for surviving_pk in ["sess#extended", "sess#zero_never_exp", "sess#neg_never_exp"] {
        let mut k_surv: Item = BTreeMap::new();
        k_surv.insert(
            "pk".to_string(),
            AttributeValue::S(surviving_pk.to_string()),
        );
        assert!(
            ctx.engine
                .get_item(&key_info, &k_surv)
                .await
                .unwrap()
                .is_some(),
            "TTL sweeper must NOT delete {surviving_pk}"
        );
    }

    let ttl_snaps = metrics.query(&extenddb_core::metrics::MetricsQuery {
        account_id: Some(ctx.account_id.clone()),
        table_name: Some(table_name.clone()),
        metric: Some(extenddb_core::metrics::MetricName::TimeToLiveDeletedItemCount),
        window: Some(extenddb_core::metrics::TimeWindow::AllTime),
        ..extenddb_core::metrics::MetricsQuery::default()
    });
    assert_eq!(ttl_snaps.len(), 1);
    assert_eq!(
        ttl_snaps[0].count, 3,
        "MetricsCollector must record 3 TimeToLiveDeletedItemCount points"
    );

    // 4. Stream across the 4 shards must contain 6 INSERTs and 3 REMOVEs with
    //    `dynamodb.amazonaws.com` Service UserIdentity!
    let sdesc = ctx
        .engine
        .describe_stream(
            &ctx.account_id,
            &DescribeStreamInput {
                stream_arn: stream_arn.clone(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        sdesc.shards.len(),
        extenddb_storage::stream_lineage::SHARDS_PER_STREAM as usize
    );
    let records = read_all_stream_records(&ctx.engine, &ctx.account_id, &stream_arn, &sdesc).await;

    assert_eq!(records.len(), 9);
    let removes: Vec<_> = records
        .iter()
        .filter(|r| r.event_name == StreamEventName::Remove)
        .collect();
    assert_eq!(removes.len(), 3);
    let mut removed_pks: BTreeSet<String> = BTreeSet::new();
    for rem in &removes {
        if let Some(AttributeValue::S(pk)) = rem.dynamodb.keys.get("pk") {
            removed_pks.insert(pk.clone());
        }
        let uid = rem
            .user_identity
            .as_ref()
            .expect("TTL REMOVE record must carry Service UserIdentity");
        assert_eq!(uid.principal_id, "dynamodb.amazonaws.com");
        assert_eq!(uid.identity_type, "Service");
    }
    assert_eq!(
        removed_pks,
        BTreeSet::from([
            "sess#expired".to_string(),
            "sess#fractional".to_string(),
            "sess#scientific".to_string(),
        ])
    );
}

// -----------------------------------------------------------------------------
// Test 11: Catalog IAM Leftovers, Metrics, Key Ring Conformance & STS Credentials
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_catalog_iam_secrets_metrics_conformance() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let unkeyed_store =
        extenddb_storage_bigtable::BigtableCatalogStore::new(ctx.client.clone(), None, false);
    let loaded_keys = extenddb_storage::server_components::load_secret_keys(
        &unkeyed_store,
        extenddb_storage::server_components::KeySource::CatalogKey,
    )
    .await
    .expect("load catalog secret keys");
    let secret_keys = loaded_keys
        .clone()
        .expect("fresh catalog must hold catalog key");
    let store = extenddb_storage_bigtable::BigtableCatalogStore::new(
        ctx.client.clone(),
        loaded_keys.clone(),
        false,
    );

    // 1. IAM leftovers conformance (#693, #694) across users, groups, roles, policies,
    //    boundaries, accounts, and failed-login rate-limit prefix isolation.
    extenddb_storage::iam_leftovers_conformance::run(&store, "888877776666").await;

    // 2. MetricsStore conformance (#728): multi-account isolation, accumulation, and pruning.
    extenddb_storage::metrics_conformance::run(
        &store,
        extenddb_storage::metrics_conformance::Expectations {
            accumulates: true,
            prunes: true,
        },
    )
    .await;

    // 3. Verify BigtableCredentialStore lookup for AKIA* (active + inactive) and ASIA*
    //    (live + expired) before key_conformance transitions the catalog onto a KEK ring.
    use extenddb_auth::CredentialStore;
    use extenddb_storage::management_store::ManagementStore;

    let cred_acct = "777766665555";
    store
        .create_account(cred_acct, "cred-test-acct")
        .await
        .expect("create cred account");
    store
        .create_user(cred_acct, "alice", None)
        .await
        .expect("create user alice");
    let created_key = store
        .create_access_key(cred_acct, "alice")
        .await
        .expect("create AKIA key");
    let akia_id = created_key.access_key_id;
    let akia_secret = created_key.secret_access_key;

    let cred_store =
        extenddb_storage_bigtable::BigtableCredentialStore::new(ctx.client.clone(), loaded_keys);
    let looked_up = cred_store
        .lookup_credential(&akia_id)
        .await
        .expect("lookup active AKIA")
        .expect("AKIA present");
    assert_eq!(looked_up.secret_key, akia_secret);
    assert_eq!(looked_up.account_id, cred_acct);
    assert_eq!(looked_up.principal_name, "alice");
    assert!(!looked_up.is_session);
    assert!(looked_up.is_active);

    let cat = extenddb_storage_bigtable::catalog::Catalog::new(&ctx.client);
    let ak_rk = extenddb_storage_bigtable::catalog::keys::access_key(&akia_id);
    let mut ak_doc = cat
        .get(&ak_rk)
        .await
        .expect("read AKIA catalog row")
        .expect("AKIA catalog row present");
    ak_doc["is_active"] = serde_json::Value::Bool(false);
    cat.put(&ak_rk, &ak_doc)
        .await
        .expect("deactivate AKIA catalog row");

    let inactive = cred_store
        .lookup_credential(&akia_id)
        .await
        .expect("lookup inactive AKIA")
        .expect("inactive AKIA still returned for S-5 rejection");
    assert!(!inactive.is_active);

    let trust = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": format!("arn:aws:iam::{cred_acct}:root")},
            "Action": "sts:AssumeRole"
        }]
    });
    store
        .create_role(cred_acct, "AppRole", &trust)
        .await
        .expect("create role");

    let live_asia_id = "ASIATESTLIVE0001";
    let exp_asia_id = "ASIATESTEXPD0002";
    let future_unix = time::OffsetDateTime::now_utc().unix_timestamp() + 1800;
    let future_exp = time::OffsetDateTime::from_unix_timestamp(future_unix).unwrap();
    let live_aad = extenddb_storage::secret_keys::SecretRow::session(
        live_asia_id,
        cred_acct,
        "AppRole",
        "sess-live",
        future_unix,
    );
    let live_cipher = secret_keys
        .encrypt(&live_aad, "live-session-secret-key")
        .expect("encrypt live ASIA session");
    store
        .store_session(
            "tok-live-session",
            live_asia_id,
            &live_cipher,
            cred_acct,
            "AppRole",
            "sess-live",
            &None,
            &None,
            future_exp,
        )
        .await
        .expect("store live ASIA session");
    let live_sess = cred_store
        .lookup_credential(live_asia_id)
        .await
        .expect("lookup live ASIA")
        .expect("live ASIA present");
    assert_eq!(live_sess.secret_key, "live-session-secret-key");
    assert!(live_sess.is_session);
    assert_eq!(live_sess.principal_name, "AppRole");
    assert_eq!(live_sess.session_name.as_deref(), Some("sess-live"));
    assert_eq!(live_sess.session_token.as_deref(), Some("tok-live-session"));

    let past_unix = time::OffsetDateTime::now_utc().unix_timestamp() - 300;
    let past_exp = time::OffsetDateTime::from_unix_timestamp(past_unix).unwrap();
    let exp_aad = extenddb_storage::secret_keys::SecretRow::session(
        exp_asia_id,
        cred_acct,
        "AppRole",
        "sess-expired",
        past_unix,
    );
    let exp_cipher = secret_keys
        .encrypt(&exp_aad, "expired-session-secret-key")
        .expect("encrypt expired ASIA session");
    store
        .store_session(
            "tok-expired-session",
            exp_asia_id,
            &exp_cipher,
            cred_acct,
            "AppRole",
            "sess-expired",
            &None,
            &None,
            past_exp,
        )
        .await
        .expect("store expired ASIA session");
    let Err(expired_err) = cred_store.lookup_credential(exp_asia_id).await else {
        panic!("expired ASIA must fail with ExpiredTokenException");
    };
    assert!(
        matches!(
            expired_err,
            extenddb_core::error::DynamoDbError::ExpiredTokenException(_)
        ),
        "expected ExpiredTokenException, got {expired_err:?}"
    );

    store
        .delete_account(cred_acct)
        .await
        .expect("delete cred account and all dependent users/roles/keys/sessions");
    assert!(
        cred_store
            .lookup_credential(&akia_id)
            .await
            .expect("lookup deleted AKIA")
            .is_none()
    );
    assert!(
        cred_store
            .lookup_credential(live_asia_id)
            .await
            .expect("lookup deleted ASIA")
            .is_none()
    );

    // 4. Full encryption-key command conformance (switch, rotate, rewrap, reset, CAS).
    extenddb_storage::key_conformance::run(
        &store,
        &store,
        extenddb_storage::key_conformance::Expectations {
            write_timestamps: false,
            data_connection: false,
        },
    )
    .await;
}

// -----------------------------------------------------------------------------
// Test 12: Stream Disable / Re-enable Lineage & Cross-Stream Shard Isolation
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_stream_disable_reenable_lineage() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_stream_lineage");
    let create_desc = ctx
        .engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: table_name.clone(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: ScalarAttributeType::S,
                }],
                billing_mode: Some(BillingMode::PayPerRequest),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::NewImage),
                }),
                ..CreateTableInput::default()
            },
        )
        .await
        .expect("create stream table");

    let arn_1 = create_desc.latest_stream_arn.clone().expect("stream 1 ARN");
    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();

    let cap_new_image = StreamCapture {
        view_type: StreamViewType::NewImage,
        user_identity: None,
        region: Arc::from("us-east-1"),
    };
    let mut item1 = Item::new();
    item1.insert("pk".to_string(), AttributeValue::S("k1".to_string()));
    ctx.engine
        .put_item(
            &key_info,
            item1,
            false,
            None,
            &ExpressionMaps::default(),
            Some(&cap_new_image),
        )
        .await
        .unwrap();

    let sdesc_1_open = ctx
        .engine
        .describe_stream(
            &ctx.account_id,
            &DescribeStreamInput {
                stream_arn: arn_1.clone(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        sdesc_1_open.stream_status,
        extenddb_core::types::StreamStatus::Enabled
    );
    assert_eq!(
        sdesc_1_open.shards.len(),
        extenddb_storage::stream_lineage::SHARDS_PER_STREAM as usize
    );
    let shard_1 = sdesc_1_open.shards[0].shard_id.clone();
    for sh in &sdesc_1_open.shards {
        assert!(
            sh.sequence_number_range.ending_sequence_number.is_none(),
            "open stream shard must not have EndingSequenceNumber"
        );
    }

    let disable_input: extenddb_core::types::UpdateTableInput =
        serde_json::from_value(serde_json::json!({
            "TableName": table_name,
            "StreamSpecification": {
                "StreamEnabled": false
            }
        }))
        .expect("deserialize disable UpdateTableInput");
    let disabled_desc = ctx
        .engine
        .update_table(&ctx.account_id, disable_input)
        .await
        .expect("disable stream 1");
    assert_eq!(
        disabled_desc.latest_stream_arn.as_deref(),
        Some(arn_1.as_str())
    );

    let sdesc_1_closed = ctx
        .engine
        .describe_stream(
            &ctx.account_id,
            &DescribeStreamInput {
                stream_arn: arn_1.clone(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        sdesc_1_closed.stream_status,
        extenddb_core::types::StreamStatus::Disabled
    );
    assert_eq!(
        sdesc_1_closed.shards.len(),
        extenddb_storage::stream_lineage::SHARDS_PER_STREAM as usize
    );
    for sh in &sdesc_1_closed.shards {
        assert!(
            sh.sequence_number_range.ending_sequence_number.is_some(),
            "every disabled stream shard must have EndingSequenceNumber"
        );
    }

    let key_info_disabled = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let mut item2 = Item::new();
    item2.insert("pk".to_string(), AttributeValue::S("k2".to_string()));
    ctx.engine
        .put_item(
            &key_info_disabled,
            item2,
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .unwrap();

    let reenable_input: extenddb_core::types::UpdateTableInput =
        serde_json::from_value(serde_json::json!({
            "TableName": table_name,
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "KEYS_ONLY"
            }
        }))
        .expect("deserialize reenable UpdateTableInput");
    let reenabled_desc = ctx
        .engine
        .update_table(&ctx.account_id, reenable_input)
        .await
        .expect("re-enable stream 2");
    let arn_2 = reenabled_desc
        .latest_stream_arn
        .clone()
        .expect("stream 2 ARN");
    assert_ne!(arn_1, arn_2, "re-enabled stream must receive a fresh ARN");

    let key_info_reenabled = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let cap_keys_only = StreamCapture {
        view_type: StreamViewType::KeysOnly,
        user_identity: None,
        region: Arc::from("us-east-1"),
    };
    let mut item3 = Item::new();
    item3.insert("pk".to_string(), AttributeValue::S("k3".to_string()));
    ctx.engine
        .put_item(
            &key_info_reenabled,
            item3,
            false,
            None,
            &ExpressionMaps::default(),
            Some(&cap_keys_only),
        )
        .await
        .unwrap();

    let (listed, _) = ctx
        .engine
        .list_streams(&ctx.account_id, Some(&table_name), 10, None)
        .await
        .unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(listed[0].stream_arn, arn_2);
    assert_eq!(listed[1].stream_arn, arn_1);

    let recs_1 =
        read_all_stream_records(&ctx.engine, &ctx.account_id, &arn_1, &sdesc_1_closed).await;
    assert_eq!(recs_1.len(), 1);
    assert_eq!(
        recs_1[0].dynamodb.keys.get("pk"),
        Some(&AttributeValue::S("k1".to_string()))
    );

    let sdesc_2 = ctx
        .engine
        .describe_stream(
            &ctx.account_id,
            &DescribeStreamInput {
                stream_arn: arn_2.clone(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        sdesc_2.stream_status,
        extenddb_core::types::StreamStatus::Enabled
    );
    assert_eq!(sdesc_2.stream_view_type, StreamViewType::KeysOnly);
    assert_eq!(
        sdesc_2.shards.len(),
        extenddb_storage::stream_lineage::SHARDS_PER_STREAM as usize
    );

    let recs_2 = read_all_stream_records(&ctx.engine, &ctx.account_id, &arn_2, &sdesc_2).await;
    assert_eq!(recs_2.len(), 1);
    assert_eq!(
        recs_2[0].dynamodb.keys.get("pk"),
        Some(&AttributeValue::S("k3".to_string()))
    );
    assert!(recs_2[0].dynamodb.new_image.is_none());

    let val_err = ctx
        .engine
        .validate_shard(&ctx.account_id, &arn_2, &shard_1)
        .await
        .expect_err("old shard must not validate under newer stream ARN");
    assert!(
        val_err
            .to_string()
            .contains(extenddb_storage::util::SHARD_DOES_NOT_EXIST),
        "expected SHARD_DOES_NOT_EXIST, got {val_err}"
    );
    let get_err = ctx
        .engine
        .get_stream_records(&ctx.account_id, &arn_2, &shard_1, None, 10)
        .await
        .expect_err("old shard must not read records under newer stream ARN");
    assert!(
        get_err
            .to_string()
            .contains(extenddb_storage::util::STREAM_DOES_NOT_EXIST),
        "expected STREAM_DOES_NOT_EXIST, got {get_err}"
    );
}

// -----------------------------------------------------------------------------
// Test 13: Split Catalog vs. Data Instance Isolation (`data_instance_id != instance_id`)
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_split_catalog_and_data_instance_isolation() {
    let suffix = &Uuid::new_v4().to_string().replace('-', "")[..8];
    let mut config = BigtableStorageConfig {
        project_id: format!("split-proj-{suffix}"),
        instance_id: format!("cat-inst-{suffix}"),
        data_instance_id: Some(format!("data-inst-{suffix}")),
        dev_mode: true,
        ..BigtableStorageConfig::default()
    };
    let Some((host, _)) = resolve_reachable_emulator_host(&config).await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };
    config.emulator_host = Some(host);

    let bootstrapper = BigtableBootstrapper::new(config.clone());
    bootstrapper
        .create_catalog_db()
        .await
        .expect("split create_catalog_db");
    bootstrapper
        .create_data_db()
        .await
        .expect("split create_data_db");

    let target = Target::at(config.connection_string());
    let lock = BigtableCatalogMigrations
        .lock(&target, Duration::from_secs(10))
        .await
        .expect("lock split migrations");
    BigtableCatalogMigrations
        .run(&lock, env!("CARGO_PKG_VERSION"), RunKind::Create)
        .await
        .expect("run split baseline migrations");
    BigtableCatalogMigrations.release(lock).await;

    let catalog_client = Arc::new(BigtableClient::connect(&config).await.expect("cat client"));
    let mut data_cfg = config.clone();
    data_cfg.instance_id = config.data_instance_id.clone().unwrap();
    let data_client = Arc::new(
        BigtableClient::connect(&data_cfg)
            .await
            .expect("data client"),
    );

    // Verify `data_instance_id` does NOT contain `__extenddb_catalog__`, proving
    // workers that mistakenly query `Catalog` on `data_client` would fail!
    let mut data_admin = AdminClient::connect(&data_client).await.unwrap();
    let data_tables_initial = data_admin.list_tables().await.unwrap();
    assert!(
        !data_tables_initial
            .iter()
            .any(|t| t.ends_with("__extenddb_catalog__")),
        "data_instance_id must not contain __extenddb_catalog__"
    );
    assert!(
        data_tables_initial
            .iter()
            .any(|t| t.ends_with("__extenddb_txn_log__")),
        "create_data_db must provision __extenddb_txn_log__ on data_instance_id"
    );
    assert!(
        data_tables_initial
            .iter()
            .any(|t| t.ends_with("__extenddb_ttl_index__")),
        "create_data_db must provision __extenddb_ttl_index__ on data_instance_id"
    );

    let engine = Arc::new(BigtableEngine::new(
        catalog_client.clone(),
        data_client.clone(),
        config.intent_timeout_secs,
    ));
    let account_id = "123456789012";
    let table_name = unique_name("tbl_split_inst");

    let desc = engine
        .create_table(
            account_id,
            CreateTableInput {
                table_name: table_name.clone(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".to_string(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "gsi_pk".to_string(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ],
                global_secondary_indexes: Some(vec![GsiInput {
                    index_name: "GsiIdx".to_string(),
                    key_schema: vec![KeySchemaElement {
                        attribute_name: "gsi_pk".to_string(),
                        key_type: KeyType::Hash,
                    }],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: None,
                    warm_throughput: None,
                }]),
                billing_mode: Some(BillingMode::PayPerRequest),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::NewAndOldImages),
                }),
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .expect("create split-instance table");
    let stream_arn = desc.latest_stream_arn.unwrap();
    engine
        .update_ttl(account_id, &table_name, "ttl_ts", true)
        .await
        .expect("enable split TTL");

    let key_info = engine
        .table_key_info(account_id, &table_name)
        .await
        .unwrap();
    let capture = StreamCapture {
        view_type: StreamViewType::NewAndOldImages,
        user_identity: None,
        region: Arc::from("us-east-1"),
    };
    let empty_maps = ExpressionMaps::default();

    let mut exp_item = Item::new();
    exp_item.insert("pk".to_string(), AttributeValue::S("exp1".to_string()));
    exp_item.insert("gsi_pk".to_string(), AttributeValue::S("g1".to_string()));
    exp_item.insert("ttl_ts".to_string(), AttributeValue::N("1000".to_string()));
    engine
        .put_item(
            &key_info,
            exp_item,
            false,
            None,
            &empty_maps,
            Some(&capture),
        )
        .await
        .expect("split put expired");

    let mut live_item = Item::new();
    live_item.insert("pk".to_string(), AttributeValue::S("live1".to_string()));
    live_item.insert("gsi_pk".to_string(), AttributeValue::S("g1".to_string()));
    live_item.insert(
        "ttl_ts".to_string(),
        AttributeValue::N("9999999999".to_string()),
    );
    engine
        .put_item(
            &key_info,
            live_item,
            false,
            None,
            &empty_maps,
            Some(&capture),
        )
        .await
        .expect("split put live");

    // All three background workers must acquire leases & metadata on `catalog_client`
    // while sweeping data/GSI/TTL/txn-log tables on `data_client`!
    extenddb_storage_bigtable::sweeper::sweep_once(&engine)
        .await
        .expect("split sweeper::sweep_once");
    extenddb_storage_bigtable::ttl_worker::sweep_once(&engine)
        .await
        .expect("split ttl_worker::sweep_once");
    extenddb_storage_bigtable::gsi_worker::sweep_once(&engine)
        .await
        .expect("split gsi_worker::sweep_once");

    let mut exp_key = Item::new();
    exp_key.insert("pk".to_string(), AttributeValue::S("exp1".to_string()));
    assert!(
        engine
            .get_item(&key_info, &exp_key)
            .await
            .unwrap()
            .is_none(),
        "split TTL sweep must delete expired item on data_instance_id"
    );

    let sdesc = engine
        .describe_stream(
            account_id,
            &DescribeStreamInput {
                stream_arn: stream_arn.clone(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    let stream_recs = read_all_stream_records(&engine, account_id, &stream_arn, &sdesc).await;
    assert_eq!(
        stream_recs.len(),
        3,
        "2 INSERTs + 1 TTL REMOVE must be persisted across the 4 shards in catalog_client stream ledger"
    );

    bootstrapper
        .drop_databases(
            config
                .data_instance_id
                .as_deref()
                .unwrap_or(&config.instance_id),
        )
        .await
        .expect("split drop_databases");
    let remaining_cat = AdminClient::connect(&catalog_client)
        .await
        .unwrap()
        .list_tables()
        .await
        .unwrap();
    let remaining_data = AdminClient::connect(&data_client)
        .await
        .unwrap()
        .list_tables()
        .await
        .unwrap();
    assert!(remaining_cat.is_empty(), "catalog_instance tables dropped");
    assert!(remaining_data.is_empty(), "data_instance tables dropped");
}

// -----------------------------------------------------------------------------
// Test 14: Reused Token Tombstone Safety, Post-Lease ABA Fencing, GSI TOCTOU & GC Workers
// -----------------------------------------------------------------------------
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_aba_fencing_reused_token_gsi_toctou_and_cleanup_workers() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_aba_hardening");
    let create_desc = ctx
        .engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: table_name.clone(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".to_string(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "gpk".to_string(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ],
                global_secondary_indexes: Some(vec![GsiInput {
                    index_name: "by_gpk".to_string(),
                    key_schema: vec![KeySchemaElement {
                        attribute_name: "gpk".to_string(),
                        key_type: KeyType::Hash,
                    }],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: None,
                    warm_throughput: None,
                }]),
                billing_mode: Some(BillingMode::PayPerRequest),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::NewAndOldImages),
                }),
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .expect("create table for ABA fencing hardening test");

    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let empty_maps = ExpressionMaps::default();
    let capture = StreamCapture {
        view_type: StreamViewType::NewAndOldImages,
        user_identity: None,
        region: Arc::from("us-east-1"),
    };

    // 1. Reused ClientRequestToken after Phase 1 ConditionFailed (`DeleteFromRow` tombstone safety):
    //    First call fails ConditionCheck -> `coord.drop(txn_id)` emits `DeleteFromRow`.
    //    Immediate retry with the SAME token (same `derive_txn_id`) must stamp cells strictly
    //    above the `DeleteFromRow` tombstone and commit cleanly!
    let retry_token_str = format!("tok-retry-ccf-{}", Uuid::new_v4());
    let mut guard_key = Item::new();
    guard_key.insert("pk".to_string(), AttributeValue::S("guard_row".into()));
    let must_exist = parse_cond("attribute_exists(pk)");

    let mut tok_item = Item::new();
    tok_item.insert("pk".to_string(), AttributeValue::S("tok_row".into()));
    tok_item.insert("gpk".to_string(), AttributeValue::S("g_tok".into()));

    let fail_ops = vec![
        TransactWriteOp::Put {
            key_info: &key_info,
            item: &tok_item,
            condition: None,
            maps: &empty_maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
            stream: Some(capture.clone()),
        },
        TransactWriteOp::ConditionCheck {
            key_info: &key_info,
            key: &guard_key,
            condition: &must_exist,
            maps: &empty_maps,
            return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
        },
    ];
    let first_attempt = ctx
        .engine
        .transact_write_items(
            &fail_ops,
            Some(IdempotencyKey {
                account_id: &ctx.account_id,
                token: &retry_token_str,
                fingerprint: "fp-retry-1",
            }),
        )
        .await;
    assert!(
        matches!(first_attempt, Err(StorageError::TransactionCanceled(_))),
        "first attempt with missing guard_row must return TransactionCanceled"
    );

    // Seed guard_row and immediately retry with the SAME ClientRequestToken!
    let mut guard_item = Item::new();
    guard_item.insert("pk".to_string(), AttributeValue::S("guard_row".into()));
    ctx.engine
        .put_item(&key_info, guard_item, false, None, &empty_maps, None)
        .await
        .unwrap();

    ctx.engine
        .transact_write_items(
            &fail_ops,
            Some(IdempotencyKey {
                account_id: &ctx.account_id,
                token: &retry_token_str,
                fingerprint: "fp-retry-1",
            }),
        )
        .await
        .expect("reused ClientRequestToken after Phase 1 rollback must commit cleanly");

    let mut tok_key = Item::new();
    tok_key.insert("pk".to_string(), AttributeValue::S("tok_row".into()));
    assert!(
        ctx.engine
            .get_item(&key_info, &tok_key)
            .await
            .unwrap()
            .is_some(),
        "retried transaction must materialize tok_row"
    );
    assert!(matches!(
        ctx.engine
            .transact_write_items(
                &fail_ops,
                Some(IdempotencyKey {
                    account_id: &ctx.account_id,
                    token: &retry_token_str,
                    fingerprint: "fp-retry-1",
                }),
            )
            .await,
        Err(StorageError::IdempotentReplay)
    ));

    // 2. Post-lease-expiry ABA fencing on BOTH base row AND GSI shadow row:
    let data_table = ctx.engine.data_table_for(&key_info).await.unwrap();
    let shadow_table = extenddb_storage_bigtable::gsi::shadow_table_id(&data_table, "by_gpk");
    let base_ops = extenddb_storage_bigtable::data::item_ops::ItemOps::new(
        ctx.engine.client_ref(),
        &data_table,
        60,
    );
    let coord = extenddb_storage_bigtable::transact::TxnCoordinator::new(
        ctx.engine.client_ref(),
        Duration::from_secs(60),
    );

    let mut item_v1 = Item::new();
    item_v1.insert("pk".to_string(), AttributeValue::S("aba_k1".into()));
    item_v1.insert("gpk".to_string(), AttributeValue::S("g1".into()));
    item_v1.insert("val".to_string(), AttributeValue::S("v1_initial".into()));
    ctx.engine
        .put_item(
            &key_info,
            item_v1.clone(),
            false,
            None,
            &empty_maps,
            Some(&capture),
        )
        .await
        .unwrap();

    let mut aba_key = Item::new();
    aba_key.insert("pk".to_string(), AttributeValue::S("aba_k1".into()));
    let (_, Some(v1)) = base_ops
        .get_with_version(&key_info, &aba_key)
        .await
        .unwrap()
    else {
        panic!("expected v1 version on aba_k1");
    };
    let aba_rk = extenddb_storage_bigtable::data::encoding::row_key::encode_key(
        &aba_key,
        &key_info.key_schema,
    )
    .unwrap();
    let aba_part = extenddb_storage_bigtable::transact::ParticipantRow {
        data_table: data_table.clone(),
        row_key: aba_rk.clone(),
    };

    // Concurrent winner advances row (and `by_gpk` shadow row) to v2 (`v2 > v1`)
    let mut item_v2 = item_v1.clone();
    item_v2.insert("val".to_string(), AttributeValue::S("v2_winner".into()));
    ctx.engine
        .put_item(
            &key_info,
            item_v2.clone(),
            false,
            None,
            &empty_maps,
            Some(&capture),
        )
        .await
        .unwrap();
    let (_, Some(v2)) = base_ops
        .get_with_version(&key_info, &aba_key)
        .await
        .unwrap()
    else {
        panic!("expected v2 version on aba_k1");
    };
    assert!(v2 > v1);

    // 2a. Also verify `(None, false)` missing-row ABA guard rejects when `v2` exists!
    let stale_insert_txn = format!("aba-insert-stale-{}", Uuid::new_v4());
    let (_, started_ins) = coord
        .open(&stale_insert_txn, std::slice::from_ref(&aba_part))
        .await
        .unwrap();
    assert!(
        coord
            .place_intent(&stale_insert_txn, &aba_part)
            .await
            .unwrap()
    );
    let stale_payload = extenddb_storage_bigtable::transact::TxnOpPayload::Put {
        item: {
            let mut s = item_v1.clone();
            s.insert("val".to_string(), AttributeValue::S("stale_loser".into()));
            s
        },
    };
    let ins_applied = coord
        .apply_fenced_mutation(
            &stale_insert_txn,
            &aba_part,
            &stale_payload,
            None,
            false,
            v1 + 1,
        )
        .await
        .unwrap();
    assert!(
        !ins_applied,
        "(None, false) ABA guard must reject when row already exists"
    );
    let _ = coord.clear_intent(&stale_insert_txn, &aba_part).await;
    let _ = coord.cleaned(&stale_insert_txn, started_ins).await;
    let _ = coord.drop(&stale_insert_txn).await;

    // 2b. Verify `(Some(v1), true)` stale coordinator cannot clobber `v2` on base OR GSI shadow!
    let stale_txn = format!("aba-stale-{}", Uuid::new_v4());
    let (_, started_stale) = coord
        .open(&stale_txn, std::slice::from_ref(&aba_part))
        .await
        .unwrap();
    assert!(coord.place_intent(&stale_txn, &aba_part).await.unwrap());
    let gsi_desc = &create_desc.global_secondary_indexes.as_ref().unwrap()[0];
    let shadow_rk = extenddb_storage_bigtable::gsi::shadow_row_key_for_item(
        &item_v1,
        &gsi_desc.key_schema,
        &key_info.key_schema,
    )
    .unwrap()
    .unwrap();
    let stale_muts = vec![
        extenddb_storage_bigtable::transact::ParticipantMutation {
            participant: aba_part.clone(),
            payload: stale_payload.clone(),
            expected_version: Some(v1),
            row_existed: true,
            new_version: Some(v1 + 1),
        },
        extenddb_storage_bigtable::transact::ParticipantMutation {
            participant: extenddb_storage_bigtable::transact::ParticipantRow {
                data_table: shadow_table.clone(),
                row_key: shadow_rk.clone(),
            },
            payload: extenddb_storage_bigtable::transact::TxnOpPayload::Delete,
            expected_version: None,
            row_existed: false,
            new_version: Some(v1 + 1),
        },
    ];
    coord
        .commit(&stale_txn, started_stale, &stale_muts, Some(&[]))
        .await
        .unwrap();

    let applied = coord
        .apply_fenced_mutation(
            &stale_txn,
            &aba_part,
            &stale_payload,
            Some(v1),
            true,
            v1 + 1,
        )
        .await
        .unwrap();
    assert!(
        !applied,
        "expected_version guard must reject stale ABA apply when m:v advanced to v2"
    );

    let stale_state = coord.get_state(&stale_txn).await.unwrap().unwrap();
    ctx.engine
        .roll_forward(&stale_txn, &stale_state)
        .await
        .unwrap();

    let survived = ctx
        .engine
        .get_item(&key_info, &aba_key)
        .await
        .unwrap()
        .expect("v2_winner must survive");
    assert_eq!(
        survived.get("val"),
        Some(&AttributeValue::S("v2_winner".into()))
    );
    let mut gsi_maps = ExpressionMaps::default();
    gsi_maps
        .values
        .insert("g".into(), AttributeValue::S("g1".into()));
    let gsi_kc = parse_kc("gpk = :g");
    let (gsi_items, _) = ctx
        .engine
        .query(
            &key_info,
            &gsi_kc,
            &gsi_maps,
            true,
            None,
            None,
            Some("by_gpk"),
        )
        .await
        .unwrap();
    assert_eq!(
        gsi_items.len(),
        1,
        "GSI shadow row of v2_winner must NOT be deleted by superseded roll_forward"
    );
    assert_eq!(
        gsi_items[0].get("val"),
        Some(&AttributeValue::S("v2_winner".into()))
    );

    // 3. GSI Reconciler TOCTOU Repair & OCC Version Stamping:
    //    Delete the GSI shadow row directly on `shadow_table`, run `gsi_worker::sweep_once`,
    //    and verify the repaired shadow row carries `v2_winner` stamped at `(v2 * 1_000) as i64`.
    ctx.client
        .data()
        .mutate_row(MutateRowRequest {
            table_name: ctx.client.full_table_name(&shadow_table),
            row_key: shadow_rk.clone(),
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromRow(
                    mutation::DeleteFromRow {},
                )),
            }],
            ..MutateRowRequest::default()
        })
        .await
        .unwrap();
    let (missing_gsi, _) = ctx
        .engine
        .query(
            &key_info,
            &gsi_kc,
            &gsi_maps,
            true,
            None,
            None,
            Some("by_gpk"),
        )
        .await
        .unwrap();
    assert!(missing_gsi.is_empty());

    extenddb_storage_bigtable::gsi_worker::sweep_once(&ctx.engine)
        .await
        .expect("gsi_worker repair sweep");

    let (repaired_gsi, _) = ctx
        .engine
        .query(
            &key_info,
            &gsi_kc,
            &gsi_maps,
            true,
            None,
            None,
            Some("by_gpk"),
        )
        .await
        .unwrap();
    assert_eq!(repaired_gsi.len(), 1);
    assert_eq!(
        repaired_gsi[0].get("val"),
        Some(&AttributeValue::S("v2_winner".into()))
    );
    let shadow_rows = ctx
        .client
        .data()
        .read_rows(ReadRowsRequest {
            table_name: ctx.client.full_table_name(&shadow_table),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![shadow_rk],
                row_ranges: vec![],
            }),
            ..ReadRowsRequest::default()
        })
        .await
        .unwrap();
    let (_, shadow_cells) = shadow_rows.into_iter().next().expect("repaired shadow row");
    for c in shadow_cells {
        assert_eq!(
            c.timestamp_micros,
            (v2 * 1_000) as i64,
            "repaired GSI shadow cells must be stamped at base row OCC timestamp v2 * 1_000"
        );
    }

    // 4. Runtime Background Cleanup Helpers (`cleanup_expired_idempotency_tokens`,
    //    `cleanup_expired_stream_records`):
    let deleted_tokens = ctx
        .engine
        .cleanup_expired_idempotency_tokens(-60)
        .await
        .expect("cleanup_expired_idempotency_tokens");
    assert!(
        deleted_tokens >= 1,
        "expired idempotency token must be pruned"
    );
    let deleted_streams = ctx
        .engine
        .cleanup_expired_stream_records(-1)
        .await
        .expect("cleanup_expired_stream_records");
    assert!(
        deleted_streams >= 1,
        "expired stream records must be pruned"
    );
}

// =============================================================================
// Concurrency, Recovery, Stream Ordering & Index Edge-Case Suite
// =============================================================================

fn s_item(pairs: &[(&str, &str)]) -> Item {
    let mut it = BTreeMap::new();
    for (k, v) in pairs {
        it.insert((*k).to_string(), AttributeValue::S((*v).to_string()));
    }
    it
}

// -----------------------------------------------------------------------------
// Test 15: 2PC CAS Commit vs Sweeper Race & Post-COMMITTED Intent Lease Protection
// -----------------------------------------------------------------------------
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_2pc_cas_sweeper_race_and_expired_committed_intent() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_2pc_sweeper_race");
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: table_name.clone(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: ScalarAttributeType::S,
                }],
                billing_mode: Some(BillingMode::PayPerRequest),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::NewAndOldImages),
                }),
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .expect("create table");
    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let data_table = ctx.engine.data_table_for(&key_info).await.unwrap();
    let empty_maps = ExpressionMaps::default();

    // --- Part A: Sweeper CAS-aborts PENDING -> ABORTED before clearing intents, and
    //             late `commit()` / `try_commit_full()` both before and after row drop
    //             MUST fail CAS without creating a zombie COMMITTED row!
    let coord = extenddb_storage_bigtable::transact::TxnCoordinator::new(
        ctx.engine.client_ref(),
        Duration::from_secs(1),
    );
    let item_a = s_item(&[("pk", "r_abort_a"), ("v", "partial_bad")]);
    let item_b = s_item(&[("pk", "r_abort_b"), ("v", "partial_bad")]);
    let part_a = extenddb_storage_bigtable::transact::ParticipantRow {
        data_table: data_table.clone(),
        row_key: extenddb_storage_bigtable::data::encoding::row_key::encode_key(
            &item_a,
            &key_info.key_schema,
        )
        .unwrap(),
    };
    let part_b = extenddb_storage_bigtable::transact::ParticipantRow {
        data_table: data_table.clone(),
        row_key: extenddb_storage_bigtable::data::encoding::row_key::encode_key(
            &item_b,
            &key_info.key_schema,
        )
        .unwrap(),
    };
    let slow_txn = extenddb_storage_bigtable::transact::TxnCoordinator::new_txn_id();
    let (created, started_at) = coord
        .open(&slow_txn, &[part_a.clone(), part_b.clone()])
        .await
        .unwrap();
    assert!(created);
    assert!(coord.place_intent(&slow_txn, &part_a).await.unwrap());
    assert!(coord.place_intent(&slow_txn, &part_b).await.unwrap());

    assert!(
        coord
            .try_abort_pending(&slow_txn, started_at, started_at)
            .await
            .expect("try_abort_pending")
    );
    let muts = vec![
        extenddb_storage_bigtable::transact::ParticipantMutation {
            participant: part_a.clone(),
            payload: extenddb_storage_bigtable::transact::TxnOpPayload::Put {
                item: item_a.clone(),
            },
            expected_version: None,
            row_existed: false,
            new_version: Some(100),
        },
        extenddb_storage_bigtable::transact::ParticipantMutation {
            participant: part_b.clone(),
            payload: extenddb_storage_bigtable::transact::TxnOpPayload::Put {
                item: item_b.clone(),
            },
            expected_version: None,
            row_existed: false,
            new_version: Some(100),
        },
    ];
    let late_commit_on_aborted = coord.commit(&slow_txn, started_at, &muts, None).await;
    assert!(
        matches!(
            late_commit_on_aborted,
            Err(StorageError::TransactionConflict(_))
        ),
        "commit() MUST fail CAS when coordinator was transitioned to ABORTED"
    );

    extenddb_storage_bigtable::sweeper::sweep_with_cutoff_override(&ctx.engine, Some(i64::MAX))
        .await
        .expect("sweeper cleans ABORTED");
    assert!(coord.get_state(&slow_txn).await.unwrap().is_none());
    let late_commit_after_drop = coord
        .try_commit_full(&slow_txn, started_at, &muts, None, None)
        .await
        .unwrap();
    assert!(
        late_commit_after_drop.is_none(),
        "try_commit_full after sweeper drop must return None"
    );
    assert!(
        coord.get_state(&slow_txn).await.unwrap().is_none(),
        "failed commit must NOT resurrect a zombie COMMITTED coordinator row"
    );

    // --- Part B: Incarnation fencing on deterministic IDs (`started_at_micros` mismatch) ---
    let det_tid = format!("det-incarnation-{}", Uuid::new_v4().simple());
    let (_, inc1_start) = coord
        .open(&det_tid, std::slice::from_ref(&part_a))
        .await
        .unwrap();
    assert!(
        coord
            .try_abort_pending(&det_tid, inc1_start, inc1_start)
            .await
            .unwrap()
    );
    coord
        .drop_matching_start(&det_tid, inc1_start)
        .await
        .unwrap();
    let (inc2_created, inc2_start) = coord
        .open(&det_tid, std::slice::from_ref(&part_a))
        .await
        .unwrap();
    assert!(inc2_created);
    assert!(inc2_start > inc1_start);
    assert!(
        coord
            .try_commit_full(&det_tid, inc1_start, &muts[..1], None, None)
            .await
            .unwrap()
            .is_none(),
        "stale incarnation started_at_micros must NOT commit a newer PENDING incarnation"
    );
    coord
        .drop_matching_start(&det_tid, inc1_start)
        .await
        .unwrap();
    assert_eq!(
        coord
            .get_state(&det_tid)
            .await
            .unwrap()
            .and_then(|s| s.started_at),
        Some(inc2_start),
        "drop_matching_start with stale started_at must NOT delete newer incarnation"
    );
    let _ = coord
        .try_abort_pending(&det_tid, inc2_start, inc2_start)
        .await;
    coord
        .drop_matching_start(&det_tid, inc2_start)
        .await
        .unwrap();

    // --- Part C: 16-round concurrent race between try_commit_full + roll_forward vs sweeper ---
    for round in 0..16u32 {
        let ka = s_item(&[("pk", &format!("race_a_{round}"))]);
        let kb = s_item(&[("pk", &format!("race_b_{round}"))]);
        let mut ia = ka.clone();
        ia.insert("val".into(), AttributeValue::S("committed".into()));
        let mut ib = kb.clone();
        ib.insert("val".into(), AttributeValue::S("committed".into()));

        let pa = extenddb_storage_bigtable::transact::ParticipantRow {
            data_table: data_table.clone(),
            row_key: extenddb_storage_bigtable::data::encoding::row_key::encode_key(
                &ka,
                &key_info.key_schema,
            )
            .unwrap(),
        };
        let pb = extenddb_storage_bigtable::transact::ParticipantRow {
            data_table: data_table.clone(),
            row_key: extenddb_storage_bigtable::data::encoding::row_key::encode_key(
                &kb,
                &key_info.key_schema,
            )
            .unwrap(),
        };
        let tid = extenddb_storage_bigtable::transact::TxnCoordinator::new_txn_id();
        let (_, st_ts) = coord.open(&tid, &[pa.clone(), pb.clone()]).await.unwrap();
        assert!(coord.place_intent(&tid, &pa).await.unwrap());
        assert!(coord.place_intent(&tid, &pb).await.unwrap());

        let race_ver = extenddb_storage_bigtable::data::item_ops::next_occ_version(None);
        let rmuts = vec![
            extenddb_storage_bigtable::transact::ParticipantMutation {
                participant: pa.clone(),
                payload: extenddb_storage_bigtable::transact::TxnOpPayload::Put { item: ia },
                expected_version: None,
                row_existed: false,
                new_version: Some(race_ver),
            },
            extenddb_storage_bigtable::transact::ParticipantMutation {
                participant: pb.clone(),
                payload: extenddb_storage_bigtable::transact::TxnOpPayload::Put { item: ib },
                expected_version: None,
                row_existed: false,
                new_version: Some(race_ver),
            },
        ];

        let eng_w = ctx.engine.clone();
        let eng_s = ctx.engine.clone();
        let tid_w = tid.clone();
        let parts_w = vec![pa, pb];
        let writer_fut = async move {
            let c = extenddb_storage_bigtable::transact::TxnCoordinator::new(
                eng_w.client_ref(),
                Duration::from_secs(1),
            );
            if let Some(cts) = c
                .try_commit_full(&tid_w, st_ts, &rmuts, None, None)
                .await
                .unwrap()
            {
                let state = extenddb_storage_bigtable::transact::TxnState {
                    state: "COMMITTED".into(),
                    state_ts_micros: cts,
                    started_at: Some(st_ts),
                    participants: Some(parts_w),
                    mutations: Some(rmuts),
                    stream_records: None,
                    ttl_mutations: None,
                    token_meta: None,
                };
                eng_w.roll_forward(&tid_w, &state).await.unwrap();
                true
            } else {
                false
            }
        };
        let sweeper_fut = async move {
            extenddb_storage_bigtable::sweeper::sweep_with_cutoff_override(&eng_s, Some(i64::MAX))
                .await
                .unwrap();
        };
        let (committed, _) = tokio::join!(writer_fut, sweeper_fut);
        extenddb_storage_bigtable::sweeper::sweep_with_cutoff_override(&ctx.engine, Some(i64::MAX))
            .await
            .unwrap();

        let got_a = ctx.engine.get_item(&key_info, &ka).await.unwrap();
        let got_b = ctx.engine.get_item(&key_info, &kb).await.unwrap();
        assert_eq!(
            got_a.is_some(),
            committed,
            "round {round}: row A state must match coordinator commit outcome"
        );
        assert_eq!(
            got_a.is_some(),
            got_b.is_some(),
            "round {round}: all-or-nothing 2PC atomicity violated (A={got_a:?}, B={got_b:?})"
        );
    }

    // --- Part D: Post-COMMITTED intent older than `intent_timeout_secs = 1` MUST NOT
    //             regress `TransactGetItems` or allow conditional `attribute_not_exists(pk)` bypass!
    let short_lease_engine = BigtableEngine::new(ctx.client.clone(), ctx.client.clone(), 1);
    let capture = StreamCapture {
        view_type: StreamViewType::NewAndOldImages,
        user_identity: None,
        region: Arc::from(ctx.engine.region()),
    };
    let committed_key = s_item(&[("pk", "expired_committed_pk")]);
    let mut committed_item = committed_key.clone();
    committed_item.insert("val".into(), AttributeValue::S("must_not_regress".into()));
    let _stale_committed_tid = short_lease_engine
        .debug_fenced_put_commit_only(&key_info, committed_item, Some(&capture))
        .await
        .expect("commit-only put");

    tokio::time::sleep(Duration::from_millis(1250)).await;

    let tget = short_lease_engine
        .transact_get_items(&[TransactGetOp {
            key_info: &key_info,
            key: &committed_key,
        }])
        .await
        .expect("transact_get_items after intent timeout");
    assert_eq!(
        tget[0].as_ref().and_then(|i| i.get("val")),
        Some(&AttributeValue::S("must_not_regress".into())),
        "TransactGetItems must NOT ignore COMMITTED intent after wall-clock lease expiry"
    );

    let cond_not_exists = parse_cond("attribute_not_exists(pk)");
    let mut intruder = committed_key.clone();
    intruder.insert("val".into(), AttributeValue::S("intruder_clobber".into()));
    let cond_res = short_lease_engine
        .put_item(
            &key_info,
            intruder,
            false,
            Some(&cond_not_exists),
            &empty_maps,
            None,
        )
        .await;
    match cond_res {
        Err(StorageError::ConditionFailed(Some(prior))) => {
            assert_eq!(
                prior.get("val"),
                Some(&AttributeValue::S("must_not_regress".into()))
            );
        }
        other => panic!("expected ConditionFailed(Some(committed)), got {other:?}"),
    }
    let after_roll = short_lease_engine
        .get_item(&key_info, &committed_key)
        .await
        .unwrap()
        .expect("committed item rolled forward");
    assert_eq!(
        after_roll.get("val"),
        Some(&AttributeValue::S("must_not_regress".into()))
    );
}

// -----------------------------------------------------------------------------
// Test 16: `TransactGetItems` Key Validation, Dedup, Double-Read Multi-Row
//          Snapshot Validation & Zero HTTP 500s
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_transact_get_items_double_read_snapshot_validation_and_zero_500s() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_tget_snap");
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: table_name.clone(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: ScalarAttributeType::S,
                }],
                billing_mode: Some(BillingMode::PayPerRequest),
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let empty_maps = ExpressionMaps::default();

    // Upfront key validation: wrong scalar type must return TransactionCanceled(ValidationError)
    let mut bad_key = Item::new();
    bad_key.insert("pk".to_string(), AttributeValue::N("123".into()));
    match ctx
        .engine
        .transact_get_items(&[TransactGetOp {
            key_info: &key_info,
            key: &bad_key,
        }])
        .await
    {
        Err(StorageError::TransactionCanceled(reasons)) => {
            assert_eq!(reasons.len(), 1);
            assert_eq!(reasons[0].code, "ValidationError");
        }
        other => panic!("expected TransactionCanceled(ValidationError), got {other:?}"),
    }

    // Conservation-of-mass accounts: bal(acct_a) + bal(acct_b) == 1000 across all commits
    for (pk, bal) in [("acct_a", "500"), ("acct_b", "500")] {
        let mut it = s_item(&[("pk", pk)]);
        it.insert("bal".into(), AttributeValue::N(bal.into()));
        ctx.engine
            .put_item(&key_info, it, false, None, &empty_maps, None)
            .await
            .unwrap();
    }

    // Duplicate keys in a single TransactGetItems preserve positional output order
    let ka_dup = s_item(&[("pk", "acct_a")]);
    let kb_dup = s_item(&[("pk", "acct_b")]);
    let dup_snap = ctx
        .engine
        .transact_get_items(&[
            TransactGetOp {
                key_info: &key_info,
                key: &ka_dup,
            },
            TransactGetOp {
                key_info: &key_info,
                key: &kb_dup,
            },
            TransactGetOp {
                key_info: &key_info,
                key: &ka_dup,
            },
        ])
        .await
        .unwrap();
    assert_eq!(dup_snap.len(), 3);
    assert_eq!(dup_snap[0], dup_snap[2]);

    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut writer_handles = Vec::new();
    for w in 0..3u32 {
        let eng = ctx.engine.clone();
        let ki = key_info.clone();
        let done_w = done.clone();
        writer_handles.push(tokio::spawn(async move {
            let ka = s_item(&[("pk", "acct_a")]);
            let kb = s_item(&[("pk", "acct_b")]);
            let act_inc = parse_upd("SET bal = bal + :d");
            let act_dec = parse_upd("SET bal = bal - :d");
            for step in 0..15u32 {
                let delta = ((w + step) % 7 + 1).to_string();
                let mut maps = ExpressionMaps::default();
                maps.values.insert("d".into(), AttributeValue::N(delta));
                let (a_acts, b_acts) = if step % 2 == 0 {
                    (&act_inc, &act_dec)
                } else {
                    (&act_dec, &act_inc)
                };
                let ops = [
                    TransactWriteOp::Update {
                        key_info: &ki,
                        key: &ka,
                        actions: a_acts,
                        condition: None,
                        maps: &maps,
                        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                        stream: None,
                    },
                    TransactWriteOp::Update {
                        key_info: &ki,
                        key: &kb,
                        actions: b_acts,
                        condition: None,
                        maps: &maps,
                        return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                        stream: None,
                    },
                ];
                match eng.transact_write_items(&ops, None).await {
                    Ok(()) | Err(StorageError::TransactionCanceled(_)) => {}
                    Err(other) => panic!("unexpected writer error: {other:?}"),
                }
            }
            done_w.store(true, std::sync::atomic::Ordering::Release);
        }));
    }

    let mut reader_handles = Vec::new();
    for _ in 0..4u32 {
        let eng = ctx.engine.clone();
        let ki = key_info.clone();
        let done_r = done.clone();
        reader_handles.push(tokio::spawn(async move {
            let ka = s_item(&[("pk", "acct_a")]);
            let kb = s_item(&[("pk", "acct_b")]);
            let mut verified = 0usize;
            let mut reads = 0usize;
            while !done_r.load(std::sync::atomic::Ordering::Acquire) || reads < 20 {
                let ops = [
                    TransactGetOp {
                        key_info: &ki,
                        key: &ka,
                    },
                    TransactGetOp {
                        key_info: &ki,
                        key: &kb,
                    },
                ];
                match eng.transact_get_items(&ops).await {
                    Ok(snap) => {
                        let ba: i64 = match snap[0].as_ref().unwrap().get("bal").unwrap() {
                            AttributeValue::N(n) => n.parse().unwrap(),
                            other => panic!("unexpected bal: {other:?}"),
                        };
                        let bb: i64 = match snap[1].as_ref().unwrap().get("bal").unwrap() {
                            AttributeValue::N(n) => n.parse().unwrap(),
                            other => panic!("unexpected bal: {other:?}"),
                        };
                        assert_eq!(
                            ba + bb,
                            1000,
                            "torn TransactGetItems snapshot observed (acct_a={ba}, acct_b={bb})"
                        );
                        verified += 1;
                    }
                    Err(StorageError::TransactionCanceled(_)) => {}
                    Err(other) => {
                        panic!("unexpected TransactGetItems error (HTTP 500): {other:?}")
                    }
                }
                reads += 1;
            }
            verified
        }));
    }

    for h in writer_handles {
        h.await.unwrap();
    }
    done.store(true, std::sync::atomic::Ordering::Release);
    let mut total_verified = 0usize;
    for h in reader_handles {
        total_verified += h.await.unwrap();
    }
    assert!(total_verified > 0);
}

// -----------------------------------------------------------------------------
// Test 17: Contiguous Per-Shard Stream Slots, Concurrent Multi-Caller Dedup,
//          Phantom Guard & Live Tailing Contention
// -----------------------------------------------------------------------------
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_streams_contiguous_slots_concurrent_dedup_phantom_and_contention() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_stream_slots");
    let desc = ctx
        .engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: table_name.clone(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: ScalarAttributeType::S,
                }],
                billing_mode: Some(BillingMode::PayPerRequest),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::NewAndOldImages),
                }),
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    let stream_arn = desc.latest_stream_arn.clone().unwrap();
    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let capture = StreamCapture {
        view_type: StreamViewType::NewAndOldImages,
        user_identity: None,
        region: Arc::from(ctx.engine.region()),
    };
    let empty_maps = ExpressionMaps::default();

    // Find two distinct keys mapping onto the EXACT same stream shard (`SHARDS_PER_STREAM = 4`)
    let target_shard = ctx
        .engine
        .assign_shard(&ctx.account_id, &table_name, "shard_probe_0")
        .await
        .unwrap();
    let mut second_pk = String::new();
    for i in 1..200u32 {
        let cand = format!("shard_probe_{i}");
        if ctx
            .engine
            .assign_shard(&ctx.account_id, &table_name, &cand)
            .await
            .unwrap()
            == target_shard
        {
            second_pk = cand;
            break;
        }
    }
    assert!(!second_pk.is_empty(), "must find second key on same shard");

    // --- Part A: Out-of-order Phase 2 persistence on same shard must NEVER skip earlier commit ---
    let txn1_id = ctx
        .engine
        .debug_fenced_put_commit_only(
            &key_info,
            s_item(&[("pk", "shard_probe_0"), ("v", "first_commit")]),
            Some(&capture),
        )
        .await
        .unwrap();

    ctx.engine
        .put_item(
            &key_info,
            s_item(&[("pk", &second_pk), ("v", "second_commit")]),
            false,
            None,
            &empty_maps,
            Some(&capture),
        )
        .await
        .unwrap();

    let (batch1, after_seq) = ctx
        .engine
        .get_stream_records(&ctx.account_id, &stream_arn, &target_shard, None, 10)
        .await
        .unwrap();
    assert_eq!(batch1.len(), 1);
    assert_eq!(
        batch1[0].dynamodb.keys.get("pk"),
        Some(&AttributeValue::S(second_pk.clone()))
    );
    let cursor1 = after_seq.expect("cursor after second_commit");

    let coord = extenddb_storage_bigtable::transact::TxnCoordinator::new(
        ctx.engine.client_ref(),
        Duration::from_secs(60),
    );
    let txn1_state = coord.get_state(&txn1_id).await.unwrap().unwrap();
    ctx.engine
        .roll_forward(&txn1_id, &txn1_state)
        .await
        .unwrap();

    let (batch2, cursor2) = ctx
        .engine
        .get_stream_records(
            &ctx.account_id,
            &stream_arn,
            &target_shard,
            Some(&cursor1),
            10,
        )
        .await
        .unwrap();
    assert_eq!(
        batch2.len(),
        1,
        "delayed Phase 2 stream record on same shard must NOT be skipped after consumer cursor advances"
    );
    assert_eq!(
        batch2[0].dynamodb.keys.get("pk"),
        Some(&AttributeValue::S("shard_probe_0".into()))
    );
    assert!(batch2[0].dynamodb.sequence_number > cursor1);
    let cursor2 = cursor2.expect("cursor after recovered first_commit");

    // --- Part B: 12 concurrent callers racing `streams::write_record_ordered`
    //     on SEPARATE client connections with the SAME `event_id` MUST emit AT MOST ONCE!
    let shared_dedup_rec = extenddb_storage_bigtable::streams::build_record(
        desc.stream_specification.as_ref(),
        Some(&capture),
        &key_info.key_schema,
        None,
        Some(&s_item(&[("pk", "shard_probe_0"), ("v", "dedup_once")])),
        extenddb_storage_bigtable::streams::UNASSIGNED_SEQUENCE_NUMBER,
        ctx.engine.region(),
    )
    .unwrap();
    let expected_event_id = shared_dedup_rec.event_id.clone();
    let mut dedup_handles = Vec::new();
    for _ in 0..12usize {
        let cat_client = ctx.client.clone();
        let sh = target_shard.clone();
        let rec = shared_dedup_rec.clone();
        dedup_handles.push(tokio::spawn(async move {
            let cat = extenddb_storage_bigtable::catalog::Catalog::new(&cat_client);
            extenddb_storage_bigtable::streams::write_record_ordered(
                &cat,
                &sh,
                &rec,
                Some("dedup-op-0"),
            )
            .await
        }));
    }
    for h in dedup_handles {
        h.await
            .unwrap()
            .expect("concurrent write_record_ordered dedup");
    }
    let (dedup_batch, cursor3) = ctx
        .engine
        .get_stream_records(
            &ctx.account_id,
            &stream_arn,
            &target_shard,
            Some(&cursor2),
            20,
        )
        .await
        .unwrap();
    assert_eq!(
        dedup_batch.len(),
        1,
        "concurrent write_record_ordered with identical event_id/dedup_key must emit exactly 1 stream slot"
    );
    assert_eq!(dedup_batch[0].event_id, expected_event_id);
    let cursor3 = cursor3.expect("cursor after dedup_once");

    // --- Part C: Superseded `roll_forward` must emit ZERO phantom stream records ---
    let data_table = ctx.engine.data_table_for(&key_info).await.unwrap();
    let sup_key = s_item(&[("pk", "shard_probe_0")]);
    let sup_part = extenddb_storage_bigtable::transact::ParticipantRow {
        data_table,
        row_key: extenddb_storage_bigtable::data::encoding::row_key::encode_key(
            &sup_key,
            &key_info.key_schema,
        )
        .unwrap(),
    };
    let phantom_rec = extenddb_storage_bigtable::streams::build_record(
        desc.stream_specification.as_ref(),
        Some(&capture),
        &key_info.key_schema,
        None,
        Some(&s_item(&[("pk", "shard_probe_0"), ("v", "phantom")])),
        extenddb_storage_bigtable::streams::UNASSIGNED_SEQUENCE_NUMBER,
        ctx.engine.region(),
    )
    .unwrap();
    let superseded_state = extenddb_storage_bigtable::transact::TxnState {
        state: "COMMITTED".into(),
        state_ts_micros: 1_000_000,
        started_at: Some(900_000),
        participants: Some(vec![sup_part.clone()]),
        mutations: Some(vec![
            extenddb_storage_bigtable::transact::ParticipantMutation {
                participant: sup_part.clone(),
                payload: extenddb_storage_bigtable::transact::TxnOpPayload::Put {
                    item: s_item(&[("pk", "shard_probe_0"), ("v", "phantom")]),
                },
                expected_version: None,
                row_existed: false,
                new_version: Some(1), // strictly older than live OCC version!
            },
        ]),
        stream_records: Some(vec![extenddb_storage_bigtable::transact::TxnStreamRecord {
            stream_arn: stream_arn.clone(),
            shard_id: target_shard.clone(),
            record: phantom_rec,
            participant: Some(sup_part),
            dedup_key: Some("txn-superseded-phantom:0".into()),
        }]),
        ttl_mutations: None,
        token_meta: None,
    };
    ctx.engine
        .roll_forward("txn-superseded-phantom", &superseded_state)
        .await
        .unwrap();
    let (phantom_check, _) = ctx
        .engine
        .get_stream_records(
            &ctx.account_id,
            &stream_arn,
            &target_shard,
            Some(&cursor3),
            10,
        )
        .await
        .unwrap();
    assert!(
        phantom_check.is_empty(),
        "superseded roll_forward must emit ZERO phantom stream records"
    );

    // --- Part D: 36-way concurrent single-item UpdateItem ADD on stream-enabled table
    //     WITH a live tailing consumer polling `get_stream_records` concurrently!
    let hot_key = s_item(&[("pk", "hot_stream_counter")]);
    let hot_shard = ctx
        .engine
        .assign_shard(&ctx.account_id, &table_name, "hot_stream_counter")
        .await
        .unwrap();
    let (_, pre_hot_cursor) = ctx
        .engine
        .get_stream_records(&ctx.account_id, &stream_arn, &hot_shard, None, 100)
        .await
        .unwrap();

    let writers_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let tail_eng = ctx.engine.clone();
    let tail_acct = ctx.account_id.clone();
    let tail_arn = stream_arn.clone();
    let tail_shard = hot_shard.clone();
    let tail_done = writers_done.clone();
    let tail_handle = tokio::spawn(async move {
        let mut cursor = pre_hot_cursor;
        let mut tailed = Vec::new();
        loop {
            let done_now = tail_done.load(std::sync::atomic::Ordering::Acquire);
            let (recs, next_cur) = tail_eng
                .get_stream_records(&tail_acct, &tail_arn, &tail_shard, cursor.as_deref(), 5)
                .await
                .unwrap();
            if !recs.is_empty() {
                cursor = next_cur;
                tailed.extend(recs);
            } else if done_now {
                break;
            } else {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        tailed
    });

    let mut handles = Vec::with_capacity(36);
    for _ in 0..36usize {
        let eng = ctx.engine.clone();
        let ki = key_info.clone();
        let hk = hot_key.clone();
        let cap = capture.clone();
        handles.push(tokio::spawn(async move {
            let acts = parse_upd("SET cnt = if_not_exists(cnt, :z) + :one");
            let mut maps = ExpressionMaps::default();
            maps.values.insert("z".into(), AttributeValue::N("0".into()));
            maps.values
                .insert("one".into(), AttributeValue::N("1".into()));
            eng.update_item(&ki, &hk, &acts, false, true, None, &maps, Some(&cap))
                .await
                .expect("unconditional single-item stream UpdateItem must not abort with TransactionConflict")
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    writers_done.store(true, std::sync::atomic::Ordering::Release);
    let tailed_hot_recs: Vec<_> = tail_handle
        .await
        .unwrap()
        .into_iter()
        .filter(|r| {
            r.dynamodb.keys.get("pk") == Some(&AttributeValue::S("hot_stream_counter".into()))
        })
        .collect();
    assert_eq!(
        tailed_hot_recs.len(),
        36,
        "live tailing consumer must observe all 36 stream events without skipping any sequence number"
    );
    for pair in tailed_hot_recs.windows(2) {
        assert!(
            pair[0].dynamodb.sequence_number < pair[1].dynamodb.sequence_number,
            "tailed sequence numbers must be strictly increasing"
        );
    }

    let final_hot = ctx
        .engine
        .get_item(&key_info, &hot_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        final_hot.get("cnt"),
        Some(&AttributeValue::N("36".into())),
        "all 36 concurrent stream-enabled UpdateItem increments must apply with zero lost updates"
    );

    let disable_stream_in: extenddb_core::types::UpdateTableInput =
        serde_json::from_value(serde_json::json!({
            "TableName": table_name,
            "StreamSpecification": {
                "StreamEnabled": false
            }
        }))
        .unwrap();
    let disabled_desc = ctx
        .engine
        .update_table(&ctx.account_id, disable_stream_in)
        .await
        .expect("disable stream via UpdateTable");
    assert_eq!(
        disabled_desc
            .stream_specification
            .as_ref()
            .map(|s| s.stream_enabled),
        Some(false)
    );
}

// -----------------------------------------------------------------------------
// Test 18: `roll_forward` TTL Index Maintenance, `1000->future->1000` ABA,
//          Same-Second Fractional Restamping & Millisecond-Aligned Bounds
// -----------------------------------------------------------------------------
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_ttl_roll_forward_aba_same_second_fractional_and_millis_bounds() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_ttl_roll_forward");
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: table_name.clone(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: ScalarAttributeType::S,
                }],
                billing_mode: Some(BillingMode::PayPerRequest),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::NewAndOldImages),
                }),
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    ctx.engine
        .update_ttl(&ctx.account_id, &table_name, "ttl", true)
        .await
        .unwrap();
    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let capture = StreamCapture {
        view_type: StreamViewType::NewAndOldImages,
        user_identity: None,
        region: Arc::from(ctx.engine.region()),
    };
    let empty_maps = ExpressionMaps::default();

    // --- Part A: Crash right after `COMMITTED` on a TTL item (`ttl = 1000`),
    //             recover via `sweeper`, then run `ttl_worker::sweep_once`:
    //             `roll_forward` MUST populate `__extenddb_ttl_index__` so `ttl_worker` evicts it!
    let mut crashed_ttl_item = s_item(&[("pk", "ttl_crash_rec")]);
    crashed_ttl_item.insert("ttl".into(), AttributeValue::N("1000".into()));
    let _tid = ctx
        .engine
        .debug_fenced_put_commit_only(&key_info, crashed_ttl_item, Some(&capture))
        .await
        .unwrap();

    extenddb_storage_bigtable::sweeper::sweep_with_cutoff_override(&ctx.engine, Some(i64::MAX))
        .await
        .expect("sweeper roll-forward TTL item");
    let crash_key = s_item(&[("pk", "ttl_crash_rec")]);
    assert!(
        ctx.engine
            .get_item(&key_info, &crash_key)
            .await
            .unwrap()
            .is_some()
    );

    sweep_once(&ctx.engine).await.unwrap();
    assert!(
        ctx.engine
            .get_item(&key_info, &crash_key)
            .await
            .unwrap()
            .is_none(),
        "roll_forward must populate __extenddb_ttl_index__ so expired item is swept"
    );

    // --- Part B: `1000 -> 9999999999 -> 1000 -> 1000.5` ABA + same-second fractional restamping +
    //     millisecond-aligned (`% 1_000 == 0`) TTL index timestamps!
    let aba_key = s_item(&[("pk", "ttl_aba_item")]);
    for exp in ["1000", "9999999999", "1000", "1000.5"] {
        let mut it = aba_key.clone();
        it.insert("ttl".into(), AttributeValue::N(exp.into()));
        ctx.engine
            .put_item(&key_info, it, false, None, &empty_maps, None)
            .await
            .unwrap();
    }
    let data_table = ctx.engine.data_table_for(&key_info).await.unwrap();
    let base_ops = extenddb_storage_bigtable::data::item_ops::ItemOps::new(
        ctx.engine.client_ref(),
        &data_table,
        60,
    );
    let (_, Some(latest_ver)) = base_ops
        .get_with_version(&key_info, &aba_key)
        .await
        .unwrap()
    else {
        panic!("expected m:v on ttl_aba_item");
    };
    let enc_aba = extenddb_storage_bigtable::data::encoding::row_key::encode_key(
        &aba_key,
        &key_info.key_schema,
    )
    .unwrap();
    let aba_ttl_rk = extenddb_storage_bigtable::data::encoding::ttl_key::encode_ttl_key(
        &ctx.account_id,
        &table_name,
        &enc_aba,
        1000,
    );
    let ttl_rows = ctx
        .client
        .data()
        .read_rows(ReadRowsRequest {
            table_name: ctx.client.full_table_name(
                extenddb_storage_bigtable::data::encoding::ttl_key::TTL_INDEX_TABLE,
            ),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![aba_ttl_rk],
                row_ranges: vec![],
            }),
            ..ReadRowsRequest::default()
        })
        .await
        .unwrap();
    let (_, ttl_cells) = ttl_rows.into_iter().next().expect("ttl index row for 1000");
    assert!(!ttl_cells.is_empty());
    for c in &ttl_cells {
        assert_eq!(
            c.timestamp_micros % 1_000,
            0,
            "TTL index cell timestamp must be millisecond-aligned"
        );
    }
    assert_eq!(
        ttl_cells[0].timestamp_micros,
        (latest_ver * 1_000) as i64,
        "same-second fractional update (1000 -> 1000.5) must restamp TTL index cell to latest OCC version"
    );

    sweep_once(&ctx.engine).await.unwrap();
    assert!(
        ctx.engine
            .get_item(&key_info, &aba_key)
            .await
            .unwrap()
            .is_none(),
        "1000 -> future -> 1000 -> 1000.5 ABA item must be evicted by ttl_worker"
    );

    // Stale-900 index entry when live item has expired ttl = 1000 (!= 900):
    let mismatch_key = s_item(&[("pk", "ttl_mismatch_expired")]);
    let mut mismatch_item = mismatch_key.clone();
    mismatch_item.insert("ttl".into(), AttributeValue::N("1000".into()));
    ctx.engine
        .put_item(&key_info, mismatch_item, false, None, &empty_maps, None)
        .await
        .unwrap();
    let enc_mk = extenddb_storage_bigtable::data::encoding::row_key::encode_key(
        &mismatch_key,
        &key_info.key_schema,
    )
    .unwrap();
    let k1000 = extenddb_storage_bigtable::data::encoding::ttl_key::encode_ttl_key(
        &ctx.account_id,
        &table_name,
        &enc_mk,
        1000,
    );
    let k900 = extenddb_storage_bigtable::data::encoding::ttl_key::encode_ttl_key(
        &ctx.account_id,
        &table_name,
        &enc_mk,
        900,
    );
    ctx.client
        .data()
        .mutate_row(MutateRowRequest {
            table_name: ctx.client.full_table_name(
                extenddb_storage_bigtable::data::encoding::ttl_key::TTL_INDEX_TABLE,
            ),
            row_key: k1000,
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromRow(
                    mutation::DeleteFromRow {},
                )),
            }],
            ..MutateRowRequest::default()
        })
        .await
        .unwrap();
    ctx.client
        .data()
        .mutate_row(MutateRowRequest {
            table_name: ctx.client.full_table_name(
                extenddb_storage_bigtable::data::encoding::ttl_key::TTL_INDEX_TABLE,
            ),
            row_key: k900,
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
                    family_name: "d".into(),
                    column_qualifier: Vec::new(),
                    timestamp_micros: 1_000_000,
                    value: Vec::new(),
                })),
            }],
            ..MutateRowRequest::default()
        })
        .await
        .unwrap();
    sweep_once(&ctx.engine).await.unwrap();
    assert!(
        ctx.engine
            .get_item(&key_info, &mismatch_key)
            .await
            .unwrap()
            .is_none(),
        "ttl_worker scanning expiry=900 must still evict live item with expired ttl=1000"
    );
}

// -----------------------------------------------------------------------------
// Test 19: `ClientRequestToken` 4 Vectors, Empty-Ops Replay, Reused-Token
//          `clear_intent_up_to`, Sorted Locking & CCF
// -----------------------------------------------------------------------------
#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_idempotency_four_vectors_sorted_locking_and_ccf() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_idem_locking");
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: table_name.clone(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: ScalarAttributeType::S,
                }],
                billing_mode: Some(BillingMode::PayPerRequest),
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let empty_maps = ExpressionMaps::default();

    // --- Part A: 8 simultaneous concurrent calls with identical ClientRequestToken ---
    let simul_key = s_item(&[("pk", "idem_simul_counter")]);
    let mut simul_handles = Vec::new();
    for _ in 0..8usize {
        let eng = ctx.engine.clone();
        let ki = key_info.clone();
        let acct = ctx.account_id.clone();
        let sk = simul_key.clone();
        simul_handles.push(tokio::spawn(async move {
            let acts = parse_upd("SET cnt = if_not_exists(cnt, :z) + :ten");
            let mut maps = ExpressionMaps::default();
            maps.values
                .insert("z".into(), AttributeValue::N("0".into()));
            maps.values
                .insert("ten".into(), AttributeValue::N("10".into()));
            let ops = [TransactWriteOp::Update {
                key_info: &ki,
                key: &sk,
                actions: &acts,
                condition: None,
                maps: &maps,
                return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                stream: None,
            }];
            let ikey = IdempotencyKey {
                account_id: &acct,
                token: "tok-simul-8way",
                fingerprint: "fp-simul-8way",
            };
            eng.transact_write_items(&ops, Some(ikey)).await
        }));
    }
    let mut ok_count = 0usize;
    let mut replay_count = 0usize;
    for h in simul_handles {
        match h.await.unwrap() {
            Ok(()) => ok_count += 1,
            Err(StorageError::IdempotentReplay) => replay_count += 1,
            other => panic!("unexpected concurrent token result: {other:?}"),
        }
    }
    assert_eq!(ok_count, 1, "exactly one leader must return Ok(())");
    assert_eq!(
        replay_count, 7,
        "all 7 followers must return Err(IdempotentReplay)"
    );
    let simul_item = ctx
        .engine
        .get_item(&key_info, &simul_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        simul_item.get("cnt"),
        Some(&AttributeValue::N("10".into())),
        "concurrent token transaction must execute AT MOST ONCE"
    );

    // --- Part B: Crashed COMMITTED token txn before `idem:` exists:
    //                mismatched payload (including empty `&[]` ops!) returns IdempotentMismatch;
    //                matching payload returns IdempotentReplay (never Ok)!
    use sha2::{Digest, Sha256};
    let tok_b = "tok-crash-b";
    let derived_tid_b = {
        let mut hasher = Sha256::new();
        hasher.update(ctx.account_id.as_bytes());
        hasher.update(b":");
        hasher.update(tok_b.as_bytes());
        format!("{:x}", hasher.finalize())
    };
    let data_table = ctx.engine.data_table_for(&key_info).await.unwrap();
    let coord = extenddb_storage_bigtable::transact::TxnCoordinator::new(
        ctx.engine.client_ref(),
        Duration::from_secs(60),
    );
    let item_b = s_item(&[("pk", "idem_b_pk"), ("v", "from_b")]);
    let part_b = extenddb_storage_bigtable::transact::ParticipantRow {
        data_table: data_table.clone(),
        row_key: extenddb_storage_bigtable::data::encoding::row_key::encode_key(
            &item_b,
            &key_info.key_schema,
        )
        .unwrap(),
    };
    let tmeta_b = extenddb_storage_bigtable::transact::TxnTokenMeta {
        account_id: ctx.account_id.clone(),
        token: tok_b.to_string(),
        fingerprint: "fp-b-good".to_string(),
    };
    let (_, st_b) = coord
        .open_with_token(
            &derived_tid_b,
            std::slice::from_ref(&part_b),
            Some(&tmeta_b),
        )
        .await
        .unwrap();
    assert!(coord.place_intent(&derived_tid_b, &part_b).await.unwrap());
    let ver_b = extenddb_storage_bigtable::data::item_ops::next_occ_version(None);
    coord
        .try_commit_full(
            &derived_tid_b,
            st_b,
            &[extenddb_storage_bigtable::transact::ParticipantMutation {
                participant: part_b.clone(),
                payload: extenddb_storage_bigtable::transact::TxnOpPayload::Put {
                    item: item_b.clone(),
                },
                expected_version: None,
                row_existed: false,
                new_version: Some(ver_b),
            }],
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

    // Empty-ops call with wrong fingerprint MUST still hit the COMMITTED coordinator row!
    let empty_mismatch_res = ctx
        .engine
        .transact_write_items(
            &[],
            Some(IdempotencyKey {
                account_id: &ctx.account_id,
                token: tok_b,
                fingerprint: "fp-b-empty-wrong",
            }),
        )
        .await;
    assert!(
        matches!(empty_mismatch_res, Err(StorageError::IdempotentMismatch)),
        "empty-ops mismatched payload against COMMITTED token txn must return IdempotentMismatch"
    );

    let replay_res = ctx
        .engine
        .transact_write_items(
            &[TransactWriteOp::Put {
                key_info: &key_info,
                item: &item_b,
                condition: None,
                maps: &empty_maps,
                return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                stream: None,
            }],
            Some(IdempotencyKey {
                account_id: &ctx.account_id,
                token: tok_b,
                fingerprint: "fp-b-good",
            }),
        )
        .await;
    assert!(
        matches!(replay_res, Err(StorageError::IdempotentReplay)),
        "matching replay must return IdempotentReplay, got {replay_res:?}"
    );

    // Verify `clear_intent_up_to` protects a newer reused-token intent timestamp from stale cleanup:
    assert!(coord.place_intent(&derived_tid_b, &part_b).await.unwrap());
    let snap_b = extenddb_storage_bigtable::data::item_ops::ItemOps::new(
        ctx.engine.client_ref(),
        &data_table,
        60,
    )
    .get_row_snapshot_by_raw_key(&part_b.row_key)
    .await
    .unwrap();
    let (_, newer_intent_ts) = snap_b
        .intents
        .iter()
        .find(|(t, _)| t == &derived_tid_b)
        .cloned()
        .expect("placed intent");
    coord
        .clear_intent_up_to(
            &derived_tid_b,
            &part_b,
            newer_intent_ts.saturating_sub(1_000),
        )
        .await
        .unwrap();
    let snap_after_stale_clear = extenddb_storage_bigtable::data::item_ops::ItemOps::new(
        ctx.engine.client_ref(),
        &data_table,
        60,
    )
    .get_row_snapshot_by_raw_key(&part_b.row_key)
    .await
    .unwrap();
    assert!(
        snap_after_stale_clear
            .intents
            .iter()
            .any(|(t, _)| t == &derived_tid_b),
        "clear_intent_up_to with older bound must NOT strip a newer reused-token intent"
    );
    coord.clear_intent(&derived_tid_b, &part_b).await.unwrap();

    // --- Part C & D: Sweeper persists `idem:` before dropping recovered token txn,
    //                      and expired (>600s) `applied_at` allows fresh execution!
    let tok_c = "tok-sweep-c";
    let derived_tid_c = {
        let mut hasher = Sha256::new();
        hasher.update(ctx.account_id.as_bytes());
        hasher.update(b":");
        hasher.update(tok_c.as_bytes());
        format!("{:x}", hasher.finalize())
    };
    let item_c = s_item(&[("pk", "idem_c_pk"), ("v", "from_sweep")]);
    let part_c = extenddb_storage_bigtable::transact::ParticipantRow {
        data_table,
        row_key: extenddb_storage_bigtable::data::encoding::row_key::encode_key(
            &item_c,
            &key_info.key_schema,
        )
        .unwrap(),
    };
    let tmeta_c = extenddb_storage_bigtable::transact::TxnTokenMeta {
        account_id: ctx.account_id.clone(),
        token: tok_c.to_string(),
        fingerprint: "fp-c-good".to_string(),
    };
    let (_, st_c) = coord
        .open_with_token(
            &derived_tid_c,
            std::slice::from_ref(&part_c),
            Some(&tmeta_c),
        )
        .await
        .unwrap();
    assert!(coord.place_intent(&derived_tid_c, &part_c).await.unwrap());
    let ver_c = extenddb_storage_bigtable::data::item_ops::next_occ_version(None);
    coord
        .try_commit_full(
            &derived_tid_c,
            st_c,
            &[extenddb_storage_bigtable::transact::ParticipantMutation {
                participant: part_c,
                payload: extenddb_storage_bigtable::transact::TxnOpPayload::Put {
                    item: item_c.clone(),
                },
                expected_version: None,
                row_existed: false,
                new_version: Some(ver_c),
            }],
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

    extenddb_storage_bigtable::sweeper::sweep_with_cutoff_override(&ctx.engine, Some(i64::MAX))
        .await
        .expect("sweeper recovers token txn c");

    assert!(matches!(
        ctx.engine
            .transact_write_items(
                &[TransactWriteOp::Put {
                    key_info: &key_info,
                    item: &item_c,
                    condition: None,
                    maps: &empty_maps,
                    return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                    stream: None,
                }],
                Some(IdempotencyKey {
                    account_id: &ctx.account_id,
                    token: tok_c,
                    fingerprint: "fp-c-good",
                }),
            )
            .await,
        Err(StorageError::IdempotentReplay)
    ));

    let cat = extenddb_storage_bigtable::catalog::Catalog::new(ctx.engine.catalog_client_ref());
    let expired_idem_key =
        extenddb_storage_bigtable::catalog::keys::idempotency(&ctx.account_id, tok_c);
    cat.put(
        &expired_idem_key,
        &serde_json::json!({
            "fingerprint": "fp-c-good",
            "txn_id": derived_tid_c,
            "applied_at": time::OffsetDateTime::now_utc().unix_timestamp() - 605,
        }),
    )
    .await
    .unwrap();

    ctx.engine
        .transact_write_items(
            &[TransactWriteOp::Put {
                key_info: &key_info,
                item: &s_item(&[("pk", "idem_c_pk"), ("v", "reused_after_10m")]),
                condition: None,
                maps: &empty_maps,
                return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                stream: None,
            }],
            Some(IdempotencyKey {
                account_id: &ctx.account_id,
                token: tok_c,
                fingerprint: "fp-c-brand-new",
            }),
        )
        .await
        .expect("expired >10m idempotency record must allow token reuse");

    // --- Part E: Sorted [k1, k2] vs [k2, k1] locking & ReturnValuesOnConditionCheckFailure ---
    let k1 = s_item(&[("pk", "lock_k1")]);
    let k2 = s_item(&[("pk", "lock_k2")]);
    let mut lock_handles = Vec::new();
    for i in 0..10usize {
        let eng = ctx.engine.clone();
        let ki = key_info.clone();
        let (first, second) = if i % 2 == 0 {
            (k1.clone(), k2.clone())
        } else {
            (k2.clone(), k1.clone())
        };
        lock_handles.push(tokio::spawn(async move {
            let acts = parse_upd("SET c = if_not_exists(c, :z) + :one");
            let mut maps = ExpressionMaps::default();
            maps.values
                .insert("z".into(), AttributeValue::N("0".into()));
            maps.values
                .insert("one".into(), AttributeValue::N("1".into()));
            let ops = [
                TransactWriteOp::Update {
                    key_info: &ki,
                    key: &first,
                    actions: &acts,
                    condition: None,
                    maps: &maps,
                    return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                    stream: None,
                },
                TransactWriteOp::Update {
                    key_info: &ki,
                    key: &second,
                    actions: &acts,
                    condition: None,
                    maps: &maps,
                    return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                    stream: None,
                },
            ];
            eng.transact_write_items(&ops, None).await
        }));
    }
    let mut committed_pairs = 0i64;
    for h in lock_handles {
        match h.await.unwrap() {
            Ok(()) => committed_pairs += 1,
            Err(StorageError::TransactionCanceled(_)) => {}
            Err(other) => {
                panic!("2PC conflict must return TransactionCanceled, got {other:?}")
            }
        }
    }
    assert!(committed_pairs >= 1);
    let r1 = ctx.engine.get_item(&key_info, &k1).await.unwrap().unwrap();
    let r2 = ctx.engine.get_item(&key_info, &k2).await.unwrap().unwrap();
    assert_eq!(
        r1.get("c"),
        Some(&AttributeValue::N(committed_pairs.to_string()))
    );
    assert_eq!(
        r2.get("c"),
        Some(&AttributeValue::N(committed_pairs.to_string()))
    );

    let cond_fail = parse_cond("attribute_not_exists(pk)");
    for (rv_ccf, expect_some) in [
        (ReturnValuesOnConditionCheckFailure::None, false),
        (ReturnValuesOnConditionCheckFailure::AllOld, true),
    ] {
        let ops = [TransactWriteOp::Put {
            key_info: &key_info,
            item: &s_item(&[("pk", "lock_k1"), ("x", "nope")]),
            condition: Some(&cond_fail),
            maps: &empty_maps,
            return_values_on_ccf: rv_ccf,
            stream: None,
        }];
        match ctx.engine.transact_write_items(&ops, None).await {
            Err(StorageError::TransactionCanceled(reasons)) => {
                assert_eq!(
                    reasons[0].item.is_some(),
                    expect_some,
                    "ReturnValuesOnConditionCheckFailure={rv_ccf:?} item presence mismatch"
                );
            }
            other => panic!("expected TransactionCanceled, got {other:?}"),
        }
    }
}

// -----------------------------------------------------------------------------
// Test 20: GSI Version Guard, CREATING Read Refusal, Full `UpdateTable` GSI
//          Lifecycle, LSI `Scan` Sparse/Sort/Deleted-ESK/LEK & CAS RMW
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_gsi_version_guard_creating_refusal_update_table_lsi_scan_and_cas() {
    use extenddb_core::types::{
        CreateGsiAction, DeleteGsiAction, GlobalSecondaryIndexUpdate, LsiInput, Tag,
        UpdateGsiAction, UpdateTableInput, WarmThroughput,
    };
    use extenddb_storage::management_store::{MetricsFilter, MetricsRow, MetricsStore};
    use extenddb_storage_bigtable::catalog_store::BigtableCatalogStore;

    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_gsi_lsi_cas");
    let initial_desc = ctx
        .engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: table_name.clone(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".to_string(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".to_string(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".to_string(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".to_string(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "score".to_string(),
                        attribute_type: ScalarAttributeType::N,
                    },
                ],
                local_secondary_indexes: Some(vec![LsiInput {
                    index_name: "by_score".to_string(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "pk".to_string(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "score".to_string(),
                            key_type: KeyType::Range,
                        },
                    ],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                }]),
                billing_mode: Some(BillingMode::PayPerRequest),
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .expect("create composite + LSI table");

    let base_ki = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let empty_maps = ExpressionMaps::default();

    // Insert 6 sparse rows WITHOUT `score` sorting BEFORE `"a"`..`"z"` in base `sk` order:
    for i in 0..6u32 {
        let mut it = s_item(&[("pk", "p1"), ("sk", &format!("00_sparse_{i}"))]);
        if i < 2 {
            it.insert("dept".into(), AttributeValue::S("eng".into()));
        }
        ctx.engine
            .put_item(&base_ki, it, false, None, &empty_maps, None)
            .await
            .unwrap();
    }
    // Insert 4 LSI-indexed rows whose `score` order INVERTS base `sk` order + ties on score=20:
    for (sk, score, dept) in [
        ("z", "10", "eng"),
        ("m2", "20", "eng"),
        ("m1", "20", "sales"),
        ("a", "30", "sales"),
    ] {
        let mut it = s_item(&[("pk", "p1"), ("sk", sk), ("dept", dept)]);
        it.insert("score".into(), AttributeValue::N(score.into()));
        ctx.engine
            .put_item(&base_ki, it, false, None, &empty_maps, None)
            .await
            .unwrap();
    }

    // --- Part A: LSI `Scan` with `limit = 2` when `key_info.key_schema` is overwritten
    //             by `extenddb-engine` to `lsi.key_schema`:
    let mut engine_lsi_ki = base_ki.clone();
    engine_lsi_ki.key_schema = base_ki.local_secondary_indexes[0].key_schema.clone();

    let (page1, lek1) = ctx
        .engine
        .scan(&engine_lsi_ki, Some(2), None, None, None, Some("by_score"))
        .await
        .unwrap();
    assert_eq!(
        page1.len(),
        2,
        "LSI Scan with Limit=2 must skip sparse rows BEFORE applying Limit"
    );
    assert_eq!(page1[0].get("sk"), Some(&AttributeValue::S("z".into())));
    assert_eq!(page1[1].get("sk"), Some(&AttributeValue::S("m1".into())));
    let lek1 = lek1.expect("LSI Scan page 1 must emit LastEvaluatedKey");
    assert!(
        lek1.contains_key("pk") && lek1.contains_key("sk") && lek1.contains_key("score"),
        "LSI Scan LastEvaluatedKey must include (pk, base_sk, lsi_sk), got {lek1:?}"
    );

    // Delete the `lek1` item ("m1") BEFORE calling page 2 to verify deleted-ESK resumption,
    // then re-insert it for subsequent GSI backfill assertions!
    let m1_base_key = s_item(&[("pk", "p1"), ("sk", "m1")]);
    ctx.engine
        .delete_item(&base_ki, &m1_base_key, false, None, &empty_maps, None)
        .await
        .unwrap();

    let (page2, lek2) = ctx
        .engine
        .scan(
            &engine_lsi_ki,
            Some(2),
            Some(&lek1),
            None,
            None,
            Some("by_score"),
        )
        .await
        .unwrap();
    assert_eq!(page2.len(), 2);
    assert_eq!(page2[0].get("sk"), Some(&AttributeValue::S("m2".into())));
    assert_eq!(page2[1].get("sk"), Some(&AttributeValue::S("a".into())));
    assert!(
        lek2.is_none(),
        "exact exhaustion after remaining LSI items must emit lek=None"
    );

    // Re-insert m1
    let mut m1_item = s_item(&[("pk", "p1"), ("sk", "m1"), ("dept", "sales")]);
    m1_item.insert("score".into(), AttributeValue::N("20".into()));
    ctx.engine
        .put_item(&base_ki, m1_item, false, None, &empty_maps, None)
        .await
        .unwrap();

    // --- Part B: `UpdateTable` GSI Create + backfill + TableClass + WarmThroughput ---
    let upd_desc = ctx
        .engine
        .update_table(
            &ctx.account_id,
            UpdateTableInput {
                table_name: table_name.clone(),
                billing_mode: None,
                table_throughput_mode: None,
                provisioned_throughput: None,
                deletion_protection_enabled: None,
                global_secondary_index_updates: Some(vec![GlobalSecondaryIndexUpdate {
                    create: Some(CreateGsiAction {
                        index_name: "by_dept".to_string(),
                        key_schema: vec![KeySchemaElement {
                            attribute_name: "dept".to_string(),
                            key_type: KeyType::Hash,
                        }],
                        projection: Projection {
                            projection_type: ProjectionType::All,
                            non_key_attributes: None,
                        },
                        provisioned_throughput: None,
                        warm_throughput: Some(WarmThroughput {
                            read_units_per_second: Some(15_000),
                            write_units_per_second: Some(5_000),
                        }),
                    }),
                    update: None,
                    delete: None,
                }]),
                attribute_definitions: Some(vec![AttributeDefinition {
                    attribute_name: "dept".to_string(),
                    attribute_type: ScalarAttributeType::S,
                }]),
                stream_specification: None,
                table_class: Some("STANDARD_INFREQUENT_ACCESS".to_string()),
                on_demand_throughput: None,
                vector_index_updates: None,
                warm_throughput: Some(WarmThroughput {
                    read_units_per_second: Some(20_000),
                    write_units_per_second: Some(8_000),
                }),
                sse_specification: None,
            },
        )
        .await
        .expect("UpdateTable GSI Create must succeed");

    assert_eq!(
        upd_desc.table_class_summary,
        Some(serde_json::json!({ "TableClass": "STANDARD_INFREQUENT_ACCESS" }))
    );
    assert_eq!(
        upd_desc
            .warm_throughput_record
            .as_ref()
            .and_then(|r| r.read_units_per_second),
        Some(20_000)
    );
    let gsis = upd_desc
        .global_secondary_indexes
        .as_ref()
        .expect("created GSI");
    assert_eq!(gsis.len(), 1);
    assert_eq!(gsis[0].index_name, "by_dept");
    assert_eq!(gsis[0].index_status, "ACTIVE");

    // Verify paginated GSI Query when `key_info.key_schema` is overwritten by `extenddb-engine`:
    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let mut engine_gsi_ki = key_info.clone();
    engine_gsi_ki.key_schema = gsis[0].key_schema.clone();
    let mut qmaps = ExpressionMaps::default();
    qmaps
        .values
        .insert("d".into(), AttributeValue::S("eng".into()));
    let kc = parse_kc("dept = :d");
    let (gsi_p1, gsi_lek1) = ctx
        .engine
        .query(
            &engine_gsi_ki,
            &kc,
            &qmaps,
            true,
            Some(2),
            None,
            Some("by_dept"),
        )
        .await
        .unwrap();
    assert_eq!(gsi_p1.len(), 2);
    let gsi_lek1 = gsi_lek1.expect("GSI LEK");
    assert!(
        gsi_lek1.contains_key("pk") && gsi_lek1.contains_key("sk") && gsi_lek1.contains_key("dept"),
        "GSI LastEvaluatedKey must include (pk, sk, dept): got {gsi_lek1:?}"
    );
    let (gsi_p2, gsi_lek2) = ctx
        .engine
        .query(
            &engine_gsi_ki,
            &kc,
            &qmaps,
            true,
            Some(5),
            Some(&gsi_lek1),
            Some("by_dept"),
        )
        .await
        .unwrap();
    assert_eq!(gsi_p2.len(), 2, "all 4 eng items must be backfilled");
    assert!(gsi_lek2.is_none());

    // While a GSI has `index_status == "CREATING"` in catalog,
    // `index_info`, `query`, and `scan` MUST reject reads with `IndexNotFound` until
    // reconciliation promotes it `"CREATING" -> "ACTIVE"`!
    let cat = extenddb_storage_bigtable::catalog::Catalog::new(ctx.engine.catalog_client_ref());
    let tbl_rk = extenddb_storage_bigtable::catalog::keys::table_meta(&ctx.account_id, &table_name);
    let mut raw_tbl = cat.get(&tbl_rk).await.unwrap().unwrap();
    let mut raw_desc: extenddb_core::types::TableDescription =
        serde_json::from_value(raw_tbl["description"].clone()).unwrap();
    raw_desc.global_secondary_indexes.as_mut().unwrap()[0].index_status = "CREATING".to_string();
    raw_tbl["description"] = serde_json::to_value(&raw_desc).unwrap();
    cat.put(&tbl_rk, &raw_tbl).await.unwrap();
    let fresh_eng = Arc::new(BigtableEngine::new(
        ctx.client.clone(),
        ctx.client.clone(),
        60,
    ));
    let fresh_ki = fresh_eng
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    assert!(matches!(
        fresh_eng
            .index_info(&ctx.account_id, &table_name, "by_dept")
            .await,
        Err(StorageError::IndexNotFound(_))
    ));
    assert!(matches!(
        fresh_eng
            .query(&fresh_ki, &kc, &qmaps, true, None, None, Some("by_dept"))
            .await,
        Err(StorageError::IndexNotFound(_))
    ));
    assert!(matches!(
        fresh_eng
            .scan(&fresh_ki, None, None, None, None, Some("by_dept"))
            .await,
        Err(StorageError::IndexNotFound(_))
    ));

    // Reconciler promotes `"CREATING" -> "ACTIVE"` via CAS once backfill completes cleanly!
    extenddb_storage_bigtable::gsi_worker::sweep_once(&fresh_eng)
        .await
        .expect("gsi_worker::sweep_once promotes CREATING -> ACTIVE");
    let promoted_ki = fresh_eng
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let (after_prom, _) = fresh_eng
        .query(&promoted_ki, &kc, &qmaps, true, None, None, Some("by_dept"))
        .await
        .expect("GSI query succeeds once promoted ACTIVE");
    assert_eq!(after_prom.len(), 4);

    // --- Part C: Delayed older version (`v1 < v2`) Put/Delete and orphan guard on GSI shadow ---
    let data_table = fresh_eng.data_table_for(&promoted_ki).await.unwrap();
    let shadow_table = extenddb_storage_bigtable::gsi::shadow_table_id(&data_table, "by_dept");
    let shadow_ops = extenddb_storage_bigtable::data::item_ops::ItemOps::new(
        fresh_eng.client_ref(),
        &shadow_table,
        60,
    );
    let target_shadow_item = &after_prom[0];
    let shadow_rk = extenddb_storage_bigtable::gsi::shadow_row_key_for_item(
        target_shadow_item,
        &gsis[0].key_schema,
        &promoted_ki.key_schema,
    )
    .unwrap()
    .unwrap();
    let mut stale_shadow_item = target_shadow_item.clone();
    stale_shadow_item.insert("corrupted".into(), AttributeValue::S("stale_v1".into()));
    assert!(
        !shadow_ops
            .put_shadow_versioned(&shadow_rk, &stale_shadow_item, 1)
            .await
            .unwrap(),
        "delayed older version Put on GSI shadow must be rejected"
    );
    assert!(
        !shadow_ops
            .delete_shadow_versioned(&shadow_rk, 1)
            .await
            .unwrap(),
        "delayed older version Delete on GSI shadow must be rejected"
    );

    // Seed an unversioned orphaned shadow row whose base row ("p_ghost", "sk_ghost") never existed,
    // verify `gsi_worker::sweep_once` cleans it via `delete_shadow_orphan_if_unchanged` (leaving a
    // monotonic `m:v` tombstone), and verify a delayed `put_shadow_versioned(..., 1)` is rejected!
    let ghost_item = s_item(&[("pk", "p_ghost"), ("sk", "sk_ghost"), ("dept", "eng")]);
    let ghost_shadow_rk = extenddb_storage_bigtable::gsi::shadow_row_key_for_item(
        &ghost_item,
        &gsis[0].key_schema,
        &promoted_ki.key_schema,
    )
    .unwrap()
    .unwrap();
    assert!(
        shadow_ops
            .put_shadow_versioned(&ghost_shadow_rk, &ghost_item, 5)
            .await
            .unwrap()
    );
    extenddb_storage_bigtable::gsi_worker::sweep_once(&fresh_eng)
        .await
        .expect("gsi_worker::sweep_once cleans orphaned shadow row");
    assert!(
        !shadow_ops
            .put_shadow_versioned(&ghost_shadow_rk, &ghost_item, 1)
            .await
            .unwrap(),
        "orphan cleanup must retain monotonic m:v tombstone rejecting delayed v1 Put"
    );
    let (post_orphan_eng, _) = fresh_eng
        .query(&promoted_ki, &kc, &qmaps, true, None, None, Some("by_dept"))
        .await
        .unwrap();
    assert_eq!(post_orphan_eng.len(), 4);

    // Update GSI WarmThroughput & Delete GSI via UpdateTable
    let upd_wt_desc = fresh_eng
        .update_table(
            &ctx.account_id,
            UpdateTableInput {
                table_name: table_name.clone(),
                billing_mode: None,
                table_throughput_mode: None,
                provisioned_throughput: None,
                deletion_protection_enabled: None,
                global_secondary_index_updates: Some(vec![GlobalSecondaryIndexUpdate {
                    create: None,
                    update: Some(UpdateGsiAction {
                        index_name: "by_dept".to_string(),
                        warm_throughput: Some(WarmThroughput {
                            read_units_per_second: Some(25_000),
                            write_units_per_second: None,
                        }),
                        provisioned_throughput: None,
                        on_demand_throughput: None,
                    }),
                    delete: None,
                }]),
                attribute_definitions: None,
                stream_specification: None,
                table_class: None,
                on_demand_throughput: None,
                vector_index_updates: None,
                warm_throughput: None,
                sse_specification: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        upd_wt_desc.global_secondary_indexes.as_ref().unwrap()[0]
            .warm_throughput_record
            .as_ref()
            .and_then(|r| r.read_units_per_second),
        Some(25_000)
    );

    let del_desc = fresh_eng
        .update_table(
            &ctx.account_id,
            UpdateTableInput {
                table_name: table_name.clone(),
                billing_mode: None,
                table_throughput_mode: None,
                provisioned_throughput: None,
                deletion_protection_enabled: None,
                global_secondary_index_updates: Some(vec![GlobalSecondaryIndexUpdate {
                    create: None,
                    update: None,
                    delete: Some(DeleteGsiAction {
                        index_name: "by_dept".to_string(),
                    }),
                }]),
                attribute_definitions: None,
                stream_specification: None,
                table_class: None,
                on_demand_throughput: None,
                vector_index_updates: None,
                warm_throughput: None,
                sse_specification: None,
            },
        )
        .await
        .unwrap();
    assert!(del_desc.global_secondary_indexes.is_none());
    assert!(
        !del_desc
            .attribute_definitions
            .iter()
            .any(|a| a.attribute_name == "dept"),
        "deleting GSI must prune orphaned dept AttributeDefinition"
    );

    // --- Part D: Concurrent CAS `MetricsStore::insert_metrics` (20-way) & `tag_resource` (16-way) ---
    let cat_store = Arc::new(BigtableCatalogStore::new(ctx.client.clone(), None, true));
    let bucket_ts = time::OffsetDateTime::from_unix_timestamp(1_750_000_020).unwrap();
    let mut m_handles = Vec::new();
    for _ in 0..20usize {
        let cs = cat_store.clone();
        let acct = ctx.account_id.clone();
        let tbl = table_name.clone();
        m_handles.push(tokio::spawn(async move {
            cs.insert_metrics(&[MetricsRow {
                bucket: bucket_ts,
                account_id: Some(acct),
                metric: "SuccessfulRequestLatency".into(),
                table_name: Some(tbl),
                index_name: None,
                operation: Some("PutItem".into()),
                sum: 1.5,
                count: 1,
                min: 1.5,
                max: 1.5,
            }])
            .await
            .unwrap();
        }));
    }
    for h in m_handles {
        h.await.unwrap();
    }
    let queried_metrics = cat_store
        .query_metrics(
            time::OffsetDateTime::from_unix_timestamp(1_750_000_019).unwrap(),
            time::OffsetDateTime::from_unix_timestamp(1_750_000_021).unwrap(),
            &MetricsFilter {
                account_id: Some(ctx.account_id.clone()),
                table_name: Some(table_name.clone()),
                metric: Some("SuccessfulRequestLatency".into()),
            },
        )
        .await
        .unwrap();
    assert_eq!(queried_metrics.len(), 1);
    assert_eq!(
        queried_metrics[0].count, 20,
        "concurrent MetricsStore::insert_metrics must accumulate all 20 increments via CAS"
    );
    assert!((queried_metrics[0].sum - 30.0).abs() < 1e-6);

    let mut t_handles = Vec::new();
    for i in 0..16usize {
        let eng = fresh_eng.clone();
        let arn = initial_desc.table_arn.clone();
        t_handles.push(tokio::spawn(async move {
            eng.tag_resource(
                &arn,
                &[Tag {
                    key: format!("k_{i}"),
                    value: format!("v_{i}"),
                }],
            )
            .await
            .unwrap();
        }));
    }
    for h in t_handles {
        h.await.unwrap();
    }
    let tags = fresh_eng.list_tags(&initial_desc.table_arn).await.unwrap();
    assert_eq!(
        tags.len(),
        16,
        "concurrent tag_resource calls must preserve all 16 tags via CAS"
    );
}

#[test]
fn test_required_flag_parsing() {
    assert!(!is_required_flag_enabled(None));
    assert!(!is_required_flag_enabled(Some("")));
    assert!(!is_required_flag_enabled(Some("   ")));
    assert!(!is_required_flag_enabled(Some("0")));
    assert!(!is_required_flag_enabled(Some(" 0 ")));
    assert!(is_required_flag_enabled(Some("1")));
    assert!(is_required_flag_enabled(Some("true")));
    assert!(is_required_flag_enabled(Some("required")));
}
