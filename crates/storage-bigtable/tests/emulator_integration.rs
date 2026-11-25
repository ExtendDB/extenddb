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
use extenddb_storage_bigtable::ttl_worker::sweep_once;
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

    let target = bootstrapper.init_target();
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

    let enabling_desc = ctx
        .engine
        .describe_ttl(&ctx.account_id, &table_name)
        .await
        .expect("describe_ttl enabling failed");
    assert_eq!(
        enabling_desc.time_to_live_status,
        TimeToLiveStatus::Enabling
    );
    assert_eq!(enabling_desc.attribute_name, Some("expires_at".to_string()));

    ctx.engine
        .create_ttl_index(&ctx.account_id, &table_name, "expires_at")
        .await
        .expect("create_ttl_index failed");

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

    // Before sweeper runs, raw Bigtable family `d` does NOT yet have the item,
    // while BOTH `GetItem` (#40) and `TransactGetItems` serve the committed
    // post-image via read-through-log!
    let data_table = ctx.engine.data_table_for(&key_info).await.unwrap();
    let crash_rk = extenddb_storage_bigtable::data::encoding::row_key::encode_key(
        &crash_key,
        &key_info.key_schema,
    )
    .unwrap();
    let raw_before_sweep = extenddb_storage_bigtable::data::item_ops::ItemOps::new(
        ctx.engine.client_ref(),
        &data_table,
        60,
    )
    .get_row_snapshot_by_raw_key(&crash_rk)
    .await
    .unwrap();
    assert!(
        raw_before_sweep.item.is_none(),
        "raw Bigtable family d before roll-forward is still unapplied"
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

    // 4. Verify `GetItem` (#40) immediately resolves a `COMMITTED` intent via
    //    `read_row_resolving_committed_intents` without waiting for the sweeper!
    let mut get_resolve_item: Item = BTreeMap::new();
    get_resolve_item.insert("pk".to_string(), AttributeValue::S("s_get_resolve".into()));
    get_resolve_item.insert("val".to_string(), AttributeValue::S("via_get_item".into()));
    ctx.engine
        .debug_fenced_put_commit_only(&key_info, get_resolve_item, Some(&capture))
        .await
        .expect("debug_fenced_put_commit_only for get_item");
    let mut get_resolve_key: Item = BTreeMap::new();
    get_resolve_key.insert("pk".to_string(), AttributeValue::S("s_get_resolve".into()));
    assert_eq!(
        ctx.engine
            .get_item(&key_info, &get_resolve_key)
            .await
            .unwrap()
            .as_ref()
            .and_then(|i| i.get("val")),
        Some(&AttributeValue::S("via_get_item".into())),
        "GetItem must resolve COMMITTED coordinator intent via read_row_resolving_committed_intents (#40)"
    );
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
    ctx.engine
        .create_ttl_index(&ctx.account_id, &table_name, "expires_at")
        .await
        .unwrap();

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
        extenddb_storage_bigtable::BigtableCatalogStore::new(ctx.client.clone(), None);
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

    let target = bootstrapper.init_target();
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
            .any(|t| t.ends_with("__extenddb_data_ledger__")),
        "M0001DataBaseline must provision __extenddb_data_ledger__ on data_instance_id"
    );
    assert!(
        data_tables_initial
            .iter()
            .any(|t| t.ends_with("__extenddb_txn_log__")),
        "M0001DataBaseline must provision __extenddb_txn_log__ on data_instance_id"
    );
    assert!(
        data_tables_initial
            .iter()
            .any(|t| t.ends_with("__extenddb_ttl_index__")),
        "M0001DataBaseline must provision __extenddb_ttl_index__ on data_instance_id"
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

    // 3. GSI Build Worker (`CREATING`-only) Repair & OCC Version Stamping:
    //    Delete the GSI shadow row directly on `shadow_table`. While `by_gpk` is
    //    `ACTIVE`, `gsi_worker::sweep_once` skips it (#25); once marked `CREATING`,
    //    `gsi_worker::sweep_once` repairs the shadow row stamped at `(v2 * 1_000) as i64`
    //    and promotes `by_gpk` back to `ACTIVE`.
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
    extenddb_storage_bigtable::gsi_worker::sweep_once(&ctx.engine)
        .await
        .expect("gsi_worker skips ACTIVE index");
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
    assert!(
        missing_gsi.is_empty(),
        "gsi_worker must not scan ACTIVE indexes (#25)"
    );

    let cat_for_gsi =
        extenddb_storage_bigtable::catalog::Catalog::new(ctx.engine.catalog_client_ref());
    let tbl_rk_aba =
        extenddb_storage_bigtable::catalog::keys::table_meta(&ctx.account_id, &table_name);
    let mut raw_aba_tbl = cat_for_gsi.get(&tbl_rk_aba).await.unwrap().unwrap();
    let mut raw_aba_desc: extenddb_core::types::TableDescription =
        serde_json::from_value(raw_aba_tbl["description"].clone()).unwrap();
    raw_aba_desc.global_secondary_indexes.as_mut().unwrap()[0].index_status =
        "CREATING".to_string();
    raw_aba_tbl["description"] = serde_json::to_value(&raw_aba_desc).unwrap();
    cat_for_gsi.put(&tbl_rk_aba, &raw_aba_tbl).await.unwrap();

    extenddb_storage_bigtable::gsi_worker::sweep_once(&ctx.engine)
        .await
        .expect("gsi_worker repair sweep on CREATING index");

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
                    open_nonce: None,
                    participants: Some(parts_w),
                    mutations: Some(rmuts),
                    stream_records: None,
                    ttl_mutations: None,
                    token_meta: None,
                    base_guard: None,
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
        open_nonce: None,
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
            created_at_ms: None,
        }]),
        ttl_mutations: None,
        token_meta: None,
        base_guard: None,
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
    ctx.engine
        .create_ttl_index(&ctx.account_id, &table_name, "ttl")
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
    let mut raw_tbl_orphan = cat.get(&tbl_rk).await.unwrap().unwrap();
    let mut raw_desc_orphan: extenddb_core::types::TableDescription =
        serde_json::from_value(raw_tbl_orphan["description"].clone()).unwrap();
    raw_desc_orphan.global_secondary_indexes.as_mut().unwrap()[0].index_status =
        "CREATING".to_string();
    raw_tbl_orphan["description"] = serde_json::to_value(&raw_desc_orphan).unwrap();
    cat.put(&tbl_rk, &raw_tbl_orphan).await.unwrap();
    extenddb_storage_bigtable::gsi_worker::sweep_once(&fresh_eng)
        .await
        .expect("gsi_worker::sweep_once cleans orphaned shadow row during CREATING build");
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
    let cat_store = Arc::new(BigtableCatalogStore::new(ctx.client.clone(), None));
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

// -----------------------------------------------------------------------------
// Test 21: Multi-Attribute GSI Keys, Base SK Prefix Bounds, LSI Projections,
//          TWI Key-Fault Precedence, Update Item-Size Limits & DDL Defaults
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_conformance_multipart_gsi_base_sk_bounds_lsi_projection_billing_size_and_txn_precedence()
 {
    use extenddb_core::types::{
        CreateGsiAction, GlobalSecondaryIndexUpdate, LsiInput, ProvisionedThroughput,
        UpdateTableInput, WarmThroughput,
    };
    use extenddb_storage::management_store::{ManagementStore, OpError, SettingsStore};
    use extenddb_storage::secret_keys::{CATALOG_KEY_SETTING, SecretKeys};
    use extenddb_storage_bigtable::BigtableCatalogStore;

    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    // 1. SettingsStore exposes `catalog_version`, IAM access keys use `AKIAEXTENDDB`
    //    / `extenddb` prefixes, and deleting an account with tables returns the
    //    reference HasDependents message.
    let cat_store = BigtableCatalogStore::new(ctx.client.clone(), None);
    let cat_ver = cat_store
        .get_setting("catalog_version")
        .await
        .expect("get catalog_version");
    assert_eq!(cat_ver, Some(MigrationId::LATEST.number().to_string()));
    let all_settings = cat_store.list_settings().await.expect("list_settings");
    assert!(all_settings.iter().any(|(k, v)| {
        k == "catalog_version" && v == &MigrationId::LATEST.number().to_string()
    }));

    let enc_b64 = cat_store
        .get_setting(CATALOG_KEY_SETTING)
        .await
        .unwrap()
        .expect("encryption_key");
    let secret_keys = Arc::new(SecretKeys::catalog_key(&enc_b64).unwrap());
    let mgmt = BigtableCatalogStore::new(ctx.client.clone(), Some(secret_keys));
    mgmt.create_account(&ctx.account_id, "conf-acct")
        .await
        .unwrap();
    mgmt.create_user(&ctx.account_id, "ak_user", Some("hash"))
        .await
        .unwrap();
    let ak = mgmt
        .create_access_key(&ctx.account_id, "ak_user")
        .await
        .unwrap();
    assert!(
        ak.access_key_id.starts_with("AKIAEXTENDDB") && ak.access_key_id.len() == 20,
        "expected 20-char AKIAEXTENDDB key, got {}",
        ak.access_key_id
    );
    assert!(
        ak.secret_access_key.starts_with("extenddb") && ak.secret_access_key.len() == 40,
        "expected 40-char extenddb secret key, got len={}",
        ak.secret_access_key.len()
    );

    // 2. CreateTable with billing_mode=None defaults to PROVISIONED (omits
    //    BillingModeSummary) and honors `control_plane_delay_seconds`.
    cat_store
        .set_setting("control_plane_delay_seconds", "10.0")
        .await
        .unwrap();
    let table_name = unique_name("tbl_multipart_conf");
    let created_desc = ctx
        .engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: table_name.clone(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".into(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".into(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "lsi_sk".into(),
                        attribute_type: ScalarAttributeType::N,
                    },
                    AttributeDefinition {
                        attribute_name: "h1".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "h2".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "r1".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "r2".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ],
                provisioned_throughput: Some(ProvisionedThroughput {
                    read_capacity_units: 5,
                    write_capacity_units: 5,
                }),
                warm_throughput: Some(WarmThroughput {
                    read_units_per_second: Some(12_000),
                    write_units_per_second: Some(4_000),
                }),
                local_secondary_indexes: Some(vec![LsiInput {
                    index_name: "lsi_keys_only".into(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "pk".into(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "lsi_sk".into(),
                            key_type: KeyType::Range,
                        },
                    ],
                    projection: Projection {
                        projection_type: ProjectionType::KeysOnly,
                        non_key_attributes: None,
                    },
                }]),
                global_secondary_indexes: Some(vec![GsiInput {
                    index_name: "multi_gsi".into(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "h1".into(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "h2".into(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "r1".into(),
                            key_type: KeyType::Range,
                        },
                        KeySchemaElement {
                            attribute_name: "r2".into(),
                            key_type: KeyType::Range,
                        },
                    ],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: Some(ProvisionedThroughput {
                        read_capacity_units: 5,
                        write_capacity_units: 5,
                    }),
                    warm_throughput: None,
                }]),
                billing_mode: None,
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .expect("create multi-attribute GSI + KEYS_ONLY LSI table");
    assert!(
        created_desc.billing_mode_summary.is_none(),
        "default PROVISIONED table must omit BillingModeSummary"
    );
    assert!(
        created_desc
            .warm_throughput_record
            .as_ref()
            .expect("warm throughput record")
            .updating_until
            > extenddb_core::types::epoch_seconds_now(),
        "WarmThroughput must remain UPDATING during control_plane_delay_seconds window"
    );

    let del_acct_err = mgmt
        .delete_account(&ctx.account_id)
        .await
        .expect_err("delete_account with active table must fail");
    assert!(matches!(
        del_acct_err,
        OpError::HasDependents(ref m)
            if m == "Cannot delete account with existing tables. Delete all tables first."
    ));

    // 3. Conflicting AttributeDefinition redeclaration on UpdateTable keeps stored type.
    let redecl_desc = ctx
        .engine
        .update_table(
            &ctx.account_id,
            UpdateTableInput {
                table_name: table_name.clone(),
                billing_mode: Some(BillingMode::PayPerRequest),
                table_throughput_mode: None,
                provisioned_throughput: None,
                deletion_protection_enabled: None,
                global_secondary_index_updates: Some(vec![GlobalSecondaryIndexUpdate {
                    create: Some(CreateGsiAction {
                        index_name: "extra_gsi".into(),
                        key_schema: vec![KeySchemaElement {
                            attribute_name: "extra_attr".into(),
                            key_type: KeyType::Hash,
                        }],
                        projection: Projection {
                            projection_type: ProjectionType::All,
                            non_key_attributes: None,
                        },
                        provisioned_throughput: None,
                        warm_throughput: None,
                    }),
                    update: None,
                    delete: None,
                }]),
                attribute_definitions: Some(vec![
                    AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::N,
                    },
                    AttributeDefinition {
                        attribute_name: "extra_attr".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ]),
                stream_specification: None,
                table_class: None,
                on_demand_throughput: None,
                vector_index_updates: None,
                warm_throughput: None,
                sse_specification: None,
            },
        )
        .await
        .expect("conflicting AttributeDefinition redeclaration must succeed");
    assert_eq!(
        redecl_desc
            .attribute_definitions
            .iter()
            .find(|a| a.attribute_name == "pk")
            .map(|a| a.attribute_type),
        Some(ScalarAttributeType::S)
    );
    let gsis = redecl_desc
        .global_secondary_indexes
        .as_ref()
        .expect("GSIs on PAY_PER_REQUEST table");
    assert_eq!(gsis.len(), 2);
    for g in gsis {
        let pt = g
            .provisioned_throughput
            .as_ref()
            .expect("PAY_PER_REQUEST GSI must still include zeroed ProvisionedThroughput");
        assert_eq!(pt.read_capacity_units, 0);
        assert_eq!(pt.write_capacity_units, 0);
    }

    let key_info = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let empty_maps = ExpressionMaps::default();

    // 4. Base-table SK prefix isolation ("@", "A", "A\0", "AB", "B") + multi-attribute GSI + LSI.
    for (sk, r1, r2, lsi_n) in [
        ("@", "F", "live", "40"),
        ("A", "T", "live", "30"),
        ("A\x00", "T", "draft", "10"),
        ("AB", "F", "live", "20"),
        ("B", "T", "zzz", "50"),
    ] {
        let mut it = s_item(&[
            ("pk", "p1"),
            ("sk", sk),
            ("h1", "tenantA"),
            ("h2", "us"),
            ("r1", r1),
            ("r2", r2),
            ("secret_payload", "should_not_project_in_lsi"),
        ]);
        it.insert("lsi_sk".into(), AttributeValue::N(lsi_n.into()));
        ctx.engine
            .put_item(&key_info, it, false, None, &empty_maps, None)
            .await
            .unwrap();
    }
    ctx.engine
        .put_item(
            &key_info,
            s_item(&[
                ("pk", "p1"),
                ("sk", "s_sparse"),
                ("h1", "tenantA"),
                ("h2", "us"),
                ("r1", "T"),
            ]),
            false,
            None,
            &empty_maps,
            None,
        )
        .await
        .unwrap();

    let query_base_sks = |expr: &str, forward: bool| {
        let engine = ctx.engine.clone();
        let ki = key_info.clone();
        let expr = expr.to_owned();
        async move {
            let kc = parse_kc(&expr);
            let mut m = ExpressionMaps::default();
            m.values.insert("p".into(), AttributeValue::S("p1".into()));
            m.values.insert("v".into(), AttributeValue::S("A".into()));
            m.values.insert("lo".into(), AttributeValue::S("@".into()));
            let (items, _) = engine
                .query(&ki, &kc, &m, forward, None, None, None)
                .await
                .unwrap();
            items
                .into_iter()
                .map(|it| match it.get("sk") {
                    Some(AttributeValue::S(s)) => s.clone(),
                    other => panic!("unexpected sk {other:?}"),
                })
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(query_base_sks("pk = :p AND sk = :v", true).await, vec!["A"]);
    assert_eq!(
        query_base_sks("pk = :p AND sk <= :v", true).await,
        vec!["@", "A"]
    );
    assert_eq!(
        query_base_sks("pk = :p AND sk BETWEEN :lo AND :v", true).await,
        vec!["@", "A"]
    );
    assert_eq!(
        query_base_sks("pk = :p AND sk > :v", true).await,
        vec!["A\x00", "AB", "B", "s_sparse"]
    );
    assert_eq!(
        query_base_sks("pk = :p AND begins_with(sk, :v)", false).await,
        vec!["AB", "A\x00", "A"]
    );

    // Overlong (> 4096-byte) SK operand in KeyConditionExpression must clamp cleanly on cbtemulator.
    let overlong_kc = parse_kc("pk = :p AND sk < :overlong");
    let mut overlong_maps = ExpressionMaps::default();
    overlong_maps
        .values
        .insert("p".into(), AttributeValue::S("p1".into()));
    overlong_maps
        .values
        .insert("overlong".into(), AttributeValue::S("z".repeat(5000)));
    let (overlong_items, _) = ctx
        .engine
        .query(
            &key_info,
            &overlong_kc,
            &overlong_maps,
            true,
            None,
            None,
            None,
        )
        .await
        .expect("overlong SK bound must clamp cleanly without Bigtable 4KB error");
    assert_eq!(overlong_items.len(), 6);

    // Out-of-partition ExclusiveStartKey on direct DataEngine::query must fail Validation.
    let wrong_pk_esk = s_item(&[("pk", "p0"), ("sk", "A")]);
    let wrong_pk_err = ctx
        .engine
        .query(
            &key_info,
            &overlong_kc,
            &overlong_maps,
            true,
            None,
            Some(&wrong_pk_esk),
            None,
        )
        .await;
    assert!(matches!(wrong_pk_err, Err(StorageError::Validation(_))));

    // Multi-HASH + multi-RANGE GSI query with aliased free-order clauses (without pre-resolving).
    let kc = parse_kc("#h2 = :h2 AND r1 = :r1 AND #h1 = :h1 AND r2 < :r2");
    let mut qmaps = ExpressionMaps::default();
    qmaps.names.insert("h1".into(), "h1".into());
    qmaps.names.insert("h2".into(), "h2".into());
    qmaps
        .values
        .insert("h1".into(), AttributeValue::S("tenantA".into()));
    qmaps
        .values
        .insert("h2".into(), AttributeValue::S("us".into()));
    qmaps
        .values
        .insert("r1".into(), AttributeValue::S("T".into()));
    qmaps
        .values
        .insert("r2".into(), AttributeValue::S("live".into()));
    let (gsi_items, _) = ctx
        .engine
        .query(&key_info, &kc, &qmaps, true, None, None, Some("multi_gsi"))
        .await
        .unwrap();
    assert_eq!(gsi_items.len(), 1);
    assert_eq!(
        gsi_items[0].get("sk"),
        Some(&AttributeValue::S("A\x00".into()))
    );

    // Invalid ExclusiveStartKey missing GSI key attributes or carrying extraneous attrs on GSI Query & Scan must fail Validation.
    let bad_gsi_esk = s_item(&[("pk", "p1"), ("sk", "A")]);
    let bad_esk_err = ctx
        .engine
        .query(
            &key_info,
            &kc,
            &qmaps,
            true,
            None,
            Some(&bad_gsi_esk),
            Some("multi_gsi"),
        )
        .await;
    assert!(matches!(bad_esk_err, Err(StorageError::Validation(_))));
    let mut extra_gsi_esk = s_item(&[
        ("pk", "p1"),
        ("sk", "A"),
        ("h1", "tenantA"),
        ("h2", "us"),
        ("r1", "T"),
        ("r2", "live"),
        ("unexpected", "attr"),
    ]);
    let bad_scan_esk_err = ctx
        .engine
        .scan(
            &key_info,
            None,
            Some(&extra_gsi_esk),
            None,
            None,
            Some("multi_gsi"),
        )
        .await;
    assert!(matches!(bad_scan_esk_err, Err(StorageError::Validation(_))));
    extra_gsi_esk.remove("unexpected");
    let (_, _) = ctx
        .engine
        .scan(
            &key_info,
            None,
            Some(&extra_gsi_esk),
            None,
            None,
            Some("multi_gsi"),
        )
        .await
        .expect("valid GSI ExclusiveStartKey on Scan must succeed");

    // 5. KEYS_ONLY LSI projection on paginated Query and Scan strips non-key attributes.
    let lsi_kc = parse_kc("pk = :p");
    let mut lsi_maps = ExpressionMaps::default();
    lsi_maps
        .values
        .insert("p".into(), AttributeValue::S("p1".into()));
    let (lsi_page1, lsi_lek1) = ctx
        .engine
        .query(
            &key_info,
            &lsi_kc,
            &lsi_maps,
            true,
            Some(2),
            None,
            Some("lsi_keys_only"),
        )
        .await
        .unwrap();
    assert_eq!(lsi_page1.len(), 2);
    assert!(lsi_lek1.is_some());
    let (lsi_page2, _) = ctx
        .engine
        .query(
            &key_info,
            &lsi_kc,
            &lsi_maps,
            true,
            Some(10),
            lsi_lek1.as_ref(),
            Some("lsi_keys_only"),
        )
        .await
        .unwrap();
    assert_eq!(lsi_page2.len(), 3);
    for it in lsi_page1.iter().chain(lsi_page2.iter()) {
        assert!(!it.contains_key("secret_payload"));
    }
    let (lsi_s_items, _) = ctx
        .engine
        .scan(&key_info, None, None, None, None, Some("lsi_keys_only"))
        .await
        .unwrap();
    assert_eq!(lsi_s_items.len(), 5);
    for it in &lsi_s_items {
        assert!(!it.contains_key("secret_payload"));
    }

    // 6. TransactWriteItems key-fault precedence & empty index key handling.
    let cond_never = parse_cond("attribute_not_exists(pk)");
    let valid_k0 = s_item(&[("pk", "p1"), ("sk", "A")]);
    let mut bad_k1 = Item::new();
    bad_k1.insert("pk".into(), AttributeValue::N("999".into()));
    bad_k1.insert("sk".into(), AttributeValue::S("AB".into()));
    let prec_res = ctx
        .engine
        .transact_write_items(
            &[
                TransactWriteOp::ConditionCheck {
                    key_info: &key_info,
                    key: &valid_k0,
                    condition: &cond_never,
                    maps: &empty_maps,
                    return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                },
                TransactWriteOp::Delete {
                    key_info: &key_info,
                    key: &bad_k1,
                    condition: None,
                    maps: &empty_maps,
                    return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                    stream: None,
                },
            ],
            None,
        )
        .await;
    match prec_res {
        Err(StorageError::TransactionCanceled(reasons)) => {
            assert_eq!(reasons.len(), 2);
            assert_eq!(reasons[0].code, "None");
            assert_eq!(reasons[1].code, "ValidationError");
        }
        other => panic!("expected TransactionCanceled with ValidationError, got {other:?}"),
    }

    let empty_idx_item = s_item(&[("pk", "p1"), ("sk", "s_empty"), ("h1", "")]);
    let empty_only_err = ctx
        .engine
        .transact_write_items(
            &[TransactWriteOp::Put {
                key_info: &key_info,
                item: &empty_idx_item,
                condition: None,
                maps: &empty_maps,
                return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                stream: None,
            }],
            None,
        )
        .await;
    assert!(matches!(empty_only_err, Err(StorageError::Validation(_))));

    // 7. max_item_size_bytes enforced on both UpdateItem and TransactWriteItems Update.
    let capped_engine = BigtableEngine::with_options(
        ctx.client.clone(),
        ctx.client.clone(),
        60,
        "us-east-1".into(),
        128,
    );
    let big_acts = parse_upd("SET big = :b");
    let mut big_maps = ExpressionMaps::default();
    big_maps
        .values
        .insert("b".into(), AttributeValue::S("x".repeat(256)));
    let upd_size_err = capped_engine
        .update_item(
            &key_info, &valid_k0, &big_acts, false, false, None, &big_maps, None,
        )
        .await;
    assert!(matches!(upd_size_err, Err(StorageError::Validation(_))));
    let twi_size_err = capped_engine
        .transact_write_items(
            &[TransactWriteOp::Update {
                key_info: &key_info,
                key: &valid_k0,
                actions: &big_acts,
                condition: None,
                maps: &big_maps,
                return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                stream: None,
            }],
            None,
        )
        .await;
    match twi_size_err {
        Err(StorageError::TransactionCanceled(reasons)) => {
            assert_eq!(reasons[0].code, "ValidationError");
        }
        other => panic!("expected TransactionCanceled ValidationError, got {other:?}"),
    }
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

// -----------------------------------------------------------------------------
// Test 22: Exact Five-Item Base Sort-Key Range Bounds (`[a, ab, b, ba, c]`, #4)
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_exact_five_item_sort_key_bounds() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_five_sk");
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: table_name.clone(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".into(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".into(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ],
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

    for sk in ["a", "ab", "b", "ba", "c"] {
        ctx.engine
            .put_item(
                &key_info,
                s_item(&[("pk", "p"), ("sk", sk)]),
                false,
                None,
                &empty_maps,
                None,
            )
            .await
            .unwrap();
    }
    ctx.engine
        .put_item(
            &key_info,
            s_item(&[("pk", "other_p"), ("sk", "ab")]),
            false,
            None,
            &empty_maps,
            None,
        )
        .await
        .unwrap();

    let run_q = |expr: &str, forward: bool| {
        let eng = ctx.engine.clone();
        let ki = key_info.clone();
        let expr = expr.to_owned();
        async move {
            let kc = parse_kc(&expr);
            let mut m = ExpressionMaps::default();
            m.values.insert("p".into(), AttributeValue::S("p".into()));
            m.values.insert("a".into(), AttributeValue::S("a".into()));
            m.values.insert("b".into(), AttributeValue::S("b".into()));
            let (items, lek) = eng
                .query(&ki, &kc, &m, forward, None, None, None)
                .await
                .unwrap();
            assert!(lek.is_none());
            items
                .into_iter()
                .map(|it| match it.get("sk") {
                    Some(AttributeValue::S(s)) => s.clone(),
                    other => panic!("expected S sk, got {other:?}"),
                })
                .collect::<Vec<_>>()
        }
    };

    assert_eq!(run_q("pk = :p AND sk = :a", true).await, vec!["a"]);
    assert_eq!(run_q("pk = :p AND sk = :a", false).await, vec!["a"]);
    assert_eq!(
        run_q("pk = :p AND sk <= :b", true).await,
        vec!["a", "ab", "b"]
    );
    assert_eq!(
        run_q("pk = :p AND sk <= :b", false).await,
        vec!["b", "ab", "a"]
    );
    assert_eq!(run_q("pk = :p AND sk < :b", true).await, vec!["a", "ab"]);
    assert_eq!(run_q("pk = :p AND sk > :b", true).await, vec!["ba", "c"]);
    assert_eq!(run_q("pk = :p AND sk > :b", false).await, vec!["c", "ba"]);
    assert_eq!(
        run_q("pk = :p AND sk >= :b", true).await,
        vec!["b", "ba", "c"]
    );
    assert_eq!(
        run_q("pk = :p AND sk BETWEEN :a AND :b", true).await,
        vec!["a", "ab", "b"]
    );
    assert_eq!(
        run_q("pk = :p AND sk BETWEEN :a AND :b", false).await,
        vec!["b", "ab", "a"]
    );
    assert_eq!(
        run_q("pk = :p AND begins_with(sk, :a)", true).await,
        vec!["a", "ab"]
    );
    assert_eq!(
        run_q("pk = :p AND begins_with(sk, :b)", true).await,
        vec!["b", "ba"]
    );
}

// -----------------------------------------------------------------------------
// Test 23: Digest-Bounded Large GSI Keys, Pre-Commit Validation & Same-Schema GSI (#5, #6)
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_gsi_large_keys_digest_and_same_key_schema_index_query() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    // Part A (#5): 2,048-byte pk + 1,024-byte sk + 2,048-byte gsi_pk + 1,024-byte gsi_sk
    let big_tbl = unique_name("tbl_big_gsi");
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: big_tbl.clone(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".into(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".into(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "gpk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "gsk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ],
                global_secondary_indexes: Some(vec![GsiInput {
                    index_name: "BigKeyGsi".into(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "gpk".into(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "gsk".into(),
                            key_type: KeyType::Range,
                        },
                    ],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: None,
                    warm_throughput: None,
                }]),
                billing_mode: Some(BillingMode::PayPerRequest),
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    let big_ki = ctx
        .engine
        .table_key_info(&ctx.account_id, &big_tbl)
        .await
        .unwrap();
    let empty_maps = ExpressionMaps::default();

    let pk_2048 = "p".repeat(2048);
    let sk_1024 = "s".repeat(1024);
    let gpk_2048 = "g".repeat(2048);
    let gsk_1024 = "r".repeat(1024);
    let big_item = s_item(&[
        ("pk", &pk_2048),
        ("sk", &sk_1024),
        ("gpk", &gpk_2048),
        ("gsk", &gsk_1024),
        ("payload", "ok"),
    ]);
    ctx.engine
        .put_item(&big_ki, big_item, false, None, &empty_maps, None)
        .await
        .expect("max-length 6,144-byte combined keys must fit in digest-bounded GSI shadow key");

    let gsi_kc = parse_kc("gpk = :g AND gsk = :r");
    let mut gsi_maps = ExpressionMaps::default();
    gsi_maps
        .values
        .insert("g".into(), AttributeValue::S(gpk_2048.clone()));
    gsi_maps
        .values
        .insert("r".into(), AttributeValue::S(gsk_1024.clone()));
    let (found, _) = ctx
        .engine
        .query(
            &big_ki,
            &gsi_kc,
            &gsi_maps,
            true,
            None,
            None,
            Some("BigKeyGsi"),
        )
        .await
        .expect("query BigKeyGsi");
    assert_eq!(found.len(), 1);
    assert_eq!(
        found[0].get("pk"),
        Some(&AttributeValue::S(pk_2048.clone()))
    );

    let base_key = s_item(&[("pk", &pk_2048), ("sk", &sk_1024)]);
    ctx.engine
        .delete_item(&big_ki, &base_key, false, None, &empty_maps, None)
        .await
        .expect("delete max-length GSI item");
    let (after_del, _) = ctx
        .engine
        .query(
            &big_ki,
            &gsi_kc,
            &gsi_maps,
            true,
            None,
            None,
            Some("BigKeyGsi"),
        )
        .await
        .unwrap();
    assert!(after_del.is_empty());

    // Pre-commit shadow key validation (#5): 4x 1,024-byte NUL-escaped RANGE keys exceed 4,096 bytes
    // and must fail BEFORE base-table `put_cas` commits anything!
    let multi_r_tbl = unique_name("tbl_4range_precheck");
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: multi_r_tbl.clone(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".into(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "gh".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "r1".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "r2".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "r3".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "r4".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ],
                global_secondary_indexes: Some(vec![GsiInput {
                    index_name: "FourRangeGsi".into(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "gh".into(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "r1".into(),
                            key_type: KeyType::Range,
                        },
                        KeySchemaElement {
                            attribute_name: "r2".into(),
                            key_type: KeyType::Range,
                        },
                        KeySchemaElement {
                            attribute_name: "r3".into(),
                            key_type: KeyType::Range,
                        },
                        KeySchemaElement {
                            attribute_name: "r4".into(),
                            key_type: KeyType::Range,
                        },
                    ],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: None,
                    warm_throughput: None,
                }]),
                billing_mode: Some(BillingMode::PayPerRequest),
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    let mr_ki = ctx
        .engine
        .table_key_info(&ctx.account_id, &multi_r_tbl)
        .await
        .unwrap();
    let nul_1024 = "\0".repeat(1024);
    let overlong_shadow_item = s_item(&[
        ("pk", "must_not_store"),
        ("gh", "h"),
        ("r1", &nul_1024),
        ("r2", &nul_1024),
        ("r3", &nul_1024),
        ("r4", &nul_1024),
    ]);
    let pre_err = ctx
        .engine
        .put_item(&mr_ki, overlong_shadow_item, false, None, &empty_maps, None)
        .await;
    assert!(matches!(pre_err, Err(StorageError::Validation(_))));
    assert!(
        ctx.engine
            .get_item(&mr_ki, &s_item(&[("pk", "must_not_store")]))
            .await
            .unwrap()
            .is_none(),
        "shadow key validation failure must happen BEFORE base-table put_cas (#5)"
    );

    // Part B (#6): GSI whose key schema (pk S, sk N) equals the base table's key schema!
    let mirror_tbl = unique_name("tbl_mirror_gsi");
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: mirror_tbl.clone(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".into(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".into(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".into(),
                        attribute_type: ScalarAttributeType::N,
                    },
                ],
                global_secondary_indexes: Some(vec![GsiInput {
                    index_name: "MirrorIdx".into(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "pk".into(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "sk".into(),
                            key_type: KeyType::Range,
                        },
                    ],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: None,
                    warm_throughput: None,
                }]),
                billing_mode: Some(BillingMode::PayPerRequest),
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    let mirror_ki = ctx
        .engine
        .table_key_info(&ctx.account_id, &mirror_tbl)
        .await
        .unwrap();
    for sk_n in ["5", "10"] {
        let mut it = s_item(&[("pk", "a")]);
        it.insert("sk".into(), AttributeValue::N(sk_n.into()));
        ctx.engine
            .put_item(&mirror_ki, it, false, None, &empty_maps, None)
            .await
            .unwrap();
    }
    let mirror_kc = parse_kc("pk = :p AND sk = :s");
    let mut mirror_maps = ExpressionMaps::default();
    mirror_maps
        .values
        .insert("p".into(), AttributeValue::S("a".into()));
    mirror_maps
        .values
        .insert("s".into(), AttributeValue::N("5".into()));
    let (mirror_res, _) = ctx
        .engine
        .query(
            &mirror_ki,
            &mirror_kc,
            &mirror_maps,
            true,
            None,
            None,
            Some("MirrorIdx"),
        )
        .await
        .expect("query GSI with identical key schema as base table (#6)");
    assert_eq!(mirror_res.len(), 1);
    assert_eq!(
        mirror_res[0].get("sk"),
        Some(&AttributeValue::N("5".into()))
    );
}

// -----------------------------------------------------------------------------
// Test 24: TTL Pre-Enable Backfill (`ENABLING -> ENABLED`) & Preservation While Off (#8)
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_ttl_backfill_preexisting_items_and_disabled_preservation() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let table_name = unique_name("tbl_ttl_backfill");
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: table_name.clone(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".into(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "pk".into(),
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

    // 1. Write expired & future items BEFORE UpdateTimeToLive(Enabled=true)!
    let mut pre_exp = s_item(&[("pk", "written_before_ttl_enable")]);
    pre_exp.insert("exp".into(), AttributeValue::N("1000".into()));
    ctx.engine
        .put_item(&key_info, pre_exp, false, None, &empty_maps, None)
        .await
        .unwrap();
    let mut pre_live = s_item(&[("pk", "future_before_ttl_enable")]);
    pre_live.insert("exp".into(), AttributeValue::N("9999999999".into()));
    ctx.engine
        .put_item(&key_info, pre_live, false, None, &empty_maps, None)
        .await
        .unwrap();

    ctx.engine
        .update_ttl(&ctx.account_id, &table_name, "exp", true)
        .await
        .unwrap();
    assert_eq!(
        ctx.engine
            .describe_ttl(&ctx.account_id, &table_name)
            .await
            .unwrap()
            .time_to_live_status,
        TimeToLiveStatus::Enabling,
        "describe_ttl must report ENABLING until backfill finishes"
    );

    ctx.engine
        .create_ttl_index(&ctx.account_id, &table_name, "exp")
        .await
        .expect("backfill pre-existing TTL items");
    assert_eq!(
        ctx.engine
            .describe_ttl(&ctx.account_id, &table_name)
            .await
            .unwrap()
            .time_to_live_status,
        TimeToLiveStatus::Enabled
    );

    sweep_once(&ctx.engine).await.unwrap();
    assert!(
        ctx.engine
            .get_item(&key_info, &s_item(&[("pk", "written_before_ttl_enable")]))
            .await
            .unwrap()
            .is_none(),
        "item written before TTL was enabled must be indexed by create_ttl_index and deleted (#8)"
    );
    assert!(
        ctx.engine
            .get_item(&key_info, &s_item(&[("pk", "future_before_ttl_enable")]))
            .await
            .unwrap()
            .is_some()
    );

    // 2. Disable TTL, seed an item + due index entry while TTL is DISABLED, verify
    //    `ttl_worker::sweep_once` does NOT delete the index entry while off, and
    //    verify re-enabling TTL deletes the item!
    ctx.engine
        .update_ttl(&ctx.account_id, &table_name, "exp", false)
        .await
        .unwrap();
    ctx.engine
        .drop_ttl_index(&ctx.account_id, &table_name, "exp")
        .await
        .unwrap();

    let mut off_exp = s_item(&[("pk", "due_while_ttl_off")]);
    off_exp.insert("exp".into(), AttributeValue::N("1000".into()));
    ctx.engine
        .put_item(&key_info, off_exp, false, None, &empty_maps, None)
        .await
        .unwrap();
    let enc_off = extenddb_storage_bigtable::data::encoding::row_key::encode_key(
        &s_item(&[("pk", "due_while_ttl_off")]),
        &key_info.key_schema,
    )
    .unwrap();
    let off_ttl_rk = extenddb_storage_bigtable::data::encoding::ttl_key::encode_ttl_key(
        &ctx.account_id,
        &table_name,
        &enc_off,
        1000,
    );
    ctx.client
        .data()
        .mutate_row(MutateRowRequest {
            table_name: ctx.client.full_table_name(
                extenddb_storage_bigtable::data::encoding::ttl_key::TTL_INDEX_TABLE,
            ),
            row_key: off_ttl_rk.clone(),
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
                    family_name: "d".into(),
                    column_qualifier: Vec::new(),
                    timestamp_micros: extenddb_storage_bigtable::data::item_ops::now_millis_micros(
                    ),
                    value: Vec::new(),
                })),
            }],
            ..MutateRowRequest::default()
        })
        .await
        .unwrap();

    sweep_once(&ctx.engine).await.unwrap();
    let preserved_rows = ctx
        .client
        .data()
        .read_rows(ReadRowsRequest {
            table_name: ctx.client.full_table_name(
                extenddb_storage_bigtable::data::encoding::ttl_key::TTL_INDEX_TABLE,
            ),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![off_ttl_rk],
                row_ranges: vec![],
            }),
            ..ReadRowsRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(
        preserved_rows.len(),
        1,
        "ttl_worker must NOT delete due index entries while TTL is DISABLED (#8)"
    );

    ctx.engine
        .update_ttl(&ctx.account_id, &table_name, "exp", true)
        .await
        .unwrap();
    // Even without calling create_ttl_index manually, ttl_worker::sweep_once resumes
    // ENABLING backfills, promotes to ENABLED, and evicts due items!
    sweep_once(&ctx.engine).await.unwrap();
    assert!(
        ctx.engine
            .get_item(&key_info, &s_item(&[("pk", "due_while_ttl_off")]))
            .await
            .unwrap()
            .is_none(),
        "re-enabled TTL must evict item that came due while TTL was off (#8)"
    );
}

// -----------------------------------------------------------------------------
// Test 25: Deleted-Table Roll-Forward (#10), Post-Commit Side-Effect Replay (#7/#9),
//          Closed-Shard Stream Guard (#12) & Version-Only Tombstone GC (#24)
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_deleted_table_roll_forward_side_effects_closed_shard_and_tombstone_gc() {
    use extenddb_core::types::{DeleteTableInput, UpdateTableInput};

    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let tbl_a = unique_name("tbl_rf_keep");
    let tbl_b = unique_name("tbl_rf_drop");
    for (name, with_gsi_stream) in [(&tbl_a, true), (&tbl_b, false)] {
        ctx.engine
            .create_table(
                &ctx.account_id,
                CreateTableInput {
                    table_name: name.clone(),
                    key_schema: vec![KeySchemaElement {
                        attribute_name: "pk".into(),
                        key_type: KeyType::Hash,
                    }],
                    attribute_definitions: if with_gsi_stream {
                        vec![
                            AttributeDefinition {
                                attribute_name: "pk".into(),
                                attribute_type: ScalarAttributeType::S,
                            },
                            AttributeDefinition {
                                attribute_name: "gpk".into(),
                                attribute_type: ScalarAttributeType::S,
                            },
                        ]
                    } else {
                        vec![AttributeDefinition {
                            attribute_name: "pk".into(),
                            attribute_type: ScalarAttributeType::S,
                        }]
                    },
                    global_secondary_indexes: with_gsi_stream.then(|| {
                        vec![GsiInput {
                            index_name: "ByGpk".into(),
                            key_schema: vec![KeySchemaElement {
                                attribute_name: "gpk".into(),
                                key_type: KeyType::Hash,
                            }],
                            projection: Projection {
                                projection_type: ProjectionType::All,
                                non_key_attributes: None,
                            },
                            provisioned_throughput: None,
                            warm_throughput: None,
                        }]
                    }),
                    stream_specification: with_gsi_stream.then_some(StreamSpecification {
                        stream_enabled: true,
                        stream_view_type: Some(StreamViewType::NewAndOldImages),
                    }),
                    billing_mode: Some(BillingMode::PayPerRequest),
                    deletion_protection_enabled: Some(false),
                    ..CreateTableInput::default()
                },
            )
            .await
            .unwrap();
    }

    let ki_a = ctx
        .engine
        .table_key_info(&ctx.account_id, &tbl_a)
        .await
        .unwrap();
    let ki_b = ctx
        .engine
        .table_key_info(&ctx.account_id, &tbl_b)
        .await
        .unwrap();
    let dt_a = ctx.engine.data_table_for(&ki_a).await.unwrap();
    let dt_b = ctx.engine.data_table_for(&ki_b).await.unwrap();
    let empty_maps = ExpressionMaps::default();

    // 1. (#10): Open & commit a 2-table transaction across tbl_a and tbl_b, then
    //    drop tbl_b BEFORE roll_forward: sweeper must succeed, materialize tbl_a,
    //    clear tbl_a's intent, and drop the coordinator row!
    let coord = extenddb_storage_bigtable::transact::TxnCoordinator::new(
        ctx.engine.client_ref(),
        Duration::from_secs(60),
    );
    let item_a = s_item(&[("pk", "survives_a"), ("v", "from_2pc")]);
    let item_b = s_item(&[("pk", "dropped_b"), ("v", "gone")]);
    let part_a = extenddb_storage_bigtable::transact::ParticipantRow {
        data_table: dt_a.clone(),
        row_key: extenddb_storage_bigtable::data::encoding::row_key::encode_key(
            &item_a,
            &ki_a.key_schema,
        )
        .unwrap(),
    };
    let part_b = extenddb_storage_bigtable::transact::ParticipantRow {
        data_table: dt_b.clone(),
        row_key: extenddb_storage_bigtable::data::encoding::row_key::encode_key(
            &item_b,
            &ki_b.key_schema,
        )
        .unwrap(),
    };
    let tid_drop = extenddb_storage_bigtable::transact::TxnCoordinator::new_txn_id();
    let (_, st_drop) = coord
        .open(&tid_drop, &[part_a.clone(), part_b.clone()])
        .await
        .unwrap();
    assert!(coord.place_intent_once(&tid_drop, &part_a).await.unwrap());
    assert!(coord.place_intent_once(&tid_drop, &part_b).await.unwrap());
    let ver_drop = extenddb_storage_bigtable::data::item_ops::next_occ_version(None);
    coord
        .try_commit_full(
            &tid_drop,
            st_drop,
            &[
                extenddb_storage_bigtable::transact::ParticipantMutation {
                    participant: part_a.clone(),
                    payload: extenddb_storage_bigtable::transact::TxnOpPayload::Put {
                        item: item_a.clone(),
                    },
                    expected_version: None,
                    row_existed: false,
                    new_version: Some(ver_drop),
                },
                extenddb_storage_bigtable::transact::ParticipantMutation {
                    participant: part_b.clone(),
                    payload: extenddb_storage_bigtable::transact::TxnOpPayload::Put {
                        item: item_b.clone(),
                    },
                    expected_version: None,
                    row_existed: false,
                    new_version: Some(ver_drop),
                },
            ],
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

    ctx.engine
        .delete_table(
            &ctx.account_id,
            DeleteTableInput {
                table_name: tbl_b.clone(),
            },
        )
        .await
        .expect("delete tbl_b before roll_forward");

    let state_drop = coord.get_state(&tid_drop).await.unwrap().unwrap();
    ctx.engine
        .roll_forward(&tid_drop, &state_drop)
        .await
        .expect("roll_forward must treat NOT_FOUND on deleted tbl_b as completed (#10)");
    assert!(
        coord.get_state(&tid_drop).await.unwrap().is_none(),
        "coordinator row must be dropped after deleted-table roll_forward"
    );
    assert_eq!(
        ctx.engine
            .get_item(&ki_a, &s_item(&[("pk", "survives_a")]))
            .await
            .unwrap()
            .as_ref()
            .and_then(|i| i.get("v")),
        Some(&AttributeValue::S("from_2pc".into()))
    );

    // 2. (#7 & #9): Durable post-commit side-effect recovery (`write_committed_side_effects`)
    let gsi_shadow_tbl = extenddb_storage_bigtable::gsi::shadow_table_id(&dt_a, "ByGpk");
    let se_item = s_item(&[("pk", "se_row"), ("gpk", "se_grp")]);
    ctx.engine
        .put_item(&ki_a, se_item.clone(), false, None, &empty_maps, None)
        .await
        .unwrap();
    let gsi_schema = vec![KeySchemaElement {
        attribute_name: "gpk".into(),
        key_type: KeyType::Hash,
    }];
    let se_shadow_rk = extenddb_storage_bigtable::gsi::shadow_row_key_for_item(
        &se_item,
        &gsi_schema,
        &ki_a.key_schema,
    )
    .unwrap()
    .unwrap();
    // Simulate a failed shadow write wiped from shadow table, then enqueued via `post-*`:
    ctx.client
        .data()
        .mutate_row(MutateRowRequest {
            table_name: ctx.client.full_table_name(&gsi_shadow_tbl),
            row_key: se_shadow_rk.clone(),
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromRow(
                    mutation::DeleteFromRow {},
                )),
            }],
            ..MutateRowRequest::default()
        })
        .await
        .unwrap();
    let post_id = format!("post-{}", Uuid::new_v4().simple());
    let recover_ver = extenddb_storage_bigtable::data::item_ops::next_occ_version(None) + 10;
    coord
        .write_committed_side_effects(
            &post_id,
            &[extenddb_storage_bigtable::transact::ParticipantMutation {
                participant: extenddb_storage_bigtable::transact::ParticipantRow {
                    data_table: gsi_shadow_tbl.clone(),
                    row_key: se_shadow_rk,
                },
                payload: extenddb_storage_bigtable::transact::TxnOpPayload::Put {
                    item: se_item.clone(),
                },
                expected_version: None,
                row_existed: false,
                new_version: Some(recover_ver),
            }],
            &[],
        )
        .await
        .expect("write_committed_side_effects");
    let post_state = coord.get_state(&post_id).await.unwrap().unwrap();
    ctx.engine
        .roll_forward(&post_id, &post_state)
        .await
        .expect("replay committed side-effect record");
    assert!(coord.get_state(&post_id).await.unwrap().is_none());
    let gsi_kc = parse_kc("gpk = :g");
    let mut gsi_maps = ExpressionMaps::default();
    gsi_maps
        .values
        .insert("g".into(), AttributeValue::S("se_grp".into()));
    let (se_found, _) = ctx
        .engine
        .query(&ki_a, &gsi_kc, &gsi_maps, true, None, None, Some("ByGpk"))
        .await
        .unwrap();
    assert_eq!(se_found.len(), 1);

    // 3. (#12): Disabling stream closes shards and `streams::write_record` refuses
    //    to append past `EndingSequenceNumber`!
    let desc_a = ctx
        .engine
        .describe_table(
            &ctx.account_id,
            DescribeTableInput {
                table_name: tbl_a.clone(),
            },
        )
        .await
        .unwrap();
    let stream_arn_a = desc_a.latest_stream_arn.clone().unwrap();
    let disable_in: UpdateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": tbl_a,
        "StreamSpecification": { "StreamEnabled": false }
    }))
    .unwrap();
    ctx.engine
        .update_table(&ctx.account_id, disable_in)
        .await
        .unwrap();
    let closed_sdesc = ctx
        .engine
        .describe_stream(
            &ctx.account_id,
            &DescribeStreamInput {
                stream_arn: stream_arn_a.clone(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    let closed_shard_id = closed_sdesc.shards[0].shard_id.clone();
    assert!(
        closed_sdesc.shards[0]
            .sequence_number_range
            .ending_sequence_number
            .is_some()
    );
    let before_closed_recs =
        read_all_stream_records(&ctx.engine, &ctx.account_id, &stream_arn_a, &closed_sdesc).await;
    let dummy_cap = StreamCapture {
        view_type: StreamViewType::NewImage,
        user_identity: None,
        region: Arc::from("us-east-1"),
    };
    let late_rec = extenddb_storage_bigtable::streams::build_record(
        Some(&StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewImage),
        }),
        Some(&dummy_cap),
        &ki_a.key_schema,
        None,
        Some(&s_item(&[("pk", "late_after_close")])),
        extenddb_storage_bigtable::streams::UNASSIGNED_SEQUENCE_NUMBER,
        "us-east-1",
    )
    .unwrap();
    let cat = extenddb_storage_bigtable::catalog::Catalog::new(ctx.engine.catalog_client_ref());
    let written_seq = extenddb_storage_bigtable::streams::write_record_ordered(
        &cat,
        &closed_shard_id,
        &late_rec,
        Some("late-closed-op"),
    )
    .await
    .expect("closed shard write must not error");
    assert!(
        written_seq.is_none(),
        "write_record_ordered must skip closed shards without writing past EndingSequenceNumber (#12)"
    );
    let after_closed_recs =
        read_all_stream_records(&ctx.engine, &ctx.account_id, &stream_arn_a, &closed_sdesc).await;
    assert_eq!(before_closed_recs.len(), after_closed_recs.len());

    // 4. (#24): Version-only `m:v` tombstone row reclamation after grace window!
    let tomb_key = s_item(&[("pk", "tomb_gc_target")]);
    ctx.engine
        .put_item(&ki_a, tomb_key.clone(), false, None, &empty_maps, None)
        .await
        .unwrap();
    ctx.engine
        .delete_item(&ki_a, &tomb_key, false, None, &empty_maps, None)
        .await
        .unwrap();
    let enc_tomb_rk =
        extenddb_storage_bigtable::data::encoding::row_key::encode_key(&tomb_key, &ki_a.key_schema)
            .unwrap();

    // Before grace window expires, `m:v` tombstone row remains:
    let before_gc = ctx
        .client
        .data()
        .read_rows(ReadRowsRequest {
            table_name: ctx.client.full_table_name(&dt_a),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![enc_tomb_rk.clone()],
                row_ranges: vec![],
            }),
            ..ReadRowsRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(
        before_gc.len(),
        1,
        "version-only m:v tombstone must exist during grace window"
    );

    // Advance sweeper cutoff past the 5-minute grace window: physical row is reclaimed!
    #[cfg(feature = "test-hooks")]
    {
        extenddb_storage_bigtable::sweeper::sweep_with_cutoff_override(
            &ctx.engine,
            Some(i64::MAX / 2),
        )
        .await
        .expect("sweeper reclaims due version-only tombstones");
        let after_gc = ctx
            .client
            .data()
            .read_rows(ReadRowsRequest {
                table_name: ctx.client.full_table_name(&dt_a),
                rows_limit: 1,
                rows: Some(RowSet {
                    row_keys: vec![enc_tomb_rk],
                    row_ranges: vec![],
                }),
                ..ReadRowsRequest::default()
            })
            .await
            .unwrap();
        assert!(
            after_gc.is_empty(),
            "sweeper must reclaim version-only m:v row after grace window (#24)"
        );
        // Live row (`survives_a`) must remain completely intact!
        assert!(
            ctx.engine
                .get_item(&ki_a, &s_item(&[("pk", "survives_a")]))
                .await
                .unwrap()
                .is_some()
        );
    }
}

// -----------------------------------------------------------------------------
// Test 26: Foreign-Table Safe `drop_databases` (#14, #22), Migration Lock +
//          `ServingRegistration` Fencing (#17, #18), Backup Refusal (#31) &
//          Redacted Fail-Closed Auth (#32)
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_drop_databases_foreign_safety_migrations_fencing_backup_and_auth_redaction() {
    use extenddb_auth::CredentialStore;
    use extenddb_core::backup_format::BackupArn;
    use extenddb_core::types::LsiInput;
    use extenddb_storage::SnapshotEngine;
    use extenddb_storage::backup_catalog::{BackupCatalog, BackupKey};
    use extenddb_storage::hooks::ServingRegistrar;
    use extenddb_storage::management_store::{ManagementStore, SettingsStore};
    use extenddb_storage::migrations::MigrationError;
    use extenddb_storage::secret_keys::{CATALOG_KEY_SETTING, SecretKeys};
    use extenddb_storage_bigtable::migrations::Timing;

    let suffix = &Uuid::new_v4().to_string().replace('-', "")[..8];
    let mut cfg = BigtableStorageConfig {
        project_id: format!("iso-proj-{suffix}"),
        instance_id: format!("iso-inst-{suffix}"),
        ..BigtableStorageConfig::default()
    };
    let Some((host, client)) = resolve_reachable_emulator_host(&cfg).await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };
    cfg.emulator_host = Some(host);

    // 1. (#17 & #18): Uninitialized catalog refused at `register_and_check` and
    //    `Target::at`; `init_target` + `lock` writes `lock:catalog_migrations` on fresh
    //    catalog; `ServingRegistration` fences concurrent `lock` until released!
    let conn_str = cfg.connection_string();
    let uninit_check = extenddb_storage_bigtable::migrations::register_and_check(
        &conn_str,
        "1.0.0",
        Timing::PRODUCTION,
    )
    .await;
    assert!(
        uninit_check.is_err(),
        "serve startup must refuse an uninitialized catalog (#17)"
    );
    let migrate_target = Target::at(conn_str.clone());
    assert!(matches!(
        BigtableCatalogMigrations
            .lock(&migrate_target, Duration::from_secs(2))
            .await,
        Err(MigrationError::Status(CatalogStatus::Uninitialized))
    ));

    let bootstrapper = BigtableBootstrapper::new(cfg.clone());
    let init_target = bootstrapper.init_target();
    let lock = BigtableCatalogMigrations
        .lock(&init_target, Duration::from_secs(5))
        .await
        .expect("lock fresh uninitialized catalog (#18)");
    let second_lock = BigtableCatalogMigrations
        .lock(&init_target, Duration::from_millis(150))
        .await;
    assert!(
        matches!(second_lock, Err(MigrationError::Locked(_))),
        "fresh catalog must hold lock:catalog_migrations row (#18)"
    );
    BigtableCatalogMigrations
        .run(&lock, env!("CARGO_PKG_VERSION"), RunKind::Create)
        .await
        .expect("run baseline migrations");
    BigtableCatalogMigrations.release(lock).await;

    bootstrapper.bootstrap_encryption_key().await.unwrap();
    bootstrapper.bootstrap_default_account().await.unwrap();
    bootstrapper.bootstrap_admin_user(None, None).await.unwrap();

    let serving_reg = extenddb_storage_bigtable::migrations::register_and_check(
        &conn_str,
        env!("CARGO_PKG_VERSION"),
        Timing::PRODUCTION,
    )
    .await
    .expect("register_and_check on current catalog (#17)");
    let fenced_lock = BigtableCatalogMigrations
        .lock(&migrate_target, Duration::from_millis(250))
        .await;
    assert!(
        matches!(fenced_lock, Err(MigrationError::Locked(_))),
        "migration lock must wait/fail while ServingRegistration is live (#18)"
    );
    ServingRegistrar::release(&serving_reg).await;

    let lock_after_dereg = BigtableCatalogMigrations
        .lock(&migrate_target, Duration::from_secs(5))
        .await
        .expect("migration lock succeeds once server deregisters");
    BigtableCatalogMigrations.release(lock_after_dereg).await;

    // 2. (#31 & #32): Backup refusal vs empty-catalog answers & redacted auth failure!
    let unkeyed = extenddb_storage_bigtable::BigtableCatalogStore::new(client.clone(), None);
    let default_acct = unkeyed
        .get_setting("default_account_id")
        .await
        .unwrap()
        .unwrap();
    let engine = BigtableEngine::new(client.clone(), client.clone(), 60);
    let tbl_name = unique_name("tbl_iso");
    engine
        .create_table(
            &default_acct,
            CreateTableInput {
                table_name: tbl_name.clone(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".into(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".into(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "gpk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "lsk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ],
                global_secondary_indexes: Some(vec![GsiInput {
                    index_name: "IsoGsi".into(),
                    key_schema: vec![KeySchemaElement {
                        attribute_name: "gpk".into(),
                        key_type: KeyType::Hash,
                    }],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: None,
                    warm_throughput: None,
                }]),
                local_secondary_indexes: Some(vec![LsiInput {
                    index_name: "IsoLsi".into(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "pk".into(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "lsk".into(),
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
        .unwrap();

    match engine.open_table_snapshot(&default_acct, &tbl_name).await {
        Err(StorageError::Unsupported(msg)) => {
            assert_eq!(msg, "CreateBackup is not supported by this storage backend");
        }
        other => panic!("expected CreateBackup Unsupported (#31), got {other:?}"),
    }
    let missing_arn = BackupArn::parse(&format!(
        "arn:aws:dynamodb:us-east-1:{default_acct}:table/{tbl_name}/backup/01700000000000-11223344"
    ))
    .unwrap();
    let del_missing = engine
        .start_backup_delete(&BackupKey::from_arn(&missing_arn))
        .await;
    assert!(
        matches!(del_missing, Err(StorageError::Validation(ref m)) if m.starts_with("Backup not found:")),
        "DeleteBackup on Bigtable must return Backup not found (#31), got {del_missing:?}"
    );

    let key_b64 = unkeyed
        .get_setting(CATALOG_KEY_SETTING)
        .await
        .unwrap()
        .unwrap();
    let secret_keys = Arc::new(SecretKeys::catalog_key(&key_b64).unwrap());
    let mgmt = extenddb_storage_bigtable::BigtableCatalogStore::new(
        client.clone(),
        Some(secret_keys.clone()),
    );
    mgmt.create_user(&default_acct, "tampered_user", None)
        .await
        .unwrap();
    let issued = mgmt
        .create_access_key(&default_acct, "tampered_user")
        .await
        .unwrap();
    let cat = extenddb_storage_bigtable::catalog::Catalog::new(&client);
    let ak_row_key = extenddb_storage_bigtable::catalog::keys::access_key(&issued.access_key_id);
    let mut ak_doc = cat.get(&ak_row_key).await.unwrap().unwrap();
    ak_doc["secret_key_encrypted_b64"] =
        serde_json::Value::String("dGhpcyBpcyBub3QgdmFsaWQgY2lwaGVydGV4dA==".into());
    cat.put(&ak_row_key, &ak_doc).await.unwrap();

    let cred_store =
        extenddb_storage_bigtable::BigtableCredentialStore::new(client.clone(), Some(secret_keys));
    let Err(auth_err) = cred_store.lookup_credential(&issued.access_key_id).await else {
        panic!("corrupted ciphertext must fail closed (#32)");
    };
    assert_eq!(auth_err.to_string(), "Internal error during authentication");
    assert!(
        !auth_err.to_string().contains(&issued.access_key_id),
        "auth error must not leak access_key_id (#32)"
    );

    // 3. (#14 & #22): Create a non-ExtendDB table `transactions_2024` (matching old `starts_with('t')`!)
    //    and verify `drop_databases` leaves `transactions_2024` and its row intact while deleting
    //    all ExtendDB system, base, GSI (`_g`), and LSI (`_l`) tables!
    let mut admin = AdminClient::connect(&client).await.unwrap();
    admin
        .create_table("transactions_2024", &[("cf", None)])
        .await
        .expect("create foreign table transactions_2024");
    client
        .data()
        .mutate_row(MutateRowRequest {
            table_name: client.full_table_name("transactions_2024"),
            row_key: b"foreign-row-1".to_vec(),
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
                    family_name: "cf".into(),
                    column_qualifier: b"q".to_vec(),
                    timestamp_micros: extenddb_storage_bigtable::data::item_ops::now_millis_micros(
                    ),
                    value: b"precious-user-data".to_vec(),
                })),
            }],
            ..MutateRowRequest::default()
        })
        .await
        .unwrap();

    bootstrapper
        .drop_databases(&cfg.instance_id)
        .await
        .expect("drop_databases");

    let remaining = admin.list_tables().await.unwrap();
    assert_eq!(
        remaining,
        vec!["transactions_2024".to_string()],
        "only foreign table transactions_2024 must remain (#14)"
    );
    let foreign_rows = client
        .data()
        .read_rows(ReadRowsRequest {
            table_name: client.full_table_name("transactions_2024"),
            rows_limit: 10,
            ..ReadRowsRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(
        foreign_rows.len(),
        1,
        "foreign table data must be untouched"
    );
}

// -----------------------------------------------------------------------------
// Test 27: Multi-Frame >2 MiB Cell `ReadRowsAssembler` (#37) & >100k-Mutation
//          Chunked Base + `ALL` GSI Round-Trip (#19)
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_multi_frame_large_cell_assembler_and_chunked_100k_mutation_gsi_roundtrip() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    // 1. (#37): Write a 2.5 MiB cell + sibling cells directly on cbtemulator so
    //    `ReadRows` splits the cell across multiple ~1 MiB `ReadRowsResponse` frames,
    //    and verify `ReadRowsAssembler` reassembles the row byte-for-byte.
    let tbl_name = unique_name("tbl_chunked_100k");
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: tbl_name.clone(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".into(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "gpk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ],
                global_secondary_indexes: Some(vec![GsiInput {
                    index_name: "AllProjGsi".into(),
                    key_schema: vec![KeySchemaElement {
                        attribute_name: "gpk".into(),
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
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    let ki = ctx
        .engine
        .table_key_info(&ctx.account_id, &tbl_name)
        .await
        .unwrap();
    let data_table = ctx.engine.data_table_for(&ki).await.unwrap();

    let large_payload: Vec<u8> = (0..5_500_000usize)
        .map(|i| ((i * 31 + 7) % 251) as u8)
        .collect();
    let raw_rk = b"__raw_multiframe_probe__".to_vec();
    let probe_ts = extenddb_storage_bigtable::data::item_ops::now_millis_micros();
    ctx.client
        .data()
        .mutate_row(MutateRowRequest {
            table_name: ctx.client.full_table_name(&data_table),
            row_key: raw_rk.clone(),
            mutations: vec![
                Mutation {
                    mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
                        family_name: "d".into(),
                        column_qualifier: b"big".to_vec(),
                        timestamp_micros: probe_ts,
                        value: large_payload.clone(),
                    })),
                },
                Mutation {
                    mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
                        family_name: "m".into(),
                        column_qualifier: b"v".to_vec(),
                        timestamp_micros: probe_ts,
                        value: 1u64.to_be_bytes().to_vec(),
                    })),
                },
            ],
            ..MutateRowRequest::default()
        })
        .await
        .expect("write 2.5 MiB cell");
    let assembled = ctx
        .client
        .data()
        .read_rows(ReadRowsRequest {
            table_name: ctx.client.full_table_name(&data_table),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![raw_rk.clone()],
                row_ranges: vec![],
            }),
            ..ReadRowsRequest::default()
        })
        .await
        .expect("read multi-frame 2.5 MiB cell");
    assert_eq!(assembled.len(), 1);
    let big_cell = assembled[0]
        .1
        .iter()
        .find(|c| c.family_name == "d" && c.qualifier == b"big")
        .expect("assembled d:big cell");
    assert_eq!(big_cell.value.len(), large_payload.len());
    assert_eq!(big_cell.value, large_payload);
    ctx.client
        .data()
        .mutate_row(MutateRowRequest {
            table_name: ctx.client.full_table_name(&data_table),
            row_key: raw_rk,
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::DeleteFromRow(
                    mutation::DeleteFromRow {},
                )),
            }],
            ..MutateRowRequest::default()
        })
        .await
        .unwrap();

    // 2. (#19): Multi-chunk CAS + overflow `MutateRow` execution across `put_cas`,
    //    `put_shadow_versioned`, `delete_shadow_versioned`, and 2PC `apply_fenced_mutation`
    //    using task-local `with_max_mutations_per_rpc(200, ...)` (avoiding `cbtemulator`'s
    //    quadratic in-memory 100k-column slice shift while exercising 4-6 RPC chunks per write;
    //    `MAX_MUTATIONS_PER_RPC = 90_000` sizing on 165k mutations is verified in unit tests).
    #[cfg(feature = "test-hooks")]
    extenddb_storage_bigtable::data::item_ops::with_max_mutations_per_rpc(200, async {
        let empty_maps = ExpressionMaps::default();
        let mut wide_item = s_item(&[("pk", "wide_chunked"), ("gpk", "g_wide")]);
        for i in 0..650u32 {
            wide_item.insert(format!("a{i:04}"), AttributeValue::Bool(true));
        }
        ctx.engine
            .put_item(&ki, wide_item, false, None, &empty_maps, None)
            .await
            .expect("multi-chunk PutItem + ALL GSI must succeed (#19)");

        let fetched = ctx
            .engine
            .get_item(&ki, &s_item(&[("pk", "wide_chunked")]))
            .await
            .unwrap()
            .expect("chunked base item");
        assert_eq!(fetched.len(), 652);

        let gsi_kc = parse_kc("gpk = :g");
        let mut gsi_maps = ExpressionMaps::default();
        gsi_maps
            .values
            .insert("g".into(), AttributeValue::S("g_wide".into()));
        let (gsi_rows, _) = ctx
            .engine
            .query(
                &ki,
                &gsi_kc,
                &gsi_maps,
                true,
                None,
                None,
                Some("AllProjGsi"),
            )
            .await
            .expect("query multi-chunk ALL GSI shadow row");
        assert_eq!(gsi_rows.len(), 1);
        assert_eq!(gsi_rows[0].len(), 652);

        // Overwrite replacing `a0000..a0399` with `b0000..b0399` (>1,050 shadow mutations -> 6 RPC chunks!)
        let mut wide_item_v2 = s_item(&[("pk", "wide_chunked"), ("gpk", "g_wide")]);
        for i in 400..650u32 {
            wide_item_v2.insert(format!("a{i:04}"), AttributeValue::Bool(true));
        }
        for i in 0..400u32 {
            wide_item_v2.insert(format!("b{i:04}"), AttributeValue::Bool(false));
        }
        ctx.engine
            .transact_write_items(
                &[TransactWriteOp::Put {
                    key_info: &ki,
                    item: &wide_item_v2,
                    condition: None,
                    maps: &empty_maps,
                    return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                    stream: None,
                }],
                None,
            )
            .await
            .expect(
                "2PC TransactWriteItems multi-chunk apply_fenced_mutation + shadow update (#19)",
            );

        let (gsi_rows_v2, _) = ctx
            .engine
            .query(
                &ki,
                &gsi_kc,
                &gsi_maps,
                true,
                None,
                None,
                Some("AllProjGsi"),
            )
            .await
            .unwrap();
        assert_eq!(gsi_rows_v2.len(), 1);
        assert_eq!(gsi_rows_v2[0].len(), 652);
        assert!(!gsi_rows_v2[0].contains_key("a0000"));
        assert_eq!(
            gsi_rows_v2[0].get("b0399"),
            Some(&AttributeValue::Bool(false))
        );

        ctx.engine
            .delete_item(
                &ki,
                &s_item(&[("pk", "wide_chunked")]),
                false,
                None,
                &empty_maps,
                None,
            )
            .await
            .expect("multi-chunk delete_shadow_versioned (#19)");
        assert!(
            ctx.engine
                .get_item(&ki, &s_item(&[("pk", "wide_chunked")]))
                .await
                .unwrap()
                .is_none()
        );
    })
    .await;

    ctx.engine
        .delete_table(
            &ctx.account_id,
            extenddb_core::types::DeleteTableInput {
                table_name: tbl_name,
            },
        )
        .await
        .unwrap();
}

// -----------------------------------------------------------------------------
// Test 28: Ordered LSI Shadow Tables (`<data_table>_l<8hex>`, #22 Option A)
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_lsi_ordered_shadow_table_lifecycle_and_queries() {
    use extenddb_core::types::{DeleteTableInput, LsiInput};

    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };
    let empty_maps = ExpressionMaps::default();

    let tbl = unique_name("tbl_lsi_shadow");
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: tbl.clone(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".into(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".into(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "rank".into(),
                        attribute_type: ScalarAttributeType::N,
                    },
                ],
                local_secondary_indexes: Some(vec![LsiInput {
                    index_name: "by_rank_inc".into(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "pk".into(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "rank".into(),
                            key_type: KeyType::Range,
                        },
                    ],
                    projection: Projection {
                        projection_type: ProjectionType::Include,
                        non_key_attributes: Some(vec!["label".into()]),
                    },
                }]),
                billing_mode: Some(BillingMode::PayPerRequest),
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();

    let ki = ctx
        .engine
        .table_key_info(&ctx.account_id, &tbl)
        .await
        .unwrap();
    let data_table = ctx.engine.data_table_for(&ki).await.unwrap();
    let lsi_shadow =
        extenddb_storage_bigtable::gsi::lsi_shadow_table_id(&data_table, "by_rank_inc");

    let mut admin = AdminClient::connect(&ctx.client).await.unwrap();
    assert!(
        admin.list_tables().await.unwrap().contains(&lsi_shadow),
        "CreateTable must provision physical LSI shadow table {lsi_shadow}"
    );

    let mut t1 = s_item(&[
        ("pk", "u1"),
        ("sk", "c"),
        ("label", "third"),
        ("extra", "hidden"),
    ]);
    t1.insert("rank".into(), AttributeValue::N("30".into()));
    let mut t2 = s_item(&[
        ("pk", "u1"),
        ("sk", "a"),
        ("label", "first"),
        ("extra", "hidden"),
    ]);
    t2.insert("rank".into(), AttributeValue::N("10".into()));
    let mut t3 = s_item(&[
        ("pk", "u1"),
        ("sk", "b"),
        ("label", "second"),
        ("extra", "hidden"),
    ]);
    t3.insert("rank".into(), AttributeValue::N("20".into()));

    ctx.engine
        .transact_write_items(
            &[
                TransactWriteOp::Put {
                    key_info: &ki,
                    item: &t1,
                    condition: None,
                    maps: &empty_maps,
                    return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                    stream: None,
                },
                TransactWriteOp::Put {
                    key_info: &ki,
                    item: &t2,
                    condition: None,
                    maps: &empty_maps,
                    return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                    stream: None,
                },
            ],
            None,
        )
        .await
        .unwrap();
    ctx.engine
        .put_item(&ki, t3, false, None, &empty_maps, None)
        .await
        .unwrap();

    let mut upd_m = ExpressionMaps::default();
    upd_m
        .values
        .insert("r".into(), AttributeValue::N("5".into()));
    ctx.engine
        .update_item(
            &ki,
            &s_item(&[("pk", "u1"), ("sk", "c")]),
            &parse_upd("SET rank = :r"),
            false,
            false,
            None,
            &upd_m,
            None,
        )
        .await
        .unwrap();

    let mut qm = ExpressionMaps::default();
    qm.values.insert("p".into(), AttributeValue::S("u1".into()));
    let kc = parse_kc("pk = :p");
    let (fwd, _) = ctx
        .engine
        .query(&ki, &kc, &qm, true, None, None, Some("by_rank_inc"))
        .await
        .unwrap();
    let fwd_sks: Vec<&str> = fwd
        .iter()
        .map(|i| match i.get("sk").unwrap() {
            AttributeValue::S(s) => s.as_str(),
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(fwd_sks, vec!["c", "a", "b"]);
    for item in &fwd {
        assert!(item.contains_key("label"));
        assert!(!item.contains_key("extra"));
    }

    let (rev, _) = ctx
        .engine
        .query(&ki, &kc, &qm, false, None, None, Some("by_rank_inc"))
        .await
        .unwrap();
    let rev_sks: Vec<&str> = rev
        .iter()
        .map(|i| match i.get("sk").unwrap() {
            AttributeValue::S(s) => s.as_str(),
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(rev_sks, vec!["b", "a", "c"]);

    let mut par_seen = Vec::new();
    for seg in 0..2i64 {
        let (seg_items, _) = ctx
            .engine
            .scan(&ki, None, None, Some(seg), Some(2), Some("by_rank_inc"))
            .await
            .unwrap();
        par_seen.extend(seg_items);
    }
    assert_eq!(par_seen.len(), 3);

    ctx.engine
        .delete_table(&ctx.account_id, DeleteTableInput { table_name: tbl })
        .await
        .unwrap();
    assert!(
        !admin.list_tables().await.unwrap().contains(&lsi_shadow),
        "DeleteTable must drop physical LSI shadow table {lsi_shadow}"
    );
}

// -----------------------------------------------------------------------------
// Fault-Injecting HTTP/2 Proxy Helper & Concurrency / Recovery Regression Suite
// -----------------------------------------------------------------------------

struct FaultInjectionProxy {
    listen_addr: String,
    shadow_delay_ms: Arc<std::sync::atomic::AtomicUsize>,
    shadow_drop_budget: Arc<std::sync::atomic::AtomicUsize>,
    shadow_pass_before_drop: Arc<std::sync::atomic::AtomicUsize>,
    updated_val_delay_ms: Arc<std::sync::atomic::AtomicUsize>,
    intent_delay_ms: Arc<std::sync::atomic::AtomicUsize>,
    stream_record_delay_ms: Arc<std::sync::atomic::AtomicUsize>,
    ending_seq_delay_ms: Arc<std::sync::atomic::AtomicUsize>,
}

impl FaultInjectionProxy {
    async fn start(upstream_addr: String) -> Self {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fault injection proxy");
        let listen_addr = listener.local_addr().expect("local_addr").to_string();
        let shadow_delay_ms = Arc::new(AtomicUsize::new(0));
        let shadow_drop_budget = Arc::new(AtomicUsize::new(0));
        let shadow_pass_before_drop = Arc::new(AtomicUsize::new(0));
        let updated_val_delay_ms = Arc::new(AtomicUsize::new(0));
        let intent_delay_ms = Arc::new(AtomicUsize::new(0));
        let stream_record_delay_ms = Arc::new(AtomicUsize::new(0));
        let ending_seq_delay_ms = Arc::new(AtomicUsize::new(0));

        let sd = shadow_delay_ms.clone();
        let sdrop = shadow_drop_budget.clone();
        let spass = shadow_pass_before_drop.clone();
        let uvd = updated_val_delay_ms.clone();
        let idel = intent_delay_ms.clone();
        let srdel = stream_record_delay_ms.clone();
        let esdel = ending_seq_delay_ms.clone();

        tokio::spawn(async move {
            while let Ok((client_sock, _)) = listener.accept().await {
                let upstream = upstream_addr.clone();
                let sd = sd.clone();
                let sdrop = sdrop.clone();
                let spass = spass.clone();
                let uvd = uvd.clone();
                let idel = idel.clone();
                let srdel = srdel.clone();
                let esdel = esdel.clone();

                tokio::spawn(async move {
                    let Ok(server_sock) = TcpStream::connect(&upstream).await else {
                        return;
                    };
                    let _ = client_sock.set_nodelay(true);
                    let _ = server_sock.set_nodelay(true);
                    let (mut cr, mut cw) = client_sock.into_split();
                    let (mut sr, mut sw) = server_sock.into_split();

                    let c2s = tokio::spawn(async move {
                        let mut buf = vec![0u8; 65_536];
                        while let Ok(n) = cr.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                            let chunk = &buf[..n];
                            let contains = |pat: &[u8]| chunk.windows(pat.len()).any(|w| w == pat);

                            let hits_shadow = contains(b"_g") && !contains(b"__extenddb_");
                            if contains(b"updated") && !hits_shadow {
                                let d = uvd.swap(0, Ordering::SeqCst);
                                if d > 0 {
                                    tokio::time::sleep(Duration::from_millis(d as u64)).await;
                                }
                            }
                            if contains(b"intent:sgl-") {
                                let d = idel.swap(0, Ordering::SeqCst);
                                if d > 0 {
                                    tokio::time::sleep(Duration::from_millis(d as u64)).await;
                                }
                            }
                            if contains(b"stream_record:") && contains(b"NEW_AND_OLD_IMAGES") {
                                let d = srdel.swap(0, Ordering::SeqCst);
                                if d > 0 {
                                    tokio::time::sleep(Duration::from_millis(d as u64)).await;
                                }
                            }
                            if contains(b"\"ending_sequence_number\":\"1") {
                                let d = esdel.swap(0, Ordering::SeqCst);
                                if d > 0 {
                                    tokio::time::sleep(Duration::from_millis(d as u64)).await;
                                }
                            }
                            if hits_shadow {
                                let d = sd.swap(0, Ordering::SeqCst);
                                if d > 0 {
                                    tokio::time::sleep(Duration::from_millis(d as u64)).await;
                                }
                                let pass_left = spass.load(Ordering::SeqCst);
                                if pass_left > 0 {
                                    spass.fetch_sub(1, Ordering::SeqCst);
                                } else {
                                    let drop_left = sdrop.load(Ordering::SeqCst);
                                    if drop_left > 0 {
                                        sdrop.fetch_sub(1, Ordering::SeqCst);
                                        break;
                                    }
                                }
                            }

                            if sw.write_all(chunk).await.is_err() {
                                break;
                            }
                        }
                    });

                    let s2c = tokio::spawn(async move {
                        let mut buf = vec![0u8; 65_536];
                        while let Ok(n) = sr.read(&mut buf).await {
                            if n == 0 {
                                break;
                            }
                            if cw.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    });

                    tokio::select! {
                        _ = c2s => {},
                        _ = s2c => {},
                    }
                });
            }
        });

        Self {
            listen_addr,
            shadow_delay_ms,
            shadow_drop_budget,
            shadow_pass_before_drop,
            updated_val_delay_ms,
            intent_delay_ms,
            stream_record_delay_ms,
            ending_seq_delay_ms,
        }
    }
}

async fn setup_fault_proxy_context(
    intent_timeout_secs: u64,
) -> Option<(
    Arc<BigtableClient>,
    Arc<BigtableEngine>,
    Arc<BigtableEngine>,
    FaultInjectionProxy,
    String,
)> {
    let suffix = &Uuid::new_v4().to_string().replace('-', "")[..8];
    let project_id = format!("flt-proj-{suffix}");
    let instance_id = format!("flt-inst-{suffix}");

    let probe_cfg = BigtableStorageConfig {
        project_id: project_id.clone(),
        instance_id: instance_id.clone(),
        pool_size: 1,
        intent_timeout_secs,
        ..BigtableStorageConfig::default()
    };
    let (emulator_host, clean_client) = resolve_reachable_emulator_host(&probe_cfg).await?;

    let mut clean_cfg = probe_cfg;
    clean_cfg.emulator_host = Some(emulator_host.clone());

    let bootstrapper = BigtableBootstrapper::new(clean_cfg);
    bootstrapper.create_catalog_db().await.unwrap();
    bootstrapper.create_data_db().await.unwrap();
    let target = bootstrapper.init_target();
    let lock = BigtableCatalogMigrations
        .lock(&target, Duration::from_secs(10))
        .await
        .unwrap();
    BigtableCatalogMigrations
        .run(&lock, env!("CARGO_PKG_VERSION"), RunKind::Create)
        .await
        .unwrap();
    BigtableCatalogMigrations.release(lock).await;

    let engine_clean = Arc::new(BigtableEngine::new(
        clean_client.clone(),
        clean_client.clone(),
        intent_timeout_secs,
    ));

    let proxy = FaultInjectionProxy::start(emulator_host).await;
    let proxy_cfg = BigtableStorageConfig {
        project_id,
        instance_id,
        emulator_host: Some(proxy.listen_addr.clone()),
        pool_size: 1,
        intent_timeout_secs,
        ..BigtableStorageConfig::default()
    };
    let proxy_client = Arc::new(BigtableClient::connect(&proxy_cfg).await.unwrap());
    let engine_proxy = Arc::new(BigtableEngine::new(
        proxy_client.clone(),
        proxy_client,
        intent_timeout_secs,
    ));

    Some((
        clean_client,
        engine_clean,
        engine_proxy,
        proxy,
        "123456789012".to_string(),
    ))
}

fn single_hash_table_with_all_gsi(name: &str) -> CreateTableInput {
    CreateTableInput {
        table_name: name.to_string(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".into(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".into(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        billing_mode: Some(BillingMode::PayPerRequest),
        deletion_protection_enabled: Some(false),
        global_secondary_indexes: Some(vec![GsiInput {
            index_name: "gsi_idx".into(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gsi_pk".into(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
            warm_throughput: None,
        }]),
        ..CreateTableInput::default()
    }
}

#[tokio::test]
async fn test_admin_create_table_idempotency_and_missing_family_reconciliation() {
    use extenddb_storage_bigtable::data::admin::gc_max_versions;

    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };
    let mut admin = AdminClient::connect(&ctx.client).await.unwrap();
    let table_id = unique_name("t_admin_reconcile");

    admin
        .create_table(&table_id, &[("d", None), ("m", Some(gc_max_versions(1)))])
        .await
        .expect("fresh create_table succeeds");

    // Re-running create_table when all families already exist must succeed without error.
    admin
        .create_table(&table_id, &[("d", None), ("m", Some(gc_max_versions(1)))])
        .await
        .expect("idempotent create_table on existing table succeeds");

    // Re-running create_table with additional families on an existing table reconciles only missing families.
    admin
        .create_table(
            &table_id,
            &[
                ("d", None),
                ("m", Some(gc_max_versions(1))),
                ("t", Some(gc_max_versions(1))),
                ("x", Some(gc_max_versions(1))),
            ],
        )
        .await
        .expect("create_table reconciles missing column families on existing table");

    ctx.client
        .data()
        .mutate_row(MutateRowRequest {
            table_name: ctx.client.full_table_name(&table_id),
            row_key: b"probe".to_vec(),
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
                    family_name: "x".into(),
                    column_qualifier: b"q".to_vec(),
                    timestamp_micros: extenddb_storage_bigtable::data::item_ops::now_millis_micros(
                    ),
                    value: b"ok".to_vec(),
                })),
            }],
            ..MutateRowRequest::default()
        })
        .await
        .expect("write to newly reconciled column family succeeds");

    admin.delete_table(&table_id).await.unwrap();
}

#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_chunked_mutation_fencing_prevents_torn_reads_and_delete_zombies() {
    use extenddb_storage_bigtable::data::encoding::{cell, row_key};
    use extenddb_storage_bigtable::data::item_ops::{
        ItemOps, now_millis_micros, with_max_mutations_per_rpc,
    };
    use extenddb_storage_bigtable::sweeper;
    use std::sync::atomic::{AtomicBool, Ordering};

    let Some((clean_client, engine1, engine_proxy, proxy, acct)) =
        setup_fault_proxy_context(2).await
    else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };
    let engine2 = Arc::new(BigtableEngine::new(
        clean_client.clone(),
        clean_client.clone(),
        2,
    ));
    let tbl = unique_name("tbl_chunked_fencing");
    let desc = engine1
        .create_table(&acct, single_hash_table_with_all_gsi(&tbl))
        .await
        .unwrap();
    let ki = engine1.table_key_info(&acct, &tbl).await.unwrap();
    let dt = format!("t{}", &desc.table_id.replace('-', "")[..16]);
    let shadow_tbl = extenddb_storage_bigtable::gsi::shadow_table_id(&dt, "gsi_idx");
    let maps = ExpressionMaps::default();

    let mk_wide = |pk: &str, gsi: &str, tag: &str| -> Item {
        let mut it = BTreeMap::new();
        it.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
        it.insert("gsi_pk".to_owned(), AttributeValue::S(gsi.to_owned()));
        for idx in 0..36 {
            it.insert(
                format!("z_attr_{idx:02}"),
                AttributeValue::S(format!("{tag}_{idx:02}")),
            );
        }
        it
    };

    let verify_generation_coherence = |it: &Item| -> Result<(), String> {
        if it.len() != 38 {
            return Err(format!("expected 38 attributes, got {}", it.len()));
        }
        let Some(AttributeValue::S(first_val)) = it.get("z_attr_00") else {
            return Err("missing z_attr_00".to_string());
        };
        let Some((prefix, _)) = first_val.rsplit_once('_') else {
            return Err(format!("unexpected z_attr_00 value {first_val}"));
        };
        for idx in 1..36 {
            let attr = format!("z_attr_{idx:02}");
            let expected = format!("{prefix}_{idx:02}");
            match it.get(&attr) {
                Some(AttributeValue::S(actual)) if actual == &expected => {}
                other => {
                    return Err(format!(
                        "inconsistent generation across chunks at {attr}: expected {expected}, got {other:?}"
                    ));
                }
            }
        }
        Ok(())
    };

    // Part 0a: Fresh absent-row `(None, false)` multi-chunk `TransactWriteItems` + `ALL`-GSI shadow (`limit = 4`).
    let fresh_txn_item = mk_wide("k_absent_txn", "g_absent_txn", "init0");
    with_max_mutations_per_rpc(4, async {
        engine1
            .transact_write_items(
                &[TransactWriteOp::Put {
                    key_info: &ki,
                    item: &fresh_txn_item,
                    condition: None,
                    maps: &maps,
                    return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                    stream: None,
                }],
                None,
            )
            .await
    })
    .await
    .expect("absent-row multi-chunk TransactWriteItems must succeed");
    let got_fresh_txn = engine1
        .get_item(&ki, &s_item(&[("pk", "k_absent_txn")]))
        .await
        .unwrap()
        .expect("k_absent_txn must exist");
    verify_generation_coherence(&got_fresh_txn).unwrap();

    // Part 0b: Unversioned existing row `(None, true)` (raw `d:` cells without `m:v`) multi-chunk `TransactWriteItems`.
    let unversioned_rk =
        row_key::encode_key(&s_item(&[("pk", "k_unversioned")]), &ki.key_schema).unwrap();
    clean_client
        .data()
        .mutate_row(MutateRowRequest {
            table_name: clean_client.full_table_name(&dt),
            row_key: unversioned_rk,
            mutations: vec![Mutation {
                mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
                    family_name: "d".into(),
                    column_qualifier: b"pk".to_vec(),
                    timestamp_micros: now_millis_micros(),
                    value: cell::encode(&AttributeValue::S("k_unversioned".into())).unwrap(),
                })),
            }],
            ..MutateRowRequest::default()
        })
        .await
        .unwrap();
    let unversioned_next = mk_wide("k_unversioned", "g_unversioned", "unv1");
    with_max_mutations_per_rpc(4, async {
        engine1
            .transact_write_items(
                &[TransactWriteOp::Put {
                    key_info: &ki,
                    item: &unversioned_next,
                    condition: None,
                    maps: &maps,
                    return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                    stream: None,
                }],
                None,
            )
            .await
    })
    .await
    .expect("unversioned-row multi-chunk TransactWriteItems must succeed");
    let got_unversioned = engine1
        .get_item(&ki, &s_item(&[("pk", "k_unversioned")]))
        .await
        .unwrap()
        .expect("k_unversioned must exist");
    verify_generation_coherence(&got_unversioned).unwrap();

    // Part 0c: Multi-chunk `UpdateItem` growing a small row above `max_mutations_per_rpc`.
    engine1
        .put_item(
            &ki,
            s_item(&[("pk", "k_upd_grow"), ("gsi_pk", "g_upd_grow")]),
            false,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();
    let mut set_clauses = Vec::new();
    let mut upd_maps = ExpressionMaps::default();
    for idx in 0..36 {
        set_clauses.push(format!("z_attr_{idx:02} = :v{idx:02}"));
        upd_maps.values.insert(
            format!("v{idx:02}"),
            AttributeValue::S(format!("updg_{idx:02}")),
        );
    }
    let upd_actions = parse_upd(&format!("SET {}", set_clauses.join(", ")));
    let (_, upd_new) = with_max_mutations_per_rpc(
        4,
        engine1.update_item(
            &ki,
            &s_item(&[("pk", "k_upd_grow")]),
            &upd_actions,
            false,
            true,
            None,
            &upd_maps,
            None,
        ),
    )
    .await
    .expect("multi-chunk UpdateItem must succeed");
    verify_generation_coherence(&upd_new.unwrap()).unwrap();

    // Part 0d: Stalled mid-chunk shadow write (`limit = 4`) immediately superseded by `delete_item` without lockout.
    let _ = engine_proxy
        .get_item(&ki, &s_item(&[("pk", "k_mid_shd")]))
        .await
        .unwrap();
    proxy.shadow_pass_before_drop.store(2, Ordering::SeqCst);
    proxy.shadow_drop_budget.store(8, Ordering::SeqCst);
    with_max_mutations_per_rpc(
        4,
        engine_proxy.put_item(
            &ki,
            mk_wide("k_mid_shd", "g_mid_shd", "stall0"),
            false,
            None,
            &maps,
            None,
        ),
    )
    .await
    .expect("put_item with mid-chunk shadow fault delegates to sweeper without error");
    proxy.shadow_drop_budget.store(0, Ordering::SeqCst);

    engine1
        .delete_item(
            &ki,
            &s_item(&[("pk", "k_mid_shd")]),
            false,
            None,
            &maps,
            None,
        )
        .await
        .expect("delete_item must immediately supersede stalled multi-chunk shadow row");
    sweeper::sweep_once(&engine1).await.unwrap();
    let kc = parse_kc("gsi_pk = :v");
    let mut qm_mid = ExpressionMaps::default();
    qm_mid
        .values
        .insert("v".into(), AttributeValue::S("g_mid_shd".into()));
    let (mid_rows, _) = engine1
        .query(&ki, &kc, &qm_mid, true, None, None, Some("gsi_idx"))
        .await
        .unwrap();
    assert!(
        mid_rows.is_empty(),
        "superseded mid-chunk shadow row must be cleanly retired: {mid_rows:?}"
    );

    // Part 1: Multi-chunk `PutItem` (`limit = 4`) vs concurrent `GetItem` and `TransactGetItems`.
    engine1
        .put_item(&ki, mk_wide("k1", "g0", "gen0"), false, None, &maps, None)
        .await
        .unwrap();

    let done = Arc::new(AtomicBool::new(false));
    let done_w = done.clone();
    let eng_w = engine1.clone();
    let ki_w = ki.clone();
    let writer = tokio::spawn(async move {
        let m = ExpressionMaps::default();
        for g in 1..=60 {
            let it = mk_wide("k1", "g0", &format!("gen{g}"));
            with_max_mutations_per_rpc(4, eng_w.put_item(&ki_w, it, false, None, &m, None))
                .await
                .unwrap();
            if done_w.load(Ordering::Relaxed) {
                break;
            }
        }
    });

    let key_k1 = s_item(&[("pk", "k1")]);
    let mut torn_get: Option<String> = None;
    let mut torn_tget: Option<String> = None;
    for _ in 0..350 {
        if torn_get.is_none()
            && let Some(it) = engine2.get_item(&ki, &key_k1).await.unwrap()
            && let Err(reason) = verify_generation_coherence(&it)
        {
            torn_get = Some(reason);
        }
        if torn_tget.is_none() {
            let tget = engine2
                .transact_get_items(&[TransactGetOp {
                    key_info: &ki,
                    key: &key_k1,
                }])
                .await
                .unwrap();
            if let Some(Some(it)) = tget.into_iter().next()
                && let Err(reason) = verify_generation_coherence(&it)
            {
                torn_tget = Some(reason);
            }
        }
        if torn_get.is_some() || torn_tget.is_some() {
            break;
        }
    }
    done.store(true, Ordering::Relaxed);
    writer.await.unwrap();

    assert!(
        torn_get.is_none(),
        "GetItem observed torn multi-chunk write: {torn_get:?}"
    );
    assert!(
        torn_tget.is_none(),
        "TransactGetItems observed torn multi-chunk write: {torn_tget:?}"
    );

    // Part 2: Concurrent multi-chunk `PutItem` (`limit = 6`) vs `DeleteItem` + base/GSI consistency.
    let mut zombie_base: Option<(u64, usize, bool)> = None;
    let mut gsi_mismatch: Option<(u64, bool, usize)> = None;
    for round in 0..50u64 {
        let pk = format!("k_del_{round}");
        let gsi_val = format!("g_del_{round}");
        let it = mk_wide(&pk, &gsi_val, "v1");
        let eng_put = engine1.clone();
        let ki_put = ki.clone();
        let put_task = tokio::spawn(async move {
            let m = ExpressionMaps::default();
            with_max_mutations_per_rpc(6, eng_put.put_item(&ki_put, it, false, None, &m, None))
                .await
        });
        tokio::time::sleep(Duration::from_micros(80 + (round % 15) * 60)).await;
        let del_key = s_item(&[("pk", &pk)]);
        let del_res = engine2
            .delete_item(&ki, &del_key, true, None, &maps, None)
            .await;
        let put_res = put_task.await.unwrap();
        let del_old = del_res.expect("concurrent DeleteItem must not fail");
        put_res.expect("concurrent multi-chunk PutItem must not fail");
        if let Some(old_item) = &del_old {
            assert_eq!(old_item.len(), 38);
            assert!(old_item.contains_key("pk"));
        }
        let final_base = engine1.get_item(&ki, &del_key).await.unwrap();
        if let Some(final_item) = &final_base
            && (final_item.len() != 38 || !final_item.contains_key("pk") || del_old.is_some())
        {
            zombie_base = Some((round, final_item.len(), final_item.contains_key("pk")));
            break;
        }
        let mut qmaps = ExpressionMaps::default();
        qmaps.values.insert("v".into(), AttributeValue::S(gsi_val));
        let (gsi_rows, _) = engine1
            .query(&ki, &kc, &qmaps, true, None, None, Some("gsi_idx"))
            .await
            .unwrap();
        let expected_gsi_len = usize::from(final_base.is_some());
        if gsi_rows.len() != expected_gsi_len || gsi_rows.first().is_some_and(|r| r.len() != 38) {
            gsi_mismatch = Some((round, final_base.is_some(), gsi_rows.len()));
            break;
        }
    }

    assert!(
        zombie_base.is_none(),
        "Concurrent multi-chunk PutItem and DeleteItem produced zombie/partial row: {zombie_base:?}"
    );
    assert!(
        gsi_mismatch.is_none(),
        "Concurrent multi-chunk PutItem and DeleteItem left GSI out of sync with base: {gsi_mismatch:?}"
    );

    // Part 3: Direct `ItemOps::put_cas` refuses unlogged multi-chunk base writes,
    // while `BigtableEngine::put_item` routes them through coordinator-logged 2PC.
    let ops = ItemOps::new(&clean_client, &dt, 2);
    let direct_item = mk_wide("k_direct", "g_direct", "direct1");
    let direct_res =
        with_max_mutations_per_rpc(5, ops.put_cas(&ki, &direct_item, None, false)).await;
    assert!(
        matches!(direct_res, Err(StorageError::Internal(_))),
        "direct ItemOps::put_cas must refuse unlogged multi-chunk staging: {direct_res:?}"
    );
    with_max_mutations_per_rpc(
        5,
        engine1.put_item(
            &ki,
            direct_item.clone(),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        ),
    )
    .await
    .unwrap();
    let got_direct = engine1
        .get_item(&ki, &s_item(&[("pk", "k_direct")]))
        .await
        .unwrap()
        .unwrap();
    verify_generation_coherence(&got_direct).unwrap();
    let (scanned_direct, _) = engine1
        .scan(&ki, None, None, None, None, None)
        .await
        .unwrap();
    let scanned_row = scanned_direct
        .iter()
        .find(|it| it.get("pk") == Some(&AttributeValue::S("k_direct".into())))
        .expect("multi-chunk row must be visible via Scan");
    verify_generation_coherence(scanned_row).unwrap();

    let shadow_ops = ItemOps::new(&clean_client, &shadow_tbl, 2);
    let shadow_rk = row_key::encode_key(&s_item(&[("pk", "shd_probe")]), &ki.key_schema).unwrap();
    assert!(
        with_max_mutations_per_rpc(
            5,
            shadow_ops.put_shadow_versioned(&shadow_rk, &direct_item, 100),
        )
        .await
        .unwrap()
    );
    assert!(
        shadow_ops
            .delete_shadow_versioned(&shadow_rk, 200)
            .await
            .unwrap()
    );
    assert!(
        !with_max_mutations_per_rpc(
            5,
            shadow_ops.put_shadow_versioned(&shadow_rk, &direct_item, 150),
        )
        .await
        .unwrap(),
        "stale multi-chunk shadow put must be rejected when m:v is already higher"
    );
    assert!(
        with_max_mutations_per_rpc(
            5,
            shadow_ops.put_shadow_versioned(&shadow_rk, &direct_item, 250),
        )
        .await
        .unwrap(),
        "newer multi-chunk shadow put after delete must succeed cleanly"
    );
}

#[tokio::test]
async fn test_delayed_inline_shadow_write_does_not_resurrect_deleted_gsi_entry() {
    use extenddb_storage_bigtable::{gsi_worker, sweeper};
    use std::sync::atomic::Ordering;

    let Some((_client, engine_clean, engine_proxy, proxy, acct)) =
        setup_fault_proxy_context(2).await
    else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };
    let tbl = unique_name("tbl_delayed_shadow");
    engine_clean
        .create_table(&acct, single_hash_table_with_all_gsi(&tbl))
        .await
        .unwrap();
    let ki = engine_clean.table_key_info(&acct, &tbl).await.unwrap();
    let maps = ExpressionMaps::default();

    let _ = engine_proxy
        .get_item(&ki, &s_item(&[("pk", "u1")]))
        .await
        .unwrap();
    proxy.shadow_delay_ms.store(2400, Ordering::SeqCst);

    let ep = engine_proxy.clone();
    let ki_w1 = ki.clone();
    let w1 = tokio::spawn(async move {
        ep.put_item(
            &ki_w1,
            s_item(&[("pk", "u1"), ("gsi_pk", "stale_val")]),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .unwrap()
    });

    tokio::time::sleep(Duration::from_millis(120)).await;
    engine_clean
        .delete_item(&ki, &s_item(&[("pk", "u1")]), false, None, &maps, None)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(2100)).await;
    sweeper::sweep_once(&engine_clean).await.unwrap();
    w1.await.unwrap();
    sweeper::sweep_once(&engine_clean).await.unwrap();
    gsi_worker::sweep_once(&engine_clean).await.unwrap();

    let base_after = engine_clean
        .get_item(&ki, &s_item(&[("pk", "u1")]))
        .await
        .unwrap();
    let kc = parse_kc("gsi_pk = :v");
    let mut qmaps = ExpressionMaps::default();
    qmaps
        .values
        .insert("v".into(), AttributeValue::S("stale_val".into()));
    let (gsi_rows, _) = engine_clean
        .query(&ki, &kc, &qmaps, true, None, None, Some("gsi_idx"))
        .await
        .unwrap();

    assert!(base_after.is_none());
    assert!(
        gsi_rows.is_empty(),
        "Delayed inline GSI write must not resurrect deleted item in GSI: {gsi_rows:?}"
    );
}

#[tokio::test]
async fn test_sweeper_deferred_side_effect_recovery_skips_deleted_base_and_cleans_multihop_gsi() {
    use extenddb_storage_bigtable::{gsi_worker, sweeper};
    use std::sync::atomic::Ordering;

    let Some((_client, engine_clean, engine_proxy, proxy, acct)) =
        setup_fault_proxy_context(2).await
    else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };
    let tbl = unique_name("tbl_deferred_recovery");
    engine_clean
        .create_table(&acct, single_hash_table_with_all_gsi(&tbl))
        .await
        .unwrap();
    let ki = engine_clean.table_key_info(&acct, &tbl).await.unwrap();
    let maps = ExpressionMaps::default();
    let kc = parse_kc("gsi_pk = :v");

    // 1. Seed `initial_val`, update to `deleted_val` with dropped shadow RPCs, then delete `u1`.
    //    Even when sweeper initially hits transient shadow failures past the tombstone grace window,
    //    neither `initial_val` nor `deleted_val` may survive once recovery settles.
    engine_clean
        .put_item(
            &ki,
            s_item(&[("pk", "u1"), ("gsi_pk", "initial_val")]),
            false,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();

    proxy.shadow_drop_budget.store(12, Ordering::SeqCst);
    engine_proxy
        .put_item(
            &ki,
            s_item(&[("pk", "u1"), ("gsi_pk", "deleted_val")]),
            false,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();
    proxy.shadow_drop_budget.store(0, Ordering::SeqCst);

    engine_clean
        .delete_item(&ki, &s_item(&[("pk", "u1")]), false, None, &maps, None)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(2100)).await;
    proxy.shadow_drop_budget.store(12, Ordering::SeqCst);
    sweeper::sweep_once(&engine_proxy).await.unwrap();
    proxy.shadow_drop_budget.store(0, Ordering::SeqCst);

    sweeper::sweep_once(&engine_clean).await.unwrap();
    gsi_worker::sweep_once(&engine_clean).await.unwrap();

    let base_after = engine_clean
        .get_item(&ki, &s_item(&[("pk", "u1")]))
        .await
        .unwrap();
    assert!(base_after.is_none());

    for check_val in ["initial_val", "deleted_val"] {
        let mut qmaps = ExpressionMaps::default();
        qmaps
            .values
            .insert("v".into(), AttributeValue::S(check_val.into()));
        let (gsi_rows, _) = engine_clean
            .query(&ki, &kc, &qmaps, true, None, None, Some("gsi_idx"))
            .await
            .unwrap();
        assert!(
            gsi_rows.is_empty(),
            "Deferred post-commit recovery must not leave '{check_val}' in GSI: {gsi_rows:?}"
        );
    }

    // 2. Multi-hop GSI update (`hop_a -> hop_b [deferred] -> hop_c`) must still delete `hop_a`
    //    while skipping superseded `Put("hop_b")`.
    engine_clean
        .put_item(
            &ki,
            s_item(&[("pk", "u_hop"), ("gsi_pk", "hop_a")]),
            false,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();

    proxy.shadow_drop_budget.store(12, Ordering::SeqCst);
    engine_proxy
        .put_item(
            &ki,
            s_item(&[("pk", "u_hop"), ("gsi_pk", "hop_b")]),
            false,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();
    proxy.shadow_drop_budget.store(0, Ordering::SeqCst);

    engine_clean
        .put_item(
            &ki,
            s_item(&[("pk", "u_hop"), ("gsi_pk", "hop_c")]),
            false,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();

    sweeper::sweep_once(&engine_clean).await.unwrap();
    gsi_worker::sweep_once(&engine_clean).await.unwrap();

    for (val, expected_count) in [("hop_a", 0usize), ("hop_b", 0usize), ("hop_c", 1usize)] {
        let mut hm = ExpressionMaps::default();
        hm.values
            .insert("v".into(), AttributeValue::S(val.to_string()));
        let (rows, _) = engine_clean
            .query(&ki, &kc, &hm, true, None, None, Some("gsi_idx"))
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            expected_count,
            "unexpected GSI row count for {val}: {rows:?}"
        );
    }
}

#[tokio::test]
async fn test_ahead_of_clock_tombstone_reclamation_preserves_subsequent_gsi_inserts() {
    use extenddb_storage_bigtable::data::encoding::row_key;
    use extenddb_storage_bigtable::data::item_ops::{ItemOps, now_millis};
    use extenddb_storage_bigtable::transact::{
        ParticipantMutation, ParticipantRow, TxnCoordinator, TxnOpPayload,
    };
    use extenddb_storage_bigtable::{gsi_worker, sweeper};
    use std::sync::atomic::Ordering;

    let Some((client, engine_clean, engine_proxy, proxy, acct)) =
        setup_fault_proxy_context(2).await
    else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };
    let tbl = unique_name("tbl_ahead_tombstone");
    let desc = engine_clean
        .create_table(&acct, single_hash_table_with_all_gsi(&tbl))
        .await
        .unwrap();
    let ki = engine_clean.table_key_info(&acct, &tbl).await.unwrap();
    let dt = format!("t{}", &desc.table_id.replace('-', "")[..16]);
    let maps = ExpressionMaps::default();

    let item_va = s_item(&[("pk", "k_hot"), ("gsi_pk", "vA")]);
    let rk = row_key::encode_key(&item_va, &ki.key_schema).unwrap();

    // Seed row at an OCC version ahead of wall clock (e.g., sub-ms hot-key burst or peer clock skew).
    let v_ahead = now_millis() + 6_000;
    let coord = TxnCoordinator::new(&client, Duration::from_secs(2));
    let part = ParticipantRow {
        data_table: dt.clone(),
        row_key: rk.clone(),
    };
    let (_, started_at) = coord
        .open_with_token("txn-ahead-seed", std::slice::from_ref(&part), None)
        .await
        .unwrap();
    assert!(
        coord
            .place_intent_once("txn-ahead-seed", &part)
            .await
            .unwrap()
    );
    coord
        .try_commit_full(
            "txn-ahead-seed",
            started_at,
            &[ParticipantMutation {
                participant: part,
                payload: TxnOpPayload::Put {
                    item: item_va.clone(),
                },
                expected_version: None,
                row_existed: false,
                new_version: Some(v_ahead),
            }],
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

    let key_hot = s_item(&[("pk", "k_hot")]);
    let _ = engine_clean.get_item(&ki, &key_hot).await.unwrap();
    engine_clean
        .put_item(&ki, item_va.clone(), false, None, &maps, None)
        .await
        .unwrap();

    proxy.shadow_drop_budget.store(6, Ordering::SeqCst);
    engine_proxy
        .delete_item(&ki, &key_hot, false, None, &maps, None)
        .await
        .unwrap();
    proxy.shadow_drop_budget.store(0, Ordering::SeqCst);

    // Direct reclamation attempt while wall clock has not caught up to the tombstone version must refuse.
    let ops = ItemOps::new(&client, &dt, 2);
    assert!(
        !ops.reclaim_version_only_row(&rk, v_ahead + 2)
            .await
            .unwrap(),
        "reclaim_version_only_row must refuse while wall clock <= expected_version"
    );

    tokio::time::sleep(Duration::from_millis(1_300)).await;
    sweeper::sweep_once(&engine_clean).await.unwrap();

    tokio::time::sleep(Duration::from_millis(900)).await;
    sweeper::sweep_once(&engine_clean).await.unwrap();

    engine_clean
        .put_item(&ki, item_va, false, None, &maps, None)
        .await
        .unwrap();

    sweeper::sweep_once(&engine_clean).await.unwrap();
    gsi_worker::sweep_once(&engine_clean).await.unwrap();

    let base_after = engine_clean.get_item(&ki, &key_hot).await.unwrap();
    let kc = parse_kc("gsi_pk = :v");
    let mut qmaps = ExpressionMaps::default();
    qmaps
        .values
        .insert("v".into(), AttributeValue::S("vA".into()));
    let (gsi_after, _) = engine_clean
        .query(&ki, &kc, &qmaps, true, None, None, Some("gsi_idx"))
        .await
        .unwrap();

    assert!(base_after.is_some());
    assert_eq!(
        gsi_after.len(),
        1,
        "Re-inserted item after ahead-of-clock delete must retain its live GSI entry: {gsi_after:?}"
    );
}

#[tokio::test]
async fn test_concurrent_put_item_during_stream_disable_never_exceeds_ending_sequence_number() {
    use extenddb_core::types::UpdateTableInput;
    use std::sync::atomic::Ordering;

    let Some((clean_client, engine_clean, engine_writer, proxy_writer, acct)) =
        setup_fault_proxy_context(10).await
    else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let emulator_host = clean_client.emulator_host.clone().unwrap();
    let proxy_closer = FaultInjectionProxy::start(emulator_host).await;
    let closer_cfg = BigtableStorageConfig {
        project_id: clean_client.project_id.clone(),
        instance_id: clean_client.instance_id.clone(),
        emulator_host: Some(proxy_closer.listen_addr.clone()),
        pool_size: 1,
        intent_timeout_secs: 10,
        ..BigtableStorageConfig::default()
    };
    let closer_client = Arc::new(BigtableClient::connect(&closer_cfg).await.unwrap());
    let engine_closer = Arc::new(BigtableEngine::new(
        closer_client.clone(),
        closer_client,
        10,
    ));

    let tbl = unique_name("tbl_stream_seal");
    let desc = engine_clean
        .create_table(
            &acct,
            CreateTableInput {
                table_name: tbl.clone(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".into(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "pk".into(),
                    attribute_type: ScalarAttributeType::S,
                }],
                billing_mode: Some(BillingMode::PayPerRequest),
                deletion_protection_enabled: Some(false),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::NewAndOldImages),
                }),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    let stream_arn = desc.latest_stream_arn.clone().unwrap();
    let ki = engine_clean.table_key_info(&acct, &tbl).await.unwrap();
    let maps = ExpressionMaps::default();
    let capture = StreamCapture {
        view_type: StreamViewType::NewAndOldImages,
        user_identity: None,
        region: Arc::from("us-east-1"),
    };

    let target_shard = engine_clean
        .assign_shard(&acct, &tbl, "k_race")
        .await
        .unwrap();

    engine_clean
        .put_item(
            &ki,
            s_item(&[("pk", "k_race"), ("v", "seed")]),
            false,
            None,
            &maps,
            Some(&capture),
        )
        .await
        .unwrap();

    let _ = engine_writer
        .get_item(&ki, &s_item(&[("pk", "k_race")]))
        .await
        .unwrap();

    proxy_writer.intent_delay_ms.store(120, Ordering::SeqCst);
    proxy_writer
        .stream_record_delay_ms
        .store(320, Ordering::SeqCst);
    proxy_closer
        .ending_seq_delay_ms
        .store(180, Ordering::SeqCst);

    let ew = engine_writer.clone();
    let ki_w = ki.clone();
    let cap_w = StreamCapture {
        view_type: StreamViewType::NewAndOldImages,
        user_identity: None,
        region: Arc::from("us-east-1"),
    };
    let w_task = tokio::spawn(async move {
        ew.put_item(
            &ki_w,
            s_item(&[("pk", "k_race"), ("v", "after_seal")]),
            false,
            None,
            &ExpressionMaps::default(),
            Some(&cap_w),
        )
        .await
        .unwrap()
    });

    tokio::time::sleep(Duration::from_millis(40)).await;
    let disable_in: UpdateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": tbl,
        "StreamSpecification": {
            "StreamEnabled": false
        }
    }))
    .unwrap();
    engine_closer
        .update_table(&acct, disable_in)
        .await
        .expect("update_table Disable streams");

    w_task.await.unwrap();

    let sdesc = engine_clean
        .describe_stream(
            &acct,
            &DescribeStreamInput {
                stream_arn: stream_arn.clone(),
                limit: None,
                exclusive_start_shard_id: None,
            },
        )
        .await
        .unwrap();
    let shard_desc = sdesc
        .shards
        .iter()
        .find(|s| s.shard_id == target_shard)
        .unwrap();
    let ending_seq = shard_desc
        .sequence_number_range
        .ending_sequence_number
        .clone()
        .expect("shard is closed with EndingSequenceNumber");

    let (recs, _) = engine_clean
        .get_stream_records(&acct, &stream_arn, &target_shard, None, 100)
        .await
        .unwrap();
    assert!(!recs.is_empty());
    let last_seq = recs.last().unwrap().dynamodb.sequence_number.clone();
    assert!(
        last_seq <= ending_seq,
        "Stream records must never exceed closed shard EndingSequenceNumber ({last_seq} > {ending_seq})"
    );

    // Defense-in-depth check: even if an unexposed record slot exists past `ending_seq`,
    // `get_stream_records` clamps at `ending_seq`.
    let post_seal_seq = extenddb_storage_bigtable::streams::slot_sequence_number(&target_shard, 99);
    assert!(post_seal_seq > ending_seq);
    let mut synthetic = recs.last().unwrap().clone();
    synthetic.dynamodb.sequence_number = post_seal_seq.clone();
    let cat = extenddb_storage_bigtable::catalog::Catalog::new(&clean_client);
    cat.put(
        &extenddb_storage_bigtable::catalog::keys::stream_record(&target_shard, &post_seal_seq),
        &serde_json::to_value(&synthetic).unwrap(),
    )
    .await
    .unwrap();

    let (clamped_recs, _) = engine_clean
        .get_stream_records(&acct, &stream_arn, &target_shard, None, 100)
        .await
        .unwrap();
    assert!(
        clamped_recs
            .iter()
            .all(|r| r.dynamodb.sequence_number <= ending_seq),
        "get_stream_records must clamp at EndingSequenceNumber"
    );
    let (after_end_recs, next_after) = engine_clean
        .get_stream_records(&acct, &stream_arn, &target_shard, Some(&ending_seq), 100)
        .await
        .unwrap();
    assert!(after_end_recs.is_empty());
    assert!(next_after.is_none());
}

#[tokio::test]
async fn test_slow_write_during_online_gsi_creation_preserves_and_updates_active_gsi_entry() {
    use extenddb_core::types::UpdateTableInput;
    use extenddb_storage_bigtable::{gsi_worker, sweeper};
    use std::sync::atomic::Ordering;

    let Some((_client, engine_clean, engine_proxy, proxy, acct)) =
        setup_fault_proxy_context(2).await
    else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };
    let tbl = unique_name("tbl_online_gsi_drift");
    engine_clean
        .create_table(
            &acct,
            CreateTableInput {
                table_name: tbl.clone(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".into(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "pk".into(),
                    attribute_type: ScalarAttributeType::S,
                }],
                billing_mode: Some(BillingMode::PayPerRequest),
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    let ki = engine_clean.table_key_info(&acct, &tbl).await.unwrap();
    let maps = ExpressionMaps::default();

    engine_clean
        .put_item(
            &ki,
            s_item(&[("pk", "k1"), ("gsi_pk", "v1"), ("payload", "initial")]),
            false,
            None,
            &maps,
            None,
        )
        .await
        .unwrap();

    let _ = engine_proxy
        .get_item(&ki, &s_item(&[("pk", "k1")]))
        .await
        .unwrap();

    proxy.updated_val_delay_ms.store(1500, Ordering::SeqCst);
    proxy.shadow_pass_before_drop.store(2, Ordering::SeqCst);
    proxy.shadow_drop_budget.store(6, Ordering::SeqCst);

    let ep = engine_proxy.clone();
    let ki_slow = ki.clone();
    let slow_writer = tokio::spawn(async move {
        ep.put_item(
            &ki_slow,
            s_item(&[("pk", "k1"), ("gsi_pk", "v1"), ("payload", "updated")]),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .unwrap()
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    let add_gsi: UpdateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": tbl,
        "AttributeDefinitions": [
            { "AttributeName": "pk", "AttributeType": "S" },
            { "AttributeName": "gsi_pk", "AttributeType": "S" }
        ],
        "GlobalSecondaryIndexUpdates": [
            {
                "Create": {
                    "IndexName": "G1",
                    "KeySchema": [{ "AttributeName": "gsi_pk", "KeyType": "HASH" }],
                    "Projection": { "ProjectionType": "ALL" }
                }
            }
        ]
    }))
    .unwrap();
    let updated_desc = engine_clean.update_table(&acct, add_gsi).await.unwrap();
    assert_eq!(
        updated_desc.global_secondary_indexes.as_ref().unwrap()[0].index_status,
        "ACTIVE"
    );

    slow_writer.await.unwrap();
    proxy.shadow_drop_budget.store(0, Ordering::SeqCst);

    sweeper::sweep_once(&engine_clean).await.unwrap();
    gsi_worker::sweep_once(&engine_clean).await.unwrap();

    let live_base = engine_clean
        .get_item(&ki, &s_item(&[("pk", "k1")]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        live_base.get("payload"),
        Some(&AttributeValue::S("updated".to_string()))
    );

    let kc = parse_kc("gsi_pk = :v");
    let mut qmaps = ExpressionMaps::default();
    qmaps
        .values
        .insert("v".into(), AttributeValue::S("v1".into()));
    let fresh_ki = engine_clean.table_key_info(&acct, &tbl).await.unwrap();
    let (gsi_rows, _) = engine_clean
        .query(&fresh_ki, &kc, &qmaps, true, None, None, Some("G1"))
        .await
        .unwrap();

    assert_eq!(
        gsi_rows.len(),
        1,
        "Online GSI entry must remain present and updated after slow-write drift repair: {gsi_rows:?}"
    );
    assert_eq!(
        gsi_rows[0].get("payload"),
        Some(&AttributeValue::S("updated".to_string()))
    );
}

#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_query_and_scan_resolve_deferred_committed_intents_and_lsi_writes() {
    use extenddb_core::types::LsiInput;

    let Some(ctx) = setup_emulator_context().await else {
        return;
    };
    let table_name = unique_name("qs_intent_lsi");
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: table_name.clone(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".into(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".into(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "lsi_sk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ],
                billing_mode: Some(BillingMode::PayPerRequest),
                local_secondary_indexes: Some(vec![LsiInput {
                    index_name: "L1".into(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "pk".into(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "lsi_sk".into(),
                            key_type: KeyType::Range,
                        },
                    ],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                }]),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();

    let ki = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();

    // 1. Commit a fenced write without running post-commit `roll_forward`, and
    // query the LSI FIRST (before any base-table `Query`): `ensure_lsi_read_after_write`
    // must roll forward the `COMMITTED` coordinator entry so `L1` returns the item!
    let mut item1 = BTreeMap::new();
    item1.insert("pk".into(), AttributeValue::S("p1".into()));
    item1.insert("sk".into(), AttributeValue::S("s1".into()));
    item1.insert("lsi_sk".into(), AttributeValue::S("l_first".into()));
    item1.insert("val".into(), AttributeValue::S("lsi_first_visible".into()));
    let _tid1 = ctx
        .engine
        .debug_fenced_put_commit_only(&ki, item1, None)
        .await
        .unwrap();

    let kc = parse_kc("pk = :p");
    let mut maps = ExpressionMaps::default();
    maps.values
        .insert("p".into(), AttributeValue::S("p1".into()));

    let (lsi_rows, _) = ctx
        .engine
        .query(&ki, &kc, &maps, true, None, None, Some("L1"))
        .await
        .unwrap();
    assert_eq!(lsi_rows.len(), 1);
    assert_eq!(
        lsi_rows[0].get("lsi_sk"),
        Some(&AttributeValue::S("l_first".into()))
    );

    // 2. Stage a second deferred `COMMITTED` write and query the base table FIRST:
    // base-table `Query` must resolve `m:intent:<tid>` directly from `__extenddb_txn_log__`.
    let mut item2 = BTreeMap::new();
    item2.insert("pk".into(), AttributeValue::S("p1".into()));
    item2.insert("sk".into(), AttributeValue::S("s2".into()));
    item2.insert("lsi_sk".into(), AttributeValue::S("l_second".into()));
    item2.insert("val".into(), AttributeValue::S("committed_via_log".into()));
    let _tid2 = ctx
        .engine
        .debug_fenced_put_commit_only(&ki, item2, None)
        .await
        .unwrap();

    let (queried, _) = ctx
        .engine
        .query(&ki, &kc, &maps, true, None, None, None)
        .await
        .unwrap();
    assert_eq!(queried.len(), 2);
    assert_eq!(
        queried[1].get("val"),
        Some(&AttributeValue::S("committed_via_log".into()))
    );
}

#[tokio::test]
async fn test_query_and_scan_resolve_committed_2pc_intents_and_skip_tombstones() {
    use extenddb_storage_bigtable::data::encoding::row_key;
    use extenddb_storage_bigtable::data::item_ops::{ItemOps, next_occ_version};
    use extenddb_storage_bigtable::transact::{
        ParticipantMutation, ParticipantRow, TxnCoordinator, TxnOpPayload,
    };

    let Some(ctx) = setup_emulator_context().await else {
        return;
    };
    let tbl = unique_name("tbl_qs_intent_res");
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: tbl.clone(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".into(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".into(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ],
                billing_mode: Some(BillingMode::PayPerRequest),
                deletion_protection_enabled: Some(false),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    let ki = ctx
        .engine
        .table_key_info(&ctx.account_id, &tbl)
        .await
        .unwrap();
    let dt = ctx.engine.data_table_for(&ki).await.unwrap();
    let maps = ExpressionMaps::default();

    // Seed s1..s5, then delete s1 and s2 so they become pure `m:v` tombstones.
    for sk in ["s1", "s2", "s3", "s4", "s5"] {
        let item = BTreeMap::from([
            ("pk".into(), AttributeValue::S("p1".into())),
            ("sk".into(), AttributeValue::S(sk.into())),
            ("v".into(), AttributeValue::S("base".into())),
        ]);
        ctx.engine
            .put_item(&ki, item, false, None, &maps, None)
            .await
            .unwrap();
    }
    for sk in ["s1", "s2"] {
        let key = BTreeMap::from([
            ("pk".into(), AttributeValue::S("p1".into())),
            ("sk".into(), AttributeValue::S(sk.into())),
        ]);
        ctx.engine
            .delete_item(&ki, &key, false, None, &maps, None)
            .await
            .unwrap();
    }

    // Stage a `COMMITTED` 2PC transaction BEFORE `roll_forward`:
    // - `s0` has NO `d:*` cells yet (brand-new Put), only `m:intent:<tid>`
    // - `s3` has `d:*` cells + `m:intent:<tid>` for a committed Delete
    // - `s4` has `d:*` cells + `m:intent:<tid>` for a committed Put
    let mk_key = |sk: &str| {
        BTreeMap::from([
            ("pk".into(), AttributeValue::S("p1".into())),
            ("sk".into(), AttributeValue::S(sk.into())),
        ])
    };
    let p0 = ParticipantRow {
        data_table: dt.clone(),
        row_key: row_key::encode_key(&mk_key("s0"), &ki.key_schema).unwrap(),
    };
    let p3 = ParticipantRow {
        data_table: dt.clone(),
        row_key: row_key::encode_key(&mk_key("s3"), &ki.key_schema).unwrap(),
    };
    let p4 = ParticipantRow {
        data_table: dt.clone(),
        row_key: row_key::encode_key(&mk_key("s4"), &ki.key_schema).unwrap(),
    };

    let coord = TxnCoordinator::new(&ctx.client, Duration::from_secs(60));
    let txn_id = TxnCoordinator::new_txn_id();
    let parts = vec![p0.clone(), p3.clone(), p4.clone()];
    let (opened, started_at) = coord.open(&txn_id, &parts).await.unwrap();
    assert!(opened);
    for p in &parts {
        assert!(coord.place_intent_once(&txn_id, p).await.unwrap());
    }

    let ops = ItemOps::new(&ctx.client, &dt, 60);
    let snap_s3 = ops.get_row_snapshot_by_raw_key(&p3.row_key).await.unwrap();
    let snap_s4 = ops.get_row_snapshot_by_raw_key(&p4.row_key).await.unwrap();

    let item_s0 = BTreeMap::from([
        ("pk".into(), AttributeValue::S("p1".into())),
        ("sk".into(), AttributeValue::S("s0".into())),
        ("v".into(), AttributeValue::S("new_from_2pc".into())),
    ]);
    let item_s4 = BTreeMap::from([
        ("pk".into(), AttributeValue::S("p1".into())),
        ("sk".into(), AttributeValue::S("s4".into())),
        ("v".into(), AttributeValue::S("committed_2pc".into())),
    ]);
    let committed_muts = vec![
        ParticipantMutation {
            participant: p0,
            payload: TxnOpPayload::Put { item: item_s0 },
            expected_version: None,
            row_existed: false,
            new_version: Some(next_occ_version(None)),
        },
        ParticipantMutation {
            participant: p3,
            payload: TxnOpPayload::Delete,
            expected_version: snap_s3.version,
            row_existed: snap_s3.item.is_some(),
            new_version: Some(next_occ_version(snap_s3.version)),
        },
        ParticipantMutation {
            participant: p4,
            payload: TxnOpPayload::Put { item: item_s4 },
            expected_version: snap_s4.version,
            row_existed: snap_s4.item.is_some(),
            new_version: Some(next_occ_version(snap_s4.version)),
        },
    ];
    let commit_ts = coord
        .try_commit_full(&txn_id, started_at, &committed_muts, None, None)
        .await
        .unwrap();
    assert!(commit_ts.is_some());

    let kc = parse_kc("pk = :p");
    let mut qmaps = ExpressionMaps::default();
    qmaps
        .values
        .insert("p".into(), AttributeValue::S("p1".into()));

    // First verify standalone `QueryScan::new` (without engine cooperative roll-forward)
    // reloads `m:v` and resolves both `s0` (`expected_version = None`), `s3` (`Delete` on
    // `Some(ver)`), and `s4` (`Put` on `Some(ver)`) while skipping `s1`/`s2` tombstones.
    let standalone_qs =
        extenddb_storage_bigtable::data::query_scan::QueryScan::new(&ctx.client, &dt);
    let (standalone_p1, standalone_lek1) = standalone_qs
        .query(&ki, false, &kc, &qmaps, true, Some(2), None)
        .await
        .unwrap();
    assert_eq!(
        standalone_p1.len(),
        2,
        "standalone page1: {standalone_p1:?}"
    );
    assert_eq!(
        standalone_p1[0].get("sk"),
        Some(&AttributeValue::S("s0".into()))
    );
    assert_eq!(
        standalone_p1[1].get("sk"),
        Some(&AttributeValue::S("s4".into()))
    );
    assert_eq!(
        standalone_p1[1].get("v"),
        Some(&AttributeValue::S("committed_2pc".into()))
    );
    assert!(standalone_lek1.is_some());

    let (page1, lek1) = ctx
        .engine
        .query(&ki, &kc, &qmaps, true, Some(2), None, None)
        .await
        .unwrap();
    assert_eq!(page1.len(), 2, "Query Limit=2 page1: {page1:?}");
    assert_eq!(page1[0].get("sk"), Some(&AttributeValue::S("s0".into())));
    assert_eq!(
        page1[0].get("v"),
        Some(&AttributeValue::S("new_from_2pc".into()))
    );
    assert_eq!(page1[1].get("sk"), Some(&AttributeValue::S("s4".into())));
    assert_eq!(
        page1[1].get("v"),
        Some(&AttributeValue::S("committed_2pc".into()))
    );
    assert!(lek1.is_some());

    let (page2, _) = ctx
        .engine
        .query(&ki, &kc, &qmaps, true, Some(2), lek1.as_ref(), None)
        .await
        .unwrap();
    assert_eq!(page2.len(), 1, "Query Limit=2 page2: {page2:?}");
    assert_eq!(page2[0].get("sk"), Some(&AttributeValue::S("s5".into())));

    let (scan_p1, scan_lek1) = ctx
        .engine
        .scan(&ki, Some(2), None, None, None, None)
        .await
        .unwrap();
    assert_eq!(scan_p1.len(), 2, "Scan Limit=2 page1: {scan_p1:?}");
    assert_eq!(scan_p1[0].get("sk"), Some(&AttributeValue::S("s0".into())));
    assert_eq!(scan_p1[1].get("sk"), Some(&AttributeValue::S("s4".into())));
    let (scan_p2, _) = ctx
        .engine
        .scan(&ki, Some(2), scan_lek1.as_ref(), None, None, None)
        .await
        .unwrap();
    assert_eq!(scan_p2.len(), 1);
    assert_eq!(scan_p2[0].get("sk"), Some(&AttributeValue::S("s5".into())));
}

#[tokio::test]
async fn test_catalog_put_shadows_higher_ts_and_expired_lock_cannot_renew() {
    use extenddb_storage_bigtable::catalog::{CATALOG_TABLE, CF, Catalog, QUALIFIER};

    let Some(ctx) = setup_emulator_context().await else {
        return;
    };
    let cat = Catalog::new(&ctx.client);
    let key = format!("setting:skew_probe_{}", Uuid::new_v4().simple());

    // Simulate a peer with a +30s skewed clock having written a `c:j` cell in the future.
    let future_ts_micros =
        (time::OffsetDateTime::now_utc().unix_timestamp() + 30).saturating_mul(1_000_000);
    let mut data = ctx.client.data();
    data.mutate_row(MutateRowRequest {
        table_name: ctx.client.full_table_name(CATALOG_TABLE),
        app_profile_id: ctx.client.app_profile_id.clone().unwrap_or_default(),
        row_key: key.as_bytes().to_vec(),
        mutations: vec![Mutation {
            mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
                family_name: CF.into(),
                column_qualifier: QUALIFIER.to_vec(),
                timestamp_micros: future_ts_micros,
                value: serde_json::to_vec(&serde_json::json!({"v": "stale_future"})).unwrap(),
            })),
        }],
        ..MutateRowRequest::default()
    })
    .await
    .unwrap();
    assert_eq!(
        cat.get(&key).await.unwrap(),
        Some(serde_json::json!({"v": "stale_future"}))
    );

    // Both `Catalog::put` and `Catalog::put_if_present` must stamp strictly past
    // `future_ts_micros` so the new value is never shadowed.
    cat.put(&key, &serde_json::json!({"v": "fresh_put"}))
        .await
        .unwrap();
    assert_eq!(
        cat.get(&key).await.unwrap(),
        Some(serde_json::json!({"v": "fresh_put"}))
    );

    assert!(
        cat.put_if_present(&key, &serde_json::json!({"v": "fresh_present"}))
            .await
            .unwrap()
    );
    assert_eq!(
        cat.get(&key).await.unwrap(),
        Some(serde_json::json!({"v": "fresh_present"}))
    );

    // Also verify `Catalog::put` overwrites a corrupt non-JSON `c:j` cell at an even higher ts.
    data.mutate_row(MutateRowRequest {
        table_name: ctx.client.full_table_name(CATALOG_TABLE),
        app_profile_id: ctx.client.app_profile_id.clone().unwrap_or_default(),
        row_key: key.as_bytes().to_vec(),
        mutations: vec![Mutation {
            mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
                family_name: CF.into(),
                column_qualifier: QUALIFIER.to_vec(),
                timestamp_micros: future_ts_micros + 5_000_000,
                value: b"not-valid-json".to_vec(),
            })),
        }],
        ..MutateRowRequest::default()
    })
    .await
    .unwrap();
    cat.put(&key, &serde_json::json!({"v": "recovered_after_corrupt"}))
        .await
        .unwrap();
    assert_eq!(
        cat.get(&key).await.unwrap(),
        Some(serde_json::json!({"v": "recovered_after_corrupt"}))
    );

    // An expired catalog lock must NOT be resurrected by `renew_lock`.
    let lock_name = format!("exp_probe_{}", Uuid::new_v4().simple());
    assert!(
        cat.try_lock(&lock_name, "holder-1", Duration::from_millis(80))
            .await
            .unwrap()
    );
    tokio::time::sleep(Duration::from_millis(140)).await;
    assert!(
        !cat.renew_lock(&lock_name, "holder-1", Duration::from_secs(30))
            .await
            .unwrap(),
        "expired lock cell must not be renewed after its expiry timestamp passes"
    );
}

#[tokio::test]
async fn test_populated_catalog_without_ledger_reports_needs_attention() {
    use extenddb_storage::migrations::CatalogStatus;

    let suffix = &Uuid::new_v4().to_string().replace('-', "")[..8];
    let mut cfg = BigtableStorageConfig {
        project_id: format!("uninit-proj-{suffix}"),
        instance_id: format!("uninit-inst-{suffix}"),
        ..BigtableStorageConfig::default()
    };
    let Some((host, client)) = resolve_reachable_emulator_host(&cfg).await else {
        return;
    };
    cfg.emulator_host = Some(host);

    let bootstrapper = BigtableBootstrapper::new(cfg.clone());
    bootstrapper.create_catalog_db().await.unwrap();
    bootstrapper.create_data_db().await.unwrap();

    // Populate an application row in `__extenddb_catalog__` WITHOUT running migrations.
    let mut data = client.data();
    data.mutate_row(MutateRowRequest {
        table_name: client.full_table_name("__extenddb_catalog__"),
        app_profile_id: String::new(),
        row_key: b"account:123456789012".to_vec(),
        mutations: vec![Mutation {
            mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
                family_name: "c".into(),
                column_qualifier: vec![],
                timestamp_micros: 1_700_000_000_000,
                value: b"{\"account_id\":\"123456789012\"}".to_vec(),
            })),
        }],
        ..MutateRowRequest::default()
    })
    .await
    .unwrap();

    let conn = cfg.connection_string();
    let status = BigtableCatalogMigrations
        .status(&Target::at(&conn))
        .await
        .unwrap();
    assert!(
        matches!(status, CatalogStatus::NeedsAttention { .. }),
        "populated catalog with raw cell missing ledger/version must report NeedsAttention, got {status:?}"
    );

    // Also verify standard `c:j` JSON application rows and `run` refusal.
    data.mutate_row(MutateRowRequest {
        table_name: client.full_table_name("__extenddb_catalog__"),
        app_profile_id: String::new(),
        row_key: b"table_meta:123456789012:Orders".to_vec(),
        mutations: vec![Mutation {
            mutation: Some(mutation::Mutation::SetCell(mutation::SetCell {
                family_name: "c".into(),
                column_qualifier: b"j".to_vec(),
                timestamp_micros: 1_700_000_000_000,
                value: b"{\"table_name\":\"Orders\"}".to_vec(),
            })),
        }],
        ..MutateRowRequest::default()
    })
    .await
    .unwrap();
    let lock_err = BigtableCatalogMigrations
        .lock(&Target::at(&conn), Duration::from_secs(5))
        .await
        .expect_err("lock on populated uninitialized catalog must fail with NeedsAttention");
    assert!(
        matches!(
            lock_err,
            extenddb_storage::migrations::MigrationError::Status(
                CatalogStatus::NeedsAttention { .. }
            )
        ),
        "expected MigrationError::Status(NeedsAttention), got {lock_err:?}"
    );

    // Also verify `creating = true` (`init`) refuses a foreign/populated uninitialized
    // `__extenddb_catalog__` with `MigrationError::Refused` without writing any lock cell.
    let creating_target = Target {
        location: conn,
        data_store: Some(cfg.instance_id.clone()),
        data_connection: Some(cfg.instance_id.clone()),
        ..Target::default()
    };
    let creating_err = BigtableCatalogMigrations
        .lock(&creating_target, Duration::from_secs(2))
        .await
        .expect_err("creating lock on populated uninitialized catalog must be Refused");
    assert!(
        matches!(
            creating_err,
            extenddb_storage::migrations::MigrationError::Refused(_)
        ),
        "expected MigrationError::Refused, got {creating_err:?}"
    );
    assert!(
        extenddb_storage_bigtable::catalog::Catalog::new(&client)
            .get("lock:catalog_migrations")
            .await
            .unwrap()
            .is_none(),
        "refused init lock must write nothing to foreign catalog table"
    );
}

#[tokio::test]
async fn test_table_id_index_lookup_and_recreated_table_ignores_stale_ttl_entry() {
    use extenddb_core::types::DeleteTableInput;

    let Some(ctx) = setup_emulator_context().await else {
        return;
    };
    let table_name = unique_name("ttl_recreate");
    let make_input = |tname: &str| CreateTableInput {
        table_name: tname.to_owned(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".into(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".into(),
                attribute_type: ScalarAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gpk".into(),
                attribute_type: ScalarAttributeType::S,
            },
        ],
        billing_mode: Some(BillingMode::PayPerRequest),
        global_secondary_indexes: Some(vec![GsiInput {
            index_name: "G1".into(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gpk".into(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
            warm_throughput: None,
        }]),
        ..CreateTableInput::default()
    };

    let desc1 = ctx
        .engine
        .create_table(&ctx.account_id, make_input(&table_name))
        .await
        .unwrap();

    // Cold-engine `index_info_by_table_id` must resolve via `table_id_index:<id>`.
    let fresh_engine = BigtableEngine::new(ctx.client.clone(), ctx.client.clone(), 10);
    let idx_info = fresh_engine
        .index_info_by_table_id(&desc1.table_id, "G1")
        .await
        .unwrap();
    assert_eq!(idx_info.index_name, "G1");

    ctx.engine
        .update_ttl(&ctx.account_id, &table_name, "exp", true)
        .await
        .unwrap();
    ctx.engine
        .create_ttl_index(&ctx.account_id, &table_name, "exp")
        .await
        .unwrap();

    let ki1 = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let past_exp = (time::OffsetDateTime::now_utc().unix_timestamp() - 60).to_string();
    let mut item1 = BTreeMap::new();
    item1.insert("pk".into(), AttributeValue::S("same_pk".into()));
    item1.insert("exp".into(), AttributeValue::N(past_exp.clone()));
    ctx.engine
        .put_item(&ki1, item1, false, None, &ExpressionMaps::default(), None)
        .await
        .unwrap();

    // Delete and recreate the table under the exact same `(account_id, table_name)`
    // with TTL enabled and a live item at `"same_pk"` carrying a past `exp` (or
    // future `exp`): the old table's stale TTL entry must be retired by `table_id`
    // mismatch without touching the new table!
    ctx.engine
        .delete_table(
            &ctx.account_id,
            DeleteTableInput {
                table_name: table_name.clone(),
            },
        )
        .await
        .unwrap();

    let desc2 = ctx
        .engine
        .create_table(&ctx.account_id, make_input(&table_name))
        .await
        .unwrap();
    assert_ne!(desc1.table_id, desc2.table_id);
    ctx.engine
        .update_ttl(&ctx.account_id, &table_name, "exp", true)
        .await
        .unwrap();
    ctx.engine
        .create_ttl_index(&ctx.account_id, &table_name, "exp")
        .await
        .unwrap();

    let ki2 = ctx
        .engine
        .table_key_info(&ctx.account_id, &table_name)
        .await
        .unwrap();
    let future_exp = (time::OffsetDateTime::now_utc().unix_timestamp() + 3_600).to_string();
    let mut item2 = BTreeMap::new();
    item2.insert("pk".into(), AttributeValue::S("same_pk".into()));
    item2.insert("exp".into(), AttributeValue::N(future_exp));
    ctx.engine
        .put_item(&ki2, item2, false, None, &ExpressionMaps::default(), None)
        .await
        .unwrap();

    sweep_once(&ctx.engine).await.unwrap();
    let still_live = ctx
        .engine
        .get_item(
            &ki2,
            &BTreeMap::from([("pk".into(), AttributeValue::S("same_pk".into()))]),
        )
        .await
        .unwrap();
    assert!(
        still_live.is_some(),
        "recreated table's item must remain live when sweeping old table_id's stale TTL entry"
    );
}

#[cfg(feature = "test-hooks")]
#[tokio::test]
async fn test_sweeper_reclaims_due_tombstones_despite_young_pending_txn_on_other_table() {
    use extenddb_storage_bigtable::sweeper;

    let Some(ctx) = setup_emulator_context().await else {
        return;
    };
    let tbl_a = unique_name("tomb_tbl_a");
    let tbl_b = unique_name("tomb_tbl_b");
    for t in [&tbl_a, &tbl_b] {
        ctx.engine
            .create_table(
                &ctx.account_id,
                CreateTableInput {
                    table_name: t.clone(),
                    key_schema: vec![KeySchemaElement {
                        attribute_name: "pk".into(),
                        key_type: KeyType::Hash,
                    }],
                    attribute_definitions: vec![AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::S,
                    }],
                    billing_mode: Some(BillingMode::PayPerRequest),
                    ..CreateTableInput::default()
                },
            )
            .await
            .unwrap();
    }

    let ki_a = ctx
        .engine
        .table_key_info(&ctx.account_id, &tbl_a)
        .await
        .unwrap();
    let ki_b = ctx
        .engine
        .table_key_info(&ctx.account_id, &tbl_b)
        .await
        .unwrap();
    let key_a = BTreeMap::from([("pk".into(), AttributeValue::S("row_a".into()))]);
    let key_protected = BTreeMap::from([("pk".into(), AttributeValue::S("row_protected".into()))]);
    for k in [&key_a, &key_protected] {
        ctx.engine
            .put_item(
                &ki_a,
                k.clone(),
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .unwrap();
        ctx.engine
            .delete_item(&ki_a, k, false, None, &ExpressionMaps::default(), None)
            .await
            .unwrap();
    }

    // Open a young `< 60s` `PENDING` transaction on `tbl_b`.
    let item_b = BTreeMap::from([("pk".into(), AttributeValue::S("row_b".into()))]);
    let _pending_tid = ctx
        .engine
        .debug_fenced_put_open_intent_only(&ki_b, &item_b)
        .await
        .unwrap();

    // Stage a young (`start > cutoff`) unrolled `COMMITTED` entry targeting `row_protected`
    // on `tbl_a`: `row_protected`'s tombstone must remain guarded while `row_a` on the SAME
    // table `tbl_a` is reclaimed without starvation.
    let dt_a = ctx.engine.data_table_for(&ki_a).await.unwrap();
    let rk_a =
        extenddb_storage_bigtable::data::encoding::row_key::encode_key(&key_a, &ki_a.key_schema)
            .unwrap();
    let rk_protected = extenddb_storage_bigtable::data::encoding::row_key::encode_key(
        &key_protected,
        &ki_a.key_schema,
    )
    .unwrap();
    let coord = extenddb_storage_bigtable::transact::TxnCoordinator::new(
        &ctx.client,
        Duration::from_secs(60),
    );
    let young_commit_id = extenddb_storage_bigtable::transact::TxnCoordinator::new_txn_id();
    let prot_part = extenddb_storage_bigtable::transact::ParticipantRow {
        data_table: dt_a.clone(),
        row_key: rk_protected.clone(),
    };
    let (opened, started_at) = coord
        .open(&young_commit_id, std::slice::from_ref(&prot_part))
        .await
        .unwrap();
    assert!(opened);
    coord
        .try_commit_full(
            &young_commit_id,
            started_at,
            &[extenddb_storage_bigtable::transact::ParticipantMutation {
                participant: prot_part,
                payload: extenddb_storage_bigtable::transact::TxnOpPayload::Put {
                    item: key_protected.clone(),
                },
                expected_version: None,
                row_existed: false,
                new_version: Some(1),
            }],
            None,
            None,
        )
        .await
        .unwrap();

    sweeper::sweep_once_with_tombstone_cutoff(&ctx.engine, Some(u64::MAX))
        .await
        .unwrap();

    let mut data = ctx.client.data();
    let raw_rows_a = data
        .read_rows(ReadRowsRequest {
            table_name: ctx.client.full_table_name(&dt_a),
            app_profile_id: String::new(),
            rows: Some(RowSet {
                row_keys: vec![rk_a],
                row_ranges: vec![],
            }),
            ..ReadRowsRequest::default()
        })
        .await
        .unwrap();
    assert!(
        raw_rows_a.is_empty(),
        "due pure-tombstone row_a on tbl_a must be reclaimed despite young PENDING/COMMITTED txns on other rows"
    );

    let raw_rows_protected = data
        .read_rows(ReadRowsRequest {
            table_name: ctx.client.full_table_name(&dt_a),
            app_profile_id: String::new(),
            rows: Some(RowSet {
                row_keys: vec![rk_protected],
                row_ranges: vec![],
            }),
            ..ReadRowsRequest::default()
        })
        .await
        .unwrap();
    assert_eq!(
        raw_rows_protected.len(),
        1,
        "tombstone for row with unrolled young COMMITTED mutation must not be prematurely reclaimed"
    );
}

#[tokio::test]
async fn test_leased_secret_paging_and_4kb_ttl_digest_eviction() {
    use extenddb_storage::secret_keys::SecretTable;
    use extenddb_storage_bigtable::catalog::Catalog;

    let Some(ctx) = setup_emulator_context().await else {
        return;
    };

    // 1. Bounded `read_secret_page` (`page_size = 2` over >= 5 keys) via `LockedStores::management()`.
    let conn = ctx.config.connection_string();
    let held = BigtableCatalogMigrations
        .lock(&Target::at(&conn), Duration::from_secs(5))
        .await
        .unwrap();
    let stores = BigtableCatalogMigrations
        .locked_stores(&held, None)
        .await
        .unwrap();
    let mgmt = stores.management();
    let acct_sec = "888899990000";
    mgmt.create_account(acct_sec, "sec_acct").await.unwrap();
    mgmt.create_user(acct_sec, "u_sec", Some("hash"))
        .await
        .unwrap();
    let cat = Catalog::new(&ctx.client);
    let prefix_tag = &Uuid::new_v4().simple().to_string()[..8].to_uppercase();
    for idx in 1..=5 {
        let akid = format!("AKIAEXTENDDB{prefix_tag}{idx:02}");
        cat.put(
            &format!("access_key:{akid}"),
            &serde_json::json!({
                "access_key_id": akid,
                "account_id": acct_sec,
                "user_name": "u_sec",
                "secret_key_encrypted_b64": "AQIDBA==",
                "is_active": true,
                "created_at": "2026-01-01T00:00:00Z",
            }),
        )
        .await
        .unwrap();
    }

    let mut matched = 0usize;
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;
    loop {
        let page = mgmt
            .read_secret_page(SecretTable::AccessKeys, cursor.as_deref(), 2)
            .await
            .unwrap();
        assert!(page.rows.len() <= 2);
        pages += 1;
        for rec in page.rows {
            if rec
                .access_key_id
                .starts_with(&format!("AKIAEXTENDDB{prefix_tag}"))
            {
                matched += 1;
            }
        }
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(matched, 5);
    assert!(pages >= 3);
    drop(stores);
    BigtableCatalogMigrations.release(held).await;

    // 2. Near-4,096-byte composite base row key + 220-char table name: inline TTL
    // row key would exceed 4,096 bytes, exercising `[0x44][SHA-256:32]` + `tv1:` end-to-end.
    let long_tbl = format!(
        "t_{}_{}",
        &Uuid::new_v4().simple().to_string()[..8],
        "x".repeat(210)
    );
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: long_tbl.clone(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".into(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".into(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ],
                billing_mode: Some(BillingMode::PayPerRequest),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();
    ctx.engine
        .update_ttl(&ctx.account_id, &long_tbl, "ttl_exp", true)
        .await
        .unwrap();
    ctx.engine
        .create_ttl_index(&ctx.account_id, &long_tbl, "ttl_exp")
        .await
        .unwrap();

    let ki = ctx
        .engine
        .table_key_info(&ctx.account_id, &long_tbl)
        .await
        .unwrap();
    // Base composite key = 1 + 4 + 3,900 + 1 + 150 = 4,056 B (`<= 4,096 B`),
    // while `11 + 12 + 221 + 4,056 = 4,300 > 4,096 B` genuinely triggers
    // `[0x44][SHA-256:32]` + `tv1:` digest encoding on `cbtemulator`.
    let big_pk = "p".repeat(3_900);
    let big_sk = "s".repeat(150);
    let past_exp_num = time::OffsetDateTime::now_utc().unix_timestamp() - 120;
    let past_exp = past_exp_num.to_string();
    let big_item = BTreeMap::from([
        ("pk".into(), AttributeValue::S(big_pk.clone())),
        ("sk".into(), AttributeValue::S(big_sk.clone())),
        ("ttl_exp".into(), AttributeValue::N(past_exp)),
    ]);
    let big_key = BTreeMap::from([
        ("pk".into(), AttributeValue::S(big_pk)),
        ("sk".into(), AttributeValue::S(big_sk)),
    ]);
    let raw_base_rk =
        extenddb_storage_bigtable::data::encoding::row_key::encode_key(&big_key, &ki.key_schema)
            .unwrap();
    let expected_ttl_rk = extenddb_storage_bigtable::data::encoding::ttl_key::encode_ttl_key(
        &ctx.account_id,
        &long_tbl,
        &raw_base_rk,
        past_exp_num,
    );
    assert!(
        expected_ttl_rk.len() < 600,
        "expected digested TTL row key (<600 B), got {} B",
        expected_ttl_rk.len()
    );

    ctx.engine
        .put_item(&ki, big_item, false, None, &ExpressionMaps::default(), None)
        .await
        .unwrap();

    sweep_once(&ctx.engine).await.unwrap();
    assert!(
        ctx.engine.get_item(&ki, &big_key).await.unwrap().is_none(),
        "digested >4KiB TTL index entry must evict expired max-size composite key item"
    );
}

#[tokio::test]
async fn test_stream_at_timestamp_millisecond_precision_fencing_and_paging() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let tbl = unique_name("bt_stream_at_ts");
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: tbl.clone(),
                key_schema: vec![KeySchemaElement {
                    attribute_name: "pk".into(),
                    key_type: KeyType::Hash,
                }],
                attribute_definitions: vec![AttributeDefinition {
                    attribute_name: "pk".into(),
                    attribute_type: ScalarAttributeType::S,
                }],
                billing_mode: Some(BillingMode::PayPerRequest),
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::NewAndOldImages),
                }),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();

    let desc = ctx
        .engine
        .describe_table(
            &ctx.account_id,
            DescribeTableInput {
                table_name: tbl.clone(),
            },
        )
        .await
        .unwrap();
    let stream_arn = desc.latest_stream_arn.clone().expect("latest_stream_arn");
    let stream_desc = ctx
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
    assert!(stream_desc.shards.len() >= 2);
    let ki = ctx
        .engine
        .table_key_info(&ctx.account_id, &tbl)
        .await
        .unwrap();
    let shard_id = ctx
        .engine
        .assign_shard(&ctx.account_id, &tbl, "k")
        .await
        .unwrap();
    let other_shard_id = stream_desc
        .shards
        .iter()
        .map(|s| s.shard_id.clone())
        .find(|sid| sid != &shard_id)
        .expect("a second shard in the same stream");

    // Non-existent shard and empty open shards all resolve to `None`.
    assert_eq!(
        ctx.engine
            .first_sequence_number_at_or_after("shardId-missing-0000000000000000", 0)
            .await
            .unwrap(),
        None
    );
    for sh in &stream_desc.shards {
        assert_eq!(
            ctx.engine
                .first_sequence_number_at_or_after(&sh.shard_id, 0)
                .await
                .unwrap(),
            None
        );
    }

    // 1. Live single-item `PutItem` and 2PC `TransactWriteItems` both capture
    //    millisecond `created_at_ms` from the same clock read as
    //    `ApproximateCreationDateTime` and support exact ms seeks.
    let capture = StreamCapture {
        view_type: StreamViewType::NewAndOldImages,
        user_identity: None,
        region: Arc::from("us-east-1"),
    };
    let (_, before_live_ms) = extenddb_storage_bigtable::streams::current_stream_clock();
    ctx.engine
        .put_item(
            &ki,
            s_item(&[("pk", "k"), ("v", "first")]),
            false,
            None,
            &ExpressionMaps::default(),
            Some(&capture),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    let (_, between_1_2_ms) = extenddb_storage_bigtable::streams::current_stream_clock();
    tokio::time::sleep(Duration::from_millis(40)).await;
    ctx.engine
        .put_item(
            &ki,
            s_item(&[("pk", "k"), ("v", "second")]),
            false,
            None,
            &ExpressionMaps::default(),
            Some(&capture),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    let (_, between_2_3_ms) = extenddb_storage_bigtable::streams::current_stream_clock();
    tokio::time::sleep(Duration::from_millis(40)).await;

    let txn_item = s_item(&[("pk", "k"), ("v", "third")]);
    let empty_maps = ExpressionMaps::default();
    ctx.engine
        .transact_write_items(
            &[TransactWriteOp::Put {
                key_info: &ki,
                item: &txn_item,
                condition: None,
                maps: &empty_maps,
                return_values_on_ccf: ReturnValuesOnConditionCheckFailure::None,
                stream: Some(capture.clone()),
            }],
            None,
        )
        .await
        .unwrap();
    let (_, after_live_ms) = extenddb_storage_bigtable::streams::current_stream_clock();

    let (live_recs, _) = ctx
        .engine
        .get_stream_records(&ctx.account_id, &stream_arn, &shard_id, None, 100)
        .await
        .unwrap();
    assert_eq!(live_recs.len(), 3);
    let seq1 = live_recs[0].dynamodb.sequence_number.clone();
    let seq2 = live_recs[1].dynamodb.sequence_number.clone();
    let seq3 = live_recs[2].dynamodb.sequence_number.clone();

    let cat = extenddb_storage_bigtable::catalog::Catalog::new(ctx.client.as_ref());
    let mut rec_ms = Vec::with_capacity(3);
    for rec in &live_recs {
        let raw = cat
            .get(&extenddb_storage_bigtable::catalog::keys::stream_record(
                &shard_id,
                &rec.dynamodb.sequence_number,
            ))
            .await
            .unwrap()
            .expect("raw stream record row");
        let (decoded, ms) = extenddb_storage_bigtable::streams::decode_stored_record(raw)
            .expect("decode stored stream record");
        assert_eq!(
            ms.div_euclid(1_000),
            decoded.dynamodb.approximate_creation_date_time,
            "created_at_ms and ApproximateCreationDateTime must share one clock read"
        );
        rec_ms.push(ms);
    }
    assert!(rec_ms[0] >= before_live_ms && rec_ms[0] <= between_1_2_ms);
    assert!(rec_ms[1] >= between_1_2_ms && rec_ms[1] <= between_2_3_ms);
    assert!(rec_ms[2] >= between_2_3_ms && rec_ms[2] <= after_live_ms);

    // Plant a record on a different shard at slot 1 inside the timestamp range
    // to prove `first_sequence_number_at_or_after` stays isolated to `shard_id`.
    let mut other_rec = live_recs[0].clone();
    other_rec.event_id = format!("evt-other-{}", Uuid::new_v4().simple());
    extenddb_storage_bigtable::streams::write_record_ordered_at(
        &cat,
        &other_shard_id,
        &other_rec,
        Some(rec_ms[0] + 2),
        None,
    )
    .await
    .unwrap()
    .expect("other shard record");

    // Exact millisecond boundaries (at-or-after, +1 ms skips, past newest -> None).
    for (at_ms, want) in [
        (0i64, Some(seq1.as_str())),
        (rec_ms[0] - 10_000, Some(seq1.as_str())),
        (rec_ms[0], Some(seq1.as_str())),
        (rec_ms[0] + 1, Some(seq2.as_str())),
        (between_1_2_ms, Some(seq2.as_str())),
        (rec_ms[1], Some(seq2.as_str())),
        (rec_ms[1] + 1, Some(seq3.as_str())),
        (between_2_3_ms, Some(seq3.as_str())),
        (rec_ms[2], Some(seq3.as_str())),
        (rec_ms[2] + 1, None),
        (after_live_ms + 1, None),
    ] {
        assert_eq!(
            ctx.engine
                .first_sequence_number_at_or_after(&shard_id, at_ms)
                .await
                .unwrap()
                .as_deref(),
            want,
            "at_ms={at_ms}"
        );
    }

    // Engine `resolve_at_timestamp` protocol: `n - 1` inclusive seek reads from
    // `seq2`, and post-tip fallback (`tip` captured before seek) observes a
    // subsequent write.
    let width = ctx.engine.sequence_number_width();
    let after_for_seq2 = format!("{:0>width$}", seq2.parse::<u128>().unwrap() - 1);
    let (from_seq2, _) = ctx
        .engine
        .get_stream_records(
            &ctx.account_id,
            &stream_arn,
            &shard_id,
            Some(&after_for_seq2),
            100,
        )
        .await
        .unwrap();
    let from_seq2_seqs: Vec<String> = from_seq2
        .into_iter()
        .map(|r| r.dynamodb.sequence_number)
        .collect();
    assert_eq!(from_seq2_seqs, vec![seq2.clone(), seq3.clone()]);

    let tip = ctx
        .engine
        .latest_sequence_number(&shard_id)
        .await
        .unwrap()
        .expect("tip after 3 writes");
    assert_eq!(tip, seq3);
    ctx.engine
        .put_item(
            &ki,
            s_item(&[("pk", "k"), ("v", "fourth")]),
            false,
            None,
            &ExpressionMaps::default(),
            Some(&capture),
        )
        .await
        .unwrap();
    let (after_tip, _) = ctx
        .engine
        .get_stream_records(&ctx.account_id, &stream_arn, &shard_id, Some(&tip), 100)
        .await
        .unwrap();
    assert_eq!(after_tip.len(), 1);
    assert_eq!(
        after_tip[0]
            .dynamodb
            .new_image
            .as_ref()
            .and_then(|img| img.get("v")),
        Some(&AttributeValue::S("fourth".into()))
    );

    // 2. Same-second fractional separation, out-of-order / clamped timestamps,
    //    legacy row fallback without `created_at_ms`, and multi-page scanning.
    let base_sec = live_recs[2].dynamodb.approximate_creation_date_time + 100;
    let base_ms = base_sec * 1_000;
    let make_rec = |tag: &str, approx_sec: i64| {
        let mut r = live_recs[0].clone();
        r.event_id = format!("evt-{tag}-{}", Uuid::new_v4().simple());
        r.dynamodb.approximate_creation_date_time = approx_sec;
        r
    };
    let seq5 = extenddb_storage_bigtable::streams::write_record_ordered_at(
        &cat,
        &shard_id,
        &make_rec("s5", base_sec),
        Some(base_ms + 50),
        None,
    )
    .await
    .unwrap()
    .expect("slot 5");
    // Slot 6 has a later timestamp than slot 7 to simulate a backward clock step.
    let seq6 = extenddb_storage_bigtable::streams::write_record_ordered_at(
        &cat,
        &shard_id,
        &make_rec("s6", base_sec),
        Some(base_ms + 700),
        None,
    )
    .await
    .unwrap()
    .expect("slot 6");
    let seq7 = extenddb_storage_bigtable::streams::write_record_ordered_at(
        &cat,
        &shard_id,
        &make_rec("s7", base_sec),
        Some(base_ms + 300),
        None,
    )
    .await
    .unwrap()
    .expect("slot 7");
    assert!(seq7 > seq6, "slot 7 sequence must follow slot 6");

    // Slot 8: legacy row without `created_at_ms` falls back to whole seconds * 1_000.
    let seq8 = extenddb_storage_bigtable::streams::slot_sequence_number(&shard_id, 8);
    let mut legacy_rec = make_rec("s8", base_sec + 5);
    legacy_rec.dynamodb.sequence_number.clone_from(&seq8);
    cat.put(
        &extenddb_storage_bigtable::catalog::keys::stream_record(&shard_id, &seq8),
        &serde_json::to_value(&legacy_rec).unwrap(),
    )
    .await
    .unwrap();

    // Paged seek (`page_size = 2`) exercises cursor advancement across multiple pages.
    assert_eq!(
        extenddb_storage_bigtable::streams::first_sequence_number_at_or_after_paged(
            &cat, &shard_id, base_ms, 2,
        )
        .await
        .unwrap()
        .as_deref(),
        Some(seq5.as_str())
    );
    assert_eq!(
        extenddb_storage_bigtable::streams::first_sequence_number_at_or_after_paged(
            &cat,
            &shard_id,
            base_ms + 51,
            2,
        )
        .await
        .unwrap()
        .as_deref(),
        Some(seq6.as_str())
    );
    assert_eq!(
        extenddb_storage_bigtable::streams::first_sequence_number_at_or_after_paged(
            &cat,
            &shard_id,
            base_ms + 300,
            2,
        )
        .await
        .unwrap()
        .as_deref(),
        Some(seq6.as_str()),
        "seek under backward clock step must return MIN(sequence_number) with created_at_ms >= at_ms"
    );
    assert_eq!(
        extenddb_storage_bigtable::streams::first_sequence_number_at_or_after_paged(
            &cat,
            &shard_id,
            base_ms + 701,
            2,
        )
        .await
        .unwrap()
        .as_deref(),
        Some(seq8.as_str())
    );
    assert_eq!(
        extenddb_storage_bigtable::streams::first_sequence_number_at_or_after_paged(
            &cat,
            &shard_id,
            (base_sec + 5) * 1_000 + 1,
            2,
        )
        .await
        .unwrap(),
        None
    );

    // 3. Closed-shard fencing: disable the stream, plant an uncleaned row past
    //    `ending_sequence_number`, and confirm both `first_sequence_number_at_or_after`
    //    and `latest_sequence_number` never expose records past `ending_sequence_number`.
    ctx.engine
        .update_table(
            &ctx.account_id,
            extenddb_core::types::UpdateTableInput {
                table_name: tbl.clone(),
                billing_mode: None,
                table_throughput_mode: None,
                provisioned_throughput: None,
                deletion_protection_enabled: None,
                global_secondary_index_updates: None,
                attribute_definitions: None,
                stream_specification: Some(StreamSpecification {
                    stream_enabled: false,
                    stream_view_type: None,
                }),
                table_class: None,
                on_demand_throughput: None,
                vector_index_updates: None,
                warm_throughput: None,
                sse_specification: None,
            },
        )
        .await
        .unwrap();

    let closed_desc = ctx
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
    let ending_seq = closed_desc
        .shards
        .iter()
        .find(|s| s.shard_id == shard_id)
        .and_then(|s| s.sequence_number_range.ending_sequence_number.clone())
        .expect("ending_sequence_number after disable");
    let ending_slot =
        extenddb_storage_bigtable::streams::parse_slot_from_sequence(&ending_seq).expect("slot");
    let post_seal_seq =
        extenddb_storage_bigtable::streams::slot_sequence_number(&shard_id, ending_slot + 1);
    let mut post_seal_rec = make_rec("post-seal", base_sec + 500);
    post_seal_rec
        .dynamodb
        .sequence_number
        .clone_from(&post_seal_seq);
    let mut post_seal_val = serde_json::to_value(&post_seal_rec).unwrap();
    post_seal_val.as_object_mut().unwrap().insert(
        "created_at_ms".into(),
        serde_json::Value::from((base_sec + 500) * 1_000),
    );
    cat.put(
        &extenddb_storage_bigtable::catalog::keys::stream_record(&shard_id, &post_seal_seq),
        &post_seal_val,
    )
    .await
    .unwrap();

    assert_eq!(
        ctx.engine
            .first_sequence_number_at_or_after(&shard_id, (base_sec + 100) * 1_000)
            .await
            .unwrap(),
        None,
        "first_sequence_number_at_or_after must never expose records past EndingSequenceNumber"
    );
    assert_eq!(
        ctx.engine
            .latest_sequence_number(&shard_id)
            .await
            .unwrap()
            .as_deref(),
        Some(ending_seq.as_str()),
        "latest_sequence_number must never probe past EndingSequenceNumber on a closed shard"
    );

    // 4. Millisecond retention pruning: re-enable the stream to obtain a fresh
    //    open shard while keeping `shard_id` closed, plant an expired (>24h)
    //    record and a retained (<24h) record on the open shard plus an expired
    //    post-seal orphan row on closed `shard_id`, sweep via
    //    `cleanup_expired_stream_records(24)`, and confirm expired rows are
    //    deleted while the retained row survives and closed-shard
    //    `ShardMeta.last_sequence_number` never advances past `ending_seq`.
    ctx.engine
        .update_table(
            &ctx.account_id,
            extenddb_core::types::UpdateTableInput {
                table_name: tbl.clone(),
                billing_mode: None,
                table_throughput_mode: None,
                provisioned_throughput: None,
                deletion_protection_enabled: None,
                global_secondary_index_updates: None,
                attribute_definitions: None,
                stream_specification: Some(StreamSpecification {
                    stream_enabled: true,
                    stream_view_type: Some(StreamViewType::NewAndOldImages),
                }),
                table_class: None,
                on_demand_throughput: None,
                vector_index_updates: None,
                warm_throughput: None,
                sse_specification: None,
            },
        )
        .await
        .unwrap();
    let open_shard_id = ctx
        .engine
        .assign_shard(&ctx.account_id, &tbl, "retention-k")
        .await
        .unwrap();
    let (now_secs, now_ms) = extenddb_storage_bigtable::streams::current_stream_clock();
    let expired_ms = now_ms - 24 * 3_600_000 - 2_000;
    let retained_ms = now_ms - 24 * 3_600_000 + 5_000;
    let expired_seq = extenddb_storage_bigtable::streams::write_record_ordered_at(
        &cat,
        &open_shard_id,
        &make_rec("expired-24h", expired_ms.div_euclid(1_000)),
        Some(expired_ms),
        None,
    )
    .await
    .unwrap()
    .expect("expired record slot");
    let retained_seq = extenddb_storage_bigtable::streams::write_record_ordered_at(
        &cat,
        &open_shard_id,
        &make_rec("retained-24h", retained_ms.div_euclid(1_000)),
        Some(retained_ms),
        None,
    )
    .await
    .unwrap()
    .expect("retained record slot");

    let expired_post_seal_seq =
        extenddb_storage_bigtable::streams::slot_sequence_number(&shard_id, ending_slot + 2);
    let mut expired_post_seal_rec = make_rec("expired-post-seal", now_secs - 25 * 3_600);
    expired_post_seal_rec
        .dynamodb
        .sequence_number
        .clone_from(&expired_post_seal_seq);
    let mut expired_post_seal_val = serde_json::to_value(&expired_post_seal_rec).unwrap();
    expired_post_seal_val.as_object_mut().unwrap().insert(
        "created_at_ms".to_owned(),
        serde_json::Value::from(expired_ms),
    );
    let expired_post_seal_rk =
        extenddb_storage_bigtable::catalog::keys::stream_record(&shard_id, &expired_post_seal_seq);
    cat.put(&expired_post_seal_rk, &expired_post_seal_val)
        .await
        .unwrap();

    let pruned = ctx
        .engine
        .cleanup_expired_stream_records(24)
        .await
        .expect("cleanup_expired_stream_records(24)");
    assert!(
        pruned >= 2,
        "expected expired record and expired post-seal orphan to be pruned, got {pruned}"
    );
    assert!(
        cat.get(&extenddb_storage_bigtable::catalog::keys::stream_record(
            &open_shard_id,
            &expired_seq,
        ))
        .await
        .unwrap()
        .is_none(),
        "expired record (>24h) must be removed by millisecond retention cleanup"
    );
    assert!(
        cat.get(&extenddb_storage_bigtable::catalog::keys::stream_record(
            &open_shard_id,
            &retained_seq,
        ))
        .await
        .unwrap()
        .is_some(),
        "record within 24h retention must not be removed"
    );
    assert!(
        cat.get(&expired_post_seal_rk).await.unwrap().is_none(),
        "expired post-seal orphan row must be deleted by retention cleanup"
    );
    let closed_meta: extenddb_storage_bigtable::streams::ShardMeta = serde_json::from_value(
        cat.get(&extenddb_storage_bigtable::catalog::keys::stream_shard(
            &shard_id,
        ))
        .await
        .unwrap()
        .expect("closed shard meta"),
    )
    .unwrap();
    assert!(
        closed_meta
            .last_sequence_number
            .as_deref()
            .is_none_or(|last| last <= ending_seq.as_str()),
        "retention pruning an expired post-seal orphan must not advance ShardMeta.last_sequence_number past EndingSequenceNumber"
    );
    assert_eq!(
        ctx.engine
            .latest_sequence_number(&shard_id)
            .await
            .unwrap()
            .as_deref(),
        Some(ending_seq.as_str()),
        "latest_sequence_number must stay clamped at EndingSequenceNumber after pruning"
    );
}

#[tokio::test]
async fn test_query_begins_with_max_unicode_scalar_base_gsi_and_lsi() {
    let Some(ctx) = setup_emulator_context().await else {
        eprintln!("Skipping Bigtable test: emulator unavailable");
        return;
    };

    let tbl = unique_name("bt_bw_max");
    ctx.engine
        .create_table(
            &ctx.account_id,
            CreateTableInput {
                table_name: tbl.clone(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".into(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".into(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "gpk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "gsk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "lsk".into(),
                        attribute_type: ScalarAttributeType::S,
                    },
                ],
                local_secondary_indexes: Some(vec![extenddb_core::types::LsiInput {
                    index_name: "lsi-max".into(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "pk".into(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "lsk".into(),
                            key_type: KeyType::Range,
                        },
                    ],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                }]),
                global_secondary_indexes: Some(vec![GsiInput {
                    index_name: "gsi-max".into(),
                    key_schema: vec![
                        KeySchemaElement {
                            attribute_name: "gpk".into(),
                            key_type: KeyType::Hash,
                        },
                        KeySchemaElement {
                            attribute_name: "gsk".into(),
                            key_type: KeyType::Range,
                        },
                    ],
                    projection: Projection {
                        projection_type: ProjectionType::All,
                        non_key_attributes: None,
                    },
                    provisioned_throughput: None,
                    warm_throughput: None,
                }]),
                billing_mode: Some(BillingMode::PayPerRequest),
                ..CreateTableInput::default()
            },
        )
        .await
        .unwrap();

    let ki = ctx
        .engine
        .table_key_info(&ctx.account_id, &tbl)
        .await
        .unwrap();
    let max_ch = "\u{10FFFF}";
    let keys: Vec<String> = vec![
        "a".into(),
        "ab".into(),
        "a\u{D7FF}".into(),
        "a\u{E000}".into(),
        "a\u{FFFF}".into(),
        "a\u{10000}".into(),
        format!("a{max_ch}"),
        format!("a{max_ch}b"),
        format!("a{max_ch}{max_ch}"),
        "b".into(),
        "z".into(),
        max_ch.into(),
        format!("{max_ch}x"),
        format!("{max_ch}{max_ch}"),
    ];
    for sk in &keys {
        ctx.engine
            .put_item(
                &ki,
                s_item(&[
                    ("pk", "p"),
                    ("sk", sk),
                    ("gpk", "gp"),
                    ("gsk", sk),
                    ("lsk", sk),
                ]),
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .unwrap();
    }

    let prefixes: Vec<String> = vec![
        "a".into(),
        format!("a{max_ch}"),
        format!("a{max_ch}{max_ch}"),
        max_ch.into(),
        format!("{max_ch}{max_ch}"),
        "a\u{D7FF}".into(),
        "a\u{FFFF}".into(),
    ];
    let kc_base =
        parse_key_condition(&tokenize("pk = :p AND begins_with(sk, :s)").unwrap()).unwrap();
    let kc_gsi =
        parse_key_condition(&tokenize("gpk = :p AND begins_with(gsk, :s)").unwrap()).unwrap();
    let kc_lsi =
        parse_key_condition(&tokenize("pk = :p AND begins_with(lsk, :s)").unwrap()).unwrap();

    for prefix in &prefixes {
        let want: Vec<String> = keys
            .iter()
            .filter(|k| k.starts_with(prefix.as_str()))
            .cloned()
            .collect();
        for forward in [true, false] {
            let expected: Vec<String> = if forward {
                want.clone()
            } else {
                let mut rev = want.clone();
                rev.reverse();
                rev
            };

            for (label, index_name, pk_val, sk_attr, kc) in [
                ("base", None, "p", "sk", &kc_base),
                ("gsi", Some("gsi-max"), "gp", "gsk", &kc_gsi),
                ("lsi", Some("lsi-max"), "p", "lsk", &kc_lsi),
            ] {
                let mut got = Vec::new();
                let mut esk = None;
                loop {
                    let mut maps = ExpressionMaps::default();
                    maps.values
                        .insert("p".into(), AttributeValue::S(pk_val.into()));
                    maps.values
                        .insert("s".into(), AttributeValue::S(prefix.clone()));
                    let (items, lek) = ctx
                        .engine
                        .query(&ki, kc, &maps, forward, Some(1), esk.as_ref(), index_name)
                        .await
                        .unwrap();
                    for it in items {
                        if let Some(AttributeValue::S(s)) = it.get(sk_attr) {
                            got.push(s.clone());
                        }
                    }
                    esk = lek;
                    if esk.is_none() {
                        break;
                    }
                }
                assert_eq!(
                    got,
                    expected,
                    "{label} begins_with forward={forward} prefix={}",
                    prefix.escape_unicode()
                );
            }
        }
    }

    // BETWEEN ranges whose bounds sit next to U+10FFFF (base table, GSI, and LSI).
    let kc_between_base =
        parse_key_condition(&tokenize("pk = :p AND sk BETWEEN :l AND :u").unwrap()).unwrap();
    let kc_between_gsi =
        parse_key_condition(&tokenize("gpk = :p AND gsk BETWEEN :l AND :u").unwrap()).unwrap();
    let kc_between_lsi =
        parse_key_condition(&tokenize("pk = :p AND lsk BETWEEN :l AND :u").unwrap()).unwrap();
    for (low, high) in [
        ("a".to_owned(), format!("a{max_ch}")),
        (format!("a{max_ch}"), "b".to_owned()),
    ] {
        let want: Vec<String> = keys
            .iter()
            .filter(|k| k.as_bytes() >= low.as_bytes() && k.as_bytes() <= high.as_bytes())
            .cloned()
            .collect();

        for (label, index_name, pk_val, sk_attr, kc) in [
            ("base", None, "p", "sk", &kc_between_base),
            ("gsi", Some("gsi-max"), "gp", "gsk", &kc_between_gsi),
            ("lsi", Some("lsi-max"), "p", "lsk", &kc_between_lsi),
        ] {
            let mut maps = ExpressionMaps::default();
            maps.values
                .insert("p".into(), AttributeValue::S(pk_val.into()));
            maps.values
                .insert("l".into(), AttributeValue::S(low.clone()));
            maps.values
                .insert("u".into(), AttributeValue::S(high.clone()));
            let (items, _) = ctx
                .engine
                .query(&ki, kc, &maps, true, None, None, index_name)
                .await
                .unwrap();
            let got: Vec<String> = items
                .into_iter()
                .filter_map(|it| match it.get(sk_attr) {
                    Some(AttributeValue::S(s)) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            assert_eq!(got, want, "{label} BETWEEN [{low:?}, {high:?}]");
        }
    }
}
