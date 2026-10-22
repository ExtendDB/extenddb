// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level tests for PostgreSQL backup restore.
//!
//! The wire suite (`tests/test_backup_restore_fidelity.py`) checks round trips
//! on every backend. Two cases are only reachable here, because they need a
//! `backup_items` row the current binary would not write:
//!
//! - a backup written before the sort-key fix, whose rows carry the item with
//!   `sk` NULL, must restore with its sort keys (they are read from the item);
//! - a backup whose copy fails part-way must not leave the target table
//!   behind in CREATING, where nothing would ever move it on and its name
//!   would stay taken;
//! - a backup with two rows for one key must fail rather than collapse them;
//! - a backup of a multi-part key table (a preview gated by
//!   `enable_multipart_keys`) must be refused, not restored wrongly;
//! - the abandoned-restore sweep, which needs a crash to reach.
//!
//! Each test builds a throwaway catalog and data database and drops them when
//! it ends, passing or failing. The tests run one at a time: each opens its
//! own pools, and running them all at once can exhaust the server's
//! connection limit. Requires `EXTENDDB_TEST_PG_CONNECTION_STRING` (a base URL with no database
//! component); without it every test reports a skip and passes.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, DescribeTableInput, Item, KeySchemaElement,
    KeyType, ScalarAttributeType, TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupEngine, DataEngine, RestoreTableOverrides, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";

/// A catalog database and a separate data database, the layout `extenddb
/// init` creates. They must be separate here: the catalog schema carries a
/// legacy `stream_shards` table with a foreign key to `tables`, which would
/// shadow the data schema's table if both were applied to one database.
struct Scratch {
    engine: PostgresEngine,
    /// The data database.
    db: PgPool,
    catalog: PgPool,
    admin: PgPool,
    db_names: [String; 2],
    _guard: DbGuard,
    _serial: tokio::sync::MutexGuard<'static, ()>,
}

/// Drops the scratch databases if the test panics, from the moment they are
/// created, including during setup.
struct DbGuard {
    names: Vec<String>,
}

/// One test at a time; see the module comment.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

impl Scratch {
    /// Close the pools and drop the scratch databases. Also run, on a fresh
    /// runtime, if a test panics before calling it.
    async fn cleanup(self) {
        self.db.close().await;
        self.catalog.close().await;
        drop_databases(&self.admin, &self.db_names).await;
        self.admin.close().await;
    }
}

async fn drop_databases(admin: &PgPool, names: &[String]) {
    for name in names {
        let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
            .execute(admin)
            .await;
    }
}

impl Drop for DbGuard {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        // A failed test: drop its databases on a separate thread and runtime,
        // since the test's own runtime is unwinding.
        let (Some(base), names) = (base_conn(), self.names.clone()) else {
            return;
        };
        let _ = std::thread::spawn(move || {
            let Ok(rt) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            rt.block_on(async {
                if let Ok(admin) = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&format!("{base}/postgres"))
                    .await
                {
                    drop_databases(&admin, &names).await;
                    admin.close().await;
                }
            });
        })
        .join();
    }
}

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

async fn connect(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .expect("connect to a scratch database")
}

async fn apply(pool: &PgPool, migrations: &[&str]) {
    for sql in migrations {
        sqlx::raw_sql(sql)
            .execute(pool)
            .await
            .expect("apply a shipped migration");
    }
}

async fn scratch() -> Scratch {
    let serial = SERIAL.lock().await;
    let base = base_conn().expect("caller checks base_conn() first");
    let stem = format!("eddb_bkup_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
    let (catalog_name, data_name) = (format!("{stem}_c"), format!("{stem}_d"));
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    let guard = DbGuard {
        names: vec![catalog_name.clone(), data_name.clone()],
    };
    for name in [&catalog_name, &data_name] {
        sqlx::query(&format!("CREATE DATABASE \"{name}\""))
            .execute(&admin)
            .await
            .expect("create a scratch database");
    }
    let catalog_url = format!("{base}/{catalog_name}");
    let data_url = format!("{base}/{data_name}");
    let catalog = connect(&catalog_url).await;
    let db = connect(&data_url).await;
    apply(
        &catalog,
        &[
            include_str!("../migrations/001_schema.sql"),
            include_str!("../migrations/002_vector_indexes.sql"),
            include_str!("../migrations/003_backup_definitions.sql"),
        ],
    )
    .await;
    apply(
        &db,
        &[
            include_str!("../data_migrations/001_data_schema.sql"),
            include_str!("../data_migrations/002_gsi_pending.sql"),
            include_str!("../data_migrations/003_idempotency_account_scope.sql"),
            include_str!("../data_migrations/004_vector_index_state.sql"),
        ],
    )
    .await;
    sqlx::query("UPDATE settings SET value = '0' WHERE key = 'control_plane_delay_seconds'")
        .execute(&catalog)
        .await
        .expect("pin the control-plane delay to zero");
    sqlx::query("UPDATE settings SET value = '0' WHERE key = 'index_propagation_delay_ms'")
        .execute(&catalog)
        .await
        .expect("make index propagation synchronous, as no queue worker runs here");
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('data_database_connection_string', $1) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(&data_url)
    .execute(&catalog)
    .await
    .expect("point the catalog at the data database");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind(ACCOUNT)
        .bind(format!("acct-{stem}"))
        .execute(&catalog)
        .await
        .expect("seed the account row");
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: catalog_url,
            pool_size: 4,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open a PostgresEngine on the scratch databases");
    Scratch {
        engine,
        db,
        catalog,
        admin,
        db_names: [catalog_name, data_name],
        _guard: guard,
        _serial: serial,
    }
}

fn composite_table(name: &str) -> CreateTableInput {
    let key = |name: &str, key_type| KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    };
    let attr = |name: &str, attribute_type| AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type,
    };
    CreateTableInput {
        table_name: name.to_owned(),
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        attribute_definitions: vec![
            attr("pk", ScalarAttributeType::S),
            attr("sk", ScalarAttributeType::N),
        ],
        billing_mode: Some(BillingMode::PayPerRequest),
        ..Default::default()
    }
}

/// A backup of an empty composite-key table, then `rows` written straight
/// into `backup_items` in the pre-fix shape: `sk` NULL, the item in
/// `item_data`.
async fn legacy_backup(s: &Scratch, source: &str, rows: &[serde_json::Value]) -> String {
    legacy_backup_of(s, composite_table(source), rows).await
}

async fn legacy_backup_of(
    s: &Scratch,
    source: CreateTableInput,
    rows: &[serde_json::Value],
) -> String {
    let source_name = source.table_name.clone();
    s.engine
        .create_table(ACCOUNT, source)
        .await
        .expect("create the source table");
    let backup = s
        .engine
        .create_backup(ACCOUNT, &source_name, "legacy")
        .await
        .expect("back up the empty source");
    for item in rows {
        sqlx::query(
            "INSERT INTO backup_items (backup_arn, pk, sk, item_data) VALUES ($1, $2, NULL, $3)",
        )
        .bind(&backup.backup_arn)
        .bind(item["pk"]["S"].as_str().unwrap_or("x"))
        .bind(item)
        .execute(&s.catalog)
        .await
        .expect("write a legacy backup row");
    }
    backup.backup_arn
}

async fn table_status(s: &Scratch, name: &str) -> Option<String> {
    sqlx::query_scalar("SELECT table_status FROM tables WHERE account_id = $1 AND table_name = $2")
        .bind(ACCOUNT)
        .bind(name)
        .fetch_optional(&s.catalog)
        .await
        .expect("read the table status")
}

#[tokio::test]
async fn legacy_composite_backup_restores_with_sort_keys() {
    if base_conn().is_none() {
        eprintln!("SKIP legacy_composite_backup_restores_with_sort_keys: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    let rows: Vec<serde_json::Value> = (0..12)
        .map(|i| {
            serde_json::json!({
                "pk": {"S": format!("p{}", i % 3)},
                "sk": {"N": format!("{}", i * 10 - 50)},
                "v": {"S": format!("value-{i}")},
            })
        })
        .collect();
    let arn = legacy_backup(&s, "legacy_src", &rows).await;

    s.engine
        .restore_table_from_backup(
            ACCOUNT,
            "legacy_dst",
            &arn,
            RestoreTableOverrides::default(),
        )
        .await
        .expect("restore a pre-fix backup");
    assert_eq!(
        table_status(&s, "legacy_dst").await.as_deref(),
        Some("ACTIVE")
    );

    let desc = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "legacy_dst".to_owned(),
            },
        )
        .await
        .expect("describe the restored table");
    assert_eq!(desc.item_count, 12);
    assert!(
        desc.table_size_bytes > 0,
        "a restored table reports its size as soon as it is ACTIVE"
    );
    let key_info = TableKeyInfo {
        table_name: "legacy_dst".to_owned(),
        account_id: ACCOUNT.to_owned(),
        table_id: desc.table_id.clone(),
        key_schema: desc.key_schema.clone(),
        base_key_schema: desc.key_schema.clone(),
        attribute_definitions: desc.attribute_definitions.clone(),
        ..Default::default()
    };

    // Every row is addressable by its full key, so the sort key landed in its
    // typed column rather than being dropped.
    for row in &rows {
        let item: Item = serde_json::from_value(row.clone()).expect("item from json");
        let key: Item = item
            .iter()
            .filter(|(k, _)| *k == "pk" || *k == "sk")
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let got = s
            .engine
            .get_item(&key_info, &key)
            .await
            .expect("point read");
        assert_eq!(got.as_ref(), Some(&item));
    }

    s.cleanup().await;
}

#[tokio::test]
async fn failed_restore_removes_the_partial_table() {
    if base_conn().is_none() {
        eprintln!("SKIP failed_restore_removes_the_partial_table: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    let rows = vec![
        serde_json::json!({"pk": {"S": "a"}, "sk": {"N": "1"}}),
        // No sort key: the copy cannot place this item.
        serde_json::json!({"pk": {"S": "b"}}),
    ];
    let arn = legacy_backup(&s, "broken_src", &rows).await;
    // A non-zero control-plane delay sends an ordinary DeleteTable through
    // DELETING; cleanup of a failed restore must not depend on that.
    sqlx::query("UPDATE settings SET value = '5' WHERE key = 'control_plane_delay_seconds'")
        .execute(&s.catalog)
        .await
        .expect("set a non-zero control-plane delay");

    let err = s
        .engine
        .restore_table_from_backup(
            ACCOUNT,
            "broken_dst",
            &arn,
            RestoreTableOverrides::default(),
        )
        .await
        .expect_err("a backup row without its sort key cannot restore");
    assert!(matches!(err, StorageError::Internal(_)), "{err:?}");

    // Neither CREATING, DELETING, nor half-filled: the target and its data
    // table are gone and the name is free.
    assert_eq!(table_status(&s, "broken_dst").await, None);
    let leftover: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE '\\_ddb\\_%'",
    )
    .fetch_one(&s.db)
    .await
    .expect("count data tables");
    assert_eq!(leftover, 1, "only the source table's data table remains");
    s.engine
        .create_table(ACCOUNT, composite_table("broken_dst"))
        .await
        .expect("the target name is free again");

    s.cleanup().await;
}

#[tokio::test]
async fn duplicate_backup_rows_fail_the_restore() {
    if base_conn().is_none() {
        eprintln!("SKIP duplicate_backup_rows_fail_the_restore: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    // 1 and 1.0 are one N key value: a backup carrying both is corrupt, and
    // the restore must say so rather than keep one and report two.
    let rows = vec![
        serde_json::json!({"pk": {"S": "a"}, "sk": {"N": "1"}, "v": {"S": "first"}}),
        serde_json::json!({"pk": {"S": "a"}, "sk": {"N": "1.0"}, "v": {"S": "second"}}),
    ];
    let arn = legacy_backup(&s, "dup_src", &rows).await;
    let err = s
        .engine
        .restore_table_from_backup(ACCOUNT, "dup_dst", &arn, RestoreTableOverrides::default())
        .await
        .expect_err("two rows for one key cannot restore");
    assert!(matches!(err, StorageError::Internal(_)), "{err:?}");
    assert_eq!(table_status(&s, "dup_dst").await, None);
    s.cleanup().await;
}

#[tokio::test]
async fn multipart_key_backup_is_refused() {
    if base_conn().is_none() {
        eprintln!("SKIP multipart_key_backup_is_refused: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    let key = |name: &str, key_type| KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    };
    let attr = |name: &str, attribute_type| AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type,
    };
    let input = CreateTableInput {
        table_name: "multi_src".to_owned(),
        key_schema: vec![
            key("h1", KeyType::Hash),
            key("h2", KeyType::Hash),
            key("r1", KeyType::Range),
        ],
        attribute_definitions: vec![
            attr("h1", ScalarAttributeType::S),
            attr("h2", ScalarAttributeType::N),
            attr("r1", ScalarAttributeType::S),
        ],
        billing_mode: Some(BillingMode::PayPerRequest),
        ..Default::default()
    };
    let rows = vec![serde_json::json!({"h1": {"S": "a"}, "h2": {"N": "1"}, "r1": {"S": "x"}})];
    let arn = legacy_backup_of(&s, input, &rows).await;
    // Multi-part base keys are a preview the item paths address only by their
    // first parts, so no restore of one can be right; it must refuse, before
    // creating anything.
    let err = s
        .engine
        .restore_table_from_backup(ACCOUNT, "multi_dst", &arn, RestoreTableOverrides::default())
        .await
        .expect_err("a multi-part key backup is refused");
    assert!(matches!(err, StorageError::Unsupported(_)), "{err:?}");
    assert_eq!(table_status(&s, "multi_dst").await, None);
    s.cleanup().await;
}

/// A provisioned (pk S, sk N) table with one GSI and one LSI, holding 30
/// items, some of them outside each index.
async fn indexed_source(s: &Scratch, name: &str) -> String {
    let input: CreateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": name,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
            {"AttributeName": "g", "AttributeType": "S"},
            {"AttributeName": "l", "AttributeType": "S"}
        ],
        "BillingMode": "PROVISIONED",
        "ProvisionedThroughput": {"ReadCapacityUnits": 7, "WriteCapacityUnits": 9},
        "GlobalSecondaryIndexes": [{
            "IndexName": "gi",
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "KEYS_ONLY"},
            "ProvisionedThroughput": {"ReadCapacityUnits": 3, "WriteCapacityUnits": 4}
        }],
        "LocalSecondaryIndexes": [{
            "IndexName": "li",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "l", "KeyType": "RANGE"}
            ],
            "Projection": {"ProjectionType": "ALL"}
        }]
    }))
    .expect("input");
    let desc = s
        .engine
        .create_table(ACCOUNT, input)
        .await
        .expect("create the source table");
    let key_info = TableKeyInfo {
        table_name: name.to_owned(),
        account_id: ACCOUNT.to_owned(),
        table_id: desc.table_id.clone(),
        key_schema: desc.key_schema.clone(),
        base_key_schema: desc.key_schema.clone(),
        attribute_definitions: desc.attribute_definitions.clone(),
        ..Default::default()
    };
    for i in 0..30 {
        let mut item =
            serde_json::json!({"pk": {"S": format!("p{}", i % 3)}, "sk": {"N": i.to_string()}});
        if i % 2 == 0 {
            item["g"] = serde_json::json!({"S": format!("g{}", i % 4)});
        }
        if i % 3 == 0 {
            item["l"] = serde_json::json!({"S": format!("l{i}")});
        }
        let item: Item = serde_json::from_value(item).expect("item");
        s.engine
            .put_item(
                &key_info,
                item,
                false,
                None,
                &extenddb_core::expression::ExpressionMaps::default(),
                None,
            )
            .await
            .expect("put");
    }
    desc.table_id
}

/// `(index_name, index_id)` of a table's secondary indexes, by name.
async fn index_ids(s: &Scratch, table_id: &str) -> Vec<(String, String)> {
    sqlx::query_as(
        "SELECT index_name, index_id FROM indexes WHERE table_id = $1 ORDER BY index_name",
    )
    .bind(table_id)
    .fetch_all(&s.catalog)
    .await
    .expect("read indexes")
}

async fn data_rows(s: &Scratch, physical_id: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM \"_ddb_{physical_id}\""))
        .fetch_one(&s.db)
        .await
        .expect("count rows")
}

async fn data_table_exists(s: &Scratch, physical_id: &str) -> bool {
    sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(format!("public.\"_ddb_{physical_id}\""))
        .fetch_one(&s.db)
        .await
        .expect("regclass")
}

async fn table_id_of(s: &Scratch, name: &str) -> Option<String> {
    sqlx::query_scalar("SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2")
        .bind(ACCOUNT)
        .bind(name)
        .fetch_optional(&s.catalog)
        .await
        .expect("table id")
}

/// Create a one-row source and return its id and quoted physical table name.
async fn snapshot_source(s: &Scratch, name: &str) -> (String, String) {
    let input = CreateTableInput {
        table_name: name.to_owned(),
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        }],
        attribute_definitions: vec![AttributeDefinition {
            attribute_name: "pk".to_owned(),
            attribute_type: ScalarAttributeType::S,
        }],
        billing_mode: Some(BillingMode::PayPerRequest),
        ..Default::default()
    };
    let desc = s
        .engine
        .create_table(ACCOUNT, input)
        .await
        .expect("create snapshot source");
    let table = format!("\"_ddb_{}\"", desc.table_id);
    insert_snapshot_item(&s.db, &table, "before").await;
    (desc.table_id, table)
}

async fn insert_snapshot_item(db: &PgPool, table: &str, key: &str) {
    sqlx::query(&format!(
        "INSERT INTO {table} (pk, item_data) VALUES ($1, $2)"
    ))
    .bind(key)
    .bind(serde_json::json!({"pk": {"S": key}}))
    .execute(db)
    .await
    .expect("insert snapshot item");
}

async fn snapshot_row_count(db: &PgPool, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(db)
        .await
        .expect("count snapshot rows")
}

#[tokio::test]
async fn restore_recreates_indexes_and_throughput() {
    if base_conn().is_none() {
        eprintln!("SKIP restore_recreates_indexes_and_throughput: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    let src = indexed_source(&s, "idx_src").await;
    let backup = s
        .engine
        .create_backup(ACCOUNT, "idx_src", "b")
        .await
        .expect("backup");
    let desc = s
        .engine
        .restore_table_from_backup(
            ACCOUNT,
            "idx_dst",
            &backup.backup_arn,
            RestoreTableOverrides::default(),
        )
        .await
        .expect("restore");
    assert_eq!(table_status(&s, "idx_dst").await.as_deref(), Some("ACTIVE"));

    let restored = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "idx_dst".to_owned(),
            },
        )
        .await
        .expect("describe");
    let pt = restored.provisioned_throughput;
    assert_eq!((pt.read_capacity_units, pt.write_capacity_units), (7, 9));
    let gsis = restored.global_secondary_indexes.expect("gsis");
    assert_eq!(gsis.len(), 1);
    let gpt = gsis[0]
        .provisioned_throughput
        .as_ref()
        .expect("gsi throughput");
    assert_eq!((gpt.read_capacity_units, gpt.write_capacity_units), (3, 4));
    assert_eq!(restored.local_secondary_indexes.map(|l| l.len()), Some(1));

    // Each index holds exactly the rows the source's does.
    let src_idx = index_ids(&s, &src).await;
    let dst_idx = index_ids(&s, &desc.table_id).await;
    assert_eq!(
        dst_idx.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
        ["gi", "li"]
    );
    for ((_, a), (_, b)) in src_idx.iter().zip(&dst_idx) {
        let want = data_rows(&s, a).await;
        assert!(want > 0);
        assert_eq!(data_rows(&s, b).await, want);
    }
    assert_eq!(data_rows(&s, &desc.table_id).await, 30);
    s.cleanup().await;
}

#[tokio::test]
async fn abandoned_restore_is_removed_only_when_unowned_and_old() {
    if base_conn().is_none() {
        eprintln!("SKIP abandoned_restore_is_removed_only_when_unowned_and_old: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    indexed_source(&s, "ab_src").await;
    let backup = s
        .engine
        .create_backup(ACCOUNT, "ab_src", "b")
        .await
        .expect("backup");
    let desc = s
        .engine
        .restore_table_from_backup(
            ACCOUNT,
            "ab_dst",
            &backup.backup_arn,
            RestoreTableOverrides::default(),
        )
        .await
        .expect("restore");
    let indexes = index_ids(&s, &desc.table_id).await;

    // The state a process killed mid-copy leaves: the target CREATING with no
    // scheduled transition, its copy transaction rolled back.
    let crash = |age: &'static str| {
        let catalog = s.catalog.clone();
        let id = desc.table_id.clone();
        async move {
            sqlx::query(&format!(
                "UPDATE tables SET table_status = 'CREATING', status_transition_at = NULL, \
                 creation_date_time = NOW() - INTERVAL '{age}' WHERE table_id = $1"
            ))
            .bind(&id)
            .execute(&catalog)
            .await
            .expect("simulate a crash");
        }
    };

    // Inside the grace period: a restore that has not taken its lock yet.
    crash("1 second").await;
    s.engine
        .process_control_plane_transitions()
        .await
        .expect("sweep");
    assert_eq!(
        table_status(&s, "ab_dst").await.as_deref(),
        Some("CREATING")
    );

    // Old, but its lock is held: a live restore on another instance. The key
    // is the account and name, so the lock covers the target before it exists.
    crash("10 minutes").await;
    let mut owner = s.catalog.acquire().await.expect("connection");
    sqlx::query("SELECT pg_advisory_lock(hashtextextended($2, $1))")
        .bind(0x0045_4452_i64)
        .bind(format!("{ACCOUNT}/ab_dst"))
        .execute(&mut *owner)
        .await
        .expect("hold the restore lock");
    s.engine
        .process_control_plane_transitions()
        .await
        .expect("sweep");
    assert_eq!(
        table_status(&s, "ab_dst").await.as_deref(),
        Some("CREATING")
    );
    sqlx::query("SELECT pg_advisory_unlock(hashtextextended($2, $1))")
        .bind(0x0045_4452_i64)
        .bind(format!("{ACCOUNT}/ab_dst"))
        .execute(&mut *owner)
        .await
        .expect("release the restore lock");
    drop(owner);

    // Old and unowned: removed, with its data and index tables.
    let transitions = s
        .engine
        .process_control_plane_transitions()
        .await
        .expect("sweep");
    assert!(
        transitions.iter().any(|(n, _)| n == "ab_dst"),
        "{transitions:?}"
    );
    assert_eq!(table_status(&s, "ab_dst").await, None);
    assert!(!data_table_exists(&s, &desc.table_id).await);
    for (_, id) in &indexes {
        assert!(
            !data_table_exists(&s, id).await,
            "index table {id} left behind"
        );
    }

    // An ACTIVE table is never a candidate, and the name is free again.
    s.engine
        .restore_table_from_backup(
            ACCOUNT,
            "ab_dst",
            &backup.backup_arn,
            RestoreTableOverrides::default(),
        )
        .await
        .expect("restore again");
    sqlx::query("UPDATE tables SET creation_date_time = NOW() - INTERVAL '10 minutes'")
        .execute(&s.catalog)
        .await
        .expect("age every table");
    s.engine
        .process_control_plane_transitions()
        .await
        .expect("sweep");
    assert_eq!(table_status(&s, "ab_dst").await.as_deref(), Some("ACTIVE"));
    s.cleanup().await;
}

#[tokio::test]
async fn failed_restore_with_indexes_leaves_no_tables() {
    if base_conn().is_none() {
        eprintln!("SKIP failed_restore_with_indexes_leaves_no_tables: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    indexed_source(&s, "fi_src").await;
    let backup = s
        .engine
        .create_backup(ACCOUNT, "fi_src", "b")
        .await
        .expect("backup");
    sqlx::query("INSERT INTO backup_items (backup_arn, pk, item_data) VALUES ($1, 'x', $2)")
        .bind(&backup.backup_arn)
        .bind(serde_json::json!({"pk": {"S": "x"}}))
        .execute(&s.catalog)
        .await
        .expect("add a row without its sort key");
    let before: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_tables WHERE schemaname = 'public'")
            .fetch_one(&s.db)
            .await
            .expect("tables");
    s.engine
        .restore_table_from_backup(
            ACCOUNT,
            "fi_dst",
            &backup.backup_arn,
            RestoreTableOverrides::default(),
        )
        .await
        .expect_err("a row without its sort key cannot restore");
    assert_eq!(table_id_of(&s, "fi_dst").await, None);
    let after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_tables WHERE schemaname = 'public'")
            .fetch_one(&s.db)
            .await
            .expect("tables");
    assert_eq!(
        after, before,
        "the target's data and index tables are dropped"
    );
    s.cleanup().await;
}

#[tokio::test]
async fn restore_keeps_table_class_sse_and_on_demand_limits() {
    if base_conn().is_none() {
        eprintln!("SKIP restore_keeps_table_class_sse_and_on_demand_limits: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    let input: CreateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": "cls_src",
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST",
        "TableClass": "STANDARD_INFREQUENT_ACCESS",
        "SSESpecification": {"Enabled": true, "SSEType": "KMS"},
        "OnDemandThroughput": {"MaxReadRequestUnits": 100, "MaxWriteRequestUnits": 50}
    }))
    .expect("input");
    let src = s.engine.create_table(ACCOUNT, input).await.expect("create");
    let backup = s
        .engine
        .create_backup(ACCOUNT, "cls_src", "b")
        .await
        .expect("backup");
    s.engine
        .restore_table_from_backup(
            ACCOUNT,
            "cls_dst",
            &backup.backup_arn,
            RestoreTableOverrides::default(),
        )
        .await
        .expect("restore");
    let read = |name: &'static str| {
        let catalog = s.catalog.clone();
        async move {
            sqlx::query_as::<
                _,
                (
                    String,
                    Option<String>,
                    Option<serde_json::Value>,
                    Option<serde_json::Value>,
                ),
            >(
                "SELECT billing_mode, table_class, sse_specification, on_demand_throughput \
                 FROM tables WHERE account_id = $1 AND table_name = $2",
            )
            .bind(ACCOUNT)
            .bind(name)
            .fetch_one(&catalog)
            .await
            .expect("row")
        }
    };
    let want = read("cls_src").await;
    assert_eq!(want.1.as_deref(), Some("STANDARD_INFREQUENT_ACCESS"));
    assert!(want.2.is_some() && want.3.is_some(), "{want:?}");
    assert_eq!(read("cls_dst").await, want);
    drop(src);
    s.cleanup().await;
}

#[tokio::test]
async fn restore_crosses_batch_boundaries() {
    if base_conn().is_none() {
        eprintln!("SKIP restore_crosses_batch_boundaries: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    let src = indexed_source(&s, "many_src").await;
    // More rows than one item batch, and a few large enough that the byte
    // budget, not the row count, ends some batches.
    let desc_rows: Vec<serde_json::Value> = (0..1_234)
        .map(|i| {
            let mut v = serde_json::json!({
                "pk": {"S": format!("bulk{}", i % 11)},
                "sk": {"N": format!("{}", 1000 + i)},
                "g": {"S": format!("g{}", i % 4)},
            });
            if i % 97 == 0 {
                v["pad"] = serde_json::json!({"S": "x".repeat(300_000)});
            }
            v
        })
        .collect();
    let backup = s
        .engine
        .create_backup(ACCOUNT, "many_src", "b")
        .await
        .expect("backup");
    sqlx::query(
        "INSERT INTO backup_items (backup_arn, pk, item_data) \
         SELECT $1, '', d FROM UNNEST($2::jsonb[]) AS t(d)",
    )
    .bind(&backup.backup_arn)
    .bind(&desc_rows)
    .execute(&s.catalog)
    .await
    .expect("add rows to the backup");
    let desc = s
        .engine
        .restore_table_from_backup(
            ACCOUNT,
            "many_dst",
            &backup.backup_arn,
            RestoreTableOverrides::default(),
        )
        .await
        .expect("restore");
    assert_eq!(data_rows(&s, &desc.table_id).await, 30 + 1_234);
    let src_gsi = &index_ids(&s, &src).await[0].1;
    let dst_gsi = &index_ids(&s, &desc.table_id).await[0].1;
    assert_eq!(
        data_rows(&s, dst_gsi).await,
        data_rows(&s, src_gsi).await + 1_234
    );
    s.cleanup().await;
}

#[tokio::test]
async fn restore_into_a_name_whose_restore_lock_is_held_is_refused() {
    if base_conn().is_none() {
        eprintln!("SKIP restore_into_a_name_whose_restore_lock_is_held_is_refused: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    indexed_source(&s, "lk_src").await;
    let backup = s
        .engine
        .create_backup(ACCOUNT, "lk_src", "b")
        .await
        .expect("backup");
    let mut other = s.catalog.acquire().await.expect("connection");
    sqlx::query("SELECT pg_advisory_lock(hashtextextended($2, $1))")
        .bind(0x0045_4452_i64)
        .bind(format!("{ACCOUNT}/lk_dst"))
        .execute(&mut *other)
        .await
        .expect("another restore owns the name");
    let err = s
        .engine
        .restore_table_from_backup(
            ACCOUNT,
            "lk_dst",
            &backup.backup_arn,
            RestoreTableOverrides::default(),
        )
        .await
        .expect_err("the name is being restored into already");
    assert!(
        matches!(err, StorageError::TableAlreadyExists(_)),
        "{err:?}"
    );
    assert_eq!(table_status(&s, "lk_dst").await, None);
    sqlx::query("SELECT pg_advisory_unlock_all()")
        .execute(&mut *other)
        .await
        .expect("release");
    drop(other);
    s.cleanup().await;
}

/// A backup taken before catalog 0.0.4 has no definition row. Rolling the
/// catalog back to that shape, replaying migration 003 twice (as an upgrade
/// interrupted between applying and recording it would), and restoring must
/// give the table as restores did before: keys and items, no secondary
/// indexes, and 5/5 throughput for a provisioned table.
#[tokio::test]
async fn pre_0_0_4_backup_restores_after_migrating() {
    if base_conn().is_none() {
        eprintln!("SKIP pre_0_0_4_backup_restores_after_migrating: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    indexed_source(&s, "old_src").await;
    let backup = s
        .engine
        .create_backup(ACCOUNT, "old_src", "b")
        .await
        .expect("backup");
    sqlx::query("DROP TABLE backup_definitions")
        .execute(&s.catalog)
        .await
        .expect("roll the catalog back to 0.0.3");
    for _ in 0..2 {
        sqlx::raw_sql(include_str!("../migrations/003_backup_definitions.sql"))
            .execute(&s.catalog)
            .await
            .expect("apply migration 003");
    }
    let version: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE key = 'catalog_version'")
            .fetch_one(&s.catalog)
            .await
            .expect("version");
    assert_eq!(version, "0.0.4");

    let desc = s
        .engine
        .restore_table_from_backup(
            ACCOUNT,
            "old_dst",
            &backup.backup_arn,
            RestoreTableOverrides::default(),
        )
        .await
        .expect("restore a pre-0.0.4 backup");
    assert_eq!(table_status(&s, "old_dst").await.as_deref(), Some("ACTIVE"));
    assert!(index_ids(&s, &desc.table_id).await.is_empty());
    assert_eq!(data_rows(&s, &desc.table_id).await, 30);
    let restored = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "old_dst".to_owned(),
            },
        )
        .await
        .expect("describe");
    assert_eq!(
        (
            restored.provisioned_throughput.read_capacity_units,
            restored.provisioned_throughput.write_capacity_units
        ),
        (5, 5)
    );
    s.cleanup().await;
}

/// A real CreateBackup pins its item snapshot before its final catalog INSERT.
/// Holding that INSERT proves exactly when the snapshot must already exist:
/// an item committed while the INSERT waits is live, but absent from the backup.
#[tokio::test]
async fn production_create_backup_excludes_item_committed_after_snapshot_pin() {
    if base_conn().is_none() {
        eprintln!(
            "SKIP production_create_backup_excludes_item_committed_after_snapshot_pin: no PostgreSQL"
        );
        return;
    }
    let s = scratch().await;
    let (_, table) = snapshot_source(&s, "prod_snap").await;

    let mut gate = s.catalog.begin().await.expect("begin backups gate");
    sqlx::query("LOCK TABLE backups IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *gate)
        .await
        .expect("block CreateBackup's catalog insert");

    let mut backup_fut = s.engine.create_backup(ACCOUNT, "prod_snap", "b");
    let early = tokio::time::timeout(std::time::Duration::from_secs(2), &mut backup_fut).await;
    assert!(
        early.is_err(),
        "CreateBackup did not wait at its final backups INSERT"
    );
    let insert_is_waiting: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_locks \
         WHERE relation = 'backups'::regclass AND NOT granted)",
    )
    .fetch_one(&s.catalog)
    .await
    .expect("observe the blocked backup insert");
    assert!(
        insert_is_waiting,
        "CreateBackup was blocked somewhere other than its final backups INSERT"
    );

    // This commit is after the production path's relation SELECT, but before
    // its first item cursor. Without that SELECT, the later cursor is the first
    // snapshot-establishing statement and incorrectly includes this row.
    insert_snapshot_item(&s.db, &table, "after").await;
    gate.commit().await.expect("release the backup insert");
    let backup = backup_fut.await.expect("finish the production backup");

    let backed_up: i64 =
        sqlx::query_scalar("SELECT count(*) FROM backup_items WHERE backup_arn = $1")
            .bind(&backup.backup_arn)
            .fetch_one(&s.catalog)
            .await
            .expect("count production backup items");
    assert_eq!(backed_up, 1, "the backup included a post-snapshot item");
    assert_eq!(
        snapshot_row_count(&s.db, &table).await,
        2,
        "the concurrent item must have committed"
    );
    s.cleanup().await;
}

/// DeleteBackup wins the backup-row lock before restore registers its target.
/// Restore must wait, then observe the deleted backup and leave no target.
#[tokio::test]
async fn delete_backup_winning_before_restore_leaves_no_target() {
    if base_conn().is_none() {
        eprintln!("SKIP delete_backup_winning_before_restore_leaves_no_target: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    snapshot_source(&s, "delete_first_src").await;
    let backup = s
        .engine
        .create_backup(ACCOUNT, "delete_first_src", "b")
        .await
        .expect("backup");

    // DeleteBackup locks the backup FOR UPDATE before reading table_restores.
    // Gate that read so the lock is held while restore reaches its FOR SHARE.
    let mut gate = s.catalog.begin().await.expect("begin provenance gate");
    sqlx::query("LOCK TABLE table_restores IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *gate)
        .await
        .expect("block DeleteBackup after its row lock");
    let mut delete_fut = s.engine.delete_backup(ACCOUNT, &backup.backup_arn);
    let early_delete =
        tokio::time::timeout(std::time::Duration::from_secs(2), &mut delete_fut).await;
    assert!(early_delete.is_err(), "DeleteBackup did not reach the gate");
    let delete_is_waiting: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_locks \
         WHERE relation = 'table_restores'::regclass AND NOT granted)",
    )
    .fetch_one(&s.catalog)
    .await
    .expect("observe blocked DeleteBackup");
    assert!(
        delete_is_waiting,
        "DeleteBackup was not paused after its row lock"
    );

    let mut restore_fut = s.engine.restore_table_from_backup(
        ACCOUNT,
        "delete_first_dst",
        &backup.backup_arn,
        RestoreTableOverrides::default(),
    );
    let early_restore =
        tokio::time::timeout(std::time::Duration::from_millis(500), &mut restore_fut).await;
    assert!(
        early_restore.is_err(),
        "restore did not wait for DeleteBackup's row lock"
    );
    assert_eq!(
        table_id_of(&s, "delete_first_dst").await,
        None,
        "restore created its target before locking the backup"
    );

    gate.commit().await.expect("let DeleteBackup finish");
    delete_fut.await.expect("DeleteBackup wins the ordering");
    let err = restore_fut
        .await
        .expect_err("restore must observe that the backup was deleted");
    assert!(
        matches!(&err, StorageError::Validation(message) if message.contains("Backup not found")),
        "{err:?}"
    );
    assert_eq!(table_id_of(&s, "delete_first_dst").await, None);
    s.cleanup().await;
}

/// Restore commits its CREATING target and provenance before copying items.
/// DeleteBackup must then see the two rows together and return BackupInUse.
#[tokio::test]
async fn restore_registration_winning_before_delete_returns_backup_in_use() {
    if base_conn().is_none() {
        eprintln!(
            "SKIP restore_registration_winning_before_delete_returns_backup_in_use: no PostgreSQL"
        );
        return;
    }
    let s = scratch().await;
    snapshot_source(&s, "restore_first_src").await;
    let backup = s
        .engine
        .create_backup(ACCOUNT, "restore_first_src", "b")
        .await
        .expect("backup");

    let mut gate = s.catalog.begin().await.expect("begin item-copy gate");
    sqlx::query("LOCK TABLE backup_items IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *gate)
        .await
        .expect("pause restore after target registration");
    let mut restore_fut = s.engine.restore_table_from_backup(
        ACCOUNT,
        "restore_first_dst",
        &backup.backup_arn,
        RestoreTableOverrides::default(),
    );
    let early_restore =
        tokio::time::timeout(std::time::Duration::from_secs(2), &mut restore_fut).await;
    assert!(
        early_restore.is_err(),
        "restore did not reach the item-copy gate"
    );
    let copy_is_waiting: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_locks \
         WHERE relation = 'backup_items'::regclass AND NOT granted)",
    )
    .fetch_one(&s.catalog)
    .await
    .expect("observe blocked restore copy");
    assert!(copy_is_waiting, "restore was not paused in its item copy");

    // One statement observes both rows, after the create-table transaction has
    // committed but before the copy can finish.
    let registered: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tables t \
         JOIN table_restores r ON r.table_id = t.table_id \
         WHERE t.account_id = $1 AND t.table_name = $2 \
           AND t.table_status = 'CREATING' AND r.source_backup_arn = $3)",
    )
    .bind(ACCOUNT)
    .bind("restore_first_dst")
    .bind(&backup.backup_arn)
    .fetch_one(&s.catalog)
    .await
    .expect("observe atomic restore registration");
    assert!(registered, "target and provenance did not commit together");

    let err = s
        .engine
        .delete_backup(ACCOUNT, &backup.backup_arn)
        .await
        .expect_err("a registered in-progress restore keeps its backup in use");
    assert!(matches!(err, StorageError::BackupInUse(_)), "{err:?}");

    gate.commit().await.expect("release the restore copy");
    let desc = restore_fut
        .await
        .expect("restore completes after the gate opens");
    assert_eq!(
        table_status(&s, "restore_first_dst").await.as_deref(),
        Some("ACTIVE")
    );
    assert_eq!(data_rows(&s, &desc.table_id).await, 1);
    s.cleanup().await;
}

/// LOCK TABLE does not establish a REPEATABLE READ snapshot. This is the old
/// CreateBackup shape: after the catalog barrier would have been released, a
/// committed insert is visible to the first item SELECT.
#[tokio::test]
async fn backup_snapshot_with_lock_only_includes_later_insert() {
    if base_conn().is_none() {
        eprintln!("SKIP backup_snapshot_with_lock_only_includes_later_insert: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    let (_, table) = snapshot_source(&s, "old_snap").await;
    assert_eq!(snapshot_row_count(&s.db, &table).await, 1);

    let mut snapshot =
        s.db.begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .expect("begin old-shape snapshot");
    sqlx::query(&format!("LOCK TABLE {table} IN ACCESS SHARE MODE"))
        .execute(&mut *snapshot)
        .await
        .expect("lock the source relation");

    // This commits after the point where CreateBackup released its catalog
    // barrier. Because no SELECT fixed the snapshot, the item read sees it.
    insert_snapshot_item(&s.db, &table, "after").await;
    let visible: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(&mut *snapshot)
        .await
        .expect("read through the old snapshot shape");
    assert_eq!(visible, 2, "LOCK TABLE unexpectedly fixed the snapshot");
    snapshot.commit().await.expect("commit snapshot");
    s.cleanup().await;
}

/// A real relation SELECT fixes the backup's REPEATABLE READ snapshot before
/// the catalog barrier is released. A later committed item is live in the
/// database but absent from every subsequent read through that snapshot.
#[tokio::test]
async fn backup_snapshot_with_relation_read_excludes_later_insert() {
    if base_conn().is_none() {
        eprintln!("SKIP backup_snapshot_with_relation_read_excludes_later_insert: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    let (_, table) = snapshot_source(&s, "new_snap").await;

    let mut snapshot =
        s.db.begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .expect("begin backup-shaped snapshot");
    sqlx::query(&format!("LOCK TABLE {table} IN ACCESS SHARE MODE"))
        .execute(&mut *snapshot)
        .await
        .expect("lock the source relation");
    let first: Option<i32> = sqlx::query_scalar(&format!("SELECT 1 FROM {table} LIMIT 1"))
        .fetch_optional(&mut *snapshot)
        .await
        .expect("fix the snapshot by reading the relation");
    assert_eq!(first, Some(1));

    // This is the first write after the catalog barrier is released.
    insert_snapshot_item(&s.db, &table, "after").await;
    let visible: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
        .fetch_one(&mut *snapshot)
        .await
        .expect("read through the fixed snapshot");
    assert_eq!(visible, 1, "the fixed snapshot included a later item");
    snapshot.commit().await.expect("commit snapshot");

    // Liveness beside the isolation assertion: the writer did commit and the
    // current database state moved, even though the backup snapshot did not.
    assert_eq!(snapshot_row_count(&s.db, &table).await, 2);
    s.cleanup().await;
}

/// A backup's ACCESS SHARE lock can delay DROP TABLE, but it must not make the
/// control-plane pass wait indefinitely or lose the DELETING row needed to
/// retry once the backup releases its snapshot.
#[tokio::test]
async fn control_plane_drop_timeout_leaves_table_deleting_for_retry() {
    if base_conn().is_none() {
        eprintln!("SKIP control_plane_drop_timeout_leaves_table_deleting_for_retry: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    let (table_id, table) = snapshot_source(&s, "drop_retry").await;
    sqlx::query(
        "UPDATE tables SET table_status = 'DELETING', status_transition_at = NOW() \
         WHERE table_id = $1",
    )
    .bind(&table_id)
    .execute(&s.catalog)
    .await
    .expect("schedule table deletion");

    let mut backup =
        s.db.begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .expect("begin backup-shaped snapshot");
    sqlx::query(&format!("LOCK TABLE {table} IN ACCESS SHARE MODE"))
        .execute(&mut *backup)
        .await
        .expect("hold the backup table lock");
    let _: Option<i32> = sqlx::query_scalar(&format!("SELECT 1 FROM {table} LIMIT 1"))
        .fetch_optional(&mut *backup)
        .await
        .expect("fix the backup snapshot");

    let first = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        s.engine.process_control_plane_transitions(),
    )
    .await
    .expect("blocked DROP must respect its lock timeout")
    .expect("a timed-out drop is retriable, not a failed worker pass");
    assert!(!first.iter().any(|(name, _)| name == "drop_retry"));
    assert_eq!(
        table_status(&s, "drop_retry").await.as_deref(),
        Some("DELETING")
    );
    assert!(data_table_exists(&s, &table_id).await);

    backup.commit().await.expect("release the backup lock");
    let second = s
        .engine
        .process_control_plane_transitions()
        .await
        .expect("retry the data drop");
    assert!(
        second.iter().any(|(name, _)| name == "drop_retry"),
        "{second:?}"
    );
    assert_eq!(table_status(&s, "drop_retry").await, None);
    assert!(!data_table_exists(&s, &table_id).await);
    s.cleanup().await;
}

/// The table row is held FOR SHARE until the data snapshot is taken, so a
/// definition change cannot commit between the two: an UpdateTable blocked
/// on the row waits for the backup's barrier, and the backup records the
/// definition in force when its items were read.
#[tokio::test]
async fn backup_definition_and_items_share_one_instant() {
    if base_conn().is_none() {
        eprintln!("SKIP backup_definition_and_items_share_one_instant: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    indexed_source(&s, "bar_src").await;
    let table_id = table_id_of(&s, "bar_src").await.expect("table");

    // Hold the row the way UpdateTable does, and change the billing mode
    // without committing yet.
    let mut writer = s.catalog.begin().await.expect("writer");
    sqlx::query("SELECT 1 FROM tables WHERE table_id = $1 FOR UPDATE")
        .bind(&table_id)
        .execute(&mut *writer)
        .await
        .expect("lock the row");
    sqlx::query("UPDATE tables SET billing_mode = 'PAY_PER_REQUEST' WHERE table_id = $1")
        .bind(&table_id)
        .execute(&mut *writer)
        .await
        .expect("change the definition");

    // The backup must wait for the writer rather than read around it.
    let arn = {
        let backup = s.engine.create_backup(ACCOUNT, "bar_src", "b");
        tokio::pin!(backup);
        let early = tokio::time::timeout(std::time::Duration::from_millis(500), &mut backup).await;
        assert!(
            early.is_err(),
            "CreateBackup read the definition past an uncommitted change"
        );
        writer.commit().await.expect("commit the change");
        backup.await.expect("backup").backup_arn
    };

    let def: serde_json::Value =
        sqlx::query_scalar("SELECT definition FROM backup_definitions WHERE backup_arn = $1")
            .bind(&arn)
            .fetch_one(&s.catalog)
            .await
            .expect("definition");
    assert_eq!(def["BillingMode"], "PAY_PER_REQUEST");
    s.cleanup().await;
}

/// UpdateTable commits a new GSI's catalog row before it builds the index's
/// data table. A backup taken in between leaves that index out (it is not
/// part of the table yet), drops the attribute definition only it used, and
/// restores.
#[tokio::test]
async fn backup_leaves_out_a_gsi_still_being_built() {
    if base_conn().is_none() {
        eprintln!("SKIP backup_leaves_out_a_gsi_still_being_built: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    let src = indexed_source(&s, "unb_src").await;
    let gi = index_ids(&s, &src)
        .await
        .into_iter()
        .find(|(n, _)| n == "gi")
        .expect("gi")
        .1;
    sqlx::query(&format!("DROP TABLE \"_ddb_{gi}\""))
        .execute(&s.db)
        .await
        .expect("make the GSI look half-built");
    let backup = s
        .engine
        .create_backup(ACCOUNT, "unb_src", "b")
        .await
        .expect("backup");
    let def: serde_json::Value =
        sqlx::query_scalar("SELECT definition FROM backup_definitions WHERE backup_arn = $1")
            .bind(&backup.backup_arn)
            .fetch_one(&s.catalog)
            .await
            .expect("definition");
    assert_eq!(def["GlobalSecondaryIndexes"], serde_json::json!([]));
    let attrs: serde_json::Value =
        sqlx::query_scalar("SELECT attribute_definitions FROM backups WHERE backup_arn = $1")
            .bind(&backup.backup_arn)
            .fetch_one(&s.catalog)
            .await
            .expect("attrs");
    let names: Vec<&str> = attrs
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|a| a["AttributeName"].as_str())
        .collect();
    assert!(!names.contains(&"g"), "{names:?}");
    assert!(names.contains(&"l"), "the LSI still uses l: {names:?}");

    let desc = s
        .engine
        .restore_table_from_backup(
            ACCOUNT,
            "unb_dst",
            &backup.backup_arn,
            RestoreTableOverrides::default(),
        )
        .await
        .expect("restore");
    assert_eq!(table_status(&s, "unb_dst").await.as_deref(), Some("ACTIVE"));
    let names: Vec<String> = index_ids(&s, &desc.table_id)
        .await
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(names, ["li"]);
    s.cleanup().await;
}

#[tokio::test]
async fn delete_table_refuses_a_restore_in_progress() {
    if base_conn().is_none() {
        eprintln!("SKIP delete_table_refuses_a_restore_in_progress: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    indexed_source(&s, "dt_src").await;
    let backup = s
        .engine
        .create_backup(ACCOUNT, "dt_src", "b")
        .await
        .expect("backup");
    let desc = s
        .engine
        .restore_table_from_backup(
            ACCOUNT,
            "dt_dst",
            &backup.backup_arn,
            RestoreTableOverrides::default(),
        )
        .await
        .expect("restore");
    sqlx::query(
        "UPDATE tables SET table_status = 'CREATING', status_transition_at = NULL \
         WHERE table_id = $1",
    )
    .bind(&desc.table_id)
    .execute(&s.catalog)
    .await
    .expect("back to in progress");
    let err = s
        .engine
        .delete_table(
            ACCOUNT,
            extenddb_core::types::DeleteTableInput {
                table_name: "dt_dst".to_owned(),
            },
        )
        .await
        .expect_err("refused");
    assert!(matches!(err, StorageError::IndexesInUse(_)), "{err:?}");
    assert_eq!(
        table_status(&s, "dt_dst").await.as_deref(),
        Some("CREATING")
    );
    s.cleanup().await;
}

/// DescribeTable reports where a restored table came from, in progress
/// while it is CREATING and done once ACTIVE; DeleteBackup is refused while a
/// restore from the backup is still running, and allowed afterwards, with
/// the summary still naming the deleted backup.
#[tokio::test]
async fn restore_summary_and_backup_in_use() {
    if base_conn().is_none() {
        eprintln!("SKIP restore_summary_and_backup_in_use: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    indexed_source(&s, "rs_src").await;
    let backup = s
        .engine
        .create_backup(ACCOUNT, "rs_src", "b")
        .await
        .expect("backup");
    let desc = s
        .engine
        .restore_table_from_backup(
            ACCOUNT,
            "rs_dst",
            &backup.backup_arn,
            RestoreTableOverrides::default(),
        )
        .await
        .expect("restore");
    let describe = |name: &'static str| {
        let engine = &s.engine;
        async move {
            engine
                .describe_table(
                    ACCOUNT,
                    DescribeTableInput {
                        table_name: name.to_owned(),
                    },
                )
                .await
                .expect("describe")
        }
    };
    let done = describe("rs_dst").await.restore_summary.expect("summary");
    assert_eq!(
        done.source_backup_arn.as_deref(),
        Some(backup.backup_arn.as_str())
    );
    assert!(!done.restore_in_progress);
    assert!(done.restore_date_time > 0.0);
    assert!(describe("rs_src").await.restore_summary.is_none());

    // While the restore is still running: in progress, and the backup in use.
    sqlx::query(
        "UPDATE tables SET table_status = 'CREATING', status_transition_at = NULL \
         WHERE table_id = $1",
    )
    .bind(&desc.table_id)
    .execute(&s.catalog)
    .await
    .expect("back to in progress");
    assert!(
        describe("rs_dst")
            .await
            .restore_summary
            .expect("summary")
            .restore_in_progress
    );
    let err = s
        .engine
        .delete_backup(ACCOUNT, &backup.backup_arn)
        .await
        .expect_err("in use");
    assert!(matches!(err, StorageError::BackupInUse(_)), "{err:?}");

    sqlx::query("UPDATE tables SET table_status = 'ACTIVE' WHERE table_id = $1")
        .bind(&desc.table_id)
        .execute(&s.catalog)
        .await
        .expect("finished");
    s.engine
        .delete_backup(ACCOUNT, &backup.backup_arn)
        .await
        .expect("deletable once the restore is done");
    let after = describe("rs_dst").await.restore_summary.expect("summary");
    assert_eq!(
        after.source_backup_arn.as_deref(),
        Some(backup.backup_arn.as_str())
    );
    s.cleanup().await;
}
