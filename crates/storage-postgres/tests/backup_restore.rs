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
//! - the reconciler's duties on a dead or failed restore, which need a crash
//!   to reach through the server and are driven here through the
//!   `BackupCatalog` methods the reconciler calls.
//!
//! Each test builds a throwaway catalog and data database and drops them when
//! it ends, passing or failing. The tests run one at a time: each opens its
//! own pools, and running them all at once can exhaust the server's
//! connection limit. Requires `EXTENDDB_TEST_PG_CONNECTION_STRING` (a base URL with no database
//! component); without it every test reports a skip and passes.

use extenddb_core::limits::LimitsConfig;
use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, DescribeTableInput, Item, KeySchemaElement,
    KeyType, ScalarAttributeType, TableDescription, TableKeyInfo, TableStatus,
};
use extenddb_core::validation::validate_create_table;
use extenddb_storage::error::StorageError;
use extenddb_storage::migrations::Runner;
use extenddb_storage::{
    DataEngine, JobPermit, LegacyBackupReader, RestoreRequest, RestoreTableOverrides, TableEngine,
};
use extenddb_storage_postgres::migrations::{PostgresMigrations, PostgresTarget};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use futures::FutureExt;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
/// A second account, for the cross-account checks.
const OTHER_ACCOUNT: &str = "210987654321";
const REGION: &str = "us-east-1";
use extenddb_storage_postgres::RESTORE_LOCK_SEED;

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

/// The server's base URL, or `None` to skip. With `EXTENDDB_TEST_PG_REQUIRED`
/// set to any value but `0`, a job that must reach a server fails here
/// instead.
fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").unwrap_or_default();
    if conn.trim().is_empty() {
        let required =
            std::env::var("EXTENDDB_TEST_PG_REQUIRED").is_ok_and(|v| !v.is_empty() && v != "0");
        assert!(
            !required,
            "EXTENDDB_TEST_PG_CONNECTION_STRING is not set, and EXTENDDB_TEST_PG_REQUIRED says \
             this job must reach a PostgreSQL server"
        );
        return None;
    }
    Some(conn.trim_end_matches('/').to_owned())
}

async fn connect(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(url)
        .await
        .expect("connect to a scratch database")
}

async fn scratch() -> Scratch {
    scratch_with_pool(4).await
}

async fn scratch_with_pool(pool_size: u32) -> Scratch {
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
    // A fresh install, migrated by the runner as `init` does.
    let target = PostgresTarget::new(catalog_url.parse().expect("a postgres URL"), &data_name)
        .with_data_connection_row(&data_url);
    let runner = PostgresMigrations::open_unlocked(&target)
        .await
        .expect("open the migration runner");
    Runner::new(&runner, "test")
        .migrate()
        .await
        .expect("migrate the scratch databases");
    runner.close().await;
    let catalog = connect(&catalog_url).await;
    let db = connect(&data_url).await;
    for key in ["control_plane_delay_seconds", "index_propagation_delay_ms"] {
        // Zero: no control-plane delay, and synchronous index propagation,
        // as no queue worker runs here.
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ($1, '0') \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(key)
        .execute(&catalog)
        .await
        .expect("set a delay to zero");
    }
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('data_database_connection_string', $1) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(&data_url)
    .execute(&catalog)
    .await
    .expect("point the catalog at the data database");
    for (account, suffix) in [(ACCOUNT, "a"), (OTHER_ACCOUNT, "b")] {
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
            .bind(account)
            .bind(format!("acct-{stem}-{suffix}"))
            .execute(&catalog)
            .await
            .expect("seed the account row");
    }
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: catalog_url,
            pool_size,
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
        .create(ACCOUNT, &source_name, "legacy", JobPermit::detached())
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

async fn table_status_in(s: &Scratch, account: &str, name: &str) -> Option<String> {
    sqlx::query_scalar("SELECT table_status FROM tables WHERE account_id = $1 AND table_name = $2")
        .bind(account)
        .bind(name)
        .fetch_optional(&s.catalog)
        .await
        .expect("read the table status")
}

/// The ARN of a backup that does not exist, with a well-formed id: `arn`
/// with its last hex digit changed.
fn flip_last_hex(arn: &str) -> String {
    let (head, last) = arn.split_at(arn.len() - 1);
    format!("{head}{}", if last == "0" { "1" } else { "0" })
}

/// What the engine does before it calls storage (CONTRACT section 5, steps
/// 4-7): resolve the backup, run the refusals, build the input with no
/// overrides, and validate it as CreateTable would.
async fn restore_request(
    s: &Scratch,
    account: &str,
    target: &str,
    backup_arn: &str,
) -> Result<RestoreRequest, StorageError> {
    let source = s.engine.restore_source(account, backup_arn).await?;
    source.ensure_restorable()?;
    let input = source.restore_input(target, &RestoreTableOverrides::default());
    validate_create_table(&input, &LimitsConfig::default())
        .map_err(|e| StorageError::Validation(format!("{e:?}")))?;
    Ok(RestoreRequest {
        backup_arn: backup_arn.to_owned(),
        source_table_arn: source.source_table_arn.clone(),
        restore_date_time: source.backup_creation_date_time,
        input,
    })
}

/// Register a restore and return the backend's response, which comes back
/// once the CREATING target is registered, before its copy finishes.
async fn start_restore_in(
    s: &Scratch,
    account: &str,
    target: &str,
    backup_arn: &str,
) -> Result<TableDescription, StorageError> {
    let request = restore_request(s, account, target, backup_arn).await?;
    let desc = s
        .engine
        .restore(account, request, JobPermit::detached())
        .await?;
    assert_eq!(
        desc.table_status,
        TableStatus::Creating,
        "a restore responds CREATING"
    );
    Ok(desc)
}

async fn start_restore(
    s: &Scratch,
    target: &str,
    backup_arn: &str,
) -> Result<TableDescription, StorageError> {
    start_restore_in(s, ACCOUNT, target, backup_arn).await
}

/// Poll the catalog until `name` is neither CREATING nor DELETING (the
/// detached copy finished, or its failure path finished removing the target),
/// and return its status then (None: removed).
async fn settled_status_in(s: &Scratch, account: &str, name: &str) -> Option<String> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(90);
    loop {
        let status = table_status_in(s, account, name).await;
        if !matches!(status.as_deref(), Some("CREATING" | "DELETING")) {
            return status;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "restore target {name} still {status:?} after 90 s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

async fn settled_status(s: &Scratch, name: &str) -> Option<String> {
    settled_status_in(s, ACCOUNT, name).await
}

/// Start a restore and wait for its detached copy to finish; returns the
/// CREATING response.
async fn restore(
    s: &Scratch,
    target: &str,
    backup_arn: &str,
) -> Result<TableDescription, StorageError> {
    let desc = start_restore(s, target, backup_arn).await?;
    settled_status(s, target).await;
    Ok(desc)
}

/// Hold `backup_items` ACCESS EXCLUSIVE: a restore's copy blocks on its first
/// read of the backup's items, after the target is registered. Commit the
/// returned transaction to let it go on.
async fn gate_copies(s: &Scratch) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut gate = s.catalog.begin().await.expect("begin item-copy gate");
    sqlx::query("LOCK TABLE backup_items IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *gate)
        .await
        .expect("pause restores in their item copy");
    gate
}

/// How many sessions are waiting on the `backup_items` gate.
async fn copies_waiting(s: &Scratch) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks WHERE relation = 'backup_items'::regclass AND NOT granted",
    )
    .fetch_one(&s.catalog)
    .await
    .expect("count blocked copies")
}

/// Wait until `n` copies are blocked on the gate.
async fn await_copies_waiting(s: &Scratch, n: i64) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while copies_waiting(s).await < n {
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected {n} restore copies blocked on the gate, saw {}",
            copies_waiting(s).await
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// The backend pid holding the restore lock for `account`/`table`, if any.
async fn restore_lock_holder(s: &Scratch, account: &str, table: &str) -> Option<i32> {
    advisory_lock_holder(s, &format!("{account}/{table}")).await
}

/// The backend pid holding the claim lock of backup `backup_id`
/// (`backup:<id>` under the shared seed), if any.
async fn backup_lock_holder(s: &Scratch, backup_id: &str) -> Option<i32> {
    advisory_lock_holder(s, &format!("backup:{backup_id}")).await
}

/// The backend pid holding the advisory lock `hashtextextended(key,
/// RESTORE_LOCK_SEED)` on the catalog database, if any.
async fn advisory_lock_holder(s: &Scratch, key: &str) -> Option<i32> {
    sqlx::query_scalar(
        "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted \
         AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
         AND ((classid::bigint << 32) | objid::bigint) = hashtextextended($1, $2)",
    )
    .bind(key)
    .bind(RESTORE_LOCK_SEED)
    .fetch_optional(&s.catalog)
    .await
    .expect("find the advisory lock holder")
}

/// Rows of `table_restores` for a target: (source backup ARN, source table
/// ARN, restore time as epoch seconds).
async fn registration(s: &Scratch, table_id: &str) -> Option<(String, String, f64)> {
    sqlx::query_as(
        "SELECT source_backup_arn, source_table_arn, \
         EXTRACT(EPOCH FROM restore_date_time)::FLOAT8 FROM table_restores WHERE table_id = $1",
    )
    .bind(table_id)
    .fetch_optional(&s.catalog)
    .await
    .expect("read table_restores")
}

/// Data tables (`_ddb_*`) in the data database.
async fn data_table_count(s: &Scratch) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE '\\_ddb\\_%'",
    )
    .fetch_one(&s.db)
    .await
    .expect("count data tables")
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

    restore(&s, "legacy_dst", &arn)
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

    // The failure is in the detached copy, so the request itself succeeds:
    // the target is registered and reported CREATING.
    let desc = start_restore(&s, "broken_dst", &arn)
        .await
        .expect("registration succeeds; the copy is what fails");
    assert_eq!(
        settled_status(&s, "broken_dst").await,
        None,
        "a backup row without its sort key cannot restore; the copy removes the target"
    );

    // Neither CREATING, DELETING, nor half-filled: the target, its
    // registration, and its data table are gone and the name is free.
    assert_eq!(registration(&s, &desc.table_id).await, None);
    assert_eq!(
        data_table_count(&s).await,
        1,
        "only the source table's data table remains"
    );
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
    start_restore(&s, "dup_dst", &arn)
        .await
        .expect("registration succeeds; the copy is what fails");
    assert_eq!(
        settled_status(&s, "dup_dst").await,
        None,
        "two rows for one key cannot restore"
    );
    assert_eq!(data_table_count(&s).await, 1);
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
    // creating anything. The refusal is the engine's (ensure_restorable, run
    // by the helper as the engine runs it); this pins that the PostgreSQL
    // restore source carries the key schema it needs to refuse.
    let err = start_restore(&s, "multi_dst", &arn)
        .await
        .expect_err("a multi-part key backup is refused");
    assert!(matches!(err, StorageError::Unsupported(_)), "{err:?}");
    assert_eq!(table_status(&s, "multi_dst").await, None);
    s.cleanup().await;
}

/// A provisioned (pk S, sk N) table with one GSI and one LSI, holding 30
/// items, some of them outside each index.
async fn indexed_source(s: &Scratch, name: &str) -> String {
    indexed_source_with(s, name, 30).await
}

/// [`indexed_source`] with `items` items written through PutItem.
async fn indexed_source_with(s: &Scratch, name: &str, items: usize) -> String {
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
            "IndexName": "gix",
            "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "KEYS_ONLY"},
            "ProvisionedThroughput": {"ReadCapacityUnits": 3, "WriteCapacityUnits": 4}
        }],
        "LocalSecondaryIndexes": [{
            "IndexName": "lix",
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
    for i in 0..items {
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
        .create(ACCOUNT, "idx_src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let desc = restore(&s, "idx_dst", &backup.backup_arn)
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
        ["gix", "lix"]
    );
    for ((_, a), (_, b)) in src_idx.iter().zip(&dst_idx) {
        let want = data_rows(&s, a).await;
        assert!(want > 0);
        assert_eq!(data_rows(&s, b).await, want);
    }
    assert_eq!(data_rows(&s, &desc.table_id).await, 30);
    s.cleanup().await;
}

/// The reconciler's duty for a legacy restore whose owner died: a RUNNING
/// job record is dead once its heartbeat is older than the stale window on
/// the catalog's clock, whatever the table's age and whether or not the
/// advisory lock is free; a stale record is marked FAILED and its target
/// removed through `remove_restore_target`, with its data and index tables;
/// a fresh record, and a record that is no longer RUNNING, are left alone.
#[tokio::test]
async fn abandoned_restore_is_removed_only_when_its_heartbeat_is_stale() {
    use extenddb_storage::BackupCatalog;
    use extenddb_storage::backup_catalog::{RestoreKind, RestoreTargetKey};
    if base_conn().is_none() {
        eprintln!(
            "SKIP abandoned_restore_is_removed_only_when_its_heartbeat_is_stale: no PostgreSQL"
        );
        return;
    }
    let stale = std::time::Duration::from_secs(300);
    let s = scratch().await;
    indexed_source(&s, "ab_src").await;
    let backup = s
        .engine
        .create(ACCOUNT, "ab_src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let desc = restore(&s, "ab_dst", &backup.backup_arn)
        .await
        .expect("restore");
    assert_eq!(table_status(&s, "ab_dst").await.as_deref(), Some("ACTIVE"));
    assert_eq!(
        job_status(&s, &desc.table_id).await.as_deref(),
        Some("DONE"),
        "a finished restore's record is DONE"
    );
    let indexes = index_ids(&s, &desc.table_id).await;
    let key = RestoreTargetKey {
        account_id: ACCOUNT.to_owned(),
        table_name: "ab_dst".to_owned(),
        table_id: desc.table_id.clone(),
    };

    // The state a process killed mid-copy leaves: the target CREATING with no
    // scheduled transition, its job record RUNNING with a heartbeat `age`
    // old, some of its copy's batches committed.
    let crash = |age: &'static str| {
        let catalog = s.catalog.clone();
        let id = desc.table_id.clone();
        async move {
            sqlx::query(
                "UPDATE tables SET table_status = 'CREATING', status_transition_at = NULL \
                 WHERE table_id = $1",
            )
            .bind(&id)
            .execute(&catalog)
            .await
            .expect("simulate a crash");
            sqlx::query(&format!(
                "UPDATE table_restores SET status = 'RUNNING', completed_at = NULL, \
                 heartbeat_at = NOW() - INTERVAL '{age}' WHERE table_id = $1"
            ))
            .bind(&id)
            .execute(&catalog)
            .await
            .expect("age the heartbeat");
        }
    };

    // A fresh heartbeat: a live restore, here or on another instance, even
    // though the table itself is old.
    crash("1 second").await;
    sqlx::query("UPDATE tables SET creation_date_time = NOW() - INTERVAL '10 minutes'")
        .execute(&s.catalog)
        .await
        .expect("age every table");
    assert!(
        s.engine
            .list_dead_restore_jobs(stale)
            .await
            .expect("list")
            .is_empty()
    );
    assert_eq!(
        table_status(&s, "ab_dst").await.as_deref(),
        Some("CREATING")
    );

    // A stale heartbeat: dead, whether or not its lock session is still
    // around (a free lock shows the session ended, not that the process did,
    // and the record's heartbeat is what the reconciler waits on).
    crash("10 minutes").await;
    let dead = s.engine.list_dead_restore_jobs(stale).await.expect("list");
    assert_eq!(dead.len(), 1, "{dead:?}");
    assert_eq!(dead[0].target, key);
    assert_eq!(dead[0].kind, RestoreKind::Legacy);
    assert_eq!(dead[0].source_backup_arn, backup.backup_arn);
    assert!(
        s.engine
            .fail_restore_job(&key, "owner dead")
            .await
            .expect("fail")
    );
    assert_eq!(
        job_status(&s, &desc.table_id).await.as_deref(),
        Some("FAILED")
    );
    assert!(
        !s.engine
            .fail_restore_job(&key, "owner dead")
            .await
            .expect("fail again"),
        "a record that is no longer RUNNING is not written twice"
    );
    assert!(s.engine.remove_restore_target(&key).await.expect("remove"));
    assert_eq!(table_status(&s, "ab_dst").await, None);
    assert!(!data_table_exists(&s, &desc.table_id).await);
    for (_, id) in &indexes {
        assert!(
            !data_table_exists(&s, id).await,
            "index table {id} left behind"
        );
    }
    assert!(
        !s.engine
            .remove_restore_target(&key)
            .await
            .expect("remove again"),
        "a removed target is removed once"
    );

    // An ACTIVE table is never a candidate, the name is free again, and a
    // key with the wrong table id (a table re-created under the name) does
    // not remove the new table.
    let again = restore(&s, "ab_dst", &backup.backup_arn)
        .await
        .expect("restore again");
    assert_eq!(table_status(&s, "ab_dst").await.as_deref(), Some("ACTIVE"));
    assert!(
        s.engine
            .list_dead_restore_jobs(stale)
            .await
            .expect("list")
            .is_empty()
    );
    assert!(
        !s.engine
            .remove_restore_target(&key)
            .await
            .expect("stale key")
    );
    sqlx::query(
        "UPDATE tables SET table_status = 'CREATING', status_transition_at = NULL WHERE table_id = $1",
    )
    .bind(&again.table_id)
    .execute(&s.catalog)
    .await
    .expect("make it a target again");
    assert!(
        !s.engine
            .remove_restore_target(&key)
            .await
            .expect("stale key"),
        "the old table id never removes the new table"
    );
    assert_eq!(
        table_status(&s, "ab_dst").await.as_deref(),
        Some("CREATING")
    );
    s.cleanup().await;
}

/// The job record's status column for a target.
async fn job_status(s: &Scratch, table_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT status FROM table_restores WHERE table_id = $1")
        .bind(table_id)
        .fetch_optional(&s.catalog)
        .await
        .expect("read the job status")
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
        .create(ACCOUNT, "fi_src", "b", JobPermit::detached())
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
    let desc = start_restore(&s, "fi_dst", &backup.backup_arn)
        .await
        .expect("registration succeeds; the copy is what fails");
    assert_eq!(
        settled_status(&s, "fi_dst").await,
        None,
        "a row without its sort key cannot restore"
    );
    assert_eq!(table_id_of(&s, "fi_dst").await, None);
    assert_eq!(registration(&s, &desc.table_id).await, None);
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

/// SSE and the on-demand limits are carried by the restore; the table class
/// is not (the service restores STANDARD; the engine's `restore_input` drops
/// it), so the restored row has no class.
#[tokio::test]
async fn restore_keeps_sse_and_on_demand_limits_but_not_table_class() {
    if base_conn().is_none() {
        eprintln!("SKIP restore_keeps_sse_and_on_demand_limits_but_not_table_class: no PostgreSQL");
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
        .create(ACCOUNT, "cls_src", "b", JobPermit::detached())
        .await
        .expect("backup");
    restore(&s, "cls_dst", &backup.backup_arn)
        .await
        .expect("restore");
    assert_eq!(table_status(&s, "cls_dst").await.as_deref(), Some("ACTIVE"));
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
    let got = read("cls_dst").await;
    assert_eq!(got.1, None, "the restore does not carry the table class");
    assert_eq!((got.0, got.2, got.3), (want.0, want.2, want.3));
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
        .create(ACCOUNT, "many_src", "b", JobPermit::detached())
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
    let desc = restore(&s, "many_dst", &backup.backup_arn)
        .await
        .expect("restore");
    assert_eq!(
        table_status(&s, "many_dst").await.as_deref(),
        Some("ACTIVE")
    );
    assert_eq!(data_rows(&s, &desc.table_id).await, 30 + 1_234);
    let src_gsi = &index_ids(&s, &src).await[0].1;
    let dst_gsi = &index_ids(&s, &desc.table_id).await[0].1;
    assert_eq!(
        data_rows(&s, dst_gsi).await,
        data_rows(&s, src_gsi).await + 1_234
    );
    s.cleanup().await;
}

/// A restore into a name whose restore lock another session holds, with no
/// CREATING row yet (the sliver before the owner's registration commits), is
/// refused as a taken name; once the owner's CREATING target is registered it
/// is `TableBeingRestored` naming that target's source backup.
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
        .create(ACCOUNT, "lk_src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let mut other = s.catalog.acquire().await.expect("connection");
    sqlx::query("SELECT pg_advisory_lock(hashtextextended($2, $1))")
        .bind(RESTORE_LOCK_SEED)
        .bind(format!("{ACCOUNT}/lk_dst"))
        .execute(&mut *other)
        .await
        .expect("another restore owns the name");
    let err = start_restore(&s, "lk_dst", &backup.backup_arn)
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

/// While a restore's target is CREATING (its copy held): a second
/// restore of the same backup is `BackupInUse`; a restore of another backup
/// into the target's name is `TableBeingRestored` naming the target and its
/// source backup; neither creates anything. Once the target is ACTIVE a
/// restore of the backup is accepted and a restore into the name is
/// `TableAlreadyExists`.
#[tokio::test]
async fn a_restore_is_refused_while_a_target_of_its_backup_is_creating() {
    if base_conn().is_none() {
        eprintln!(
            "SKIP a_restore_is_refused_while_a_target_of_its_backup_is_creating: no PostgreSQL"
        );
        return;
    }
    // Room for four gated copies plus the requests the test sends (the
    // default fixture pool of 4 would leave none).
    let s = scratch_with_pool(20).await;
    indexed_source(&s, "iu_src").await;
    let b1 = s
        .engine
        .create(ACCOUNT, "iu_src", "b1", JobPermit::detached())
        .await
        .expect("backup 1");
    let b2 = s
        .engine
        .create(ACCOUNT, "iu_src", "b2", JobPermit::detached())
        .await
        .expect("backup 2");
    // Backups for the other restores that run alongside, taken before the
    // gate (a backup writes `backup_items`, which the gate locks).
    const MORE_RESTORES: usize = 4;
    let mut more_backups = Vec::new();
    for i in 3..=(MORE_RESTORES + 1) {
        more_backups.push(
            s.engine
                .create(ACCOUNT, "iu_src", &format!("b{i}"), JobPermit::detached())
                .await
                .expect("backup")
                .backup_arn,
        );
    }
    let gate = gate_copies(&s).await;
    start_restore(&s, "iu_dst", &b1.backup_arn)
        .await
        .expect("first restore registers");
    await_copies_waiting(&s, 1).await;
    assert_eq!(
        table_status(&s, "iu_dst").await.as_deref(),
        Some("CREATING")
    );

    let second = start_restore(&s, "iu_dst2", &b1.backup_arn).await;
    assert!(
        matches!(&second, Err(StorageError::BackupInUse(m)) if m.contains("iu_dst")),
        "a second restore of a backup with a CREATING target: {second:?}"
    );
    assert_eq!(table_status(&s, "iu_dst2").await, None);
    let into_target = start_restore(&s, "iu_dst", &b2.backup_arn).await;
    match into_target {
        Err(StorageError::TableBeingRestored {
            table_name,
            backup_arn,
        }) => {
            assert_eq!(table_name, "iu_dst");
            assert_eq!(backup_arn, b1.backup_arn);
        }
        other => panic!("a restore into a CREATING restore target: {other:?}"),
    }
    // What the engine's own name check reads before its job permit: the
    // target describes as CREATING with its source backup in the
    // RestoreSummary, which is what it answers TableInUse with.
    let described = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "iu_dst".to_owned(),
            },
        )
        .await
        .expect("describe the CREATING target");
    assert_eq!(described.table_status, TableStatus::Creating);
    assert_eq!(
        described
            .restore_summary
            .as_ref()
            .and_then(|r| r.source_backup_arn.as_deref()),
        Some(b1.backup_arn.as_str()),
        "the CREATING target names its source backup"
    );
    // The name is checked before the backup: the same backup into the same
    // name, and a backup that does not exist into it, are refused as the
    // name, naming the target's source backup.
    let missing_arn = flip_last_hex(&b2.backup_arn);
    let mut missing = restore_request(&s, ACCOUNT, "iu_dst", &b2.backup_arn)
        .await
        .expect("request");
    missing.backup_arn = missing_arn;
    for (what, result) in [
        (
            "the same backup into its CREATING target",
            start_restore(&s, "iu_dst", &b1.backup_arn).await,
        ),
        (
            "a missing backup into a CREATING restore target",
            s.engine
                .restore(ACCOUNT, missing, JobPermit::detached())
                .await,
        ),
    ] {
        match result {
            Err(StorageError::TableBeingRestored {
                table_name,
                backup_arn,
            }) => {
                assert_eq!(table_name, "iu_dst", "{what}");
                assert_eq!(backup_arn, b1.backup_arn, "{what}");
            }
            other => panic!("{what}: {other:?}"),
        }
    }
    // Only the first copy is waiting: the refused restores never started one.
    assert_eq!(copies_waiting(&s).await, 1);
    // With several copies running, a client retrying its own restore is
    // still answered at once.
    let mut more = Vec::new();
    for (i, backup) in more_backups.iter().enumerate() {
        let target = format!("iu_more{i}");
        start_restore(&s, &target, backup)
            .await
            .expect("a restore alongside the first");
        more.push(target);
    }
    await_copies_waiting(&s, i64::try_from(MORE_RESTORES).unwrap()).await;
    let retried = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        start_restore(&s, "iu_dst", &b1.backup_arn),
    )
    .await
    .expect("a retry into a CREATING restore target is answered at once");
    assert!(
        matches!(&retried, Err(StorageError::TableBeingRestored { table_name, .. }) if table_name == "iu_dst"),
        "{retried:?}"
    );

    gate.commit().await.expect("release the copies");
    assert_eq!(
        settled_status(&s, "iu_dst").await.as_deref(),
        Some("ACTIVE")
    );
    for target in &more {
        assert_eq!(settled_status(&s, target).await.as_deref(), Some("ACTIVE"));
    }
    restore(&s, "iu_dst2", &b1.backup_arn)
        .await
        .expect("a restore once the first target is ACTIVE");
    let taken = start_restore(&s, "iu_dst", &b2.backup_arn).await;
    assert!(
        matches!(&taken, Err(StorageError::TableAlreadyExists(n)) if n == "iu_dst"),
        "{taken:?}"
    );
    // The former target describes as ACTIVE, so the engine's name check
    // answers TableAlreadyExists for it too.
    let former = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "iu_dst".to_owned(),
            },
        )
        .await
        .expect("describe the former target");
    assert_eq!(former.table_status, TableStatus::Active);
    s.cleanup().await;
}

/// Simultaneous: four restores of one backup started together,
/// exactly one is accepted and three are `BackupInUse`; the registration
/// locks the backup row FOR UPDATE, so the check cannot be passed twice.
#[tokio::test]
async fn simultaneous_restores_of_one_backup_admit_exactly_one() {
    if base_conn().is_none() {
        eprintln!("SKIP simultaneous_restores_of_one_backup_admit_exactly_one: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    indexed_source(&s, "sim_src").await;
    let backup = s
        .engine
        .create(ACCOUNT, "sim_src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let gate = gate_copies(&s).await;
    let mut requests = Vec::new();
    for i in 0..4 {
        requests.push(
            restore_request(&s, ACCOUNT, &format!("sim_dst{i}"), &backup.backup_arn)
                .await
                .expect("request"),
        );
    }
    let results = futures::future::join_all(
        requests
            .into_iter()
            .map(|request| s.engine.restore(ACCOUNT, request, JobPermit::detached())),
    )
    .await;
    let accepted: Vec<usize> = results
        .iter()
        .enumerate()
        .filter_map(|(i, r)| r.is_ok().then_some(i))
        .collect();
    assert_eq!(accepted.len(), 1, "exactly one is accepted: {results:?}");
    for (i, result) in results.iter().enumerate() {
        if i != accepted[0] {
            assert!(
                matches!(result, Err(StorageError::BackupInUse(_))),
                "restore {i}: {result:?}"
            );
        }
    }
    await_copies_waiting(&s, 1).await;
    gate.commit().await.expect("release the copy");
    let winner = format!("sim_dst{}", accepted[0]);
    assert_eq!(settled_status(&s, &winner).await.as_deref(), Some("ACTIVE"));
    s.cleanup().await;
}

/// A backup taken before catalog 0.0.4 has no definition row. Rolling the
/// catalog back to that shape, replaying migration 1's catalog file twice
/// (as a rerun after an interrupted upgrade would), and restoring must
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
        .create(ACCOUNT, "old_src", "b", JobPermit::detached())
        .await
        .expect("backup");
    sqlx::query("DROP TABLE backup_definitions")
        .execute(&s.catalog)
        .await
        .expect("roll the catalog back to 0.0.3");
    // Migration 1's catalog file, twice, as a rerun after an interrupted
    // upgrade runs it.
    for _ in 0..2 {
        sqlx::raw_sql(include_str!("../migrations/0001_baseline.sql"))
            .execute(&s.catalog)
            .await
            .expect("apply migration 1");
    }
    let recreated: bool =
        sqlx::query_scalar("SELECT to_regclass('public.backup_definitions') IS NOT NULL")
            .fetch_one(&s.catalog)
            .await
            .expect("look up backup_definitions");
    assert!(recreated);

    // A backup taken before 0.0.4 recorded every attribute definition of its
    // table, including those only its GSI and LSI used. It restores with no
    // secondary indexes, so the engine path keeps only the key's attribute
    // definitions; otherwise validate_create_table would refuse it.
    let source = s
        .engine
        .restore_source(ACCOUNT, &backup.backup_arn)
        .await
        .expect("legacy source");
    assert!(source.definition.is_none());
    assert_eq!(source.legacy_vector_index_count, 0);
    assert!(
        source.attribute_definitions.len() > source.key_schema.len(),
        "the fixture must record index-only attribute definitions, or this proves nothing"
    );
    let desc = restore(&s, "old_dst", &backup.backup_arn)
        .await
        .expect("restore a pre-0.0.4 backup of an indexed table");
    assert_eq!(
        settled_status(&s, "old_dst").await.as_deref(),
        Some("ACTIVE")
    );
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

    let mut backup_fut = s
        .engine
        .create(ACCOUNT, "prod_snap", "b", JobPermit::detached());
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
        .create(ACCOUNT, "delete_first_src", "b", JobPermit::detached())
        .await
        .expect("backup");

    // DeleteBackup locks the backup FOR UPDATE before reading table_restores.
    // Gate that read so the lock is held while restore reaches its FOR SHARE.
    let mut gate = s.catalog.begin().await.expect("begin provenance gate");
    sqlx::query("LOCK TABLE table_restores IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *gate)
        .await
        .expect("block DeleteBackup after its row lock");
    let mut delete_fut = s.engine.delete(ACCOUNT, &backup.backup_arn);
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

    let mut restore_fut = Box::pin(start_restore(&s, "delete_first_dst", &backup.backup_arn));
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
        .create(ACCOUNT, "restore_first_src", "b", JobPermit::detached())
        .await
        .expect("backup");

    let gate = gate_copies(&s).await;
    let desc = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        start_restore(&s, "restore_first_dst", &backup.backup_arn),
    )
    .await
    .expect("the restore responds once registered, without waiting for its copy")
    .expect("restore registers");
    await_copies_waiting(&s, 1).await;

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
        .delete(ACCOUNT, &backup.backup_arn)
        .await
        .expect_err("a registered in-progress restore keeps its backup in use");
    assert!(matches!(err, StorageError::BackupInUse(_)), "{err:?}");

    gate.commit().await.expect("release the restore copy");
    assert_eq!(
        settled_status(&s, "restore_first_dst").await.as_deref(),
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
        let backup = s
            .engine
            .create(ACCOUNT, "bar_src", "b", JobPermit::detached());
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
        .find(|(n, _)| n == "gix")
        .expect("gix")
        .1;
    sqlx::query(&format!("DROP TABLE \"_ddb_{gi}\""))
        .execute(&s.db)
        .await
        .expect("make the GSI look half-built");
    let backup = s
        .engine
        .create(ACCOUNT, "unb_src", "b", JobPermit::detached())
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

    let desc = restore(&s, "unb_dst", &backup.backup_arn)
        .await
        .expect("restore");
    assert_eq!(table_status(&s, "unb_dst").await.as_deref(), Some("ACTIVE"));
    let names: Vec<String> = index_ids(&s, &desc.table_id)
        .await
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(names, ["lix"]);
    s.cleanup().await;
}

/// (f) While the detached copy runs, the target and its source are
/// protected: DeleteTable on the target is refused (the engine maps
/// IndexesInUse to ResourceInUseException) and DeleteBackup of the source is
/// BackupInUse. Both are allowed once the copy has finished.
#[tokio::test]
async fn delete_table_and_delete_backup_are_refused_during_the_copy() {
    if base_conn().is_none() {
        eprintln!("SKIP delete_table_and_delete_backup_are_refused_during_the_copy: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    indexed_source(&s, "dt_src").await;
    let backup = s
        .engine
        .create(ACCOUNT, "dt_src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let gate = gate_copies(&s).await;
    let desc = start_restore(&s, "dt_dst", &backup.backup_arn)
        .await
        .expect("restore registers");
    await_copies_waiting(&s, 1).await;

    let err = s
        .engine
        .delete_table(
            ACCOUNT,
            extenddb_core::types::DeleteTableInput {
                table_name: "dt_dst".to_owned(),
            },
        )
        .await
        .expect_err("DeleteTable of a target being restored is refused");
    assert!(matches!(err, StorageError::IndexesInUse(_)), "{err:?}");
    let err = s
        .engine
        .delete(ACCOUNT, &backup.backup_arn)
        .await
        .expect_err("DeleteBackup of a backup being restored from is refused");
    assert!(matches!(err, StorageError::BackupInUse(_)), "{err:?}");
    // The window was real: the copy had not finished when both were refused.
    assert_eq!(
        table_status(&s, "dt_dst").await.as_deref(),
        Some("CREATING")
    );
    assert_eq!(data_rows(&s, &desc.table_id).await, 0);

    gate.commit().await.expect("release the copy");
    assert_eq!(
        settled_status(&s, "dt_dst").await.as_deref(),
        Some("ACTIVE")
    );
    assert_eq!(data_rows(&s, &desc.table_id).await, 30);
    s.engine
        .delete(ACCOUNT, &backup.backup_arn)
        .await
        .expect("deletable once the restore is done");
    s.engine
        .delete_table(
            ACCOUNT,
            extenddb_core::types::DeleteTableInput {
                table_name: "dt_dst".to_owned(),
            },
        )
        .await
        .expect("deletable once the restore is done");
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
        .create(ACCOUNT, "rs_src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let gate = gate_copies(&s).await;
    let desc = start_restore(&s, "rs_dst", &backup.backup_arn)
        .await
        .expect("restore registers");
    await_copies_waiting(&s, 1).await;
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
    let source = s
        .engine
        .restore_source(ACCOUNT, &backup.backup_arn)
        .await
        .expect("source");
    // (g) The registration records what the engine passed: the source table's
    // ARN, and the backup's creation time as the restore time.
    let (reg_backup, reg_table_arn, reg_time) =
        registration(&s, &desc.table_id).await.expect("registered");
    assert_eq!(reg_backup, backup.backup_arn);
    assert_eq!(reg_table_arn, source.source_table_arn);
    assert!(reg_table_arn.ends_with(":table/rs_src"), "{reg_table_arn}");
    assert_eq!(reg_time, backup.backup_creation_date_time);

    // While the restore is running: in progress, naming its source.
    let running = describe("rs_dst").await.restore_summary.expect("summary");
    assert!(running.restore_in_progress);
    assert_eq!(
        running.source_backup_arn.as_deref(),
        Some(backup.backup_arn.as_str())
    );
    assert_eq!(
        running.source_table_arn.as_deref(),
        Some(source.source_table_arn.as_str())
    );
    assert_eq!(running.restore_date_time, backup.backup_creation_date_time);
    assert!(describe("rs_src").await.restore_summary.is_none());
    let err = s
        .engine
        .delete(ACCOUNT, &backup.backup_arn)
        .await
        .expect_err("in use");
    assert!(matches!(err, StorageError::BackupInUse(_)), "{err:?}");

    gate.commit().await.expect("release the copy");
    assert_eq!(
        settled_status(&s, "rs_dst").await.as_deref(),
        Some("ACTIVE")
    );
    let done = describe("rs_dst").await.restore_summary.expect("summary");
    assert!(!done.restore_in_progress);
    s.engine
        .delete(ACCOUNT, &backup.backup_arn)
        .await
        .expect("deletable once the restore is done");
    let after = describe("rs_dst").await.restore_summary.expect("summary");
    assert_eq!(
        after.source_backup_arn.as_deref(),
        Some(backup.backup_arn.as_str())
    );
    s.cleanup().await;
}

/// Backup rows for an [`indexed_source`] table, every one inside both the GSI
/// (`g`) and the LSI (`l`).
fn bulk_rows(n: usize) -> Vec<serde_json::Value> {
    (0..n)
        .map(|i| {
            serde_json::json!({
                "pk": {"S": format!("bulk{}", i % 11)},
                "sk": {"N": format!("{}", 1000 + i)},
                "g": {"S": format!("g{}", i % 4)},
                "l": {"S": format!("l{i}")},
            })
        })
        .collect()
}

async fn add_backup_rows(s: &Scratch, backup_arn: &str, rows: &[serde_json::Value]) {
    sqlx::query(
        "INSERT INTO backup_items (backup_arn, pk, item_data) \
         SELECT $1, '', d FROM UNNEST($2::jsonb[]) AS t(d)",
    )
    .bind(backup_arn)
    .bind(rows)
    .execute(&s.catalog)
    .await
    .expect("add rows to the backup");
}

/// Physical ids of a table's `gi` and `li` index tables.
async fn gi_li(s: &Scratch, table_id: &str) -> (String, String) {
    let ids = index_ids(s, table_id).await;
    let get = |n: &str| {
        ids.iter()
            .find(|(name, _)| name == n)
            .unwrap_or_else(|| panic!("index {n}"))
            .1
            .clone()
    };
    (get("gix"), get("lix"))
}

/// Distinct inserting transactions of a data table's rows.
async fn xmins(s: &Scratch, physical_id: &str) -> Vec<String> {
    sqlx::query_scalar(&format!(
        "SELECT DISTINCT xmin::text FROM \"_ddb_{physical_id}\" ORDER BY 1"
    ))
    .fetch_all(&s.db)
    .await
    .expect("xmins")
}

/// (a) RestoreTableFromBackup returns CREATING once the target is
/// registered, while the copy has not read a single item, and the detached
/// copy then fills the base table and both indexes across several batches
/// (each its own committed transaction) and flips the target ACTIVE.
#[tokio::test]
async fn restore_returns_creating_and_the_detached_copy_finishes() {
    if base_conn().is_none() {
        eprintln!("SKIP restore_returns_creating_and_the_detached_copy_finishes: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    let src = indexed_source(&s, "cr_src").await;
    let backup = s
        .engine
        .create(ACCOUNT, "cr_src", "b", JobPermit::detached())
        .await
        .expect("backup");
    add_backup_rows(&s, &backup.backup_arn, &bulk_rows(1_200)).await;

    let gate = gate_copies(&s).await;
    let desc = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        start_restore(&s, "cr_dst", &backup.backup_arn),
    )
    .await
    .expect("the restore must return before its copy completes")
    .expect("restore registers");
    await_copies_waiting(&s, 1).await;
    assert_eq!(
        table_status(&s, "cr_dst").await.as_deref(),
        Some("CREATING")
    );
    assert_eq!(data_rows(&s, &desc.table_id).await, 0);

    gate.commit().await.expect("release the copy");
    assert_eq!(
        settled_status(&s, "cr_dst").await.as_deref(),
        Some("ACTIVE")
    );
    let (src_gi, src_li) = gi_li(&s, &src).await;
    let (dst_gi, dst_li) = gi_li(&s, &desc.table_id).await;
    assert_eq!(data_rows(&s, &desc.table_id).await, 30 + 1_200);
    assert_eq!(
        data_rows(&s, &dst_gi).await,
        data_rows(&s, &src_gi).await + 1_200
    );
    assert_eq!(
        data_rows(&s, &dst_li).await,
        data_rows(&s, &src_li).await + 1_200
    );
    let restored = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: "cr_dst".to_owned(),
            },
        )
        .await
        .expect("describe");
    assert_eq!(restored.item_count, 1_230);

    // 1,230 items at 500 per batch is three batches, each committed on its
    // own; every index row was written in its batch's transaction.
    let base = xmins(&s, &desc.table_id).await;
    assert_eq!(
        base.len(),
        3,
        "one committed transaction per batch: {base:?}"
    );
    for idx in [&dst_gi, &dst_li] {
        let x = xmins(&s, idx).await;
        assert!(x.iter().all(|t| base.contains(t)), "{x:?} vs {base:?}");
        assert_eq!(x.len(), 3, "index rows written per batch: {x:?}");
    }
    s.cleanup().await;
}

/// (b) A request dropped after its first poll (a client that disconnected)
/// does not stop the restore: the target appears and reaches ACTIVE with
/// every item.
#[tokio::test]
async fn a_dropped_restore_request_still_reaches_active() {
    if base_conn().is_none() {
        eprintln!("SKIP a_dropped_restore_request_still_reaches_active: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    indexed_source(&s, "drop_src").await;
    let backup = s
        .engine
        .create(ACCOUNT, "drop_src", "b", JobPermit::detached())
        .await
        .expect("backup");
    add_backup_rows(&s, &backup.backup_arn, &bulk_rows(1_200)).await;
    let request = restore_request(&s, ACCOUNT, "drop_dst", &backup.backup_arn)
        .await
        .expect("request");
    let fut = s.engine.restore(ACCOUNT, request, JobPermit::detached());
    assert!(
        fut.now_or_never().is_none(),
        "the request cannot have finished on its first poll"
    );

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while table_status(&s, "drop_dst").await.is_none() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the dropped request's restore must still create the target"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        settled_status(&s, "drop_dst").await.as_deref(),
        Some("ACTIVE")
    );
    let id = table_id_of(&s, "drop_dst").await.expect("id");
    assert_eq!(data_rows(&s, &id).await, 1_230);
    s.cleanup().await;
}

/// (d) The restore's lock session is terminated mid-copy. Either the
/// heartbeat notices and the copy stops and removes its target, or the
/// reconciler finds the record's heartbeat stale first; in both cases there
/// is one outcome, nothing is left behind, and the name restores again.
#[tokio::test]
async fn losing_the_lock_session_mid_copy_has_one_outcome() {
    use extenddb_storage::BackupCatalog;
    if base_conn().is_none() {
        eprintln!("SKIP losing_the_lock_session_mid_copy_has_one_outcome: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    indexed_source(&s, "hb_src").await;
    let backup = s
        .engine
        .create(ACCOUNT, "hb_src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let tables_before = data_table_count(&s).await;
    let terminate = |pid: i32| {
        let catalog = s.catalog.clone();
        async move {
            let done: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
                .bind(pid)
                .fetch_one(&catalog)
                .await
                .expect("terminate");
            assert!(done, "terminated the lock session");
        }
    };

    // Heartbeat path: no sweep runs; the copy itself must notice the lost
    // session (within one heartbeat period, 15 s) and remove its target.
    let gate = gate_copies(&s).await;
    let first = start_restore(&s, "hb_dst", &backup.backup_arn)
        .await
        .expect("registers");
    await_copies_waiting(&s, 1).await;
    let pid = restore_lock_holder(&s, ACCOUNT, "hb_dst")
        .await
        .expect("the restore holds its lock while copying");
    terminate(pid).await;
    let started = std::time::Instant::now();
    assert_eq!(
        settled_status(&s, "hb_dst").await,
        None,
        "the copy stops and removes its target when its lock session is lost"
    );
    assert!(
        started.elapsed() < std::time::Duration::from_secs(40),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(registration(&s, &first.table_id).await, None);
    assert_eq!(data_table_count(&s).await, tables_before);
    gate.commit().await.expect("release the gate");

    // Reconciler path: the session is lost and the reconciler finds the
    // record's heartbeat stale before the copy's own heartbeat would notice;
    // it fails the record and removes the target, and the copy, released
    // afterwards, must not bring it back.
    let gate = gate_copies(&s).await;
    let second = start_restore(&s, "hb_dst", &backup.backup_arn)
        .await
        .expect("registers again");
    await_copies_waiting(&s, 1).await;
    let pid = restore_lock_holder(&s, ACCOUNT, "hb_dst")
        .await
        .expect("lock held");
    terminate(pid).await;
    sqlx::query(
        "UPDATE table_restores SET heartbeat_at = NOW() - INTERVAL '10 minutes' WHERE table_id = $1",
    )
    .bind(&second.table_id)
    .execute(&s.catalog)
    .await
    .expect("age the heartbeat past the stale window");
    let dead = s
        .engine
        .list_dead_restore_jobs(std::time::Duration::from_secs(300))
        .await
        .expect("list");
    assert_eq!(dead.len(), 1, "{dead:?}");
    assert!(
        s.engine
            .fail_restore_job(&dead[0].target, "owner dead")
            .await
            .expect("fail")
    );
    assert!(
        s.engine
            .remove_restore_target(&dead[0].target)
            .await
            .expect("remove")
    );
    gate.commit().await.expect("release the copy");
    // Past the next heartbeat, so the stopped copy has run its cleanup.
    tokio::time::sleep(std::time::Duration::from_secs(16)).await;
    assert_eq!(table_status(&s, "hb_dst").await, None);
    assert_eq!(registration(&s, &second.table_id).await, None);
    assert!(!data_table_exists(&s, &second.table_id).await);
    assert_eq!(data_table_count(&s).await, tables_before);

    // Liveness: nothing stale holds the name or the backup.
    let third = restore(&s, "hb_dst", &backup.backup_arn)
        .await
        .expect("restore again");
    assert_eq!(table_status(&s, "hb_dst").await.as_deref(), Some("ACTIVE"));
    assert_eq!(data_rows(&s, &third.table_id).await, 30);
    s.cleanup().await;
}

/// (e) After a multi-batch restore, every base and index row is exactly the
/// row the write path (PutItem) wrote for the source: same key columns, same
/// projection, same membership.
/// (e) The restore heartbeat is conditioned on `status = 'RUNNING'`: when the
/// reconciler has failed a record whose copy is still alive (its heartbeat
/// found stale through a slow catalog, and its removal of the target then
/// errored, so the target is CREATING under a FAILED record), the copy's next
/// heartbeat refreshes nothing and the copy stops there, performing no
/// further store or table operation (design 4(e): "the owner itself aborts at
/// its next heartbeat"). Without the condition the heartbeat keeps confirming
/// on the FAILED record and the copy runs on. The copy is held at the gate
/// through the heartbeat, the record is written FAILED by hand (the state the
/// reconciler's write leaves), and the claim session's end is awaited before
/// the gate opens: the target is still CREATING, the record FAILED with its
/// heartbeat not refreshed, the data table empty, and the pair is what the
/// reconciler's FAILED-target duty then lists and removes.
#[tokio::test]
async fn a_copy_whose_record_the_reconciler_failed_stops_at_its_next_heartbeat() {
    use extenddb_storage::BackupCatalog;
    if base_conn().is_none() {
        eprintln!(
            "SKIP a_copy_whose_record_the_reconciler_failed_stops_at_its_next_heartbeat: no \
             PostgreSQL"
        );
        return;
    }
    let s = scratch().await;
    indexed_source(&s, "hf_src").await;
    let backup = s
        .engine
        .create(ACCOUNT, "hf_src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let gate = gate_copies(&s).await;
    let desc = start_restore(&s, "hf_dst", &backup.backup_arn)
        .await
        .expect("registers");
    await_copies_waiting(&s, 1).await;
    assert!(
        restore_lock_holder(&s, ACCOUNT, "hf_dst").await.is_some(),
        "the copy holds its claim session while it waits at the gate"
    );
    let record = |s: &Scratch| {
        let catalog = s.catalog.clone();
        let table_id = desc.table_id.clone();
        async move {
            let row: (String, f64, Option<f64>) = sqlx::query_as(
                "SELECT status, EXTRACT(EPOCH FROM heartbeat_at)::FLOAT8, \
                 EXTRACT(EPOCH FROM completed_at)::FLOAT8 FROM table_restores WHERE table_id = $1",
            )
            .bind(&table_id)
            .fetch_one(&catalog)
            .await
            .expect("the job record");
            row
        }
    };
    let (status, heartbeat_before, _) = record(&s).await;
    assert_eq!(status, "RUNNING");

    // The reconciler's FAILED write, with its removal not landed.
    let written = sqlx::query(
        "UPDATE table_restores SET status = 'FAILED', completed_at = now(), \
         failure_reason = 'owner dead' WHERE table_id = $1 AND status = 'RUNNING'",
    )
    .bind(&desc.table_id)
    .execute(&s.catalog)
    .await
    .expect("fail the record")
    .rows_affected();
    assert_eq!(written, 1);

    // The copy's next heartbeat matches no row; the copy stops and its claim
    // session ends, with the gate still held. Observed through the advisory
    // lock the session holds rather than a sleep; the deadline is well past
    // one heartbeat period.
    let started = std::time::Instant::now();
    let deadline = tokio::time::Instant::now()
        + extenddb_storage::backup_catalog::JOB_HEARTBEAT_INTERVAL
        + std::time::Duration::from_secs(25);
    while restore_lock_holder(&s, ACCOUNT, "hf_dst").await.is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the copy did not stop at its next heartbeat after its record was failed"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        started.elapsed() >= std::time::Duration::from_secs(5),
        "the copy stopped at a heartbeat, not at the write: {:?}",
        started.elapsed()
    );
    let (status, heartbeat_after, completed_at) = record(&s).await;
    assert_eq!(
        status, "FAILED",
        "the record is left as the reconciler wrote it"
    );
    assert_eq!(
        heartbeat_after, heartbeat_before,
        "a heartbeat on a FAILED record refreshes nothing"
    );
    assert!(completed_at.is_some());
    assert_eq!(
        table_status(&s, "hf_dst").await.as_deref(),
        Some("CREATING"),
        "the target is left to the reconciler, not removed by the copy"
    );
    gate.commit().await.expect("release the gate");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    assert_eq!(
        data_rows(&s, &desc.table_id).await,
        0,
        "the stopped copy wrote nothing once the gate opened"
    );
    assert_eq!(
        table_status(&s, "hf_dst").await.as_deref(),
        Some("CREATING")
    );
    assert!(restore_lock_holder(&s, ACCOUNT, "hf_dst").await.is_none());

    // The pair is the FAILED-target duty's: listed once the record is stale,
    // and the removal lands.
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let dead = s
        .engine
        .list_dead_restore_jobs(std::time::Duration::from_secs(1))
        .await
        .expect("list");
    assert_eq!(dead.len(), 1, "{dead:?}");
    assert_eq!(
        dead[0].status,
        extenddb_storage::backup_catalog::JobStatus::Failed
    );
    assert!(
        s.engine
            .remove_restore_target(&dead[0].target)
            .await
            .expect("remove")
    );
    assert_eq!(table_status(&s, "hf_dst").await, None);
    assert!(!data_table_exists(&s, &desc.table_id).await);
    s.cleanup().await;
}

#[tokio::test]
async fn restored_index_rows_match_the_write_path() {
    if base_conn().is_none() {
        eprintln!("SKIP restored_index_rows_match_the_write_path: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    let src = indexed_source_with(&s, "wp_src", 1_100).await;
    let backup = s
        .engine
        .create(ACCOUNT, "wp_src", "b", JobPermit::detached())
        .await
        .expect("backup");
    let desc = restore(&s, "wp_dst", &backup.backup_arn)
        .await
        .expect("restore");
    assert_eq!(table_status(&s, "wp_dst").await.as_deref(), Some("ACTIVE"));
    let rows = |id: String| {
        let db = s.db.clone();
        async move {
            sqlx::query_scalar::<_, String>(&format!(
                "SELECT to_jsonb(t)::text FROM \"_ddb_{id}\" t ORDER BY 1"
            ))
            .fetch_all(&db)
            .await
            .expect("rows")
        }
    };
    let (src_gi, src_li) = gi_li(&s, &src).await;
    let (dst_gi, dst_li) = gi_li(&s, &desc.table_id).await;
    let base = rows(desc.table_id.clone()).await;
    assert_eq!(base.len(), 1_100);
    assert_eq!(base, rows(src.clone()).await);
    for (a, b) in [(src_gi, dst_gi), (src_li, dst_li)] {
        let want = rows(a).await;
        assert!(!want.is_empty() && want.len() < 1_100, "partial membership");
        assert_eq!(rows(b).await, want);
    }
    assert!(xmins(&s, &desc.table_id).await.len() >= 3);
    s.cleanup().await;
}

/// The definition read reports a lost connection as `Transient`, not
/// `Internal`: DescribeBackup answers without feature details only for a
/// definition that does not decode, so a failover must not reach it as
/// `Internal`. The engine's catalog sessions are terminated, as a failover or
/// an administrator restart would; the next read runs on one of them (the
/// pool does not test a connection before handing it out).
#[tokio::test]
async fn a_definition_read_that_loses_its_connection_is_transient() {
    if base_conn().is_none() {
        eprintln!("SKIP a_definition_read_that_loses_its_connection_is_transient: no PostgreSQL");
        return;
    }
    let s = scratch().await;
    let missing = format!("arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/t/backup/01");
    match s.engine.restore_source(ACCOUNT, &missing).await {
        Err(StorageError::Validation(m)) => assert!(m.contains("Backup not found"), "{m}"),
        other => panic!("a missing backup before the failover: {:?}", other.err()),
    }

    let catalog_db = &s.db_names[0];
    sqlx::query(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
         WHERE datname = $1 AND pid <> pg_backend_pid()",
    )
    .bind(catalog_db)
    .execute(&s.admin)
    .await
    .expect("terminate the catalog sessions");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let left: i64 =
            sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname = $1")
                .bind(catalog_db)
                .fetch_one(&s.admin)
                .await
                .expect("pg_stat_activity");
        if left == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{left} sessions survived"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    match s.engine.restore_source(ACCOUNT, &missing).await {
        Err(StorageError::Transient(m)) => assert!(m.contains("Database error"), "{m}"),
        other => panic!("a read on a terminated session must be Transient: {other:?}"),
    }
    // And the engine recovers: a later read reconnects.
    let mut recovered = false;
    for _ in 0..10 {
        if let Err(StorageError::Validation(_)) = s.engine.restore_source(ACCOUNT, &missing).await {
            recovered = true;
            break;
        }
    }
    assert!(recovered, "the pool reconnects after the failover");
    s.cleanup().await;
}

// --- the catalog index -----------------------------------------------------------

mod catalog_index {
    use extenddb_core::backup_format::BackupArn;
    use extenddb_storage::BackupCatalog;
    use extenddb_storage::backup_catalog::{
        BackupRow, BackupStatus, DeleteStart, Heartbeat, JobStatus, RestoreTargetKey, StatusFields,
        StoreIdentity, StoreKind, TimeBounds, UNOWNED_GENERATION, WriteOutcome,
    };
    use futures::TryStreamExt;

    use super::*;

    fn store() -> StoreIdentity {
        StoreIdentity {
            store_kind: StoreKind::Filesystem,
            store_root: "/var/lib/extenddb/backups".to_owned(),
            store_id: "store-1".to_owned(),
        }
    }

    /// A row for a backup of `table` in `account` minted now, with the
    /// manifest fixture's source table.
    fn row(account: &str, table: &str, name: &str) -> BackupRow {
        let backup_id = extenddb_storage::util::new_backup_id();
        let backup_arn = extenddb_storage::util::backup_arn(REGION, account, table, &backup_id);
        let manifest: serde_json::Value = serde_json::from_str(include_str!(
            "../../core/testdata/extenddb-manifest/v1.json"
        ))
        .expect("fixture");
        BackupRow {
            backup_id: backup_id.clone(),
            account_id: account.to_owned(),
            table_name: table.to_owned(),
            backup_arn: backup_arn.clone(),
            backup_name: name.to_owned(),
            status: BackupStatus::Creating,
            store: store(),
            size_bytes: 0,
            item_count: 0,
            created_at: extenddb_storage::util::backup_creation_date_time(&backup_id).unwrap(),
            completed_at: None,
            failure_reason: None,
            source_table_details_json: manifest["source_table"].clone(),
            owner_generation: 0,
            heartbeat_at: 0.0,
        }
    }

    fn arn_of(row: &BackupRow) -> BackupArn {
        BackupArn::parse(&row.backup_arn).unwrap()
    }

    /// The claim: a second claim of the same backup is refused while the
    /// first is held, the heartbeat confirms the claim and refreshes the
    /// row, a status write is conditioned on the generation and the `from`
    /// status, and the row is read back by account, backup id, table and ARN.
    #[tokio::test]
    async fn claim_backup_excludes_a_second_owner_and_fences_its_writes() {
        if base_conn().is_none() {
            eprintln!(
                "SKIP claim_backup_excludes_a_second_owner_and_fences_its_writes: no PostgreSQL"
            );
            return;
        }
        let s = scratch().await;
        assert!(
            uuid::Uuid::parse_str(&s.engine.catalog_id().await.expect("id")).is_ok(),
            "the catalog has an id from the migration"
        );
        assert!(!s.engine.check_maintenance_claim().await.expect("check"));
        let row = row(ACCOUNT, "Music", "nightly");
        assert_eq!(backup_lock_holder(&s, &row.backup_id).await, None);
        let claim = s.engine.claim_backup(&row).await.expect("claim");
        assert_ne!(claim.generation(), UNOWNED_GENERATION);
        // The claim is the advisory lock on the claim session, held for the
        // claim's life: what excludes a second owner before any row exists.
        let holder = backup_lock_holder(&s, &row.backup_id)
            .await
            .expect("the claim session holds the backup's advisory lock");
        let second = s.engine.claim_backup(&row).await.err();
        assert!(
            matches!(second, Some(StorageError::AlreadyExists(_))),
            "{second:?}"
        );
        assert_eq!(
            backup_lock_holder(&s, &row.backup_id).await,
            Some(holder),
            "the refused claim took nothing"
        );
        let stored = s
            .engine
            .read_backup_row(&arn_of(&row))
            .await
            .expect("read")
            .expect("the row exists");
        assert_eq!(stored.status, BackupStatus::Creating);
        assert_eq!(stored.owner_generation, claim.generation());
        assert_eq!(stored.store, store());
        assert_eq!(stored.created_at, row.created_at);
        assert_eq!(
            stored.source_table_details_json,
            row.source_table_details_json
        );
        let before = stored.heartbeat_at;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(
            claim.heartbeat().await.expect("heartbeat"),
            Heartbeat::Confirmed
        );
        let after = s
            .engine
            .read_backup_row(&arn_of(&row))
            .await
            .unwrap()
            .unwrap()
            .heartbeat_at;
        assert!(after > before, "{after} > {before}");

        // The same id under another account, table or region finds nothing.
        let other_account =
            BackupArn::new("aws", REGION, OTHER_ACCOUNT, "Music", &row.backup_id).unwrap();
        assert!(
            s.engine
                .read_backup_row(&other_account)
                .await
                .unwrap()
                .is_none()
        );
        let other_table = BackupArn::new("aws", REGION, ACCOUNT, "Other", &row.backup_id).unwrap();
        assert!(
            s.engine
                .read_backup_row(&other_table)
                .await
                .unwrap()
                .is_none()
        );
        let other_region =
            BackupArn::new("aws", "eu-west-1", ACCOUNT, "Music", &row.backup_id).unwrap();
        assert!(
            s.engine
                .read_backup_row(&other_region)
                .await
                .unwrap()
                .is_none()
        );

        // A write from the wrong state matches nothing; the right one matches
        // once, and the row then carries what it set.
        let fields = StatusFields {
            size_bytes: Some(1234),
            item_count: Some(5),
            failure_reason: None,
            completed: true,
        };
        assert_eq!(
            claim
                .set_status(JobStatus::Running, JobStatus::Available, &fields)
                .await
                .unwrap(),
            WriteOutcome::NotMatched
        );
        assert_eq!(
            claim
                .set_status(JobStatus::Creating, JobStatus::Available, &fields)
                .await
                .unwrap(),
            WriteOutcome::Matched
        );
        assert_eq!(
            claim
                .set_status(JobStatus::Creating, JobStatus::Available, &fields)
                .await
                .unwrap(),
            WriteOutcome::NotMatched
        );
        let read = claim.read().await.expect("read");
        assert_eq!(read.status, JobStatus::Available);
        assert_eq!(read.owner_generation, claim.generation());
        let stored = s
            .engine
            .read_backup_row(&arn_of(&row))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, BackupStatus::Available);
        assert_eq!(stored.size_bytes, 1234);
        assert_eq!(stored.item_count, 5);
        assert!(stored.completed_at.is_some());

        // A row another generation took over confirms nothing for this claim.
        sqlx::query(
            "UPDATE backup_index SET owner_generation = owner_generation + 1 WHERE backup_id = $1",
        )
        .bind(&row.backup_id)
        .execute(&s.catalog)
        .await
        .expect("take over");
        assert_eq!(claim.heartbeat().await.expect("heartbeat"), Heartbeat::Lost);
        assert_eq!(
            claim
                .set_status(
                    JobStatus::Available,
                    JobStatus::Failed,
                    &StatusFields::default()
                )
                .await
                .unwrap(),
            WriteOutcome::NotMatched
        );
        drop(claim);
        // The lock went with the claim's session (the server releases it when
        // the connection closes, a moment after the drop): the row can be
        // claimed by a successor only through its insert, which the existing
        // row refuses.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while backup_lock_holder(&s, &row.backup_id).await.is_some() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the advisory lock outlived the dropped claim"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let third = s.engine.claim_backup(&row).await.err();
        assert!(
            matches!(third, Some(StorageError::AlreadyExists(_))),
            "{third:?}"
        );
        s.cleanup().await;
    }

    /// `start_backup_delete` runs under the `backup_index` row's lock: a
    /// transaction holding the row `FOR UPDATE` while it registers a CREATING
    /// target makes the delete wait, and when it commits the delete sees the
    /// registration and answers `InUse`, with the row left AVAILABLE. Without
    /// the `FOR UPDATE` the delete reads the row and the in-use rule past the
    /// uncommitted registration and then waits on the row in its DELETING
    /// write instead, so a wait alone cannot tell the two apart: the
    /// `pg_stat_activity` probe below is scoped to the `SELECT ... FOR
    /// UPDATE` text, and a delete that waits in its `UPDATE` never satisfies
    /// it and fails the probe's deadline. That probe is what pins the lock;
    /// the `InUse` answer after the commit is the second check.
    #[tokio::test]
    async fn start_backup_delete_waits_for_the_row_lock_and_then_sees_the_registration() {
        if base_conn().is_none() {
            eprintln!(
                "SKIP start_backup_delete_waits_for_the_row_lock_and_then_sees_the_registration: no PostgreSQL"
            );
            return;
        }
        let s = scratch().await;
        let row = row(ACCOUNT, "Music", "nightly");
        let key = row.key();
        let claim = s.engine.claim_backup(&row).await.expect("claim");
        assert_eq!(
            claim
                .set_status(
                    JobStatus::Creating,
                    JobStatus::Available,
                    &StatusFields::default()
                )
                .await
                .unwrap(),
            WriteOutcome::Matched
        );
        indexed_source(&s, "lk_target").await;
        let target_id = table_id_of(&s, "lk_target").await.unwrap();

        // The registration's side: lock the row, then register the target,
        // in one transaction that stays open.
        let mut registration = s.catalog.begin().await.expect("begin");
        let status: String = sqlx::query_scalar(
            "SELECT status FROM backup_index WHERE account_id = $1 AND backup_id = $2 FOR UPDATE",
        )
        .bind(&key.account_id)
        .bind(&key.backup_id)
        .fetch_one(&mut *registration)
        .await
        .expect("lock the row");
        assert_eq!(status, "AVAILABLE");
        sqlx::query(
            "INSERT INTO table_restores (table_id, source_backup_arn, source_table_arn, \
             restore_date_time, status, kind, owner_generation, heartbeat_at) \
             VALUES ($1, $2, 'arn:aws:dynamodb:us-east-1:123456789012:table/Music', now(), \
             'RUNNING', 'manifest', 7, now())",
        )
        .bind(&target_id)
        .bind(&row.backup_arn)
        .execute(&mut *registration)
        .await
        .expect("register a target");
        sqlx::query(
            "UPDATE tables SET table_status = 'CREATING', status_transition_at = NULL \
             WHERE table_id = $1",
        )
        .bind(&target_id)
        .execute(&mut *registration)
        .await
        .expect("make it CREATING");

        let engine = s.engine.clone();
        let deleting = tokio::spawn(async move {
            let key = key;
            engine.start_backup_delete(&key).await
        });
        // The delete is waiting on the registration's transaction, in its
        // `SELECT ... FOR UPDATE` (scoped to this database and that statement:
        // other tests of this binary produce row-lock waits of their own, and
        // without the lock the wait would be the DELETING write's instead).
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() \
                 AND wait_event_type = 'Lock' \
                 AND query LIKE 'SELECT status FROM backup_index%FOR UPDATE'",
            )
            .fetch_one(&s.catalog)
            .await
            .expect("count waiters");
            if waiting >= 1 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "start_backup_delete did not wait for the row lock"
            );
            assert!(
                !deleting.is_finished(),
                "the delete answered under the lock"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            !deleting.is_finished(),
            "the delete answered under the lock"
        );
        registration
            .commit()
            .await
            .expect("commit the registration");
        let answer = tokio::time::timeout(std::time::Duration::from_secs(30), deleting)
            .await
            .expect("the delete finishes once the lock is released")
            .expect("task")
            .expect("start_backup_delete");
        assert_eq!(answer, DeleteStart::InUse("lk_target".to_owned()));
        assert_eq!(
            s.engine
                .read_backup_row(&arn_of(&row))
                .await
                .unwrap()
                .unwrap()
                .status,
            BackupStatus::Available,
            "the refused delete leaves the row AVAILABLE"
        );
        drop(claim);
        s.cleanup().await;
    }

    /// The FAILED-target duty's listing: a FAILED record whose table is
    /// still a CREATING restore target (the FAILED write landed and the
    /// removal did not) is not listed while the record is fresh (the job
    /// may still be on its own failure path), is listed once `completed_at`
    /// is older than the stale window, with its status, and its target is
    /// then removed with no second FAILED write; a FAILED record without a
    /// `completed_at` is aged by its heartbeat; a FAILED record whose table
    /// is ACTIVE is never listed. While the target is CREATING under the
    /// FAILED record the backup is in use (the rule reads the target's
    /// status, not the job's) and DeleteBackup is refused; the removal frees
    /// it.
    #[tokio::test]
    async fn a_failed_record_whose_target_removal_failed_is_listed_once_stale_and_removed() {
        use extenddb_storage::backup_catalog::RestoreKind;
        if base_conn().is_none() {
            eprintln!(
                "SKIP a_failed_record_whose_target_removal_failed_is_listed_once_stale_and_removed: no PostgreSQL"
            );
            return;
        }
        let stale = std::time::Duration::from_secs(300);
        let s = scratch().await;
        indexed_source(&s, "ft_src").await;
        let backup = s
            .engine
            .create(ACCOUNT, "ft_src", "b", JobPermit::detached())
            .await
            .expect("backup");
        let desc = restore(&s, "ft_dst", &backup.backup_arn)
            .await
            .expect("restore");
        let key = RestoreTargetKey {
            account_id: ACCOUNT.to_owned(),
            table_name: "ft_dst".to_owned(),
            table_id: desc.table_id.clone(),
        };
        // The state the probe left: a dead restore, failed by the reconciler
        // (or by the job's own failure path), whose target removal failed.
        sqlx::query(
            "UPDATE tables SET table_status = 'CREATING', status_transition_at = NULL \
             WHERE table_id = $1",
        )
        .bind(&desc.table_id)
        .execute(&s.catalog)
        .await
        .expect("interrupt");
        sqlx::query(
            "UPDATE table_restores SET status = 'RUNNING', completed_at = NULL, \
             heartbeat_at = NOW() - INTERVAL '10 minutes' WHERE table_id = $1",
        )
        .bind(&desc.table_id)
        .execute(&s.catalog)
        .await
        .expect("age the heartbeat");
        let dead = s.engine.list_dead_restore_jobs(stale).await.expect("list");
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].status, JobStatus::Running);
        assert!(
            s.engine
                .fail_restore_job(&key, "owner dead")
                .await
                .expect("fail")
        );
        assert_eq!(
            job_status(&s, &desc.table_id).await.as_deref(),
            Some("FAILED")
        );
        assert_eq!(
            table_status(&s, "ft_dst").await.as_deref(),
            Some("CREATING")
        );

        // Fresh: the record was just failed; whoever failed it may still be
        // removing the target.
        assert!(
            s.engine
                .list_dead_restore_jobs(stale)
                .await
                .expect("list")
                .is_empty(),
            "a FAILED record within the stale window is left to its writer"
        );
        // Stale: listed, as FAILED, for the FAILED-target duty.
        sqlx::query(
            "UPDATE table_restores SET completed_at = NOW() - INTERVAL '10 minutes' \
             WHERE table_id = $1",
        )
        .bind(&desc.table_id)
        .execute(&s.catalog)
        .await
        .expect("age the FAILED record");
        let dead = s.engine.list_dead_restore_jobs(stale).await.expect("list");
        assert_eq!(dead.len(), 1, "{dead:?}");
        assert_eq!(dead[0].target, key);
        assert_eq!(dead[0].status, JobStatus::Failed);
        assert_eq!(dead[0].kind, RestoreKind::Legacy);
        assert_eq!(dead[0].source_backup_arn, backup.backup_arn);
        // A FAILED record without `completed_at` (a failure path that did
        // not stamp one) is aged by its heartbeat: fresh, not listed; stale,
        // listed.
        sqlx::query(
            "UPDATE table_restores SET completed_at = NULL, heartbeat_at = NOW() \
             WHERE table_id = $1",
        )
        .bind(&desc.table_id)
        .execute(&s.catalog)
        .await
        .expect("clear completed_at, fresh heartbeat");
        assert!(
            s.engine
                .list_dead_restore_jobs(stale)
                .await
                .expect("list")
                .is_empty()
        );
        sqlx::query(
            "UPDATE table_restores SET heartbeat_at = NOW() - INTERVAL '10 minutes' \
             WHERE table_id = $1",
        )
        .bind(&desc.table_id)
        .execute(&s.catalog)
        .await
        .expect("age the heartbeat");
        assert_eq!(
            s.engine
                .list_dead_restore_jobs(stale)
                .await
                .expect("list")
                .len(),
            1
        );
        // A FAILED record whose table the removal already moved to DELETING
        // (the claim landed, the drop did not) is the control plane's row.
        sqlx::query("UPDATE tables SET table_status = 'DELETING' WHERE table_id = $1")
            .bind(&desc.table_id)
            .execute(&s.catalog)
            .await
            .expect("DELETING");
        assert!(
            s.engine
                .list_dead_restore_jobs(stale)
                .await
                .expect("list")
                .is_empty()
        );
        sqlx::query("UPDATE tables SET table_status = 'CREATING' WHERE table_id = $1")
            .bind(&desc.table_id)
            .execute(&s.catalog)
            .await
            .expect("CREATING again");
        // The in-use rule reads the target's status, not the job's: with the
        // record FAILED and the target still CREATING, the backup is in use
        // (naming the target) and the legacy delete is refused.
        let backup_key = extenddb_storage::backup_catalog::BackupKey::from_arn(
            &extenddb_core::backup_format::BackupArn::parse(&backup.backup_arn).expect("arn"),
        );
        assert_eq!(
            s.engine
                .backup_in_use(&backup_key)
                .await
                .expect("in use")
                .as_deref(),
            Some("ft_dst"),
            "a CREATING target under a FAILED record keeps its backup in use"
        );
        let refused = s.engine.delete(ACCOUNT, &backup.backup_arn).await;
        assert!(
            matches!(refused, Err(StorageError::BackupInUse(_))),
            "DeleteBackup while the target is CREATING: {refused:?}"
        );
        // The reconciler's FAILED-target duty: no second FAILED write, the
        // target removed.
        assert!(
            !s.engine
                .fail_restore_job(&key, "owner dead")
                .await
                .expect("fail again"),
            "a FAILED record is not RUNNING"
        );
        assert!(s.engine.remove_restore_target(&key).await.expect("remove"));
        assert_eq!(table_status(&s, "ft_dst").await, None);
        assert!(!data_table_exists(&s, &desc.table_id).await);
        // The removal frees the backup: no longer in use, and deletable.
        assert_eq!(
            s.engine.backup_in_use(&backup_key).await.expect("in use"),
            None
        );
        assert!(
            s.engine
                .list_dead_restore_jobs(stale)
                .await
                .expect("list")
                .is_empty()
        );

        // A FAILED record whose table is ACTIVE (a flip that won against a
        // late FAILED write) is nobody's duty.
        let again = restore(&s, "ft_dst", &backup.backup_arn)
            .await
            .expect("restore again");
        assert_eq!(table_status(&s, "ft_dst").await.as_deref(), Some("ACTIVE"));
        sqlx::query(
            "UPDATE table_restores SET status = 'FAILED', \
             completed_at = NOW() - INTERVAL '10 minutes' WHERE table_id = $1",
        )
        .bind(&again.table_id)
        .execute(&s.catalog)
        .await
        .expect("fail the record of an ACTIVE table");
        assert!(
            s.engine
                .list_dead_restore_jobs(stale)
                .await
                .expect("list")
                .is_empty(),
            "an ACTIVE table is never a target to remove"
        );
        s.engine
            .delete(ACCOUNT, &backup.backup_arn)
            .await
            .expect("DeleteBackup once no CREATING target reads the backup");
        s.cleanup().await;
    }

    /// The two conditions that make a target's removal exclusive with its
    /// ACTIVE flip and with every other table: `abort_restore`'s claim is
    /// conditional on CREATING, so against an ACTIVE table with the right id
    /// it does nothing (as does `remove_restore_target`); and
    /// `remove_restore_target` matches the account and name as well as the
    /// id, so the right id under another account or name removes nothing.
    #[tokio::test]
    async fn a_target_is_removed_only_while_creating_and_only_under_its_full_key() {
        if base_conn().is_none() {
            eprintln!(
                "SKIP a_target_is_removed_only_while_creating_and_only_under_its_full_key: no PostgreSQL"
            );
            return;
        }
        let s = scratch().await;
        indexed_source(&s, "ex_src").await;
        let backup = s
            .engine
            .create(ACCOUNT, "ex_src", "b", JobPermit::detached())
            .await
            .expect("backup");
        let desc = restore(&s, "ex_dst", &backup.backup_arn)
            .await
            .expect("restore");
        assert_eq!(table_status(&s, "ex_dst").await.as_deref(), Some("ACTIVE"));
        let key = RestoreTargetKey {
            account_id: ACCOUNT.to_owned(),
            table_name: "ex_dst".to_owned(),
            table_id: desc.table_id.clone(),
        };

        // ACTIVE with the right id: the ordering where the copy's flip won.
        assert!(
            !s.engine
                .abort_restore(&desc.table_id)
                .await
                .expect("abort against ACTIVE"),
            "abort_restore claims only a CREATING table"
        );
        assert_eq!(table_status(&s, "ex_dst").await.as_deref(), Some("ACTIVE"));
        assert!(data_table_exists(&s, &desc.table_id).await);
        assert!(
            !s.engine
                .remove_restore_target(&key)
                .await
                .expect("remove against ACTIVE")
        );
        assert_eq!(table_status(&s, "ex_dst").await.as_deref(), Some("ACTIVE"));

        // CREATING, the right id, the wrong account or name: nothing.
        sqlx::query(
            "UPDATE tables SET table_status = 'CREATING', status_transition_at = NULL \
             WHERE table_id = $1",
        )
        .bind(&desc.table_id)
        .execute(&s.catalog)
        .await
        .expect("make it a target");
        let other_account = RestoreTargetKey {
            account_id: OTHER_ACCOUNT.to_owned(),
            ..key.clone()
        };
        let other_name = RestoreTargetKey {
            table_name: "ex_other".to_owned(),
            ..key.clone()
        };
        for wrong in [&other_account, &other_name] {
            assert!(
                !s.engine
                    .remove_restore_target(wrong)
                    .await
                    .expect("remove under the wrong key"),
                "{wrong:?}"
            );
            assert_eq!(
                table_status(&s, "ex_dst").await.as_deref(),
                Some("CREATING")
            );
            assert!(data_table_exists(&s, &desc.table_id).await);
        }
        // The full key removes it.
        assert!(s.engine.remove_restore_target(&key).await.expect("remove"));
        assert_eq!(table_status(&s, "ex_dst").await, None);
        assert!(!data_table_exists(&s, &desc.table_id).await);
        s.cleanup().await;
    }

    /// DeleteBackup's first step and the tombstone writes: only an AVAILABLE
    /// row moves to DELETING; a CREATING row answers its status; a backup a
    /// CREATING restore target reads answers the target's name; DELETING
    /// moves to DELETED once; a DELETED row is removed once; a missing row is
    /// "Backup not found".
    #[tokio::test]
    async fn start_backup_delete_checks_the_status_and_the_in_use_rule_under_the_lock() {
        if base_conn().is_none() {
            eprintln!(
                "SKIP start_backup_delete_checks_the_status_and_the_in_use_rule_under_the_lock: no PostgreSQL"
            );
            return;
        }
        let s = scratch().await;
        let row = row(ACCOUNT, "Music", "nightly");
        let key = row.key();
        assert!(matches!(
            s.engine.start_backup_delete(&key).await,
            Err(StorageError::Validation(m)) if m.contains("Backup not found")
        ));
        let claim = s.engine.claim_backup(&row).await.expect("claim");
        assert_eq!(
            s.engine.start_backup_delete(&key).await.unwrap(),
            DeleteStart::Status(BackupStatus::Creating)
        );
        assert_eq!(
            claim
                .set_status(
                    JobStatus::Creating,
                    JobStatus::Available,
                    &StatusFields::default()
                )
                .await
                .unwrap(),
            WriteOutcome::Matched
        );
        assert_eq!(s.engine.backup_in_use(&key).await.unwrap(), None);

        // A CREATING restore target registered from the backup: in use,
        // naming the target; an ACTIVE one is not.
        indexed_source(&s, "del_target").await;
        let target_id = table_id_of(&s, "del_target").await.unwrap();
        sqlx::query(
            "INSERT INTO table_restores (table_id, source_backup_arn, source_table_arn, \
             restore_date_time, status, kind, owner_generation, heartbeat_at) \
             VALUES ($1, $2, 'arn:aws:dynamodb:us-east-1:123456789012:table/Music', now(), \
             'RUNNING', 'manifest', 7, now())",
        )
        .bind(&target_id)
        .bind(&row.backup_arn)
        .execute(&s.catalog)
        .await
        .expect("register a target");
        sqlx::query("UPDATE tables SET table_status = 'CREATING', status_transition_at = NULL WHERE table_id = $1")
            .bind(&target_id)
            .execute(&s.catalog)
            .await
            .expect("make it CREATING");
        assert_eq!(
            s.engine.backup_in_use(&key).await.unwrap().as_deref(),
            Some("del_target")
        );
        assert_eq!(
            s.engine.start_backup_delete(&key).await.unwrap(),
            DeleteStart::InUse("del_target".to_owned())
        );
        assert_eq!(
            s.engine
                .read_backup_row(&arn_of(&row))
                .await
                .unwrap()
                .unwrap()
                .status,
            BackupStatus::Available,
            "a refused delete leaves the row AVAILABLE"
        );
        sqlx::query("UPDATE tables SET table_status = 'ACTIVE' WHERE table_id = $1")
            .bind(&target_id)
            .execute(&s.catalog)
            .await
            .expect("make it ACTIVE");
        assert_eq!(s.engine.backup_in_use(&key).await.unwrap(), None);

        assert_eq!(
            s.engine.start_backup_delete(&key).await.unwrap(),
            DeleteStart::Deleting
        );
        assert_eq!(
            s.engine.start_backup_delete(&key).await.unwrap(),
            DeleteStart::Status(BackupStatus::Deleting),
            "a retry finds the row DELETING"
        );
        assert!(
            !s.engine.remove_backup_row(&key).await.unwrap(),
            "DELETING is not a tombstone"
        );
        assert!(s.engine.set_backup_deleted(&key).await.unwrap());
        assert!(!s.engine.set_backup_deleted(&key).await.unwrap());
        let stored = s
            .engine
            .read_backup_row(&arn_of(&row))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, BackupStatus::Deleted);
        assert!(stored.completed_at.is_some());
        assert!(s.engine.remove_backup_row(&key).await.unwrap());
        assert!(!s.engine.remove_backup_row(&key).await.unwrap());
        assert!(
            s.engine
                .read_backup_row(&arn_of(&row))
                .await
                .unwrap()
                .is_none()
        );
        drop(claim);
        s.cleanup().await;
    }

    /// The listings: by account with an optional table and the time bounds
    /// (lower inclusive, upper exclusive) in (created_at, ARN) order, and by
    /// store and status.
    #[tokio::test]
    async fn the_listings_are_scoped_and_ordered() {
        if base_conn().is_none() {
            eprintln!("SKIP the_listings_are_scoped_and_ordered: no PostgreSQL");
            return;
        }
        let s = scratch().await;
        let mut rows = Vec::new();
        for (account, table, name) in [
            (ACCOUNT, "Music", "a"),
            (ACCOUNT, "Music", "b"),
            (ACCOUNT, "Other", "c"),
            (OTHER_ACCOUNT, "Music", "d"),
        ] {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            let row = row(account, table, name);
            let claim = s.engine.claim_backup(&row).await.expect("claim");
            if name == "b" {
                assert_eq!(
                    claim
                        .set_status(
                            JobStatus::Creating,
                            JobStatus::Failed,
                            &StatusFields::default()
                        )
                        .await
                        .unwrap(),
                    WriteOutcome::Matched
                );
            }
            rows.push((row, claim));
        }
        let names = |listed: Vec<BackupRow>| -> Vec<String> {
            listed.into_iter().map(|r| r.backup_name).collect()
        };
        let all: Vec<BackupRow> = s
            .engine
            .list_account_backup_rows(ACCOUNT, None, &TimeBounds::default())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            names(all.clone()),
            ["a", "b", "c"],
            "every status, in creation order"
        );
        assert_eq!(all[1].status, BackupStatus::Failed);
        let music: Vec<BackupRow> = s
            .engine
            .list_account_backup_rows(ACCOUNT, Some("Music"), &TimeBounds::default())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(names(music), ["a", "b"]);
        let bounds = TimeBounds {
            lower: Some(rows[1].0.created_at),
            upper: Some(rows[2].0.created_at),
        };
        let bounded: Vec<BackupRow> = s
            .engine
            .list_account_backup_rows(ACCOUNT, None, &bounds)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(names(bounded), ["b"], "lower inclusive, upper exclusive");
        let other: Vec<BackupRow> = s
            .engine
            .list_account_backup_rows(OTHER_ACCOUNT, None, &TimeBounds::default())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(names(other), ["d"]);

        let by_store: Vec<BackupRow> = s
            .engine
            .list_backup_rows(None, &store())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(names(by_store), ["a", "b", "c", "d"]);
        let failed: Vec<BackupRow> = s
            .engine
            .list_backup_rows(Some(BackupStatus::Failed), &store())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(names(failed), ["b"]);
        let elsewhere = StoreIdentity {
            store_id: "store-2".to_owned(),
            ..store()
        };
        let none: Vec<BackupRow> = s
            .engine
            .list_backup_rows(None, &elsewhere)
            .try_collect()
            .await
            .unwrap();
        assert!(none.is_empty(), "another store's rows are not listed");

        // Two rows with one `created_at` order by the ARN in byte order
        // (`backup_arn` is `COLLATE "C"`), the order the engine merges and
        // pages by, not the database's collation: under `en_US` "a" sorts
        // before "B", in byte order "B" (0x42) before "a" (0x61). The
        // column's collation is asserted first, so the cell has teeth on a
        // cluster whose default collation is already `C` (where the two
        // orders agree and the rows alone would show nothing).
        let collation: String = sqlx::query_scalar(
            "SELECT c.collname FROM pg_attribute a JOIN pg_collation c ON c.oid = a.attcollation \
             WHERE a.attrelid = 'backup_index'::regclass AND a.attname = 'backup_arn'",
        )
        .fetch_one(&s.catalog)
        .await
        .expect("the column's collation");
        assert_eq!(collation, "C", "backup_index.backup_arn is COLLATE \"C\"");
        const CASE_ACCOUNT: &str = "333333333333";
        let mut same_instant = Vec::new();
        for table in ["a", "B"] {
            let mut r = row(CASE_ACCOUNT, table, table);
            r.created_at = 1_700_000_000.0;
            let claim = s.engine.claim_backup(&r).await.expect("claim");
            same_instant.push((r, claim));
        }
        let listed: Vec<BackupRow> = s
            .engine
            .list_account_backup_rows(CASE_ACCOUNT, None, &TimeBounds::default())
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            names(listed),
            ["B", "a"],
            "equal created_at: byte order of the ARN, whatever the database's collation"
        );
        drop(same_instant);
        drop(rows);
        s.cleanup().await;
    }

    /// A catalog at the shape before migration 5 holding a finished restore
    /// and a restore the stop interrupted (its target CREATING) migrates with
    /// the registrations backfilled: the finished one DONE, the interrupted
    /// one RUNNING with `heartbeat_at` at the epoch and the unowned
    /// generation, so the reconciler's first pass fails it and removes its
    /// target.
    #[tokio::test]
    async fn migration_5_backfills_the_registrations_and_the_reconciler_removes_the_interrupted_one()
     {
        if base_conn().is_none() {
            eprintln!(
                "SKIP migration_5_backfills_the_registrations_and_the_reconciler_removes_the_interrupted_one: no PostgreSQL"
            );
            return;
        }
        let s = scratch().await;
        indexed_source(&s, "bf_src").await;
        let backup = s
            .engine
            .create(ACCOUNT, "bf_src", "b", JobPermit::detached())
            .await
            .expect("backup");
        let finished = restore(&s, "bf_done", &backup.backup_arn)
            .await
            .expect("restore");
        let interrupted = restore(&s, "bf_cut", &backup.backup_arn)
            .await
            .expect("restore");
        let catalog_id = s.engine.catalog_id().await.expect("id");

        // Back to the shape before migration 5: the job record's columns and
        // index gone, the registrations as the old release wrote them, the
        // interrupted target CREATING, and the migration forgotten.
        sqlx::raw_sql(
            "DROP INDEX IF EXISTS idx_table_restores_status; \
             ALTER TABLE table_restores DROP COLUMN status, DROP COLUMN kind, \
                 DROP COLUMN store_kind, DROP COLUMN store_root, DROP COLUMN store_id, \
                 DROP COLUMN owner_generation, DROP COLUMN heartbeat_at, DROP COLUMN spool_host, \
                 DROP COLUMN file_index, DROP COLUMN items_applied, DROP COLUMN failure_reason, \
                 DROP COLUMN completed_at;",
        )
        .execute(&s.catalog)
        .await
        .expect("drop the job record's columns");
        sqlx::query(
            "UPDATE tables SET table_status = 'CREATING', status_transition_at = NULL WHERE table_id = $1",
        )
        .bind(&interrupted.table_id)
        .execute(&s.catalog)
        .await
        .expect("interrupt the second restore");
        let target =
            PostgresTarget::new(s.catalog.connect_options().as_ref().clone(), &s.db_names[1]);
        let runner = PostgresMigrations::open_unlocked(&target)
            .await
            .expect("open the runner");
        let migrations = Runner::new(&runner, "test");
        migrations.forget(5).await.expect("forget migration 5");
        let report = migrations.migrate().await.expect("migrate again");
        runner.close().await;
        assert_eq!(report.applied.iter().map(|a| a.id).collect::<Vec<_>>(), [5]);

        #[allow(clippy::type_complexity)]
        let records: Vec<(String, String, String, i64, f64, Option<f64>)> = sqlx::query_as(
            "SELECT t.table_name, r.status, r.kind, r.owner_generation, \
             EXTRACT(EPOCH FROM r.heartbeat_at)::FLOAT8, \
             EXTRACT(EPOCH FROM r.completed_at - t.creation_date_time)::FLOAT8 \
             FROM table_restores r JOIN tables t ON t.table_id = r.table_id ORDER BY t.table_name",
        )
        .fetch_all(&s.catalog)
        .await
        .expect("read the records");
        assert_eq!(records.len(), 2, "{records:?}");
        let (name, status, kind, generation, heartbeat, completed) = &records[0];
        assert_eq!(name, "bf_cut");
        assert_eq!(status, "RUNNING");
        assert_eq!(kind, "legacy");
        assert_eq!(*generation, i64::try_from(UNOWNED_GENERATION).unwrap());
        assert_eq!(*heartbeat, 0.0, "the epoch, not NULL");
        assert_eq!(*completed, None);
        let (name, status, kind, _, _, completed) = &records[1];
        assert_eq!(name, "bf_done");
        assert_eq!(status, "DONE");
        assert_eq!(kind, "legacy");
        assert_eq!(
            *completed,
            Some(0.0),
            "completed when the table was created"
        );
        assert_eq!(
            s.engine.catalog_id().await.expect("id"),
            catalog_id,
            "a second run keeps the catalog id"
        );

        // The first reconciler pass: the interrupted restore is dead.
        let dead = s
            .engine
            .list_dead_restore_jobs(std::time::Duration::from_secs(300))
            .await
            .expect("list");
        assert_eq!(dead.len(), 1, "{dead:?}");
        assert_eq!(
            dead[0].target,
            RestoreTargetKey {
                account_id: ACCOUNT.to_owned(),
                table_name: "bf_cut".to_owned(),
                table_id: interrupted.table_id.clone(),
            }
        );
        assert_eq!(dead[0].owner_generation, UNOWNED_GENERATION);
        assert!(
            s.engine
                .fail_restore_job(&dead[0].target, "owner dead")
                .await
                .unwrap()
        );
        assert!(
            s.engine
                .remove_restore_target(&dead[0].target)
                .await
                .unwrap()
        );
        assert_eq!(table_status(&s, "bf_cut").await, None);
        assert!(!data_table_exists(&s, &interrupted.table_id).await);
        assert_eq!(table_status(&s, "bf_done").await.as_deref(), Some("ACTIVE"));
        assert_eq!(table_status(&s, "bf_src").await.as_deref(), Some("ACTIVE"));
        let _ = finished;
        s.cleanup().await;
    }

    /// The legacy restore registers through the job record: RUNNING under
    /// the claim's generation while the copy runs, DONE with the ACTIVE flip.
    #[tokio::test]
    async fn a_legacy_restore_registers_a_running_job_record_and_finishes_it_done() {
        if base_conn().is_none() {
            eprintln!(
                "SKIP a_legacy_restore_registers_a_running_job_record_and_finishes_it_done: no PostgreSQL"
            );
            return;
        }
        let s = scratch().await;
        indexed_source(&s, "jr_src").await;
        let backup = s
            .engine
            .create(ACCOUNT, "jr_src", "b", JobPermit::detached())
            .await
            .expect("backup");
        let gate = gate_copies(&s).await;
        let desc = start_restore(&s, "jr_dst", &backup.backup_arn)
            .await
            .expect("registers");
        await_copies_waiting(&s, 1).await;
        let record: (String, String, i64, f64) = sqlx::query_as(
            "SELECT status, kind, owner_generation, EXTRACT(EPOCH FROM now() - heartbeat_at)::FLOAT8 \
             FROM table_restores WHERE table_id = $1",
        )
        .bind(&desc.table_id)
        .fetch_one(&s.catalog)
        .await
        .expect("record");
        assert_eq!(record.0, "RUNNING");
        assert_eq!(record.1, "legacy");
        assert_ne!(record.2, i64::try_from(UNOWNED_GENERATION).unwrap());
        assert!(record.3 < 60.0, "a fresh heartbeat: {} s old", record.3);
        assert!(
            s.engine
                .list_dead_restore_jobs(std::time::Duration::from_secs(300))
                .await
                .unwrap()
                .is_empty(),
            "a running restore is not dead"
        );
        gate.commit().await.expect("release");
        assert_eq!(
            settled_status(&s, "jr_dst").await.as_deref(),
            Some("ACTIVE")
        );
        let record: (String, Option<f64>) = sqlx::query_as(
            "SELECT status, EXTRACT(EPOCH FROM completed_at)::FLOAT8 FROM table_restores WHERE table_id = $1",
        )
        .bind(&desc.table_id)
        .fetch_one(&s.catalog)
        .await
        .expect("record");
        assert_eq!(record.0, "DONE");
        assert!(record.1.is_some());
        s.cleanup().await;
    }
}
