// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level tests for the task that builds a GSI after `UpdateTable` has
//! returned (issue #654): what it leaves behind when the backfill fails, how it
//! shares an index with the recovery sweep, and what DeleteTable answers while
//! it runs.
//!
//! The client has had its 200 with the index `CREATING` before the task starts,
//! so a failed backfill no longer reaches it as an error. These tests check the
//! catalog and the data tables instead.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind
//! on purpose, named `eddb_gsit_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it every test here
//! reports a skip and passes, the same convention the wire suites use.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeValue, BillingMode, CreateTableInput, DeleteTableInput, DescribeTableInput, Item,
    TableDescription, TableKeyInfo, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TableEngine};
use extenddb_storage_postgres::{
    PostgresConfig, PostgresEngine, hold_gsi_backfill_for_tests, reconcile_creating_gsis,
};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const TABLE: &str = "t_gsi_build_task";
/// More than two backfill pages of 256 keys, so a build can be stopped between
/// pages.
const ITEMS: u32 = 600;
/// The service's DeleteTable refusal while an index is being created (measured
/// 2026-10-30).
const IN_USE: &str = "Attempt to change a resource which is still in use: Cannot delete table \
                      while indexes are being created, updated, or deleted.";

struct Scratch {
    engine: PostgresEngine,
    db: PgPool,
    admin: PgPool,
    db_name: String,
}

impl Scratch {
    async fn cleanup(self) {
        let Scratch {
            engine,
            db,
            admin,
            db_name,
        } = self;
        drop(engine);
        db.close().await;
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
        ))
        .execute(&admin)
        .await
        .expect("drop the scratch database");
        admin.close().await;
    }
}

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

fn shipped_migrations() -> Vec<String> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut out = Vec::new();
    for dir in ["migrations", "data_migrations"] {
        let mut files: Vec<_> = std::fs::read_dir(root.join(dir))
            .expect("read the migrations directory")
            .map(|e| e.expect("directory entry").path())
            .filter(|p| p.extension().is_some_and(|e| e == "sql"))
            .collect();
        files.sort();
        for f in files {
            out.push(std::fs::read_to_string(&f).expect("read a migration file"));
        }
    }
    out
}

fn skip(test: &str) {
    eprintln!(
        "SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, so there is no PostgreSQL \
         to build a scratch catalog in."
    );
}

fn config(url: &str) -> PostgresConfig {
    PostgresConfig {
        connection_string: url.to_owned(),
        pool_size: 10,
        max_item_size_bytes: 400_000,
    }
}

async fn scratch() -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_gsit_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(&admin)
        .await
        .expect("create the scratch database");
    let url = format!("{base}/{db_name}");
    let db = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to the scratch database");
    // One scratch database stands in for both the catalog and the data database.
    for sql in shipped_migrations() {
        sqlx::raw_sql(&sql)
            .execute(&db)
            .await
            .expect("apply a shipped migration");
    }
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0') \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .execute(&db)
    .await
    .expect("pin the control-plane delay to zero");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind(ACCOUNT)
        .bind(format!("acct-{db_name}"))
        .execute(&db)
        .await
        .expect("seed the account row");
    let engine = PostgresEngine::new(&config(&url), REGION)
        .await
        .expect("open a PostgresEngine on the scratch database")
        .start_gsi_workers();
    Scratch {
        engine,
        db,
        admin,
        db_name,
    }
}

async fn create_table(s: &Scratch, name: &str, protected: bool) -> TableKeyInfo {
    let mut input: CreateTableInput = serde_json::from_value(serde_json::json!({
        "TableName": name,
        "KeySchema": [
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "N"},
        ],
        "DeletionProtectionEnabled": protected,
    }))
    .expect("build the CreateTable input");
    input.billing_mode = Some(BillingMode::PayPerRequest);
    s.engine
        .create_table(ACCOUNT, input)
        .await
        .expect("create the table");
    s.engine
        .table_key_info(ACCOUNT, name)
        .await
        .expect("read the key info")
}

/// The `UpdateTable` input that adds GSI `gidx` keyed on `g` (S) and `r` (N).
fn add_gidx(table: &str) -> UpdateTableInput {
    serde_json::from_value(serde_json::json!({
        "TableName": table,
        "AttributeDefinitions": [
            {"AttributeName": "g", "AttributeType": "S"},
            {"AttributeName": "r", "AttributeType": "N"},
        ],
        "GlobalSecondaryIndexUpdates": [{"Create": {
            "IndexName": "gidx",
            "KeySchema": [
                {"AttributeName": "g", "KeyType": "HASH"},
                {"AttributeName": "r", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }}],
    }))
    .expect("build the UpdateTable input")
}

fn item(n: u32, g: &str) -> Item {
    BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S(format!("p{}", n % 5))),
        ("sk".to_owned(), AttributeValue::N(n.to_string())),
        ("g".to_owned(), AttributeValue::S(g.to_owned())),
        ("r".to_owned(), AttributeValue::N(n.to_string())),
    ])
}

async fn load_items(s: &Scratch, key_info: &TableKeyInfo, count: u32) {
    for n in 0..count {
        s.engine
            .put_item(
                key_info,
                item(n, &format!("g{}", n % 7)),
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .expect("put");
    }
}

async fn describe(s: &Scratch, table: &str) -> TableDescription {
    s.engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: table.to_owned(),
            },
        )
        .await
        .expect("describe the table")
}

async fn delete_table(s: &Scratch, table: &str) -> Result<TableDescription, StorageError> {
    s.engine
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: table.to_owned(),
            },
        )
        .await
}

/// `IndexStatus` of `gidx` in a description, or `None` when it is not listed.
fn gidx_status(desc: &TableDescription) -> Option<String> {
    desc.global_secondary_indexes
        .as_ref()
        .and_then(|gsis| gsis.iter().find(|g| g.index_name == "gidx"))
        .map(|g| g.index_status.clone())
}

async fn table_id(db: &PgPool, table: &str) -> String {
    sqlx::query_scalar("SELECT table_id FROM tables WHERE table_name = $1")
        .bind(table)
        .fetch_one(db)
        .await
        .expect("look up the table id")
}

/// `(index_id, index_status)` of `gidx` on `table` in the catalog.
async fn gidx_row(db: &PgPool, table: &str) -> Option<(String, String)> {
    sqlx::query_as(
        "SELECT i.index_id, i.index_status FROM indexes i JOIN tables t \
         ON t.table_id = i.table_id WHERE t.table_name = $1 AND i.index_name = 'gidx'",
    )
    .bind(table)
    .fetch_optional(db)
    .await
    .expect("look up gidx")
}

/// Poll the catalog status of `gidx` on `table` until it is `want` (`None`: the
/// row is gone), for at most 30 s, and return the last status seen.
async fn wait_status(db: &PgPool, table: &str, want: Option<&str>) -> Option<String> {
    let mut last = None;
    for _ in 0..600 {
        last = gidx_row(db, table).await.map(|r| r.1);
        if last.as_deref() == want {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    last
}

async fn relation_exists(db: &PgPool, id: &str) -> bool {
    sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(format!("\"_ddb_{id}\""))
        .fetch_one(db)
        .await
        .expect("look up the relation")
}

async fn index_row_count(db: &PgPool, index_id: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT count(*) FROM \"_ddb_{index_id}\""))
        .fetch_one(db)
        .await
        .expect("count the index rows")
}

/// Wait until the backfill has written at least `n` rows, which with a hold
/// after the first page means the first page is in and the build has stopped.
async fn wait_index_rows_at_least(db: &PgPool, index_id: &str, n: i64) -> i64 {
    let mut rows = 0;
    for _ in 0..600 {
        rows = index_row_count(db, index_id).await;
        if rows >= n {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    rows
}

/// Granted advisory locks on the scratch database. Build ownership is the only
/// advisory lock these tests take, one per index being built.
async fn ownership_locks(s: &Scratch) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks l JOIN pg_database d ON d.oid = l.database \
         WHERE l.locktype = 'advisory' AND l.granted AND d.datname = $1",
    )
    .bind(&s.db_name)
    .fetch_one(&s.db)
    .await
    .expect("count the advisory locks")
}

async fn wait_queue_empty(db: &PgPool) {
    for _ in 0..500 {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM gsi_pending")
            .fetch_one(db)
            .await
            .expect("count pending rows");
        if n == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the propagation queue did not drain");
}

/// The index holds exactly the base items that carry both key attributes, each
/// once, with the projected item equal to the base item (the GSI projects ALL).
async fn assert_index_matches_base(db: &PgPool, table_id: &str, index_id: &str) {
    wait_queue_empty(db).await;
    let expected: BTreeSet<(String, String, String, String)> = sqlx::query_as(&format!(
        "SELECT pk, sk_n::text, item_data->'g'->>'S', item_data::text FROM \"_ddb_{table_id}\" \
         WHERE item_data ? 'g' AND item_data ? 'r'"
    ))
    .fetch_all(db)
    .await
    .expect("read the base table")
    .into_iter()
    .collect();
    let indexed: BTreeSet<(String, String, String, String)> = sqlx::query_as(&format!(
        "SELECT base_pk, base_sk_n::text, pk, item_data::text FROM \"_ddb_{index_id}\""
    ))
    .fetch_all(db)
    .await
    .expect("read the index table")
    .into_iter()
    .collect();
    assert_eq!(
        indexed, expected,
        "the index rows differ from the base items"
    );
    assert_eq!(
        index_row_count(db, index_id).await,
        expected.len() as i64,
        "an item is indexed more than once"
    );
}

/// DeleteTable on a deletion-protected table answers the in-use refusal while
/// a GSI is being created, and the protection refusal before and after.
///
/// Measured 2026-10-30 on a protected table: the protection ValidationException
/// with no index change running, and while the index was CREATING, before and
/// after `Backfilling` turned true, ResourceInUseException with [`IN_USE`]. With
/// the protection check ahead of the CREATING check, the second answer is the
/// protection refusal and this test fails there.
#[tokio::test]
async fn delete_table_of_a_protected_table_during_a_build_is_in_use() {
    let test = "delete_table_of_a_protected_table_during_a_build_is_in_use";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s, TABLE, true).await;
    load_items(&s, &key_info, 20).await;
    let table_id = table_id(&s.db, TABLE).await;

    let before = delete_table(&s, TABLE).await;
    assert!(
        matches!(&before, Err(StorageError::DeletionProtected(_))),
        "DeleteTable of the protected table before the build answered {before:?}"
    );

    hold_gsi_backfill_for_tests(&table_id, Some(0));
    let response = s
        .engine
        .update_table(ACCOUNT, add_gidx(TABLE))
        .await
        .expect("add the GSI");
    assert_eq!(gidx_status(&response).as_deref(), Some("CREATING"));
    let during = delete_table(&s, TABLE).await;
    assert!(
        matches!(&during, Err(StorageError::IndexesInUse(msg)) if msg == IN_USE),
        "DeleteTable of the protected table during the build answered {during:?}"
    );

    hold_gsi_backfill_for_tests(&table_id, None);
    assert_eq!(
        wait_status(&s.db, TABLE, Some("ACTIVE")).await.as_deref(),
        Some("ACTIVE")
    );
    let after = delete_table(&s, TABLE).await;
    assert!(
        matches!(&after, Err(StorageError::DeletionProtected(_))),
        "DeleteTable of the protected table after the build answered {after:?}"
    );
    s.cleanup().await;
}

/// A backfill that fails the same way on every attempt removes the index and
/// its data table after `UpdateTable` has answered with the index `CREATING`,
/// and once the cause is gone the same Create builds the index.
///
/// With every failure treated as transient, the index stays `CREATING` and the
/// wait for its removal fails.
#[tokio::test]
async fn a_build_that_fails_every_time_removes_the_index() {
    let test = "a_build_that_fails_every_time_removes_the_index";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s, TABLE, false).await;
    load_items(&s, &key_info, ITEMS).await;
    let table_id = table_id(&s.db, TABLE).await;

    // One stored item that no longer decodes, so the backfill fails on it every
    // time.
    let base = format!("\"_ddb_{table_id}\"");
    let original: String = sqlx::query_scalar(&format!(
        "SELECT item_data::text FROM {base} WHERE pk = 'p0' AND sk_n = 0"
    ))
    .fetch_one(&s.db)
    .await
    .expect("read the item to break");
    sqlx::query(&format!(
        "UPDATE {base} SET item_data = $1::jsonb WHERE pk = 'p0' AND sk_n = 0"
    ))
    .bind(r#"{"pk":{"S":"p0"},"sk":{"N":"0"},"g":{"BOGUS":1}}"#)
    .execute(&s.db)
    .await
    .expect("make one stored item undecodable");

    let response = s
        .engine
        .update_table(ACCOUNT, add_gidx(TABLE))
        .await
        .expect("UpdateTable answers before the backfill fails");
    assert_eq!(gidx_status(&response).as_deref(), Some("CREATING"));
    let (index_id, _) = gidx_row(&s.db, TABLE).await.expect("the gidx row");

    assert_eq!(
        wait_status(&s.db, TABLE, None).await,
        None,
        "the index whose backfill fails every time was not removed"
    );
    // The data table is dropped GRACE_PUBLISH (1 s) after the catalog row.
    let mut exists = true;
    for _ in 0..100 {
        exists = relation_exists(&s.db, &index_id).await;
        if !exists {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!exists, "the removed index's data table was left behind");
    assert_eq!(gidx_status(&describe(&s, TABLE).await), None);

    sqlx::query(&format!(
        "UPDATE {base} SET item_data = $1::jsonb WHERE pk = 'p0' AND sk_n = 0"
    ))
    .bind(&original)
    .execute(&s.db)
    .await
    .expect("restore the item");
    s.engine
        .update_table(ACCOUNT, add_gidx(TABLE))
        .await
        .expect("create the index again");
    assert_eq!(
        wait_status(&s.db, TABLE, Some("ACTIVE")).await.as_deref(),
        Some("ACTIVE")
    );
    let (index_id, _) = gidx_row(&s.db, TABLE).await.expect("the new gidx row");
    assert_index_matches_base(&s.db, &table_id, &index_id).await;
    s.cleanup().await;
}

/// A backfill that fails for a passing reason leaves the index `CREATING` with
/// its data table, and its task ends, which frees the build's ownership lock;
/// the next recovery sweep publishes the index.
///
/// With every failure treated as permanent, the index is removed instead and
/// the `CREATING` assertion fails.
#[tokio::test]
async fn a_build_that_fails_for_a_passing_reason_stays_creating() {
    let test = "a_build_that_fails_for_a_passing_reason_stays_creating";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s, TABLE, false).await;
    load_items(&s, &key_info, ITEMS).await;
    let table_id = table_id(&s.db, TABLE).await;

    hold_gsi_backfill_for_tests(&table_id, Some(0));
    s.engine
        .update_table(ACCOUNT, add_gidx(TABLE))
        .await
        .expect("add the GSI");
    let (index_id, _) = gidx_row(&s.db, TABLE).await.expect("the gidx row");
    // An outside transaction holds the base table's ROW EXCLUSIVE lock, so every
    // write barrier attempt times out with SQLSTATE 55P03, which is transient.
    let mut blocker = s.db.begin().await.expect("begin the blocker");
    sqlx::query(&format!(
        "LOCK TABLE \"_ddb_{table_id}\" IN ROW EXCLUSIVE MODE"
    ))
    .execute(&mut *blocker)
    .await
    .expect("take the blocking lock");
    hold_gsi_backfill_for_tests(&table_id, None);

    // The barrier's six attempts take about 20 s.
    let mut held = ownership_locks(&s).await;
    for _ in 0..80 {
        if held == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        held = ownership_locks(&s).await;
    }
    assert_eq!(
        held, 0,
        "the build kept its ownership lock after a transient failure"
    );
    assert_eq!(
        gidx_row(&s.db, TABLE).await.map(|r| r.1).as_deref(),
        Some("CREATING")
    );
    assert!(
        relation_exists(&s.db, &index_id).await,
        "the index's data table was dropped"
    );

    blocker.rollback().await.expect("release the blocker");
    let finished = reconcile_creating_gsis(&s.engine)
        .await
        .expect("run the recovery sweep");
    assert_eq!(finished, 1, "the recovery sweep did not finish the build");
    assert_index_matches_base(&s.db, &table_id, &index_id).await;
    s.cleanup().await;
}

/// The detached build holds its ownership lock from before the index row
/// commits until it ends, so a recovery sweep that runs during the build skips
/// the index rather than starting a second build of it.
///
/// With the ownership dropped before the task starts, the sweep takes the index
/// and its own build stops at the same hold, so the sweep does not return in
/// time.
#[tokio::test]
async fn the_recovery_sweep_leaves_a_running_build_alone() {
    let test = "the_recovery_sweep_leaves_a_running_build_alone";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s, TABLE, false).await;
    load_items(&s, &key_info, ITEMS).await;
    let table_id = table_id(&s.db, TABLE).await;

    hold_gsi_backfill_for_tests(&table_id, Some(1));
    s.engine
        .update_table(ACCOUNT, add_gidx(TABLE))
        .await
        .expect("add the GSI");
    let (index_id, _) = gidx_row(&s.db, TABLE).await.expect("the gidx row");
    let rows = wait_index_rows_at_least(&s.db, &index_id, 256).await;
    assert!(
        rows >= 256,
        "the first backfill page did not land: {rows} rows"
    );

    let swept =
        tokio::time::timeout(Duration::from_secs(5), reconcile_creating_gsis(&s.engine)).await;
    assert!(
        matches!(swept, Ok(Ok(0))),
        "the recovery sweep did not skip the running build: {swept:?}"
    );
    hold_gsi_backfill_for_tests(&table_id, None);
    assert_eq!(
        wait_status(&s.db, TABLE, Some("ACTIVE")).await.as_deref(),
        Some("ACTIVE")
    );
    assert_index_matches_base(&s.db, &table_id, &index_id).await;
    s.cleanup().await;
}
