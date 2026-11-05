// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level tests for a GSI build that runs after `UpdateTable` returns.
//!
//! `UpdateTable` with a GSI `Create` answers once the index row is committed,
//! with the index `CREATING`, and a detached task backfills it (issue #654).
//! These tests hold that task with the crate's test hook and check what a
//! client sees meanwhile, what the backfill leaves behind once released, and
//! that a build cut off by a server restart still reaches `ACTIVE`.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind
//! on purpose, named `eddb_gsia_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it every test here
//! reports a skip and passes, the same convention the wire suites use.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateTableInput, DeleteTableInput,
    DescribeTableInput, Item, KeySchemaElement, KeyType, ScalarAttributeType, TableDescription,
    TableKeyInfo, TableStatus, UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine, hold_gsi_backfill_for_tests};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const TABLE: &str = "t_gsi_async_build";
/// Items the tests load before adding the index: more than two backfill pages
/// of 256 keys, so a build can be stopped between pages.
const ITEMS: u32 = 600;

struct Scratch {
    engine: PostgresEngine,
    db: PgPool,
    admin: PgPool,
    db_name: String,
    url: String,
}

impl Scratch {
    async fn cleanup(self) {
        let Scratch {
            engine,
            db,
            admin,
            db_name,
            ..
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
    let db_name = format!("eddb_gsia_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
        .max_connections(2)
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
        url,
    }
}

async fn create_table(s: &Scratch) -> TableKeyInfo {
    s.engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: TABLE.to_owned(),
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "pk".to_owned(),
                        key_type: KeyType::Hash,
                    },
                    KeySchemaElement {
                        attribute_name: "sk".to_owned(),
                        key_type: KeyType::Range,
                    },
                ],
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "pk".to_owned(),
                        attribute_type: ScalarAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "sk".to_owned(),
                        attribute_type: ScalarAttributeType::N,
                    },
                ],
                billing_mode: Some(BillingMode::PayPerRequest),
                ..Default::default()
            },
        )
        .await
        .expect("create the table");
    s.engine
        .table_key_info(ACCOUNT, TABLE)
        .await
        .expect("read the key info")
}

/// Load `ITEMS` items, every one carrying `g`, so the backfill has more than
/// two pages to write.
async fn load_items(s: &Scratch, key_info: &TableKeyInfo) {
    for n in 0..ITEMS {
        put(s, key_info, item(n, Some(&format!("g{}", n % 7)), Some(n))).await;
    }
}

/// The `UpdateTable` input that adds GSI `gidx` keyed on `g` (S) and `r` (N).
fn add_gidx() -> UpdateTableInput {
    serde_json::from_value(serde_json::json!({
        "TableName": TABLE,
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

fn item(n: u32, g: Option<&str>, r: Option<u32>) -> Item {
    let mut it = BTreeMap::from([
        ("pk".to_owned(), AttributeValue::S(format!("p{}", n % 5))),
        ("sk".to_owned(), AttributeValue::N(n.to_string())),
    ]);
    if let Some(g) = g {
        it.insert("g".to_owned(), AttributeValue::S(g.to_owned()));
    }
    if let Some(r) = r {
        it.insert("r".to_owned(), AttributeValue::N(r.to_string()));
    }
    it
}

fn key(n: u32) -> Item {
    item(n, None, None)
}

async fn put(s: &Scratch, key_info: &TableKeyInfo, it: Item) {
    s.engine
        .put_item(key_info, it, false, None, &ExpressionMaps::default(), None)
        .await
        .expect("put");
}

async fn delete(s: &Scratch, key_info: &TableKeyInfo, n: u32) {
    s.engine
        .delete_item(
            key_info,
            &key(n),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect("delete");
}

async fn describe(s: &Scratch) -> TableDescription {
    s.engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: TABLE.to_owned(),
            },
        )
        .await
        .expect("describe the table")
}

/// `IndexStatus` of `gidx` in a description, as a client would read it.
fn gidx_status(desc: &TableDescription) -> String {
    desc.global_secondary_indexes
        .as_ref()
        .and_then(|gsis| gsis.iter().find(|g| g.index_name == "gidx"))
        .map(|g| g.index_status.clone())
        .expect("the description lists gidx")
}

const IDS_SQL: &str = "SELECT t.table_id, i.index_id, i.index_status FROM tables t \
                       JOIN indexes i ON i.table_id = t.table_id \
                       WHERE t.table_name = $1 AND i.index_name = 'gidx'";

async fn ids(db: &PgPool) -> (String, String, String) {
    sqlx::query_as(IDS_SQL)
        .bind(TABLE)
        .fetch_one(db)
        .await
        .expect("look up the ids")
}

/// `ids` for an index another thread's `UpdateTable` is adding: wait for its
/// catalog row to commit.
async fn wait_ids(db: &PgPool) -> (String, String, String) {
    for _ in 0..600 {
        let row: Option<(String, String, String)> = sqlx::query_as(IDS_SQL)
            .bind(TABLE)
            .fetch_optional(db)
            .await
            .expect("look up the ids");
        if let Some(row) = row {
            return row;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the gidx row never appeared");
}

async fn data_table_id(db: &PgPool) -> String {
    sqlx::query_scalar("SELECT table_id FROM tables WHERE table_name = $1")
        .bind(TABLE)
        .fetch_one(db)
        .await
        .expect("look up the table id")
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

/// `(base_pk, base_sk, g, item_data)` for every row of the index: the key
/// columns and the projected item, so a row with the right key but a stale
/// projection shows up as a difference.
async fn index_rows(db: &PgPool, index_id: &str) -> BTreeSet<(String, String, String, String)> {
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(&format!(
        "SELECT base_pk, base_sk_n::text, pk, item_data::text FROM \"_ddb_{index_id}\""
    ))
    .fetch_all(db)
    .await
    .expect("read the index table");
    rows.into_iter().collect()
}

/// `(pk, sk, g, item_data)` for every base item carrying `g`; the GSI projects
/// ALL, so the index row's item is the base item.
async fn expected_rows(db: &PgPool, table_id: &str) -> BTreeSet<(String, String, String, String)> {
    let rows: Vec<(String, String, String, String)> = sqlx::query_as(&format!(
        "SELECT pk, sk_n::text, item_data->'g'->>'S', item_data::text FROM \"_ddb_{table_id}\" \
         WHERE item_data ? 'g'"
    ))
    .fetch_all(db)
    .await
    .expect("read the base table");
    rows.into_iter().collect()
}

/// Wait for GSI `gidx` to become ACTIVE, which the detached build does when its
/// backfill finishes. A build that hit a transient failure stays CREATING for
/// the recovery worker, which does not run in these tests, so a wait that never
/// sees ACTIVE tells the two apart.
async fn wait_active(db: &PgPool) -> String {
    let mut status = String::new();
    for _ in 0..600 {
        status = ids(db).await.2;
        if status == "ACTIVE" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    status
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

/// The index holds exactly the base items that carry `g`, each once.
async fn assert_index_matches_base(db: &PgPool, table_id: &str, index_id: &str) {
    wait_queue_empty(db).await;
    let expected = expected_rows(db, table_id).await;
    let indexed = index_rows(db, index_id).await;
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

/// Writes made while the backfill is stopped after its first page. The scan
/// runs in `(pk, sk)` order and the first page covers every `p0` and `p1` item,
/// so `p0` writes land behind the scan and `p3`/`p4` writes ahead of it.
async fn write_during_build(s: &Scratch, key_info: &TableKeyInfo) {
    // Behind the scan: a changed key, a removed item, a new item, and an item
    // that loses `g` and must leave the index.
    put(s, key_info, item(0, Some("moved"), Some(0))).await;
    delete(s, key_info, 5).await;
    put(s, key_info, item(1000, Some("new"), Some(1000))).await;
    put(s, key_info, item(10, None, Some(10))).await;
    // Ahead of the scan: the same four shapes.
    put(s, key_info, item(3, Some("moved"), Some(3))).await;
    delete(s, key_info, 4).await;
    put(s, key_info, item(1003, Some("new"), Some(1003))).await;
    put(s, key_info, item(13, None, Some(13))).await;
}

/// `UpdateTable` returns with the index `CREATING` while the backfill has not
/// started; a read of the index, and a DeleteTable, are refused as the service
/// refuses them; writes made during the build end up indexed once; and the index
/// reaches `ACTIVE` when the build finishes.
///
/// With the build awaited inside `UpdateTable` again, the first assertion
/// fails: the request waits out the hold.
#[tokio::test]
async fn update_table_returns_while_the_backfill_runs() {
    let test = "update_table_returns_while_the_backfill_runs";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    load_items(&s, &key_info).await;
    let table_id = data_table_id(&s.db).await;

    hold_gsi_backfill_for_tests(&table_id, Some(0));
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        s.engine.update_table(ACCOUNT, add_gidx()),
    )
    .await
    .expect("UpdateTable waited for the held backfill instead of returning")
    .expect("add the GSI");
    assert_eq!(response.table_status, TableStatus::Active);
    assert_eq!(gidx_status(&response), "CREATING");
    let described = describe(&s).await;
    assert_eq!(described.table_status, TableStatus::Active);
    assert_eq!(gidx_status(&described), "CREATING");
    let (_, index_id, status) = ids(&s.db).await;
    assert_eq!(status, "CREATING");
    assert_eq!(index_row_count(&s.db, &index_id).await, 0);

    // Measured 2026-10-30: a Query or Scan naming the index while it backfills
    // is a ValidationException with this text.
    let read = s.engine.index_info_by_table_id(&table_id, "gidx").await;
    assert!(
        matches!(
            &read,
            Err(StorageError::Validation(msg))
                if msg == "Cannot read from backfilling global secondary index: gidx"
        ),
        "a read of the CREATING index answered {read:?}"
    );

    // Measured 2026-10-30: DeleteTable while the index is CREATING is a
    // ResourceInUseException with this text, and the table stays.
    let refused = s
        .engine
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: TABLE.to_owned(),
            },
        )
        .await;
    assert!(
        matches!(
            &refused,
            Err(StorageError::IndexesInUse(msg))
                if msg == "Attempt to change a resource which is still in use: Cannot delete \
                           table while indexes are being created, updated, or deleted."
        ),
        "DeleteTable during the build answered {refused:?}"
    );
    assert_eq!(describe(&s).await.table_status, TableStatus::Active);

    // Let the build write one page, then stop it again and write around it.
    hold_gsi_backfill_for_tests(&table_id, Some(1));
    let rows = wait_index_rows_at_least(&s.db, &index_id, 256).await;
    assert!(
        rows >= 256,
        "the first backfill page did not land: {rows} rows"
    );
    assert_eq!(ids(&s.db).await.2, "CREATING");
    write_during_build(&s, &key_info).await;

    hold_gsi_backfill_for_tests(&table_id, None);
    assert_eq!(wait_active(&s.db).await, "ACTIVE");
    assert_eq!(gidx_status(&describe(&s).await), "ACTIVE");
    assert_index_matches_base(&s.db, &table_id, &index_id).await;
    s.engine
        .index_info_by_table_id(&table_id, "gidx")
        .await
        .expect("the ACTIVE index resolves");

    // Once the index is ACTIVE the table can be deleted.
    s.engine
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: TABLE.to_owned(),
            },
        )
        .await
        .expect("DeleteTable after the build");
    s.cleanup().await;
}

/// A server that dies partway through a backfill leaves the index `CREATING`
/// with a partial data table; the next server's recovery sweep finishes it, and
/// writes made in between are indexed once.
///
/// The first server runs on a runtime of its own, on another thread, so that
/// dropping the runtime cancels its detached build the way a crash would, and
/// closes the session that holds the build's ownership lock.
#[tokio::test]
async fn a_restart_during_the_backfill_still_reaches_active() {
    let test = "a_restart_during_the_backfill_still_reaches_active";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let key_info = create_table(&s).await;
    load_items(&s, &key_info).await;
    let table_id = data_table_id(&s.db).await;

    hold_gsi_backfill_for_tests(&table_id, Some(1));
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let url = s.url.clone();
    let first_server = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build the first server's runtime");
        rt.block_on(async {
            let engine = PostgresEngine::new(&config(&url), REGION)
                .await
                .expect("open the first server")
                .start_gsi_workers();
            let response = engine
                .update_table(ACCOUNT, add_gidx())
                .await
                .expect("add the GSI on the first server");
            assert_eq!(gidx_status(&response), "CREATING");
        });
        // The detached build keeps running on `rt` after `block_on` returns.
        stop_rx.recv().expect("wait for the crash signal");
        rt.shutdown_timeout(Duration::from_secs(5));
    });

    let (_, index_id, _) = wait_ids(&s.db).await;
    let rows = wait_index_rows_at_least(&s.db, &index_id, 256).await;
    assert!(
        rows >= 256,
        "the first backfill page did not land: {rows} rows"
    );
    stop_tx.send(()).expect("signal the crash");
    first_server.join().expect("the first server's thread");
    assert_eq!(ids(&s.db).await.2, "CREATING");

    hold_gsi_backfill_for_tests(&table_id, None);
    write_during_build(&s, &key_info).await;

    // The dead server's ownership session is closed with its connection; the
    // sweep skips the index until the server notices, so give it a few passes.
    let mut finished = 0;
    for _ in 0..100 {
        finished = extenddb_storage_postgres::reconcile_creating_gsis(&s.engine)
            .await
            .expect("run the recovery sweep");
        if finished > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(finished, 1, "the recovery sweep did not finish the build");
    assert_eq!(ids(&s.db).await.2, "ACTIVE");
    assert_eq!(gidx_status(&describe(&s).await), "ACTIVE");
    assert_index_matches_base(&s.db, &table_id, &index_id).await;
    s.cleanup().await;
}
