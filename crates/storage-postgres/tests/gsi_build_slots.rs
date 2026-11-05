// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level tests for the bound on GSI builds one engine runs at once.
//!
//! `UpdateTable` returns before its GSI build starts (issue #654), so a client
//! that adds an index to one table after another would otherwise start every
//! build at once. Each build takes one of the engine's build slots, the recovery
//! sweep's builds included. These tests give the engine one slot and check that
//! a second build waits for it without holding a connection, that the waiting
//! index answers as a `CREATING` index does, that both indexes get built, and
//! that the waiting build leaves the index alone when another server's recovery
//! sweep has built it in the meantime. The last test runs on an engine with its
//! own number of slots, `GSI_BUILD_SLOTS`, and starts one build more than that.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind
//! on purpose, named `eddb_gsis_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it every test here
//! reports a skip and passes, the same convention the wire suites use.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use extenddb_core::expression::ExpressionMaps;
use extenddb_core::types::{
    AttributeValue, BillingMode, CreateTableInput, Item, TableDescription, TableKeyInfo,
    UpdateTableInput,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{DataEngine, TableEngine};
use extenddb_storage_postgres::{
    GSI_BUILD_SLOTS, PostgresConfig, PostgresEngine, hold_gsi_backfill_for_tests,
    reconcile_creating_gsis,
};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const FIRST: &str = "t_gsi_slots_first";
const SECOND: &str = "t_gsi_slots_second";
/// More than two backfill pages of 256 keys, so a build can be stopped between
/// pages.
const ITEMS: u32 = 600;

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

/// A scratch database and an engine on it with one GSI build slot.
async fn scratch() -> Scratch {
    scratch_with_slots(Some(1)).await
}

/// A scratch database and an engine on it with `slots` GSI build slots, or with
/// the engine's own `GSI_BUILD_SLOTS` for `None`.
async fn scratch_with_slots(slots: Option<usize>) -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_gsis_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
    let config = PostgresConfig {
        connection_string: url,
        pool_size: 10,
        max_item_size_bytes: 400_000,
    };
    let engine = PostgresEngine::new(&config, REGION)
        .await
        .expect("open a PostgresEngine on the scratch database");
    let engine = match slots {
        Some(n) => engine.with_gsi_build_slots_for_tests(n),
        None => engine,
    }
    .start_gsi_workers();
    Scratch {
        engine,
        db,
        admin,
        db_name,
    }
}

/// A second engine on the scratch database, with the engine's own build slots,
/// standing in for another server process. It runs no background worker; a
/// test calls its recovery sweep directly.
async fn peer_engine(s: &Scratch) -> PostgresEngine {
    let base = base_conn().expect("caller checks base_conn() first");
    let config = PostgresConfig {
        connection_string: format!("{base}/{}", s.db_name),
        pool_size: 10,
        max_item_size_bytes: 400_000,
    };
    PostgresEngine::new(&config, REGION)
        .await
        .expect("open a second PostgresEngine on the scratch database")
}

/// A table of `ITEMS` items, every one carrying the index's key attributes.
async fn table_with_items(s: &Scratch, name: &str) -> String {
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
    }))
    .expect("build the CreateTable input");
    input.billing_mode = Some(BillingMode::PayPerRequest);
    s.engine
        .create_table(ACCOUNT, input)
        .await
        .expect("create the table");
    let key_info: TableKeyInfo = s
        .engine
        .table_key_info(ACCOUNT, name)
        .await
        .expect("read the key info");
    for n in 0..ITEMS {
        let it: Item = BTreeMap::from([
            ("pk".to_owned(), AttributeValue::S(format!("p{}", n % 5))),
            ("sk".to_owned(), AttributeValue::N(n.to_string())),
            ("g".to_owned(), AttributeValue::S(format!("g{}", n % 7))),
            ("r".to_owned(), AttributeValue::N(n.to_string())),
        ]);
        s.engine
            .put_item(&key_info, it, false, None, &ExpressionMaps::default(), None)
            .await
            .expect("put");
    }
    sqlx::query_scalar("SELECT table_id FROM tables WHERE table_name = $1")
        .bind(name)
        .fetch_one(&s.db)
        .await
        .expect("look up the table id")
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

/// `IndexStatus` of `gidx` in a description, or `None` when it is not listed.
fn gidx_status(desc: &TableDescription) -> Option<String> {
    desc.global_secondary_indexes
        .as_ref()
        .and_then(|gsis| gsis.iter().find(|g| g.index_name == "gidx"))
        .map(|g| g.index_status.clone())
}

/// `(index_id, index_status)` of `gidx` on `table` in the catalog.
async fn gidx_row(db: &PgPool, table: &str) -> (String, String) {
    sqlx::query_as(
        "SELECT i.index_id, i.index_status FROM indexes i JOIN tables t \
         ON t.table_id = i.table_id WHERE t.table_name = $1 AND i.index_name = 'gidx'",
    )
    .bind(table)
    .fetch_one(db)
    .await
    .expect("look up gidx")
}

/// Poll the catalog status of `gidx` on `table` until it is ACTIVE, for at most
/// 30 s, and return the last status seen.
async fn wait_active(db: &PgPool, table: &str) -> String {
    let mut status = String::new();
    for _ in 0..600 {
        status = gidx_row(db, table).await.1;
        if status == "ACTIVE" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    status
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

/// Wait up to 10 s for the scratch database to hold `n` granted advisory locks,
/// and return the last count. Build ownership is the only advisory lock these
/// tests take, one per index being built; a lock is released when its session
/// ends, which the server sees a moment after the owner drops it.
async fn wait_ownership_locks(s: &Scratch, n: i64) -> i64 {
    let mut held = -1;
    for _ in 0..200 {
        held = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks l JOIN pg_database d ON d.oid = l.database \
             WHERE l.locktype = 'advisory' AND l.granted AND d.datname = $1",
        )
        .bind(&s.db_name)
        .fetch_one(&s.db)
        .await
        .expect("count the advisory locks");
        if held == n {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    held
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

/// The index holds exactly the base items, each once, with the projected item
/// equal to the base item (every item carries the key attributes, and the GSI
/// projects ALL).
async fn assert_index_matches_base(db: &PgPool, table_id: &str, index_id: &str) {
    wait_queue_empty(db).await;
    let expected: BTreeSet<(String, String, String, String)> = sqlx::query_as(&format!(
        "SELECT pk, sk_n::text, item_data->'g'->>'S', item_data::text FROM \"_ddb_{table_id}\""
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

/// Start the first table's build and stop it after its first page, so it holds
/// the engine's one build slot; then add the index to the second table, which
/// must answer at once. Returns the two tables' ids and index ids.
async fn first_build_holds_the_slot(s: &Scratch) -> (String, String, String, String) {
    let first = table_with_items(s, FIRST).await;
    let second = table_with_items(s, SECOND).await;
    hold_gsi_backfill_for_tests(&first, Some(1));
    hold_gsi_backfill_for_tests(&second, Some(1));
    s.engine
        .update_table(ACCOUNT, add_gidx(FIRST))
        .await
        .expect("add the GSI to the first table");
    let first_index = gidx_row(&s.db, FIRST).await.0;
    let rows = wait_index_rows_at_least(&s.db, &first_index, 256).await;
    assert!(
        rows >= 256,
        "the first build's first page did not land: {rows} rows"
    );

    let response = tokio::time::timeout(
        Duration::from_secs(10),
        s.engine.update_table(ACCOUNT, add_gidx(SECOND)),
    )
    .await
    .expect("UpdateTable waited for a build slot")
    .expect("add the GSI to the second table");
    assert_eq!(gidx_status(&response).as_deref(), Some("CREATING"));
    let (second_index, status) = gidx_row(&s.db, SECOND).await;
    assert_eq!(status, "CREATING");
    (first, first_index, second, second_index)
}

/// With one build slot, the second table's build waits for the first to finish.
/// While it waits, its index is `CREATING` and refuses reads as a backfilling
/// index, it has written nothing, and it holds no ownership session; once the
/// first index is ACTIVE, its own task takes ownership and builds it.
///
/// With the build task taking no slot, the second build writes its first page
/// while the first build holds the slot, and the row count check fails.
#[tokio::test]
async fn a_build_waits_for_a_free_slot_holding_no_session() {
    let test = "a_build_waits_for_a_free_slot_holding_no_session";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let (first, first_index, second, second_index) = first_build_holds_the_slot(&s).await;

    let read = s.engine.index_info_by_table_id(&second, "gidx").await;
    assert!(
        matches!(
            &read,
            Err(StorageError::Validation(msg))
                if msg == "Cannot read from backfilling global secondary index: gidx"
        ),
        "a read of the waiting index answered {read:?}"
    );
    // A build of the second index that had started would have written its first
    // page in this time: the first build's took its 1 s wait and well under a
    // second more.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        index_row_count(&s.db, &second_index).await,
        0,
        "the second build ran while the first held the only slot"
    );
    assert_eq!(
        wait_ownership_locks(&s, 1).await,
        1,
        "the waiting build holds an ownership session"
    );

    hold_gsi_backfill_for_tests(&first, None);
    assert_eq!(wait_active(&s.db, FIRST).await, "ACTIVE");
    let rows = wait_index_rows_at_least(&s.db, &second_index, 256).await;
    assert!(
        rows >= 256,
        "the second build did not start once the slot was free: {rows} rows"
    );
    assert_eq!(
        wait_ownership_locks(&s, 1).await,
        1,
        "the second build runs without its ownership lock"
    );
    hold_gsi_backfill_for_tests(&second, None);
    assert_eq!(wait_active(&s.db, SECOND).await, "ACTIVE");
    assert_index_matches_base(&s.db, &first, &first_index).await;
    assert_index_matches_base(&s.db, &second, &second_index).await;
    s.cleanup().await;
}

/// A recovery sweep that runs while the only slot is taken takes the waiting
/// index, which has no owner yet, and waits for the slot too. When the slot
/// frees, the index's own task finds the sweep owning it and leaves it, and the
/// sweep builds and publishes it.
///
/// With the sweep taking no slot, it builds the index at once and the row count
/// check fails. With the waiting task building without ownership, it publishes
/// the index first and the sweep publishes nothing.
#[tokio::test]
async fn the_recovery_sweep_waits_for_a_free_slot() {
    let test = "the_recovery_sweep_waits_for_a_free_slot";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let (first, first_index, second, second_index) = first_build_holds_the_slot(&s).await;

    let engine = s.engine.clone();
    let sweep = tokio::spawn(async move { reconcile_creating_gsis(&engine).await });
    assert_eq!(
        wait_ownership_locks(&s, 2).await,
        2,
        "the sweep did not take the waiting index"
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        index_row_count(&s.db, &second_index).await,
        0,
        "the sweep built the index while the first build held the only slot"
    );

    hold_gsi_backfill_for_tests(&first, None);
    assert_eq!(wait_active(&s.db, FIRST).await, "ACTIVE");
    let rows = wait_index_rows_at_least(&s.db, &second_index, 256).await;
    assert!(
        rows >= 256,
        "the sweep's build did not start once the slot was free: {rows} rows"
    );
    hold_gsi_backfill_for_tests(&second, None);
    let swept = tokio::time::timeout(Duration::from_secs(60), sweep)
        .await
        .expect("the recovery sweep did not finish")
        .expect("the sweep task")
        .expect("the recovery sweep");
    assert_eq!(swept, 1, "the recovery sweep did not publish the index");
    assert_eq!(gidx_row(&s.db, SECOND).await.1, "ACTIVE");
    assert_index_matches_base(&s.db, &first, &first_index).await;
    assert_index_matches_base(&s.db, &second, &second_index).await;
    s.cleanup().await;
}

/// A build that waited for its slot leaves its index alone when the slot comes
/// if another server's recovery sweep has built and published the index in the
/// meantime. While the build waits its index has no owner, so the other server's
/// sweep (a second engine on the same database) takes it; once that sweep has
/// let go of the ownership lock, the waiting task can take the lock, and it must
/// find the index ACTIVE rather than build it again. A hold at the index's build
/// entry makes a build that starts visible: it keeps its ownership lock while it
/// is held there.
///
/// With the waiting task building without re-reading the index, it holds the
/// published index's ownership lock at that hold, and the lock count check fails.
#[tokio::test]
async fn a_waiting_build_leaves_an_index_a_peer_published() {
    let test = "a_waiting_build_leaves_an_index_a_peer_published";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch().await;
    let peer = peer_engine(&s).await;
    let (first, first_index, second, second_index) = first_build_holds_the_slot(&s).await;
    assert_eq!(
        wait_ownership_locks(&s, 1).await,
        1,
        "the waiting build holds an ownership session"
    );

    hold_gsi_backfill_for_tests(&second, None);
    let swept = reconcile_creating_gsis(&peer)
        .await
        .expect("the peer's recovery sweep");
    assert_eq!(
        swept, 1,
        "the peer's sweep did not publish the waiting index"
    );
    assert_eq!(gidx_row(&s.db, SECOND).await.1, "ACTIVE");
    assert_eq!(
        wait_ownership_locks(&s, 1).await,
        1,
        "the peer's sweep kept the ownership lock of the index it published"
    );

    // A build of the second index that starts from here waits at its entry,
    // holding the ownership lock. Releasing the first build frees the slot the
    // second index's own task is waiting for.
    hold_gsi_backfill_for_tests(&second, Some(0));
    hold_gsi_backfill_for_tests(&first, None);
    assert_eq!(wait_active(&s.db, FIRST).await, "ACTIVE");
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        wait_ownership_locks(&s, 0).await,
        0,
        "the waiting build took the published index's ownership lock to build it again"
    );
    hold_gsi_backfill_for_tests(&second, None);
    assert_index_matches_base(&s.db, &first, &first_index).await;
    assert_index_matches_base(&s.db, &second, &second_index).await;
    drop(peer);
    s.cleanup().await;
}

/// On an engine with its own number of build slots, the builds of
/// `GSI_BUILD_SLOTS` tables run, each holding its ownership lock, and the build
/// of one table more waits: its index has written nothing and has no owner.
///
/// With the engine's slots unbounded, the last build runs too and writes its
/// first page, and the row count check fails.
#[tokio::test]
async fn an_engine_runs_at_most_gsi_build_slots_builds() {
    let test = "an_engine_runs_at_most_gsi_build_slots_builds";
    if base_conn().is_none() {
        return skip(test);
    }
    let s = scratch_with_slots(None).await;
    let mut tables = Vec::new();
    for n in 0..=GSI_BUILD_SLOTS {
        let name = format!("t_gsi_slots_{n}");
        let table_id = table_with_items(&s, &name).await;
        hold_gsi_backfill_for_tests(&table_id, Some(1));
        tables.push((name, table_id));
    }
    for (name, _) in &tables {
        s.engine
            .update_table(ACCOUNT, add_gidx(name))
            .await
            .expect("add the GSI");
    }
    for (name, _) in &tables[..GSI_BUILD_SLOTS] {
        let index_id = gidx_row(&s.db, name).await.0;
        let rows = wait_index_rows_at_least(&s.db, &index_id, 256).await;
        assert!(
            rows >= 256,
            "the build on {name} did not start: {rows} rows"
        );
    }
    let last_index = gidx_row(&s.db, &tables[GSI_BUILD_SLOTS].0).await.0;
    // A build of the last index that had started would have written its first
    // page in this time, as the running builds did.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        index_row_count(&s.db, &last_index).await,
        0,
        "a build ran beyond the engine's {GSI_BUILD_SLOTS} build slots"
    );
    assert_eq!(
        wait_ownership_locks(&s, GSI_BUILD_SLOTS as i64).await,
        GSI_BUILD_SLOTS as i64,
        "the ownership locks are not one per running build"
    );

    for (_, table_id) in &tables {
        hold_gsi_backfill_for_tests(table_id, None);
    }
    for (name, table_id) in &tables {
        assert_eq!(wait_active(&s.db, name).await, "ACTIVE");
        let index_id = gidx_row(&s.db, name).await.0;
        assert_index_matches_base(&s.db, table_id, &index_id).await;
    }
    s.cleanup().await;
}
