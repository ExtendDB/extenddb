// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level tests for a `DeleteTable` whose storage drop is interrupted.
//!
//! The data tables of a deleted table are dropped after the catalog change
//! commits. An outside transaction holding `ACCESS SHARE` on the base data
//! table makes the `DROP TABLE` wait, which opens the window these tests use:
//! the request future is dropped while the drop waits (a client that gave up),
//! or the process that ran the request goes away (a server kill). In both
//! cases the catalog row must still name the table as `DELETING`, so that the
//! drop is finished by the detached task or by the next startup pass, and no
//! `_ddb_` table is left behind without a catalog row.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind
//! on purpose, named `eddb_delt_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it every test here
//! reports a skip and passes, the same convention the wire suites use.

use std::time::Duration;

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, DeleteTableInput, DescribeTableInput,
    GsiInput, KeySchemaElement, KeyType, Projection, ProjectionType, ScalarAttributeType,
    TableStatus,
};
use extenddb_storage::TableEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, PgConnection, PgPool};

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const TABLE: &str = "t_delete_interrupt";

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

/// Every `.sql` file under `migrations/` and `data_migrations/`, each
/// directory in filename order.
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

async fn open_engine(url: &str) -> PostgresEngine {
    PostgresEngine::new(
        &PostgresConfig {
            connection_string: url.to_owned(),
            pool_size: 10,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open a PostgresEngine on the scratch database")
}

async fn scratch() -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_delt_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
    // One scratch database stands in for both the catalog and the data
    // database, so every shipped migration file from both directories is
    // applied to it, in filename order.
    for sql in shipped_migrations() {
        sqlx::raw_sql(&sql)
            .execute(&db)
            .await
            .expect("apply a shipped migration");
    }
    // Zero takes the path that removes the table before DeleteTable answers.
    sqlx::query("UPDATE settings SET value = '0' WHERE key = 'control_plane_delay_seconds'")
        .execute(&db)
        .await
        .expect("pin the control-plane delay to zero");
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind(ACCOUNT)
        .bind(format!("acct-{db_name}"))
        .execute(&db)
        .await
        .expect("seed the account row");
    let engine = open_engine(&url).await;
    Scratch {
        engine,
        db,
        admin,
        db_name,
        url,
    }
}

fn s_key(name: &str, key_type: KeyType) -> (KeySchemaElement, AttributeDefinition) {
    (
        KeySchemaElement {
            attribute_name: name.to_owned(),
            key_type,
        },
        AttributeDefinition {
            attribute_name: name.to_owned(),
            attribute_type: ScalarAttributeType::S,
        },
    )
}

fn create_input() -> CreateTableInput {
    let (pk, pk_def) = s_key("pk", KeyType::Hash);
    let (g, g_def) = s_key("g", KeyType::Hash);
    CreateTableInput {
        table_name: TABLE.to_owned(),
        key_schema: vec![pk],
        attribute_definitions: vec![pk_def, g_def],
        billing_mode: Some(BillingMode::PayPerRequest),
        global_secondary_indexes: Some(vec![GsiInput {
            index_name: "g-index".to_owned(),
            key_schema: vec![g],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: None,
        }]),
        ..Default::default()
    }
}

/// Create the table with one GSI; returns the base table id and every index id.
async fn table_with_gsi(s: &Scratch) -> (String, Vec<String>) {
    s.engine
        .create_table(ACCOUNT, create_input())
        .await
        .expect("create the table");
    let table_id: String =
        sqlx::query_scalar("SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2")
            .bind(ACCOUNT)
            .bind(TABLE)
            .fetch_one(&s.db)
            .await
            .expect("read the table id");
    let index_ids: Vec<String> =
        sqlx::query_scalar("SELECT index_id FROM indexes WHERE table_id = $1")
            .bind(&table_id)
            .fetch_all(&s.db)
            .await
            .expect("read the index ids");
    assert_eq!(index_ids.len(), 1, "the table has one GSI");
    (table_id, index_ids)
}

async fn catalog_status(db: &PgPool) -> Option<String> {
    sqlx::query_scalar("SELECT table_status FROM tables WHERE account_id = $1 AND table_name = $2")
        .bind(ACCOUNT)
        .bind(TABLE)
        .fetch_optional(db)
        .await
        .expect("read the table status")
}

/// The `_ddb_` data tables, among the given ids, that still exist.
async fn data_tables_present(db: &PgPool, ids: &[String]) -> Vec<String> {
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT tablename::TEXT FROM pg_tables WHERE schemaname = 'public' \
         AND tablename LIKE '\\_ddb\\_%' ORDER BY tablename",
    )
    .fetch_all(db)
    .await
    .expect("list the data tables");
    names
        .into_iter()
        .filter(|n| ids.iter().any(|id| n.ends_with(id.as_str())))
        .collect()
}

/// An outside transaction holding `ACCESS SHARE` on the base data table, which
/// a `DROP TABLE` has to wait for. Stands in for a slow reader.
async fn hold_lock(url: &str, table_id: &str) -> PgConnection {
    let mut conn = PgConnection::connect(url)
        .await
        .expect("open the locking connection");
    sqlx::query("BEGIN")
        .execute(&mut conn)
        .await
        .expect("begin the locking transaction");
    sqlx::query(&format!(
        "LOCK TABLE \"_ddb_{table_id}\" IN ACCESS SHARE MODE"
    ))
    .execute(&mut conn)
    .await
    .expect("take ACCESS SHARE on the base data table");
    conn
}

async fn release_lock(mut conn: PgConnection) {
    sqlx::query("ROLLBACK")
        .execute(&mut conn)
        .await
        .expect("release the lock");
    conn.close().await.expect("close the locking connection");
}

async fn drops_waiting(db: &PgPool) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() \
         AND query ILIKE 'DROP TABLE%' AND wait_event_type = 'Lock'",
    )
    .fetch_one(db)
    .await
    .expect("count waiting drops")
}

async fn wait_for_drop_to_wait(db: &PgPool) {
    for _ in 0..100 {
        if drops_waiting(db).await > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no DROP TABLE started waiting on the held lock within 5 s");
}

/// Poll until the catalog row and every data table are gone, or give up.
async fn wait_until_gone(db: &PgPool, ids: &[String]) -> (Option<String>, Vec<String>) {
    let mut state = (None, Vec::new());
    for _ in 0..150 {
        state = (catalog_status(db).await, data_tables_present(db, ids).await);
        if state.0.is_none() && state.1.is_empty() {
            return state;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    state
}

/// A client that gives up on the request while the drop is waiting on a lock.
///
/// The request future is dropped (what a disconnect does to the handler), the
/// lock is released, and the drop must still complete: the row is `DELETING`
/// in between, and afterwards neither the row nor any data table remains.
#[tokio::test]
async fn request_dropped_while_the_drop_waits_still_removes_the_data_tables() {
    if base_conn().is_none() {
        return skip("request_dropped_while_the_drop_waits_still_removes_the_data_tables");
    }
    let s = scratch().await;
    let (table_id, index_ids) = table_with_gsi(&s).await;
    let mut ids = vec![table_id.clone()];
    ids.extend(index_ids.iter().cloned());

    let lock = hold_lock(&s.url, &table_id).await;

    let request = s.engine.delete_table(
        ACCOUNT,
        DeleteTableInput {
            table_name: TABLE.to_owned(),
        },
    );
    // Give up once the DROP TABLE is waiting on the held lock, however long
    // the catalog work before it takes on a loaded host; the pass's 3 s lock
    // timeout starts only when the drop begins waiting.
    let gave_up = tokio::select! {
        done = request => Err(done),
        () = wait_for_drop_to_wait(&s.db) => Ok(()),
    };
    assert!(
        gave_up.is_ok(),
        "the delete must still be waiting on the lock when the drop starts waiting"
    );
    // The `select!` dropped the request future: the client has disconnected.

    assert_eq!(
        catalog_status(&s.db).await.as_deref(),
        Some("DELETING"),
        "the catalog row stays DELETING until the drop has committed"
    );
    assert_eq!(
        data_tables_present(&s.db, &ids).await.len(),
        2,
        "both data tables are still present while the drop waits"
    );
    assert_eq!(
        drops_waiting(&s.db).await,
        1,
        "the DROP TABLE is waiting on the lock"
    );

    // What a client sees in the window: DescribeTable reports DELETING and
    // CreateTable with the name is refused as in use.
    let described = s
        .engine
        .describe_table(
            ACCOUNT,
            DescribeTableInput {
                table_name: TABLE.to_owned(),
            },
        )
        .await
        .expect("describe the table while DELETING");
    assert_eq!(described.table_status, TableStatus::Deleting);
    let recreate = s.engine.create_table(ACCOUNT, create_input()).await;
    assert!(
        matches!(recreate, Err(StorageError::TableAlreadyExists(ref n)) if n == TABLE),
        "CreateTable with a DELETING name is refused: {recreate:?}"
    );

    release_lock(lock).await;

    let (status, tables) = wait_until_gone(&s.db, &ids).await;
    assert_eq!(
        status, None,
        "the catalog row is removed once the drop has committed"
    );
    assert!(
        tables.is_empty(),
        "no data table is left behind: {tables:?}"
    );

    s.cleanup().await;
}

/// The process running the request dies while the drop is waiting on a lock.
///
/// The request runs on its own runtime with its own engine; dropping that
/// runtime cancels every task and closes its connections, the way a kill does.
/// The waiting `DROP TABLE` rolls back when the lock is released. The catalog
/// row must still be `DELETING`, so the startup pass of the next process
/// (`process_control_plane_transitions` on a fresh engine) drops the tables
/// and removes the row.
#[tokio::test]
async fn process_killed_while_the_drop_waits_is_finished_by_the_startup_pass() {
    if base_conn().is_none() {
        return skip("process_killed_while_the_drop_waits_is_finished_by_the_startup_pass");
    }
    let s = scratch().await;
    let (table_id, index_ids) = table_with_gsi(&s).await;
    let mut ids = vec![table_id.clone()];
    ids.extend(index_ids.iter().cloned());

    let lock = hold_lock(&s.url, &table_id).await;

    let url = s.url.clone();
    let dying = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build the dying process's runtime");
        rt.block_on(async {
            let engine = open_engine(&url).await;
            let request = engine.delete_table(
                ACCOUNT,
                DeleteTableInput {
                    table_name: TABLE.to_owned(),
                },
            );
            // Hold the request until its DROP is waiting, then "die".
            let _ = tokio::time::timeout(Duration::from_millis(1500), request).await;
        });
        drop(rt);
    });
    wait_for_drop_to_wait(&s.db).await;
    dying.join().expect("the dying process's thread ends");

    assert_eq!(
        catalog_status(&s.db).await.as_deref(),
        Some("DELETING"),
        "the catalog row is still DELETING after the process died mid-drop"
    );

    release_lock(lock).await;
    // The abandoned backend gets the lock, finds its client gone and rolls
    // back; wait for it to leave pg_stat_activity.
    for _ in 0..100 {
        if drops_waiting(&s.db).await == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        data_tables_present(&s.db, &ids).await.len(),
        2,
        "the interrupted drop rolled back and both data tables remain"
    );
    assert_eq!(
        catalog_status(&s.db).await.as_deref(),
        Some("DELETING"),
        "nothing finished the delete while no process was running"
    );

    // The next process starts: its startup pass finishes the delete.
    let restarted = open_engine(&s.url).await;
    let transitions = restarted
        .process_control_plane_transitions()
        .await
        .expect("run the startup pass");
    assert_eq!(
        transitions,
        vec![(TABLE.to_owned(), "DELETING → deleted")],
        "the startup pass removes the table"
    );
    assert_eq!(catalog_status(&s.db).await, None);
    let tables = data_tables_present(&s.db, &ids).await;
    assert!(
        tables.is_empty(),
        "no data table is left behind: {tables:?}"
    );
    drop(restarted);

    s.cleanup().await;
}

/// The drop times out on a lock, so the request answers `DELETING` with the
/// row and the data tables still in place. The task must then wake the
/// control-plane poller, so the table is finished on the poller's next tick
/// rather than at its idle sweep.
#[tokio::test]
async fn request_that_times_out_on_the_lock_wakes_the_poller() {
    if base_conn().is_none() {
        return skip("request_that_times_out_on_the_lock_wakes_the_poller");
    }
    let s = scratch().await;
    let (table_id, index_ids) = table_with_gsi(&s).await;
    let mut ids = vec![table_id.clone()];
    ids.extend(index_ids.iter().cloned());

    // Drain the permit create_table left on the notifier, so the wait below
    // sees only the wake this request sends.
    let notify = s.engine.control_plane_notify();
    let _ = tokio::time::timeout(Duration::from_millis(100), notify.notified()).await;

    let lock = hold_lock(&s.url, &table_id).await;
    let desc = s
        .engine
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: TABLE.to_owned(),
            },
        )
        .await
        .expect("the request answers once the pass gives up on the lock");
    assert_eq!(desc.table_status, TableStatus::Deleting);
    assert_eq!(
        catalog_status(&s.db).await.as_deref(),
        Some("DELETING"),
        "the row stays DELETING when the drop timed out on the lock"
    );
    assert_eq!(
        data_tables_present(&s.db, &ids).await.len(),
        2,
        "both data tables are still present"
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .is_ok(),
        "the poller must be woken when the table was not removed inline"
    );

    release_lock(lock).await;
    let transitions = s
        .engine
        .process_control_plane_transitions()
        .await
        .expect("the poller pass finishes the delete");
    assert_eq!(
        transitions,
        vec![(TABLE.to_owned(), "DELETING → deleted")],
        "the poller pass removes the table"
    );
    assert_eq!(catalog_status(&s.db).await, None);
    let tables = data_tables_present(&s.db, &ids).await;
    assert!(
        tables.is_empty(),
        "no data table is left behind: {tables:?}"
    );

    s.cleanup().await;
}

/// The pass `DeleteTable` runs inline is scoped to its own table. Another
/// table whose `DELETING` row is already due is left for the poller, and the
/// request still reports its own table removed.
#[tokio::test]
async fn inline_pass_finishes_only_its_own_table() {
    const OTHER: &str = "t_other_deleting";
    if base_conn().is_none() {
        return skip("inline_pass_finishes_only_its_own_table");
    }
    let s = scratch().await;
    let (table_id, index_ids) = table_with_gsi(&s).await;
    let mut ids = vec![table_id.clone()];
    ids.extend(index_ids.iter().cloned());

    // A second table, marked DELETING by hand with a transition time that has
    // passed: a poller candidate that the scoped pass must not touch.
    let other = CreateTableInput {
        table_name: OTHER.to_owned(),
        ..create_input()
    };
    s.engine
        .create_table(ACCOUNT, other)
        .await
        .expect("create the other table");
    sqlx::query(
        "UPDATE tables SET table_status = 'DELETING', status_transition_at = NOW() \
         WHERE account_id = $1 AND table_name = $2",
    )
    .bind(ACCOUNT)
    .bind(OTHER)
    .execute(&s.db)
    .await
    .expect("mark the other table DELETING");
    let other_status = || async {
        sqlx::query_scalar::<_, String>(
            "SELECT table_status FROM tables WHERE account_id = $1 AND table_name = $2",
        )
        .bind(ACCOUNT)
        .bind(OTHER)
        .fetch_optional(&s.db)
        .await
        .expect("read the other table's status")
    };

    let desc = s
        .engine
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: TABLE.to_owned(),
            },
        )
        .await
        .expect("delete the table with no lock held");
    assert_eq!(desc.table_status, TableStatus::Deleting);
    assert_eq!(
        catalog_status(&s.db).await,
        None,
        "the requested table is gone when the response arrives"
    );
    assert!(
        data_tables_present(&s.db, &ids).await.is_empty(),
        "the requested table's data tables are gone"
    );
    assert_eq!(
        other_status().await.as_deref(),
        Some("DELETING"),
        "the other table's due DELETING row is left to the poller"
    );

    let transitions = s
        .engine
        .process_control_plane_transitions()
        .await
        .expect("the poller pass");
    assert_eq!(
        transitions,
        vec![(OTHER.to_owned(), "DELETING → deleted")],
        "the poller pass finishes the other table"
    );
    assert_eq!(other_status().await, None);

    s.cleanup().await;
}
