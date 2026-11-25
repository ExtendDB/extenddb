// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level test for `drop_ttl_index` on a table a reader holds.
//!
//! The index drop needs `ACCESS EXCLUSIVE`, and while it waits, every later
//! lock request on the table queues behind it. It therefore waits only a
//! short time per attempt and retries, so a long reader stalls the table's
//! other requests for at most that time, and the drop completes once the
//! reader is gone.
//!
//! Each test builds its own throwaway database, applies the shipped migrations
//! to it, and drops it when it passes. A failing test leaves its database behind
//! on purpose, named `eddb_putr_*`, so the state that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component, pointing at a server whose role may create and drop databases.
//! Without it every test here reports a skip and passes.

use std::time::Duration;

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, KeySchemaElement, KeyType,
    ScalarAttributeType, TableKeyInfo,
};
use extenddb_storage::{MetadataEngine, TableEngine};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const TABLE: &str = "t_put_create_race";

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

/// Every `.sql` file under `migrations/` and `data_migrations/`, each
/// directory in filename order, so a new migration is picked up here without
/// editing this file.
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

async fn scratch() -> Scratch {
    let base = base_conn().expect("caller checks base_conn() first");
    let db_name = format!("eddb_putr_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
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
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: url,
            pool_size: 10,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open a PostgresEngine on the scratch database");
    Scratch {
        engine,
        db,
        admin,
        db_name,
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

/// Create the test table, keyed on `pk` and, when `range` is set, also on `sk`.
async fn table(s: &Scratch, range: bool) -> TableKeyInfo {
    let mut keys = vec![s_key("pk", KeyType::Hash)];
    if range {
        keys.push(s_key("sk", KeyType::Range));
    }
    let (key_schema, attribute_definitions) = keys.into_iter().unzip();
    s.engine
        .create_table(
            ACCOUNT,
            CreateTableInput {
                table_name: TABLE.to_owned(),
                key_schema,
                attribute_definitions,
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

async fn data_table(db: &PgPool) -> String {
    let id: String =
        sqlx::query_scalar("SELECT table_id FROM tables WHERE account_id = $1 AND table_name = $2")
            .bind(ACCOUNT)
            .bind(TABLE)
            .fetch_one(db)
            .await
            .expect("look up the table id");
    format!("\"_ddb_{id}\"")
}

async fn index_exists(db: &PgPool) -> bool {
    sqlx::query_scalar("SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'idx_ttl_%'")
        .fetch_one(db)
        .await
        .expect("look up the TTL index")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_ttl_disable_behind_a_reader_does_not_stall_the_table_and_completes_after() {
    if base_conn().is_none() {
        return skip("a_ttl_disable_behind_a_reader_does_not_stall_the_table_and_completes_after");
    }
    let s = scratch().await;
    let _ = table(&s, false).await;
    s.engine
        .update_ttl(ACCOUNT, TABLE, "exp", true)
        .await
        .expect("enable TTL");
    s.engine
        .create_ttl_index(ACCOUNT, TABLE, "exp")
        .await
        .expect("build the TTL index");
    assert!(index_exists(&s.db).await);

    // A reader: ACCESS SHARE on the data table, held until committed.
    let data = data_table(&s.db).await;
    let mut reader = s.db.begin().await.expect("begin the reader");
    sqlx::query(&format!("SELECT count(*) FROM {data}"))
        .fetch_one(&mut *reader)
        .await
        .expect("read under the reader");

    let engine = std::sync::Arc::new(s.engine);
    let drop_task = {
        let engine = engine.clone();
        tokio::spawn(async move { engine.drop_ttl_index(ACCOUNT, TABLE, "exp").await })
    };
    // Let the drop make several attempts against the reader.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        !drop_task.is_finished(),
        "the drop cannot finish under the reader"
    );

    // Without a lock timeout the drop waits for the reader and this read
    // queues behind it; with one, it waits for at most one attempt.
    let probe = tokio::time::timeout(
        Duration::from_secs(2),
        sqlx::query(&format!("SELECT count(*) FROM {data}")).fetch_one(&s.db),
    )
    .await;
    assert!(probe.is_ok(), "a read queued behind the index drop");
    probe.unwrap().expect("the read");

    reader.commit().await.expect("release the reader");
    drop_task
        .await
        .expect("join the drop")
        .expect("the drop completes once the reader is gone");
    assert!(!index_exists(&s.db).await, "the TTL index was dropped");

    let engine = std::sync::Arc::into_inner(engine).expect("sole owner of the engine");
    Scratch {
        engine,
        db: s.db,
        admin: s.admin,
        db_name: s.db_name,
    }
    .cleanup()
    .await;
}
