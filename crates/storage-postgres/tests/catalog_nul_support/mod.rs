// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared by the tests of attribute names that hold U+0000 or U+0001
//! (`catalog_nul_names.rs` and `catalog_nul_index_writes.rs`): a catalog
//! database and a data database that the runner migrated as `extenddb init`
//! does, an engine on them, and the text the catalog stores for a name.
//!
//! Every test needs `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no
//! database, for a role that may create databases. Without it every test
//! reports a skip and passes; `EXTENDDB_TEST_PG_REQUIRED` turns the skip into
//! a failure. Each test drops its databases when it ends, passing or failing.

#![allow(dead_code)]

use std::time::Duration;

use extenddb_core::types::{
    AttributeDefinition, KeySchemaElement, KeyType, Projection, ProjectionType, ScalarAttributeType,
};
use extenddb_storage::migrations::Runner;
use extenddb_storage_postgres::migrations::{
    PostgresMigrations, PostgresTarget, try_create_vector_extension,
};
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

pub const ACCOUNT: &str = "123456789012";
pub const REGION: &str = "us-east-1";

/// The server's base URL, or `None` to skip.
pub fn base_conn() -> Option<String> {
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

pub fn skip(test: &str) {
    eprintln!("SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
}

/// One test at a time in each test binary: each opens an engine pool, and
/// the server is shared with other jobs.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// What a scratch deployment runs.
#[derive(Clone, Copy, Default)]
pub struct Options {
    /// Start the propagation queue's workers, for a non-zero delay.
    pub gsi_workers: bool,
    /// Install pgvector in the data database, for vector indexes.
    pub pgvector: bool,
}

/// A catalog database and a data database with an engine on them.
pub struct Scratch {
    pub engine: PostgresEngine,
    pub catalog: PgPool,
    pub data: PgPool,
    _guard: Guard,
    _serial: tokio::sync::MutexGuard<'static, ()>,
}

/// Drops the databases when the scratch goes, a failed assertion included.
struct Guard {
    base: String,
    names: Vec<String>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        let (base, names) = (self.base.clone(), self.names.clone());
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
                    for n in &names {
                        let _ =
                            sqlx::query(&format!("DROP DATABASE IF EXISTS \"{n}\" WITH (FORCE)"))
                                .execute(&admin)
                                .await;
                    }
                    admin.close().await;
                }
            });
        })
        .join();
    }
}

async fn pool(url: &str, size: u32) -> PgPool {
    PgPoolOptions::new()
        .max_connections(size)
        .connect(url)
        .await
        .unwrap_or_else(|e| panic!("connect to {url}: {e}"))
}

/// A fresh deployment, or `None` when `options.pgvector` asks for pgvector
/// and the server has no pgvector package.
pub async fn scratch(test: &str, options: Options) -> Option<Scratch> {
    let serial = SERIAL.lock().await;
    let base = base_conn().expect("the caller checks base_conn() first");
    let stem = format!(
        "eddb_nul_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    );
    let (catalog_name, data_name) = (format!("{stem}_c"), format!("{stem}_d"));
    let guard = Guard {
        base: base.clone(),
        names: vec![catalog_name.clone(), data_name.clone()],
    };
    let admin = pool(&format!("{base}/postgres"), 1).await;
    for n in [&catalog_name, &data_name] {
        sqlx::query(&format!("CREATE DATABASE \"{n}\""))
            .execute(&admin)
            .await
            .expect("create a scratch database");
    }
    admin.close().await;
    let catalog_url = format!("{base}/{catalog_name}");
    let data_url = format!("{base}/{data_name}");
    let target = PostgresTarget::new(catalog_url.parse().expect("a postgres URL"), &data_name)
        .with_data_connection_row(&data_url);
    let runner = PostgresMigrations::open_unlocked(&target)
        .await
        .expect("open the migration runner");
    let report = Runner::new(&runner, "test").migrate().await;
    runner.close().await;
    report.expect("migrate the scratch databases");
    if options.pgvector {
        let line =
            try_create_vector_extension(&data_url.parse().expect("a postgres URL"), None).await;
        if !line.starts_with("pgvector available") {
            eprintln!("SKIP {test}: {line}");
            return None;
        }
    }
    let catalog = pool(&catalog_url, 2).await;
    let data = pool(&data_url, 2).await;
    for key in ["control_plane_delay_seconds", "index_propagation_delay_ms"] {
        set(&catalog, key, "0").await;
    }
    sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
        .bind(ACCOUNT)
        .bind(format!("acct-{stem}"))
        .execute(&catalog)
        .await
        .expect("seed the account row");
    let mut engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: catalog_url,
            pool_size: 4,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open an engine on the scratch databases");
    if options.gsi_workers {
        engine = engine.start_gsi_workers();
    }
    Some(Scratch {
        engine,
        catalog,
        data,
        _guard: guard,
        _serial: serial,
    })
}

/// Write a settings row, as main's PostgreSQL tests do: the migrations seed
/// none.
pub async fn set(catalog: &PgPool, key: &str, value: &str) {
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ($1, $2) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(key)
    .bind(value)
    .execute(catalog)
    .await
    .expect("write a setting");
}

pub fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_owned(),
        key_type,
    }
}

pub fn s_attr(name: &str) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_owned(),
        attribute_type: ScalarAttributeType::S,
    }
}

pub fn include(names: &[&str]) -> Projection {
    Projection {
        projection_type: ProjectionType::Include,
        non_key_attributes: Some(names.iter().map(|n| (*n).to_owned()).collect()),
    }
}

pub fn names(ks: &[KeySchemaElement]) -> Vec<&str> {
    ks.iter().map(|k| k.attribute_name.as_str()).collect()
}

/// The names of attribute definitions, sorted.
pub fn def_names(ad: &[AttributeDefinition]) -> Vec<&str> {
    let mut v: Vec<&str> = ad.iter().map(|a| a.attribute_name.as_str()).collect();
    v.sort_unstable();
    v
}

/// How `jsonb::text` shows `name` stored through the escape: U+0000 as
/// U+0001 U+0001, U+0001 as U+0001 U+0002, and each control character
/// written `\u00NN`. The names in these tests hold no control character but
/// U+0000, U+0001 and U+0002.
pub fn stored_text(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        match c {
            '\u{0}' => out.push_str("\\u0001\\u0001"),
            '\u{1}' => out.push_str("\\u0001\\u0002"),
            '\u{2}' => out.push_str("\\u0002"),
            c => out.push(c),
        }
    }
    out
}

/// The `jsonb::text` of the documents a query returns, one per row.
pub async fn documents(pool: &PgPool, sql: &str, bind: &str) -> Vec<String> {
    sqlx::query_scalar(sql)
        .bind(bind)
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// The catalog documents of a table and its indexes, as stored.
pub async fn table_documents(catalog: &PgPool, table: &str) -> String {
    let mut out = documents(
        catalog,
        "SELECT key_schema::text || ' ' || attribute_definitions::text FROM tables \
         WHERE account_id = '123456789012' AND table_name = $1",
        table,
    )
    .await;
    out.extend(
        documents(
            catalog,
            "SELECT i.key_schema::text || ' ' || i.projection::text FROM indexes i \
             JOIN tables t ON t.table_id = i.table_id \
             WHERE t.account_id = '123456789012' AND t.table_name = $1 ORDER BY i.index_name",
            table,
        )
        .await,
    );
    out.join(" ")
}

/// Wait until the table is `ACTIVE` in the catalog.
pub async fn wait_active(catalog: &PgPool, table: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let status: Option<String> = sqlx::query_scalar(
            "SELECT table_status FROM tables WHERE account_id = $1 AND table_name = $2",
        )
        .bind(ACCOUNT)
        .bind(table)
        .fetch_optional(catalog)
        .await
        .expect("read the table status");
        if status.as_deref() == Some("ACTIVE") {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{table} still {status:?} after 90 s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
