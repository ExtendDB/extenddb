// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! The scratch database each plan test builds, and the skip that runs when
//! there is no server to build it on.

use super::*;
use extenddb_storage_postgres::PostgresConfig;
use sqlx::postgres::PgPoolOptions;

/// A scratch database and an engine on it. The guard drops the database.
pub(super) struct Scratch {
    pub(super) engine: PostgresEngine,
    pub(super) db: PgPool,
    _guard: DbGuard,
}

/// Drops the scratch database when the test ends, a failed assertion
/// included. Runs on its own thread and runtime, because the test's runtime
/// cannot be awaited from `Drop`; `WITH (FORCE)` ends the sessions the
/// engine's pool still holds.
struct DbGuard {
    base: String,
    name: String,
}

impl Drop for DbGuard {
    fn drop(&mut self) {
        let (base, name) = (self.base.clone(), self.name.clone());
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
                    let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
                        "DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"
                    )))
                    .execute(&admin)
                    .await;
                    admin.close().await;
                }
            });
        })
        .join();
    }
}

/// The server's base URL, or `None` to skip.
fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").unwrap_or_default();
    if conn.trim().is_empty() {
        return None;
    }
    Some(conn.trim_end_matches('/').to_owned())
}

/// Whether `EXTENDDB_TEST_PG_REQUIRED` is set to any value but `0`.
fn required() -> bool {
    std::env::var("EXTENDDB_TEST_PG_REQUIRED").is_ok_and(|v| !v.is_empty() && v != "0")
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

/// Report a skip, or fail when [`required`].
fn skip(test: &str, reason: &str) {
    assert!(
        !required(),
        "{test}: {reason}, and EXTENDDB_TEST_PG_REQUIRED says this job must run the PostgreSQL \
         tests"
    );
    eprintln!("SKIP {test}: {reason}");
}

/// A scratch database on a server with `EXPLAIN (GENERIC_PLAN)`, or `None`
/// after [`skip`].
pub(super) async fn scratch_or_skip(test: &str) -> Option<Scratch> {
    let Some(base) = base_conn() else {
        skip(test, "EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return None;
    };
    let s = scratch(&base).await;
    let version: i32 = sqlx::query_scalar("SELECT current_setting('server_version_num')::int")
        .fetch_one(&s.db)
        .await
        .expect("read the server version");
    if version < 160_000 {
        skip(
            test,
            &format!(
                "EXPLAIN (GENERIC_PLAN) needs PostgreSQL 16 or later, the server reports \
                 server_version_num {version}"
            ),
        );
        return None;
    }
    Some(s)
}

async fn scratch(base: &str) -> Scratch {
    let db_name = format!("eddb_lsip_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the postgres maintenance database");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE DATABASE \"{db_name}\""
    )))
    .execute(&admin)
    .await
    .expect("create the scratch database");
    admin.close().await;
    let guard = DbGuard {
        base: base.to_owned(),
        name: db_name.clone(),
    };
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
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
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
        _guard: guard,
    }
}
