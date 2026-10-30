// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level test for the data migration that adds the GSI queue's per-table
//! claim index, `005_gsi_pending_table_claim.sql`.
//!
//! A deployment one data migration behind (001 to 004 recorded, the catalog
//! current) must report 005 pending, `migrate` must apply it and record it, and
//! the index the per-table claim walks must exist afterwards. The server's
//! startup check (`claim_index_present`, behind the warning that names
//! `extenddb migrate --yes`) is false before and true after. The catalog version
//! does not move: 005 is a data migration with its own ledger.
//!
//! Builds its own catalog and data databases (`eddb_dmig_*`) and drops them when
//! it passes; a failing run leaves them for inspection. Requires
//! `EXTENDDB_TEST_PG_CONNECTION_STRING` (a base URL with no database component);
//! without it the test reports a skip and passes.

use extenddb_storage::bootstrapper::{BootstrapConfig, Bootstrapper};
use extenddb_storage_postgres::{PostgresBootstrapper, claim_index_present};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const CATALOG_MIGRATIONS: [(&str, &str); 3] = [
    (
        "001_schema.sql",
        include_str!("../migrations/001_schema.sql"),
    ),
    (
        "002_vector_indexes.sql",
        include_str!("../migrations/002_vector_indexes.sql"),
    ),
    (
        "003_backup_definitions.sql",
        include_str!("../migrations/003_backup_definitions.sql"),
    ),
];

/// The data database the release before 005 left behind: four SQL migrations
/// and the one code migration recorded.
const DATA_MIGRATIONS_BEFORE_005: [(&str, &str); 5] = [
    (
        "001_data_schema.sql",
        include_str!("../data_migrations/001_data_schema.sql"),
    ),
    (
        "002_gsi_pending.sql",
        include_str!("../data_migrations/002_gsi_pending.sql"),
    ),
    (
        "003_idempotency_account_scope.sql",
        include_str!("../data_migrations/003_idempotency_account_scope.sql"),
    ),
    (
        "004_vector_index_state.sql",
        include_str!("../data_migrations/004_vector_index_state.sql"),
    ),
    ("003_gsi_base_key_index", ""),
];

const INDEX: &str = "idx_gsi_pending_table_claim";
const MIGRATION_005: &str = "005_gsi_pending_table_claim.sql";

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

/// `postgres://user:pass@host:port` into a bootstrap config for `catalog` and
/// `data`, with the same role as admin and application user.
fn bootstrap_config(base: &str, catalog: &str, data: &str) -> BootstrapConfig {
    let rest = base
        .strip_prefix("postgres://")
        .or_else(|| base.strip_prefix("postgresql://"))
        .expect("a postgres:// URL");
    let (creds, addr) = rest.split_once('@').expect("user:pass@host:port");
    let (user, pass) = creds.split_once(':').expect("user:pass");
    let (host, port) = addr.split_once(':').unwrap_or((addr, "5432"));
    BootstrapConfig {
        host: host.to_owned(),
        port: port.parse().expect("port"),
        admin_user: user.to_owned(),
        admin_password: Some(pass.to_owned()),
        app_user: user.to_owned(),
        app_password: pass.to_owned(),
        catalog_db: catalog.to_owned(),
        data_db: data.to_owned(),
    }
}

async fn apply_recorded(pool: &PgPool, migrations: &[(&str, &str)]) {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS schema_history (filename TEXT PRIMARY KEY, \
         applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
    )
    .execute(pool)
    .await
    .expect("ledger");
    for (file, sql) in migrations {
        if !sql.is_empty() {
            sqlx::raw_sql(sql).execute(pool).await.expect(file);
        }
        sqlx::query("INSERT INTO schema_history (filename) VALUES ($1)")
            .bind(file)
            .execute(pool)
            .await
            .expect("record");
    }
}

async fn index_exists(pool: &PgPool) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_indexes \
         WHERE tablename = 'gsi_pending' AND indexname = $1)",
    )
    .bind(INDEX)
    .fetch_one(pool)
    .await
    .expect("pg_indexes")
}

async fn recorded(pool: &PgPool, file: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM schema_history WHERE filename = $1)")
        .bind(file)
        .fetch_one(pool)
        .await
        .expect("schema_history")
}

#[tokio::test]
async fn migrate_adds_the_claim_index_to_a_data_database_one_migration_behind() {
    let test = "migrate_adds_the_claim_index_to_a_data_database_one_migration_behind";
    let Some(base) = base_conn() else {
        eprintln!("SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    };
    let stem = format!("eddb_dmig_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
    let names = [format!("{stem}_c"), format!("{stem}_d")];
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("admin");
    for n in &names {
        sqlx::query(&format!("CREATE DATABASE \"{n}\""))
            .execute(&admin)
            .await
            .expect("create scratch database");
    }
    let catalog = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/{}", names[0]))
        .await
        .expect("catalog");
    apply_recorded(&catalog, &CATALOG_MIGRATIONS).await;
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('data_database_connection_string', $1) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(format!("{base}/{}", names[1]))
    .execute(&catalog)
    .await
    .expect("data connection string");
    let version: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE key = 'catalog_version'")
            .fetch_one(&catalog)
            .await
            .expect("catalog version");
    let data = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/{}", names[1]))
        .await
        .expect("data");
    apply_recorded(&data, &DATA_MIGRATIONS_BEFORE_005).await;
    assert!(!index_exists(&data).await, "004 does not create {INDEX}");
    assert!(!claim_index_present(&data).await.expect("check"));

    let b = PostgresBootstrapper::new(bootstrap_config(&base, &names[0], &names[1]));
    assert_eq!(
        b.pending_data_migrations().await.expect("pending"),
        vec![MIGRATION_005.to_owned()],
        "005 is the one pending migration; the catalog has none"
    );

    b.run_data_migrations().await.expect("migrate");

    assert!(index_exists(&data).await, "{MIGRATION_005} creates {INDEX}");
    assert!(claim_index_present(&data).await.expect("check"));
    assert!(
        recorded(&data, MIGRATION_005).await,
        "recorded in the data ledger"
    );
    assert!(
        b.pending_data_migrations()
            .await
            .expect("pending")
            .is_empty(),
        "a second run has nothing to apply"
    );
    let after: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE key = 'catalog_version'")
            .fetch_one(&catalog)
            .await
            .expect("catalog version");
    assert_eq!(
        after, version,
        "a data migration leaves the catalog version alone"
    );

    catalog.close().await;
    data.close().await;
    for n in &names {
        sqlx::query(&format!("DROP DATABASE IF EXISTS \"{n}\" WITH (FORCE)"))
            .execute(&admin)
            .await
            .expect("drop scratch database");
    }
    admin.close().await;
}
