// SPDX-License-Identifier: Apache-2.0
//! Catalogs stamped with the current version whose shape or ledger does not
//! match this binary's migration files.
//!
//! - A catalog that ran the earlier revision of `003_backup_definitions.sql`
//!   (no `table_restores.source_table_arn`, `DEFAULT NOW()` on
//!   `restore_date_time`): `serve` adds the column at startup
//!   (`PostgresEngine::prepare_catalog_for_serve`), waiting at most a few
//!   seconds for the table lock, and keeps the default; `extenddb migrate`
//!   reports and applies both steps.
//! - A catalog stamped 0.0.4 by a different `003_*` file: `serve` refuses it
//!   naming this binary's 003; `extenddb migrate` reports that file as
//!   pending and applies it.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING` (a base URL
//! `postgres://user:pass@host:port`, no database); without it every test
//! reports a skip and passes. Each test creates and drops its own databases.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, CreateTableInput, DescribeTableInput, KeySchemaElement,
    KeyType, ScalarAttributeType,
};
use extenddb_storage::TableEngine;
use extenddb_storage::bootstrapper::{BootstrapConfig, Bootstrapper};
use extenddb_storage::server_components::BackendError;
use extenddb_storage_postgres::{PostgresBootstrapper, PostgresConfig, PostgresEngine};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const ACCOUNT: &str = "123456789012";
const EARLIER_003: &str = include_str!("fixtures/003_backup_definitions_earlier.sql");

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

/// `postgres://user:pass@host:port` into a bootstrap config for `catalog`
/// and `data`, with the same role as admin and application user.
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

struct Scratch {
    base: String,
    admin: PgPool,
    catalog: PgPool,
    names: [String; 2],
    _guard: DbGuard,
}

/// Drops the scratch databases if the test panics, from the moment they
/// are created.
struct DbGuard {
    base: String,
    names: [String; 2],
}

impl Drop for DbGuard {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        // The test's runtime is unwinding: use a separate thread and runtime.
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

impl Scratch {
    /// A catalog with 001 and 002 applied and recorded, then `third`
    /// applied and recorded under `third_name`; and a data database with
    /// every data migration applied and recorded.
    async fn new(third_name: &str, third: &str) -> Self {
        let base = base_conn().expect("caller checks base_conn() first");
        let stem = format!("eddb_conv_{}", uuid::Uuid::new_v4().simple())[..24].to_owned();
        let names = [format!("{stem}_c"), format!("{stem}_d")];
        let guard = DbGuard {
            base: base.clone(),
            names: names.clone(),
        };
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
            .max_connections(2)
            .connect(&format!("{base}/{}", names[0]))
            .await
            .expect("catalog");
        for (file, sql) in [
            (
                "001_schema.sql",
                include_str!("../migrations/001_schema.sql"),
            ),
            (
                "002_vector_indexes.sql",
                include_str!("../migrations/002_vector_indexes.sql"),
            ),
            (third_name, third),
        ] {
            sqlx::raw_sql(sql).execute(&catalog).await.expect(file);
            sqlx::query("INSERT INTO schema_history (filename) VALUES ($1)")
                .bind(file)
                .execute(&catalog)
                .await
                .expect("record");
        }
        let s = Self {
            base,
            admin,
            catalog,
            names,
            _guard: guard,
        };
        // The data database the way `init` leaves it, applied directly: the
        // bootstrapper's data step would also converge the catalog.
        let data = PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!("{}/{}", s.base, s.names[1]))
            .await
            .expect("data");
        sqlx::query(
            "CREATE TABLE schema_history (filename TEXT PRIMARY KEY, \
             applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())",
        )
        .execute(&data)
        .await
        .expect("data ledger");
        for (file, sql) in [
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
            (
                "005_gsi_pending_table_claim.sql",
                include_str!("../data_migrations/005_gsi_pending_table_claim.sql"),
            ),
            ("003_gsi_base_key_index", ""),
        ] {
            if !sql.is_empty() {
                sqlx::raw_sql(sql).execute(&data).await.expect(file);
            }
            sqlx::query("INSERT INTO schema_history (filename) VALUES ($1)")
                .bind(file)
                .execute(&data)
                .await
                .expect("record");
        }
        data.close().await;
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ('data_database_connection_string', $1) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(format!("{}/{}", s.base, s.names[1]))
        .execute(&s.catalog)
        .await
        .expect("point the catalog at the data database");
        sqlx::query("UPDATE settings SET value = '0' WHERE key = 'control_plane_delay_seconds'")
            .execute(&s.catalog)
            .await
            .expect("no control-plane delay");
        sqlx::query("INSERT INTO accounts (account_id, account_name) VALUES ($1, $2)")
            .bind(ACCOUNT)
            .bind(format!("acct-{stem}"))
            .execute(&s.catalog)
            .await
            .expect("account");
        s
    }

    fn bootstrapper(&self) -> PostgresBootstrapper {
        PostgresBootstrapper::new(bootstrap_config(&self.base, &self.names[0], &self.names[1]))
    }

    async fn engine(&self) -> PostgresEngine {
        PostgresEngine::new(
            &PostgresConfig {
                connection_string: format!("{}/{}", self.base, self.names[0]),
                pool_size: 10,
                max_item_size_bytes: 400_000,
            },
            "us-east-1",
        )
        .await
        .expect("engine")
    }

    async fn catalog_version(&self) -> String {
        sqlx::query_scalar("SELECT value FROM settings WHERE key = 'catalog_version'")
            .fetch_one(&self.catalog)
            .await
            .expect("catalog_version")
    }

    async fn column_default(&self, column: &str) -> Option<Option<String>> {
        sqlx::query_scalar(
            "SELECT column_default::text FROM information_schema.columns \
             WHERE table_schema = 'public' AND table_name = 'table_restores' \
             AND column_name = $1",
        )
        .bind(column)
        .fetch_optional(&self.catalog)
        .await
        .expect("information_schema")
    }

    async fn cleanup(self) {
        self.catalog.close().await;
        for n in &self.names {
            let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{n}\" WITH (FORCE)"))
                .execute(&self.admin)
                .await;
        }
        self.admin.close().await;
    }
}

fn describe(name: &str) -> DescribeTableInput {
    DescribeTableInput {
        table_name: name.to_owned(),
    }
}

fn table(name: &str) -> CreateTableInput {
    CreateTableInput {
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
    }
}

/// The earlier 0.0.4 shape: DescribeTable fails until converged; serve's
/// startup step adds the column (an earlier row kept and reported without a
/// SourceTableArn) and keeps the `restore_date_time` default, so an
/// earlier-build server's INSERT, which omits that column, still works; a
/// second serve converge does nothing. `migrate` reports both steps before,
/// the default's step after, and `migrate --yes` drops the default.
#[tokio::test]
async fn an_earlier_0_0_4_catalog_converges_at_serve_and_describe_table_works() {
    if base_conn().is_none() {
        eprintln!("SKIP an_earlier_0_0_4_catalog_converges_at_serve: no PostgreSQL");
        return;
    }
    let s = Scratch::new("003_backup_definitions.sql", EARLIER_003).await;
    assert_eq!(s.catalog_version().await, "0.0.4");
    assert_eq!(s.column_default("source_table_arn").await, None);
    assert!(
        matches!(s.column_default("restore_date_time").await, Some(Some(_))),
        "the earlier shape has a default on restore_date_time"
    );

    let engine = s.engine().await;
    engine
        .check_catalog_version()
        .await
        .expect("the version gate passes on this catalog");
    engine
        .create_table(ACCOUNT, table("plain"))
        .await
        .expect("create a table");
    let before = engine.describe_table(ACCOUNT, describe("plain")).await;
    assert!(
        format!("{:?}", before.as_ref().err()).contains("source_table_arn"),
        "DescribeTable on the unconverged shape: {before:?}"
    );
    // A restore registration the earlier build wrote.
    let restored = engine
        .create_table(ACCOUNT, table("restored"))
        .await
        .expect("create a second table");
    sqlx::query("INSERT INTO table_restores (table_id, source_backup_arn) VALUES ($1, 'arn:b')")
        .bind(&restored.table_id)
        .execute(&s.catalog)
        .await
        .expect("an earlier-shape registration row");

    let pending = s
        .bootstrapper()
        .pending_data_migrations()
        .await
        .expect("pending");
    assert_eq!(
        pending,
        vec![
            "catalog: add table_restores.source_table_arn".to_owned(),
            "catalog: drop the default of table_restores.restore_date_time".to_owned(),
        ]
    );

    let applied = engine
        .prepare_catalog_for_serve()
        .await
        .expect("serve's startup converge");
    assert_eq!(
        applied,
        vec!["add table_restores.source_table_arn".to_owned()],
        "serve adds the column and nothing else"
    );
    assert_eq!(s.column_default("source_table_arn").await, Some(None));
    assert!(
        matches!(s.column_default("restore_date_time").await, Some(Some(_))),
        "serve keeps the default an earlier-build server still relies on"
    );
    assert!(
        engine
            .prepare_catalog_for_serve()
            .await
            .expect("second serve converge")
            .is_empty()
    );
    // The earlier build's registration INSERT, still accepted.
    let third = engine
        .create_table(ACCOUNT, table("restored_by_earlier"))
        .await
        .expect("create a third table");
    sqlx::query("INSERT INTO table_restores (table_id, source_backup_arn) VALUES ($1, 'arn:c')")
        .bind(&third.table_id)
        .execute(&s.catalog)
        .await
        .expect("an earlier-build registration after serve converged");

    let plain = engine
        .describe_table(ACCOUNT, describe("plain"))
        .await
        .expect("DescribeTable after converging");
    assert!(plain.restore_summary.is_none());
    let earlier = engine
        .describe_table(ACCOUNT, describe("restored"))
        .await
        .expect("DescribeTable of the earlier restored table");
    let summary = earlier.restore_summary.expect("its registration is kept");
    assert_eq!(summary.source_backup_arn.as_deref(), Some("arn:b"));
    assert_eq!(summary.source_table_arn, None);
    assert_eq!(
        s.bootstrapper()
            .pending_data_migrations()
            .await
            .expect("pending"),
        vec!["catalog: drop the default of table_restores.restore_date_time".to_owned()],
        "migrate still reports the step serve does not run"
    );
    s.bootstrapper()
        .run_data_migrations()
        .await
        .expect("migrate --yes");
    assert_eq!(s.column_default("restore_date_time").await, Some(None));
    assert!(
        s.bootstrapper()
            .pending_data_migrations()
            .await
            .expect("pending")
            .is_empty()
    );
    drop(engine);
    s.cleanup().await;
}

/// Serve's startup ALTER waits at most a few seconds for its lock: with a
/// transaction open on `table_restores` (another server mid-request), startup
/// fails with the migrate advice and changes nothing, and once the
/// transaction ends the table is not left queued behind it. When the lock is
/// free, the same call converges.
#[tokio::test]
async fn serve_converge_gives_up_on_a_held_lock_and_refuses_to_start() {
    if base_conn().is_none() {
        eprintln!("SKIP serve_converge_gives_up_on_a_held_lock: no PostgreSQL");
        return;
    }
    let s = Scratch::new("003_backup_definitions.sql", EARLIER_003).await;
    let engine = s.engine().await;
    let mut holder = s.catalog.begin().await.expect("holder transaction");
    sqlx::query("SELECT count(*) FROM table_restores")
        .execute(&mut *holder)
        .await
        .expect("touch table_restores");

    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        engine.prepare_catalog_for_serve(),
    )
    .await
    .expect("startup must not wait for the lock beyond its timeout");
    let waited = started.elapsed();
    match outcome {
        Err(BackendError::InitializationFailed(m)) => {
            assert!(m.contains("could not lock the table within 5 s"), "{m}");
            assert!(m.contains("extenddb migrate --yes"), "{m}");
        }
        other => panic!("startup must refuse while the lock is held: {other:?}"),
    }
    assert!(
        waited >= std::time::Duration::from_secs(4),
        "it waited for the lock first: {waited:?}"
    );
    assert_eq!(
        s.column_default("source_table_arn").await,
        None,
        "nothing changed"
    );
    // The holder's session still works on the table: the ALTER is gone
    // from the lock queue.
    sqlx::query("SELECT count(*) FROM table_restores")
        .execute(&mut *holder)
        .await
        .expect("the holder keeps working");
    holder.commit().await.expect("release");

    let applied = engine
        .prepare_catalog_for_serve()
        .await
        .expect("converge once the lock is free");
    assert_eq!(
        applied,
        vec!["add table_restores.source_table_arn".to_owned()]
    );
    drop(engine);
    s.cleanup().await;
}

/// The same shape converged by `extenddb migrate --yes` (its
/// `run_data_migrations` step) rather than by serve.
#[tokio::test]
async fn migrate_converges_an_earlier_0_0_4_catalog() {
    if base_conn().is_none() {
        eprintln!("SKIP migrate_converges_an_earlier_0_0_4_catalog: no PostgreSQL");
        return;
    }
    let s = Scratch::new("003_backup_definitions.sql", EARLIER_003).await;
    s.bootstrapper()
        .run_data_migrations()
        .await
        .expect("migrate --yes");
    assert_eq!(s.column_default("source_table_arn").await, Some(None));
    assert_eq!(s.column_default("restore_date_time").await, Some(None));
    let engine = s.engine().await;
    engine
        .create_table(ACCOUNT, table("plain"))
        .await
        .expect("create a table");
    engine
        .describe_table(ACCOUNT, describe("plain"))
        .await
        .expect("DescribeTable after migrate");
    drop(engine);
    s.cleanup().await;
}

/// A catalog stamped 0.0.4 by another branch's `003_*` (recorded under its
/// own filename). The version matches, but this binary's 003 is unrecorded:
/// `serve` refuses to start, naming the file and `extenddb migrate --yes`,
/// and changes nothing; `migrate` reports it and applies it; afterwards
/// nothing is pending, serve starts, and DescribeTable works.
#[tokio::test]
async fn an_unrecorded_catalog_migration_is_pending_even_at_the_current_version() {
    if base_conn().is_none() {
        eprintln!("SKIP an_unrecorded_catalog_migration_is_pending: no PostgreSQL");
        return;
    }
    let s = Scratch::new(
        "003_other_branch.sql",
        "UPDATE settings SET value = '0.0.4' WHERE key = 'catalog_version';",
    )
    .await;
    assert_eq!(s.catalog_version().await, "0.0.4");
    let b = s.bootstrapper();
    assert_eq!(
        b.read_catalog_version().await.expect("version").as_deref(),
        Some(b.expected_catalog_version().as_str()),
        "the version string alone shows nothing pending"
    );
    let engine = s.engine().await;
    match engine.prepare_catalog_for_serve().await {
        Err(BackendError::InitializationFailed(m)) => {
            assert!(m.contains("003_backup_definitions.sql"), "{m}");
            assert!(m.contains("extenddb migrate --yes"), "{m}");
            assert!(
                !m.contains("001_schema.sql"),
                "only the unrecorded file: {m}"
            );
        }
        other => panic!("serve must refuse an unrecorded catalog file: {other:?}"),
    }
    let restores_exist: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables \
         WHERE table_schema = 'public' AND table_name = 'table_restores')",
    )
    .fetch_one(&s.catalog)
    .await
    .expect("information_schema");
    assert!(!restores_exist, "the refusal changed nothing");
    assert_eq!(
        b.pending_data_migrations().await.expect("pending"),
        vec!["catalog 003_backup_definitions.sql".to_owned()]
    );
    b.run_data_migrations().await.expect("migrate --yes");
    assert!(
        b.pending_data_migrations()
            .await
            .expect("pending")
            .is_empty()
    );
    assert!(
        engine
            .prepare_catalog_for_serve()
            .await
            .expect("serve starts after migrate")
            .is_empty()
    );
    engine
        .create_table(ACCOUNT, table("plain"))
        .await
        .expect("create a table");
    engine
        .describe_table(ACCOUNT, describe("plain"))
        .await
        .expect("DescribeTable after migrate");
    drop(engine);
    s.cleanup().await;
}

/// `serve` once refused an unreadable stored version with
/// "Internal error: <parse error>"; it now gives the shared guard's refusal,
/// the text `extenddb migrate` prints for the same catalog, unprefixed.
#[tokio::test]
async fn serve_refuses_an_unreadable_catalog_version_with_the_shared_refusal() {
    if base_conn().is_none() {
        eprintln!("SKIP serve_refuses_an_unreadable_catalog_version: no PostgreSQL");
        return;
    }
    let s = Scratch::new(
        "003_tampered_version.sql",
        "UPDATE settings SET value = 'garbage' WHERE key = 'catalog_version';",
    )
    .await;
    assert_eq!(s.catalog_version().await, "garbage");
    let expected = extenddb_storage::util::check_stored_catalog_version(
        Some("garbage"),
        extenddb_storage_postgres::CATALOG_VERSION,
    )
    .expect_err("unreadable");
    let engine = s.engine().await;
    match engine.prepare_catalog_for_serve().await {
        Err(BackendError::InitializationFailed(m)) => assert_eq!(m, expected),
        other => panic!("serve must refuse an unreadable version: {other:?}"),
    }
    drop(engine);
    s.cleanup().await;
}
