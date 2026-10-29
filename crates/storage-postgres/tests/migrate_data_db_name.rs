// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! `extenddb migrate` on a deployment whose data database is not named
//! `<catalog-db>` minus `_catalog`.
//!
//! `init` records the data database name in the catalog's settings
//! (`data_database_name`). The target the commands resolve from the catalog's
//! connection string must use that name, so the status and the runner read
//! and write the real data database; the derivation is only the fallback for
//! a catalog with no recorded name, and an explicit `--data-db` wins over
//! both.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING` (a base URL
//! `postgres://user:pass@host:port`, no database); without it every test
//! reports a skip and passes. Each test creates its own databases and removes
//! them when its deployment is dropped, on a panic too.

use extenddb_storage::bootstrapper::{BootstrapConfig, Bootstrapper};
use extenddb_storage::migrations::{CatalogMigrations, CatalogStatus, Target};
use extenddb_storage_postgres::PostgresBootstrapper;
use extenddb_storage_postgres::migrations::{PostgresCatalogMigrations, resolve};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
}

/// Report a skip, or fail when `EXTENDDB_TEST_PG_REQUIRED` says this job must
/// run the PostgreSQL tests.
fn skip_unless_required() {
    let required =
        std::env::var("EXTENDDB_TEST_PG_REQUIRED").is_ok_and(|v| !v.is_empty() && v != "0");
    assert!(
        !required,
        "EXTENDDB_TEST_PG_CONNECTION_STRING is not set, and EXTENDDB_TEST_PG_REQUIRED says this \
         job must run the PostgreSQL tests"
    );
    eprintln!("skipped: EXTENDDB_TEST_PG_CONNECTION_STRING not set");
}

struct Parts {
    host: String,
    port: u16,
    user: String,
    pass: String,
}

fn parts(base: &str) -> Parts {
    let rest = base
        .strip_prefix("postgres://")
        .or_else(|| base.strip_prefix("postgresql://"))
        .expect("a postgres:// URL");
    let (creds, addr) = rest.split_once('@').expect("user:pass@host:port");
    let (user, pass) = creds.split_once(':').expect("user:pass");
    let (host, port) = addr.split_once(':').unwrap_or((addr, "5432"));
    Parts {
        host: host.to_owned(),
        port: port.parse().expect("port"),
        user: user.to_owned(),
        pass: pass.to_owned(),
    }
}

/// A deployment initialised the way `init --catalog-db <c> --data-db <d>`
/// does it, with a data database name unrelated to the catalog's. Dropping
/// it drops its three databases and its config file.
struct Deployment {
    base: String,
    admin: PgPool,
    catalog_db: String,
    data_db: String,
    derived_db: String,
}

impl Drop for Deployment {
    fn drop(&mut self) {
        // Runs on a panic too, so it cannot await on the test's runtime: a
        // thread of its own with its own runtime and connection.
        let base = self.base.clone();
        let names = [
            self.catalog_db.clone(),
            self.data_db.clone(),
            self.derived_db.clone(),
        ];
        let dropped = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("cleanup runtime");
            rt.block_on(async {
                let Ok(admin) = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&format!("{base}/postgres"))
                    .await
                else {
                    return;
                };
                for n in names {
                    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{n}\" WITH (FORCE)"))
                        .execute(&admin)
                        .await;
                }
                admin.close().await;
            });
        })
        .join();
        if dropped.is_err() {
            eprintln!("cleanup of the test databases failed");
        }
    }
}

impl Deployment {
    async fn new() -> Self {
        let base = base_conn().expect("caller checks base_conn() first");
        let stem = format!("eddb_mdn_{}", uuid::Uuid::new_v4().simple())[..20].to_owned();
        let catalog_db = format!("{stem}_catalog");
        let data_db = format!("{stem}_elsewhere");
        let p = parts(&base);
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!("{base}/postgres"))
            .await
            .expect("admin");
        let config = BootstrapConfig {
            host: p.host.clone(),
            port: p.port,
            admin_user: p.user.clone(),
            admin_password: Some(p.pass.clone()),
            app_user: p.user.clone(),
            app_password: p.pass.clone(),
            catalog_db: catalog_db.clone(),
            data_db: data_db.clone(),
        };
        let init = PostgresBootstrapper::connect(config)
            .await
            .expect("connect");
        init.create_catalog_db().await.expect("catalog db");
        init.create_data_db().await.expect("data db");
        let catalog: sqlx::postgres::PgConnectOptions =
            format!("{base}/{catalog_db}").parse().expect("catalog URL");
        extenddb_storage_postgres::migrations::migrate(
            &extenddb_storage_postgres::migrations::PostgresTarget::new(catalog, data_db.clone()),
            "test",
            std::time::Duration::ZERO,
        )
        .await
        .expect("catalog migrations");
        init.record_data_connection().await.expect("record");
        Self {
            base,
            admin,
            catalog_db,
            data_db,
            derived_db: stem,
        }
    }

    /// The catalog's connection string, as the config file `init` writes
    /// gives it.
    fn catalog_url(&self) -> String {
        format!("{}/{}", self.base, self.catalog_db)
    }

    /// The target `extenddb migrate` builds, with `data_db` as its
    /// `--data-db`.
    fn target(&self, data_db: Option<&str>) -> Target {
        Target {
            data_store: data_db.map(str::to_owned),
            ..Target::at(self.catalog_url())
        }
    }
}

/// Without `--data-db`, the commands' target is the data database `init`
/// recorded: the status reads that database's ledger (and finds it current)
/// instead of failing on the derived name, which is never created. An
/// explicit `--data-db` wins over the recorded name.
#[tokio::test]
async fn migrate_uses_the_data_database_name_init_recorded() {
    let Some(_) = base_conn() else {
        skip_unless_required();
        return;
    };
    let d = Deployment::new().await;
    let resolved = resolve(&d.target(None))
        .await
        .expect("resolve")
        .expect("the catalog database exists");
    assert_eq!(resolved.data_database, d.data_db);
    let status = PostgresCatalogMigrations
        .status(&d.target(None))
        .await
        .expect("status on the recorded data database");
    assert!(
        matches!(status, CatalogStatus::Current { .. }),
        "{status:?}"
    );
    let derived_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(&d.derived_db)
            .fetch_one(&d.admin)
            .await
            .expect("pg_database");
    assert!(
        !derived_exists,
        "migrate must not touch the derived name {}",
        d.derived_db
    );

    let explicit = d.target(Some(&d.derived_db));
    let err = PostgresCatalogMigrations
        .status(&explicit)
        .await
        .expect_err("the derived database does not exist");
    assert!(
        err.to_string().contains(&d.derived_db),
        "the error names the explicit database: {err}"
    );
    // Once it exists, the explicit name wins over the database the catalog's
    // connection string row names, on that row's server: the status reads
    // its empty ledger instead of refusing the two names.
    sqlx::query(&format!("CREATE DATABASE \"{}\"", d.derived_db))
        .execute(&d.admin)
        .await
        .expect("create the derived database");
    let status = PostgresCatalogMigrations.status(&explicit).await;
    let _ = sqlx::query(&format!(
        "DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)",
        d.derived_db
    ))
    .execute(&d.admin)
    .await;
    assert!(
        matches!(status, Ok(CatalogStatus::Behind { .. })),
        "{status:?}"
    );
    drop(d);
}

/// A catalog with no recorded name (an older `init`) keeps the derivation.
#[tokio::test]
async fn a_catalog_without_a_recorded_name_falls_back_to_the_derivation() {
    let Some(_) = base_conn() else {
        skip_unless_required();
        return;
    };
    let d = Deployment::new().await;
    let catalog = PgPoolOptions::new()
        .max_connections(1)
        .connect(&d.catalog_url())
        .await
        .expect("catalog");
    sqlx::query("DELETE FROM settings WHERE key = 'data_database_name'")
        .execute(&catalog)
        .await
        .expect("forget the name");
    catalog.close().await;
    let resolved = resolve(&d.target(None))
        .await
        .expect("resolve")
        .expect("the catalog database exists");
    assert_eq!(resolved.data_database, d.derived_db);
    let err = PostgresCatalogMigrations
        .status(&d.target(None))
        .await
        .expect_err("the derived database does not exist");
    assert!(
        err.to_string().contains(&d.derived_db),
        "the fallback is the derived name: {err}"
    );
    drop(d);
}
