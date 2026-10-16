// SPDX-License-Identifier: Apache-2.0
//! `extenddb migrate` on a deployment whose data database is not named
//! `<catalog-db>` minus `_catalog`.
//!
//! `init` records the data database name in the catalog's settings
//! (`data_database_name`). The bootstrapper built from a config file and the
//! `migrate` argument vector (no `--data-db`) must use that name, so the data
//! migrations are checked and applied on the real data database; the
//! derivation is only the fallback for a catalog with no recorded name, and
//! an explicit `--data-db` wins over both.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING` (a base URL
//! `postgres://user:pass@host:port`, no database); without it every test
//! reports a skip and passes. Each test creates its own databases and config
//! file and removes them when its deployment is dropped, on a panic too. The
//! config file carries the application password, as the one `init` writes
//! does: the bootstrapper reads the catalog connection string from a file
//! only and connects to the catalog with that password, so the file is
//! written owner-readable and removed with the deployment.

use extenddb_storage::bootstrapper::{BootstrapConfig, Bootstrapper};
use extenddb_storage_postgres::PostgresBootstrapper;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

fn base_conn() -> Option<String> {
    let conn = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").ok()?;
    (!conn.trim().is_empty()).then(|| conn.trim_end_matches('/').to_owned())
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

/// Write `contents` to a new file only its owner can read.
fn write_private(path: &std::path::Path, contents: &str) {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .and_then(|mut f| f.write_all(contents.as_bytes()))
        .expect("config file");
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
    config_path: std::path::PathBuf,
}

impl Drop for Deployment {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.config_path);
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
        init.run_catalog_migrations()
            .await
            .expect("catalog migrations");
        init.record_data_connection().await.expect("record");
        init.run_data_migrations().await.expect("data migrations");
        // The config file `migrate` reads: the catalog connection string only,
        // as `init` writes it. Owner-readable, removed when `self` drops.
        let target = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target")
            });
        let config_path = target.join(format!("{stem}.toml"));
        write_private(
            &config_path,
            &format!(
                "[storage]\nbackend = \"postgres\"\n\n[storage.postgres]\nconnection_string = \
                 \"postgres://{}:{}@{}:{}/{catalog_db}\"\n",
                p.user, p.pass, p.host, p.port
            ),
        );
        Self {
            base,
            admin,
            catalog_db,
            data_db,
            derived_db: stem,
            config_path,
        }
    }

    /// The argument vector `extenddb migrate` hands the bootstrapper.
    fn migrate_args(&self, extra: &[&str]) -> Vec<String> {
        let p = parts(&self.base);
        let mut args = vec![
            "extenddb".to_owned(),
            "migrate".to_owned(),
            "--config".to_owned(),
            self.config_path.display().to_string(),
            "--pg-user".to_owned(),
            p.user,
            "--pg-pass".to_owned(),
            p.pass,
        ];
        args.extend(extra.iter().map(|s| (*s).to_owned()));
        args
    }

    async fn bootstrapper(&self, extra: &[&str]) -> PostgresBootstrapper {
        PostgresBootstrapper::from_config(
            self.config_path.to_str().expect("UTF-8 path"),
            &self.migrate_args(extra),
        )
        .await
        .expect("from_config")
    }
}

/// Without `--data-db`, the bootstrapper `migrate` builds reaches the data
/// database `init` recorded: `pending_data_migrations` reads that database's
/// ledger (and finds it current) instead of failing on the derived name, and
/// `migrate`'s one write to the data database, `run_data_migrations`, lands
/// there. The derived database is never created.
#[tokio::test]
async fn migrate_uses_the_data_database_name_init_recorded() {
    let Some(_) = base_conn() else {
        eprintln!("skipped: EXTENDDB_TEST_PG_CONNECTION_STRING not set");
        return;
    };
    let d = Deployment::new().await;
    let stored: String =
        sqlx::query_scalar("SELECT value FROM settings WHERE key = 'data_database_name'")
            .fetch_one(
                &PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&format!("{}/{}", d.base, d.catalog_db))
                    .await
                    .expect("catalog"),
            )
            .await
            .expect("recorded name");
    assert_eq!(stored, d.data_db, "init records the data database name");

    let migrate = d.bootstrapper(&[]).await;
    let pending = migrate
        .pending_data_migrations()
        .await
        .expect("pending data migrations on the recorded data database");
    assert!(pending.is_empty(), "{pending:?}");
    migrate
        .run_data_migrations()
        .await
        .expect("data migrations");
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

    // An explicit --data-db wins over the recorded name.
    let explicit = d.bootstrapper(&["--data-db", &d.derived_db]).await;
    let err = explicit
        .pending_data_migrations()
        .await
        .expect_err("the derived database does not exist");
    assert!(
        format!("{err:?}").contains(&d.derived_db),
        "the error names the explicit database: {err:?}"
    );
    drop(d);
}

/// A catalog with no recorded name (an older `init`) keeps the derivation.
#[tokio::test]
async fn a_catalog_without_a_recorded_name_falls_back_to_the_derivation() {
    let Some(_) = base_conn() else {
        eprintln!("skipped: EXTENDDB_TEST_PG_CONNECTION_STRING not set");
        return;
    };
    let d = Deployment::new().await;
    let catalog = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{}/{}", d.base, d.catalog_db))
        .await
        .expect("catalog");
    sqlx::query("DELETE FROM settings WHERE key = 'data_database_name'")
        .execute(&catalog)
        .await
        .expect("forget the name");
    let migrate = d.bootstrapper(&[]).await;
    let err = migrate
        .pending_data_migrations()
        .await
        .expect_err("the derived database does not exist");
    assert!(
        format!("{err:?}").contains(&d.derived_db),
        "the fallback is the derived name: {err:?}"
    );
    drop(d);
}
