// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! extenddb — the ExtendDB server binary.
//!
//! This is the reference thin bin for the per-backend packaging model: it
//! installs exactly one backend and hands off to the shared `extenddb-app` CLI.
//! A third-party backend author copies this file, swaps the `backend()` call for
//! their crate, and ships their own `extenddb-<backend>` image — with no edits to
//! any ExtendDB core crate.
//!
//! In-tree backends are selected by mutually exclusive Cargo features:
//! `postgres` (the default production backend), `mongodb` (production, built with
//! `--no-default-features --features mongodb`), `cassandra`, and `sqlite`/`sqlite-memory`
//! (the dev/CI backend). Exactly one must be enabled: [`set_backend`] installs one
//! backend per process, so a build with more than one would be ambiguous and is
//! rejected at compile time.

// Exactly one backend feature must be enabled.
#[cfg(any(
    all(feature = "postgres", feature = "cassandra"),
    all(feature = "postgres", feature = "mongodb"),
    all(feature = "postgres", feature = "sqlite"),
    all(feature = "sqlite", feature = "cassandra"),
    all(feature = "sqlite", feature = "mongodb"),
    all(feature = "cassandra", feature = "mongodb"),
))]
compile_error!(
    "the `postgres`, `mongodb`, `sqlite`, and `cassandra` features are mutually exclusive: \
     a thin bin installs exactly one backend (e.g. build the MongoDB binary with \
     `--no-default-features --features mongodb)"
);

#[cfg(not(any(
    feature = "postgres",
    feature = "mongodb",
    feature = "sqlite",
    feature = "cassandra"
)))]
compile_error!(
    "no backend selected: enable the `postgres` (default), `mongodb`, `sqlite` or `cassandra` feature"
);

// Developer mode relaxes the security posture (plain HTTP on loopback, open
// authorization). It is a dev/CI-only profile and must be built only with a
// dev/CI-suitable backend. Rather than denying each production backend by name
// (every backend is a production backend unless proven otherwise, so a deny-list
// would have to grow with each new one), require a known dev backend: dev-mode
// compiles only when `sqlite` is enabled. `sqlite-memory` enables `sqlite`, so it
// is covered too; postgres, mongodb — or any future production backend — fail the
// build, so there is no path by which a production deployment can serve in dev mode.
#[cfg(all(feature = "dev-mode", not(feature = "sqlite")))]
compile_error!(
    "the `dev-mode` feature requires a dev/CI backend such as `sqlite`; it must \
     not be built with a production backend like `postgres` or `mongodb` (build \
     with `--no-default-features --features sqlite-memory,dev-mode`)"
);

fn main() -> anyhow::Result<()> {
    // Install the compiled-in backend before dispatch. The compiler checks this
    // call; there is no link-time auto-registration and no name to resolve, so a
    // missing or mistyped backend cannot become a runtime error.
    #[cfg(feature = "cassandra")]
    extenddb_storage::set_backend(extenddb_storage_cassandra::backend())?;
    #[cfg(feature = "mongodb")]
    extenddb_storage::set_backend(extenddb_storage_mongodb::backend())?;
    #[cfg(feature = "postgres")]
    extenddb_storage::set_backend(extenddb_storage_postgres::backend())?;
    #[cfg(feature = "sqlite")]
    extenddb_storage::set_backend(extenddb_storage_sqlite::backend())?;

    extenddb_app::run(extenddb_app::BuildInfo {
        // Read from the bin crate so the reported version is the deployed
        // artifact's, not a library crate's.
        version: env!("CARGO_PKG_VERSION"),
        git_hash: env!("EXTENDDB_GIT_HASH"),
        build_time: env!("EXTENDDB_BUILD_TIME"),
        test_hooks_enabled: cfg!(feature = "mongodb-test-hooks"),
    })
}

#[cfg(test)]
mod tests {
    /// Install this binary's backend once for the test process. Only the
    /// postgres and sqlite builds have tests that need it.
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    fn install_backend() {
        #[cfg(feature = "cassandra")]
        let _ = extenddb_storage::set_backend(extenddb_storage_cassandra::backend());
        #[cfg(feature = "mongodb")]
        let _ = extenddb_storage::set_backend(extenddb_storage_mongodb::backend());
        #[cfg(feature = "postgres")]
        let _ = extenddb_storage::set_backend(extenddb_storage_postgres::backend());
        #[cfg(feature = "sqlite")]
        let _ = extenddb_storage::set_backend(extenddb_storage_sqlite::backend());
    }

    /// Zero-config serve contract: with the SQLite backend installed,
    /// built-in defaults deserialize with no config file, bind to loopback
    /// (so the dev-mode loopback guard passes), and select the backend's
    /// default storage path.
    #[cfg(feature = "sqlite")]
    #[test]
    fn builtin_defaults_load_for_sqlite_and_bind_loopback() {
        install_backend();
        let cfg = extenddb_config::load_builtin_defaults()
            .expect("sqlite storage config has no required fields");
        assert_eq!(cfg.server.bind_addr, "127.0.0.1");
        assert_eq!(cfg.server.port, 18443);
        // The sqlite config absolutizes a relative file path against the
        // working directory, so match on the invariant part of each default.
        let path = cfg.storage.connection_config();
        if cfg!(feature = "sqlite-memory") {
            assert_eq!(path, ":memory:");
        } else {
            assert!(
                path.ends_with("extenddb.sqlite"),
                "default file path should be extenddb.sqlite, got: {path}"
            );
        }
    }

    /// `load_builtin_defaults` is only reachable from dev-mode builds (a
    /// postgres + dev-mode binary is a compile error), but its defaults must
    /// never relax the production posture regardless of backend: loopback
    /// bind and TLS enabled.
    #[cfg(feature = "postgres")]
    #[test]
    fn builtin_defaults_keep_loopback_and_tls_for_postgres() {
        install_backend();
        let cfg = extenddb_config::load_builtin_defaults()
            .expect("postgres storage config defaults to a local dev connection");
        assert_eq!(cfg.server.bind_addr, "127.0.0.1");
        assert!(cfg.server.tls.enabled);
    }

    /// No PostgreSQL migration is declared additive until CI runs the
    /// previous release against a catalog this binary migrated; the PR that
    /// adds that job deletes this test. It lives here because the backend's
    /// table is public and this crate builds with the postgres feature in
    /// `cargo test --workspace`.
    #[cfg(feature = "postgres")]
    #[test]
    fn no_postgres_migration_is_additive_before_the_old_binary_job() {
        extenddb_storage::migrations::refuse_additive(
            "postgres",
            extenddb_storage_postgres::migrations::migration,
        )
        .unwrap_or_else(|e| panic!("{e}"));
    }

    /// `migrate --yes` on PostgreSQL, as the command runs it (the lock, then
    /// the status under it), refuses two empty databases (a DBA created
    /// them ahead of `init`) as `Uninitialized`, exit 1, and writes nothing
    /// to either. Needs `EXTENDDB_TEST_PG_CONNECTION_STRING` (a base URL
    /// with no database, for a role that may create databases); skips
    /// without it.
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn migrate_yes_refuses_empty_postgres_databases() {
        use extenddb_storage::migrations::{
            CatalogStatus, MigrationError, RunKind, Target, exit, installed, run_locked,
        };

        let Some(base) = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING")
            .ok()
            .map(|s| s.trim().trim_end_matches('/').to_owned())
            .filter(|s| !s.is_empty())
        else {
            eprintln!("skipped: EXTENDDB_TEST_PG_CONNECTION_STRING not set");
            return;
        };
        install_backend();
        let stem = format!(
            "eddb_bin_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        let databases = [format!("{stem}_catalog"), stem.clone()];
        let admin = sqlx::PgPool::connect(&format!("{base}/postgres"))
            .await
            .expect("admin connection");
        for name in &databases {
            sqlx::query(&format!("CREATE DATABASE \"{name}\""))
                .execute(&admin)
                .await
                .expect("create database");
        }
        let target = Target::at(format!("{base}/{}", databases[0]));
        let migrations = installed().expect("the postgres backend migrates on the runner");
        let result = async {
            let lock = migrations
                .lock(&target, std::time::Duration::from_secs(10))
                .await?;
            run_locked(migrations, &target, &lock, "test", RunKind::Migrate).await
        }
        .await;
        let mut written = Vec::new();
        for name in &databases {
            let pool = sqlx::PgPool::connect(&format!("{base}/{name}"))
                .await
                .expect("database connection");
            let tables: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public'")
                    .fetch_one(&pool)
                    .await
                    .expect("count tables");
            pool.close().await;
            written.push(tables);
        }
        for name in &databases {
            let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
                .execute(&admin)
                .await;
        }
        admin.close().await;
        let err = result.expect_err("an empty catalog database is not a catalog");
        assert_eq!(err, MigrationError::Status(CatalogStatus::Uninitialized));
        assert_eq!(err.exit_code(), exit::ERROR);
        assert_eq!(written, [0, 0], "tables written to the empty databases");
    }
}
