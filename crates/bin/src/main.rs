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
//! `--no-default-features --features mongodb`), `sqlite`/`sqlite-memory` (the
//! dev/CI backend), and `duckdb`/`duckdb-memory` (embedded, columnar). Exactly
//! one must be enabled: [`set_backend`] installs one
//! backend per process, so a build with more than one would be ambiguous and is
//! rejected at compile time.

// Exactly one backend feature must be enabled.
#[cfg(any(
    all(feature = "postgres", feature = "sqlite"),
    all(feature = "postgres", feature = "mongodb"),
    all(feature = "postgres", feature = "duckdb"),
    all(feature = "sqlite", feature = "mongodb"),
    all(feature = "sqlite", feature = "duckdb"),
    all(feature = "mongodb", feature = "duckdb"),
))]
compile_error!(
    "the `postgres`, `mongodb`, `sqlite`, and `duckdb` features are mutually exclusive: a \
     thin bin installs exactly one backend (e.g. build the MongoDB binary with \
     `--no-default-features --features mongodb`)"
);
#[cfg(not(any(
    feature = "postgres",
    feature = "mongodb",
    feature = "sqlite",
    feature = "duckdb"
)))]
compile_error!(
    "no backend selected: enable the `postgres` (default), `mongodb`, `sqlite`, or `duckdb` feature"
);

// Developer mode relaxes the security posture (plain HTTP on loopback, open
// authorization). It is a dev/CI-only profile and must be built only with a
// dev/CI-suitable backend. Rather than denying each production backend by name
// (every backend is a production backend unless proven otherwise, so a deny-list
// would have to grow with each new one), require a known dev backend: dev-mode
// compiles only when `sqlite` or `duckdb` is enabled (both embedded, both with an
// in-memory mode). `sqlite-memory` / `duckdb-memory` enable their base feature,
// so they are covered too; postgres, mongodb — or any future production backend —
// fail the build, so there is no path by which a production deployment can serve
// in dev mode.
#[cfg(all(feature = "dev-mode", not(any(feature = "sqlite", feature = "duckdb"))))]
compile_error!(
    "the `dev-mode` feature requires a dev/CI backend such as `sqlite` or `duckdb`; \
     it must not be built with a production backend like `postgres` or `mongodb` \
     (build with `--no-default-features --features sqlite-memory,dev-mode`)"
);

fn main() -> anyhow::Result<()> {
    // Install the compiled-in backend before dispatch. The compiler checks this
    // call; there is no link-time auto-registration and no name to resolve, so a
    // missing or mistyped backend cannot become a runtime error.
    #[cfg(feature = "postgres")]
    extenddb_storage::set_backend(extenddb_storage_postgres::backend())?;
    #[cfg(feature = "sqlite")]
    extenddb_storage::set_backend(extenddb_storage_sqlite::backend())?;
    #[cfg(feature = "mongodb")]
    extenddb_storage::set_backend(extenddb_storage_mongodb::backend())?;
    #[cfg(feature = "duckdb")]
    extenddb_storage::set_backend(extenddb_storage_duckdb::backend())?;

    extenddb_app::run(extenddb_app::BuildInfo {
        // Read from the bin crate so the reported version is the deployed
        // artifact's, not a library crate's.
        version: env!("CARGO_PKG_VERSION"),
        git_hash: env!("EXTENDDB_GIT_HASH"),
        build_time: env!("EXTENDDB_BUILD_TIME"),
    })
}

#[cfg(test)]
mod tests {
    /// Install this binary's backend once for the test process.
    fn install_backend() {
        #[cfg(feature = "postgres")]
        let _ = extenddb_storage::set_backend(extenddb_storage_postgres::backend());
        #[cfg(feature = "sqlite")]
        let _ = extenddb_storage::set_backend(extenddb_storage_sqlite::backend());
        #[cfg(feature = "mongodb")]
        let _ = extenddb_storage::set_backend(extenddb_storage_mongodb::backend());
        #[cfg(feature = "duckdb")]
        let _ = extenddb_storage::set_backend(extenddb_storage_duckdb::backend());
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

    /// Zero-config serve contract for the DuckDB backend, mirroring the SQLite
    /// one: built-in defaults load with no config file, bind to loopback, and
    /// select the backend's default database path.
    #[cfg(feature = "duckdb")]
    #[test]
    fn builtin_defaults_load_for_duckdb_and_bind_loopback() {
        install_backend();
        let cfg = extenddb_config::load_builtin_defaults()
            .expect("duckdb storage config has no required fields");
        assert_eq!(cfg.server.bind_addr, "127.0.0.1");
        assert_eq!(cfg.server.port, 18443);
        let path = cfg.storage.connection_config();
        if cfg!(feature = "duckdb-memory") {
            assert_eq!(path, ":memory:");
        } else {
            assert!(
                path.ends_with("extenddb.duckdb"),
                "default file path should be extenddb.duckdb, got: {path}"
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
}
