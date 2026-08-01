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
//! In-tree backends are selected by mutually exclusive Cargo features
//! (`postgres` is the default; `sqlite`/`sqlite-memory` build the dev/CI
//! backend). Exactly one must be enabled: [`set_backend`] installs one backend
//! per process, so a build with both would be ambiguous and is rejected at
//! compile time.

// Exactly one backend must be selected. `dev-mode` is the self-contained
// SQLite dev profile, so it counts as the SQLite backend.
#[cfg(all(feature = "postgres", any(feature = "sqlite", feature = "dev-mode")))]
compile_error!(
    "the `postgres` and `sqlite`/`dev-mode` features are mutually exclusive: a \
     thin bin installs exactly one backend (build the SQLite binary with \
     `--no-default-features --features sqlite`, or the dev binary with \
     `--no-default-features --features dev-mode`)"
);
#[cfg(not(any(feature = "postgres", feature = "sqlite", feature = "dev-mode")))]
compile_error!(
    "no backend selected: enable the `postgres` (default), `sqlite`, or `dev-mode` feature"
);

// Developer mode relaxes the security posture (plain HTTP on loopback, open
// authorization) and deliberately omits the TLS stack from the build. The
// production features carry `tls`, so combining them with `dev-mode` is a
// production/dev mismatch worth failing loudly: there must be no path by
// which a production deployment serves in dev mode, and no dev binary that
// silently carries the full TLS stack.
#[cfg(all(feature = "dev-mode", feature = "tls"))]
compile_error!(
    "`dev-mode` is a self-contained dev profile without the TLS stack; do not \
     combine it with `sqlite`/`sqlite-memory`/`tls` (build with \
     `--no-default-features --features dev-mode`)"
);

fn main() -> anyhow::Result<()> {
    // Install the compiled-in backend before dispatch. The compiler checks this
    // call; there is no link-time auto-registration and no name to resolve, so a
    // missing or mistyped backend cannot become a runtime error.
    #[cfg(feature = "postgres")]
    extenddb_storage::set_backend(extenddb_storage_postgres::backend())?;
    #[cfg(any(feature = "sqlite", feature = "dev-mode"))]
    extenddb_storage::set_backend(extenddb_storage_sqlite::backend())?;

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
        #[cfg(any(feature = "sqlite", feature = "dev-mode"))]
        let _ = extenddb_storage::set_backend(extenddb_storage_sqlite::backend());
    }

    /// Zero-config serve contract: with the SQLite backend installed,
    /// built-in defaults deserialize with no config file, bind to loopback
    /// (so the dev-mode loopback guard passes), and select the backend's
    /// default storage path.
    #[cfg(any(feature = "sqlite", feature = "dev-mode"))]
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
        if cfg!(any(feature = "sqlite-memory", feature = "dev-mode")) {
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
}
