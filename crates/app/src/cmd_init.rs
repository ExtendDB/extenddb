// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb init` — initialize a new extenddb deployment (REQ-CAT-011).
//!
//! Creates the catalog and data databases, runs schema migrations,
//! records the data database connection, and generates `extenddb.toml`.

use std::path::Path;

use clap::Args;

use crate::init_helpers::{generate_config, generate_tls_cert_if_needed};
use extenddb_config as config;

#[derive(Args)]
#[allow(clippy::doc_markdown)] // Clap help text, not rustdoc
pub struct InitArgs {
    /// Storage backend. Optional: the binary's compiled-in backend is used;
    /// when given, it is validated against that backend.
    #[arg(long)]
    backend: Option<String>,

    /// Data database name (default: extenddb)
    #[arg(long)]
    data_db: Option<String>,

    /// Catalog database name (default: <data-db>_catalog)
    #[arg(long)]
    catalog_db: Option<String>,

    /// SQLite database file path (default: extenddb.sqlite). The chosen path
    /// is written to the generated config file, so `serve` finds it without
    /// further flags. SQLite backend only.
    #[arg(long)]
    sqlite_path: Option<String>,

    /// DuckDB database file path (default: extenddb.duckdb). The chosen path
    /// is written to the generated config file, so `serve` finds it without
    /// further flags. DuckDB backend only.
    #[arg(long)]
    duckdb_path: Option<String>,

    /// PostgreSQL host (hostname, IP address, or absolute Unix socket directory path)
    #[arg(long)]
    pg_host: Option<String>,

    /// PostgreSQL port
    #[arg(long)]
    pg_port: Option<u16>,

    /// PostgreSQL admin user (for CREATE DATABASE)
    #[arg(long)]
    pg_user: Option<String>,

    /// PostgreSQL admin password (required for remote/Aurora connections).
    /// Prefer `EXTENDDB_PG_PASSWORD` to keep the value out of process arguments.
    #[arg(long)]
    pg_pass: Option<String>,

    /// extenddb application user
    #[arg(long)]
    extenddb_user: Option<String>,

    /// extenddb application password.
    /// Prefer `EXTENDDB_APP_PASSWORD` to keep the value out of process arguments.
    #[arg(long)]
    extenddb_pass: Option<String>,

    /// Output config file path
    #[arg(long, default_value = "extenddb.toml")]
    config: String,

    /// Server bind address (included as a SAN in the self-signed certificate)
    #[arg(long)]
    bind_addr: Option<String>,

    /// Additional Subject Alternative Name for the self-signed certificate,
    /// repeatable. Added to the default localhost/127.0.0.1/bind-addr list so
    /// the cert is also valid for names like an in-cluster service DNS name.
    #[arg(long = "tls-san")]
    tls_san: Vec<String>,

    /// Overwrite existing config file (default: --no-overwrite, exit 255 if exists)
    #[arg(long, overrides_with = "no_overwrite")]
    overwrite: bool,

    /// Do not overwrite existing config file (exit 255 if exists). This is the default.
    #[arg(long, overrides_with = "overwrite")]
    no_overwrite: bool,
}

/// Search for the rendered docs directory in well-known locations.
///
/// Checks (in order):
/// 1. `docs/rendered/` relative to the current executable
/// 2. `docs/rendered/` relative to the current working directory
/// 3. `~/.extenddb/docs/rendered/`
///
/// Returns the first path that contains a `manifest.json` file.
fn discover_docs_dir() -> Option<String> {
    let candidates: Vec<std::path::PathBuf> = {
        let mut v = Vec::new();
        // Relative to the binary.
        if let Ok(exe) = std::env::current_exe()
            && let Some(dir) = exe.parent()
        {
            v.push(dir.join("docs/rendered"));
            // Also check one level up (binary in target/release/).
            if let Some(parent) = dir.parent() {
                v.push(parent.join("docs/rendered"));
            }
        }
        // Relative to cwd.
        v.push(std::path::PathBuf::from("docs/rendered"));
        // Well-known install path.
        if let Ok(home) = std::env::var("HOME") {
            v.push(std::path::PathBuf::from(format!(
                "{home}/.extenddb/docs/rendered"
            )));
        }
        v
    };

    for candidate in candidates {
        if candidate.join("manifest.json").is_file() {
            // Canonicalize to get an absolute path for the config file.
            if let Ok(abs) = candidate.canonicalize() {
                return Some(abs.to_string_lossy().into_owned());
            }
        }
    }
    None
}

/// Returns exit code: 0 = success, 255 = existing config preserved.
pub async fn run(args: InitArgs) -> anyhow::Result<u8> {
    // The installed backend is authoritative — there is exactly one compiled
    // into this binary. An explicit --backend (or a config file's backend key)
    // is validated against it rather than driving dispatch, so a mismatch is a
    // clear startup error instead of a mis-generated config.
    let backend = extenddb_storage::backend_name()
        .ok_or_else(|| anyhow::anyhow!("no storage backend installed"))?
        .to_owned();
    if let Some(ref requested) = args.backend
        && *requested != backend
    {
        anyhow::bail!(
            "this binary is built with the '{backend}' backend; \
             --backend {requested} requires the extenddb-{requested} binary"
        );
    }
    if args.backend.is_none() && Path::new(&args.config).exists() {
        let app_config = config::load(&args.config)?;
        if app_config.storage.backend != backend {
            anyhow::bail!(
                "config file '{}' selects backend '{}', but this binary is built \
                 with the '{backend}' backend",
                args.config,
                app_config.storage.backend,
            );
        }
    }

    println!("=== extenddb init (backend: {backend}) ===");

    // Check config file conflict early, before any database work
    if Path::new(&args.config).exists() && !args.overwrite {
        eprintln!(
            "Error: Config file \"{}\" already exists. \
             Use --overwrite to delete and regenerate it.",
            args.config
        );
        return Ok(255);
    }

    // Collect CLI args for backend-specific parsing. Environment-sourced
    // secrets are appended only to this in-process copy, not OS-visible argv.
    let mut cli_args: Vec<String> = std::env::args().collect();
    crate::util::append_secret_arg(
        &mut cli_args,
        "--pg-pass",
        std::env::var_os("EXTENDDB_PG_PASSWORD"),
        "EXTENDDB_PG_PASSWORD",
    )?;
    crate::util::append_secret_arg(
        &mut cli_args,
        "--extenddb-pass",
        std::env::var_os("EXTENDDB_APP_PASSWORD"),
        "EXTENDDB_APP_PASSWORD",
    )?;

    // Extract bind_addr from CLI args
    let bind_addr =
        extract_arg(&cli_args, "--bind-addr").unwrap_or_else(|| "127.0.0.1".to_string());

    // Generate the self-signed TLS certificate if it isn't already present,
    // covering the bind address plus any --tls-san values so it matches the URLs
    // clients use. This runs before any database work so that an unusable
    // --tls-san fails before we create users or databases.
    generate_tls_cert_if_needed(&bind_addr, &args.tls_san)?;

    // Create bootstrapper via registry (no hardcoded match!)
    let bootstrapper = extenddb_storage::bootstrapper::create_bootstrapper(&args.config, &cli_args)
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    // Ensure application user exists.
    bootstrapper
        .ensure_app_user()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    // Grant the application role to the admin user so CREATE DATABASE ... OWNER
    // succeeds on RDS/Aurora where the admin is not a true superuser.
    bootstrapper
        .grant_app_role_to_admin()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    // Create catalog database — abort if it already exists.
    bootstrapper
        .create_catalog_db()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    // Create data database — abort if it already exists.
    bootstrapper
        .create_data_db()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    // Take the same lock `migrate` uses around the schema work below, so an
    // init cannot race a migrate running on another replica and fail on a
    // duplicate pg_type entry from concurrent `CREATE TABLE IF NOT EXISTS`.
    // Two concurrent inits cannot get this far: the second aborts earlier, at
    // `create_catalog_db`, because the database already exists. The catalog
    // database does exist by this point, so the lock connection can be opened.
    bootstrapper
        .acquire_migration_lock()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    let migration_result = run_init_migrations(bootstrapper.as_ref()).await;
    if let Err(e) = bootstrapper.release_migration_lock().await {
        tracing::warn!("Failed to release migration lock: {e:?}");
    }
    migration_result?;

    bootstrapper
        .bootstrap_encryption_key()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?; // REQ-AUTH-010

    bootstrapper
        .bootstrap_default_account()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    // REQ-AUTH-003
    let env_user = std::env::var("EXTENDDB_ADMIN_USER").ok();
    let env_pass = std::env::var("EXTENDDB_ADMIN_PASSWORD").ok();
    let admin_result = bootstrapper
        .bootstrap_admin_user(env_user.as_deref(), env_pass.as_deref())
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    if admin_result.already_existed {
        // Already printed by the bootstrap store.
    } else if admin_result.from_env {
        println!(
            "    Admin user '{}' created (credentials from environment).",
            admin_result.username
        );
    } else if let Some(ref password) = admin_result.generated_password {
        println!(
            "\n  ┌─────────────────────────────────────────────────┐\
             \n  │  Admin credentials (shown once, save them now)  │\
             \n  │                                                 │\
             \n  │  Username: {:<37} │\
             \n  │  Password: {:<37} │\
             \n  └─────────────────────────────────────────────────┘\n",
            admin_result.username, password,
        );
    }

    // AI-1: Discover rendered docs directory for the config file.
    let docs_dir = discover_docs_dir();
    if let Some(ref d) = docs_dir {
        println!("--- Documentation found: {d}");
    } else {
        println!(
            "--- Documentation not found. Set docs_dir in the config file \
             to enable /console/docs. Run `python3 docs/build-docs.py` to \
             render documentation."
        );
    }

    // Generate or update extenddb.toml.
    let config_path = &args.config;

    if Path::new(config_path).exists() {
        std::fs::remove_file(config_path)?;
    }
    generate_config(
        config_path,
        &backend,
        bootstrapper.as_ref(),
        &bind_addr,
        docs_dir.as_deref(),
    )?;

    println!(
        "\n=== extenddb init complete ===\nStart the server with: extenddb serve --config {config_path}"
    );

    Ok(0)
}

/// Apply the catalog and data schema while the migration lock is held. Split out
/// of `run` so that the lock is released on every path, including errors.
async fn run_init_migrations(
    bootstrapper: &dyn extenddb_storage::bootstrapper::Bootstrapper,
) -> anyhow::Result<()> {
    // Check if catalog is already initialized.
    let initialized = bootstrapper
        .is_catalog_initialized()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    if initialized {
        println!("--- Catalog already initialized. Use 'extenddb migrate' for pending migrations.");
    } else {
        bootstrapper
            .run_catalog_migrations()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    }

    // Record data database connection in catalog.
    bootstrapper
        .record_data_connection()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    // Initialize data database schema.
    bootstrapper
        .run_data_migrations()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    Ok(())
}

/// Extract a CLI argument value by flag name.
fn extract_arg(args: &[String], flag: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
}

#[cfg(test)]
mod tests {
    use super::InitArgs;
    use clap::Args as _;
    use clap::FromArgMatches as _;

    fn parse(args: &[&str]) -> Result<InitArgs, clap::Error> {
        let cmd = InitArgs::augment_args(clap::Command::new("init"));
        let matches =
            cmd.try_get_matches_from(std::iter::once("init").chain(args.iter().copied()))?;
        InitArgs::from_arg_matches(&matches)
    }

    /// Regression test for issue #267: `--sqlite-path` was documented and read
    /// by the SQLite bootstrapper via `extract_arg`, but never declared to
    /// clap, so every invocation was rejected with "unexpected argument"
    /// before the bootstrapper could run.
    #[test]
    fn init_accepts_sqlite_path() {
        let args =
            parse(&["--backend", "sqlite", "--sqlite-path", "/data/x.sqlite"]).expect("parse");
        assert_eq!(args.sqlite_path.as_deref(), Some("/data/x.sqlite"));
    }

    /// The control: an actually-unknown flag must still be rejected, proving
    /// the test above discriminates on the declaration rather than on clap
    /// somehow accepting arbitrary arguments.
    #[test]
    fn init_rejects_unknown_flags() {
        assert!(parse(&["--sqlite-pathological", "/data/x.sqlite"]).is_err());
    }

    /// `--duckdb-path` is the DuckDB backend's analogue of `--sqlite-path` and
    /// is read by that backend's bootstrapper from the raw argv, so it must be
    /// declared here or clap rejects it before the bootstrapper ever sees it.
    #[test]
    fn init_accepts_duckdb_path() {
        let args =
            parse(&["--backend", "duckdb", "--duckdb-path", "/data/x.duckdb"]).expect("parse");
        assert_eq!(args.duckdb_path.as_deref(), Some("/data/x.duckdb"));
    }
}
