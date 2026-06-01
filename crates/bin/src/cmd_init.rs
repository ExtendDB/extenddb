// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb init` — initialize a new extenddb deployment (REQ-CAT-011).
//!
//! Creates the catalog and data databases, runs schema migrations,
//! records the data database connection, and generates `extenddb.toml`.

use std::path::Path;

use clap::Args;

use crate::config;
use crate::init_helpers::{generate_config, generate_tls_cert_if_needed};

#[derive(Args)]
#[allow(clippy::doc_markdown)] // Clap help text, not rustdoc
pub struct InitArgs {
    /// Storage backend (postgres) (default: postgres)
    #[arg(long, default_value = "postgres")]
    backend: Option<String>,

    /// Data database name (default: extenddb)
    #[arg(long)]
    data_db: Option<String>,

    /// Catalog database name (default: <data-db>_catalog)
    #[arg(long)]
    catalog_db: Option<String>,

    /// PostgreSQL host
    #[arg(long)]
    pg_host: Option<String>,

    /// PostgreSQL port
    #[arg(long)]
    pg_port: Option<u16>,

    /// PostgreSQL admin user (for CREATE DATABASE)
    #[arg(long)]
    pg_user: Option<String>,

    /// PostgreSQL admin password (required for remote/Aurora connections).
    #[arg(long)]
    pg_pass: Option<String>,

    /// extenddb application user
    #[arg(long)]
    extenddb_user: Option<String>,

    /// extenddb application password
    #[arg(long)]
    extenddb_pass: Option<String>,

    /// Output config file path
    #[arg(long, default_value = "extenddb.toml")]
    config: String,

    /// Server bind address (included as a SAN in the self-signed certificate)
    #[arg(long)]
    bind_addr: Option<String>,

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
    // Determine backend: CLI flag > config file > default
    let backend = if let Some(ref b) = args.backend {
        b.clone()
    } else if Path::new(&args.config).exists() {
        let app_config = config::load(&args.config)?;
        app_config.storage._backend
    } else {
        "postgres".to_owned()
    };

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

    // Collect CLI args for backend-specific parsing
    let cli_args: Vec<String> = std::env::args().collect();

    // Create bootstrapper via registry (no hardcoded match!)
    let bootstrapper =
        extenddb_storage::bootstrapper::create_bootstrapper(&backend, &args.config, &cli_args)
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

    // Extract bind_addr from CLI args
    let bind_addr =
        extract_arg(&cli_args, "--bind-addr").unwrap_or_else(|| "127.0.0.1".to_string());

    // Generate self-signed TLS certificate if not already present.
    // Include the server bind address as a SAN so the cert matches the URL.
    generate_tls_cert_if_needed(&bind_addr)?;

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
    let backend = args.backend.as_deref().unwrap_or("postgres");
    let config_path = &args.config;

    if Path::new(config_path).exists() {
        std::fs::remove_file(config_path)?;
    }
    generate_config(
        config_path,
        backend,
        bootstrapper.as_ref(),
        &bind_addr,
        docs_dir.as_deref(),
    )?;

    println!(
        "\n=== extenddb init complete ===\nStart the server with: extenddb serve --config {config_path}"
    );

    Ok(0)
}

/// Extract a CLI argument value by flag name.
fn extract_arg(args: &[String], flag: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
}
