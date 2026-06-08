// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb verify` — validate a extenddb deployment (REQ-CAT-013).
//!
//! Connects to catalog, checks version, enumerates tables and indexes,
//! connects to data database, reports healthy/unhealthy.
//!
//! This command uses raw `sqlx` queries intentionally. It is a diagnostic
//! tool that runs outside the server process and needs direct database
//! access to verify infrastructure health. Routing through the storage
//! abstraction would defeat the purpose of an independent health check.

use clap::Args;

use crate::config;

#[derive(Args)]
pub struct VerifyArgs {
    /// Path to configuration file
    #[arg(short, long, default_value = "extenddb.toml")]
    config: String,
}

pub async fn run(args: VerifyArgs) -> anyhow::Result<()> {
    if !std::path::Path::new(&args.config).exists() {
        anyhow::bail!(
            "Config file '{}' not found. Run 'extenddb init' to set up a deployment, \
             or use --config <path> to specify a different location.",
            args.config,
        );
    }
    let app_config = config::load(&args.config)?;
    let backend = &app_config.storage.backend;
    let expected_version = extenddb_storage::operations::catalog_version(backend)
        .unwrap_or_else(|_| "unknown".to_string());

    // Parse connection string to get database name for display
    let parts = extenddb_storage::operations::parse_connection_string(
        backend,
        app_config.storage.connection_config(),
    )
    .map_err(|e| anyhow::anyhow!("Failed to parse connection string: {e}"))?;

    let mut errors = 0u32;

    println!("=== extenddb verify ===");
    println!("Config:           {}", args.config);
    println!("Catalog database: {}", parts.database);
    println!();

    // Create settings and diagnostics store
    println!("--- Checking catalog connection...");
    let store = match extenddb_storage::settings_store::create_settings_store(
        backend,
        app_config.storage.connection_config(),
    )
    .await
    {
        Ok(store) => {
            println!("  OK: Connected to catalog.");
            store
        }
        Err(e) => {
            println!("  FAIL: Cannot connect to catalog database: {e}");
            anyhow::bail!("Cannot proceed without catalog connection");
        }
    };

    // Check 2: Catalog version (D-10: strict parsing).
    println!("--- Checking catalog version...");
    match store.get_setting("catalog_version").await {
        Ok(Some(v)) => {
            if v == expected_version {
                println!("  OK: Catalog version {v}");
            } else {
                println!("  WARN: Catalog version {v} (binary expects {expected_version})");
                errors += 1;
            }
        }
        Ok(None) => {
            println!("  FAIL: No catalog version found. Run 'extenddb init'.");
            errors += 1;
        }
        Err(e) => {
            println!("  FAIL: Failed to read catalog version: {e:?}");
            errors += 1;
        }
    }

    // Check 3: Data database connection.
    println!("--- Checking data database...");

    // Create diagnostics store (reuse for data DB test and table/index counts)
    let diag_store = extenddb_storage::diagnostics_store::create_diagnostics_store(
        backend,
        app_config.storage.connection_config(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create diagnostics store: {e}"))?;

    match diag_store.test_data_database_connection().await {
        Ok(db_name) => println!("  OK: Connected to data database '{db_name}'."),
        Err(e) => {
            println!("  FAIL: {e}");
            errors += 1;
        }
    }

    // Check 4: Enumerate tables and indexes.
    println!("--- Enumerating tables...");

    let table_count = diag_store.count_tables().await.unwrap_or(0);
    println!("  Tables: {table_count}");

    let index_count = diag_store.count_indexes().await.unwrap_or(0);
    println!("  Indexes: {index_count}");

    println!();
    if errors == 0 {
        println!("=== HEALTHY: All checks passed ===");
    } else {
        println!("=== UNHEALTHY: {errors} check(s) failed ===");
        std::process::exit(1);
    }

    Ok(())
}
