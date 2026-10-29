// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb verify` — validate a extenddb deployment (REQ-CAT-013).
//!
//! Connects to catalog, checks version, enumerates tables and indexes,
//! connects to data database, reports healthy/unhealthy.
//!
//! On a backend on the migration runner the version check is the catalog
//! status, read without a lock or a write, and `verify` exits with the
//! status's code (3 pending, 4 ahead by a breaking migration, 5 a person
//! must act) before the 1 of a failed check.
//!
//! This command uses raw `sqlx` queries intentionally. It is a diagnostic
//! tool that runs outside the server process and needs direct database
//! access to verify infrastructure health. Routing through the storage
//! abstraction would defeat the purpose of an independent health check.

use clap::Args;

use extenddb_config as config;

#[derive(Args)]
pub struct VerifyArgs {
    /// Path to configuration file (default: extenddb.toml; a dev-mode build
    /// without one reads the EXTENDDB__ environment variables, as serve does)
    #[arg(short, long)]
    config: Option<String>,
}

/// Run `extenddb verify`; returns the process exit status.
pub async fn run(args: VerifyArgs, release: &str) -> anyhow::Result<u8> {
    let (app_config, config_file) = crate::util::command_config(args.config.as_deref())?;
    let expected_version =
        extenddb_storage::operations::catalog_version().unwrap_or_else(|_| "unknown".to_string());

    // Parse connection string to get database name for display
    let parts = extenddb_storage::operations::parse_connection_string(
        app_config.storage.connection_config(),
    )
    .map_err(|e| anyhow::anyhow!("Failed to parse connection string: {e}"))?;

    let mut errors = 0u32;

    println!("=== extenddb verify ===");
    println!(
        "Config:           {}",
        config_file
            .as_deref()
            .unwrap_or("none (built-in defaults and EXTENDDB__ variables)")
    );
    println!("Catalog database: {}", parts.database);
    println!();

    // Read first, before anything opens the catalog: a store that does not
    // exist is reported, not created.
    let status_code = match extenddb_storage::migrations::installed() {
        Some(migrations) => {
            let code =
                check_migrations(migrations, config_file.as_deref(), &app_config, release).await;
            if code == extenddb_storage::migrations::exit::ERROR {
                println!();
                println!("=== UNHEALTHY: see the catalog check above ===");
                return Ok(code);
            }
            Some(code)
        }
        None => None,
    };

    // Create settings and diagnostics store
    println!("--- Checking catalog connection...");
    let store = match extenddb_storage::settings_store::create_settings_store(
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

    // Check 2: Catalog version (D-10: strict parsing), on a backend that
    // still records one; the runner's status replaced it above.
    if status_code.is_none() {
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
    }

    // Check 3: Data database connection.
    println!("--- Checking data database...");

    // Create diagnostics store (reuse for data DB test and table/index counts)
    let diag_store = extenddb_storage::diagnostics_store::create_diagnostics_store(
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
    let status_code = status_code.unwrap_or(0);
    if errors == 0 && status_code == 0 {
        println!("=== HEALTHY: All checks passed ===");
        return Ok(0);
    }
    if errors > 0 {
        println!("=== UNHEALTHY: {errors} check(s) failed ===");
    } else {
        println!("=== UNHEALTHY: the catalog needs the action above ===");
    }
    Ok(if status_code == 0 { 1 } else { status_code })
}

/// Print the catalog status and the notes the ledger records, and return
/// the status's exit code.
async fn check_migrations(
    migrations: &dyn extenddb_storage::migrations::CatalogMigrations,
    config_file: Option<&str>,
    app_config: &config::AppConfig,
    release: &str,
) -> u8 {
    use extenddb_storage::migrations::advice::{self, AdviceFor};
    use extenddb_storage::migrations::{CatalogStatus, Target};

    println!("--- Checking catalog migrations...");
    let location = app_config.storage.connection_config();
    let target = Target::at(location);
    let advice = AdviceFor::new(
        release,
        config::redact_password(location),
        Some(migrations),
        &target,
        config_file,
    );
    let advice = advice.advice();
    let status = match migrations.status(&target).await {
        Ok(status) => status,
        Err(e) => {
            println!("  FAIL: {}", advice::error_message(&e, &advice));
            return e.exit_code();
        }
    };
    match &status {
        CatalogStatus::Current { .. } => println!("  OK: Current: {status}"),
        CatalogStatus::AheadCompatible { .. } => println!("  WARN: {status}"),
        _ => {
            for (i, line) in advice::status_message(&status, &advice).lines().enumerate() {
                println!("  {}{line}", if i == 0 { "FAIL: " } else { "" });
            }
        }
    }
    match migrations.status_notes(&target).await {
        Ok(notes) => {
            for note in notes {
                println!("  WARN: {}", note.text);
            }
        }
        Err(e) => println!("  WARN: {}", advice::error_message(&e, &advice)),
    }
    if let Ok(rows) = migrations.catalog_ledger(&target).await {
        for row in rows.iter().filter(|r| !r.notes.is_empty()) {
            println!("  Notes of migration {} {}:", row.id, row.name);
            for note in row.notes.lines() {
                println!("    {note}");
            }
        }
    }
    status.exit_code()
}
