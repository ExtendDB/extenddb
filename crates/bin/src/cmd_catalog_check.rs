// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb catalog-check` — integrity check for catalog and data databases.
//!
//! Detects orphaned data/GSI tables, missing data tables, and catalog
//! inconsistencies. Refuses to run while the server is up (PID file check).
//!
//! This command uses raw `sqlx` queries intentionally. It is a diagnostic
//! tool that runs outside the server process and needs direct database
//! access to cross-reference catalog metadata against physical `PostgreSQL`
//! tables. Routing through the storage abstraction would defeat the purpose
//! of an independent integrity check.

use std::collections::HashSet;

use clap::Args;
use sqlx::postgres::PgPoolOptions;

use crate::config;

#[derive(Args)]
pub struct CatalogCheckArgs {
    /// Path to configuration file
    #[arg(short, long, default_value = "extenddb.toml")]
    config: String,

    /// Clean up orphaned tables (default: report only)
    #[arg(long)]
    fix: bool,
}

// Linear sequence of 4 independent checks — splitting into helpers would scatter
// the logic without improving readability, and each helper would need the same
// pool/fix parameters threaded through.
#[allow(clippy::too_many_lines)]
pub async fn run(args: CatalogCheckArgs) -> anyhow::Result<()> {
    if !std::path::Path::new(&args.config).exists() {
        anyhow::bail!(
            "Config file '{}' not found. Run 'extenddb init' to set up a deployment, \
             or use --config <path> to specify a different location.",
            args.config,
        );
    }
    let app_config = config::load(&args.config)?;
    let port = app_config.server.port;
    let run_dir = config::expand_tilde(&app_config.server.run_dir);

    // Refuse to run while server is up.
    let pid_path = crate::serve_helpers::pid_file_path(&run_dir, port);
    if let Ok(contents) = std::fs::read_to_string(&pid_path)
        && let Ok(pid) = contents.trim().parse::<i32>()
        && crate::util::is_process_alive(pid)
    {
        anyhow::bail!(
            "Server is running (PID {pid}). Stop it with `extenddb stop` before \
                     running catalog-check."
        );
    }

    println!("=== extenddb catalog-check ===");
    let mut errors = 0u32;

    // Connect to catalog.
    let catalog_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(app_config.storage.connection_config())
        .await
        .map_err(|e| anyhow::anyhow!("Cannot connect to catalog: {e}"))?;
    println!("Connected to catalog.");

    // Connect to data database.
    let data_conn: Option<(String,)> =
        sqlx::query_as("SELECT value FROM settings WHERE key = 'data_database_connection_string'")
            .fetch_optional(&catalog_pool)
            .await?;
    let Some((data_conn_str,)) = data_conn else {
        anyhow::bail!("No data database connection string in settings. Run `extenddb init`.");
    };
    let data_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&data_conn_str)
        .await
        .map_err(|e| anyhow::anyhow!("Cannot connect to data database: {e}"))?;
    println!("Connected to data database.");
    println!();

    // Build expected set from catalog: base tables and GSI tables.
    // Include CREATING tables — they have data tables even before transitioning
    // to ACTIVE. Excluding them would flag their data tables as orphaned.
    let catalog_tables: Vec<(String, String)> = sqlx::query_as(
        "SELECT account_id, table_name FROM tables \
         WHERE table_status IN ('ACTIVE', 'CREATING')",
    )
    .fetch_all(&catalog_pool)
    .await?;

    let catalog_indexes: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT t.account_id, t.table_name, i.index_name \
         FROM indexes i JOIN tables t ON i.table_id = t.table_id \
         WHERE t.table_status IN ('ACTIVE', 'CREATING')",
    )
    .fetch_all(&catalog_pool)
    .await?;

    let mut expected: HashSet<String> = HashSet::new();
    for (acct, name) in &catalog_tables {
        expected.insert(format!("_ddb_{acct}_{name}"));
    }
    for (acct, name, idx) in &catalog_indexes {
        expected.insert(format!("_ddb_{acct}_{name}__gsi__{idx}"));
    }

    // Get actual tables in data database (only _ddb_ prefixed).
    let actual_tables: Vec<(String,)> = sqlx::query_as(
        "SELECT tablename FROM pg_tables \
         WHERE schemaname = 'public' AND tablename LIKE '_ddb_%'",
    )
    .fetch_all(&data_pool)
    .await?;

    let actual: HashSet<String> = actual_tables.into_iter().map(|(n,)| n).collect();

    // Check 1: Orphaned data tables (in data DB but not in catalog).
    println!("--- Checking for orphaned data tables...");
    let orphaned: Vec<&String> = actual.difference(&expected).collect();
    if orphaned.is_empty() {
        println!("  OK: No orphaned tables.");
    } else {
        println!("  FOUND: {} orphaned table(s):", orphaned.len());
        for name in &orphaned {
            println!("    - {name}");
        }
        errors += u32::try_from(orphaned.len()).unwrap_or(u32::MAX);

        if args.fix {
            println!("  Cleaning up orphaned tables...");
            for name in &orphaned {
                // Table names are from pg_tables (trusted), but quote anyway.
                let quoted = format!("\"{}\"", name.replace('"', "\"\""));
                let ddl = format!("DROP TABLE IF EXISTS {quoted}");
                match sqlx::query(&ddl).execute(&data_pool).await {
                    Ok(_) => println!("    Dropped: {name}"),
                    Err(e) => println!("    FAILED to drop {name}: {e}"),
                }
            }
        }
    }

    // Check 2: Missing data tables (in catalog but not in data DB).
    println!("--- Checking for missing data tables...");
    let missing: Vec<&String> = expected.difference(&actual).collect();
    if missing.is_empty() {
        println!("  OK: All catalog tables have backing data tables.");
    } else {
        println!("  FOUND: {} missing data table(s):", missing.len());
        for name in &missing {
            println!("    - {name}");
        }
        errors += u32::try_from(missing.len()).unwrap_or(u32::MAX);
    }

    // Check 3: Indexes without parent table in catalog.
    println!("--- Checking for orphaned index catalog entries...");
    let orphaned_indexes: Vec<(String,)> = sqlx::query_as(
        "SELECT i.index_name FROM indexes i \
         LEFT JOIN tables t ON i.table_id = t.table_id \
         WHERE t.table_id IS NULL",
    )
    .fetch_all(&catalog_pool)
    .await?;
    if orphaned_indexes.is_empty() {
        println!("  OK: No orphaned index catalog entries.");
    } else {
        println!(
            "  FOUND: {} orphaned index catalog entries:",
            orphaned_indexes.len()
        );
        for (name,) in &orphaned_indexes {
            println!("    - {name}");
        }
        errors += u32::try_from(orphaned_indexes.len()).unwrap_or(u32::MAX);
    }

    // Check 4: Tables stuck in transitional states.
    println!("--- Checking for stuck transitions...");
    let stuck: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_name, table_status FROM tables \
         WHERE table_status IN ('CREATING', 'DELETING') \
         AND status_transition_at < NOW() - INTERVAL '10 minutes'",
    )
    .fetch_all(&catalog_pool)
    .await?;
    if stuck.is_empty() {
        println!("  OK: No stuck transitions.");
    } else {
        println!("  FOUND: {} stuck table(s):", stuck.len());
        for (name, status) in &stuck {
            println!("    - {name} ({status})");
        }
        errors += u32::try_from(stuck.len()).unwrap_or(u32::MAX);
    }

    println!();
    if errors == 0 {
        println!("=== HEALTHY: All catalog checks passed ===");
    } else {
        println!("=== {errors} issue(s) found ===");
        if !args.fix {
            println!("Run with --fix to clean up orphaned data tables.");
        }
        std::process::exit(1);
    }

    Ok(())
}
