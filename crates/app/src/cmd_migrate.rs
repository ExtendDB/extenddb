// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb migrate` — apply catalog schema migrations (REQ-CAT-014).
//!
//! Reads current catalog version, runs pending migrations, and reports the result.

use clap::Args;

use extenddb_config as config;

#[derive(Args)]
pub struct MigrateArgs {
    /// Path to configuration file
    #[arg(short, long, default_value = "extenddb.toml")]
    config: String,

    /// `PostgreSQL` admin user (for catalog migrations)
    #[arg(long)]
    pg_user: Option<String>,

    /// `PostgreSQL` admin password.
    /// Prefer `EXTENDDB_PG_PASSWORD` to keep the value out of process arguments.
    #[arg(long)]
    pg_pass: Option<String>,

    /// Confirm migration (required, no interactive prompt)
    #[arg(long)]
    yes: bool,
}

pub async fn run(args: MigrateArgs) -> anyhow::Result<()> {
    if !std::path::Path::new(&args.config).exists() {
        anyhow::bail!(
            "Config file '{}' not found. Run 'extenddb init' to set up a deployment, \
             or use --config <path> to specify a different location.",
            args.config,
        );
    }
    // Load the config for validation only: migrate drives the bootstrapper from
    // the config path directly, but a malformed config should fail here rather
    // than midway through a migration.
    config::load(&args.config)?;

    println!("=== extenddb migrate ===");
    println!("Config:           {}", args.config);
    println!();

    // Collect CLI args for backend-specific parsing. The environment-sourced
    // secret is appended only to this in-process copy, not OS-visible argv.
    let mut cli_args: Vec<String> = std::env::args().collect();
    crate::util::append_secret_arg(
        &mut cli_args,
        "--pg-pass",
        std::env::var_os("EXTENDDB_PG_PASSWORD"),
        "EXTENDDB_PG_PASSWORD",
    )?;

    // Create bootstrapper via registry
    let bootstrap = extenddb_storage::bootstrapper::create_bootstrapper(&args.config, &cli_args)
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    // Serialize concurrent migrators, such as several replicas starting at
    // once, so they don't race each other applying schema changes.
    bootstrap
        .acquire_migration_lock()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    let result = apply_migrations(bootstrap.as_ref(), &args).await;
    if let Err(e) = bootstrap.release_migration_lock().await {
        tracing::warn!("Failed to release migration lock: {e:?}");
    }
    result
}

/// Run the version checks and apply pending migrations while the migration lock
/// is held. Split out of `run` so that the lock is always released afterwards.
async fn apply_migrations(
    bootstrap: &dyn extenddb_storage::bootstrapper::Bootstrapper,
    args: &MigrateArgs,
) -> anyhow::Result<()> {
    // Show current catalog version.
    println!("--- Checking current catalog version...");
    let current = bootstrap
        .read_catalog_version()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    let current_display = current.as_deref().unwrap_or("none");
    println!("  Current version: {current_display}");

    let expected = bootstrap.expected_catalog_version();
    let catalog_pending = current.as_deref() != Some(expected.as_str());

    // Data migrations are tracked in the data database's own ledger, separate
    // from and independent of the catalog version, so they must be checked (and
    // applied) on their own — a release that only changes the data schema does
    // not bump the catalog version.
    println!("--- Checking data migrations...");
    let data_pending = bootstrap
        .pending_data_migrations()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    if data_pending.is_empty() {
        println!("  Data migrations up to date.");
    } else {
        println!("  Pending: {}", data_pending.join(", "));
    }

    if !catalog_pending && data_pending.is_empty() {
        println!();
        println!("Everything is up to date (catalog version {expected}). No migrations needed.");
        return Ok(());
    }

    if !args.yes {
        let mut what = Vec::new();
        if catalog_pending {
            what.push(format!("catalog {current_display} -> {expected}"));
        }
        if !data_pending.is_empty() {
            what.push(format!("data migrations [{}]", data_pending.join(", ")));
        }
        anyhow::bail!(
            "--yes is required to apply migrations. Pending: {}.",
            what.join("; ")
        );
    }

    if catalog_pending {
        bootstrap
            .run_catalog_migrations()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    }

    // Always run data migrations: the data-database ledger is idempotent
    // (already-applied migrations are skipped) and independent of the catalog
    // version, so an upgrade that only touched the data schema is still applied.
    bootstrap
        .run_data_migrations()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    // Read new catalog version.
    let new = bootstrap
        .read_catalog_version()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    let new_display = new.as_deref().unwrap_or("none");

    println!();
    println!("=== extenddb migrate complete ===");
    println!("Catalog version: {current_display} -> {new_display}");

    Ok(())
}
