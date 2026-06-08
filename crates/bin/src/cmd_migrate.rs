// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb migrate` — apply catalog schema migrations (REQ-CAT-014).
//!
//! Reads current catalog version, runs pending migrations, and reports the result.

use clap::Args;

use crate::config;

#[derive(Args)]
pub struct MigrateArgs {
    /// Path to configuration file
    #[arg(short, long, default_value = "extenddb.toml")]
    config: String,

    /// `PostgreSQL` admin user (for catalog migrations)
    #[arg(long)]
    pg_user: Option<String>,

    /// `PostgreSQL` admin password
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
    let app_config = config::load(&args.config)?;
    let backend = &app_config.storage.backend;

    println!("=== extenddb migrate ===");
    println!("Config:           {}", args.config);
    println!();

    // Collect CLI args for backend-specific parsing
    let cli_args: Vec<String> = std::env::args().collect();

    // Create bootstrapper via registry
    let bootstrap =
        extenddb_storage::bootstrapper::create_bootstrapper(backend, &args.config, &cli_args)
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    // Show current version.
    println!("--- Checking current catalog version...");
    let current = bootstrap
        .read_catalog_version()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    let current_display = current.as_deref().unwrap_or("none");
    println!("  Current version: {current_display}");

    let expected = bootstrap.expected_catalog_version();
    if current.as_deref() == Some(expected.as_str()) {
        println!();
        println!("Catalog is up to date (version {expected}). No migrations needed.");
        return Ok(());
    }

    if !args.yes {
        anyhow::bail!(
            "--yes is required to apply migrations. \
             Current version: {current_display}, target version: {expected}."
        );
    }

    bootstrap
        .run_catalog_migrations()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    // Read new version.
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
