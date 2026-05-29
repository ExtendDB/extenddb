// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb migrate` — apply catalog schema migrations (REQ-CAT-014).
//!
//! Reads current catalog version, runs pending migrations, and reports the result.

use clap::Args;
use extenddb_storage::bootstrapper::BootstrapOptions;

use crate::config;

#[derive(Args)]
pub struct MigrateArgs {
    /// Path to configuration file
    #[arg(short, long, default_value = "extenddb.toml")]
    config: String,

    /// Storage admin user (for catalog migrations)
    #[arg(long = "storage-admin-user")]
    storage_admin_user: Option<String>,

    /// Storage admin password
    #[arg(long = "storage-admin-password")]
    storage_admin_password: Option<String>,

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
    let backend = &app_config.storage._backend;

    println!("=== extenddb migrate ===");
    println!("Config:           {}", args.config);
    println!();

    // Create bootstrapper via registry
    let bootstrap = extenddb_storage::bootstrapper::create_bootstrapper(
        backend,
        &args.config,
        BootstrapOptions {
            admin_user: args.storage_admin_user.clone(),
            admin_password: args.storage_admin_password.clone(),
            ..BootstrapOptions::default()
        },
    )
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
    let catalog_version_matches = current.as_deref() == Some(expected.as_str());
    if catalog_version_matches && !args.yes {
        println!();
        println!(
            "Catalog version is current ({expected}). Use --yes to check and apply backend schema migrations."
        );
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
    bootstrap
        .run_data_migrations()
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
