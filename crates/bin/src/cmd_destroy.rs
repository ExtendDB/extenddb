// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb destroy` — tear down a extenddb deployment (REQ-CAT-012).
//!
//! Reads config, enumerates tables, requires `--yes` to confirm, drops both databases.

use clap::Args;
use extenddb_storage::bootstrapper::BootstrapOptions;

use crate::config;

#[derive(Args)]
#[allow(clippy::doc_markdown)] // Clap help text, not rustdoc
pub struct DestroyArgs {
    /// Path to configuration file
    #[arg(short, long, default_value = "extenddb.toml")]
    config: String,

    /// Storage admin user (for DROP DATABASE)
    #[arg(long = "storage-admin-user")]
    storage_admin_user: Option<String>,

    /// Storage admin password
    #[arg(long = "storage-admin-password")]
    storage_admin_password: Option<String>,

    /// Confirm destruction (required, no interactive prompt)
    #[arg(long)]
    yes: bool,
}

pub async fn run(args: DestroyArgs) -> anyhow::Result<()> {
    if !std::path::Path::new(&args.config).exists() {
        anyhow::bail!(
            "Config file '{}' not found. Nothing to destroy, or use --config <path> \
             to specify a different location.",
            args.config,
        );
    }
    let app_config = config::load(&args.config)?;
    let backend = &app_config.storage._backend;

    let bootstrap_options = BootstrapOptions {
        admin_user: args.storage_admin_user.clone(),
        admin_password: args.storage_admin_password.clone(),
        ..BootstrapOptions::default()
    };

    println!("=== extenddb destroy ===");
    println!("Config:           {}", args.config);
    println!();

    // Create bootstrap store for catalog queries and database teardown.
    let bootstrap = extenddb_storage::bootstrapper::create_bootstrapper(
        backend,
        &args.config,
        bootstrap_options.clone(),
    )
    .await;

    let mut data_db = String::new();

    if let Ok(ref bootstrap) = bootstrap {
        let catalog_db = bootstrap.catalog_database_name();
        let endpoint = bootstrap.endpoint_info();
        println!("Catalog database: {catalog_db}");
        println!("{backend}:         {endpoint}");
        println!();

        println!("--- Tables in catalog:");
        let tables = bootstrap.list_table_names().await.unwrap_or_default();
        if tables.is_empty() {
            println!("  (none)");
        } else {
            for name in &tables {
                println!("  {name}");
            }
        }

        // Get data database name.
        if let Ok(Some(db)) = bootstrap.get_data_db_name().await {
            data_db = db;
            println!();
            println!("Data database:    {data_db}");
        }
    } else {
        println!("--- (could not connect to catalog)");
    }

    println!();
    println!("WARNING: This will permanently destroy ALL data in both databases.");
    println!();

    if !args.yes {
        anyhow::bail!(
            "--yes is required to confirm destruction. This will permanently destroy \
             ALL data in both databases."
        );
    }

    // For drop, we need a fresh bootstrap store connected as admin (not to the
    // catalog DB we're about to drop).
    if !data_db.is_empty() {
        // Defense-in-depth: validate even though this came from the catalog.
        config::validate_identifier(backend, &data_db, "data database name")?;
    }

    // Reconnect as admin for DDL operations (the catalog pool must be dropped
    // before we can DROP DATABASE).
    drop(bootstrap);
    let bootstrap = extenddb_storage::bootstrapper::create_bootstrapper(
        backend,
        &args.config,
        bootstrap_options,
    )
    .await
    .map_err(|e| anyhow::anyhow!("Cannot connect as admin: {e:?}"))?;

    bootstrap
        .drop_databases(&data_db)
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    println!();
    println!("=== extenddb destroy complete ===");
    Ok(())
}
