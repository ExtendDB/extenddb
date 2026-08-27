// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb migrate` — apply catalog schema migrations (REQ-CAT-014).
//!
//! Reads the current catalog version, runs both migrators (which validate the
//! checksums of already-applied migrations), and reports the result.

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
    // ADR-0003: a catalog created by the pre-sqlx runner cannot be upgraded in
    // place. Refuse with the re-init directive instead of failing later on a
    // non-idempotent DDL re-run.
    if bootstrap
        .catalog_predates_sqlx()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?
    {
        anyhow::bail!(
            "This catalog predates the sqlx migration system (ADR-0003). In-place \
             upgrade is not supported. Run 'extenddb destroy' then 'extenddb init' \
             to recreate both databases (this drops all data). See \
             docs/manuals/07-upgrade-manual.md."
        );
    }

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

    // Detect table metadata damaged by the pre-fix UpdateTable (#259), which
    // deleted a table's own key attribute definitions when a GSI was added.
    //
    // Detection is read-only and runs even when the catalog version is already
    // current: the damage is in a row's contents, not in the schema, so an
    // up-to-date deployment is exactly the case that needs checking. Nothing is
    // written until --yes has been given, below.
    println!("--- Checking table key metadata...");
    let detected = bootstrap
        .repair_lost_sort_key_definitions(false)
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    let repair_pending = !detected.repaired.is_empty();
    if !repair_pending && detected.needs_attention.is_empty() {
        println!("  Table key metadata intact.");
    } else {
        for line in &detected.repaired {
            println!("  Repairable: {line}");
        }
        for line in &detected.needs_attention {
            println!("  NEEDS ATTENTION: {line}");
        }
    }

    let has_pending = catalog_pending || !data_pending.is_empty() || repair_pending;

    if has_pending && !args.yes {
        let mut what = Vec::new();
        if catalog_pending {
            what.push(format!("catalog {current_display} -> {expected}"));
        }
        if !data_pending.is_empty() {
            what.push(format!("data migrations [{}]", data_pending.join(", ")));
        }
        if repair_pending {
            what.push(format!(
                "table key metadata repairs [{}]",
                detected.repaired.join("; ")
            ));
        }
        anyhow::bail!(
            "--yes is required to apply migrations. Pending: {}.",
            what.join("; ")
        );
    }

    // Run both migrators unconditionally, even when nothing is pending. sqlx
    // validates the checksum of every already-applied migration on each run, so
    // a migration file edited after it shipped is caught loudly here instead of
    // drifting silently. Applying is idempotent; an up-to-date run applies
    // nothing.
    bootstrap
        .run_catalog_migrations()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    bootstrap
        .run_data_migrations()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;

    // Apply the table key metadata repair after the schema migrations, so it never
    // reads or writes a pre-migration catalog shape.
    if repair_pending {
        let applied = bootstrap
            .repair_lost_sort_key_definitions(true)
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        for line in &applied.repaired {
            println!("  Restored sort key definition: {line}");
        }
    }

    // Read new catalog version.
    let new = bootstrap
        .read_catalog_version()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    let new_display = new.as_deref().unwrap_or("none");

    println!();
    if has_pending {
        println!("=== extenddb migrate complete ===");
        println!("Catalog version: {current_display} -> {new_display}");
    } else if detected.needs_attention.is_empty() {
        println!("Everything is up to date (catalog version {expected}). No migrations applied.");
    } else {
        // Nothing is automatically applicable, but saying "everything is up
        // to date" straight after a NEEDS ATTENTION line would be
        // contradictory: those tables require a human.
        println!(
            "No migrations applied (catalog version {expected}), but {} item(s) above \
             need manual attention.",
            detected.needs_attention.len()
        );
    }

    Ok(())
}
