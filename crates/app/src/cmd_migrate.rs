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
    // Load the config for validation and to reach the catalog for the policy
    // rewrite: migrate drives the schema steps through the bootstrapper, but a
    // malformed config should fail here rather than midway through a migration.
    let app_config = config::load(&args.config)?;

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
    let result = apply_migrations(
        bootstrap.as_ref(),
        &args,
        app_config.storage.connection_config(),
    )
    .await;
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
    connection: &str,
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

    // Detect stored policies that still name a dynamodb:ResourceTag condition
    // key. Detection is read-only and runs on every migrate (like the repair
    // above), so an interrupted run is finished by the next. The catalog is
    // reached through a catalog-only management store, not the bootstrapper's
    // schema connection.
    println!("--- Checking resource-tag condition keys...");
    let mgmt = extenddb_storage::management_store_factory::create_management_store(connection)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let settings = extenddb_storage::settings_store::create_settings_store(connection)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let rtag_detect = extenddb_storage::policy_migration::migrate_resource_tag_condition_keys(
        mgmt.as_ref(),
        false,
    )
    .await;
    let marker_present = settings
        .get_setting(extenddb_core::settings_keys::RESOURCE_TAG_MIGRATION)
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?
        .is_some();
    if rtag_detect.rewritten.is_empty() && rtag_detect.needs_attention.is_empty() {
        println!("  No dynamodb:ResourceTag condition keys found.");
    } else {
        for r in &rtag_detect.rewritten {
            println!(
                "  Rewrite: account {} {} policy {} ({} key(s))",
                r.account_id, r.principal, r.policy_name, r.rewrites
            );
        }
        for n in &rtag_detect.needs_attention {
            println!(
                "  NEEDS ATTENTION: account {} {} policy {} (aws:ResourceTag and \
                 dynamodb:ResourceTag forms collide; left unchanged)",
                n.account_id, n.principal, n.policy_name
            );
        }
    }
    if rtag_detect.errors > 0 {
        println!(
            "  WARNING: {} policy lookup(s) failed during detection.",
            rtag_detect.errors
        );
    }
    let rtag_pending = !rtag_detect.rewritten.is_empty() || rtag_detect.errors > 0;
    // A catalog with no marker must have one written, even when there is
    // nothing to rewrite, so that serve will start.
    let marker_pending = !marker_present;

    if !catalog_pending
        && data_pending.is_empty()
        && !repair_pending
        && !rtag_pending
        && !marker_pending
    {
        println!();
        if detected.needs_attention.is_empty() && rtag_detect.needs_attention.is_empty() {
            println!(
                "Everything is up to date (catalog version {expected}). No migrations needed."
            );
        } else {
            // Nothing is automatically applicable, but saying "everything is up
            // to date" straight after a NEEDS ATTENTION line would be
            // contradictory: those items require a human.
            println!(
                "No migrations needed (catalog version {expected}), but {} item(s) above \
                 need manual attention.",
                detected.needs_attention.len() + rtag_detect.needs_attention.len()
            );
        }
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
        if repair_pending {
            what.push(format!(
                "table key metadata repairs [{}]",
                detected.repaired.join("; ")
            ));
        }
        if !rtag_detect.rewritten.is_empty() {
            what.push(format!(
                "resource-tag condition-key rewrites [{} policy(ies)]",
                rtag_detect.rewritten.len()
            ));
        }
        if marker_pending {
            what.push("record the resource-tag migration marker".to_owned());
        }
        if rtag_detect.errors > 0 {
            what.push(format!(
                "{} policy lookup(s) that failed and must be retried",
                rtag_detect.errors
            ));
        }
        if !rtag_detect.needs_attention.is_empty() {
            what.push(format!(
                "{} policy(ies) listed as NEEDS ATTENTION, which must be fixed by hand before \
                 the migration can complete",
                rtag_detect.needs_attention.len()
            ));
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

    // Rewrite stored policies from dynamodb:ResourceTag to aws:ResourceTag,
    // after the schema migrations. Every storage error is counted; on a
    // non-zero count migrate reports it, does NOT record the completion
    // marker, and exits non-zero, so the next run finishes the rewrite. No
    // policy body is logged.
    let rtag_applied = extenddb_storage::policy_migration::migrate_resource_tag_condition_keys(
        mgmt.as_ref(),
        true,
    )
    .await;
    for r in &rtag_applied.rewritten {
        println!(
            "  Rewrote resource-tag keys: account {} {} policy {} ({} key(s))",
            r.account_id, r.principal, r.policy_name, r.rewrites
        );
    }
    for n in &rtag_applied.needs_attention {
        println!(
            "  NEEDS ATTENTION: account {} {} policy {} left unchanged (collision)",
            n.account_id, n.principal, n.policy_name
        );
    }
    if rtag_applied.errors > 0 {
        anyhow::bail!(
            "resource-tag migration incomplete: {} policy lookup(s) failed. \
             Re-run `extenddb migrate --yes` to finish; the migration marker was not recorded.",
            rtag_applied.errors
        );
    }
    // A policy left unchanged because both resource-tag forms name the same
    // tag key in one operator block is not migrated. Recording the marker
    // would let serve evaluate it with the dynamodb: half absent, which can
    // turn a Deny off. Withhold the marker so serve keeps refusing until the
    // policy is fixed by hand and migrate is run again.
    if !rtag_applied.needs_attention.is_empty() {
        anyhow::bail!(
            "resource-tag migration incomplete: {} policy(ies) above name both \
             aws:ResourceTag/<key> and dynamodb:ResourceTag/<key> for the same key in one \
             condition operator and were left unchanged. Remove one of the two forms from \
             each, then re-run `extenddb migrate --yes`; the migration marker was not recorded.",
            rtag_applied.needs_attention.len()
        );
    }
    // Record the completion marker only after a clean rewrite. serve reads this
    // row and refuses to start without it.
    settings
        .set_setting(
            extenddb_core::settings_keys::RESOURCE_TAG_MIGRATION,
            extenddb_core::settings_keys::RESOURCE_TAG_MIGRATION_DONE,
        )
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
