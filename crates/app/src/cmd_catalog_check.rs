// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb catalog-check`: integrity check for the catalog and data databases.
//!
//! Detects data relations no catalog row names, catalog rows whose data
//! relations are gone, and catalog inconsistencies. Refuses to run while the
//! server is up (PID file check).
//!
//! The data relations are named by the storage backend from catalog ids, so
//! checks 1 and 2 run through the backend's `DiagnosticsStore`, which
//! computes the expected names with the same functions the engine uses to
//! create them. Checks 3 and 4 read the catalog with raw `sqlx` queries, as
//! the command always has; they need direct access to cross-reference catalog
//! rows against each other.

use clap::Args;
use sqlx::postgres::PgPoolOptions;

use extenddb_config as config;
use extenddb_storage::diagnostics::{DiagError, OrphanDrop};

#[derive(Args)]
pub struct CatalogCheckArgs {
    /// Path to configuration file
    #[arg(short, long, default_value = "extenddb.toml")]
    config: String,

    /// Clean up orphaned tables (default: report only)
    #[arg(long)]
    fix: bool,
}

// A linear sequence of 4 independent checks. Splitting it into helpers would
// scatter the logic without improving readability, and each helper would need
// the same pool and fix parameters threaded through.
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
    let pid_path = extenddb_config::pid_file_path(&run_dir, port);
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

    // The backend's own view of its data relations. It connects to the data
    // database the catalog names, and refuses a catalog that names none, as
    // this command always has.
    let diag_store = extenddb_storage::diagnostics_store::create_diagnostics_store(
        app_config.storage.connection_config(),
    )
    .await
    .map_err(|e| anyhow::anyhow!("Cannot open the storage backend's diagnostics: {e}"))?;
    let relations = diag_store
        .check_storage_relations()
        .await
        .map_err(|e| match e {
            DiagError::NotConfigured(msg) => anyhow::anyhow!("{msg}"),
            e => anyhow::anyhow!("Cannot compare data relations with the catalog: {e}"),
        })?;
    println!("Compared data relations with the catalog.");
    println!();

    // Check 1: Orphaned data tables (in data DB but no catalog row names them).
    println!("--- Checking for orphaned data tables...");
    if relations.orphaned.is_empty() {
        println!("  OK: No orphaned tables.");
    } else {
        println!("  FOUND: {} orphaned table(s):", relations.orphaned.len());
        for name in &relations.orphaned {
            println!("    - {name}");
        }
        errors += u32::try_from(relations.orphaned.len()).unwrap_or(u32::MAX);

        if args.fix {
            println!("  Cleaning up orphaned tables...");
            // The backend waits for the index changes in flight, checks each
            // relation against the catalog again, and drops each one that no
            // row names with a short lock wait of its own.
            match diag_store
                .drop_orphaned_relations(relations.orphaned.clone())
                .await
            {
                Ok(outcomes) => {
                    for (name, outcome) in &outcomes {
                        match outcome {
                            OrphanDrop::Dropped => println!("    Dropped: {name}"),
                            OrphanDrop::AlreadyGone => println!(
                                "    Already gone: {name} (another session dropped it first)"
                            ),
                            OrphanDrop::Kept(reason) => println!("    Kept: {name} ({reason})"),
                            OrphanDrop::Failed(e) => println!("    FAILED to drop {name}: {e}"),
                        }
                    }
                }
                Err(e) => println!("  FAILED to clean up orphaned tables: {e}"),
            }
        }
    }

    // Check 2: Missing data tables (a catalog row names them, data DB lacks them).
    println!("--- Checking for missing data tables...");
    if relations.missing.is_empty() {
        println!("  OK: All catalog tables have backing data tables.");
    } else {
        println!(
            "  FOUND: {} missing data table(s):",
            relations.missing.len()
        );
        for entry in &relations.missing {
            println!("    - {} ({})", entry.relation, entry.owner);
        }
        errors += u32::try_from(relations.missing.len()).unwrap_or(u32::MAX);
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

/// The data database connection string that the row `stored` holds: the
/// row as it is in a catalog without a key ring; with a ring, the row
/// decrypted under a DEK of the ring, which the KEK of `[auth.encryption]
/// key_file` opens (RFC-0000 section 12). A row that no DEK opens is
/// refused, plaintext included, as every reader but `switch` refuses it.
async fn data_connection_text(
    catalog: &sqlx::PgPool,
    app_config: &config::AppConfig,
    stored: String,
) -> anyhow::Result<zeroize::Zeroizing<String>> {
    use extenddb_storage::key_ring::{KEY_RING_SETTING, open_ring};
    use extenddb_storage::secret_keys::CATALOG_ID_SETTING;
    use extenddb_storage::server_components::KeySource;

    let setting = async |key: &str| -> anyhow::Result<Option<String>> {
        let row: Option<(String,)> = sqlx::query_as("SELECT value FROM settings WHERE key = $1")
            .bind(key)
            .fetch_optional(catalog)
            .await?;
        Ok(row.map(|(value,)| value))
    };
    let Some(ring) = setting(KEY_RING_SETTING).await? else {
        return Ok(zeroize::Zeroizing::new(stored));
    };
    let KeySource::Kek(kek) = extenddb_server::key_file::key_source(&app_config.auth.encryption)
        .map_err(anyhow::Error::msg)?
    else {
        anyhow::bail!(
            "The catalog has a key ring, so its data database connection string is encrypted. \
             Set [auth.encryption] key_file to the KEK file that the ring names"
        );
    };
    let catalog_id = setting(CATALOG_ID_SETTING).await?;
    let open = open_ring(&ring, catalog_id.as_deref(), &kek).map_err(|e| anyhow::anyhow!("{e}"))?;
    open.deks
        .iter()
        .find_map(|dek| dek.decrypt_setting(&stored).ok())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No DEK of the key ring opens the data_database_connection_string row. Run \
                 `extenddb encryption-key status`"
            )
        })
}
