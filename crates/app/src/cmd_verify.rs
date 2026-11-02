// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb verify` — validate a extenddb deployment (REQ-CAT-013).
//!
//! Connects to catalog, checks version, enumerates tables and indexes,
//! connects to data database, reports healthy/unhealthy.
//!
//! On a backend on the migration runner the version check is the catalog
//! status, read without a lock or a write. `verify` exits with the status's
//! code 4 (ahead by a breaking migration) or 5 (a person must act) before
//! the 1 of a failed check, and with that 1 before the 3 of pending
//! migrations, which `migrate --yes` cannot apply while a check fails.
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

    /// Print every note the migration ledger records. Without it, verify
    /// prints the number of notes of each migration and its first 5
    #[arg(long)]
    all_notes: bool,
}

/// How many notes of each migration `verify` prints without `--all-notes`.
const NOTES_SHOWN: usize = 5;

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
            let code = check_migrations(
                migrations,
                config_file.as_deref(),
                &app_config,
                release,
                args.all_notes,
            )
            .await;
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
    let code = exit_code(status_code.unwrap_or(0), errors);
    if code == 0 {
        println!("=== HEALTHY: All checks passed ===");
    } else if errors > 0 {
        println!("=== UNHEALTHY: {errors} check(s) failed ===");
    } else {
        println!("=== UNHEALTHY: the catalog needs the action above ===");
    }
    Ok(code)
}

/// The exit status of a `verify` whose catalog status exits `status_code`
/// and in which `failed` checks failed: the status's 4 or 5, which ask a
/// person to act whatever the checks found, else 1 when a check failed,
/// else the status's code. A failed check comes before the 3 of pending
/// migrations because `migrate --yes` cannot apply them while the check
/// fails, for example while the data database cannot be reached.
fn exit_code(status_code: u8, failed: u32) -> u8 {
    use extenddb_storage::migrations::exit;
    match status_code {
        exit::AHEAD_INCOMPATIBLE | exit::ATTENTION => status_code,
        _ if failed > 0 => exit::ERROR,
        _ => status_code,
    }
}

/// The lines `verify` prints for the notes the catalog ledger records: per
/// migration with notes, how many it has and the first [`NOTES_SHOWN`], or
/// every note with `all`.
fn ledger_note_lines(rows: &[extenddb_storage::migrations::LedgerRow], all: bool) -> Vec<String> {
    let mut lines = Vec::new();
    for row in rows.iter().filter(|r| !r.notes.is_empty()) {
        let notes: Vec<&str> = row.notes.lines().collect();
        let count = notes.len();
        let shown = if all { count } else { count.min(NOTES_SHOWN) };
        lines.push(format!(
            "  Notes of migration {} {} ({count} note{}):",
            row.id,
            row.name,
            if count == 1 { "" } else { "s" }
        ));
        lines.extend(notes.iter().take(shown).map(|note| format!("    {note}")));
        if shown < count {
            lines.push(format!(
                "    and {} more; verify --all-notes prints every note",
                count - shown
            ));
        }
    }
    lines
}

/// Print the catalog status, what its pending migrations change, and the
/// notes the ledger records (each migration's first [`NOTES_SHOWN`], or
/// every note with `all_notes`), and return the status's exit code.
async fn check_migrations(
    migrations: &dyn extenddb_storage::migrations::CatalogMigrations,
    config_file: Option<&str>,
    app_config: &config::AppConfig,
    release: &str,
    all_notes: bool,
) -> u8 {
    use extenddb_storage::migrations::advice::{self, AdviceFor};
    use extenddb_storage::migrations::{CatalogStatus, Target};

    println!("--- Checking catalog migrations...");
    let location = app_config.storage.connection_config();
    let target = Target::at(location);
    // The backup clause names the data store the catalog records.
    let named = Target {
        data_store: migrations.data_store_name(&target).await,
        ..target.clone()
    };
    let advice = AdviceFor::new(
        release,
        config::redact_password(location),
        Some(migrations),
        &named,
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
    if matches!(status, CatalogStatus::Behind { .. }) {
        match migrations.pending_changes(&target, &status).await {
            Ok(lines) => {
                for line in lines {
                    println!("  {line}");
                }
            }
            Err(e) => println!("  WARN: {}", advice::error_message(&e, &advice)),
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
        for line in ledger_note_lines(&rows, all_notes) {
            println!("{line}");
        }
    }
    status.exit_code()
}

#[cfg(test)]
mod tests {
    use extenddb_storage::migrations::{Kind, LedgerRow, Outcome, exit};

    use super::{exit_code, ledger_note_lines};

    fn row(id: u32, notes: usize) -> LedgerRow {
        LedgerRow {
            id,
            name: format!("m{id}"),
            kind: Kind::Breaking,
            checksum: "c".to_owned(),
            outcome: Outcome::Applied,
            notes: (0..notes)
                .map(|i| format!("note {i}"))
                .collect::<Vec<_>>()
                .join("\n"),
            applied_at: String::new(),
            applied_by: "1.0.0".to_owned(),
        }
    }

    /// Each migration's note count and first 5 notes; `--all-notes` prints
    /// every one.
    #[test]
    fn the_ledger_notes_are_counted_and_the_first_five_printed() {
        let rows = [row(1, 0), row(2, 7), row(4, 1)];
        assert_eq!(
            ledger_note_lines(&rows, false),
            [
                "  Notes of migration 2 m2 (7 notes):",
                "    note 0",
                "    note 1",
                "    note 2",
                "    note 3",
                "    note 4",
                "    and 2 more; verify --all-notes prints every note",
                "  Notes of migration 4 m4 (1 note):",
                "    note 0",
            ]
        );
        let all = ledger_note_lines(&rows, true);
        assert_eq!(all.len(), 1 + 7 + 1 + 1, "{all:?}");
        assert_eq!(all[7], "    note 6");
        assert!(all.iter().all(|l| !l.contains("more")), "{all:?}");
        let five = ledger_note_lines(&[row(3, 5)], false);
        assert_eq!(five.len(), 6, "{five:?}");
    }

    /// A failed check, such as an unreachable data database, exits 1 before
    /// the 3 of pending migrations; 4 and 5 still ask a person to act.
    #[test]
    fn a_failed_check_exits_one_before_a_pending_status() {
        assert_eq!(exit_code(exit::PENDING, 1), exit::ERROR);
        assert_eq!(exit_code(exit::PENDING, 0), exit::PENDING);
        assert_eq!(exit_code(exit::OK, 1), exit::ERROR);
        assert_eq!(exit_code(exit::OK, 0), exit::OK);
        for code in [exit::AHEAD_INCOMPATIBLE, exit::ATTENTION] {
            assert_eq!(exit_code(code, 1), code);
            assert_eq!(exit_code(code, 0), code);
        }
    }
}
