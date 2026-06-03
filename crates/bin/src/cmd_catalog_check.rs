// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb catalog-check` — backend-owned catalog/data integrity checks.
//!
//! The CLI handles config loading, server-liveness protection, and reporting.
//! Physical integrity rules live in backend crates because PostgreSQL companion
//! tables and TiDB native online-DDL artifacts have different invariants.

use clap::Args;
use extenddb_storage::operations::{CatalogCheckFix, CatalogCheckIssue, CatalogCheckReport};

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

pub async fn run(args: CatalogCheckArgs) -> anyhow::Result<()> {
    if !std::path::Path::new(&args.config).exists() {
        anyhow::bail!(
            "Config file '{}' not found. Run 'extenddb init' to set up a deployment, \
             or use --config <path> to specify a different location.",
            args.config,
        );
    }
    let app_config = config::load(&args.config)?;
    let backend = &app_config.storage._backend;
    let port = app_config.server.port;
    let run_dir = config::expand_tilde(&app_config.server.run_dir);

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
    println!("Backend: {backend}");
    println!();

    let operations = extenddb_storage::operations::get_operations_engine(backend)
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    let report = operations
        .catalog_check(app_config.storage.connection_config(), args.fix)
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    print_report(&report, args.fix);

    let errors = report.issue_count();
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

fn print_report(report: &CatalogCheckReport, fix: bool) {
    for section in &report.sections {
        println!("--- {}...", section.title);
        if section.issues.is_empty() {
            println!("  OK: {}", section.ok_message);
            continue;
        }

        println!("  FOUND: {} issue(s):", section.issues.len());
        for issue in &section.issues {
            print_issue(issue, fix);
        }
    }
}

fn print_issue(issue: &CatalogCheckIssue, fix: bool) {
    match (&issue.detail, &issue.fix) {
        (Some(detail), Some(CatalogCheckFix::Applied(action))) if fix => {
            println!("    - {} ({detail}) [{action}]", issue.name);
        }
        (None, Some(CatalogCheckFix::Applied(action))) if fix => {
            println!("    - {} [{action}]", issue.name);
        }
        (Some(detail), Some(CatalogCheckFix::Failed(error))) if fix => {
            println!("    - {} ({detail}) [fix failed: {error}]", issue.name);
        }
        (None, Some(CatalogCheckFix::Failed(error))) if fix => {
            println!("    - {} [fix failed: {error}]", issue.name);
        }
        (Some(detail), _) => println!("    - {} ({detail})", issue.name),
        (None, _) => println!("    - {}", issue.name),
    }
}
