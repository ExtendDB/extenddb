// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb migrate` on a backend whose catalog migrates through the shared
//! runner. Without `--yes` it reads the catalog status, taking no lock and
//! writing nothing, so it runs beside a server. `--yes` reads the status
//! again under the migration lock and applies; it refuses a store that is
//! not a catalog, which `extenddb init` creates. `--reconverge` reruns every
//! migration on a catalog a release before 1.0 wrote to, `--forget` clears
//! an unreleased migration, and `--list` prints the migrations without
//! opening the catalog.
//!
//! Exit status: 0 up to date, applied, or ahead by additive migrations only;
//! 1 an error or an uninitialized catalog; 3 migrations pending (the
//! preview); 4 the catalog has a breaking migration this binary does not
//! know; 5 a person must act; 6 the lock was not free within
//! `--lock-timeout`.

use std::time::Duration;

use extenddb_config as config;
use extenddb_storage::migrations::advice::{self, Advice, AdviceFor};
use extenddb_storage::migrations::{
    AdminRole, CatalogMigrations, CatalogStatus, Listed, MigrationError, MigrationId, RunKind,
    RunReport, Target, before_lock, exit, forget_locked, run_locked,
};

use crate::cmd_migrate::MigrateArgs;

/// Run `extenddb migrate` on `migrations`; returns the exit status.
pub(crate) async fn run(
    args: &MigrateArgs,
    migrations: &dyn CatalogMigrations,
    release: &str,
) -> anyhow::Result<u8> {
    if args.list {
        print_list(migrations);
        return Ok(exit::OK);
    }
    if args.lock_timeout.is_some() && !args.yes && args.forget.is_none() {
        anyhow::bail!(
            "--lock-timeout applies to --yes and --forget, which take the migration lock; the \
             preview takes none"
        );
    }
    let (app_config, config_file) = crate::util::command_config(args.config.as_deref())?;
    let location = app_config.storage.connection_config().to_owned();
    let shown = config::redact_password(&location);
    let target = Target {
        location,
        data_store: args.data_db.clone(),
        admin: admin_role_for(
            extenddb_storage::backend_name(),
            args,
            std::env::var_os("EXTENDDB_PG_PASSWORD"),
            std::env::var_os("EXTENDDB_CASSANDRA_PASSWORD"),
        )?,
        data_connection: None,
    };
    let advice = AdviceFor::new(
        release,
        shown.clone(),
        Some(migrations),
        &target,
        config_file.as_deref(),
    );
    let advice = advice.advice();

    println!("=== extenddb migrate ===");
    println!(
        "Config:           {}",
        config_file
            .as_deref()
            .unwrap_or("none (built-in defaults and EXTENDDB__ variables)")
    );
    println!(
        "Catalog:          {shown} ({})",
        extenddb_storage::backend_name().unwrap_or("unknown")
    );
    println!();

    let lock_timeout = Duration::from_secs(
        args.lock_timeout
            .unwrap_or(crate::cmd_migrate::DEFAULT_LOCK_TIMEOUT_SECS),
    );
    let outcome = if let Some(id) = args.forget {
        forget(migrations, &target, id, lock_timeout).await
    } else if args.yes {
        let kind = if args.reconverge {
            RunKind::Reconverge
        } else {
            RunKind::Migrate
        };
        apply(migrations, &target, release, kind, lock_timeout).await
    } else {
        preview(migrations, &target, &advice).await
    };
    Ok(outcome.unwrap_or_else(|error| {
        eprintln!("Error: {}", advice::error_message(&error, &advice));
        error.exit_code()
    }))
}

/// The admin role of the backend this binary serves: on Cassandra the role
/// the runner connects as instead of the config file's (`--cassandra-user`,
/// `--cassandra-pass` or `EXTENDDB_CASSANDRA_PASSWORD`); on every other
/// backend the PostgreSQL role of the pgvector attempt (`--pg-user`,
/// `--pg-pass` or `EXTENDDB_PG_PASSWORD`), which only PostgreSQL reads. The
/// other backend's flags are ignored, as they were before Cassandra moved
/// onto the runner.
fn admin_role_for(
    backend: Option<&str>,
    args: &MigrateArgs,
    pg_password_env: Option<std::ffi::OsString>,
    cassandra_password_env: Option<std::ffi::OsString>,
) -> anyhow::Result<Option<AdminRole>> {
    if backend == Some("cassandra") {
        admin_role(
            args.cassandra_user.as_deref(),
            args.cassandra_pass.as_deref(),
            ("EXTENDDB_CASSANDRA_PASSWORD", cassandra_password_env),
        )
    } else {
        admin_role(
            args.pg_user.as_deref(),
            args.pg_pass.as_deref(),
            ("EXTENDDB_PG_PASSWORD", pg_password_env),
        )
    }
}

/// The admin role a user flag names (`--pg-user` or `--cassandra-user`),
/// with its password flag or else the password variable `env_name` names;
/// `None` without the user flag.
fn admin_role(
    user: Option<&str>,
    password: Option<&str>,
    (env_name, password_env): (&str, Option<std::ffi::OsString>),
) -> anyhow::Result<Option<AdminRole>> {
    let Some(user) = user else {
        return Ok(None);
    };
    let password = match (password, password_env) {
        (Some(password), _) => Some(password.to_owned()),
        (None, Some(env)) => Some(
            env.into_string()
                .map_err(|_| anyhow::anyhow!("{env_name} must contain valid UTF-8"))?,
        ),
        (None, None) => None,
    };
    Ok(Some(AdminRole {
        user: user.to_owned(),
        password,
    }))
}

/// Every migration this binary carries, with its kind on this backend.
fn print_list(migrations: &dyn CatalogMigrations) {
    let backend = extenddb_storage::backend_name().unwrap_or("unknown");
    println!("Migrations this binary carries, with their kind on the {backend} backend:");
    let width = MigrationId::ALL
        .iter()
        .map(|id| id.name().len())
        .max()
        .unwrap_or_default();
    for id in MigrationId::ALL {
        let entry = match migrations.listed(id) {
            Listed::Applies {
                kind,
                convergent,
                stores,
            } => {
                let stores: Vec<&str> = stores.iter().map(|s| s.as_str()).collect();
                format!(
                    "{:<9} {:<13} {}",
                    kind.as_str(),
                    if convergent {
                        "convergent"
                    } else {
                        "not convergent"
                    },
                    stores.join(", ")
                )
            }
            Listed::NotApplicable(reason) => format!("not applicable: {reason}"),
        };
        println!(
            "  {:>2} {:<width$}  {entry}\n       {}",
            id.number(),
            id.name(),
            id.description()
        );
    }
}

/// The status, with nothing locked or written.
async fn preview(
    migrations: &dyn CatalogMigrations,
    target: &Target,
    advice: &Advice<'_>,
) -> Result<u8, MigrationError> {
    let status = migrations.status(target).await?;
    for note in migrations.status_notes(target).await? {
        println!("Note: {}", note.text);
    }
    let latest = MigrationId::LATEST.number();
    match &status {
        CatalogStatus::Current { version } => {
            println!(
                "Up to date: the catalog is at migration {version}, the latest this binary \
                 carries. No migrations needed."
            );
        }
        CatalogStatus::AheadCompatible { .. } => {
            println!("WARNING: {status}. Nothing to apply.");
        }
        CatalogStatus::Behind { version, pending } => {
            if version.is_none() {
                println!(
                    "Stop every server that uses this catalog before applying: a server of a \
                     release before 1.0 is not detected while it runs."
                );
            }
            match version {
                Some(v) => {
                    println!("The catalog is at migration {v}; this binary carries {latest}.")
                }
                None => println!(
                    "The catalog has no migration ledger (a release before 1.0 created it); this \
                     binary carries migrations 1 to {latest}."
                ),
            }
            println!("Pending:");
            for p in pending {
                let description =
                    MigrationId::from_number(p.id).map_or("", MigrationId::description);
                println!(
                    "  {} {} ({}, {}): {description}",
                    p.id,
                    p.name,
                    p.store.as_str(),
                    p.kind.as_str()
                );
            }
            println!();
            println!(
                "Nothing was changed; {}, then run again with --yes to apply (exit status {}: \
                 migrations pending).",
                advice.backup,
                exit::PENDING
            );
        }
        _ => {
            eprintln!("Error: {}", advice::status_message(&status, advice));
        }
    }
    Ok(status.exit_code())
}

/// Take the lock, run, and print what the run did.
async fn apply(
    migrations: &dyn CatalogMigrations,
    target: &Target,
    release: &str,
    kind: RunKind,
    lock_timeout: Duration,
) -> Result<u8, MigrationError> {
    for note in before_lock(migrations, target, kind).await? {
        println!("Note: {}", note.text);
    }
    println!(
        "--- Taking the migration lock (waiting up to {} s for a server or another migration)...",
        lock_timeout.as_secs()
    );
    let lock = migrations.lock(target, lock_timeout).await?;
    let report = run_locked(migrations, target, &lock, release, kind).await;
    migrations.release(lock).await;
    let report = report?;
    print_report(&report, kind);
    if report.applied.is_empty() && kind == RunKind::Migrate {
        // A catalog ahead by additive migrations has nothing to apply and
        // serves; the status says which.
        if let Ok(status @ CatalogStatus::AheadCompatible { .. }) = migrations.status(target).await
        {
            println!("WARNING: {status}.");
        }
    }
    Ok(exit::OK)
}

fn print_report(report: &RunReport, kind: RunKind) {
    for note in &report.notes {
        println!("  {note}");
    }
    let verb = match kind {
        RunKind::Create | RunKind::Migrate => "Applied",
        RunKind::Reconverge => "Ran again",
    };
    crate::util::print_applied(report, "  ", verb);
    let show =
        |v: Option<u32>| v.map_or_else(|| "no ledger".to_owned(), |v| format!("migration {v}"));
    println!();
    if report.applied.is_empty() {
        println!(
            "Up to date: the catalog is at {}. No migrations needed.",
            show(report.to)
        );
    } else {
        println!("=== extenddb migrate complete ===");
        println!("Catalog: {} -> {}", show(report.from), show(report.to));
    }
}

/// Clear an unreleased migration's rows under the lock. A store that is not
/// a catalog is refused before anything opens it to write.
async fn forget(
    migrations: &dyn CatalogMigrations,
    target: &Target,
    id: u32,
    lock_timeout: Duration,
) -> Result<u8, MigrationError> {
    let lock = migrations.lock(target, lock_timeout).await?;
    let forgotten = forget_locked(migrations, target, &lock, id).await;
    migrations.release(lock).await;
    forgotten?;
    println!(
        "Forgot migration {id}: its ledger rows are deleted, and the next \
         `extenddb migrate --yes` runs it again."
    );
    Ok(exit::OK)
}

#[cfg(test)]
#[path = "cmd_migrate_runner_tests.rs"]
mod tests;
