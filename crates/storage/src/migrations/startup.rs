// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! What `serve` does with the catalog before it opens it: the startup
//! policy, `[server] catalog_upgrade`, which `serve --migrate` sets to
//! `apply` for one run.
//!
//! The status is read with no lock. `Current` serves, and so does
//! `AheadCompatible`, with a warning. Under `apply` a catalog behind this
//! binary is migrated first, by the run `migrate --yes` makes
//! ([`run_with_lock`]): the migration lock, which waits for another
//! migration and for running servers, the status read again under it, so a
//! catalog another process migrated meanwhile is left as it is, `detect`
//! for every pending code body before the first write, the SQLite copy, the
//! runner, and the release. Under `refuse`, and under `apply` for every
//! other status, the server stops with the status's exit code: no setting
//! serves a catalog this binary cannot use.
//!
//! A dev build serves a store that is not a catalog yet, which its server
//! creates. A store that exists only inside the server (an in-memory SQLite
//! database) is created by the server when it starts and is not read here.

use std::time::Duration;

use tokio::time::Instant;

use super::advice::pending_line;
use super::catalog::{CatalogMigrations, RunKind, Target, run_with_lock};
use super::registry::MigrationId;
use super::status::{CatalogStatus, MigrationError, RunReport};

/// How a server starts on the catalog.
#[derive(Debug, Clone, Copy)]
pub struct Startup<'a> {
    /// Apply the migrations of a catalog behind this binary instead of
    /// refusing it (`catalog_upgrade = "apply"`).
    pub apply: bool,
    /// Leave a store that is not a catalog yet to the server, which creates
    /// it: a dev build, which needs no `init`.
    pub creates_uninitialized: bool,
    /// The ExtendDB release, recorded in the ledger rows the run writes.
    pub release: &'a str,
    /// How long the run waits for the migration lock and for running
    /// servers before it stops with exit 6.
    pub lock_timeout: Duration,
    /// How often a line says that the run is still going. Greater than
    /// zero.
    pub heartbeat: Duration,
}

impl Startup<'_> {
    /// How often a server says that its run is still going: well inside
    /// the 15 s the npm launcher waits after the last such line.
    pub const HEARTBEAT: Duration = Duration::from_secs(5);
}

/// What [`prepare_to_serve`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Prepared {
    /// What the run under the migration lock did, or `None` when no run was
    /// made. A report with no step applied means another process applied
    /// the migrations while this one waited for the lock.
    pub run: Option<RunReport>,
    /// For the operator: the catalog is ahead of this binary by additive
    /// migrations only, which this binary serves.
    pub warning: Option<String>,
}

/// Read the status of the catalog at `target` and decide whether a server
/// may open it, applying its pending migrations first when `startup` says
/// so. `progress` receives a line for each step of a run, and one every
/// [`Startup::heartbeat`] while it goes on, which the server prints on
/// stderr.
///
/// # Errors
///
/// [`MigrationError::Status`] with the status a server refuses, which
/// carries its exit code; [`MigrationError::Locked`] (exit 6) when the lock
/// or a running server outlasted the timeout and the catalog is still
/// behind; or as [`run_with_lock`].
pub async fn prepare_to_serve(
    migrations: &dyn CatalogMigrations,
    target: &Target,
    startup: &Startup<'_>,
    progress: &(dyn Fn(&str) + Send + Sync),
) -> Result<Prepared, MigrationError> {
    if migrations.in_process(target) {
        return Ok(Prepared::default());
    }
    let status = migrations.status(target).await?;
    match &status {
        CatalogStatus::Current { .. } => Ok(Prepared::default()),
        CatalogStatus::AheadCompatible { .. } => Ok(served_ahead(&status)),
        CatalogStatus::Uninitialized if startup.creates_uninitialized => Ok(Prepared::default()),
        CatalogStatus::Behind { version, pending } if startup.apply => {
            let latest = MigrationId::LATEST.number();
            progress(&match version {
                Some(v) => format!(
                    "the catalog is at migration {v} and this binary needs migration {latest}; \
                     applying the pending migrations before the server starts"
                ),
                None => format!(
                    "the catalog has no migration ledger (a release before 1.0 created it); \
                     applying migrations 1 to {latest} before the server starts"
                ),
            });
            progress(&pending_line(pending));
            apply(migrations, target, startup, progress).await
        }
        _ => Err(MigrationError::Status(status)),
    }
}

/// A catalog ahead of this binary by additive migrations, which it serves.
fn served_ahead(status: &CatalogStatus) -> Prepared {
    Prepared {
        run: None,
        warning: Some(format!(
            "{status}; serving it, because a binary may serve migrations it does not know when \
             every one of them is additive"
        )),
    }
}

/// The run under the lock, with a line every heartbeat while it goes on.
async fn apply(
    migrations: &dyn CatalogMigrations,
    target: &Target,
    startup: &Startup<'_>,
    progress: &(dyn Fn(&str) + Send + Sync),
) -> Result<Prepared, MigrationError> {
    progress(&format!(
        "taking the migration lock, waiting up to {} s for a server or another migration",
        startup.lock_timeout.as_secs()
    ));
    let started = Instant::now();
    let run = run_with_lock(
        migrations,
        target,
        startup.release,
        RunKind::Migrate,
        startup.lock_timeout,
    );
    let mut run = std::pin::pin!(run);
    let mut heartbeat = tokio::time::interval_at(started + startup.heartbeat, startup.heartbeat);
    let result = loop {
        tokio::select! {
            result = &mut run => break result,
            _ = heartbeat.tick() => {
                progress(&format!("in progress for {} s", started.elapsed().as_secs()));
            }
        }
    };
    match result {
        Ok(report) => {
            for line in report_lines(&report) {
                progress(&line);
            }
            Ok(Prepared {
                run: Some(report),
                warning: None,
            })
        }
        // A process that migrated the catalog while this one waited, and
        // then started serving it, holds what the lock waits for. The
        // catalog then needs nothing, and the server starts as `serve`
        // would on it; the server's own checks decide whether it may.
        Err(MigrationError::Locked(message)) => match migrations.status(target).await {
            Ok(CatalogStatus::Current { .. }) => {
                progress(MIGRATED_ELSEWHERE);
                Ok(Prepared::default())
            }
            Ok(status @ CatalogStatus::AheadCompatible { .. }) => {
                progress(MIGRATED_ELSEWHERE);
                Ok(served_ahead(&status))
            }
            _ => Err(MigrationError::Locked(message)),
        },
        Err(error) => Err(error),
    }
}

/// The line for a run that found the catalog migrated by another process.
const MIGRATED_ELSEWHERE: &str =
    "nothing to apply: another process migrated the catalog while this one waited for the lock";

/// What a run did, one line each: the backend's lines (the SQLite copy's
/// path), each step with its notes indented below it, and the version the
/// catalog is at.
fn report_lines(report: &RunReport) -> Vec<String> {
    let mut lines = report.notes.clone();
    let several = report
        .applied
        .windows(2)
        .any(|w| w[0].store_name != w[1].store_name);
    for step in &report.applied {
        lines.push(if several {
            format!("applied {} {} in {}", step.id, step.name, step.store_name)
        } else {
            format!("applied {} {}", step.id, step.name)
        });
        lines.extend(step.notes.iter().map(|note| format!("  {note}")));
    }
    if report.applied.is_empty() {
        lines.push(MIGRATED_ELSEWHERE.to_owned());
    } else if let Some(to) = report.to {
        lines.push(format!("the catalog is at migration {to}"));
    }
    lines
}

#[cfg(test)]
#[path = "startup_tests.rs"]
mod tests;
