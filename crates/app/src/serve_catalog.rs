// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The catalog check `extenddb serve` makes before it binds its port and
//! daemonizes: the status, read by the startup policy (`[server]
//! catalog_upgrade`, or `--migrate` for one run), and under `apply` the run
//! that migrates a catalog behind this binary. It runs in the invoking
//! process, on a runtime that is shut down before the server forks, so in
//! daemon mode the exit code and the message of a refusal reach the
//! invoking shell as they do in the foreground. The server checks the
//! catalog again when it registers, after the fork.

use std::time::{Duration, Instant};

use extenddb_config::{self as config, CatalogUpgrade, ServerConfig};
use extenddb_storage::migrations::advice::{self, AdviceFor};
use extenddb_storage::migrations::{
    CatalogMigrations, Startup, Target, exit, installed, prepare_to_serve,
};

/// The start of every line `serve` prints on stderr while it checks and
/// migrates the catalog. The npm launcher's startup timeout restarts at
/// each one.
pub(crate) const PROGRESS_PREFIX: &str = "catalog migration: ";

/// How long the check's runtime may take to stop. A blocking thread still
/// running after it keeps a daemon from forking.
const SHUTDOWN_CAP: Duration = Duration::from_secs(10);

/// How `serve` was started, as the catalog check reads it.
pub(crate) struct ServeStart<'a> {
    /// The `[server]` section: `catalog_upgrade` and its lock timeout.
    pub(crate) server: &'a ServerConfig,
    /// The storage location the config gives.
    pub(crate) location: &'a str,
    /// `serve --migrate`, which sets `catalog_upgrade = "apply"` for this
    /// run.
    pub(crate) migrate: bool,
    /// The ExtendDB release, recorded in the ledger rows a run writes.
    pub(crate) release: &'a str,
    /// The config file the server was started with, named in the command a
    /// refusal prints.
    pub(crate) config_file: Option<&'a str>,
    /// Whether the server forks once the check has passed (daemon mode).
    pub(crate) daemonizes: bool,
}

/// `[server] catalog_upgrade_lock_timeout`, refused above a day.
///
/// # Errors
///
/// The timeout is above a day.
pub(crate) fn lock_timeout(server: &ServerConfig) -> anyhow::Result<Duration> {
    let secs = server.catalog_upgrade_lock_timeout;
    if secs > config::MAX_CATALOG_UPGRADE_LOCK_TIMEOUT {
        anyhow::bail!(
            "server.catalog_upgrade_lock_timeout is {secs} s; it may be at most {} s (a day)",
            config::MAX_CATALOG_UPGRADE_LOCK_TIMEOUT
        );
    }
    Ok(Duration::from_secs(secs))
}

/// Check the catalog `start` names before the server binds its port,
/// printing on stderr the lines of the check and of a run, and any
/// refusal. Returns the exit code: 0 when the server may start, else the
/// code `serve` exits with.
///
/// The warnings the backends log during the check, such as PostgreSQL's
/// retries after a lock timeout, go to stderr through a subscriber set for
/// the check alone, as `migrate --yes` prints them. The global default stays
/// free for the server's own subscriber.
///
/// # Errors
///
/// The lock timeout is above a day, or the runtime could not be built.
pub(crate) fn check_catalog(start: &ServeStart<'_>) -> anyhow::Result<u8> {
    let Some(migrations) = installed() else {
        return Ok(exit::OK);
    };
    let subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        check(migrations, start, SHUTDOWN_CAP, &|line| eprintln!("{line}"))
    })
}

/// [`check_catalog`] on `migrations`, writing each line to `out`, with
/// `shutdown_cap` for the check's runtime to stop.
fn check(
    migrations: &dyn CatalogMigrations,
    start: &ServeStart<'_>,
    shutdown_cap: Duration,
    out: &(dyn Fn(&str) + Send + Sync),
) -> anyhow::Result<u8> {
    let upgrade = if start.migrate {
        CatalogUpgrade::Apply
    } else {
        start.server.catalog_upgrade
    };
    let target = Target::at(start.location);
    let startup = Startup {
        apply: upgrade == CatalogUpgrade::Apply,
        // A dev build's server creates its catalog in a missing or empty
        // database file.
        creates_uninitialized: cfg!(feature = "dev-mode"),
        release: start.release,
        lock_timeout: lock_timeout(start.server)?,
        heartbeat: Startup::HEARTBEAT,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    // Each line of a progress message gets the prefix, so a note that spans
    // several lines counts as progress to the launcher on each of them.
    let progress = |text: &str| {
        for line in text.lines() {
            out(&format!("{PROGRESS_PREFIX}{line}"));
        }
    };
    let result = runtime.block_on(prepare_to_serve(migrations, &target, &startup, &progress));
    let capped = shut_down(runtime, shutdown_cap);
    match result {
        Ok(prepared) => {
            if let Some(warning) = prepared.warning {
                out(&format!("WARNING: {warning}"));
            }
            if capped && start.daemonizes {
                out(&format!(
                    "Error: a blocking thread of the catalog check, such as a host name lookup, \
                     was still running {} s after the check ended. The server does not fork \
                     while it runs, since a lock that thread holds would stay held in the \
                     child, where the thread does not exist. Run extenddb serve again, or with \
                     --foreground.",
                    shutdown_cap.as_secs()
                ));
                return Ok(exit::ERROR);
            }
            Ok(exit::OK)
        }
        Err(error) => {
            let advice = AdviceFor::new(
                start.release,
                config::redact_password(start.location),
                Some(migrations),
                &target,
                start.config_file,
            )
            .for_server();
            out(&format!(
                "Error: {}",
                advice::error_message(&error, &advice.advice())
            ));
            // A refusal never lets the server start, whatever its status's
            // code.
            Ok(error.exit_code().max(exit::ERROR))
        }
    }
}

/// Shut `runtime` down, waiting up to `cap` for its tasks and blocking
/// threads to end. `true` when the wait reached the cap, so a blocking
/// thread may still be running: a fork copies only the thread that calls
/// it, and a lock such a thread holds would stay held in the child.
fn shut_down(runtime: tokio::runtime::Runtime, cap: Duration) -> bool {
    let started = Instant::now();
    runtime.shutdown_timeout(cap);
    started.elapsed() >= cap
}

#[cfg(test)]
#[path = "serve_catalog_tests.rs"]
mod tests;
