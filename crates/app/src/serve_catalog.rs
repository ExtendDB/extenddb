// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The catalog check `extenddb serve` makes before it daemonizes: the
//! status, read by the startup policy (`[server] catalog_upgrade`, or
//! `--migrate` for one run), and under `apply` the run that migrates a
//! catalog behind this binary. It runs in the invoking process, on a
//! runtime that is shut down before the server forks, so in daemon mode the
//! exit code and the message of a refusal reach the invoking shell as they
//! do in the foreground. The server checks the catalog again when it
//! registers, after the fork.

use std::time::Duration;

use extenddb_config::{self as config, AppConfig, CatalogUpgrade};
use extenddb_storage::migrations::advice::{self, AdviceFor};
use extenddb_storage::migrations::{Startup, Target, exit, installed, prepare_to_serve};

/// The start of every line `serve` prints on stderr while it migrates the
/// catalog. The npm launcher's startup timeout restarts at each one.
pub(crate) const PROGRESS_PREFIX: &str = "catalog migration: ";

/// Check the catalog of `app_config` before the server starts, printing
/// the lines of a run and any refusal on stderr. Returns the status's exit
/// code: 0 when the server may start, else the code `serve` exits with.
/// `config_file` is the config file the server was started with, named in
/// the command a refusal prints.
///
/// # Errors
///
/// The runtime could not be built.
pub(crate) fn check_catalog(
    app_config: &AppConfig,
    upgrade: CatalogUpgrade,
    release: &str,
    config_file: Option<&str>,
) -> anyhow::Result<u8> {
    let Some(migrations) = installed() else {
        return Ok(exit::OK);
    };
    let location = app_config.storage.connection_config();
    let target = Target::at(location);
    let startup = Startup {
        apply: upgrade == CatalogUpgrade::Apply,
        // A dev build's server creates its catalog in a missing or empty
        // database file.
        creates_uninitialized: cfg!(feature = "dev-mode"),
        release,
        lock_timeout: Duration::from_secs(app_config.server.catalog_upgrade_lock_timeout),
        heartbeat: Startup::HEARTBEAT,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(prepare_to_serve(migrations, &target, &startup, &|line| {
        eprintln!("{PROGRESS_PREFIX}{line}");
    }));
    // The fork that follows copies only the thread that calls it: every
    // task and blocking thread of this runtime ends first.
    runtime.shutdown_timeout(Duration::from_secs(10));
    match result {
        Ok(prepared) => {
            if let Some(warning) = prepared.warning {
                eprintln!("WARNING: {warning}");
            }
            Ok(exit::OK)
        }
        Err(error) => {
            let advice = AdviceFor::new(
                release,
                config::redact_password(location),
                Some(migrations),
                &target,
                config_file,
            )
            .for_server();
            eprintln!("Error: {}", advice::error_message(&error, &advice.advice()));
            // A refusal never lets the server start, whatever its status's
            // code.
            Ok(error.exit_code().max(exit::ERROR))
        }
    }
}
