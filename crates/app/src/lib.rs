// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! ExtendDB application/CLI library.
//!
//! Owns the command-line interface (`serve`, `init`, `destroy`, `backup`,
//! `verify`, `migrate`, `status`, `stop`, `settings`, `manage`,
//! `catalog-check`, `healthcheck`) and the
//! subcommand dispatch. It is backend-agnostic: a backend's thin `main`
//! installs its backend with
//! [`set_backend`](extenddb_storage::set_backend) and then calls [`run`]:
//!
//! ```ignore
//! fn main() -> anyhow::Result<()> {
//!     extenddb_storage::set_backend(my_backend::backend())?;
//!     extenddb_app::run(extenddb_app::BuildInfo {
//!         version: env!("CARGO_PKG_VERSION"),
//!         git_hash: env!("MY_GIT_HASH"),
//!         build_time: env!("MY_BUILD_TIME"),
//!         test_hooks_enabled: false,
//!     })
//! }
//! ```

mod cmd_backup;
mod cmd_catalog_check;
mod cmd_destroy;
mod cmd_healthcheck;
mod cmd_init;
mod cmd_manage;
mod cmd_migrate;
mod cmd_migrate_runner;
mod cmd_serve;
mod cmd_settings;
mod cmd_status;
mod cmd_stop;
mod cmd_verify;
mod init_helpers;
mod manage_http;
mod manage_types;
mod serve_helpers;
mod util;

use clap::{Parser, Subcommand};

/// Build provenance supplied by the deployed binary.
///
/// Defined by `extenddb-server` (which consumes it for the startup banner and
/// console version string) and re-exported here so a thin `main` only needs the
/// `extenddb-app` dependency.
pub use extenddb_server::BuildInfo;

#[derive(Parser)]
#[command(name = "extenddb", about = "ExtendDB — DynamoDB-compatible API server")]
struct Cli {
    /// Print version and exit
    #[arg(short = 'V', long)]
    version: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start the extenddb server
    Serve(cmd_serve::ServeArgs),
    /// Initialize a new extenddb deployment
    Init(cmd_init::InitArgs),
    /// Tear down a extenddb deployment (the databases; the backup store is left untouched)
    Destroy(cmd_destroy::DestroyArgs),
    /// List or purge the backups of the configured backup store
    Backup(cmd_backup::BackupArgs),
    /// Validate a extenddb deployment
    Verify(cmd_verify::VerifyArgs),
    /// Apply catalog migrations (exit 0 up to date or applied, 1 error, 3 pending without --yes,
    /// 4 the catalog has a breaking migration this binary lacks, 5 a person must act, 6 the
    /// catalog stayed locked)
    Migrate(cmd_migrate::MigrateArgs),
    /// Check if the extenddb server is running
    Status(cmd_status::StatusArgs),
    /// Stop the running extenddb server
    Stop(cmd_stop::StopArgs),
    /// Read or write runtime settings
    Settings(cmd_settings::SettingsArgs),
    /// Manage admin users and accounts via the management API
    Manage(cmd_manage::ManageArgs),
    /// Check catalog and data database integrity
    CatalogCheck(cmd_catalog_check::CatalogCheckArgs),
    /// Probe the local /health endpoint over HTTPS (exit 0 healthy, 1 not)
    Healthcheck(cmd_healthcheck::HealthcheckArgs),
    /// Print version, catalog version, git commit, and build timestamp
    Version,
}

/// Parse the command line and dispatch the selected subcommand.
///
/// The backend must already be installed via
/// [`extenddb_storage::set_backend`] before this is called.
///
/// # Errors
///
/// Returns any error produced by the selected subcommand.
pub fn run(build: BuildInfo) -> anyhow::Result<()> {
    let cli = Cli::parse();

    if cli.version {
        print_version(build);
        return Ok(());
    }

    match cli.command.unwrap_or(Command::Version) {
        Command::Serve(args) => cmd_serve::run(&args, build),
        Command::Init(args) => {
            let code = run_interactive(cmd_init::run(args, build.version))?;
            if code != 0 {
                std::process::exit(i32::from(code));
            }
            Ok(())
        }
        Command::Destroy(args) => run_interactive(cmd_destroy::run(args)),
        Command::Backup(args) => run_interactive(cmd_backup::run(args, build.version)),
        Command::Verify(args) => {
            let code = run_interactive(cmd_verify::run(args, build.version))?;
            if code != 0 {
                std::process::exit(i32::from(code));
            }
            Ok(())
        }
        Command::Migrate(args) => {
            // 0 up to date or applied; cmd_migrate::EXIT_PENDING for a preview
            // that found work; on the migration ledger, the status's code;
            // errors exit 1 through main.
            let code = run_interactive(cmd_migrate::run(args, build.version))?;
            if code != 0 {
                std::process::exit(i32::from(code));
            }
            Ok(())
        }
        Command::Status(args) => {
            cmd_status::run(&args);
            Ok(())
        }
        Command::Stop(args) => {
            cmd_stop::run(&args);
            Ok(())
        }
        Command::Settings(args) => run_interactive(cmd_settings::run(args)),
        Command::Manage(args) => run_interactive(cmd_manage::run(args)),
        Command::CatalogCheck(args) => run_interactive(cmd_catalog_check::run(args)),
        Command::Healthcheck(args) => match cmd_healthcheck::run(&args) {
            Ok(()) => Ok(()),
            Err(e) => {
                eprintln!("healthcheck failed: {e}");
                std::process::exit(1);
            }
        },
        Command::Version => {
            print_version(build);
            Ok(())
        }
    }
}

/// Print version, catalog version, git commit hash, and build timestamp.
fn print_version(build: BuildInfo) {
    println!("extenddb {}", build.version);

    // One backend is compiled into this binary; report the catalog it needs:
    // the highest migration it carries, or a backend's catalog version.
    match extenddb_storage::backend_name() {
        Some(backend) => println!(
            "catalog {} ({backend})",
            extenddb_storage::migrations::binary_catalog_label()
        ),
        None => println!("catalog unknown (no backend installed)"),
    }

    println!("commit {}", build.git_hash);
    println!("built {}", build.build_time);
    println!(
        "test-hooks {}",
        if build.test_hooks_enabled {
            "enabled"
        } else {
            "disabled"
        }
    );
}

/// Run an async subcommand with a single-threaded tokio runtime and stderr logging.
/// All non-serve subcommands are interactive (D-24).
fn run_interactive<T>(
    future: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    tracing_subscriber::fmt()
        .try_init()
        .unwrap_or_else(|e| eprintln!("Warning: logging init failed: {e}"));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    run_then_shut_down(runtime, future)
}

/// How long the process waits at exit for the runtime's blocking threads
/// after the command's future returns. A thread parked in a filesystem call
/// on a mount that has stopped answering (a backup store check the engine
/// abandoned at its bound) returns only when the mount does; past this bound
/// the thread is detached and the process exits without it.
pub(crate) const RUNTIME_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Run `future` to completion on `runtime`, then end the runtime within
/// [`RUNTIME_SHUTDOWN_TIMEOUT`]. Dropping the runtime instead joins every
/// blocking thread, so one parked syscall held the process open past its
/// shutdown until SIGKILL.
pub(crate) fn run_then_shut_down<T>(
    runtime: tokio::runtime::Runtime,
    future: impl std::future::Future<Output = T>,
) -> T {
    let output = runtime.block_on(future);
    runtime.shutdown_timeout(RUNTIME_SHUTDOWN_TIMEOUT);
    output
}

#[cfg(test)]
mod tests {
    use super::{RUNTIME_SHUTDOWN_TIMEOUT, run_then_shut_down};

    /// A blocking task that never returns (a filesystem call on a mount that
    /// has stopped answering) whose handle the caller dropped does not hold
    /// the runtime's end past the bound, on both runtimes the binary builds
    /// (`serve`'s multi-thread one and the commands' current-thread one).
    #[test]
    fn a_parked_blocking_thread_does_not_hold_the_runtime_past_its_bound() {
        let builders = [
            tokio::runtime::Builder::new_multi_thread(),
            tokio::runtime::Builder::new_current_thread(),
        ];
        for mut builder in builders {
            let runtime = builder.enable_all().build().unwrap();
            let (done, finished) = std::sync::mpsc::channel();
            let started = std::time::Instant::now();
            std::thread::spawn(move || {
                run_then_shut_down(runtime, async {
                    // Parked for good; the thread is leaked on purpose and
                    // dies with the test process.
                    drop(tokio::task::spawn_blocking(|| {
                        loop {
                            std::thread::park();
                        }
                    }));
                    // Let the blocking pool pick the task up before the
                    // runtime ends, so a thread is parked when it does.
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                });
                let _ = done.send(());
            });
            // The test's own guard, well past the bound: a shutdown that
            // joins the parked thread fails here instead of hanging.
            finished
                .recv_timeout(RUNTIME_SHUTDOWN_TIMEOUT * 6)
                .expect("the runtime ends within its shutdown bound");
            let elapsed = started.elapsed();
            assert!(
                elapsed < RUNTIME_SHUTDOWN_TIMEOUT * 3,
                "the runtime's end took {elapsed:?}"
            );
        }
    }
}
