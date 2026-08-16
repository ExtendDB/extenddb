// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! ExtendDB application/CLI library.
//!
//! Owns the command-line interface (`serve`, `init`, `destroy`, `verify`,
//! `migrate`, `status`, `stop`, `settings`, `manage`, `catalog-check`,
//! `healthcheck`) and the
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
//!     })
//! }
//! ```

mod cmd_catalog_check;
mod cmd_destroy;
mod cmd_healthcheck;
mod cmd_init;
mod cmd_manage;
mod cmd_migrate;
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
    /// Tear down a extenddb deployment
    Destroy(cmd_destroy::DestroyArgs),
    /// Validate a extenddb deployment
    Verify(cmd_verify::VerifyArgs),
    /// Apply catalog schema migrations
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
            let code = run_interactive(cmd_init::run(args))?;
            if code != 0 {
                std::process::exit(i32::from(code));
            }
            Ok(())
        }
        Command::Destroy(args) => run_interactive(cmd_destroy::run(args)),
        Command::Verify(args) => run_interactive(cmd_verify::run(args)),
        Command::Migrate(args) => run_interactive(cmd_migrate::run(args)),
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

    // One backend is compiled into this binary; report its catalog version.
    match (
        extenddb_storage::backend_name(),
        extenddb_storage::operations::catalog_version(),
    ) {
        (Some(backend), Ok(version)) => println!("catalog {version} ({backend})"),
        (Some(backend), Err(_)) => println!("catalog unknown ({backend})"),
        (None, _) => println!("catalog unknown (no backend installed)"),
    }

    println!("commit {}", build.git_hash);
    println!("built {}", build.build_time);
}

/// Run an async subcommand with a single-threaded tokio runtime and stderr logging.
/// All non-serve subcommands are interactive (D-24).
fn run_interactive<T>(
    future: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    tracing_subscriber::fmt()
        .try_init()
        .unwrap_or_else(|e| eprintln!("Warning: logging init failed: {e}"));
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(future)
}
