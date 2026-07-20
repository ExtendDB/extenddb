// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! ExtendDB application/CLI library.
//!
//! Owns the command-line interface (`serve`, `init`, `destroy`, `verify`,
//! `migrate`, `status`, `stop`, `settings`, `manage`, `catalog-check`) and the
//! subcommand dispatch. It is backend-agnostic: a backend's thin `main`
//! registers its backend into the [`BackendRegistry`](extenddb_storage::registry),
//! installs it, and then calls [`run`]:
//!
//! ```ignore
//! fn main() -> anyhow::Result<()> {
//!     let mut registry = extenddb_storage::BackendRegistry::new();
//!     my_backend::register(&mut registry);
//!     extenddb_storage::set_registry(registry).expect("registry already set");
//!     extenddb_app::run(extenddb_app::BuildInfo {
//!         git_hash: env!("MY_GIT_HASH"),
//!         build_time: env!("MY_BUILD_TIME"),
//!     })
//! }
//! ```

mod cmd_catalog_check;
mod cmd_destroy;
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
/// The library cannot read the bin's `build.rs` environment variables, so the
/// thin `main` passes them in. Surfaced by `extenddb version` and the console
/// version string.
#[derive(Debug, Clone, Copy)]
pub struct BuildInfo {
    /// Short git commit hash of the build (e.g. `env!("EXTENDDB_GIT_HASH")`).
    pub git_hash: &'static str,
    /// Build timestamp (e.g. `env!("EXTENDDB_BUILD_TIME")`).
    pub build_time: &'static str,
}

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
    /// Print version, catalog version, git commit, and build timestamp
    Version,
}

/// Parse the command line and dispatch the selected subcommand.
///
/// The backend registry must already be installed via
/// [`extenddb_storage::set_registry`] before this is called.
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
        Command::Serve(args) => cmd_serve::run(&args, build.git_hash),
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
        Command::Version => {
            print_version(build);
            Ok(())
        }
    }
}

/// Print version, catalog version, git commit hash, and build timestamp.
fn print_version(build: BuildInfo) {
    println!("extenddb {}", env!("CARGO_PKG_VERSION"));

    // Report catalog version(s) for all registered backend(s)
    let backends = extenddb_storage::operations::list_operations_backends();
    if backends.is_empty() {
        println!("catalog unknown (no backends registered)");
    } else {
        for backend in backends {
            let version = extenddb_storage::operations::catalog_version(backend)
                .unwrap_or_else(|_| "unknown".to_string());
            println!("catalog {version} ({backend})");
        }
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
