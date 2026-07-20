// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! extenddb binary — entry point for the Virtual `DynamoDB` server.
//!
//! Provides subcommands for server operation and lifecycle management:
//! `serve`, `init`, `destroy`, `verify`, `migrate`, `status`, `stop`, `settings`.
//! Running with no subcommand prints version information.

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
mod config;
mod init_helpers;
mod manage_http;
mod manage_types;
mod serve_helpers;
mod util;
mod workers;

use clap::{Parser, Subcommand};

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

fn main() -> anyhow::Result<()> {
    // Wire the available backend(s) into the process registry before any
    // subcommand runs. This is the single place backends are selected — the
    // compiler checks it, and adding a backend is a plain `register` call
    // rather than a link-time side effect.
    let mut registry = extenddb_storage::BackendRegistry::new();
    #[cfg(feature = "postgres")]
    extenddb_storage_postgres::register(&mut registry);
    extenddb_storage::set_registry(registry)?;

    let cli = Cli::parse();

    if cli.version {
        print_version();
        return Ok(());
    }

    match cli.command.unwrap_or(Command::Version) {
        Command::Serve(args) => cmd_serve::run(&args),
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
            print_version();
            Ok(())
        }
    }
}

/// Print version, catalog version, git commit hash, and build timestamp.
fn print_version() {
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

    println!("commit {}", env!("EXTENDDB_GIT_HASH"));
    println!("built {}", env!("EXTENDDB_BUILD_TIME"));
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
