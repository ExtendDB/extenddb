// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb status` — check if the extenddb server is running.
//!
//! Reads the port from `extenddb.toml` (or `--port` override) and checks whether
//! anything is listening on it. Reports the daemon PID from the PID file when
//! available.

use std::net::TcpStream;

use clap::Args;

use crate::config;

#[derive(Args)]
pub struct StatusArgs {
    /// Path to configuration file
    #[arg(short, long, default_value = "extenddb.toml")]
    config: String,

    /// Override port to check (defaults to port in config file)
    #[arg(short, long)]
    port: Option<u16>,
}

/// Check server status by attempting a TCP connection to the configured port.
/// This is a sync command — no tokio runtime needed.
pub fn run(args: &StatusArgs) {
    let port = match resolve_port(args) {
        Ok(p) => p,
        Err(e) => {
            println!("Error: {e}");
            std::process::exit(1);
        }
    };

    // Loopback connections fail immediately if nothing is listening,
    // so no explicit timeout is needed for the common case.
    let addr = format!("127.0.0.1:{port}");
    if TcpStream::connect(&addr).is_ok() {
        // D-3: Include PID from the PID file when available.
        // Validate the PID is alive to avoid reporting stale PIDs after unclean shutdown.
        // Try config-based run_dir first, fall back to default.
        let pid_file = config::load(&args.config).map_or_else(
            |_| crate::serve_helpers::pid_file_path_default(port),
            |c| crate::serve_helpers::pid_file_path(&config::expand_tilde(&c.server.run_dir), port),
        );
        let pid_label = std::fs::read_to_string(&pid_file)
            .ok()
            .and_then(|contents| {
                let pid = contents.trim().to_owned();
                let pid_num: i32 = pid.parse().ok()?;
                if crate::util::is_process_alive(pid_num) {
                    Some(pid)
                } else {
                    None
                }
            });
        match pid_label {
            Some(pid) => println!("extenddb is running on port {port} (pid {pid})"),
            None => println!("extenddb is running on port {port} (pid unknown)"),
        }
    } else {
        println!(
            "extenddb is not running (port {port} is not in use).\n\
             Start the server with: extenddb serve --config {}",
            args.config,
        );
        std::process::exit(1);
    }
}

/// Determine the port to check: CLI override > config file > default.
fn resolve_port(args: &StatusArgs) -> anyhow::Result<u16> {
    if let Some(port) = args.port {
        return Ok(port);
    }
    if !std::path::Path::new(&args.config).exists() {
        anyhow::bail!(
            "Config file '{}' not found. Use --port <port> to check a specific port, \
             or --config <path> to specify a config file.",
            args.config,
        );
    }
    let app_config = config::load(&args.config)?;
    Ok(app_config.server.port)
}
