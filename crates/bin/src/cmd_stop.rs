// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb stop` — stop the running extenddb server.
//!
//! Reads the PID file, sends SIGTERM, polls until the process exits (or
//! times out), and cleans up the stale PID file.

use std::thread;
use std::time::{Duration, Instant};

use clap::Args;

use crate::config;
use crate::util::is_process_alive;

/// Maximum time to wait for the process to exit after SIGTERM.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

/// Poll interval when waiting for process exit.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Args)]
pub struct StopArgs {
    /// Path to configuration file
    #[arg(short, long, default_value = "extenddb.toml")]
    config: String,

    /// Override port (determines which PID file to read)
    #[arg(short, long)]
    port: Option<u16>,
}

/// Stop the extenddb daemon by reading its PID file and sending SIGTERM.
pub fn run(args: &StopArgs) {
    let app_config = config::load(&args.config).ok();

    let port = args
        .port
        .or_else(|| app_config.as_ref().map(|c| c.server.port))
        .unwrap_or(8000);

    let pid_file = match &app_config {
        Some(c) => {
            crate::serve_helpers::pid_file_path(&config::expand_tilde(&c.server.run_dir), port)
        }
        None => crate::serve_helpers::pid_file_path_default(port),
    };

    let pid_str = match std::fs::read_to_string(&pid_file) {
        Ok(s) => s.trim().to_owned(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!(
                "No extenddb server is running on port {port} (PID file {} not found).\n\
                 Start one with: extenddb serve --config {}",
                pid_file.display(),
                args.config,
            );
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Failed to read PID file {}: {e}", pid_file.display());
            std::process::exit(1);
        }
    };

    let pid: i32 = if let Ok(p) = pid_str.parse() {
        p
    } else {
        eprintln!("Invalid PID in {}: '{pid_str}'", pid_file.display());
        std::process::exit(1);
    };

    // Check if the process is alive before sending SIGTERM.
    if !is_process_alive(pid) {
        eprintln!("Process {pid} is not running. Cleaning up stale PID file.");
        let _ = std::fs::remove_file(&pid_file);
        // Exit 0: the user's intent ("stop extenddb") is satisfied — the server
        // is not running. This makes scripting easier (e.g. `extenddb stop && ...`).
        return;
    }

    // Send SIGTERM.
    // SAFETY: libc::kill is a standard POSIX function. We pass a valid PID
    // and signal number. The only side effect is delivering a signal.
    let ret = unsafe { libc::kill(pid, libc::SIGTERM) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        eprintln!("Failed to send SIGTERM to pid {pid}: {err}");
        std::process::exit(1);
    }

    // Poll until the process exits or we time out.
    let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
    loop {
        if !is_process_alive(pid) {
            // Process exited — clean up PID file if the server didn't already.
            let _ = std::fs::remove_file(&pid_file);
            println!("extenddb stopped (pid {pid})");
            return;
        }
        if Instant::now() >= deadline {
            eprintln!(
                "extenddb (pid {pid}) did not exit within {}s after SIGTERM",
                SHUTDOWN_TIMEOUT.as_secs(),
            );
            std::process::exit(1);
        }
        thread::sleep(POLL_INTERVAL);
    }
}
