// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `extenddb stop` — stop the running extenddb server.
//!
//! Reads the PID file, checks that a running server holds it, sends SIGTERM,
//! polls until the process exits (or times out), and cleans up the stale PID
//! file.

#[cfg(unix)]
use std::thread;
#[cfg(unix)]
use std::time::{Duration, Instant};

use clap::Args;

#[cfg(unix)]
use crate::util::is_process_alive;
#[cfg(unix)]
use extenddb_config as config;

/// Maximum time to wait for the process to exit after SIGTERM: the sum of
/// the server's shutdown bounds, in the order it runs them (the listener's
/// drain of active connections, the backup jobs' failure paths, the
/// registration supervisor's return with the background workers' stop and
/// the serving registration's release, the runtime's blocking threads), so
/// `stop` does not give up on a server that is still inside one of them.
#[cfg(unix)]
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(
    extenddb_server::CONNECTION_DRAIN_TIMEOUT.as_secs()
        + extenddb_server::JOB_DRAIN_TIMEOUT.as_secs()
        + extenddb_server::WORKER_STOP_TIMEOUT.as_secs()
        + crate::RUNTIME_SHUTDOWN_TIMEOUT.as_secs(),
);

/// Poll interval when waiting for process exit.
#[cfg(unix)]
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

/// Non-unix: daemon mode does not exist, so there is never a PID file to
/// read or a SIGTERM to send. The server always runs in the foreground
/// here — stop it from whatever started it (Ctrl+C, the supervisor, or the
/// npm launcher's `stop()`).
#[cfg(not(unix))]
pub fn run(_args: &StopArgs) {
    eprintln!(
        "`extenddb stop` is not supported on this platform: daemon mode is \
         unix-only, so the server always runs in the foreground. Stop it from \
         the terminal or supervisor that started it."
    );
    std::process::exit(1);
}

/// Stop the extenddb daemon by reading its PID file and sending SIGTERM.
#[cfg(unix)]
pub fn run(args: &StopArgs) {
    let app_config = config::load(&args.config).ok();

    let port = args
        .port
        .or_else(|| app_config.as_ref().map(|c| c.server.port))
        .unwrap_or(18443);

    let pid_file = match &app_config {
        Some(c) => extenddb_config::pid_file_path(&config::expand_tilde(&c.server.run_dir), port),
        None => extenddb_config::pid_file_path_default(port),
    };

    // The PID and whether a running server holds the file, from one open of
    // it, so a server that replaces the file meanwhile cannot pair its lock
    // with the stale PID.
    let contents = match extenddb_server::pid_file::read(&pid_file) {
        Ok(contents) => contents,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // `serve --foreground` writes no PID file, so a missing file does
            // not mean nothing is running. Probe the port before saying so.
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                eprintln!(
                    "A server is listening on port {port} but wrote no PID file ({}).\n\
                     It was most likely started with `extenddb serve --foreground`, which \
                     leaves process supervision to the container runtime, systemd, or your \
                     shell. Stop it there (or send it SIGTERM directly).",
                    pid_file.display(),
                );
            } else {
                eprintln!(
                    "No extenddb server is running on port {port} (PID file {} not found).\n\
                     Start one with: extenddb serve --config {}",
                    pid_file.display(),
                    args.config,
                );
            }
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Failed to read PID file {}: {e}", pid_file.display());
            std::process::exit(1);
        }
    };
    let pid_str = contents.text.trim();

    let pid: i32 = if let Ok(p) = pid_str.parse() {
        p
    } else {
        eprintln!("Invalid PID in {}: '{pid_str}'", pid_file.display());
        std::process::exit(1);
    };

    // A running server holds a lock on its PID file. When nothing holds it, the
    // PID is stale: the server is gone and the system may have given the PID to
    // another process, which must not be signalled. The lock is checked first
    // because `is_process_alive` reports a server another user runs as gone.
    if !contents.held {
        if !is_process_alive(pid) {
            eprintln!("Process {pid} is not running. Cleaning up stale PID file.");
            let _ = extenddb_server::pid_file::remove_stale(&pid_file, &contents);
            // Exit 0: the user's intent ("stop extenddb") is satisfied, since
            // the server is not running. This makes scripting easier (e.g.
            // `extenddb stop && ...`).
            return;
        }
        // The address `serve` would bind with the same config file and
        // `EXTENDDB__` variables, including when there is no file.
        let bind_addr = app_config
            .as_ref()
            .map(|c| c.server.bind_addr.clone())
            .or_else(|| {
                extenddb_config::load_builtin_defaults()
                    .ok()
                    .map(|c| c.server.bind_addr)
            })
            .unwrap_or_else(|| extenddb_config::ServerConfig::default().bind_addr);
        let probed = listen_probe_addrs(&bind_addr, port);
        if let Some(addr) = probed.iter().find(|a| is_listening(a)) {
            eprintln!(
                "PID file {} names process {pid}, but no extenddb server holds the file, so \
                 `stop` cannot tell whether that process is the server listening on {addr}, \
                 and does not signal it.\n\
                 A server that an earlier extenddb release started with `serve --foreground \
                 --write-pid-file` holds no lock on its PID file. If process {pid} is that \
                 server, stop it with: kill {pid}",
                pid_file.display(),
            );
            std::process::exit(1);
        }
        let probed: Vec<String> = probed.iter().map(ToString::to_string).collect();
        eprintln!(
            "PID file {} names process {pid}, which is not an extenddb server: no server \
             holds the file and nothing listens on {}. Cleaning up stale PID file.",
            pid_file.display(),
            probed.join(", "),
        );
        let _ = extenddb_server::pid_file::remove_stale(&pid_file, &contents);
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
            // Process exited: clean up its PID file if the server didn't
            // already, but not a file a new server has put in its place.
            let _ = extenddb_server::pid_file::remove_stale(&pid_file, &contents);
            println!("extenddb stopped (pid {pid})");
            return;
        }
        if Instant::now() >= deadline {
            eprintln!(
                "extenddb (pid {pid}) did not exit within {}s after SIGTERM (the server \
                 drains connections for up to {}s, waits up to {}s for its backup jobs to end, \
                 then up to {}s to stop its background workers and release its serving \
                 registration, and {}s for its runtime)",
                SHUTDOWN_TIMEOUT.as_secs(),
                extenddb_server::CONNECTION_DRAIN_TIMEOUT.as_secs(),
                extenddb_server::JOB_DRAIN_TIMEOUT.as_secs(),
                extenddb_server::WORKER_STOP_TIMEOUT.as_secs(),
                crate::RUNTIME_SHUTDOWN_TIMEOUT.as_secs(),
            );
            std::process::exit(1);
        }
        thread::sleep(POLL_INTERVAL);
    }
}

/// The addresses a server on `port` configured with `bind_addr` answers on:
/// that address, both loopback addresses for a wildcard one, the addresses a
/// host name resolves to, and 127.0.0.1 in every case.
#[cfg(unix)]
fn listen_probe_addrs(bind_addr: &str, port: u16) -> Vec<std::net::SocketAddr> {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs};

    // serve binds `{bind_addr}:{port}`, so it also accepts an IPv6 address in
    // brackets.
    let host = bind_addr
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(bind_addr);
    let mut addrs = vec![SocketAddr::from((Ipv4Addr::LOCALHOST, port))];
    match host.parse::<IpAddr>() {
        Ok(ip) if ip.is_unspecified() => addrs.push(SocketAddr::from((Ipv6Addr::LOCALHOST, port))),
        Ok(ip) => addrs.push(SocketAddr::new(ip, port)),
        Err(_) => addrs.extend((host, port).to_socket_addrs().into_iter().flatten()),
    }
    addrs.sort_unstable();
    addrs.dedup();
    addrs
}

/// Whether something accepts a TCP connection on `addr`.
#[cfg(unix)]
fn is_listening(addr: &std::net::SocketAddr) -> bool {
    std::net::TcpStream::connect_timeout(addr, Duration::from_millis(500)).is_ok()
}

#[cfg(all(test, unix))]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use super::{SHUTDOWN_TIMEOUT, listen_probe_addrs};

    fn addrs(list: &[&str]) -> Vec<SocketAddr> {
        let mut v: Vec<SocketAddr> = list.iter().map(|a| a.parse().unwrap()).collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn a_specific_address_is_probed_with_loopback() {
        assert_eq!(
            listen_probe_addrs("10.1.2.3", 18443),
            addrs(&["10.1.2.3:18443", "127.0.0.1:18443"])
        );
        for ipv6 in ["::1", "[::1]"] {
            assert_eq!(
                listen_probe_addrs(ipv6, 18443),
                addrs(&["[::1]:18443", "127.0.0.1:18443"]),
                "{ipv6}"
            );
        }
    }

    #[test]
    fn a_wildcard_address_is_probed_on_both_loopbacks() {
        for wildcard in ["0.0.0.0", "::", "[::]"] {
            assert_eq!(
                listen_probe_addrs(wildcard, 18443),
                addrs(&["127.0.0.1:18443", "[::1]:18443"]),
                "{wildcard}"
            );
        }
    }

    #[test]
    fn the_default_bind_address_is_probed_once() {
        assert_eq!(
            listen_probe_addrs("127.0.0.1", 18443),
            addrs(&["127.0.0.1:18443"])
        );
    }

    /// `stop` waits at least as long as the server's shutdown can take with
    /// a backup job in its failure path: 5 s of connection drain, 30 s of
    /// job drain, 36 s after it (10 s for the supervisor's return, 16 s for
    /// the workers' stop: 5 s of drain, 1 s of abort and 10 s behind a
    /// supervisor still stopping them, and 10 s for the registration's
    /// release) and 5 s for the runtime.
    #[test]
    fn stop_waits_for_every_shutdown_bound() {
        assert_eq!(
            extenddb_server::WORKER_STOP_TIMEOUT,
            Duration::from_secs(36)
        );
        assert_eq!(SHUTDOWN_TIMEOUT, Duration::from_secs(76));
        assert!(
            SHUTDOWN_TIMEOUT
                >= extenddb_server::JOB_DRAIN_TIMEOUT
                    + extenddb_server::WORKER_STOP_TIMEOUT
                    + crate::RUNTIME_SHUTDOWN_TIMEOUT
                    + extenddb_server::CONNECTION_DRAIN_TIMEOUT
        );
    }
}
