// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helper functions for `extenddb serve`: daemonize health checks, PID file
//! management, config permission checks, and syslog utilities.

use std::path::PathBuf;

use crate::config;

/// P57 Bug 7: Best-effort raw syslog write for fatal errors. Used when the
/// tracing subscriber may not be initialized (e.g., errors during early
/// startup before syslog tracing is configured).
pub fn log_to_syslog_raw(msg: &str) {
    // SAFETY: openlog/syslog are POSIX-standard C functions. The ident
    // string is a static C string literal with 'static lifetime.
    unsafe {
        libc::openlog(
            c"extenddb".as_ptr(),
            libc::LOG_PID | libc::LOG_NDELAY,
            libc::LOG_DAEMON,
        );
        if let Ok(cmsg) = std::ffi::CString::new(msg.to_owned()) {
            libc::syslog(libc::LOG_CRIT, c"%s".as_ptr(), cmsg.as_ptr());
        }
    }
}

/// Platform-appropriate hint for viewing syslog output.
fn syslog_hint() -> &'static str {
    if cfg!(target_os = "macos") {
        "Check syslog: log show --predicate 'process == \"extenddb\"' --last 5m"
    } else {
        "Check syslog: journalctl -t extenddb"
    }
}

/// P57 Bug 7: After daemonizing, the parent waits for the PID file to appear
/// and then verifies the daemon process is still alive. This catches early
/// startup failures (bad config, missing catalog tables, TLS cert errors)
/// that would otherwise be invisible because stderr is /dev/null after fork.
pub fn verify_daemon_started(pid_file: &PathBuf, bind_addr: &str) -> anyhow::Result<()> {
    let hint = syslog_hint();

    // Wait up to 5 seconds for the PID file to be written with a valid PID.
    // The daemonize crate creates the file and writes the grandchild PID after
    // the double-fork. On macOS there is a window where the file exists but is
    // empty or partially written, so we retry both read and parse.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let pid: u32 = loop {
        if let Ok(content) = std::fs::read_to_string(pid_file)
            && let Ok(p) = content.trim().parse::<u32>()
        {
            break p;
        }
        if std::time::Instant::now() >= deadline {
            eprintln!("Server failed to start: PID file not created within 5 seconds.\n{hint}");
            std::process::exit(1);
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    };

    // Give the daemon a moment to initialize (connect to Postgres, load TLS
    // certs, etc.). Check every 200ms for up to 3 seconds.
    let check_deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        // kill(pid, 0) checks if the process exists without sending a signal.
        // SAFETY: Standard POSIX signal check. pid is from our own PID file.
        let alive = unsafe { libc::kill(pid as i32, 0) } == 0;
        if !alive {
            eprintln!(
                "Server failed to start: daemon process {pid} exited during startup.\n{hint}"
            );
            std::process::exit(1);
        }
        if std::time::Instant::now() >= check_deadline {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(200));
    }

    println!("extenddb server started (pid {pid}, {bind_addr})");
    Ok(())
}

/// PID file path for a given port and run directory.
/// Used by `serve` (write) and `status` (read).
pub fn pid_file_path(run_dir: &str, port: u16) -> PathBuf {
    PathBuf::from(format!("{run_dir}/extenddb-{port}.pid"))
}

/// PID file path using the default run directory. Used by `status` when
/// no config file is loaded.
pub fn pid_file_path_default(port: u16) -> PathBuf {
    let run_dir = config::AppConfig::default().server.run_dir;
    pid_file_path(&run_dir, port)
}

/// Check that the config file has permissions no more permissive than `0600`.
///
/// The config file may contain the encryption key for credential storage.
/// If group or other bits are set, refuse to start with a clear error message.
///
/// # Errors
///
/// Returns an error if the file permissions are too open or cannot be read.
pub fn check_config_permissions(config_path: &str) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let path = std::path::Path::new(config_path);
    if !path.exists() {
        // Config file is optional — `config::load` handles missing files.
        return Ok(());
    }

    let metadata = std::fs::metadata(path)
        .map_err(|e| anyhow::anyhow!("Cannot read config file metadata for {config_path}: {e}"))?;
    let mode = metadata.permissions().mode() & 0o777;

    if mode & 0o077 != 0 {
        // Auto-fix like SSH does: warn and tighten permissions rather than refusing
        // to start. The config file may contain the encryption key for credential
        // storage, so group/other access is a security risk.
        tracing::warn!(
            "Config file {} has permissions {:04o}, fixing to 0600. \
             The config file may contain the encryption key for credential storage.",
            config_path,
            mode,
        );
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(|e| {
            anyhow::anyhow!(
                "Cannot fix config file permissions for {config_path}: {e}. \
                 Fix manually with: chmod 600 {config_path}"
            )
        })?;
    }

    Ok(())
}
