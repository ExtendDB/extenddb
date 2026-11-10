// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Helper functions for `extenddb serve`: daemonize health checks, PID file
//! management, config permission checks, and syslog utilities.

#[cfg(unix)]
use std::path::PathBuf;

/// Platform-appropriate hint for viewing syslog output.
#[cfg(unix)]
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
#[cfg(unix)]
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

/// Check that the config file has permissions no more permissive than `0600`.
///
/// The config file may contain the encryption key for credential storage.
/// If group or other bits are set, refuse to start with a clear error message.
///
/// # Errors
///
/// Returns an error if the file permissions are too open or cannot be read.
pub fn check_config_permissions(config_path: &str) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let path = std::path::Path::new(config_path);
        if !path.exists() {
            // Config file is optional — `config::load` handles missing files.
            return Ok(());
        }

        let metadata = std::fs::metadata(path).map_err(|e| {
            anyhow::anyhow!("Cannot read config file metadata for {config_path}: {e}")
        })?;
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
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).map_err(
                |e| {
                    anyhow::anyhow!(
                        "Cannot fix config file permissions for {config_path}: {e}. \
                         Fix manually with: chmod 600 {config_path}"
                    )
                },
            )?;
        }

        Ok(())
    }
    // Windows has no POSIX permission bits; access control is via ACLs, which
    // have no meaningful 0600 equivalent to enforce here. The dev/CI use case
    // on Windows (the npm launcher) runs with an in-memory backend and no
    // config file, so this check is a no-op rather than a porting project.
    #[cfg(not(unix))]
    {
        let _ = config_path;
        Ok(())
    }
}

/// Set the TLS private key at `key_path` to mode 0600 when group or other
/// bits are set, and return the warning for the caller to print.
///
/// `init` creates the key 0600, but an older `init` that stopped between
/// writing the key and setting its mode left it at the umask mode, and an
/// operator can copy a key in the same way. `serve` and the `init` path that
/// keeps an existing key both call this.
///
/// Returns `None` when the key is missing or already 0600 or narrower; a
/// missing key is reported by whoever loads it. A key whose mode cannot be
/// changed (a read-only mount, or a file another user owns) gets a warning
/// with the manual fix rather than an error, because the process may still
/// be able to read it and refusing to start would be a regression for those
/// deployments.
pub fn narrow_tls_key_permissions(key_path: &str) -> Option<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = std::fs::metadata(key_path).ok()?.permissions().mode() & 0o777;
        if mode & 0o077 == 0 {
            return None;
        }
        Some(
            match std::fs::set_permissions(key_path, std::fs::Permissions::from_mode(0o600)) {
                Ok(()) => format!(
                    "WARNING: TLS private key {key_path} had permissions {mode:04o}, \
                     which let other users read it; set it to 0600."
                ),
                Err(e) => format!(
                    "WARNING: TLS private key {key_path} has permissions {mode:04o}, \
                     which let other users read it, and setting it to 0600 failed: {e}. \
                     Fix it with: chmod 600 {key_path}"
                ),
            },
        )
    }
    #[cfg(not(unix))]
    {
        let _ = key_path;
        None
    }
}

#[cfg(all(test, unix))]
mod tls_key_permission_tests {
    use super::narrow_tls_key_permissions;
    use std::os::unix::fs::PermissionsExt;

    fn mode(path: &std::path::Path) -> u32 {
        std::fs::metadata(path)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777
    }

    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "extenddb-tls-key-mode-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir(&dir).expect("scratch dir");
        dir
    }

    /// A 0644 key is set to 0600, and the warning names the key and its old
    /// mode. Its contents are untouched.
    #[test]
    fn a_key_readable_by_others_is_set_to_0600_with_a_warning() {
        let dir = scratch_dir("narrow");
        let key = dir.join("key.pem");
        std::fs::write(&key, b"key").expect("key");
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o644)).expect("chmod");
        let path = key.to_str().expect("utf-8 path");

        let warning = narrow_tls_key_permissions(path).expect("a warning");

        assert_eq!(mode(&key), 0o600);
        assert!(warning.contains(path), "{warning}");
        assert!(warning.contains("0644"), "{warning}");
        assert!(warning.contains("set it to 0600"), "{warning}");
        assert_eq!(std::fs::read(&key).expect("read"), b"key");
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// Group bits alone are enough to narrow the key.
    #[test]
    fn a_group_readable_key_is_set_to_0600() {
        let dir = scratch_dir("group");
        let key = dir.join("key.pem");
        std::fs::write(&key, b"key").expect("key");
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o640)).expect("chmod");

        assert!(narrow_tls_key_permissions(key.to_str().expect("utf-8 path")).is_some());
        assert_eq!(mode(&key), 0o600);
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// A key that is already owner-only is left as it is, with no warning,
    /// including one narrower than 0600.
    #[test]
    fn an_owner_only_key_is_left_alone() {
        let dir = scratch_dir("private");
        for m in [0o600, 0o400] {
            let key = dir.join(format!("key-{m:o}.pem"));
            std::fs::write(&key, b"key").expect("key");
            std::fs::set_permissions(&key, std::fs::Permissions::from_mode(m)).expect("chmod");

            assert_eq!(
                narrow_tls_key_permissions(key.to_str().expect("utf-8")),
                None
            );
            assert_eq!(mode(&key), m);
        }
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// A missing key gives no warning; loading it reports the error.
    #[test]
    fn a_missing_key_gives_no_warning() {
        let dir = scratch_dir("missing");
        let key = dir.join("key.pem");
        assert_eq!(
            narrow_tls_key_permissions(key.to_str().expect("utf-8")),
            None
        );
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// A key whose mode cannot be changed gets a warning with the manual fix,
    /// not an error, and its mode stays as it was. `/etc/hosts` stands in for
    /// a key another user owns: it is root's and 0644 on Linux and macOS.
    #[test]
    fn a_key_that_cannot_be_narrowed_gets_the_manual_fix() {
        use std::os::unix::fs::MetadataExt;

        let path = "/etc/hosts";
        let Ok(meta) = std::fs::metadata(path) else {
            eprintln!("skipped: no {path}");
            return;
        };
        // SAFETY: geteuid has no preconditions and cannot fail.
        let euid = unsafe { libc::geteuid() };
        if meta.uid() == euid || euid == 0 || meta.permissions().mode() & 0o077 == 0 {
            // This user could change its mode, or it is already private, so it
            // does not exercise the failure; never change a system file.
            eprintln!("skipped: {path} is not a wider file owned by another user");
            return;
        }
        let before = meta.permissions().mode() & 0o777;

        let warning = narrow_tls_key_permissions(path).expect("a warning");

        assert!(warning.contains("setting it to 0600 failed"), "{warning}");
        assert!(warning.contains("chmod 600 /etc/hosts"), "{warning}");
        assert_eq!(mode(std::path::Path::new(path)), before);
    }
}
