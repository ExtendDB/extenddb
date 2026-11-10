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

/// How the key warnings name the key: by its setting, not its path. The TLS
/// load error leaves the path out so that it stays out of logs that may be
/// aggregated (docs/troubleshooting.md), and `serve --foreground` prints these
/// warnings to the same stream.
const TLS_KEY: &str = "TLS private key (server.tls.key_path)";

/// The last sentence of the warning for a key that was narrowed: while it was
/// readable it may have been copied.
const TLS_KEY_ROTATE: &str = "Other users could read it while its mode was wider; if that \
     matters, replace the key and certificate (see \"TLS private key readable by other users\" \
     in docs/troubleshooting.md).";

/// The last sentence of the warnings for a key whose mode did not change: it
/// stays readable, and a key replaced before its mode is fixed lands at the
/// same mode.
const TLS_KEY_STILL_OPEN: &str = "Other users can read it until its mode is fixed; after \
     that, replace the key and certificate, since it may already have been copied (see \"TLS \
     private key readable by other users\" in docs/troubleshooting.md).";

/// A key another user owns, as the advice for it needs it.
#[derive(Debug, Clone, Copy)]
struct OtherOwner {
    uid: u32,
    /// The key's group.
    gid: u32,
    /// Whether this process is in the key's group, so that removing other
    /// access keeps its own.
    server_in_group: bool,
}

/// [`narrow_tls_key_permissions`] on the key a server with this TLS section
/// loads: `None` without a look at the key when the server loads none (TLS
/// off, or a `dev-mode` build, which serves plain HTTP), and the key path with
/// a leading `~` expanded otherwise.
pub fn narrow_served_tls_key(tls: &extenddb_config::TlsConfig) -> Option<String> {
    if !extenddb_config::serves_tls(tls) {
        return None;
    }
    narrow_tls_key_permissions(&extenddb_config::expand_tilde(&tls.key_path))
}

/// Set the TLS private key at `key_path` to mode 0600 when other users can
/// read it, and return the warning for the caller to print.
///
/// `init` creates the key 0600, but an older `init` that stopped between
/// writing the key and setting its mode left it at the umask mode, and an
/// operator can copy a key in the same way. `serve` and the `init` path that
/// keeps an existing key both call this.
///
/// Returns `None` when the key is missing (whoever loads it reports that),
/// when it is already 0600 or narrower, and when another user owns it and
/// only that owner and its group can read it: that is a key the server reads
/// through its group, a mode the owner chose and this server cannot change.
///
/// Narrowing a key this server owns removes group and other access, so a
/// service that read it through its group can no longer read it; the warning
/// says so. The mode change follows a symlink and applies to the file it
/// names, unlike `init`'s writes, which refuse to write through a symlink.
///
/// A key whose mode cannot be changed gets a warning with advice for the
/// cause rather than an error, because the process may still be able to read
/// it and refusing to start would be a regression for those deployments.
pub fn narrow_tls_key_permissions(key_path: &str) -> Option<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let meta = std::fs::metadata(key_path).ok()?;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 == 0 {
            return None;
        }
        // SAFETY: geteuid has no preconditions and cannot fail.
        let euid = unsafe { libc::geteuid() };
        let other_owner = (meta.uid() != euid).then(|| OtherOwner {
            uid: meta.uid(),
            gid: meta.gid(),
            server_in_group: process_in_group(meta.gid()),
        });
        if other_owner.is_some() && mode & 0o007 == 0 {
            return None;
        }
        Some(
            match std::fs::set_permissions(key_path, std::fs::Permissions::from_mode(0o600)) {
                Ok(()) => narrowed_warning(
                    mode,
                    std::fs::metadata(key_path)
                        .ok()
                        .map(|m| m.permissions().mode() & 0o777),
                ),
                Err(e) => failed_warning(mode, other_owner, server_gid(), &e),
            },
        )
    }
    #[cfg(not(unix))]
    {
        let _ = key_path;
        None
    }
}

/// The warning after a mode change that returned success, from the mode
/// before it and the mode read back after it. Some filesystems accept a mode
/// change and keep no mode (vfat mounted with `quiet`, some SMB and 9p
/// mounts), so success is the mode read back, not the call's result.
fn narrowed_warning(before: u32, after: Option<u32>) -> String {
    match after {
        Some(0o600) => format!(
            "WARNING: {TLS_KEY} had permissions {before:04o}, which let other users read it. \
             It is now 0600: group and other access were removed, so only its owner can read \
             it. {TLS_KEY_ROTATE}"
        ),
        after => format!(
            "WARNING: {TLS_KEY} has permissions {before:04o}, which let other users read it. \
             Setting it to 0600 was accepted but did not take effect (its mode is now {}), so \
             its filesystem does not keep file modes. Keep the key on a filesystem that does, \
             or mount it with an owner-only mode. {TLS_KEY_STILL_OPEN}",
            after.map_or_else(|| "unknown".to_owned(), |m| format!("{m:04o}"))
        ),
    }
}

/// The warning when the mode change failed with `error`, with the fix for its
/// cause. `other_owner` is set when another user owns the key; `server_gid`
/// is this process's effective group.
fn failed_warning(
    mode: u32,
    other_owner: Option<OtherOwner>,
    server_gid: u32,
    error: &std::io::Error,
) -> String {
    if error.kind() == std::io::ErrorKind::ReadOnlyFilesystem {
        // A Secret volume's files are root's unless the pod sets `fsGroup`,
        // and the image runs as uid 10001, so 0400 would lock the server out.
        return format!(
            "WARNING: {TLS_KEY} has permissions {mode:04o}, which let other users read it, and \
             it is on a read-only filesystem, so its mode cannot be changed here. Set the mode \
             where the file is provided, for example `defaultMode: 0440` on a Kubernetes Secret \
             volume with the pod's `fsGroup` set to this server's group (gid {server_gid}). \
             {TLS_KEY_STILL_OPEN}"
        );
    }
    if let Some(owner) = other_owner {
        let OtherOwner { uid, gid, .. } = owner;
        if owner.server_in_group {
            return format!(
                "WARNING: {TLS_KEY} has permissions {mode:04o}, which let every user read it. \
                 Another user (uid {uid}) owns it, so this server cannot change its mode. Ask \
                 its owner to remove other access with `chmod o-rwx` on the file; this server \
                 reads it through its group (gid {gid}), which keeps that access. \
                 {TLS_KEY_STILL_OPEN}"
            );
        }
        return format!(
            "WARNING: {TLS_KEY} has permissions {mode:04o}, which let every user read it. \
             Another user (uid {uid}) owns it, so this server cannot change its mode, and this \
             server is not in its group (gid {gid}): it reads the key through the access every \
             user has, so removing that alone would stop it reading the key. Ask its owner to \
             give the key to this server's group and then remove other access: \
             `chgrp {server_gid}` and then `chmod 640` on the file. {TLS_KEY_STILL_OPEN}"
        );
    }
    format!(
        "WARNING: {TLS_KEY} has permissions {mode:04o}, which let other users read it, and \
         setting it to 0600 failed: {error}. Fix it with `chmod 600` on the file. \
         {TLS_KEY_STILL_OPEN}"
    )
}

/// This process's effective group.
#[cfg(unix)]
fn server_gid() -> u32 {
    // SAFETY: getegid has no preconditions and cannot fail.
    unsafe { libc::getegid() }
}

/// Whether `gid` is this process's effective group or one of its
/// supplementary groups: the groups whose bits decide its access to a file.
#[cfg(unix)]
fn process_in_group(gid: u32) -> bool {
    if gid == server_gid() {
        return true;
    }
    // SAFETY: a zero-length call writes nothing and returns the count.
    let count = unsafe { libc::getgroups(0, std::ptr::null_mut()) };
    let Ok(len) = usize::try_from(count) else {
        return false;
    };
    let mut groups: Vec<libc::gid_t> = vec![0; len];
    // SAFETY: `groups` has room for `count` entries.
    let filled = unsafe { libc::getgroups(count, groups.as_mut_ptr()) };
    let Ok(filled) = usize::try_from(filled) else {
        return false;
    };
    groups.truncate(filled);
    groups.contains(&gid)
}

#[cfg(all(test, unix))]
mod tls_key_permission_tests {
    use super::{
        OtherOwner, TLS_KEY, TLS_KEY_ROTATE, TLS_KEY_STILL_OPEN, failed_warning,
        narrow_served_tls_key, narrow_tls_key_permissions, narrowed_warning, process_in_group,
    };
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

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

    /// A key file in a scratch directory with mode `m`.
    fn key_with_mode(dir: &std::path::Path, m: u32) -> std::path::PathBuf {
        let key = dir.join("key.pem");
        std::fs::write(&key, b"key").expect("key");
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(m)).expect("chmod");
        key
    }

    fn euid() -> u32 {
        // SAFETY: geteuid has no preconditions and cannot fail.
        unsafe { libc::geteuid() }
    }

    /// A 0644 key is set to 0600 with the whole warning: the old mode, that
    /// group and other access were removed, and the rotation pointer. The
    /// path is not in it. Its contents are untouched.
    #[test]
    fn a_key_readable_by_others_is_set_to_0600_with_a_warning() {
        let dir = scratch_dir("narrow");
        let key = key_with_mode(&dir, 0o644);
        let path = key.to_str().expect("utf-8 path");

        let warning = narrow_tls_key_permissions(path).expect("a warning");

        assert_eq!(mode(&key), 0o600);
        assert_eq!(
            warning,
            format!(
                "WARNING: {TLS_KEY} had permissions 0644, which let other users read it. It is \
                 now 0600: group and other access were removed, so only its owner can read it. \
                 {TLS_KEY_ROTATE}"
            )
        );
        assert!(!warning.contains(path), "{warning}");
        assert_eq!(std::fs::read(&key).expect("read"), b"key");
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// Group bits alone are enough to narrow a key this user owns.
    #[test]
    fn a_group_readable_key_is_set_to_0600() {
        let dir = scratch_dir("group");
        let key = key_with_mode(&dir, 0o640);

        let warning = narrow_tls_key_permissions(key.to_str().expect("utf-8 path"));

        assert!(
            warning
                .as_deref()
                .is_some_and(|w| w.contains("had permissions 0640,")),
            "{warning:?}"
        );
        assert_eq!(mode(&key), 0o600);
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    /// A key reached through a symlink: the file it names is narrowed.
    #[test]
    fn a_symlinked_key_has_its_target_narrowed() {
        let dir = scratch_dir("symlink");
        let key = key_with_mode(&dir, 0o644);
        let link = dir.join("link.pem");
        std::os::unix::fs::symlink(&key, &link).expect("symlink");

        assert!(narrow_tls_key_permissions(link.to_str().expect("utf-8 path")).is_some());
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

    /// The first of `candidates` that another user owns, with a mode that
    /// `wanted` accepts, and that this user cannot change (not root).
    fn file_of_another_user(candidates: &[&str], wanted: fn(u32) -> bool) -> Option<String> {
        let euid = euid();
        if euid == 0 {
            return None;
        }
        candidates.iter().find_map(|path| {
            let meta = std::fs::metadata(path).ok()?;
            (meta.uid() != euid && wanted(meta.permissions().mode() & 0o777))
                .then(|| (*path).to_owned())
        })
    }

    /// A key another user owns that every user can read gets advice that keeps
    /// this server's access: `chmod o-rwx` when the server is in the key's
    /// group, and `chgrp` then `chmod 640` when it reads the key through the
    /// other bits. Never `chmod 600`. Its mode stays as it was. `/etc/hosts`
    /// stands in for such a key: it is root's and 0644 on Linux and macOS.
    #[test]
    fn a_world_readable_key_of_another_user_gets_advice_that_keeps_access() {
        let Some(path) = file_of_another_user(&["/etc/hosts"], |m| m & 0o004 != 0) else {
            eprintln!("skipped: no world-readable file of another user");
            return;
        };
        let meta = std::fs::metadata(&path).expect("metadata");
        let before = mode(std::path::Path::new(&path));

        let warning = narrow_tls_key_permissions(&path).expect("a warning");

        assert!(warning.contains("Another user (uid "), "{warning}");
        if process_in_group(meta.gid()) {
            assert!(warning.contains("`chmod o-rwx`"), "{warning}");
        } else {
            assert!(warning.contains("`chmod 640`"), "{warning}");
            assert!(warning.contains("`chgrp "), "{warning}");
            assert!(!warning.contains("chmod o-rwx"), "{warning}");
        }
        assert!(!warning.contains("chmod 600"), "{warning}");
        assert!(!warning.contains(&path), "{warning}");
        assert!(warning.ends_with(TLS_KEY_STILL_OPEN), "{warning}");
        assert_eq!(mode(std::path::Path::new(&path)), before);
    }

    /// A key another user owns that only its owner and group can read is the
    /// layout where the server reads it through its group: no warning, and
    /// no attempt to change it. `/etc/sudoers` (0440) and `/etc/shadow`
    /// (0640) stand in for such a key.
    #[test]
    fn a_group_readable_key_of_another_user_is_left_alone() {
        let Some(path) = file_of_another_user(&["/etc/sudoers", "/etc/shadow"], |m| {
            m & 0o070 != 0 && m & 0o007 == 0
        }) else {
            eprintln!("skipped: no group-only file of another user");
            return;
        };
        let before = mode(std::path::Path::new(&path));

        assert_eq!(narrow_tls_key_permissions(&path), None);
        assert_eq!(mode(std::path::Path::new(&path)), before);
    }

    /// A mode change that returned success but left the key wide is reported
    /// as not having taken effect, not as done.
    #[test]
    fn a_mode_change_the_filesystem_dropped_is_not_reported_as_done() {
        let warning = narrowed_warning(0o644, Some(0o644));
        assert!(
            warning.contains("did not take effect (its mode is now 0644)"),
            "{warning}"
        );
        assert!(!warning.contains("It is now 0600"), "{warning}");
        assert!(warning.ends_with(TLS_KEY_STILL_OPEN), "{warning}");
        assert!(narrowed_warning(0o644, None).contains("its mode is now unknown"));
        assert!(narrowed_warning(0o644, Some(0o600)).ends_with(TLS_KEY_ROTATE));
    }

    /// The group check counts the effective group and every supplementary
    /// group, and no other.
    #[test]
    fn the_group_check_knows_this_processes_groups() {
        // SAFETY: getegid has no preconditions and cannot fail.
        let egid = unsafe { libc::getegid() };
        assert!(process_in_group(egid));
        // SAFETY: a zero-length call writes nothing and returns the count.
        let count = unsafe { libc::getgroups(0, std::ptr::null_mut()) };
        let mut groups: Vec<libc::gid_t> = vec![0; usize::try_from(count).expect("count")];
        // SAFETY: `groups` has room for `count` entries.
        let filled = unsafe { libc::getgroups(count, groups.as_mut_ptr()) };
        groups.truncate(usize::try_from(filled).expect("filled"));
        for gid in &groups {
            assert!(process_in_group(*gid), "gid {gid}");
        }
        let outside = (1..u32::MAX)
            .rev()
            .find(|gid| *gid != egid && !groups.contains(gid))
            .expect("a gid this process is not in");
        assert!(!process_in_group(outside), "gid {outside}");
    }

    /// The advice follows the cause of a failed change, and keeps the
    /// server's access to the key: a read-only mount gets `defaultMode: 0440`
    /// with `fsGroup` (0400 would leave a root-owned Secret file unreadable to
    /// the server), another owner gets `chmod o-rwx` when the server is in the
    /// key's group and `chgrp` then `chmod 640` when it is not, and anything
    /// else `chmod 600`. None of them says the key was readable only in the
    /// past.
    #[test]
    fn a_failed_change_gets_the_fix_for_its_cause() {
        let read_only = std::io::Error::from(std::io::ErrorKind::ReadOnlyFilesystem);
        let denied = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        let in_group = OtherOwner {
            uid: 0,
            gid: 7,
            server_in_group: true,
        };
        let outside = OtherOwner {
            server_in_group: false,
            ..in_group
        };

        let warning = failed_warning(0o644, None, 10001, &read_only);
        assert!(warning.contains("read-only filesystem"));
        assert!(warning.contains("`defaultMode: 0440`"));
        assert!(warning.contains("`fsGroup` set to this server's group (gid 10001)"));
        assert!(!warning.contains("0400"));
        assert!(!warning.contains("chmod"));

        let warning = failed_warning(0o644, Some(in_group), 10001, &denied);
        assert!(warning.contains("`chmod o-rwx`"));
        assert!(warning.contains("through its group (gid 7)"));
        assert!(!warning.contains("chmod 600"));

        let warning = failed_warning(0o644, Some(outside), 10001, &denied);
        assert!(warning.contains("not in its group (gid 7)"));
        assert!(warning.contains("`chgrp 10001` and then `chmod 640`"));
        assert!(!warning.contains("chmod o-rwx"));
        assert!(!warning.contains("chmod 600"));

        let warning = failed_warning(0o644, None, 10001, &denied);
        assert!(warning.contains("Fix it with `chmod 600` on the file."));

        for warning in [
            failed_warning(0o644, None, 10001, &read_only),
            failed_warning(0o644, Some(in_group), 10001, &denied),
            failed_warning(0o644, Some(outside), 10001, &denied),
            failed_warning(0o644, None, 10001, &denied),
        ] {
            assert!(warning.contains("permissions 0644,"));
            assert!(warning.ends_with(TLS_KEY_STILL_OPEN));
            assert!(!warning.contains("while its mode was wider"));
        }
    }

    /// `serve`'s check: the configured key, with `~` expanded, when the
    /// server loads one, and no look at it when TLS is off or in a `dev-mode`
    /// build, which serves plain HTTP.
    #[test]
    fn serve_narrows_the_key_it_loads_only() {
        let dir = scratch_dir("served");
        let key = key_with_mode(&dir, 0o644);
        let tls = |key_path: String, enabled: bool| extenddb_config::TlsConfig {
            enabled,
            key_path,
            ..extenddb_config::TlsConfig::default()
        };
        let absolute = key.to_str().expect("utf-8 path").to_owned();

        assert_eq!(narrow_served_tls_key(&tls(absolute.clone(), false)), None);
        assert_eq!(mode(&key), 0o644);

        let served = narrow_served_tls_key(&tls(absolute.clone(), true));
        if cfg!(feature = "dev-mode") {
            assert_eq!(served, None);
            assert_eq!(mode(&key), 0o644);
        } else {
            assert!(served.is_some());
            assert_eq!(mode(&key), 0o600);
        }

        // The same key named from `~`: `~/` and one `..` per component of
        // $HOME, then the path from `/`. Unexpanded, `~` is a directory name
        // that does not exist, and there is no key to narrow.
        // The kernel resolves `..` from the directory a symlink names, so the
        // count comes from $HOME with its symlinks resolved.
        if !cfg!(feature = "dev-mode")
            && let Ok(home) = std::env::var("HOME")
            && let Ok(home) = std::fs::canonicalize(&home)
        {
            std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o644)).expect("chmod");
            let ups = home
                .components()
                .filter(|c| matches!(c, std::path::Component::Normal(_)))
                .count();
            let from_tilde = format!("~/{}{}", "../".repeat(ups), &absolute[1..]);

            assert!(narrow_served_tls_key(&tls(from_tilde, true)).is_some());
            assert_eq!(mode(&key), 0o600);
        }
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }
}
