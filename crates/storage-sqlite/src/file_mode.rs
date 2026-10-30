// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Owner-only modes for a SQLite database file and the files SQLite keeps
//! beside it.
//!
//! The database stores the AES key next to the secrets it encrypts
//! (access-key secrets, password hashes), so neither the file nor its `-wal`,
//! `-shm` and `-journal` files may be readable by other local users. SQLite's
//! unix VFS creates a database file with mode 0644 less the umask, and creates
//! each sidecar with the mode of the database file. So every pool opener on a
//! file database calls [`prepare`] before it connects, which creates the
//! database file with mode 0600 (or sets an existing one to 0600) before the
//! connection can create a sidecar from it, and [`restrict`] after it
//! connects, which sets the file and every sidecar present to 0600 whoever
//! created them.
//!
//! As with the lock file in `serve_lock`, a mode that cannot be set is an
//! error. The directory's mode is left alone. On platforms other than Unix
//! both functions do nothing.

#[cfg(unix)]
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::str::FromStr;

#[cfg(unix)]
use crate::serve_lock::{database_file, resolve_dangling_symlinks};
#[cfg(unix)]
use crate::sqlite_util::sqlite_url;

/// Before a pool connects to `location`: create the database file with mode
/// 0600 if the connection would create it, then set the file and any sidecar
/// left by an earlier run to 0600, so the sidecars this connection creates
/// take mode 0600 from the database file.
///
/// A location that names no file on this machine (in-memory, unparseable, a
/// remote `file:` authority) is left to the connection, which reports it in
/// its own terms.
///
/// # Errors
///
/// A file's mode cannot be set.
#[cfg(unix)]
pub(crate) fn prepare(location: &str) -> Result<(), String> {
    let Ok(Some(path)) = database_file(location) else {
        return Ok(());
    };
    if open_creates_file(location) {
        create_owner_only(&resolve_dangling_symlinks(&path))?;
    }
    restrict_files(&path)
}

#[cfg(not(unix))]
pub(crate) fn prepare(_location: &str) -> Result<(), String> {
    Ok(())
}

/// After a pool connects to `location`: set the database file and every
/// `-wal`, `-shm` and `-journal` file present to mode 0600.
///
/// # Errors
///
/// A file's mode cannot be set.
#[cfg(unix)]
pub(crate) fn restrict(location: &str) -> Result<(), String> {
    match database_file(location) {
        Ok(Some(path)) => restrict_files(&path),
        _ => Ok(()),
    }
}

#[cfg(not(unix))]
pub(crate) fn restrict(_location: &str) -> Result<(), String> {
    Ok(())
}

/// Whether connecting to `location` creates the database file when it is
/// missing. sqlx reads the pairs after the first `?` as form-urlencoded and
/// passes `SQLITE_OPEN_CREATE` for `mode=rwc` unless `mode=ro` is also given;
/// a plain path always gets `mode=rwc` from `sqlite_url`. Two forms are left
/// to SQLite because they can keep it from creating a file on disk: a `file:`
/// URI with its own query (its `mode` can narrow the open) and a VFS outside
/// the unix family.
#[cfg(unix)]
fn open_creates_file(location: &str) -> bool {
    let url = sqlite_url(location);
    let Some((_, query)) = url.split_once('?') else {
        return false;
    };
    let (mut rwc, mut ro, mut unix_vfs) = (false, false, true);
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match &*key {
            "mode" => {
                rwc |= value == "rwc";
                ro |= value == "ro";
            }
            "vfs" => unix_vfs &= value.starts_with("unix"),
            _ => {}
        }
    }
    if !rwc || ro || !unix_vfs {
        return false;
    }
    sqlx::sqlite::SqliteConnectOptions::from_str(&url).is_ok_and(|options| {
        let name = options.get_filename().to_string_lossy();
        !(name.starts_with("file:") && name.contains('?'))
    })
}

/// Create `path` with mode 0600 if nothing is there yet.
#[cfg(unix)]
fn create_owner_only(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        // The umask applies to `mode` and can remove owner bits as well.
        Ok(file) => file
            .set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|e| {
                format!(
                    "cannot set the mode of SQLite database {}: {e}",
                    path.display()
                )
            }),
        // The file exists, or cannot be created here. SQLite's own open
        // decides what that means and reports it in its usual terms.
        Err(_) => Ok(()),
    }
}

/// Set the database file and each sidecar that exists to mode 0600.
#[cfg(unix)]
fn restrict_files(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    // SQLite follows symlinks before it names the sidecars, so they sit next
    // to the target, not next to the link.
    let base = std::fs::canonicalize(path).unwrap_or_else(|_| resolve_dangling_symlinks(path));
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let mut name = base.clone().into_os_string();
        name.push(suffix);
        let file = PathBuf::from(name);
        match std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(format!(
                    "cannot set the mode of SQLite file {}: {e}",
                    file.display()
                ));
            }
        }
    }
    Ok(())
}
