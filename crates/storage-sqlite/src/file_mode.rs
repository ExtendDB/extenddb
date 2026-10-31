// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Owner-only modes for a SQLite database file and the files SQLite keeps
//! beside it.
//!
//! The database stores the AES key next to the secrets it encrypts
//! (access-key secrets, password hashes), so neither the file nor its `-wal`,
//! `-shm` and `-journal` files may be readable by other local users. SQLite's
//! unix VFS creates a database file with mode 0644 less the umask, and creates
//! each sidecar with the mode of the database file. So every pool on a file
//! database is opened through [`connect`], which calls [`prepare`] before the
//! pool's first connection and [`restrict`] after it. `prepare` creates the
//! database file with mode 0600 (or narrows an existing one to 0600) before the
//! connection can create a sidecar from it; `restrict` narrows the file and
//! every sidecar present to 0600 whoever created them.
//!
//! Only a regular file with a permission bit outside 0600 is changed, and it is
//! set to 0600. Anything else at one of those names is left alone: a symlink
//! (never followed), a directory, a FIFO, a socket, a device, or a regular file
//! already 0600 or narrower. As with the lock file in `serve_lock`, a regular
//! file that cannot be narrowed is an error. The directory's mode is left
//! alone. On platforms other than Unix `prepare` and `restrict` do nothing.

#[cfg(unix)]
use std::path::{Path, PathBuf};
#[cfg(unix)]
use std::str::FromStr;

#[cfg(unix)]
use crate::serve_lock::{database_file, resolve_dangling_symlinks};
use crate::sqlite_util::sqlite_url;

/// Open a pool on `location` (a path, `:memory:` or a `sqlite:` URL) with
/// `options`, keeping the database file and its sidecars owner read/write
/// only.
///
/// # Errors
///
/// The pool cannot connect, or a file's mode cannot be set.
pub(crate) async fn connect(
    location: &str,
    options: sqlx::sqlite::SqlitePoolOptions,
) -> Result<sqlx::SqlitePool, String> {
    prepare(location)?;
    let pool = options
        .connect(&sqlite_url(location))
        .await
        .map_err(|e| e.to_string())?;
    restrict(location)?;
    Ok(pool)
}

/// The calls that change a mode. [`SYSTEM`] makes them; tests substitute them
/// to read a file's mode before it changes, or to make the change fail.
#[cfg(unix)]
struct ModeCalls<'a> {
    /// Sets a file `prepare` has just created to 0600, through its descriptor.
    created: &'a dyn Fn(&std::fs::File) -> std::io::Result<()>,
    /// Sets an existing regular file to 0600, given the path to change (on
    /// Linux, the file's handle under `/proc/self/fd`).
    existing: &'a dyn Fn(&Path) -> std::io::Result<()>,
}

#[cfg(unix)]
const SYSTEM: ModeCalls<'static> = ModeCalls {
    created: &set_created_owner_only,
    existing: &set_existing_owner_only,
};

/// Before the pool connects to `location`: create the database file with
/// mode 0600 if the connection would create it, then narrow the file and any
/// sidecar left by an earlier run to 0600, so the sidecars this connection
/// creates take mode 0600 from the database file.
///
/// A location that names no file on this machine (in-memory, unparseable, a
/// remote `file:` authority) is left to the connection, which reports it in
/// its own terms.
///
/// # Errors
///
/// A file's mode cannot be set.
#[cfg(unix)]
fn prepare(location: &str) -> Result<(), String> {
    prepare_with(location, &SYSTEM)
}

#[cfg(unix)]
fn prepare_with(location: &str, calls: &ModeCalls<'_>) -> Result<(), String> {
    let Some(opened) = opened_file(location) else {
        return Ok(());
    };
    if opened.created {
        create_owner_only(&resolve_dangling_symlinks(&opened.path), calls)?;
    }
    restrict_files(&opened.path, calls)
}

#[cfg(not(unix))]
fn prepare(_location: &str) -> Result<(), String> {
    Ok(())
}

/// After the pool connects to `location`: narrow the database file and every
/// `-wal`, `-shm` and `-journal` file present to mode 0600.
///
/// # Errors
///
/// A file's mode cannot be set.
#[cfg(unix)]
fn restrict(location: &str) -> Result<(), String> {
    restrict_with(location, &SYSTEM)
}

#[cfg(unix)]
fn restrict_with(location: &str, calls: &ModeCalls<'_>) -> Result<(), String> {
    match opened_file(location) {
        Some(opened) => restrict_files(&opened.path, calls),
        None => Ok(()),
    }
}

#[cfg(not(unix))]
fn restrict(_location: &str) -> Result<(), String> {
    Ok(())
}

/// The file a connection to a location opens on disk.
#[cfg(unix)]
struct OpenedFile {
    path: PathBuf,
    /// Whether the connection creates the file when it is missing.
    created: bool,
}

/// The file a connection to `location` opens on disk, or `None` for a
/// location left to the connection: in memory, unparseable, a remote `file:`
/// authority, or a `file:` name with `vfs` or `immutable` in the query. sqlx
/// wraps such a name in a second `file:` URI, so SQLite opens a relative path
/// spelled `file:...`, not the file the URI names.
///
/// sqlx reads the pairs after the first `?` as form-urlencoded: any
/// `mode=rwc` passes `SQLITE_OPEN_CREATE` unless a `mode=ro` is also given,
/// and the last `vfs` and the last `immutable` win. A plain path always gets
/// `mode=rwc` from `sqlite_url`. A `file:` URI's own query is then read as
/// SQLite reads it (`sqlite_uri`), and the file is created only if SQLite
/// would create it on disk, with a VFS in the unix family.
#[cfg(unix)]
fn opened_file(location: &str) -> Option<OpenedFile> {
    let path = database_file(location).ok()??;
    let url = sqlite_url(location);
    let (mut rwc, mut ro, mut vfs, mut immutable) = (false, false, None, false);
    if let Some((_, query)) = url.split_once('?') {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            match &*key {
                "mode" => {
                    rwc |= value == "rwc";
                    ro |= value == "ro";
                }
                "vfs" => vfs = Some(value),
                "immutable" => immutable = matches!(&*value, "true" | "1"),
                _ => {}
            }
        }
    }
    let options = sqlx::sqlite::SqliteConnectOptions::from_str(&url).ok()?;
    let name = options.get_filename().to_string_lossy();
    let uri = name.strip_prefix("file:");
    if uri.is_some() && (vfs.is_some() || immutable) {
        return None;
    }
    let created = rwc
        && !ro
        && vfs.is_none_or(|vfs| vfs.starts_with("unix"))
        && uri.is_none_or(|rest| {
            crate::sqlite_uri::FileUri::parse(rest).is_ok_and(|uri| uri.creates_file())
        });
    Some(OpenedFile { path, created })
}

/// Create `path` with mode 0600 if nothing is there yet. `create_new` refuses
/// any existing entry, a symlink included, so nothing is followed or replaced.
#[cfg(unix)]
fn create_owner_only(path: &Path, calls: &ModeCalls<'_>) -> Result<(), String> {
    use std::os::unix::fs::OpenOptionsExt;
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        // The umask applies to `mode` and can remove owner bits as well.
        Ok(file) => (calls.created)(&file).map_err(|e| {
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

/// Set a file this module has just created to 0600. Closing its descriptor
/// afterwards releases no lock: the file is new, so no SQLite connection of
/// this process has it open.
#[cfg(unix)]
fn set_created_owner_only(file: &std::fs::File) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
}

/// Narrow the database file and each sidecar of it to mode 0600 where needed.
/// The sidecars are named after the database file's resolved path, because
/// SQLite resolves symlinks before it names them.
#[cfg(unix)]
fn restrict_files(path: &Path, calls: &ModeCalls<'_>) -> Result<(), String> {
    let base = std::fs::canonicalize(path).unwrap_or_else(|_| resolve_dangling_symlinks(path));
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let mut name = base.clone().into_os_string();
        name.push(suffix);
        restrict_file(&PathBuf::from(name), calls)?;
    }
    Ok(())
}

/// Whether `metadata` is a regular file with a permission bit outside 0600.
#[cfg(unix)]
fn wider_than_owner_only(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.file_type().is_file() && metadata.permissions().mode() & 0o7777 & !0o600 != 0
}

#[cfg(unix)]
fn mode_error(file: &Path, e: &std::io::Error) -> String {
    format!("cannot set the mode of SQLite file {}: {e}", file.display())
}

/// Set `file` to 0600 if it is a regular file with a permission bit outside
/// 0600, and leave anything else at the name alone (see the module docs).
///
/// The name is opened with `O_PATH | O_NOFOLLOW`, which opens no file for
/// reading or writing: a symlink yields a handle on the link itself, a FIFO
/// or a device is not opened, so nothing waits, and closing the handle keeps
/// the POSIX locks this process's SQLite connections hold on the file, which
/// closing a descriptor opened for reading would release. The type and mode
/// are read from the handle and the mode is changed through
/// `/proc/self/fd/<n>`, so the check and the change act on the same file.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn restrict_file(file: &Path, calls: &ModeCalls<'_>) -> Result<(), String> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;
    let handle = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_PATH | libc::O_NOFOLLOW)
        .open(file)
    {
        Ok(handle) => handle,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(mode_error(file, &e)),
    };
    let metadata = handle.metadata().map_err(|e| mode_error(file, &e))?;
    if !wider_than_owner_only(&metadata) {
        return Ok(());
    }
    let by_handle = Path::new("/proc/self/fd").join(handle.as_raw_fd().to_string());
    (calls.existing)(&by_handle).map_err(|e| mode_error(file, &e))
}

/// `chmod` of a `/proc/self/fd/<n>` name changes the file the handle refers
/// to, whatever is at the original name by then.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn set_existing_owner_only(by_handle: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(by_handle, std::fs::Permissions::from_mode(0o600)).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            std::io::Error::new(
                e.kind(),
                format!("{} is not available ({e})", by_handle.display()),
            )
        } else {
            e
        }
    })
}

/// As the Linux version, on a Unix without `O_PATH` (macOS, the BSDs): the
/// type and mode are read with `lstat` and the mode is changed with
/// `fchmodat(AT_SYMLINK_NOFOLLOW)`, which follows no link and opens nothing,
/// so no descriptor is closed on a file SQLite holds locks on. The two calls
/// name the same directory entry; replacing that entry between them takes
/// write access to the database's directory.
#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
fn restrict_file(file: &Path, calls: &ModeCalls<'_>) -> Result<(), String> {
    let metadata = match std::fs::symlink_metadata(file) {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(mode_error(file, &e)),
    };
    if !wider_than_owner_only(&metadata) {
        return Ok(());
    }
    (calls.existing)(file).map_err(|e| mode_error(file, &e))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
fn set_existing_owner_only(file: &Path) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(file.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    // SAFETY: `fchmodat` reads the NUL-terminated name, which `name` owns for
    // the duration of the call, and touches no other memory.
    let rc = unsafe {
        libc::fchmodat(
            libc::AT_FDCWD,
            name.as_ptr(),
            0o600,
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(all(test, unix))]
mod tests;
