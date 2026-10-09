// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! One owner per SQLite database file.
//!
//! Writers on this backend are serialized by a lock inside the server process
//! (`SqliteEngine::write_lock`), and startup recovery assumes no other server
//! is using the file: it removes restore targets left CREATING and rebuilds
//! indexes left mid-build, which would destroy the work of a second server
//! that is still running. So `extenddb serve` takes an exclusive advisory lock
//! on `<database>.lock` before touching the database, holds it for the life of
//! the process, and refuses to start if another process holds it.
//!
//! The commands that replace or rewrite the file take the same lock for their
//! duration: `init` and `migrate` (through the bootstrapper's migration lock)
//! and `destroy` (before it unlinks the file). Read-only commands (`settings`,
//! `manage`, `verify`, `status`, ...) do not, and run alongside a server.
//!
//! The lock file is named after the file SQLite actually opens, not after the
//! configured string: the location is parsed exactly as sqlx parses it
//! (`sqlite:` URL forms, percent-encoding, `file:` URIs), and the resulting
//! path is canonicalized so two spellings of one file, or a symlink and its
//! target, resolve to one lock file.
//!
//! The lock is `flock(2)` on Unix: released by the kernel when the process
//! exits, however it exits, so a crash never leaves a stale lock behind. The
//! lock file itself is left in place; its presence means nothing. On other
//! platforms no lock is taken and a warning is logged. `flock` is advisory and
//! its behaviour on network filesystems depends on the server and mount
//! options; a database on NFS or similar is outside what this check promises.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use sqlx::sqlite::SqliteConnectOptions;

use crate::sqlite_util::sqlite_url;

/// The database file a configured SQLite location refers to, resolved the way
/// the engine resolves it, or `Ok(None)` for an in-memory database.
///
/// # Errors
///
/// The location does not parse as a SQLite connection string, or names a
/// `file:` URI with an authority this process cannot map to a local path.
pub(crate) fn database_file(location: &str) -> Result<Option<PathBuf>, String> {
    let url = sqlite_url(location);
    let options = SqliteConnectOptions::from_str(&url)
        .map_err(|e| format!("cannot parse SQLite location {location:?}: {e}"))?;
    // `mode=memory` is recorded in a private field; read it off the query the
    // same way sqlx does (form-urlencoded pairs after the first `?`).
    let mode_memory = url
        .split_once('?')
        .map(|(_, q)| {
            q.split('&')
                .filter_map(|kv| kv.split_once('='))
                .any(|(k, v)| k == "mode" && v == "memory")
        })
        .unwrap_or(false);
    let filename = options.get_filename().to_string_lossy().into_owned();
    if mode_memory
        || filename == ":memory:"
        || filename.starts_with("file::memory:")
        || filename.starts_with("file:sqlx-in-memory-")
    {
        return Ok(None);
    }
    // sqlx opens every name with SQLITE_OPEN_URI, so a `file:` name is a
    // SQLite URI: `file:/abs`, `file:rel`, `file:///abs`, `file://localhost/abs`,
    // each with an optional `?query`. Anything else must be a plain path.
    let path = match filename.strip_prefix("file:") {
        Some(rest) => {
            let rest = rest.split('?').next().unwrap_or(rest);
            let rest = rest.split('#').next().unwrap_or(rest);
            match rest.strip_prefix("//") {
                Some(after_slashes) => {
                    let (authority, path) = after_slashes
                        .find('/')
                        .map_or((after_slashes, ""), |i| after_slashes.split_at(i));
                    if !(authority.is_empty() || authority == "localhost") {
                        return Err(format!(
                            "cannot lock SQLite location {location:?}: `file:` URI authority \
                             {authority:?} is not a local path"
                        ));
                    }
                    path.to_owned()
                }
                None => rest.to_owned(),
            }
        }
        None => filename,
    };
    if path.is_empty() {
        return Err(format!(
            "cannot lock SQLite location {location:?}: it names no database file"
        ));
    }
    Ok(Some(PathBuf::from(path)))
}

/// Holds the lock until dropped.
#[derive(Debug)]
pub(crate) struct ServeLock {
    _file: File,
    path: PathBuf,
}

impl ServeLock {
    /// Path of the lock file for a database file: `<canonical db path>.lock`.
    ///
    /// The database file is canonicalized if it exists. Otherwise symlinks in
    /// the path are followed by hand (a dangling link to a file that `init`
    /// has not created yet must still name the target, or a server started
    /// through the link and one started on the target would each lock a
    /// different file), and then the parent directory is canonicalized, so
    /// the first `serve` against a not-yet-created file and every later one
    /// agree on the lock. A parent that does not exist either is left as
    /// written; opening the database fails on it anyway.
    pub(crate) fn lock_path(db_path: &Path) -> PathBuf {
        let canonical = std::fs::canonicalize(db_path).unwrap_or_else(|_| {
            let resolved = resolve_dangling_symlinks(db_path);
            match (resolved.parent(), resolved.file_name()) {
                (Some(parent), Some(name)) if !parent.as_os_str().is_empty() => {
                    std::fs::canonicalize(parent)
                        .map(|p| p.join(name))
                        .unwrap_or_else(|_| resolved.clone())
                }
                (_, Some(name)) => std::env::current_dir()
                    .map(|cwd| cwd.join(name))
                    .unwrap_or_else(|_| resolved.clone()),
                _ => resolved.clone(),
            }
        });
        let mut name = canonical.into_os_string();
        name.push(".lock");
        PathBuf::from(name)
    }

    /// Take the lock for `db_path`, or report who holds it.
    ///
    /// # Errors
    ///
    /// A message for the operator if another process holds the lock or the
    /// lock file cannot be opened (for example, the database's directory is
    /// not writable; the lock file lives next to the database).
    pub(crate) fn acquire(db_path: &Path) -> Result<Self, String> {
        let path = Self::lock_path(db_path);
        let file = open_options_0600()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| {
                format!(
                    "cannot open lock file {}: {e} (the directory holding a SQLite database \
                     must be writable so the server can create its lock file)",
                    path.display()
                )
            })?;
        // The database and its sidecars are 0600 so other local users cannot
        // read secrets out of them. The lock file must match: flock needs
        // only a read handle, so a world-readable lock file lets any local
        // user hold the lock and keep the server from starting. `create`
        // honours the umask, and the file may predate this rule, so set the
        // mode explicitly on every open.
        restrict_to_owner(&file, &path)?;
        try_lock_exclusive(&file).map_err(|e| {
            if e.kind() == std::io::ErrorKind::WouldBlock {
                format!(
                    "another extenddb process is already using {} (lock held on {}); \
                     stop it first, or point this command at a different database",
                    db_path.display(),
                    path.display()
                )
            } else {
                format!("cannot lock {}: {e}", path.display())
            }
        })?;
        Ok(Self { _file: file, path })
    }

    /// Take the lock for a configured location, or `Ok(None)` if the location
    /// is an in-memory database that belongs to this process alone.
    ///
    /// # Errors
    ///
    /// As [`ServeLock::acquire`], plus a location that cannot be resolved to
    /// a file.
    pub(crate) fn acquire_for_location(location: &str) -> Result<Option<Self>, String> {
        match database_file(location)? {
            Some(path) => Self::acquire(&path).map(Some),
            None => Ok(None),
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

/// Follow symlinks along `path` as far as they go, by hand. `canonicalize`
/// refuses a path whose final target does not exist; this returns that
/// target instead, so a dangling link still names the file it will become.
fn resolve_dangling_symlinks(path: &Path) -> PathBuf {
    let mut current = path.to_path_buf();
    // Bounded like the kernel's own symlink-loop limit.
    for _ in 0..40 {
        match std::fs::read_link(&current) {
            Ok(target) => {
                current = if target.is_absolute() {
                    target
                } else {
                    current
                        .parent()
                        .map_or_else(|| target.clone(), |p| p.join(&target))
                };
            }
            Err(_) => break,
        }
    }
    current
}

#[cfg(unix)]
fn open_options_0600() -> std::fs::OpenOptions {
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = std::fs::OpenOptions::new();
    options.mode(0o600);
    options
}

#[cfg(not(unix))]
fn open_options_0600() -> std::fs::OpenOptions {
    std::fs::OpenOptions::new()
}

#[cfg(unix)]
fn restrict_to_owner(file: &File, path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(|e| format!("cannot set the mode of lock file {}: {e}", path.display()))
}

#[cfg(not(unix))]
fn restrict_to_owner(_file: &File, _path: &Path) -> Result<(), String> {
    Ok(())
}

#[cfg(unix)]
fn try_lock_exclusive(file: &File) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    // SAFETY: `flock` takes a file descriptor and flags and touches no memory
    // the caller owns. The descriptor is valid: it belongs to `file`, which is
    // borrowed for the duration of the call.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn try_lock_exclusive(_file: &File) -> std::io::Result<()> {
    tracing::warn!(
        "no file lock is taken on this platform: nothing prevents a second extenddb \
         process from opening the same SQLite database"
    );
    Ok(())
}

#[cfg(test)]
mod database_file_tests {
    use super::database_file;
    use std::path::PathBuf;

    fn file(location: &str) -> Option<PathBuf> {
        database_file(location).expect(location)
    }

    #[test]
    fn plain_paths_and_sqlite_urls() {
        assert_eq!(
            file("/var/lib/x.sqlite"),
            Some(PathBuf::from("/var/lib/x.sqlite"))
        );
        assert_eq!(file("rel.sqlite"), Some(PathBuf::from("rel.sqlite")));
        assert_eq!(
            file("sqlite:///var/lib/x.sqlite?mode=rwc"),
            Some(PathBuf::from("/var/lib/x.sqlite"))
        );
        assert_eq!(file("sqlite:rel.sqlite"), Some(PathBuf::from("rel.sqlite")));
    }

    #[test]
    fn percent_encoding_is_decoded_as_sqlx_decodes_it() {
        assert_eq!(
            file("sqlite:///tmp/a%20b.sqlite"),
            Some(PathBuf::from("/tmp/a b.sqlite"))
        );
        // An encoded `?` is part of the file name, not a query.
        assert_eq!(
            file("sqlite:///tmp/x%3Fmode=memory.db"),
            Some(PathBuf::from("/tmp/x?mode=memory.db"))
        );
    }

    #[test]
    fn file_uris_resolve_to_the_path_sqlite_opens() {
        assert_eq!(
            file("file:/tmp/db.sqlite"),
            Some(PathBuf::from("/tmp/db.sqlite"))
        );
        assert_eq!(
            file("file:///tmp/db.sqlite"),
            Some(PathBuf::from("/tmp/db.sqlite"))
        );
        assert_eq!(
            file("file://localhost/tmp/db.sqlite"),
            Some(PathBuf::from("/tmp/db.sqlite"))
        );
        assert_eq!(file("file:rel.sqlite"), Some(PathBuf::from("rel.sqlite")));
        assert!(database_file("file://otherhost/tmp/db.sqlite").is_err());
    }

    #[test]
    fn a_disk_file_named_like_a_memory_parameter_is_still_a_file() {
        assert_eq!(
            file("/tmp/mode=memory.db"),
            Some(PathBuf::from("/tmp/mode=memory.db"))
        );
    }

    #[test]
    fn in_memory_forms_take_no_lock() {
        for mem in [
            ":memory:",
            "sqlite::memory:",
            "file::memory:",
            "sqlite://x?mode=memory",
        ] {
            assert_eq!(file(mem), None, "{mem}");
        }
        // `sqlite_url` appends `?mode=rwc` to a bare path, so a path carrying
        // its own query is not something the engine can open either; the lock
        // reports the same parse error the engine would.
        assert!(database_file("file::memory:?cache=shared").is_err());
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::ServeLock;

    fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "extenddb-serve-lock-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).expect("dir");
        dir
    }

    #[test]
    fn a_second_lock_on_the_same_database_is_refused() {
        let dir = temp_dir();
        let db = dir.join("db.sqlite");
        let first = ServeLock::acquire(&db).expect("first lock");
        let err = ServeLock::acquire(&db).expect_err("second lock is refused");
        assert!(err.contains("another extenddb process"), "{err}");
        drop(first);
        let again = ServeLock::acquire(&db).expect("free again once released");
        drop(again);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn different_databases_do_not_conflict() {
        let dir = temp_dir();
        let la = ServeLock::acquire(&dir.join("a.sqlite")).expect("a");
        let lb = ServeLock::acquire(&dir.join("b.sqlite")).expect("b");
        drop((la, lb));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn spellings_of_one_file_share_a_lock() {
        let dir = temp_dir();
        let db = dir.join("db.sqlite");
        std::fs::write(&db, b"").expect("db file");
        let via_dots = dir.join("sub/../db.sqlite");
        std::fs::create_dir_all(dir.join("sub")).expect("sub");
        let first = ServeLock::acquire(&db).expect("first lock");
        assert!(
            ServeLock::acquire(&via_dots).is_err(),
            "`..` spelling bypassed"
        );
        drop(first);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_lock_file_is_readable_by_its_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = temp_dir();
        let db = dir.join("db.sqlite");
        let lock = ServeLock::acquire(&db).expect("lock");
        let mode = std::fs::metadata(lock.path())
            .expect("stat")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "fresh lock file mode {mode:o}");
        drop(lock);
        // A lock file left by an earlier build with a wider mode is tightened.
        std::fs::set_permissions(
            ServeLock::lock_path(&db),
            std::fs::Permissions::from_mode(0o644),
        )
        .expect("widen");
        let lock = ServeLock::acquire(&db).expect("lock again");
        let mode = std::fs::metadata(lock.path())
            .expect("stat")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "existing lock file mode {mode:o}");
        drop(lock);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_dangling_symlink_and_its_future_target_share_a_lock() {
        let dir = temp_dir();
        let target = dir.join("target.sqlite");
        let link = dir.join("link.sqlite");
        std::os::unix::fs::symlink(&target, &link).expect("symlink");
        assert!(!target.exists(), "the target must not exist yet");
        let via_link = ServeLock::acquire(&link).expect("lock through the link");
        assert!(
            ServeLock::acquire(&target).is_err(),
            "dangling symlink bypassed: target locked while the link is held"
        );
        drop(via_link);
        let via_target = ServeLock::acquire(&target).expect("lock on the target");
        assert!(
            ServeLock::acquire(&link).is_err(),
            "dangling symlink bypassed: link locked while the target is held"
        );
        drop(via_target);
        // A chain of links, the last one dangling.
        let link2 = dir.join("link2.sqlite");
        std::os::unix::fs::symlink("link.sqlite", &link2).expect("relative symlink");
        let via_link2 = ServeLock::acquire(&link2).expect("lock through two links");
        assert!(
            ServeLock::acquire(&target).is_err(),
            "two-link chain bypassed"
        );
        drop(via_link2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_symlink_and_its_target_share_a_lock() {
        let dir = temp_dir();
        let real = dir.join("real.sqlite");
        std::fs::write(&real, b"").expect("db file");
        let link = dir.join("link.sqlite");
        std::os::unix::fs::symlink(&real, &link).expect("symlink");
        let first = ServeLock::acquire(&real).expect("first lock");
        assert!(ServeLock::acquire(&link).is_err(), "file symlink bypassed");
        drop(first);

        let real_dir = dir.join("realdir");
        std::fs::create_dir_all(&real_dir).expect("realdir");
        let link_dir = dir.join("linkdir");
        std::os::unix::fs::symlink(&real_dir, &link_dir).expect("dir symlink");
        // The database does not exist yet: the parent is what gets canonicalized.
        let first = ServeLock::acquire(&real_dir.join("new.sqlite")).expect("first lock");
        assert!(
            ServeLock::acquire(&link_dir.join("new.sqlite")).is_err(),
            "directory symlink bypassed"
        );
        drop(first);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
