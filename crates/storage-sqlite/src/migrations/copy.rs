// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The copy of the database taken before the first write of a run that
//! applies anything.
//!
//! The copy is taken with SQLite's online backup API, on a connection to the
//! database and one to the copy, both opened for it on a blocking thread.
//! The backup reads the database, the committed frames of its WAL included,
//! in one read transaction, so the copy holds every write committed when
//! that transaction began, whatever another process writes meanwhile. It
//! takes time in proportion to the database's size. `VACUUM INTO`, which it
//! replaces, creates every table and index again, and its time grows faster
//! than the number of tables. The copy is then switched to rollback-journal
//! mode, as `VACUUM INTO` left it: the backup copies the database's WAL
//! mark, and a WAL database cannot be read without a `-shm` file beside it.
//!
//! Only SQLite opens the database file. POSIX advisory locks belong to a
//! process and a file, so closing any file descriptor of the database that
//! other code in this process opened would release every lock the process
//! holds on it, the shared lock of the runner's connection included. Another
//! process that closed its connection then would take itself for the last
//! one, checkpoint the WAL and delete it, and the commits the runner made
//! after that would go to the deleted file and be lost. SQLite keeps its own
//! descriptors of a file open while a connection of the process holds a lock
//! on it. The search for earlier copies below opens files under a copy's
//! name, so it skips a symbolic link and a hard link to the database, its
//! WAL or its index, which a restore with `ln` can leave under such a name.
//!
//! The copy is created owner-only (0600) before SQLite writes to it,
//! whatever the database's mode: the database holds the encryption key, and
//! a catalog that 0.1.x `init` created and no server opened is 0644.
//!
//! Every copy's name carries the UTC time the run took it,
//! `<db>.pre-migration-<from>-to-<to>-<YYYYMMDDTHHMMSSZ>`, and a name that
//! already exists is an error, so no run reuses or overwrites a copy.
//!
//! A copy is written to `<copy>.partial` first. A run holds the database's
//! lock before it copies, so a partial file beside the database when a run
//! starts was left by a run that was stopped while it copied; the run
//! removes every one, with the files SQLite keeps beside it, and names
//! them.
//!
//! A run that stops before it commits anything removes its copy, since the
//! database is unchanged; a run that committed a step keeps its copy and
//! names it in its error. A run that was killed after its copy was complete
//! and before it committed anything leaves the copy. Once the next run's
//! own copy is complete, the run removes such a copy, and names it, when
//! the copy has the same bytes as its own: the same database contents, so
//! its own copy holds everything the earlier one does, and runs killed in
//! that place one after the other leave one copy. A run that then stops
//! before its first commit removes its own copy as well and names the
//! earlier one with its error. The same `<from>` alone would not show that
//! nothing changed: a reconverge runs bodies again without changing the
//! catalog version, and `--forget` brings a catalog back to a version it had
//! before. A copy with a `-wal` or `-journal` file beside it, which a
//! process opened, is left as it is. Nothing else removes a copy.

use std::fs::File;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use extenddb_storage::migrations::MigrationError;
use sqlx::SqlitePool;

/// Free space a run needs, as a multiple of the database's size: the copy
/// takes one; migration 1's rebuild of `backup_items` writes the table again
/// and its WAL holds the new pages until the checkpoint. Measured on a
/// catalog of 250,000 backup items: a copy of 1.0 times the database, a WAL
/// of 1.0 times at migration 1's commit, and a database file of 2.0 times
/// after the run.
const SPACE_FACTOR: u64 = 3;

/// What a copy calls, or waits for, that tests replace.
#[derive(Clone, Copy)]
pub(crate) struct Hooks {
    /// The free bytes of a directory's filesystem.
    pub(crate) free_space: fn(&Path) -> Option<u64>,
    /// Runs between the sync and the rename; tests stop the process there.
    pub(crate) before_rename: fn(&Path),
    /// How long the copy waits while another connection holds a lock the
    /// backup needs.
    pub(crate) locked_for: Duration,
}

impl Hooks {
    /// The hooks of a run.
    pub(crate) const RUN: Self = Self {
        free_space,
        before_rename: no_hook,
        locked_for: Duration::from_secs(30),
    };
}

fn no_hook(_: &Path) {}

/// Take the copy at `copy`. The database is written to `<copy>.partial`,
/// synced, and renamed to `copy`, so a process that is stopped while it
/// copies leaves no file under the copy's name. Prints a line before the
/// copy with its path and the database's size, and one after it with the
/// time it took, since a run holds the lock while it copies. Returns the
/// lines for the operator: the partial copies of stopped runs it removed,
/// then the copy.
pub(crate) async fn pre_migration_copy(
    pool: &SqlitePool,
    db: &Path,
    copy: &Path,
    hooks: Hooks,
) -> Result<Vec<String>, MigrationError> {
    if std::fs::symlink_metadata(copy).is_ok() {
        return Err(MigrationError::Refused(format!(
            "{} already exists, and a copy taken before migrating is never reused or replaced; \
             run the migration again in a second, or move the file away",
            copy.display()
        )));
    }
    // Before the space check: the partial copies of stopped runs may be what
    // fills the disk.
    let mut lines = remove_stopped_partials(db);
    let live = live_size(pool).await?;
    let dir = parent_dir(copy);
    if let Some(free) = (hooks.free_space)(&dir) {
        refuse_short_space(live, free, &dir)?;
    }
    let partial = partial_path(copy);
    progress(&format!(
        "--- Copying the database ({live} bytes) to {}...",
        copy.display()
    ));
    let started = Instant::now();
    if let Err(e) = write_partial(db, copy, &partial, hooks.locked_for).await {
        remove_partial(&partial);
        return Err(e);
    }
    (hooks.before_rename)(&partial);
    if let Err(e) = std::fs::rename(&partial, copy) {
        remove_partial(&partial);
        return Err(MigrationError::store(
            format!("rename {} to {}", partial.display(), copy.display()),
            e,
        ));
    }
    // The rename is durable once the directory is synced.
    let _ = sync_dir(&dir).await;
    let seconds = started.elapsed().as_secs_f64();
    progress(&format!("--- Copied the database in {seconds:.1} s"));
    lines.push(format!(
        "copied the database to {} before migrating, in {seconds:.1} s",
        copy.display()
    ));
    Ok(lines)
}

/// Write the copy of `db` to `partial`, created owner-only, with SQLite's
/// online backup API on a blocking thread, and sync it: a large database
/// takes a while, and a server that migrates at startup goes on with its
/// other tasks meanwhile.
async fn write_partial(
    db: &Path,
    copy: &Path,
    partial: &Path,
    locked_for: Duration,
) -> Result<(), MigrationError> {
    create_partial(partial).map_err(|e| copy_failed(copy, e))?;
    let (source, target) = (db.to_path_buf(), partial.to_path_buf());
    tokio::task::spawn_blocking(move || backup::copy(&source, &target, locked_for))
        .await
        .map_err(|e| copy_failed(copy, e))?
        .map_err(|e| copy_failed(copy, e))?;
    sync_file(partial).await
}

fn copy_failed(copy: &Path, e: impl std::fmt::Display) -> MigrationError {
    MigrationError::store(format!("copy the database to {}", copy.display()), e)
}

/// SQLite's online backup API, which sqlx does not expose, on connections
/// opened for one copy and closed after it.
mod backup {
    use std::ffi::{CStr, CString, c_int};
    use std::path::Path;
    use std::time::{Duration, Instant};

    use libsqlite3_sys as ffi;

    /// A connection, closed when dropped.
    struct Connection(*mut ffi::sqlite3);

    impl Connection {
        /// Open the existing database file at `path` for reading and
        /// writing. SQLite creates no database file here.
        fn open(path: &Path) -> Result<Self, String> {
            let name = path
                .to_str()
                .and_then(|name| CString::new(name).ok())
                .ok_or_else(|| format!("{} is not a UTF-8 path without NUL", path.display()))?;
            let mut handle = std::ptr::null_mut();
            // SAFETY: `name` is NUL-terminated and outlives the call, and
            // `handle` is a valid place for the connection's pointer.
            let rc = unsafe {
                ffi::sqlite3_open_v2(
                    name.as_ptr(),
                    &raw mut handle,
                    ffi::SQLITE_OPEN_READWRITE,
                    std::ptr::null(),
                )
            };
            // SQLite returns a connection to close even when the open fails,
            // unless it could not allocate one.
            let connection = Self(handle);
            if rc != ffi::SQLITE_OK {
                return Err(format!(
                    "open {}: {}",
                    path.display(),
                    connection.last_error(rc)
                ));
            }
            // SAFETY: the connection is open.
            unsafe { ffi::sqlite3_busy_timeout(connection.0, 5_000) };
            Ok(connection)
        }

        /// Run `sql` on the connection; rows it returns are not read.
        fn execute(&self, sql: &CStr) -> Result<(), String> {
            // SAFETY: the connection is open and `sql` is NUL-terminated.
            // With no callback and no place for an error string, SQLite
            // allocates nothing for the caller to free, finalizes its own
            // statements, and leaves an error on the connection.
            let rc = unsafe {
                ffi::sqlite3_exec(
                    self.0,
                    sql.as_ptr(),
                    None,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            };
            if rc == ffi::SQLITE_OK {
                Ok(())
            } else {
                Err(self.last_error(rc))
            }
        }

        /// The text of the connection's last error, or of `rc` when SQLite
        /// returned no connection.
        fn last_error(&self, rc: c_int) -> String {
            if self.0.is_null() {
                return result_text(rc);
            }
            // SAFETY: the connection is open; `sqlite3_errmsg` returns a
            // NUL-terminated string that SQLite owns, copied here at once.
            unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(self.0)) }
                .to_string_lossy()
                .into_owned()
        }
    }

    impl Drop for Connection {
        fn drop(&mut self) {
            // SAFETY: the connection is not used after this, and closing a
            // null pointer does nothing. The connection has no prepared
            // statement, and `copy` finishes its backup before the
            // connections are dropped, so the close does not return
            // SQLITE_BUSY and leave the connection open.
            unsafe { ffi::sqlite3_close(self.0) };
        }
    }

    /// The English text of a result code.
    fn result_text(rc: c_int) -> String {
        // SAFETY: `sqlite3_errstr` takes any result code and returns a
        // static NUL-terminated string.
        unsafe { CStr::from_ptr(ffi::sqlite3_errstr(rc)) }
            .to_string_lossy()
            .into_owned()
    }

    /// Copy the database at `source` into the database file at `target`,
    /// which is empty, in one step: the source is read in one read
    /// transaction, so the target is the database as that transaction saw
    /// it. Waits up to `locked_for` while another connection holds a lock
    /// the backup needs. The target is then put in rollback-journal mode:
    /// the backup copies page 1 with the source's WAL mark, and a WAL
    /// database cannot be read without a `-shm` file beside it, so a copy
    /// in a directory its reader may not write could not be opened.
    pub(super) fn copy(source: &Path, target: &Path, locked_for: Duration) -> Result<(), String> {
        let from = Connection::open(source)?;
        let to = Connection::open(target)?;
        // SAFETY: both connections are open, used by this thread alone, and
        // outlive the backup, which is finished below.
        let backup =
            unsafe { ffi::sqlite3_backup_init(to.0, c"main".as_ptr(), from.0, c"main".as_ptr()) };
        if backup.is_null() {
            return Err(to.last_error(ffi::SQLITE_ERROR));
        }
        let deadline = Instant::now() + locked_for;
        let stepped = loop {
            // SAFETY: the backup is live until it is finished below.
            let rc = unsafe { ffi::sqlite3_backup_step(backup, -1) };
            if matches!(rc, ffi::SQLITE_BUSY | ffi::SQLITE_LOCKED) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            break rc;
        };
        // SAFETY: the backup is live and is not used after this call, which
        // frees it. It returns the last step's result unless that was
        // SQLITE_DONE, and leaves a failed step's error on `to`.
        let finished = unsafe { ffi::sqlite3_backup_finish(backup) };
        match stepped {
            ffi::SQLITE_DONE if finished == ffi::SQLITE_OK => {}
            ffi::SQLITE_DONE => return Err(to.last_error(finished)),
            ffi::SQLITE_BUSY | ffi::SQLITE_LOCKED => {
                return Err(format!(
                    "another connection held a lock on the database for {} s",
                    locked_for.as_secs()
                ));
            }
            _ => return Err(to.last_error(stepped)),
        }
        to.execute(c"PRAGMA journal_mode = DELETE")
    }
}

/// Create `partial` owner-only; it must not exist.
fn create_partial(partial: &Path) -> std::io::Result<File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(partial)
}

/// A line the operator reads while the run holds the lock. A closed stdout
/// does not stop the run.
fn progress(line: &str) {
    let _ = writeln!(std::io::stdout().lock(), "{line}");
}

/// The directory of `path`, `.` for a bare file name.
fn parent_dir(path: &Path) -> PathBuf {
    match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// Once this run keeps its copy (its first step committed), remove each
/// complete copy an earlier run took that has the same `<from>`, the same
/// size and the same bytes as this run's copy, and no `-wal` or `-journal`
/// file beside it. Such a copy holds the database contents this run's copy
/// holds, so it is a stopped run's copy that this one replaces. Reads the
/// files on a blocking thread. Returns one line naming each file removed,
/// and each one that could not be.
pub(crate) async fn remove_superseded(db: &Path, copy: &Path) -> Vec<String> {
    let (db, copy) = (db.to_path_buf(), copy.to_path_buf());
    tokio::task::spawn_blocking(move || superseded(&db, &copy))
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|path| match std::fs::remove_file(&path) {
            Ok(()) => format!(
                "removed {}, a copy an earlier run took of the same database contents, which \
                 this run's copy replaces",
                path.display()
            ),
            Err(e) => format!(
                "could not remove {} ({e}), a copy an earlier run took of the same database \
                 contents, which this run's copy replaces",
                path.display()
            ),
        })
        .collect()
}

/// The complete copies beside `db` that [`remove_superseded`] removes, in
/// name order.
fn superseded(db: &Path, copy: &Path) -> Vec<PathBuf> {
    let Some(prefix) = copy_prefix(db) else {
        return Vec::new();
    };
    let Some(name) = copy.file_name().and_then(|n| n.to_str()) else {
        return Vec::new();
    };
    let Some((from, _)) = copy_versions(&prefix, name) else {
        return Vec::new();
    };
    let Ok(size) = std::fs::metadata(copy).map(|m| m.len()) else {
        return Vec::new();
    };
    let Ok(entries) = std::fs::read_dir(parent_dir(db)) else {
        return Vec::new();
    };
    // A link under a copy's name to the database, its WAL or its index is
    // not a copy, and it is never opened: closing a descriptor of the
    // database that this code opened would release every lock the process
    // holds on it. A symbolic link is skipped by its own metadata, a hard
    // link by its device and inode.
    let database: Vec<std::fs::Metadata> = ["", "-wal", "-shm"]
        .iter()
        .filter_map(|suffix| {
            let mut name = db.as_os_str().to_owned();
            name.push(suffix);
            std::fs::metadata(PathBuf::from(name)).ok()
        })
        .collect();
    let mut found: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
                n != name && copy_versions(&prefix, n).is_some_and(|(f, _)| f == from)
            }) && !opened(path)
                && std::fs::symlink_metadata(path).is_ok_and(|m| {
                    m.file_type().is_file()
                        && m.len() == size
                        && !database.iter().any(|d| same_file(d, &m))
                })
        })
        .collect();
    found.sort();
    found.retain(|path| same_bytes(path, copy));
    found
}

/// Whether a `-wal` or `-journal` file is beside `copy`: a process opened it,
/// and its contents may not be the main file's alone.
fn opened(copy: &Path) -> bool {
    ["-wal", "-journal"].iter().any(|suffix| {
        let mut side = copy.as_os_str().to_owned();
        side.push(suffix);
        std::fs::symlink_metadata(PathBuf::from(side)).is_ok()
    })
}

/// Whether `a` and `b` are the metadata of one file.
#[cfg(unix)]
fn same_file(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    a.dev() == b.dev() && a.ino() == b.ino()
}

/// On Windows a lock belongs to the handle that took it, so closing
/// another handle of the file releases nothing; no link is told apart.
#[cfg(not(unix))]
fn same_file(_: &std::fs::Metadata, _: &std::fs::Metadata) -> bool {
    false
}

/// Whether the files at `a` and `b` hold the same bytes; `false` when either
/// cannot be read.
fn same_bytes(a: &Path, b: &Path) -> bool {
    let (Ok(mut a), Ok(mut b)) = (File::open(a), File::open(b)) else {
        return false;
    };
    let (mut x, mut y) = (vec![0; 1 << 20], vec![0; 1 << 20]);
    loop {
        let (Ok(n), Ok(m)) = (fill(&mut a, &mut x), fill(&mut b, &mut y)) else {
            return false;
        };
        if x[..n] != y[..m] {
            return false;
        }
        if n == 0 {
            return true;
        }
    }
}

/// Read from `file` until `buf` is full or the file ends; the bytes read.
fn fill(file: &mut File, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match file.read(&mut buf[filled..])? {
            0 => break,
            n => filled += n,
        }
    }
    Ok(filled)
}

/// `<db file name>.pre-migration-`, which every copy's name starts with.
fn copy_prefix(db: &Path) -> Option<String> {
    db.file_name()
        .and_then(|n| n.to_str())
        .map(|name| format!("{name}.pre-migration-"))
}

/// The `<from>` and `<to>` of a complete copy's name,
/// `<prefix><from>-to-<to>-<YYYYMMDDTHHMMSSZ>`; `None` for any other name,
/// a partial copy's included.
fn copy_versions<'a>(prefix: &str, name: &'a str) -> Option<(&'a str, u32)> {
    let rest = name.strip_prefix(prefix)?;
    let (from, rest) = rest.split_once("-to-")?;
    let (to, stamp) = rest.split_once('-')?;
    let stamp = stamp.as_bytes();
    let stamped = stamp.len() == 16
        && stamp[8] == b'T'
        && stamp[15] == b'Z'
        && stamp[..8]
            .iter()
            .chain(&stamp[9..15])
            .all(u8::is_ascii_digit);
    if !stamped || from.is_empty() {
        return None;
    }
    Some((from, to.parse().ok()?))
}

/// Remove every `<db>.pre-migration-*.partial` beside `db`, with the files
/// SQLite keeps beside it: the partial copies of runs that were stopped
/// while they copied.
/// Every run that holds the database's lock calls it, whether or not it
/// copies, so a run with nothing to apply removes them too. Returns one
/// line naming each file removed, and each one that could not be.
pub(crate) fn remove_stopped_partials(db: &Path) -> Vec<String> {
    let Some(name) = db.file_name().and_then(|n| n.to_str()) else {
        return Vec::new();
    };
    let dir = match db.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let prefix = format!("{name}.pre-migration-");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut found: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
                n.starts_with(&prefix)
                    && (n.ends_with(".partial")
                        || PARTIAL_SIDE_FILES
                            .iter()
                            .any(|suffix| n.ends_with(&format!(".partial{suffix}"))))
            })
        })
        .collect();
    found.sort();
    found
        .into_iter()
        .map(|path| match std::fs::remove_file(&path) {
            Ok(()) => format!(
                "removed {}, left by an earlier run that was stopped while it copied the database",
                path.display()
            ),
            Err(e) => format!(
                "could not remove {} ({e}), left by an earlier run that was stopped while it \
                 copied the database",
                path.display()
            ),
        })
        .collect()
}

/// Remove a copy that a run took and then did not need: it stopped before
/// it committed anything. Returns the line to print when the copy stays.
pub(crate) fn remove_copy(copy: &Path) -> Result<(), String> {
    std::fs::remove_file(copy).map_err(|e| {
        format!(
            "the copy taken before this run, {}, could not be removed ({e}); nothing was \
             committed, so it is not needed",
            copy.display()
        )
    })
}

/// `<copy>.partial`, where the copy is written before it is renamed.
fn partial_path(copy: &Path) -> PathBuf {
    let mut name = copy.as_os_str().to_owned();
    name.push(".partial");
    PathBuf::from(name)
}

/// The files SQLite may keep beside a partial copy: the rollback journal
/// while the backup writes, and the WAL and its index while the copy is
/// switched to rollback-journal mode.
const PARTIAL_SIDE_FILES: [&str; 3] = ["-journal", "-wal", "-shm"];

/// Remove a partial copy and the files SQLite keeps beside it.
fn remove_partial(partial: &Path) {
    let _ = std::fs::remove_file(partial);
    for suffix in PARTIAL_SIDE_FILES {
        let mut side = partial.as_os_str().to_owned();
        side.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(side));
    }
}

/// Sync the file at `path` to disk on a blocking thread, as the backup
/// runs. The file is opened for writing: on Windows `sync_all` is
/// `FlushFileBuffers`, which needs a handle with write access.
async fn sync_file(path: &Path) -> Result<(), MigrationError> {
    sync(path, true).await
}

/// Sync the directory `dir`, so that a rename in it is durable. A
/// directory is opened for reading, which Windows does not do with these
/// options; the caller ignores the error.
async fn sync_dir(dir: &Path) -> Result<(), MigrationError> {
    sync(dir, false).await
}

async fn sync(path: &Path, write: bool) -> Result<(), MigrationError> {
    let owned = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        std::fs::OpenOptions::new()
            .read(!write)
            .write(write)
            .open(&owned)
            .and_then(|f| f.sync_all())
    })
    .await
    .map_err(std::io::Error::other)
    .and_then(|synced| synced)
    .map_err(|e| MigrationError::store(format!("sync {}", path.display()), e))
}

/// Stop, with the figures, when `dir` has less than three times the
/// database's size free.
fn refuse_short_space(live: u64, free: u64, dir: &Path) -> Result<(), MigrationError> {
    let needed = live.saturating_mul(SPACE_FACTOR);
    if free < needed {
        return Err(MigrationError::Refused(format!(
            "migrating needs three times the database's size free, {needed} bytes for a \
             {live}-byte database: the copy taken before migrating, and the rewrite and WAL of \
             migration 1's backup_items rebuild. {} has {free} bytes free; free some space and \
             run the migration again",
            dir.display()
        )));
    }
    Ok(())
}

/// `<db>.pre-migration-<from>-to-<to>-<YYYYMMDDTHHMMSSZ>`, with `at` in UTC.
pub(crate) fn copy_path(db: &Path, from: &str, to: u32, at: SystemTime) -> PathBuf {
    let t = time::OffsetDateTime::from(at);
    let stamp = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    );
    let mut name = db.as_os_str().to_owned();
    name.push(format!(".pre-migration-{from}-to-{to}-{stamp}"));
    PathBuf::from(name)
}

async fn live_size(pool: &SqlitePool) -> Result<u64, MigrationError> {
    let pages: i64 = sqlx::query_scalar("PRAGMA page_count")
        .fetch_one(pool)
        .await
        .map_err(|e| MigrationError::store("PRAGMA page_count", e))?;
    let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
        .fetch_one(pool)
        .await
        .map_err(|e| MigrationError::store("PRAGMA page_size", e))?;
    Ok(u64::try_from(pages.saturating_mul(page_size)).unwrap_or(0))
}

/// Bytes available to this process on the filesystem holding `dir`.
#[cfg(unix)]
// The statvfs field types differ by platform.
#[allow(clippy::useless_conversion, clippy::unnecessary_cast)]
pub(crate) fn free_space(dir: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    // SAFETY: an all-zero statvfs is a valid value of a plain C struct.
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `path` is a NUL-terminated string and `stat` a valid struct,
    // both live for the duration of the call.
    let rc = unsafe { libc::statvfs(path.as_ptr(), &raw mut stat) };
    (rc == 0).then(|| (stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

#[cfg(not(unix))]
pub(crate) fn free_space(_dir: &Path) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::{copy_path, copy_versions, free_space, refuse_short_space};
    use std::path::Path;
    use std::time::{Duration, SystemTime};

    /// A lock another connection holds past the wait stops the copy, and
    /// the error says how long it waited: `sqlite3_backup_finish` returns
    /// the last step's SQLITE_BUSY, which alone reads "database is locked".
    #[tokio::test]
    async fn a_lock_held_past_the_wait_stops_the_copy_and_says_so() {
        let db = crate::migrations::tests::temp_db("copy-busy");
        let pool = crate::migrations::tests::plain_pool(&db).await;
        sqlx::raw_sql(
            "PRAGMA journal_mode = DELETE; CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1);",
        )
        .execute(&pool)
        .await
        .expect("a database");
        let mut holder = pool.acquire().await.expect("a connection");
        sqlx::raw_sql("BEGIN EXCLUSIVE")
            .execute(&mut *holder)
            .await
            .expect("an exclusive lock");
        let target = std::path::PathBuf::from(format!("{}.copy", db.display()));
        super::create_partial(&target).expect("the target");
        let source = db.to_path_buf();
        let started = std::time::Instant::now();
        let err = tokio::task::spawn_blocking(move || {
            super::backup::copy(&source, &target, Duration::from_secs(1))
        })
        .await
        .expect("the copy's thread")
        .expect_err("the lock is held");
        assert_eq!(
            err,
            "another connection held a lock on the database for 1 s"
        );
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "{:?}",
            started.elapsed()
        );
        sqlx::raw_sql("ROLLBACK")
            .execute(&mut *holder)
            .await
            .expect("release");
        drop(holder);
        pool.close().await;
    }

    #[test]
    fn a_complete_copy_name_gives_its_versions_and_no_other_name_does() {
        let prefix = "x.sqlite.pre-migration-";
        let name = |rest: &str| format!("{prefix}{rest}");
        assert_eq!(
            copy_versions(prefix, &name("legacy-to-4-20261029T061706Z")),
            Some(("legacy", 4))
        );
        assert_eq!(
            copy_versions(prefix, &name("3-to-4-20261029T061706Z")),
            Some(("3", 4))
        );
        for other in [
            "3-to-4-20261029T061706Z.partial",
            "3-to-4-20261029T061706Z.partial-journal",
            "3-to-4-20261029T061706Z.partial-wal",
            "3-to-4-20261029T061706Z.partial-shm",
            "3-to-4-20261029T061706Z-wal",
            "3-to-4-2026102",
            "-to-4-20261029T061706Z",
            "3-to-x-20261029T061706Z",
            "3-4-20261029T061706Z",
        ] {
            assert_eq!(copy_versions(prefix, &name(other)), None, "{other}");
        }
        assert_eq!(
            copy_versions(prefix, "y.sqlite.pre-migration-3-to-4-20261029T061706Z"),
            None
        );
    }

    #[test]
    fn a_short_filesystem_is_refused_with_the_figures() {
        let err = refuse_short_space(4096, 12287, Path::new("/data")).expect_err("short");
        let message = err.to_string();
        assert!(
            message.contains("three times the database's size"),
            "{message}"
        );
        assert!(
            message.contains("12288 bytes for a 4096-byte database"),
            "{message}"
        );
        assert!(message.contains("/data has 12287 bytes free"), "{message}");
        assert_eq!(err.exit_code(), extenddb_storage::migrations::exit::ERROR);
        refuse_short_space(4096, 12288, Path::new("/data")).expect("enough");
    }

    #[test]
    fn the_free_space_probe_reads_the_filesystem() {
        let free = free_space(&std::env::temp_dir());
        if cfg!(unix) {
            assert!(free.is_some_and(|n| n > 0), "{free:?}");
        }
    }

    #[test]
    fn copy_names_carry_the_utc_time() {
        // 2026-10-29T06:17:06Z
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_793_254_626);
        assert_eq!(
            copy_path(Path::new("/var/db/x.sqlite"), "legacy", 4, at),
            Path::new("/var/db/x.sqlite.pre-migration-legacy-to-4-20261029T061706Z")
        );
    }
}
