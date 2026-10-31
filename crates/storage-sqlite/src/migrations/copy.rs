// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The copy of the database taken before the first write of a run that
//! applies anything.
//!
//! `VACUUM INTO 'file:<copy>?modeof=<db>'` gives the copy the database's file
//! mode: the database holds the encryption key and is 0600, and a plain
//! `VACUUM INTO` writes a file with the process umask's mode (0644 measured).
//!
//! Every copy's name carries the UTC time the run took it,
//! `<db>.pre-migration-<from>-to-<to>-<YYYYMMDDTHHMMSSZ>`, and a name that
//! already exists is an error, so no run keeps or replaces another run's
//! copy. A run that stops before it commits anything removes its copy, since
//! the database is unchanged; a run that committed a step keeps its copy and
//! names it in its error. Nothing else removes a copy.
//!
//! A copy is written to `<copy>.partial` first. A run holds the database's
//! lock before it copies, so a partial file beside the database when a run
//! starts was left by a run that was stopped while it copied; the run
//! removes every one, with its journal, and names them.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use extenddb_storage::migrations::MigrationError;
use sqlx::SqlitePool;

/// Free space a run needs, as a multiple of the database's size: the copy
/// takes one; migration 1's rebuild of `backup_items` writes the table again
/// and its WAL holds the new pages until the checkpoint. Measured on a
/// catalog of 250,000 backup items: a copy of 1.0 times the database, a WAL
/// of 1.0 times at migration 1's commit, and a database file of 2.0 times
/// after the run.
const SPACE_FACTOR: u64 = 3;

/// Take the copy at `copy`. `free_space` reads the free bytes of a
/// directory's filesystem ([`free_space`] outside tests). The database is
/// written to `<copy>.partial`, synced, and renamed to `copy`, so a process
/// that is stopped while it copies leaves no file under the copy's name;
/// `before_rename` runs between the sync and the rename (tests stop the
/// process there). Returns the lines for the operator: the partial copies
/// of stopped runs it removed, then the copy.
pub(crate) async fn pre_migration_copy(
    pool: &SqlitePool,
    db: &Path,
    copy: &Path,
    free_space: fn(&Path) -> Option<u64>,
    before_rename: fn(&Path),
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
    let dir = match copy.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    if let Some(free) = free_space(&dir) {
        refuse_short_space(live, free, &dir)?;
    }
    let partial = partial_path(copy);
    let target = format!("file:{}?modeof={}", uri_path(&partial), uri_path(db));
    let copied = sqlx::query("VACUUM INTO ?")
        .bind(&target)
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| MigrationError::store(format!("copy the database to {}", copy.display()), e))
        .and_then(|()| sync_file(&partial));
    if let Err(e) = copied {
        remove_partial(&partial);
        return Err(e);
    }
    before_rename(&partial);
    if let Err(e) = std::fs::rename(&partial, copy) {
        remove_partial(&partial);
        return Err(MigrationError::store(
            format!("rename {} to {}", partial.display(), copy.display()),
            e,
        ));
    }
    // The rename is durable once the directory is synced.
    let _ = sync_file(&dir);
    lines.push(format!(
        "copied the database to {} before migrating; nothing removes the copy",
        copy.display()
    ));
    Ok(lines)
}

/// Remove every `<db>.pre-migration-*.partial` beside `db`, with its
/// journal: the partial copies of runs that were stopped while they copied.
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
                    && (n.ends_with(".partial") || n.ends_with(".partial-journal"))
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

/// Remove a partial copy and the rollback journal `VACUUM INTO` keeps
/// beside it while it writes.
fn remove_partial(partial: &Path) {
    let _ = std::fs::remove_file(partial);
    let mut journal = partial.as_os_str().to_owned();
    journal.push("-journal");
    let _ = std::fs::remove_file(PathBuf::from(journal));
}

fn sync_file(path: &Path) -> Result<(), MigrationError> {
    std::fs::File::open(path)
        .and_then(|f| f.sync_all())
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

/// A path as the path part of a SQLite URI filename.
fn uri_path(path: &Path) -> String {
    let mut out = String::new();
    for byte in path.to_string_lossy().bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{copy_path, free_space, refuse_short_space, uri_path};
    use std::path::Path;
    use std::time::{Duration, SystemTime};

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
    fn copy_names_carry_the_utc_time_and_uri_paths_are_escaped() {
        // 2026-10-29T06:17:06Z
        let at = SystemTime::UNIX_EPOCH + Duration::from_secs(1_793_254_626);
        assert_eq!(
            copy_path(Path::new("/var/db/x.sqlite"), "legacy", 4, at),
            Path::new("/var/db/x.sqlite.pre-migration-legacy-to-4-20261029T061706Z")
        );
        assert_eq!(uri_path(Path::new("/a b/c?d#e%")), "/a%20b/c%3Fd%23e%25");
    }
}
