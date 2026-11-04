// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Writing an `ExportTableToPointInTime` file, and removing it when the export
//! does not complete.
//!
//! `handle_export_table` creates the file with `create_new` and hands it to
//! [`write_export_file`], which writes one line per item and returns only
//! once the file is complete on disk. tokio's `File` runs each write on the
//! blocking pool and reports its error to the next write or flush on the
//! file, so the last write can still be running when `write_all` returns.
//! The file is therefore flushed, which waits for that write and returns its
//! error, and then synced. `sync_all` on its own waits for the last write but
//! does not return its error (tokio keeps only the error's kind for a later
//! write or flush), so an export whose last line hit a full disk would answer
//! `COMPLETED` with a short file.
//!
//! The directory that holds the file is synced after the file, because a
//! file's fsync does not make its new directory entry durable on every
//! filesystem. Only that directory is synced. The handler refuses a
//! `FilePath` whose parent does not exist, so the directories above the file
//! existed before the export, and the export adds an entry to none of them.
//! The exception is the account directory the handler creates on an
//! account's first export: its entry in the export root is not synced here.
//! When the directory cannot be opened for its sync (`EACCES` or `EPERM`, as
//! for a directory the server can write to but not read) or its filesystem
//! has no fsync for directories (`EINVAL`, `ENOTSUP`, `EOPNOTSUPP` or
//! `EBADF`), the export logs a warning and completes without the directory
//! sync; the file itself has been synced. Every other error, `EIO` among
//! them, fails the export. `PostgreSQL` skips `EACCES` at the open and
//! `EBADF` and `EINVAL` at the fsync, and logs nothing; the export also skips
//! `EPERM`, `ENOTSUP` and `EOPNOTSUPP`, and logs a warning.
//!
//! An export that does not complete removes its file. A guard created before
//! the first page is read removes it on every error, when the export's future
//! is dropped (a client that disconnects), and on a panic. The handler opened
//! the file with `create_new`, so nothing else was at the path then, but the
//! name can change hands while the export runs: someone can remove the file
//! and another export can create its own at the same path. On Unix the guard
//! therefore records the device and inode of the open file, and unlinks the
//! name only while `symlink_metadata` of the path reports the same pair; when
//! the name is gone or names another file, it leaves the path alone. The
//! check and the unlink are two calls, so a file put at the path between them
//! is removed. Recording the pair needs a spare file descriptor; if that
//! fails, the export fails before it writes a line and the guard unlinks the
//! name without the check. On other targets the guard unlinks the name
//! without the check. The unlink never follows a symlink.
//!
//! Three cases leave a file: a future dropped while tokio's open is still
//! running on the blocking pool, before the handler holds the file (that open
//! cannot hand back a guard), a process that ends during the export, and a
//! file the server could not remove, which it logs as a warning.

use std::future::Future;
use std::path::{Path, PathBuf};

use extenddb_core::error::DynamoDbError;
use extenddb_core::types::Item;
use tokio::io::AsyncWriteExt;

/// Write every item `next_page` returns to `file`, one line each, then flush
/// and sync the file and sync the directory that holds it. Returns the number
/// of items written.
///
/// `file` is the file the caller has just created at `path` with
/// `create_new`. Unless this returns `Ok`, the file is removed while `path`
/// still names it, including when the returned future is dropped before it
/// finishes. On Unix, when the file's device and inode cannot be read (there
/// is no spare file descriptor), this fails before it writes a line and
/// unlinks `path` without checking what it names. On other targets `path` is
/// always unlinked without that check.
///
/// `next_page` is called with the last key of the previous page (`None` for
/// the first) and answers one page of items and its last key; a page without
/// a last key is the final one.
///
/// # Errors
///
/// `Export item count exceeds maximum` when the table holds more than
/// `max_items` items, `Cannot write export file` when the file's identity
/// cannot be read or a write, the flush or a sync fails, and any error
/// `next_page` returns.
pub(crate) async fn write_export_file<F, Fut>(
    file: &mut tokio::fs::File,
    path: &Path,
    max_items: u64,
    mut next_page: F,
) -> Result<i64, DynamoDbError>
where
    F: FnMut(Option<Item>) -> Fut,
    Fut: Future<Output = Result<(Vec<Item>, Option<Item>), DynamoDbError>>,
{
    let mut created = RemoveOnDrop::new(path);
    created.record_identity(file).map_err(|_| cannot_write())?;

    let mut item_count: i64 = 0;
    let mut exclusive_start_key: Option<Item> = None;
    loop {
        let (items, last_key) = next_page(exclusive_start_key.take()).await?;

        item_count += i64::from(u16::try_from(items.len()).unwrap_or(u16::MAX));

        if u64::try_from(item_count).unwrap_or(u64::MAX) > max_items {
            return Err(DynamoDbError::ValidationException(format!(
                "Export item count exceeds maximum ({max_items})"
            )));
        }

        for item in &items {
            // One encoding for the export line and the backup data file.
            let line = extenddb_core::backup_format::encode_item_line(item).map_err(|e| {
                tracing::error!(internal_error = %e, "failed to serialize export item");
                DynamoDbError::InternalServerError("Internal server error".to_owned())
            })?;
            file.write_all(&line).await.map_err(|_| cannot_write())?;
        }

        if last_key.is_none() {
            break;
        }
        exclusive_start_key = last_key;
    }

    // Waits for the last write and returns its error, which `sync_all` would
    // not (see the module comment).
    file.flush().await.map_err(|_| cannot_write())?;
    // Defence in depth, no test: an fsync failure cannot be injected without
    // a faulty filesystem, and the fsync matters only across a crash or a
    // power loss.
    file.sync_all().await.map_err(|_| cannot_write())?;
    sync_parent(path).await.map_err(|_| cannot_write())?;

    created.disarm();
    Ok(item_count)
}

fn cannot_write() -> DynamoDbError {
    DynamoDbError::ValidationException("Cannot write export file".to_owned())
}

/// Fsync the directory that holds `path`, so the entry `create_new` made for
/// the file survives a crash. Opening the directory needs read permission on
/// it. An error `can_skip_dir_sync` accepts is logged, and the directory is
/// left unsynced. Windows has no directory handle to sync.
async fn sync_parent(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let dir = match path.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir,
            _ => Path::new("."),
        };
        let synced = match tokio::fs::File::open(dir).await {
            Ok(handle) => handle.sync_all().await.map_err(|e| (DirStep::Fsync, e)),
            Err(e) => Err((DirStep::Open, e)),
        };
        if let Err((step, e)) = synced {
            if !can_skip_dir_sync(step, &e) {
                return Err(e);
            }
            tracing::warn!(
                path = %dir.display(),
                error = %e,
                "could not sync the directory of an export file; the export completes without it"
            );
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

/// The call of the directory sync that failed.
#[cfg(unix)]
#[derive(Clone, Copy, Debug)]
enum DirStep {
    /// Opening the directory for reading.
    Open,
    /// The directory's fsync.
    Fsync,
}

/// Whether an export completes without its directory sync after `step`
/// failed with `err`: the directory cannot be opened (`EACCES`, `EPERM`), or
/// its filesystem has no fsync for directories (`EINVAL`, `ENOTSUP`,
/// `EOPNOTSUPP`, `EBADF`). Every other error, `EIO` among them, fails the
/// export.
#[cfg(unix)]
fn can_skip_dir_sync(step: DirStep, err: &std::io::Error) -> bool {
    let Some(code) = err.raw_os_error() else {
        return false;
    };
    let skipped: &[i32] = match step {
        DirStep::Open => &[libc::EACCES, libc::EPERM],
        // `ENOTSUP` and `EOPNOTSUPP` are one number on Linux and two on macOS.
        DirStep::Fsync => &[libc::EINVAL, libc::ENOTSUP, libc::EOPNOTSUPP, libc::EBADF],
    };
    skipped.contains(&code)
}

/// Removes the export file from `Drop` unless disarmed. Every error return of
/// [`write_export_file`], its future being dropped at any await, and a panic
/// end here. The check and the unlink are synchronous because `Drop` cannot
/// await. A failure of either is logged: the export has already failed with
/// an error of its own, and the file stays at the path.
struct RemoveOnDrop {
    path: PathBuf,
    /// The device and inode of the file this export created, once recorded.
    #[cfg(unix)]
    created: Option<(u64, u64)>,
    armed: bool,
}

impl RemoveOnDrop {
    fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            #[cfg(unix)]
            created: None,
            armed: true,
        }
    }

    /// Record the device and inode of `file`, the open export file, so the
    /// guard unlinks `path` only while it names that file. Reading them from
    /// the open file takes a duplicate of its descriptor. Nothing to record
    /// on other targets.
    fn record_identity(&mut self, file: &tokio::fs::File) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            use std::os::fd::AsFd;
            let file = std::fs::File::from(file.as_fd().try_clone_to_owned()?);
            self.created = Some(file_identity(&file.metadata()?));
        }
        #[cfg(not(unix))]
        {
            let _ = file;
        }
        Ok(())
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    /// Whether `path` still names the file this export created. Without a
    /// recorded identity, and on targets other than Unix, the name is taken
    /// for the export's own.
    fn names_the_created_file(&self) -> std::io::Result<bool> {
        #[cfg(unix)]
        if let Some(created) = self.created {
            let now = file_identity(&std::fs::symlink_metadata(&self.path)?);
            return Ok(now == created);
        }
        Ok(true)
    }
}

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let removed = match self.names_the_created_file() {
            Ok(true) => std::fs::remove_file(&self.path),
            // Another file has the name: this export's file is no longer there.
            Ok(false) => return,
            Err(e) => Err(e),
        };
        match removed {
            Ok(()) => {}
            // Nothing is at the path.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!(
                path = %self.path.display(),
                error = %e,
                "could not remove the file of a failed export"
            ),
        }
    }
}

/// The device and inode `meta` reports, which together name one file.
#[cfg(unix)]
fn file_identity(meta: &std::fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    (meta.dev(), meta.ino())
}

#[cfg(test)]
#[path = "export_file_tests.rs"]
mod tests;
