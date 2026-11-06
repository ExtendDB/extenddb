// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Path jail primitives for the bulk put source and sink: lexical
//! component checks, the verified account directory handle, and the
//! handle-relative component walks that open input files and report
//! directories without following a symlink out of the account subtree.

use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use crate::import_export_io::{account_scoped_roots, validate_path};

use super::*;

/// The blocking half of [`FsSource::bucket_exists`].
pub(super) fn bucket_exists_blocking(
    roots: &[Arc<PathBuf>],
    account: &str,
    bucket: &str,
) -> Result<bool, SourceError> {
    check_component(bucket).map_err(SourceError::Jail)?;
    let scoped =
        account_scoped_roots(roots, account).map_err(|e| SourceError::Jail(e.to_string()))?;
    for scoped_root in &scoped {
        match bucket_dir_state(scoped_root, roots, account, bucket)? {
            BucketDirState::Missing => {}
            BucketDirState::NotADirectory | BucketDirState::Symlink => return Ok(false),
            BucketDirState::Directory(_) => return Ok(true),
        }
    }
    Ok(false)
}

/// What sits at `{account}/{bucket}` under one scoped root, decided by the
/// handle walk: the verified account handle, then one `openat(O_DIRECTORY |
/// O_NOFOLLOW)` of the bucket component, so the answer and the handle the
/// enumeration descends from are the same open and no path is resolved
/// between them.
#[cfg(unix)]
pub(super) enum BucketDirState {
    /// The account subtree or the bucket component does not exist.
    Missing,
    /// A symlink sits at the bucket name; never followed, wherever it
    /// points.
    Symlink,
    /// Something that is neither a directory nor a symlink.
    NotADirectory,
    /// A real directory, held open for the walk.
    Directory(std::fs::File),
}

/// The non-unix shape carries the checked path instead of a handle; the
/// fallback walk below resolves by path, and the request handler refuses
/// the API on these targets.
#[cfg(not(unix))]
pub(super) enum BucketDirState {
    Missing,
    Symlink,
    NotADirectory,
    Directory(PathBuf),
}

#[cfg(unix)]
pub(super) fn bucket_dir_state(
    scoped_root: &Path,
    roots: &[Arc<PathBuf>],
    account: &str,
    bucket: &str,
) -> Result<BucketDirState, SourceError> {
    use rustix::fs::{Mode, OFlags, openat};
    let account_dir = match open_verified_account_dir(scoped_root, roots, account) {
        Ok(Some(handle)) => handle,
        Ok(None) => return Ok(BucketDirState::Missing),
        Err(AccountDirError::Jail(message)) => return Err(SourceError::Jail(message)),
        Err(AccountDirError::Io(e)) => return Err(SourceError::Io(e)),
    };
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    match openat(&account_dir, bucket, flags, Mode::empty()) {
        Ok(fd) => Ok(BucketDirState::Directory(std::fs::File::from(fd))),
        Err(e) if e == rustix::io::Errno::NOENT => Ok(BucketDirState::Missing),
        Err(e) if e == rustix::io::Errno::LOOP => Ok(BucketDirState::Symlink),
        Err(e) if e == rustix::io::Errno::NOTDIR => Ok(BucketDirState::NotADirectory),
        Err(e) => Err(SourceError::Io(e.into())),
    }
}

/// The non-unix fallback keeps the containment check and the
/// `symlink_metadata` decision without the handle-relative hardening; the
/// request handler refuses the API on these targets.
#[cfg(not(unix))]
pub(super) fn bucket_dir_state(
    scoped_root: &Path,
    roots: &[Arc<PathBuf>],
    account: &str,
    bucket: &str,
) -> Result<BucketDirState, SourceError> {
    let bucket_dir = scoped_root.join(bucket);
    let meta = match std::fs::symlink_metadata(&bucket_dir) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BucketDirState::Missing);
        }
        Err(e) => return Err(SourceError::Io(e)),
    };
    if meta.file_type().is_symlink() {
        return Ok(BucketDirState::Symlink);
    }
    if !meta.is_dir() {
        return Ok(BucketDirState::NotADirectory);
    }
    let raw = bucket_dir
        .to_str()
        .ok_or_else(|| SourceError::Jail("bucket directory path is not valid UTF-8".to_owned()))?;
    validate_path(raw, roots, account).map_err(|e| SourceError::Jail(e.to_string()))?;
    Ok(BucketDirState::Directory(bucket_dir))
}

/// Reject a name that is not one normal path component. Bucket names, the
/// operation id, and the lease owner each contribute exactly one directory
/// or filename level to a composed path, so none of them may carry a
/// separator or a dot component. The request handler enforces the same rule
/// on the wire fields at Start; re-checking here keeps the jail decision
/// local to the code that composes the path.
pub(super) fn check_component(name: &str) -> Result<(), String> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.contains('\0')
    {
        return Err("path component is empty or contains a separator".to_owned());
    }
    Ok(())
}

/// Lexical rules for the output key prefix: relative, no `..` component,
/// no backslash, no NUL. The empty prefix is allowed. A prefix may contain
/// `/` and so span several directory levels. Enforced at Start on the wire
/// field and re-checked here before the prefix is joined onto a base path,
/// where an absolute prefix would otherwise replace the base outright.
pub(super) fn check_prefix(prefix: &str) -> Result<(), String> {
    if prefix.contains('\\') || prefix.contains('\0') {
        return Err("prefix must not contain a backslash or NUL".to_owned());
    }
    if prefix.starts_with('/') {
        return Err("prefix must not begin with a separator".to_owned());
    }
    for component in Path::new(prefix).components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            _ => return Err("prefix must not contain '..' or root components".to_owned()),
        }
    }
    Ok(())
}

/// Split a bucket-relative input path into its components for the walk.
/// Rejects empty, `.`, `..`, and NUL-carrying components, so an absolute
/// path, a doubled separator, and a traversal are all refused before any
/// filesystem access. Enumeration never produces such a path; the check
/// keeps the jail decision local to the code that opens the file.
pub(super) fn split_input_components(rel: &str) -> Result<Vec<&str>, String> {
    if rel.is_empty() {
        return Err("input path is empty".to_owned());
    }
    let mut components = Vec::new();
    for component in rel.split('/') {
        if component.is_empty() || component == "." || component == ".." || component.contains('\0')
        {
            return Err("input path component is empty or a dot component".to_owned());
        }
        components.push(component);
    }
    Ok(components)
}

/// Resolve and open one enumerated file beneath one scoped root by the
/// component walk. `Ok(None)` when the account subtree or a walked
/// component does not exist under this root, so the caller can try the
/// next root the way enumeration chose among them.
#[cfg(unix)]
pub(super) fn open_one(
    scoped_root: &Path,
    roots: &[Arc<PathBuf>],
    account: &str,
    bucket: &str,
    components: &[&str],
) -> Result<Option<std::fs::File>, SourceError> {
    let account_dir = match open_verified_account_dir(scoped_root, roots, account) {
        Ok(Some(handle)) => handle,
        Ok(None) => return Ok(None),
        Err(AccountDirError::Jail(message)) => return Err(SourceError::Jail(message)),
        Err(AccountDirError::Io(e)) => return Err(SourceError::Io(e)),
    };
    walk_open_input(&account_dir, bucket, components)
}

/// Walk `bucket` and the path components from the verified account handle:
/// one `openat(O_DIRECTORY | O_NOFOLLOW)` per directory component, then an
/// `openat(O_NOFOLLOW | O_NONBLOCK)` of the final component, whose handle
/// must be a regular file. A symlink at any component fails its own
/// `openat`, so no ancestor swap can redirect the walk, and nothing is
/// compared against a path string after the account root, so there is no
/// window between a check and the open. `O_NONBLOCK` keeps a swapped-in
/// FIFO from blocking the open and is cleared once the handle is known to
/// be a regular file, so ordinary reads block normally. `Ok(None)` when a
/// component does not exist.
#[cfg(unix)]
pub(super) fn walk_open_input(
    account_dir: &std::fs::File,
    bucket: &str,
    components: &[&str],
) -> Result<Option<std::fs::File>, SourceError> {
    use rustix::fs::{Mode, OFlags, openat};
    let Some((file_name, dirs)) = components.split_last() else {
        return Err(SourceError::Jail("input path is empty".to_owned()));
    };
    let dir_flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let mut cursor = match openat(account_dir, bucket, dir_flags, Mode::empty()) {
        Ok(fd) => std::fs::File::from(fd),
        Err(e) => return map_input_walk_errno(e),
    };
    for component in dirs.iter().copied() {
        cursor = match openat(&cursor, component, dir_flags, Mode::empty()) {
            Ok(fd) => std::fs::File::from(fd),
            Err(e) => return map_input_walk_errno(e),
        };
    }
    let file_flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
    let file = match openat(&cursor, *file_name, file_flags, Mode::empty()) {
        Ok(fd) => std::fs::File::from(fd),
        Err(e) => return map_input_walk_errno(e),
    };
    let got = file.metadata().map_err(SourceError::Io)?;
    if !got.file_type().is_file() {
        return Err(SourceError::NotRegularFile);
    }
    let flags = rustix::fs::fcntl_getfl(&file).map_err(|e| SourceError::Io(e.into()))?;
    rustix::fs::fcntl_setfl(&file, flags.difference(rustix::fs::OFlags::NONBLOCK))
        .map_err(|e| SourceError::Io(e.into()))?;
    Ok(Some(file))
}

/// One verdict per walked component: a missing component means the file is
/// not under this root, a symlink is a jail violation wherever it points,
/// and a non-directory in the middle of the path is one as well.
#[cfg(unix)]
fn map_input_walk_errno(e: rustix::io::Errno) -> Result<Option<std::fs::File>, SourceError> {
    if e == rustix::io::Errno::NOENT {
        return Ok(None);
    }
    if e == rustix::io::Errno::LOOP {
        return Err(SourceError::Jail("symlink in the input path".to_owned()));
    }
    if e == rustix::io::Errno::NOTDIR {
        return Err(SourceError::Jail(
            "input path component is not a directory".to_owned(),
        ));
    }
    Err(SourceError::Io(e.into()))
}

/// Open a handle on the caller's account directory beneath one root and
/// verify it: the shared `validate_path` containment check on the
/// directory, then a device and inode match between the handle and the
/// canonicalized account root. The account directory's parent is the
/// configured root, which no tenant can write, so this reference cannot be
/// redirected the way a tenant-owned ancestor could; every later component
/// is resolved by the walk from the handle, never by a path lookup.
/// `Ok(None)` when the account subtree does not exist under this root.
#[cfg(unix)]
pub(super) fn open_verified_account_dir(
    dir: &Path,
    roots: &[Arc<PathBuf>],
    account: &str,
) -> Result<Option<std::fs::File>, AccountDirError> {
    use rustix::fs::{CWD, Mode, OFlags, openat};
    use std::os::unix::fs::MetadataExt;
    match std::fs::symlink_metadata(dir) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(AccountDirError::Io(e)),
    }
    let raw = dir.to_str().ok_or_else(|| {
        AccountDirError::Jail("account directory path is not valid UTF-8".to_owned())
    })?;
    // Canonical containment and the symlink refusal on the directory
    // itself, shared with every other import/export path decision.
    let canonical =
        validate_path(raw, roots, account).map_err(|e| AccountDirError::Jail(e.to_string()))?;
    let expected = std::fs::symlink_metadata(&canonical).map_err(AccountDirError::Io)?;
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let handle = match openat(CWD, dir, flags, Mode::empty()) {
        Ok(fd) => std::fs::File::from(fd),
        Err(e) => return Err(AccountDirError::Io(e.into())),
    };
    let got = handle.metadata().map_err(AccountDirError::Io)?;
    if !got.is_dir() || got.dev() != expected.dev() || got.ino() != expected.ino() {
        return Err(AccountDirError::Jail(
            "account directory changed between validation and open".to_owned(),
        ));
    }
    Ok(Some(handle))
}

/// A failure while verifying the account directory handle.
#[cfg(unix)]
#[derive(Debug)]
pub(super) enum AccountDirError {
    Jail(String),
    Io(std::io::Error),
}

/// The bulk input layout is POSIX-only; on other platforms the open keeps
/// the containment check and the file-type check without the walk-based
/// hardening, and the request handler refuses the API.
#[cfg(not(unix))]
pub(super) fn open_one(
    scoped_root: &Path,
    roots: &[Arc<PathBuf>],
    account: &str,
    bucket: &str,
    components: &[&str],
) -> Result<Option<std::fs::File>, SourceError> {
    let mut full = scoped_root.join(bucket);
    for component in components.iter().copied() {
        full.push(component);
    }
    match std::fs::symlink_metadata(&full) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(SourceError::Io(e)),
    }
    let raw = full
        .to_str()
        .ok_or_else(|| SourceError::Jail("input file path is not valid UTF-8".to_owned()))?;
    let canonical =
        validate_path(raw, roots, account).map_err(|e| SourceError::Jail(e.to_string()))?;
    let file = std::fs::File::open(&canonical).map_err(SourceError::Io)?;
    let got = file.metadata().map_err(SourceError::Io)?;
    if !got.file_type().is_file() {
        return Err(SourceError::NotRegularFile);
    }
    Ok(Some(file))
}

/// One walk step on the write side: open the component as a real
/// directory, creating it with `mkdirat` on the parent's handle when it is
/// missing. An `EEXIST` from `mkdirat` means another runner raced the
/// creation, and the open after it decides. `create_dir_all` is never
/// used: it would traverse a symlink racing this loop. A directory created
/// here holds reports, whose records carry item values, so it is open to
/// the server's own user only; an existing one keeps its mode.
#[cfg(unix)]
pub(super) fn open_or_create_report_component(
    parent: &std::fs::File,
    component: &str,
) -> Result<std::fs::File, SinkError> {
    use rustix::fs::{Mode, OFlags, mkdirat, openat};
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    match openat(parent, component, flags, Mode::empty()) {
        Ok(fd) => return Ok(std::fs::File::from(fd)),
        Err(e) if e == rustix::io::Errno::NOENT => {}
        Err(e) => return Err(map_report_walk_errno(parent, component, e)),
    }
    match mkdirat(parent, component, Mode::from_raw_mode(0o700)) {
        Ok(()) => {}
        Err(e) if e == rustix::io::Errno::EXIST => {}
        Err(e) => return Err(SinkError::Io(e.into())),
    }
    match openat(parent, component, flags, Mode::empty()) {
        Ok(fd) => Ok(std::fs::File::from(fd)),
        Err(e) => Err(map_report_walk_errno(parent, component, e)),
    }
}

/// The open-only walk step used for the bucket component: the same
/// `openat(O_DIRECTORY | O_NOFOLLOW)` but nothing is created, so a missing
/// bucket directory fails the render.
#[cfg(unix)]
pub(super) fn open_report_component(
    parent: &std::fs::File,
    component: &str,
) -> Result<std::fs::File, SinkError> {
    use rustix::fs::{Mode, OFlags, openat};
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    match openat(parent, component, flags, Mode::empty()) {
        Ok(fd) => Ok(std::fs::File::from(fd)),
        Err(e) if e == rustix::io::Errno::NOENT => Err(SinkError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "report bucket directory does not exist",
        ))),
        Err(e) => Err(map_report_walk_errno(parent, component, e)),
    }
}

/// One verdict per chain component: a symlink fails the render, wherever
/// it points, and so does anything that is not a directory. `O_DIRECTORY |
/// O_NOFOLLOW` answers `ENOTDIR` for a symlink as well as for a plain
/// file; the refusal is already decided when this runs, so the extra stat
/// only picks the error variant.
#[cfg(unix)]
fn map_report_walk_errno(
    parent: &std::fs::File,
    component: &str,
    e: rustix::io::Errno,
) -> SinkError {
    let symlink = || SinkError::Symlink("symlink in the report path".to_owned());
    if e == rustix::io::Errno::LOOP {
        return symlink();
    }
    if e == rustix::io::Errno::NOTDIR {
        if let Ok(stat) =
            rustix::fs::statat(parent, component, rustix::fs::AtFlags::SYMLINK_NOFOLLOW)
            && rustix::fs::FileType::from_raw_mode(stat.st_mode) == rustix::fs::FileType::Symlink
        {
            return symlink();
        }
        return SinkError::Io(std::io::Error::new(
            std::io::ErrorKind::NotADirectory,
            "report path component is not a directory",
        ));
    }
    SinkError::Io(e.into())
}
