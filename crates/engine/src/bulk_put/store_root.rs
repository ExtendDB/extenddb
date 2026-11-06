// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bulk store root resolution and per-account directory creation.
//!
//! The `[bulk]` store is one directory tree, `{root}/{account id}/
//! {S3Bucket}/...`, holding both inputs and reports. The server resolves
//! the configured root once at startup with [`resolve_store_root`] and
//! creates the per-account level with [`ensure_account_dir`]: at startup
//! for every existing account, when an account is created, and at Start
//! when the directory is missing. The account level is the trust boundary
//! between tenants, so its check reads `symlink_metadata` and refuses a
//! symlink outright; a planted link at `{root}/{account id}` would
//! otherwise redirect every path the enumeration and the report sink
//! build beneath it.

use std::io;
use std::path::{Path, PathBuf};

/// Resolve a configured bulk store root: create the directory if it is
/// missing, canonicalize it, and require the result to be a directory.
/// The canonical path is what the server logs and what every containment
/// comparison uses.
///
/// # Errors
///
/// Returns the underlying I/O error when the directory cannot be created
/// or canonicalized, and `io::ErrorKind::Other` when the resolved path is
/// not a directory.
pub fn resolve_store_root(path: &Path) -> io::Result<PathBuf> {
    std::fs::create_dir_all(path)?;
    let canonical = std::fs::canonicalize(path)?;
    if !std::fs::metadata(&canonical)?.is_dir() {
        return Err(io::Error::other(format!(
            "bulk store root {} is not a directory",
            canonical.display()
        )));
    }
    Ok(canonical)
}

/// Create `{root}/{account_id}` if it is missing and return its path.
///
/// The account id must be one normal path component: an id that is empty,
/// contains a separator or NUL, or is `.` or `..` is refused before any
/// filesystem access, mirroring `account_scoped_roots` in
/// `import_export_io`. Every caller feeds a validated id; the check here
/// is defence in depth, so a malformed id from a catalog row can never
/// name a directory outside the root.
///
/// `create_dir` racing another creator is fine: `AlreadyExists` is
/// success. Whatever is at the path afterwards must be a directory and
/// not a symlink, checked with `symlink_metadata` so the check itself
/// never follows a link.
///
/// # Errors
///
/// Returns `io::ErrorKind::InvalidInput` when the account id is not one
/// normal path component, the underlying I/O error when the directory
/// cannot be created or read, and `io::ErrorKind::Other` when the path
/// holds a symlink or a non-directory.
pub fn ensure_account_dir(root: &Path, account_id: &str) -> io::Result<PathBuf> {
    if account_id.is_empty()
        || account_id.contains('/')
        || account_id.contains('\\')
        || account_id.contains('\0')
        || account_id == "."
        || account_id == ".."
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "bulk store account id is empty or contains a path separator",
        ));
    }
    let dir = root.join(account_id);
    match std::fs::create_dir(&dir) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(e),
    }
    let meta = std::fs::symlink_metadata(&dir)?;
    if meta.file_type().is_symlink() {
        return Err(io::Error::other(format!(
            "bulk store account directory {} is a symlink",
            dir.display()
        )));
    }
    if !meta.is_dir() {
        return Err(io::Error::other(format!(
            "bulk store account directory {} is not a directory",
            dir.display()
        )));
    }
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ACCOUNT: &str = "111122223333";

    /// A fresh directory acting as a configured bulk store root.
    struct Root(PathBuf);

    impl Root {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("eb-bulk-root-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir.canonicalize().unwrap())
        }
    }

    impl Drop for Root {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn resolve_creates_a_missing_root() {
        let base = Root::new();
        let target = base.0.join("store");
        let resolved = resolve_store_root(&target).unwrap();
        assert!(resolved.is_dir());
        assert_eq!(resolved, target.canonicalize().unwrap());
    }

    #[test]
    fn resolve_accepts_an_existing_root_and_canonicalizes() {
        let base = Root::new();
        let target = base.0.join("store");
        std::fs::create_dir_all(&target).unwrap();
        let dotted = base.0.join(".").join("store");
        let resolved = resolve_store_root(&dotted).unwrap();
        assert_eq!(resolved, target.canonicalize().unwrap());
    }

    #[test]
    fn resolve_refuses_a_file_at_the_root_path() {
        let base = Root::new();
        let target = base.0.join("store");
        std::fs::write(&target, b"not a directory").unwrap();
        let err = resolve_store_root(&target).unwrap_err();
        assert_ne!(err.kind(), io::ErrorKind::NotFound, "{err}");
    }

    #[test]
    fn ensure_creates_a_missing_account_dir() {
        let root = Root::new();
        let dir = ensure_account_dir(&root.0, ACCOUNT).unwrap();
        assert_eq!(dir, root.0.join(ACCOUNT));
        assert!(std::fs::symlink_metadata(&dir).unwrap().is_dir());
    }

    #[test]
    fn ensure_accepts_an_existing_account_dir() {
        let root = Root::new();
        std::fs::create_dir(root.0.join(ACCOUNT)).unwrap();
        let dir = ensure_account_dir(&root.0, ACCOUNT).unwrap();
        assert_eq!(dir, root.0.join(ACCOUNT));
    }

    #[cfg(unix)]
    #[test]
    fn ensure_refuses_a_symlink_at_the_account_dir() {
        let root = Root::new();
        let elsewhere = root.0.join("elsewhere");
        std::fs::create_dir(&elsewhere).unwrap();
        std::os::unix::fs::symlink(&elsewhere, root.0.join(ACCOUNT)).unwrap();
        let err = ensure_account_dir(&root.0, ACCOUNT).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(err.to_string().contains("symlink"), "{err}");
    }

    #[test]
    fn ensure_refuses_a_file_at_the_account_dir() {
        let root = Root::new();
        std::fs::write(root.0.join(ACCOUNT), b"not a directory").unwrap();
        let err = ensure_account_dir(&root.0, ACCOUNT).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(err.to_string().contains("not a directory"), "{err}");
    }

    #[test]
    fn ensure_refuses_an_account_id_that_is_not_one_component() {
        let root = Root::new();
        for id in ["", ".", "..", "a/b", "a\\b", "a\0b", "../111122223333"] {
            let err = ensure_account_dir(&root.0, id).unwrap_err();
            assert_eq!(
                err.kind(),
                io::ErrorKind::InvalidInput,
                "id {id:?} must be refused as invalid input, got {err}"
            );
        }
        // Nothing was created inside the root by any of the refusals.
        assert_eq!(std::fs::read_dir(&root.0).unwrap().count(), 0);
    }

    /// The escape probe: `"../escape"` must refuse and
    /// create nothing beside the root.
    #[test]
    fn ensure_refuses_an_escaping_account_id_and_creates_nothing() {
        let base = Root::new();
        let store = base.0.join("store");
        std::fs::create_dir(&store).unwrap();
        let err = ensure_account_dir(&store, "../escape").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(
            !base.0.join("escape").exists(),
            "no directory may be created beside the root"
        );
    }
}
