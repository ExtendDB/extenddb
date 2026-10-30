// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Creation of the configured import and export roots at startup.
//!
//! A root that does not exist yet is created owner-only (`0700` on Unix),
//! with any missing ancestors, because it holds import and export files and
//! those carry table data. A root that already exists keeps its mode.

use std::path::PathBuf;
use std::sync::Arc;

use extenddb_config as config;

/// Expand `~` in each configured root, create the root when it is missing, and
/// canonicalize it. `label` names the list (`import` or `export`) in errors.
pub(crate) fn resolve_roots(
    raw_paths: &[String],
    label: &str,
) -> anyhow::Result<Vec<Arc<PathBuf>>> {
    let mut resolved = Vec::new();
    for raw in raw_paths {
        let expanded = config::expand_tilde(raw);
        let path = PathBuf::from(&expanded);
        if !path.exists() {
            extenddb_engine::private_fs::create_private_dir_all(&path)
                .map_err(|e| anyhow::anyhow!("Cannot create {label} path {expanded}: {e}"))?;
        }
        let canonical = path
            .canonicalize()
            .map_err(|e| anyhow::anyhow!("Cannot canonicalize {label} path {expanded}: {e}"))?;
        resolved.push(Arc::new(canonical));
    }
    Ok(resolved)
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, MutexGuard};

    use super::resolve_roots;

    /// Serializes the tests that change the process-wide umask. While one of
    /// them runs, every other test in this binary creates files under the umask
    /// it set, so a test that checks the mode of a file it creates must hold
    /// this lock and set its own umask. The lock and the `Umask` guard are
    /// private to this module; make them `pub(crate)` to use them elsewhere.
    static UMASK_LOCK: Mutex<()> = Mutex::new(());

    /// Sets the process umask for the lifetime of the value, then restores it.
    struct Umask {
        _guard: MutexGuard<'static, ()>,
        previous: libc::mode_t,
    }

    impl Umask {
        fn set(mask: libc::mode_t) -> Self {
            let guard = UMASK_LOCK
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // SAFETY: umask(2) has no preconditions and cannot fail; the lock
            // keeps the tests in this module from interleaving their changes.
            let previous = unsafe { libc::umask(mask) };
            Self {
                _guard: guard,
                previous,
            }
        }
    }

    impl Drop for Umask {
        fn drop(&mut self) {
            // SAFETY: see `set`.
            unsafe {
                libc::umask(self.previous);
            }
        }
    }

    /// A fresh `0755` directory under the temp dir, whatever the umask.
    fn scratch() -> PathBuf {
        let base = std::env::temp_dir().join(format!(
            "extenddb-roots-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true).mode(0o755);
        builder.create(&base).unwrap();
        base
    }

    fn mode_of(path: &Path) -> u32 {
        std::fs::symlink_metadata(path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777
    }

    #[test]
    fn a_missing_root_is_created_owner_only() {
        let _umask = Umask::set(0);
        let base = scratch();
        let root = base.join("exports").join("nested");

        let resolved = resolve_roots(&[root.display().to_string()], "export").unwrap();

        assert_eq!(resolved.len(), 1);
        assert_eq!(*resolved[0], root.canonicalize().unwrap());
        assert_eq!(mode_of(&root), 0o700);
        assert_eq!(mode_of(&base.join("exports")), 0o700);
        assert_eq!(mode_of(&base), 0o755, "an existing ancestor keeps its mode");
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn an_existing_root_keeps_its_mode() {
        let _umask = Umask::set(0);
        let base = scratch();
        let root = base.join("shared");
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o775);
        builder.create(&root).unwrap();

        resolve_roots(&[root.display().to_string()], "import").unwrap();

        assert_eq!(mode_of(&root), 0o775);
        std::fs::remove_dir_all(&base).unwrap();
    }
}
