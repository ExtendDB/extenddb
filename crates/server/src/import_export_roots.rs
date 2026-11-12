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
    //! These tests run under the umask the test binary inherited and never
    //! change it: the umask is process-wide, and the other tests in this
    //! binary create files in parallel. The modes they assert hold under any
    //! umask that masks no owner bit, which covers the umasks the server runs
    //! under. The same directory creation under umask 0 is covered by the
    //! engine's `tests/private_modes.rs`.

    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};

    use super::resolve_roots;

    /// A fresh `0755` directory under the temp dir.
    fn scratch() -> PathBuf {
        let base = std::env::temp_dir().join(format!(
            "extenddb-roots-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&base).unwrap();
        set_mode(&base, 0o755);
        base
    }

    fn set_mode(path: &Path, mode: u32) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
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
        let base = scratch();
        let root = base.join("shared");
        std::fs::create_dir(&root).unwrap();
        set_mode(&root, 0o775);

        resolve_roots(&[root.display().to_string()], "import").unwrap();

        assert_eq!(mode_of(&root), 0o775);
        std::fs::remove_dir_all(&base).unwrap();
    }
}
