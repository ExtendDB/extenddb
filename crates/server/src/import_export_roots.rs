// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Creation of the configured import and export roots at startup.
//!
//! A root that does not exist yet is created owner-only (`0700` on Unix),
//! because it holds import and export files and those carry table data. Its
//! missing ancestors are created with the default mode, as `mkdir -p -m 0700`
//! does, and a directory that already exists keeps its mode.

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

    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    use std::path::Path;

    use tempfile::TempDir;

    use super::resolve_roots;

    /// A permission mode that prints in octal when an assertion fails.
    #[derive(Clone, Copy, PartialEq, Eq)]
    struct Mode(u32);

    impl std::fmt::Debug for Mode {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:04o}", self.0)
        }
    }

    /// A fresh `0755` directory under the canonical temp dir, removed when
    /// dropped, also when the test panics.
    fn scratch() -> TempDir {
        let dir = tempfile::Builder::new()
            .prefix("extenddb-roots-")
            .tempdir_in(std::env::temp_dir().canonicalize().unwrap())
            .unwrap();
        set_mode(dir.path(), 0o755);
        dir
    }

    fn set_mode(path: &Path, mode: u32) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    fn mode_of(path: &Path) -> Mode {
        Mode(
            std::fs::symlink_metadata(path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
        )
    }

    /// The mode a directory created without a mode gets under the inherited
    /// umask: `0777` less the umask's bits.
    fn default_dir_mode() -> Mode {
        let base = scratch();
        let probe = base.path().join("probe");
        std::fs::DirBuilder::new()
            .mode(0o777)
            .create(&probe)
            .unwrap();
        let mode = mode_of(&probe);
        assert_eq!(
            mode.0 & 0o700,
            0o700,
            "these tests need a umask that masks no owner bit, not {:03o}",
            0o777 & !mode.0
        );
        mode
    }

    #[test]
    fn a_missing_root_is_created_owner_only() {
        let default = default_dir_mode();
        if default == Mode(0o700) {
            eprintln!("skipped: the inherited umask gives every directory 0700");
            return;
        }
        let dir = scratch();
        let base = dir.path();
        let root = base.join("exports").join("nested");

        let resolved = resolve_roots(&[root.display().to_string()], "export").unwrap();

        assert_eq!(resolved.len(), 1);
        assert_eq!(*resolved[0], root.canonicalize().unwrap());
        assert_eq!(mode_of(&root), Mode(0o700));
        assert_eq!(
            mode_of(&base.join("exports")),
            default,
            "a missing ancestor gets the default mode"
        );
        assert_eq!(
            mode_of(base),
            Mode(0o755),
            "an existing ancestor keeps its mode"
        );
    }

    #[test]
    fn an_existing_root_keeps_its_mode() {
        let dir = scratch();
        let base = dir.path();
        let root = base.join("shared");
        std::fs::create_dir(&root).unwrap();
        set_mode(&root, 0o775);

        resolve_roots(&[root.display().to_string()], "import").unwrap();

        assert_eq!(mode_of(&root), Mode(0o775));
        assert_eq!(
            mode_of(base),
            Mode(0o755),
            "the root's parent keeps its mode"
        );
    }
}
