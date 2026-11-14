// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Creation of the configured import and export roots at startup.
//!
//! A root that does not exist yet is created owner-only (`0700` on Unix),
//! because it holds import and export files and those carry table data. Its
//! missing ancestors are created `0755`, and a directory that already exists
//! keeps its mode. Both are less the umask's bits.
//!
//! A user who can write to an ancestor of a root can rename the root away
//! and put a directory of their own at its path, which the server, holding
//! only the path, then reads imports from and writes exports into. Startup
//! warns about such an ancestor and goes on: it may be a layout the operator
//! chose.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use extenddb_config as config;

/// Expand `~` in each configured root, create the root when it is missing, and
/// canonicalize it. `label` names the list (`import` or `export`) in errors.
/// Logs one warning for each root with an ancestor that group or others can
/// write to (`replaceable_root_notice`).
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
        if let Some(notice) = replaceable_root_notice(&canonical, label) {
            tracing::warn!("{notice}");
        }
        resolved.push(Arc::new(canonical));
    }
    Ok(resolved)
}

/// The warning for a canonical `root` whose ancestors include a directory
/// writable by group or others without the sticky bit, naming each such
/// ancestor and its mode, or `None` when there is none. A sticky directory
/// such as `/tmp` lets only an entry's owner rename it, so it is not named.
/// An ancestor that cannot be read is skipped. Always `None` on targets
/// other than Unix.
fn replaceable_root_notice(root: &Path, label: &str) -> Option<String> {
    let ancestors = writable_ancestors(root);
    if ancestors.is_empty() {
        return None;
    }
    let listed = ancestors
        .iter()
        .map(|(dir, mode)| format!("{} (mode {mode:04o})", dir.display()))
        .collect::<Vec<_>>()
        .join(", ");
    let (noun, pronoun) = if ancestors.len() == 1 {
        ("an ancestor", "it")
    } else {
        ("ancestors", "them")
    };
    Some(format!(
        "The {label} root {} has {noun} writable by group or others without the sticky bit: \
         {listed}. Another user could replace the {label} root with a directory of their own. \
         Remove group and other write from {pronoun} (chmod go-w), or move the root",
        root.display()
    ))
}

#[cfg(unix)]
fn writable_ancestors(root: &Path) -> Vec<(PathBuf, u32)> {
    use std::os::unix::fs::MetadataExt;
    root.ancestors()
        .skip(1)
        .filter_map(|dir| {
            let mode = std::fs::symlink_metadata(dir).ok()?.mode() & 0o7777;
            let sticky = mode & 0o1000 != 0;
            (mode & 0o022 != 0 && !sticky).then(|| (dir.to_path_buf(), mode))
        })
        .collect()
}

#[cfg(not(unix))]
fn writable_ancestors(_root: &Path) -> Vec<(PathBuf, u32)> {
    Vec::new()
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

    use super::{replaceable_root_notice, resolve_roots};

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
            Mode(default.0 & 0o755),
            "a missing ancestor is 0755 less the umask"
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

    /// The warning names every ancestor that group or others can write to,
    /// with its mode, and only those.
    /// The warning names every ancestor that group or others can write to,
    /// nearest first, with its mode. Ancestors of the temp dir that the rule
    /// names, if any, follow those created here, so the test checks a prefix.
    #[test]
    fn an_ancestor_writable_by_group_or_others_is_named() {
        let dir = scratch();
        let group = dir.path().join("group");
        let world = group.join("world");
        let root = world.join("exports");
        std::fs::create_dir_all(&root).unwrap();
        set_mode(&group, 0o775);
        set_mode(&world, 0o757);

        let notice = replaceable_root_notice(&root, "export").expect("a warning");

        assert!(
            notice.starts_with(&format!(
                "The export root {} has ancestors writable by group or others without the \
                 sticky bit: {} (mode 0757), {} (mode 0775)",
                root.display(),
                world.display(),
                group.display()
            )),
            "{notice}"
        );
        assert!(
            !notice.contains(&format!("{} (mode", dir.path().display())),
            "a 0755 ancestor is not named: {notice}"
        );
    }

    /// No directory created here is named: not a sticky `1777` ancestor, not
    /// a `0755` one, and not the root, whose own mode is the operator's to
    /// choose. On a host whose temp dir has no ancestor the rule names, there
    /// is no warning at all.
    #[test]
    fn owner_only_and_sticky_ancestors_are_not_named() {
        let dir = scratch();
        let sticky = dir.path().join("sticky");
        let owner = sticky.join("owner");
        let root = owner.join("exports");
        std::fs::create_dir_all(&root).unwrap();
        set_mode(&sticky, 0o1777);
        set_mode(&owner, 0o755);
        set_mode(&root, 0o777);

        let notice = replaceable_root_notice(&root, "import").unwrap_or_default();

        for created in [
            dir.path(),
            sticky.as_path(),
            owner.as_path(),
            root.as_path(),
        ] {
            assert!(
                !notice.contains(&format!("{} (mode", created.display())),
                "{} is named: {notice}",
                created.display()
            );
        }
    }
}
