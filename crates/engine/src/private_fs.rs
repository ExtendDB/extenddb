// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Owner-only files and directories for table data on the local filesystem.
//!
//! An export file holds a table's items, so it is created `0600`, and the
//! directory extenddb creates to hold such files is created `0700`. Both
//! modes are set at creation time, so the process umask cannot widen them; it
//! can only narrow them. Missing parents of that directory are created `0755`
//! (less the umask's bits), so other users can still reach siblings created
//! there later but no parent extenddb creates is writable by group or others.
//! A directory that already exists keeps its mode. On targets other than Unix
//! the helpers fall back to the platform defaults.

use std::io;
use std::path::Path;

/// Mode of a directory extenddb creates for table data.
const PRIVATE_DIR_MODE: u32 = 0o700;

/// Mode of a missing parent of such a directory: searchable by everyone,
/// writable by the owner only, so no other user can rename the directory
/// below it or put another in its place.
const PARENT_DIR_MODE: u32 = 0o755;

/// Mode of a file that holds table data.
#[cfg(unix)]
const PRIVATE_FILE_MODE: u32 = 0o600;

/// Create `path`, owner-only on Unix, and every missing ancestor `0755`, both
/// less the umask's bits.
///
/// Only `path` itself is made owner-only: an ancestor may be a directory
/// such as `/srv/data` that other users need for siblings created later.
/// Like `std::fs::create_dir_all`, this succeeds when `path` already exists,
/// and it never changes the mode of a directory it did not create. A umask
/// that masks owner bits narrows the modes further (under umask 0200 `path`
/// is `0500`, and nothing can be created in it).
///
/// # Errors
///
/// Returns the I/O error from creating any missing directory.
pub fn create_private_dir_all(path: &Path) -> io::Result<()> {
    // The same steps as `create_dir_all`, with the modes above.
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    match dir_builder(PRIVATE_DIR_MODE).create(path) {
        Ok(()) => return Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(_) if path.is_dir() => return Ok(()),
        Err(e) => return Err(e),
    }
    match path.parent() {
        Some(parent) => dir_builder(PARENT_DIR_MODE)
            .recursive(true)
            .create(parent)?,
        None => return Err(io::Error::other("failed to create whole tree")),
    }
    create_once_parent_exists(path)
}

/// The last step of `create_private_dir_all`, once the parent of `path`
/// exists. Another caller, such as a concurrent first export of the same
/// account, may have created `path` since the first attempt; that counts as
/// success.
fn create_once_parent_exists(path: &Path) -> io::Result<()> {
    match dir_builder(PRIVATE_DIR_MODE).create(path) {
        Ok(()) => Ok(()),
        Err(_) if path.is_dir() => Ok(()),
        Err(e) => Err(e),
    }
}

/// A `DirBuilder` that creates directories with `mode` on Unix.
#[cfg(unix)]
fn dir_builder(mode: u32) -> std::fs::DirBuilder {
    use std::os::unix::fs::DirBuilderExt;
    let mut builder = std::fs::DirBuilder::new();
    builder.mode(mode);
    builder
}

/// A `DirBuilder` with the platform's default mode.
#[cfg(not(unix))]
fn dir_builder(_mode: u32) -> std::fs::DirBuilder {
    std::fs::DirBuilder::new()
}

/// Options that create a new file, owner-only on Unix, and refuse any path
/// that already exists (`create_new`).
pub(crate) fn private_new_file_options() -> tokio::fs::OpenOptions {
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(PRIVATE_FILE_MODE);
    options
}

#[cfg(all(test, unix))]
mod tests {
    //! These tests run under the umask the test binary inherited and never
    //! change it: the umask is process-wide, and the other tests in this
    //! binary create files in parallel. The modes they assert hold under any
    //! umask that masks no owner bit, which covers the umasks the server runs
    //! under (022, 002, 027 as a daemon, 077). `tests/private_modes.rs` runs
    //! the directory and export cases under umask 0, as the only test in its
    //! binary.

    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    use std::sync::Arc;

    use extenddb_core::error::DynamoDbError;
    use tempfile::TempDir;

    use super::*;
    use crate::import_export::create_export_file;
    use crate::import_export_io::ensure_account_dirs;

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
    fn scratch(name: &str) -> TempDir {
        let dir = tempfile::Builder::new()
            .prefix(&format!("extenddb-private-fs-{name}-"))
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
        let base = scratch("probe");
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

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(future)
    }

    #[test]
    fn create_private_dir_all_makes_only_the_last_directory_owner_only() {
        let parent_mode = Mode(default_dir_mode().0 & 0o755);
        let dir = scratch("dirs");
        let base = dir.path();
        let leaf = base.join("a").join("b").join("c");

        create_private_dir_all(&leaf).unwrap();

        for dir in [base.join("a"), base.join("a").join("b")] {
            assert_eq!(
                mode_of(&dir),
                parent_mode,
                "a missing ancestor is 0755 less the umask, {}",
                dir.display()
            );
        }
        assert_eq!(mode_of(&leaf), Mode(0o700));
        assert_eq!(
            mode_of(base),
            Mode(0o755),
            "the existing parent keeps its mode"
        );
    }

    #[test]
    fn create_private_dir_all_accepts_an_empty_path_as_create_dir_all_does() {
        create_private_dir_all(Path::new("")).unwrap();
        std::fs::create_dir_all("").unwrap();
    }

    #[test]
    fn create_private_dir_all_leaves_an_existing_directory_alone() {
        let dir = scratch("existing");
        let shared = dir.path().join("shared");
        std::fs::create_dir(&shared).unwrap();
        set_mode(&shared, 0o775);

        create_private_dir_all(&shared).unwrap();
        assert_eq!(
            mode_of(&shared),
            Mode(0o775),
            "an existing directory keeps its mode"
        );

        let child = shared.join("child");
        create_private_dir_all(&child).unwrap();
        assert_eq!(
            mode_of(&shared),
            Mode(0o775),
            "an existing parent keeps its mode"
        );
        assert_eq!(mode_of(&child), Mode(0o700));

        create_private_dir_all(&child).unwrap();
        assert_eq!(
            mode_of(&child),
            Mode(0o700),
            "a second call changes nothing"
        );
    }

    /// The last directory created by another caller after the first attempt
    /// found its parent missing, as when two first exports of one account
    /// race: the second create fails with `EEXIST`, which counts as success,
    /// and the other caller's directory keeps its mode.
    #[test]
    fn the_last_directory_created_meanwhile_counts_as_created() {
        let dir = scratch("race");
        let leaf = dir.path().join("account");
        std::fs::create_dir(&leaf).unwrap();
        set_mode(&leaf, 0o750);

        create_once_parent_exists(&leaf).unwrap();

        assert_eq!(mode_of(&leaf), Mode(0o750));
    }

    /// The tolerance above covers only a directory: a file in its place is
    /// still an error.
    #[test]
    fn a_file_in_place_of_the_last_directory_is_an_error() {
        let dir = scratch("race-file");
        let leaf = dir.path().join("account");
        std::fs::write(&leaf, b"").unwrap();

        let err = create_once_parent_exists(&leaf).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
    }

    #[test]
    fn export_file_is_created_owner_only() {
        let dir = scratch("file");
        let path = dir.path().join("export.json");

        block_on(async {
            let mut file = create_export_file(&path).await.unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut file, b"{}\n")
                .await
                .unwrap();
        });

        assert_eq!(mode_of(&path), Mode(0o600));
        assert_eq!(std::fs::read(&path).unwrap(), b"{}\n");
    }

    #[test]
    fn export_file_refuses_an_existing_path() {
        let dir = scratch("exists");
        let path = dir.path().join("export.json");
        std::fs::write(&path, b"keep\n").unwrap();

        match block_on(create_export_file(&path)) {
            Err(DynamoDbError::ValidationException(m)) => {
                assert_eq!(m, "Export file already exists; choose a different FilePath");
            }
            Err(other) => panic!("expected ValidationException, got {other:?}"),
            Ok(_) => panic!("an existing file was opened"),
        }
        assert_eq!(std::fs::read(&path).unwrap(), b"keep\n");
    }

    #[test]
    fn account_export_directory_is_created_owner_only() {
        let dir = scratch("account");
        let base = dir.path().to_path_buf();

        ensure_account_dirs(&[Arc::new(base.clone())], "111122223333").unwrap();

        assert_eq!(mode_of(&base.join("111122223333")), Mode(0o700));
        assert_eq!(
            mode_of(&base),
            Mode(0o755),
            "the configured root keeps its mode"
        );
    }

    /// Control: the standard calls these helpers replace give other modes
    /// under the inherited umask, so the tests above tell the two apart.
    /// Under a umask that gives the standard calls `0700` or `0600` too, such
    /// as 077, there is nothing to tell apart and the test returns early.
    #[test]
    fn standard_library_defaults_follow_the_umask() {
        let dir_mode = default_dir_mode().0;
        let file_mode = dir_mode & 0o666;
        if dir_mode == 0o700 || file_mode == 0o600 {
            eprintln!("skipped: the inherited umask gives the standard calls owner-only modes");
            return;
        }
        let base = scratch("control");
        let dir = base.path().join("plain");
        let file = base.path().join("plain.json");

        std::fs::create_dir_all(&dir).unwrap();
        block_on(async {
            tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&file)
                .await
                .unwrap();
        });

        assert_eq!(mode_of(&dir), Mode(dir_mode));
        assert_eq!(mode_of(&file), Mode(file_mode));
    }
}
