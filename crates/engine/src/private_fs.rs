// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Owner-only files and directories for table data on the local filesystem.
//!
//! An export file holds a table's items, so it is created `0600`, and the
//! directory extenddb creates to hold such files is created `0700`. Both
//! modes are set at creation time, so there is no window in which the process
//! umask decides who can read the data. Missing parents of that directory are
//! created with the default mode, as `mkdir -p -m 0700` does, and a directory
//! that already exists keeps its mode. On targets other than Unix the helpers
//! fall back to the platform defaults.

use std::io;
use std::path::Path;

/// Mode of a directory extenddb creates for table data.
#[cfg(unix)]
const PRIVATE_DIR_MODE: u32 = 0o700;

/// Mode of a file that holds table data.
#[cfg(unix)]
const PRIVATE_FILE_MODE: u32 = 0o600;

/// Create `path`, owner-only on Unix, and every missing ancestor with the
/// default mode (`0777` less the umask), as `mkdir -p -m 0700` does.
///
/// Only `path` itself is made owner-only: an ancestor may be a directory
/// such as `/srv/data` that other users need for siblings created later.
/// Like `std::fs::create_dir_all`, this succeeds when `path` already exists,
/// and it never changes the mode of a directory it did not create.
///
/// # Errors
///
/// Returns the I/O error from creating any missing directory.
pub fn create_private_dir_all(path: &Path) -> io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(PRIVATE_DIR_MODE);
    }
    // The same steps as `create_dir_all`, with the mode on the last one.
    if path.as_os_str().is_empty() {
        return Ok(());
    }
    match builder.create(path) {
        Ok(()) => return Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(_) if path.is_dir() => return Ok(()),
        Err(e) => return Err(e),
    }
    match path.parent() {
        Some(parent) => std::fs::create_dir_all(parent)?,
        None => return Err(io::Error::other("failed to create whole tree")),
    }
    match builder.create(path) {
        Ok(()) => Ok(()),
        Err(_) if path.is_dir() => Ok(()),
        Err(e) => Err(e),
    }
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
    //! the same cases under umask 0, as the only test in its binary.

    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    use std::path::PathBuf;
    use std::sync::Arc;

    use extenddb_core::error::DynamoDbError;

    use super::*;
    use crate::import_export::create_export_file;
    use crate::import_export_io::ensure_account_dirs;

    /// A fresh `0755` directory under the temp dir.
    fn scratch(name: &str) -> PathBuf {
        let base = std::env::temp_dir().join(format!(
            "extenddb-private-fs-{name}-{}-{}",
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

    /// The mode a directory created without a mode gets under the inherited
    /// umask: `0777` less the umask's bits.
    fn default_dir_mode() -> u32 {
        let base = scratch("probe");
        let probe = base.join("probe");
        std::fs::DirBuilder::new()
            .mode(0o777)
            .create(&probe)
            .unwrap();
        let mode = mode_of(&probe);
        std::fs::remove_dir_all(&base).unwrap();
        assert_eq!(
            mode & 0o700,
            0o700,
            "these tests need a umask that masks no owner bit, not {:03o}",
            0o777 & !mode
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
        let default = default_dir_mode();
        let base = scratch("dirs");
        let leaf = base.join("a").join("b").join("c");

        create_private_dir_all(&leaf).unwrap();

        for dir in [base.join("a"), base.join("a").join("b")] {
            assert_eq!(
                mode_of(&dir),
                default,
                "a missing ancestor, {}",
                dir.display()
            );
        }
        assert_eq!(mode_of(&leaf), 0o700);
        assert_eq!(mode_of(&base), 0o755, "the existing parent keeps its mode");
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn create_private_dir_all_accepts_an_empty_path_as_create_dir_all_does() {
        create_private_dir_all(Path::new("")).unwrap();
        std::fs::create_dir_all("").unwrap();
    }

    #[test]
    fn create_private_dir_all_leaves_an_existing_directory_alone() {
        let base = scratch("existing");
        let shared = base.join("shared");
        std::fs::create_dir(&shared).unwrap();
        set_mode(&shared, 0o775);

        create_private_dir_all(&shared).unwrap();
        assert_eq!(
            mode_of(&shared),
            0o775,
            "an existing directory keeps its mode"
        );

        let child = shared.join("child");
        create_private_dir_all(&child).unwrap();
        assert_eq!(mode_of(&shared), 0o775, "an existing parent keeps its mode");
        assert_eq!(mode_of(&child), 0o700);

        create_private_dir_all(&child).unwrap();
        assert_eq!(mode_of(&child), 0o700, "a second call changes nothing");
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn export_file_is_created_owner_only() {
        let base = scratch("file");
        let path = base.join("export.json");

        block_on(async {
            let mut file = create_export_file(&path).await.unwrap();
            tokio::io::AsyncWriteExt::write_all(&mut file, b"{}\n")
                .await
                .unwrap();
        });

        assert_eq!(mode_of(&path), 0o600);
        assert_eq!(std::fs::read(&path).unwrap(), b"{}\n");
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn export_file_refuses_an_existing_path() {
        let base = scratch("exists");
        let path = base.join("export.json");
        std::fs::write(&path, b"keep\n").unwrap();

        match block_on(create_export_file(&path)) {
            Err(DynamoDbError::ValidationException(m)) => {
                assert_eq!(m, "Export file already exists; choose a different FilePath");
            }
            Err(other) => panic!("expected ValidationException, got {other:?}"),
            Ok(_) => panic!("an existing file was opened"),
        }
        assert_eq!(std::fs::read(&path).unwrap(), b"keep\n");
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn account_export_directory_is_created_owner_only() {
        let base = scratch("account");

        ensure_account_dirs(&[Arc::new(base.clone())], "111122223333").unwrap();

        assert_eq!(mode_of(&base.join("111122223333")), 0o700);
        assert_eq!(mode_of(&base), 0o755, "the configured root keeps its mode");
        std::fs::remove_dir_all(&base).unwrap();
    }

    /// Control: the standard calls these helpers replace give other modes
    /// under the inherited umask, so the tests above tell the two apart.
    /// Under a umask that gives the standard calls `0700` or `0600` too, such
    /// as 077, there is nothing to tell apart and the test returns early.
    #[test]
    fn standard_library_defaults_follow_the_umask() {
        let dir_mode = default_dir_mode();
        let file_mode = dir_mode & 0o666;
        if dir_mode == 0o700 || file_mode == 0o600 {
            eprintln!("skipped: the inherited umask gives the standard calls owner-only modes");
            return;
        }
        let base = scratch("control");
        let dir = base.join("plain");
        let file = base.join("plain.json");

        std::fs::create_dir_all(&dir).unwrap();
        block_on(async {
            tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&file)
                .await
                .unwrap();
        });

        assert_eq!(mode_of(&dir), dir_mode);
        assert_eq!(mode_of(&file), file_mode);
        std::fs::remove_dir_all(&base).unwrap();
    }
}
