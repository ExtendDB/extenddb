// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Owner-only files and directories for table data on the local filesystem.
//!
//! An export file holds a table's items, so it is created `0600`, and the
//! directories extenddb creates to hold such files are created `0700`. Both
//! modes are set at creation time, so there is no window in which the process
//! umask decides who can read the data. A directory that already exists keeps
//! its mode. On targets other than Unix the helpers fall back to the platform
//! defaults.

use std::io;
use std::path::Path;

/// Mode of a directory extenddb creates for table data.
#[cfg(unix)]
const PRIVATE_DIR_MODE: u32 = 0o700;

/// Mode of a file that holds table data.
#[cfg(unix)]
const PRIVATE_FILE_MODE: u32 = 0o600;

/// Create `path` and every missing ancestor, each owner-only on Unix.
///
/// Like `std::fs::create_dir_all`, this succeeds when `path` already exists,
/// and it never changes the mode of a directory it did not create.
///
/// # Errors
///
/// Returns the I/O error from creating any missing directory.
pub fn create_private_dir_all(path: &Path) -> io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(PRIVATE_DIR_MODE);
    }
    builder.create(path)
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
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex, MutexGuard};

    use extenddb_core::error::DynamoDbError;

    use super::*;
    use crate::import_export::create_export_file;
    use crate::import_export_io::ensure_account_dirs;

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
    fn scratch(name: &str) -> PathBuf {
        let base = std::env::temp_dir().join(format!(
            "extenddb-private-fs-{name}-{}-{}",
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

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(future)
    }

    #[test]
    fn create_private_dir_all_creates_owner_only_directories() {
        let _umask = Umask::set(0);
        let base = scratch("dirs");
        let leaf = base.join("a").join("b").join("c");

        create_private_dir_all(&leaf).unwrap();

        for dir in [base.join("a"), base.join("a").join("b"), leaf] {
            assert_eq!(mode_of(&dir), 0o700, "{}", dir.display());
        }
        assert_eq!(mode_of(&base), 0o755, "the existing parent keeps its mode");
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn create_private_dir_all_leaves_an_existing_directory_alone() {
        let _umask = Umask::set(0);
        let base = scratch("existing");
        let shared = base.join("shared");
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o775);
        builder.create(&shared).unwrap();
        assert_eq!(mode_of(&shared), 0o775);

        let child = shared.join("child");
        create_private_dir_all(&child).unwrap();
        assert_eq!(mode_of(&shared), 0o775);
        assert_eq!(mode_of(&child), 0o700);

        create_private_dir_all(&child).unwrap();
        assert_eq!(mode_of(&child), 0o700, "a second call changes nothing");
        std::fs::remove_dir_all(&base).unwrap();
    }

    #[test]
    fn export_file_is_created_owner_only() {
        let _umask = Umask::set(0);
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
        let _umask = Umask::set(0);
        let base = scratch("account");

        ensure_account_dirs(&[Arc::new(base.clone())], "111122223333").unwrap();

        assert_eq!(mode_of(&base.join("111122223333")), 0o700);
        assert_eq!(mode_of(&base), 0o755, "the configured root keeps its mode");
        std::fs::remove_dir_all(&base).unwrap();
    }

    /// Control: the standard calls these helpers replace follow the umask, so
    /// the tests above run under a permissive one.
    #[test]
    fn standard_library_defaults_follow_the_umask() {
        let _umask = Umask::set(0);
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

        assert_eq!(mode_of(&dir), 0o777);
        assert_eq!(mode_of(&file), 0o666);
        std::fs::remove_dir_all(&base).unwrap();
    }
}
