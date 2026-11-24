// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The database file `init` and the migration runner create, under a umask
//! that removes the owner's write bit.
//!
//! The umask is process-wide, so this binary holds a single test, which sets
//! umask 0o277 before it creates anything. Under that umask a file created
//! with mode 0600 comes out 0400, which SQLite opens read-only, so the first
//! write fails. The scratch directory is a `tempfile::TempDir`, made before
//! the umask is set, so a failed assertion does not leave it behind.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;

use extenddb_storage::bootstrapper::Bootstrapper;
use extenddb_storage_sqlite::SqliteBootstrapper;
use extenddb_storage_sqlite::migrations::SqliteMigrations;

/// `init` creates the database file (`create_catalog_db`) and then writes
/// the catalog through the migration runner's connection, which switches the
/// journal to WAL. A dev-mode `serve` and `migrate --yes` on a missing
/// database open the runner's connection alone, which creates the file. In
/// both cases the database file and its `-wal` and `-shm` files are 0600 and
/// the runner's open succeeds.
#[tokio::test]
async fn init_and_the_migration_runner_create_0600_files_under_umask_0277() {
    let dir = tempfile::Builder::new()
        .prefix("extenddb-file-mode-umask-")
        .tempdir()
        .expect("dir");
    // SAFETY: umask(2) has no preconditions and cannot fail. This is the only
    // test in the binary, so nothing else creates files under this umask.
    unsafe { libc::umask(0o277) };

    let init_db = dir.path().join("init.sqlite");
    SqliteBootstrapper::new(init_db.to_string_lossy())
        .create_catalog_db()
        .await
        .expect("create_catalog_db");
    let runner_db = dir.path().join("runner.sqlite");
    for db in [init_db, runner_db] {
        let runner = SqliteMigrations::open(&db.to_string_lossy())
            .await
            .unwrap_or_else(|e| panic!("the runner's open of {}: {e:?}", db.display()));
        for suffix in ["", "-wal", "-shm"] {
            let mut name = db.clone().into_os_string();
            name.push(suffix);
            let mode = std::fs::metadata(&name)
                .unwrap_or_else(|e| panic!("{}: {e}", name.display()))
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "{} is {mode:o}", name.display());
        }
        drop(runner);
    }
}
