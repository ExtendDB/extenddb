// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Opening the database file for the runner: the file's mode when the runner
//! creates it, and the error for a database that cannot be opened.

use std::path::Path;

use extenddb_storage::migrations::MigrationError;

/// SQLite's `SQLITE_READONLY_DIRECTORY`: the database cannot be opened
/// because the process may not write to its directory.
const READONLY_DIRECTORY: &str = "1544";

/// The error for a database that could not be opened. A WAL database whose
/// directory this process may not write fails with `SQLITE_READONLY_DIRECTORY`
/// even for a reader, which has to create `<db>-shm` there, and the message
/// says so.
pub(super) fn open_error(location: &str, e: &sqlx::Error) -> MigrationError {
    let code = e.as_database_error().and_then(|d| d.code());
    if code.as_deref() == Some(READONLY_DIRECTORY) {
        return MigrationError::Store(format!(
            "open SQLite database {location}: the database is in WAL mode, and reading it needs \
             write access to its directory, where SQLite creates {location}-shm while a reader \
             has it open ({e})"
        ));
    }
    MigrationError::store(format!("open SQLite database {location}"), e)
}

/// Create the database file with mode 0600 when it does not exist, before
/// SQLite opens it: the database holds the encryption key once `init` writes
/// it, and SQLite gives its `-wal` and `-shm` files the database file's mode.
pub(super) fn create_private(path: &Path) -> Result<(), MigrationError> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(MigrationError::store(
            format!("create the database file {}", path.display()),
            e,
        )),
    }
}
