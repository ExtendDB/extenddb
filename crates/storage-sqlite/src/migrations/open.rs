// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Opening the database file for the runner: the error for a database that
//! cannot be opened. The runner creates a missing database file with
//! `file_mode::create_owner_only`.

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
