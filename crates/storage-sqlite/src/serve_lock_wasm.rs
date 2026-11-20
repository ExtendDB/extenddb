// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Serve-lock surface for the browser build.
//!
//! The serve lock is a `flock(2)` guard that gives one process exclusive
//! ownership of a SQLite database file so startup recovery and the single
//! in-process write lock are safe. A browser build owns one in-memory database
//! that no other process can reach and opens no file, so it takes no lock.
//!
//! This floor provides only what the shared engine code names: a location
//! classifier that reports an in-memory database (the sole location this build
//! opens) and a `ServeLock` type to name the engine's lock field, which is
//! always `None` here. The full resolver the server and CLI use — which defers
//! to SQLite's own URL parsing for `sqlite:`/`file:` spellings — lives in the
//! native `serve_lock` module.

use std::path::PathBuf;

/// The database file a configured SQLite location refers to, or `Ok(None)` for
/// an in-memory database.
///
/// This build opens only an in-memory database, so it recognises the in-memory
/// forms and treats any other location as a plain file path. It does not parse
/// the `sqlite:`/`file:` URL spellings the native resolver handles, because
/// this build never opens a file on disk.
///
/// # Errors
///
/// Never returns `Err` here; the `Result` mirrors the native signature so the
/// shared callers are identical on both targets.
pub(crate) fn database_file(location: &str) -> Result<Option<PathBuf>, String> {
    let url = crate::sqlite_util::sqlite_url(location);
    let mode_memory = url.split_once('?').is_some_and(|(_, query)| {
        query
            .split('&')
            .filter_map(|kv| kv.split_once('='))
            .any(|(k, v)| k == "mode" && v == "memory")
    });
    if mode_memory || location == ":memory:" || url.contains(":memory:") {
        return Ok(None);
    }
    Ok(Some(PathBuf::from(location)))
}

/// Names the type of the engine's serve-lock field. The browser build takes no
/// file lock, so the field is always `None` and this type is never constructed.
#[derive(Debug)]
pub(crate) struct ServeLock;
