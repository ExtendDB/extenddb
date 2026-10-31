// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The SQLite backend's catalog migrations.
//!
//! The database file is the backend's one store, [`StoreKind::Catalog`].
//! [`migration`] is the table: an exhaustive `match` from every
//! [`MigrationId`] to this backend's body. [`RELEASED`] pins the released
//! bodies; the release PR that raises `RELEASED_THROUGH` pastes the lines
//! the checksum test prints.
//!
//! Entry points: [`catalog_status`] reads, [`migrate`] takes the database's
//! lock and applies, [`migrate_holding`] applies for a caller that already
//! holds the lock (the server's factory), and [`SqliteMigrations::on_pool`]
//! runs on an in-memory database's single connection.

mod backend;
mod commands;
mod copy;
mod open;
// The migration bodies are frozen once released: neither rustfmt nor a later
// clippy lint may require an edit that changes their checksums.
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0001_baseline;
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0003_sort_key_definitions;
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0004_index_table_base_key;
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0005_backup_catalog_index;
mod postcheck;

use std::time::Duration;

use extenddb_storage::migrations::m0002_resource_tag_condition_keys::{
    self, ResourceTagConditionKeys,
};
use extenddb_storage::migrations::{
    CatalogStatus, Kind, Migration, MigrationBackend, MigrationError, MigrationId, Released,
    RunReport, Runner, StoreBody, StoreKind,
};

pub use backend::{SqliteMigrations, SqliteStores};
pub use commands::SqliteCatalogMigrations;

/// The definitions migration 1 creates in an empty database, which the
/// post-check compares the database with the first time migration 1 is
/// applied to it.
const BASELINE_EXPECTED: &str = include_str!("../../migrations/0001_baseline.expected.json");

/// Checksums of the released bodies (none until v1.0).
pub(crate) const RELEASED: &[Released] = &[];

/// This backend's entry for every id. Public so `migrate --list` can print
/// each migration's kind without opening the database.
#[must_use]
pub fn migration(id: MigrationId) -> Migration<SqliteStores> {
    match id {
        MigrationId::Baseline => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &m0001_baseline::Baseline,
                m0001_baseline::SOURCE,
            )],
        },
        MigrationId::ResourceTagConditionKeys => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &ResourceTagConditionKeys,
                m0002_resource_tag_condition_keys::SOURCE,
            )],
        },
        MigrationId::SortKeyDefinitions => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &m0003_sort_key_definitions::SortKeyDefinitions,
                m0003_sort_key_definitions::SOURCE,
            )],
        },
        MigrationId::IndexTableBaseKey => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &m0004_index_table_base_key::IndexTableBaseKey,
                m0004_index_table_base_key::SOURCE,
            )],
        },
        MigrationId::BackupCatalogIndex => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &m0005_backup_catalog_index::BackupCatalogIndex,
                m0005_backup_catalog_index::SOURCE,
            )],
        },
    }
}

/// The longest wait a lock timeout can ask for that `Instant` can add.
const MAX_LOCK_WAIT: Duration = Duration::from_secs(86_400);

/// The database file's lock, held for a migration. Dropping it releases it.
#[derive(Debug)]
pub struct MigrationLock {
    _lock: crate::serve_lock::ServeLock,
}

/// Take the lock `extenddb serve` holds on the database file, retrying every
/// 250 ms while another process holds it, for up to `timeout`. `Ok(None)` for
/// an in-memory database, which no other process can open.
///
/// # Errors
///
/// [`MigrationError::Locked`] when the lock is still held at the timeout
/// (exit 6), or a store error when the lock file cannot be opened.
pub async fn lock_for_migration(
    location: &str,
    timeout: Duration,
) -> Result<Option<MigrationLock>, MigrationError> {
    let Some(file) = crate::serve_lock::database_file(location).map_err(MigrationError::Store)?
    else {
        return Ok(None);
    };
    // The lock file goes beside the database, in a directory that exists:
    // `init` creates both before it takes the lock.
    let now = tokio::time::Instant::now();
    let deadline = now.checked_add(timeout).unwrap_or(now + MAX_LOCK_WAIT);
    loop {
        match crate::serve_lock::ServeLock::try_acquire(&file) {
            Ok(lock) => return Ok(Some(MigrationLock { _lock: lock })),
            Err(crate::serve_lock::LockFailure::Failed(m)) => return Err(MigrationError::Store(m)),
            Err(crate::serve_lock::LockFailure::Held(m)) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(MigrationError::Locked(format!(
                        "{m}; waited {} s",
                        timeout.as_secs()
                    )));
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

/// The catalog status of the database at `location`. Read-only: a file that
/// does not exist is `Uninitialized` and is not created, and nothing is
/// written. Takes no lock, so it runs beside a server.
///
/// # Errors
///
/// The database cannot be opened or its ledger read.
pub async fn catalog_status(location: &str) -> Result<CatalogStatus, MigrationError> {
    let Some(file) = crate::serve_lock::database_file(location).map_err(MigrationError::Store)?
    else {
        return Err(in_memory_refusal());
    };
    if !file.exists() {
        return Ok(CatalogStatus::Uninitialized);
    }
    let migrations = SqliteMigrations::open_for_status(location).await?;
    // A read records nothing, so no release is named.
    let status = Runner::new(&migrations, "").status().await;
    migrations.close().await;
    status
}

/// Apply every pending migration to the database at `location`: take the
/// lock (waiting up to `lock_timeout`), open the runner's connection, run,
/// and close the connection and release the lock on every exit path.
///
/// # Errors
///
/// As [`Runner::migrate`] and [`lock_for_migration`].
pub async fn migrate(
    location: &str,
    release: &str,
    lock_timeout: Duration,
) -> Result<RunReport, MigrationError> {
    if crate::serve_lock::database_file(location)
        .map_err(MigrationError::Store)?
        .is_none()
    {
        return Err(in_memory_refusal());
    }
    let _lock = lock_for_migration(location, lock_timeout).await?;
    run_on_file(location, release).await
}

/// [`migrate`] for a caller that already holds the database's lock, which a
/// second `flock` from this process could never take: the server's factory,
/// which takes the lock before it opens the engine. `held` must be the lock
/// of `location`'s file.
///
/// # Errors
///
/// [`MigrationError::Refused`] when `held` is another file's lock or the
/// location is in memory, or as [`Runner::migrate`].
pub(crate) async fn migrate_holding(
    location: &str,
    release: &str,
    held: &crate::serve_lock::ServeLock,
) -> Result<RunReport, MigrationError> {
    let file = crate::serve_lock::database_file(location)
        .map_err(MigrationError::Store)?
        .ok_or_else(in_memory_refusal)?;
    let expected = crate::serve_lock::ServeLock::lock_path(&file);
    if held.path() != expected {
        return Err(MigrationError::Refused(format!(
            "the lock held is {}, and the database at {location} is locked by {}",
            held.path().display(),
            expected.display()
        )));
    }
    run_on_file(location, release).await
}

/// Decide whether a server may open the file database at `location`, whose
/// lock the server holds: `Current` and `AheadCompatible` serve (the second
/// with a warning naming the migrations above this binary's), and in a dev
/// build `Behind` and an `Uninitialized` file with no tables (missing or
/// empty) are migrated first. An `Uninitialized` file that holds tables is
/// another program's database, and is refused as the commands refuse it,
/// naming `extenddb init`, before anything writes to it. Every other status
/// refuses with [`MigrationError::Status`], in a dev build too.
///
/// # Errors
///
/// The refusal, or as [`migrate_holding`].
pub(crate) async fn prepare_for_serve(
    location: &str,
    release: &str,
    held: &crate::serve_lock::ServeLock,
    dev: bool,
) -> Result<(), MigrationError> {
    match catalog_status(location).await? {
        CatalogStatus::Current { .. } => Ok(()),
        status @ CatalogStatus::AheadCompatible { .. } => {
            tracing::warn!(
                "{status}; serving it, because a binary may serve migrations it does not know \
                 when every one of them is additive"
            );
            Ok(())
        }
        CatalogStatus::Uninitialized if dev && !holds_tables(location).await? => {
            log_report(&migrate_holding(location, release, held).await?);
            Ok(())
        }
        CatalogStatus::Behind { .. } if dev => {
            log_report(&migrate_holding(location, release, held).await?);
            Ok(())
        }
        status => Err(MigrationError::Status(status)),
    }
}

/// Whether the file database at `location` exists and holds a table, read
/// on a read-only connection.
async fn holds_tables(location: &str) -> Result<bool, MigrationError> {
    let exists = crate::serve_lock::database_file(location)
        .map_err(MigrationError::Store)?
        .is_some_and(|file| file.exists());
    if !exists {
        return Ok(false);
    }
    let migrations = SqliteMigrations::open_for_status(location).await?;
    let empty = migrations.catalog_is_empty().await;
    migrations.close().await;
    Ok(!empty?)
}

/// Log what a run the server started did: the copy's path, then each step
/// with its notes.
pub(crate) fn log_report(report: &RunReport) {
    for note in &report.notes {
        tracing::info!("{note}");
    }
    for step in &report.applied {
        tracing::info!("applied catalog migration {} {}", step.id, step.name);
        for note in &step.notes {
            tracing::info!("  {note}");
        }
    }
}

/// Open the runner's connection to a file database, run, and close it.
async fn run_on_file(location: &str, release: &str) -> Result<RunReport, MigrationError> {
    let migrations = SqliteMigrations::open(location).await?;
    let result = Runner::new(&migrations, release).migrate().await;
    let result = migrations.discard_unused_copy(result);
    migrations.close().await;
    result
}

/// Bring a test database to the latest migration: on the engine's own
/// connection for an in-memory database, else through [`migrate`] on its
/// file. Replaces the schema setup the engine's unit tests did before the
/// runner existed.
#[cfg(test)]
pub(crate) async fn apply_for_tests(pool: &sqlx::SqlitePool) {
    if pool.options().get_max_connections() == 1 {
        let migrations =
            SqliteMigrations::on_pool(pool.clone(), ":memory:").expect("one connection");
        Runner::new(&migrations, "test")
            .migrate()
            .await
            .expect("migrate the test database");
    } else {
        let file = pool
            .connect_options()
            .get_filename()
            .to_string_lossy()
            .into_owned();
        migrate(&file, "test", Duration::ZERO)
            .await
            .expect("migrate the test database");
    }
}

/// A new connection to an in-memory location opens a new, empty database,
/// so the file-based entry points refuse one.
fn in_memory_refusal() -> MigrationError {
    MigrationError::Refused(
        "an in-memory SQLite database exists only inside a running server, which creates its \
         catalog when it starts; init, migrate and verify have no database to work on"
            .to_owned(),
    )
}

#[cfg(test)]
mod crash_tests;
#[cfg(test)]
mod golden_tests;
#[cfg(test)]
mod open_tests;
#[cfg(test)]
mod refusal_tests;
#[cfg(test)]
mod released_tests;
#[cfg(test)]
mod serve_tests;
#[cfg(test)]
mod tests;
