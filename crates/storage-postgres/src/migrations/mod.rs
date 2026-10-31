// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The PostgreSQL backend's catalog migrations.
//!
//! Two stores: the catalog database ([`StoreKind::Catalog`]) and the data
//! database ([`StoreKind::Data`]). [`migration`] is the table, an exhaustive
//! `match` from every [`MigrationId`] to this backend's bodies. [`RELEASED`]
//! pins the released bodies; the release PR that raises `RELEASED_THROUGH`
//! pastes the lines the checksum test prints.
//!
//! Entry points: [`catalog_status`] reads with no lock and no write;
//! [`migrate`] takes the migration lock (which also waits for running
//! servers to stop), applies and releases; [`migrate_holding`] applies for a
//! caller that took [`MigrationLock`] itself (the commands, which take it
//! through [`PostgresCatalogMigrations`]); [`ServingRegistration`] is what a
//! server holds while it serves; [`try_create_vector_extension`] is the
//! pgvector attempt `migrate` makes on every run.
//! [`PostgresCatalogMigrations`] is how the commands reach them.

mod backend;
mod commands;
mod connect;
mod failure;
mod held;
mod index_helper;
mod locks;
// Frozen once released: neither rustfmt nor a later clippy lint may require
// an edit that changes their checksums.
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0003_sort_key_definitions;
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0004_index_table_base_key;
mod postcheck;
mod rows;
mod sessions;
mod vector_extension;

use std::time::Duration;

use extenddb_storage::file_body;
use extenddb_storage::migrations::m0002_resource_tag_condition_keys::{
    self, ResourceTagConditionKeys,
};
use extenddb_storage::migrations::{
    CatalogStatus, Kind, Migration, MigrationBackend, MigrationError, MigrationId, Released,
    RunReport, Runner, StatusNote, StoreBody, StoreKind,
};
use sqlx::postgres::PgConnectOptions;

pub use backend::{PostgresMigrations, PostgresStores};
pub use commands::{PostgresCatalogMigrations, resolve};
pub use locks::{KEEPALIVE_INTERVAL, MigrationLock, ServingRegistration};
#[cfg(feature = "test-util")]
pub use postcheck::{Column, Definitions, Index, compare, describe};
pub use sessions::{require_data_connection_row, served_data_options};
pub use vector_extension::try_create_vector_extension;

/// The definitions migration 1 creates in an empty catalog database, which
/// the post-check compares the database with the first time migration 1 is
/// applied to it.
pub(crate) const CATALOG_EXPECTED: &str =
    include_str!("../../migrations/0001_baseline.expected.json");

/// The same for the data database.
pub(crate) const DATA_EXPECTED: &str =
    include_str!("../../data_migrations/0001_baseline.expected.json");

/// Checksums of the released bodies (none until v1.0).
pub(crate) const RELEASED: &[Released] = &[];

/// This backend's entry for every id. Public so `migrate --list` can print
/// each migration's kind without connecting.
#[must_use]
pub fn migration(id: MigrationId) -> Migration<PostgresStores> {
    match id {
        MigrationId::Baseline => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![
                file_body!(StoreKind::Catalog, "migrations", "0001_baseline.sql"),
                file_body!(StoreKind::Data, "data_migrations", "0001_baseline.sql"),
            ],
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
                StoreKind::Data,
                &m0004_index_table_base_key::IndexTableBaseKey,
                m0004_index_table_base_key::SOURCE,
            )],
        },
    }
}

/// How a migration's code bodies run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunsIn {
    /// Inside the runner's transaction, committed with the ledger row.
    Transaction,
    /// Statement by statement outside a transaction (`CREATE INDEX
    /// CONCURRENTLY` cannot run in one), through the index helper; the
    /// ledger row is written after the last statement.
    Autocommit,
}

/// How each id's code bodies run. File bodies always run in the runner's
/// transaction.
pub(crate) const fn runs_in(id: MigrationId) -> RunsIn {
    match id {
        MigrationId::Baseline
        | MigrationId::ResourceTagConditionKeys
        | MigrationId::SortKeyDefinitions => RunsIn::Transaction,
        MigrationId::IndexTableBaseKey => RunsIn::Autocommit,
    }
}

/// Where the runner connects: the catalog database, and the data database.
///
/// The runner reaches the data database with the catalog's
/// `data_database_connection_string` row, which the server connects with,
/// when the catalog has it, and refuses when that row names another database
/// than `data_database` unless `data_database_given`; without the row it
/// uses the catalog's server and credentials with `data_database`.
#[derive(Debug, Clone)]
pub struct PostgresTarget {
    /// The catalog database, with the credentials the command was given.
    pub catalog: PgConnectOptions,
    /// The data database's name.
    pub data_database: String,
    /// Whether `data_database` was given on the command line (`--data-db`),
    /// so it overrides the database the connection string row names.
    pub data_database_given: bool,
    /// For `init`: the value of the `data_database_connection_string` row,
    /// which migration 1's catalog body writes with `data_database_name`.
    pub data_connection_row: Option<String>,
    /// Timeout for each connection attempt.
    pub(crate) connect_timeout: Duration,
}

impl PostgresTarget {
    /// A target with the default connect timeout of 10 s and no data
    /// connection rows to write.
    #[must_use]
    pub fn new(catalog: PgConnectOptions, data_database: impl Into<String>) -> Self {
        Self {
            catalog,
            data_database: data_database.into(),
            data_database_given: false,
            data_connection_row: None,
            connect_timeout: Duration::from_secs(10),
        }
    }

    /// Mark the data database as given on the command line.
    #[must_use]
    pub fn with_given_data_database(mut self) -> Self {
        self.data_database_given = true;
        self
    }

    /// Have migration 1's catalog body write the data connection rows, as
    /// `init` needs.
    #[must_use]
    pub fn with_data_connection_row(mut self, connection_string: impl Into<String>) -> Self {
        self.data_connection_row = Some(connection_string.into());
        self
    }

    /// The catalog database's name.
    #[must_use]
    pub fn catalog_database(&self) -> String {
        self.catalog.get_database().unwrap_or("postgres").to_owned()
    }

    /// The data database on the catalog's server, with the catalog's
    /// credentials: where the runner connects when the catalog records no
    /// data connection string.
    #[must_use]
    pub fn data(&self) -> PgConnectOptions {
        self.catalog.clone().database(&self.data_database)
    }
}

/// The catalog status. Read-only: the connections are read-only sessions, no
/// lock is taken, and nothing is written, so it runs beside a server. A
/// catalog database that does not exist is `Uninitialized`.
///
/// # Errors
///
/// A connection fails, or a ledger cannot be read.
pub async fn catalog_status(target: &PostgresTarget) -> Result<CatalogStatus, MigrationError> {
    let migrations = match PostgresMigrations::open_for_status(target).await {
        Ok(m) => m,
        Err(backend::OpenError::NoCatalogDatabase) => return Ok(CatalogStatus::Uninitialized),
        Err(backend::OpenError::Failed(e)) => return Err(e),
    };
    // A read records nothing, so no release is named.
    let status = Runner::new(&migrations, "").status().await;
    migrations.close().await;
    status
}

/// The status notes: for each database, the tables, views and sequences the
/// application role does not own ([`sessions::ownership_note`]). Read-only,
/// like [`catalog_status`]; a catalog database that does not exist has
/// none.
///
/// # Errors
///
/// A connection fails, or the owners cannot be read.
pub async fn status_notes(target: &PostgresTarget) -> Result<Vec<StatusNote>, MigrationError> {
    let migrations = match PostgresMigrations::open_for_status(target).await {
        Ok(m) => m,
        Err(backend::OpenError::NoCatalogDatabase) => return Ok(Vec::new()),
        Err(backend::OpenError::Failed(e)) => return Err(e),
    };
    let notes = async {
        let stores = migrations.stores();
        let mut notes = Vec::new();
        for (pool, kind) in [
            (&stores.catalog, StoreKind::Catalog),
            (&stores.data, StoreKind::Data),
        ] {
            for name in migrations.store_names(kind).await? {
                notes.extend(sessions::ownership_note(pool, kind, &name).await?);
            }
        }
        Ok(notes)
    }
    .await;
    migrations.close().await;
    notes
}

/// Apply every pending migration: take the migration lock, waiting up to
/// `lock_timeout` for another migrator and then for running servers, apply,
/// and release the lock and close every connection on every exit path.
///
/// # Errors
///
/// [`MigrationError::Locked`] (exit 6) when the lock or a running server
/// outlasts `lock_timeout`, or as [`Runner::migrate`].
pub async fn migrate(
    target: &PostgresTarget,
    release: &str,
    lock_timeout: Duration,
) -> Result<RunReport, MigrationError> {
    let lock = MigrationLock::acquire(&target.catalog, lock_timeout).await?;
    let result = migrate_holding(target, release, &lock).await;
    lock.release().await;
    result
}

/// [`migrate`] for a caller that holds the migration lock.
///
/// # Errors
///
/// As [`Runner::migrate`].
pub async fn migrate_holding(
    target: &PostgresTarget,
    release: &str,
    lock: &MigrationLock,
) -> Result<RunReport, MigrationError> {
    let migrations = PostgresMigrations::open(target, lock).await?;
    let result = Runner::new(&migrations, release).migrate().await;
    migrations.close().await;
    result
}

/// Bring a fresh catalog database and a fresh data database to the current
/// schema, for the test harnesses in this crate's `tests/` directory, so they
/// do not carry their own copy of the migration table.
///
/// With two databases this is [`migrate`], as `extenddb migrate --yes` runs
/// it: every migration through the runner, each recorded in the ledger of
/// the database it changed. When `catalog` and `data` reach one database
/// (the layout the engine uses when the catalog has no
/// `data_database_connection_string` row) the runner cannot apply both
/// stores, because each database holds one ledger and both stores record
/// migration 1 in it. There the file bodies of every migration are executed
/// in id order, catalog first, as this crate's one-database tests execute
/// the shipped files, and no ledger row is written. No code body runs on
/// that layout: each one converts rows or tables that a fresh database does
/// not have.
///
/// # Errors
///
/// Returns the first error, formatted.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub async fn apply_all_migrations(
    catalog: &sqlx::PgPool,
    data: &sqlx::PgPool,
) -> Result<(), String> {
    let catalog_options = (*catalog.connect_options()).clone();
    let data_options = data.connect_options();
    let one_database = catalog_options.get_host() == data_options.get_host()
        && catalog_options.get_port() == data_options.get_port()
        && catalog_options.get_database() == data_options.get_database();
    if one_database {
        for id in MigrationId::ALL {
            for body in migration(id).bodies() {
                if let extenddb_storage::migrations::Body::File { name, text } = &body.body {
                    sqlx::raw_sql(text)
                        .execute(catalog)
                        .await
                        .map_err(|e| format!("{name}: {e:?}"))?;
                }
            }
        }
        return Ok(());
    }
    let data_database = data_options.get_database().unwrap_or("postgres").to_owned();
    migrate(
        &PostgresTarget::new(catalog_options, data_database),
        "test",
        Duration::ZERO,
    )
    .await
    .map(|_| ())
    .map_err(|e| format!("{e:?}"))
}

#[cfg(test)]
mod tests;
