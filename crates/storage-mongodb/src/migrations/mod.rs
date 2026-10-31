// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The MongoDB backend's catalog migrations.
//!
//! Two stores: the catalog database ([`StoreKind::Catalog`]) and the data
//! database ([`StoreKind::Data`]). [`migration`] is the table, an exhaustive
//! `match` from every [`MigrationId`] to this backend's bodies. [`RELEASED`]
//! pins the released bodies; the release PR that raises `RELEASED_THROUGH`
//! pastes the lines the checksum test prints.
//!
//! Entry points: [`catalog_status`] reads with no lock and no write;
//! [`MigrationLock`] serializes migrators and waits for running servers;
//! [`migrate_holding`] applies under it; [`ServingRegistration`] is what a
//! server holds while it serves, and [`register_and_check`] takes it and
//! reads the status before a server opens its engine.
//! [`MongoCatalogMigrations`] is how the commands reach them.
//!
//! MongoDB has no transaction around a body and its ledger row here: every
//! request a body sends is safe to send again, the row is written after the
//! body completes with write concern majority, and a crash runs the body
//! again. Every request waits at most [`REQUEST_TIMEOUT`], and every
//! connection is opened with the same timeout and the connection string the
//! server uses.

mod backend;
mod commands;
mod guarded;
mod locks;
mod registrations;
// Frozen once released: neither rustfmt nor a later clippy lint may require
// an edit that changes its checksum.
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0001_baseline;
mod postcheck;

use std::time::Duration;

use extenddb_storage::migrations::m0002_resource_tag_condition_keys::{
    self, ResourceTagConditionKeys,
};
use extenddb_storage::migrations::{
    CatalogStatus, Kind, Migration, MigrationError, MigrationId, Released, RunReport, Runner,
    StoreBody, StoreKind,
};

pub use backend::{MongoMigrations, MongoStores};
pub(crate) use commands::lease_of;
pub use commands::{MongoCatalogMigrations, lock};
pub(crate) use locks::{Lease, is_duplicate_key};
pub use locks::{MigrationLock, Timing};
#[cfg(feature = "test-util")]
pub use postcheck::{Collection, Definitions, Index, describe_store};
pub use registrations::ServingRegistration;

/// The catalog database every MongoDB deployment uses.
pub const CATALOG_DATABASE: &str = "extenddb_catalog";
/// The data database every MongoDB deployment uses.
pub const DATA_DATABASE: &str = "extenddb_data";
/// The ledger collection in each database.
pub const LEDGER: &str = "extenddb_migrations";
/// The collection the serving registrations live in, created by migration 1.
pub const SERVERS: &str = "extenddb_servers";
/// The `_id` of the migration lock in the catalog's `settings` collection.
pub const LOCK_ID: &str = "migration_lock";
/// How long any one request waits for its answer, and how long a connection
/// may take to open: shorter than the lock's 25 s margin.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The definitions migration 1 creates in an empty catalog database, which
/// the post-check compares the database with the first time migration 1 is
/// applied to it.
pub(crate) const CATALOG_EXPECTED: &str =
    include_str!("../../migrations/catalog/0001_baseline.expected.json");

/// The same for the data database.
pub(crate) const DATA_EXPECTED: &str =
    include_str!("../../migrations/data/0001_baseline.expected.json");

/// Checksums of the released bodies (none until v1.0).
pub(crate) const RELEASED: &[Released] = &[];

/// This backend's entry for every id. Public so `migrate --list` can print
/// each migration's kind without connecting.
#[must_use]
pub fn migration(id: MigrationId) -> Migration<MongoStores> {
    match id {
        MigrationId::Baseline => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![
                StoreBody::code(
                    StoreKind::Catalog,
                    &m0001_baseline::CatalogBaseline,
                    m0001_baseline::SOURCE,
                ),
                StoreBody::code(
                    StoreKind::Data,
                    &m0001_baseline::DataBaseline,
                    m0001_baseline::SOURCE,
                ),
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
        MigrationId::SortKeyDefinitions => Migration::NotApplicable(
            "MongoDB's #259 defect differed: the attribute definitions of index keys were not \
             stored, which issue #648 covers",
        ),
        MigrationId::IndexTableBaseKey => Migration::NotApplicable(
            "index collections get their compound index when they are created",
        ),
    }
}

/// The deployment the runner works on: the connection string the server
/// uses, and the two databases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MongoTarget {
    pub connection_string: String,
    pub catalog: String,
    pub data: String,
}

impl MongoTarget {
    /// The deployment at `connection_string`, with its two databases.
    #[must_use]
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            catalog: CATALOG_DATABASE.to_owned(),
            data: DATA_DATABASE.to_owned(),
        }
    }

    /// A deployment in databases of other names, for tests that share one
    /// server.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn with_databases(
        connection_string: impl Into<String>,
        catalog: impl Into<String>,
        data: impl Into<String>,
    ) -> Self {
        Self {
            connection_string: connection_string.into(),
            catalog: catalog.into(),
            data: data.into(),
        }
    }
}

/// A client for the runner: the connection string's checks (primary reads,
/// a write concern no weaker than majority), with [`REQUEST_TIMEOUT`] to
/// connect and to select a server.
pub(crate) async fn connect(connection_string: &str) -> Result<mongodb::Client, MigrationError> {
    let mut options = crate::guarded_client_options(connection_string, None, false)
        .await
        .map_err(|e| MigrationError::store("connect to MongoDB", e))?;
    options.connect_timeout = Some(REQUEST_TIMEOUT);
    options.server_selection_timeout = Some(REQUEST_TIMEOUT);
    mongodb::Client::with_options(options)
        .map_err(|e| MigrationError::store("connect to MongoDB", e))
}

/// A database with the engine's catalog options: primary reads, read
/// concern `local`, write concern majority.
pub(crate) fn catalog_handle(client: &mongodb::Client, name: &str) -> mongodb::Database {
    crate::catalog_database_named(client, name)
}

/// A driver error, with the server's error code when it reported one.
pub(crate) fn driver_error(
    what: impl std::fmt::Display,
    error: &mongodb::error::Error,
) -> MigrationError {
    use mongodb::error::{ErrorKind, WriteFailure};
    let code = match error.kind.as_ref() {
        ErrorKind::Command(e) => Some((e.code, e.code_name.clone())),
        ErrorKind::Write(WriteFailure::WriteError(e)) => Some((e.code, String::new())),
        ErrorKind::Write(WriteFailure::WriteConcernError(e)) => Some((e.code, e.code_name.clone())),
        _ => None,
    };
    match code {
        Some((code, name)) if !name.is_empty() => {
            MigrationError::Store(format!("{what}: {error} (code {code}, {name})"))
        }
        Some((code, _)) => MigrationError::Store(format!("{what}: {error} (code {code})")),
        None => MigrationError::store(what, error),
    }
}

/// The catalog status. Read-only: no lock is taken and nothing is written,
/// so it runs beside a server. A database that holds no ledger and no
/// `catalog_version` row, such as an empty one, one holding only the
/// migration lock, or another program's, is `Uninitialized`.
///
/// # Errors
///
/// A connection fails, or a ledger cannot be read.
pub async fn catalog_status(target: &MongoTarget) -> Result<CatalogStatus, MigrationError> {
    let migrations = MongoMigrations::open(target, None).await?;
    // A read records nothing, so no release is named.
    let status = Runner::new(&migrations, "").status().await;
    migrations.close().await;
    status
}

/// The catalog status, read as the holder of `lock`: every request checks
/// its lease and is progress, so a status that runs migration 2's `detect`
/// on a catalog with many principals keeps the lock.
///
/// # Errors
///
/// As [`catalog_status`], or the lock was lost.
pub async fn status_holding(
    target: &MongoTarget,
    lock: &MigrationLock,
) -> Result<CatalogStatus, MigrationError> {
    let migrations = MongoMigrations::open(target, Some(lock)).await?;
    let status = Runner::new(&migrations, "").status().await;
    migrations.close().await;
    status
}

/// Apply every pending migration under `lock`, which this process holds.
///
/// # Errors
///
/// As [`Runner::migrate`].
pub async fn migrate_holding(
    target: &MongoTarget,
    release: &str,
    lock: &MigrationLock,
) -> Result<RunReport, MigrationError> {
    let migrations = MongoMigrations::open(target, Some(lock)).await?;
    let result = Runner::new(&migrations, release).migrate().await;
    migrations.close().await;
    result
}

/// Register as the catalog's server and read its status, before anything
/// opens the engine: a server that refuses has written nothing but its
/// registration, which it deletes. The registration is written before the
/// lock and the status are read, so a migration that starts in between
/// waits for this server.
///
/// # Errors
///
/// [`MigrationError::Locked`] while a migration holds the lock, the status
/// when it is not `Current` or `AheadCompatible`, or a request failed.
pub async fn register_and_check(
    target: &MongoTarget,
    release: &str,
    timing: Timing,
) -> Result<ServingRegistration, MigrationError> {
    let registration = ServingRegistration::register(target, release, timing).await?;
    let status = match catalog_status(target).await {
        Ok(status) => status,
        Err(e) => {
            registration.release().await;
            return Err(e);
        }
    };
    match status {
        CatalogStatus::Current { .. } => Ok(registration),
        CatalogStatus::AheadCompatible { .. } => {
            tracing::warn!(
                "{status}; serving it, because a binary may serve migrations it does not know \
                 when every one of them is additive"
            );
            Ok(registration)
        }
        status => {
            registration.release().await;
            Err(MigrationError::Status(status))
        }
    }
}

#[cfg(test)]
mod tests;
