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
mod foreign;
mod guarded;
mod locks;
mod registrar;
mod registrations;
// Frozen once released: neither rustfmt nor a later clippy lint may require
// an edit that changes its checksum.
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0001_baseline;
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0005_drop_continuous_backups;
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0006_backup_catalog_index;
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0010_account_metrics;
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0012_unique_access_key_ids;
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
#[cfg(feature = "test-util")]
pub use commands::held_lock_for_tests;
pub(crate) use commands::lease_of;
pub use commands::{MongoCatalogMigrations, lock};
pub use foreign::foreign_contents;
pub(crate) use locks::{Lease, is_duplicate_key};
pub use locks::{MigrationLock, Timing};
#[cfg(feature = "test-util")]
pub use postcheck::{Collection, Definitions, Index, describe_store};
pub use registrar::ServerRegistration;
pub use registrations::{SERVE_COMMAND, ServingRegistration};

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

/// Checksums of the released bodies, every migration at or below
/// `RELEASED_THROUGH`.
pub(crate) const RELEASED: &[Released] = &[
    Released {
        id: MigrationId::Baseline,
        store: StoreKind::Catalog,
        checksum: "1ce04b3b2869e54a399f325d1e5c482b45d1cf5621e7ab13461a130108f85266",
        fingerprint: Some("0001_baseline/mongodb/1"),
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::Baseline,
        store: StoreKind::Data,
        checksum: "1ce04b3b2869e54a399f325d1e5c482b45d1cf5621e7ab13461a130108f85266",
        fingerprint: Some("0001_baseline/mongodb/1"),
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::ResourceTagConditionKeys,
        store: StoreKind::Catalog,
        checksum: "80c502c54ef9240ab5f64b24b060261a295e41d29954d9b492b83c64c444170e",
        fingerprint: Some("0002_resource_tag_condition_keys/1"),
        kind: Kind::Breaking,
        convergent: false,
        rebless: None,
    },
    Released {
        id: MigrationId::SortKeyDefinitions,
        store: StoreKind::Catalog,
        checksum: "242923091cd3a8e6eb263c05ecd5cf96e5bb1d7d94c20772b06df1ec44a09937",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::IndexTableBaseKey,
        store: StoreKind::Catalog,
        checksum: "786ea5e51bd39a29abea044bdf04cc7d3076f183b57847ccdaddd445b0c25604",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::DropContinuousBackups,
        store: StoreKind::Catalog,
        checksum: "3ca80eefd6a2d3cef629e81666c90c34228b012626769712e3893c2dba6efba1",
        fingerprint: Some("0005_drop_continuous_backups/mongodb/1"),
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::BackupCatalogIndex,
        store: StoreKind::Catalog,
        checksum: "68e0d07b8ca85db637aa70600b5834c76aeaada3d4deae4b2e67b9a313471fec",
        fingerprint: Some("0006_backup_catalog_index/mongodb/1"),
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::StreamPending,
        store: StoreKind::Catalog,
        checksum: "261c42e26a802f1850859b051468a83472cffbf0506040e09e83e5410b721f61",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::RestoreProgress,
        store: StoreKind::Catalog,
        checksum: "1743840a61b647fd1f7f53c3837a6d574d0a72e00620a1ab8ffddce625573034",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::WarmThroughput,
        store: StoreKind::Catalog,
        checksum: "77444ad166c919980cdcf1a5f51ab65eca55724d186f0551fbe1b9a81c85cbe5",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::AccountMetrics,
        store: StoreKind::Catalog,
        checksum: "3b9ff3ab729a20d26f99566d6f3c2ebdda6ca1bcf5c8e9442fa0c7328928e5b0",
        fingerprint: Some("0010_account_metrics/mongodb/1"),
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::AttributeNameEscape,
        store: StoreKind::Catalog,
        checksum: "671888396581cd761f1dff0a78a417e09c9be96a898fd1495ea3422817ca5c46",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::UniqueAccessKeyIds,
        store: StoreKind::Catalog,
        checksum: "8ebbabc18a490f6eed27571527b0666eda217e7dfac888c6943f08d133cd401e",
        fingerprint: Some("0012_unique_access_key_ids/mongodb/1"),
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
];

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
            // A `dynamodb:ResourceTag` key written through the API after the
            // rewrite has no effect, as on the service; running the rewrite
            // again would give it one.
            convergent: false,
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
        MigrationId::DropContinuousBackups => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &m0005_drop_continuous_backups::DropContinuousBackups,
                m0005_drop_continuous_backups::SOURCE,
            )],
        },
        // Breaking: a binary without it neither reads the index nor writes
        // the job record on the restore registrations it creates, so a
        // restore it ran would be owned by nobody; and the index's unique
        // indexes are what refuse a second claim of one backup. Convergent:
        // the collections and indexes are created only when absent, the
        // backfill touches only a registration without a job record, which
        // no registration this release writes is (each is inserted with its
        // record), and the catalog id is written only when absent; so a
        // second run over data written since changes nothing but a
        // registration a release before it wrote, which is the repair
        // `--reconverge` is for.
        MigrationId::BackupCatalogIndex => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &m0006_backup_catalog_index::BackupCatalogIndex,
                m0006_backup_catalog_index::SOURCE,
            )],
        },
        MigrationId::StreamPending => Migration::NotApplicable(
            "a MongoDB write draws its stream sequence number from its shard's counter inside its \
             transaction, so the records of a shard commit in number order",
        ),
        MigrationId::RestoreProgress => Migration::NotApplicable(
            "a store-backed restore records its progress on the target's restore job record \
             (restore_job.file_index); the restore_progress table is PostgreSQL's data \
             database's",
        ),
        MigrationId::WarmThroughput => Migration::NotApplicable(
            "the catalog collections have no validator, so a table or index document carries \
             a warm_throughput field without a schema change",
        ),
        MigrationId::AccountMetrics => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &m0010_account_metrics::AccountMetrics,
                m0010_account_metrics::SOURCE,
            )],
        },
        MigrationId::AttributeNameEscape => Migration::NotApplicable(
            "MongoDB stores the catalog's attribute names as BSON string values, which carry their \
             length and so can hold U+0000, and every name reads back as it was written",
        ),
        MigrationId::UniqueAccessKeyIds => Migration::Applies {
            // A unique index is never additive (CONTRIBUTING.md): the
            // previous release does not expect the index to refuse a write.
            kind: Kind::Breaking,
            // Once the index is built no ID has a second copy, so running
            // the body again deletes nothing and keeps the index.
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &m0012_unique_access_key_ids::UniqueAccessKeyIds,
                m0012_unique_access_key_ids::SOURCE,
            )],
        },
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

/// Take the migration lock on `target`, apply every pending migration
/// through `id`, and release the lock: for the crate's live tests, which
/// build catalogs in databases of their own.
///
/// # Errors
///
/// As [`Runner::migrate_through`], or the lock could not be taken.
#[cfg(test)]
pub(crate) async fn migrate_through_for_tests(
    target: &MongoTarget,
    id: MigrationId,
) -> Result<RunReport, MigrationError> {
    let (held, target) = lock(target, true, Duration::from_secs(10), Timing::PRODUCTION).await?;
    let result = async {
        let migrations = MongoMigrations::open(&target, Some(&held)).await?;
        let result = Runner::new(&migrations, "test").migrate_through(id).await;
        migrations.close().await;
        result
    }
    .await;
    held.release().await;
    result
}

/// Run migration 6's catalog body on `target`'s catalog alone, with no lock
/// and no ledger row, as a migrator stopped before it wrote the row leaves
/// it: for the live tests of the body's backfill.
///
/// # Errors
///
/// As the body.
#[cfg(test)]
pub(crate) async fn run_backup_catalog_index(
    target: &MongoTarget,
) -> Result<extenddb_storage::migrations::Done, MigrationError> {
    use extenddb_storage::migrations::{CodeBody, MigrationBackend};
    let migrations = MongoMigrations::open(target, None).await?;
    let done = m0006_backup_catalog_index::BackupCatalogIndex
        .run(migrations.stores())
        .await;
    migrations.close().await;
    done
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
) -> Result<ServerRegistration, MigrationError> {
    register_and_check_as(target, release, SERVE_COMMAND, timing).await
}

/// [`register_and_check`] for the process `command` names: `extenddb serve`
/// ([`SERVE_COMMAND`]) or a command that opens the catalog as a server does
/// (`extenddb backup list`), whose registration carries its name
/// ([`ServingRegistration::register_as`]).
///
/// # Errors
///
/// As [`register_and_check`].
pub async fn register_and_check_as(
    target: &MongoTarget,
    release: &str,
    command: &'static str,
    timing: Timing,
) -> Result<ServerRegistration, MigrationError> {
    let registration = ServingRegistration::register_as(target, release, command, timing).await?;
    let status = match catalog_status(target).await {
        Ok(status) => status,
        Err(e) => {
            registration.release().await;
            return Err(e);
        }
    };
    match status {
        CatalogStatus::Current { .. } => {}
        CatalogStatus::AheadCompatible { .. } => {
            tracing::warn!(
                "{status}; serving it, because a binary may serve migrations it does not know \
                 when every one of them is additive"
            );
        }
        status => {
            registration.release().await;
            return Err(MigrationError::Status(status));
        }
    }
    Ok(ServerRegistration::new(
        target,
        release,
        command,
        timing,
        status,
        registration,
    ))
}

#[cfg(test)]
mod tests;
