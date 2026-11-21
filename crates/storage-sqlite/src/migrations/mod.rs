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
//! lock and applies, [`migrate_holding`](file_database::migrate_holding)
//! applies for a caller that already holds the lock (the server's factory),
//! and [`SqliteMigrations::on_pool`] runs on an in-memory database's single
//! connection.

mod backend;
// The CLI's migration commands.
#[cfg(not(target_arch = "wasm32"))]
mod commands;
// The copy taken before a migration, through `libsqlite3-sys`'s online backup.
#[cfg(not(target_arch = "wasm32"))]
mod copy;
// Creates and opens database files, for `SqliteMigrations::open_with` only.
#[cfg(not(target_arch = "wasm32"))]
mod open;
// Locks, reads and migrates a database file; wasm32 opens none.
#[cfg(not(target_arch = "wasm32"))]
mod file_database;
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
mod m0006_backup_catalog_index;
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0009_warm_throughput;
mod postcheck;

use extenddb_storage::file_body;
use extenddb_storage::migrations::m0002_resource_tag_condition_keys::{
    self, ResourceTagConditionKeys,
};
use extenddb_storage::migrations::{
    Kind, Migration, MigrationError, MigrationId, Released, RunReport, Runner, StoreBody, StoreKind,
};

pub use backend::{SqliteMigrations, SqliteStores};
// From `commands` and `file_database`, which wasm32 does not compile.
#[cfg(not(target_arch = "wasm32"))]
pub use {
    commands::SqliteCatalogMigrations,
    file_database::{catalog_status, lock_for_migration, migrate},
};
// Callers of these are native only: the commands and the server's factory.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use file_database::{in_memory_refusal, log_report, prepare_for_serve};

/// The definitions migration 1 creates in an empty database, which the
/// post-check compares the database with the first time migration 1 is
/// applied to it.
const BASELINE_EXPECTED: &str = include_str!("../../migrations/0001_baseline.expected.json");

/// Checksums of the released bodies, every migration at or below
/// `RELEASED_THROUGH`.
pub(crate) const RELEASED: &[Released] = &[
    Released {
        id: MigrationId::Baseline,
        store: StoreKind::Catalog,
        checksum: "b72a83faad465556bceeb9c37554abbbc3aca316abd2823c3b117cd39e62349c",
        fingerprint: Some("0001_baseline/sqlite/1"),
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
        checksum: "a6f6841cb02734cc578f82b50ceb05fd7600d1647e6a0fed9e46fcd0c28c0e97",
        fingerprint: Some("0003_sort_key_definitions/sqlite/1"),
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::IndexTableBaseKey,
        store: StoreKind::Catalog,
        checksum: "33863368aef25b7d9e678ddb4c9676de5a3c0408c818e4b3a91a88253d1b46c2",
        fingerprint: Some("0004_index_table_base_key/sqlite/1"),
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::DropContinuousBackups,
        store: StoreKind::Catalog,
        checksum: "b7531559f55e4903371072c3e059ce3b82147f9b7fa7ba405dbb403b399e095c",
        fingerprint: None,
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::BackupCatalogIndex,
        store: StoreKind::Catalog,
        checksum: "8528b29cd83afb95ac2e2bb637d90efe51dfaa704b5280e479edde4b7b2fa1cf",
        fingerprint: Some("0006_backup_catalog_index/sqlite/1"),
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::StreamPending,
        store: StoreKind::Catalog,
        checksum: "99dc4a51692c3cc58d619420e2055130e33f93870dab2e02c087d4d44f1e5402",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::RestoreProgress,
        store: StoreKind::Catalog,
        checksum: "6d2e68d8284d72072de839c13f121be4005e8950fda90c5d9f7742c6958175c9",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::WarmThroughput,
        store: StoreKind::Catalog,
        checksum: "fd2dd35c3afbf3c8efcafe8dc6bddbfccbfb8c820c123bd5e74f87f60321953b",
        fingerprint: Some("0009_warm_throughput/sqlite/1"),
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::AccountMetrics,
        store: StoreKind::Catalog,
        checksum: "6c75cabed24c7d7488b33148800fe492d3dc2f561dd286ae70f203098b5375df",
        fingerprint: None,
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::AttributeNameEscape,
        store: StoreKind::Catalog,
        checksum: "d1d6e06c5e8e048d163b16f9b2d816a2d4f636bfc642be4d5e1d608cbac9994a",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::UniqueAccessKeyIds,
        store: StoreKind::Catalog,
        checksum: "5003dffcdb2e46d560d794360fdb7a6a9d5eb000968082450b8bee9a0bc23d02",
        fingerprint: None,
        kind: Kind::Additive,
        convergent: true,
        rebless: None,
    },
];

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
        MigrationId::DropContinuousBackups => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![file_body!(
                StoreKind::Catalog,
                "migrations",
                "0005_drop_continuous_backups.sql"
            )],
        },
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
            "SQLite runs one writer at a time, so its stream records commit in the order of \
             their sequence numbers",
        ),
        MigrationId::RestoreProgress => Migration::NotApplicable(
            "the restore progress of a store-backed restore is on table_restores (file_index, \
             items_applied) on SQLite; the table is PostgreSQL's data database's",
        ),
        MigrationId::WarmThroughput => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![StoreBody::code(
                StoreKind::Catalog,
                &m0009_warm_throughput::WarmThroughput,
                m0009_warm_throughput::SOURCE,
            )],
        },
        MigrationId::AccountMetrics => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![file_body!(
                StoreKind::Catalog,
                "migrations",
                "0010_account_metrics.sql"
            )],
        },
        MigrationId::AttributeNameEscape => Migration::NotApplicable(
            "SQLite stores the documents that carry attribute names as JSON text in TEXT columns, \
             where U+0000 is the six-character escape \\u0000, so every name reads back as it was \
             written",
        ),
        MigrationId::UniqueAccessKeyIds => Migration::NotApplicable(
            "access_key_id is the primary key of access_keys, so an ID can be stored once",
        ),
    }
}

/// The database file's lock, held for a migration. Dropping it releases it.
// Only `file_database` takes one and `commands` reads it; wasm32 compiles neither.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct MigrationLock {
    _lock: crate::serve_lock::ServeLock,
}

/// Create or bring up to date the catalog of an in-memory database, on the
/// engine's own pool: a new connection to an in-memory location opens another,
/// empty database, so the runner must use the one connection the engine holds.
/// The server's in-memory startup and the embedded in-memory engine both
/// migrate through this function.
///
/// # Errors
///
/// The pool allows more than one connection, or the run fails.
pub(crate) async fn migrate_in_memory(
    pool: &sqlx::SqlitePool,
    location: &str,
    release: &str,
) -> Result<RunReport, MigrationError> {
    let migrations = SqliteMigrations::on_pool(pool.clone(), location)?;
    Runner::new(&migrations, release).migrate().await
}

/// Bring a test database to the latest migration: on the engine's own
/// connection for an in-memory database, else through [`migrate`] on its
/// file. Replaces the schema setup the engine's unit tests did before the
/// runner existed.
#[cfg(test)]
pub(crate) async fn apply_for_tests(pool: &sqlx::SqlitePool) {
    if pool.options().get_max_connections() == 1 {
        migrate_in_memory(pool, ":memory:", "test")
            .await
            .expect("migrate the test database");
    } else {
        let file = pool
            .connect_options()
            .get_filename()
            .to_string_lossy()
            .into_owned();
        migrate(&file, "test", std::time::Duration::ZERO)
            .await
            .expect("migrate the test database");
    }
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
