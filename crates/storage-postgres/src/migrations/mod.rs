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
//! server holds while it serves, and [`ServerRegistration`] is the server's
//! startup check around it and the backend's side of registering again;
//! [`try_create_vector_extension`] is the pgvector attempt `migrate` makes
//! on every run. [`PostgresCatalogMigrations`] is how the commands reach
//! them.

mod backend;
mod commands;
mod connect;
mod connection_row;
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
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0011_attribute_name_escape;
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
mod m0013_escape_control_chars;
mod postcheck;
mod registrar;
mod rows;
mod sessions;
mod vector_extension;

use std::sync::Arc;
use std::time::Duration;

use extenddb_storage::file_body;
use extenddb_storage::key_ring::Kek;
use extenddb_storage::migrations::m0002_resource_tag_condition_keys::{
    self, ResourceTagConditionKeys,
};
use extenddb_storage::migrations::{
    CatalogStatus, Kind, Migration, MigrationBackend, MigrationError, MigrationId, Released,
    RunReport, Runner, StatusNote, StoreBody, StoreKind,
};
use extenddb_storage::secret_keys::SecretKeys;
use extenddb_storage::server_components::KeySource;
use sqlx::postgres::PgConnectOptions;
use zeroize::Zeroizing;

pub use backend::{PostgresMigrations, PostgresStores};
pub use commands::{PostgresCatalogMigrations, resolve};
pub use locks::{KEEPALIVE_INTERVAL, MigrationLock, SERVE_APPLICATION, ServingRegistration};
#[cfg(feature = "test-util")]
pub use postcheck::{Column, Definitions, Index, compare, describe};
pub use registrar::ServerRegistration;
pub use sessions::{data_connection_string, require_data_connection_row, served_data_options};
pub use vector_extension::try_create_vector_extension;

/// The definitions migration 1 creates in an empty catalog database, which
/// the post-check compares the database with the first time migration 1 is
/// applied to it.
pub(crate) const CATALOG_EXPECTED: &str =
    include_str!("../../migrations/0001_baseline.expected.json");

/// The same for the data database.
pub(crate) const DATA_EXPECTED: &str =
    include_str!("../../data_migrations/0001_baseline.expected.json");

/// Checksums of the released bodies, every migration at or below
/// `RELEASED_THROUGH`.
pub(crate) const RELEASED: &[Released] = &[
    Released {
        id: MigrationId::Baseline,
        store: StoreKind::Catalog,
        checksum: "1353083a65512973a4e54c35242ce1e11e68f8773d9e59c2f9c60ef4e56d63d3",
        fingerprint: None,
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::Baseline,
        store: StoreKind::Data,
        checksum: "aeb9f87144e5ca589669b8bd32e71d36bf54d1c7a926c5aae79ed2897a5bd98d",
        fingerprint: None,
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
        checksum: "76e4e40ede2d02020703cff333f71feba2839da573f83aa0be0e3ff5b50aafb3",
        fingerprint: Some("0003_sort_key_definitions/postgres/1"),
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::IndexTableBaseKey,
        store: StoreKind::Data,
        checksum: "534ef4a81efc85815ce055bfb2410de8d099f0c3756883b5871b7da04e02b002",
        fingerprint: Some("0004_index_table_base_key/postgres/1"),
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::DropContinuousBackups,
        store: StoreKind::Catalog,
        checksum: "b685348472df535fc4873e7439b9a81ab2b9dff60c703f95ec3de769048da596",
        fingerprint: None,
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::BackupCatalogIndex,
        store: StoreKind::Catalog,
        checksum: "7d67534ae535c17d283c8cb1477fb8be0820d2bb96b6a91179f7e17e116ef8c4",
        fingerprint: None,
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::StreamPending,
        store: StoreKind::Data,
        checksum: "57ecfc569703552867fba309ecb6b82e5f81b7b1f87d27558943bdff3ae82341",
        fingerprint: None,
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::RestoreProgress,
        store: StoreKind::Data,
        checksum: "366157a591a030703fbf2d1c899b90e8b57e9c67c68d8e0a71b74b3eaee8d546",
        fingerprint: None,
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::WarmThroughput,
        store: StoreKind::Catalog,
        checksum: "3fedea10abfba13ee3aecf0a61daafb8cce470d70a8e134b5dcd53b007759f28",
        fingerprint: None,
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::AccountMetrics,
        store: StoreKind::Catalog,
        checksum: "abf2b0b69ce379b42ffbd9fbd7c22d61cbd9624b21d42b1856e02005771f315d",
        fingerprint: None,
        kind: Kind::Breaking,
        convergent: true,
        rebless: None,
    },
    Released {
        id: MigrationId::AttributeNameEscape,
        store: StoreKind::Catalog,
        checksum: "284c9e0acc37bc8fe0390188b1fc4b0a74467854b7919c616b525aafff8bba05",
        fingerprint: Some("0011_attribute_name_escape/postgres/1"),
        kind: Kind::Breaking,
        convergent: false,
        rebless: None,
    },
    Released {
        id: MigrationId::AttributeNameEscape,
        store: StoreKind::Data,
        checksum: "284c9e0acc37bc8fe0390188b1fc4b0a74467854b7919c616b525aafff8bba05",
        fingerprint: Some("0011_attribute_name_escape/postgres/1"),
        kind: Kind::Breaking,
        convergent: false,
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
                StoreKind::Data,
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
            bodies: vec![file_body!(
                StoreKind::Catalog,
                "migrations",
                "0006_backup_catalog_index.sql"
            )],
        },
        MigrationId::StreamPending => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![file_body!(
                StoreKind::Data,
                "data_migrations",
                "0007_stream_pending.sql"
            )],
        },
        MigrationId::RestoreProgress => Migration::Applies {
            // A new table is additive by CONTRIBUTING's step 4, but declaring
            // `Kind::Additive` needs the old-binary job, which does not exist
            // yet (the paragraph after step 4; `refuse_additive` in
            // `crates/bin` pins it).
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![file_body!(
                StoreKind::Data,
                "data_migrations",
                "0008_restore_progress.sql"
            )],
        },
        MigrationId::WarmThroughput => Migration::Applies {
            kind: Kind::Breaking,
            convergent: true,
            bodies: vec![file_body!(
                StoreKind::Catalog,
                "migrations",
                "0009_warm_throughput.sql"
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
        MigrationId::AttributeNameEscape => Migration::Applies {
            // A binary without the decoder reads an escaped name as another
            // name, and one without the encoder writes a U+0001 raw, which
            // this release reads as another name.
            kind: Kind::Breaking,
            // An escaped U+0001 and a raw U+0001 U+0002 are the same bytes,
            // so running the rewrite again would change the names written
            // since it ran.
            convergent: false,
            bodies: vec![
                StoreBody::code(
                    StoreKind::Catalog,
                    &m0011_attribute_name_escape::CatalogDocuments,
                    m0011_attribute_name_escape::SOURCE,
                ),
                StoreBody::code(
                    StoreKind::Data,
                    &m0011_attribute_name_escape::QueuedContexts,
                    m0011_attribute_name_escape::SOURCE,
                ),
            ],
        },
        MigrationId::UniqueAccessKeyIds => Migration::NotApplicable(
            "access_key_id is the primary key of access_keys, so an ID can be stored once",
        ),
        MigrationId::EscapeControlChars => Migration::Applies {
            kind: Kind::Breaking,
            // The escape is not idempotent: running the bodies again would
            // escape the strings written through the API since they ran.
            convergent: false,
            bodies: vec![
                StoreBody::code(
                    StoreKind::Catalog,
                    &m0013_escape_control_chars::EscapeBackupItems,
                    m0013_escape_control_chars::SOURCE,
                ),
                StoreBody::code(
                    StoreKind::Data,
                    &m0013_escape_control_chars::EscapeDataRows,
                    m0013_escape_control_chars::SOURCE,
                ),
            ],
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
        | MigrationId::SortKeyDefinitions
        | MigrationId::DropContinuousBackups
        | MigrationId::BackupCatalogIndex
        | MigrationId::StreamPending
        | MigrationId::RestoreProgress
        | MigrationId::WarmThroughput
        | MigrationId::AccountMetrics
        | MigrationId::AttributeNameEscape
        | MigrationId::UniqueAccessKeyIds
        | MigrationId::EscapeControlChars => RunsIn::Transaction,
        MigrationId::IndexTableBaseKey => RunsIn::Autocommit,
    }
}

/// Where the runner connects: the catalog database, and the data database.
///
/// The runner reaches the data database with the catalog's
/// `data_database_connection_string` row, which the server connects with,
/// when the catalog has it, and refuses when that row names another database
/// than `data_database` unless `data_database_given`; without the row it
/// uses the catalog's server and credentials with `data_database`. In a
/// catalog with a key ring the row is encrypted under the ring's DEK, and
/// `data_key` opens it.
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
    /// What opens the connection string row of a catalog with a key ring.
    pub(crate) data_key: DataKey,
    /// Whether a status read covers the catalog store alone, reaching no
    /// data database ([`PostgresMigrations::open_for_status`]).
    pub(crate) catalog_store_alone: bool,
    /// Whether a run leaves out migration 13's check for other sessions
    /// ([`PostgresTarget::with_other_sessions_allowed`]).
    pub(crate) other_sessions_allowed: bool,
}

/// What opens the `data_database_connection_string` row of a catalog with a
/// key ring (RFC-0000 section 12). A catalog without a ring keeps the row in
/// plaintext, and every variant reads it as it is.
#[derive(Clone, Default)]
pub(crate) enum DataKey {
    /// No key: a catalog with a ring is refused.
    #[default]
    None,
    /// The KEK the command read: each read unwraps the ring with it and
    /// opens the row with a DEK of the ring. `plaintext_during_switch`
    /// accepts the plaintext row while the ring's `switch_from_catalog_key`
    /// is true, for `encryption-key switch` alone.
    Kek {
        kek: Kek,
        plaintext_during_switch: bool,
    },
    /// The key holder the server's registration loaded.
    Loaded(Arc<SecretKeys>),
    /// The connection string the command read from standard input
    /// (`encryption-key reset`, RFC-0000 section 8.6), in place of the row,
    /// which no key of the run opens.
    Given(Arc<Zeroizing<String>>),
}

impl std::fmt::Debug for DataKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::Kek {
                plaintext_during_switch,
                ..
            } => write!(
                f,
                "Kek {{ plaintext_during_switch: {plaintext_during_switch} }}"
            ),
            Self::Loaded(keys) => write!(f, "Loaded({keys:?})"),
            Self::Given(_) => f.write_str("Given(..)"),
        }
    }
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
            data_key: DataKey::None,
            catalog_store_alone: false,
            other_sessions_allowed: false,
        }
    }

    /// Leave out migration 13's check for other sessions on the databases it
    /// rewrites, for a test harness whose own sessions hold its scratch
    /// databases (lock holders, observers). Every path an operator runs keeps
    /// the check, apart from `EXTENDDB_MIGRATE_IGNORE_CONNECTIONS`.
    #[cfg(any(feature = "test-util", feature = "test-hooks"))]
    #[must_use]
    pub fn with_other_sessions_allowed(mut self) -> Self {
        self.other_sessions_allowed = true;
        self
    }

    /// Open the connection string row of a catalog with a key ring with the
    /// key the command read: a KEK, which unwraps the ring at each read. The
    /// catalog key and no key open no such row. `plaintext_during_switch` is
    /// for `encryption-key switch` alone: it accepts the plaintext row while
    /// the ring's `switch_from_catalog_key` is true.
    #[must_use]
    pub fn with_key_source(mut self, key_source: KeySource, plaintext_during_switch: bool) -> Self {
        self.data_key = match key_source {
            KeySource::Kek(kek) => DataKey::Kek {
                kek,
                plaintext_during_switch,
            },
            KeySource::CatalogKey | KeySource::None => DataKey::None,
        };
        self
    }

    /// Open the connection string row of a catalog with a key ring with a
    /// key holder already loaded, as the server's registration does.
    #[must_use]
    pub fn with_secret_keys(mut self, keys: Arc<SecretKeys>) -> Self {
        self.data_key = DataKey::Loaded(keys);
        self
    }

    /// Reach the data database with `connection_string` in place of the
    /// catalog's row, which `encryption-key reset` reads from standard input
    /// because no key of the run opens the row (RFC-0000 section 8.6).
    #[must_use]
    pub fn with_given_data_connection(mut self, connection_string: &str) -> Self {
        self.data_key = DataKey::Given(Arc::new(Zeroizing::new(connection_string.to_owned())));
        self
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

/// Apply every pending migration: take the migration lock, waiting for
/// another migrator, then for running servers, then for an earlier
/// migrator's statements, all within `lock_timeout`, apply, and release the
/// lock and close every connection on every exit path.
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
    // The harness's own pools hold the scratch databases.
    migrate(
        &PostgresTarget::new(catalog_options, data_database).with_other_sessions_allowed(),
        "test",
        Duration::ZERO,
    )
    .await
    .map(|_| ())
    .map_err(|e| format!("{e:?}"))
}

#[cfg(test)]
mod m0013_guard_tests;
#[cfg(test)]
mod m0013_keys_tests;
#[cfg(test)]
mod m0013_order_tests;
#[cfg(test)]
mod m0013_tests;
#[cfg(test)]
mod tests;
