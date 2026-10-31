// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The runner's SQLite parts.
//!
//! Every body runs on one dedicated connection, a pool of exactly one
//! connection that this module opens and closes, so code bodies that take a
//! pool (the management store, for migration 2) run inside the runner's
//! transaction. Per body: `PRAGMA foreign_keys = OFF`, `BEGIN IMMEDIATE`,
//! `PRAGMA foreign_key_check`, the body, the check again (a violating row the
//! body added rolls back and stops the run; one that was there before is
//! noted), the post-check the first time migration 1 is applied, the ledger
//! row and the version row, `COMMIT`, then `PRAGMA foreign_keys = ON`. The
//! pragma has no effect inside a transaction, and a table rebuild with
//! foreign keys on deletes child rows through `ON DELETE CASCADE`. The
//! connection is closed when the runner is done, on every exit path, because
//! every other pool assumes the pragma its `after_connect` set.
//!
//! An in-memory database exists only on the engine's one connection, so the
//! runner runs on that connection, with no lock and no copy.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use extenddb_storage::management_store::ManagementStore;
use extenddb_storage::migrations::{
    CatalogTraces, Done, HasManagementStore, LedgerColumns, LedgerRow, LedgerWrites, Migration,
    MigrationBackend, MigrationError, MigrationId, Released, RunReport, Step, StoreKind, StoreRef,
    in_transaction, record_rows, sql_ledger_ddl,
};
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{ConnectOptions, Connection, SqlitePool};

use crate::catalog_store::SqliteCatalogStore;
use crate::sqlite_util::{set_connection_pragmas, sqlite_url};

/// What SQLite code bodies receive: the runner's connection, and a
/// management store over it.
pub struct SqliteStores {
    pub(crate) pool: SqlitePool,
    management: SqliteCatalogStore,
}

impl HasManagementStore for SqliteStores {
    fn management_store(&self) -> &dyn ManagementStore {
        &self.management
    }
}

/// The ledger's `applied_at` column in SQLite, which has no time type: the
/// UTC time as text, to the millisecond.
const APPLIED_AT: &str = "TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))";

/// The ledger rows of the runner's connection, in SQLite's statements.
struct Rows<'a>(&'a SqlitePool);

#[async_trait]
impl LedgerWrites for Rows<'_> {
    type Error = MigrationError;

    async fn create_ledger(&self) -> Result<(), MigrationError> {
        let ddl = sql_ledger_ddl(APPLIED_AT);
        sqlx::raw_sql(&ddl)
            .execute(self.0)
            .await
            .map(|_| ())
            .map_err(|e| MigrationError::store(&ddl, e))
    }

    async fn insert_row(&self, row: &LedgerRow) -> Result<(), MigrationError> {
        sqlx::query(
            "INSERT INTO extenddb_migrations \
             (id, name, kind, checksum, outcome, notes, applied_by) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(i64::from(row.id))
        .bind(&row.name)
        .bind(row.kind.as_str())
        .bind(&row.checksum)
        .bind(row.outcome.as_str())
        .bind(&row.notes)
        .bind(&row.applied_by)
        .execute(self.0)
        .await
        .map(|_| ())
        .map_err(|e| MigrationError::store(format!("record migration {} {}", row.id, row.name), e))
    }

    async fn write_version(&self, version: u32) -> Result<(), MigrationError> {
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ('catalog_version', ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(version.to_string())
        .execute(self.0)
        .await
        .map(|_| ())
        .map_err(|e| MigrationError::store("write catalog_version", e))
    }
}

/// The SQLite backend of the migration runner.
pub struct SqliteMigrations {
    stores: SqliteStores,
    location: String,
    /// The database file; `None` for an in-memory database.
    file: Option<PathBuf>,
    /// Whether the connection is this runner's own, closed by [`Self::close`].
    owned: bool,
    /// The copy this run took before its first write, if it took one.
    copy: Mutex<Option<PathBuf>>,
    /// The lines naming the partial copies of stopped runs that opening the
    /// database for writing removed, reported with the run.
    removed: Mutex<Vec<String>>,
    /// `PRAGMA foreign_keys = ON` failed after a step committed: the step is
    /// reported as applied, and the run stops with this error at its next
    /// call ([`Self::discard_unused_copy`] when the step was its last).
    restore_failed: Mutex<Option<MigrationError>>,
    /// Tests make the pragma after a commit fail with this.
    #[cfg(test)]
    pub(crate) fail_restore: std::sync::atomic::AtomicBool,
}

impl SqliteMigrations {
    /// Open the runner's connection to the database at `location`, creating
    /// the file when it does not exist: `init` creates it before its first
    /// run, and a dev-mode server opens a new database this way.
    ///
    /// # Errors
    ///
    /// The location does not parse, or the database cannot be opened.
    pub async fn open(location: &str) -> Result<Self, MigrationError> {
        Self::open_with(location, true).await
    }

    /// Open a read-only connection for reading the status: SQLite opens the
    /// file read-only, so reading changes nothing in it (no journal mode
    /// switch, no WAL checkpoint) and creates no file.
    ///
    /// # Errors
    ///
    /// As [`Self::open`].
    pub async fn open_for_status(location: &str) -> Result<Self, MigrationError> {
        Self::open_with(location, false).await
    }

    async fn open_with(location: &str, write: bool) -> Result<Self, MigrationError> {
        let file = crate::serve_lock::database_file(location).map_err(MigrationError::Store)?;
        if write && let Some(path) = &file {
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent).map_err(|e| {
                    MigrationError::store(format!("create the directory of {location}"), e)
                })?;
            }
            super::open::create_private(path)?;
        }
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(&sqlite_url(location))
            .map_err(|e| MigrationError::store(format!("open SQLite database {location}"), e))?
            .read_only(!write)
            .create_if_missing(write);
        // A test build counts every statement as slow, so the test that a run
        // prints no slow-statement warning needs no large catalog.
        #[cfg(test)]
        let options =
            options.log_slow_statements(log::LevelFilter::Warn, std::time::Duration::ZERO);
        // The operator lines report every step; sqlx's slow-statement
        // warnings would print SQL between them.
        let options = options.disable_statement_logging();
        // A database that cannot be opened, or whose pragmas fail, is
        // reported at once with SQLite's cause: the pool would run the
        // failing connection again until its acquire timeout.
        let mut probe = options
            .connect()
            .await
            .map_err(|e| super::open::open_error(location, &e))?;
        let pragmas = set_connection_pragmas(&mut probe, write).await;
        let _ = probe.close().await;
        pragmas.map_err(|e| super::open::open_error(location, &e))?;
        // The connection is opened once. A second connect would mean the first
        // was lost, together with any open transaction, and statements would
        // then commit one by one, so it is refused. A connection counts as
        // opened once its pragmas succeeded; the first failure is returned as
        // it is, without waiting for the pool's acquire timeout.
        let opened = Arc::new(AtomicUsize::new(0));
        let first_error: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let failure = Arc::clone(&first_error);
        let connected = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .after_connect(move |conn, _| {
                let opened = Arc::clone(&opened);
                let failure = Arc::clone(&failure);
                Box::pin(async move {
                    if opened.load(Ordering::SeqCst) > 0 {
                        return Err(sqlx::Error::Protocol(
                            "the migration connection was lost and is not reopened".to_owned(),
                        ));
                    }
                    match set_connection_pragmas(conn, write).await {
                        Ok(()) => {
                            opened.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }
                        Err(e) => {
                            let mut first = failure.lock().unwrap_or_else(PoisonError::into_inner);
                            first.get_or_insert_with(|| e.to_string());
                            Err(e)
                        }
                    }
                })
            })
            .connect_with(options)
            .await;
        let pool = match connected {
            Ok(pool) => pool,
            Err(e) => {
                let first = first_error
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                let cause = first.unwrap_or_else(|| e.to_string());
                return Err(MigrationError::Store(format!(
                    "open SQLite database {location}: {cause}"
                )));
            }
        };
        let migrations = Self::with_pool(pool, location, file, true);
        // Every run that writes holds the database's lock, so a partial copy
        // beside the database now is a stopped run's, whether or not this
        // run copies.
        if write && let Some(path) = &migrations.file {
            *migrations
                .removed
                .lock()
                .unwrap_or_else(PoisonError::into_inner) =
                super::copy::remove_stopped_partials(path);
        }
        Ok(migrations)
    }

    /// Run on the engine's pool of an in-memory database, which is left
    /// open. The pool must hold exactly one connection: the runner's
    /// transaction and the body's statements have to share it.
    ///
    /// # Errors
    ///
    /// [`MigrationError::Refused`] for a pool that allows more than one
    /// connection, which a file database's engine pool does.
    pub fn on_pool(pool: SqlitePool, location: &str) -> Result<Self, MigrationError> {
        let max = pool.options().get_max_connections();
        if max != 1 {
            return Err(MigrationError::Refused(format!(
                "SqliteMigrations::on_pool runs on an in-memory database's single connection, \
                 and this pool allows {max}; migrate a file database with migrate()"
            )));
        }
        Ok(Self::with_pool(pool, location, None, false))
    }

    fn with_pool(pool: SqlitePool, location: &str, file: Option<PathBuf>, owned: bool) -> Self {
        Self {
            stores: SqliteStores {
                management: SqliteCatalogStore::new(pool.clone()),
                pool,
            },
            location: location.to_owned(),
            file,
            owned,
            copy: Mutex::new(None),
            removed: Mutex::new(Vec::new()),
            restore_failed: Mutex::new(None),
            #[cfg(test)]
            fail_restore: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The error a failed `PRAGMA foreign_keys = ON` left after a commit.
    fn take_restore_failure(&self) -> Option<MigrationError> {
        self.restore_failed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// `result` as the caller reports it: a run that stopped before it
    /// committed anything left the database as it was, so the copy it took
    /// is removed and the error is returned alone. A run whose last step
    /// committed and whose pragma after it failed stops with that error and
    /// keeps its copy. A report starts with the partial copies of stopped
    /// runs that opening the database removed.
    pub(crate) fn discard_unused_copy(
        &self,
        result: Result<RunReport, MigrationError>,
    ) -> Result<RunReport, MigrationError> {
        let result = match (result, self.take_restore_failure()) {
            (Ok(report), Some(error)) => Err(MigrationError::Stopped {
                error: Box::new(error),
                report,
            }),
            (result, _) => result,
        };
        let removed =
            std::mem::take(&mut *self.removed.lock().unwrap_or_else(PoisonError::into_inner));
        let result = match result {
            Ok(mut report) => {
                report.notes.splice(0..0, removed);
                Ok(report)
            }
            Err(MigrationError::Stopped { error, mut report }) => {
                report.notes.splice(0..0, removed);
                Err(MigrationError::Stopped { error, report })
            }
            other => other,
        };
        match result {
            Err(MigrationError::Stopped { error, report }) if report.applied.is_empty() => {
                let copy = self
                    .copy
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take();
                match copy.map(|copy| super::copy::remove_copy(&copy)) {
                    Some(Err(note)) => Err(MigrationError::Stopped {
                        error,
                        report: RunReport {
                            notes: vec![note],
                            ..report
                        },
                    }),
                    _ => Err(*error),
                }
            }
            other => other,
        }
    }

    /// Close the runner's own connection. Callers close it on every exit
    /// path; see [`migrate`](super::migrate) for the pattern.
    pub async fn close(self) {
        if self.owned {
            self.stores.pool.close().await;
        }
    }

    #[cfg(test)]
    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.stores.pool
    }

    async fn exec(&self, sql: &str) -> Result<(), MigrationError> {
        sqlx::raw_sql(sql)
            .execute(&self.stores.pool)
            .await
            .map(|_| ())
            .map_err(|e| MigrationError::store(sql, e))
    }

    async fn table_exists(&self, name: &str) -> Result<bool, MigrationError> {
        crate::schema::table_exists(&self.stores.pool, name)
            .await
            .map_err(|e| MigrationError::Store(format!("look up table {name}: {e:?}")))
    }

    /// The checks, the body and the rows, inside the open transaction.
    async fn body_and_rows(
        &self,
        id: MigrationId,
        step: Step<'_, SqliteStores>,
        row: Option<LedgerRow>,
        catalog_version: Option<u32>,
    ) -> Result<Done, MigrationError> {
        let pool = &self.stores.pool;
        let before = foreign_key_violations(pool).await?;
        let mut done = match step {
            Step::File { name, text } => {
                sqlx::raw_sql(text)
                    .execute(pool)
                    .await
                    .map_err(|e| MigrationError::store(format!("migration {id} ({name})"), e))?;
                Done::default()
            }
            Step::Code(body) => body.run(&self.stores).await?,
            Step::Record => Done::default(),
        };
        let added: Vec<Violation> = foreign_key_violations(pool)
            .await?
            .into_iter()
            .filter(|v| !before.contains(v))
            .collect();
        if !added.is_empty() {
            return Err(MigrationError::Store(format!(
                "migration {id} left {} row(s) that violate a foreign key, in {}; nothing was \
                 committed",
                added.len(),
                by_table(&added)
            )));
        }
        if !before.is_empty() {
            done.notes.push(format!(
                "{} row(s) violated a foreign key before migration {id} ran, in {}; they are \
                 left as they are",
                before.len(),
                by_table(&before)
            ));
        }
        // The first application only: on a catalog at a later migration the
        // tables migration 1 creates may carry what later migrations added.
        if id == MigrationId::Baseline && row.is_some() {
            done.notes
                .extend(super::postcheck::check(pool, super::BASELINE_EXPECTED).await?);
        }
        record_rows(&Rows(pool), row, &done.notes, catalog_version).await?;
        Ok(done)
    }
}

/// One row of `PRAGMA foreign_key_check`: the child table, its rowid (none
/// for a table without one), the parent table and the constraint's index.
type Violation = (String, Option<i64>, String, i64);

async fn foreign_key_violations(pool: &SqlitePool) -> Result<Vec<Violation>, MigrationError> {
    sqlx::query_as("PRAGMA foreign_key_check")
        .fetch_all(pool)
        .await
        .map_err(|e| MigrationError::store("PRAGMA foreign_key_check", e))
}

/// `table (n), ...` for a list of violations.
fn by_table(violations: &[Violation]) -> String {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for (table, ..) in violations {
        *counts.entry(table.as_str()).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(table, n)| format!("{table} ({n})"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[async_trait]
impl MigrationBackend for SqliteMigrations {
    type Stores = SqliteStores;

    fn backend_name(&self) -> &'static str {
        "sqlite"
    }

    fn migration(&self, id: MigrationId) -> Migration<SqliteStores> {
        super::migration(id)
    }

    fn released(&self) -> &'static [Released] {
        super::RELEASED
    }

    fn stores(&self) -> &SqliteStores {
        &self.stores
    }

    async fn store_names(&self, kind: StoreKind) -> Result<Vec<String>, MigrationError> {
        Ok(match kind {
            StoreKind::Catalog => vec![self.location.clone()],
            StoreKind::Data | StoreKind::Account => Vec::new(),
        })
    }

    async fn read_ledger(&self, _store: &StoreRef) -> Result<Vec<LedgerRow>, MigrationError> {
        if !self.table_exists("extenddb_migrations").await? {
            return Ok(Vec::new());
        }
        let rows: Vec<LedgerColumns<i64>> = sqlx::query_as(
            "SELECT id, name, kind, checksum, outcome, notes, applied_at, applied_by \
             FROM extenddb_migrations ORDER BY id",
        )
        .fetch_all(&self.stores.pool)
        .await
        .map_err(|e| MigrationError::store("read extenddb_migrations", e))?;
        rows.into_iter()
            .map(|columns| LedgerRow::from_columns(None, columns))
            .collect()
    }

    async fn read_traces(&self) -> Result<CatalogTraces, MigrationError> {
        if !self.table_exists("settings").await? {
            return Ok(CatalogTraces::default());
        }
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT key, value FROM settings \
             WHERE key IN ('catalog_version', 'resource_tag_migration')",
        )
        .fetch_all(&self.stores.pool)
        .await
        .map_err(|e| MigrationError::store("read settings", e))?;
        Ok(CatalogTraces::from_settings(&rows))
    }

    async fn catalog_is_empty(&self) -> Result<bool, MigrationError> {
        let tables: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'")
                .fetch_one(&self.stores.pool)
                .await
                .map_err(|e| MigrationError::store("count tables", e))?;
        Ok(tables == 0)
    }

    async fn before_first_write(
        &self,
        from: Option<u32>,
        to: u32,
    ) -> Result<Vec<String>, MigrationError> {
        let Some(file) = &self.file else {
            return Ok(Vec::new());
        };
        if self.catalog_is_empty().await? {
            // An empty database (init) holds nothing to keep.
            return Ok(Vec::new());
        }
        let from = from.map_or_else(|| "legacy".to_owned(), |v| v.to_string());
        let copy = super::copy::copy_path(file, &from, to, std::time::SystemTime::now());
        let lines = super::copy::pre_migration_copy(
            &self.stores.pool,
            file,
            &copy,
            super::copy::free_space,
            |_| {},
        )
        .await?;
        *self.copy.lock().unwrap_or_else(PoisonError::into_inner) = Some(copy);
        Ok(lines)
    }

    async fn apply(
        &self,
        _store: &StoreRef,
        id: MigrationId,
        step: Step<'_, SqliteStores>,
        row: Option<LedgerRow>,
        catalog_version: Option<u32>,
    ) -> Result<Done, MigrationError> {
        if let Some(error) = self.take_restore_failure() {
            return Err(error);
        }
        self.exec("PRAGMA foreign_keys = OFF").await?;
        let result = in_transaction(
            self.exec("BEGIN IMMEDIATE"),
            self.body_and_rows(id, step, row, catalog_version),
            self.exec("COMMIT"),
            async {
                let _ = self.exec("ROLLBACK").await;
            },
        )
        .await;
        let restored = self.exec("PRAGMA foreign_keys = ON").await;
        #[cfg(test)]
        let restored = if self.fail_restore.load(Ordering::SeqCst) {
            Err(MigrationError::Store(
                "PRAGMA foreign_keys = ON: the connection was lost".to_owned(),
            ))
        } else {
            restored
        };
        let done = result?;
        // The step is committed, so it is reported as applied whatever the
        // pragma did; a failed pragma stops the run after it.
        if let Err(e) = restored {
            *self
                .restore_failed
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(MigrationError::Store(format!(
                "migration {id} was committed, and then {e}; the run stops here"
            )));
        }
        Ok(done)
    }

    async fn write_catalog_version(&self, version: u32) -> Result<(), MigrationError> {
        Rows(&self.stores.pool).write_version(version).await
    }

    async fn delete_ledger_row(&self, _store: &StoreRef, id: u32) -> Result<(), MigrationError> {
        sqlx::query("DELETE FROM extenddb_migrations WHERE id = ?")
            .bind(i64::from(id))
            .execute(&self.stores.pool)
            .await
            .map(|_| ())
            .map_err(|e| MigrationError::store(format!("forget migration {id}"), e))
    }
}
