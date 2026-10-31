// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The runner's PostgreSQL parts.
//!
//! The runner opens one connection to each database, as a pool of exactly one
//! connection, so code bodies that take a pool (the management store, for
//! migration 2) run inside the runner's transaction. A connection that is
//! lost is not reopened: its transaction is gone, and statements would then
//! commit one by one.
//!
//! Per body, in its store: `BEGIN`, `SET LOCAL lock_timeout = '5s'`, the body,
//! for migration 1's catalog body the data connection rows `init` passes,
//! the post-check the first time migration 1 is applied, the ledger row and
//! (in the catalog) the version row, `COMMIT`. A lock timeout (SQLSTATE
//! 55P03) rolls back and runs the body again, three times, then stops naming
//! the tables other sessions hold locks on. A body that runs outside a
//! transaction (migration 4's concurrent index builds) runs on the pool in
//! autocommit through the index helper, and its row is written after its
//! last statement. Before every body the migration lock's connection is
//! checked.

use async_trait::async_trait;
use extenddb_storage::management_store::ManagementStore;
use extenddb_storage::migrations::{
    CatalogTraces, Done, HasManagementStore, LedgerColumns, LedgerRow, LedgerWrites, Migration,
    MigrationBackend, MigrationError, MigrationId, Released, Step, StoreKind, StoreRef,
    in_transaction, record_rows,
};
use sqlx::PgPool;

use super::connect::{
    missing_database, one_connection, relation_exists, runner_application, sql_error,
};
use super::failure::{Failure, current_txid, exec};
use super::locks::{LockHandle, MigrationLock};
use super::rows::Rows;
use super::sessions::{
    CONNECTION_CHECK, connection_check_supported, lock_timeout_stop, ownership_stop, served_data,
    wait_for_earlier_runners,
};
use super::{PostgresTarget, RunsIn, runs_in};
use crate::catalog_store::PostgresCatalogStore;

/// What PostgreSQL code bodies receive: the runner's connection to each
/// database, a management store over the catalog connection, and the
/// migration lock to check before each statement that runs outside a
/// transaction.
pub struct PostgresStores {
    pub(crate) catalog: PgPool,
    pub(crate) data: PgPool,
    /// A second connection to the data database, on its server and as its
    /// role, from which the index helper watches and cancels a statement
    /// the data connection runs. A status session has none.
    data_watch: Option<PgPool>,
    management: PostgresCatalogStore,
    lock: Option<LockHandle>,
    /// The `application_name` of this runner's sessions.
    pub(crate) application: String,
}

impl PostgresStores {
    /// Fail when the migration lock's connection is lost. A status session,
    /// or a test driving the runner itself, has no lock to check.
    pub(crate) async fn check_lock(&self) -> Result<(), MigrationError> {
        match &self.lock {
            Some(lock) => lock.check_alive().await,
            None => Ok(()),
        }
    }

    fn pool(&self, kind: StoreKind) -> &PgPool {
        match kind {
            StoreKind::Catalog => &self.catalog,
            StoreKind::Data | StoreKind::Account => &self.data,
        }
    }

    /// The connection from which a statement running on `pool` is watched
    /// and cancelled: [`Self::data_watch`] for the data connection, which
    /// reaches the data database's server as its role, and the catalog
    /// connection otherwise. Only data bodies run outside a transaction.
    pub(crate) fn watch_for(&self, pool: &PgPool) -> &PgPool {
        match &self.data_watch {
            Some(watch) if std::ptr::eq(pool, &self.data) => watch,
            _ => &self.catalog,
        }
    }
}

impl HasManagementStore for PostgresStores {
    fn management_store(&self) -> &dyn ManagementStore {
        &self.management
    }
}

/// How long each statement of a body waits for a table lock.
const LOCK_TIMEOUT_SECS: u64 = 5;

/// How many times a body is run again after a lock timeout.
const LOCK_RETRIES: u32 = 3;

/// Why opening failed.
pub(crate) enum OpenError {
    /// The catalog database does not exist (SQLSTATE 3D000).
    NoCatalogDatabase,
    Failed(MigrationError),
}

/// The PostgreSQL backend of the migration runner.
pub struct PostgresMigrations {
    stores: PostgresStores,
    catalog_name: String,
    data_name: String,
    data_connection_row: Option<String>,
    read_only: bool,
    /// Whether each database accepts [`CONNECTION_CHECK`]: catalog, data.
    connection_check: [bool; 2],
}

impl PostgresMigrations {
    /// Open the runner's connections. `lock` is the migration lock the
    /// caller holds, checked before every body. This waits until no
    /// statement of an earlier migrator runs in either database.
    ///
    /// # Errors
    ///
    /// A database cannot be reached, the catalog's data connection rows
    /// disagree with the target, or an earlier migrator's statements outlast
    /// the lock's timeout.
    pub async fn open(
        target: &PostgresTarget,
        lock: &MigrationLock,
    ) -> Result<Self, MigrationError> {
        Self::open_runner(target, Some(lock.handle())).await
    }

    /// [`PostgresMigrations::open`] with no lock, for a test that drives the
    /// runner itself; nothing is checked before each body.
    ///
    /// # Errors
    ///
    /// As [`PostgresMigrations::open`].
    #[cfg(feature = "test-util")]
    pub async fn open_unlocked(target: &PostgresTarget) -> Result<Self, MigrationError> {
        Self::open_runner(target, None).await
    }

    async fn open_runner(
        target: &PostgresTarget,
        lock: Option<LockHandle>,
    ) -> Result<Self, MigrationError> {
        Self::open_with(target, lock, false)
            .await
            .map_err(|e| match e {
                OpenError::NoCatalogDatabase => MigrationError::Store(format!(
                    "the catalog database {} does not exist; run extenddb init",
                    target.catalog_database()
                )),
                OpenError::Failed(e) => e,
            })
    }

    pub(crate) async fn open_for_status(target: &PostgresTarget) -> Result<Self, OpenError> {
        Self::open_with(target, None, true).await
    }

    async fn open_with(
        target: &PostgresTarget,
        lock: Option<LockHandle>,
        read_only: bool,
    ) -> Result<Self, OpenError> {
        let catalog_name = target.catalog_database();
        let application = runner_application();
        let named = |options: sqlx::postgres::PgConnectOptions| {
            if read_only {
                options
            } else {
                options.application_name(&application)
            }
        };
        let catalog = one_connection(
            named(target.catalog.clone()),
            read_only,
            target.connect_timeout,
        )
        .await
        .map_err(|e| {
            if missing_database(&e) {
                OpenError::NoCatalogDatabase
            } else {
                OpenError::Failed(MigrationError::store(
                    format!("connect to the catalog database {catalog_name}"),
                    e,
                ))
            }
        })?;
        let data_options = match served_data(target, &catalog).await {
            Ok(options) => options,
            Err(e) => {
                catalog.close().await;
                return Err(OpenError::Failed(e));
            }
        };
        let data_name = data_options.get_database().unwrap_or("postgres").to_owned();
        let data_options_for_watch = data_options.clone();
        let data =
            match one_connection(named(data_options), read_only, target.connect_timeout).await {
                Ok(pool) => pool,
                Err(e) => {
                    catalog.close().await;
                    return Err(OpenError::Failed(MigrationError::store(
                        format!("connect to the data database {data_name}"),
                        e,
                    )));
                }
            };
        let close_both = |e: MigrationError| {
            let (catalog, data) = (catalog.clone(), data.clone());
            async move {
                catalog.close().await;
                data.close().await;
                OpenError::Failed(e)
            }
        };
        let mut connection_check = [false; 2];
        let mut data_watch = None;
        if !read_only {
            data_watch =
                match one_connection(named(data_options_for_watch), false, target.connect_timeout)
                    .await
                {
                    Ok(pool) => Some(pool),
                    Err(e) => {
                        return Err(close_both(MigrationError::store(
                            format!("connect a second session to the data database {data_name}"),
                            e,
                        ))
                        .await);
                    }
                };
            if let Some(lock) = &lock {
                let databases = [
                    (&catalog, catalog_name.as_str()),
                    (&data, data_name.as_str()),
                ];
                if let Err(e) =
                    wait_for_earlier_runners(&databases, &application, lock.timeout()).await
                {
                    if let Some(watch) = &data_watch {
                        watch.close().await;
                    }
                    return Err(close_both(e).await);
                }
            }
            connection_check = [
                connection_check_supported(&catalog).await,
                connection_check_supported(&data).await,
            ];
        }
        Ok(Self {
            stores: PostgresStores {
                management: PostgresCatalogStore::new(catalog.clone()),
                catalog,
                data,
                data_watch,
                lock,
                application,
            },
            catalog_name,
            data_name,
            data_connection_row: target.data_connection_row.clone(),
            read_only,
            connection_check,
        })
    }

    /// Close the runner's connections. Callers close them on every exit path.
    /// A status session's read-only transaction is rolled back first, so a
    /// pooler can hand its backend to the next client.
    pub async fn close(self) {
        if self.read_only {
            let _ = exec(&self.stores.catalog, "ROLLBACK").await;
            let _ = exec(&self.stores.data, "ROLLBACK").await;
        }
        self.stores.catalog.close().await;
        self.stores.data.close().await;
        if let Some(watch) = self.stores.data_watch {
            watch.close().await;
        }
    }

    /// One attempt: the body and the rows in one transaction.
    async fn in_transaction(
        &self,
        store: &StoreRef,
        id: MigrationId,
        step: &Step<'_, PostgresStores>,
        row: Option<LedgerRow>,
        catalog_version: Option<u32>,
    ) -> Result<Done, Failure> {
        let pool = self.stores.pool(store.kind);
        in_transaction(
            exec(pool, "BEGIN"),
            self.body_and_rows(store, id, step, row, catalog_version),
            exec(pool, "COMMIT"),
            async {
                let _ = exec(pool, "ROLLBACK").await;
            },
        )
        .await
    }

    async fn body_and_rows(
        &self,
        store: &StoreRef,
        id: MigrationId,
        step: &Step<'_, PostgresStores>,
        row: Option<LedgerRow>,
        catalog_version: Option<u32>,
    ) -> Result<Done, Failure> {
        let pool = self.stores.pool(store.kind);
        exec(pool, &self.transaction_settings(store.kind)).await?;
        let txid = current_txid(pool).await?;
        let mut done = match step {
            Step::File { name, text } => {
                sqlx::raw_sql(text)
                    .execute(pool)
                    .await
                    .map_err(|e| Failure::sqlx(&format!("migration {id} ({name})"), &e))?;
                Done::default()
            }
            Step::Code(body) => body.run(&self.stores).await?,
            Step::Record => Done::default(),
        };
        if id == MigrationId::Baseline
            && store.kind == StoreKind::Catalog
            && let Some(connection) = &self.data_connection_row
        {
            for (key, value) in [
                ("data_database_connection_string", connection.as_str()),
                ("data_database_name", self.data_name.as_str()),
            ] {
                sqlx::query(
                    "INSERT INTO settings (key, value) VALUES ($1, $2) \
                     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                )
                .bind(key)
                .bind(value)
                .execute(pool)
                .await
                .map_err(|e| Failure::sqlx(&format!("record {key}"), &e))?;
            }
        }
        if current_txid(pool).await? != txid {
            return Err(Failure::Other(MigrationError::Store(format!(
                "migration {id} committed the runner's transaction itself, so its changes and its \
                 ledger row are no longer one commit; the body must not open or end transactions"
            ))));
        }
        // The first application only: on a store at a later migration the
        // objects migration 1 creates may carry what later migrations added.
        if id == MigrationId::Baseline && row.is_some() {
            let expected = match store.kind {
                StoreKind::Catalog => super::CATALOG_EXPECTED,
                StoreKind::Data | StoreKind::Account => super::DATA_EXPECTED,
            };
            done.notes
                .extend(super::postcheck::check(pool, expected, &store.name).await?);
        }
        record_rows(&Rows(pool), row, &done.notes, catalog_version).await?;
        Ok(done)
    }

    /// The `SET LOCAL` statements every runner transaction starts with: the
    /// lock timeout, no notices (every `IF NOT EXISTS` that finds its object
    /// would print one through the log), and, where the server supports it,
    /// the check that ends a statement whose client is gone.
    fn transaction_settings(&self, kind: StoreKind) -> String {
        let mut settings = format!(
            "SET LOCAL lock_timeout = '{LOCK_TIMEOUT_SECS}s'; \
             SET LOCAL client_min_messages = warning"
        );
        let check = match kind {
            StoreKind::Catalog => self.connection_check[0],
            StoreKind::Data | StoreKind::Account => self.connection_check[1],
        };
        if check {
            settings.push_str("; ");
            settings.push_str(CONNECTION_CHECK);
        }
        settings
    }

    /// A body that runs outside a transaction, then its rows.
    async fn autocommit(
        &self,
        store: &StoreRef,
        id: MigrationId,
        step: &Step<'_, PostgresStores>,
        row: Option<LedgerRow>,
        catalog_version: Option<u32>,
    ) -> Result<Done, MigrationError> {
        let Step::Code(body) = step else {
            return Err(MigrationError::Store(format!(
                "migration {id} runs outside a transaction and has no code body"
            )));
        };
        let done = body.run(&self.stores).await?;
        self.stores.check_lock().await?;
        let pool = self.stores.pool(store.kind);
        let rows = async {
            exec(pool, "BEGIN").await?;
            exec(pool, &self.transaction_settings(store.kind)).await?;
            record_rows(&Rows(pool), row, &done.notes, catalog_version).await?;
            exec(pool, "COMMIT").await
        };
        if let Err(e) = rows.await {
            let _ = exec(pool, "ROLLBACK").await;
            return Err(e.into_error());
        }
        Ok(done)
    }
}

#[async_trait]
impl MigrationBackend for PostgresMigrations {
    type Stores = PostgresStores;

    fn backend_name(&self) -> &'static str {
        "postgres"
    }

    fn migration(&self, id: MigrationId) -> Migration<PostgresStores> {
        super::migration(id)
    }

    fn released(&self) -> &'static [Released] {
        super::RELEASED
    }

    fn stores(&self) -> &PostgresStores {
        &self.stores
    }

    async fn store_names(&self, kind: StoreKind) -> Result<Vec<String>, MigrationError> {
        Ok(match kind {
            StoreKind::Catalog => vec![self.catalog_name.clone()],
            StoreKind::Data => vec![self.data_name.clone()],
            StoreKind::Account => Vec::new(),
        })
    }

    async fn read_ledger(&self, store: &StoreRef) -> Result<Vec<LedgerRow>, MigrationError> {
        let pool = self.stores.pool(store.kind);
        if !relation_exists(pool, "extenddb_migrations").await? {
            return Ok(Vec::new());
        }
        let rows: Vec<LedgerColumns<i32>> = sqlx::query_as(
            "SELECT id, name, kind, checksum, outcome, notes, applied_at::text, applied_by \
             FROM extenddb_migrations ORDER BY id",
        )
        .fetch_all(pool)
        .await
        .map_err(|e| sql_error(&format!("read extenddb_migrations in {}", store.name), &e))?;
        rows.into_iter()
            .map(|columns| LedgerRow::from_columns(Some(&store.name), columns))
            .collect()
    }

    async fn read_traces(&self) -> Result<CatalogTraces, MigrationError> {
        let pool = &self.stores.catalog;
        if !relation_exists(pool, "settings").await? {
            return Ok(CatalogTraces::default());
        }
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT key, value FROM settings \
             WHERE key IN ('catalog_version', 'resource_tag_migration')",
        )
        .fetch_all(pool)
        .await
        .map_err(|e| sql_error("read settings", &e))?;
        Ok(CatalogTraces::from_settings(&rows))
    }

    async fn catalog_is_empty(&self) -> Result<bool, MigrationError> {
        let empty: bool = sqlx::query_scalar(
            "SELECT NOT EXISTS (SELECT 1 FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p'))",
        )
        .fetch_one(&self.stores.catalog)
        .await
        .map_err(|e| MigrationError::store("count tables", e))?;
        Ok(empty)
    }

    async fn before_first_write(
        &self,
        _from: Option<u32>,
        _to: u32,
    ) -> Result<Vec<String>, MigrationError> {
        self.stores.check_lock().await?;
        Ok(Vec::new())
    }

    async fn apply(
        &self,
        store: &StoreRef,
        id: MigrationId,
        step: Step<'_, PostgresStores>,
        row: Option<LedgerRow>,
        catalog_version: Option<u32>,
    ) -> Result<Done, MigrationError> {
        self.stores.check_lock().await?;
        if matches!(step, Step::Code(_)) && runs_in(id) == RunsIn::Autocommit {
            return self
                .autocommit(store, id, &step, row, catalog_version)
                .await;
        }
        let mut retries = 0;
        loop {
            match self
                .in_transaction(store, id, &step, row.clone(), catalog_version)
                .await
            {
                Ok(done) => return Ok(done),
                Err(Failure::LockTimeout(e)) if retries < LOCK_RETRIES => {
                    retries += 1;
                    tracing::warn!(
                        "migration {id}: a lock wait in {} passed {LOCK_TIMEOUT_SECS} s \
                         ({e}); running it again, {retries} of {LOCK_RETRIES}",
                        store.name
                    );
                    self.stores.check_lock().await?;
                }
                Err(Failure::LockTimeout(e)) => {
                    return Err(lock_timeout_stop(
                        self.stores.pool(store.kind),
                        store,
                        id,
                        &e.to_string(),
                        LOCK_TIMEOUT_SECS,
                        LOCK_RETRIES,
                    )
                    .await);
                }
                Err(Failure::Privilege(e)) => {
                    return Err(ownership_stop(
                        self.stores.pool(store.kind),
                        store,
                        id,
                        &e.to_string(),
                    )
                    .await);
                }
                Err(Failure::Other(e)) => return Err(e),
            }
        }
    }

    async fn write_catalog_version(&self, version: u32) -> Result<(), MigrationError> {
        Rows(&self.stores.catalog)
            .write_version(version)
            .await
            .map_err(Failure::into_error)
    }

    async fn delete_ledger_row(&self, store: &StoreRef, id: u32) -> Result<(), MigrationError> {
        sqlx::query("DELETE FROM extenddb_migrations WHERE id = $1")
            .bind(i32::try_from(id).unwrap_or(i32::MAX))
            .execute(self.stores.pool(store.kind))
            .await
            .map(|_| ())
            .map_err(|e| MigrationError::store(format!("forget migration {id}"), e))
    }
}
