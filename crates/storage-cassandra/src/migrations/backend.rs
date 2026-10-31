// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The Cassandra [`MigrationBackend`].
//!
//! Stores: the catalog keyspace, and one account keyspace per row of the
//! catalog's `accounts` table. Every store has a ledger,
//! `extenddb_migrations`, which the runner creates with the first row it
//! writes. Cassandra has no transactions: every statement of a body is
//! idempotent, and a body's ledger row is written at `QUORUM` after the body
//! completes, so a crash runs the body again. Only the holder of the
//! migration lock writes ledger rows, except CreateAccount, which records the
//! rows of the one keyspace it creates while its server is registered, so no
//! migration can run.

mod constructors;
mod ledger;

use std::sync::Arc;

use async_trait::async_trait;
use cdrs_tokio::consistency::Consistency;
use extenddb_storage::management_store::ManagementStore;
use extenddb_storage::migrations::{
    CatalogTraces, Done, HasManagementStore, LedgerColumns, LedgerRow, Migration, MigrationBackend,
    MigrationError, MigrationId, Released, Step, StoreKind, StoreRef, record_rows,
};

use super::agreement::{AGREEMENT_TIMEOUT, wait_for_agreement_with};
use super::connect::{
    LOCKED_DDL_TIMEOUT, UNLOCKED_DDL_TIMEOUT, column, has_columns, nullable, replication_of, rows,
    send, send_within, table_exists,
};
use super::cql::statements;
use super::guarded::LeasedManagementStore;
use super::locks::{LeaseHandle, MigrationLock};
use super::{ACCOUNT_EXPECTED, CATALOG_EXPECTED, RELEASED, migration, postcheck};
use crate::cassandra_util::CassandraSession;
use crate::catalog_store::CassandraCatalogStore;

/// The handle code bodies receive: the session, the keyspace names and the
/// catalog's management store (migration 2 rewrites policies through it),
/// every call of which is bounded by the request timeout and, under the
/// lock, checked against the lease.
pub struct CassandraStores {
    pub(crate) session: Arc<CassandraSession>,
    pub(crate) keyspace_prefix: String,
    management: LeasedManagementStore,
}

impl CassandraStores {
    fn new(
        session: Arc<CassandraSession>,
        keyspace_prefix: &str,
        lease: Option<LeaseHandle>,
    ) -> Self {
        Self {
            management: LeasedManagementStore::new(
                CassandraCatalogStore::catalog_only(session.clone(), keyspace_prefix.to_owned()),
                lease,
            ),
            session,
            keyspace_prefix: keyspace_prefix.to_owned(),
        }
    }

    pub(crate) fn catalog_keyspace(&self) -> String {
        format!("{}_catalog", self.keyspace_prefix)
    }
}

impl HasManagementStore for CassandraStores {
    fn management_store(&self) -> &dyn ManagementStore {
        &self.management
    }
}

/// The account keyspaces a run works on.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Accounts {
    /// The keyspace of every row of the catalog's `accounts` table.
    All,
    /// One keyspace: CreateAccount's.
    One(String),
    /// None: the catalog keyspace alone, for tests whose shared catalogs
    /// hold `accounts` rows of keyspaces they dropped.
    #[cfg(any(test, feature = "test-util"))]
    None,
}

/// The Cassandra backend for the runner, for one deployment.
pub struct CassandraMigrations<'l> {
    stores: CassandraStores,
    accounts: Accounts,
    lock: Option<&'l MigrationLock>,
    /// The lease of a lock another part of the command holds, for a run
    /// that does not own the lock: the account run `init` sends for the
    /// default account while it holds the migration lock. Checked and
    /// renewed like [`Self::lock`]'s.
    lease: Option<LeaseHandle>,
    /// migrator killed at that point stops: `None` outside the tests.
    stop_after: Option<Arc<std::sync::atomic::AtomicUsize>>,
    /// The application role, when this run connects as another role: an
    /// account keyspace the run creates belongs to the role it connects as,
    /// so the application role is granted its permissions on it.
    app_role: Option<String>,
}

/// The bound on a schema statement sent by a run that holds the lock
/// (`locked`) or by CreateAccount's run, which holds no lease: under the
/// lock [`LOCKED_DDL_TIMEOUT`], which is shorter than the lease's margin, so
/// a statement sent before the holder's deadline is done before its row can
/// expire, and shorter than its stall; without one the longer bound a loaded
/// node needs.
pub(super) const fn ddl_bound(locked: bool) -> std::time::Duration {
    if locked {
        LOCKED_DDL_TIMEOUT
    } else {
        UNLOCKED_DDL_TIMEOUT
    }
}

/// Whether the test hook's request budget is spent; takes one request.
pub(crate) fn stopped(left: Option<&std::sync::atomic::AtomicUsize>) -> bool {
    use std::sync::atomic::Ordering;
    let Some(left) = left else {
        return false;
    };
    // A compare-exchange loop: `fetch_update` is deprecated on current
    // stable and its replacement does not exist on 1.88.
    let mut n = left.load(Ordering::SeqCst);
    loop {
        let Some(next) = n.checked_sub(1) else {
            return true;
        };
        match left.compare_exchange(n, next, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return false,
            Err(actual) => n = actual,
        }
    }
}

impl CassandraMigrations<'_> {
    fn session(&self) -> &CassandraSession {
        &self.stores.session
    }

    /// Stop before a write when the lock's deadline has passed, or when the
    /// test hook's budget is spent.
    fn check(&self) -> Result<(), MigrationError> {
        if stopped(self.stop_after.as_deref()) {
            return Err(MigrationError::Store(
                "stopped by the test hook before a write, as a killed migrator stops".to_owned(),
            ));
        }
        match (self.lock, &self.lease) {
            (Some(lock), _) => lock.check(),
            (None, Some(lease)) => lease.check(),
            (None, None) => Ok(()),
        }
    }

    /// The bound on a schema statement: [`LOCKED_DDL_TIMEOUT`] under the
    /// lock, which keeps it shorter than the lease's margin, and
    /// [`UNLOCKED_DDL_TIMEOUT`] for CreateAccount's runs, which hold no
    /// lease.
    pub(super) fn ddl_timeout(&self) -> std::time::Duration {
        ddl_bound(self.lock.is_some() || self.lease.is_some())
    }

    /// Record progress of a locked run: a request of the runner's completed.
    fn progress(&self) {
        match (self.lock, &self.lease) {
            (Some(lock), _) => lock.progress(),
            (None, Some(lease)) => lease.progress(),
            (None, None) => {}
        }
    }

    /// Wait for schema agreement, as progress of a locked run.
    async fn agree(&self) -> Result<(), MigrationError> {
        wait_for_agreement_with(self.session(), AGREEMENT_TIMEOUT, || self.progress()).await?;
        self.progress();
        Ok(())
    }

    /// Run `work` under the lock's lease when there is one. A run under a
    /// lease alone (`init`'s default account, `for_account_under`) is not
    /// raced against the lease's end here: it runs account-store bodies
    /// only, and every one of those is a file today. A code body there
    /// would send nothing after the deadline through the management store
    /// it is handed, which checks the lease before each call; a request it
    /// sent on `CassandraStores::session` itself would not be checked.
    async fn guarded<T>(
        &self,
        work: impl std::future::Future<Output = Result<T, MigrationError>>,
    ) -> Result<T, MigrationError> {
        match self.lock {
            Some(lock) => lock.guard(work).await,
            None => work.await,
        }
    }

    /// Execute a file body's statements in `keyspace`, checking the lock
    /// before each one, recording progress when each completes, and waiting
    /// for schema agreement after each DDL statement.
    async fn run_file(&self, keyspace: &str, name: &str, text: &str) -> Result<(), MigrationError> {
        for statement in statements(keyspace, name, text)? {
            self.check()?;
            send_within(
                self.session(),
                &statement.text,
                cdrs_tokio::query_values!(),
                Consistency::Quorum,
                None,
                &format!("migration file {name} in {keyspace}"),
                self.ddl_timeout(),
            )
            .await?;
            self.progress();
            if statement.ddl {
                self.agree().await?;
            }
        }
        Ok(())
    }

    /// Create a missing account keyspace with the catalog keyspace's
    /// replication, read from `system_schema`: an `accounts` row whose
    /// keyspace a stopped CreateAccount never created (Cassandra item 5).
    async fn ensure_account_keyspace(&self, keyspace: &str) -> Result<(), MigrationError> {
        if replication_of(self.session(), keyspace).await?.is_some() {
            return Ok(());
        }
        let catalog = self.stores.catalog_keyspace();
        let replication = replication_of(self.session(), &catalog)
            .await?
            .ok_or_else(|| {
                MigrationError::Store(format!("the catalog keyspace {catalog} does not exist"))
            })?;
        self.check()?;
        send_within(
            self.session(),
            &format!("CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {replication}"),
            cdrs_tokio::query_values!(),
            Consistency::Quorum,
            None,
            &format!("create the account keyspace {keyspace}"),
            self.ddl_timeout(),
        )
        .await?;
        self.progress();
        self.agree().await?;
        // The keyspace belongs to the role this run connects as; the server
        // serves the account as the application role. The lock is checked
        // before each GRANT, as before every other write.
        if let Some(role) = &self.app_role {
            super::grant_role(
                self.session(),
                keyspace,
                role,
                true,
                || self.check(),
                || self.progress(),
            )
            .await?;
        }
        Ok(())
    }

    /// Whether this run writes the deployment's `catalog_version` row.
    /// CreateAccount adds a keyspace with every account migration, which
    /// leaves the deployment's version as it was.
    fn writes_catalog_version(&self) -> bool {
        !matches!(self.accounts, Accounts::One(_))
    }
}

/// The writes that record a body in one keyspace.
struct KeyspaceLedger<'a> {
    backend: &'a CassandraMigrations<'a>,
    keyspace: &'a str,
    /// The `catalog_version` to write with this keyspace's row. Migration
    /// 1's row and the version go in one logged batch: a stop between two
    /// writes would leave that row beside a 0.1.x version row, which the
    /// next run reads as a catalog an older release wrote to. A later row
    /// goes on its own, after the version, since its notes can be long and a
    /// logged batch is refused past the node's `batch_size_fail_threshold`
    /// (50 KB by default). A stop between them leaves the version ahead of
    /// the ledger and the id pending, which the next run applies; the other
    /// order left the last id's version behind with nothing pending.
    version: Option<u32>,
    /// Whether the row's batch wrote `version`.
    wrote_version: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl MigrationBackend for CassandraMigrations<'_> {
    type Stores = CassandraStores;

    fn backend_name(&self) -> &'static str {
        "cassandra"
    }

    fn migration(&self, id: MigrationId) -> Migration<CassandraStores> {
        migration(id)
    }

    fn released(&self) -> &'static [Released] {
        RELEASED
    }

    fn stores(&self) -> &CassandraStores {
        &self.stores
    }

    async fn store_names(&self, kind: StoreKind) -> Result<Vec<String>, MigrationError> {
        match kind {
            StoreKind::Catalog => Ok(vec![self.stores.catalog_keyspace()]),
            StoreKind::Data => Ok(Vec::new()),
            StoreKind::Account => match &self.accounts {
                Accounts::One(keyspace) => Ok(vec![keyspace.clone()]),
                #[cfg(any(test, feature = "test-util"))]
                Accounts::None => Ok(Vec::new()),
                Accounts::All => {
                    let catalog = self.stores.catalog_keyspace();
                    self.agree().await?;
                    // Another program's keyspace of the same name may hold an
                    // `accounts` table of its own.
                    if !has_columns(self.session(), &catalog, "accounts", &["account_id"]).await? {
                        return Ok(Vec::new());
                    }
                    let found = rows(
                        self.session(),
                        &format!("SELECT account_id FROM {catalog}.accounts"),
                        cdrs_tokio::query_values!(),
                        Consistency::Quorum,
                        "list the accounts",
                    )
                    .await?;
                    found
                        .iter()
                        .map(|row| {
                            let id: String = column(row, "account_id", "list the accounts")?;
                            Ok(format!("{}_account_{id}", self.stores.keyspace_prefix))
                        })
                        .collect()
                }
            },
        }
    }

    async fn read_ledger(&self, store: &StoreRef) -> Result<Vec<LedgerRow>, MigrationError> {
        // A ledger that does not exist reads as no rows only once every live
        // node agrees on the schema; any other failure is an error.
        self.agree().await?;
        if !table_exists(self.session(), &store.name, "extenddb_migrations").await? {
            return Ok(Vec::new());
        }
        let context = format!("read the ledger of {}", store.name);
        let found = rows(
            self.session(),
            &format!(
                "SELECT id, name, kind, checksum, outcome, notes, toUnixTimestamp(applied_at) AS \
                 applied_ms, applied_by FROM {}.extenddb_migrations",
                store.name
            ),
            cdrs_tokio::query_values!(),
            Consistency::Quorum,
            &context,
        )
        .await?;
        found
            .iter()
            .map(|row| {
                let text = |name: &str| -> Result<String, MigrationError> {
                    Ok(nullable::<String>(row, name, &context)?.unwrap_or_default())
                };
                let applied_ms: Option<i64> = nullable(row, "applied_ms", &context)?;
                let applied_at = applied_ms
                    .and_then(chrono::DateTime::from_timestamp_millis)
                    .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
                    .unwrap_or_default();
                let columns: LedgerColumns<i32> = (
                    column(row, "id", &context)?,
                    text("name")?,
                    text("kind")?,
                    text("checksum")?,
                    text("outcome")?,
                    text("notes")?,
                    applied_at,
                    text("applied_by")?,
                );
                LedgerRow::from_columns(Some(&store.name), columns)
            })
            .collect()
    }

    async fn read_traces(&self) -> Result<CatalogTraces, MigrationError> {
        let catalog = self.stores.catalog_keyspace();
        // A `settings` table without the catalog's columns is another
        // program's, and holds no trace of a release.
        if !has_columns(self.session(), &catalog, "settings", &["key", "value"]).await? {
            return Ok(CatalogTraces::default());
        }
        let found = rows(
            self.session(),
            &format!(
                "SELECT key, value FROM {catalog}.settings WHERE key IN ('catalog_version', \
                 'resource_tag_migration')"
            ),
            cdrs_tokio::query_values!(),
            Consistency::Quorum,
            "read the catalog's version rows",
        )
        .await?;
        let pairs = found
            .iter()
            .map(|row| {
                Ok((
                    column::<String>(row, "key", "read settings")?,
                    nullable::<String>(row, "value", "read settings")?.unwrap_or_default(),
                ))
            })
            .collect::<Result<Vec<_>, MigrationError>>()?;
        Ok(CatalogTraces::from_settings(&pairs))
    }

    /// A catalog keyspace with no ledger row and no `catalog_version` row is
    /// not a catalog yet: `init` created its `settings` table before taking
    /// the lock, or an `init` stopped part way through migration 1. No
    /// release wrote data to it, so a code body's `detect` has nothing to
    /// find, and the tables it reads may not exist until migration 1 runs.
    async fn catalog_is_empty(&self) -> Result<bool, MigrationError> {
        let catalog = StoreRef {
            kind: StoreKind::Catalog,
            name: self.stores.catalog_keyspace(),
        };
        Ok(self.read_ledger(&catalog).await?.is_empty()
            && self.read_traces().await?.catalog_version.is_none())
    }

    async fn before_first_write(
        &self,
        _from: Option<u32>,
        _to: u32,
    ) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn apply(
        &self,
        store: &StoreRef,
        id: MigrationId,
        step: Step<'_, CassandraStores>,
        row: Option<LedgerRow>,
        catalog_version: Option<u32>,
    ) -> Result<Done, MigrationError> {
        if store.kind == StoreKind::Account {
            self.ensure_account_keyspace(&store.name).await?;
        }
        let done = match step {
            Step::File { name, text } => {
                self.run_file(&store.name, name, text).await?;
                Done::default()
            }
            Step::Code(body) => {
                self.check()?;
                self.guarded(body.run(&self.stores)).await?
            }
            Step::Record => Done::default(),
        };
        // The first time migration 1 is applied to a store, the store must
        // match the v1.0 schema; a later --reconverge leaves its row as it
        // is and is not checked, since later migrations change the schema.
        if id == MigrationId::Baseline && row.is_some() {
            self.agree().await?;
            let expected = match store.kind {
                StoreKind::Catalog => CATALOG_EXPECTED,
                StoreKind::Account | StoreKind::Data => ACCOUNT_EXPECTED,
            };
            postcheck::check(self.session(), expected, &store.name).await?;
        }
        self.agree().await?;
        let ledger = KeyspaceLedger {
            backend: self,
            keyspace: &store.name,
            version: catalog_version.filter(|_| self.writes_catalog_version()),
            wrote_version: std::sync::atomic::AtomicBool::new(false),
        };
        record_rows(&ledger, row, &done.notes, catalog_version).await?;
        Ok(done)
    }

    async fn write_catalog_version(&self, version: u32) -> Result<(), MigrationError> {
        if !self.writes_catalog_version() {
            return Ok(());
        }
        self.check()?;
        send(
            self.session(),
            &format!(
                "INSERT INTO {}.settings (key, value) VALUES ('catalog_version', ?)",
                self.stores.catalog_keyspace()
            ),
            cdrs_tokio::query_values!(version.to_string()),
            Consistency::Quorum,
            None,
            "write the catalog version",
        )
        .await
        .map(drop)
    }

    async fn delete_ledger_row(&self, store: &StoreRef, id: u32) -> Result<(), MigrationError> {
        self.check()?;
        let id = i32::try_from(id)
            .map_err(|_| MigrationError::Store(format!("migration id {id} is too large")))?;
        send(
            self.session(),
            &format!(
                "DELETE FROM {}.extenddb_migrations WHERE id = ?",
                store.name
            ),
            cdrs_tokio::query_values!(id),
            Consistency::Quorum,
            None,
            &format!("forget migration {id} in {}", store.name),
        )
        .await
        .map(drop)
    }
}
