// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `PostgreSQL` storage backend for extenddb.
//!
//! Implements the `TableEngine` and `DataEngine` traits from `extenddb-storage`
//! using `PostgreSQL` via `sqlx`. All SQL uses parameterized queries exclusively
//! — no dynamic SQL, except for per-DynamoDB-table DDL where table names are
//! validated at the engine layer.

mod admin_store;
mod authorization_store;
mod backup_engine;
mod bootstrapper;
mod catalog_store;
pub mod config;
mod create_table;
mod credential_store;
mod data;
mod delete_table;
pub(crate) mod gsi_queue;
mod management_store;
mod metadata_engine;
mod migrations;
mod operations;
mod pg_util;
#[cfg(test)]
#[path = "../../config/src/sample_toml_support.rs"]
mod sample_toml_support;
#[cfg(test)]
mod sample_toml_tests;
mod snapshot_engine;
#[doc(hidden)]
pub use snapshot_engine::map_fetch_error;
mod stream_engine;
#[doc(hidden)]
pub mod stream_records_sql;
mod table_engine;
mod table_helpers;
mod ttl_worker;
mod update_table;
mod vector;
mod vector_search;
mod worker_store;
mod workers;

pub use bootstrapper::PostgresBootstrapper;
pub use catalog_store::PostgresCatalogStore;
pub use config::PostgresStorageConfig;
pub use config::parse_connection_string;
pub use credential_store::DbCredentialStore;
/// Finish GSI builds left `CREATING`, once. The server runs it from a background
/// worker; exported for its test.
pub use data::gsi_build::reconcile_creating_gsis;
/// Make writes pause at a chosen point, so a test can produce a write that missed
/// a GSI created in between. Hidden because no deployment path calls it.
#[doc(hidden)]
pub use data::gsi_build::{PauseWrites, pause_writes_for_tests};
/// Apply one queued vector row from its own context.
///
/// Reachable so an integration test can drive the classification the propagation
/// worker performs, and hidden because starting or running a deployment never calls
/// it, unlike the two recovery entry points above.
#[doc(hidden)]
pub use data::vector_index::apply_claimed_vector_row;
/// Try to take ownership of one vector index's build.
///
/// Reachable so an integration test can assert that ownership is held in a session
/// of its own and given back when the owner is dropped, which is only observable
/// from a second session. Hidden for the same reason as the row applier: no
/// deployment path calls it.
#[doc(hidden)]
pub use data::vector_index::build_ownership;
/// Flip one vector index to ACTIVE the way a finished build does. Reachable so a
/// test can drive completion against a catalog row that is already gone, and
/// hidden for the same reason as the two entry points above.
#[doc(hidden)]
pub use data::vector_index::mark_vector_index_active;
/// Rebuild vector index builds whose heartbeat has gone stale. The runtime half of
/// the same repair, exported for the same reason and for its test.
pub use data::vector_index::rebuild_stuck_vector_indexes;
/// Rebuild vector indexes a crash left mid-build. A startup step, exported because
/// it is part of bringing a deployment up rather than an internal detail.
pub use data::vector_index::reconcile_incomplete_vector_indexes;
/// The relation names a table's storage uses, for tests that pin what a drop
/// removes; the crate owns the format (`data/mod.rs`).
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub use data::{data_table_name, index_table_name, vector_table_name};
/// Hold a `DeleteTable` between its in-use probe and its drops, or (separate
/// data database) between its catalog commit and its data commit; and shorten
/// the bound on the drop retried after a failed data commit.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub use delete_table::test_pause::{
    DataDropRetryBound, DeleteTablePause, DeleteTablePoint, arm_delete_table_pause,
    arm_delete_table_pause_at, shorten_data_drop_retry_bound_for_tests,
};
/// Every migration, as `extenddb migrate --yes` applies them, for the test
/// harnesses in `tests/` so they do not carry their own copy of the lists.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub use migrations::apply_all_migrations;

/// The `PostgreSQL` storage backend.
///
/// A thin `main` installs it before dispatching any subcommand:
///
/// ```ignore
/// extenddb_storage::set_backend(extenddb_storage_postgres::backend())?;
/// ```
pub fn backend() -> extenddb_storage::Backend {
    extenddb_storage::Backend {
        name: "postgres",
        bootstrapper: |config_path, cli_args| {
            Box::pin(async move {
                let store = PostgresBootstrapper::from_config(&config_path, &cli_args).await?;
                Ok(Box::new(store) as Box<dyn extenddb_storage::bootstrapper::Bootstrapper>)
            })
        },
        operations: &operations::PostgresOperationsEngine,
        storage_config: |table| {
            let config: PostgresStorageConfig = table
                .clone()
                .try_into()
                .map_err(|e: toml::de::Error| format!("Failed to parse postgres config: {e}"))?;
            Ok(Box::new(config) as Box<dyn extenddb_storage::config::StorageConfig>)
        },
        settings_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let pool = sqlx::PgPool::connect(&connection_string)
                    .await
                    .map_err(|e| {
                        extenddb_storage::settings_store::SettingsStoreError::ConnectionFailed(
                            e.to_string(),
                        )
                    })?;
                Ok(Box::new(PostgresCatalogStore::new(pool))
                    as Box<
                        dyn extenddb_storage::management_store::SettingsStore,
                    >)
            })
        },
        management_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let pool = sqlx::PgPool::connect(&connection_string)
                    .await
                    .map_err(|e| {
                        extenddb_storage::management_store_factory::ManagementStoreError::ConnectionFailed(
                            e.to_string(),
                        )
                    })?;
                Ok(Box::new(PostgresCatalogStore::new(pool))
                    as Box<
                        dyn extenddb_storage::management_store::ManagementStore,
                    >)
            })
        },
        diagnostics_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let pool = sqlx::PgPool::connect(&connection_string)
                    .await
                    .map_err(|e| {
                        extenddb_storage::diagnostics_store::DiagnosticsStoreError::ConnectionFailed(
                            e.to_string(),
                        )
                    })?;
                Ok(Box::new(PostgresCatalogStore::new(pool))
                    as Box<dyn extenddb_storage::diagnostics::DiagnosticsStore>)
            })
        },
        server_components: server_components_factory,
    }
}

use std::sync::Arc;

use extenddb_core::version::CatalogVersion;
use extenddb_storage::error::StorageError;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// Expected catalog version — compiled into the binary (REQ-CAT-006, D-9).
///
/// The tuple is the single source of truth. Use `CATALOG_VERSION.to_string()`
/// wherever a string representation is needed.
pub const CATALOG_VERSION: CatalogVersion = CatalogVersion::new(0, 0, 4);

/// Minimum number of connections allowed per pool.
///
/// Each `DynamoDB` request triggers an auth/authz query fanout against the
/// catalog pool. Pools smaller than this floor starve under concurrent load.
/// Configured values below the floor are clamped at startup with a warning.
const MIN_POOL_SIZE: u32 = 10;

/// `PostgreSQL` storage backend configuration.
pub struct PostgresConfig {
    pub connection_string: String,
    pub pool_size: u32,
    /// Maximum item size in bytes for post-update validation.
    pub max_item_size_bytes: usize,
}

/// `PostgreSQL` storage backend.
///
/// The engine no longer stores a single `account_id`. Instead, `account_id`
/// is passed per-request through the storage trait methods, enabling
/// multi-account isolation (Phase 12f).
///
/// Uses two connection pools: `pool` for catalog metadata (tables, indexes,
/// settings, accounts, IAM) and `data_pool` for per-DynamoDB-table data
/// (`_ddb_*` tables, GSI tables). This separation allows the catalog and
/// data to live in different `PostgreSQL` databases (Bug 1, P54).
/// Default GSI propagation delay (milliseconds) when the
/// `index_propagation_delay_ms` setting is absent. Mirrors the value seeded by
/// the catalog schema, and is the single definition used by both the live read
/// on the write path and the background refresh worker.
pub(crate) const DEFAULT_INDEX_PROPAGATION_DELAY_MS: u64 = 10;

/// Read the propagation-delay setting, preferring the canonical key and falling back
/// to the pre-rename one.
///
/// A catalog created before the rename holds the operator's value under the old name,
/// and the server refuses to start on a catalog-version mismatch rather than migrating,
/// so no upgrade step ever rewrites that row. Reading past it would silently reset a
/// configured delay to the default; since 0 means synchronous, the silent change would
/// be from strict to eventually consistent. `ORDER BY ... DESC` makes the preference
/// deterministic when both rows exist.
pub(crate) const INDEX_PROPAGATION_DELAY_QUERY: &str = "SELECT value FROM settings \
     WHERE key IN ('index_propagation_delay_ms', 'gsi_propagation_delay_ms') \
     ORDER BY key = 'index_propagation_delay_ms' DESC LIMIT 1";

/// Cloning is cheap and shares the pools and caches: a clone is another
/// handle on the same engine. RestoreTableFromBackup hands one to the detached
/// task that copies the backup's items, so the copy does not borrow from the
/// request.
#[derive(Clone)]
pub struct PostgresEngine {
    pub(crate) pool: PgPool,
    /// Connection pool for the data database where `_ddb_*` tables live.
    pub(crate) data_pool: PgPool,
    pub(crate) region: String,
    pub(crate) max_item_size_bytes: usize,
    /// F-3: Wakes the control plane poller when a table enters CREATING or
    /// DELETING state, so transitions are processed without polling delay.
    pub(crate) control_plane_notify: Arc<tokio::sync::Notify>,
    /// D-4: Async GSI update queue. `None` until `start_gsi_workers()` is called.
    pub(crate) gsi_queue: Option<Arc<gsi_queue::GsiQueue>>,
    /// P119: Cached GSI default propagation delay (milliseconds). Refreshed by
    /// the background poller every 30s and re-warmed by `index_propagation_delay`.
    /// This is only a fallback for when the live read fails; the write path
    /// reads the setting live so a runtime change applies to the next write.
    pub index_propagation_delay_cache: Arc<std::sync::atomic::AtomicU64>,
    /// Whether the data database has the pgvector extension, probed once at
    /// construction.
    ///
    /// Vector indexes live in `vector(N)` columns, a type pgvector defines, so
    /// the capability is a property of the server rather than of this build.
    /// Cached rather than probed per request: a control-plane feature does not
    /// justify a round trip on every call, and the cost of caching is that
    /// installing pgvector on a live server needs an ExtendDB restart to be
    /// noticed. `DataEngine::as_vector_search` stays at its `None` default until
    /// the search path exists, so this backend still refuses vector operations
    /// over the wire; the flag is what that decision will read.
    pub(crate) vector_capable: bool,
    /// Restores this engine (one per server process) may have in flight
    /// across all accounts; see `backup_engine::restore_server_slot_count`.
    pub(crate) restore_server_slots: Arc<tokio::sync::Semaphore>,
    /// The number of permits `restore_server_slots` was created with, for
    /// the refusal message.
    pub(crate) restore_server_slot_count: usize,
    /// Restores each account may run at once in this process; see
    /// `backup_engine::RestoreAccountSlots`.
    pub(crate) restore_account_slots: backup_engine::RestoreAccountSlots,
    /// Bounds how many table snapshots may be open at once (one permit from
    /// the open until the stream's transaction ends): half the pool
    /// ([`snapshot_slot_count`]) on either layout.
    ///
    /// On one shared database an open and its stream use one connection and
    /// one transaction for catalog and data, so the permits cap snapshots at
    /// half the pool's connections and no open ever waits for a second
    /// connection while holding one. With a separate data database an open
    /// holds a catalog connection (the barrier) while it takes a data
    /// connection, and the stream keeps the data connection; the permits cap
    /// barriers at half the catalog pool and streams at half the data pool,
    /// and the catalog-then-data order every path follows cannot form a
    /// cycle.
    ///
    /// What the permits do not prevent: request load alone exhausting a pool
    /// (an open then waits for its connection up to the acquire timeout like
    /// any request), and restores, which take their own slots (half the
    /// pool, at most 16: `backup_engine::restore_server_slot_count`), so
    /// snapshots and restores together can hold the whole pool. On the separate layout requests that block on
    /// the `tables` row an open holds `FOR SHARE` (`UpdateTable`,
    /// `DeleteTable` of that table) hold catalog connections while they
    /// wait; the open needs only a data connection by then, so they delay it
    /// no further.
    pub(crate) snapshot_open_slots: Arc<tokio::sync::Semaphore>,
    /// Whether `data_pool` is the catalog pool (no separate data database).
    /// The snapshot open uses one transaction on that layout, and
    /// `DeleteTable` runs its in-use probe and its synchronous drop on the
    /// connection it holds rather than taking a second from the same pool.
    pub(crate) shared_data_pool: bool,
    /// Drop attempts the control plane has made (`control_plane_pass`), a
    /// `DELETING` table its probe found held and skipped counted as one, for
    /// tests that count them. Shared by clones.
    #[cfg(feature = "test-hooks")]
    pub(crate) drop_attempts: Arc<std::sync::atomic::AtomicU64>,
}

/// Table snapshots one engine may hold open at once: half the pool (at least
/// five, since the pool is at least `MIN_POOL_SIZE`). Each open snapshot
/// holds one connection of the pool its items come from (on one shared
/// database, the one pool), so open snapshots leave at least half of it to
/// requests; see `PostgresEngine::snapshot_open_slots` for what that does and
/// does not prevent on each layout.
fn snapshot_slot_count(pool_size: u32) -> usize {
    usize::try_from((pool_size / 2).max(1)).unwrap_or(usize::MAX)
}

/// Stops the poller started by
/// [`PostgresEngine::spawn_control_plane_poller_for_tests`] when dropped.
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub struct ControlPlanePollerGuard {
    token: extenddb_storage::CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "test-hooks")]
impl Drop for ControlPlanePollerGuard {
    fn drop(&mut self) {
        self.token.cancel();
        self.handle.abort();
    }
}

impl PostgresEngine {
    pub async fn new(config: &PostgresConfig, region: &str) -> Result<Self, StorageError> {
        // Enforce a minimum of 10 connections per pool. Smaller values starve
        // the auth/authz query fanout under concurrent load. If the configured
        // value is below the floor, log a warning and clamp.
        let pool_size = if config.pool_size < MIN_POOL_SIZE {
            tracing::warn!(
                "storage.postgres.pool_size = {} is below the minimum of {}; clamping to {}",
                config.pool_size,
                MIN_POOL_SIZE,
                MIN_POOL_SIZE
            );
            MIN_POOL_SIZE
        } else {
            config.pool_size
        };

        // P79/P6: Set min_connections to avoid cold-start latency on first requests.
        let min_conns = pool_size.min(2);
        let pool = PgPoolOptions::new()
            .max_connections(pool_size)
            .min_connections(min_conns)
            .test_before_acquire(false)
            .max_lifetime(std::time::Duration::from_secs(1800))
            .connect(&config.connection_string)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        // P54 Bug 1: Read data database connection string from catalog settings.
        // Falls back to the catalog pool if no separate data database is configured.
        let mut shared_data_pool = false;
        let data_pool = match sqlx::query_as::<_, (String,)>(
            "SELECT value FROM settings WHERE key = 'data_database_connection_string'",
        )
        .fetch_optional(&pool)
        .await
        {
            Ok(Some((data_conn,))) if !data_conn.is_empty() => PgPoolOptions::new()
                .max_connections(pool_size)
                .min_connections(min_conns)
                .test_before_acquire(false)
                .max_lifetime(std::time::Duration::from_secs(1800))
                .connect(&data_conn)
                .await
                .map_err(|e| {
                    StorageError::Connection(format!("data database connection failed: {e}"))
                })?,
            _ => {
                shared_data_pool = true;
                pool.clone()
            }
        };

        // P119: Read initial GSI propagation delay from settings table.
        let initial_gsi_delay: u64 = sqlx::query_as::<_, (String,)>(INDEX_PROPAGATION_DELAY_QUERY)
            .fetch_optional(&pool)
            .await
            .ok()
            .flatten()
            .and_then(|(v,)| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_INDEX_PROPAGATION_DELAY_MS);

        // Probe the pgvector extension once, on the data database, where vector
        // data tables live. Logged at startup either way so an operator can see
        // which answer this process is serving without reading the catalog.
        let vector_version = vector::probe_vector_extension(&data_pool).await;
        let vector_capable = vector_version.is_some();
        match &vector_version {
            Some(version) => tracing::info!(
                "pgvector {version} detected on the data database; vector index storage available"
            ),
            None => tracing::info!(
                "pgvector not installed on the data database; vector indexes are not supported"
            ),
        }

        let server_slots = backup_engine::restore_server_slot_count(pool_size);
        Ok(Self {
            pool,
            data_pool,
            region: region.to_owned(),
            max_item_size_bytes: config.max_item_size_bytes,
            control_plane_notify: Arc::new(tokio::sync::Notify::new()),
            gsi_queue: None,
            index_propagation_delay_cache: Arc::new(std::sync::atomic::AtomicU64::new(
                initial_gsi_delay,
            )),
            vector_capable,
            restore_server_slots: Arc::new(tokio::sync::Semaphore::new(server_slots)),
            restore_server_slot_count: server_slots,
            restore_account_slots: backup_engine::RestoreAccountSlots::default(),
            snapshot_open_slots: Arc::new(tokio::sync::Semaphore::new(snapshot_slot_count(
                pool_size,
            ))),
            shared_data_pool,
            #[cfg(feature = "test-hooks")]
            drop_attempts: Arc::default(),
        })
    }

    /// Start the async GSI worker tasks (D-4).
    ///
    /// Must be called after construction, before serving requests.
    /// Returns `&Self` for chaining.
    #[must_use]
    /// Current GSI propagation delay (ms); `0` means synchronous.
    ///
    /// Reads the `index_propagation_delay_ms` setting live from the catalog so an
    /// out-of-process change (`extenddb settings set`) applies to the next write
    /// rather than up to 30 s later when the poll worker refreshes the cache.
    /// Callers skip this entirely for tables with no secondary indexes, so a
    /// table that cannot propagate pays nothing.
    ///
    /// On a read error the cached value is used and the error is logged, so a
    /// degraded catalog serves a stale delay loudly rather than silently. On
    /// success the cache is re-warmed, keeping the fallback fresh.
    pub(crate) async fn index_propagation_delay(&self) -> u64 {
        use std::sync::atomic::Ordering;
        let live = sqlx::query_as::<_, (String,)>(INDEX_PROPAGATION_DELAY_QUERY)
            .fetch_optional(&self.pool)
            .await;
        match live {
            Ok(row) => {
                let ms = row
                    .and_then(|(v,)| v.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_INDEX_PROPAGATION_DELAY_MS);
                self.index_propagation_delay_cache
                    .store(ms, Ordering::Relaxed);
                ms
            }
            Err(e) => {
                tracing::debug!("index_propagation_delay: live read failed, using cache: {e:?}");
                self.index_propagation_delay_cache.load(Ordering::Relaxed)
            }
        }
    }

    pub fn start_gsi_workers(mut self) -> Self {
        self.gsi_queue = Some(gsi_queue::GsiQueue::spawn(self.data_pool.clone()));
        self
    }

    /// Returns a handle to the control plane notify, for use by the
    /// background poller task (F-3).
    #[must_use]
    pub fn control_plane_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.control_plane_notify)
    }

    /// Run the control-plane poller `serve` runs (`spawn_workers`), on a
    /// clone of this engine with a settings store on its catalog pool, until
    /// the returned guard is dropped. For tests that need the production
    /// wake-up path rather than calling `process_control_plane_transitions`
    /// themselves.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn spawn_control_plane_poller_for_tests(&self) -> ControlPlanePollerGuard {
        self.spawn_control_plane_poller_with_active_poll_for_tests(workers::ACTIVE_POLL)
    }

    /// [`Self::spawn_control_plane_poller_for_tests`] with the active loop's
    /// retry interval set to `active_poll` instead of one second, so a test
    /// can tell a pass run by a wake from one run by the timer.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn spawn_control_plane_poller_with_active_poll_for_tests(
        &self,
        active_poll: std::time::Duration,
    ) -> ControlPlanePollerGuard {
        let token = extenddb_storage::CancellationToken::new();
        let handle = tokio::spawn(workers::poll_control_plane_transitions(
            Arc::new(self.clone()),
            self.control_plane_notify(),
            Arc::new(PostgresCatalogStore::new(self.pool.clone())),
            token.clone(),
            active_poll,
        ));
        ControlPlanePollerGuard { token, handle }
    }

    /// Run the startup recovery `serve` runs before it starts the poller
    /// (`server_components_factory`), on this engine.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub async fn recover_control_plane_at_startup_for_tests(&self) {
        workers::recover_control_plane_at_startup(self).await;
    }

    /// Drop attempts this engine's control plane has made so far, a table
    /// its probe found held and skipped counted as one.
    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn control_plane_drop_attempts_for_tests(&self) -> u64 {
        self.drop_attempts
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Defense-in-depth: validate `account_id` before use in SQL identifiers.
    ///
    /// `account_id` is interpolated into SQL identifiers via `data_table_name()`.
    /// Called by all methods that use `data_table_name()` or `format!`-based DDL.
    /// Reject values that could break quoted identifiers.
    /// See `docs/adr/sql-injection-defense.md`.
    pub(crate) fn validate_account_id(account_id: &str) -> Result<(), StorageError> {
        if account_id.contains('"') || account_id.contains('\0') || !account_id.is_ascii() {
            return Err(StorageError::Internal(
                "account_id contains invalid characters for use in SQL identifiers".to_owned(),
            ));
        }
        Ok(())
    }

    /// Validate catalog version matches the compiled-in expectation (REQ-CAT-007, D-10).
    ///
    /// Reads the version string from the `settings` table and parses it
    /// strictly into a `CatalogVersion`. Rejects malformed strings.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::CatalogNotInitialized` if the catalog tables don't exist.
    /// Returns `StorageError::CatalogVersionMismatch` if the version doesn't match.
    /// Returns `StorageError::Internal` if the stored version string is malformed, carrying
    /// `check_stored_catalog_version`'s refusal text.
    pub async fn check_catalog_version(&self) -> Result<(), StorageError> {
        // Check table existence via information_schema (robust, not string-matching).
        let exists: (bool,) = sqlx::query_as(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'settings' AND table_schema = 'public')",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !exists.0 {
            return Err(StorageError::CatalogNotInitialized);
        }

        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM settings WHERE key = 'catalog_version'")
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

        let found_str = row.ok_or(StorageError::CatalogNotInitialized)?.0;

        // An unreadable version gets the shared guard's refusal, the text
        // `extenddb migrate` prints for the same catalog.
        let found = found_str.parse::<CatalogVersion>().map_err(|e| {
            StorageError::Internal(
                extenddb_storage::util::check_stored_catalog_version(
                    Some(&found_str),
                    CATALOG_VERSION,
                )
                .err()
                .unwrap_or_else(|| e.to_string()),
            )
        })?;

        if found != CATALOG_VERSION {
            return Err(StorageError::CatalogVersionMismatch {
                expected: CATALOG_VERSION.to_string(),
                found: found_str,
            });
        }

        Ok(())
    }

    /// Everything `serve` checks and changes in the catalog before serving,
    /// in order; returns the shape steps applied.
    ///
    /// 1. The version gate ([`Self::check_catalog_version`]).
    /// 2. The ledger: a catalog migration file this binary carries that the
    ///    catalog's `schema_history` does not record refuses startup, naming
    ///    the files. The version alone does not show it: another migration of
    ///    the same number (another branch's `003_*`) can have stamped the
    ///    current version. Every catalog has a ledger (`001_schema.sql`
    ///    creates it and the runner records each file it applies), so a
    ///    catalog that ran every file lists none here.
    /// 3. The startup shape steps (`migrations::converge_catalog_shapes_at_serve`):
    ///    a catalog stamped with the current version by an earlier revision of
    ///    a migration file is otherwise served in a shape on which every
    ///    DescribeTable fails. Each step waits at most
    ///    `migrations::SERVE_SHAPE_LOCK_TIMEOUT_SECS` for its lock. On a
    ///    current catalog this only reads `information_schema`.
    ///
    /// # Errors
    ///
    /// `BackendError::CatalogVersionMismatch` from the gate;
    /// `BackendError::InitializationFailed` for every other failure, with
    /// the advice to run `extenddb migrate --yes` for steps 2 and 3.
    pub async fn prepare_catalog_for_serve(&self) -> Result<Vec<String>, BackendError> {
        self.check_catalog_version().await.map_err(|e| match e {
            StorageError::CatalogVersionMismatch { expected, found } => {
                BackendError::CatalogVersionMismatch { expected, found }
            }
            // The unreadable-version refusal: printed as is, not as
            // "Internal error: ...".
            StorageError::Internal(refusal) => BackendError::InitializationFailed(refusal),
            _ => BackendError::InitializationFailed(e.to_string()),
        })?;
        let migrate =
            "; stop the servers and run `extenddb migrate --yes` with the admin credentials";
        let unrecorded = migrations::pending_catalog_migrations(&self.pool)
            .await
            .map_err(|e| BackendError::InitializationFailed(format!("{}{migrate}", op_msg(e))))?;
        if !unrecorded.is_empty() {
            return Err(BackendError::InitializationFailed(format!(
                "the catalog does not record these catalog migration files as applied: {}{migrate}",
                unrecorded.join(", ")
            )));
        }
        migrations::converge_catalog_shapes_at_serve(&self.pool)
            .await
            .map_err(|e| BackendError::InitializationFailed(format!("{}{migrate}", op_msg(e))))
    }

    /// Query the data database name from the catalog for the startup banner (REQ-LOG-001).
    ///
    /// Returns `"(not configured)"` if no data database has been registered.
    ///
    /// # Errors
    ///
    /// Returns `StorageError::Connection` if the query fails.
    pub async fn get_data_database_info(&self) -> Result<String, StorageError> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM settings WHERE key = 'data_database_name'")
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(row.map_or_else(|| "(not configured)".to_owned(), |(name,)| name))
    }

    /// Returns a reference to the data pool for use by background workers
    /// that operate on `_ddb_*` tables (e.g., TTL cleanup, table size refresh).
    #[must_use]
    pub fn data_pool(&self) -> &PgPool {
        &self.data_pool
    }

    /// Milliseconds to pause between vector backfill batches.
    ///
    /// Read live for the same reason the propagation delay is: a test sets it with
    /// `settings set` and needs it to apply to the next backfill rather than up to
    /// 30 s later. Zero when unset or unparseable, which is the production value,
    /// so a malformed setting cannot slow a real backfill down.
    pub(crate) async fn vector_backfill_batch_delay(&self) -> u64 {
        let live: Result<Option<(String,)>, _> =
            sqlx::query_as("SELECT value FROM settings WHERE key = $1")
                .bind(extenddb_core::settings_keys::VECTOR_BACKFILL_BATCH_DELAY_MS)
                .fetch_optional(&self.pool)
                .await;
        match live {
            Ok(row) => row.and_then(|(v,)| v.parse::<u64>().ok()).unwrap_or(0),
            Err(e) => {
                tracing::debug!("vector_backfill_batch_delay: live read failed, using 0: {e:?}");
                0
            }
        }
    }

    /// Minimum milliseconds an UpdateTable-created vector index stays `CREATING`
    /// before its `ACTIVE` flip. See
    /// [`extenddb_core::settings_keys::VECTOR_INDEX_MIN_CREATING_MS`] for why the
    /// hold exists; the SQLite backend applies the same floor. Defaults to 1000
    /// when unset or unparseable; zero disables it.
    pub(crate) async fn vector_index_min_creating_ms(&self) -> u64 {
        const DEFAULT_MS: u64 = 1_000;
        let live: Result<Option<(String,)>, _> =
            sqlx::query_as("SELECT value FROM settings WHERE key = $1")
                .bind(extenddb_core::settings_keys::VECTOR_INDEX_MIN_CREATING_MS)
                .fetch_optional(&self.pool)
                .await;
        match live {
            Ok(row) => row
                .and_then(|(v,)| v.parse::<u64>().ok())
                .unwrap_or(DEFAULT_MS),
            Err(e) => {
                tracing::debug!(
                    "vector_index_min_creating_ms: live read failed, using {DEFAULT_MS}: {e:?}"
                );
                DEFAULT_MS
            }
        }
    }

    /// Milliseconds to hold a new vector index in the resource-allocation phase.
    ///
    /// A test lever, zero in production, read live for the same reason the batch
    /// delay is. Held inside the detached build task rather than in the request
    /// path, because the phase is only observable to a client after `UpdateTable`
    /// has returned.
    pub(crate) async fn vector_allocation_phase_delay(&self) -> u64 {
        let live: Result<Option<(String,)>, _> =
            sqlx::query_as("SELECT value FROM settings WHERE key = $1")
                .bind(extenddb_core::settings_keys::VECTOR_ALLOCATION_PHASE_DELAY_MS)
                .fetch_optional(&self.pool)
                .await;
        match live {
            Ok(row) => row.and_then(|(v,)| v.parse::<u64>().ok()).unwrap_or(0),
            Err(e) => {
                tracing::debug!("vector_allocation_phase_delay: live read failed, using 0: {e:?}");
                0
            }
        }
    }

    /// Whether the data database has pgvector, as probed at construction.
    ///
    /// Public so that a deployment check, and the tests that pin the
    /// fail-closed behaviour, can read the same answer the engine acts on.
    #[must_use]
    pub fn vector_capable(&self) -> bool {
        self.vector_capable
    }
}

// ============================================================================
// ServerComponents Factory Registration
// ============================================================================

use extenddb_auth::CredentialStore;
use extenddb_storage::hooks::{ServerRuntimeHooks, WorkerContext};
use extenddb_storage::server_components::{BackendError, ServerComponents};

/// The message of a management-store error, without its variant name.
fn op_msg(e: extenddb_storage::management_store::OpError) -> String {
    use extenddb_storage::management_store::OpError;
    match e {
        OpError::Validation(m)
        | OpError::AlreadyExists(m)
        | OpError::NotFound(m)
        | OpError::HasDependents(m)
        | OpError::Internal(m) => m,
    }
}

/// Backend-specific runtime hooks for `PostgreSQL`.
struct PostgresRuntimeHooks {
    engine: Arc<PostgresEngine>,
    control_plane_notify: Arc<tokio::sync::Notify>,
    index_propagation_delay_cache: Arc<std::sync::atomic::AtomicU64>,
    data_db_name: String,
}

#[async_trait::async_trait]
impl ServerRuntimeHooks for PostgresRuntimeHooks {
    async fn spawn_workers(&self, ctx: &WorkerContext) -> Vec<tokio::task::JoinHandle<()>> {
        // Backend-specific workers that need PostgreSQL internals. Each takes
        // the shutdown token and returns at its next tick after cancellation;
        // the handles are returned so `serve` can drain them.

        // 1. Control plane transitions poller
        let storage_for_poller = self.engine.clone();
        let cp_notify = self.control_plane_notify.clone();
        let catalog_store = ctx.catalog_store.clone();
        let token = ctx.shutdown.clone();
        let control_plane = tokio::spawn(async move {
            workers::poll_control_plane_transitions(
                storage_for_poller,
                cp_notify,
                catalog_store,
                token,
                workers::ACTIVE_POLL,
            )
            .await;
        });

        // 2. Table size refresh worker
        let storage_for_size = self.engine.clone();
        let token = ctx.shutdown.clone();
        let table_size = tokio::spawn(async move {
            workers::table_size_refresh_worker(storage_for_size, token).await
        });

        // 3. Stream record cleanup worker
        let storage_for_stream = self.engine.clone();
        let metrics = ctx.metrics.clone();
        let token = ctx.shutdown.clone();
        let stream_cleanup = tokio::spawn(async move {
            workers::stream_record_cleanup_worker(storage_for_stream, metrics, token).await;
        });

        // 4. Idempotency token cleanup worker
        let storage_for_token = self.engine.clone();
        let metrics = ctx.metrics.clone();
        let token = ctx.shutdown.clone();
        let idempotency_cleanup = tokio::spawn(async move {
            workers::idempotency_token_cleanup_worker(storage_for_token, metrics, token).await;
        });

        // 5. TTL cleanup worker
        let storage_for_ttl = self.engine.clone();
        let metrics = ctx.metrics.clone();
        let token = ctx.shutdown.clone();
        let ttl = tokio::spawn(async move {
            ttl_worker::ttl_cleanup_worker(storage_for_ttl, metrics, token).await;
        });

        // 6. Stuck vector build sweep
        let storage_for_builds = self.engine.clone();
        let token = ctx.shutdown.clone();
        let vector_builds = tokio::spawn(async move {
            workers::vector_stuck_build_worker(storage_for_builds, token).await;
        });

        // 6b. GSI builds a crash left CREATING. Not a startup step: a rebuild
        // scans the table, and writes keep a CREATING index current meanwhile.
        let storage_for_gsi_builds = self.engine.clone();
        let token = ctx.shutdown.clone();
        let gsi_builds = tokio::spawn(async move {
            crate::data::gsi_build::recovery_worker(storage_for_gsi_builds, token).await;
        });

        // 7. Pool metrics worker - needs both catalog and data pools
        let catalog_pool = self.engine.pool.clone();
        let data_pool = self.engine.data_pool().clone();
        let metrics = ctx.metrics.clone();
        let token = ctx.shutdown.clone();
        let pool_metrics = tokio::spawn(async move {
            workers::pool_metrics_worker(catalog_pool, data_pool, metrics, token).await;
        });

        // 8. GSI delay poller
        let catalog_store_for_gsi = ctx.catalog_store.clone();
        let gsi_delay = self.index_propagation_delay_cache.clone();
        let token = ctx.shutdown.clone();
        let gsi_poller = tokio::spawn(async move {
            workers::poll_gsi_delay(catalog_store_for_gsi, gsi_delay, token).await;
        });

        vec![
            control_plane,
            table_size,
            stream_cleanup,
            idempotency_cleanup,
            ttl,
            vector_builds,
            gsi_builds,
            pool_metrics,
            gsi_poller,
        ]
    }

    fn backend_info(&self) -> Option<String> {
        Some(format!("data_db={}", self.data_db_name))
    }
}

/// Build server components for the Postgres backend (registered in [`register`]).
fn server_components_factory(
    config: &(dyn extenddb_storage::config::StorageConfig + 'static),
    region: &str,
    // PostgreSQL bootstrap needs operator input (databases, roles, admin
    // credentials), so `bootstrap_if_uninitialized` is not honored here:
    // an uninitialized catalog fails with the explicit-`init` guidance.
    _options: extenddb_storage::server_components::ServerComponentsOptions,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<ServerComponents, BackendError>> + Send>,
> {
    let connection_string = config.connection_config().to_string();
    let max_connections = config.max_connections();
    let max_catalog_connections = config.max_catalog_connections();
    let region = region.to_string();
    Box::pin(async move {
        // Build PostgresConfig from extracted values
        let pg_config = PostgresConfig {
            connection_string: connection_string.clone(),
            pool_size: max_connections,
            max_item_size_bytes: 400_000,
        };

        // Create PostgresEngine
        let engine = PostgresEngine::new(&pg_config, &region)
            .await
            .map_err(|e| BackendError::ConnectionFailed {
                backend: "postgres".to_string(),
                details: e.to_string(),
            })?;

        // Version gate, unrecorded migration files, startup shape steps.
        for step in engine.prepare_catalog_for_serve().await? {
            tracing::warn!("converged the catalog shape at startup: {step}");
        }

        // Rebuild any vector index a crash left CREATING, before serving: an index
        // in that state is not searchable, and nothing else will repair it.
        match crate::data::vector_index::reconcile_incomplete_vector_indexes(&engine).await {
            Ok(n) if n > 0 => {
                tracing::info!("Reconciled {n} incomplete vector index(es) at startup");
            }
            Ok(_) => {}
            Err(e) => tracing::error!("Failed to reconcile incomplete vector indexes: {e}"),
        }

        // Recover control plane transitions (errors are logged, not fatal).
        // A drop deferred by a session still holding the table wakes the
        // poller, which `spawn_workers` starts later.
        workers::recover_control_plane_at_startup(&engine).await;

        // Start GSI workers
        let engine = engine.start_gsi_workers();

        // Get data database name for logging (before wrapping in Arc)
        let data_db_name = engine
            .get_data_database_info()
            .await
            .unwrap_or_else(|_| "(query failed)".to_owned());

        // Get references to fields we need before wrapping
        let control_plane_notify = engine.control_plane_notify.clone();
        let index_propagation_delay_cache = engine.index_propagation_delay_cache.clone();

        // Wrap engine in Arc
        let engine = Arc::new(engine);

        // Create catalog store. Honors storage.postgres.catalog_pool_size,
        // defaulting to pool_size when unset. Clamped to the same minimum
        // as the engine pool.
        let catalog_pool_size = if max_catalog_connections < MIN_POOL_SIZE {
            tracing::warn!(
                "storage.postgres.catalog_pool_size = {} is below the minimum of {}; clamping to {}",
                max_catalog_connections,
                MIN_POOL_SIZE,
                MIN_POOL_SIZE
            );
            MIN_POOL_SIZE
        } else {
            max_catalog_connections
        };
        let catalog_pool = PgPoolOptions::new()
            .max_connections(catalog_pool_size)
            .min_connections(catalog_pool_size.min(2))
            .test_before_acquire(false)
            .max_lifetime(std::time::Duration::from_secs(1800))
            .connect(&connection_string)
            .await
            .map_err(|e| BackendError::ConnectionFailed {
                backend: "postgres".to_string(),
                details: format!("Failed to create catalog pool: {e}"),
            })?;

        // Load encryption key
        let enc_key: Option<String> =
            sqlx::query_scalar("SELECT value FROM settings WHERE key = 'encryption_key'")
                .fetch_optional(&catalog_pool)
                .await
                .map_err(|e| {
                    BackendError::InitializationFailed(format!(
                        "Failed to fetch encryption key: {e}"
                    ))
                })?;

        let catalog_store = Arc::new(match enc_key {
            Some(k) => PostgresCatalogStore::with_encryption_key(catalog_pool.clone(), k),
            None => return Err(BackendError::MissingEncryptionKey),
        }) as Arc<dyn extenddb_storage::CatalogStore>;

        // Create auth provider
        let enc_key = extenddb_storage::CatalogStore::cached_encryption_key(&*catalog_store)
            .ok_or(BackendError::MissingEncryptionKey)?;
        let cred_store: Arc<dyn CredentialStore> =
            Arc::new(DbCredentialStore::new(catalog_pool.clone(), enc_key));

        // Create runtime hooks
        let runtime_hooks = Box::new(PostgresRuntimeHooks {
            engine: engine.clone(),
            control_plane_notify,
            index_propagation_delay_cache,
            data_db_name,
        });

        Ok(ServerComponents {
            engine,
            catalog_store,
            credential_store: cred_store,
            runtime_hooks: Some(runtime_hooks),
        })
    })
}
