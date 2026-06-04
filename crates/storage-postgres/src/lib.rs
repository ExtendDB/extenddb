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
mod stream_engine;
mod table_engine;
mod table_helpers;
mod ttl_worker;
mod update_table;
mod worker_store;
mod workers;

pub use bootstrapper::PostgresBootstrapper;
pub use catalog_store::PostgresCatalogStore;
pub use config::PostgresStorageConfig;
pub use config::parse_connection_string;
pub use credential_store::DbCredentialStore;

// Auto-register the Postgres backend at compile time
inventory::submit! {
    extenddb_storage::bootstrapper::BackendRegistration {
        name: "postgres",
        factory: |config_path, cli_args| {
            Box::pin(async move {
                let store = PostgresBootstrapper::from_config(&config_path, &cli_args).await?;
                Ok(Box::new(store) as Box<dyn extenddb_storage::bootstrapper::Bootstrapper>)
            })
        }
    }
}

// Auto-register PostgreSQL operations engine
inventory::submit! {
    extenddb_storage::operations::OperationsEngineRegistration {
        name: "postgres",
        operations: &operations::PostgresOperationsEngine,
    }
}

// Auto-register PostgreSQL config deserializer
inventory::submit! {
    extenddb_storage::config::StorageConfigRegistration {
        backend: "postgres",
        deserializer: |table| {
            let config: PostgresStorageConfig = table.clone().try_into()
                .map_err(|e: toml::de::Error| format!("Failed to parse postgres config: {}", e))?;
            Ok(Box::new(config) as Box<dyn extenddb_storage::config::StorageConfig>)
        },
    }
}

// Auto-register PostgreSQL settings store factory
inventory::submit! {
    extenddb_storage::settings_store::SettingsStoreRegistration {
        backend: "postgres",
        factory: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let pool = sqlx::PgPool::connect(&connection_string)
                    .await
                    .map_err(|e| extenddb_storage::settings_store::SettingsStoreError::ConnectionFailed(e.to_string()))?;
                Ok(Box::new(PostgresCatalogStore::new(pool)) as Box<dyn extenddb_storage::management_store::SettingsStore>)
            })
        },
    }
}

// Auto-register PostgreSQL diagnostics store factory
inventory::submit! {
    extenddb_storage::diagnostics_store::DiagnosticsStoreRegistration {
        backend: "postgres",
        factory: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let pool = sqlx::PgPool::connect(&connection_string)
                    .await
                    .map_err(|e| extenddb_storage::diagnostics_store::DiagnosticsStoreError::ConnectionFailed(e.to_string()))?;
                Ok(Box::new(PostgresCatalogStore::new(pool)) as Box<dyn extenddb_storage::diagnostics::DiagnosticsStore>)
            })
        },
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
pub const CATALOG_VERSION: CatalogVersion = CatalogVersion::new(0, 0, 2);

/// Minimum number of connections allowed per pool.
///
/// Each DynamoDB request triggers an auth/authz query fanout against the
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
/// data to live in different PostgreSQL databases (Bug 1, P54).
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
    /// P119: Cached GSI default propagation delay (milliseconds). Updated by
    /// background poller every 30s. Avoids per-request DB query on write path.
    pub gsi_default_delay_ms: Arc<std::sync::atomic::AtomicU64>,
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
            _ => pool.clone(),
        };

        // P119: Read initial GSI propagation delay from settings table.
        let initial_gsi_delay: u64 = sqlx::query_as::<_, (String,)>(
            "SELECT value FROM settings WHERE key = 'gsi_propagation_delay_ms'",
        )
        .fetch_optional(&pool)
        .await
        .ok()
        .flatten()
        .and_then(|(v,)| v.parse::<u64>().ok())
        .unwrap_or(10);

        Ok(Self {
            pool,
            data_pool,
            region: region.to_owned(),
            max_item_size_bytes: config.max_item_size_bytes,
            control_plane_notify: Arc::new(tokio::sync::Notify::new()),
            gsi_queue: None,
            gsi_default_delay_ms: Arc::new(std::sync::atomic::AtomicU64::new(initial_gsi_delay)),
        })
    }

    /// Start the async GSI worker tasks (D-4).
    ///
    /// Must be called after construction, before serving requests.
    /// Returns `&Self` for chaining.
    pub fn start_gsi_workers(mut self) -> Self {
        self.gsi_queue = Some(gsi_queue::GsiQueue::spawn(self.data_pool.clone()));
        self
    }

    /// Returns a handle to the control plane notify, for use by the
    /// background poller task (F-3).
    pub fn control_plane_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.control_plane_notify)
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
    /// Returns `StorageError::Internal` if the stored version string is malformed.
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

        let found = found_str
            .parse::<CatalogVersion>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if found != CATALOG_VERSION {
            return Err(StorageError::CatalogVersionMismatch {
                expected: CATALOG_VERSION.to_string(),
                found: found_str,
            });
        }

        Ok(())
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
    pub fn data_pool(&self) -> &PgPool {
        &self.data_pool
    }
}

// ============================================================================
// ServerComponents Factory Registration
// ============================================================================

use extenddb_auth::CredentialStore;
use extenddb_storage::hooks::{ServerRuntimeHooks, WorkerContext};
use extenddb_storage::server_components::{
    BackendError, ServerComponents, ServerComponentsRegistration,
};

/// Backend-specific runtime hooks for PostgreSQL.
struct PostgresRuntimeHooks {
    engine: Arc<PostgresEngine>,
    control_plane_notify: Arc<tokio::sync::Notify>,
    gsi_default_delay_ms: Arc<std::sync::atomic::AtomicU64>,
    data_db_name: String,
}

#[async_trait::async_trait]
impl ServerRuntimeHooks for PostgresRuntimeHooks {
    async fn spawn_workers(&self, ctx: &WorkerContext) {
        // Backend-specific workers that need PostgreSQL internals

        // 1. Control plane transitions poller
        let storage_for_poller = self.engine.clone();
        let cp_notify = self.control_plane_notify.clone();
        let catalog_store = ctx.catalog_store.clone();
        tokio::spawn(async move {
            workers::poll_control_plane_transitions(storage_for_poller, cp_notify, catalog_store)
                .await
        });

        // 2. Table size refresh worker
        let storage_for_size = self.engine.clone();
        tokio::spawn(async move { workers::table_size_refresh_worker(storage_for_size).await });

        // 3. Stream record cleanup worker
        let storage_for_stream = self.engine.clone();
        let metrics = ctx.metrics.clone();
        tokio::spawn(async move {
            workers::stream_record_cleanup_worker(storage_for_stream, metrics).await
        });

        // 4. Idempotency token cleanup worker
        let storage_for_token = self.engine.clone();
        let metrics = ctx.metrics.clone();
        tokio::spawn(async move {
            workers::idempotency_token_cleanup_worker(storage_for_token, metrics).await
        });

        // 5. TTL cleanup worker
        let storage_for_ttl = self.engine.clone();
        let metrics = ctx.metrics.clone();
        tokio::spawn(async move { ttl_worker::ttl_cleanup_worker(storage_for_ttl, metrics).await });

        // 6. Pool metrics worker - needs both catalog and data pools
        let catalog_pool = self.engine.pool.clone();
        let data_pool = self.engine.data_pool().clone();
        let metrics = ctx.metrics.clone();
        tokio::spawn(async move {
            workers::pool_metrics_worker(catalog_pool, data_pool, metrics).await
        });

        // 7. GSI delay poller
        let catalog_store_for_gsi = ctx.catalog_store.clone();
        let gsi_delay = self.gsi_default_delay_ms.clone();
        tokio::spawn(
            async move { workers::poll_gsi_delay(catalog_store_for_gsi, gsi_delay).await },
        );
    }

    fn backend_info(&self) -> Option<String> {
        Some(format!("data_db={}", self.data_db_name))
    }
}

// Register the PostgreSQL backend factory
inventory::submit! {
    ServerComponentsRegistration {
        backend: "postgres",
        factory: |config, region| {
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

                // Check catalog version
                engine.check_catalog_version().await.map_err(|e| match e {
                    StorageError::CatalogVersionMismatch { expected, found } => {
                        BackendError::CatalogVersionMismatch { expected, found }
                    }
                    _ => BackendError::InitializationFailed(e.to_string()),
                })?;

                // Recover control plane transitions (ignore errors)
                match engine.process_control_plane_transitions().await {
                    Ok(ref t) if t.is_empty() => {}
                    Ok(transitions) => {
                        for (name, transition) in &transitions {
                            tracing::info!("Recovered table '{name}': {transition}");
                        }
                    }
                    Err(e) => tracing::error!("Failed to recover control plane transitions: {e}"),
                }

                // Start GSI workers
                let engine = engine.start_gsi_workers();

                // Get data database name for logging (before wrapping in Arc)
                let data_db_name = engine
                    .get_data_database_info()
                    .await
                    .unwrap_or_else(|_| "(query failed)".to_owned());

                // Get references to fields we need before wrapping
                let control_plane_notify = engine.control_plane_notify.clone();
                let gsi_default_delay_ms = engine.gsi_default_delay_ms.clone();

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
                        .map_err(|e| BackendError::InitializationFailed(format!("Failed to fetch encryption key: {e}")))?;

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
                    gsi_default_delay_ms,
                    data_db_name,
                });

                Ok(ServerComponents {
                    engine,
                    catalog_store,
                    credential_store: cred_store,
                    runtime_hooks: Some(runtime_hooks),
                })
            })
        },
    }
}
