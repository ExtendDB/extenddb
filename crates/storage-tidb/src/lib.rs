// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `TiDB` storage backend for extenddb.
//!
//! Implements the `TableEngine` and `DataEngine` traits from `extenddb-storage`
//! using `TiDB` via `sqlx`. All SQL uses parameterized queries exclusively
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
mod management_store;
mod metadata_engine;
mod migrations;
mod operations;
mod stream_engine;
mod table_engine;
mod table_helpers;
mod throughput;
mod tidb_util;
mod ttl_worker;
mod update_table;
mod worker_store;
mod workers;

pub use bootstrapper::TidbBootstrapper;
pub use catalog_store::TidbCatalogStore;
pub use config::TidbStorageConfig;
pub use config::parse_connection_string;
pub use credential_store::DbCredentialStore;

// Auto-register the Tidb backend at compile time
inventory::submit! {
    extenddb_storage::bootstrapper::BackendRegistration {
        name: "tidb",
        factory: |config_path, options| {
            Box::pin(async move {
                let store = TidbBootstrapper::from_config(&config_path, options).await?;
                Ok(Box::new(store) as Box<dyn extenddb_storage::bootstrapper::Bootstrapper>)
            })
        }
    }
}

// Auto-register TiDB operations engine
inventory::submit! {
    extenddb_storage::operations::OperationsEngineRegistration {
        name: "tidb",
        operations: &operations::TidbOperationsEngine,
    }
}

// Auto-register TiDB config deserializer
inventory::submit! {
    extenddb_storage::config::StorageConfigRegistration {
        backend: "tidb",
        deserializer: |table| {
            let config: TidbStorageConfig = table.clone().try_into()
                .map_err(|e: toml::de::Error| format!("Failed to parse tidb config: {}", e))?;
            Ok(Box::new(config) as Box<dyn extenddb_storage::config::StorageConfig>)
        },
        default_config: || {
            Box::new(TidbStorageConfig::default()) as Box<dyn extenddb_storage::config::StorageConfig>
        },
        default_priority: Some(50),
    }
}

// Auto-register TiDB settings store factory
inventory::submit! {
    extenddb_storage::settings_store::SettingsStoreRegistration {
        backend: "tidb",
        factory: |connection_string| {
            let connection_string = config::sqlx_connection_string(connection_string);
            Box::pin(async move {
                let pool = sqlx::MySqlPool::connect(&connection_string)
                    .await
                    .map_err(|e| extenddb_storage::settings_store::SettingsStoreError::ConnectionFailed(e.to_string()))?;
                Ok(Box::new(TidbCatalogStore::new(pool)) as Box<dyn extenddb_storage::management_store::SettingsStore>)
            })
        },
    }
}

// Auto-register TiDB diagnostics store factory
inventory::submit! {
    extenddb_storage::diagnostics_store::DiagnosticsStoreRegistration {
        backend: "tidb",
        factory: |connection_string| {
            let connection_string = config::sqlx_connection_string(connection_string);
            Box::pin(async move {
                let pool = sqlx::MySqlPool::connect(&connection_string)
                    .await
                    .map_err(|e| extenddb_storage::diagnostics_store::DiagnosticsStoreError::ConnectionFailed(e.to_string()))?;
                Ok(Box::new(TidbCatalogStore::new(pool)) as Box<dyn extenddb_storage::diagnostics::DiagnosticsStore>)
            })
        },
    }
}

use std::sync::Arc;

use extenddb_core::version::CatalogVersion;
use extenddb_storage::error::StorageError;
use sqlx::MySqlPool;
use sqlx::mysql::MySqlPoolOptions;

/// Expected catalog version — compiled into the binary (REQ-CAT-006, D-9).
///
/// The tuple is the single source of truth. Use `CATALOG_VERSION.to_string()`
/// wherever a string representation is needed.
pub const CATALOG_VERSION: CatalogVersion = CatalogVersion::new(0, 0, 10);

/// Minimum number of connections allowed per pool.
///
/// Each DynamoDB request triggers an auth/authz query fanout against the
/// catalog pool. Pools smaller than this floor starve under concurrent load.
/// Configured values below the floor are clamped at startup with a warning.
const MIN_POOL_SIZE: u32 = 10;

/// `TiDB` storage backend configuration.
pub struct TidbConfig {
    pub connection_string: String,
    pub pool_size: u32,
    /// Maximum item size in bytes for post-update validation.
    pub max_item_size_bytes: usize,
    pub native_backup: extenddb_storage::config::NativeBackupConfig,
}

/// `TiDB` storage backend.
///
/// The engine no longer stores a single `account_id`. Instead, `account_id`
/// is passed per-request through the storage trait methods, enabling
/// multi-account isolation (Phase 12f).
///
/// Uses two connection pools: `pool` for catalog metadata (tables, indexes,
/// settings, accounts, IAM) and `data_pool` for per-DynamoDB-table data
/// (`_ddb_*` tables and native generated-column secondary indexes). This separation allows the catalog and
/// data to live in different TiDB databases (Bug 1, P54).
pub struct TidbEngine {
    pub(crate) pool: MySqlPool,
    /// Connection pool for the data database where `_ddb_*` tables live.
    pub(crate) data_pool: MySqlPool,
    pub(crate) region: String,
    pub(crate) max_item_size_bytes: usize,
    pub(crate) native_backup: backup_engine::TidbNativeBackupConfig,
    /// Wakes the control plane poller when a table enters CREATING, UPDATING,
    /// or DELETING state, so transitions are processed without polling delay.
    pub(crate) control_plane_notify: Arc<tokio::sync::Notify>,
}

impl TidbEngine {
    pub async fn new(config: &TidbConfig, region: &str) -> Result<Self, StorageError> {
        // Enforce a minimum of 10 connections per pool. Smaller values starve
        // the auth/authz query fanout under concurrent load. If the configured
        // value is below the floor, log a warning and clamp.
        let pool_size = if config.pool_size < MIN_POOL_SIZE {
            tracing::warn!(
                "storage.tidb.pool_size = {} is below the minimum of {}; clamping to {}",
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
        let pool = MySqlPoolOptions::new()
            .max_connections(pool_size)
            .min_connections(min_conns)
            .test_before_acquire(false)
            .max_lifetime(std::time::Duration::from_secs(1800))
            .connect(&crate::config::sqlx_connection_string(
                &config.connection_string,
            ))
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        // P54 Bug 1: Read data database connection string from catalog settings.
        // Falls back to the catalog pool if no separate data database is configured.
        let data_pool = match sqlx::query_as::<_, (String,)>(
            "SELECT value FROM settings WHERE `key` = 'data_database_connection_string'",
        )
        .fetch_optional(&pool)
        .await
        {
            Ok(Some((data_conn,))) if !data_conn.is_empty() => MySqlPoolOptions::new()
                .max_connections(pool_size)
                .min_connections(min_conns)
                .test_before_acquire(false)
                .max_lifetime(std::time::Duration::from_secs(1800))
                .connect(&crate::config::sqlx_connection_string(&data_conn))
                .await
                .map_err(|e| {
                    StorageError::Connection(format!("data database connection failed: {e}"))
                })?,
            _ => pool.clone(),
        };

        Ok(Self {
            pool,
            data_pool,
            region: region.to_owned(),
            max_item_size_bytes: config.max_item_size_bytes,
            native_backup: backup_engine::TidbNativeBackupConfig::from_storage_config(
                config.native_backup.clone(),
            ),
            control_plane_notify: Arc::new(tokio::sync::Notify::new()),
        })
    }

    /// Returns a handle to the control plane notify for the background poller.
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
        if account_id.contains('`') || account_id.contains('\0') || !account_id.is_ascii() {
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
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'settings' AND table_schema = DATABASE())",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !exists.0 {
            return Err(StorageError::CatalogNotInitialized);
        }

        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM settings WHERE `key` = 'catalog_version'")
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
            sqlx::query_as("SELECT value FROM settings WHERE `key` = 'data_database_name'")
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(row.map_or_else(|| "(not configured)".to_owned(), |(name,)| name))
    }

    /// Returns a reference to the data pool for use by background workers
    /// that operate on `_ddb_*` tables (e.g., TTL cleanup, table size refresh).
    pub fn data_pool(&self) -> &MySqlPool {
        &self.data_pool
    }
}

// ============================================================================
// ServerComponents Factory Registration
// ============================================================================

use extenddb_auth::BuiltinAuthProvider;
use extenddb_storage::hooks::{ServerRuntimeHooks, WorkerContext};
use extenddb_storage::server_components::{
    BackendError, ServerComponents, ServerComponentsRegistration,
};

/// Backend-specific runtime hooks for TiDB.
struct TidbRuntimeHooks {
    engine: Arc<TidbEngine>,
    control_plane_notify: Arc<tokio::sync::Notify>,
    data_db_name: String,
}

#[async_trait::async_trait]
impl ServerRuntimeHooks for TidbRuntimeHooks {
    async fn spawn_workers(&self, ctx: &WorkerContext) {
        // Backend-specific workers that need TiDB internals

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

        // 3. TTL cleanup worker for stream-enabled user tables. TiDB native TTL
        // handles internal retention tables and user tables without Streams.
        let storage_for_ttl = self.engine.clone();
        let metrics = ctx.metrics.clone();
        tokio::spawn(async move { ttl_worker::ttl_cleanup_worker(storage_for_ttl, metrics).await });

        // 4. Pool metrics worker - needs both catalog and data pools
        let catalog_pool = self.engine.pool.clone();
        let data_pool = self.engine.data_pool().clone();
        let metrics = ctx.metrics.clone();
        tokio::spawn(async move {
            workers::pool_metrics_worker(catalog_pool, data_pool, metrics).await
        });
    }

    fn backend_info(&self) -> Option<String> {
        Some(format!("data_db={}", self.data_db_name))
    }
}

// Register the TiDB backend factory
inventory::submit! {
    ServerComponentsRegistration {
        backend: "tidb",
        factory: |config, region| {
            let connection_string = config.connection_config().to_string();
            let max_connections = config.max_connections();
            let max_catalog_connections = config.max_catalog_connections();
            let max_item_size_bytes = config
                .runtime_limits()
                .map_or_else(
                    || extenddb_core::limits::LimitsConfig::default().max_item_size_bytes,
                    |limits| limits.max_item_size_bytes,
                );
            let native_backup = config.native_backup_config().unwrap_or_default();
            let region = region.to_string();
            Box::pin(async move {
                // Build TidbConfig from extracted values
                let tidb_config = TidbConfig {
                    connection_string: connection_string.clone(),
                    pool_size: max_connections,
                    max_item_size_bytes,
                    native_backup,
                };

                // Create TidbEngine
                let engine = TidbEngine::new(&tidb_config, &region)
                    .await
                    .map_err(|e| BackendError::ConnectionFailed {
                        backend: "tidb".to_string(),
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

                // Get data database name for logging (before wrapping in Arc)
                let data_db_name = engine
                    .get_data_database_info()
                    .await
                    .unwrap_or_else(|_| "(query failed)".to_owned());

                // Get references to fields we need before wrapping
                let control_plane_notify = engine.control_plane_notify.clone();

                // Wrap engine in Arc
                let engine = Arc::new(engine);

                // Create catalog store. Honors storage.tidb.catalog_pool_size,
                // defaulting to pool_size when unset. Clamped to the same minimum
                // as the engine pool.
                let catalog_pool_size = if max_catalog_connections < MIN_POOL_SIZE {
                    tracing::warn!(
                        "storage.tidb.catalog_pool_size = {} is below the minimum of {}; clamping to {}",
                        max_catalog_connections,
                        MIN_POOL_SIZE,
                        MIN_POOL_SIZE
                    );
                    MIN_POOL_SIZE
                } else {
                    max_catalog_connections
                };
                let catalog_pool = MySqlPoolOptions::new()
                    .max_connections(catalog_pool_size)
                    .min_connections(catalog_pool_size.min(2))
                    .test_before_acquire(false)
                    .max_lifetime(std::time::Duration::from_secs(1800))
                    .connect(&crate::config::sqlx_connection_string(&connection_string))
                    .await
                    .map_err(|e| BackendError::ConnectionFailed {
                        backend: "tidb".to_string(),
                        details: format!("Failed to create catalog pool: {e}"),
                    })?;

                // Load encryption key
                let enc_key: Option<String> =
                    sqlx::query_scalar("SELECT value FROM settings WHERE `key` = 'encryption_key'")
                        .fetch_optional(&catalog_pool)
                        .await
                        .map_err(|e| BackendError::InitializationFailed(format!("Failed to fetch encryption key: {e}")))?;

                let catalog_store = Arc::new(match enc_key {
                    Some(k) => TidbCatalogStore::with_encryption_key(catalog_pool.clone(), k),
                    None => return Err(BackendError::MissingEncryptionKey),
                }) as Arc<dyn extenddb_storage::CatalogStore>;

                // Create auth provider
                let enc_key = extenddb_storage::CatalogStore::cached_encryption_key(&*catalog_store)
                    .ok_or(BackendError::MissingEncryptionKey)?;
                let cred_store = DbCredentialStore::new(catalog_pool.clone(), enc_key);
                let auth_provider = Arc::new(BuiltinAuthProvider::new(cred_store));

                // Create runtime hooks
                let runtime_hooks = Box::new(TidbRuntimeHooks {
                    engine: engine.clone(),
                    control_plane_notify,
                    data_db_name,
                });

                Ok(ServerComponents {
                    engine,
                    catalog_store,
                    auth_provider,
                    runtime_hooks: Some(runtime_hooks),
                })
            })
        },
    }
}
