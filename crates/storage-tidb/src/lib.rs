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
mod cluster_topology;
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
                let pool = tidb_pool_options(MIN_POOL_SIZE, 0)
                    .connect(&connection_string)
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
                let pool = tidb_pool_options(MIN_POOL_SIZE, 0)
                    .connect(&connection_string)
                    .await
                    .map_err(|e| extenddb_storage::diagnostics_store::DiagnosticsStoreError::ConnectionFailed(e.to_string()))?;
                Ok(Box::new(TidbCatalogStore::new(pool)) as Box<dyn extenddb_storage::diagnostics::DiagnosticsStore>)
            })
        },
    }
}

use std::sync::Arc;
use std::time::Duration;

use extenddb_core::limits::LimitsConfig;
use extenddb_core::version::CatalogVersion;
use extenddb_storage::error::StorageError;
use sqlx::MySqlPool;

use crate::tidb_util::{
    is_table_not_found_tidb_sqlx_error, tidb_default_read_pool_options_with_resource_group,
    tidb_pool_options, tidb_pool_options_with_resource_group,
};

/// Expected catalog version — compiled into the binary.
///
/// The tuple is the single source of truth. Use `CATALOG_VERSION.to_string()`
/// wherever a string representation is needed.
pub const CATALOG_VERSION: CatalogVersion = CatalogVersion::new(0, 0, 23);

/// Minimum number of connections allowed per pool.
///
/// Each DynamoDB request triggers an auth/authz query fanout against the
/// catalog pool. Pools smaller than this floor starve under concurrent load.
/// Configured values below the floor are clamped at startup with a warning.
const MIN_POOL_SIZE: u32 = 10;

/// `TiDB` storage backend configuration.
pub struct TidbConfig {
    pub connection_string: String,
    /// Maximum connections for strong and default-read data-plane pools.
    pub pool_size: u32,
    /// Maximum connections for catalog metadata and control-plane work.
    pub catalog_pool_size: u32,
    /// Runtime limits that TiDB must enforce after storage-side mutations.
    pub limits: LimitsConfig,
    pub native_backup: extenddb_storage::config::NativeBackupConfig,
    /// Optional TiDB Resource Control group for all runtime SQL sessions.
    pub resource_group: Option<String>,
}

/// `TiDB` storage backend.
///
/// The engine no longer stores a single `account_id`. Instead, `account_id`
/// is passed per-request through the storage trait methods, enabling
/// multi-account isolation.
///
/// Uses separate connection pools for catalog metadata, strong data-plane
/// work, and default-read data-plane work. The default-read pool enables TiDB
/// follower read (`closest-adaptive`) without contaminating write transactions
/// or `ConsistentRead=true` operations.
pub struct TidbEngine {
    pub(crate) pool: MySqlPool,
    /// Connection pool for the data database where `_ddb_*` tables live.
    pub(crate) data_pool: MySqlPool,
    /// Read-only data pool for DynamoDB reads that did not request
    /// `ConsistentRead=true`.
    pub(crate) data_default_read_pool: MySqlPool,
    pub(crate) region: String,
    pub(crate) limits: LimitsConfig,
    pub(crate) native_backup: backup_engine::TidbNativeBackupConfig,
    /// Wakes the control plane poller when a table enters CREATING, UPDATING,
    /// or DELETING state, so transitions are processed without polling delay.
    pub(crate) control_plane_notify: Arc<tokio::sync::Notify>,
}

impl TidbEngine {
    pub async fn new(config: &TidbConfig, region: &str) -> Result<Self, StorageError> {
        validate_tidb_limits(&config.limits)?;

        let data_pool_size = normalized_pool_size("storage.tidb.pool_size", config.pool_size);
        let catalog_pool_size =
            normalized_pool_size("storage.tidb.catalog_pool_size", config.catalog_pool_size);

        // Keep a couple of warm connections to avoid first-request latency.
        let catalog_min_conns = catalog_pool_size.min(2);
        let data_min_conns = data_pool_size.min(2);
        let catalog_connection_string =
            crate::config::sqlx_connection_string(&config.connection_string);
        let catalog_pool_options = tidb_pool_options_with_resource_group(
            catalog_pool_size,
            catalog_min_conns,
            config.resource_group.as_deref(),
        )?;
        let pool = catalog_pool_options
            .connect(&catalog_connection_string)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        // Read the data database connection string from catalog settings.
        // Fall back to the catalog database when no separate data database is
        // configured.
        let data_connection_string = match sqlx::query_as::<_, (String,)>(
            "SELECT value FROM settings WHERE `key` = 'data_database_connection_string'",
        )
        .fetch_optional(&pool)
        .await
        {
            Ok(Some((data_conn,))) if !data_conn.is_empty() => {
                crate::config::sqlx_connection_string(&data_conn)
            }
            Ok(_) => catalog_connection_string.clone(),
            Err(error) if is_table_not_found_tidb_sqlx_error(&error) => {
                catalog_connection_string.clone()
            }
            Err(error) => {
                return Err(StorageError::Connection(format!(
                    "read data database connection from catalog settings: {error}"
                )));
            }
        };
        let data_pool_options = tidb_pool_options_with_resource_group(
            data_pool_size,
            data_min_conns,
            config.resource_group.as_deref(),
        )?;
        let data_pool = data_pool_options
            .connect(&data_connection_string)
            .await
            .map_err(|e| {
                StorageError::Connection(format!("data database connection failed: {e}"))
            })?;
        cluster_topology::validate_catalog_data_same_cluster(
            &pool,
            &catalog_connection_string,
            &data_pool,
            &data_connection_string,
        )
        .await?;
        let data_default_read_pool_options = tidb_default_read_pool_options_with_resource_group(
            data_pool_size,
            data_min_conns,
            config.resource_group.as_deref(),
        )?;
        let data_default_read_pool = data_default_read_pool_options
            .connect(&data_connection_string)
            .await
            .map_err(|e| {
                StorageError::Connection(format!("default-read data connection failed: {e}"))
            })?;

        Ok(Self {
            pool,
            data_pool,
            data_default_read_pool,
            region: region.to_owned(),
            limits: config.limits.clone(),
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

    /// Validate catalog version matches the compiled-in expectation.
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

    /// Returns a reference to the data pool for backend runtime workers.
    pub fn data_pool(&self) -> &MySqlPool {
        &self.data_pool
    }

    /// Select the native TiDB read pool for a DynamoDB read request.
    pub(crate) fn data_read_pool(&self, consistent_read: bool) -> &MySqlPool {
        if consistent_read {
            &self.data_pool
        } else {
            &self.data_default_read_pool
        }
    }
}

fn effective_pool_size(configured: u32) -> u32 {
    configured.max(MIN_POOL_SIZE)
}

fn normalized_pool_size(config_key: &'static str, configured: u32) -> u32 {
    let effective = effective_pool_size(configured);
    if effective != configured {
        tracing::warn!(
            config_key,
            configured,
            minimum = MIN_POOL_SIZE,
            effective,
            "clamping TiDB connection pool size to minimum"
        );
    }
    effective
}

fn validate_tidb_limits(limits: &LimitsConfig) -> Result<(), StorageError> {
    if limits.max_partition_key_size_bytes > data::DYNAMODB_HASH_KEY_COLUMN_BYTES {
        return Err(StorageError::Configuration(format!(
            "TiDB backend supports partition keys up to {} bytes because native clustered and secondary indexes must fit TiDB's 3072-byte key limit; configured limit is {}",
            data::DYNAMODB_HASH_KEY_COLUMN_BYTES,
            limits.max_partition_key_size_bytes
        )));
    }
    if limits.max_sort_key_size_bytes > data::DYNAMODB_SORT_KEY_COLUMN_BYTES {
        return Err(StorageError::Configuration(format!(
            "TiDB backend supports sort keys up to {} bytes because native clustered and secondary indexes must fit TiDB's 3072-byte key limit; configured limit is {}",
            data::DYNAMODB_SORT_KEY_COLUMN_BYTES,
            limits.max_sort_key_size_bytes
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use extenddb_core::limits::LimitsConfig;
    use extenddb_storage::error::StorageError;

    use super::{MIN_POOL_SIZE, effective_pool_size, validate_tidb_limits};

    #[test]
    fn tidb_pool_sizes_are_clamped_to_the_backend_minimum() {
        assert_eq!(effective_pool_size(0), MIN_POOL_SIZE);
        assert_eq!(effective_pool_size(MIN_POOL_SIZE - 1), MIN_POOL_SIZE);
        assert_eq!(effective_pool_size(MIN_POOL_SIZE), MIN_POOL_SIZE);
        assert_eq!(effective_pool_size(MIN_POOL_SIZE + 1), MIN_POOL_SIZE + 1);
    }

    #[test]
    fn tidb_limits_accept_dynamodb_key_defaults() {
        validate_tidb_limits(&LimitsConfig::default()).expect("default limits");
    }

    #[test]
    fn tidb_limits_reject_partition_keys_wider_than_native_index_shape() {
        let limits = LimitsConfig {
            max_partition_key_size_bytes: 2049,
            ..LimitsConfig::default()
        };

        let err = validate_tidb_limits(&limits).unwrap_err();

        assert!(matches!(err, StorageError::Configuration(_)));
        assert!(err.to_string().contains("partition keys up to 2048 bytes"));
    }

    #[test]
    fn tidb_limits_reject_sort_keys_wider_than_native_index_shape() {
        let limits = LimitsConfig {
            max_sort_key_size_bytes: 1025,
            ..LimitsConfig::default()
        };

        let err = validate_tidb_limits(&limits).unwrap_err();

        assert!(matches!(err, StorageError::Configuration(_)));
        assert!(err.to_string().contains("sort keys up to 1024 bytes"));
    }
}

// ============================================================================
// ServerComponents Factory Registration
// ============================================================================

use extenddb_auth::BuiltinAuthProvider;
use extenddb_storage::hooks::{BackendHealthError, ServerRuntimeHooks, WorkerContext};
use extenddb_storage::server_components::{
    BackendError, ServerComponents, ServerComponentsRegistration,
};

/// Backend-specific runtime hooks for TiDB.
struct TidbRuntimeHooks {
    engine: Arc<TidbEngine>,
    catalog_store_pool: MySqlPool,
    control_plane_notify: Arc<tokio::sync::Notify>,
    data_db_name: String,
    resource_group: Option<String>,
}

const BACKEND_HEALTH_TIMEOUT: Duration = Duration::from_secs(2);

async fn check_tidb_catalog_pool(
    name: &'static str,
    pool: &MySqlPool,
) -> Result<(), BackendHealthError> {
    let query = sqlx::query_scalar::<_, String>(
        "SELECT value FROM settings WHERE `key` = 'catalog_version' LIMIT 1",
    )
    .fetch_optional(pool);

    match tokio::time::timeout(BACKEND_HEALTH_TIMEOUT, query).await {
        Ok(Ok(Some(_))) => Ok(()),
        Ok(Ok(None)) => Err(BackendHealthError::new(format!(
            "{name}: catalog_version missing"
        ))),
        Ok(Err(error)) => Err(BackendHealthError::new(format!("{name}: {error}"))),
        Err(_) => Err(BackendHealthError::new(format!("{name}: timed out"))),
    }
}

async fn check_tidb_data_pool(
    name: &'static str,
    pool: &MySqlPool,
) -> Result<(), BackendHealthError> {
    let query =
        sqlx::query_scalar::<_, i32>("SELECT 1 FROM stream_records WHERE shard_id = '' LIMIT 1")
            .fetch_optional(pool);

    match tokio::time::timeout(BACKEND_HEALTH_TIMEOUT, query).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(BackendHealthError::new(format!("{name}: {error}"))),
        Err(_) => Err(BackendHealthError::new(format!("{name}: timed out"))),
    }
}

fn health_result(
    results: impl IntoIterator<Item = Result<(), BackendHealthError>>,
) -> Result<(), BackendHealthError> {
    let failures: Vec<String> = results
        .into_iter()
        .filter_map(|result| result.err().map(|error| error.to_string()))
        .collect();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(BackendHealthError::new(failures.join("; ")))
    }
}

#[async_trait::async_trait]
impl ServerRuntimeHooks for TidbRuntimeHooks {
    async fn spawn_workers(&self, ctx: &WorkerContext) {
        // Backend-specific workers that need TiDB internals

        // 1. Control plane transitions poller
        let storage_for_poller = self.engine.clone();
        let cp_notify = self.control_plane_notify.clone();
        tokio::spawn(async move {
            workers::poll_control_plane_transitions(storage_for_poller, cp_notify).await
        });

        // 2. Pool metrics worker - samples every TiDB pool opened by this frontend.
        let pools = vec![
            self.engine.pool.clone(),
            self.engine.data_pool().clone(),
            self.engine.data_default_read_pool.clone(),
            self.catalog_store_pool.clone(),
        ];
        let metrics = ctx.metrics.clone();
        tokio::spawn(async move { workers::pool_metrics_worker(pools, metrics).await });
    }

    async fn health_check(&self) -> Result<(), BackendHealthError> {
        let catalog = check_tidb_catalog_pool("tidb.catalog_metadata_pool", &self.engine.pool);
        let strong_data = check_tidb_data_pool("tidb.strong_data_pool", self.engine.data_pool());
        let default_read = check_tidb_data_pool(
            "tidb.default_read_data_pool",
            &self.engine.data_default_read_pool,
        );
        let catalog_store =
            check_tidb_catalog_pool("tidb.catalog_store_pool", &self.catalog_store_pool);

        let results = tokio::join!(catalog, strong_data, default_read, catalog_store);
        health_result([results.0, results.1, results.2, results.3])
    }

    fn backend_info(&self) -> Option<String> {
        Some(match &self.resource_group {
            Some(resource_group) => {
                format!(
                    "data_db={}, resource_group={resource_group}",
                    self.data_db_name
                )
            }
            None => format!("data_db={}", self.data_db_name),
        })
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
            let limits = config.runtime_limits().cloned().unwrap_or_default();
            let native_backup = config.native_backup_config().unwrap_or_default();
            let resource_group = config.native_capacity_resource_group().map(str::to_owned);
            let region = region.to_string();
            Box::pin(async move {
                let pool_size = normalized_pool_size("storage.tidb.pool_size", max_connections);
                let catalog_pool_size = normalized_pool_size(
                    "storage.tidb.catalog_pool_size",
                    max_catalog_connections,
                );

                // Build TidbConfig from extracted values
                let tidb_config = TidbConfig {
                    connection_string: connection_string.clone(),
                    pool_size,
                    catalog_pool_size,
                    limits,
                    native_backup,
                    resource_group: resource_group.clone(),
                };

                // Create TidbEngine
                let engine = TidbEngine::new(&tidb_config, &region)
                    .await
                    .map_err(|error| match error {
                        StorageError::Configuration(details) => {
                            BackendError::InitializationFailed(details)
                        }
                        error => BackendError::ConnectionFailed {
                            backend: "tidb".to_string(),
                            details: error.to_string(),
                        },
                    })?;

                // Check catalog version
                engine.check_catalog_version().await.map_err(|e| match e {
                    StorageError::CatalogVersionMismatch { expected, found } => {
                        BackendError::CatalogVersionMismatch { expected, found }
                    }
                    _ => BackendError::InitializationFailed(e.to_string()),
                })?;

                engine.repair_native_ttl().await.map_err(|e| {
                    BackendError::InitializationFailed(format!(
                        "Failed to repair TiDB native TTL artifacts: {e}"
                    ))
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

                // Create catalog store on the same independently sized catalog
                // pool budget used by the engine's metadata/control-plane pool.
                let catalog_pool_options = tidb_pool_options_with_resource_group(
                    catalog_pool_size,
                    catalog_pool_size.min(2),
                    resource_group.as_deref(),
                )
                .map_err(|e| BackendError::InitializationFailed(e.to_string()))?;
                let catalog_pool = catalog_pool_options
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
                let runtime_hooks = Arc::new(TidbRuntimeHooks {
                    engine: engine.clone(),
                    catalog_store_pool: catalog_pool.clone(),
                    control_plane_notify,
                    data_db_name,
                    resource_group,
                }) as Arc<dyn ServerRuntimeHooks>;

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
