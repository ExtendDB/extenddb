// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `MongoDB` storage backend for extenddb.
//!
//! Implements the storage traits from `extenddb-storage` using `MongoDB`
//! as the backing store. Phase 1 covers `TableEngine`, `DataEngine`,
//! Bootstrapper, and `StorageConfig` with condition filter pushdown.

#![allow(unused)]

mod admin_store;
mod authorization_store;
mod backup_engine;
mod bootstrapper;
mod catalog_store;
pub mod condition;
pub mod config;
mod credential_store;
mod data;
mod data_engine;
mod management_store;
mod metadata_engine;
mod operations;
pub mod pushdown;
mod stream_engine;
mod table_engine;
mod ttl_worker;
mod worker_store;

pub use bootstrapper::MongoBootstrapper;
pub use catalog_store::MongoCatalogStore;
pub use config::MongoStorageConfig;
pub use credential_store::MongoCredentialStore;

use std::sync::Arc;

use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;

// ============================================================================
// OperationsEngineRegistration
// ============================================================================

inventory::submit! {
    extenddb_storage::operations::OperationsEngineRegistration {
        name: "mongodb",
        operations: &operations::MongoOperationsEngine,
    }
}

// ============================================================================
// BackendRegistration
// ============================================================================

inventory::submit! {
    extenddb_storage::bootstrapper::BackendRegistration {
        name: "mongodb",
        factory: |config_path, cli_args| {
            Box::pin(async move {
                let store = MongoBootstrapper::from_config(&config_path, &cli_args).await?;
                Ok(Box::new(store) as Box<dyn extenddb_storage::bootstrapper::Bootstrapper>)
            })
        }
    }
}

// ============================================================================
// StorageConfigRegistration
// ============================================================================

inventory::submit! {
    extenddb_storage::config::StorageConfigRegistration {
        backend: "mongodb",
        deserializer: |table| {
            let config: MongoStorageConfig = table.clone().try_into()
                .map_err(|e: toml::de::Error| format!("Failed to parse mongodb config: {e}"))?;
            Ok(Box::new(config) as Box<dyn extenddb_storage::config::StorageConfig>)
        },
    }
}

// ============================================================================
// SettingsStoreRegistration
// ============================================================================

inventory::submit! {
    extenddb_storage::settings_store::SettingsStoreRegistration {
        backend: "mongodb",
        factory: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let client = mongodb::Client::with_uri_str(&connection_string)
                    .await
                    .map_err(|e| extenddb_storage::settings_store::SettingsStoreError::ConnectionFailed(e.to_string()))?;
                Ok(Box::new(MongoCatalogStore::new(client)) as Box<dyn extenddb_storage::management_store::SettingsStore>)
            })
        },
    }
}

// ============================================================================
// DiagnosticsStoreRegistration
// ============================================================================

inventory::submit! {
    extenddb_storage::diagnostics_store::DiagnosticsStoreRegistration {
        backend: "mongodb",
        factory: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let client = mongodb::Client::with_uri_str(&connection_string)
                    .await
                    .map_err(|e| extenddb_storage::diagnostics_store::DiagnosticsStoreError::ConnectionFailed(e.to_string()))?;
                Ok(Box::new(MongoCatalogStore::new(client)) as Box<dyn extenddb_storage::diagnostics::DiagnosticsStore>)
            })
        },
    }
}

// ============================================================================
// ServerComponentsRegistration
// ============================================================================

use extenddb_auth::BuiltinAuthProvider;
use extenddb_storage::hooks::{ServerRuntimeHooks, WorkerContext};
use extenddb_storage::server_components::{
    BackendError, ServerComponents, ServerComponentsRegistration,
};

/// Backend-specific runtime hooks for `MongoDB`.
struct MongoRuntimeHooks {
    engine: Arc<MongoEngine>,
}

#[async_trait::async_trait]
impl ServerRuntimeHooks for MongoRuntimeHooks {
    async fn spawn_workers(&self, ctx: &WorkerContext) {
        let storage_for_ttl = self.engine.clone();
        let metrics = ctx.metrics.clone();
        tokio::spawn(async move { ttl_worker::ttl_cleanup_worker(storage_for_ttl, metrics).await });
        let storage_for_stream = self.engine.clone();
        tokio::spawn(async move {
            ttl_worker::stream_record_cleanup_worker(storage_for_stream).await;
        });
        let storage_for_backfill = self.engine.clone();
        tokio::spawn(async move {
            ttl_worker::gsi_backfill_worker(storage_for_backfill).await;
        });
        tracing::info!("MongoDB backend: TTL, stream cleanup, and GSI backfill workers spawned");
    }

    fn backend_info(&self) -> Option<String> {
        Some("mongodb".to_string())
    }
}

inventory::submit! {
    ServerComponentsRegistration {
        backend: "mongodb",
        factory: |config, region| {
            let connection_string = config.connection_config().to_string();
            let max_connections = config.max_connections();
            let region = region.to_string();
            Box::pin(async move {
                // Create MongoEngine
                let engine = MongoEngine::new(&connection_string, &region, max_connections)
                    .await
                    .map_err(|e| BackendError::ConnectionFailed {
                        backend: "mongodb".to_string(),
                        details: e.to_string(),
                    })?;

                let engine = Arc::new(engine);

                // Create catalog store
                let catalog_client = mongodb::Client::with_uri_str(&connection_string)
                    .await
                    .map_err(|e| BackendError::ConnectionFailed {
                        backend: "mongodb".to_string(),
                        details: format!("Failed to create catalog client: {e}"),
                    })?;

                // Load encryption key from settings collection
                let catalog_db = catalog_client.database("extenddb_catalog");
                let settings_coll = catalog_db.collection::<mongodb::bson::Document>("settings");
                let enc_key = settings_coll
                    .find_one(mongodb::bson::doc! { "_id": "encryption_key" })
                    .await
                    .map_err(|e| BackendError::InitializationFailed(format!("Load encryption key: {e}")))?
                    .and_then(|d| d.get_str("value").ok().map(std::borrow::ToOwned::to_owned))
                    .unwrap_or_default();

                let catalog_store = Arc::new(
                    MongoCatalogStore::with_encryption_key(catalog_client, enc_key.clone())
                ) as Arc<dyn extenddb_storage::CatalogStore>;

                // Create credential store. The bin layer wraps this in
                // CachedCredentialStore using the operator-configured TTL
                // before constructing the auth provider.
                let auth_client = mongodb::Client::with_uri_str(&connection_string)
                    .await
                    .map_err(|e| BackendError::InitializationFailed(format!("Auth client: {e}")))?;
                let cred_store: Arc<dyn extenddb_auth::CredentialStore> =
                    Arc::new(MongoCredentialStore::new(auth_client, enc_key));

                // Create runtime hooks
                let runtime_hooks = Box::new(MongoRuntimeHooks {
                    engine: engine.clone(),
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

// ============================================================================
// MongoEngine
// ============================================================================

/// TTL for entries in [`MongoEngine::gsi_cache`].
///
/// The GSI cache is per-process. When multiple ExtendDB instances share a
/// catalog, an admin creating or dropping a GSI on instance A does not
/// invalidate instance B's cache. Bounding cache entries by wall-clock age
/// gives eventual convergence at a small cost (one catalog `find` per table
/// per TTL window), which is far cheaper than the cost of silently skipping
/// index updates on tables where GSIs were added out-of-band.
const GSI_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// `MongoDB` storage backend.
pub struct MongoEngine {
    client: mongodb::Client,
    pub(crate) catalog_db: mongodb::Database,
    data_db: mongodb::Database,
    region: String,
    max_connections: u32,
    /// Cache of `table_id` -> (`has_gsi`, insertion time). Avoids catalog
    /// queries on every write for tables with no GSIs. Entries older than
    /// [`GSI_CACHE_TTL`] are treated as misses and re-read from the catalog,
    /// so GSI additions/removals on other ExtendDB instances converge within
    /// the TTL window.
    gsi_cache: dashmap::DashMap<String, (bool, std::time::Instant)>,
}

impl MongoEngine {
    pub async fn new(
        connection_string: &str,
        region: &str,
        max_connections: u32,
    ) -> Result<Self, StorageError> {
        let mut options = mongodb::options::ClientOptions::parse(connection_string)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;
        options.max_pool_size = Some(max_connections);

        // Reject non-primary read preferences. DynamoDB's `ConsistentRead=true`
        // requires linearizable reads; MongoDB's Primary read concern is the
        // only mode that provides that. A connection string like
        // `mongodb://.../?readPreference=secondaryPreferred` would silently
        // route reads to a secondary and return stale data — a fidelity
        // violation the caller has no way to detect.
        if let Some(sel) = options.selection_criteria.as_ref() {
            use mongodb::options::{ReadPreference, SelectionCriteria};
            let is_non_primary = match sel {
                SelectionCriteria::ReadPreference(rp) => !matches!(rp, ReadPreference::Primary),
                _ => false,
            };
            if is_non_primary {
                return Err(StorageError::Connection(
                    "MongoDB connection string must use readPreference=primary. \
                     Non-primary read preferences (secondary, secondaryPreferred, \
                     nearest, primaryPreferred) route reads to replicas and \
                     silently break ConsistentRead=true."
                        .to_owned(),
                ));
            }
        }

        let client = mongodb::Client::with_options(options)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let catalog_db = client.database("extenddb_catalog");
        let data_db = client.database("extenddb_data");

        Ok(Self {
            client,
            catalog_db,
            data_db,
            region: region.to_owned(),
            max_connections,
            gsi_cache: dashmap::DashMap::new(),
        })
    }

    /// Look up a fresh GSI-cache entry for `table_id`.
    ///
    /// Returns `Some(has_gsi)` when a cache entry exists and is younger than
    /// [`GSI_CACHE_TTL`], `None` otherwise (either no entry or expired).
    /// Callers that get `None` must fall back to reading the catalog.
    pub(crate) fn gsi_cache_get_fresh(&self, table_id: &str) -> Option<bool> {
        let entry = self.gsi_cache.get(table_id)?;
        let (has_gsi, inserted) = *entry;
        if inserted.elapsed() <= GSI_CACHE_TTL {
            Some(has_gsi)
        } else {
            None
        }
    }

    /// Record a fresh GSI-cache observation for `table_id`.
    pub(crate) fn gsi_cache_set(&self, table_id: &str, has_gsi: bool) {
        self.gsi_cache
            .insert(table_id.to_owned(), (has_gsi, std::time::Instant::now()));
    }

    /// Remove a GSI-cache entry (e.g., on GSI drop or table delete).
    pub(crate) fn gsi_cache_invalidate(&self, table_id: &str) {
        self.gsi_cache.remove(table_id);
    }

    /// Validate `account_id` against injection attacks.
    fn validate_account_id(account_id: &str) -> Result<(), StorageError> {
        if account_id.contains('$')
            || account_id.contains('.')
            || account_id.contains('\0')
            || !account_id.is_ascii()
        {
            return Err(StorageError::Validation(format!(
                "Invalid account_id: {account_id}"
            )));
        }
        Ok(())
    }
}
