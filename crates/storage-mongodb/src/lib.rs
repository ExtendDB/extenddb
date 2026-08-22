// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `MongoDB` storage backend for extenddb.
//!
//! Implements the storage traits from `extenddb-storage` using `MongoDB`
//! as the backing store.

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

// ============================================================================
// Backend registration
// ============================================================================

use extenddb_storage::hooks::{ServerRuntimeHooks, WorkerContext};
use extenddb_storage::server_components::{BackendError, ServerComponents};

/// Backend-specific runtime hooks for `MongoDB`.
struct MongoRuntimeHooks {
    engine: Arc<MongoEngine>,
}

#[async_trait::async_trait]
impl ServerRuntimeHooks for MongoRuntimeHooks {
    async fn spawn_workers(&self, ctx: &WorkerContext) -> Vec<tokio::task::JoinHandle<()>> {
        let storage_for_ttl = self.engine.clone();
        let metrics = ctx.metrics.clone();
        let ttl = tokio::spawn(async move {
            ttl_worker::ttl_cleanup_worker(storage_for_ttl, metrics).await;
        });
        let storage_for_stream = self.engine.clone();
        let stream = tokio::spawn(async move {
            ttl_worker::stream_record_cleanup_worker(storage_for_stream).await;
        });
        let storage_for_backfill = self.engine.clone();
        let backfill = tokio::spawn(async move {
            ttl_worker::gsi_backfill_worker(storage_for_backfill).await;
        });
        let storage_for_control_plane = self.engine.clone();
        let control_plane = tokio::spawn(async move {
            ttl_worker::control_plane_worker(storage_for_control_plane).await;
        });
        tracing::info!(
            "MongoDB backend: TTL, stream cleanup, GSI backfill, and control-plane workers spawned"
        );
        vec![ttl, stream, backfill, control_plane]
    }

    fn backend_info(&self) -> Option<String> {
        Some("mongodb".to_string())
    }
}

/// Build the assembled server components for the mongo backend (`serve`).
fn server_components_factory(
    config: &dyn extenddb_storage::config::StorageConfig,
    region: &str,
    // MongoDB bootstrap needs operator input (databases, admin credentials), so
    // `bootstrap_if_uninitialized` is not honored here: an uninitialized catalog
    // fails with the explicit-`init` guidance, matching the PostgreSQL backend.
    _options: extenddb_storage::server_components::ServerComponentsOptions,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<ServerComponents, BackendError>> + Send>,
> {
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
        let catalog_client = connect_guarded(&connection_string, None, false)
            .await
            .map_err(|e| BackendError::ConnectionFailed {
                backend: "mongodb".to_string(),
                details: format!("Failed to create catalog client: {e}"),
            })?;

        // Load encryption key from settings collection. A missing key must be
        // a hard failure: an empty key would base64-decode to zero bytes and
        // panic in aes_gcm (`Key::from_slice` requires 32 bytes). Refuse to
        // start instead, matching the postgres backend.
        let catalog_db = catalog_client.database("extenddb_catalog");
        let settings_coll = catalog_db.collection::<mongodb::bson::Document>("settings");
        let enc_key = settings_coll
            .find_one(mongodb::bson::doc! { "_id": "encryption_key" })
            .await
            .map_err(|e| BackendError::InitializationFailed(format!("Load encryption key: {e}")))?
            .and_then(|d| d.get_str("value").ok().map(std::borrow::ToOwned::to_owned))
            .ok_or(BackendError::MissingEncryptionKey)?;

        let catalog_store = Arc::new(MongoCatalogStore::with_encryption_key(
            catalog_client,
            enc_key.clone(),
        )) as Arc<dyn extenddb_storage::CatalogStore>;

        // Create credential store. The bin layer wraps this in
        // CachedCredentialStore using the operator-configured TTL
        // before constructing the auth provider.
        let auth_client = connect_guarded(&connection_string, None, false)
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
}

/// The MongoDB storage backend. A thin bin installs it via
/// `extenddb_storage::set_backend(extenddb_storage_mongodb::backend())`.
pub fn backend() -> extenddb_storage::Backend {
    extenddb_storage::Backend {
        name: "mongodb",
        bootstrapper: |config_path, cli_args| {
            Box::pin(async move {
                let store = MongoBootstrapper::from_config(&config_path, &cli_args).await?;
                Ok(Box::new(store) as Box<dyn extenddb_storage::bootstrapper::Bootstrapper>)
            })
        },
        storage_config: |table| {
            let config: MongoStorageConfig = table
                .clone()
                .try_into()
                .map_err(|e: toml::de::Error| format!("Failed to parse mongodb config: {e}"))?;
            Ok(Box::new(config) as Box<dyn extenddb_storage::config::StorageConfig>)
        },
        operations: &operations::MongoOperationsEngine,
        settings_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let client = connect_guarded(&connection_string, None, false)
                    .await
                    .map_err(|e| {
                        extenddb_storage::settings_store::SettingsStoreError::ConnectionFailed(
                            e.to_string(),
                        )
                    })?;
                Ok(Box::new(MongoCatalogStore::new(client))
                    as Box<
                        dyn extenddb_storage::management_store::SettingsStore,
                    >)
            })
        },
        diagnostics_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let client = connect_guarded(&connection_string, None, false)
                    .await
                    .map_err(|e| {
                        extenddb_storage::diagnostics_store::DiagnosticsStoreError::ConnectionFailed(
                            e.to_string(),
                        )
                    })?;
                Ok(Box::new(MongoCatalogStore::new(client))
                    as Box<dyn extenddb_storage::diagnostics::DiagnosticsStore>)
            })
        },
        server_components: server_components_factory,
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
    /// Cache of `table_id` -> (`has_gsi`, insertion time). Avoids catalog
    /// queries on every write for tables with no GSIs. Entries older than
    /// [`GSI_CACHE_TTL`] are treated as misses and re-read from the catalog,
    /// so GSI additions/removals on other ExtendDB instances converge within
    /// the TTL window.
    gsi_cache: dashmap::DashMap<String, (bool, std::time::Instant)>,
    /// Enables the deterministic GSI backfill race-test gate only when the
    /// MongoDB integration runner explicitly identifies itself.
    test_backfill_gate_enabled: bool,
}

/// Build a MongoDB client from a connection string, applying the shared
/// connection guards so every client in the backend is protected, not just the
/// data client: reject non-primary read preferences (they route reads to
/// replicas and silently break `ConsistentRead=true`).
///
/// `max_pool_size` is applied when provided (the data client sizes its pool;
/// catalog/auth/bootstrapper clients pass `None`).
///
/// `warn_on_no_tls` gates the no-TLS warning to the long-running server data
/// client only. Short-lived CLI/management clients (settings, catalog checks,
/// bootstrapper) pass `false`: they all share the same connection string, so a
/// single warning at server startup is enough, and emitting it on every CLI
/// invocation both spams logs and pollutes command stdout that tooling parses.
pub(crate) async fn connect_guarded(
    connection_string: &str,
    max_pool_size: Option<u32>,
    warn_on_no_tls: bool,
) -> Result<mongodb::Client, StorageError> {
    let mut options = mongodb::options::ClientOptions::parse(connection_string)
        .await
        .map_err(|e| StorageError::Connection(e.to_string()))?;
    if let Some(n) = max_pool_size {
        options.max_pool_size = Some(n);
    }

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

    if warn_on_no_tls && !matches!(options.tls, Some(mongodb::options::Tls::Enabled(_))) {
        tracing::warn!(
            "MongoDB connection is not using TLS; credentials and data will \
             traverse the network in cleartext. Enable TLS with `?tls=true` \
             in the connection string, or use a `mongodb+srv://` URI."
        );
    }

    mongodb::Client::with_options(options).map_err(|e| StorageError::Connection(e.to_string()))
}

impl MongoEngine {
    pub async fn new(
        connection_string: &str,
        region: &str,
        max_connections: u32,
    ) -> Result<Self, StorageError> {
        let client = connect_guarded(connection_string, Some(max_connections), true).await?;

        let catalog_db = client.database("extenddb_catalog");
        let data_db = client.database("extenddb_data");
        let test_backfill_gate_enabled =
            std::env::var_os("EXTENDDB_TEST_MONGODB_CONTAINER").is_some();

        Ok(Self {
            client,
            catalog_db,
            data_db,
            region: region.to_owned(),
            gsi_cache: dashmap::DashMap::new(),
            test_backfill_gate_enabled,
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
