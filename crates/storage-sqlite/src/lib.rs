// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite storage backend for extenddb.
//!
//! Implements the `TableEngine`, `DataEngine`, `MetadataEngine`, `StreamEngine`,
//! `BackupEngine`, and `WorkerStore` traits using SQLite via sqlx.

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
mod sqlite_util;
mod stream_engine;
mod table_engine;
mod table_helpers;
mod update_table;
mod worker_store;
mod workers;

pub use bootstrapper::SqliteBootstrapper;
pub use catalog_store::SqliteCatalogStore;
pub use config::SqliteStorageConfig;
pub use credential_store::SqliteCredentialStore;

// Auto-register the SQLite backend at compile time
inventory::submit! {
    extenddb_storage::bootstrapper::BackendRegistration {
        name: "sqlite",
        factory: |config_path, cli_args| {
            Box::pin(async move {
                let store = SqliteBootstrapper::from_config(&config_path, &cli_args).await?;
                Ok(Box::new(store) as Box<dyn extenddb_storage::bootstrapper::Bootstrapper>)
            })
        }
    }
}

// Auto-register SQLite operations engine
inventory::submit! {
    extenddb_storage::operations::OperationsEngineRegistration {
        name: "sqlite",
        operations: &operations::SqliteOperationsEngine,
    }
}

// Auto-register SQLite config deserializer
inventory::submit! {
    extenddb_storage::config::StorageConfigRegistration {
        backend: "sqlite",
        deserializer: |table| {
            let config: SqliteStorageConfig = table.clone().try_into()
                .map_err(|e: toml::de::Error| format!("Failed to parse sqlite config: {}", e))?;
            Ok(Box::new(config) as Box<dyn extenddb_storage::config::StorageConfig>)
        },
    }
}

// Auto-register SQLite settings store factory
inventory::submit! {
    extenddb_storage::settings_store::SettingsStoreRegistration {
        backend: "sqlite",
        factory: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                use sqlx::sqlite::SqlitePoolOptions;
                let pool = SqlitePoolOptions::new()
                    .max_connections(5)
                    .connect(&connection_string)
                    .await
                    .map_err(|e| extenddb_storage::settings_store::SettingsStoreError::ConnectionFailed(e.to_string()))?;
                Ok(Box::new(SqliteCatalogStore::new(pool)) as Box<dyn extenddb_storage::management_store::SettingsStore>)
            })
        },
    }
}

// Auto-register SQLite diagnostics store factory
inventory::submit! {
    extenddb_storage::diagnostics_store::DiagnosticsStoreRegistration {
        backend: "sqlite",
        factory: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                use sqlx::sqlite::SqlitePoolOptions;
                let pool = SqlitePoolOptions::new()
                    .max_connections(5)
                    .connect(&connection_string)
                    .await
                    .map_err(|e| extenddb_storage::diagnostics_store::DiagnosticsStoreError::ConnectionFailed(e.to_string()))?;
                Ok(Box::new(SqliteCatalogStore::new(pool)) as Box<dyn extenddb_storage::diagnostics::DiagnosticsStore>)
            })
        },
    }
}

use std::sync::Arc;

use extenddb_storage::error::StorageError;
use engine::SqliteEngine;

pub use engine::CATALOG_VERSION;

mod engine;

use extenddb_auth::BuiltinAuthProvider;
use extenddb_storage::hooks::{ServerRuntimeHooks, WorkerContext};
use extenddb_storage::server_components::{
    BackendError, ServerComponents, ServerComponentsRegistration,
};

/// Backend-specific runtime hooks for SQLite.
struct SqliteRuntimeHooks {
    engine: Arc<SqliteEngine>,
    control_plane_notify: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl ServerRuntimeHooks for SqliteRuntimeHooks {
    async fn spawn_workers(&self, ctx: &WorkerContext) {
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
    }

    fn backend_info(&self) -> Option<String> {
        Some("backend=sqlite".to_owned())
    }
}

// Register the SQLite backend factory
inventory::submit! {
    ServerComponentsRegistration {
        backend: "sqlite",
        factory: |config, region| {
            let path = config.connection_config().to_string();
            let pool_size = config.max_connections();
            let region = region.to_string();
            Box::pin(async move {
                let conn_str = if path == ":memory:" {
                    "sqlite::memory:".to_owned()
                } else {
                    format!("sqlite://{}?mode=rwc", path)
                };

                let sqlite_config = engine::SqliteConfig {
                    connection_string: conn_str.clone(),
                    pool_size,
                    max_item_size_bytes: 400_000,
                };

                let engine = SqliteEngine::new(&sqlite_config, &region)
                    .await
                    .map_err(|e| BackendError::ConnectionFailed {
                        backend: "sqlite".to_string(),
                        details: e.to_string(),
                    })?;

                engine.check_catalog_version().await.map_err(|e| match e {
                    StorageError::CatalogVersionMismatch { expected, found } => {
                        BackendError::CatalogVersionMismatch { expected, found }
                    }
                    _ => BackendError::InitializationFailed(e.to_string()),
                })?;

                // Recover pending control-plane transitions at startup.
                match engine.process_control_plane_transitions().await {
                    Ok(ref t) if t.is_empty() => {}
                    Ok(transitions) => {
                        for (name, transition) in &transitions {
                            tracing::info!("Recovered table '{name}': {transition}");
                        }
                    }
                    Err(e) => tracing::error!("Failed to recover control plane transitions: {e}"),
                }

                let control_plane_notify = engine.control_plane_notify.clone();

                let engine = Arc::new(engine);

                // Build the catalog pool (same DB, separate pool for catalog ops).
                use sqlx::sqlite::SqlitePoolOptions;
                let catalog_pool = SqlitePoolOptions::new()
                    .max_connections(pool_size)
                    .min_connections(2)
                    .after_connect(|conn, _| {
                        Box::pin(async move {
                            use sqlx::Executor;
                            conn.execute("PRAGMA journal_mode=WAL").await?;
                            conn.execute("PRAGMA foreign_keys=ON").await?;
                            conn.execute("PRAGMA synchronous=NORMAL").await?;
                            conn.execute("PRAGMA busy_timeout=5000").await?;
                            Ok(())
                        })
                    })
                    .connect(&conn_str)
                    .await
                    .map_err(|e| BackendError::ConnectionFailed {
                        backend: "sqlite".to_string(),
                        details: format!("Failed to create catalog pool: {e}"),
                    })?;

                // Load encryption key
                let enc_key: Option<String> =
                    sqlx::query_scalar("SELECT value FROM settings WHERE key = 'encryption_key'")
                        .fetch_optional(&catalog_pool)
                        .await
                        .map_err(|e| BackendError::InitializationFailed(format!("Failed to fetch encryption key: {e}")))?;

                let catalog_store = Arc::new(match enc_key {
                    Some(k) => SqliteCatalogStore::with_encryption_key(catalog_pool.clone(), k),
                    None => return Err(BackendError::MissingEncryptionKey),
                }) as Arc<dyn extenddb_storage::CatalogStore>;

                // Create auth provider
                let enc_key = extenddb_storage::CatalogStore::cached_encryption_key(&*catalog_store)
                    .ok_or(BackendError::MissingEncryptionKey)?;
                let cred_store = SqliteCredentialStore::new(catalog_pool.clone(), enc_key);
                let auth_provider = Arc::new(BuiltinAuthProvider::new(cred_store));

                let runtime_hooks = Box::new(SqliteRuntimeHooks {
                    engine: engine.clone(),
                    control_plane_notify,
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
