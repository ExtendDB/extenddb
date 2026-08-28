// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra storage backend for ExtendDB.
//!
//! This crate implements the ExtendDB storage traits using Apache Cassandra
//! as the underlying database. It provides a DynamoDB-compatible API backed
//! by Cassandra's distributed architecture.

mod admin_store;
mod authorization_store;
mod backup_engine;
pub mod bootstrapper;
pub mod cassandra_util;
pub mod catalog_store;
pub mod config;
pub mod create_table;
mod credential_store;
pub mod data;
mod delete_table;
pub mod engine;
pub mod gsi_queue;
mod management_store;
mod metadata_engine;
pub mod migrations;
pub mod operations;
mod stream_engine;
pub mod stream_util;
pub mod table_engine;
mod table_helpers;
mod update_table;
mod worker_store;
pub mod workers;

pub use bootstrapper::CassandraBootstrapper;
pub use catalog_store::CassandraCatalogStore;
pub use config::CassandraStorageConfig;
pub use engine::{CassandraEngine, CassandraSession};

use cdrs_tokio::types::IntoRustByName;
use extenddb_storage::hooks::{ServerRuntimeHooks, WorkerContext};
use extenddb_storage::server_components::{BackendError, ServerComponents};
use std::sync::Arc;

// ============================================================================
// Backend-Specific Runtime Hooks
// ============================================================================

/// Backend-specific runtime hooks for Cassandra.
struct CassandraRuntimeHooks {
    engine: Arc<CassandraEngine>,
    control_plane_notify: Arc<tokio::sync::Notify>,
    gsi_worker_guard: std::sync::OnceLock<workers::GsiWorkerGuard>,
}

#[async_trait::async_trait]
impl ServerRuntimeHooks for CassandraRuntimeHooks {
    async fn spawn_workers(&self, ctx: &WorkerContext) -> Vec<tokio::task::JoinHandle<()>> {
        // Backend-specific workers that need Cassandra internals.
        // Each handle is returned so `serve` can drain them on shutdown.

        let storage_for_poller = self.engine.clone();
        let cp_notify = self.control_plane_notify.clone();
        let catalog_store = ctx.catalog_store.clone();
        let control_plane = tokio::spawn(async move {
            workers::poll_control_plane_transitions(storage_for_poller, cp_notify, catalog_store)
                .await
        });

        let engine_for_recovery = self.engine.clone();
        let transaction_recovery = tokio::spawn(async move {
            workers::poll_transaction_recovery(
                engine_for_recovery,
                std::time::Duration::from_secs(60),
                std::time::Duration::from_secs(30),
            )
            .await
        });

        // Read the initial GSI delay immediately so the atomic is correct from
        // the first request, not after the first 30s sleep.
        let gsi_delay = self.engine.gsi_default_delay_ms.clone();
        if let Ok(Some(val)) = ctx
            .catalog_store
            .get_setting("gsi_propagation_delay_ms")
            .await
            && let Ok(ms) = val.parse::<u64>() {
                gsi_delay.store(ms, std::sync::atomic::Ordering::Relaxed);
            }
        let catalog_store_for_gsi = ctx.catalog_store.clone();
        let gsi_delay_poller =
            tokio::spawn(
                async move { workers::poll_gsi_delay(catalog_store_for_gsi, gsi_delay).await },
            );

        // GSI propagation workers (one per partition).
        let guard = workers::spawn_gsi_workers(self.engine.clone());
        let _ = self.gsi_worker_guard.set(guard);

        vec![control_plane, transaction_recovery, gsi_delay_poller]
    }

    fn backend_info(&self) -> Option<String> {
        Some(format!("keyspace_prefix={}", self.engine.keyspace_prefix))
    }
}

// ============================================================================
// Backend Registration
// ============================================================================

/// Returns the Cassandra storage backend descriptor.
///
/// The thin `main` installs it before dispatching any subcommand:
///
/// ```ignore
/// extenddb_storage::set_backend(extenddb_storage_cassandra::backend())?;
/// ```
pub fn backend() -> extenddb_storage::Backend {
    extenddb_storage::Backend {
        name: "cassandra",
        bootstrapper: |config_path, cli_args| {
            Box::pin(async move {
                let store = CassandraBootstrapper::from_config(&config_path, &cli_args).await?;
                Ok(Box::new(store) as Box<dyn extenddb_storage::bootstrapper::Bootstrapper>)
            })
        },
        operations: &operations::CassandraOperationsEngine,
        storage_config: |table| {
            let mut config: CassandraStorageConfig = table
                .clone()
                .try_into()
                .map_err(|e: toml::de::Error| format!("Failed to parse cassandra config: {e}"))?;
            config.ensure_cached_connection_string();
            Ok(Box::new(config) as Box<dyn extenddb_storage::config::StorageConfig>)
        },
        settings_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let catalog_store = make_catalog_store_from_connection_string(&connection_string)
                    .await
                    .map_err(
                        extenddb_storage::settings_store::SettingsStoreError::ConnectionFailed,
                    )?;
                Ok(Box::new(catalog_store)
                    as Box<
                        dyn extenddb_storage::management_store::SettingsStore,
                    >)
            })
        },
        diagnostics_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let catalog_store = make_catalog_store_from_connection_string(&connection_string)
                    .await
                    .map_err(extenddb_storage::diagnostics_store::DiagnosticsStoreError::ConnectionFailed)?;
                Ok(Box::new(catalog_store)
                    as Box<dyn extenddb_storage::diagnostics::DiagnosticsStore>)
            })
        },
        server_components: server_components_factory,
    }
}

/// Build a `CassandraCatalogStore` from a bare connection string.
///
/// Used by the `settings_store` and `diagnostics_store` factories, which
/// receive only a connection string (no full config object).
async fn make_catalog_store_from_connection_string(
    connection_string: &str,
) -> Result<CassandraCatalogStore, String> {
    let (contact_points, keyspace_prefix) =
        CassandraStorageConfig::parse_connection_string(connection_string);

    if contact_points.is_empty() {
        return Err("No contact points provided".to_string());
    }

    use cdrs_tokio::authenticators::StaticPasswordAuthenticatorProvider;
    use cdrs_tokio::cluster::NodeTcpConfigBuilder;
    use cdrs_tokio::cluster::session::{SessionBuilder, TcpSessionBuilder};
    use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;

    let mut node_builder = NodeTcpConfigBuilder::new();
    for contact_point in &contact_points {
        node_builder = node_builder.with_contact_point(contact_point.clone().into());
    }
    let auth_provider = Arc::new(StaticPasswordAuthenticatorProvider::new(
        "cassandra",
        "cassandra",
    ));
    node_builder = node_builder.with_authenticator_provider(auth_provider);

    let cluster_config = node_builder
        .build()
        .await
        .map_err(|e| format!("Failed to build cluster config: {e}"))?;

    let session = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), cluster_config)
        .build()
        .await
        .map_err(|e| format!("Failed to create session: {e}"))?;

    Ok(CassandraCatalogStore::new(
        Arc::new(session),
        keyspace_prefix,
        "datacenter1".to_string(),
        1,
    ))
}

fn server_components_factory(
    config: &dyn extenddb_storage::config::StorageConfig,
    region: &str,
    _options: extenddb_storage::server_components::ServerComponentsOptions,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<ServerComponents, BackendError>> + Send>,
> {
    let config = config.clone_box();
    let region = region.to_string();

    Box::pin(async move {
        let cassandra_config = config
            .as_any()
            .downcast_ref::<CassandraStorageConfig>()
            .ok_or_else(|| {
                BackendError::InitializationFailed("Expected CassandraStorageConfig".to_string())
            })?;

        let engine = CassandraEngine::new(cassandra_config, &region)
            .await
            .map_err(|e| BackendError::ConnectionFailed {
                backend: "cassandra".to_string(),
                details: e.to_string(),
            })?;

        let control_plane_notify = engine.control_plane_notify.clone();
        let engine = Arc::new(engine);

        match engine.process_control_plane_transitions().await {
            Ok(ref t) if t.is_empty() => {}
            Ok(transitions) => {
                for (name, transition) in &transitions {
                    tracing::info!("Recovered table '{name}': {transition}");
                }
            }
            Err(e) => tracing::error!("Failed to recover control plane transitions: {e}"),
        }

        // Load encryption key from catalog.
        let catalog_keyspace = format!("{}_catalog", cassandra_config.keyspace_prefix);
        let enc_key_query =
            format!("SELECT value FROM {catalog_keyspace}.settings WHERE key = 'encryption_key'");
        let enc_key_result = engine.session.query(&enc_key_query).await.map_err(|e| {
            BackendError::InitializationFailed(format!("Failed to load encryption key: {e}"))
        })?;
        let enc_key_body = enc_key_result.response_body().map_err(|e| {
            BackendError::InitializationFailed(format!(
                "Failed to parse encryption key response: {e}"
            ))
        })?;
        let enc_key_rows = enc_key_body
            .into_rows()
            .ok_or(BackendError::MissingEncryptionKey)?;
        let enc_key_row = enc_key_rows
            .into_iter()
            .next()
            .ok_or(BackendError::MissingEncryptionKey)?;
        let enc_key: String = enc_key_row.get_r_by_name("value").map_err(|e| {
            BackendError::InitializationFailed(format!("Failed to parse encryption key: {e}"))
        })?;

        let catalog_store = Arc::new(CassandraCatalogStore::with_encryption_key(
            engine.session.clone(),
            cassandra_config.keyspace_prefix.clone(),
            cassandra_config.datacenter.clone(),
            cassandra_config.replication_factor,
            enc_key.clone(),
        ));

        let credential_store: Arc<dyn extenddb_auth::CredentialStore> =
            Arc::new(credential_store::CassandraCredentialStore::new(
                engine.session.clone(),
                cassandra_config.keyspace_prefix.clone(),
                enc_key,
            ));

        let runtime_hooks = Box::new(CassandraRuntimeHooks {
            engine: engine.clone(),
            control_plane_notify,
            gsi_worker_guard: std::sync::OnceLock::new(),
        });

        Ok(ServerComponents {
            engine: engine as Arc<dyn extenddb_storage::StorageEngine>,
            catalog_store: catalog_store as Arc<dyn extenddb_storage::CatalogStore>,
            credential_store,
            runtime_hooks: Some(runtime_hooks),
        })
    })
}
