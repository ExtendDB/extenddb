// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cloud Bigtable storage backend for ExtendDB.

pub mod backup_stubs;
pub mod bootstrapper;
pub mod catalog;
pub mod catalog_store;
pub mod config;
pub mod data;
pub mod dev_auth;
pub mod engine;
pub mod gsi;
pub mod gsi_worker;
// Frozen once released: neither rustfmt nor a later clippy lint may require
// an edit that changes its checksum.
#[rustfmt::skip]
#[allow(clippy::all, deprecated)]
pub mod m0001_baseline;
pub mod migrations;
pub mod operations;
pub mod runtime_hooks;
pub mod streams;
pub mod sweeper;
pub mod transact;
pub mod ttl_worker;

pub use bootstrapper::BigtableBootstrapper;
pub use catalog_store::{BigtableCatalogStore, BigtableCredentialStore};
pub use config::BigtableStorageConfig;
pub use data::client::BigtableClient;
pub use engine::BigtableEngine;

/// Backend identifier registered with ExtendDB.
pub const BACKEND_NAME: &str = "bigtable";

/// Default control-plane transition delay (`control_plane_delay_seconds`) in seconds.
pub(crate) const DEFAULT_CONTROL_PLANE_DELAY_SECONDS: f64 = 0.25;

/// Default GSI propagation delay (`index_propagation_delay_ms`) in milliseconds.
pub(crate) const DEFAULT_INDEX_PROPAGATION_DELAY_MS: u64 = 10;

async fn catalog_store_from_connection_string(
    connection_string: &str,
) -> Result<BigtableCatalogStore, String> {
    let mut cfg = BigtableStorageConfig::from_connection_string(connection_string)?;
    if let Some(cp) = cfg.catalog_pool_size {
        cfg.pool_size = cp;
    }
    let client = std::sync::Arc::new(crate::data::client::BigtableClient::connect(&cfg).await?);
    Ok(BigtableCatalogStore::new(client, None, cfg.dev_mode))
}

#[must_use]
pub fn backend() -> extenddb_storage::Backend {
    extenddb_storage::Backend {
        name: BACKEND_NAME,
        bootstrapper: |config_path, cli_args| {
            Box::pin(async move {
                let store = BigtableBootstrapper::from_config(&config_path, &cli_args).await?;
                Ok(Box::new(store) as Box<dyn extenddb_storage::bootstrapper::Bootstrapper>)
            })
        },
        storage_config: |table| {
            let config: BigtableStorageConfig = table
                .clone()
                .try_into()
                .map_err(|e: toml::de::Error| format!("Failed to parse bigtable config: {e}"))?;
            Ok(Box::new(config) as Box<dyn extenddb_storage::config::StorageConfig>)
        },
        operations: &operations::BigtableOperationsEngine,
        settings_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let store = catalog_store_from_connection_string(&connection_string)
                    .await
                    .map_err(
                        extenddb_storage::settings_store::SettingsStoreError::ConnectionFailed,
                    )?;
                Ok(Box::new(store)
                    as Box<
                        dyn extenddb_storage::management_store::SettingsStore,
                    >)
            })
        },
        management_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let store = catalog_store_from_connection_string(&connection_string)
                    .await
                    .map_err(
                        extenddb_storage::management_store_factory::ManagementStoreError::ConnectionFailed,
                    )?;
                Ok(Box::new(store)
                    as Box<
                        dyn extenddb_storage::management_store::ManagementStore,
                    >)
            })
        },
        diagnostics_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let store = catalog_store_from_connection_string(&connection_string)
                    .await
                    .map_err(
                        extenddb_storage::diagnostics_store::DiagnosticsStoreError::ConnectionFailed,
                    )?;
                Ok(Box::new(store) as Box<dyn extenddb_storage::diagnostics::DiagnosticsStore>)
            })
        },
        server_components: server_components_factory,
        catalog_migrations: &migrations::BigtableCatalogMigrations,
    }
}

fn server_components_factory(
    config: &(dyn extenddb_storage::config::StorageConfig + 'static),
    region: &str,
    mut options: extenddb_storage::server_components::ServerComponentsOptions,
) -> futures::future::BoxFuture<
    'static,
    Result<extenddb_storage::ServerComponents, extenddb_storage::BackendError>,
> {
    let conn_str = config.connection_config().to_string();
    let region = region.to_owned();
    let max_item_size_bytes = options.max_item_size_bytes;
    let key_source = std::mem::take(&mut options.key_source);
    Box::pin(async move {
        let cfg = crate::config::BigtableStorageConfig::from_connection_string(&conn_str)
            .map_err(extenddb_storage::BackendError::InitializationFailed)?;
        let mut cat_cfg = cfg.clone();
        if let Some(cp) = cfg.catalog_pool_size {
            cat_cfg.pool_size = cp;
        }
        let client = std::sync::Arc::new(
            crate::data::client::BigtableClient::connect(&cat_cfg)
                .await
                .map_err(|e| extenddb_storage::BackendError::ConnectionFailed {
                    backend: BACKEND_NAME.to_string(),
                    details: e,
                })?,
        );

        let temp_store = BigtableCatalogStore::new(client.clone(), None, cfg.dev_mode);
        let secret_keys =
            extenddb_storage::server_components::load_secret_keys(&temp_store, key_source).await?;

        let data_client = match cfg.data_instance_id.as_deref() {
            Some(data_inst) if !data_inst.is_empty() && data_inst != cfg.instance_id => {
                let mut data_cfg = cfg.clone();
                data_cfg.instance_id = data_inst.to_owned();
                std::sync::Arc::new(
                    crate::data::client::BigtableClient::connect(&data_cfg)
                        .await
                        .map_err(|e| extenddb_storage::BackendError::ConnectionFailed {
                            backend: BACKEND_NAME.to_string(),
                            details: e,
                        })?,
                )
            }
            _ => client.clone(),
        };

        let engine_concrete = std::sync::Arc::new(BigtableEngine::with_options(
            client.clone(),
            data_client,
            cfg.intent_timeout_secs,
            region,
            max_item_size_bytes,
        ));
        let engine = engine_concrete.clone() as std::sync::Arc<dyn extenddb_storage::StorageEngine>;
        let catalog_store = std::sync::Arc::new(BigtableCatalogStore::new(
            client.clone(),
            secret_keys.clone(),
            cfg.dev_mode,
        )) as std::sync::Arc<dyn extenddb_storage::CatalogStore>;

        let cred_store = std::sync::Arc::new(BigtableCredentialStore::new(client, secret_keys))
            as std::sync::Arc<dyn extenddb_auth::CredentialStore>;

        let runtime_hooks: Option<Box<dyn extenddb_storage::ServerRuntimeHooks>> =
            Some(Box::new(crate::runtime_hooks::BigtableRuntimeHooks {
                engine: engine_concrete,
                config: cfg,
            }));

        Ok(extenddb_storage::ServerComponents {
            engine,
            catalog_store,
            credential_store: cred_store,
            runtime_hooks,
        })
    })
}
