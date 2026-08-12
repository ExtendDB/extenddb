//! BigTable storage backend for ExtendDB.

pub mod bootstrapper;
pub mod catalog;
pub mod config;
pub mod data;

pub use bootstrapper::BigtableBootstrapper;
pub use config::BigtableStorageConfig;

/// Backend identifier registered with ExtendDB.
pub const BACKEND_NAME: &str = "bigtable";

pub mod catalog_store;
pub mod crypto;
pub mod dev_auth;
pub mod engine;
pub mod gsi;
pub mod gsi_worker;
pub mod operations;
pub mod runtime_hooks;
pub mod streams;
pub mod sweeper;
pub mod transact;
pub mod ttl_worker;
pub use catalog_store::{BigtableCatalogStore, BigtableCredentialStore};
pub use engine::BigtableEngine;

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
                let cfg = BigtableStorageConfig::from_connection_string(&connection_string)
                    .map_err(|e| {
                        extenddb_storage::settings_store::SettingsStoreError::ConnectionFailed(
                            e.to_string(),
                        )
                    })?;
                let client = std::sync::Arc::new(
                    crate::data::client::BigtableClient::connect(&cfg)
                        .await
                        .map_err(|e| {
                            extenddb_storage::settings_store::SettingsStoreError::ConnectionFailed(
                                e.to_string(),
                            )
                        })?,
                );
                Ok(Box::new(BigtableCatalogStore::new(client, None, cfg.dev_mode))
                    as Box<dyn extenddb_storage::management_store::SettingsStore>)
            })
        },
        diagnostics_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let cfg = BigtableStorageConfig::from_connection_string(&connection_string)
                    .map_err(|e| {
                        extenddb_storage::diagnostics_store::DiagnosticsStoreError::ConnectionFailed(
                            e.to_string(),
                        )
                    })?;
                let client = std::sync::Arc::new(
                    crate::data::client::BigtableClient::connect(&cfg)
                        .await
                        .map_err(|e| {
                            extenddb_storage::diagnostics_store::DiagnosticsStoreError::ConnectionFailed(
                                e.to_string(),
                            )
                        })?,
                );
                Ok(Box::new(BigtableCatalogStore::new(client, None, cfg.dev_mode))
                    as Box<dyn extenddb_storage::diagnostics::DiagnosticsStore>)
            })
        },
        server_components: server_components_factory,
    }
}

fn server_components_factory(
    config: &dyn extenddb_storage::config::StorageConfig,
    _region: &str,
    _options: extenddb_storage::server_components::ServerComponentsOptions,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<
                    extenddb_storage::ServerComponents,
                    extenddb_storage::BackendError,
                >,
            > + Send,
    >,
> {
    let conn_str = config.connection_config().to_string();
    Box::pin(async move {
        let cfg = crate::config::BigtableStorageConfig::from_connection_string(&conn_str)
            .map_err(extenddb_storage::BackendError::InitializationFailed)?;
        let client = std::sync::Arc::new(
            crate::data::client::BigtableClient::connect(&cfg)
                .await
                .map_err(|e| extenddb_storage::BackendError::ConnectionFailed {
                    backend: BACKEND_NAME.to_string(),
                    details: e,
                })?,
        );

        let cat = crate::catalog::Catalog::new(&client);
        let enc_key = cat
            .get(crate::catalog::keys::KEY_MATERIAL_ENC)
            .await
            .map_err(extenddb_storage::BackendError::InitializationFailed)?
            .and_then(|v| v.get("key_b64").and_then(|s| s.as_str()).map(str::to_owned))
            .ok_or(extenddb_storage::BackendError::MissingEncryptionKey)?;

        let data_client = if let Some(ref data_inst) = cfg.data_instance_id {
            let mut data_cfg = cfg.clone();
            data_cfg.instance_id = data_inst.clone();
            std::sync::Arc::new(
                crate::data::client::BigtableClient::connect(&data_cfg)
                    .await
                    .map_err(|e| extenddb_storage::BackendError::ConnectionFailed {
                        backend: BACKEND_NAME.to_string(),
                        details: e,
                    })?,
            )
        } else {
            client.clone()
        };

        let engine_concrete = std::sync::Arc::new(BigtableEngine::new(
            client.clone(),
            data_client,
            cfg.intent_timeout_secs,
        ));
        let engine = engine_concrete.clone() as std::sync::Arc<dyn extenddb_storage::StorageEngine>;
        let catalog_store = std::sync::Arc::new(BigtableCatalogStore::new(
            client.clone(),
            Some(enc_key.clone()),
            cfg.dev_mode,
        )) as std::sync::Arc<dyn extenddb_storage::CatalogStore>;

        let cred_store = std::sync::Arc::new(BigtableCredentialStore::new(
            client.clone(),
            enc_key.clone(),
        )) as std::sync::Arc<dyn extenddb_auth::CredentialStore>;

        let runtime_hooks: Option<Box<dyn extenddb_storage::ServerRuntimeHooks>> =
            Some(Box::new(crate::runtime_hooks::BigtableRuntimeHooks {
                engine: engine_concrete,
                config: cfg.clone(),
            }));

        Ok(extenddb_storage::ServerComponents {
            engine,
            catalog_store,
            credential_store: cred_store,
            runtime_hooks,
        })
    })
}
