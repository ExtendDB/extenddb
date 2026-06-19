// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! "DynamoDB at home" storage backend for ExtendDB.
//!
//! The third entry in the satirical-but-functional backend series, and the only
//! one that actually works the way the marketing implies: ExtendDB speaks the
//! DynamoDB wire protocol, and this backend stores its data in *actual*
//! DynamoDB. The point is not the encoding — there is barely any, because
//! DynamoDB is already a key/value database — it is the deployment posture. Run
//! ExtendDB yourself, pointed at DynamoDB, and you are technically "self-hosted."
//! The execs stop asking.
//!
//! Data plane forwards to DynamoDB; the catalog/IAM/auth plane is delegated to
//! the Postgres backend (`extenddb-storage-postgres`), because DynamoDB has
//! opinions about what a database is and "relational IAM catalog" is not one.

pub(crate) mod backup_engine;
pub mod bootstrapper;
pub mod client;
pub mod config;
pub(crate) mod data_engine;
pub mod encoding;
pub(crate) mod errors;
pub(crate) mod expression;
pub(crate) mod metadata_engine;
pub mod naming;
pub mod operations;
mod server_components;
pub(crate) mod stream_engine;
pub(crate) mod table_engine;
pub(crate) mod worker_store;

/// The DynamoDB-at-home storage engine: forwards the data/table plane to a real
/// DynamoDB endpoint. Catalog/auth are composed separately (see server_components, later task).
pub struct DynamoEngine {
    pub(crate) client: aws_sdk_dynamodb::Client,
    pub(crate) namer: crate::naming::Namer,
}

/// Compile-time assertion: `DynamoEngine` satisfies the `StorageEngine` supertrait.
///
/// If any of the six component traits (`TableEngine`, `DataEngine`, `MetadataEngine`,
/// `StreamEngine`, `BackupEngine`, `WorkerStore`) is missing, this function will
/// produce a compiler error.
#[allow(dead_code)]
fn _assert_storage_engine(e: &DynamoEngine) -> &dyn extenddb_storage::StorageEngine {
    e
}

impl DynamoEngine {
    /// Build the engine from config (constructs the SDK client and the namer).
    pub async fn from_config(cfg: &crate::config::DynamoStorageConfig) -> Self {
        Self {
            client: crate::client::build_client(cfg).await,
            namer: crate::naming::Namer::new(&cfg.table_prefix),
        }
    }
}

// ============================================================================
// Inventory registrations — auto-register the DynamoDB backend at compile time
// ============================================================================

// 1. Bootstrapper registration
inventory::submit! {
    extenddb_storage::bootstrapper::BackendRegistration {
        name: "dynamodb",
        factory: |config_path, cli_args| {
            Box::pin(async move {
                let store = bootstrapper::DynamoBootstrapper::from_config(&config_path, &cli_args).await?;
                Ok(Box::new(store) as Box<dyn extenddb_storage::bootstrapper::Bootstrapper>)
            })
        },
    }
}

// 2. Operations engine registration
inventory::submit! {
    extenddb_storage::operations::OperationsEngineRegistration {
        name: "dynamodb",
        operations: &operations::DynamoOperationsEngine,
    }
}

// 3. Storage config deserializer registration
inventory::submit! {
    extenddb_storage::config::StorageConfigRegistration {
        backend: "dynamodb",
        deserializer: |table| {
            crate::config::DynamoStorageConfig::from_table(table)
                .map(|c| Box::new(c) as Box<dyn extenddb_storage::config::StorageConfig>)
                .map_err(|e| format!("Failed to parse dynamodb config: {e}"))
        },
    }
}

// 4. Settings store factory registration (catalog pool -> PostgresCatalogStore)
inventory::submit! {
    extenddb_storage::settings_store::SettingsStoreRegistration {
        backend: "dynamodb",
        factory: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let pool = sqlx::PgPool::connect(&connection_string)
                    .await
                    .map_err(|e| extenddb_storage::settings_store::SettingsStoreError::ConnectionFailed(e.to_string()))?;
                Ok(Box::new(extenddb_storage_postgres::PostgresCatalogStore::new(pool))
                    as Box<dyn extenddb_storage::management_store::SettingsStore>)
            })
        },
    }
}

// 5. Diagnostics store factory registration (catalog pool -> PostgresCatalogStore)
inventory::submit! {
    extenddb_storage::diagnostics_store::DiagnosticsStoreRegistration {
        backend: "dynamodb",
        factory: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let pool = sqlx::PgPool::connect(&connection_string)
                    .await
                    .map_err(|e| extenddb_storage::diagnostics_store::DiagnosticsStoreError::ConnectionFailed(e.to_string()))?;
                Ok(Box::new(extenddb_storage_postgres::PostgresCatalogStore::new(pool))
                    as Box<dyn extenddb_storage::diagnostics::DiagnosticsStore>)
            })
        },
    }
}
