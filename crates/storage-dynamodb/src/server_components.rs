// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! ServerComponents factory for the DynamoDB-at-home backend.
//!
//! Data plane: real DynamoDB via `DynamoEngine`.
//! Catalog/IAM/auth plane: PostgreSQL via `extenddb-storage-postgres`.

use std::sync::Arc;

use extenddb_auth::CredentialStore;
use extenddb_storage::StorageEngine;
use extenddb_storage::config::StorageConfig as _;
use extenddb_storage::server_components::{
    BackendError, ServerComponents, ServerComponentsRegistration,
};
use extenddb_storage_postgres::{DbCredentialStore, PostgresCatalogStore};
use sqlx::postgres::PgPoolOptions;

use crate::DynamoEngine;

inventory::submit! {
    ServerComponentsRegistration {
        backend: "dynamodb",
        factory: |config, region| {
            let _region = region.to_string();
            let cfg = match config.as_any().downcast_ref::<crate::config::DynamoStorageConfig>() {
                Some(c) => c.clone(),
                None => return Box::pin(async {
                    Err(BackendError::InitializationFailed(
                        "dynamodb ServerComponents factory received a non-dynamodb config".into(),
                    ))
                }),
            };
            Box::pin(async move {
                // 1. Data engine -> real DynamoDB
                let engine: Arc<dyn StorageEngine> =
                    Arc::new(DynamoEngine::from_config(&cfg).await);

                // 2. Catalog pool from the Postgres catalog connection string
                let catalog_pool = PgPoolOptions::new()
                    .max_connections(cfg.max_catalog_connections())
                    .connect(&cfg.catalog_connection_string)
                    .await
                    .map_err(|e| BackendError::ConnectionFailed {
                        backend: "dynamodb".into(),
                        details: format!("catalog pool: {e}"),
                    })?;

                // 3. Fetch encryption key from the catalog settings table
                let enc_key: Option<String> =
                    sqlx::query_scalar("SELECT value FROM settings WHERE key = 'encryption_key'")
                        .fetch_optional(&catalog_pool)
                        .await
                        .map_err(|e| BackendError::InitializationFailed(
                            format!("fetch encryption key: {e}"),
                        ))?;

                let catalog_store = Arc::new(match enc_key {
                    Some(k) => PostgresCatalogStore::with_encryption_key(catalog_pool.clone(), k),
                    None => return Err(BackendError::MissingEncryptionKey),
                }) as Arc<dyn extenddb_storage::CatalogStore>;

                // 4. Credential store (reuse Postgres implementation); the bin
                //    layer wraps it in CachedCredentialStore and builds the
                //    auth provider.
                let enc_key =
                    extenddb_storage::CatalogStore::cached_encryption_key(&*catalog_store)
                        .ok_or(BackendError::MissingEncryptionKey)?;
                let cred_store: Arc<dyn CredentialStore> =
                    Arc::new(DbCredentialStore::new(catalog_pool.clone(), enc_key));

                // 5. No background workers needed: DynamoDB drives TTL/streams/control-plane itself.
                Ok(ServerComponents {
                    engine,
                    catalog_store,
                    credential_store: cred_store,
                    runtime_hooks: None,
                })
            })
        },
    }
}
