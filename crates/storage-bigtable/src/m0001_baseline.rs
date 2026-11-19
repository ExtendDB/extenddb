// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 1 (`baseline`) for the Cloud Bigtable storage backend.
//!
//! Ensures the three system Bigtable tables (`__extenddb_catalog__`,
//! `__extenddb_txn_log__`, and `__extenddb_ttl_index__`) exist with their
//! column-family GC rules and seeds `setting:catalog_id` if absent.

use async_trait::async_trait;
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};
use serde_json::json;
use uuid::Uuid;

use crate::catalog::{CATALOG_TABLE, CF, Catalog, keys};
use crate::data::admin::AdminClient;
use crate::migrations::BigtableMigrationStores;

pub struct M0001Baseline;

#[async_trait]
impl CodeBody<BigtableMigrationStores> for M0001Baseline {
    fn fingerprint(&self) -> &'static str {
        "0001_baseline/bigtable-v1"
    }

    async fn detect(
        &self,
        _stores: &BigtableMigrationStores,
    ) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &BigtableMigrationStores) -> Result<Done, MigrationError> {
        let mut admin = AdminClient::connect(&stores.client)
            .await
            .map_err(|e| MigrationError::store("connect Bigtable admin client", e))?;
        admin
            .create_table(
                CATALOG_TABLE,
                &[(CF, Some(crate::data::admin::gc_max_versions(1)))],
            )
            .await
            .map_err(|e| MigrationError::store("ensure __extenddb_catalog__", e))?;

        crate::transact::ensure_txn_log_table(&stores.client)
            .await
            .map_err(|e| MigrationError::store("ensure __extenddb_txn_log__", e))?;
        crate::ttl_worker::ensure_ttl_index_table(&stores.client)
            .await
            .map_err(|e| MigrationError::store("ensure __extenddb_ttl_index__", e))?;

        let cat = Catalog::new(&stores.client);
        let _ = cat
            .put_if_absent(
                &keys::setting("catalog_id"),
                &json!({ "key": "catalog_id", "value": Uuid::new_v4().to_string() }),
            )
            .await
            .map_err(|e| MigrationError::store("seed setting:catalog_id", e))?;

        Ok(Done::default())
    }
}
