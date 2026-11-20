// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Migration 1 (`baseline`) for the Cloud Bigtable storage backend.
//!
//! Splits baseline provisioning across `StoreKind::Catalog` (`instance_id`:
//! `__extenddb_catalog__` and `setting:catalog_id`) and `StoreKind::Data`
//! (`effective_data_instance_id`: `__extenddb_data_ledger__`,
//! `__extenddb_txn_log__`, and `__extenddb_ttl_index__`) so deployments with
//! `data_instance_id` never create orphaned data tables on the catalog instance
//! and always maintain a migration ledger on the data instance.

use async_trait::async_trait;
use extenddb_storage::migrations::{CodeBody, Done, MigrationError};
use serde_json::json;
use uuid::Uuid;

use crate::catalog::{CATALOG_TABLE, CF, Catalog, DATA_LEDGER_TABLE, keys};
use crate::data::admin::{AdminClient, gc_max_versions};
use crate::migrations::BigtableMigrationStores;

pub struct M0001CatalogBaseline;
pub struct M0001DataBaseline;

#[async_trait]
impl CodeBody<BigtableMigrationStores> for M0001CatalogBaseline {
    fn fingerprint(&self) -> &'static str {
        "0001_baseline/bigtable-catalog-v1"
    }

    async fn detect(
        &self,
        _stores: &BigtableMigrationStores,
    ) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &BigtableMigrationStores) -> Result<Done, MigrationError> {
        stores.check_lease()?;
        let mut admin = AdminClient::connect(&stores.catalog_client)
            .await
            .map_err(|e| MigrationError::store("connect Bigtable catalog admin client", e))?;
        admin
            .create_table(CATALOG_TABLE, &[(CF, Some(gc_max_versions(1)))])
            .await
            .map_err(|e| MigrationError::store("ensure __extenddb_catalog__", e))?;
        stores.progressed();

        stores.check_lease()?;
        let cat = Catalog::new(&stores.catalog_client);
        let _ = cat
            .put_if_absent(
                &keys::setting("catalog_id"),
                &json!({ "key": "catalog_id", "value": Uuid::new_v4().to_string() }),
            )
            .await
            .map_err(|e| MigrationError::store("seed setting:catalog_id", e))?;
        stores.progressed();

        Ok(Done::default())
    }
}

#[async_trait]
impl CodeBody<BigtableMigrationStores> for M0001DataBaseline {
    fn fingerprint(&self) -> &'static str {
        "0001_baseline/bigtable-data-v1"
    }

    async fn detect(
        &self,
        _stores: &BigtableMigrationStores,
    ) -> Result<Vec<String>, MigrationError> {
        Ok(Vec::new())
    }

    async fn run(&self, stores: &BigtableMigrationStores) -> Result<Done, MigrationError> {
        stores.check_lease()?;
        let mut data_admin = AdminClient::connect(&stores.data_client)
            .await
            .map_err(|e| MigrationError::store("connect Bigtable data admin client", e))?;
        data_admin
            .create_table(DATA_LEDGER_TABLE, &[(CF, Some(gc_max_versions(1)))])
            .await
            .map_err(|e| MigrationError::store("ensure __extenddb_data_ledger__", e))?;
        stores.progressed();

        stores.check_lease()?;
        crate::transact::ensure_txn_log_table(&stores.data_client)
            .await
            .map_err(|e| MigrationError::store("ensure __extenddb_txn_log__", e))?;
        stores.progressed();

        stores.check_lease()?;
        crate::ttl_worker::ensure_ttl_index_table(&stores.data_client)
            .await
            .map_err(|e| MigrationError::store("ensure __extenddb_ttl_index__", e))?;
        stores.progressed();

        Ok(Done::default())
    }
}
