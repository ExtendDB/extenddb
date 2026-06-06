// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Catalog store implementation for `MongoDB`.

use futures::future::BoxFuture;
use mongodb::bson::doc;

/// `MongoDB` catalog store.
pub struct MongoCatalogStore {
    client: mongodb::Client,
    catalog_db: mongodb::Database,
    pub(crate) encryption_key: Option<String>,
}

impl MongoCatalogStore {
    #[must_use]
    pub fn new(client: mongodb::Client) -> Self {
        let catalog_db = client.database("extenddb_catalog");
        Self {
            client,
            catalog_db,
            encryption_key: None,
        }
    }

    #[must_use]
    pub fn with_encryption_key(client: mongodb::Client, encryption_key: String) -> Self {
        let catalog_db = client.database("extenddb_catalog");
        Self {
            client,
            catalog_db,
            encryption_key: Some(encryption_key),
        }
    }

    /// Get a reference to the catalog database.
    pub(crate) fn catalog_db(&self) -> &mongodb::Database {
        &self.catalog_db
    }

    /// Get a reference to the `MongoDB` client.
    pub(crate) fn client(&self) -> &mongodb::Client {
        &self.client
    }
}

// Implement CatalogStore supertrait
impl extenddb_storage::CatalogStore for MongoCatalogStore {
    fn cached_encryption_key(&self) -> Option<String> {
        self.encryption_key.clone()
    }
}

// Implement DiagnosticsStore
impl extenddb_storage::diagnostics::DiagnosticsStore for MongoCatalogStore {
    fn count_tables(&self) -> BoxFuture<'_, extenddb_storage::diagnostics::DiagResult<i64>> {
        Box::pin(async {
            let coll = self
                .catalog_db
                .collection::<mongodb::bson::Document>("tables");
            let count = coll.count_documents(doc! {}).await.map_err(|e| {
                extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
            })?;
            Ok(count as i64)
        })
    }

    fn count_indexes(&self) -> BoxFuture<'_, extenddb_storage::diagnostics::DiagResult<i64>> {
        Box::pin(async {
            use futures::TryStreamExt;

            // Count tables that have GSIs or LSIs defined
            let coll = self
                .catalog_db
                .collection::<mongodb::bson::Document>("tables");
            let mut cursor = coll.find(doc! {}).await.map_err(|e| {
                extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
            })?;

            let mut index_count: i64 = 0;
            while let Some(table_doc) = cursor
                .try_next()
                .await
                .map_err(|e| extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string()))?
            {
                if let Ok(gsis) = table_doc.get_array("global_secondary_indexes") {
                    index_count += gsis.len() as i64;
                }
                if let Ok(lsis) = table_doc.get_array("local_secondary_indexes") {
                    index_count += lsis.len() as i64;
                }
            }
            Ok(index_count)
        })
    }

    fn test_data_database_connection(
        &self,
    ) -> BoxFuture<'_, extenddb_storage::diagnostics::DiagResult<String>> {
        Box::pin(async {
            // Ping the data database to verify connectivity
            let data_db = self.client.database("extenddb_data");
            data_db.run_command(doc! { "ping": 1 }).await.map_err(|e| {
                extenddb_storage::diagnostics::DiagError::ConnectionFailed(e.to_string())
            })?;
            Ok("extenddb_data".to_string())
        })
    }
}
