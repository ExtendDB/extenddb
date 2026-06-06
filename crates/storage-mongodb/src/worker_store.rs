// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `WorkerStore` implementation for `MongoDB`.
//!
//! Processes control-plane state transitions (CREATING → ACTIVE, DELETING → deleted)
//! as a background safety net for incomplete operations.

use futures::TryStreamExt;
use futures::future::BoxFuture;
use mongodb::bson::{Document, doc};

use extenddb_storage::WorkerStore;
use extenddb_storage::error::StorageError;

use crate::MongoEngine;
use crate::data::data_collection_name;

impl WorkerStore for MongoEngine {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        Box::pin(async move {
            let mut transitions = Vec::new();

            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let now = mongodb::bson::DateTime::now();

            // CREATING → ACTIVE: find tables stuck in CREATING whose transition time has passed
            let creating_filter = doc! {
                "table_status": "CREATING",
                "status_transition_at": { "$lte": now },
            };
            let mut cursor = tables_coll
                .find(creating_filter.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            while let Some(table_doc) = cursor
                .try_next()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                let table_name = table_doc
                    .get_str("table_name")
                    .unwrap_or_default()
                    .to_owned();
                let account_id = table_doc.get_str("account_id").unwrap_or_default();

                tables_coll
                    .update_one(
                        doc! {
                            "account_id": account_id,
                            "table_name": &table_name,
                            "table_status": "CREATING",
                        },
                        doc! {
                            "$set": { "table_status": "ACTIVE" },
                            "$unset": { "status_transition_at": "" },
                        },
                    )
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                transitions.push((table_name, "CREATING → active"));
            }

            // DELETING → deleted: find tables stuck in DELETING whose transition time has passed
            let deleting_filter = doc! {
                "table_status": "DELETING",
                "status_transition_at": { "$lte": now },
            };
            let mut cursor = tables_coll
                .find(deleting_filter.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            while let Some(table_doc) = cursor
                .try_next()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                let table_name = table_doc
                    .get_str("table_name")
                    .unwrap_or_default()
                    .to_owned();
                let account_id = table_doc
                    .get_str("account_id")
                    .unwrap_or_default()
                    .to_owned();
                let table_id = table_doc.get_str("table_id").unwrap_or_default().to_owned();
                let table_arn = table_doc
                    .get_str("table_arn")
                    .unwrap_or_default()
                    .to_owned();

                // Drop the data collection
                let coll_name = data_collection_name(&table_id);
                let _ = self.data_db.collection::<Document>(&coll_name).drop().await;

                // Drop index collections
                let indexes_coll = self.catalog_db.collection::<Document>("indexes");
                let mut idx_cursor = indexes_coll
                    .find(doc! { "_id.table_id": &table_id })
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                while let Some(idx_doc) = idx_cursor
                    .try_next()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?
                {
                    if let Ok(index_id) = idx_doc.get_str("index_id") {
                        let idx_coll_name = data_collection_name(index_id);
                        let _ = self
                            .data_db
                            .collection::<Document>(&idx_coll_name)
                            .drop()
                            .await;
                    }
                }

                // Delete index catalog entries
                indexes_coll
                    .delete_many(doc! { "_id.table_id": &table_id })
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                // Delete tags for this resource
                self.catalog_db
                    .collection::<Document>("tags")
                    .delete_many(doc! { "resource_arn": &table_arn })
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                // Delete the table catalog entry
                tables_coll
                    .delete_one(doc! { "account_id": &account_id, "table_name": &table_name })
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                transitions.push((table_name, "DELETING → deleted"));
            }

            Ok(transitions)
        })
    }
}
