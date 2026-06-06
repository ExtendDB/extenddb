// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `MetadataEngine` implementation for `MongoDB`.
//!
//! Handles TTL configuration, resource tags, and table size bookkeeping.

use futures::TryStreamExt;
use futures::future::BoxFuture;
use mongodb::IndexModel;
use mongodb::bson::{Document, doc};
use mongodb::options::IndexOptions;

use extenddb_core::types::{Item, Tag, TimeToLiveDescription, TimeToLiveStatus};
use extenddb_storage::MetadataEngine;
use extenddb_storage::TtlTableInfo;
use extenddb_storage::error::StorageError;

use crate::MongoEngine;
use crate::data::{data_collection_name, document_to_item};

fn extract_id_fields(doc: &Document) -> (String, String) {
    let id = doc.get_document("_id").ok();
    let account_id = id
        .and_then(|d| d.get_str("account_id").ok())
        .unwrap_or_default()
        .to_owned();
    let table_name = id
        .and_then(|d| d.get_str("table_name").ok())
        .unwrap_or_default()
        .to_owned();
    (account_id, table_name)
}

impl MetadataEngine for MongoEngine {
    fn describe_ttl(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<TimeToLiveDescription, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            let coll = self.catalog_db.collection::<Document>("tables");
            let table_doc = coll
                .find_one(doc! { "_id": { "account_id": &account_id, "table_name": &table_name } })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

            match table_doc.get_str("ttl_attribute") {
                Ok(attr) => Ok(TimeToLiveDescription {
                    time_to_live_status: TimeToLiveStatus::Enabled,
                    attribute_name: Some(attr.to_owned()),
                }),
                Err(_) => Ok(TimeToLiveDescription {
                    time_to_live_status: TimeToLiveStatus::Disabled,
                    attribute_name: None,
                }),
            }
        })
    }

    fn update_ttl(
        &self,
        account_id: &str,
        table_name: &str,
        attribute_name: &str,
        enabled: bool,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        let attribute_name = attribute_name.to_string();
        Box::pin(async move {
            let coll = self.catalog_db.collection::<Document>("tables");

            let ttl_val = if enabled {
                mongodb::bson::Bson::String(attribute_name)
            } else {
                mongodb::bson::Bson::Null
            };

            let result = coll
                .update_one(
                    doc! {
                        "_id": { "account_id": &account_id, "table_name": &table_name },
                        "table_status": "ACTIVE",
                    },
                    doc! {
                        "$set": {
                            "ttl_attribute": ttl_val,
                            "ttl_index_ready": false,
                        }
                    },
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            if result.matched_count == 0 {
                let exists = coll
                    .find_one(
                        doc! { "_id": { "account_id": &account_id, "table_name": &table_name } },
                    )
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                return match exists {
                    None => Err(StorageError::TableNotFound(table_name)),
                    Some(_) => Err(StorageError::TableNotActive(table_name)),
                };
            }

            Ok(())
        })
    }

    fn tag_resource(&self, arn: &str, tags: &[Tag]) -> BoxFuture<'_, Result<(), StorageError>> {
        let arn = arn.to_string();
        let tags = tags.to_vec();
        Box::pin(async move {
            let coll = self.catalog_db.collection::<Document>("tags");

            for tag in &tags {
                coll.update_one(
                    doc! { "resource_arn": &arn, "tag_key": &tag.key },
                    doc! { "$set": { "resource_arn": &arn, "tag_key": &tag.key, "tag_value": &tag.value } },
                )
                .upsert(true)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn untag_resource(
        &self,
        arn: &str,
        tag_keys: &[String],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let arn = arn.to_string();
        let tag_keys = tag_keys.to_vec();
        Box::pin(async move {
            let coll = self.catalog_db.collection::<Document>("tags");

            for key in &tag_keys {
                coll.delete_one(doc! { "resource_arn": &arn, "tag_key": key })
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn list_tags(&self, arn: &str) -> BoxFuture<'_, Result<Vec<Tag>, StorageError>> {
        let arn = arn.to_string();
        Box::pin(async move {
            let coll = self.catalog_db.collection::<Document>("tags");
            let mut cursor = coll
                .find(doc! { "resource_arn": &arn })
                .sort(doc! { "tag_key": 1 })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let mut tags = Vec::new();
            while let Some(doc) = cursor
                .try_next()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                let key = doc.get_str("tag_key").unwrap_or_default().to_owned();
                let value = doc.get_str("tag_value").unwrap_or_default().to_owned();
                tags.push(Tag { key, value });
            }
            Ok(tags)
        })
    }

    fn tables_with_ttl(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        let account_id = account_id.to_string();
        Box::pin(async move {
            let coll = self.catalog_db.collection::<Document>("tables");
            let mut cursor = coll
                .find(doc! {
                    "_id.account_id": &account_id,
                    "ttl_attribute": { "$ne": mongodb::bson::Bson::Null },
                    "table_status": "ACTIVE",
                })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let mut results = Vec::new();
            while let Some(doc) = cursor
                .try_next()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                let name = doc
                    .get_document("_id")
                    .ok()
                    .and_then(|id| id.get_str("table_name").ok())
                    .unwrap_or_default()
                    .to_owned();
                let attr = doc.get_str("ttl_attribute").unwrap_or_default().to_owned();
                results.push((name, attr));
            }
            Ok(results)
        })
    }

    fn all_tables_with_ttl(&self) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        Box::pin(async move {
            let coll = self.catalog_db.collection::<Document>("tables");
            let mut cursor = coll
                .find(doc! {
                    "ttl_attribute": { "$ne": mongodb::bson::Bson::Null },
                    "table_status": "ACTIVE",
                })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let mut results = Vec::new();
            while let Some(doc) = cursor
                .try_next()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                let (account_id, name) = extract_id_fields(&doc);
                let attr = doc.get_str("ttl_attribute").unwrap_or_default().to_owned();
                results.push((account_id, name, attr));
            }
            Ok(results)
        })
    }

    fn all_tables_with_ttl_index_ready(
        &self,
    ) -> BoxFuture<'_, Result<Vec<TtlTableInfo>, StorageError>> {
        Box::pin(async move {
            let coll = self.catalog_db.collection::<Document>("tables");
            let mut cursor = coll
                .find(doc! {
                    "ttl_attribute": { "$ne": mongodb::bson::Bson::Null },
                    "ttl_index_ready": true,
                    "table_status": "ACTIVE",
                })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let mut results = Vec::new();
            while let Some(doc) = cursor
                .try_next()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                let (account_id, name) = extract_id_fields(&doc);
                let attr = doc.get_str("ttl_attribute").unwrap_or_default().to_owned();
                results.push((account_id, name, attr));
            }
            Ok(results)
        })
    }

    fn create_ttl_index(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        let ttl_attribute = ttl_attribute.to_string();
        Box::pin(async move {
            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let id_filter =
                doc! { "_id": { "account_id": &account_id, "table_name": &table_name } };
            let table_doc = tables_coll
                .find_one(id_filter.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

            let table_id = table_doc
                .get_str("table_id")
                .map_err(|_| StorageError::Internal("missing table_id".to_string()))?;

            let coll_name = data_collection_name(table_id);
            let data_coll = self.data_db.collection::<Document>(&coll_name);

            let index_name = format!("idx_ttl_{ttl_attribute}");
            let index_key = format!("item_data.{ttl_attribute}.N");

            let index = IndexModel::builder()
                .keys(doc! { &index_key: 1 })
                .options(
                    IndexOptions::builder()
                        .name(index_name)
                        .sparse(true)
                        .build(),
                )
                .build();

            data_coll
                .create_index(index)
                .await
                .map_err(|e| StorageError::Internal(format!("TTL index creation failed: {e}")))?;

            tables_coll
                .update_one(id_filter, doc! { "$set": { "ttl_index_ready": true } })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(())
        })
    }

    fn drop_ttl_index(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let id_filter =
                doc! { "_id": { "account_id": &account_id, "table_name": &table_name } };

            // Mark index as not ready first
            tables_coll
                .update_one(
                    id_filter.clone(),
                    doc! { "$set": { "ttl_index_ready": false } },
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let table_doc = tables_coll
                .find_one(id_filter)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

            let table_id = table_doc
                .get_str("table_id")
                .map_err(|_| StorageError::Internal("missing table_id".to_string()))?;

            let ttl_attribute = table_doc.get_str("ttl_attribute").unwrap_or_default();
            let index_name = format!("idx_ttl_{ttl_attribute}");

            let coll_name = data_collection_name(table_id);
            let data_coll = self.data_db.collection::<Document>(&coll_name);

            data_coll
                .drop_index(index_name)
                .await
                .map_err(|e| StorageError::Internal(format!("TTL index drop failed: {e}")))?;

            Ok(())
        })
    }

    fn find_expired_items_indexed(
        &self,
        account_id: &str,
        table_name: &str,
        ttl_attribute: &str,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<Item>, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        let ttl_attribute = ttl_attribute.to_string();
        Box::pin(async move {
            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let table_doc = tables_coll
                .find_one(doc! { "_id": { "account_id": &account_id, "table_name": &table_name } })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

            let table_id = table_doc
                .get_str("table_id")
                .map_err(|_| StorageError::Internal("missing table_id".to_string()))?;

            let coll_name = data_collection_name(table_id);
            let data_coll = self.data_db.collection::<Document>(&coll_name);

            let now_epoch = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;

            let ttl_field = format!("item_data.{ttl_attribute}.N");

            // Find items where TTL attribute N value is between 1 and now (expired)
            // DynamoDB stores numbers as strings in the N field
            let filter = doc! {
                &ttl_field: {
                    "$exists": true,
                    "$ne": mongodb::bson::Bson::Null,
                }
            };

            let mut cursor = data_coll
                .find(filter)
                .sort(doc! { &ttl_field: 1 })
                .limit(limit as i64 * 2) // over-fetch since we filter in app
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let mut items = Vec::new();
            while let Some(doc) = cursor
                .try_next()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                if items.len() >= limit {
                    break;
                }
                // Parse the TTL value and check if expired
                if let Ok(item_data) = doc.get_document("item_data") {
                    if let Ok(ttl_obj) = item_data.get_document(&ttl_attribute) {
                        if let Ok(n_str) = ttl_obj.get_str("N") {
                            if let Ok(ttl_val) = n_str.parse::<i64>() {
                                if ttl_val >= 1 && ttl_val <= now_epoch {
                                    let item = document_to_item(&doc)?;
                                    items.push(item);
                                }
                            }
                        }
                    }
                }
            }
            Ok(items)
        })
    }

    fn refresh_table_size(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let id_filter =
                doc! { "_id": { "account_id": &account_id, "table_name": &table_name } };
            let table_doc = tables_coll
                .find_one(id_filter.clone())
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

            let table_id = table_doc
                .get_str("table_id")
                .map_err(|_| StorageError::Internal("missing table_id".to_string()))?;

            let coll_name = data_collection_name(table_id);
            let data_coll = self.data_db.collection::<Document>(&coll_name);

            let item_count = data_coll
                .count_documents(doc! {})
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                as i64;

            // Approximate size via collStats
            let stats_result = self
                .data_db
                .run_command(doc! { "collStats": &coll_name })
                .await;

            let table_size = match stats_result {
                Ok(stats) => stats.get_i64("size").unwrap_or(0),
                Err(_) => 0,
            };

            tables_coll
                .update_one(
                    doc! { "_id": { "account_id": &account_id, "table_name": &table_name }, "table_status": "ACTIVE" },
                    doc! { "$set": { "item_count": item_count, "table_size_bytes": table_size } },
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(())
        })
    }

    fn list_active_table_names(
        &self,
        account_id: &str,
    ) -> BoxFuture<'_, Result<Vec<String>, StorageError>> {
        let account_id = account_id.to_string();
        Box::pin(async move {
            let coll = self.catalog_db.collection::<Document>("tables");
            let mut cursor = coll
                .find(doc! { "_id.account_id": &account_id, "table_status": "ACTIVE" })
                .sort(doc! { "_id.table_name": 1 })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let mut names = Vec::new();
            while let Some(doc) = cursor
                .try_next()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                let name = doc
                    .get_document("_id")
                    .ok()
                    .and_then(|id| id.get_str("table_name").ok())
                    .unwrap_or_default()
                    .to_owned();
                names.push(name);
            }
            Ok(names)
        })
    }

    fn all_active_tables(&self) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        Box::pin(async move {
            let coll = self.catalog_db.collection::<Document>("tables");
            let mut cursor = coll
                .find(doc! { "table_status": "ACTIVE" })
                .sort(doc! { "_id.account_id": 1, "_id.table_name": 1 })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let mut results = Vec::new();
            while let Some(doc) = cursor
                .try_next()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                let (account_id, name) = extract_id_fields(&doc);
                results.push((account_id, name));
            }
            Ok(results)
        })
    }
}
