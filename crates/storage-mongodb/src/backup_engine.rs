// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BackupEngine` implementation for `MongoDB`.
//!
//! Backups are stored as documents in a `backups` collection (metadata) and
//! a `backup_items` collection (snapshotted items). Uses `$out`-style cloning
//! approach: read all items from the data collection and bulk-insert into
//! the backup items collection tagged with `backup_arn`.

use futures::TryStreamExt;
use futures::future::BoxFuture;
use mongodb::bson::{Document, doc};

use extenddb_core::types::{
    BackupDescription, BackupDetails, BackupSummary, ContinuousBackupsDescription,
    KeySchemaElement, PointInTimeRecoveryDescription, SourceTableDetails, TableDescription,
};
use extenddb_storage::BackupEngine;
use extenddb_storage::TableEngine;
use extenddb_storage::error::StorageError;

use crate::MongoEngine;
use crate::data::data_collection_name;

fn epoch_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[allow(clippy::cast_precision_loss)]
fn now_epoch_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as f64
}

impl BackupEngine for MongoEngine {
    fn create_backup(
        &self,
        account_id: &str,
        table_name: &str,
        backup_name: &str,
    ) -> BoxFuture<'_, Result<BackupDetails, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        let backup_name = backup_name.to_string();
        Box::pin(async move {
            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let table_doc = tables_coll
                .find_one(doc! {
                    "_id": { "account_id": &account_id, "table_name": &table_name },
                    "table_status": "ACTIVE",
                })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

            let table_id = table_doc
                .get_str("table_id")
                .map_err(|_| StorageError::Internal("missing table_id".to_string()))?
                .to_owned();
            let table_arn = table_doc
                .get_str("table_arn")
                .unwrap_or_default()
                .to_owned();
            let key_schema_bson = table_doc
                .get_array("key_schema")
                .map_err(|_| StorageError::Internal("missing key_schema".to_string()))?
                .clone();
            let attr_defs_bson = table_doc
                .get_array("attribute_definitions")
                .map_err(|_| StorageError::Internal("missing attribute_definitions".to_string()))?
                .clone();
            let billing_mode = table_doc
                .get_str("billing_mode")
                .unwrap_or("PAY_PER_REQUEST")
                .to_owned();
            let table_size = table_doc.get_i64("table_size_bytes").unwrap_or(0);
            let item_count = table_doc.get_i64("item_count").unwrap_or(0);

            let backup_arn = format!(
                "arn:aws:dynamodb:{region}:{account_id}:table/{table_name}/backup/{ts}",
                region = self.region,
                ts = epoch_millis()
            );

            // Snapshot items from the data collection
            let coll_name = data_collection_name(&table_id);
            let data_coll = self.data_db.collection::<Document>(&coll_name);

            let mut cursor = data_coll
                .find(doc! {})
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let backup_items_coll = self.catalog_db.collection::<Document>("backup_items");
            let mut actual_count: i64 = 0;

            while let Some(item_doc) = cursor
                .try_next()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                let mut backup_doc = Document::new();
                backup_doc.insert("backup_arn", &backup_arn);
                backup_doc.insert(
                    "item_data",
                    item_doc
                        .get("item_data")
                        .cloned()
                        .unwrap_or(mongodb::bson::Bson::Null),
                );
                backup_doc.insert("pk", item_doc.get_str("pk").unwrap_or_default());
                if let Ok(sk) = item_doc.get_str("sk_s") {
                    backup_doc.insert("sk", sk);
                } else if let Some(sk_n) = item_doc.get("sk_n") {
                    backup_doc.insert("sk_n", sk_n.clone());
                }

                backup_items_coll
                    .insert_one(backup_doc)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                actual_count += 1;
            }

            let created_at = now_epoch_secs();

            // Store backup metadata
            let backups_coll = self.catalog_db.collection::<Document>("backups");
            let backup_meta = doc! {
                "_id": &backup_arn,
                "backup_name": &backup_name,
                "backup_status": "AVAILABLE",
                "backup_type": "USER",
                "table_id": &table_id,
                "table_name": &table_name,
                "table_arn": &table_arn,
                "account_id": &account_id,
                "backup_size_bytes": table_size,
                "item_count": actual_count,
                "key_schema": key_schema_bson,
                "attribute_definitions": attr_defs_bson,
                "billing_mode": &billing_mode,
                "created_at": mongodb::bson::DateTime::now(),
                "table_creation_date_time": created_at,
            };

            backups_coll
                .insert_one(backup_meta)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(BackupDetails {
                backup_arn,
                backup_name,
                backup_status: "AVAILABLE".to_owned(),
                backup_type: "USER".to_owned(),
                backup_size_bytes: table_size,
                backup_creation_date_time: created_at,
            })
        })
    }

    fn describe_backup(
        &self,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let backups_coll = self.catalog_db.collection::<Document>("backups");
            let backup_doc = backups_coll
                .find_one(doc! { "_id": &backup_arn })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .ok_or_else(|| {
                    StorageError::Validation(format!("Backup not found: {backup_arn}"))
                })?;

            let name = backup_doc
                .get_str("backup_name")
                .unwrap_or_default()
                .to_owned();
            let status = backup_doc
                .get_str("backup_status")
                .unwrap_or("AVAILABLE")
                .to_owned();
            let table_id = backup_doc
                .get_str("table_id")
                .unwrap_or_default()
                .to_owned();
            let table_name = backup_doc
                .get_str("table_name")
                .unwrap_or_default()
                .to_owned();
            let table_arn = backup_doc
                .get_str("table_arn")
                .unwrap_or_default()
                .to_owned();
            let size = backup_doc.get_i64("backup_size_bytes").unwrap_or(0);
            let count = backup_doc.get_i64("item_count").unwrap_or(0);
            let billing = backup_doc
                .get_str("billing_mode")
                .unwrap_or("PAY_PER_REQUEST")
                .to_owned();

            let created_at = backup_doc
                .get_datetime("created_at")
                .map(|dt| dt.timestamp_millis() as f64 / 1000.0)
                .unwrap_or(0.0);
            let table_created = backup_doc
                .get_f64("table_creation_date_time")
                .unwrap_or(created_at);

            let key_schema_bson = backup_doc
                .get_array("key_schema")
                .map_err(|_| StorageError::Internal("missing key_schema in backup".to_string()))?;
            let key_schema_json = serde_json::to_value(key_schema_bson)
                .map_err(|e| StorageError::Internal(format!("serialize key_schema: {e}")))?;
            let key_schema: Vec<KeySchemaElement> = serde_json::from_value(key_schema_json)
                .map_err(|e| StorageError::Internal(format!("parse key_schema: {e}")))?;

            Ok(BackupDescription {
                backup_details: BackupDetails {
                    backup_arn: backup_arn.clone(),
                    backup_name: name,
                    backup_status: status,
                    backup_type: "USER".to_owned(),
                    backup_size_bytes: size,
                    backup_creation_date_time: created_at,
                },
                source_table_details: SourceTableDetails {
                    table_name,
                    table_id,
                    table_arn,
                    key_schema,
                    item_count: count,
                    table_size_bytes: size,
                    billing_mode: Some(billing),
                    table_creation_date_time: table_created,
                },
            })
        })
    }

    fn list_backups(
        &self,
        account_id: &str,
        table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.map(std::string::ToString::to_string);
        Box::pin(async move {
            let backups_coll = self.catalog_db.collection::<Document>("backups");

            let mut filter = doc! {
                "account_id": &account_id,
                "backup_status": { "$ne": "DELETED" },
            };
            if let Some(tn) = &table_name {
                filter.insert("table_name", tn.as_str());
            }

            let mut cursor = backups_coll
                .find(filter)
                .sort(doc! { "created_at": -1 })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let mut results = Vec::new();
            while let Some(doc) = cursor
                .try_next()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                let arn = doc.get_str("_id").unwrap_or_default().to_owned();
                let name = doc.get_str("backup_name").unwrap_or_default().to_owned();
                let tn = doc.get_str("table_name").unwrap_or_default().to_owned();
                let table_arn = doc.get_str("table_arn").unwrap_or_default().to_owned();
                let status = doc
                    .get_str("backup_status")
                    .unwrap_or("AVAILABLE")
                    .to_owned();
                let size = doc.get_i64("backup_size_bytes").unwrap_or(0);
                let created_at = doc
                    .get_datetime("created_at")
                    .map(|dt| dt.timestamp_millis() as f64 / 1000.0)
                    .unwrap_or(0.0);

                results.push(BackupSummary {
                    backup_arn: arn,
                    backup_name: name,
                    table_name: tn,
                    table_arn,
                    backup_status: status,
                    backup_type: "USER".to_owned(),
                    backup_size_bytes: size,
                    backup_creation_date_time: created_at,
                });
            }
            Ok(results)
        })
    }

    fn delete_backup(
        &self,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let desc = self.describe_backup(&backup_arn).await?;

            // Delete backup items
            let backup_items_coll = self.catalog_db.collection::<Document>("backup_items");
            backup_items_coll
                .delete_many(doc! { "backup_arn": &backup_arn })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            // Mark backup as deleted
            let backups_coll = self.catalog_db.collection::<Document>("backups");
            backups_coll
                .update_one(
                    doc! { "_id": &backup_arn },
                    doc! { "$set": { "backup_status": "DELETED" } },
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(BackupDescription {
                backup_details: BackupDetails {
                    backup_status: "DELETED".to_owned(),
                    ..desc.backup_details
                },
                source_table_details: desc.source_table_details,
            })
        })
    }

    fn restore_table_from_backup(
        &self,
        account_id: &str,
        target_table_name: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_string();
        let target_table_name = target_table_name.to_string();
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let backups_coll = self.catalog_db.collection::<Document>("backups");
            let backup_doc = backups_coll
                .find_one(doc! { "_id": &backup_arn, "backup_status": "AVAILABLE" })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .ok_or_else(|| {
                    StorageError::Validation(format!("Backup not found: {backup_arn}"))
                })?;

            let key_schema_bson = backup_doc
                .get_array("key_schema")
                .map_err(|_| StorageError::Internal("missing key_schema".to_string()))?;
            let attr_defs_bson = backup_doc
                .get_array("attribute_definitions")
                .map_err(|_| StorageError::Internal("missing attribute_definitions".to_string()))?;
            let billing = backup_doc
                .get_str("billing_mode")
                .unwrap_or("PAY_PER_REQUEST");

            let ks_json = serde_json::to_value(key_schema_bson)
                .map_err(|e| StorageError::Internal(format!("serialize key_schema: {e}")))?;
            let ad_json = serde_json::to_value(attr_defs_bson)
                .map_err(|e| StorageError::Internal(format!("serialize attr_defs: {e}")))?;

            let key_schema: Vec<extenddb_core::types::KeySchemaElement> =
                serde_json::from_value(ks_json)
                    .map_err(|e| StorageError::Internal(format!("parse key_schema: {e}")))?;
            let attr_defs: Vec<extenddb_core::types::AttributeDefinition> =
                serde_json::from_value(ad_json)
                    .map_err(|e| StorageError::Internal(format!("parse attr_defs: {e}")))?;

            let billing_mode = if billing == "PAY_PER_REQUEST" {
                Some(extenddb_core::types::BillingMode::PayPerRequest)
            } else {
                Some(extenddb_core::types::BillingMode::Provisioned)
            };

            let create_input = extenddb_core::types::CreateTableInput {
                table_name: target_table_name.clone(),
                key_schema,
                attribute_definitions: attr_defs,
                billing_mode,
                provisioned_throughput: Some(extenddb_core::types::ProvisionedThroughput {
                    read_capacity_units: 5,
                    write_capacity_units: 5,
                }),
                global_secondary_indexes: None,
                local_secondary_indexes: None,
                stream_specification: None,
                tags: None,
                deletion_protection_enabled: None,
                sse_specification: None,
                table_class: None,
                on_demand_throughput: None,
            };

            let desc = self.create_table(&account_id, create_input).await?;

            // Restore items from backup
            let backup_items_coll = self.catalog_db.collection::<Document>("backup_items");
            let mut cursor = backup_items_coll
                .find(doc! { "backup_arn": &backup_arn })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let new_coll_name = data_collection_name(&desc.table_id);
            let new_data_coll = self.data_db.collection::<Document>(&new_coll_name);

            let mut item_count: i64 = 0;
            while let Some(backup_item) = cursor
                .try_next()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                // Re-insert using the original document structure
                let mut restore_doc = Document::new();
                if let Some(pk) = backup_item.get("pk") {
                    restore_doc.insert("pk", pk.clone());
                }
                if let Some(item_data) = backup_item.get("item_data") {
                    restore_doc.insert("item_data", item_data.clone());
                }
                if let Ok(sk) = backup_item.get_str("sk") {
                    restore_doc.insert("sk_s", sk);
                    let pk_str = backup_item.get_str("pk").unwrap_or_default();
                    restore_doc.insert("_id", format!("{pk_str}#{sk}"));
                } else if let Some(sk_n) = backup_item.get("sk_n") {
                    restore_doc.insert("sk_n", sk_n.clone());
                    let pk_str = backup_item.get_str("pk").unwrap_or_default();
                    restore_doc.insert("_id", format!("{pk_str}#{sk_n}"));
                } else {
                    let pk_str = backup_item.get_str("pk").unwrap_or_default();
                    restore_doc.insert("_id", pk_str);
                }

                new_data_coll
                    .insert_one(restore_doc)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                item_count += 1;
            }

            // Update item count and mark table ACTIVE
            let tables_coll = self.catalog_db.collection::<Document>("tables");
            tables_coll
                .update_one(
                    doc! { "_id": { "account_id": &account_id, "table_name": &target_table_name } },
                    doc! { "$set": { "item_count": item_count, "table_status": "ACTIVE" } },
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(desc)
        })
    }

    fn describe_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let exists = tables_coll
                .find_one(doc! { "_id": { "account_id": &account_id, "table_name": &table_name } })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            if exists.is_none() {
                return Err(StorageError::TableNotFound(table_name));
            }

            let cb_coll = self.catalog_db.collection::<Document>("continuous_backups");
            let pitr_doc = cb_coll
                .find_one(doc! { "account_id": &account_id, "table_name": &table_name })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let pitr_enabled = pitr_doc
                .as_ref()
                .and_then(|d| d.get_bool("pitr_enabled").ok())
                .unwrap_or(false);

            let now_epoch = now_epoch_secs();

            Ok(ContinuousBackupsDescription {
                continuous_backups_status: "ENABLED".to_owned(),
                point_in_time_recovery_description: Some(PointInTimeRecoveryDescription {
                    point_in_time_recovery_status: if pitr_enabled {
                        "ENABLED".to_owned()
                    } else {
                        "DISABLED".to_owned()
                    },
                    earliest_restorable_date_time: if pitr_enabled {
                        Some(now_epoch - 35.0 * 24.0 * 3600.0)
                    } else {
                        None
                    },
                    latest_restorable_date_time: if pitr_enabled { Some(now_epoch) } else { None },
                }),
            })
        })
    }

    fn update_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
        pitr_enabled: bool,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        let account_id = account_id.to_string();
        let table_name = table_name.to_string();
        Box::pin(async move {
            let tables_coll = self.catalog_db.collection::<Document>("tables");
            let exists = tables_coll
                .find_one(doc! { "_id": { "account_id": &account_id, "table_name": &table_name } })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            if exists.is_none() {
                return Err(StorageError::TableNotFound(table_name.clone()));
            }

            let cb_coll = self.catalog_db.collection::<Document>("continuous_backups");
            cb_coll
                .update_one(
                    doc! { "account_id": &account_id, "table_name": &table_name },
                    doc! { "$set": {
                        "account_id": &account_id,
                        "table_name": &table_name,
                        "pitr_enabled": pitr_enabled,
                    }},
                )
                .upsert(true)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            self.describe_continuous_backups(&account_id, &table_name)
                .await
        })
    }

    fn restore_table_to_point_in_time(
        &self,
        account_id: &str,
        source_table_name: &str,
        target_table_name: &str,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_string();
        let source_table_name = source_table_name.to_string();
        let target_table_name = target_table_name.to_string();
        Box::pin(async move {
            let backup = self
                .create_backup(&account_id, &source_table_name, "__pitr_restore__")
                .await?;
            let desc = self
                .restore_table_from_backup(&account_id, &target_table_name, &backup.backup_arn)
                .await?;
            let _ = self.delete_backup(&backup.backup_arn).await;
            Ok(desc)
        })
    }
}
