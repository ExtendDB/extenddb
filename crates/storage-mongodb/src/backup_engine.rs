// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BackupEngine` implementation for `MongoDB`.
//!
//! Backups are stored as one MongoDB collection per backup, plus a `backups`
//! metadata collection in the catalog. `CreateBackup` uses MongoDB's
//! server-side aggregation `$out` stage to clone the source data collection
//! into `_backup_<backup_id>` in the data database — no per-item traffic
//! between the driver and the server. `RestoreTableFromBackup` uses the same
//! stage in reverse. `DeleteBackup` drops the collection.
//!
//! Backup metadata carries a `backup_id` UUID; the collection name is derived
//! from that id so the `backup_arn` (which contains slashes and colons) never
//! appears in a collection name.

use futures::TryStreamExt;
use futures::future::BoxFuture;
use mongodb::bson::{self, Document, doc};
use serde::Serialize;
use serde::de::DeserializeOwned;

use extenddb_core::types::{
    AttributeDefinition, BackupDescription, BackupDetails, BackupSummary,
    ContinuousBackupsDescription, GsiInput, KeySchemaElement, LsiInput,
    PointInTimeRecoveryDescription, Projection, ProvisionedThroughput, SourceTableDetails,
    TableDescription,
};
use extenddb_storage::BackupEngine;
use extenddb_storage::error::StorageError;

use crate::MongoEngine;
use crate::data::data_collection_name;

fn epoch_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Return the MongoDB collection name that holds items for a given backup.
///
/// The collection lives in the data database. The name is derived from the
/// backup's UUID so it is safe for MongoDB (no colons, slashes, or dots) and
/// bounded in length regardless of how long the source `backup_arn` is.
fn backup_collection_name(backup_id: &str) -> String {
    format!("_backup_{backup_id}")
}

#[allow(clippy::cast_precision_loss)]
fn now_epoch_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as f64
}

fn decode_optional<T: DeserializeOwned>(
    doc: &Document,
    key: &str,
) -> Result<Option<T>, StorageError> {
    match doc.get(key) {
        None | Some(bson::Bson::Null) => Ok(None),
        Some(value) => bson::from_bson(value.clone())
            .map(Some)
            .map_err(|e| StorageError::Internal(format!("parse {key}: {e}"))),
    }
}

fn decode_required<T: DeserializeOwned>(doc: &Document, key: &str) -> Result<T, StorageError> {
    let value = doc
        .get(key)
        .ok_or_else(|| StorageError::Internal(format!("missing {key}")))?;
    bson::from_bson(value.clone()).map_err(|e| StorageError::Internal(format!("parse {key}: {e}")))
}

fn insert_non_empty_array<T: Serialize>(
    doc: &mut Document,
    key: &str,
    values: &[T],
) -> Result<(), StorageError> {
    if !values.is_empty() {
        let value = bson::to_bson(values)
            .map_err(|e| StorageError::Internal(format!("serialize {key}: {e}")))?;
        doc.insert(key, value);
    }
    Ok(())
}

fn restore_provisioned_throughput(
    billing_mode: &str,
    stored: Option<ProvisionedThroughput>,
) -> Option<ProvisionedThroughput> {
    if billing_mode == "PROVISIONED" {
        Some(stored.unwrap_or(ProvisionedThroughput {
            read_capacity_units: 5,
            write_capacity_units: 5,
        }))
    } else {
        None
    }
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
            let _item_count = table_doc.get_i64("item_count").unwrap_or(0);

            // Preserve TableClass / SSESpecification / OnDemandThroughput so
            // RestoreTableFromBackup can recreate the table with the same
            // configuration.
            let table_class_bson = table_doc
                .get("table_class")
                .cloned()
                .unwrap_or(mongodb::bson::Bson::Null);
            let sse_spec_bson = table_doc
                .get("sse_specification")
                .cloned()
                .unwrap_or(mongodb::bson::Bson::Null);
            let on_demand_bson = table_doc
                .get("on_demand_throughput")
                .cloned()
                .unwrap_or(mongodb::bson::Bson::Null);

            // Preserve secondary-index definitions separately from the base
            // item snapshot. The base collection does not contain the index
            // metadata needed to recreate GSI/LSI collections on restore.
            let indexes_coll = self.catalog_db.collection::<Document>("indexes");
            let index_cursor = indexes_coll
                .find(doc! { "_id.table_id": &table_id })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let index_docs: Vec<Document> = index_cursor
                .try_collect()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let attribute_definitions: Vec<AttributeDefinition> = bson::from_bson(
                mongodb::bson::Bson::Array(attr_defs_bson.clone()),
            )
            .map_err(|e| StorageError::Internal(format!("parse attribute_definitions: {e}")))?;
            let mut global_secondary_indexes = Vec::new();
            let mut local_secondary_indexes = Vec::new();
            for index_doc in index_docs {
                let index_name = index_doc
                    .get_document("_id")
                    .and_then(|id| id.get_str("index_name"))
                    .map_err(|_| StorageError::Internal("missing index_name".to_string()))?
                    .to_owned();
                let key_schema: Vec<KeySchemaElement> = decode_required(&index_doc, "key_schema")?;
                let projection: Projection = decode_required(&index_doc, "projection")?;

                if matches!(
                    index_doc.get_str("index_type").unwrap_or("GSI"),
                    "GSI" | "LSI"
                ) {
                    for key in &key_schema {
                        if !attribute_definitions
                            .iter()
                            .any(|definition| definition.attribute_name == key.attribute_name)
                        {
                            return Err(StorageError::Validation(format!(
                                "Cannot create backup for table '{table_name}': index '{index_name}' references key attribute '{}' missing from attribute_definitions",
                                key.attribute_name
                            )));
                        }
                    }
                }

                match index_doc.get_str("index_type").unwrap_or("GSI") {
                    "GSI" => {
                        global_secondary_indexes.push(GsiInput {
                            index_name,
                            key_schema,
                            projection,
                            provisioned_throughput: decode_optional(
                                &index_doc,
                                "provisioned_throughput",
                            )?,
                        });
                    }
                    "LSI" => {
                        local_secondary_indexes.push(LsiInput {
                            index_name,
                            key_schema,
                            projection,
                        });
                    }
                    _ => {}
                }
            }
            let provisioned_throughput_bson = table_doc
                .get("provisioned_throughput")
                .cloned()
                .unwrap_or(mongodb::bson::Bson::Null);

            // The trailing backup-id component is a timestamp plus an 8-hex-char
            // random suffix, so a backup ARN (which is a capability) is not
            // guessable from the creation time alone. Matches the postgres
            // backend.
            let arn_suffix: u32 = {
                use rand::Rng;
                rand::rng().random()
            };
            let backup_arn = format!(
                "arn:aws:dynamodb:{region}:{account_id}:table/{table_name}/backup/{ts}-{arn_suffix:08x}",
                region = self.region,
                ts = epoch_millis()
            );
            let backup_id = uuid::Uuid::new_v4().to_string();

            // Snapshot items from the data collection using a server-side
            // `$out` aggregation. Items are copied directly between
            // collections in MongoDB — no per-item traffic to the driver.
            let src_coll_name = data_collection_name(&table_id);
            let dst_coll_name = backup_collection_name(&backup_id);
            let data_coll = self.data_db.collection::<Document>(&src_coll_name);

            let pipeline = vec![doc! { "$out": &dst_coll_name }];
            let out_cursor = data_coll
                .aggregate(pipeline)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            // `$out` writes to the target collection and returns an empty
            // cursor; consume it to ensure the stage has fully completed
            // before we count.
            let _drained: Vec<Document> = out_cursor
                .try_collect()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let dst_coll = self.data_db.collection::<Document>(&dst_coll_name);
            let actual_count = dst_coll
                .count_documents(doc! {})
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                as i64;

            let created_at = now_epoch_secs();

            // Store backup metadata. `backup_id` is what maps to the
            // physical collection; `backup_arn` remains the caller-visible
            // handle and stays the `_id` for compatibility with existing
            // describe/list callers.
            let backups_coll = self.catalog_db.collection::<Document>("backups");
            let mut backup_meta = doc! {
                "_id": &backup_arn,
                "backup_id": &backup_id,
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
                "provisioned_throughput": provisioned_throughput_bson,
                "created_at": mongodb::bson::DateTime::now(),
                "table_creation_date_time": created_at,
                "table_class": table_class_bson,
                "sse_specification": sse_spec_bson,
                "on_demand_throughput": on_demand_bson,
            };

            insert_non_empty_array(
                &mut backup_meta,
                "global_secondary_indexes",
                &global_secondary_indexes,
            )?;
            insert_non_empty_array(
                &mut backup_meta,
                "local_secondary_indexes",
                &local_secondary_indexes,
            )?;

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
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = account_id.to_string();
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let backups_coll = self.catalog_db.collection::<Document>("backups");
            // Scope the lookup to the calling account so a backup ARN cannot be
            // read cross-account, and exclude DELETED backups so a deleted
            // backup reads as BackupNotFoundException. Matches the postgres
            // backend.
            let backup_doc = backups_coll
                .find_one(doc! {
                    "_id": &backup_arn,
                    "account_id": &account_id,
                    "backup_status": { "$ne": "DELETED" },
                })
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
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = account_id.to_string();
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let desc = self.describe_backup(&account_id, &backup_arn).await?;

            // Look up the physical collection name from metadata (account-scoped).
            let backups_coll = self.catalog_db.collection::<Document>("backups");
            let meta = backups_coll
                .find_one(doc! { "_id": &backup_arn, "account_id": &account_id })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .ok_or_else(|| {
                    StorageError::Validation(format!("Backup not found: {backup_arn}"))
                })?;

            // Drop the backup collection. If backup_id is absent (e.g., a
            // pre-`$out` backup on an old catalog) we skip — nothing to drop
            // at the collection level in that case.
            if let Ok(backup_id) = meta.get_str("backup_id") {
                let coll_name = backup_collection_name(backup_id);
                let coll = self.data_db.collection::<Document>(&coll_name);
                coll.drop()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }

            // Mark backup as deleted (account-scoped)
            backups_coll
                .update_one(
                    doc! { "_id": &backup_arn, "account_id": &account_id },
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
                // Scope to the calling account (defence-in-depth: the engine
                // layer already enforces ARN ownership, and describe/delete are
                // account-scoped at the storage layer too).
                .find_one(doc! {
                    "_id": &backup_arn,
                    "account_id": &account_id,
                    "backup_status": "AVAILABLE",
                })
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

            // New backups preserve these fields. Keep the old 5/5 fallback
            // for backups created before the metadata was added, while
            // correctly omitting provisioned throughput for on-demand tables.
            let provisioned_throughput: Option<ProvisionedThroughput> =
                decode_optional(&backup_doc, "provisioned_throughput")?;
            let provisioned_throughput =
                restore_provisioned_throughput(billing, provisioned_throughput);
            let global_secondary_indexes: Option<Vec<GsiInput>> =
                decode_optional(&backup_doc, "global_secondary_indexes")?;
            let local_secondary_indexes: Option<Vec<LsiInput>> =
                decode_optional(&backup_doc, "local_secondary_indexes")?;

            // Preserve the source table's TableClass / SSESpecification /
            // OnDemandThroughput settings when recreating.
            let table_class = backup_doc.get_str("table_class").ok().map(str::to_owned);
            let sse_specification: Option<serde_json::Value> =
                backup_doc.get("sse_specification").and_then(|b| {
                    if matches!(b, mongodb::bson::Bson::Null) {
                        None
                    } else {
                        bson::from_bson(b.clone()).ok()
                    }
                });
            let on_demand_throughput: Option<extenddb_core::types::OnDemandThroughput> =
                backup_doc.get("on_demand_throughput").and_then(|b| {
                    if matches!(b, mongodb::bson::Bson::Null) {
                        None
                    } else {
                        bson::from_bson(b.clone()).ok()
                    }
                });

            let create_input = extenddb_core::types::CreateTableInput {
                table_name: target_table_name.clone(),
                key_schema,
                attribute_definitions: attr_defs,
                billing_mode,
                provisioned_throughput,
                global_secondary_indexes,
                local_secondary_indexes,
                stream_specification: None,
                tags: None,
                deletion_protection_enabled: None,
                sse_specification,
                table_class,
                on_demand_throughput,
                // Fields for features this backend does not implement, vector
                // indexes today, take their defaults. Adding one to
                // CreateTableInput then does not break this build.
                ..Default::default()
            };

            // Create the table with the ACTIVE transition deferred: it enters
            // CREATING with no scheduled flip, so the table cannot become
            // ACTIVE until we schedule it below, after the data copy completes.
            let desc = self
                .create_table_impl(&account_id, create_input, true)
                .await?;

            // Restore items from the backup collection using server-side `$out`.
            // The backup collection was written by `create_backup` in the same
            // document shape as the source data collection, so this is a
            // direct clone — no per-item transformation needed.
            let backup_id = backup_doc
                .get_str("backup_id")
                .map_err(|_| {
                    StorageError::Internal("backup metadata missing backup_id".to_string())
                })?
                .to_owned();
            let src_coll_name = backup_collection_name(&backup_id);
            let src_coll = self.data_db.collection::<Document>(&src_coll_name);
            let new_coll_name = data_collection_name(&desc.table_id);

            let pipeline = vec![doc! { "$out": &new_coll_name }];
            let out_cursor = src_coll
                .aggregate(pipeline)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let _drained: Vec<Document> = out_cursor
                .try_collect()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let new_data_coll = self.data_db.collection::<Document>(&new_coll_name);
            let item_count = new_data_coll
                .count_documents(doc! {})
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                as i64;

            // The base `$out` copy is complete, but restored secondary-index
            // collections are still empty. Mark the table as pending restore
            // completion and leave its indexes CREATING so the shared worker
            // can backfill them in bounded, restartable batches.
            let status_update = doc! {
                "$set": {
                    "item_count": item_count,
                    "restore_backfill_pending": true,
                },
            };
            let tables_coll = self.catalog_db.collection::<Document>("tables");
            tables_coll
                .update_one(
                    doc! { "_id": { "account_id": &account_id, "table_name": &target_table_name } },
                    status_update,
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            // The table remains CREATING until the worker has populated every
            // restored index. `desc` therefore reports CREATING, matching the
            // DynamoDB restore lifecycle while allowing the request to return
            // before potentially hundreds of thousands of index writes finish.

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
            let _ = self.delete_backup(&account_id, &backup.backup_arn).await;
            Ok(desc)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{insert_non_empty_array, restore_provisioned_throughput};
    use extenddb_core::types::{GsiInput, ProvisionedThroughput};
    use mongodb::bson::Document;

    #[test]
    fn backup_omits_empty_secondary_index_metadata() {
        let mut backup = Document::new();
        insert_non_empty_array::<GsiInput>(&mut backup, "global_secondary_indexes", &[])
            .expect("empty index metadata should be accepted");

        assert!(!backup.contains_key("global_secondary_indexes"));
    }

    #[test]
    fn restore_preserves_stored_provisioned_throughput() {
        let stored = ProvisionedThroughput {
            read_capacity_units: 7,
            write_capacity_units: 9,
        };
        assert_eq!(
            restore_provisioned_throughput("PROVISIONED", Some(stored.clone())),
            Some(stored)
        );
    }

    #[test]
    fn restore_uses_legacy_fallback_when_capacity_metadata_is_missing() {
        assert_eq!(
            restore_provisioned_throughput("PROVISIONED", None),
            Some(ProvisionedThroughput {
                read_capacity_units: 5,
                write_capacity_units: 5,
            })
        );
    }

    #[test]
    fn restore_drops_capacity_for_pay_per_request() {
        let stored = ProvisionedThroughput {
            read_capacity_units: 7,
            write_capacity_units: 9,
        };
        assert_eq!(
            restore_provisioned_throughput("PAY_PER_REQUEST", Some(stored)),
            None
        );
    }
}
