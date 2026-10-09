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
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupEngine, RestoreTableOverrides};

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

/// GSI throughput follows the table's billing mode the same way: a table
/// switched from PROVISIONED to PAY_PER_REQUEST keeps its indexes' old
/// capacities in the catalog, and restoring them would make the restored
/// on-demand table's indexes report capacity a fresh one does not.
fn restore_gsi_throughput(
    billing_mode: &str,
    gsis: Option<Vec<GsiInput>>,
) -> Option<Vec<GsiInput>> {
    if billing_mode == "PROVISIONED" {
        return gsis;
    }
    gsis.map(|gsis| {
        gsis.into_iter()
            .map(|mut gsi| {
                gsi.provisioned_throughput = None;

                gsi
            })
            .collect()
    })
}

/// A restore first claims backup metadata, then transfers protection to the
/// provenance-bearing CREATING target. DeleteBackup claims the same metadata
/// before dropping data, so neither operation can pass the other between its
/// check and destructive action.
fn restore_claim_filter(account_id: &str, backup_arn: &str) -> Document {
    doc! {
        "_id": backup_arn,
        "account_id": account_id,
        "backup_status": "AVAILABLE",
        "restore_in_progress": { "$exists": false },
        "delete_in_progress": { "$exists": false },
    }
}

fn delete_claim_filter(account_id: &str, backup_arn: &str) -> Document {
    doc! {
        "_id": backup_arn,
        "account_id": account_id,
        "backup_status": "AVAILABLE",
        "restore_in_progress": { "$exists": false },
        "delete_in_progress": { "$exists": false },
    }
}

fn restoring_target_filter(account_id: &str, backup_arn: &str) -> Document {
    doc! {
        "_id.account_id": account_id,
        "restore_source_backup_arn": backup_arn,
        "table_status": "CREATING",
    }
}

struct BackupMetadataClaim {
    backups: mongodb::Collection<Document>,
    account_id: String,
    backup_arn: String,
    field: &'static str,
    operation_id: String,
    armed: bool,
}

impl BackupMetadataClaim {
    fn new(
        backups: mongodb::Collection<Document>,
        account_id: &str,
        backup_arn: &str,
        field: &'static str,
        operation_id: String,
    ) -> Self {
        Self {
            backups,
            account_id: account_id.to_owned(),
            backup_arn: backup_arn.to_owned(),
            field,
            operation_id,
            armed: true,
        }
    }

    fn filter(&self) -> Document {
        let mut filter = doc! {
            "_id": &self.backup_arn,
            "account_id": &self.account_id,
        };
        filter.insert(self.field, &self.operation_id);
        filter
    }

    fn unset_update(field: &'static str) -> Document {
        let mut unset = Document::new();
        unset.insert(field, "");
        doc! { "$unset": unset }
    }

    async fn release(&mut self) -> Result<(), StorageError> {
        if !self.armed {
            return Ok(());
        }
        self.backups
            .update_one(self.filter(), Self::unset_update(self.field))
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        self.armed = false;
        Ok(())
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for BackupMetadataClaim {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let backups = self.backups.clone();
        let filter = self.filter();
        let update = Self::unset_update(self.field);
        let backup_arn = self.backup_arn.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let _cleanup = runtime.spawn(async move {
                if let Err(error) = backups.update_one(filter, update).await {
                    tracing::error!(
                        "could not clear cancelled backup metadata claim for {backup_arn}: {error}"
                    );
                }
            });
        }
    }
}

impl MongoEngine {
    /// Clear the backup-metadata claims a process that died mid-operation
    /// left behind. Both markers belong to one process: `restore_in_progress`
    /// is held only until the restore's target exists (after that the
    /// CREATING target itself keeps DeleteBackup out), and `delete_in_progress`
    /// only until the drop and the DELETED stamp. Neither survives a crash
    /// usefully, and left in place each would make the backup undeletable or
    /// unrestorable for good. Run at startup, before requests are served, so
    /// no live claim can be swept.
    ///
    /// A stale restore claim is simply removed: the restore never created its
    /// target (the marker is released once it has), so there is nothing else
    /// to undo. A stale delete claim is finished rather than released: the
    /// caller already received an answer or died waiting for one, the
    /// collection drop is idempotent, and leaving the backup AVAILABLE could
    /// point at data the crash already dropped.
    pub async fn sweep_stale_backup_claims(&self) -> Result<(usize, usize), StorageError> {
        let backups_coll = self.catalog_db.collection::<Document>("backups");
        let restores = backups_coll
            .update_many(
                doc! { "restore_in_progress": { "$exists": true } },
                doc! { "$unset": { "restore_in_progress": "" } },
            )
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
            .modified_count;

        let mut deletes = 0usize;
        let mut pending = backups_coll
            .find(doc! { "delete_in_progress": { "$exists": true } })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        while let Some(meta) = pending
            .try_next()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?
        {
            if let Ok(backup_id) = meta.get_str("backup_id") {
                self.drop_collection_if_exists(&backup_collection_name(backup_id))
                    .await?;
            }
            let id = meta
                .get_str("_id")
                .map_err(|_| StorageError::Internal("backup without _id".to_owned()))?;
            backups_coll
                .update_one(
                    doc! { "_id": id },
                    doc! {
                        "$set": { "backup_status": "DELETED" },
                        "$unset": { "delete_in_progress": "" },
                    },
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            deletes += 1;
        }
        Ok((usize::try_from(restores).unwrap_or(usize::MAX), deletes))
    }
}

impl MongoEngine {
    /// Remove a partial restore target without allowing its provenance to keep
    /// the source backup in use. The full predicate ensures cleanup can only
    /// claim the target created by this restore.
    async fn abort_backup_restore(
        &self,
        account_id: &str,
        target_table_name: &str,
        backup_arn: &str,
        restore_operation_id: &str,
    ) -> Result<bool, StorageError> {
        let removed = self
            .catalog_db
            .collection::<Document>("tables")
            .find_one_and_delete(doc! {
                "_id": { "account_id": account_id, "table_name": target_table_name },
                "restore_source_backup_arn": backup_arn,
                "restore_operation_id": restore_operation_id,
                "table_status": "CREATING",
                "status_transition_at": bson::Bson::Null,
            })
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let Some(removed) = removed else {
            return Ok(false);
        };
        let table_id = removed
            .get_str("table_id")
            .map_err(|_| StorageError::Internal("restore target missing table_id".to_owned()))?;

        // The catalog row is already gone, so even a failed physical cleanup
        // cannot leave provenance that blocks DeleteBackup. Try every cleanup
        // step before returning the first error so retries have less to do.
        let mut cleanup_error = None;
        let collection = data_collection_name(table_id);
        if let Err(e) = self.drop_collection_if_exists(&collection).await {
            cleanup_error = Some(e);
        }
        if let Err(e) = self.drop_index_collections_for_table(table_id).await {
            cleanup_error.get_or_insert(e);
        }
        self.gsi_cache_invalidate(table_id);

        if let Some(e) = cleanup_error {
            Err(e)
        } else {
            Ok(true)
        }
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
            let backups_coll = self.catalog_db.collection::<Document>("backups");
            let tables_coll = self.catalog_db.collection::<Document>("tables");

            // Fast-path the durable half of the two-phase restore claim.
            if let Some(target) = tables_coll
                .find_one(restoring_target_filter(&account_id, &backup_arn))
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                let name = target
                    .get_document("_id")
                    .ok()
                    .and_then(|id| id.get_str("table_name").ok())
                    .unwrap_or("?");
                return Err(StorageError::BackupInUse(format!(
                    "Backup is being used to restore table {name}: {backup_arn}"
                )));
            }

            // Atomically claim deletion only when restore has not claimed the
            // metadata. Restore uses the inverse predicate, so once either
            // marker is installed the other operation cannot start.
            let delete_operation_id = uuid::Uuid::new_v4().to_string();
            let meta = backups_coll
                .find_one_and_update(
                    delete_claim_filter(&account_id, &backup_arn),
                    doc! { "$set": { "delete_in_progress": &delete_operation_id } },
                )
                .return_document(mongodb::options::ReturnDocument::Before)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let Some(meta) = meta else {
                let restoring = tables_coll
                    .find_one(restoring_target_filter(&account_id, &backup_arn))
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?
                    .is_some();
                let claimed_by_restore = backups_coll
                    .find_one(doc! {
                        "_id": &backup_arn,
                        "account_id": &account_id,
                        "backup_status": "AVAILABLE",
                        "restore_in_progress": { "$exists": true },
                    })
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?
                    .is_some();
                if restoring || claimed_by_restore {
                    return Err(StorageError::BackupInUse(format!(
                        "Backup is being used by a restore: {backup_arn}"
                    )));
                }
                return Err(StorageError::Validation(format!(
                    "Backup not found: {backup_arn}"
                )));
            };
            let mut delete_claim = BackupMetadataClaim::new(
                backups_coll.clone(),
                &account_id,
                &backup_arn,
                "delete_in_progress",
                delete_operation_id,
            );

            // A restore can transfer its marker to a target between the first
            // target read and our metadata claim. Recheck after claiming; new
            // restores are now excluded by delete_in_progress.
            if let Some(target) = tables_coll
                .find_one(restoring_target_filter(&account_id, &backup_arn))
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
            {
                let name = target
                    .get_document("_id")
                    .ok()
                    .and_then(|id| id.get_str("table_name").ok())
                    .unwrap_or("?")
                    .to_owned();
                delete_claim.release().await?;
                return Err(StorageError::BackupInUse(format!(
                    "Backup is being used to restore table {name}: {backup_arn}"
                )));
            }

            // Drop the backup collection. If backup_id is absent (for an old
            // pre-$out backup), there is no physical collection to drop.
            if let Ok(backup_id) = meta.get_str("backup_id") {
                let coll_name = backup_collection_name(backup_id);
                let coll = self.data_db.collection::<Document>(&coll_name);
                if let Err(error) = coll.drop().await {
                    if let Err(cleanup) = delete_claim.release().await {
                        tracing::error!(
                            "DeleteBackup {backup_arn} failed ({error}) and its metadata claim \
                             could not be cleared: {cleanup}"
                        );
                    }
                    return Err(StorageError::Internal(error.to_string()));
                }
            }

            backups_coll
                .update_one(
                    delete_claim.filter(),
                    doc! {
                        "$set": { "backup_status": "DELETED" },
                        "$unset": { "delete_in_progress": "" },
                    },
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            delete_claim.disarm();

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
        overrides: RestoreTableOverrides,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_string();
        let target_table_name = target_table_name.to_string();
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let backups_coll = self.catalog_db.collection::<Document>("backups");
            let restore_operation_id = uuid::Uuid::new_v4().to_string();
            let backup_doc = backups_coll
                // This metadata marker is phase one of the restore claim. It is
                // installed atomically with the AVAILABLE check, before any
                // restore state is derived, and excludes DeleteBackup's claim.
                .find_one_and_update(
                    restore_claim_filter(&account_id, &backup_arn),
                    doc! { "$set": { "restore_in_progress": &restore_operation_id } },
                )
                .return_document(mongodb::options::ReturnDocument::After)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?
                .ok_or_else(|| {
                    StorageError::Validation(format!("Backup not found: {backup_arn}"))
                })?;
            let mut restore_claim = BackupMetadataClaim::new(
                backups_coll.clone(),
                &account_id,
                &backup_arn,
                "restore_in_progress",
                restore_operation_id.clone(),
            );

            let prepared = async {
                let key_schema_bson = backup_doc
                    .get_array("key_schema")
                    .map_err(|_| StorageError::Internal("missing key_schema".to_string()))?;
                let attr_defs_bson =
                    backup_doc.get_array("attribute_definitions").map_err(|_| {
                        StorageError::Internal("missing attribute_definitions".to_string())
                    })?;
                let billing = backup_doc
                    .get_str("billing_mode")
                    .unwrap_or("PAY_PER_REQUEST");
                let effective_billing = match overrides.billing_mode {
                    Some(extenddb_core::types::BillingMode::Provisioned) => "PROVISIONED",
                    Some(extenddb_core::types::BillingMode::PayPerRequest) => "PAY_PER_REQUEST",
                    None => billing,
                };

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

                let billing_mode = if effective_billing == "PAY_PER_REQUEST" {
                    Some(extenddb_core::types::BillingMode::PayPerRequest)
                } else {
                    Some(extenddb_core::types::BillingMode::Provisioned)
                };

                // New backups preserve these fields. Keep the old 5/5 fallback
                // for backups created before the metadata was added, while
                // correctly omitting provisioned throughput for on-demand tables.
                let mut provisioned_throughput: Option<ProvisionedThroughput> =
                    decode_optional(&backup_doc, "provisioned_throughput")?;
                if let Some(throughput) = overrides.provisioned_throughput.clone() {
                    provisioned_throughput = Some(throughput);
                }
                let provisioned_throughput =
                    restore_provisioned_throughput(effective_billing, provisioned_throughput);
                let global_secondary_indexes: Option<Vec<GsiInput>> =
                    decode_optional(&backup_doc, "global_secondary_indexes")?;
                let global_secondary_indexes =
                    restore_gsi_throughput(effective_billing, global_secondary_indexes);
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

                if effective_billing == "PROVISIONED"
                    && let Some(index) = global_secondary_indexes
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .find(|index| index.provisioned_throughput.is_none())
                {
                    return Err(StorageError::Validation(format!(
                        "One or more parameter values were invalid: GlobalSecondaryIndexOverride \
                     must be specified for index: {} when BillingModeOverride is PROVISIONED",
                        index.index_name
                    )));
                }

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
                Ok::<_, StorageError>(create_input)
            }
            .await;
            let create_input = match prepared {
                Ok(input) => input,
                Err(error) => {
                    if let Err(cleanup) = restore_claim.release().await {
                        tracing::error!(
                            "restore of {backup_arn} failed validation ({error}) and its metadata \
                             claim could not be cleared: {cleanup}"
                        );
                    }
                    return Err(error);
                }
            };

            // Provenance is part of the initial CREATING document. Keep the
            // metadata marker until target creation returns: protection then
            // overlaps, and DeleteBackup can observe neither an unclaimed
            // backup nor an ownerless target.
            let restore_at = bson::DateTime::now();
            let created = self
                .create_table_for_restore(
                    &account_id,
                    create_input,
                    &backup_arn,
                    restore_at,
                    &restore_operation_id,
                )
                .await;
            let mut desc = match created {
                Ok(desc) => {
                    if let Err(claim_error) = restore_claim.release().await {
                        if let Err(cleanup) = self
                            .abort_backup_restore(
                                &account_id,
                                &target_table_name,
                                &backup_arn,
                                &restore_operation_id,
                            )
                            .await
                        {
                            tracing::error!(
                                "restore of {backup_arn} could not transfer its metadata claim \
                                 ({claim_error}) and target cleanup failed: {cleanup}"
                            );
                        }
                        return Err(claim_error);
                    }
                    desc
                }
                Err(error) => {
                    if let Err(cleanup) = self
                        .abort_backup_restore(
                            &account_id,
                            &target_table_name,
                            &backup_arn,
                            &restore_operation_id,
                        )
                        .await
                    {
                        tracing::error!(
                            "restore of {backup_arn} into {target_table_name} failed during \
                             target creation ({error}), and cleanup failed: {cleanup}"
                        );
                    }
                    if let Err(cleanup) = restore_claim.release().await {
                        tracing::error!(
                            "restore of {backup_arn} failed during target creation ({error}), and \
                             its metadata claim could not be cleared: {cleanup}"
                        );
                    }
                    return Err(error);
                }
            };
            #[allow(clippy::cast_precision_loss)]
            {
                desc.restore_summary = Some(extenddb_core::types::RestoreSummary {
                    source_backup_arn: Some(backup_arn.clone()),
                    restore_date_time: restore_at.timestamp_millis() as f64 / 1000.0,
                    restore_in_progress: true,
                });
            }

            let copied = async {
                // Restore items from the backup collection using server-side
                // `$out`. The backup collection has the source document shape,
                // so this is a direct clone with no per-item transformation.
                let backup_id = backup_doc
                    .get_str("backup_id")
                    .map_err(|_| {
                        StorageError::Internal("backup metadata missing backup_id".to_string())
                    })?
                    .to_owned();
                let src_coll_name = backup_collection_name(&backup_id);
                let src_coll = self.data_db.collection::<Document>(&src_coll_name);
                let new_coll_name = data_collection_name(&desc.table_id);

                // The test-hook gate holds the restore after its CREATING index
                // metadata exists but before the base `$out` copy begins. This
                // lets the integration test force a worker tick through the
                // dangerous pre-copy window.
                self.wait_for_gsi_backfill_test_gate(&target_table_name)
                    .await?;

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
                    "$unset": { "restore_operation_id": "" },
                };
                let tables_coll = self.catalog_db.collection::<Document>("tables");
                tables_coll
                .update_one(
                    doc! { "_id": { "account_id": &account_id, "table_name": &target_table_name } },
                    status_update,
                )
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

                // The table remains CREATING until the worker has populated
                // every restored index. The response therefore matches the
                // restore lifecycle without waiting for the backfill.
                Ok::<(), StorageError>(())
            }
            .await;

            if let Err(e) = copied {
                match self
                    .abort_backup_restore(
                        &account_id,
                        &target_table_name,
                        &backup_arn,
                        &restore_operation_id,
                    )
                    .await
                {
                    Ok(true) => tracing::error!(
                        "restore of {backup_arn} into {target_table_name} failed and the \
                         partial target was removed: {e}"
                    ),
                    Ok(false) => tracing::error!(
                        "restore of {backup_arn} into {target_table_name} failed after its \
                         target was already removed: {e}"
                    ),
                    Err(cleanup) => tracing::error!(
                        "restore of {backup_arn} into {target_table_name} failed ({e}), and \
                         physical cleanup also failed: {cleanup}"
                    ),
                }
                return Err(e);
            }

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
                .restore_table_from_backup(
                    &account_id,
                    &target_table_name,
                    &backup.backup_arn,
                    RestoreTableOverrides::default(),
                )
                .await?;
            let _ = self.delete_backup(&account_id, &backup.backup_arn).await;
            Ok(desc)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        delete_claim_filter, insert_non_empty_array, restore_claim_filter, restore_gsi_throughput,
        restore_provisioned_throughput, restoring_target_filter,
    };
    use extenddb_core::types::{GsiInput, ProvisionedThroughput};
    use mongodb::bson::Document;

    #[test]
    fn gsi_throughput_is_dropped_for_an_on_demand_table() {
        use extenddb_core::types::{
            GsiInput, KeySchemaElement, KeyType, Projection, ProjectionType,
        };
        let gsi = GsiInput {
            index_name: "g".to_owned(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gpk".to_owned(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: ProjectionType::All,
                non_key_attributes: None,
            },
            provisioned_throughput: Some(ProvisionedThroughput {
                read_capacity_units: 7,
                write_capacity_units: 8,
            }),
        };
        let kept = restore_gsi_throughput("PROVISIONED", Some(vec![gsi.clone()])).expect("gsis");
        assert!(kept[0].provisioned_throughput.is_some());
        let dropped = restore_gsi_throughput("PAY_PER_REQUEST", Some(vec![gsi])).expect("gsis");
        assert!(dropped[0].provisioned_throughput.is_none());
        assert!(restore_gsi_throughput("PAY_PER_REQUEST", None).is_none());
    }

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

    #[test]
    fn restore_and_delete_claims_exclude_each_other() {
        let restore = restore_claim_filter("123456789012", "backup");
        let delete = delete_claim_filter("123456789012", "backup");
        for filter in [&restore, &delete] {
            assert_eq!(filter.get_str("backup_status"), Ok("AVAILABLE"));
            assert_eq!(
                filter
                    .get_document("restore_in_progress")
                    .and_then(|predicate| predicate.get_bool("$exists")),
                Ok(false)
            );
            assert_eq!(
                filter
                    .get_document("delete_in_progress")
                    .and_then(|predicate| predicate.get_bool("$exists")),
                Ok(false)
            );
        }
    }

    #[test]
    fn durable_restore_target_predicate_is_account_scoped_and_creating() {
        let filter = restoring_target_filter("123456789012", "backup");
        assert_eq!(filter.get_str("_id.account_id"), Ok("123456789012"));
        assert_eq!(filter.get_str("restore_source_backup_arn"), Ok("backup"));
        assert_eq!(filter.get_str("table_status"), Ok("CREATING"));
    }
}
