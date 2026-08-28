// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backup and restore implementation for Cassandra storage.
//!
//! Cassandra's native snapshots are node-local operational artifacts and require
//! filesystem/JMX orchestration that is not available through CQL. This module
//! therefore implements the DynamoDB-facing API as a logical snapshot: immutable
//! item JSON is stored in bounded partitions in the owning account keyspace,
//! while metadata and list access paths are stored in the catalog keyspace.

use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::query::BatchQueryBuilder;
use cdrs_tokio::types::{IntoRustByName, rows::Row};
use extenddb_core::types::{
    BackupDescription, BackupDetails, BackupSummary, BillingMode, ContinuousBackupsDescription,
    CreateTableInput, PointInTimeRecoveryDescription, ProvisionedThroughput, SourceTableDetails,
    TableDescription, TableKeyInfo,
};
use extenddb_storage::BackupEngine;
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;

use crate::CassandraEngine;

/// Maximum number of item payloads stored in one Cassandra partition.
///
/// DynamoDB items are at most 400 KiB, so 64 items bound a partition to roughly
/// 25 MiB before Cassandra overhead.
const BACKUP_ITEMS_PER_BUCKET: i64 = 64;
const BACKUP_SCAN_PAGE_SIZE: i64 = 1_000;

/// Current epoch milliseconds, used in backup identifiers and timestamps.
fn epoch_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Build the trailing `backup/<id>` component of a backup ARN.
fn backup_id() -> String {
    use rand::Rng;
    let suffix: u32 = rand::rng().random();
    format!("{}-{suffix:08x}", epoch_millis())
}

fn bucket_count(item_count: i64) -> i64 {
    if item_count <= 0 {
        0
    } else {
        (item_count + BACKUP_ITEMS_PER_BUCKET - 1) / BACKUP_ITEMS_PER_BUCKET
    }
}

#[derive(Debug)]
struct StoredBackup {
    backup_arn: String,
    account_id: String,
    backup_name: String,
    table_id: String,
    table_name: String,
    table_arn: String,
    backup_status: String,
    backup_type: String,
    backup_size_bytes: i64,
    item_count: i64,
    key_schema: String,
    attribute_definitions: String,
    billing_mode: String,
    provisioned_throughput: Option<String>,
    stream_specification: Option<String>,
    table_created_at: i64,
    created_at: i64,
}

impl StoredBackup {
    fn from_row(row: &Row) -> Result<Self, StorageError> {
        Ok(Self {
            backup_arn: row
                .get_r_by_name("backup_arn")
                .map_err(|e| StorageError::Internal(format!("Parse backup_arn: {e}")))?,
            account_id: row
                .get_r_by_name("account_id")
                .map_err(|e| StorageError::Internal(format!("Parse account_id: {e}")))?,
            backup_name: row
                .get_r_by_name("backup_name")
                .map_err(|e| StorageError::Internal(format!("Parse backup_name: {e}")))?,
            table_id: row
                .get_r_by_name("table_id")
                .map_err(|e| StorageError::Internal(format!("Parse table_id: {e}")))?,
            table_name: row
                .get_r_by_name("table_name")
                .map_err(|e| StorageError::Internal(format!("Parse table_name: {e}")))?,
            table_arn: row
                .get_r_by_name("table_arn")
                .map_err(|e| StorageError::Internal(format!("Parse table_arn: {e}")))?,
            backup_status: row
                .get_r_by_name("backup_status")
                .map_err(|e| StorageError::Internal(format!("Parse backup_status: {e}")))?,
            backup_type: row
                .get_r_by_name("backup_type")
                .map_err(|e| StorageError::Internal(format!("Parse backup_type: {e}")))?,
            backup_size_bytes: row.get_r_by_name("backup_size_bytes").unwrap_or(0),
            item_count: row.get_r_by_name("item_count").unwrap_or(0),
            key_schema: row
                .get_r_by_name("key_schema")
                .map_err(|e| StorageError::Internal(format!("Parse key_schema: {e}")))?,
            attribute_definitions: row
                .get_r_by_name("attribute_definitions")
                .map_err(|e| StorageError::Internal(format!("Parse attribute_definitions: {e}")))?,
            billing_mode: row
                .get_r_by_name("billing_mode")
                .map_err(|e| StorageError::Internal(format!("Parse billing_mode: {e}")))?,
            provisioned_throughput: row.get_by_name("provisioned_throughput").ok().flatten(),
            stream_specification: row.get_by_name("stream_specification").ok().flatten(),
            table_created_at: row
                .get_r_by_name("table_created_at")
                .map_err(|e| StorageError::Internal(format!("Parse table_created_at: {e}")))?,
            created_at: row
                .get_r_by_name("created_at")
                .map_err(|e| StorageError::Internal(format!("Parse created_at: {e}")))?,
        })
    }

    fn details(&self) -> BackupDetails {
        BackupDetails {
            backup_arn: self.backup_arn.clone(),
            backup_name: self.backup_name.clone(),
            backup_status: self.backup_status.clone(),
            backup_type: self.backup_type.clone(),
            backup_size_bytes: self.backup_size_bytes,
            backup_creation_date_time: crate::cassandra_util::millis_to_seconds_f64(self.created_at),
        }
    }

    fn summary(&self) -> BackupSummary {
        BackupSummary {
            backup_arn: self.backup_arn.clone(),
            backup_name: self.backup_name.clone(),
            table_name: self.table_name.clone(),
            table_arn: self.table_arn.clone(),
            backup_status: self.backup_status.clone(),
            backup_type: self.backup_type.clone(),
            backup_size_bytes: self.backup_size_bytes,
            backup_creation_date_time: crate::cassandra_util::millis_to_seconds_f64(self.created_at),
        }
    }

    fn description(&self) -> Result<BackupDescription, StorageError> {
        let key_schema = serde_json::from_str(&self.key_schema)
            .map_err(|e| StorageError::Internal(format!("Parse key schema: {e}")))?;
        Ok(BackupDescription {
            backup_details: self.details(),
            source_table_details: SourceTableDetails {
                table_name: self.table_name.clone(),
                table_id: self.table_id.clone(),
                table_arn: self.table_arn.clone(),
                key_schema,
                item_count: self.item_count,
                table_size_bytes: self.backup_size_bytes,
                billing_mode: Some(self.billing_mode.clone()),
                table_creation_date_time: crate::cassandra_util::millis_to_seconds_f64(self.table_created_at),
            },
        })
    }
}

impl CassandraEngine {
    async fn load_backup(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> Result<StoredBackup, StorageError> {
        let query = format!(
            "SELECT * FROM {}.backups_by_arn WHERE account_id = ? AND backup_arn = ?",
            self.catalog_keyspace()
        );
        let result = self
            .session
            .query_with_values(&query, cdrs_tokio::query_values!(account_id, backup_arn))
            .await
            .map_err(|e| StorageError::Internal(format!("Query backup: {e}")))?;
        let rows = result
            .response_body()
            .map_err(|e| StorageError::Internal(format!("Parse backup response: {e}")))?
            .into_rows()
            .unwrap_or_default();
        let backup = rows
            .first()
            .map(StoredBackup::from_row)
            .transpose()?
            .filter(|backup| backup.account_id == account_id)
            .ok_or_else(|| StorageError::Validation(format!("Backup not found: {backup_arn}")))?;
        Ok(backup)
    }

    async fn load_available_backup(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> Result<StoredBackup, StorageError> {
        let backup = self.load_backup(account_id, backup_arn).await?;
        if backup.backup_status != "AVAILABLE" {
            return Err(StorageError::Validation(format!(
                "Backup not found: {backup_arn}"
            )));
        }
        Ok(backup)
    }

    async fn cleanup_backup_payload(
        &self,
        account_id: &str,
        backup_arn: &str,
        item_count: i64,
    ) -> Result<(), StorageError> {
        let account_keyspace = self.account_keyspace(account_id);
        let query = format!(
            "DELETE FROM {account_keyspace}.backup_items WHERE backup_arn = ? AND bucket = ?"
        );
        for bucket in 0..bucket_count(item_count) {
            #[allow(clippy::cast_possible_truncation)]
            let bucket_i32 = bucket as i32;
            self.session
                .query_with_values(&query, cdrs_tokio::query_values!(backup_arn, bucket_i32))
                .await
                .map_err(|e| StorageError::Internal(format!("Delete backup payload: {e}")))?;
        }
        Ok(())
    }

    async fn remove_backup_index_rows(&self, backup: &StoredBackup) -> Result<(), StorageError> {
        let catalog = self.catalog_keyspace();
        let by_account = format!(
            "DELETE FROM {catalog}.backups_by_account WHERE account_id = ? AND created_at = ? AND backup_arn = ?"
        );
        self.session
            .query_with_values(
                &by_account,
                cdrs_tokio::query_values!(
                    backup.account_id.as_str(),
                    backup.created_at,
                    backup.backup_arn.as_str()
                ),
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Delete account backup index: {e}")))?;

        let by_table = format!(
            "DELETE FROM {catalog}.backups_by_table WHERE account_id = ? AND table_name = ? AND created_at = ? AND backup_arn = ?"
        );
        self.session
            .query_with_values(
                &by_table,
                cdrs_tokio::query_values!(
                    backup.account_id.as_str(),
                    backup.table_name.as_str(),
                    backup.created_at,
                    backup.backup_arn.as_str()
                ),
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Delete table backup index: {e}")))?;
        Ok(())
    }

    async fn mark_table_active_after_restore(
        &self,
        account_id: &str,
        table_name: &str,
        table_id: &str,
        item_count: i64,
        table_size_bytes: i64,
    ) -> Result<(), StorageError> {
        let query = format!(
            "UPDATE {}.tables SET table_status = 'ACTIVE', status_transition_at = NULL, item_count = ?, table_size_bytes = ? WHERE account_id = ? AND table_name = ? IF table_id = ? AND table_status = 'CREATING'",
            self.catalog_keyspace()
        );
        let result = self
            .session
            .query_with_values(
                &query,
                cdrs_tokio::query_values!(
                    item_count,
                    table_size_bytes,
                    account_id,
                    table_name,
                    table_id
                ),
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Activate restored table: {e}")))?;
        let rows = result
            .response_body()
            .map_err(|e| StorageError::Internal(format!("Parse restore activation: {e}")))?
            .into_rows()
            .unwrap_or_default();
        if let Some(row) = rows.first() {
            let applied: bool = row.get_r_by_name("[applied]").map_err(|e| {
                StorageError::Internal(format!("Parse restore activation result: {e}"))
            })?;
            if !applied {
                return Err(StorageError::Internal(
                    "Restore target changed before activation".to_owned(),
                ));
            }
        }
        Ok(())
    }

    async fn table_exists_for_backup(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> Result<bool, StorageError> {
        let query = format!(
            "SELECT table_name FROM {}.tables WHERE account_id = ? AND table_name = ?",
            self.catalog_keyspace()
        );
        let rows = self
            .session
            .query_with_values(&query, cdrs_tokio::query_values!(account_id, table_name))
            .await
            .map_err(|e| StorageError::Internal(format!("Query table: {e}")))?
            .response_body()
            .map_err(|e| StorageError::Internal(format!("Parse table response: {e}")))?
            .into_rows()
            .unwrap_or_default();
        Ok(!rows.is_empty())
    }
}

impl BackupEngine for CassandraEngine {
    fn create_backup(
        &self,
        account_id: &str,
        table_name: &str,
        backup_name: &str,
    ) -> BoxFuture<'_, Result<BackupDetails, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        let backup_name = backup_name.to_owned();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;

            // fetch_table_key_info enforces the DynamoDB requirement that only
            // ACTIVE tables can be backed up.
            let key_info = self.fetch_table_key_info(&account_id, &table_name).await?;
            let table = self
                .build_table_description(&account_id, &table_name)
                .await?;
            let backup_arn = format!(
                "arn:aws:dynamodb:{region}:{account_id}:table/{table_name}/backup/{id}",
                region = self.region,
                id = backup_id()
            );
            let created_at = chrono::Utc::now().timestamp_millis();
            #[allow(clippy::cast_possible_truncation)]
            let table_created_at = (table.creation_date_time * 1_000.0) as i64;
            let billing_mode = if table.billing_mode_summary.is_some() {
                "PAY_PER_REQUEST"
            } else {
                "PROVISIONED"
            };
            let key_schema = serde_json::to_string(&table.key_schema)
                .map_err(|e| StorageError::Internal(format!("Serialize key schema: {e}")))?;
            let attribute_definitions = serde_json::to_string(&table.attribute_definitions)
                .map_err(|e| StorageError::Internal(format!("Serialize attributes: {e}")))?;
            let provisioned = ProvisionedThroughput {
                read_capacity_units: table.provisioned_throughput.read_capacity_units,
                write_capacity_units: table.provisioned_throughput.write_capacity_units,
            };
            let provisioned_throughput = serde_json::to_string(&provisioned)
                .map_err(|e| StorageError::Internal(format!("Serialize throughput: {e}")))?;
            let stream_specification = table
                .stream_specification
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|e| StorageError::Internal(format!("Serialize stream: {e}")))?;

            let catalog = self.catalog_keyspace();
            let insert_metadata = format!(
                "INSERT INTO {catalog}.backups_by_arn (backup_arn, account_id, backup_name, table_id, table_name, table_arn, backup_status, backup_type, backup_size_bytes, item_count, key_schema, attribute_definitions, billing_mode, provisioned_throughput, stream_specification, table_created_at, created_at) VALUES (?, ?, ?, ?, ?, ?, 'CREATING', 'USER', ?, 0, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS"
            );
            self.session
                .query_with_values(
                    &insert_metadata,
                    cdrs_tokio::query_values!(
                        backup_arn.as_str(),
                        account_id.as_str(),
                        backup_name.as_str(),
                        table.table_id.as_str(),
                        table_name.as_str(),
                        table.table_arn.as_str(),
                        table.table_size_bytes,
                        key_schema.as_str(),
                        attribute_definitions.as_str(),
                        billing_mode,
                        provisioned_throughput.as_str(),
                        stream_specification.as_deref(),
                        table_created_at,
                        created_at
                    ),
                )
                .await
                .map_err(|e| StorageError::Internal(format!("Create backup metadata: {e}")))?;

            let account_keyspace = self.account_keyspace(&account_id);
            let insert_item = format!(
                "INSERT INTO {account_keyspace}.backup_items (backup_arn, bucket, item_index, item_data) VALUES (?, ?, ?, ?)"
            );
            let mut item_count = 0_i64;
            let mut payload_size = 0_i64;
            let mut start_key = None;

            let snapshot_result: Result<(), StorageError> = async {
                loop {
                    let (items, next_key) = self
                        .scan_impl(
                            &key_info,
                            Some(BACKUP_SCAN_PAGE_SIZE),
                            start_key.as_ref(),
                            None,
                            None,
                            None,
                        )
                        .await?;
                    for item in items {
                        let item_data = serde_json::to_string(&item).map_err(|e| {
                            StorageError::Internal(format!("Serialize backup item: {e}"))
                        })?;
                        let bucket = item_count / BACKUP_ITEMS_PER_BUCKET;
                        #[allow(clippy::cast_possible_truncation)]
                        let bucket_i32 = bucket as i32;
                        self.session
                            .query_with_values(
                                &insert_item,
                                cdrs_tokio::query_values!(
                                    backup_arn.as_str(),
                                    bucket_i32,
                                    item_count,
                                    item_data.as_str()
                                ),
                            )
                            .await
                            .map_err(|e| {
                                StorageError::Internal(format!("Write backup item: {e}"))
                            })?;
                        item_count += 1;
                        #[allow(clippy::cast_possible_wrap)]
                        let item_len = item_data.len() as i64;
                        payload_size = payload_size.saturating_add(item_len);
                    }
                    match next_key {
                        Some(key) => start_key = Some(key),
                        None => break,
                    }
                }
                Ok(())
            }
            .await;

            if let Err(error) = snapshot_result {
                let _ = self
                    .cleanup_backup_payload(&account_id, &backup_arn, item_count)
                    .await;
                let delete_metadata = format!(
                    "DELETE FROM {catalog}.backups_by_arn WHERE account_id = ? AND backup_arn = ?"
                );
                let _ = self
                    .session
                    .query_with_values(
                        &delete_metadata,
                        cdrs_tokio::query_values!(account_id.as_str(), backup_arn.as_str()),
                    )
                    .await;
                return Err(error);
            }

            let backup_size_bytes = table.table_size_bytes.max(payload_size);
            let insert_by_account = format!(
                "INSERT INTO {catalog}.backups_by_account (account_id, created_at, backup_arn, backup_name, table_id, table_name, table_arn, backup_size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            );
            let insert_by_table = format!(
                "INSERT INTO {catalog}.backups_by_table (account_id, table_name, created_at, backup_arn, backup_name, table_id, table_arn, backup_size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            );
            let publish = format!(
                "UPDATE {catalog}.backups_by_arn SET backup_status = 'AVAILABLE', backup_size_bytes = ?, item_count = ? WHERE account_id = ? AND backup_arn = ?"
            );
            // Publication is a fixed three-statement operation. A logged batch
            // prevents readers from observing list rows without the matching
            // AVAILABLE authoritative row, or vice versa.
            let publish_result: Result<(), StorageError> = async {
                let batch = BatchQueryBuilder::new()
                    .with_consistency(Consistency::LocalQuorum)
                    .add_query(
                        insert_by_account,
                        cdrs_tokio::query_values!(
                            account_id.as_str(),
                            created_at,
                            backup_arn.as_str(),
                            backup_name.as_str(),
                            table.table_id.as_str(),
                            table_name.as_str(),
                            table.table_arn.as_str(),
                            backup_size_bytes
                        ),
                    )
                    .add_query(
                        insert_by_table,
                        cdrs_tokio::query_values!(
                            account_id.as_str(),
                            table_name.as_str(),
                            created_at,
                            backup_arn.as_str(),
                            backup_name.as_str(),
                            table.table_id.as_str(),
                            table.table_arn.as_str(),
                            backup_size_bytes
                        ),
                    )
                    .add_query(
                        publish,
                        cdrs_tokio::query_values!(
                            backup_size_bytes,
                            item_count,
                            account_id.as_str(),
                            backup_arn.as_str()
                        ),
                    );
                self.session
                    .batch(
                        batch
                            .build()
                            .map_err(|e| StorageError::Internal(e.to_string()))?,
                    )
                    .await
                    .map_err(|e| StorageError::Internal(format!("Publish backup batch: {e}")))?;
                Ok(())
            }
            .await;

            if let Err(error) = publish_result {
                if let Ok(backup) = self.load_backup(&account_id, &backup_arn).await {
                    let _ = self.remove_backup_index_rows(&backup).await;
                }
                let _ = self
                    .cleanup_backup_payload(&account_id, &backup_arn, item_count)
                    .await;
                let delete_metadata = format!(
                    "DELETE FROM {catalog}.backups_by_arn WHERE account_id = ? AND backup_arn = ?"
                );
                let _ = self
                    .session
                    .query_with_values(
                        &delete_metadata,
                        cdrs_tokio::query_values!(account_id.as_str(), backup_arn.as_str()),
                    )
                    .await;
                return Err(error);
            }

            Ok(BackupDetails {
                backup_arn,
                backup_name,
                backup_status: "AVAILABLE".to_owned(),
                backup_type: "USER".to_owned(),
                backup_size_bytes,
                backup_creation_date_time: crate::cassandra_util::millis_to_seconds_f64(created_at),
            })
        })
    }

    fn describe_backup(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            self.load_available_backup(&account_id, &backup_arn)
                .await?
                .description()
        })
    }

    fn list_backups(
        &self,
        account_id: &str,
        table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.map(ToOwned::to_owned);
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let catalog = self.catalog_keyspace();
            let (query, result) = if let Some(table_name) = table_name.as_deref() {
                let query = format!(
                    "SELECT backup_arn, backup_name, table_name, table_arn, backup_size_bytes, created_at FROM {catalog}.backups_by_table WHERE account_id = ? AND table_name = ?"
                );
                let result = self
                    .session
                    .query_with_values(
                        &query,
                        cdrs_tokio::query_values!(account_id.as_str(), table_name),
                    )
                    .await;
                (query, result)
            } else {
                let query = format!(
                    "SELECT backup_arn, backup_name, table_name, table_arn, backup_size_bytes, created_at FROM {catalog}.backups_by_account WHERE account_id = ?"
                );
                let result = self
                    .session
                    .query_with_values(&query, cdrs_tokio::query_values!(account_id.as_str()))
                    .await;
                (query, result)
            };
            let _ = query;
            let rows = result
                .map_err(|e| StorageError::Internal(format!("List backups: {e}")))?
                .response_body()
                .map_err(|e| StorageError::Internal(format!("Parse backup list: {e}")))?
                .into_rows()
                .unwrap_or_default();

            let mut summaries = Vec::with_capacity(rows.len());
            for row in rows {
                let backup_arn: String = row
                    .get_r_by_name("backup_arn")
                    .map_err(|e| StorageError::Internal(format!("Parse backup_arn: {e}")))?;
                match self.load_available_backup(&account_id, &backup_arn).await {
                    Ok(backup) => summaries.push(backup.summary()),
                    Err(StorageError::Validation(_)) => {
                        // A stale denormalized row from an interrupted create or
                        // delete must never advertise an unavailable backup.
                    }
                    Err(error) => return Err(error),
                }
            }
            Ok(summaries)
        })
    }

    fn delete_backup(
        &self,
        account_id: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            let mut backup = self.load_backup(&account_id, &backup_arn).await?;
            if backup.backup_status != "AVAILABLE" && backup.backup_status != "DELETING" {
                return Err(StorageError::Validation(format!(
                    "Backup not found: {backup_arn}"
                )));
            }
            let original_description = backup.description()?;
            let mark_deleting = format!(
                "UPDATE {}.backups_by_arn SET backup_status = 'DELETING' WHERE account_id = ? AND backup_arn = ?",
                self.catalog_keyspace()
            );
            self.session
                .query_with_values(
                    &mark_deleting,
                    cdrs_tokio::query_values!(account_id.as_str(), backup_arn.as_str()),
                )
                .await
                .map_err(|e| StorageError::Internal(format!("Mark backup deleting: {e}")))?;

            self.remove_backup_index_rows(&backup).await?;
            self.cleanup_backup_payload(&account_id, &backup_arn, backup.item_count)
                .await?;

            let mark_deleted = format!(
                "UPDATE {}.backups_by_arn SET backup_status = 'DELETED' WHERE account_id = ? AND backup_arn = ?",
                self.catalog_keyspace()
            );
            self.session
                .query_with_values(
                    &mark_deleted,
                    cdrs_tokio::query_values!(account_id.as_str(), backup_arn.as_str()),
                )
                .await
                .map_err(|e| StorageError::Internal(format!("Mark backup deleted: {e}")))?;

            backup.backup_status = "DELETED".to_owned();
            Ok(BackupDescription {
                backup_details: BackupDetails {
                    backup_status: "DELETED".to_owned(),
                    ..original_description.backup_details
                },
                source_table_details: original_description.source_table_details,
            })
        })
    }

    fn restore_table_from_backup(
        &self,
        account_id: &str,
        target_table_name: &str,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let target_table_name = target_table_name.to_owned();
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            Self::validate_account_id(&account_id)?;
            let backup = self.load_available_backup(&account_id, &backup_arn).await?;
            let key_schema: Vec<extenddb_core::types::KeySchemaElement> =
                serde_json::from_str(&backup.key_schema)
                    .map_err(|e| StorageError::Internal(format!("Parse key schema: {e}")))?;
            let attribute_definitions: Vec<extenddb_core::types::AttributeDefinition> =
                serde_json::from_str(&backup.attribute_definitions)
                    .map_err(|e| StorageError::Internal(format!("Parse attributes: {e}")))?;
            let billing_mode = if backup.billing_mode == "PAY_PER_REQUEST" {
                Some(BillingMode::PayPerRequest)
            } else {
                Some(BillingMode::Provisioned)
            };
            let mut provisioned_throughput: ProvisionedThroughput = backup
                .provisioned_throughput
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(|e| StorageError::Internal(format!("Parse throughput: {e}")))?
                .unwrap_or(ProvisionedThroughput {
                    read_capacity_units: 5,
                    write_capacity_units: 5,
                });
            if provisioned_throughput.read_capacity_units <= 0 {
                provisioned_throughput.read_capacity_units = 5;
            }
            if provisioned_throughput.write_capacity_units <= 0 {
                provisioned_throughput.write_capacity_units = 5;
            }
            let stream_specification = backup
                .stream_specification
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(|e| StorageError::Internal(format!("Parse stream: {e}")))?;

            let create_input = CreateTableInput {
                table_name: target_table_name.clone(),
                key_schema: key_schema.clone(),
                attribute_definitions: attribute_definitions.clone(),
                billing_mode,
                provisioned_throughput: Some(provisioned_throughput),
                global_secondary_indexes: None,
                local_secondary_indexes: None,
                stream_specification,
                tags: None,
                deletion_protection_enabled: None,
                sse_specification: None,
                table_class: None,
                on_demand_throughput: None,
                vector_indexes: None,
            };
            let description = self
                .create_table_for_restore_impl(&account_id, create_input)
                .await?;
            let key_info = TableKeyInfo {
                table_name: target_table_name.clone(),
                account_id: account_id.clone(),
                table_id: description.table_id.clone(),
                key_schema: key_schema.clone(),
                base_key_schema: key_schema,
                attribute_definitions,
                has_lsi: false,
                global_secondary_indexes: Vec::new(),
                local_secondary_indexes: Vec::new(),
                stream_specification: description.stream_specification.clone(),
                vector_indexes: Vec::new(),
            };

            let restore_result: Result<(i64, i64), StorageError> = async {
                let account_keyspace = self.account_keyspace(&account_id);
                let query = format!(
                    "SELECT item_data FROM {account_keyspace}.backup_items WHERE backup_arn = ? AND bucket = ?"
                );
                let mut restored_count = 0_i64;
                let mut restored_size = 0_i64;
                for bucket in 0..bucket_count(backup.item_count) {
                    #[allow(clippy::cast_possible_truncation)]
                    let bucket_i32 = bucket as i32;
                    let rows = self
                        .session
                        .query_with_values(
                            &query,
                            cdrs_tokio::query_values!(backup_arn.as_str(), bucket_i32),
                        )
                        .await
                        .map_err(|e| StorageError::Internal(format!("Read backup payload: {e}")))?
                        .response_body()
                        .map_err(|e| StorageError::Internal(format!("Parse backup payload: {e}")))?
                        .into_rows()
                        .unwrap_or_default();
                    for row in rows {
                        let item_data: String = row.get_r_by_name("item_data").map_err(|e| {
                            StorageError::Internal(format!("Parse backup item: {e}"))
                        })?;
                        let item = serde_json::from_str(&item_data).map_err(|e| {
                            StorageError::Internal(format!("Deserialize backup item: {e}"))
                        })?;
                        self.put_item_impl(&key_info, item, false, None, &Default::default(), None)
                            .await?;
                        restored_count += 1;
                        #[allow(clippy::cast_possible_wrap)]
                        let item_len = item_data.len() as i64;
                        restored_size = restored_size.saturating_add(item_len);
                    }
                }
                if restored_count != backup.item_count {
                    return Err(StorageError::Internal(format!(
                        "Backup item count mismatch: expected {}, restored {}",
                        backup.item_count, restored_count
                    )));
                }
                Ok((restored_count, restored_size))
            }
            .await;

            let completion = match restore_result {
                Ok((restored_count, restored_size)) => {
                    self.mark_table_active_after_restore(
                        &account_id,
                        &target_table_name,
                        &description.table_id,
                        restored_count,
                        backup.backup_size_bytes.max(restored_size),
                    )
                    .await
                }
                Err(error) => Err(error),
            };

            match completion {
                Ok(()) => Ok(description),
                Err(error) => {
                    let _ = self
                        .delete_table_impl(
                            &account_id,
                            extenddb_core::types::DeleteTableInput {
                                table_name: target_table_name,
                            },
                        )
                        .await;
                    Err(error)
                }
            }
        })
    }

    fn describe_continuous_backups(
        &self,
        account_id: &str,
        table_name: &str,
    ) -> BoxFuture<'_, Result<ContinuousBackupsDescription, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            if !self
                .table_exists_for_backup(&account_id, &table_name)
                .await?
            {
                return Err(StorageError::TableNotFound(format!(
                    "Table not found: {table_name}"
                )));
            }
            let query = format!(
                "SELECT pitr_enabled FROM {}.continuous_backups WHERE account_id = ? AND table_name = ?",
                self.catalog_keyspace()
            );
            let rows = self
                .session
                .query_with_values(
                    &query,
                    cdrs_tokio::query_values!(account_id.as_str(), table_name.as_str()),
                )
                .await
                .map_err(|e| StorageError::Internal(format!("Query continuous backups: {e}")))?
                .response_body()
                .map_err(|e| StorageError::Internal(format!("Parse continuous backups: {e}")))?
                .into_rows()
                .unwrap_or_default();
            let pitr_enabled = rows
                .first()
                .and_then(|row| {
                    let value: Result<bool, _> = row.get_r_by_name("pitr_enabled");
                    value.ok()
                })
                .unwrap_or(false);
            #[allow(clippy::cast_precision_loss)]
            let now = epoch_millis() as f64 / 1_000.0;
            Ok(ContinuousBackupsDescription {
                continuous_backups_status: "ENABLED".to_owned(),
                point_in_time_recovery_description: Some(PointInTimeRecoveryDescription {
                    point_in_time_recovery_status: if pitr_enabled {
                        "ENABLED".to_owned()
                    } else {
                        "DISABLED".to_owned()
                    },
                    earliest_restorable_date_time: pitr_enabled
                        .then_some(now - 35.0 * 24.0 * 3_600.0),
                    latest_restorable_date_time: pitr_enabled.then_some(now),
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
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            if !self
                .table_exists_for_backup(&account_id, &table_name)
                .await?
            {
                return Err(StorageError::TableNotFound(format!(
                    "Table not found: {table_name}"
                )));
            }
            let query = format!(
                "INSERT INTO {}.continuous_backups (account_id, table_name, pitr_enabled) VALUES (?, ?, ?)",
                self.catalog_keyspace()
            );
            self.session
                .query_with_values(
                    &query,
                    cdrs_tokio::query_values!(
                        account_id.as_str(),
                        table_name.as_str(),
                        pitr_enabled
                    ),
                )
                .await
                .map_err(|e| StorageError::Internal(format!("Update continuous backups: {e}")))?;
            self.describe_continuous_backups(&account_id, &table_name)
                .await
        })
    }

    fn restore_table_to_point_in_time(
        &self,
        _account_id: &str,
        _source_table_name: &str,
        _target_table_name: &str,
    ) -> BoxFuture<'_, Result<TableDescription, StorageError>> {
        Box::pin(async move {
            Err(StorageError::Validation(
                "Point-in-time recovery restore is not yet supported".to_owned(),
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_identifier_has_timestamp_and_random_suffix() {
        let id = backup_id();
        let (timestamp, suffix) = id.split_once('-').expect("backup id separator");
        assert!(!timestamp.is_empty());
        assert!(timestamp.chars().all(|ch| ch.is_ascii_digit()));
        assert_eq!(suffix.len(), 8);
        assert!(suffix.chars().all(|ch| ch.is_ascii_hexdigit()));
    }

    #[test]
    fn item_buckets_are_bounded() {
        assert_eq!(bucket_count(0), 0);
        assert_eq!(bucket_count(1), 1);
        assert_eq!(bucket_count(BACKUP_ITEMS_PER_BUCKET), 1);
        assert_eq!(bucket_count(BACKUP_ITEMS_PER_BUCKET + 1), 2);
    }
}
