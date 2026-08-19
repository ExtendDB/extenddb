// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BackupEngine` for the SQLite backend.
//!
//! A backup snapshots every item's `item_data` into `backup_items`. Restore
//! recreates the table via `create_table` and upserts the snapshot under the
//! engine write lock. `RestoreTableToPointInTime` is implemented as a
//! snapshot-then-restore (then discard the temporary backup), matching the
//! PostgreSQL backend's behaviour.

use extenddb_core::types::{
    AttributeDefinition, BackupDescription, BackupDetails, BackupSummary, BillingMode,
    ContinuousBackupsDescription, CreateTableInput, KeySchemaElement,
    PointInTimeRecoveryDescription, ProvisionedThroughput, SourceTableDetails, TableDescription,
    TableKeyInfo,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BackupEngine, TableEngine};
use futures::future::BoxFuture;

use crate::data::{data_table_name, upsert_item_in_tx};
use crate::sqlite_util::parse_timestamp;
use crate::store::SqliteEngine;

fn epoch_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Backup id: creation timestamp plus an 8-hex random suffix, matching the
/// PostgreSQL backend. The suffix makes ARNs non-guessable and prevents two
/// backups created in the same millisecond from colliding.
fn backup_id() -> String {
    use rand::Rng;
    let suffix: u32 = rand::rng().random();
    format!("{ts}-{suffix:08x}", ts = epoch_millis())
}

#[allow(clippy::cast_precision_loss)]
fn ts_to_epoch(s: &str) -> f64 {
    parse_timestamp(s)
        .map(|dt| dt.unix_timestamp() as f64)
        .unwrap_or(0.0)
}

impl BackupEngine for SqliteEngine {
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
            let row: Option<(String, String, String, String, i64)> = sqlx::query_as(
                "SELECT table_id, key_schema, attribute_definitions, billing_mode, table_size_bytes \
                 FROM tables WHERE account_id = ? AND table_name = ? AND table_status = 'ACTIVE'",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (table_id, key_schema, attr_defs, billing_mode, size_bytes) =
                row.ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;

            let backup_arn = format!(
                "arn:aws:dynamodb:{region}:{account_id}:table/{table_name}/backup/{id}",
                region = self.region,
                id = backup_id()
            );

            let ddb_table = data_table_name(&table_id);
            let items: Vec<(String,)> =
                sqlx::query_as(&format!("SELECT item_data FROM {ddb_table}"))
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            let item_count = i64::try_from(items.len()).unwrap_or(i64::MAX);

            let _writer = self.write_lock.lock().await;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            sqlx::query(
                "INSERT INTO backups (backup_arn, backup_name, table_id, table_name, account_id, \
                 backup_status, backup_size_bytes, item_count, key_schema, attribute_definitions, \
                 billing_mode) VALUES (?, ?, ?, ?, ?, 'AVAILABLE', ?, ?, ?, ?, ?)",
            )
            .bind(&backup_arn)
            .bind(&backup_name)
            .bind(&table_id)
            .bind(&table_name)
            .bind(&account_id)
            .bind(size_bytes)
            .bind(item_count)
            .bind(&key_schema)
            .bind(&attr_defs)
            .bind(&billing_mode)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            for (item_data,) in &items {
                sqlx::query(
                    "INSERT INTO backup_items (backup_arn, pk, sk, item_data) VALUES (?, '', NULL, ?)",
                )
                .bind(&backup_arn)
                .bind(item_data)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            }

            let created_at: (String,) =
                sqlx::query_as("SELECT created_at FROM backups WHERE backup_arn = ?")
                    .bind(&backup_arn)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(BackupDetails {
                backup_arn,
                backup_name,
                backup_status: "AVAILABLE".to_owned(),
                backup_type: "USER".to_owned(),
                backup_size_bytes: size_bytes,
                backup_creation_date_time: ts_to_epoch(&created_at.0),
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
            #[allow(clippy::type_complexity)]
            let row: Option<(String, String, String, String, i64, i64, String, String, String, String)> =
                sqlx::query_as(
                    "SELECT b.backup_name, b.backup_status, b.table_id, b.table_name, \
                     b.backup_size_bytes, b.item_count, b.key_schema, b.billing_mode, \
                     COALESCE(t.table_arn, \
                       'arn:aws:dynamodb:' || ? || ':' || b.account_id || ':table/' || b.table_name), \
                     b.created_at \
                     FROM backups b LEFT JOIN tables t ON t.table_id = b.table_id \
                     WHERE b.backup_arn = ? AND b.account_id = ? \
                     AND b.backup_status != 'DELETED'",
                )
                .bind(&self.region)
                .bind(&backup_arn)
                .bind(&account_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (name, status, table_id, table_name, size, count, ks, billing, table_arn, created) =
                row.ok_or_else(|| {
                    StorageError::Validation(format!("Backup not found: {backup_arn}"))
                })?;

            let key_schema: Vec<KeySchemaElement> =
                serde_json::from_str(&ks).map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(BackupDescription {
                backup_details: BackupDetails {
                    backup_arn: backup_arn.clone(),
                    backup_name: name,
                    backup_status: status,
                    backup_type: "USER".to_owned(),
                    backup_size_bytes: size,
                    backup_creation_date_time: ts_to_epoch(&created),
                },
                source_table_details: SourceTableDetails {
                    table_name,
                    table_id,
                    table_arn,
                    key_schema,
                    item_count: count,
                    table_size_bytes: size,
                    billing_mode: Some(billing),
                    table_creation_date_time: ts_to_epoch(&created),
                },
            })
        })
    }

    fn list_backups(
        &self,
        account_id: &str,
        table_name: Option<&str>,
    ) -> BoxFuture<'_, Result<Vec<BackupSummary>, StorageError>> {
        let account_id = account_id.to_owned();
        let table_name = table_name.map(str::to_owned);
        Box::pin(async move {
            let rows: Vec<(String, String, String, String, i64, String, String)> =
                if let Some(tn) = table_name {
                    sqlx::query_as(
                        "SELECT b.backup_arn, b.backup_name, b.table_name, b.backup_status, \
                         b.backup_size_bytes, \
                         COALESCE(t.table_arn, 'arn:aws:dynamodb:' || ? || ':' || b.account_id || ':table/' || b.table_name), \
                         b.created_at FROM backups b LEFT JOIN tables t ON t.table_id = b.table_id \
                         WHERE b.account_id = ? AND b.table_name = ? AND b.backup_status != 'DELETED' \
                         ORDER BY b.created_at DESC",
                    )
                    .bind(&self.region)
                    .bind(&account_id)
                    .bind(tn)
                    .fetch_all(&self.pool)
                    .await
                } else {
                    sqlx::query_as(
                        "SELECT b.backup_arn, b.backup_name, b.table_name, b.backup_status, \
                         b.backup_size_bytes, \
                         COALESCE(t.table_arn, 'arn:aws:dynamodb:' || ? || ':' || b.account_id || ':table/' || b.table_name), \
                         b.created_at FROM backups b LEFT JOIN tables t ON t.table_id = b.table_id \
                         WHERE b.account_id = ? AND b.backup_status != 'DELETED' \
                         ORDER BY b.created_at DESC",
                    )
                    .bind(&self.region)
                    .bind(&account_id)
                    .fetch_all(&self.pool)
                    .await
                }
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            Ok(rows
                .into_iter()
                .map(
                    |(arn, name, tn, status, size, table_arn, created)| BackupSummary {
                        backup_arn: arn,
                        backup_name: name,
                        table_name: tn,
                        table_arn,
                        backup_status: status,
                        backup_type: "USER".to_owned(),
                        backup_size_bytes: size,
                        backup_creation_date_time: ts_to_epoch(&created),
                    },
                )
                .collect())
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
            // Resolves account-scoped, so a backup owned by another account is
            // reported missing here and the writes below never run.
            let desc = self.describe_backup(&account_id, &backup_arn).await?;

            // The account predicate is repeated on both writes rather than
            // relying on the lookup above, so the statements are correct on
            // their own terms.
            sqlx::query(
                "DELETE FROM backup_items WHERE backup_arn = ?1 AND EXISTS (\
                 SELECT 1 FROM backups b WHERE b.backup_arn = ?1 AND b.account_id = ?2)",
            )
            .bind(&backup_arn)
            .bind(&account_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query(
                "UPDATE backups SET backup_status = 'DELETED' \
                 WHERE backup_arn = ? AND account_id = ?",
            )
            .bind(&backup_arn)
            .bind(&account_id)
            .execute(&self.pool)
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
        let account_id = account_id.to_owned();
        let target_table_name = target_table_name.to_owned();
        let backup_arn = backup_arn.to_owned();
        Box::pin(async move {
            let row: Option<(String, String, String)> = sqlx::query_as(
                "SELECT key_schema, attribute_definitions, billing_mode \
                 FROM backups WHERE backup_arn = ? AND account_id = ? \
                 AND backup_status = 'AVAILABLE'",
            )
            .bind(&backup_arn)
            .bind(&account_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            let (ks, ad, billing) = row.ok_or_else(|| {
                StorageError::Validation(format!("Backup not found: {backup_arn}"))
            })?;
            let key_schema: Vec<KeySchemaElement> =
                serde_json::from_str(&ks).map_err(|e| StorageError::Internal(e.to_string()))?;
            let attr_defs: Vec<AttributeDefinition> =
                serde_json::from_str(&ad).map_err(|e| StorageError::Internal(e.to_string()))?;
            let billing_mode = Some(if billing == "PAY_PER_REQUEST" {
                BillingMode::PayPerRequest
            } else {
                BillingMode::Provisioned
            });

            let create_input = CreateTableInput {
                table_name: target_table_name.clone(),
                key_schema: key_schema.clone(),
                attribute_definitions: attr_defs.clone(),
                billing_mode,
                provisioned_throughput: Some(ProvisionedThroughput {
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
                ..Default::default()
            };

            let desc = self.create_table(&account_id, create_input).await?;
            let key_info = TableKeyInfo {
                table_name: target_table_name.clone(),
                account_id: account_id.clone(),
                table_id: desc.table_id.clone(),
                base_key_schema: key_schema.clone(),
                key_schema,
                attribute_definitions: attr_defs,
                has_lsi: false,
                // The restored table is created above without secondary
                // indexes, so there is no index metadata to carry.
                global_secondary_indexes: Vec::new(),
                local_secondary_indexes: Vec::new(),
                stream_specification: None,
                ..Default::default()
            };

            let items: Vec<(String,)> =
                sqlx::query_as("SELECT item_data FROM backup_items WHERE backup_arn = ?")
                    .bind(&backup_arn)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            let item_count = i64::try_from(items.len()).unwrap_or(i64::MAX);

            let _writer = self.write_lock.lock().await;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            for (item_json,) in &items {
                let item: extenddb_core::types::Item = serde_json::from_str(item_json)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                upsert_item_in_tx(&mut tx, &key_info, &item).await?;
            }
            sqlx::query("UPDATE tables SET item_count = ? WHERE account_id = ? AND table_name = ?")
                .bind(item_count)
                .bind(&account_id)
                .bind(&target_table_name)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            // Mark the restored table ACTIVE immediately: the data is fully
            // populated and ready to serve. This matches the Postgres backend
            // and real DynamoDB, where a restored table becomes ACTIVE once the
            // restore completes (the CREATING status is transient) rather than
            // waiting for the control-plane transition poller.
            sqlx::query(
                "UPDATE tables SET table_status = 'ACTIVE', status_transition_at = NULL \
                 WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&target_table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            tx.commit()
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
        let account_id = account_id.to_owned();
        let table_name = table_name.to_owned();
        Box::pin(async move {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE account_id = ? AND table_name = ?)",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            if !exists {
                return Err(StorageError::TableNotFound(table_name));
            }

            let pitr: Option<(bool,)> = sqlx::query_as(
                "SELECT pitr_enabled FROM continuous_backups WHERE account_id = ? AND table_name = ?",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            let enabled = pitr.is_some_and(|r| r.0);

            #[allow(clippy::cast_precision_loss)]
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as f64;

            Ok(ContinuousBackupsDescription {
                continuous_backups_status: "ENABLED".to_owned(),
                point_in_time_recovery_description: Some(PointInTimeRecoveryDescription {
                    point_in_time_recovery_status: if enabled { "ENABLED" } else { "DISABLED" }
                        .to_owned(),
                    earliest_restorable_date_time: enabled.then_some(now - 35.0 * 24.0 * 3600.0),
                    latest_restorable_date_time: enabled.then_some(now),
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
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE account_id = ? AND table_name = ?)",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            if !exists {
                return Err(StorageError::TableNotFound(table_name));
            }
            sqlx::query(
                "INSERT INTO continuous_backups (account_id, table_name, pitr_enabled) \
                 VALUES (?, ?, ?) \
                 ON CONFLICT (account_id, table_name) DO UPDATE SET pitr_enabled = excluded.pitr_enabled",
            )
            .bind(&account_id)
            .bind(&table_name)
            .bind(pitr_enabled)
            .execute(&self.pool)
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
        let account_id = account_id.to_owned();
        let source_table_name = source_table_name.to_owned();
        let target_table_name = target_table_name.to_owned();
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
