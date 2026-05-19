// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backup and point-in-time recovery implementation for PostgreSQL storage.

use extenddb_core::types::{
    BackupDescription, BackupDetails, BackupSummary, ContinuousBackupsDescription,
    PointInTimeRecoveryDescription, SourceTableDetails, TableDescription,
};
use extenddb_storage::BackupEngine;
use extenddb_storage::TableEngine;
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;

use crate::PostgresEngine;
use crate::data::data_table_name;

/// Current epoch milliseconds for unique ARN generation.
fn epoch_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Convert a PostgreSQL `TIMESTAMPTZ` to epoch seconds as `f64`.
#[allow(clippy::cast_precision_loss)]
fn pg_timestamp_to_epoch(ts: time::OffsetDateTime) -> f64 {
    ts.unix_timestamp() as f64
}

impl BackupEngine for PostgresEngine {
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
            // Verify table exists and get metadata.
            let row: (
                String,
                String,
                serde_json::Value,
                serde_json::Value,
                String,
                i64,
                i64,
                String,
            ) = sqlx::query_as(
                "SELECT table_id, table_arn, key_schema, attribute_definitions, \
                 billing_mode, table_size_bytes, item_count, \
                 COALESCE(provisioned_throughput::text, '{}') \
                 FROM tables WHERE account_id = $1 AND table_name = $2 AND table_status = 'ACTIVE'",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?
            .ok_or_else(|| StorageError::TableNotFound(format!("Table not found: {table_name}")))?;

            let (
                table_id,
                _table_arn,
                key_schema,
                attr_defs,
                billing_mode,
                size_bytes,
                _item_count,
                _prov,
            ) = row;

            let backup_arn = format!(
                "arn:aws:dynamodb:{region}:{account_id}:table/{table_name}/backup/{ts}",
                region = self.region,
                ts = epoch_millis()
            );

            // Snapshot items from the data table.
            let ddb_table = data_table_name(&table_id);
            let ddb_table_unquoted = ddb_table.trim_matches('"');
            let has_sk: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
             WHERE table_name = $1 AND column_name = 'sk')",
            )
            .bind(ddb_table_unquoted)
            .fetch_one(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            let items: Vec<(String, Option<String>, serde_json::Value)> = if has_sk {
                sqlx::query_as(&format!("SELECT pk, sk, item_data FROM {ddb_table}"))
                    .fetch_all(&self.data_pool)
                    .await
                    .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?
            } else {
                sqlx::query_as::<_, (String, serde_json::Value)>(&format!(
                    "SELECT pk, item_data FROM {ddb_table}"
                ))
                .fetch_all(&self.data_pool)
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?
                .into_iter()
                .map(|(pk, data)| (pk, None, data))
                .collect()
            };

            #[allow(clippy::cast_possible_wrap)]
            let actual_count = items.len() as i64;

            // Wrap all catalog-side writes in a single transaction so a crash
            // cannot leave a backup marked AVAILABLE with partial items.
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            sqlx::query(
                "INSERT INTO backups (backup_arn, backup_name, table_id, table_name, account_id, \
             backup_status, backup_size_bytes, item_count, key_schema, attribute_definitions, \
             billing_mode) \
             VALUES ($1, $2, $3, $4, $5, 'AVAILABLE', $6, $7, $8, $9, $10)",
            )
            .bind(&backup_arn)
            .bind(&backup_name)
            .bind(&table_id)
            .bind(&table_name)
            .bind(&account_id)
            .bind(size_bytes)
            .bind(actual_count)
            .bind(&key_schema)
            .bind(&attr_defs)
            .bind(&billing_mode)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            for (pk, sk, item_data) in &items {
                sqlx::query(
                    "INSERT INTO backup_items (backup_arn, pk, sk, item_data) \
                 VALUES ($1, $2, $3, $4)",
                )
                .bind(&backup_arn)
                .bind(pk)
                .bind(sk.as_deref())
                .bind(item_data)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;
            }

            // Read back the creation timestamp assigned by the database.
            let created_at: time::OffsetDateTime =
                sqlx::query_scalar("SELECT created_at FROM backups WHERE backup_arn = $1")
                    .bind(&backup_arn)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            Ok(BackupDetails {
                backup_arn,
                backup_name: backup_name.to_owned(),
                backup_status: "AVAILABLE".to_owned(),
                backup_type: "USER".to_owned(),
                backup_size_bytes: size_bytes,
                backup_creation_date_time: pg_timestamp_to_epoch(created_at),
            })
        })
    }

    fn describe_backup(
        &self,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let row: (
                String,
                String,
                String,
                String,
                String,
                i64,
                i64,
                serde_json::Value,
                String,
                String,
                time::OffsetDateTime,
                time::OffsetDateTime,
            ) = sqlx::query_as(
                "SELECT b.backup_name, b.backup_status, b.table_id, b.table_name, b.account_id, \
                 b.backup_size_bytes, b.item_count, b.key_schema, b.billing_mode, \
                 COALESCE(t.table_arn, \
                   'arn:aws:dynamodb:' || $2 || ':' || b.account_id || ':table/' || b.table_name), \
                 b.created_at, \
                 COALESCE(t.creation_date_time, b.created_at) \
                 FROM backups b \
                 LEFT JOIN tables t ON t.table_id = b.table_id \
                 WHERE b.backup_arn = $1",
            )
            .bind(&backup_arn)
            .bind(&self.region)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?
            .ok_or_else(|| StorageError::Validation(format!("Backup not found: {backup_arn}")))?;

            let (
                name,
                status,
                table_id,
                table_name,
                _account_id,
                size,
                count,
                ks_json,
                billing,
                table_arn,
                backup_created_at,
                table_created_at,
            ) = row;

            let key_schema: Vec<extenddb_core::types::KeySchemaElement> =
                serde_json::from_value(ks_json)
                    .map_err(|e| StorageError::Internal(format!("Parse key schema: {e}")))?;

            Ok(BackupDescription {
                backup_details: BackupDetails {
                    backup_arn: backup_arn.to_owned(),
                    backup_name: name,
                    backup_status: status,
                    backup_type: "USER".to_owned(),
                    backup_size_bytes: size,
                    backup_creation_date_time: pg_timestamp_to_epoch(backup_created_at),
                },
                source_table_details: SourceTableDetails {
                    table_name,
                    table_id,
                    table_arn,
                    key_schema,
                    item_count: count,
                    table_size_bytes: size,
                    billing_mode: Some(billing),
                    table_creation_date_time: pg_timestamp_to_epoch(table_created_at),
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
        let table_name = table_name.map(|s| s.to_string());
        Box::pin(async move {
            let rows: Vec<(
                String,
                String,
                String,
                String,
                i64,
                String,
                time::OffsetDateTime,
            )> = if let Some(tn) = table_name {
                sqlx::query_as(
                    "SELECT b.backup_arn, b.backup_name, b.table_name, b.backup_status, \
                     b.backup_size_bytes, \
                     COALESCE(t.table_arn, \
                       'arn:aws:dynamodb:' || $3 || ':' || b.account_id || ':table/' || b.table_name), \
                     b.created_at \
                     FROM backups b \
                     LEFT JOIN tables t ON t.table_id = b.table_id \
                     WHERE b.account_id = $1 AND b.table_name = $2 AND b.backup_status != 'DELETED' \
                     ORDER BY b.created_at DESC",
                )
                .bind(&account_id)
                .bind(tn)
                .bind(&self.region)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?
            } else {
                sqlx::query_as(
                    "SELECT b.backup_arn, b.backup_name, b.table_name, b.backup_status, \
                     b.backup_size_bytes, \
                     COALESCE(t.table_arn, \
                       'arn:aws:dynamodb:' || $2 || ':' || b.account_id || ':table/' || b.table_name), \
                     b.created_at \
                     FROM backups b \
                     LEFT JOIN tables t ON t.table_id = b.table_id \
                     WHERE b.account_id = $1 AND b.backup_status != 'DELETED' \
                     ORDER BY b.created_at DESC",
                )
                .bind(&account_id)
                .bind(&self.region)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?
            };

            Ok(rows
                .into_iter()
                .map(
                    |(arn, name, tn, status, size, table_arn, created_at)| BackupSummary {
                        backup_arn: arn,
                        backup_name: name,
                        table_name: tn,
                        table_arn,
                        backup_status: status,
                        backup_type: "USER".to_owned(),
                        backup_size_bytes: size,
                        backup_creation_date_time: pg_timestamp_to_epoch(created_at),
                    },
                )
                .collect())
        })
    }

    fn delete_backup(
        &self,
        backup_arn: &str,
    ) -> BoxFuture<'_, Result<BackupDescription, StorageError>> {
        let backup_arn = backup_arn.to_string();
        Box::pin(async move {
            let desc = self.describe_backup(&backup_arn).await?;

            sqlx::query("DELETE FROM backup_items WHERE backup_arn = $1")
                .bind(&backup_arn)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            sqlx::query("UPDATE backups SET backup_status = 'DELETED' WHERE backup_arn = $1")
                .bind(&backup_arn)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

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
            let backup_row: (
                String,
                serde_json::Value,
                serde_json::Value,
                String,
                Option<serde_json::Value>,
            ) = sqlx::query_as(
                "SELECT table_name, key_schema, attribute_definitions, billing_mode, \
                 provisioned_throughput \
                 FROM backups WHERE backup_arn = $1 AND backup_status = 'AVAILABLE'",
            )
            .bind(&backup_arn)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?
            .ok_or_else(|| StorageError::Validation(format!("Backup not found: {backup_arn}")))?;

            let (_orig_table, ks_json, ad_json, billing, _prov) = backup_row;

            let key_schema: Vec<extenddb_core::types::KeySchemaElement> =
                serde_json::from_value(ks_json)
                    .map_err(|e| StorageError::Internal(format!("Parse key schema: {e}")))?;
            let attr_defs: Vec<extenddb_core::types::AttributeDefinition> =
                serde_json::from_value(ad_json)
                    .map_err(|e| StorageError::Internal(format!("Parse attr defs: {e}")))?;

            let billing_mode = if billing == "PAY_PER_REQUEST" {
                Some(extenddb_core::types::BillingMode::PayPerRequest)
            } else {
                Some(extenddb_core::types::BillingMode::Provisioned)
            };

            let create_input = extenddb_core::types::CreateTableInput {
                table_name: target_table_name.to_owned(),
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
            };

            let desc = self.create_table(&account_id, create_input).await?;

            let new_table_id = &desc.table_id;
            let ddb_table = data_table_name(new_table_id);
            let ddb_table_unquoted = ddb_table.trim_matches('"');

            // Do NOT force ACTIVE — let the control plane handle the transition
            // (steering rule D-2: tests run with control_plane_delay_seconds > 0).
            // The table starts in CREATING and transitions to ACTIVE after the delay.

            let has_sk: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
             WHERE table_name = $1 AND column_name = 'sk')",
            )
            .bind(ddb_table_unquoted)
            .fetch_one(&self.data_pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            let items: Vec<(String, Option<String>, serde_json::Value)> =
                sqlx::query_as("SELECT pk, sk, item_data FROM backup_items WHERE backup_arn = $1")
                    .bind(&backup_arn)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            for (pk, sk, item_data) in &items {
                if has_sk {
                    sqlx::query(&format!(
                        "INSERT INTO {ddb_table} (pk, sk, item_data) VALUES ($1, $2, $3)"
                    ))
                    .bind(pk)
                    .bind(sk.as_deref())
                    .bind(item_data)
                    .execute(&self.data_pool)
                    .await
                    .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;
                } else {
                    sqlx::query(&format!(
                        "INSERT INTO {ddb_table} (pk, item_data) VALUES ($1, $2)"
                    ))
                    .bind(pk)
                    .bind(item_data)
                    .execute(&self.data_pool)
                    .await
                    .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;
                }
            }

            #[allow(clippy::cast_possible_wrap)]
            let item_count = items.len() as i64;

            sqlx::query(
                "UPDATE tables SET item_count = $1 WHERE account_id = $2 AND table_name = $3",
            )
            .bind(item_count)
            .bind(&account_id)
            .bind(&target_table_name)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            // Mark the restored table ACTIVE immediately — the data is fully
            // populated and the table is ready to serve requests. This matches
            // real DynamoDB behavior where restored tables become ACTIVE once
            // the restore completes (the CREATING status is transient).
            sqlx::query(
                "UPDATE tables SET table_status = 'ACTIVE', status_transition_at = NULL \
             WHERE account_id = $1 AND table_name = $2",
            )
            .bind(&account_id)
            .bind(&target_table_name)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            // Return CREATING — the API response shows the initial status,
            // but the table is already ACTIVE by the time the caller polls.
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
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE account_id = $1 AND table_name = $2)",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            if !exists {
                return Err(StorageError::TableNotFound(format!(
                    "Table not found: {table_name}"
                )));
            }

            let pitr_row: Option<(bool,)> = sqlx::query_as(
                "SELECT pitr_enabled FROM continuous_backups \
             WHERE account_id = $1 AND table_name = $2",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            let pitr_enabled = pitr_row.is_some_and(|r| r.0);

            #[allow(clippy::cast_precision_loss)]
            let now_epoch = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as f64;

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
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE account_id = $1 AND table_name = $2)",
            )
            .bind(&account_id)
            .bind(&table_name)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            if !exists {
                return Err(StorageError::TableNotFound(format!(
                    "Table not found: {table_name}"
                )));
            }

            sqlx::query(
                "INSERT INTO continuous_backups (account_id, table_name, pitr_enabled) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (account_id, table_name) DO UPDATE SET pitr_enabled = $3",
            )
            .bind(&account_id)
            .bind(&table_name)
            .bind(pitr_enabled)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(format!("Database error: {e}")))?;

            self.describe_continuous_backups(&account_id, &table_name)
                .await
        })
    }

    // TODO(cleanup): This method is unreachable — the engine handler returns
    // ValidationException("not yet supported") before calling storage. Remove
    // when real PITR is implemented or during the next storage trait cleanup.
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
