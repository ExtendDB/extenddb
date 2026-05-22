// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_table` implementation for `SqliteEngine`.

use extenddb_core::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, TableDescription, UpdateTableInput,
};
use extenddb_storage::error::StorageError;

use crate::engine::SqliteEngine;

impl SqliteEngine {
    pub(crate) async fn update_table_impl(
        &self,
        account_id: &str,
        input: UpdateTableInput,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row: Option<(String, String, String, String)> = sqlx::query_as(
            "SELECT table_status, table_id, key_schema, attribute_definitions \
             FROM tables WHERE account_id = ? AND table_name = ?",
        )
        .bind(account_id)
        .bind(&input.table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (status, table_id, ks_str, ad_str) =
            row.ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;
        if status != "ACTIVE" {
            return Err(StorageError::TableNotActive(input.table_name.clone()));
        }

        // No-op rejection for same PROVISIONED billing mode/throughput.
        if matches!(input.billing_mode, Some(BillingMode::Provisioned)) {
            if let Some(ref pt) = input.provisioned_throughput {
                let current_row: Option<(Option<String>, Option<String>)> = sqlx::query_as(
                    "SELECT billing_mode, provisioned_throughput FROM tables \
                     WHERE account_id = ? AND table_name = ?",
                )
                .bind(account_id)
                .bind(&input.table_name)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

                if let Some((current_bm, current_pt_opt)) = current_row {
                    let current_pt_val: serde_json::Value = current_pt_opt
                        .as_deref()
                        .and_then(|s| serde_json::from_str(s).ok())
                        .unwrap_or(serde_json::Value::Object(Default::default()));
                    let is_provisioned =
                        current_bm.as_deref() == Some("PROVISIONED") || current_bm.is_none();
                    let current_rcu = current_pt_val
                        .get("ReadCapacityUnits")
                        .or_else(|| current_pt_val.get("read_capacity_units"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);
                    let current_wcu = current_pt_val
                        .get("WriteCapacityUnits")
                        .or_else(|| current_pt_val.get("write_capacity_units"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);

                    if is_provisioned
                        && current_rcu == pt.read_capacity_units
                        && current_wcu == pt.write_capacity_units
                    {
                        return Err(StorageError::NoOpUpdate(format!(
                            "The provisioned throughput for the table will not change. \
                             The requested value equals the current value. \
                             Current ReadCapacityUnits provisioned for the table: {}. \
                             Requested ReadCapacityUnits: {}. \
                             Current WriteCapacityUnits provisioned for the table: {}. \
                             Requested WriteCapacityUnits: {}.",
                            current_rcu,
                            pt.read_capacity_units,
                            current_wcu,
                            pt.write_capacity_units
                        )));
                    }
                }
            }
        }

        // Apply billing mode change.
        if let Some(bm) = &input.billing_mode {
            let bm_str = match bm {
                BillingMode::Provisioned => "PROVISIONED",
                BillingMode::PayPerRequest => "PAY_PER_REQUEST",
            };
            sqlx::query(
                "UPDATE tables SET billing_mode = ? WHERE account_id = ? AND table_name = ?",
            )
            .bind(bm_str)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply provisioned throughput change.
        if let Some(pt) = &input.provisioned_throughput {
            let pt_json = serde_json::to_string(pt)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query(
                "UPDATE tables SET provisioned_throughput = ? \
                 WHERE account_id = ? AND table_name = ?",
            )
            .bind(&pt_json)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply deletion protection change.
        if let Some(dp) = input.deletion_protection_enabled {
            sqlx::query(
                "UPDATE tables SET deletion_protection_enabled = ? \
                 WHERE account_id = ? AND table_name = ?",
            )
            .bind(dp)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply stream specification change.
        if let Some(spec) = &input.stream_specification {
            let spec_json = serde_json::to_string(spec)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query(
                "UPDATE tables SET stream_specification = ? \
                 WHERE account_id = ? AND table_name = ?",
            )
            .bind(&spec_json)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            if spec.stream_enabled {
                let existing: Option<(String,)> = sqlx::query_as(
                    "SELECT shard_id FROM stream_shards WHERE table_id = ? LIMIT 1",
                )
                .bind(&table_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

                if existing.is_none() {
                    Self::init_stream_shards(
                        &mut tx,
                        account_id,
                        &input.table_name,
                        &table_id,
                    )
                    .await?;
                } else {
                    let current_label: Option<String> = sqlx::query_scalar(
                        "SELECT stream_label FROM tables \
                         WHERE account_id = ? AND table_name = ?",
                    )
                    .bind(account_id)
                    .bind(&input.table_name)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                    if current_label.is_none() {
                        sqlx::query(
                            "UPDATE tables \
                             SET stream_label = strftime('%Y-%m-%dT%H:%M:%S', 'now') \
                             WHERE account_id = ? AND table_name = ?",
                        )
                        .bind(account_id)
                        .bind(&input.table_name)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    }
                }
            }
        }

        // Apply GSI updates (create/delete).
        let mut created_index_ids: Vec<String> = Vec::new();
        let mut deleted_index_ids: Vec<String> = Vec::new();
        if let Some(updates) = &input.global_secondary_index_updates {
            for update in updates {
                if let Some(create) = &update.create {
                    let existing: Option<(String,)> = sqlx::query_as(
                        "SELECT index_name FROM indexes WHERE table_id = ? AND index_name = ?",
                    )
                    .bind(&table_id)
                    .bind(&create.index_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                    if existing.is_some() {
                        return Err(StorageError::IndexAlreadyExists(create.index_name.clone()));
                    }

                    let gsi_ks = serde_json::to_string(&create.key_schema)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let gsi_proj = serde_json::to_string(&create.projection)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let gsi_pt = create
                        .provisioned_throughput
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()
                        .map_err(|e| StorageError::Internal(e.to_string()))?;

                    let index_id = uuid::Uuid::new_v4().to_string();
                    sqlx::query(
                        "INSERT INTO indexes \
                          (table_id, index_name, index_id, index_type, key_schema, projection, \
                           index_status, provisioned_throughput) \
                         VALUES (?, ?, ?, 'GSI', ?, ?, 'ACTIVE', ?)",
                    )
                    .bind(&table_id)
                    .bind(&create.index_name)
                    .bind(&index_id)
                    .bind(&gsi_ks)
                    .bind(&gsi_proj)
                    .bind(&gsi_pt)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                    created_index_ids.push(index_id);
                }

                if let Some(delete) = &update.delete {
                    let existing: Option<(String, String)> = sqlx::query_as(
                        "SELECT index_name, index_id FROM indexes \
                         WHERE table_id = ? AND index_name = ?",
                    )
                    .bind(&table_id)
                    .bind(&delete.index_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                    let (_, del_index_id) = existing
                        .ok_or_else(|| StorageError::IndexNotFound(delete.index_name.clone()))?;

                    sqlx::query("DELETE FROM indexes WHERE table_id = ? AND index_name = ?")
                        .bind(&table_id)
                        .bind(&delete.index_name)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    deleted_index_ids.push(del_index_id);
                }
            }

            // Update attribute_definitions if new ones provided.
            if let Some(new_attr_defs) = &input.attribute_definitions {
                let ad_json = serde_json::to_string(new_attr_defs)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                sqlx::query(
                    "UPDATE tables SET attribute_definitions = ? \
                     WHERE account_id = ? AND table_name = ?",
                )
                .bind(&ad_json)
                .bind(account_id)
                .bind(&input.table_name)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Execute data DDL after catalog commit.
        if let Some(updates) = &input.global_secondary_index_updates {
            let base_key_schema: Vec<KeySchemaElement> = serde_json::from_str(&ks_str)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let base_attr_defs: Vec<AttributeDefinition> = serde_json::from_str(&ad_str)
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let effective_attr_defs = input
                .attribute_definitions
                .as_deref()
                .unwrap_or(&base_attr_defs);

            let mut create_idx = 0usize;
            let mut delete_idx = 0usize;
            for update in updates {
                if let Some(create) = &update.create {
                    let idx_id = &created_index_ids[create_idx];
                    create_idx += 1;
                    let data_result = async {
                        let mut data_tx = self
                            .pool
                            .begin()
                            .await
                            .map_err(|e| StorageError::Internal(e.to_string()))?;

                        Self::create_index_data_table(
                            &mut data_tx,
                            idx_id,
                            &create.key_schema,
                            effective_attr_defs,
                            &base_key_schema,
                            &base_attr_defs,
                        )
                        .await?;

                        Self::backfill_gsi(
                            &mut data_tx,
                            &table_id,
                            idx_id,
                            &create.key_schema,
                            effective_attr_defs,
                            &base_key_schema,
                            &base_attr_defs,
                            &create.projection,
                        )
                        .await?;

                        data_tx
                            .commit()
                            .await
                            .map_err(|e| StorageError::Internal(e.to_string()))?;
                        Ok::<(), StorageError>(())
                    }
                    .await;

                    if let Err(e) = data_result {
                        tracing::error!(
                            "Failed to create data table for GSI '{}' on '{}': {e}",
                            create.index_name,
                            input.table_name,
                        );
                        let _ = sqlx::query(
                            "DELETE FROM indexes WHERE table_id = ? AND index_name = ?",
                        )
                        .bind(&table_id)
                        .bind(&create.index_name)
                        .execute(&self.pool)
                        .await;
                        return Err(e);
                    }
                }

                if update.delete.is_some() {
                    let idx_id = &deleted_index_ids[delete_idx];
                    delete_idx += 1;
                    let idx_table = Self::index_table_name_static(idx_id);
                    if let Err(e) = sqlx::query(&format!("DROP TABLE IF EXISTS {idx_table}"))
                        .execute(&self.pool)
                        .await
                    {
                        tracing::warn!(
                            "Failed to drop data table for deleted GSI on '{}': {e}",
                            input.table_name,
                        );
                    }
                }
            }
        }

        self.build_table_description(account_id, &input.table_name)
            .await
    }
}
