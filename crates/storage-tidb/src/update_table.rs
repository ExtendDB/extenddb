// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `update_table` implementation for `TidbEngine`.

use extenddb_core::types::{
    BillingMode, ProvisionedThroughput, TableDescription, UpdateTableInput,
};
use extenddb_storage::error::StorageError;

use crate::TidbEngine;
use crate::throughput::provisioned_throughput_description;

impl TidbEngine {
    /// Core implementation of `update_table` (REQ-CTRL-003).
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

        // Lock the row and fetch the durable table id used by data artifacts.
        let row: Option<(String, String, Option<String>, bool)> = sqlx::query_as(
            "SELECT table_status, table_id, ttl_attribute, ttl_native_enabled FROM tables \
             WHERE account_id = ? AND table_name = ? FOR UPDATE",
        )
        .bind(account_id)
        .bind(&input.table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let (status, table_id, ttl_attribute, ttl_native_enabled) =
            row.ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;
        if status != "ACTIVE" {
            return Err(StorageError::TableNotActive(input.table_name.clone()));
        }
        let has_gsi_updates = input
            .global_secondary_index_updates
            .as_ref()
            .is_some_and(|updates| !updates.is_empty());
        let enables_stream = input
            .stream_specification
            .as_ref()
            .is_some_and(|spec| spec.stream_enabled);
        let changes_stream = input.stream_specification.is_some();
        let reconfigures_ttl = changes_stream && ttl_attribute.is_some();
        let has_control_plane_updates = has_gsi_updates || enables_stream || reconfigures_ttl;
        let must_disable_native_ttl_before_stream_visible = enables_stream && ttl_native_enabled;

        // No-op rejection: setting same billing mode to PROVISIONED with same
        // throughput values is rejected by DynamoDB. This check runs under the
        // FOR UPDATE lock to eliminate the TOCTOU race that existed when the
        // check was in the engine layer.
        if matches!(input.billing_mode, Some(BillingMode::Provisioned)) {
            if let Some(ref pt) = input.provisioned_throughput {
                let current_row: Option<(Option<String>, Option<serde_json::Value>)> =
                    sqlx::query_as(
                        "SELECT billing_mode, provisioned_throughput FROM tables \
                     WHERE account_id = ? AND table_name = ?",
                    )
                    .bind(account_id)
                    .bind(&input.table_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                if let Some((current_bm, current_pt_opt)) = current_row {
                    let is_provisioned =
                        current_bm.as_deref() == Some("PROVISIONED") || current_bm.is_none();
                    let current_pt: Option<ProvisionedThroughput> = current_pt_opt
                        .map(serde_json::from_value)
                        .transpose()
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let (current_rcu, current_wcu) = current_pt.as_ref().map_or((0, 0), |pt| {
                        (pt.read_capacity_units, pt.write_capacity_units)
                    });

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

        if has_control_plane_updates {
            sqlx::query(
                "UPDATE tables SET table_status = 'UPDATING', \
                    status_transition_at = CURRENT_TIMESTAMP(6) \
                 WHERE account_id = ? AND table_name = ?",
            )
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
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
            let pt_json =
                serde_json::to_value(pt).map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query("UPDATE tables SET provisioned_throughput = ? WHERE account_id = ? AND table_name = ?")
                .bind(&pt_json)
                .bind(account_id)
                .bind(&input.table_name)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply deletion protection change.
        if let Some(dp) = input.deletion_protection_enabled {
            sqlx::query("UPDATE tables SET deletion_protection_enabled = ? WHERE account_id = ? AND table_name = ?")
                .bind(dp)
                .bind(account_id)
                .bind(&input.table_name)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        // Apply GSI updates (create/delete).
        if let Some(updates) = &input.global_secondary_index_updates {
            for update in updates {
                if let Some(create) = &update.create {
                    // Check for duplicate index name.
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

                    let gsi_ks = serde_json::to_value(&create.key_schema)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let gsi_proj = serde_json::to_value(&create.projection)
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    let gsi_pt_description = create
                        .provisioned_throughput
                        .as_ref()
                        .map(provisioned_throughput_description);
                    let gsi_pt = gsi_pt_description
                        .as_ref()
                        .map(serde_json::to_value)
                        .transpose()
                        .map_err(|e| StorageError::Internal(e.to_string()))?;

                    let index_id = uuid::Uuid::new_v4().to_string();
                    sqlx::query(
                        r"INSERT INTO indexes
                           (table_id, index_name, index_id, index_type, key_schema, projection,
                            index_status, provisioned_throughput)
                           VALUES (?, ?, ?, 'GSI', ?, ?, 'CREATING', ?)",
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
                }

                if let Some(delete) = &update.delete {
                    // Verify the index exists and fetch its index_id.
                    let existing: Option<(String,)> = sqlx::query_as(
                        "SELECT index_name FROM indexes \
                         WHERE table_id = ? AND index_name = ? AND index_type = 'GSI' \
                           AND index_status = 'ACTIVE'",
                    )
                    .bind(&table_id)
                    .bind(&delete.index_name)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                    let (_existing_name,) = existing
                        .ok_or_else(|| StorageError::IndexNotFound(delete.index_name.clone()))?;

                    // Hide the index from read/write paths immediately. Metadata
                    // is deleted only after the TiDB data table is dropped.
                    sqlx::query(
                        "UPDATE indexes SET index_status = 'DELETING' \
                         WHERE table_id = ? AND index_name = ? AND index_type = 'GSI'",
                    )
                    .bind(&table_id)
                    .bind(&delete.index_name)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                }
            }

            // Update attribute_definitions on the table if new ones were provided.
            if let Some(new_attr_defs) = &input.attribute_definitions {
                let ad_json = serde_json::to_value(new_attr_defs)
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                sqlx::query("UPDATE tables SET attribute_definitions = ? WHERE account_id = ? AND table_name = ?")
                    .bind(&ad_json)
                    .bind(account_id)
                    .bind(&input.table_name)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        }

        // Apply stream specification after user-visible validation has passed.
        // Stream metadata must not become visible until the TiDB artifacts needed
        // to capture every write are already present.
        if let Some(spec) = &input.stream_specification {
            if spec.stream_enabled {
                Self::ensure_stream_shard_rows(&self.data_pool, &table_id).await?;
            }
            if must_disable_native_ttl_before_stream_visible {
                self.disable_native_ttl_for_table_id(&table_id).await?;
            }

            let spec_json =
                serde_json::to_value(spec).map_err(|e| StorageError::Internal(e.to_string()))?;
            let new_label = spec.stream_enabled.then(Self::new_stream_label);
            sqlx::query(
                "UPDATE tables SET stream_specification = ?, \
                 stream_label = CASE WHEN ? THEN COALESCE(stream_label, ?) ELSE stream_label END \
                 WHERE account_id = ? AND table_name = ?",
            )
            .bind(&spec_json)
            .bind(spec.stream_enabled)
            .bind(&new_label)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            if reconfigures_ttl {
                sqlx::query(
                    "UPDATE tables SET ttl_index_ready = FALSE, ttl_native_enabled = FALSE \
                     WHERE account_id = ? AND table_name = ?",
                )
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

        let desc = self
            .build_table_description(account_id, &input.table_name)
            .await?;
        if has_control_plane_updates {
            self.control_plane_notify.notify_one();
        }

        Ok(desc)
    }
}
