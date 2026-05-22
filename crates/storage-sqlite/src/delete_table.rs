// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `delete_table` implementation for `SqliteEngine`.

use extenddb_core::types::{DeleteTableInput, TableDescription, TableStatus};
use extenddb_storage::error::StorageError;

use crate::engine::SqliteEngine;
use crate::table_helpers::{IndexRow, TableRow};

impl SqliteEngine {
    pub(crate) async fn delete_table_impl(
        &self,
        account_id: &str,
        input: DeleteTableInput,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;

        let row: Option<TableRow> = sqlx::query_as(
            "SELECT table_name, key_schema, attribute_definitions, billing_mode, \
              provisioned_throughput, stream_specification, table_status, \
              creation_date_time, \
              table_size_bytes, item_count, table_arn, table_id, \
              deletion_protection_enabled, stream_label \
             FROM tables \
             WHERE account_id = ? AND table_name = ? AND table_status IN ('ACTIVE', 'CREATING')",
        )
        .bind(account_id)
        .bind(&input.table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row = row.ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;

        if row.deletion_protection_enabled {
            return Err(StorageError::DeletionProtected(row.table_arn.clone()));
        }

        let index_rows: Vec<IndexRow> = sqlx::query_as(
            "SELECT index_name, index_id, index_type, key_schema, projection, \
              index_status, provisioned_throughput \
             FROM indexes WHERE table_id = ?",
        )
        .bind(&row.table_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let delay_row: Option<(String,)> = sqlx::query_as(
            "SELECT value FROM settings WHERE key = 'control_plane_delay_seconds'",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        let delay_secs: f64 = delay_row
            .and_then(|(v,)| v.parse::<f64>().ok())
            .unwrap_or(0.25);

        let index_ids: Vec<String> = index_rows.iter().map(|r| r.index_id.clone()).collect();

        if delay_secs < 1.0 {
            sqlx::query("DELETE FROM tags WHERE resource_arn = ?")
                .bind(&row.table_arn)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            sqlx::query("DELETE FROM tables WHERE table_id = ?")
                .bind(&row.table_id)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            // Drop data tables.
            let mut data_tx = self
                .pool
                .begin()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            for idx_id in &index_ids {
                Self::drop_index_data_table(&mut data_tx, idx_id).await?;
            }
            Self::drop_data_table(&mut data_tx, &row.table_id).await?;
            data_tx
                .commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        } else {
            let secs = delay_secs as i64;
            sqlx::query(
                "UPDATE tables SET table_status = 'DELETING', \
                  status_transition_at = datetime('now', '+' || ? || ' seconds') \
                 WHERE account_id = ? AND table_name = ?",
            )
            .bind(secs)
            .bind(account_id)
            .bind(&input.table_name)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            self.control_plane_notify.notify_one();
        }

        let desc = self.build_table_description_from_row(account_id, row, index_rows)?;

        Ok(TableDescription {
            table_status: TableStatus::Deleting,
            ..desc
        })
    }
}
