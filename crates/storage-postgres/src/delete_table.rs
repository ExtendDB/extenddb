// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `delete_table` implementation for `PostgresEngine`.

use extenddb_core::types::{DeleteTableInput, TableDescription, TableStatus};
use extenddb_storage::error::StorageError;

use crate::PostgresEngine;
use crate::table_helpers::{IndexRow, TableRow};

impl PostgresEngine {
    /// Core implementation of `delete_table` (H-5).
    pub(crate) async fn delete_table_impl(
        &self,
        account_id: &str,
        input: DeleteTableInput,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Lock and fetch the row atomically with SELECT ... FOR UPDATE
        let row: Option<TableRow> = sqlx::query_as(
            r"SELECT table_name, key_schema, attribute_definitions, billing_mode,
                      provisioned_throughput, stream_specification, table_status,
                      EXTRACT(EPOCH FROM creation_date_time)::FLOAT8 as creation_epoch,
                      table_size_bytes, item_count, table_arn, table_id,
                      deletion_protection_enabled, stream_label,
                      table_class, sse_specification, on_demand_throughput
               FROM tables WHERE account_id = $1 AND table_name = $2 AND table_status IN ('ACTIVE', 'CREATING')
               FOR UPDATE",
        )
        .bind(account_id)
        .bind(&input.table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row = row.ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;

        // REQ: DeletionProtectionEnabled check — real DynamoDB returns ValidationException
        if row.deletion_protection_enabled {
            return Err(StorageError::DeletionProtected(row.table_arn.clone()));
        }

        // Fetch indexes for the response description.
        let index_rows: Vec<IndexRow> = sqlx::query_as(
            r"SELECT index_name, index_id, index_type, key_schema, projection,
                      index_status, provisioned_throughput
               FROM indexes WHERE table_id = $1",
        )
        .bind(&row.table_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        // H-5 (delete): synchronous control-plane shortcut when delay < 1s.
        // When control_plane_delay_seconds is small, the poller may not run
        // before the next request, causing stale DELETING rows. Synchronous
        // delete avoids this. Mirrors create_table's H-5 fast path.
        let delay_row: (f64,) = sqlx::query_as(
            "SELECT COALESCE((SELECT value::FLOAT8 FROM settings WHERE key = 'control_plane_delay_seconds'), 0.25)",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        let delay_secs = delay_row.0;

        let index_ids: Vec<String> = index_rows.iter().map(|r| r.index_id.clone()).collect();

        if delay_secs < 1.0 {
            // Synchronous delete: remove tags, catalog row, and data tables inline.
            // Note: deleting the tables row cascades to indexes and stream rows via FK CASCADE.
            sqlx::query("DELETE FROM tags WHERE resource_arn = $1")
                .bind(&row.table_arn)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            sqlx::query("DELETE FROM tables WHERE table_id = $1")
                .bind(&row.table_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            // Drop data tables after catalog commit.
            let mut data_tx = self
                .data_pool
                .begin()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            for idx_id in &index_ids {
                Self::drop_index_data_table(&mut data_tx, idx_id).await?;
            }
            Self::drop_data_table(&mut data_tx, &row.table_id).await?;

            // Drop any still-pending GSI propagation rows for this table in the
            // same transaction that drops the tables. Workers tolerate orphans
            // (savepoint-skip on 42P01), but removing them here avoids the
            // wasted claim→deserialize→attempt→skip cycle and the log noise,
            // especially when many items were written just before deletion.
            sqlx::query("DELETE FROM gsi_pending WHERE table_id = $1")
                .bind(&row.table_id)
                .execute(&mut *data_tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            data_tx
                .commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        } else {
            // Async delete: set DELETING status with scheduled removal time.
            sqlx::query(
                r"UPDATE tables SET table_status = 'DELETING',
                    status_transition_at = NOW() + make_interval(secs => $3)
                   WHERE account_id = $1 AND table_name = $2",
            )
            .bind(account_id)
            .bind(&input.table_name)
            .bind(delay_secs)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            // F-3: Wake the control plane poller so it processes the DELETING →
            // removed transition without waiting for the idle timeout.
            self.control_plane_notify.notify_one();
        }

        // Build description from the fetched row data
        let desc = self.build_table_description_from_row(account_id, row, index_rows)?;

        Ok(TableDescription {
            table_status: TableStatus::Deleting,
            ..desc
        })
    }
}
