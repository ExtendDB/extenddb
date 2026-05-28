// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `delete_table` implementation for `TidbEngine`.

use extenddb_core::types::{DeleteTableInput, TableDescription, TableStatus};
use extenddb_storage::error::StorageError;

use crate::TidbEngine;
use crate::table_helpers::{IndexRow, TableRow};

impl TidbEngine {
    /// Core implementation of `delete_table`.
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
                      CAST(UNIX_TIMESTAMP(creation_date_time) AS DOUBLE) as creation_epoch,
                      table_size_bytes, item_count, table_arn, table_id,
                      deletion_protection_enabled, stream_label
               FROM tables WHERE account_id = ? AND table_name = ? AND table_status IN ('ACTIVE', 'CREATING')
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
            r"SELECT index_name, index_type, key_schema, projection,
                      index_status, provisioned_throughput
               FROM indexes WHERE table_id = ?",
        )
        .bind(&row.table_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Schedule physical cleanup through the control-plane reconciler so TiDB
        // data artifacts are dropped before catalog metadata.
        let delay_row: (f64,) = sqlx::query_as(
            "SELECT COALESCE((SELECT CAST(value AS DOUBLE) FROM settings WHERE `key` = 'control_plane_delay_seconds'), 0.25)",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        let delay_secs = delay_row.0;

        // Set DELETING status with a scheduled removal time. The control-plane
        // reconciler drops TiDB data artifacts first, then removes catalog
        // metadata, so a failed cleanup remains retryable.
        sqlx::query(
            r"UPDATE tables SET table_status = 'DELETING',
                status_transition_at = DATE_ADD(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND)
               WHERE account_id = ? AND table_name = ?",
        )
        .bind(delay_secs.max(0.0))
        .bind(account_id)
        .bind(&input.table_name)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Wake the control plane poller so it processes the DELETING → removed
        // transition without waiting for the idle timeout.
        self.control_plane_notify.notify_one();

        // Build description from the fetched row data
        let desc = self.build_table_description_from_row(account_id, row, index_rows)?;

        Ok(TableDescription {
            table_status: TableStatus::Deleting,
            ..desc
        })
    }
}
