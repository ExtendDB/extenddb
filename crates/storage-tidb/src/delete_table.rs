// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `delete_table` implementation for `TidbEngine`.

use extenddb_core::types::{DeleteTableInput, TableDescription, TableStatus};
use extenddb_storage::error::StorageError;

use crate::TidbEngine;
use crate::stream_engine::StreamGenerationCatalog;
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
                      table_arn, table_id, deletion_protection_enabled, stream_label
               FROM tables WHERE account_id = ? AND table_name = ? AND table_status IN ('ACTIVE', 'CREATING', 'UPDATING')
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

        let stats = self.current_table_stats(&row.table_id).await?;

        if let Some(stream_label) = row.stream_label.as_deref() {
            let stream_specification = row.stream_specification.as_ref().ok_or_else(|| {
                StorageError::Internal(format!(
                    "stream label exists without stream specification for table {}",
                    input.table_name
                ))
            })?;
            Self::disable_stream_generation_in_tx(
                &mut tx,
                StreamGenerationCatalog {
                    account_id,
                    table_name: &input.table_name,
                    table_id: &row.table_id,
                    stream_label,
                    key_schema: &row.key_schema,
                    stream_specification,
                },
            )
            .await?;
        }

        // Set DELETING status with immediate cleanup eligibility. The control-plane
        // reconciler drops TiDB data artifacts first, then removes catalog
        // metadata, so a failed cleanup remains retryable. TiDB owns distributed
        // online DDL scheduling; ExtendDB does not add an artificial delay.
        sqlx::query(
            r"UPDATE tables SET table_status = 'DELETING',
                status_transition_at = CURRENT_TIMESTAMP(6)
               WHERE account_id = ? AND table_name = ?",
        )
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
        let desc = self.build_table_description_from_row(account_id, row, index_rows, stats)?;

        Ok(TableDescription {
            table_status: TableStatus::Deleting,
            ..desc
        })
    }
}
