// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `delete_table` implementation for `SqliteEngine`.
//!
//! With `control_plane_delay_seconds < 1` the table and its data tables are
//! dropped immediately; otherwise the table is marked DELETING with a scheduled
//! transition that the control-plane worker completes.

use extenddb_core::types::{DeleteTableInput, TableDescription, TableStatus};
use extenddb_storage::error::StorageError;

use crate::sqlite_util::format_timestamp;
use crate::store::SqliteEngine;
use crate::table_helpers::{INDEX_COLUMNS, IndexRow, TABLE_COLUMNS, TableRow};

impl SqliteEngine {
    pub(crate) async fn delete_table_impl(
        &self,
        account_id: &str,
        input: DeleteTableInput,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;

        let row: Option<TableRow> = sqlx::query_as(&format!(
            "SELECT {TABLE_COLUMNS} FROM tables \
             WHERE account_id = ? AND table_name = ? AND table_status IN ('ACTIVE', 'CREATING')"
        ))
        .bind(account_id)
        .bind(&input.table_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row = row.ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;
        if row.deletion_protection_enabled {
            return Err(StorageError::DeletionProtected(row.table_arn.clone()));
        }

        let index_rows: Vec<IndexRow> = sqlx::query_as(&format!(
            "SELECT {INDEX_COLUMNS} FROM indexes WHERE table_id = ?"
        ))
        .bind(&row.table_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let delay_secs: f64 = sqlx::query_scalar::<_, String>(
            "SELECT value FROM settings WHERE key = 'control_plane_delay_seconds'",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(0.25);

        let index_ids: Vec<String> = index_rows.iter().map(|r| r.index_id.clone()).collect();
        let table_id = row.table_id.clone();
        let table_arn = row.table_arn.clone();

        if delay_secs < 1.0 {
            let _writer = self.write_lock.lock().await;
            let mut tx = self
                .pool
                .begin_with("BEGIN IMMEDIATE")
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            sqlx::query("DELETE FROM tags WHERE resource_arn = ?")
                .bind(&table_arn)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            // CASCADE removes indexes, stream shards/records.
            sqlx::query("DELETE FROM tables WHERE table_id = ?")
                .bind(&table_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            for idx_id in &index_ids {
                Self::drop_index_data_table(&mut tx, idx_id).await?;
            }
            Self::drop_data_table(&mut tx, &table_id).await?;
            // Drop any still-pending GSI propagation rows for this table in the
            // same transaction that drops the tables, so the worker doesn't
            // waste a claim→deserialize→skip cycle on now-orphaned rows.
            sqlx::query("DELETE FROM gsi_pending WHERE table_id = ?")
                .bind(&table_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        } else {
            let transition = format_timestamp(
                time::OffsetDateTime::now_utc() + time::Duration::seconds_f64(delay_secs),
            );
            sqlx::query(
                "UPDATE tables SET table_status = 'DELETING', status_transition_at = ? \
                 WHERE account_id = ? AND table_name = ?",
            )
            .bind(&transition)
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
