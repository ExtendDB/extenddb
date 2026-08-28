// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `delete_table` implementation for `CassandraEngine`.

use cdrs_tokio::types::IntoRustByName;
use extenddb_core::types::{DeleteTableInput, TableDescription};
use extenddb_storage::error::StorageError;

use crate::CassandraEngine;

impl CassandraEngine {
    pub(crate) async fn delete_table_impl(
        &self,
        account_id: &str,
        input: DeleteTableInput,
    ) -> Result<TableDescription, StorageError> {
        let catalog_keyspace = self.catalog_keyspace();

        // Fetch table metadata before deletion
        let table_query = format!(
            "SELECT * FROM {}.tables WHERE account_id = ? AND table_name = ?",
            catalog_keyspace
        );

        let table_result = self
            .session
            .query_with_values(
                &table_query,
                cdrs_tokio::query_values!(account_id, input.table_name.as_str()),
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Query table: {}", e)))?;

        let table_body = table_result
            .response_body()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let table_rows = table_body
            .into_rows()
            .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;

        let table_row = table_rows
            .first()
            .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;

        // Check deletion protection
        let deletion_protection: bool = table_row
            .get_r_by_name("deletion_protection_enabled")
            .unwrap_or(false);

        if deletion_protection {
            let table_arn: String = table_row
                .get_r_by_name("table_arn")
                .map_err(|e| StorageError::Internal(format!("Parse table_arn: {}", e)))?;
            return Err(StorageError::DeletionProtected(table_arn));
        }

        let table_id: String = table_row
            .get_r_by_name("table_id")
            .map_err(|e| StorageError::Internal(format!("Parse table_id: {}", e)))?;

        // Fetch indexes for response
        let index_query = format!(
            "SELECT * FROM {}.indexes WHERE table_id = ?",
            catalog_keyspace
        );

        let index_result = self
            .session
            .query_with_values(&index_query, cdrs_tokio::query_values!(table_id.as_str()))
            .await
            .map_err(|e| StorageError::Internal(format!("Query indexes: {}", e)))?;

        let index_body = index_result
            .response_body()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let index_rows = index_body.into_rows().unwrap_or_default();

        // Build description for response (before deletion)
        let description =
            self.build_table_description_from_row(account_id, table_row, index_rows.clone())?;

        // Delete physical tables from the owning account keyspace. Catalog
        // metadata and user data intentionally live in different keyspaces.
        let account_keyspace = self.account_keyspace(account_id);
        self.drop_data_table(&account_keyspace, &table_id)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Delete index data tables
        for idx_row in &index_rows {
            let index_id: String = idx_row
                .get_r_by_name("index_id")
                .map_err(|e| StorageError::Internal(format!("Parse index_id: {}", e)))?;
            self.drop_index_data_table(&account_keyspace, &index_id)
                .await
                .map_err(|e| StorageError::Internal(format!("Drop index table: {e}")))?;
        }

        // Delete catalog entries (indexes first due to FK)
        let delete_indexes_query = format!(
            "DELETE FROM {}.indexes WHERE table_id = ?",
            catalog_keyspace
        );
        self.session
            .query_with_values(
                &delete_indexes_query,
                cdrs_tokio::query_values!(table_id.as_str()),
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Delete indexes: {}", e)))?;

        // Delete table-scoped continuous-backup configuration so recreating the
        // same table name does not inherit stale PITR state.
        let delete_continuous_backup_query = format!(
            "DELETE FROM {}.continuous_backups WHERE account_id = ? AND table_name = ?",
            catalog_keyspace
        );
        self.session
            .query_with_values(
                &delete_continuous_backup_query,
                cdrs_tokio::query_values!(account_id, input.table_name.as_str()),
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Delete continuous backup state: {e}")))?;

        // Delete table catalog entry
        let delete_table_query = format!(
            "DELETE FROM {}.tables WHERE account_id = ? AND table_name = ?",
            catalog_keyspace
        );
        self.session
            .query_with_values(
                &delete_table_query,
                cdrs_tokio::query_values!(account_id, input.table_name.as_str()),
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Delete table: {}", e)))?;

        Ok(description)
    }
}
