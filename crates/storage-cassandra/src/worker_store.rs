// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `WorkerStore` trait implementation and control plane transition processing.

use cdrs_tokio::types::IntoRustByName;
use futures::future::BoxFuture;

use extenddb_storage::WorkerStore;
use extenddb_storage::error::StorageError;

use crate::CassandraEngine;

impl WorkerStore for CassandraEngine {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        Box::pin(async move {
            // Delegate to the inherent method.
            Self::process_control_plane_transitions(self).await
        })
    }
}

impl CassandraEngine {
    /// Process pending control plane transitions (H-5).
    ///
    /// Tables in CREATING state whose `status_transition_at` has passed are
    /// moved to ACTIVE. Tables in DELETING state whose transition time has
    /// passed are removed (along with their indexes and tags).
    ///
    /// Called by the background poller in `cmd_serve`. Also called at startup
    /// to recover in-flight operations from a previous server instance.
    ///
    /// Returns a list of `(table_name, transition)` pairs describing what
    /// changed, so the caller can log meaningful state-change messages (D-4).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the database is unreachable or a query fails.
    pub async fn process_control_plane_transitions(
        &self,
    ) -> Result<Vec<(String, &'static str)>, StorageError> {
        let mut transitions = Vec::new();
        let catalog_keyspace = self.catalog_keyspace();

        // CREATING → ACTIVE
        // Note: Cassandra requires ALLOW FILTERING for non-key columns in WHERE clause
        let query = format!(
            "SELECT account_id, table_name, table_id FROM {}.tables \
             WHERE table_status = 'CREATING' AND status_transition_at <= toTimestamp(now()) \
             ALLOW FILTERING",
            catalog_keyspace
        );

        let result = self.session.query(&query).await.map_err(|e| {
            StorageError::Internal(format!("Failed to query CREATING tables: {}", e))
        })?;

        let body = result.response_body().map_err(|e| {
            StorageError::Internal(format!("Failed to parse CREATING tables response: {}", e))
        })?;

        let rows = body.into_rows().unwrap_or_default();

        for row in rows {
            let account_id: String = row.get_r_by_name("account_id").map_err(|e| {
                StorageError::Internal(format!("Failed to parse account_id: {}", e))
            })?;
            let table_name: String = row.get_r_by_name("table_name").map_err(|e| {
                StorageError::Internal(format!("Failed to parse table_name: {}", e))
            })?;

            // Update to ACTIVE (PRIMARY KEY is account_id, table_name)
            let update = format!(
                "UPDATE {}.tables SET table_status = 'ACTIVE', status_transition_at = null \
                 WHERE account_id = ? AND table_name = ?",
                catalog_keyspace
            );
            self.session
                .query_with_values(
                    &update,
                    cdrs_tokio::query_values!(account_id.as_str(), table_name.as_str()),
                )
                .await
                .map_err(|e| StorageError::Internal(format!("Failed to activate table: {}", e)))?;

            transitions.push((table_name, "CREATING → active"));
        }

        // DELETING → remove row (with tags and data table cleanup)
        let query = format!(
            "SELECT account_id, table_name, table_arn, table_id FROM {}.tables \
             WHERE table_status = 'DELETING' AND status_transition_at <= toTimestamp(now()) \
             ALLOW FILTERING",
            catalog_keyspace
        );

        let result = self.session.query(&query).await.map_err(|e| {
            StorageError::Internal(format!("Failed to query DELETING tables: {}", e))
        })?;

        let body = result.response_body().map_err(|e| {
            StorageError::Internal(format!("Failed to parse DELETING tables response: {}", e))
        })?;

        let rows = body.into_rows().unwrap_or_default();

        for row in rows {
            let account_id: String = row.get_r_by_name("account_id").map_err(|e| {
                StorageError::Internal(format!("Failed to parse account_id: {}", e))
            })?;
            let table_name: String = row.get_r_by_name("table_name").map_err(|e| {
                StorageError::Internal(format!("Failed to parse table_name: {}", e))
            })?;
            let table_arn: String = row
                .get_r_by_name("table_arn")
                .map_err(|e| StorageError::Internal(format!("Failed to parse table_arn: {}", e)))?;
            let table_id: String = row
                .get_r_by_name("table_id")
                .map_err(|e| StorageError::Internal(format!("Failed to parse table_id: {}", e)))?;

            // Delete tags
            let tag_delete = format!(
                "DELETE FROM {}.tags WHERE resource_arn = ?",
                catalog_keyspace
            );
            self.session
                .query_with_values(&tag_delete, cdrs_tokio::query_values!(table_arn.as_str()))
                .await
                .map_err(|e| StorageError::Internal(format!("Failed to delete tags: {}", e)))?;

            // Delete indexes (catalog + data tables)
            let account_keyspace = self.account_keyspace(&account_id);
            crate::data::index::delete_indexes_for_table(
                &self.session_arc(),
                &catalog_keyspace,
                &account_keyspace,
                &table_id,
                self,
            )
            .await?;

            let continuous_backup_delete = format!(
                "DELETE FROM {}.continuous_backups WHERE account_id = ? AND table_name = ?",
                catalog_keyspace
            );
            self.session
                .query_with_values(
                    &continuous_backup_delete,
                    cdrs_tokio::query_values!(account_id.as_str(), table_name.as_str()),
                )
                .await
                .map_err(|e| {
                    StorageError::Internal(format!("Failed to delete continuous backup state: {e}"))
                })?;

            // Delete table row (PRIMARY KEY is account_id, table_name)
            let table_delete = format!(
                "DELETE FROM {}.tables WHERE account_id = ? AND table_name = ?",
                catalog_keyspace
            );
            self.session
                .query_with_values(
                    &table_delete,
                    cdrs_tokio::query_values!(account_id.as_str(), table_name.as_str()),
                )
                .await
                .map_err(|e| StorageError::Internal(format!("Failed to delete table: {}", e)))?;

            // Drop base table in account keyspace
            self.drop_data_table(&account_keyspace, &table_id).await?;

            transitions.push((table_name, "DELETING → deleted"));
        }

        Ok(transitions)
    }
}
