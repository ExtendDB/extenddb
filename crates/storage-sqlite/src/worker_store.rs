// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `WorkerStore` trait implementation and control plane transition processing.

use futures::future::BoxFuture;

use extenddb_storage::WorkerStore;
use extenddb_storage::error::StorageError;

use crate::engine::SqliteEngine;

impl WorkerStore for SqliteEngine {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        Box::pin(async move { Self::process_control_plane_transitions(self).await })
    }
}

impl SqliteEngine {
    /// Process pending control plane transitions (H-5).
    ///
    /// Tables in CREATING state whose `status_transition_at` has passed are
    /// moved to ACTIVE. Tables in DELETING state whose transition time has
    /// passed are removed.
    pub async fn process_control_plane_transitions(
        &self,
    ) -> Result<Vec<(String, &'static str)>, StorageError> {
        let mut transitions = Vec::new();

        // CREATING → ACTIVE
        let activated: Vec<(String,)> = sqlx::query_as(
            "UPDATE tables SET table_status = 'ACTIVE', status_transition_at = NULL \
             WHERE table_status = 'CREATING' AND status_transition_at <= datetime('now') \
             RETURNING table_name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        for (name,) in activated {
            transitions.push((name, "CREATING → active"));
        }

        // DELETING → remove. Collect index ids before deletion since CASCADE
        // removes them when the table row is deleted.
        let candidates: Vec<(String, String, String, String)> = sqlx::query_as(
            "SELECT account_id, table_name, table_arn, table_id FROM tables \
             WHERE table_status = 'DELETING' AND status_transition_at <= datetime('now')",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let mut drop_info: Vec<(String, Vec<String>)> = Vec::new();

        for (_acct_id, name, arn, table_id) in &candidates {
            let index_ids: Vec<(String,)> =
                sqlx::query_as("SELECT index_id FROM indexes WHERE table_id = ?")
                    .bind(table_id)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

            sqlx::query("DELETE FROM tags WHERE resource_arn = ?")
                .bind(arn)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            sqlx::query("DELETE FROM tables WHERE table_id = ?")
                .bind(table_id)
                .execute(&self.pool)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            drop_info.push((
                table_id.clone(),
                index_ids.into_iter().map(|(n,)| n).collect(),
            ));

            transitions.push((name.clone(), "DELETING → deleted"));
        }

        // Drop data tables after catalog rows deleted.
        for (table_id, index_ids) in &drop_info {
            let mut data_tx = self
                .pool
                .begin()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            for idx_id in index_ids {
                Self::drop_index_data_table(&mut data_tx, idx_id).await?;
            }
            Self::drop_data_table(&mut data_tx, table_id).await?;
            data_tx
                .commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        Ok(transitions)
    }
}
