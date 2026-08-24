// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `WorkerStore` trait implementation and control plane transition processing.

use futures::future::BoxFuture;

use extenddb_storage::WorkerStore;
use extenddb_storage::error::StorageError;

use crate::PostgresEngine;

impl WorkerStore for PostgresEngine {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        Box::pin(async move {
            // Delegate to the inherent method.
            Self::process_control_plane_transitions(self).await
        })
    }
}

impl PostgresEngine {
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

        // CREATING → ACTIVE
        let activated: Vec<(String,)> = sqlx::query_as(
            r"UPDATE tables SET table_status = 'ACTIVE', status_transition_at = NULL
               WHERE table_status = 'CREATING' AND status_transition_at <= NOW()
               RETURNING table_name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        for (name,) in activated {
            transitions.push((name, "CREATING → active"));
        }

        // DELETING → remove row (with tags and data table cleanup).
        //
        // P57 Bug 1 fix: Collect index names BEFORE deleting the table row.
        // The `indexes` table has `ON DELETE CASCADE` referencing `tables`,
        // so `DELETE FROM tables` immediately removes all index rows. The old
        // code did DELETE first then SELECT on indexes — always got zero rows,
        // orphaning all GSI data tables.
        //
        // Strategy: SELECT ... FOR UPDATE SKIP LOCKED to lock candidates,
        // collect index names, then DELETE. All in one transaction.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let candidates: Vec<(String, String, String, String)> = sqlx::query_as(
            r"SELECT account_id, table_name, table_arn, table_id FROM tables
               WHERE table_status = 'DELETING' AND status_transition_at <= NOW()
               FOR UPDATE SKIP LOCKED",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Collect index ids while the rows still exist. Vector indexes are read
        // here for the same reason and at the same moment as the secondary ones:
        // their catalog rows cascade away with the table row, and each id names a
        // data table that still has to be dropped.
        let mut drop_info: Vec<(String, Vec<String>, Vec<String>)> = Vec::new();

        for (_acct_id, name, arn, table_id) in &candidates {
            let index_ids: Vec<(String,)> =
                sqlx::query_as("SELECT index_id FROM indexes WHERE table_id = $1")
                    .bind(table_id)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            let vector_index_ids: Vec<String> =
                sqlx::query_scalar("SELECT index_id FROM vector_indexes WHERE table_id = $1")
                    .bind(table_id)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

            // Delete tags explicitly (not covered by CASCADE from tables).
            sqlx::query("DELETE FROM tags WHERE resource_arn = $1")
                .bind(arn)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            // Now delete the table row. CASCADE removes indexes and streams.
            sqlx::query("DELETE FROM tables WHERE table_id = $1")
                .bind(table_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            drop_info.push((
                table_id.clone(),
                index_ids.into_iter().map(|(n,)| n).collect(),
                vector_index_ids,
            ));

            transitions.push((name.clone(), "DELETING → deleted"));
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // P54 Bug 1: Drop data tables on the data pool after catalog commit.
        for (table_id, index_ids, vector_index_ids) in &drop_info {
            let mut data_tx = self
                .data_pool
                .begin()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            for idx_id in index_ids {
                Self::drop_index_data_table(&mut data_tx, idx_id).await?;
            }
            for idx_id in vector_index_ids {
                Self::drop_vector_data_table(&mut data_tx, idx_id).await?;
            }
            Self::drop_data_table(&mut data_tx, table_id).await?;

            // Drop any still-pending GSI propagation rows for this table in the
            // same transaction that drops the tables, so workers don't waste a
            // claim→deserialize→attempt→skip cycle on now-orphaned rows.
            sqlx::query("DELETE FROM gsi_pending WHERE table_id = $1")
                .bind(table_id)
                .execute(&mut *data_tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            data_tx
                .commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
        }

        Ok(transitions)
    }
}
