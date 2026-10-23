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
        // Lock candidate catalog rows while their index ids are collected. The
        // data tables are dropped before the catalog row: if any data DDL
        // fails, the transaction rolls back and the row remains DELETING for
        // the next pass. Once the data transaction commits, deleting the table
        // row cascades its index and stream rows without orphaning data tables.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let candidates: Vec<(String, String, String)> = sqlx::query_as(
            r"SELECT table_name, table_arn, table_id FROM tables
               WHERE table_status = 'DELETING' AND status_transition_at <= NOW()
               FOR UPDATE SKIP LOCKED",
        )
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        for (name, arn, table_id) in &candidates {
            let index_ids: Vec<String> =
                sqlx::query_scalar("SELECT index_id FROM indexes WHERE table_id = $1")
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

            let dropped = async {
                let mut data_tx = self
                    .data_pool
                    .begin()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                // CreateBackup holds ACCESS SHARE through its item read. Do not
                // let a DROP waiting for that lock stall every transition in
                // this worker pass; a timeout rolls this transaction back and
                // leaves the catalog row DELETING for a later retry.
                sqlx::query("SET LOCAL lock_timeout = '3s'")
                    .execute(&mut *data_tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                for idx_id in &index_ids {
                    Self::drop_index_data_table(&mut data_tx, idx_id).await?;
                }
                for idx_id in &vector_index_ids {
                    Self::drop_vector_data_table(&mut data_tx, idx_id).await?;
                }
                Self::drop_data_table(&mut data_tx, table_id).await?;

                // Drop any still-pending GSI propagation rows for this table in
                // the same transaction that drops the tables, so workers don't
                // waste a claim→deserialize→attempt→skip cycle on orphaned rows.
                sqlx::query("DELETE FROM gsi_pending WHERE table_id = $1")
                    .bind(table_id)
                    .execute(&mut *data_tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                // The table is gone, so no vector build can release this hold;
                // leaving it would block later claims for the table id.
                sqlx::query("DELETE FROM vector_index_holds WHERE table_id = $1")
                    .bind(table_id)
                    .execute(&mut *data_tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

                data_tx
                    .commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                Ok::<(), StorageError>(())
            }
            .await;

            if let Err(e) = dropped {
                tracing::warn!(
                    "could not drop data tables for '{name}' ({table_id}); the table remains \
                     DELETING and the control plane will retry: {e}"
                );
                continue;
            }

            // Delete tags explicitly (not covered by CASCADE from tables).
            sqlx::query("DELETE FROM tags WHERE resource_arn = $1")
                .bind(arn)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            // The successful data drop makes it safe to remove the catalog row.
            // CASCADE removes indexes and streams.
            sqlx::query("DELETE FROM tables WHERE table_id = $1")
                .bind(table_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            transitions.push((name.clone(), "DELETING → deleted"));
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // CREATING with no owner → removed. A restore whose process died
        // mid-copy leaves its target CREATING with no scheduled transition.
        // The poller is notification-driven with a 60-second idle timeout, so
        // the 60-second grace means an idle abandoned target is removed about
        // 60–120 seconds after ownership is lost. Every pass opens a fresh
        // out-of-pool connection for each old candidate to probe its advisory
        // lock; a still-owned restore closes that probe and is checked again on
        // the next pass. Failures are logged and skipped, so they cannot hold
        // up the transitions above.
        match self.sweep_abandoned_restores().await {
            Ok(names) => {
                for name in names {
                    transitions.push((name, "abandoned restore → removed"));
                }
            }
            Err(e) => tracing::warn!("abandoned restore sweep failed: {e}"),
        }

        Ok(transitions)
    }
}
