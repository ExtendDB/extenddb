// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `WorkerStore` trait implementation and control-plane transition processing.
//!
//! Tables in CREATING whose `status_transition_at` has passed become ACTIVE;
//! tables in DELETING whose time has passed are removed (catalog row cascades
//! to indexes/shards/records) and their `_ddb_*` data tables dropped. The
//! engine write lock replaces PostgreSQL's `FOR UPDATE SKIP LOCKED`, and the
//! `<= now` comparison binds an RFC 3339 cutoff computed in Rust.

use extenddb_storage::WorkerStore;
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;

use crate::sqlite_util::format_timestamp;
use crate::store::SqliteEngine;

impl WorkerStore for SqliteEngine {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        Box::pin(async move { Self::process_control_plane_transitions(self).await })
    }
}

impl SqliteEngine {
    /// Process due control-plane transitions. Returns `(table_name, change)`
    /// pairs for logging. Called by the background poller and at startup.
    pub async fn process_control_plane_transitions(
        &self,
    ) -> Result<Vec<(String, &'static str)>, StorageError> {
        let _writer = self.write_lock.lock().await;
        let now = format_timestamp(time::OffsetDateTime::now_utc());
        let mut transitions = Vec::new();

        // CREATING → ACTIVE.
        let activated: Vec<(String,)> = sqlx::query_as(
            "UPDATE tables SET table_status = 'ACTIVE', status_transition_at = NULL \
             WHERE table_status = 'CREATING' AND status_transition_at IS NOT NULL \
               AND status_transition_at <= ? RETURNING table_name",
        )
        .bind(&now)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        for (name,) in activated {
            transitions.push((name, "CREATING → active"));
        }

        // DELETING → removed.
        let mut tx = self
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let candidates: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT table_name, table_arn, table_id FROM tables \
             WHERE table_status = 'DELETING' AND status_transition_at IS NOT NULL \
               AND status_transition_at <= ?",
        )
        .bind(&now)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        for (name, arn, table_id) in &candidates {
            // Collect index ids before deleting the table row (CASCADE removes them).
            let index_ids: Vec<(String,)> =
                sqlx::query_as("SELECT index_id FROM indexes WHERE table_id = ?")
                    .bind(table_id)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

            sqlx::query("DELETE FROM tags WHERE resource_arn = ?")
                .bind(arn)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            sqlx::query("DELETE FROM tables WHERE table_id = ?")
                .bind(table_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            for (idx_id,) in &index_ids {
                Self::drop_index_data_table(&mut tx, idx_id).await?;
            }
            Self::drop_data_table(&mut tx, table_id).await?;
            // Remove orphaned pending GSI rows in the same drop transaction.
            sqlx::query("DELETE FROM gsi_pending WHERE table_id = ?")
                .bind(table_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            transitions.push((name.clone(), "DELETING → deleted"));
        }

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        Ok(transitions)
    }
}
