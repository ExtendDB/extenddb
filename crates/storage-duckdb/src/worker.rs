// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `WorkerStore` trait implementation and control-plane transition processing.
//!
//! Tables in CREATING whose `status_transition_at` has passed become ACTIVE;
//! tables in DELETING whose time has passed are removed (catalog row cascades
//! to indexes/shards/records) and their `_ddb_*` data tables dropped. The
//! engine write lock replaces PostgreSQL's `FOR UPDATE SKIP LOCKED`, and the
//! `<= now` comparison binds an RFC 3339 cutoff computed in Rust.

use crate::db;
use extenddb_storage::WorkerStore;
use extenddb_storage::error::StorageError;
use futures::future::BoxFuture;

use crate::duckdb_util::format_timestamp;
use crate::store::DuckDbEngine;

impl WorkerStore for DuckDbEngine {
    fn process_control_plane_transitions(
        &self,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, StorageError>> {
        Box::pin(async move { Self::process_control_plane_transitions(self).await })
    }
}

impl DuckDbEngine {
    /// Process due control-plane transitions. Returns `(table_name, change)`
    /// pairs for logging. Called by the background poller and at startup.
    pub async fn process_control_plane_transitions(
        &self,
    ) -> Result<Vec<(String, &'static str)>, StorageError> {
        let _writer = self.write_lock.lock().await;
        let now = format_timestamp(time::OffsetDateTime::now_utc());
        let mut transitions = Vec::new();

        // CREATING → ACTIVE. The table flip and the with-table index flip below
        // ride one transaction so no DescribeTable can observe the table ACTIVE
        // with its own creation-time index still CREATING: measured against the
        // service (2026-08-21, eu-west-2, three runs at 250ms), both reach
        // ACTIVE in the same poll with no gap in either direction.
        let mut activate_tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        let activated: Vec<(String,)> = db::query_as(
            "UPDATE tables SET table_status = 'ACTIVE', status_transition_at = NULL \
             WHERE table_status = 'CREATING' AND status_transition_at IS NOT NULL \
               AND status_transition_at <= ? RETURNING table_name",
        )
        .bind(&now)
        .fetch_all(&mut *activate_tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Vector indexes created WITH their table (backfilling IS NULL — the
        // UpdateTable path writes `false` from the first instant, so it is
        // never selected here). Written as a self-healing catch-all over every
        // ACTIVE table rather than just the rows activated above, so a crash
        // between a past table flip and its index flip cannot strand an index
        // in CREATING forever; re-running it is a no-op.
        db::query(
            "UPDATE vector_indexes SET index_status = 'ACTIVE' \
             WHERE index_status = 'CREATING' AND backfilling IS NULL \
               AND table_id IN (SELECT table_id FROM tables WHERE table_status = 'ACTIVE')",
        )
        .execute(&mut *activate_tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        activate_tx
            .commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
        for (name,) in activated {
            transitions.push((name, "CREATING → active"));
        }

        // DELETING → removed.
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let candidates: Vec<(String, String, String)> = db::query_as(
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
                db::query_as("SELECT index_id FROM indexes WHERE table_id = ?")
                    .bind(table_id)
                    .fetch_all(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;

            db::query("DELETE FROM tags WHERE resource_arn = ?")
                .bind(arn)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            crate::referential::delete_table_children(&mut tx, table_id)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            db::query("DELETE FROM tables WHERE table_id = ?")
                .bind(table_id)
                .execute(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

            for (idx_id,) in &index_ids {
                Self::drop_index_data_table(&mut tx, idx_id).await?;
            }
            Self::drop_data_table(&mut tx, table_id).await?;
            // Remove orphaned pending GSI rows in the same drop transaction.
            db::query("DELETE FROM gsi_pending WHERE table_id = ?")
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
