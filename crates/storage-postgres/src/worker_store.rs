// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `WorkerStore` trait implementation and control plane transition processing.

use futures::future::BoxFuture;

use extenddb_storage::WorkerStore;
use extenddb_storage::error::StorageError;

use crate::PostgresEngine;
use crate::data::LockHolders;
use crate::delete_table::SYNC_DROP_LOCK_TIMEOUT;

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

/// What one pass of [`PostgresEngine::control_plane_pass`] did.
pub(crate) struct ControlPlanePass {
    /// `(table_name, transition)` pairs, for the caller's log.
    pub(crate) transitions: Vec<(String, &'static str)>,
    /// `DELETING` rows that were due but still in use in this pass: the
    /// probe found another session holding a lock on one of the table's
    /// relations, or the drop failed in a lock wait
    /// (`data::lock_wait_failed`). The drop succeeds once that session ends.
    /// While this is non-zero the poller keeps polling rather than wait for a
    /// wake, since the holder (a plain transaction, `pg_dump`, a client read
    /// or write) may not be one that wakes it. A drop that failed for any
    /// other reason (a dependent object, a privilege) is not counted: it
    /// would fail again every second, so it is logged and retried at the
    /// poller's normal cadence, the next pass after a wake or the 60 s idle
    /// pass.
    pub(crate) deferred_drops: usize,
}

impl PostgresEngine {
    /// Process pending control plane transitions (H-5).
    ///
    /// Tables in CREATING state whose `status_transition_at` has passed are
    /// moved to ACTIVE. Tables in DELETING state whose transition time has
    /// passed are removed (along with their indexes and tags).
    ///
    /// Called by tests that drive the control plane themselves. Startup
    /// recovery (`workers::recover_control_plane_at_startup`) and the
    /// background poller call `control_plane_pass`, which also reports the
    /// drops this pass had to defer.
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
        Ok(self.control_plane_pass().await?.transitions)
    }

    /// One pass of the control plane: [`Self::process_control_plane_transitions`]
    /// plus the count of `DELETING` rows whose drop was deferred by the lock
    /// timeout ([`ControlPlanePass::deferred_drops`]), which the poller uses
    /// to decide whether to stay active.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the database is unreachable or a query fails.
    pub(crate) async fn control_plane_pass(&self) -> Result<ControlPlanePass, StorageError> {
        let mut transitions = Vec::new();
        let mut deferred_drops = 0;

        // CREATING → ACTIVE, in its own statement ahead of the drops, so a
        // table that is due is activated at the start of every pass whatever
        // the drops below find.
        for name in self.activate_due_tables().await? {
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

            let dropped = self
                .drop_unless_held(&mut tx, table_id, &index_ids, &vector_index_ids)
                .await?;
            #[cfg(feature = "test-hooks")]
            self.drop_attempts
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            match dropped {
                None => {
                    tracing::debug!(
                        "table '{name}' ({table_id}) stays DELETING: a session holds a lock on \
                         its data tables"
                    );
                    deferred_drops += 1;
                    continue;
                }
                Some(Ok(())) => {}
                Some(Err(e)) if crate::data::lock_wait_failed(&e) => {
                    tracing::info!(
                        "table '{name}' ({table_id}) stays DELETING: a session holds its data \
                         tables ({e}); the control plane retries the drop while it waits"
                    );
                    deferred_drops += 1;
                    continue;
                }
                Some(Err(e)) => {
                    tracing::warn!(
                        "could not drop data tables for '{name}' ({table_id}); the table remains \
                         DELETING and the control plane will retry: {e}"
                    );
                    continue;
                }
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

        Ok(ControlPlanePass {
            transitions,
            deferred_drops,
        })
    }

    /// Moves every `CREATING` table whose `status_transition_at` has passed
    /// to `ACTIVE` and returns their names.
    async fn activate_due_tables(&self) -> Result<Vec<String>, StorageError> {
        let activated: Vec<(String,)> = sqlx::query_as(
            r"UPDATE tables SET table_status = 'ACTIVE', status_transition_at = NULL
               WHERE table_status = 'CREATING' AND status_transition_at <= NOW()
               RETURNING table_name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        Ok(activated.into_iter().map(|(name,)| name).collect())
    }

    /// Drops a `DELETING` table's data tables unless another session holds a
    /// lock on one of them. `None` means the probe found one held and nothing
    /// was tried; `Some` carries the drop's result.
    async fn drop_unless_held(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        table_id: &str,
        index_ids: &[String],
        vector_index_ids: &[String],
    ) -> Result<Option<Result<(), sqlx::Error>>, StorageError> {
        // A table snapshot, the in-catalog backup, a client read or write,
        // an index build or `VACUUM` holds a lock on the table's relations
        // for as long as it runs, and a `DROP` would wait for it. So the
        // drop is tried only when no other session holds any lock on them
        // (on the connection this pass holds when catalog and data share
        // a pool, as in `DeleteTable`), and a held table is skipped at
        // once. A pass therefore costs a probe per held table rather than
        // a lock wait per held table, and a table whose holder has ended
        // is dropped at the next pass, about `active_poll` after the
        // release however many other tables are held.
        let relations = crate::data::table_storage_relations(table_id, index_ids, vector_index_ids);
        let held = if self.shared_data_pool {
            crate::data::storage_in_use(&mut **tx, relations, LockHolders::Any).await
        } else {
            crate::data::storage_in_use(&self.data_pool, relations, LockHolders::Any).await
        }
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        // A table the probe found free is dropped under the same budget
        // as DeleteTable's drops: a holder that took its lock after the
        // probe, or a claim in flight on the table's queue rows, costs
        // this pass at most `SYNC_DROP_LOCK_TIMEOUT`. See
        // `drop_table_storage` for why the queue is stopped before
        // anything is dropped. Any failure leaves the catalog row
        // DELETING. A lock wait is counted, so the poller keeps retrying
        // every `active_poll` until the holder is gone rather than waiting
        // for a wake that only a snapshot stream or the backup sends; any
        // other failure is not (`ControlPlanePass`).
        if held {
            return Ok(None);
        }
        Ok(Some(
            Self::drop_table_storage(
                &self.data_pool,
                table_id,
                index_ids,
                vector_index_ids,
                SYNC_DROP_LOCK_TIMEOUT,
            )
            .await,
        ))
    }
}
