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
    /// moved to ACTIVE. A DELETING table whose deadline has passed is taken
    /// with a compare-and-set on that deadline and removed by the same steps
    /// `DeleteTable` runs (see `remove_claimed_table`); a row the swap does
    /// not take is left to the actor that holds it.
    ///
    /// Called by the background poller in `cmd_serve`. Also called at startup
    /// to recover in-flight operations from a previous server instance.
    ///
    /// Returns a list of `(table_name, transition)` pairs describing what
    /// changed, so the caller can log meaningful state-change messages (D-4).
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] if the catalog cannot be read or a row cannot
    /// be parsed; a failure to remove one `DELETING` table is logged and the
    /// table is left for a later pass.
    pub async fn process_control_plane_transitions(
        &self,
    ) -> Result<Vec<(String, &'static str)>, StorageError> {
        let mut transitions = Vec::new();
        let catalog_keyspace = self.catalog_keyspace();

        // CREATING → ACTIVE
        // Note: Cassandra requires ALLOW FILTERING for non-key columns in WHERE clause
        let query = format!(
            "SELECT account_id, table_name, table_id FROM {catalog_keyspace}.tables \
             WHERE table_status = 'CREATING' AND status_transition_at <= toTimestamp(now()) \
             ALLOW FILTERING"
        );

        let result =
            self.session.query(&query).await.map_err(|e| {
                StorageError::Internal(format!("Failed to query CREATING tables: {e}"))
            })?;

        let body = result.response_body().map_err(|e| {
            StorageError::Internal(format!("Failed to parse CREATING tables response: {e}"))
        })?;

        let rows = body.into_rows().unwrap_or_default();

        for row in rows {
            let account_id: String = row
                .get_r_by_name("account_id")
                .map_err(|e| StorageError::Internal(format!("Failed to parse account_id: {e}")))?;
            let table_name: String = row
                .get_r_by_name("table_name")
                .map_err(|e| StorageError::Internal(format!("Failed to parse table_name: {e}")))?;

            // Update to ACTIVE via LWT to avoid mixing plain/LWT writes on the
            // tables row (created with IF NOT EXISTS). See ADR-0021.
            let update = format!(
                "UPDATE {catalog_keyspace}.tables SET table_status = 'ACTIVE', status_transition_at = null \
                 WHERE account_id = ? AND table_name = ? IF table_status = 'CREATING'"
            );
            crate::cassandra_util::query_lwt(
                &self.session,
                &update,
                cdrs_tokio::query_values!(account_id.as_str(), table_name.as_str()),
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to activate table: {e}")))?;

            transitions.push((table_name, "CREATING → active"));
        }

        // DELETING → removed. Only a DeleteTable claim and the renewals of it
        // give a DELETING row a deadline, so a row here is a claim whose call
        // stopped, a call still running past its lease, or a removal by an
        // earlier pass that stopped part way.
        let query = format!(
            "SELECT account_id, table_name, table_arn, table_id, \
             toUnixTimestamp(status_transition_at) AS deadline_ms \
             FROM {catalog_keyspace}.tables \
             WHERE table_status = 'DELETING' AND status_transition_at <= toTimestamp(now()) \
             ALLOW FILTERING"
        );

        let result =
            self.session.query(&query).await.map_err(|e| {
                StorageError::Internal(format!("Failed to query DELETING tables: {e}"))
            })?;

        let body = result.response_body().map_err(|e| {
            StorageError::Internal(format!("Failed to parse DELETING tables response: {e}"))
        })?;

        let rows = body.into_rows().unwrap_or_default();

        for row in rows {
            let account_id: String = row
                .get_r_by_name("account_id")
                .map_err(|e| StorageError::Internal(format!("Failed to parse account_id: {e}")))?;
            let table_name: String = row
                .get_r_by_name("table_name")
                .map_err(|e| StorageError::Internal(format!("Failed to parse table_name: {e}")))?;
            let table_arn: String = row
                .get_r_by_name("table_arn")
                .map_err(|e| StorageError::Internal(format!("Failed to parse table_arn: {e}")))?;
            let table_id: String = row
                .get_r_by_name("table_id")
                .map_err(|e| StorageError::Internal(format!("Failed to parse table_id: {e}")))?;
            let deadline_ms: i64 = row
                .get_r_by_name("deadline_ms")
                .map_err(|e| StorageError::Internal(format!("Failed to parse deadline: {e}")))?;

            // Take the row from the deadline this pass read. The swap applies
            // for one actor; a row that another sweeper took, that its call
            // renewed, or that was removed or created again under the name
            // since the select is left alone.
            let removal = async {
                let Some(lease) = self
                    .renew_delete_lease(&account_id, &table_name, &table_id, deadline_ms)
                    .await?
                else {
                    return Ok(false);
                };
                // The call's removal, under this pass's lease. It returns false,
                // having deleted nothing keyed on the name, when the lease is
                // taken from this pass before the name-keyed deletes.
                self.remove_claimed_table(
                    &account_id,
                    &table_name,
                    &table_id,
                    &table_arn,
                    Some(lease),
                )
                .await
            };
            match removal.await {
                Ok(true) => transitions.push((table_name, "DELETING → deleted")),
                Ok(false) => {}
                // Logged and left, as the abandoned-restore sweep does, so one
                // row that cannot be removed does not stop the rows after it. A
                // later pass takes it again once this pass's lease has passed.
                Err(error) => tracing::warn!(
                    "Control-plane sweep: removing table {table_name} (account {account_id}): \
                     {error}; retrying on a later pass"
                ),
            }
        }

        Ok(transitions)
    }
}
