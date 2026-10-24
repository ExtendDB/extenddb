// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `delete_table` implementation for `PostgresEngine`.

use extenddb_core::types::{DeleteTableInput, TableDescription, TableStatus};
use extenddb_storage::error::StorageError;

use crate::PostgresEngine;
use crate::table_helpers::{IndexRow, TableRow};

impl PostgresEngine {
    /// Core implementation of `delete_table` (H-5).
    pub(crate) async fn delete_table_impl(
        &self,
        account_id: &str,
        input: DeleteTableInput,
    ) -> Result<TableDescription, StorageError> {
        Self::validate_account_id(account_id)?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        // Lock and fetch the row atomically with SELECT ... FOR UPDATE
        let row: Option<TableRow> = sqlx::query_as(
            r"SELECT table_name, key_schema, attribute_definitions, billing_mode,
                      provisioned_throughput, stream_specification, table_status,
                      EXTRACT(EPOCH FROM creation_date_time)::FLOAT8 as creation_epoch,
                      table_size_bytes, item_count, table_arn, table_id,
                      deletion_protection_enabled, stream_label,
                      table_class, sse_specification, on_demand_throughput
               FROM tables WHERE account_id = $1 AND table_name = $2 AND table_status IN ('ACTIVE', 'CREATING')
               FOR UPDATE",
        )
        .bind(account_id)
        .bind(&input.table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row = row.ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;

        // REQ: DeletionProtectionEnabled check — real DynamoDB returns ValidationException
        if row.deletion_protection_enabled {
            return Err(StorageError::DeletionProtected(row.table_arn.clone()));
        }

        // A restore target (CREATING with no scheduled transition) is being
        // filled by RestoreTableFromBackup. The service refuses to delete a
        // table it is still creating, and deleting this one would make the
        // restore fail part-way; refuse until the restore finishes. An
        // abandoned target is removed by the restore sweep, not here.
        if row.table_status == "CREATING" {
            let restoring: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tables WHERE table_id = $1 \
                 AND table_status = 'CREATING' AND status_transition_at IS NULL)",
            )
            .bind(&row.table_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            if restoring {
                return Err(StorageError::IndexesInUse(format!(
                    "Attempt to change a resource which is still in use: Table is being \
                     restored: {}",
                    row.table_name
                )));
            }
        }

        // Fetch indexes for the response description.
        let index_rows: Vec<IndexRow> = sqlx::query_as(
            r"SELECT index_name, index_type, key_schema, projection,
                      index_status, provisioned_throughput
               FROM indexes WHERE table_id = $1",
        )
        .bind(&row.table_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        // H-5 (delete): the control-plane delay decides whether the table is
        // removed before this call answers or by the poller later. Both paths
        // mark the row DELETING here and commit; the data tables are dropped
        // afterwards by the control-plane pass, which deletes the catalog row
        // only once the data transaction has committed. A server that dies
        // between the two leaves a DELETING row that the startup pass finishes,
        // instead of data tables that no catalog row names.
        let delay_row: (f64,) = sqlx::query_as(
            "SELECT COALESCE((SELECT value::FLOAT8 FROM settings WHERE key = 'control_plane_delay_seconds'), 0.25)",
        )
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        let delay_secs = delay_row.0;
        let immediate = delay_secs < 1.0;
        let scheduled_delay = if immediate { 0.0 } else { delay_secs };

        sqlx::query(
            r"UPDATE tables SET table_status = 'DELETING',
                status_transition_at = NOW() + make_interval(secs => $3)
               WHERE account_id = $1 AND table_name = $2",
        )
        .bind(account_id)
        .bind(&input.table_name)
        .bind(scheduled_delay)
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if immediate {
            // Finish the delete before answering. In the common case (no lock
            // held on the data tables) the name is free when the response
            // arrives, and a poller that may not run before the next request
            // leaves no stale DELETING row (mirrors create_table's H-5 fast
            // path). The exceptions, where the response is DELETING and the
            // poller finishes the drop: the drop waited past the pass's 3 s
            // lock timeout, or it was the victim of a deadlock with the GSI
            // propagation worker. The work runs in a spawned task, as
            // UpdateTable runs a GSI build: a client that times out drops this
            // future, and a drop that was waiting on a lock must not be rolled
            // back with it. The task, not this future, wakes the poller when the
            // table is not removed, so a client that is gone by then does not
            // leave the row to the poller's idle timeout. If this future is
            // dropped during the commit above and the commit landed, no task is
            // spawned and the row stays DELETING until the poller's idle sweep;
            // one round trip wide, same as the delayed path, no data at risk.
            let engine = self.clone();
            let table_id = row.table_id.clone();
            let table_name = input.table_name.clone();
            let task = tokio::spawn(async move {
                match engine.finish_table_delete(&table_id).await {
                    Ok(true) => return,
                    Ok(false) => {}
                    Err(e) => tracing::warn!(
                        "DeleteTable '{table_name}': could not finish the delete inline, the \
                         table remains DELETING and the control plane will retry: {e}"
                    ),
                }
                engine.control_plane_notify.notify_one();
            });
            if let Err(e) = task.await {
                tracing::warn!(
                    "DeleteTable '{}': the delete task ended early, the table remains DELETING \
                     and the control plane will retry: {e}",
                    input.table_name
                );
                self.control_plane_notify.notify_one();
            }
        } else {
            // F-3: Wake the control plane poller so it processes the DELETING →
            // removed transition without waiting for the idle timeout.
            self.control_plane_notify.notify_one();
        }

        // Build description from the fetched row data
        let desc = self.build_table_description_from_row(account_id, row, index_rows)?;

        Ok(TableDescription {
            table_status: TableStatus::Deleting,
            ..desc
        })
    }
}
