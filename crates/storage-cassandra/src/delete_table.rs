// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `delete_table` implementation for `CassandraEngine`.

use crate::cassandra_util::PolicySession as _;
use crate::cassandra_util::Stmt;
use cdrs_tokio::types::IntoRustByName;
use extenddb_core::types::{DeleteTableInput, TableDescription, TableStatus};
use extenddb_storage::error::StorageError;

use crate::CassandraEngine;

/// How far ahead of a `DeleteTable` claim, or of a renewal of it, the row's
/// `status_transition_at` is set.
///
/// The control-plane sweep (`process_control_plane_transitions`) selects a
/// `DELETING` row only once that deadline has passed, and takes it with
/// [`CassandraEngine::renew_delete_lease`] from the deadline it read. Until
/// then the row is left to the call that claimed it. When a statement of the
/// removal fails after the claim or the take, the actor moves the deadline to
/// the current time (`release_delete_lease`), so the sweep's next pass takes
/// the row. A call whose process stops after the claim leaves the row to the
/// sweep, which removes the table after the deadline; with no deadline the
/// sweep would never select the row, and the name would stay taken. A
/// removal that stops part way, the call's or the sweep's, keeps the catalog
/// row, since the row delete is its last statement, and a later pass takes
/// the row over once the deadline written by the last claim, renewal or
/// release has passed.
///
/// The length decides how long a stopped call keeps the name refused, and
/// how slow a call can be before the sweep takes the row over and runs the
/// remaining drops a second time. The renewal before the name-keyed deletes
/// is what keeps two actors apart: an actor whose renewal does not apply
/// leaves those deletes and the row delete to the actor that holds the row.
const DELETE_CLAIM_LEASE_SECS: i64 = 300;

fn lease_deadline_ms() -> i64 {
    (chrono::Utc::now() + chrono::Duration::seconds(DELETE_CLAIM_LEASE_SECS)).timestamp_millis()
}

/// Who is deleting a table, which decides what the delete refuses.
#[derive(Clone, Copy)]
pub(crate) enum DeleteTableCaller<'a> {
    /// DeleteTable: refuses a deletion-protected table and a restore target
    /// still being restored.
    Client,
    /// The restore copy or the reconciler removing a restore target. Deletes only the incarnation with this `table_id`, so a target
    /// that was already removed and whose name was reused is left alone.
    RestoreCleanup { table_id: &'a str },
}

/// The refusal for DeleteTable on a table being restored. Same text as the
/// MongoDB backend's.
pub(crate) fn table_being_restored(table_name: &str) -> StorageError {
    StorageError::IndexesInUse(format!(
        "Attempt to change a resource which is still in use: Table is being restored: {table_name}"
    ))
}

impl CassandraEngine {
    pub(crate) async fn delete_table_impl(
        &self,
        account_id: &str,
        input: DeleteTableInput,
    ) -> Result<TableDescription, StorageError> {
        let description = self
            .delete_table_as(account_id, input, DeleteTableCaller::Client)
            .await?;
        // The row this call claimed is DELETING while the drops below it run,
        // which is the status the service reports in the DeleteTable response
        // and the status the other three backends report.
        Ok(TableDescription {
            table_status: TableStatus::Deleting,
            ..description
        })
    }

    /// Claims the catalog row for `DeleteTable`: sets `table_status` to
    /// `DELETING` and `status_transition_at` to [`DELETE_CLAIM_LEASE_SECS`]
    /// from now, if the row still holds `table_id` and is not `DELETING`
    /// already. Returns whether the claim applied; it does not when another
    /// `DeleteTable` holds the row or the row is gone.
    ///
    /// The table id in the condition keeps a stalled call from claiming a
    /// table created into the same name after the one it read was gone. The
    /// condition is the one the MongoDB backend's conditioned mark uses.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Internal`] when the lightweight transaction
    /// fails or its answer cannot be read.
    #[doc(hidden)]
    pub async fn claim_table_for_delete(
        &self,
        account_id: &str,
        table_name: &str,
        table_id: &str,
    ) -> Result<bool, StorageError> {
        Ok(self
            .claim_delete_lease(account_id, table_name, table_id)
            .await?
            .is_some())
    }

    /// [`Self::claim_table_for_delete`], returning the deadline it wrote, or
    /// `None` when the claim did not apply.
    pub(crate) async fn claim_delete_lease(
        &self,
        account_id: &str,
        table_name: &str,
        table_id: &str,
    ) -> Result<Option<i64>, StorageError> {
        let lease_until = lease_deadline_ms();
        let claim = format!(
            "UPDATE {}.tables \
             SET table_status = 'DELETING', status_transition_at = ? \
             WHERE account_id = ? AND table_name = ? \
             IF table_id = ? AND table_status != 'DELETING'",
            self.catalog_keyspace()
        );
        let result = crate::cassandra_util::query_lwt(
            &self.session,
            &claim,
            cdrs_tokio::query_values!(lease_until, account_id, table_name, table_id),
        )
        .await?;
        Ok(crate::cassandra_util::lwt_applied(&result)?.then_some(lease_until))
    }

    /// Moves a claimed row's deadline from `held_ms` to
    /// [`DELETE_CLAIM_LEASE_SECS`] from now, if the row still holds
    /// `table_id`, still reads `DELETING` and still carries `held_ms`.
    /// Returns the new deadline, or `None` when the swap did not apply.
    ///
    /// The control-plane sweep takes a row with it, from the deadline its
    /// select returned; an actor renews its own claim with it before the
    /// name-keyed deletes. Whoever's swap applies holds the row until the new
    /// deadline. A sweeper whose select is stale, a second sweeper, and a call
    /// whose row was taken over all find the deadline changed (or the row
    /// gone, or holding another table id) and stop.
    #[doc(hidden)]
    pub async fn renew_delete_lease(
        &self,
        account_id: &str,
        table_name: &str,
        table_id: &str,
        held_ms: i64,
    ) -> Result<Option<i64>, StorageError> {
        let lease_until = lease_deadline_ms();
        Ok(self
            .swap_delete_deadline(account_id, table_name, table_id, held_ms, lease_until)
            .await?
            .then_some(lease_until))
    }

    /// Hands a row this actor holds back to the control-plane sweep after a
    /// statement of its removal failed: moves the deadline `held_ms` that the
    /// actor's claim, take or last renewal wrote to the current time, with the
    /// compare-and-set the renewal uses, so the sweep's next pass selects the
    /// row and takes it. The swap does not apply when the row was taken over
    /// or removed since, and then there is nothing to hand back. A swap that
    /// fails is logged, and the row waits for `held_ms` as it does when the
    /// actor's process stops; so does a row whose renewal applied but whose
    /// answer was lost, since its deadline is then not `held_ms`.
    async fn release_delete_lease(
        &self,
        account_id: &str,
        table_name: &str,
        table_id: &str,
        held_ms: i64,
    ) {
        let now_ms = chrono::Utc::now().timestamp_millis();
        if let Err(error) = self
            .swap_delete_deadline(account_id, table_name, table_id, held_ms, now_ms)
            .await
        {
            tracing::warn!(
                "Removing table {table_name} (account {account_id}): handing the row to the \
                 control-plane sweep failed: {error}; the sweep takes it after its deadline"
            );
        }
    }

    /// The compare-and-set of [`Self::renew_delete_lease`] and
    /// [`Self::release_delete_lease`]: sets the row's deadline to `until_ms`
    /// if the row still holds `table_id`, still reads `DELETING` and still
    /// carries `held_ms`. Returns whether it applied.
    async fn swap_delete_deadline(
        &self,
        account_id: &str,
        table_name: &str,
        table_id: &str,
        held_ms: i64,
        until_ms: i64,
    ) -> Result<bool, StorageError> {
        let swap = format!(
            "UPDATE {}.tables SET status_transition_at = ? \
             WHERE account_id = ? AND table_name = ? \
             IF table_id = ? AND table_status = 'DELETING' AND status_transition_at = ?",
            self.catalog_keyspace()
        );
        let result = crate::cassandra_util::query_lwt(
            &self.session,
            &swap,
            cdrs_tokio::query_values!(until_ms, account_id, table_name, table_id, held_ms),
        )
        .await?;
        crate::cassandra_util::lwt_applied(&result)
    }

    pub(crate) async fn delete_table_as(
        &self,
        account_id: &str,
        input: DeleteTableInput,
        caller: DeleteTableCaller<'_>,
    ) -> Result<TableDescription, StorageError> {
        let catalog_keyspace = self.catalog_keyspace();

        // Fetch table metadata before deletion
        let table_query = format!(
            "SELECT * FROM {catalog_keyspace}.tables WHERE account_id = ? AND table_name = ?"
        );

        let table_result = self
            .session
            .run(
                Stmt::Read,
                &table_query,
                cdrs_tokio::query_values!(account_id, input.table_name.as_str()),
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Query table: {e}")))?;

        let table_body = table_result
            .response_body()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let table_rows = table_body
            .into_rows()
            .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;

        let table_row = table_rows
            .first()
            .ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;

        let table_id: String = table_row
            .get_r_by_name("table_id")
            .map_err(|e| StorageError::Internal(format!("Parse table_id: {e}")))?;

        match caller {
            DeleteTableCaller::Client => {
                // A DELETING row belongs to the deletion already under way
                // (a concurrent DeleteTable, or the removal of a restore target).
                // The service refuses a second DeleteTable in that window as a
                // table still in use (measured 2026-10-24); checked before
                // deletion protection, as the SQL backends order it.
                let status: String =
                    crate::cassandra_util::get_column(table_row, "table_status", "delete_table")?;
                if status == "DELETING" {
                    return Err(StorageError::TableBeingDeleted(input.table_name.clone()));
                }

                // Check deletion protection
                let deletion_protection: bool = table_row
                    .get_r_by_name("deletion_protection_enabled")
                    .unwrap_or(false);

                if deletion_protection {
                    let table_arn: String = table_row
                        .get_r_by_name("table_arn")
                        .map_err(|e| StorageError::Internal(format!("Parse table_arn: {e}")))?;
                    return Err(StorageError::DeletionProtected(table_arn));
                }

                // A restore target is owned by its copy until the copy
                // activates it (or the reconciler removes it). Deleting it mid-copy
                // would race the copy's writes into dropped tables.
                let restoring_from: Option<String> = table_row
                    .get_by_name("restore_source_backup_arn")
                    .ok()
                    .flatten();
                if status == "CREATING" && restoring_from.is_some() {
                    return Err(table_being_restored(&input.table_name));
                }
            }
            DeleteTableCaller::RestoreCleanup { table_id: expected } => {
                if table_id != expected {
                    return Err(StorageError::TableNotFound(input.table_name.clone()));
                }
            }
        }

        // The response is built before the claim: these two steps only read,
        // so a failure here leaves the table as it was rather than claimed
        // for a removal the client was told failed.
        let index_query = format!("SELECT * FROM {catalog_keyspace}.indexes WHERE table_id = ?");

        let index_result = self
            .session
            .run(
                Stmt::Read,
                &index_query,
                cdrs_tokio::query_values!(table_id.as_str()),
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Query indexes: {e}")))?;

        let index_body = index_result
            .response_body()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        let index_rows = index_body.into_rows().unwrap_or_default();

        let description =
            self.build_table_description_from_row(account_id, table_row, index_rows)?;

        // The claim and the removal run in a task of their own, which the call
        // awaits, as the PostgreSQL backend's drops do. A request dropped from
        // here on (its client closed the connection) detaches the task, so it
        // does not stop the removal after the claim and leave the row to the
        // lease.
        let claim = matches!(caller, DeleteTableCaller::Client);
        let engine = self.clone();
        let account_id = account_id.to_owned();
        let table_name = input.table_name;
        let table_arn = description.table_arn.clone();
        tokio::spawn(async move {
            // Claim the row before the first drop. Two concurrent calls both
            // read the same row; the claim applies for one of them and the
            // other answers TableBeingDeleted, as the service answers the
            // second of two concurrent calls (measured 2026-10-25, five
            // rounds of five).
            let lease = if claim {
                Some(
                    engine
                        .claim_delete_lease(&account_id, &table_name, &table_id)
                        .await?
                        .ok_or_else(|| StorageError::TableBeingDeleted(table_name.clone()))?,
                )
            } else {
                None
            };

            // When the renewal inside does not apply, this call outlived its
            // lease and the control-plane sweep took the row over; the sweep
            // finishes the removal and the call answers with the description
            // all the same.
            let removed = engine
                .remove_claimed_table(&account_id, &table_name, &table_id, &table_arn, lease)
                .await;
            if removed.is_err() && lease.is_some() {
                // The failed removal handed the row back to the sweep. The
                // wake has the poller start its next pass at once, and that
                // pass takes the row and finishes the removal. A failure
                // inside the sweep does not wake the poller: the poller runs
                // that pass, and a wake from inside it would keep it polling
                // every second for as long as the failure lasts.
                engine.control_plane_notify.notify_one();
            }
            removed
        })
        .await
        .map_err(|e| StorageError::Internal(format!("DeleteTable task: {e}")))??;
        Ok(description)
    }

    /// Removes the table `table_id` whose catalog row this actor holds:
    /// `lease` is the deadline its claim (a `DeleteTable` call) or its take
    /// (the control-plane sweep) wrote, and `None` for a restore target, which
    /// carries no deadline. Returns `false`, having changed nothing keyed on
    /// the name, when the renewal of `lease` does not apply.
    ///
    /// Every statement up to the renewal is keyed on the table id and can run
    /// twice: the base data table and the index data tables are dropped `IF
    /// EXISTS`, the TTL entries are cleared, and the index metadata goes after
    /// the index tables, so a removal that stops part way leaves the ids the
    /// next attempt needs. The tags are keyed on the table ARN, which carries
    /// no table id. They are deleted last, after the renewal, while the
    /// catalog row still holds the name, so a `CreateTable` of the name is
    /// refused until the row delete releases it, and a tag write that landed
    /// during the drops is removed with them. The renewal is the fence: an
    /// actor whose lease was taken over, or whose row was removed or created
    /// again under the name, stops before that delete. It holds while the two
    /// statements after the renewal finish within the lease it wrote.
    ///
    /// When a statement fails, the actor hands the row back to the sweep
    /// (`release_delete_lease`) before it returns the error, so the sweep's
    /// next pass runs the removal again without waiting for the lease.
    pub(crate) async fn remove_claimed_table(
        &self,
        account_id: &str,
        table_name: &str,
        table_id: &str,
        table_arn: &str,
        lease: Option<i64>,
    ) -> Result<bool, StorageError> {
        let mut held = lease;
        let removal = self
            .run_removal(account_id, table_name, table_id, table_arn, &mut held)
            .await;
        if removal.is_err()
            && let Some(held) = held
        {
            self.release_delete_lease(account_id, table_name, table_id, held)
                .await;
        }
        removal
    }

    /// The statements of [`Self::remove_claimed_table`]. `held` is the
    /// deadline the actor holds, which the renewal moves.
    async fn run_removal(
        &self,
        account_id: &str,
        table_name: &str,
        table_id: &str,
        table_arn: &str,
        held: &mut Option<i64>,
    ) -> Result<bool, StorageError> {
        let catalog_keyspace = self.catalog_keyspace();
        // Catalog metadata and user data intentionally live in different
        // keyspaces.
        let account_keyspace = self.account_keyspace(account_id);

        // The base table is dropped *before* the TTL queue is cleared. Clearing
        // first would leave a table that still holds items with no expiration
        // entries and nothing to rebuild them if the drop below fails.
        self.drop_data_table(&account_keyspace, table_id).await?;
        self.clear_ttl_entries_for_table_id(account_id, table_id)
            .await?;
        crate::data::index::delete_indexes_for_table(
            &self.session_arc(),
            &catalog_keyspace,
            &account_keyspace,
            table_id,
            self,
        )
        .await?;

        if let Some(lease) = *held {
            let Some(renewed) = self
                .renew_delete_lease(account_id, table_name, table_id, lease)
                .await?
            else {
                return Ok(false);
            };
            *held = Some(renewed);
        }

        // Delete tags for this table so they don't survive recreation under the same name.
        let delete_tags_query =
            format!("DELETE FROM {catalog_keyspace}.tags WHERE resource_arn = ?");
        self.session
            .run(
                Stmt::Write,
                &delete_tags_query,
                cdrs_tokio::query_values!(table_arn),
            )
            .await
            .map_err(|e| StorageError::Internal(format!("Delete tags: {e}")))?;

        // Delete table catalog entry, only if it is still this incarnation. A
        // lightweight transaction, to avoid mixing plain and LWT writes on a
        // row created with IF NOT EXISTS. See ADR-0021.
        let delete_table_query = format!(
            "DELETE FROM {catalog_keyspace}.tables WHERE account_id = ? AND table_name = ? IF table_id = ?"
        );
        crate::cassandra_util::query_lwt(
            &self.session,
            &delete_table_query,
            cdrs_tokio::query_values!(account_id, table_name, table_id),
        )
        .await
        .map_err(|e| StorageError::Internal(format!("Delete table: {e}")))?;

        Ok(true)
    }
}
