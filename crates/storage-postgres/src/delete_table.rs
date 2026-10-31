// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `delete_table` implementation for `PostgresEngine`.
//!
//! The synchronous path (`control_plane_delay_seconds` below 1 and no reader
//! on the table's relations) removes the catalog row and drops the data
//! relations in the request. On one shared database both go in one commit.
//! With a separate data database the drops are run first and held
//! uncommitted, the catalog removal commits, and the data transaction
//! commits after it ([`drop_on_data_database`]); a data commit that fails
//! then has the drop retried on fresh connections for
//! [`DATA_DROP_RETRY_BOUND`] before the request gives up. What is left
//! behind when it does, and how to remove it, is stated at
//! [`DATA_DROP_RETRY_BOUND`].

use std::time::{Duration, Instant};

use extenddb_core::types::{DeleteTableInput, TableDescription, TableStatus};
use extenddb_storage::error::StorageError;

use crate::PostgresEngine;
use crate::data::LockHolders;
use crate::table_helpers::{IndexRow, TableRow};

impl PostgresEngine {
    /// Core implementation of `delete_table` (H-5).
    #[allow(clippy::too_many_lines)]
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

        // Lock and fetch the row atomically with SELECT ... FOR UPDATE. A
        // DELETING row is fetched too: DeleteTable of a table that is already
        // being deleted returns its description, as the service does.
        let row: Option<TableRow> = sqlx::query_as(
            r"SELECT table_name, key_schema, attribute_definitions, billing_mode,
                      provisioned_throughput, stream_specification, table_status,
                      EXTRACT(EPOCH FROM creation_date_time)::FLOAT8 as creation_epoch,
                      table_size_bytes, item_count, table_arn, table_id,
                      deletion_protection_enabled, stream_label,
                      table_class, sse_specification, on_demand_throughput
               FROM tables WHERE account_id = $1 AND table_name = $2
                 AND table_status IN ('ACTIVE', 'CREATING', 'DELETING')
               FOR UPDATE",
        )
        .bind(account_id)
        .bind(&input.table_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        let row = row.ok_or_else(|| StorageError::TableNotFound(input.table_name.clone()))?;

        // Fetch indexes for the response description.
        let index_rows: Vec<IndexRow> = sqlx::query_as(
            r"SELECT index_name, index_id, index_type, key_schema, projection,
                      index_status, provisioned_throughput
               FROM indexes WHERE table_id = $1",
        )
        .bind(&row.table_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;

        if row.table_status == "DELETING" {
            // Already being removed (a deferred drop the control plane
            // retries, or a delay of a second or more): nothing to change.
            // The rollback releases the row lock.
            tx.rollback()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            let desc = self.build_table_description_from_row(account_id, row, index_rows)?;
            return Ok(TableDescription {
                table_status: TableStatus::Deleting,
                ..desc
            });
        }

        // REQ: DeletionProtectionEnabled check — real DynamoDB returns ValidationException
        if row.deletion_protection_enabled {
            return Err(StorageError::DeletionProtected(row.table_arn.clone()));
        }

        // A restore target (CREATING with no scheduled transition) is being
        // filled by RestoreTableFromBackup. The service refuses to delete a
        // table it is still creating, and deleting this one would make the
        // restore fail part-way; refuse until the restore finishes. An
        // abandoned target is removed by the reconciler, not here.
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

        // Vector index ids, read while the rows still exist. Deleting the table
        // row cascades them away, and each id names a data table that has to be
        // dropped afterwards: the same ordering the GSI path uses, and for the same
        // reason.
        let vector_index_ids: Vec<String> =
            sqlx::query_scalar("SELECT index_id FROM vector_indexes WHERE table_id = $1")
                .bind(&row.table_id)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;

        // H-5 (delete): synchronous control-plane shortcut when delay < 1s.
        // When control_plane_delay_seconds is small, the poller may not run
        // before the next request, causing stale DELETING rows. Synchronous
        // delete avoids this. Mirrors create_table's H-5 fast path.
        let delay_row: (f64,) = sqlx::query_as(
            "SELECT COALESCE((SELECT value::FLOAT8 FROM settings WHERE key = 'control_plane_delay_seconds'), $1)",
        )
        .bind(crate::DEFAULT_CONTROL_PLANE_DELAY_SECONDS)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
        let delay_secs = delay_row.0;

        let index_ids: Vec<String> = index_rows.iter().map(|r| r.index_id.clone()).collect();

        // Whether another session is reading the table. A `DROP TABLE`
        // waits behind any lock on the relation, so the synchronous path is
        // taken only when no session holds `ACCESS SHARE` on any relation the
        // drop touches (`table_storage_relations`, the list the drops
        // remove); otherwise the row is left `DELETING` and the
        // control-plane poller drops the tables once the reader is done
        // (`workers.rs`).
        //
        // Matched: `AccessShareLock` holders, which are the long readers: a
        // table snapshot (`snapshot_engine.rs`) and the in-catalog backup hold
        // it on the base data table for as long as they stream; a Query or
        // Scan with `IndexName` holds it on a GSI data table for the
        // statement; pg_dump and any plain `SELECT` or `LOCK ... IN ACCESS
        // SHARE MODE` hold it too.
        //
        // Not matched: every other mode, which PostgreSQL takes instead of
        // `ACCESS SHARE` for `SELECT ... FOR SHARE`/`FOR UPDATE`, for
        // `INSERT`/`UPDATE`/`DELETE`, for `LOCK ... IN SHARE MODE`, for an
        // index build or `VACUUM`: a client write transaction, a GSI backfill
        // batch (`gsi_build.rs` reads the base `FOR SHARE SKIP LOCKED` and
        // writes the index table), a queue worker's claim, a TTL index build.
        // Most are short, and the synchronous path waits for them, within
        // `SYNC_DROP_LOCK_TIMEOUT` for the drops; one that is not makes the
        // drops run out that budget, and the row is left `DELETING` as for a
        // reader, with the drops rolled back (below for each layout). The
        // queue claim is left unmatched on purpose: the pending-row delete
        // waits for it with no timeout.
        //
        // The probe runs only when its answer is used (the synchronous path
        // needs `delay_secs < 1.0`). On one shared pool it runs on the
        // connection this transaction already holds: taking a second one from
        // the pool while holding the first lets `pool_size` concurrent
        // DeleteTables hold every connection and wait out the acquire timeout
        // for a second. `pg_locks` is cluster-wide, and `current_database()`
        // is then the data database. With a separate data database it runs on
        // the data pool; every path acquires catalog then data, so that order
        // cannot form a cycle. `pid <> pg_backend_pid()` is defensive: this
        // transaction has touched only catalog tables, so it cannot hold a
        // lock on a data relation and no test can make it; the clause keeps a
        // future statement added above the probe from matching itself.
        let data_table_in_use = if delay_secs < 1.0 {
            let relations =
                crate::data::table_storage_relations(&row.table_id, &index_ids, &vector_index_ids);
            if self.shared_data_pool {
                crate::data::storage_in_use(&mut *tx, relations, LockHolders::Readers).await
            } else {
                crate::data::storage_in_use(&self.data_pool, relations, LockHolders::Readers).await
            }
            .map_err(|e| StorageError::Internal(e.to_string()))?
        } else {
            false
        };

        // Between the probe and the drops: the window in which a reader that
        // the probe did not see can take its lock.
        #[cfg(feature = "test-hooks")]
        test_pause::reached(test_pause::DeleteTablePoint::BeforeDrops).await;

        // Deleting the tables row cascades to indexes and stream rows via FK
        // CASCADE, and to vector index rows through
        // vector_indexes_table_id_fkey. The data tables need dropping, and
        // the cascade takes away the rows that name them, which is why the
        // index ids are collected before the catalog row goes. On either
        // layout nothing commits before every drop has its lock: a reader
        // the probe missed (an open between its snapshot and its `LOCK
        // TABLE`, a session that took its lock after the probe) or a holder
        // above makes a drop wait, `SYNC_DROP_LOCK_TIMEOUT` bounds the wait
        // for the whole group of drops, and a wait that fails
        // (`lock_wait_failed`) rolls the drops back and takes the
        // asynchronous path below on this transaction, with the row still
        // held `FOR UPDATE`. A caller cancelled before the commit leaves both
        // transactions to roll back: the table `ACTIVE` with its data.
        let mut data_commit: Option<sqlx::Transaction<'static, sqlx::Postgres>> = None;
        let removed = if delay_secs < 1.0 && !data_table_in_use {
            if self.shared_data_pool {
                // One database: the drops run in this transaction, behind a
                // savepoint, so the row and its data tables go in one commit
                // or not at all.
                remove_table_and_storage(
                    &mut tx,
                    &row.table_arn,
                    &row.table_id,
                    &index_ids,
                    &vector_index_ids,
                )
                .await?
            } else if let Some(data_tx) = drop_on_data_database(
                &self.data_pool,
                &row.table_id,
                &index_ids,
                &vector_index_ids,
            )
            .await?
            {
                // Separate data database: no transaction covers both. The
                // drops hold their locks, uncommitted, in `data_tx`; the
                // catalog removal is made here and the two commits follow,
                // catalog first, below.
                sqlx::query("DELETE FROM tags WHERE resource_arn = $1")
                    .bind(&row.table_arn)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                sqlx::query("DELETE FROM tables WHERE table_id = $1")
                    .bind(&row.table_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
                data_commit = Some(data_tx);
                true
            } else {
                false
            }
        } else {
            false
        };

        if removed {
            if let Some(data_tx) = data_commit {
                // Both commits, and the retries a failed data commit needs,
                // run in a task of their own, which the request awaits: a
                // caller cancelled from here on cannot stop between the
                // commits and leave the row gone with the data tables kept,
                // and cannot stop the retries either (dropping the request
                // detaches the task; it does not cancel it). The drops
                // already hold their locks, so the data commit does not
                // wait. Once the catalog commit is in, the row is gone
                // whatever happens next: a data commit that fails (the
                // connection terminated, a failover, a network fault) has
                // the drop redone on a fresh connection by
                // `drop_after_catalog_commit`, and only a process that dies,
                // or a data database unreachable for longer than
                // `DATA_DROP_RETRY_BOUND`, between the catalog commit and
                // the last retry leaves the data tables without a row.
                let data_pool = self.data_pool.clone();
                let table_id = row.table_id.clone();
                tokio::spawn(async move {
                    tx.commit()
                        .await
                        .map_err(|e| StorageError::Internal(e.to_string()))?;
                    #[cfg(feature = "test-hooks")]
                    test_pause::reached(test_pause::DeleteTablePoint::BetweenCommits).await;
                    match data_tx.commit().await {
                        Ok(()) => Ok(()),
                        Err(e) => {
                            drop_after_catalog_commit(
                                &data_pool,
                                &table_id,
                                &index_ids,
                                &vector_index_ids,
                                &e,
                            )
                            .await
                        }
                    }
                })
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))??;
            } else {
                tx.commit()
                    .await
                    .map_err(|e| StorageError::Internal(e.to_string()))?;
            }
        } else {
            // Async delete: set DELETING status with a scheduled removal time.
            // A drop deferred because the table is in use (`delay_secs < 1`)
            // is due at once: the table is already being removed, and a
            // reader that ends right after this has the poller's next pass
            // drop it rather than a pass after the delay.
            let schedule_secs = if delay_secs < 1.0 { 0.0 } else { delay_secs };
            sqlx::query(
                r"UPDATE tables SET table_status = 'DELETING',
                    status_transition_at = NOW() + make_interval(secs => $3)
                   WHERE account_id = $1 AND table_name = $2",
            )
            .bind(account_id)
            .bind(&input.table_name)
            .bind(schedule_secs)
            .execute(&mut *tx)
            .await
            .map_err(|e| StorageError::Internal(e.to_string()))?;
            tx.commit()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
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

/// The budget of the synchronous `DROP TABLE`s of one `DeleteTable`, and of
/// each drop the control plane retries (`worker_store.rs`). One deadline
/// for the whole group of drops (`data::drop_table_relations`), not a lock
/// timeout per `DROP`.
///
/// `DeleteTable`'s drops run only after the in-use probe found no `ACCESS
/// SHARE` holder, so what they usually wait on is short: a write
/// transaction, a GSI backfill batch, or an open between its snapshot and
/// its `LOCK TABLE`. One second covers one such batch or request at
/// ordinary latencies; a holder that keeps its lock longer (an open write
/// transaction, an index build, `VACUUM`) costs the request this second and
/// sends it down the asynchronous path. The bound that results for a
/// `DeleteTable` is the wait for the pending queue rows' claims, which has no
/// timeout by design (a claim is short, and the drop is meant to wait for
/// it), plus at most this budget for the drops. It is also the longest a
/// drop's queued `ACCESS EXCLUSIVE` request can stall other requests on the
/// table, and, after the claims, the longest this `DeleteTable` holds its
/// connections and the row `FOR UPDATE` before it gives up; giving up costs
/// only the asynchronous path.
pub(crate) const SYNC_DROP_LOCK_TIMEOUT: Duration = Duration::from_secs(1);

/// Separate data database: how long a `DeleteTable` whose data commit failed
/// after its catalog commit keeps retrying the drop of the table's data
/// relations ([`drop_after_catalog_commit`]). Attempts start until this much
/// time has passed since the failed commit; the last one may run on past it
/// for its own waits (the drop's `SYNC_DROP_LOCK_TIMEOUT`, or the data pool's
/// 30 s acquire timeout when the server does not answer at all).
///
/// One minute covers the outages a data database recovers from on its own
/// (a terminated or lost connection, a restart, a failover, each of which
/// the pool's own acquire timeout bounds at 30 s), and keeps the request,
/// which the client is still waiting on, from outliving the client's
/// patience by much. An outage longer than this is an incident an operator
/// is looking at, and the error log written then (the table id, the relation
/// names, and the `DROP TABLE` to run) is what they need to finish the
/// delete by hand.
///
/// What is left behind when the retries are exhausted, or when the process
/// dies between the catalog commit and the last retry: the table's data
/// relations (`_ddb_<table_id>`, one `_ddb_<index_id>` per GSI or LSI, one
/// `_ddb_vec_<index_id>` per vector index) on the data database with no
/// catalog row naming them, and possibly a `vector_index_holds` row for the
/// table id, which the hold sweep clears by age. Nothing in the server drops
/// such relations later: `DescribeTable` reports the table gone, a
/// `CreateTable` of the same name gets a new table id, and the control
/// plane enumerates relations from catalog rows only. They stay until an
/// operator drops them (`docs/troubleshooting.md`, "Data tables without a
/// catalog row"). On one shared database the row and the drops are one
/// commit, and none of this applies.
pub(crate) const DATA_DROP_RETRY_BOUND: Duration = Duration::from_secs(60);

/// The retry bound in force: [`DATA_DROP_RETRY_BOUND`], or the shorter one a
/// test set (`test-hooks` only).
fn data_drop_retry_bound() -> Duration {
    #[cfg(feature = "test-hooks")]
    if let Some(bound) = test_pause::shortened_data_drop_retry_bound() {
        return bound;
    }
    DATA_DROP_RETRY_BOUND
}

/// Separate data database: the catalog commit is in and the data commit
/// failed with `commit_error`, so the table's row is gone while its data
/// relations may still exist (a commit whose reply was lost may also have
/// gone through; nothing here can tell). Redo the drop on fresh connections
/// from `data` until it succeeds or [`DATA_DROP_RETRY_BOUND`] has passed.
///
/// Each attempt is the control plane's own drop
/// ([`PostgresEngine::drop_table_storage`]: a queue hold, the pending queue
/// rows, every relation, the hold, under `SYNC_DROP_LOCK_TIMEOUT`), whose
/// statements are all idempotent (`DROP TABLE IF EXISTS`, `DELETE ... WHERE
/// table_id = $1`, `INSERT ... ON CONFLICT DO NOTHING`), so repeating a drop
/// that did go through changes nothing. Every failure is retried, after a
/// backoff from 100 ms doubling to 5 s: a lock wait that fails
/// (`lock_wait_failed`) because a session took a lock after the first drop's
/// locks were released with its connection, a connection the pool cannot
/// open, a statement the server did not answer. On success the delete is
/// complete and the caller returns the description as usual. When the bound
/// is exhausted the relation names and the table id are logged at error with
/// the `DROP TABLE` that removes them, and the request fails with
/// `Internal`; the row is gone either way.
async fn drop_after_catalog_commit(
    data: &sqlx::PgPool,
    table_id: &str,
    index_ids: &[String],
    vector_index_ids: &[String],
    commit_error: &sqlx::Error,
) -> Result<(), StorageError> {
    let bound = data_drop_retry_bound();
    let started = Instant::now();
    let relations = crate::data::table_storage_relations(table_id, index_ids, vector_index_ids);
    tracing::warn!(
        table_id,
        ?relations,
        "DeleteTable: the catalog row is removed but the data commit failed ({commit_error}); \
         retrying the drop of the data relations for up to {bound:?}"
    );
    let mut backoff = Duration::from_millis(100);
    let mut attempts: u32 = 0;
    let last_error = loop {
        attempts += 1;
        match PostgresEngine::drop_table_storage(
            data,
            table_id,
            index_ids,
            vector_index_ids,
            SYNC_DROP_LOCK_TIMEOUT,
        )
        .await
        {
            Ok(()) => {
                tracing::info!(
                    table_id,
                    "DeleteTable: the data relations were dropped on attempt {attempts}, \
                     {:?} after the failed data commit",
                    started.elapsed()
                );
                return Ok(());
            }
            Err(e) => {
                let elapsed = started.elapsed();
                if elapsed >= bound {
                    break e;
                }
                tokio::time::sleep(backoff.min(bound.saturating_sub(elapsed))).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
    };
    let drop_statement = format!("DROP TABLE IF EXISTS {};", relations.join(", "));
    tracing::error!(
        table_id,
        ?relations,
        "DeleteTable: the catalog row of table {table_id} is removed but its data relations \
         could not be dropped in {attempts} attempt(s) over {:?} ({last_error}); they stay on \
         the data database until dropped by hand: {drop_statement}",
        started.elapsed()
    );
    Err(StorageError::Internal(format!(
        "DeleteTable: table {table_id} is removed from the catalog but its data relations \
         {relations:?} could not be dropped ({last_error})"
    )))
}

/// Separate data database: begin a data transaction and run the table's
/// storage drop in it, leaving it uncommitted for the caller to commit after
/// the catalog removal. `Ok(Some(tx))`: every drop holds its lock. `Ok(None)`:
/// a lock wait failed (`data::lock_wait_failed`), the data transaction is
/// rolled back, and the caller takes the asynchronous path. Any other failure
/// is returned, and the data transaction rolls back when it is dropped.
///
/// Nothing commits here, so a failure up to and including the catalog commit
/// leaves the table as it was. After the catalog commit the row is gone, and
/// a data commit that fails for any reason (not only a process death) is
/// followed by [`drop_after_catalog_commit`]'s retries; what a process death
/// or an outage longer than [`DATA_DROP_RETRY_BOUND`] leaves behind is
/// stated there.
///
/// The order is `remove_table_and_storage`'s: the pending queue rows first,
/// with no timeout (a claim in flight is short and the drop waits for it, as
/// the control plane's drop does), then the relations, base table first, by
/// one deadline `SYNC_DROP_LOCK_TIMEOUT` away. Unlike the control plane's
/// drop (`PostgresEngine::drop_table_storage`), no queue hold is committed
/// first. The hold stops new claims for the table, and none is needed here:
/// a claim taken after the pending rows are deleted holds a base row `FOR
/// SHARE` before it writes an index table, so it either finishes before the
/// base drop is granted (the drop waits for it within the deadline) or waits
/// for the drop, holding nothing the drop needs; it cannot hold an index
/// table the drop is waiting for, because the base table is dropped first.
/// Committing a hold would also be the one write here that outlives a
/// cancelled request, and a hold left behind stops the table's queue until
/// the recovery sweep removes it ten minutes later.
async fn drop_on_data_database(
    data: &sqlx::PgPool,
    table_id: &str,
    index_ids: &[String],
    vector_index_ids: &[String],
) -> Result<Option<sqlx::Transaction<'static, sqlx::Postgres>>, StorageError> {
    let mut data_tx = data
        .begin()
        .await
        .map_err(|e| StorageError::Internal(e.to_string()))?;
    let dropped = async {
        crate::data::delete_pending_queue_rows(&mut data_tx, table_id).await?;
        let deadline = std::time::Instant::now() + SYNC_DROP_LOCK_TIMEOUT;
        crate::data::drop_table_relations(
            &mut data_tx,
            table_id,
            index_ids,
            vector_index_ids,
            deadline,
        )
        .await
    }
    .await;
    match dropped {
        Ok(()) => Ok(Some(data_tx)),
        Err(e) if crate::data::lock_wait_failed(&e) => {
            tracing::info!(
                table_id,
                "DeleteTable: a session holds the table's storage ({e}); leaving it DELETING \
                 for the control plane to drop"
            );
            data_tx
                .rollback()
                .await
                .map_err(|e| StorageError::Internal(e.to_string()))?;
            Ok(None)
        }
        Err(e) => Err(StorageError::Internal(e.to_string())),
    }
}

/// One shared database: drop the table's storage and remove its tags and
/// catalog row in `tx`, behind a savepoint. `Ok(true)`: done, for the caller
/// to commit. `Ok(false)`: a lock wait failed (`data::lock_wait_failed`:
/// the drops ran out `SYNC_DROP_LOCK_TIMEOUT`, were chosen as a deadlock
/// victim, or hit a `statement_timeout`) and everything since the savepoint
/// is rolled back, so the caller can take the asynchronous path on the same
/// transaction with the row still held `FOR UPDATE`. Any other failure is
/// returned, and the caller's rollback leaves the table as it was. The row
/// and the drops are one commit, so on this layout no failure at any point
/// leaves a data relation without its row (the separate layout's exposure is
/// at [`DATA_DROP_RETRY_BOUND`]).
///
/// The order is `drop_on_data_database`'s, and for the same reason no queue
/// hold is committed first (nothing here commits before the row goes): the
/// pending queue rows first, which waits for the claims in flight to commit
/// with no timeout (a caller cancelled in that wait has committed nothing),
/// then the relations, base table first, by one deadline
/// `SYNC_DROP_LOCK_TIMEOUT` away.
async fn remove_table_and_storage(
    tx: &mut sqlx::PgConnection,
    table_arn: &str,
    table_id: &str,
    index_ids: &[String],
    vector_index_ids: &[String],
) -> Result<bool, StorageError> {
    let internal = |e: sqlx::Error| StorageError::Internal(e.to_string());
    sqlx::query("SAVEPOINT delete_table_storage")
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
    match remove_after_savepoint(tx, table_arn, table_id, index_ids, vector_index_ids).await {
        Ok(()) => {
            sqlx::query("RELEASE SAVEPOINT delete_table_storage")
                .execute(&mut *tx)
                .await
                .map_err(internal)?;
            Ok(true)
        }
        Err(e) if crate::data::lock_wait_failed(&e) => {
            tracing::info!(
                table_id,
                "DeleteTable: a session holds the table's storage ({e}); leaving it DELETING \
                 for the control plane to drop"
            );
            sqlx::query("ROLLBACK TO SAVEPOINT delete_table_storage")
                .execute(&mut *tx)
                .await
                .map_err(internal)?;
            Ok(false)
        }
        Err(e) => Err(internal(e)),
    }
}

async fn remove_after_savepoint(
    tx: &mut sqlx::PgConnection,
    table_arn: &str,
    table_id: &str,
    index_ids: &[String],
    vector_index_ids: &[String],
) -> Result<(), sqlx::Error> {
    crate::data::delete_pending_queue_rows(tx, table_id).await?;
    let deadline = std::time::Instant::now() + SYNC_DROP_LOCK_TIMEOUT;
    crate::data::drop_table_relations(tx, table_id, index_ids, vector_index_ids, deadline).await?;
    sqlx::query("DELETE FROM tags WHERE resource_arn = $1")
        .bind(table_arn)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM tables WHERE table_id = $1")
        .bind(table_id)
        .execute(&mut *tx)
        .await?;
    Ok(())
}

/// Test hooks: hold the next `DeleteTable` at a chosen point, and shorten
/// the retry bound of a drop redone after the catalog commit. Compiled only
/// with `test-hooks`.
#[cfg(feature = "test-hooks")]
pub(crate) mod test_pause {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use extenddb_storage::test_pause::{Pause, PausePoint};

    /// Where a `DeleteTable` can be held.
    #[doc(hidden)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum DeleteTablePoint {
        /// Between the in-use probe and the drops, so a test can take a lock
        /// on the table in the window a reader the probe missed has.
        BeforeDrops,
        /// Separate data database only: between the catalog commit and the
        /// data commit of the synchronous path, so a test can make the data
        /// commit fail with the row already gone.
        BetweenCommits,
    }

    static PAUSE: PausePoint<DeleteTablePoint> = PausePoint::new();

    /// A pause armed by [`arm_delete_table_pause`] or
    /// [`arm_delete_table_pause_at`]. Dropping it disarms the pause and
    /// releases a `DeleteTable` waiting at it.
    #[doc(hidden)]
    pub type DeleteTablePause = Pause<DeleteTablePoint>;

    /// Arm a pause for the next `DeleteTable` in this process that reaches
    /// the point between its probe and its drops (one call, once), whatever
    /// its engine or table. Process-global: a test that arms it holds the
    /// snapshot hooks lock, as every test in `tests/table_snapshot.rs` does.
    ///
    /// # Panics
    ///
    /// In debug builds, when a pause is already armed ([`PausePoint::arm`]).
    #[doc(hidden)]
    #[must_use]
    pub fn arm_delete_table_pause() -> DeleteTablePause {
        arm_delete_table_pause_at(DeleteTablePoint::BeforeDrops)
    }

    /// Arm a pause for the next `DeleteTable` in this process that reaches
    /// `point`; otherwise as [`arm_delete_table_pause`].
    ///
    /// # Panics
    ///
    /// In debug builds, when a pause is already armed ([`PausePoint::arm`]).
    #[doc(hidden)]
    #[must_use]
    pub fn arm_delete_table_pause_at(point: DeleteTablePoint) -> DeleteTablePause {
        PAUSE.arm(point)
    }

    pub(crate) async fn reached(point: DeleteTablePoint) {
        PAUSE.reached(point).await;
    }

    /// Milliseconds; 0 means the production bound.
    static DATA_DROP_RETRY_BOUND_MS: AtomicU64 = AtomicU64::new(0);

    /// The shortened bound set by [`shorten_data_drop_retry_bound_for_tests`]
    /// while its guard lives.
    pub(crate) fn shortened_data_drop_retry_bound() -> Option<Duration> {
        match DATA_DROP_RETRY_BOUND_MS.load(Ordering::Relaxed) {
            0 => None,
            ms => Some(Duration::from_millis(ms)),
        }
    }

    /// Restores the production retry bound when dropped.
    #[doc(hidden)]
    pub struct DataDropRetryBound(());

    impl Drop for DataDropRetryBound {
        fn drop(&mut self) {
            DATA_DROP_RETRY_BOUND_MS.store(0, Ordering::Relaxed);
        }
    }

    /// Make every `DeleteTable` in this process whose data commit fails
    /// after its catalog commit give up after `bound` instead of
    /// `DATA_DROP_RETRY_BOUND`, until the guard is dropped. Process-global,
    /// under the snapshot hooks lock like the pauses.
    #[doc(hidden)]
    #[must_use]
    pub fn shorten_data_drop_retry_bound_for_tests(bound: Duration) -> DataDropRetryBound {
        let ms = u64::try_from(bound.as_millis()).unwrap_or(u64::MAX).max(1);
        DATA_DROP_RETRY_BOUND_MS.store(ms, Ordering::Relaxed);
        DataDropRetryBound(())
    }
}
