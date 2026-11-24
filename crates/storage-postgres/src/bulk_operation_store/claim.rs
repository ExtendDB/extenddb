// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The lifecycle writes for the `PostgreSQL` `BulkOperationStore`: cancel,
//! the runnable claim with `FOR UPDATE SKIP LOCKED` and lease renewal, the
//! checkpoint that advances the cursor and persists report records, and the
//! terminal finish.

use super::*;

pub(super) fn request_cancel<'a>(
    engine: &'a PostgresEngine,
    account_id: &str,
    operation_id: &str,
) -> BoxFuture<'a, Result<CancelOutcome, StorageError>> {
    let account_id = account_id.to_owned();
    let operation_id = operation_id.to_owned();
    Box::pin(async move {
        // One transaction for the conditional update and the fallback
        // read, matching SQLite: a purge landing between the two
        // statements must not turn AlreadyTerminal into NotFound.
        let mut tx = begin(&engine.pool).await?;
        // One conditional update, so the answer is never a racy pre-read.
        let updated: Option<(String,)> = sqlx::query_as(
            "UPDATE bulk_operations SET cancel_requested = TRUE, status = 'CANCELLING' \
             WHERE account_id = $1 AND operation_id = $2 \
               AND status IN ('IN_PROGRESS', 'CANCELLING') \
               RETURNING status",
        )
        .bind(&account_id)
        .bind(&operation_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?;
        if updated.is_some() {
            tx.commit().await.map_err(internal)?;
            return Ok(CancelOutcome::Cancelling);
        }
        let current: Option<(String,)> = sqlx::query_as(
            "SELECT status FROM bulk_operations WHERE account_id = $1 AND operation_id = $2",
        )
        .bind(&account_id)
        .bind(&operation_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?;
        tx.commit().await.map_err(internal)?;
        Ok(match current {
            Some((status,)) => CancelOutcome::AlreadyTerminal(status),
            None => CancelOutcome::NotFound,
        })
    })
}

pub(super) fn claim_runnable<'a>(
    engine: &'a PostgresEngine,
    owner: &str,
    lease: Duration,
    max: usize,
    exclude_accounts: &[String],
    exclude_operations: &[String],
) -> BoxFuture<'a, Result<Vec<BulkOperationRecord>, StorageError>> {
    let owner = owner.to_owned();
    let exclude = exclude_accounts.to_vec();
    let exclude_ops = exclude_operations.to_vec();
    Box::pin(async move {
        // The end time of a row the claim ends as unreadable. Leases are
        // not read or written on this clock: the statements below take
        // them from the database's clock, so processes on hosts whose
        // clocks disagree agree on when a lease runs out.
        let now = now_ms();
        let lease_ms = lease_millis(lease);
        let max = i64::try_from(max).unwrap_or(i64::MAX);
        // A claim that finds nothing to claim takes no write lock and opens
        // no write transaction: this read selects on the predicate the
        // update below selects on, and an idle server answers from it on
        // every tick. A row that becomes runnable after it is claimed on the
        // next tick.
        let runnable: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM bulk_operations \
                WHERE status IN ('IN_PROGRESS', 'CANCELLING') \
                  AND (lease_until_ms IS NULL \
                       OR lease_until_ms < (extract(epoch FROM clock_timestamp()) * 1000)::bigint) \
                  AND account_id <> ALL($1) \
                  AND operation_id <> ALL($2))",
        )
        .bind(&exclude)
        .bind(&exclude_ops)
        .fetch_one(&engine.pool)
        .await
        .map_err(internal)?;
        if !runnable {
            return Ok(Vec::new());
        }
        // SKIP LOCKED keeps two claiming processes from serialising on
        // each other's candidate rows: each takes what the other has not
        // locked, and no operation is handed to both. `<> ALL` against an
        // empty array is true for every row, so neither exclusion needs
        // branching. The lease and the ending of any row that does not
        // decode commit together, so a batch never leases a row it does
        // not return.
        let mut tx = begin(&engine.pool).await?;
        let rows = sqlx::query(
            "WITH candidates AS (\
               SELECT account_id, operation_id FROM bulk_operations \
                WHERE status IN ('IN_PROGRESS', 'CANCELLING') \
                  AND (lease_until_ms IS NULL \
                       OR lease_until_ms < (extract(epoch FROM clock_timestamp()) * 1000)::bigint) \
                  AND account_id <> ALL($1) \
                  AND operation_id <> ALL($2) \
                ORDER BY start_time_ms, operation_id LIMIT $3 \
                FOR UPDATE SKIP LOCKED) \
             UPDATE bulk_operations b SET lease_owner = $4, \
                 lease_until_ms = (extract(epoch FROM clock_timestamp()) * 1000)::bigint + $5 \
               FROM candidates c \
              WHERE b.account_id = c.account_id AND b.operation_id = c.operation_id \
             RETURNING b.*",
        )
        .bind(&exclude)
        .bind(&exclude_ops)
        .bind(max)
        .bind(&owner)
        .bind(lease_ms)
        .fetch_all(&mut *tx)
        .await
        .map_err(internal)?;
        // Each row decodes on its own. A row that does not decode is ended
        // in this transaction rather than failing the claim, which would
        // return none of the rows it had just leased.
        let mut records = Vec::with_capacity(rows.len());
        for row in &rows {
            let key = unreadable::row_key(row)?;
            let record = match unreadable::decode_or_log(row, &key, "the claim ends it") {
                Some(record) => {
                    super::files::with_files_or_log(&mut tx, record, "the claim ends it").await?
                }
                None => None,
            };
            match record {
                Some(record) => records.push(record),
                None => unreadable::fail_unreadable(&mut tx, &key, now).await?,
            }
        }
        tx.commit().await.map_err(internal)?;
        // The order of UPDATE ... RETURNING output is unspecified and
        // tracks the join plan; the CTE's ORDER BY only chooses which
        // rows LIMIT takes. Sort so callers receive the claimed rows
        // oldest start first, as SQLite does.
        records.sort_unstable_by(|a, b| {
            (a.start_time_ms, &a.operation_id).cmp(&(b.start_time_ms, &b.operation_id))
        });
        Ok(records)
    })
}

pub(super) fn checkpoint<'a>(
    engine: &'a PostgresEngine,
    account_id: &str,
    operation_id: &str,
    owner: &str,
    lease: Duration,
    progress: Progress,
) -> BoxFuture<'a, Result<LeaseState, StorageError>> {
    let account_id = account_id.to_owned();
    let operation_id = operation_id.to_owned();
    let owner = owner.to_owned();
    Box::pin(async move {
        let report_record_count = i64::try_from(progress.report_record_count).unwrap_or(i64::MAX);
        let report_bytes = i64::try_from(progress.report_bytes).unwrap_or(i64::MAX);
        let mut tx = begin(&engine.pool).await?;
        // The renewed deadline is taken from the database's clock, as the
        // claim's is.
        let updated = sqlx::query(
            "UPDATE bulk_operations SET \
                 cursor_file_index = $1, cursor_row_index = $2, \
                 processed_item_count = $3, failed_item_count = $4, \
                 processed_input_bytes = $5, report_record_count = $6, \
                 report_bytes = $7, usage = $8, \
                 lease_until_ms = (extract(epoch FROM clock_timestamp()) * 1000)::bigint + $9 \
             WHERE account_id = $10 AND operation_id = $11 AND lease_owner = $12",
        )
        .bind(progress.cursor_file_index)
        .bind(progress.cursor_row_index)
        .bind(progress.processed_item_count)
        .bind(progress.failed_item_count)
        .bind(progress.processed_input_bytes)
        .bind(report_record_count)
        .bind(report_bytes)
        .bind(&progress.usage)
        .bind(lease_millis(lease))
        .bind(&account_id)
        .bind(&operation_id)
        .bind(&owner)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        if updated.rows_affected() == 0 {
            // The rollback discards the transaction, so the record inserts
            // below never happen for a lost lease.
            tx.rollback().await.map_err(internal)?;
            return Ok(LeaseState::Lost);
        }
        // ON CONFLICT DO NOTHING on the five-column primary key: a record
        // is durable exactly when the checkpoint covering its row is, so a
        // duplicate key here can only be a replayed row whose record
        // already committed. Keeping the first record is the second line
        // of defence, the same as the ledger. The body is stored as serialized JSON text, not
        // jsonb: a record embeds submitted item values, a DynamoDB string
        // may contain U+0000, and jsonb refuses the \u0000 escape, which
        // would refuse every retry of the same checkpoint and leave the
        // operation permanently non-terminal.
        for record in &progress.records {
            let body = serde_json::to_string(&record.record)
                .map_err(|e| StorageError::Internal(format!("serialize a report record: {e}")))?;
            sqlx::query(
                "INSERT INTO bulk_operation_report_records \
                     (account_id, operation_id, kind, file_index, row_index, record) \
                 VALUES ($1, $2, $3, $4, $5, $6) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(&account_id)
            .bind(&operation_id)
            .bind(record.kind.as_str())
            .bind(record.file_index)
            .bind(record.row_index)
            .bind(&body)
            .execute(&mut *tx)
            .await
            .map_err(internal)?;
        }
        // The finished files' figures and the abandoned files' outcomes
        // land on their rows of `bulk_operation_input_files` in the same
        // transaction as the cursor that passed them; no other file's row
        // is touched, so a checkpoint costs the same whatever the file
        // count. An index past the enumerated files names no row, which is
        // a runner defect, and refuses the whole checkpoint.
        let past = |index: usize| {
            StorageError::Internal(format!(
                "input file index {index} is past the enumerated files"
            ))
        };
        for finished in &progress.finished_files {
            let rows = i64::try_from(finished.rows).unwrap_or(i64::MAX);
            // A file finished without a tag keeps the one the listing gave.
            let updated = sqlx::query(
                "UPDATE bulk_operation_input_files \
                 SET row_count = $4, etag = COALESCE($5, etag) \
                 WHERE account_id = $1 AND operation_id = $2 AND file_index = $3",
            )
            .bind(&account_id)
            .bind(&operation_id)
            .bind(i64::try_from(finished.index).unwrap_or(i64::MAX))
            .bind(rows)
            .bind(&finished.etag)
            .execute(&mut *tx)
            .await
            .map_err(internal)?
            .rows_affected();
            if updated == 0 {
                return Err(past(finished.index));
            }
        }
        for abandoned in &progress.abandoned_files {
            let stored = FileAbandon {
                row: i64::try_from(abandoned.row).unwrap_or(i64::MAX),
                kind: abandoned.kind,
                bytes: Some(i64::try_from(abandoned.bytes).unwrap_or(i64::MAX)),
            }
            .to_json();
            let updated = sqlx::query(
                "UPDATE bulk_operation_input_files SET abandon = $4 \
                 WHERE account_id = $1 AND operation_id = $2 AND file_index = $3",
            )
            .bind(&account_id)
            .bind(&operation_id)
            .bind(i64::try_from(abandoned.index).unwrap_or(i64::MAX))
            .bind(&stored)
            .execute(&mut *tx)
            .await
            .map_err(internal)?
            .rows_affected();
            if updated == 0 {
                return Err(past(abandoned.index));
            }
        }
        tx.commit().await.map_err(internal)?;
        Ok(LeaseState::Held)
    })
}

pub(super) fn finish<'a>(
    engine: &'a PostgresEngine,
    account_id: &str,
    operation_id: &str,
    owner: &str,
    outcome: Terminal,
) -> BoxFuture<'a, Result<FinishState, StorageError>> {
    let account_id = account_id.to_owned();
    let operation_id = operation_id.to_owned();
    let owner = owner.to_owned();
    Box::pin(async move {
        let mut tx = begin(&engine.pool).await?;
        // A CANCELLING row becomes CANCELLED whatever the runner computed:
        // a cancel that landed after the runner's last boundary check must
        // still win, and the CASE decides that in the same statement that
        // checks the lease. RETURNING hands back the status the row now
        // stores, so the caller reports the stored status, not the one it
        // sent.
        let stored: Option<String> = sqlx::query_scalar(
            // failure_code and failure_message follow the SAME CASE as
            // status: when a cancel forces the row to CANCELLED, the
            // runner-computed failure fields are replaced by the pair a
            // cancelled operation carries on the wire (measured on the
            // service). processed_input_bytes is not touched here, so the
            // runner's last checkpointed figure stands: the bytes read
            // before the stop, which the service reports for a cancelled
            // operation. Column references in a SET list read the pre-update
            // row, so every CASE expression tests the same original status.
            "UPDATE bulk_operations SET \
                 status = CASE WHEN status = 'CANCELLING' THEN 'CANCELLED' ELSE $1 END, \
                 end_time_ms = $2, \
                 failure_code = CASE WHEN status = 'CANCELLING' THEN $9 ELSE $3 END, \
                 failure_message = CASE WHEN status = 'CANCELLING' THEN $10 ELSE $4 END, \
                 usage = $5, \
                 lease_owner = NULL, lease_until_ms = NULL \
             WHERE account_id = $6 AND operation_id = $7 AND lease_owner = $8 \
               AND status IN ('IN_PROGRESS', 'CANCELLING') \
             RETURNING status",
        )
        .bind(&outcome.status)
        .bind(outcome.end_time_ms)
        .bind(&outcome.failure_code)
        .bind(&outcome.failure_message)
        .bind(&outcome.usage)
        .bind(&account_id)
        .bind(&operation_id)
        .bind(&owner)
        .bind(extenddb_storage::CANCELLED_FAILURE_CODE)
        .bind(extenddb_storage::CANCELLED_FAILURE_MESSAGE)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?;
        let Some(status) = stored else {
            tx.rollback().await.map_err(internal)?;
            return Ok(FinishState::Lost);
        };
        sqlx::query(
            "DELETE FROM bulk_operation_report_records \
             WHERE account_id = $1 AND operation_id = $2",
        )
        .bind(&account_id)
        .bind(&operation_id)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
        tx.commit().await.map_err(internal)?;
        Ok(FinishState::Held(status))
    })
}
