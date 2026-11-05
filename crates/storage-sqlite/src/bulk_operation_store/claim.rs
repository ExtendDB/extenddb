// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The lifecycle writes for the `SQLite` `BulkOperationStore`: cancel, the
//! runnable claim and lease renewal, the checkpoint that advances the cursor
//! and persists report records, and the terminal finish.

use super::*;

pub(super) fn request_cancel<'a>(
    engine: &'a SqliteEngine,
    account_id: &str,
    operation_id: &str,
) -> BoxFuture<'a, Result<CancelOutcome, StorageError>> {
    let account_id = account_id.to_owned();
    let operation_id = operation_id.to_owned();
    Box::pin(async move {
        let _writer = engine.write_lock.lock().await;
        let mut tx = engine
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(internal)?;
        // One conditional update; the answer is never a racy pre-read.
        let updated: Option<(String,)> = sqlx::query_as(
            "UPDATE bulk_operations SET cancel_requested = 1, status = 'CANCELLING' \
             WHERE account_id = ? AND operation_id = ? \
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
        // Nothing updated: the row is terminal or absent. Read inside the
        // same transaction so the answer cannot race a delete.
        let status: Option<(String,)> = sqlx::query_as(
            "SELECT status FROM bulk_operations WHERE account_id = ? AND operation_id = ?",
        )
        .bind(&account_id)
        .bind(&operation_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?;
        tx.commit().await.map_err(internal)?;
        Ok(match status {
            Some((s,)) => CancelOutcome::AlreadyTerminal(s),
            None => CancelOutcome::NotFound,
        })
    })
}

pub(super) fn claim_runnable<'a>(
    engine: &'a SqliteEngine,
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
        let now = now_epoch_ms();
        // The deadline is computed here rather than in SQL so a saturated
        // lease cannot overflow inside the database.
        let lease_until = now.saturating_add(lease_ms(lease));
        let max = i64::try_from(max).unwrap_or(i64::MAX);
        // One positional placeholder per excluded account and per
        // excluded operation; an empty list contributes no clause.
        // Placeholders only, per ADR-0002.
        let exclude_clause = if exclude.is_empty() {
            String::new()
        } else {
            let marks = vec!["?"; exclude.len()].join(", ");
            format!("AND account_id NOT IN ({marks}) ")
        };
        let exclude_ops_clause = if exclude_ops.is_empty() {
            String::new()
        } else {
            let marks = vec!["?"; exclude_ops.len()].join(", ");
            format!("AND operation_id NOT IN ({marks}) ")
        };
        // A claim that finds nothing to claim takes no write lock: this read
        // selects on the predicate the update below selects on, outside the
        // engine write lock, and an idle server answers from it on every
        // tick. A row that becomes runnable after it is claimed on the next
        // tick.
        let probe = format!(
            "SELECT EXISTS (SELECT 1 FROM bulk_operations \
              WHERE status IN ('IN_PROGRESS', 'CANCELLING') \
                AND (lease_until_ms IS NULL OR lease_until_ms < ?) \
                {exclude_clause}\
                {exclude_ops_clause})"
        );
        let mut probe_query = sqlx::query_scalar::<_, bool>(&probe).bind(now);
        for account in &exclude {
            probe_query = probe_query.bind(account);
        }
        for operation in &exclude_ops {
            probe_query = probe_query.bind(operation);
        }
        if !probe_query
            .fetch_one(&engine.pool)
            .await
            .map_err(internal)?
        {
            return Ok(Vec::new());
        }
        let _writer = engine.write_lock.lock().await;
        let mut tx = engine
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(internal)?;
        // Same shape as the PostgreSQL claim, with `rowid IN (...)` in
        // place of `FOR UPDATE SKIP LOCKED`: BEGIN IMMEDIATE already
        // excludes every competing writer. The RETURNING clause names the
        // record columns so `client_token_hash` never leaves storage.
        let sql = format!(
            "UPDATE bulk_operations SET lease_owner = ?, lease_until_ms = ? \
             WHERE rowid IN ( \
                 SELECT rowid FROM bulk_operations \
                  WHERE status IN ('IN_PROGRESS', 'CANCELLING') \
                    AND (lease_until_ms IS NULL OR lease_until_ms < ?) \
                    {exclude_clause}\
                    {exclude_ops_clause}\
                  ORDER BY start_time_ms, operation_id LIMIT ?) \
             RETURNING {RECORD_COLUMNS}"
        );
        let mut query = sqlx::query(&sql).bind(&owner).bind(lease_until).bind(now);
        for account in &exclude {
            query = query.bind(account);
        }
        for operation in &exclude_ops {
            query = query.bind(operation);
        }
        let rows = query
            .bind(max)
            .fetch_all(&mut *tx)
            .await
            .map_err(internal)?;
        // Each row decodes on its own. A row that does not decode is ended
        // in this transaction rather than failing the claim: an error here
        // would roll the whole claim back, and the same row, still the
        // oldest, would be selected again on every tick.
        let mut records = Vec::with_capacity(rows.len());
        for row in &rows {
            let key = unreadable::row_key(row)?;
            let record = match unreadable::decode_or_log(row, &key, "the claim ends it") {
                Some(record) => {
                    files::with_files_or_log(&mut tx, record, "the claim ends it").await?
                }
                None => None,
            };
            match record {
                Some(record) => records.push(record),
                None => unreadable::fail_unreadable(&mut tx, &key, now).await?,
            }
        }
        tx.commit().await.map_err(internal)?;
        // SQLite defines the order of RETURNING output as undefined; the
        // subselect's ORDER BY only chooses which rows LIMIT takes. Sort
        // so callers receive the claimed rows oldest start first.
        records.sort_unstable_by(|a, b| {
            (a.start_time_ms, &a.operation_id).cmp(&(b.start_time_ms, &b.operation_id))
        });
        Ok(records)
    })
}

pub(super) fn checkpoint<'a>(
    engine: &'a SqliteEngine,
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
        let usage = serde_json::to_string(&progress.usage).map_err(internal)?;
        let now = now_epoch_ms();
        // The deadline is computed here rather than in SQL so a saturated
        // lease cannot overflow inside the database.
        let lease_until = now.saturating_add(lease_ms(lease));
        let report_record_count = i64::try_from(progress.report_record_count).unwrap_or(i64::MAX);
        let report_bytes = i64::try_from(progress.report_bytes).unwrap_or(i64::MAX);
        let _writer = engine.write_lock.lock().await;
        let mut tx = engine
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(internal)?;
        let updated = sqlx::query(
            "UPDATE bulk_operations SET cursor_file_index = ?, cursor_row_index = ?, \
             processed_item_count = ?, failed_item_count = ?, processed_input_bytes = ?, \
             report_record_count = ?, report_bytes = ?, usage = ?, lease_until_ms = ? \
             WHERE account_id = ? AND operation_id = ? AND lease_owner = ?",
        )
        .bind(progress.cursor_file_index)
        .bind(progress.cursor_row_index)
        .bind(progress.processed_item_count)
        .bind(progress.failed_item_count)
        .bind(progress.processed_input_bytes)
        .bind(report_record_count)
        .bind(report_bytes)
        .bind(&usage)
        .bind(lease_until)
        .bind(&account_id)
        .bind(&operation_id)
        .bind(&owner)
        .execute(&mut *tx)
        .await
        .map_err(internal)?
        .rows_affected();
        if updated == 0 {
            // Dropping the transaction rolls it back; a lost lease writes
            // nothing, report records included.
            return Ok(LeaseState::Lost);
        }
        // OR IGNORE on the five-column primary key: a record is durable
        // exactly when the checkpoint covering its row is, so a duplicate
        // key here can only be a replayed row whose record already
        // committed. Keeping the first record is the second line of
        // defence, the same as the ledger.
        for record in &progress.records {
            let body = serde_json::to_string(&record.record).map_err(internal)?;
            sqlx::query(
                "INSERT OR IGNORE INTO bulk_operation_report_records \
                 (account_id, operation_id, kind, file_index, row_index, record) \
                 VALUES (?, ?, ?, ?, ?, ?)",
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
                "UPDATE bulk_operation_input_files SET row_count = ?, etag = COALESCE(?, etag) \
                 WHERE account_id = ? AND operation_id = ? AND file_index = ?",
            )
            .bind(rows)
            .bind(&finished.etag)
            .bind(&account_id)
            .bind(&operation_id)
            .bind(i64::try_from(finished.index).unwrap_or(i64::MAX))
            .execute(&mut *tx)
            .await
            .map_err(internal)?
            .rows_affected();
            if updated == 0 {
                return Err(past(finished.index));
            }
        }
        // The abandon is stored as its JSON text.
        for abandoned in &progress.abandoned_files {
            let stored = FileAbandon {
                row: i64::try_from(abandoned.row).unwrap_or(i64::MAX),
                kind: abandoned.kind,
                bytes: Some(i64::try_from(abandoned.bytes).unwrap_or(i64::MAX)),
            }
            .to_json()
            .to_string();
            let updated = sqlx::query(
                "UPDATE bulk_operation_input_files SET abandon = ? \
                 WHERE account_id = ? AND operation_id = ? AND file_index = ?",
            )
            .bind(stored)
            .bind(&account_id)
            .bind(&operation_id)
            .bind(i64::try_from(abandoned.index).unwrap_or(i64::MAX))
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
    engine: &'a SqliteEngine,
    account_id: &str,
    operation_id: &str,
    owner: &str,
    outcome: Terminal,
) -> BoxFuture<'a, Result<FinishState, StorageError>> {
    let account_id = account_id.to_owned();
    let operation_id = operation_id.to_owned();
    let owner = owner.to_owned();
    Box::pin(async move {
        let usage = serde_json::to_string(&outcome.usage).map_err(internal)?;
        let writer = engine.write_lock.lock().await;
        let mut tx = engine
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(internal)?;
        // A CANCELLING row becomes CANCELLED whatever the runner computed:
        // a cancel that landed after the runner's last boundary check must
        // still win. RETURNING hands back the status
        // the row now stores, so the caller reports the stored status,
        // not the one it sent.
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
             status = CASE WHEN status = 'CANCELLING' THEN 'CANCELLED' ELSE ? END, \
             end_time_ms = ?, \
             failure_code = CASE WHEN status = 'CANCELLING' THEN ? ELSE ? END, \
             failure_message = CASE WHEN status = 'CANCELLING' THEN ? ELSE ? END, \
             usage = ?, \
             lease_owner = NULL, lease_until_ms = NULL \
             WHERE account_id = ? AND operation_id = ? AND lease_owner = ? \
               AND status IN ('IN_PROGRESS', 'CANCELLING') \
               RETURNING status",
        )
        .bind(&outcome.status)
        .bind(outcome.end_time_ms)
        .bind(extenddb_storage::CANCELLED_FAILURE_CODE)
        .bind(&outcome.failure_code)
        .bind(extenddb_storage::CANCELLED_FAILURE_MESSAGE)
        .bind(&outcome.failure_message)
        .bind(&usage)
        .bind(&account_id)
        .bind(&operation_id)
        .bind(&owner)
        .fetch_optional(&mut *tx)
        .await
        .map_err(internal)?;
        let Some(status) = stored else {
            return Ok(FinishState::Lost);
        };
        tx.commit().await.map_err(internal)?;
        drop(writer);
        // The records go after the transition commits, in chunks that each
        // take the write lock, so other writers run between them. A crash
        // between chunks leaves records the retention purge deletes with the
        // operation; nothing reads the records of a terminal operation.
        if let Err(error) = delete_report_records(engine, &account_id, &operation_id).await {
            tracing::warn!(
                account_id = %account_id,
                operation_id = %operation_id,
                error = %error,
                "report records of a finished bulk operation were not all deleted; \
                 the retention purge deletes the rest"
            );
        }
        Ok(FinishState::Held(status))
    })
}

/// Rows one chunk of an end-of-operation delete removes. Each chunk is one
/// statement under the engine write lock, which other writers take between
/// chunks; a delete of the whole set in one statement held the lock for
/// seconds at a few hundred thousand records.
const DELETE_CHUNK_ROWS: i64 = 2_000;

/// Delete every report record of one operation, [`DELETE_CHUNK_ROWS`] at a
/// time, each chunk its own statement under the write lock.
pub(super) async fn delete_report_records(
    engine: &SqliteEngine,
    account_id: &str,
    operation_id: &str,
) -> Result<(), StorageError> {
    let chunk = u64::try_from(DELETE_CHUNK_ROWS).unwrap_or(u64::MAX);
    loop {
        let _writer = engine.write_lock.lock().await;
        let deleted = sqlx::query(
            "DELETE FROM bulk_operation_report_records WHERE rowid IN ( \
                 SELECT rowid FROM bulk_operation_report_records \
                  WHERE account_id = ? AND operation_id = ? LIMIT ?)",
        )
        .bind(account_id)
        .bind(operation_id)
        .bind(DELETE_CHUNK_ROWS)
        .execute(&engine.pool)
        .await
        .map_err(internal)?
        .rows_affected();
        if deleted < chunk {
            return Ok(());
        }
    }
}
