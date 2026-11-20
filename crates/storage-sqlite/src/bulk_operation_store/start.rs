// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The start transaction for the `SQLite` `BulkOperationStore`: the
//! five-step start, the token replay window, the per-table guard and the
//! account cap, plus the two helpers the insert and token lookup use.

use super::*;

use extenddb_storage::CLIENT_TOKEN_WINDOW_MS;
use extenddb_storage::bulk_operation_store::TABLE_DELETING_MESSAGE;

/// The record a token started inside the replay window: `since_ms` is the
/// requesting start's time minus [`CLIENT_TOKEN_WINDOW_MS`], so a record
/// at or past the window reads as no record.
async fn fetch_record_by_token(
    conn: &mut sqlx::SqliteConnection,
    account_id: &str,
    token_hash: &str,
    since_ms: i64,
) -> Result<Option<BulkOperationRecord>, StorageError> {
    let row = sqlx::query(&format!(
        "SELECT {RECORD_COLUMNS} FROM bulk_operations \
         WHERE account_id = ? AND client_token_hash = ? AND start_time_ms > ?"
    ))
    .bind(account_id)
    .bind(token_hash)
    .bind(since_ms)
    .fetch_optional(&mut *conn)
    .await
    .map_err(internal)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let mut record = record_from_row(&row)?;
    files::attach_files(conn, &mut record).await?;
    Ok(Some(record))
}

/// Step 4 of the start transaction: the insert. `OR IGNORE` swallows any
/// unique conflict (table guard, token, primary key) without aborting the
/// transaction, so step 5 can classify it. Returns the rows inserted: one,
/// or zero on a conflict.
async fn insert_operation_row(
    conn: &mut sqlx::SqliteConnection,
    account_id: &str,
    req: &NewBulkOperation,
) -> Result<u64, StorageError> {
    let input_config = serde_json::to_string(&req.input_config).map_err(internal)?;
    let output_config = serde_json::to_string(&req.output_config).map_err(internal)?;
    let run_context = req
        .run_context
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(internal)?;
    let inserted = sqlx::query(
        "INSERT OR IGNORE INTO bulk_operations ( \
             account_id, operation_id, table_id, table_name, table_arn, status, \
             client_token, client_token_hash, request_fingerprint, input_config, \
             output_config, input_files, max_write_request_units, performance_tier, \
             start_time_ms, end_time_ms, processed_item_count, failed_item_count, \
             processed_input_bytes, total_input_bytes, report_record_count, \
             report_bytes, usage, failure_code, failure_message, cancel_requested, \
             cursor_file_index, cursor_row_index, lease_owner, lease_until_ms, \
             run_context \
         ) VALUES (?, ?, ?, ?, ?, 'IN_PROGRESS', ?, ?, ?, ?, ?, ?, ?, ?, ?, \
                   NULL, 0, 0, 0, ?, 0, 0, '{}', NULL, NULL, 0, 0, 0, NULL, NULL, ?)",
    )
    .bind(account_id)
    .bind(&req.operation_id)
    .bind(&req.table_id)
    .bind(&req.table_name)
    .bind(&req.table_arn)
    .bind(&req.client_token)
    .bind(&req.client_token_hash)
    .bind(&req.request_fingerprint)
    .bind(&input_config)
    .bind(&output_config)
    // The files go one row each into `bulk_operation_input_files`.
    .bind(NO_INPUT_FILES)
    .bind(req.max_write_request_units)
    .bind(&req.performance_tier)
    .bind(req.start_time_ms)
    .bind(req.total_input_bytes)
    .bind(&run_context)
    .execute(conn)
    .await
    .map_err(internal)?
    .rows_affected();
    Ok(inserted)
}

/// The five-step start transaction.
///
/// `BEGIN IMMEDIATE` acquires `SQLite`'s reserved write lock up front, so
/// two racing starts are fully serialized and the second one's step 2
/// SELECT sees the first one's committed row. The insert still uses
/// `INSERT OR IGNORE` and recovers from `changes() = 0` (step 5), because
/// the same decision tree must hold for a second process on the same
/// database file, where this process's write lock does not reach.
pub(super) fn start_bulk_operation<'a>(
    engine: &'a SqliteEngine,
    account_id: &str,
    req: NewBulkOperation,
    account_limit: u32,
) -> BoxFuture<'a, Result<StartOutcome, StorageError>> {
    let account_id = account_id.to_owned();
    // Taken when the start is called, before any await (see `index_guard`).
    let before = super::index_guard::BuildMark::now(&req.table_id);
    Box::pin(async move {
        if before.building() {
            return Ok(StartOutcome::IndexOperationInProgress);
        }

        // D1: every writer holds the engine write lock.
        let _writer = engine.write_lock.lock().await;
        let mut tx = engine
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(internal)?;

        // Step 1: a table being deleted refuses the start with the answer
        // the handler gives at its lookup, since `DeleteTable` can set it
        // `DELETING` after that lookup; then an index that is not ACTIVE,
        // or a build in this process that began since the call, refuses
        // it. Read under the write lock `DeleteTable` and an index creation
        // also hold.
        if super::index_guard::table_deleting(&mut tx, &account_id, &req.table_id).await? {
            tx.commit().await.map_err(internal)?;
            return Err(StorageError::Validation(TABLE_DELETING_MESSAGE.to_owned()));
        }
        let busy = super::index_guard::index_busy(&mut tx, &req.table_id).await?;
        if busy || super::index_guard::BuildMark::now(&req.table_id).began_since(&before) {
            tx.commit().await.map_err(internal)?;
            return Ok(StartOutcome::IndexOperationInProgress);
        }

        // Step 2: token lookup. A token replayed inside the window is
        // answered from the existing row, whatever its status; the
        // guard and the account limit are not consulted. A token whose
        // record is older than the window is unseen: its hash is
        // released from the unique index (the record keeps the token
        // itself for Describe) and the start proceeds as new.
        let token_since_ms = req.start_time_ms.saturating_sub(CLIENT_TOKEN_WINDOW_MS);
        if let Some(hash) = &req.client_token_hash {
            if let Some(existing) =
                fetch_record_by_token(&mut tx, &account_id, hash, token_since_ms).await?
            {
                tx.commit().await.map_err(internal)?;
                return Ok(if existing.request_fingerprint == req.request_fingerprint {
                    StartOutcome::IdempotentReplay(existing)
                } else {
                    StartOutcome::IdempotentMismatch
                });
            }
            sqlx::query(
                "UPDATE bulk_operations SET client_token_hash = NULL \
                 WHERE account_id = ? AND client_token_hash = ? AND start_time_ms <= ?",
            )
            .bind(&account_id)
            .bind(hash)
            .bind(token_since_ms)
            .execute(&mut *tx)
            .await
            .map_err(internal)?;
        }

        // Step 3a: the per-table guard. While the table has an operation
        // IN_PROGRESS or CANCELLING the start answers busy and nothing is
        // inserted. The guard answers before the account limit when both
        // would reject, so a caller blocked by its own running operation
        // learns which table is busy. The partial unique index on the
        // insert below remains the authoritative guard; this read only
        // fixes the order of the answers.
        let table_busy: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM bulk_operations \
             WHERE account_id = ? AND table_id = ? \
             AND status IN ('IN_PROGRESS', 'CANCELLING')",
        )
        .bind(&account_id)
        .bind(&req.table_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(internal)?;
        if table_busy > 0 {
            tx.commit().await.map_err(internal)?;
            return Ok(StartOutcome::TableBusy);
        }

        // Step 3b: account concurrency limit, exact because the write lock
        // and BEGIN IMMEDIATE serialize every start.
        let non_terminal: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM bulk_operations \
             WHERE account_id = ? AND status IN ('IN_PROGRESS', 'CANCELLING')",
        )
        .bind(&account_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(internal)?;
        if non_terminal >= i64::from(account_limit) {
            tx.commit().await.map_err(internal)?;
            return Ok(StartOutcome::AccountLimitReached);
        }

        // Step 4: the insert, in [`insert_operation_row`]. OR IGNORE
        // swallows any unique conflict without aborting the
        // transaction, so step 5 can classify it.
        let inserted = insert_operation_row(&mut tx, &account_id, &req).await?;

        if inserted == 1 {
            files::insert_files(&mut tx, &account_id, &req.operation_id, &req.input_files).await?;
            let record = fetch_record(&mut tx, &account_id, &req.operation_id)
                .await?
                .ok_or_else(|| internal("bulk operation row missing after insert"))?;
            tx.commit().await.map_err(internal)?;
            return Ok(StartOutcome::Created(record));
        }

        // Step 5: the insert was ignored and the transaction is still
        // live. Re-run the step 2 SELECT: a token row present now means
        // the conflict was the idempotency index, so the answer is replay
        // or mismatch by fingerprint, never TableBusy. No token row means
        // the conflict was some other unique constraint, and the per-table
        // guard is the one a well-formed request can hit.
        if let Some(hash) = &req.client_token_hash
            && let Some(existing) =
                fetch_record_by_token(&mut tx, &account_id, hash, token_since_ms).await?
        {
            tx.commit().await.map_err(internal)?;
            return Ok(if existing.request_fingerprint == req.request_fingerprint {
                StartOutcome::IdempotentReplay(existing)
            } else {
                StartOutcome::IdempotentMismatch
            });
        }
        tx.commit().await.map_err(internal)?;
        Ok(StartOutcome::TableBusy)
    })
}

pub(super) fn count_non_terminal_for_table<'a>(
    engine: &'a SqliteEngine,
    account_id: &str,
    table_id: &str,
) -> BoxFuture<'a, Result<u64, StorageError>> {
    let account_id = account_id.to_owned();
    let table_id = table_id.to_owned();
    Box::pin(async move {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM bulk_operations \
             WHERE account_id = ? AND table_id = ? \
             AND status IN ('IN_PROGRESS', 'CANCELLING')",
        )
        .bind(&account_id)
        .bind(&table_id)
        .fetch_one(&engine.pool)
        .await
        .map_err(internal)?;
        Ok(u64::try_from(count).unwrap_or(0))
    })
}

pub(super) fn count_non_terminal<'a>(
    engine: &'a SqliteEngine,
    account_id: &str,
) -> BoxFuture<'a, Result<u64, StorageError>> {
    let account_id = account_id.to_owned();
    Box::pin(async move {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM bulk_operations \
             WHERE account_id = ? AND status IN ('IN_PROGRESS', 'CANCELLING')",
        )
        .bind(&account_id)
        .fetch_one(&engine.pool)
        .await
        .map_err(internal)?;
        // COUNT(*) is non-negative by construction.
        Ok(u64::try_from(count).unwrap_or(0))
    })
}
