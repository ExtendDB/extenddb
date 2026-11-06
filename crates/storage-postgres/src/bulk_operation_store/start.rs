// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The start transaction for the `PostgreSQL` `BulkOperationStore`: the
//! advisory-locked five-step start, the token replay window, the per-table
//! guard and the account and terminal-born caps, plus the two helpers the
//! insert and token lookup use.

use super::*;

use extenddb_storage::CLIENT_TOKEN_WINDOW_MS;
use extenddb_storage::bulk_operation_store::TABLE_DELETING_MESSAGE;

/// How long one attempt of the start transaction waits for the account's
/// advisory lock before it rolls back and gives its connection back to the
/// pool (see `index_guard`). The table row is taken without waiting.
const LOCK_WAIT: &str = "200ms";

/// How long a start keeps trying before it answers that the service is
/// unavailable, a retryable answer. The pool waits as long for a connection.
const START_LOCK_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// The token lookup steps 2 and 5 of the start transaction share.
/// The record a token started inside the replay window: `since_ms` is the
/// requesting start's time minus [`CLIENT_TOKEN_WINDOW_MS`], so a record
/// at or past the window reads as no record.
async fn fetch_by_token(
    conn: &mut sqlx::PgConnection,
    account_id: &str,
    token_hash: &str,
    since_ms: i64,
) -> Result<Option<BulkOperationRecord>, StorageError> {
    let row = sqlx::query(
        "SELECT * FROM bulk_operations \
         WHERE account_id = $1 AND client_token_hash = $2 AND start_time_ms > $3",
    )
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
    super::files::attach_files(conn, &mut record).await?;
    Ok(Some(record))
}

/// Step 4 of the start transaction: the insert that can conflict, written
/// not to raise. A constraint violation would abort the transaction and
/// turn a replayed token into an error, so recovery in step 5 depends on
/// ON CONFLICT DO NOTHING rather than on catching a violation. An initial
/// failure lands as a row that is terminal from its first instant: FAILED,
/// end time equal to start time, no lease. Returns the inserted row, or
/// `None` on a conflict.
async fn insert_operation_row(
    conn: &mut sqlx::PgConnection,
    account_id: &str,
    req: &NewBulkOperation,
) -> Result<Option<PgRow>, StorageError> {
    let (status, end_time_ms, failure_code, failure_message) = match &req.initial_failure {
        Some(failure) => (
            "FAILED",
            Some(req.start_time_ms),
            Some(failure.code.as_str()),
            Some(failure.message.as_str()),
        ),
        None => ("IN_PROGRESS", None, None, None),
    };
    sqlx::query(
        "INSERT INTO bulk_operations (\
             account_id, operation_id, table_id, table_name, table_arn, status, \
             client_token, client_token_hash, request_fingerprint, \
             input_config, output_config, input_files, \
             max_write_request_units, performance_tier, \
             start_time_ms, end_time_ms, \
             processed_item_count, failed_item_count, \
             processed_input_bytes, total_input_bytes, \
             report_record_count, report_bytes, \
             usage, failure_code, failure_message, cancel_requested, \
             cursor_file_index, cursor_row_index, lease_owner, lease_until_ms, \
             run_context) \
         VALUES ($1, $2, $3, $4, $5, $6, \
             $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, \
             0, 0, 0, $17, 0, 0, $18, $19, $20, FALSE, 0, 0, NULL, NULL, $21) \
         ON CONFLICT DO NOTHING \
         RETURNING *",
    )
    .bind(account_id)
    .bind(&req.operation_id)
    .bind(&req.table_id)
    .bind(&req.table_name)
    .bind(&req.table_arn)
    .bind(status)
    .bind(&req.client_token)
    .bind(&req.client_token_hash)
    .bind(&req.request_fingerprint)
    .bind(&req.input_config)
    .bind(&req.output_config)
    // The files go one row each into `bulk_operation_input_files`.
    .bind(serde_json::json!([]))
    .bind(req.max_write_request_units)
    .bind(&req.performance_tier)
    .bind(req.start_time_ms)
    .bind(end_time_ms)
    .bind(req.total_input_bytes)
    // The first checkpoint overwrites this; until then the operation
    // has consumed nothing.
    .bind(serde_json::json!({}))
    .bind(failure_code)
    .bind(failure_message)
    .bind(&req.run_context)
    .fetch_optional(conn)
    .await
    .map_err(internal)
}

pub(super) fn start_bulk_operation<'a>(
    engine: &'a PostgresEngine,
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

        // A lock wait past `LOCK_WAIT`, or a table row another transaction
        // holds, rolls the attempt back, which gives its connection back to
        // the pool, and the start tries again after a pause, until
        // `START_LOCK_DEADLINE` (see `index_guard`).
        let called = std::time::Instant::now();
        let mut pause = std::time::Duration::from_millis(10);
        loop {
            if let Some(outcome) =
                start_attempt(engine, &account_id, &req, account_limit, before).await?
            {
                return Ok(outcome);
            }
            if called.elapsed() >= START_LOCK_DEADLINE {
                return Err(StorageError::Transient(format!(
                    "bulk operation start waited {} s for its account lock or table row",
                    START_LOCK_DEADLINE.as_secs()
                )));
            }
            tokio::time::sleep(pause).await;
            pause = (pause * 2).min(std::time::Duration::from_millis(200));
        }
    })
}

/// One attempt at the start transaction: steps 1 to 5. `Ok(None)` when the
/// wait for the account's advisory lock ran past [`LOCK_WAIT`] or the table
/// row was held by another transaction; the attempt is rolled back and has
/// inserted nothing.
async fn start_attempt(
    engine: &PostgresEngine,
    account_id: &str,
    req: &NewBulkOperation,
    account_limit: u32,
    before: super::index_guard::BuildMark,
) -> Result<Option<StartOutcome>, StorageError> {
    let mut tx = begin(&engine.pool).await?;

    // The wait for the account's lock in step 1 is bounded, and step 1b
    // does not wait (see `index_guard`); the statements after them wait as
    // long as they need.
    sqlx::query("SELECT set_config('lock_timeout', $1, true)")
        .bind(LOCK_WAIT)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;

    // Step 1. Serialise starts within one account. The lock is what
    // makes the account-limit count in step 3 exact, and it releases
    // with the transaction on every return path below.
    match sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
        .bind(account_id)
        .execute(&mut *tx)
        .await
    {
        Ok(_) => {}
        Err(error) if super::index_guard::lock_timed_out(&error) => {
            tx.rollback().await.map_err(internal)?;
            return Ok(None);
        }
        Err(error) => return Err(internal(error)),
    }
    sqlx::query("SELECT set_config('lock_timeout', '0', true)")
        .execute(&mut *tx)
        .await
        .map_err(internal)?;

    // Step 1b. The table row `FOR SHARE`, the lock an index creation takes
    // `FOR UPDATE` and `DeleteTable` takes to set `DELETING` or to remove
    // the row (see `index_guard`). Taken without waiting: a row another
    // transaction holds ends the attempt, so the account's lock is never
    // held while a start waits for a row.
    let read = super::index_guard::lock_table_row(&mut tx, account_id, &req.table_id).await?;
    if matches!(read, super::index_guard::RowRead::Locked) {
        tx.rollback().await.map_err(internal)?;
        return Ok(None);
    }

    // Step 1c. A table being deleted, or removed since the handler's
    // lookup, refuses the start with the answer the handler gives at its
    // lookup for a table being deleted; then an index that is not ACTIVE,
    // or a build in this process that began since the call, refuses it.
    if matches!(read, super::index_guard::RowRead::Deleting) {
        tx.commit().await.map_err(internal)?;
        return Err(StorageError::Validation(TABLE_DELETING_MESSAGE.to_owned()));
    }
    let busy = super::index_guard::index_busy(&mut tx, &req.table_id).await?;
    if busy || super::index_guard::BuildMark::now(&req.table_id).began_since(before) {
        tx.commit().await.map_err(internal)?;
        return Ok(Some(StartOutcome::IndexOperationInProgress));
    }

    // Step 2. A token seen inside the replay window answers from
    // the existing row. A token whose record is older than the
    // window is unseen: its hash is released from the unique index
    // (the record keeps the token itself for Describe) and the
    // start proceeds as new.
    let token_since_ms = req.start_time_ms.saturating_sub(CLIENT_TOKEN_WINDOW_MS);
    if let Some(hash) = req.client_token_hash.as_deref() {
        if let Some(existing) = fetch_by_token(&mut tx, account_id, hash, token_since_ms).await? {
            tx.commit().await.map_err(internal)?;
            return Ok(Some(
                if existing.request_fingerprint == req.request_fingerprint {
                    StartOutcome::IdempotentReplay(existing)
                } else {
                    StartOutcome::IdempotentMismatch
                },
            ));
        }
        sqlx::query(
            "UPDATE bulk_operations SET client_token_hash = NULL \
             WHERE account_id = $1 AND client_token_hash = $2 AND start_time_ms <= $3",
        )
        .bind(account_id)
        .bind(hash)
        .bind(token_since_ms)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
    }

    // Step 3a. The per-table guard binds on every start, including one
    // whose input configuration already failed: while the table has an
    // operation IN_PROGRESS or CANCELLING the start answers busy and
    // nothing is inserted (measured on the service: with an operation
    // running on the table, a Start naming an input bucket that does
    // not exist answered the busy message and created nothing). The
    // guard answers before the account limit and before the
    // terminal-born cap when either would also reject, so a caller
    // blocked by its own running operation learns which table is busy.
    // For a start that is not born terminal, the partial unique index
    // on the insert below remains the authoritative guard and this read
    // only fixes the order of the answers. A row born terminal is
    // outside that index, so for it this read is the guard, exact
    // because the advisory lock held since step 1 serialises the
    // account's starts.
    let (table_busy,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM bulk_operations \
         WHERE account_id = $1 AND table_id = $2 \
         AND status IN ('IN_PROGRESS', 'CANCELLING')",
    )
    .bind(account_id)
    .bind(&req.table_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(internal)?;
    if table_busy > 0 {
        tx.commit().await.map_err(internal)?;
        return Ok(Some(StartOutcome::TableBusy));
    }

    if let Some(failure) = &req.initial_failure {
        // A row born terminal takes no account concurrency slot, so
        // step 3b does not apply to it; its own cap does.
        //
        // Terminal-born cap, counted under the advisory lock held
        // since step 1, so a concurrent burst of absent-bucket
        // starts cannot overshoot it: each start sees the others'
        // committed rows because the lock serialises them. A row
        // is terminal-born when its end_time equals its start_time
        // and it is FAILED, which is exactly the shape the insert
        // below writes.
        let (born,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM bulk_operations \
             WHERE account_id = $1 AND status = 'FAILED' \
               AND end_time_ms = start_time_ms AND start_time_ms >= $2",
        )
        .bind(account_id)
        .bind(failure.window_start_ms)
        .fetch_one(&mut *tx)
        .await
        .map_err(internal)?;
        if u64::try_from(born).unwrap_or(0) >= failure.cap {
            tx.commit().await.map_err(internal)?;
            return Ok(Some(StartOutcome::FailedStartLimit));
        }
    } else {
        // Step 3b. Non-terminal operations count against the
        // account limit.
        let (running,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM bulk_operations \
             WHERE account_id = $1 AND status IN ('IN_PROGRESS', 'CANCELLING')",
        )
        .bind(account_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(internal)?;
        if running >= i64::from(account_limit) {
            tx.commit().await.map_err(internal)?;
            return Ok(Some(StartOutcome::AccountLimitReached));
        }
    }

    // Step 4, in [`insert_operation_row`]: the insert that can
    // conflict, recovered from in step 5 via ON CONFLICT DO
    // NOTHING.
    let inserted = insert_operation_row(&mut tx, account_id, req).await?;

    if let Some(row) = inserted {
        let mut record = record_from_row(&row)?;
        super::files::insert_files(&mut tx, account_id, &req.operation_id, &req.input_files)
            .await?;
        record.input_files = req.input_files.clone();
        tx.commit().await.map_err(internal)?;
        return Ok(Some(StartOutcome::Created(record)));
    }

    // Step 5. Zero rows and no error: some unique index was hit and
    // the transaction is still live. The token lookup decides which
    // one it was, so the answer does not depend on which index the
    // conflict physically hit.
    let outcome = if let Some(hash) = req.client_token_hash.as_deref() {
        match fetch_by_token(&mut tx, account_id, hash, token_since_ms).await? {
            Some(existing) if existing.request_fingerprint == req.request_fingerprint => {
                StartOutcome::IdempotentReplay(existing)
            }
            Some(_) => StartOutcome::IdempotentMismatch,
            None => StartOutcome::TableBusy,
        }
    } else {
        StartOutcome::TableBusy
    };
    tx.commit().await.map_err(internal)?;
    Ok(Some(outcome))
}

pub(super) fn count_non_terminal_for_table<'a>(
    engine: &'a PostgresEngine,
    account_id: &str,
    table_id: &str,
) -> BoxFuture<'a, Result<u64, StorageError>> {
    let account_id = account_id.to_owned();
    let table_id = table_id.to_owned();
    Box::pin(async move {
        let (count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM bulk_operations \
             WHERE account_id = $1 AND table_id = $2 \
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
    engine: &'a PostgresEngine,
    account_id: &str,
) -> BoxFuture<'a, Result<u64, StorageError>> {
    let account_id = account_id.to_owned();
    Box::pin(async move {
        let (count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM bulk_operations \
             WHERE account_id = $1 AND status IN ('IN_PROGRESS', 'CANCELLING')",
        )
        .bind(&account_id)
        .fetch_one(&engine.pool)
        .await
        .map_err(internal)?;
        // COUNT(*) is non-negative by construction.
        Ok(u64::try_from(count).unwrap_or(0))
    })
}

pub(super) fn count_terminal_born_since<'a>(
    engine: &'a PostgresEngine,
    account_id: &str,
    since_ms: i64,
) -> BoxFuture<'a, Result<u64, StorageError>> {
    let account_id = account_id.to_owned();
    Box::pin(async move {
        let (count,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM bulk_operations \
             WHERE account_id = $1 AND status = 'FAILED' \
             AND end_time_ms = start_time_ms AND start_time_ms >= $2",
        )
        .bind(&account_id)
        .bind(since_ms)
        .fetch_one(&engine.pool)
        .await
        .map_err(internal)?;
        // COUNT(*) is non-negative by construction.
        Ok(u64::try_from(count).unwrap_or(0))
    })
}
