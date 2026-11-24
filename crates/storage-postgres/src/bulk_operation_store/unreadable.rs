// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Rows of `bulk_operations` that do not decode into a record. The claim
//! ends such a row instead of leasing it, and the listing leaves it out, so
//! one row a hand edit or a newer build wrote cannot stop the other
//! operations on the server from running or being listed.

use extenddb_storage::bulk_operation_store::{UNREADABLE_FAILURE_CODE, UNREADABLE_FAILURE_MESSAGE};
use extenddb_storage::{CANCELLED_FAILURE_CODE, CANCELLED_FAILURE_MESSAGE};

use super::*;

/// The key and ordering columns of a row, read without decoding the rest.
pub(super) struct RowKey {
    pub account_id: String,
    pub operation_id: String,
    pub start_time_ms: i64,
}

pub(super) fn row_key(row: &PgRow) -> Result<RowKey, StorageError> {
    Ok(RowKey {
        account_id: col(row, "account_id")?,
        operation_id: col(row, "operation_id")?,
        start_time_ms: col(row, "start_time_ms")?,
    })
}

/// Decodes one row, or logs at ERROR why it does not decode and answers
/// `None`. The line names the account and the operation so an operator can
/// find the row, and `action` says what the caller does with it.
pub(super) fn decode_or_log(
    row: &PgRow,
    key: &RowKey,
    action: &str,
) -> Option<BulkOperationRecord> {
    match record_from_row(row) {
        Ok(record) => Some(record),
        Err(error) => {
            tracing::error!(
                account_id = %key.account_id,
                operation_id = %key.operation_id,
                "bulk operation record does not decode, {action}: {error}"
            );
            None
        }
    }
}

/// Ends a row that does not decode, by the rule `finish` follows: a row
/// that is `CANCELLING`, or whose cancel was requested, ends `CANCELLED`
/// with the cancelled failure pair, and any other ends `FAILED` with the
/// internal failure pair; `end_time_ms` now, lease cleared. The statement
/// reads `status` and `cancel_requested` itself, so the ending needs no
/// decode and is not refused by a defect in another column. Keyed on the
/// primary key alone and guarded on the non-terminal statuses the claim
/// selected. The row's report records are deleted with it, in the
/// caller's transaction.
pub(super) async fn fail_unreadable(
    conn: &mut sqlx::PgConnection,
    key: &RowKey,
    now: i64,
) -> Result<(), StorageError> {
    // Column references in a SET list read the pre-update row, so every
    // CASE tests the same original status.
    let ended = sqlx::query(
        "UPDATE bulk_operations SET \
             status = CASE WHEN status = 'CANCELLING' OR cancel_requested \
                 THEN 'CANCELLED' ELSE 'FAILED' END, \
             failure_code = CASE WHEN status = 'CANCELLING' OR cancel_requested \
                 THEN $1 ELSE $2 END, \
             failure_message = CASE WHEN status = 'CANCELLING' OR cancel_requested \
                 THEN $3 ELSE $4 END, \
             end_time_ms = $5, lease_owner = NULL, lease_until_ms = NULL \
         WHERE account_id = $6 AND operation_id = $7 \
           AND status IN ('IN_PROGRESS', 'CANCELLING')",
    )
    .bind(CANCELLED_FAILURE_CODE)
    .bind(UNREADABLE_FAILURE_CODE)
    .bind(CANCELLED_FAILURE_MESSAGE)
    .bind(UNREADABLE_FAILURE_MESSAGE)
    .bind(now)
    .bind(&key.account_id)
    .bind(&key.operation_id)
    .execute(&mut *conn)
    .await
    .map_err(internal)?;
    if ended.rows_affected() == 0 {
        return Ok(());
    }
    // The operation's report records go in the same transaction, as
    // `finish` deletes them: nothing reads them for a terminal row.
    sqlx::query(
        "DELETE FROM bulk_operation_report_records \
         WHERE account_id = $1 AND operation_id = $2",
    )
    .bind(&key.account_id)
    .bind(&key.operation_id)
    .execute(conn)
    .await
    .map_err(internal)?;
    Ok(())
}
