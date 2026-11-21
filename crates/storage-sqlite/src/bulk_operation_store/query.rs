// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The read and purge paths for the `SQLite` `BulkOperationStore`: describe
//! one operation, list an account's operations by page, page a report's
//! records, and the two-step purge of terminal operations.

use super::*;

pub(super) fn get_bulk_operation<'a>(
    engine: &'a SqliteEngine,
    account_id: &str,
    operation_id: &str,
) -> BoxFuture<'a, Result<Option<BulkOperationRecord>, StorageError>> {
    let account_id = account_id.to_owned();
    let operation_id = operation_id.to_owned();
    Box::pin(async move {
        let mut conn = engine.pool.acquire().await.map_err(internal)?;
        fetch_record(&mut conn, &account_id, &operation_id).await
    })
}

/// The boundary read: the operation row's status, cancel flag and lease
/// holder, without the file list a full read attaches.
pub(super) fn get_bulk_operation_flags<'a>(
    engine: &'a SqliteEngine,
    account_id: &str,
    operation_id: &str,
) -> BoxFuture<'a, Result<Option<OperationFlags>, StorageError>> {
    let account_id = account_id.to_owned();
    let operation_id = operation_id.to_owned();
    Box::pin(async move {
        let row: Option<(String, i64, Option<String>)> = sqlx::query_as(
            "SELECT status, cancel_requested, lease_owner FROM bulk_operations \
             WHERE account_id = ? AND operation_id = ?",
        )
        .bind(&account_id)
        .bind(&operation_id)
        .fetch_optional(&engine.pool)
        .await
        .map_err(internal)?;
        Ok(
            row.map(|(status, cancel_requested, lease_owner)| OperationFlags {
                status,
                cancel_requested: cancel_requested != 0,
                lease_owner,
            }),
        )
    })
}

pub(super) fn list_bulk_operations<'a>(
    engine: &'a SqliteEngine,
    account_id: &str,
    limit: u32,
    after: Option<ListCursor>,
) -> BoxFuture<'a, BulkOperationListResult> {
    let account_id = account_id.to_owned();
    Box::pin(async move {
        // The trait requires limit >= 1: an empty page for 0 would read
        // as an account with no operations.
        if limit == 0 {
            return Err(StorageError::Validation(
                "list_bulk_operations requires a limit of at least 1".to_owned(),
            ));
        }
        let page = i64::from(limit);
        // Fetch one row past the page to learn whether another page
        // exists without a second query.
        let rows = match &after {
            Some(cursor) => {
                sqlx::query(&format!(
                    "SELECT {RECORD_COLUMNS} FROM bulk_operations \
                     WHERE account_id = ? \
                       AND (start_time_ms < ? \
                            OR (start_time_ms = ? AND operation_id < ?)) \
                     ORDER BY start_time_ms DESC, operation_id DESC LIMIT ?"
                ))
                .bind(&account_id)
                .bind(cursor.start_time_ms)
                .bind(cursor.start_time_ms)
                .bind(&cursor.operation_id)
                .bind(page + 1)
                .fetch_all(&engine.pool)
                .await
            }
            None => {
                sqlx::query(&format!(
                    "SELECT {RECORD_COLUMNS} FROM bulk_operations \
                     WHERE account_id = ? \
                     ORDER BY start_time_ms DESC, operation_id DESC LIMIT ?"
                ))
                .bind(&account_id)
                .bind(page + 1)
                .fetch_all(&engine.pool)
                .await
            }
        }
        .map_err(internal)?;

        // A row that does not decode is logged and left out; its key still
        // counts toward the page and the cursor, so the next page starts
        // past it rather than failing on it again.
        let page = usize::try_from(page).unwrap_or(usize::MAX);
        let more = rows.len() > page;
        let mut records = Vec::with_capacity(rows.len().min(page));
        let mut last = None;
        let mut conn = engine.pool.acquire().await.map_err(internal)?;
        for row in rows.iter().take(page) {
            let key = unreadable::row_key(row)?;
            if let Some(record) =
                unreadable::decode_or_log(row, &key, "leaving it out of the listing")
                && let Some(record) =
                    files::with_files_or_log(&mut conn, record, "leaving it out of the listing")
                        .await?
            {
                records.push(record);
            }
            last = Some(key);
        }
        let cursor = if more {
            last.map(|key| ListCursor {
                start_time_ms: key.start_time_ms,
                operation_id: key.operation_id,
            })
        } else {
            None
        };
        Ok((records, cursor))
    })
}

pub(super) fn report_records_page<'a>(
    engine: &'a SqliteEngine,
    account_id: &str,
    operation_id: &str,
    kind: ReportKind,
    after: Option<(i64, i64)>,
    limit: usize,
) -> BoxFuture<'a, Result<Vec<ReportRecord>, StorageError>> {
    let account_id = account_id.to_owned();
    let operation_id = operation_id.to_owned();
    Box::pin(async move {
        // The trait requires limit >= 1: an empty page for 0 would read
        // as the end of the report.
        if limit == 0 {
            return Err(StorageError::Validation(
                "report_records_page requires a limit of at least 1".to_owned(),
            ));
        }
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows: Vec<(i64, i64, String)> = match after {
            // Strictly after the cursor in (file_index, row_index)
            // ascending order, as a row-value comparison: SQLite seeks the
            // primary key index to the cursor for it, where the expanded
            // `file_index > ? OR (file_index = ? AND row_index > ?)` form
            // scans every record of the kind before the cursor, so paging
            // through a report cost time in the square of its size.
            Some((file, row)) => {
                sqlx::query_as(
                    "SELECT file_index, row_index, record \
                     FROM bulk_operation_report_records \
                     WHERE account_id = ? AND operation_id = ? AND kind = ? \
                       AND (file_index, row_index) > (?, ?) \
                     ORDER BY file_index, row_index LIMIT ?",
                )
                .bind(&account_id)
                .bind(&operation_id)
                .bind(kind.as_str())
                .bind(file)
                .bind(row)
                .bind(limit)
                .fetch_all(&engine.pool)
                .await
            }
            None => {
                sqlx::query_as(
                    "SELECT file_index, row_index, record \
                     FROM bulk_operation_report_records \
                     WHERE account_id = ? AND operation_id = ? AND kind = ? \
                     ORDER BY file_index, row_index LIMIT ?",
                )
                .bind(&account_id)
                .bind(&operation_id)
                .bind(kind.as_str())
                .bind(limit)
                .fetch_all(&engine.pool)
                .await
            }
        }
        .map_err(internal)?;
        rows.into_iter()
            .map(|(file_index, row_index, text)| {
                Ok(ReportRecord {
                    kind,
                    file_index,
                    row_index,
                    record: serde_json::from_str(&text).map_err(internal)?,
                })
            })
            .collect()
    })
}

pub(super) fn list_purgeable_before(
    engine: &SqliteEngine,
    cutoff: SystemTime,
    limit: usize,
) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
    Box::pin(async move {
        let cutoff_ms = cutoff
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_millis()).ok())
            .unwrap_or(0);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        sqlx::query_as(
            "SELECT account_id, operation_id FROM bulk_operations \
             WHERE status NOT IN ('IN_PROGRESS', 'CANCELLING') \
               AND end_time_ms IS NOT NULL AND end_time_ms < ? \
             ORDER BY end_time_ms LIMIT ?",
        )
        .bind(cutoff_ms)
        .bind(limit)
        .fetch_all(&engine.pool)
        .await
        .map_err(internal)
    })
}

pub(super) fn purge_operations<'a>(
    engine: &'a SqliteEngine,
    pairs: &[(String, String)],
) -> BoxFuture<'a, Result<(), StorageError>> {
    let pairs = pairs.to_vec();
    Box::pin(async move {
        if pairs.is_empty() {
            return Ok(());
        }
        let _writer = engine.write_lock.lock().await;
        let mut tx = engine
            .pool
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(internal)?;
        for (account_id, operation_id) in &pairs {
            // The row goes first, re-checking the terminal predicate,
            // so a pair that does not name a terminal row deletes
            // nothing; its report records follow only when the row was
            // deleted, in the same transaction.
            let deleted = sqlx::query(
                "DELETE FROM bulk_operations \
                 WHERE account_id = ? AND operation_id = ? \
                   AND status NOT IN ('IN_PROGRESS', 'CANCELLING')",
            )
            .bind(account_id)
            .bind(operation_id)
            .execute(&mut *tx)
            .await
            .map_err(internal)?
            .rows_affected();
            if deleted > 0 {
                sqlx::query(
                    "DELETE FROM bulk_operation_report_records \
                     WHERE account_id = ? AND operation_id = ?",
                )
                .bind(account_id)
                .bind(operation_id)
                .execute(&mut *tx)
                .await
                .map_err(internal)?;
                files::delete_files(&mut tx, account_id, operation_id).await?;
            }
        }
        tx.commit().await.map_err(internal)?;
        Ok(())
    })
}
