// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The read and purge paths for the `PostgreSQL` `BulkOperationStore`:
//! describe one operation, list an account's operations by page, page a
//! report's records, and the two-step purge of terminal operations.

use super::*;

pub(super) fn get_bulk_operation<'a>(
    engine: &'a PostgresEngine,
    account_id: &str,
    operation_id: &str,
) -> BoxFuture<'a, Result<Option<BulkOperationRecord>, StorageError>> {
    let account_id = account_id.to_owned();
    let operation_id = operation_id.to_owned();
    Box::pin(async move {
        let row = sqlx::query(
            "SELECT * FROM bulk_operations WHERE account_id = $1 AND operation_id = $2",
        )
        .bind(&account_id)
        .bind(&operation_id)
        .fetch_optional(&engine.pool)
        .await
        .map_err(internal)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let mut record = record_from_row(&row)?;
        let mut conn = engine.pool.acquire().await.map_err(internal)?;
        super::files::attach_files(&mut conn, &mut record).await?;
        Ok(Some(record))
    })
}

/// The boundary read: the operation row's status, cancel flag and lease
/// holder, without the file list a full read attaches.
pub(super) fn get_bulk_operation_flags<'a>(
    engine: &'a PostgresEngine,
    account_id: &str,
    operation_id: &str,
) -> BoxFuture<'a, Result<Option<OperationFlags>, StorageError>> {
    let account_id = account_id.to_owned();
    let operation_id = operation_id.to_owned();
    Box::pin(async move {
        let row: Option<(String, bool, Option<String>)> = sqlx::query_as(
            "SELECT status, cancel_requested, lease_owner FROM bulk_operations \
             WHERE account_id = $1 AND operation_id = $2",
        )
        .bind(&account_id)
        .bind(&operation_id)
        .fetch_optional(&engine.pool)
        .await
        .map_err(internal)?;
        Ok(
            row.map(|(status, cancel_requested, lease_owner)| OperationFlags {
                status,
                cancel_requested,
                lease_owner,
            }),
        )
    })
}

pub(super) fn list_bulk_operations<'a>(
    engine: &'a PostgresEngine,
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
        let page = usize::try_from(limit)
            .map_err(|_| StorageError::Internal("list limit does not fit in usize".to_owned()))?;
        // One row past the page decides whether a next cursor exists.
        let fetch = i64::from(limit).saturating_add(1);
        let rows = match &after {
            None => {
                sqlx::query(
                    "SELECT * FROM bulk_operations WHERE account_id = $1 \
                     ORDER BY start_time_ms DESC, operation_id DESC LIMIT $2",
                )
                .bind(&account_id)
                .bind(fetch)
                .fetch_all(&engine.pool)
                .await
            }
            // Exclusive cursor: strictly after the cursor row in the
            // (start_time_ms DESC, operation_id DESC) ordering, which is a
            // row-value comparison below it.
            Some(cursor) => {
                sqlx::query(
                    "SELECT * FROM bulk_operations WHERE account_id = $1 \
                     AND (start_time_ms, operation_id) < ($2, $3) \
                     ORDER BY start_time_ms DESC, operation_id DESC LIMIT $4",
                )
                .bind(&account_id)
                .bind(cursor.start_time_ms)
                .bind(&cursor.operation_id)
                .bind(fetch)
                .fetch_all(&engine.pool)
                .await
            }
        }
        .map_err(internal)?;

        // A row that does not decode is logged and left out; its key still
        // counts toward the page and the cursor, so the next page starts
        // past it rather than failing on it again.
        let more = rows.len() > page;
        let mut records = Vec::with_capacity(rows.len().min(page));
        let mut last = None;
        let mut conn = engine.pool.acquire().await.map_err(internal)?;
        for row in rows.iter().take(page) {
            let key = unreadable::row_key(row)?;
            if let Some(record) =
                unreadable::decode_or_log(row, &key, "leaving it out of the listing")
                && let Some(record) = super::files::with_files_or_log(
                    &mut conn,
                    record,
                    "leaving it out of the listing",
                )
                .await?
            {
                records.push(record);
            }
            last = Some(key);
        }
        let next = if more {
            last.map(|key| ListCursor {
                start_time_ms: key.start_time_ms,
                operation_id: key.operation_id,
            })
        } else {
            None
        };
        Ok((records, next))
    })
}

pub(super) fn report_records_page<'a>(
    engine: &'a PostgresEngine,
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
            // ascending order, as a row-value comparison the way the list
            // cursor is.
            Some((file, row)) => {
                sqlx::query_as(
                    "SELECT file_index, row_index, record \
                     FROM bulk_operation_report_records \
                     WHERE account_id = $1 AND operation_id = $2 AND kind = $3 \
                       AND (file_index, row_index) > ($4, $5) \
                     ORDER BY file_index, row_index LIMIT $6",
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
                     WHERE account_id = $1 AND operation_id = $2 AND kind = $3 \
                     ORDER BY file_index, row_index LIMIT $4",
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
                // Stored as serialized JSON text (see the checkpoint
                // insert); parse it back into the value the trait hands
                // out.
                Ok(ReportRecord {
                    kind,
                    file_index,
                    row_index,
                    record: serde_json::from_str(&text).map_err(|e| {
                        StorageError::Internal(format!("parse a report record: {e}"))
                    })?,
                })
            })
            .collect()
    })
}

pub(super) fn list_purgeable_before(
    engine: &PostgresEngine,
    cutoff: SystemTime,
    limit: usize,
) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
    Box::pin(async move {
        let cutoff_ms = system_time_ms(cutoff);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        sqlx::query_as(
            "SELECT account_id, operation_id FROM bulk_operations \
             WHERE status NOT IN ('IN_PROGRESS', 'CANCELLING') \
               AND end_time_ms IS NOT NULL AND end_time_ms < $1 \
             ORDER BY end_time_ms LIMIT $2",
        )
        .bind(cutoff_ms)
        .bind(limit)
        .fetch_all(&engine.pool)
        .await
        .map_err(internal)
    })
}

pub(super) fn purge_operations<'a>(
    engine: &'a PostgresEngine,
    pairs: &[(String, String)],
) -> BoxFuture<'a, Result<(), StorageError>> {
    let pairs = pairs.to_vec();
    Box::pin(async move {
        if pairs.is_empty() {
            return Ok(());
        }
        let mut tx = begin(&engine.pool).await?;
        for (account_id, operation_id) in &pairs {
            // The row goes first, re-checking the terminal predicate,
            // so a pair that does not name a terminal row deletes
            // nothing; its report records follow only when the row was
            // deleted, in the same transaction.
            let deleted = sqlx::query(
                "DELETE FROM bulk_operations \
                 WHERE account_id = $1 AND operation_id = $2 \
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
                     WHERE account_id = $1 AND operation_id = $2",
                )
                .bind(account_id)
                .bind(operation_id)
                .execute(&mut *tx)
                .await
                .map_err(internal)?;
                super::files::delete_files(&mut tx, account_id, operation_id).await?;
            }
        }
        tx.commit().await.map_err(internal)?;
        Ok(())
    })
}
