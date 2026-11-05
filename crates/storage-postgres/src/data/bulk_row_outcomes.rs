// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Row-outcome ledger for bulk put operations (`bulk_row_outcomes`) and
//! the operation's write owner (`bulk_write_owners`), both created in the
//! data database by migration 8.
//!
//! The catalog checkpoint cannot be atomic with a data write because the
//! catalog and the data tables are separate databases, so the fact "this row
//! already happened" lives in the data database and rides the same
//! transaction as the item write, the way `gsi_pending` rows do. Every row a
//! bulk batch applies is recorded, with the size of the item it replaced and,
//! when the runner asks, that item: a resumed run reads them back and
//! accounts for the rows without writing them again.
//!
//! The ledger key is `(account_id, operation_id, file_index, row_index)`:
//! the catalog primary key permits the same `operation_id` in two accounts,
//! so an unscoped key would interleave two operations' rows on an id
//! collision.

use std::fmt::Write as _;

use extenddb_core::types::{Item, item_size_bytes};
use extenddb_storage::error::StorageError;
use extenddb_storage::{BulkRowMarker, RecordedRow, RowOutcome};

use crate::PostgresEngine;

/// One marked row's ledger entry: its outcome, the size of `old`, the item
/// the row replaced or was evaluated against, and that item when the marker
/// asks to keep it or the condition failed. A batch builds one as each
/// marked row is written and inserts them together before its commit.
pub(crate) struct LedgerRow<'m> {
    marker: &'m BulkRowMarker,
    outcome: RowOutcome,
    old_size: Option<i64>,
    old_image: Option<serde_json::Value>,
}

impl<'m> LedgerRow<'m> {
    pub(crate) fn new(
        marker: &'m BulkRowMarker,
        outcome: RowOutcome,
        old: Option<&Item>,
    ) -> Result<Self, StorageError> {
        let old_size = old.map(|item| i64::try_from(item_size_bytes(item)).unwrap_or(i64::MAX));
        let keep = marker.keep_old || outcome == RowOutcome::ConditionFailed;
        let old_image = match old {
            Some(item) if keep => Some(
                serde_json::to_value(item).map_err(|e| StorageError::Internal(e.to_string()))?,
            ),
            _ => None,
        };
        Ok(Self {
            marker,
            outcome,
            old_size,
            old_image,
        })
    }
}

/// Values one ledger row binds: the seven columns of `insert_sql`.
const LEDGER_COLUMNS: usize = 7;

/// Ledger rows per `INSERT`. A PostgreSQL statement binds at most 65,535
/// values: the Bind message counts its parameters in an unsigned 16-bit
/// field, and sqlx refuses a query with more before sending it. At seven
/// values a row that is 9,362 rows. The bulk runner's batches hold at most
/// 100 rows (`bulk_put_write_batch_rows`), so each is one statement.
const LEDGER_ROWS_PER_STATEMENT: usize = u16::MAX as usize / LEDGER_COLUMNS;

/// The ledger `INSERT` of `rows` rows: `($1, ..., $7), ($8, ..., $14), ...`.
fn insert_sql(rows: usize) -> String {
    let mut sql = String::from(
        "INSERT INTO bulk_row_outcomes \
         (account_id, operation_id, file_index, row_index, outcome, old_size, old_image) VALUES ",
    );
    for row in 0..rows {
        sql.push_str(if row == 0 { "(" } else { ", (" });
        for column in 0..LEDGER_COLUMNS {
            if column > 0 {
                sql.push_str(", ");
            }
            let _ = write!(sql, "${}", row * LEDGER_COLUMNS + column + 1);
        }
        sql.push(')');
    }
    sql
}

/// Insert ledger rows inside an open transaction, one multi-row `INSERT`
/// per `LEDGER_ROWS_PER_STATEMENT` rows. A plain insert: a second outcome
/// for one position, in these rows or already recorded, means the row was
/// submitted after it had been applied, and the primary key fails the
/// statement and the transaction with it.
pub(crate) async fn insert_outcomes_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    rows: &[LedgerRow<'_>],
) -> Result<(), StorageError> {
    for chunk in rows.chunks(LEDGER_ROWS_PER_STATEMENT) {
        let sql = insert_sql(chunk.len());
        let mut query = sqlx::query(&sql);
        for row in chunk {
            query = query
                .bind(&row.marker.account_id)
                .bind(&row.marker.operation_id)
                .bind(row.marker.file_index)
                .bind(row.marker.row_index)
                .bind(row.outcome.as_str())
                .bind(row.old_size)
                .bind(&row.old_image);
        }
        query
            .execute(&mut **tx)
            .await
            .map_err(crate::data::index::db_error)?;
    }
    Ok(())
}

/// Insert one ledger row inside an open transaction, for a single write
/// that carries a marker outside a batch.
pub(crate) async fn insert_outcome_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    marker: &BulkRowMarker,
    outcome: RowOutcome,
    old: Option<&Item>,
) -> Result<(), StorageError> {
    insert_outcomes_in_tx(tx, &[LedgerRow::new(marker, outcome, old)?]).await
}

/// The batch-level ledger work, before any row is written: bound the
/// transaction's idle time, lock the operation's owner row and refuse the
/// batch unless `marker.owner` is the owner it names, then delete the
/// ledger rows the last checkpoint covers. The lock makes a takeover wait
/// for this batch, or this batch wait for the takeover and then read the
/// new owner. A process stopped inside the batch would hold the owner row,
/// and the next claim's takeover with it, for as long as it stays stopped,
/// so the database ends a batch transaction left idle for the bound the
/// bulk store puts on its own transactions, which rolls it back.
pub(crate) async fn fence_and_prune(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    marker: &BulkRowMarker,
) -> Result<(), StorageError> {
    sqlx::query("SELECT set_config('idle_in_transaction_session_timeout', $1, true)")
        .bind(crate::bulk_operation_store::IDLE_IN_TRANSACTION_TIMEOUT)
        .execute(&mut **tx)
        .await
        .map_err(crate::data::index::db_error)?;
    let owner: Option<String> = sqlx::query_scalar(
        "SELECT owner FROM bulk_write_owners \
         WHERE account_id = $1 AND operation_id = $2 FOR UPDATE",
    )
    .bind(&marker.account_id)
    .bind(&marker.operation_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(crate::data::index::db_error)?;
    if owner.as_deref() != Some(marker.owner.as_str()) {
        return Err(StorageError::Transient(format!(
            "bulk write batch refused: operation {} is owned by another claim",
            marker.operation_id
        )));
    }
    let (file, row) = marker.settled;
    sqlx::query(
        "DELETE FROM bulk_row_outcomes WHERE account_id = $1 AND operation_id = $2 \
         AND (file_index < $3 OR (file_index = $3 AND row_index < $4))",
    )
    .bind(&marker.account_id)
    .bind(&marker.operation_id)
    .bind(file)
    .bind(row)
    .execute(&mut **tx)
    .await
    .map_err(crate::data::index::db_error)?;
    Ok(())
}

fn parse_outcome(outcome: &str) -> Result<RowOutcome, StorageError> {
    RowOutcome::parse(outcome).ok_or_else(|| {
        StorageError::Internal(format!(
            "bulk_row_outcomes holds an unknown outcome: {outcome}"
        ))
    })
}

impl PostgresEngine {
    /// Record a failed condition on the single-row path as its own
    /// single-statement transaction: a failed condition writes nothing
    /// else, so there is no write transaction for the outcome to ride.
    pub(crate) async fn record_condition_failed_outcome(
        &self,
        marker: &BulkRowMarker,
        old: Option<&Item>,
    ) -> Result<(), StorageError> {
        let mut tx = self
            .data_pool
            .begin()
            .await
            .map_err(crate::data::index::db_error)?;
        insert_outcome_in_tx(&mut tx, marker, RowOutcome::ConditionFailed, old).await?;
        tx.commit().await.map_err(crate::data::index::db_error)?;
        Ok(())
    }

    /// Implementation of `DataEngine::bulk_row_outcome`.
    pub(crate) async fn bulk_row_outcome_impl(
        &self,
        account_id: &str,
        operation_id: &str,
        file_index: i64,
        row_index: i64,
    ) -> Result<Option<RowOutcome>, StorageError> {
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT outcome FROM bulk_row_outcomes \
             WHERE account_id = $1 AND operation_id = $2 \
               AND file_index = $3 AND row_index = $4",
        )
        .bind(account_id)
        .bind(operation_id)
        .bind(file_index)
        .bind(row_index)
        .fetch_optional(&self.data_pool)
        .await
        .map_err(crate::data::index::db_error)?;
        row.map(|(outcome,)| parse_outcome(&outcome)).transpose()
    }

    /// Implementation of `DataEngine::bulk_write_takeover`. The upsert takes
    /// the owner row's lock, so it waits for a batch of the earlier owner
    /// that holds it.
    pub(crate) async fn bulk_write_takeover_impl(
        &self,
        account_id: &str,
        operation_id: &str,
        owner: &str,
        cursor: (i64, i64),
    ) -> Result<u32, StorageError> {
        let stalled: i64 = sqlx::query_scalar(
            "INSERT INTO bulk_write_owners AS o \
             (account_id, operation_id, owner, cursor_file, cursor_row, stalled) \
             VALUES ($1, $2, $3, $4, $5, 0) \
             ON CONFLICT (account_id, operation_id) DO UPDATE SET \
             owner = EXCLUDED.owner, \
             stalled = CASE WHEN o.cursor_file = EXCLUDED.cursor_file \
               AND o.cursor_row = EXCLUDED.cursor_row THEN o.stalled + 1 ELSE 0 END, \
             cursor_file = EXCLUDED.cursor_file, cursor_row = EXCLUDED.cursor_row \
             RETURNING stalled",
        )
        .bind(account_id)
        .bind(operation_id)
        .bind(owner)
        .bind(cursor.0)
        .bind(cursor.1)
        .fetch_one(&self.data_pool)
        .await
        .map_err(crate::data::index::db_error)?;
        Ok(u32::try_from(stalled).unwrap_or(u32::MAX))
    }

    /// Implementation of `DataEngine::bulk_rows_recorded`.
    pub(crate) async fn bulk_rows_recorded_impl(
        &self,
        account_id: &str,
        operation_id: &str,
        file_index: i64,
        from_row: i64,
        limit: i64,
    ) -> Result<Vec<RecordedRow>, StorageError> {
        let rows: Vec<(i64, String, Option<i64>, Option<serde_json::Value>)> = sqlx::query_as(
            "SELECT row_index, outcome, old_size, old_image FROM bulk_row_outcomes \
             WHERE account_id = $1 AND operation_id = $2 AND file_index = $3 \
               AND row_index >= $4 ORDER BY row_index LIMIT $5",
        )
        .bind(account_id)
        .bind(operation_id)
        .bind(file_index)
        .bind(from_row)
        .bind(limit)
        .fetch_all(&self.data_pool)
        .await
        .map_err(crate::data::index::db_error)?;
        rows.into_iter()
            .map(|(row_index, outcome, old_size, old_image)| {
                Ok(RecordedRow {
                    row_index,
                    outcome: parse_outcome(&outcome)?,
                    old_size,
                    old_image: old_image
                        .map(serde_json::from_value)
                        .transpose()
                        .map_err(|e| StorageError::Internal(e.to_string()))?,
                })
            })
            .collect()
    }

    /// Implementation of `DataEngine::delete_bulk_row_outcomes`: the
    /// operation's ledger rows, then its owner row.
    pub(crate) async fn delete_bulk_row_outcomes_impl(
        &self,
        account_id: &str,
        operation_id: &str,
    ) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "DELETE FROM bulk_row_outcomes WHERE account_id = $1 AND operation_id = $2",
        )
        .bind(account_id)
        .bind(operation_id)
        .execute(&self.data_pool)
        .await
        .map_err(crate::data::index::db_error)?;
        sqlx::query("DELETE FROM bulk_write_owners WHERE account_id = $1 AND operation_id = $2")
            .bind(account_id)
            .bind(operation_id)
            .execute(&self.data_pool)
            .await
            .map_err(crate::data::index::db_error)?;
        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_ledger_row_is_the_single_row_statement() {
        assert_eq!(
            insert_sql(1),
            "INSERT INTO bulk_row_outcomes \
             (account_id, operation_id, file_index, row_index, outcome, old_size, old_image) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)"
        );
    }

    #[test]
    fn a_full_ledger_statement_numbers_every_value_once() {
        let sql = insert_sql(LEDGER_ROWS_PER_STATEMENT);
        let last = LEDGER_ROWS_PER_STATEMENT * LEDGER_COLUMNS;
        assert_eq!(last, 65_534, "9,362 rows of seven values");
        assert!(sql.ends_with(&format!(
            "(${}, ${}, ${}, ${}, ${}, ${}, ${last})",
            last - 6,
            last - 5,
            last - 4,
            last - 3,
            last - 2,
            last - 1
        )));
        assert_eq!(sql.matches('$').count(), last);
    }
}
