// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BulkOperationStore` for the `SQLite` backend.
//!
//! Operation records live in the `bulk_operations` catalog table; report
//! records in `bulk_operation_report_records`. Every write here holds the
//! engine write lock (D1) and multi-statement writes run under
//! `BEGIN IMMEDIATE`, matching the conventions in `store.rs`. All values are
//! bound as parameters; no request data reaches a SQL identifier
//! (docs/adr/0002-sql-injection-defense.md).
//!
//! The trait impl below delegates each method to a free function in one of
//! the sibling modules, grouped by what its SQL does: `start` (the start
//! transaction, token replay, the per-table and account caps), `claim`
//! (claim, lease renewal, checkpoint, cancel, finish), and `query` (describe,
//! list, report paging, purge). The shared SQL constant and the row mappers
//! stay here.

use std::time::{Duration, SystemTime};

use futures::future::BoxFuture;
use serde_json::Value;
use sqlx::Row;
use sqlx::sqlite::SqliteRow;

use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BulkOperationListResult, BulkOperationRecord, BulkOperationStore, CancelOutcome, FileAbandon,
    FinishState, InputFile, LeaseState, ListCursor, NewBulkOperation, OperationFlags, Progress,
    ReportKind, ReportRecord, StartOutcome, Terminal,
};

use crate::store::SqliteEngine;

mod claim;
mod files;
mod index_guard;
mod query;
mod start;
mod unreadable;

pub(crate) use index_guard::{BuildGuard, refuse_index_creation_during_run};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod stall_tests;

/// The `BulkOperationRecord` columns, in declaration order, for SELECTs.
/// `client_token_hash` is deliberately absent: it exists only to carry the
/// idempotency index and never leaves storage.
const RECORD_COLUMNS: &str = "account_id, operation_id, table_id, table_name, table_arn, \
     status, client_token, request_fingerprint, input_config, output_config, input_files, \
     max_write_request_units, performance_tier, start_time_ms, end_time_ms, \
     processed_item_count, failed_item_count, processed_input_bytes, total_input_bytes, \
     report_record_count, report_bytes, \
     usage, failure_code, failure_message, cancel_requested, cursor_file_index, \
     cursor_row_index, lease_owner, lease_until_ms, run_context";

/// What the operation row's `input_files` column holds. The files live
/// one row each in `bulk_operation_input_files` (see `files`); a row whose
/// column holds anything else was not written by this build, and does not
/// decode.
const NO_INPUT_FILES: &str = "[]";

fn internal(e: impl std::fmt::Display) -> StorageError {
    StorageError::Internal(e.to_string())
}

fn now_epoch_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

fn lease_ms(lease: Duration) -> i64 {
    i64::try_from(lease.as_millis()).unwrap_or(i64::MAX)
}

fn record_from_row(row: &SqliteRow) -> Result<BulkOperationRecord, StorageError> {
    let json_col = |col: &str| -> Result<Value, StorageError> {
        let text: String = row.try_get(col).map_err(internal)?;
        serde_json::from_str(&text).map_err(internal)
    };
    // The totals are non-negative by construction (the store only ever binds
    // unsigned figures); a negative value is corruption, not a shape to model.
    let unsigned_col = |col: &str| -> Result<u64, StorageError> {
        let value: i64 = row.try_get(col).map_err(internal)?;
        u64::try_from(value)
            .map_err(|_| StorageError::Internal(format!("bulk_operations.{col} is negative")))
    };
    // The counters stay `i64` on the record, but a negative stored value is
    // the same corruption and gets the same refusal, so Describe, List, and
    // the runner's claim all sit behind one boundary.
    let non_negative_col = |col: &str| -> Result<i64, StorageError> {
        let value: i64 = row.try_get(col).map_err(internal)?;
        if value < 0 {
            return Err(StorageError::Internal(format!(
                "bulk_operations.{col} is negative"
            )));
        }
        Ok(value)
    };
    let input_files_text: String = row.try_get("input_files").map_err(internal)?;
    if input_files_text != NO_INPUT_FILES {
        return Err(StorageError::Internal(
            "bulk_operations.input_files holds a file list, which this build does not write"
                .to_owned(),
        ));
    }
    let cancel_requested: i64 = row.try_get("cancel_requested").map_err(internal)?;
    Ok(BulkOperationRecord {
        account_id: row.try_get("account_id").map_err(internal)?,
        operation_id: row.try_get("operation_id").map_err(internal)?,
        table_id: row.try_get("table_id").map_err(internal)?,
        table_name: row.try_get("table_name").map_err(internal)?,
        table_arn: row.try_get("table_arn").map_err(internal)?,
        status: row.try_get("status").map_err(internal)?,
        client_token: row.try_get("client_token").map_err(internal)?,
        request_fingerprint: row.try_get("request_fingerprint").map_err(internal)?,
        input_config: json_col("input_config")?,
        output_config: json_col("output_config")?,
        // Attached from `bulk_operation_input_files` by the caller.
        input_files: Vec::new(),
        max_write_request_units: row.try_get("max_write_request_units").map_err(internal)?,
        performance_tier: row.try_get("performance_tier").map_err(internal)?,
        start_time_ms: row.try_get("start_time_ms").map_err(internal)?,
        end_time_ms: row.try_get("end_time_ms").map_err(internal)?,
        processed_item_count: non_negative_col("processed_item_count")?,
        failed_item_count: non_negative_col("failed_item_count")?,
        processed_input_bytes: non_negative_col("processed_input_bytes")?,
        total_input_bytes: {
            let value: Option<i64> = row.try_get("total_input_bytes").map_err(internal)?;
            if value.is_some_and(|v| v < 0) {
                return Err(StorageError::Internal(
                    "bulk_operations.total_input_bytes is negative".to_owned(),
                ));
            }
            value
        },
        report_record_count: unsigned_col("report_record_count")?,
        report_bytes: unsigned_col("report_bytes")?,
        usage: json_col("usage")?,
        failure_code: row.try_get("failure_code").map_err(internal)?,
        failure_message: row.try_get("failure_message").map_err(internal)?,
        cancel_requested: cancel_requested != 0,
        cursor_file_index: non_negative_col("cursor_file_index")?,
        cursor_row_index: non_negative_col("cursor_row_index")?,
        lease_owner: row.try_get("lease_owner").map_err(internal)?,
        lease_until_ms: row.try_get("lease_until_ms").map_err(internal)?,
        run_context: {
            let text: Option<String> = row.try_get("run_context").map_err(internal)?;
            text.map(|text| serde_json::from_str(&text).map_err(internal))
                .transpose()?
        },
    })
}

async fn fetch_record(
    conn: &mut sqlx::SqliteConnection,
    account_id: &str,
    operation_id: &str,
) -> Result<Option<BulkOperationRecord>, StorageError> {
    let row = sqlx::query(&format!(
        "SELECT {RECORD_COLUMNS} FROM bulk_operations \
         WHERE account_id = ? AND operation_id = ?"
    ))
    .bind(account_id)
    .bind(operation_id)
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

impl BulkOperationStore for SqliteEngine {
    /// The five-step start transaction.
    fn start_bulk_operation(
        &self,
        account_id: &str,
        req: NewBulkOperation,
        account_limit: u32,
    ) -> BoxFuture<'_, Result<StartOutcome, StorageError>> {
        start::start_bulk_operation(self, account_id, req, account_limit)
    }

    fn get_bulk_operation(
        &self,
        account_id: &str,
        operation_id: &str,
    ) -> BoxFuture<'_, Result<Option<BulkOperationRecord>, StorageError>> {
        query::get_bulk_operation(self, account_id, operation_id)
    }

    fn get_bulk_operation_flags(
        &self,
        account_id: &str,
        operation_id: &str,
    ) -> BoxFuture<'_, Result<Option<OperationFlags>, StorageError>> {
        query::get_bulk_operation_flags(self, account_id, operation_id)
    }

    fn count_non_terminal(&self, account_id: &str) -> BoxFuture<'_, Result<u64, StorageError>> {
        start::count_non_terminal(self, account_id)
    }

    fn count_non_terminal_for_table(
        &self,
        account_id: &str,
        table_id: &str,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        start::count_non_terminal_for_table(self, account_id, table_id)
    }

    fn count_terminal_born_since(
        &self,
        account_id: &str,
        since_ms: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        start::count_terminal_born_since(self, account_id, since_ms)
    }

    fn list_bulk_operations(
        &self,
        account_id: &str,
        limit: u32,
        after: Option<ListCursor>,
    ) -> BoxFuture<'_, BulkOperationListResult> {
        query::list_bulk_operations(self, account_id, limit, after)
    }

    fn request_cancel(
        &self,
        account_id: &str,
        operation_id: &str,
    ) -> BoxFuture<'_, Result<CancelOutcome, StorageError>> {
        claim::request_cancel(self, account_id, operation_id)
    }

    fn claim_runnable(
        &self,
        owner: &str,
        lease: Duration,
        max: usize,
        exclude_accounts: &[String],
        exclude_operations: &[String],
    ) -> BoxFuture<'_, Result<Vec<BulkOperationRecord>, StorageError>> {
        claim::claim_runnable(
            self,
            owner,
            lease,
            max,
            exclude_accounts,
            exclude_operations,
        )
    }

    fn checkpoint(
        &self,
        account_id: &str,
        operation_id: &str,
        owner: &str,
        lease: Duration,
        progress: Progress,
    ) -> BoxFuture<'_, Result<LeaseState, StorageError>> {
        claim::checkpoint(self, account_id, operation_id, owner, lease, progress)
    }

    fn report_records_page(
        &self,
        account_id: &str,
        operation_id: &str,
        kind: ReportKind,
        after: Option<(i64, i64)>,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<ReportRecord>, StorageError>> {
        query::report_records_page(self, account_id, operation_id, kind, after, limit)
    }

    fn finish(
        &self,
        account_id: &str,
        operation_id: &str,
        owner: &str,
        outcome: Terminal,
    ) -> BoxFuture<'_, Result<FinishState, StorageError>> {
        claim::finish(self, account_id, operation_id, owner, outcome)
    }

    fn list_purgeable_before(
        &self,
        cutoff: SystemTime,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>> {
        query::list_purgeable_before(self, cutoff, limit)
    }

    fn purge_operations(
        &self,
        pairs: &[(String, String)],
    ) -> BoxFuture<'_, Result<(), StorageError>> {
        query::purge_operations(self, pairs)
    }
}
