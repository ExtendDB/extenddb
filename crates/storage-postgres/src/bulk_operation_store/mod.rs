// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `BulkOperationStore` for the `PostgreSQL` backend.
//!
//! Backs the shared bulk put lifecycle with the `bulk_operations` and
//! `bulk_operation_report_records` catalog tables (migration 8). The
//! lifecycle itself (claim policy, batching, resume, rendering, terminal
//! transitions) lives in the engine, as ADR-0005 requires; this file holds
//! only the primitives, and the two rules that are correct only inside one
//! transaction: the start transaction and the claim.
//!
//! Every query binds request data as parameters, per ADR-0002. No identifier
//! here is built from request data.
//!
//! The trait impl below delegates each method to a free function in one of
//! the sibling modules, grouped by what its SQL does: `start` (the start
//! transaction, token replay, the per-table and account caps), `claim`
//! (claim, lease renewal, checkpoint, cancel, finish), and `query` (describe,
//! list, report paging, purge). The shared time helpers and the row mappers
//! stay here.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::future::BoxFuture;
use serde_json::Value;
use sqlx::Row;
use sqlx::postgres::PgRow;

use extenddb_storage::error::StorageError;
use extenddb_storage::{
    BulkOperationListResult, BulkOperationRecord, BulkOperationStore, CancelOutcome, FileAbandon,
    FinishState, InputFile, LeaseState, ListCursor, NewBulkOperation, OperationFlags, Progress,
    ReportKind, ReportRecord, StartOutcome, Terminal,
};

use crate::PostgresEngine;

mod claim;
mod files;
mod index_guard;
mod query;
mod start;
mod unreadable;

pub(crate) use index_guard::{BuildGuard, refuse_index_creation_during_run};

/// `map_err` hands the error by value, so the helper takes it that way.
#[allow(clippy::needless_pass_by_value)]
fn internal(e: sqlx::Error) -> StorageError {
    StorageError::Internal(e.to_string())
}

/// Current wall clock as epoch milliseconds. Saturates rather than wrapping,
/// on a clock before the epoch or after the i64 range.
fn now_ms() -> i64 {
    let now = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX);
    #[cfg(test)]
    let now = now.saturating_add(pg_tests::clock_skew_ms());
    now
}

/// A `SystemTime` as epoch milliseconds, saturating like [`now_ms`].
fn system_time_ms(t: SystemTime) -> i64 {
    i64::try_from(t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()).unwrap_or(i64::MAX)
}

/// A lease duration as milliseconds, capped at a hundred years so the
/// deadline the database adds it to its own clock stays inside the bigint
/// range.
fn lease_millis(lease: Duration) -> i64 {
    const MAX_LEASE_MS: i64 = 100 * 365 * 24 * 60 * 60 * 1000;
    i64::try_from(lease.as_millis())
        .unwrap_or(i64::MAX)
        .min(MAX_LEASE_MS)
}

/// The longest a bulk operation store transaction may sit idle between two
/// of its statements. A process stopped inside one (paused, swapped out,
/// frozen by a debugger or a signal) keeps the operation row locked: a
/// cancel waits on it and the claim's `SKIP LOCKED` passes over it for as
/// long as the process stays stopped. The server ends such a session after
/// this long, which rolls the transaction back and releases the row. The
/// statements of a running transaction follow each other within
/// milliseconds.
pub(crate) const IDLE_IN_TRANSACTION_TIMEOUT: &str = "10s";

/// Begin a bulk operation store transaction with
/// [`IDLE_IN_TRANSACTION_TIMEOUT`] set for it alone.
async fn begin(
    pool: &sqlx::PgPool,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, StorageError> {
    let mut tx = pool.begin().await.map_err(internal)?;
    sqlx::query("SELECT set_config('idle_in_transaction_session_timeout', $1, true)")
        .bind(IDLE_IN_TRANSACTION_TIMEOUT)
        .execute(&mut *tx)
        .await
        .map_err(internal)?;
    Ok(tx)
}

/// One column of a `bulk_operations` row, with the column name in the error.
fn col<'r, T>(row: &'r PgRow, name: &str) -> Result<T, StorageError>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get::<T, _>(name)
        .map_err(|e| StorageError::Internal(format!("bulk_operations.{name}: {e}")))
}

/// Whether the operation row's `input_files` column holds what this build
/// writes there, an empty array. The files live one row each in
/// `bulk_operation_input_files` (see `files`); a row whose column holds
/// anything else was not written by this build, and does not decode.
fn no_input_files(stored: &Value) -> bool {
    stored.as_array().is_some_and(Vec::is_empty)
}

/// Decode one `bulk_operations` row into the record the trait hands out.
fn record_from_row(row: &PgRow) -> Result<BulkOperationRecord, StorageError> {
    let input_files: Value = col(row, "input_files")?;
    if !no_input_files(&input_files) {
        return Err(StorageError::Internal(
            "bulk_operations.input_files holds a file list, which this build does not write"
                .to_owned(),
        ));
    }
    // The totals are non-negative by construction (the store only ever binds
    // unsigned figures); a negative value is corruption, not a shape to model.
    let unsigned = |name: &str| -> Result<u64, StorageError> {
        let value: i64 = col(row, name)?;
        u64::try_from(value)
            .map_err(|_| StorageError::Internal(format!("bulk_operations.{name} is negative")))
    };
    // The counters stay `i64` on the record, but a negative stored value is
    // the same corruption and gets the same refusal, so Describe, List, and
    // the runner's claim all sit behind one boundary.
    let non_negative = |name: &str| -> Result<i64, StorageError> {
        let value: i64 = col(row, name)?;
        if value < 0 {
            return Err(StorageError::Internal(format!(
                "bulk_operations.{name} is negative"
            )));
        }
        Ok(value)
    };
    Ok(BulkOperationRecord {
        account_id: col(row, "account_id")?,
        operation_id: col(row, "operation_id")?,
        table_id: col(row, "table_id")?,
        table_name: col(row, "table_name")?,
        table_arn: col(row, "table_arn")?,
        status: col(row, "status")?,
        client_token: col(row, "client_token")?,
        request_fingerprint: col(row, "request_fingerprint")?,
        input_config: col(row, "input_config")?,
        output_config: col(row, "output_config")?,
        // Attached from `bulk_operation_input_files` by the caller.
        input_files: Vec::new(),
        max_write_request_units: col(row, "max_write_request_units")?,
        performance_tier: col(row, "performance_tier")?,
        start_time_ms: col(row, "start_time_ms")?,
        end_time_ms: col(row, "end_time_ms")?,
        processed_item_count: non_negative("processed_item_count")?,
        failed_item_count: non_negative("failed_item_count")?,
        processed_input_bytes: non_negative("processed_input_bytes")?,
        total_input_bytes: {
            let value: Option<i64> = col(row, "total_input_bytes")?;
            if value.is_some_and(|v| v < 0) {
                return Err(StorageError::Internal(
                    "bulk_operations.total_input_bytes is negative".to_owned(),
                ));
            }
            value
        },
        report_record_count: unsigned("report_record_count")?,
        report_bytes: unsigned("report_bytes")?,
        usage: col(row, "usage")?,
        failure_code: col(row, "failure_code")?,
        failure_message: col(row, "failure_message")?,
        cancel_requested: col(row, "cancel_requested")?,
        cursor_file_index: non_negative("cursor_file_index")?,
        cursor_row_index: non_negative("cursor_row_index")?,
        lease_owner: col(row, "lease_owner")?,
        lease_until_ms: col(row, "lease_until_ms")?,
        run_context: col(row, "run_context")?,
    })
}

impl BulkOperationStore for PostgresEngine {
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

#[cfg(test)]
mod pg_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_empty_file_list_is_what_this_build_writes() {
        assert!(no_input_files(&serde_json::json!([])));
        assert!(!no_input_files(
            &serde_json::json!([{"path": "in/part-0.json", "size": 1}])
        ));
        assert!(!no_input_files(&serde_json::json!({})));
    }
}
