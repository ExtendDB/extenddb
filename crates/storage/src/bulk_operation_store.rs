// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Account-scoped bulk operation records and the primitives the shared
//! bulk put lifecycle in the engine drives. Backends implement storage only;
//! the lifecycle (claim policy, batching, resume, rendering, terminal
//! transitions) lives once in the engine, as ADR-0005 requires.

use std::time::{Duration, SystemTime};

use futures::future::BoxFuture;
use serde_json::Value;

use crate::error::StorageError;

mod abandon;

pub use abandon::{AbandonKind, AbandonedFile, FileAbandon};

/// Everything a backend needs to insert a new bulk operation row. The engine
/// resolves the table, enumerates the input files, and computes the token
/// hash and fingerprint before calling storage.
pub struct NewBulkOperation {
    pub operation_id: String,
    pub table_id: String,
    pub table_name: String,
    pub table_arn: String,
    pub client_token: Option<String>,
    pub client_token_hash: Option<String>, // sha256 hex of the token
    pub request_fingerprint: String,
    pub input_config: Value,
    pub output_config: Value,
    pub input_files: Vec<InputFile>,
    pub max_write_request_units: Option<i64>,
    pub performance_tier: Option<String>,
    pub start_time_ms: i64,
    /// The enumerated input total, or `None` when the operation never
    /// reached enumeration (every initial failure is decided before the
    /// walk). Stored as NULL and rendered as an absent `TotalInputBytes`,
    /// the shape the hosted service answers for a failure born before the
    /// input was read (measured on the service: a missing input bucket and an
    /// input owner mismatch both omit the field, counters at zero).
    pub total_input_bytes: Option<i64>,
    /// A failure the engine decided before any row was read, such as a
    /// named bucket directory that does not exist. When set, the start
    /// transaction inserts the row already terminal: status `FAILED`, the
    /// code and message, `end_time_ms` equal to `start_time_ms`, zero
    /// counters, no lease. The token hash and fingerprint are recorded as
    /// usual so the idempotency lookup finds the row. The per-table guard
    /// is consulted as for every start: while the table has an operation
    /// `IN_PROGRESS` or `CANCELLING`, the start answers
    /// `StartOutcome::TableBusy` and nothing is inserted (measured on the
    /// hosted service: a Start naming an input bucket that does not exist,
    /// on a table with an operation running, answered the busy message and
    /// created nothing). The account limit is not consulted: it binds on
    /// non-terminal rows only, and this row is terminal from its first
    /// instant, so it never makes the table busy for a later start either.
    pub initial_failure: Option<InitialFailure>,
    /// What the engine records at Start for the operation's runner: the
    /// store the operation started on and the caller it acts for. Opaque to
    /// storage, which stores it as given (NULL when `None`) and hands it
    /// back on every read; no wire response renders it.
    pub run_context: Option<Value>,
}

/// The failure code and message of an operation inserted already terminal.
pub struct InitialFailure {
    pub code: String,
    pub message: String,
    /// The ceiling on terminal-born rows for this account in the trailing
    /// window, and the window start in epoch milliseconds. The start
    /// transaction counts terminal-born rows at or after `window_start_ms`
    /// under the per-account lock it already holds and refuses the insert
    /// with `StartOutcome::FailedStartLimit` at or above `cap`, so the
    /// bound is exact under a concurrent burst rather than a pre-check that
    /// several requests can race past. The count is read after the
    /// per-table guard, so a busy table answers `TableBusy` first.
    pub cap: u64,
    pub window_start_ms: i64,
}

/// One input file, as a bucket-relative path plus its size in bytes, and
/// what the runner learned about it once it read the file to its end or
/// abandoned it.
#[derive(Clone, Debug, Default)]
pub struct InputFile {
    pub path: String,
    pub size: i64,
    /// Physical lines the file held, recorded by the checkpoint that moved
    /// the cursor past the file. Absent until then, and absent for a file
    /// the runner abandoned. The manifest reads it instead of counting the
    /// file again, so a resume never re-reads a finished file.
    pub rows: Option<i64>,
    /// The file's entity tag. Start records the tag the listing gave, when
    /// it gave one; the checkpoint that moves the cursor past a finished
    /// file records the lowercase hex MD5 of the file's first `size` bytes,
    /// computed while the runner read them. The manifest asks the store
    /// for a tag when it is absent.
    pub etag: Option<String>,
    /// How the runner abandoned the file, recorded by the checkpoint that
    /// moved the cursor past it. Absent for a file read to its end and for
    /// a file the cursor has not passed.
    pub abandon: Option<FileAbandon>,
}

/// What one checkpoint records about an input file read to its end.
#[derive(Clone, Debug)]
pub struct FinishedFile {
    /// Index into the operation's `input_files`.
    pub index: usize,
    pub rows: u64,
    /// `None` when the reader did not reach the file's end through the
    /// hashing path (an abandoned file is never finished this way).
    pub etag: Option<String>,
}

/// A bulk operation row as stored, everything the wire surface and the
/// runner need.
#[derive(Clone, Debug)]
pub struct BulkOperationRecord {
    pub account_id: String,
    pub operation_id: String,
    pub table_id: String,
    pub table_name: String,
    pub table_arn: String,
    pub status: String, // one of the six wire statuses
    pub client_token: Option<String>,
    pub request_fingerprint: String,
    pub input_config: Value,
    pub output_config: Value,
    pub input_files: Vec<InputFile>,
    pub max_write_request_units: Option<i64>,
    pub performance_tier: Option<String>,
    pub start_time_ms: i64,
    pub end_time_ms: Option<i64>,
    pub processed_item_count: i64,
    pub failed_item_count: i64,
    pub processed_input_bytes: i64,
    /// `None` when the input was never enumerated (a failure born before the
    /// walk); see [`NewBulkOperation::total_input_bytes`].
    pub total_input_bytes: Option<i64>,
    /// Report records kept so far, against `max_bulk_report_records`. Set
    /// absolutely by each checkpoint, so a resumed runner continues from the
    /// persisted total without reading the records back.
    pub report_record_count: u64,
    /// Serialized bytes of the kept records, against `max_bulk_report_bytes`.
    /// Set absolutely by each checkpoint, like the count.
    pub report_bytes: u64,
    pub usage: Value,
    pub failure_code: Option<String>,
    pub failure_message: Option<String>,
    pub cancel_requested: bool,
    pub cursor_file_index: i64,
    pub cursor_row_index: i64,
    pub lease_owner: Option<String>,
    pub lease_until_ms: Option<i64>,
    /// The value Start recorded; see [`NewBulkOperation::run_context`].
    /// `None` for a row written without one.
    pub run_context: Option<Value>,
}

/// Outcome of the start transaction. Callers destructure this once and drop
/// it, so the record rides in the variant rather than behind a Box.
#[allow(clippy::large_enum_variant)]
pub enum StartOutcome {
    Created(BulkOperationRecord),
    IdempotentReplay(BulkOperationRecord),
    IdempotentMismatch,
    TableBusy,
    AccountLimitReached,
    /// A terminal-born start refused because the account is at or above
    /// its cap of terminal-born rows in the trailing window, counted under
    /// the per-account lock inside the start transaction.
    FailedStartLimit,
    /// The table has a secondary or vector index that is not `ACTIVE`, or
    /// an index created while the start waited for its transaction. Read
    /// inside the start transaction, under a lock an index creation also
    /// takes, so a start and an index creation that cross are never both
    /// accepted.
    IndexOperationInProgress,
}

/// The text an index creation is refused with while a bulk operation on
/// the table is `IN_PROGRESS` or `CANCELLING` (measured on the hosted
/// service, `ResourceInUseException`). The engine's check before the
/// backend call and the backends' check inside the `UpdateTable`
/// transaction answer with this one text.
#[must_use]
pub fn index_creation_refused_message(table_name: &str) -> String {
    format!(
        "Attempt to change a resource which is still in use: Index creation is not \
         allowed while table has ongoing bulk operation: {table_name}"
    )
}

/// The text a start on a table being deleted is refused with (measured on
/// the hosted service, `ValidationException`, immediately after
/// `DeleteTable` and three seconds later; the table is not named). The
/// start handler answers it at its table lookup, and the start transaction
/// answers it as [`StorageError::Validation`] for a table set `DELETING`
/// after that lookup.
pub const TABLE_DELETING_MESSAGE: &str = "Invalid Request: Table not found";

pub enum CancelOutcome {
    /// Row was `IN_PROGRESS` or CANCELLING; it is now CANCELLING.
    Cancelling,
    /// Row is terminal; carries its status for the `ValidationException` text.
    AlreadyTerminal(String),
    NotFound,
}

/// Whether the caller still held the lease when its conditional write ran.
/// `Lost` means zero rows matched the owner predicate and nothing was written.
#[derive(Debug)]
pub enum LeaseState {
    Held,
    Lost,
}

/// Whether the runner still held the lease when `finish` ran, and the
/// terminal status the row now stores when it did. The stored status can
/// differ from the one the runner computed: a CANCELLING row lands
/// CANCELLED whatever the outcome says, so a caller that reports the
/// terminal status must use this value, not the one it sent.
pub enum FinishState {
    /// The transition ran; carries the stored terminal status.
    Held(String),
    /// Zero rows matched the owner predicate and nothing was written.
    Lost,
}

/// Which report file a record belongs to: the error report or the
/// condition-check-failure report.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ReportKind {
    Error,
    ConditionCheckFailure,
}

impl ReportKind {
    /// The stored vocabulary for this kind, shared by every backend so the
    /// column text never diverges.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::ConditionCheckFailure => "condition_check_failure",
        }
    }

    /// Parse the stored vocabulary back; `None` for anything else.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "error" => Some(Self::Error),
            "condition_check_failure" => Some(Self::ConditionCheckFailure),
            _ => None,
        }
    }
}

/// One report record produced by the runner for a failed or condition-failed
/// row, persisted with the checkpoint that covers the row.
pub struct ReportRecord {
    pub kind: ReportKind,
    pub file_index: i64,
    pub row_index: i64,
    pub record: Value,
}

/// One checkpoint's worth of progress, applied atomically. Every figure is
/// absolute, the report totals included; the runner owns accumulation.
pub struct Progress {
    pub cursor_file_index: i64,
    pub cursor_row_index: i64,
    pub processed_item_count: i64,
    pub failed_item_count: i64,
    pub processed_input_bytes: i64,
    /// Report records kept so far, stored on the operation record.
    pub report_record_count: u64,
    /// Serialized bytes of the kept records, stored on the operation record.
    pub report_bytes: u64,
    pub usage: Value,
    pub records: Vec<ReportRecord>,
    /// Input files this checkpoint moves the cursor past, with their row
    /// count and entity tag. Stored on the matching `input_files` entries
    /// in the same transaction as the cursor, so a committed cursor past a
    /// file always has that file's figures beside it.
    pub finished_files: Vec<FinishedFile>,
    /// Abandoned input files this checkpoint moves the cursor past, with
    /// the row and the kind of the abandon. Stored on the matching
    /// `input_files` entries in the same transaction as the cursor.
    pub abandoned_files: Vec<AbandonedFile>,
}

/// `FailureCode` a cancelled operation carries (measured on the service:
/// `CANCELLED` descriptions carry this code and
/// [`CANCELLED_FAILURE_MESSAGE`]). Both backends store the pair whenever
/// `finish` turns a `CANCELLING` row into `CANCELLED`, whatever the runner
/// computed, and when a claim ends a cancelled row it cannot decode.
pub const CANCELLED_FAILURE_CODE: &str = "RequestCancelled";
/// `FailureMessage` a cancelled operation carries (measured on the service).
pub const CANCELLED_FAILURE_MESSAGE: &str = "The request was cancelled";

/// `FailureCode` a claim stores on a runnable row it cannot decode and
/// whose cancel was not requested. The row cannot be run, so the claim
/// ends it in the same transaction instead of leasing it, and the other
/// rows of the claim are returned.
pub const UNREADABLE_FAILURE_CODE: &str = "InternalServerError";
/// `FailureMessage` stored with [`UNREADABLE_FAILURE_CODE`]: the message
/// every internal fault carries on the wire. The decode error goes to the
/// log, never to a client.
pub const UNREADABLE_FAILURE_MESSAGE: &str = "Internal server error";

/// How long a `ClientToken` replays the operation it started, in
/// milliseconds: eight hours, the window the hosted service documents.
/// Measured on the service: the same request
/// replays within seconds of its start and creates a new operation when
/// re-sent 25 hours later. A start whose `start_time_ms` is at or past
/// `existing.start_time_ms + CLIENT_TOKEN_WINDOW_MS` treats the token as
/// unseen: the aged record's token hash is released so the per-account
/// unique index admits the new row, and the record keeps its `ClientToken`
/// for Describe.
pub const CLIENT_TOKEN_WINDOW_MS: i64 = 8 * 60 * 60 * 1000;

/// The terminal transition the runner requests when a run ends.
pub struct Terminal {
    pub status: String, // COMPLETED | COMPLETED_WITH_ERRORS | FAILED | CANCELLED
    pub end_time_ms: i64,
    pub failure_code: Option<String>,
    pub failure_message: Option<String>,
    pub usage: Value,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListCursor {
    pub start_time_ms: i64,
    pub operation_id: String,
}

/// Result of a list call: one page of records, newest first, plus the cursor
/// for the next page when more rows exist.
pub type BulkOperationListResult =
    Result<(Vec<BulkOperationRecord>, Option<ListCursor>), StorageError>;

pub trait BulkOperationStore: Send + Sync {
    /// The start transaction. Besides the token, the per-table guard and
    /// the account caps, it answers
    /// [`StartOutcome::IndexOperationInProgress`] when an index on the table
    /// is not `ACTIVE`, or was created between the call and the
    /// transaction, read under a lock that `UpdateTable`'s index creation
    /// also takes. That creation, in turn, is refused with
    /// [`index_creation_refused_message`] while the table has a
    /// non-terminal operation, read inside its own transaction.
    ///
    /// A table whose stored status is `DELETING` when the transaction locks
    /// its row is refused with `StorageError::Validation` carrying
    /// [`TABLE_DELETING_MESSAGE`], before the other checks the transaction
    /// makes and before anything is inserted: the start handler gives that
    /// answer at its table lookup, and the table can be set `DELETING`
    /// after it. A table row that is absent is not refused here.
    fn start_bulk_operation(
        &self,
        account_id: &str,
        req: NewBulkOperation,
        account_limit: u32,
    ) -> BoxFuture<'_, Result<StartOutcome, StorageError>>;

    fn get_bulk_operation(
        &self,
        account_id: &str,
        operation_id: &str,
    ) -> BoxFuture<'_, Result<Option<BulkOperationRecord>, StorageError>>;

    /// The batch boundary read, [`crate::OperationFlags`]; the default reads the full record.
    fn get_bulk_operation_flags(
        &self,
        account_id: &str,
        operation_id: &str,
    ) -> BoxFuture<'_, Result<Option<crate::OperationFlags>, StorageError>> {
        crate::OperationFlags::of_read(self.get_bulk_operation(account_id, operation_id))
    }

    /// The account's non-terminal (`IN_PROGRESS` or CANCELLING) operation
    /// count, read outside any transaction. The start transaction keeps
    /// its own exact check under its locks; this read lets the start
    /// handler refuse a saturated account before input enumeration
    /// runs.
    fn count_non_terminal(&self, account_id: &str) -> BoxFuture<'_, Result<u64, StorageError>>;

    /// The non-terminal operations bound to one table of the account, by
    /// table id, read outside any transaction. `UpdateTable` refuses an
    /// index creation while this is above zero (measured on the hosted
    /// service). Keyed by id, not name, so an operation still running on a
    /// deleted table does not count against a new table of the same name.
    /// Backends without bulk operations have none, so the default answers
    /// zero; the two backends that run them override it, and the contract
    /// suite holds them to it.
    fn count_non_terminal_for_table(
        &self,
        account_id: &str,
        table_id: &str,
    ) -> BoxFuture<'_, Result<u64, StorageError>> {
        let _ = (account_id, table_id);
        Box::pin(async { Ok(0) })
    }

    /// The account's terminal-born rows younger than `since_ms`: rows with
    /// status `FAILED`, `end_time_ms` equal to `start_time_ms`, and
    /// `start_time_ms` at or after `since_ms`. Read outside any
    /// transaction; the start handler uses it to cap how many terminal
    /// rows an account can insert per window, so concurrent requests can
    /// overshoot the cap by the number in flight.
    fn count_terminal_born_since(
        &self,
        account_id: &str,
        since_ms: i64,
    ) -> BoxFuture<'_, Result<u64, StorageError>>;

    /// Newest first by (`start_time_ms` DESC, `operation_id` DESC); `after` is
    /// exclusive. `limit` must be at least 1; 0 is refused with
    /// `StorageError::Validation` rather than answered as an empty page,
    /// which would be indistinguishable from an account with no operations.
    /// A row that does not decode is logged at ERROR with its account,
    /// operation id and the decode error and left out of the page; the
    /// cursor still moves past it, so a page can hold fewer than `limit`
    /// records and still carry a cursor.
    fn list_bulk_operations(
        &self,
        account_id: &str,
        limit: u32,
        after: Option<ListCursor>,
    ) -> BoxFuture<'_, BulkOperationListResult>;

    fn request_cancel(
        &self,
        account_id: &str,
        operation_id: &str,
    ) -> BoxFuture<'_, Result<CancelOutcome, StorageError>>;

    /// Claims up to `max` rows with status `IN_PROGRESS` or CANCELLING whose
    /// lease is null or expired, oldest start first, setting `lease_owner`
    /// and `lease_until_ms`. Rows whose account is in `exclude_accounts` and
    /// rows whose operation id is in `exclude_operations` are skipped; an
    /// empty list is no filter. The worker passes the accounts already
    /// holding their per-account share of its slots and the operation ids
    /// already running in its process, so a run that outlives its lease is
    /// never claimed a second time by the process still running it.
    ///
    /// Each claimed row is decoded on its own. A row that does not decode is
    /// logged at ERROR with its account, operation id and the decode error,
    /// left out of the result, and ended in the claim's own transaction by
    /// the rule `finish` follows: a row that is `CANCELLING` or has
    /// `cancel_requested` set ends `CANCELLED` with
    /// [`CANCELLED_FAILURE_CODE`] and [`CANCELLED_FAILURE_MESSAGE`], and any
    /// other ends `FAILED` with [`UNREADABLE_FAILURE_CODE`] and
    /// [`UNREADABLE_FAILURE_MESSAGE`]; `end_time_ms` now, lease cleared, and
    /// its report records deleted in the same transaction, as `finish`
    /// deletes them. The
    /// other rows are returned with their lease, so one such row never stops
    /// the rest from being claimed.
    fn claim_runnable(
        &self,
        owner: &str,
        lease: Duration,
        max: usize,
        exclude_accounts: &[String],
        exclude_operations: &[String],
    ) -> BoxFuture<'_, Result<Vec<BulkOperationRecord>, StorageError>>;

    /// One transaction: cursor, counters, usage, report records, lease renewal.
    /// Conditional on `lease_owner` = owner; zero rows updated is Lost.
    fn checkpoint(
        &self,
        account_id: &str,
        operation_id: &str,
        owner: &str,
        lease: Duration,
        progress: Progress,
    ) -> BoxFuture<'_, Result<LeaseState, StorageError>>;

    /// One page of report records of one kind, ordered `(file_index,
    /// row_index)` ascending, strictly after `after`. `limit` must be at
    /// least 1; 0 is refused with `StorageError::Validation`, because an
    /// empty page would read as the end of the report.
    fn report_records_page(
        &self,
        account_id: &str,
        operation_id: &str,
        kind: ReportKind,
        after: Option<(i64, i64)>,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<ReportRecord>, StorageError>>;

    /// Conditional terminal transition: status IN `('IN_PROGRESS','CANCELLING')`
    /// AND `lease_owner` = owner. A CANCELLING row becomes CANCELLED whatever
    /// `outcome.status` says. Clears the lease. Deletes the operation's report
    /// records before it returns (on SQLite after the transition, in chunks).
    /// Zero rows updated is Lost; otherwise `Held` carries the status the row now stores.
    fn finish(
        &self,
        account_id: &str,
        operation_id: &str,
        owner: &str,
        outcome: Terminal,
    ) -> BoxFuture<'_, Result<FinishState, StorageError>>;

    /// The `(account_id, operation_id)` pair of every terminal row with
    /// `end_time_ms` before the cutoff, up to `limit` rows: the rows
    /// `purge_operations` may delete. Reads only. `limit` bounds the pass
    /// so the retention worker can drain a large backlog in separately
    /// committed chunks rather than one unbounded transaction. The
    /// retention worker deletes each listed pair's row-outcome ledger rows
    /// (which live in the data database, out of this store's reach) and
    /// then purges only the pairs whose ledger delete succeeded, so ledger
    /// rows die while their operation record still exists and a crash
    /// between the two steps re-lists the same pairs on the next pass.
    fn list_purgeable_before(
        &self,
        cutoff: SystemTime,
        limit: usize,
    ) -> BoxFuture<'_, Result<Vec<(String, String)>, StorageError>>;

    /// Deletes exactly the named operations' records and report records.
    /// Only terminal rows are deleted, and a pair that no longer names a
    /// row deletes nothing, so the call is idempotent across retries.
    fn purge_operations(
        &self,
        pairs: &[(String, String)],
    ) -> BoxFuture<'_, Result<(), StorageError>>;
}
