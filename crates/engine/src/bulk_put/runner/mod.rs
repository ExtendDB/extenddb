// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The bulk put runner: drives one claimed operation to a terminal state.
//!
//! The runner reads the enumerated input files in order, applies each row
//! with `PutItem` semantics through the same validation and expression
//! functions `handle_put_item` uses, and checkpoints cursor, counters,
//! usage, and report records after every batch and at the end of every
//! file. Every checkpoint and the terminal transition are conditional on
//! the lease owner; a lost lease stops the runner without another write.
//!
//! Every row is applied exactly once. Each write batch records every row
//! it applies in the row-outcome ledger, in the batch's own data
//! transaction, with what the write found; a claim records itself as the
//! operation's write owner before its first batch, and a batch of an
//! earlier owner writes nothing. A resumed run reads the rows recorded past
//! its checkpoint and accounts for them from the ledger instead of writing
//! them again (`runner::resume`).
//!
//! Report records are rendered to files once, at the end, from the rows
//! the checkpoints persisted, so a record is durable exactly when the
//! checkpoint covering its row is.

pub(in crate::bulk_put::runner) use std::borrow::Cow;
pub(in crate::bulk_put::runner) use std::sync::Arc;
pub(in crate::bulk_put::runner) use std::time::{Duration, Instant};

pub(in crate::bulk_put::runner) use serde_json::Value;

pub(in crate::bulk_put::runner) use extenddb_core::error::DynamoDbError;
pub(in crate::bulk_put::runner) use extenddb_core::expression::{Expr, ExpressionMaps};
pub(in crate::bulk_put::runner) use extenddb_core::limits::LimitsConfig;
pub(in crate::bulk_put::runner) use extenddb_core::metrics::{MetricName, MetricsCollector};
pub(in crate::bulk_put::runner) use extenddb_core::types::{
    DescribeTableInput, InputCompressionType, InputS3Configuration, Item, OutputS3Configuration,
    OutputType, TableKeyInfo, TableStatus, item_size_bytes,
};
pub(in crate::bulk_put::runner) use extenddb_storage::error::StorageError;
pub(in crate::bulk_put::runner) use extenddb_storage::{
    AbandonKind, AbandonedFile, BatchRow, BatchRowResult, BulkOperationRecord, BulkRowMarker,
    CancellationToken, FinishState, FinishedFile, InputFile, LeaseState, Progress, RecordedRow,
    ReportKind, ReportRecord, RowOutcome, StorageEngine, Terminal,
};

pub(in crate::bulk_put::runner) use crate::bulk_put::authorize::{
    self, BulkAuthorizer, BulkPrincipal, Decision, S3Action,
};
pub(in crate::bulk_put::runner) use crate::bulk_put::epoch_millis;
pub(in crate::bulk_put::runner) use crate::bulk_put::pacer::{WritePacer, write_units};
pub(in crate::bulk_put::runner) use crate::bulk_put::report;
pub(in crate::bulk_put::runner) use crate::bulk_put::rows::{self, Row, RowCondition, RowError};
pub(in crate::bulk_put::runner) use crate::bulk_put::source::{
    BucketRef, BulkSink, BulkSource, InputCodec, LineReader, OutputSse, SinkError, SourceError,
    StoreFault, StoreUnavailable,
};
pub(in crate::bulk_put::runner) use crate::bulk_put::usage::UsageAccum;
pub(in crate::bulk_put::runner) use crate::put_item::{
    PutPayloadFault, resolve_put_condition, stream_capture_for, validate_put_payload,
};

mod admit;
mod cut;
mod failure;
mod finalize;
mod held;
mod in_place;
mod manifest;
mod outputs;
mod permission;
mod read;
mod reader;
mod recorded;
mod resume;
mod state;
pub(crate) use failure::OBJECTS_COUNT_LIMIT_CODE;
mod wait;
mod write;

pub(in crate::bulk_put::runner) use admit::*;
pub(in crate::bulk_put::runner) use cut::*;
pub(in crate::bulk_put::runner) use failure::*;
pub(in crate::bulk_put::runner) use finalize::*;
pub(in crate::bulk_put::runner) use held::*;
pub(in crate::bulk_put::runner) use in_place::*;
pub(in crate::bulk_put::runner) use manifest::*;
pub(in crate::bulk_put::runner) use outputs::*;
pub(in crate::bulk_put::runner) use permission::*;
pub(in crate::bulk_put::runner) use read::*;
pub(in crate::bulk_put::runner) use reader::*;
pub(in crate::bulk_put::runner) use recorded::*;
#[cfg(test)]
pub(crate) use resume::RECORDED_PAGE;
pub(in crate::bulk_put::runner) use resume::*;
pub(in crate::bulk_put::runner) use state::*;
pub(in crate::bulk_put::runner) use wait::*;
pub(in crate::bulk_put::runner) use write::*;

/// Wire statuses the runner writes or reacts to.
const STATUS_CANCELLING: &str = "CANCELLING";
const STATUS_COMPLETED: &str = "COMPLETED";
const STATUS_COMPLETED_WITH_ERRORS: &str = "COMPLETED_WITH_ERRORS";
const STATUS_CANCELLED: &str = "CANCELLED";
const STATUS_FAILED: &str = "FAILED";

/// The in-memory report buffer is flushed to a checkpoint once the records
/// pending since the last checkpoint reach this many serialized bytes,
/// regardless of `bulk_put_batch_size`. This caps the heap a single runner
/// holds for its report buffer on failing-heavy input: without it, a large
/// `batch_size` lets `pending` grow to `batch_size` records before a flush.
/// 16 MiB is far above a normal batch's failures and far below a memory
/// hazard, and a record is at most one item image, so one record never
/// overshoots it on its own.
const PENDING_FLUSH_BYTES: u64 = 16 * 1024 * 1024;

/// `FailureCode` values. The table-deleted code is the measured string,
/// which is not the wire exception name; the others are exception type
/// names.
const CODE_RESOURCE_NOT_FOUND: &str = "DynamoDBResourceNotFoundException";
/// The measured `FailureMessage` for a table deleted mid-operation.
const MESSAGE_RESOURCE_NOT_FOUND: &str = "Requested resource not found";
const CODE_VALIDATION: &str = "ValidationException";
const CODE_INTERNAL: &str = "InternalServerError";

/// The one `FailureMessage` every internal fault carries; detail goes to
/// the log, never to a client.
const INTERNAL_MESSAGE: &str = "Internal server error";

/// Tuning for one runner, sourced from configuration by the caller.
pub(crate) struct RunnerConfig {
    /// Rows per checkpoint (default 25).
    pub batch_size: usize,
    /// Rows per storage transaction (default 25, at most 100). Staged rows
    /// are flushed when this many are waiting, at every checkpoint, at the
    /// end of every file, and before every flow that leaves the read loop.
    pub write_batch_rows: usize,
    /// Longest accepted input line in bytes (4 MiB).
    pub max_line_bytes: usize,
    /// Physical lines one operation may consume, blank lines included;
    /// exceeding it fails the operation. A bound on the work one
    /// operation can demand, not on the customer-visible
    /// `ProcessedItemCount`.
    pub max_item_count: u64,
    /// Report records kept per operation; beyond it records are dropped.
    pub max_report_records: u64,
    /// Serialized bytes of report records kept per operation; beyond it
    /// records are dropped.
    pub max_report_bytes: u64,
    /// Lease length passed to every checkpoint.
    pub lease: Duration,
}

/// Everything a runner needs to drive one operation.
pub(crate) struct RunnerDeps {
    pub storage: Arc<dyn StorageEngine>,
    pub source: Arc<dyn BulkSource>,
    pub sink: Arc<dyn BulkSink>,
    pub limits: Arc<LimitsConfig>,
    pub region: Arc<str>,
    /// Lease owner id of this process.
    pub owner: String,
    pub config: RunnerConfig,
    /// Collector for the terminal-transition metrics: `finalize` records
    /// `BulkOperationsFinished` and `BulkPutItemsWritten`.
    pub metrics: Arc<MetricsCollector>,
    /// The process-wide write pacer, shared by every runner in this server
    /// and sized by `bulk_put_write_units_per_second`. Every row write takes
    /// its units here first. See [`crate::bulk_put::pacer`].
    pub pacer: Arc<WritePacer>,
    /// The kind of the store `source` and `sink` belong to
    /// ([`crate::bulk_put::run_context::store_kind`]).
    pub store_kind: &'static str,
    /// Evaluates the S3 actions the run performs as the operation's caller
    /// (`runner::permission`); `None` in developer mode, where no check
    /// runs.
    pub authorizer: Option<Arc<dyn BulkAuthorizer>>,
    /// The server's shutdown token. Once it is cancelled the runner stops at
    /// the next row: it writes the staged rows, checkpoints with a lease
    /// that has already run out, so the next claim need not wait for it,
    /// and returns without a terminal transition.
    pub shutdown: CancellationToken,
}

/// The borrowed context of one run, fixed once `run` has validated the
/// claim: the process dependencies plus the operation's record, parsed
/// configuration, and table key metadata. The file and row steps take it
/// as one parameter.
#[derive(Clone, Copy)]
struct RunContext<'a> {
    deps: &'a RunnerDeps,
    record: &'a BulkOperationRecord,
    config: &'a OperationConfig,
    /// The operation's own write ceiling, from `MaxWriteRequestUnits` when
    /// the request carried one; unbounded otherwise.
    op_pacer: &'a WritePacer,
}

/// A programming or persistence fault the runner cannot record. The
/// operation is left non-terminal; the next claim after the lease expires
/// resumes it from the last committed checkpoint.
#[derive(Debug)]
// The payload is read through the derived Debug impl in the worker's error
// log line, which dead code analysis deliberately ignores.
#[allow(dead_code)]
pub(crate) enum RunnerError {
    /// The catalog refused a checkpoint or terminal write for a reason
    /// other than lease loss, or the claimed record's persisted counters
    /// are corrupt (negative) and the runner declined the claim.
    Storage(StorageError),
}

/// Drive one claimed operation to a terminal state. Returns `Ok(())` when
/// the operation was finished by this runner, and also when the lease was
/// lost and the runner stopped cleanly. `Err` is reserved for faults the
/// runner cannot record, such as a catalog that refuses the checkpoint.
pub(crate) async fn run(deps: &RunnerDeps, record: BulkOperationRecord) -> Result<(), RunnerError> {
    // A record whose persisted counters do not restore is corrupt; the
    // runner declines the claim rather than running on made-up state.
    // A fault an earlier claim held is held again (`runner::held`).
    let st = RunState::restore(&record).map_err(RunnerError::Storage)?;
    let mut st = restore_held_fault(&record, st);
    // The claim that handed this record over granted a fresh lease an
    // instant ago; the store's clock and this one may differ, so the local
    // deadline is a floor on when the lease can have run out, never a
    // promise that it still holds.
    st.lease_deadline = Some(Instant::now() + deps.config.lease);
    let config = match OperationConfig::parse(&record) {
        Ok(config) => Some(config),
        Err(detail) => {
            tracing::error!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                detail,
                "bulk operation configuration does not parse"
            );
            None
        }
    };

    // A record started on another store than this server's (the operator
    // changed `[bulk] store` with operations in flight) is neither read
    // nor rendered here: it ends with the internal failure.
    let recorded = match check_recorded_store(deps, &record) {
        Ok(recorded) => recorded,
        Err(detail) => {
            tracing::error!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                configured_store = deps.store_kind,
                detail,
                "bulk operation was not started on the configured store; it is failed unread"
            );
            st.failure = Some(Failure::internal());
            return finalize(deps, &record, None, st).await;
        }
    };
    let config = config.map(|config| OperationConfig {
        caller: recorded.principal,
        ..config
    });

    // Before anything is written: this claim becomes the operation's only
    // write owner, and the rows applied past the checkpoint are loaded.
    if !take_over(deps, &record, &mut st).await? {
        // The resume bound is spent. A table deleted meanwhile ends the
        // operation with the deleted-table failure, as every other claim
        // after the deletion would; a pending cancel outranks both.
        if record.status != STATUS_CANCELLING
            && matches!(table_binding_holds(deps, &record).await, Ok(false))
        {
            st.failure = Some(Failure::table_not_found());
        }
        return finalize(deps, &record, config.as_ref(), st).await;
    }

    // Step 1: a claimed CANCELLING operation reads no input. The persisted
    // counters and records are rendered and the operation ends CANCELLED.
    if record.status == STATUS_CANCELLING {
        st.cancelled = true;
        return finalize(deps, &record, config.as_ref(), st).await;
    }
    let Some(config) = config else {
        st.failure = Some(Failure::internal());
        return finalize(deps, &record, None, st).await;
    };

    // Step 2: the table binding, then the key metadata. The metadata is
    // read again at every checkpoint boundary (see `refresh_key_info`).
    match table_binding_holds(deps, &record).await {
        Ok(true) => {}
        Ok(false) => {
            st.failure = Some(Failure::table_not_found());
            return finalize(deps, &record, Some(&config), st).await;
        }
        Err(error) => {
            tracing::error!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                error = %error,
                "table binding check failed"
            );
            st.failure = Some(Failure::internal());
            return finalize(deps, &record, Some(&config), st).await;
        }
    }
    let key_info = match deps
        .storage
        .table_key_info(&record.account_id, &record.table_name)
        .await
    {
        Ok(info) if info.table_id == record.table_id => info,
        // A different table_id under the same name, or a table the data
        // plane cannot serve, ends the operation the way a deleted table
        // does.
        Ok(_) | Err(StorageError::TableNotFound(_) | StorageError::TableNotActive(_)) => {
            st.failure = Some(Failure::table_not_found());
            return finalize(deps, &record, Some(&config), st).await;
        }
        Err(error) => {
            tracing::error!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                error = %error,
                "table key info fetch failed"
            );
            st.failure = Some(Failure::internal());
            return finalize(deps, &record, Some(&config), st).await;
        }
    };
    st.key_info = key_info;

    drive(deps, &record, &config, st).await
}

#[cfg(test)]
mod tests;
