// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Resuming an operation where its applied rows end.
//!
//! A write batch commits in the data database and the checkpoint that
//! covers it commits afterwards in the catalog, so an interruption between
//! the two (a kill, a lease takeover, a transient fault on a later batch of
//! the same checkpoint window) leaves rows applied past the persisted
//! cursor. Every batch records each row it applies in the row-outcome
//! ledger, in the batch's own transaction, with the size of the item the
//! row replaced and, when the index charge or a condition-failure record
//! needs it, that item. A claim first records itself as the operation's
//! write owner, which makes every batch of an earlier owner either already
//! committed or refused, and then reads the rows recorded past its
//! checkpoint. The run reads those rows again for the counters and the
//! records, and applies each recorded result in place of a write, so the
//! table, the counters, the usage and the reports are those of a run that
//! was never interrupted, and a client write that landed after a row's
//! batch committed is never overwritten by that row again.
//!
//! The claim also bounds resumes that make no progress: the owner record
//! counts the claims in a row that found the checkpoint where the claim
//! before them found it, and once that count reaches
//! [`MAX_STALLED_RESUMES`] the operation ends without reading input:
//! `FAILED` with the internal failure, the deleted-table failure when its
//! table is gone, or `CANCELLED` when a cancel is pending.

use super::*;

/// Consecutive resumes that find the checkpoint where the resume before
/// them found it; the one that reaches this count ends the operation
/// `FAILED` with `InternalServerError` instead of running.
pub(super) const MAX_STALLED_RESUMES: u32 = 10;

/// The status the store records for an operation whose resume bound is
/// spent: `finish` ends a `CANCELLING` row `CANCELLED` with the cancel
/// pair whatever the runner computed, and any other row `FAILED`.
pub(super) fn outcome_after_the_bound(status: &str) -> &'static str {
    if status == STATUS_CANCELLING {
        "CANCELLED"
    } else {
        "FAILED"
    }
}

/// Record this claim as the operation's write owner and load the rows the
/// ledger holds past the restored checkpoint. `Ok(false)` means the resume
/// bound ended the operation: the failure is set and the caller finalizes
/// without reading input. An error leaves the operation for the next claim.
pub(super) async fn take_over(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    st: &mut RunState,
) -> Result<bool, RunnerError> {
    let account = &record.account_id;
    let operation = &record.operation_id;
    let cursor = (record.cursor_file_index, record.cursor_row_index);
    let stalled = match deps
        .storage
        .bulk_write_takeover(account, operation, &deps.owner, cursor)
        .await
    {
        Ok(stalled) => stalled,
        Err(error) => {
            tracing::warn!(
                account_id = %account,
                operation_id = %operation,
                error = %error,
                "bulk operation write owner could not be recorded; the operation resumes later"
            );
            return Err(RunnerError::Storage(error));
        }
    };
    if stalled >= MAX_STALLED_RESUMES {
        let ends = outcome_after_the_bound(&record.status);
        tracing::error!(
            account_id = %account,
            operation_id = %operation,
            resumes = stalled,
            cursor_file_index = cursor.0,
            cursor_row_index = cursor.1,
            "bulk operation resumed {MAX_STALLED_RESUMES} times without its checkpoint advancing; it ends {ends}"
        );
        st.failure = Some(Failure::internal());
        st.resumes_spent = true;
        return Ok(false);
    }
    if stalled > 0 {
        tracing::warn!(
            account_id = %account,
            operation_id = %operation,
            attempt = stalled,
            max_attempts = MAX_STALLED_RESUMES,
            cursor_file_index = cursor.0,
            cursor_row_index = cursor.1,
            "bulk operation resumed without its checkpoint advancing since the last resume"
        );
    }
    st.settled = cursor;
    st.recorded_next = u64::try_from(cursor.1)
        .ok()
        .map(|row| (st.cursor_file, row));
    load_recorded_page(deps, record, st).await?;
    if !st.recorded.is_empty() {
        tracing::info!(
            account_id = %account,
            operation_id = %operation,
            rows = st.recorded.len(),
            more = st.recorded_next.is_some(),
            "bulk operation resumes past rows applied after its last checkpoint"
        );
    }
    Ok(true)
}

/// Rows of the ledger read in one page. A recorded row can keep the item
/// it replaced, up to the item size limit, so a resumed run holds at most
/// this many of them beyond the write batch it is flushing.
pub(crate) const RECORDED_PAGE: i64 = 128;

/// Read the next page of recorded rows into `st.recorded` and move
/// `st.recorded_next` past it, or clear it when the page was not full.
async fn load_recorded_page(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    st: &mut RunState,
) -> Result<(), RunnerError> {
    let Some((file, from)) = st.recorded_next else {
        return Ok(());
    };
    let (Ok(file_index), Ok(from_row)) = (i64::try_from(file), i64::try_from(from)) else {
        st.recorded_next = None;
        return Ok(());
    };
    let rows = deps
        .storage
        .bulk_rows_recorded(
            &record.account_id,
            &record.operation_id,
            file_index,
            from_row,
            RECORDED_PAGE,
        )
        .await
        .map_err(RunnerError::Storage)?;
    let full = i64::try_from(rows.len()).unwrap_or(i64::MAX) >= RECORDED_PAGE;
    let mut last = None;
    for row in rows {
        if let Ok(index) = u64::try_from(row.row_index) {
            last = Some(index);
            st.recorded.insert((file, index), row);
        }
    }
    st.recorded_next = match last {
        Some(last) if full => Some((file, last.saturating_add(1))),
        _ => None,
    };
    Ok(())
}

/// Read pages of recorded rows until every recorded row of `file` up to
/// `through` is in `st.recorded`, so a flush of staged rows up to that row
/// finds each one the ledger holds.
pub(super) async fn load_recorded_through(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    st: &mut RunState,
    file: usize,
    through: u64,
) -> Result<(), RunnerError> {
    while let Some((next_file, next_row)) = st.recorded_next {
        if next_file != file || next_row > through {
            break;
        }
        load_recorded_page(deps, record, st).await?;
    }
    Ok(())
}

/// Account for one staged row the ledger holds as applied, the way the
/// run that applied it accounted for its write result: the table charge
/// from the replaced item's size, the index charge from the replaced item,
/// and a failed condition's charge and record with the item it was
/// evaluated against. Nothing is written and no write units are taken.
pub(super) fn apply_recorded(
    ctx: &RunContext<'_>,
    st: &mut RunState,
    staged: Staged,
    recorded: RecordedRow,
) {
    let RunContext {
        deps,
        record,
        config,
        ..
    } = *ctx;
    let has_indexes = !st.key_info.global_secondary_indexes.is_empty()
        || !st.key_info.local_secondary_indexes.is_empty()
        || !st.key_info.vector_indexes.is_empty();
    match recorded.outcome {
        RowOutcome::Applied => {
            let old_size = recorded
                .old_size
                .map_or(0, |size| usize::try_from(size).unwrap_or(usize::MAX));
            st.usage.add_base(staged.item_bytes.max(old_size));
            if has_indexes {
                st.usage.add_index_writes(
                    recorded.old_image.as_ref(),
                    staged.image.as_ref(),
                    &st.key_info,
                );
            }
        }
        RowOutcome::ConditionFailed => {
            st.usage.add_condition_failed();
            let submitted = staged.image.unwrap_or_default();
            let path = &record.input_files[staged.file_index].path;
            st.push_record(
                &ReportSink {
                    max_records: deps.config.max_report_records,
                    max_bytes: deps.config.max_report_bytes,
                    requested: &config.output_types,
                },
                ReportKind::ConditionCheckFailure,
                staged.file_index,
                staged.row_index,
                report::condition_record(
                    &config.input_bucket,
                    path,
                    Some(to_i64(staged.row_index)),
                    &submitted,
                    recorded.old_image.as_ref(),
                ),
            );
        }
    }
}
