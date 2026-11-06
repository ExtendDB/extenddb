// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Finalizing a run: rendering the manifest and the error and
//! condition-check-failure reports from persisted records, writing the
//! terminal transition, and recording the terminal metrics.

use super::*;

/// Page size the report renderer reads persisted records with.
pub(super) const RENDER_PAGE_SIZE: usize = 1000;

/// Log a storage read failure during rendering; the operation ends with
/// the internal failure and the detail stays in the log.
pub(super) fn render_read_failure(account: &str, operation: &str, error: &StorageError) -> Failure {
    tracing::error!(
        account_id = %account,
        operation_id = %operation,
        error = %error,
        "report records could not be read for rendering"
    );
    Failure::internal()
}

/// Log a sink failure during rendering; the operation ends with the
/// internal failure and no path reaches a client.
pub(super) fn render_sink_failure(account: &str, operation: &str, error: &SinkError) -> Failure {
    tracing::error!(
        account_id = %account,
        operation_id = %operation,
        error = ?error,
        "bulk report rendering failed"
    );
    // A refused report path (a symlink or a non-directory component in the
    // caller's tree, a jail escape) is the caller's to fix; an I/O error is
    // the server's, except the report bucket itself being gone, which is
    // the caller's location fault (the manifest check that follows the
    // render reports it in the measured form).
    match error {
        // The store was unavailable past its retries: the operation
        // resumes and writes its outputs again.
        SinkError::Io(io) if StoreUnavailable::of_io(io) => Failure::store_unavailable(),
        SinkError::Jail(_) | SinkError::Symlink(_) => Failure::access_denied(),
        SinkError::Io(io) if io.kind() == std::io::ErrorKind::NotFound => {
            Failure::output_bucket_missing()
        }
        // A report path the caller's own tree makes unusable (a regular
        // file where a directory component must be, a directory the server
        // may not write into) is the caller's location fault, the same
        // class as a symlink or jail refusal; the hosted service answers
        // S3AccessDenied for an unwritable report location. Genuinely
        // unexpected I/O kinds stay on InternalServerError.
        SinkError::Io(io) if location_denied(io) => Failure::access_denied(),
        SinkError::Io(_) => Failure::internal(),
        // A fault the store rendered keeps its own code and message.
        SinkError::Store(fault) => Failure::from_store(fault),
    }
}

/// Whether an I/O error from the report-path walk or the `create_new` is a
/// location the caller's own tree makes unusable: a component that is not a
/// directory (`ENOTDIR`), a file where a directory component must be created
/// (`EEXIST`), or a directory the server may not write into (`EACCES`,
/// `EPERM`). These are the caller's to fix, so they map to `S3AccessDenied`
/// the way the jail and symlink refusals do. Classified by the error kind,
/// never by message text.
fn location_denied(error: &std::io::Error) -> bool {
    use std::io::ErrorKind;
    // `PermissionDenied` covers EACCES and EPERM, `AlreadyExists` EEXIST and
    // `NotADirectory` ENOTDIR on Unix, and the matching codes on Windows, so
    // the classification does not depend on the platform's raw numbers.
    matches!(
        error.kind(),
        ErrorKind::PermissionDenied | ErrorKind::AlreadyExists | ErrorKind::NotADirectory
    )
}

/// Render each report kind that has persisted records: read the records in
/// pages, encode each page into the writer as it arrives, and finish the
/// file once, so the whole report is never held in memory. A failure
/// mid-render drops the writer, which removes the temp file.
/// How a render pass ended when it did not fail: every report was written,
/// or the lease had passed to another process and the runner must stop.
pub(super) enum Render {
    Rendered,
    LeaseLost,
    /// The lease re-read itself failed. Nothing can be decided about
    /// ownership, so the runner stops without a terminal transition and the
    /// record stays in progress for the next claim, as a failed checkpoint
    /// does.
    Interrupted(StorageError),
}

pub(super) async fn render_reports(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    config: &OperationConfig,
    output_bucket: &str,
) -> Result<Render, Failure> {
    let account = &record.account_id;
    let operation = &record.operation_id;
    for kind in [ReportKind::Error, ReportKind::ConditionCheckFailure] {
        let mut page = deps
            .storage
            .report_records_page(account, operation, kind, None, RENDER_PAGE_SIZE)
            .await
            .map_err(|error| render_read_failure(account, operation, &error))?;
        if page.is_empty() {
            continue;
        }
        // The caller's s3:PutObject on each report object, before the page
        // that writes into it; a writer dropped on a denial removes what it
        // wrote.
        let object_key =
            |index| report::report_key(&config.output_prefix, account, operation, kind, index);
        if let Some(failure) =
            put_object_failure(deps, record, config, output_bucket, &object_key(0)).await
        {
            return Err(failure);
        }
        let mut checked = 0;
        let mut writer = deps
            .sink
            .open(
                &config.output_at(account, output_bucket),
                &config.output_prefix,
                operation,
                kind,
                &deps.owner,
                &config.output_sse,
            )
            .await
            .map_err(|error| render_sink_failure(account, operation, &error))?;
        loop {
            let full = page.len() == RENDER_PAGE_SIZE;
            let after = page
                .last()
                .map(|record| (record.file_index, record.row_index));
            let bodies: Vec<Value> = page.into_iter().map(|record| record.record).collect();
            let index = writer.object_index();
            if index > checked {
                let key = object_key(index);
                if let Some(failure) =
                    put_object_failure(deps, record, config, output_bucket, &key).await
                {
                    return Err(failure);
                }
                checked = index;
            }
            writer
                .write_page(bodies)
                .await
                .map_err(|error| render_sink_failure(account, operation, &error))?;
            if !full {
                break;
            }
            page = deps
                .storage
                .report_records_page(account, operation, kind, after, RENDER_PAGE_SIZE)
                .await
                .map_err(|error| render_read_failure(account, operation, &error))?;
            if page.is_empty() {
                break;
            }
        }
        // Confirm the lease right before the
        // rename, once per report kind. Rendering is unbounded work; a
        // render outliving the lease must not replace a report the new
        // owner has already written, so a runner that no longer owns the
        // record drops its writer (the writer's Drop removes the temp
        // file) and stops with no further write of any kind.
        let holder = match deps.storage.get_bulk_operation(account, operation).await {
            Ok(holder) => holder,
            Err(error) => {
                tracing::warn!(
                    account_id = %account,
                    operation_id = %operation,
                    error = %error,
                    "lease could not be re-read before the report rename; the runner stops"
                );
                drop(writer);
                return Ok(Render::Interrupted(error));
            }
        };
        let held =
            holder.as_ref().and_then(|row| row.lease_owner.as_deref()) == Some(deps.owner.as_str());
        if !held {
            tracing::info!(
                account_id = %account,
                operation_id = %operation,
                "lease lost during report rendering; the temp file is dropped"
            );
            drop(writer);
            return Ok(Render::LeaseLost);
        }
        let key = writer
            .finish()
            .await
            .map_err(|error| render_sink_failure(account, operation, &error))?;
        tracing::info!(
            account_id = %account,
            operation_id = %operation,
            key = %key,
            "bulk report rendered"
        );
    }
    Ok(Render::Rendered)
}

/// Step 9: the final checkpoint, the report files, the conditional
/// terminal transition, and the ledger delete once the transition holds.
pub(super) async fn finalize(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    config: Option<&OperationConfig>,
    mut st: RunState,
) -> Result<(), RunnerError> {
    let account = &record.account_id;
    let operation = &record.operation_id;

    // The final checkpoint persists the last rows and records and proves
    // the lease is still held before any file is rendered. No row is
    // staged here: every path out of the read loop flushed or discarded
    // them (`read_file`).
    debug_assert!(
        st.staged.is_empty(),
        "finalize reached with rows staged; the read loop must flush or discard them"
    );
    let progress = st.progress(&record.table_arn);
    let renewed_at = Instant::now();
    match deps
        .storage
        .checkpoint(account, operation, &deps.owner, deps.config.lease, progress)
        .await
    {
        Err(error) => return Err(RunnerError::Storage(error)),
        Ok(LeaseState::Lost) => {
            tracing::info!(
                account_id = %account,
                operation_id = %operation,
                "lease lost at the final checkpoint; the runner stops"
            );
            return Ok(());
        }
        Ok(LeaseState::Held) => {}
    }
    // A store unavailable past its retries is no outcome: with the progress
    // checkpointed, the operation stays in progress for the next claim.
    if st
        .failure
        .as_ref()
        .is_some_and(Failure::is_store_unavailable)
        && stop_for_unavailable_store(record, &mut st)
    {
        return Ok(());
    }
    // The report and manifest writes wait on the output store; the lease
    // the final checkpoint renewed is kept while they wait.
    let mut deadline = Some(renewed_at + deps.config.lease);

    if st.dropped_records > 0 {
        tracing::info!(
            account_id = %account,
            operation_id = %operation,
            dropped_records = st.dropped_records,
            "report records beyond the record count or byte cap were dropped"
        );
    }

    // Render each report kind that has records, when a report bucket is
    // configured. A rendering failure fails the operation; nothing about
    // the path reaches a client. A lease observed lost during rendering
    // stops the runner here: no finish, no further writes.
    if let Some(config) = config
        && let Some(output_bucket) = config.output_bucket.as_deref()
        && !st.skip_render
    {
        match render_outputs(deps, record, config, output_bucket, &mut st, &mut deadline).await {
            Outputs::Finish => {}
            Outputs::Stop => return Ok(()),
            Outputs::Interrupted(error) => return Err(RunnerError::Storage(error)),
            Outputs::Unavailable => {
                if stop_for_unavailable_store(record, &mut st) {
                    return Ok(());
                }
            }
        }
    }

    // An abandon that left bytes of its file uncounted, a file refused as
    // not a compressed object, and an object overwritten after its listing
    // all end the operation with the validation pair once the run has
    // finished its remaining files (measured on the service for the
    // not-compressed class, the cut and the overwritten object). The
    // abandons of earlier incarnations are restored with their kinds.
    if st.failure.is_none()
        && !st.cancelled
        && (st.cut_file_seen || st.abandoned.values().any(|a| a.fails_operation()))
    {
        st.failure = Some(Failure::items_failed_validation());
    }
    let status = terminal_status(st.failure.as_ref(), st.cancelled, st.failed);
    let (failure_code, failure_message) = match st.failure {
        Some(failure) => (Some(failure.code.into_owned()), Some(failure.message)),
        None => (None, None),
    };
    let outcome = Terminal {
        status: status.to_owned(),
        end_time_ms: epoch_millis(),
        failure_code,
        failure_message,
        usage: st.usage.to_value(&record.table_arn),
    };
    // finish is conditional on the lease and turns a CANCELLING row into
    // CANCELLED whatever status the runner computed; it answers with the
    // status the row now stores.
    match deps
        .storage
        .finish(account, operation, &deps.owner, outcome)
        .await
    {
        Err(error) => Err(RunnerError::Storage(error)),
        Ok(FinishState::Lost) => {
            tracing::info!(
                account_id = %account,
                operation_id = %operation,
                "lease lost at the terminal transition; the runner stops"
            );
            Ok(())
        }
        Ok(FinishState::Held(stored)) => {
            // The ledger rows are deleted only after the terminal
            // transition succeeds. On the failure path the persisted
            // cursor is the last checkpoint, and the markers of rows a
            // resume would re-read must survive any crash before
            // `finish`; once the operation is terminal it is never
            // resumed, so the rows are never read again. A lost lease skips the delete with
            // everything else, and if the delete fails the retention
            // purge removes the rows later.
            if let Err(error) = deps
                .storage
                .delete_bulk_row_outcomes(account, operation)
                .await
            {
                tracing::warn!(
                    account_id = %account,
                    operation_id = %operation,
                    error = %error,
                    "bulk row outcome ledger delete failed"
                );
            }
            tracing::info!(
                account_id = %account,
                operation_id = %operation,
                table_name = %record.table_name,
                status = %stored,
                processed_item_count = st.processed,
                failed_item_count = st.failed,
                "bulk operation reached a terminal state"
            );
            record_finished_metrics(deps, &record.table_name, &stored, st.written);
            Ok(())
        }
    }
}

/// Whether the run stops for a store unavailable past its retries, leaving
/// the operation in progress for the next claim to resume from its
/// checkpoint. A claim the resume bound refused does not stop: the
/// operation ends with the internal failure instead.
fn stop_for_unavailable_store(record: &BulkOperationRecord, st: &mut RunState) -> bool {
    if st.resumes_spent {
        if st
            .failure
            .as_ref()
            .is_none_or(Failure::is_store_unavailable)
        {
            st.failure = Some(Failure::internal());
        }
        return false;
    }
    tracing::warn!(
        account_id = %record.account_id,
        operation_id = %record.operation_id,
        "the bulk store was unavailable past its retries; the operation resumes at its checkpoint"
    );
    true
}

/// The terminal-transition metrics, recorded only on the transition that
/// held the lease, so a resumed operation is counted once. The status
/// dimension is the status `finish` stored, which for a CANCELLING row is
/// CANCELLED whatever the runner computed. `written`
/// counts the rows whose write committed in this process; rows an earlier
/// claim applied and condition failures are not counted.
fn record_finished_metrics(deps: &RunnerDeps, table_name: &str, stored: &str, written: u64) {
    deps.metrics.record(
        MetricName::BulkOperationsFinished,
        1.0,
        Some(table_name),
        None,
        Some(stored),
    );
    if written > 0 {
        // Bounded by max_bulk_item_count, far below 2^53, so the value
        // is exact.
        #[allow(clippy::cast_precision_loss)]
        deps.metrics.record(
            MetricName::BulkPutItemsWritten,
            written as f64,
            Some(table_name),
            None,
            None,
        );
    }
}
