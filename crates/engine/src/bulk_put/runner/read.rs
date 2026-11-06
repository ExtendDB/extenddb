// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The read side: driving one operation file by file, resuming across a
//! crash by skipping a committed region, checking the output configuration
//! at the first record, and the end-of-file and abandon bookkeeping.

use super::*;

/// Whether the catalog still holds the operation's table: same `table_id`,
/// status `ACTIVE` or `UPDATING`. A missing table answers false; any other
/// catalog failure is the caller's to map.
pub(super) async fn table_binding_holds(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
) -> Result<bool, StorageError> {
    let input = DescribeTableInput {
        table_name: record.table_name.clone(),
    };
    match deps.storage.describe_table(&record.account_id, input).await {
        Ok(description) => Ok(description.table_id == record.table_id
            && matches!(
                description.table_status,
                TableStatus::Active | TableStatus::Updating
            )),
        Err(StorageError::TableNotFound(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

/// What the caller of a row or boundary step does next.
pub(super) enum Flow {
    /// Keep reading the current file.
    NextRow,
    /// The current file is finished or abandoned; move to the next one.
    FileDone,
    /// The lease is lost or the record is gone; stop without writing.
    Stop,
    /// A terminal condition is set on the state; run the end sequence.
    Finish,
}

/// The per-operation input quotas, checked against the enumerated file
/// list before any file is opened. The hosted service accepts the Start
/// and ends the operation `FAILED` with its own code for each class
/// (measured on the service), so the runner answers the same
/// way for a record whose enumeration no longer fits the configured caps.
///
/// The returned flag says whether the output prefix is left untouched:
/// the count class writes neither reports nor manifest, while
/// the size class writes a manifest with the file `NOT_PROCESSED`.
/// `None` means both quotas hold and the files are read.
fn input_quota_failure(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    config: &OperationConfig,
) -> Option<(Failure, bool)> {
    let limits = &deps.limits;
    let count = u64::try_from(record.input_files.len()).unwrap_or(u64::MAX);
    if count > limits.max_bulk_input_files {
        let failure = Failure::objects_count_limit_exceeded(
            &config.input_prefix,
            limits.max_bulk_input_files,
        );
        return Some((failure, true));
    }
    let over_size = record
        .input_files
        .iter()
        .any(|file| u64::try_from(file.size).unwrap_or(0) > limits.max_bulk_input_object_bytes);
    over_size.then(|| {
        (
            Failure::input_object_too_large(limits.max_bulk_input_object_bytes),
            false,
        )
    })
}

/// Steps 4 to 8: read every file from the restored cursor, then run the
/// end sequence. Step 10 falls out of the loop: an empty file list goes
/// straight to the end sequence with zero counters.
pub(super) async fn drive(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    config: &OperationConfig,
    mut st: RunState,
) -> Result<(), RunnerError> {
    if let Some((failure, skip_render)) = input_quota_failure(deps, record, config) {
        tracing::warn!(
            account_id = %record.account_id,
            operation_id = %record.operation_id,
            input_files = record.input_files.len(),
            max_bulk_input_files = deps.limits.max_bulk_input_files,
            max_bulk_input_object_bytes = deps.limits.max_bulk_input_object_bytes,
            failure_code = %failure.code,
            "bulk operation input passes a configured quota; no file is read"
        );
        st.failure = Some(failure);
        st.skip_render = skip_render;
        return finalize(deps, record, Some(config), st).await;
    }
    // `MaxWriteRequestUnits` was validated at Start to be at least 1 when
    // present; a stored value outside that range (hand-edited catalog) is
    // treated as absent rather than refusing the claim.
    let op_pacer = WritePacer::new(
        record
            .max_write_request_units
            .and_then(|units| u64::try_from(units).ok())
            .unwrap_or(0),
    );
    let ctx = RunContext {
        deps,
        record,
        config,
        op_pacer: &op_pacer,
    };
    let mut file_index = st.cursor_file;
    let mut skip_rows = st.cursor_row;
    while file_index < record.input_files.len() {
        let file = &record.input_files[file_index];
        let file_size = u64::try_from(file.size).unwrap_or(0);
        st.cursor_file = file_index;
        let step = read_file(&ctx, &mut st, file, file_index, file_size, skip_rows).await?;
        skip_rows = 0;
        match step {
            Flow::Stop => return Ok(()),
            Flow::Finish => break,
            Flow::FileDone | Flow::NextRow => file_index += 1,
        }
    }
    let paced = op_pacer.waited();
    if !paced.is_zero() {
        tracing::info!(
            account_id = %record.account_id,
            operation_id = %record.operation_id,
            paced_ms = paced.as_millis(),
            max_write_request_units = ?record.max_write_request_units,
            "bulk operation writes paced by MaxWriteRequestUnits"
        );
    }
    end_with_held_location_fault(&mut st);
    finalize(deps, record, Some(config), st).await
}

/// Resume: skip the physical lines the restored checkpoint already
/// covered. Their counters and records are already persisted. Skipped
/// lines count toward the batch boundary and the line cap so the lease
/// renews, a cancel is observed, and the work bound holds at the same
/// cadence as fresh reads. `Some` ends the file or the run; `None` hands
/// the reader back positioned for fresh reads.
pub(super) async fn skip_covered_region(
    ctx: &RunContext<'_>,
    st: &mut RunState,
    reader: &mut dyn LineReader,
    file: &InputFile,
    file_index: usize,
    file_size: u64,
    skip_rows: u64,
) -> Result<Option<Flow>, RunnerError> {
    let RunContext { deps, record, .. } = *ctx;
    let mut row_index: u64 = 0;
    while row_index < skip_rows {
        let next =
            match keeping_lease(ctx, st, reader.next_line(deps.config.max_line_bytes)).await? {
                Waited::Answer(next) => next,
                Waited::Flow(flow) => return Ok(Some(flow)),
            };
        match next {
            Ok(Some(_)) => {
                row_index += 1;
                if st.consume_line(deps.config.max_item_count) {
                    return Ok(Some(Flow::Finish));
                }
                if st.checkpoint_due(deps.config.batch_size) || deps.shutdown.is_cancelled() {
                    match boundary(ctx, st).await? {
                        Flow::NextRow => {}
                        other => return Ok(Some(other)),
                    }
                }
            }
            Ok(None) => {
                let etag = reader.etag();
                return end_of_file(ctx, st, file_index, file_size, etag)
                    .await
                    .map(Some);
            }
            // The object was overwritten while the covered region was read
            // again: the rows the earlier claim counted stand, and so do
            // their bytes, as at a resumed open.
            Err(SourceError::Overwritten) => {
                let counted = st.bytes_floor.saturating_sub(st.completed_bytes);
                return record_overwritten(
                    ctx, st, file, file_index, skip_rows, counted, file_size,
                )
                .await
                .map(Some);
            }
            // A line over the line limit, or one that is not valid UTF-8,
            // inside the covered region means the file changed since a
            // committed checkpoint counted those rows, as an early end of
            // file does. The file is abandoned
            // with no record and no counter change; a resume must never
            // re-count or re-record a row index a committed checkpoint
            // covers.
            Err(error) if skip_fault_abandons(&error) => {
                tracing::warn!(
                    account_id = %record.account_id,
                    operation_id = %record.operation_id,
                    path = %file.path,
                    error = ?error,
                    "bulk input file changed under the cursor; the file is abandoned"
                );
                return end_of_file(ctx, st, file_index, file_size, None)
                    .await
                    .map(Some);
            }
            // Any other read fault does not imply a changed file and fails
            // the operation exactly as it would on a first read.
            Err(error) => {
                tracing::error!(
                    account_id = %record.account_id,
                    operation_id = %record.operation_id,
                    path = %file.path,
                    error = ?error,
                    "bulk input file failed mid-read"
                );
                if is_unavailable_object(&error) {
                    st.unavailable_file = Some(file_index);
                }
                st.failure = Some(source_failure(&error));
                return Ok(Some(Flow::Finish));
            }
        }
    }
    Ok(None)
}

/// The output configuration, checked when report records are produced. The
/// caller's `s3:PutObject` on object 0 of each report kind is checked at
/// that kind's first record in this claim, and the sink's location check
/// runs once, at the operation's first record (the kept count moves from
/// zero to one): on the filesystem store the output bucket directory must
/// exist, decided with the sink's handle primitives, and the output
/// `S3BucketOwner`, when given, must be the caller's account. Measured in
/// both directions: a missing output bucket, or a mismatched owner, with no
/// failing rows completes, and the first record finds the fault, which
/// fails the operation with `S3NoSuchBucket` or `S3AccessDenied`. Ownership
/// is decided before existence, as at Start.
///
/// A denial, or a fault the store renders, is held (`hold_location_fault`):
/// the run reads on to the end of the input, writing and checkpointing
/// rows, and then fails with it (`end_with_held_location_fault`). Any other
/// check failure is the server's: the run state reverts to the last
/// checkpoint and the operation ends at once with the internal failure.
/// Nothing is rendered on either path (the records were never validated
/// against the output configuration); the render-time missing-bucket
/// failure stays as a last line of defence. A claim that resumes checks
/// each kind again at its own first record; the policies decide the same
/// way unless they changed.
///
/// Returns `Some(Flow::Finish)` when the operation must end, `None` to
/// continue.
pub(super) async fn check_output_config_at_first_record(
    ctx: &RunContext<'_>,
    st: &mut RunState,
) -> Option<Flow> {
    let RunContext {
        deps,
        record,
        config,
        ..
    } = *ctx;
    if st.kept_records == 0 || st.held_location.is_some() {
        return None;
    }
    let Some(bucket) = config.output_bucket.as_deref() else {
        // No report is written; there is nothing to validate.
        st.output_checked = true;
        return None;
    };
    // The caller's s3:PutObject on object 0 of each kind waiting for a
    // checkpoint and not yet checked, oldest record first, is held the same
    // way as a location fault (measured on the service: a bucket policy
    // denying the report write). A check that cannot be evaluated is the
    // server's fault.
    let mut kinds: Vec<ReportKind> = Vec::new();
    for pending in &st.pending {
        if !st.put_checked.contains(&pending.kind) && !kinds.contains(&pending.kind) {
            kinds.push(pending.kind);
        }
    }
    for kind in kinds {
        st.put_checked.push(kind);
        let key = report::report_key(
            &config.output_prefix,
            &record.account_id,
            &record.operation_id,
            kind,
            0,
        );
        match put_object_decision(deps, record, config, bucket, &key).await {
            Decision::Allowed => {}
            Decision::Denied(message) => {
                st.output_checked = true;
                hold_location_fault(
                    st,
                    Failure {
                        code: Cow::Borrowed("S3AccessDenied"),
                        message,
                    },
                );
                return None;
            }
            Decision::Unavailable => return Some(end_at_first_record(ctx, st).await),
        }
    }
    if st.output_checked {
        return None;
    }
    st.output_checked = true;
    // The key the location check names is report object 0 of the kind of
    // the oldest record waiting for a checkpoint (the error report when
    // none is waiting); the filesystem store does not read it.
    let kind = st
        .pending
        .first()
        .map_or(ReportKind::Error, |first| first.kind);
    let key = report::report_key(
        &config.output_prefix,
        &record.account_id,
        &record.operation_id,
        kind,
        0,
    );
    // The check waits on the store for up to its bound. It runs before the
    // checkpoint that would persist the record it checks for, so the lease
    // is kept without moving the cursor. Every caller flushed its staged
    // rows before the check, so a cancel or a deleted table ends the run
    // here as at a boundary.
    let mut deadline = st.lease_deadline;
    let checked = keeping_lease_in_place(
        deps,
        record,
        &mut deadline,
        Watch::CancelAndTable,
        deps.sink.check_location(
            &config.output_at(&record.account_id, bucket),
            &key,
            &config.output_sse,
        ),
    )
    .await;
    st.lease_deadline = deadline;
    let answer = match checked {
        InPlace::Answer(answer) => answer,
        InPlace::Lost => return Some(Flow::Stop),
        InPlace::Cancelled => {
            st.cancelled = true;
            return Some(Flow::Finish);
        }
        InPlace::TableGone => {
            st.failure = Some(Failure::table_not_found());
            return Some(Flow::Finish);
        }
        InPlace::Interrupted(error) => {
            tracing::error!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                error = %error,
                "lease renewal failed during the output check; the runner stops"
            );
            return Some(Flow::Stop);
        }
    };
    match answer {
        Ok(()) => None,
        Err(SinkError::Store(fault)) => {
            tracing::info!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                failure_code = fault.code,
                "output location fault at the first report record; held to the end of the input"
            );
            hold_location_fault(st, Failure::from_store(&fault));
            None
        }
        // The store was unavailable past its retries. The record that
        // asked for the check is behind no checkpoint, so the run reverts
        // to the last one, the next claim checks again at the same record,
        // and the run stops in progress (`finalize`).
        Err(SinkError::Io(io)) if StoreUnavailable::of_io(&io) => {
            revert_to_last_checkpoint(ctx, st).await;
            st.output_checked = false;
            st.failure = Some(Failure::store_unavailable());
            Some(Flow::Finish)
        }
        Err(error) => {
            tracing::error!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                error = ?error,
                "output bucket check failed at the first report record"
            );
            Some(end_at_first_record(ctx, st).await)
        }
    }
}

/// End the operation at once with the internal failure, from the first
/// report record: revert to the last checkpoint (the current window is
/// never checkpointed, and `output_checked` stays set so the revert is not
/// repeated) and render nothing.
async fn end_at_first_record(ctx: &RunContext<'_>, st: &mut RunState) -> Flow {
    revert_to_last_checkpoint(ctx, st).await;
    st.output_checked = true;
    st.failure = Some(Failure::internal());
    st.skip_render = true;
    Flow::Finish
}

/// The end-of-file bookkeeping and boundary: `ProcessedInputBytes`
/// advances by `file_size`, the bytes this file contributed (its
/// enumerated size, or the end of the fault's read when an abandon cut it
/// short, or zero for a file refused as not compressed), and the cursor
/// moves to the
/// next file before the checkpoint. `etag` is the reader's entity tag when
/// the raw stream was consumed whole, `None` on an abandon or a changed
/// file; the checkpoint that passes a finished file stores its row count
/// and tag on the record, so neither is computed again by a resume or by
/// the manifest.
pub(super) async fn end_of_file(
    ctx: &RunContext<'_>,
    st: &mut RunState,
    file_index: usize,
    file_size: u64,
    etag: Option<String>,
) -> Result<Flow, RunnerError> {
    // The cursor row at end of file is the count of physical lines the
    // file held; the manifest reports the last counted row from it. An
    // abandoned file keeps its abandon row instead, and the bytes it
    // contributed, so a later claim restores a cut without inferring it
    // from the progress counter. Either way the next checkpoint stores the
    // file's outcome beside the cursor that passes it, so a later claim and
    // the manifest see it.
    if let Some(abandon) = st.abandoned.get(&file_index).copied() {
        st.pending_abandoned.push(AbandonedFile {
            index: file_index,
            row: abandon.row,
            kind: abandon.kind,
            bytes: file_size,
        });
    } else {
        st.file_row_counts.insert(file_index, st.cursor_row);
        if let Some(etag) = &etag {
            st.file_etags.insert(file_index, etag.clone());
        }
        st.pending_finished.push(FinishedFile {
            index: file_index,
            rows: st.cursor_row,
            etag,
        });
    }
    st.completed_bytes = st.completed_bytes.saturating_add(file_size);
    st.current_bytes = 0;
    st.cursor_file = file_index + 1;
    st.cursor_row = 0;
    // The next file has not been opened yet; the manifest reports a file
    // the operation never opened as `NOT_PROCESSED`.
    st.current_file_opened = false;
    match boundary(ctx, st).await? {
        Flow::NextRow => Ok(Flow::FileDone),
        other => Ok(other),
    }
}

/// The bytes an abandoned file contributes to `ProcessedInputBytes`, by
/// the measured read rule: the end of the read that held the fault when
/// bytes of the file lie beyond it, the enumerated size otherwise. A cut is
/// recorded on the state, which ends the operation with the
/// items-failed-validation pair, and lowers the byte floor to the cut
/// total so a checkpoint inside the same file cannot hold the counter
/// above it (measured on the service).
///
/// Under a decoder the same rule runs on the compressed stream with the
/// decoder's read geometry (measured on the service), and
/// `fault_offset` is the compressed bytes the read chain had pulled when
/// the faulting line was decoded. ExtendDB's chain pulls the stored object
/// in 8 KiB fills, so that offset can sit up to 8 KiB past the compressed
/// byte the decoder needed; a fault whose compressed position lies within
/// one fill of a read boundary may be attributed to the next read.
pub(super) fn abandon_file_bytes(
    st: &mut RunState,
    codec: InputCodec,
    fault_offset: u64,
    file_size: u64,
) -> u64 {
    match ReadGeometry::of(codec).cut_processed_bytes(fault_offset, file_size) {
        Some(cut) => {
            st.cut_file_seen = true;
            st.bytes_floor = st.bytes_floor.min(st.completed_bytes.saturating_add(cut));
            cut
        }
        None => file_size,
    }
}

/// Count and record an abandon: the offending row is processed and failed,
/// the record carries the one abandon message with no `itemIndex` and no
/// `newImage`, and the rest of the file is not read. The line itself was
/// already counted against the cap by the caller.
pub(super) fn record_abandon(
    deps: &RunnerDeps,
    config: &OperationConfig,
    st: &mut RunState,
    file: &InputFile,
    file_index: usize,
    abandon: Abandon,
    message: &str,
) {
    st.processed += 1;
    st.failed += 1;
    st.abandoned.insert(file_index, abandon);
    st.push_record(
        &ReportSink {
            max_records: deps.config.max_report_records,
            max_bytes: deps.config.max_report_bytes,
            requested: &config.output_types,
        },
        ReportKind::Error,
        file_index,
        abandon.row,
        report::error_record(&config.input_bucket, &file.path, None, message, None),
    );
}

#[cfg(test)]
mod m60_tests {
    use super::*;

    // The abandon cut lowers `bytes_floor` so a
    // resume whose restored byte figure already ran past the cut's read end
    // (decoder read-ahead a prior incarnation checkpointed) reports the cut,
    // not the read-ahead. The forward single-pass cut tests never reach it,
    // because in one pass the cut's read end only grows; it bites only when
    // a restored floor exceeds a later cut. Here a restored floor of 200,000
    // bytes meets a gzip abandon in the first read (cut 65,546), and the cut
    // must lower the floor to 65,546. Without the lowering the floor would
    // stand at 200,000 and the operation would over-report its processed
    // bytes.
    #[test]
    fn a_compressed_cut_lowers_the_bytes_floor_below_a_restored_read_ahead_figure() {
        let mut st = RunState {
            bytes_floor: 200_000,
            completed_bytes: 0,
            ..RunState::default()
        };
        let cut = abandon_file_bytes(&mut st, InputCodec::Gzip, 100, 204_490);
        assert_eq!(
            cut, 65_546,
            "the cut is the end of the gzip read that held the fault"
        );
        assert!(st.cut_file_seen, "the cut is recorded");
        assert_eq!(
            st.bytes_floor, 65_546,
            "the cut lowers the floor below the read-ahead figure a resume restored"
        );
    }
}
