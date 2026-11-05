// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The write side: flushing staged rows to storage in one transaction,
//! handling a batch failure, reverting to the last checkpoint, and the
//! per-boundary checkpoint and key-info refresh.

use super::*;

/// Hand the staged rows to storage as one batch and apply each result.
///
/// A staged row the ledger already holds as applied (a resumed run reading
/// rows an earlier claim wrote past its checkpoint) is accounted for from
/// its ledger entry and not sent (`runner::resume`). The batch's write
/// units are taken from the process bucket and the operation bucket before
/// the call. `None` means the batch committed and the run continues;
/// `Some(flow)` is the terminal flow after a batch error, with the run
/// state reverted to the last checkpoint (nothing since it was committed).
pub(super) async fn flush_staged(
    ctx: &RunContext<'_>,
    st: &mut RunState,
) -> Result<Option<Flow>, RunnerError> {
    if st.staged.is_empty() {
        return Ok(None);
    }
    let RunContext {
        deps,
        record,
        config,
        op_pacer,
    } = *ctx;
    let mut staged = Vec::with_capacity(st.staged.len());
    if let Some(last) = st.staged.iter().map(|s| (s.file_index, s.row_index)).max() {
        resume::load_recorded_through(deps, record, st, last.0, last.1).await?;
    }
    for s in std::mem::take(&mut st.staged) {
        match st.recorded.remove(&(s.file_index, s.row_index)) {
            Some(recorded) => apply_recorded(ctx, st, s, recorded),
            None => staged.push(s),
        }
    }
    if staged.is_empty() {
        return Ok(check_output_config_at_first_record(ctx, st).await);
    }
    // A lease that has run out by this process's own clock may already
    // belong to another process that resumed from the last checkpoint and
    // ran ahead. Writing the staged rows now would land them after that
    // owner's later rows for the same keys, so the rows are dropped (they
    // are behind no checkpoint and the new owner reads them again) and the
    // runner stops without another catalog write.
    if st
        .lease_deadline
        .is_some_and(|deadline| Instant::now() >= deadline)
    {
        let dropped = staged.len();
        tracing::info!(
            account_id = %record.account_id,
            operation_id = %record.operation_id,
            dropped_rows = dropped,
            "lease deadline passed before a write batch; the runner stops"
        );
        return Ok(Some(Flow::Stop));
    }
    let mut rows = Vec::with_capacity(staged.len());
    let mut metas = Vec::with_capacity(staged.len());
    let mut units: u64 = 0;
    for mut s in staged {
        units = units.saturating_add(write_units(s.item_bytes));
        if let Some(marker) = s.row.marker.as_mut() {
            marker.settled = st.settled;
        }
        rows.push(s.row);
        metas.push((s.file_index, s.row_index, s.item_bytes, s.image));
    }
    deps.pacer.acquire(units).await;
    op_pacer.acquire(units).await;

    let stream = stream_capture_for(&st.key_info, &deps.region);
    let results = match deps
        .storage
        .put_items_batch(&st.key_info, rows, stream.as_ref())
        .await
    {
        Ok(results) => results,
        // A transient storage fault (a connection reset, an admin or crash
        // shutdown, a serialization or deadlock abort, a lock timeout, a
        // full disk or another exhausted resource, a busy or locked
        // database, a pool timeout) left the batch rolled back, and a batch
        // refused because another claim took the operation over wrote
        // nothing either. Returning it as a runner storage error leaves the
        // record non-terminal and its lease to expire, the same path a
        // checkpoint fault takes: the next claim restores from the last
        // checkpoint and finds every row the earlier batches of this window
        // applied in the ledger. Every other storage fault (validation, a
        // missing table, a genuine internal error) stays terminal through
        // `batch_failure`.
        Err(error) if write_batch_fault_resumes(&error) => {
            if lease_passed_on(ctx).await {
                tracing::info!(
                    account_id = %record.account_id,
                    operation_id = %record.operation_id,
                    "another claim owns the operation; the batch wrote nothing and the runner stops"
                );
                return Ok(Some(Flow::Stop));
            }
            tracing::warn!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                table_name = %record.table_name,
                error = %error,
                "bulk batch write hit a transient storage fault; the operation resumes at the last checkpoint"
            );
            return Err(RunnerError::Storage(error));
        }
        Err(error) => return Ok(Some(batch_failure(ctx, st, &error).await)),
    };
    if results.len() != metas.len() {
        tracing::error!(
            account_id = %record.account_id,
            operation_id = %record.operation_id,
            rows = metas.len(),
            results = results.len(),
            "bulk batch answered a result count different from its row count"
        );
        let error = StorageError::Internal("bulk batch result count mismatch".to_owned());
        return Ok(Some(batch_failure(ctx, st, &error).await));
    }

    let has_indexes = !st.key_info.global_secondary_indexes.is_empty()
        || !st.key_info.local_secondary_indexes.is_empty()
        || !st.key_info.vector_indexes.is_empty();
    let bucket = &config.input_bucket;
    let record_cap = deps.config.max_report_records;
    let byte_cap = deps.config.max_report_bytes;
    for ((file_index, row_index, item_bytes, image), result) in metas.into_iter().zip(results) {
        let path = &record.input_files[file_index].path;
        match result {
            BatchRowResult::Written { old } => {
                st.written = st.written.saturating_add(1);
                // Table charge: the larger of the old and new item sizes,
                // each floored to one kilobyte, the rule PutItem's consumed
                // capacity uses (`put_item.rs`: `new.max(old)` then the
                // floor; `add_base` applies the floor and the larger
                // distributes over it). The old image is the stored item the
                // write replaced: `reader.rs` asks storage for it on every
                // write, on a table with no index as well (measured: a
                // plain table's shrinking overwrite is charged the larger),
                // so only a true insert has no old image and is charged the
                // new item.
                let old_size = old.as_ref().map_or(0, item_size_bytes);
                st.usage.add_base(item_bytes.max(old_size));
                if has_indexes {
                    // The index charge from the old and new images: an
                    // unchanged projection under the same index key is
                    // charged nothing and reports no member, a changed
                    // projection the larger of old and new, a moved index key
                    // old plus new (measured on the service).
                    st.usage
                        .add_index_writes(old.as_ref(), image.as_ref(), &st.key_info);
                }
            }
            // A failed condition is billed the one kilobyte floor whatever
            // the item's size, counted as processed only (it was,
            // at staging), and recorded with both images.
            BatchRowResult::ConditionFailed { old } => {
                st.usage.add_condition_failed();
                let submitted = image.unwrap_or_default();
                st.push_record(
                    &ReportSink {
                        max_records: record_cap,
                        max_bytes: byte_cap,
                        requested: &config.output_types,
                    },
                    ReportKind::ConditionCheckFailure,
                    file_index,
                    row_index,
                    report::condition_record(
                        bucket,
                        path,
                        Some(to_i64(row_index)),
                        &submitted,
                        old.as_ref(),
                    ),
                );
            }
        }
    }
    // A condition record of this batch may be the first of its kind, whose
    // report object is checked now, after the rows before it were written.
    Ok(check_output_config_at_first_record(ctx, st).await)
}

/// Whether the record names another lease owner, or is gone or terminal,
/// after a batch failed with a transient fault: the batch was refused
/// because another claim took the operation over, and this runner stops
/// as at a lost lease. A record that cannot be read answers false, so the
/// fault is handled as a transient one.
async fn lease_passed_on(ctx: &RunContext<'_>) -> bool {
    let RunContext { deps, record, .. } = *ctx;
    match deps
        .storage
        .get_bulk_operation(&record.account_id, &record.operation_id)
        .await
    {
        Ok(Some(current)) => current.lease_owner.as_deref() != Some(deps.owner.as_str()),
        Ok(None) => true,
        Err(_) => false,
    }
}

/// Step 8: a batch that did not commit. The batch rolled back, but earlier
/// write batches of the same checkpoint window committed when
/// `bulk_put_write_batch_rows` is below `bulk_put_batch_size`: their rows
/// are in the table and in the ledger, while the window's counters, usage
/// and records live only in this state. The state reverts to the last
/// checkpoint and the operation ends with the deleted-table failure or the
/// internal one, so its counters are the checkpoint's, a floor on the rows
/// written, as a cancelled operation's are; the detail goes to the log
/// either way. A crash before `finish` then resumes at the checkpoint and
/// re-reads the window, finding the rows earlier batches of it applied in
/// the ledger.
async fn batch_failure(ctx: &RunContext<'_>, st: &mut RunState, error: &StorageError) -> Flow {
    let RunContext { deps, record, .. } = *ctx;
    tracing::error!(
        account_id = %record.account_id,
        operation_id = %record.operation_id,
        table_name = %record.table_name,
        error = %error,
        "bulk batch write failed"
    );
    revert_to_last_checkpoint(ctx, st).await;
    st.failure = Some(match table_binding_holds(deps, record).await {
        Ok(false) => Failure::table_not_found(),
        Ok(true) => Failure::internal(),
        Err(check_error) => {
            tracing::error!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                error = %check_error,
                "table binding check failed after a batch write error"
            );
            Failure::internal()
        }
    });
    Flow::Finish
}

/// Whether a storage error from a write batch is a transient fault a resume
/// is expected to clear, so the operation is left non-terminal rather than
/// ended `FAILED`. The class is the storage layer's
/// [`StorageError::is_transient_write_fault`], shared so the backends' own
/// tests drive the classifier the runner uses with real database errors.
pub(super) fn write_batch_fault_resumes(error: &StorageError) -> bool {
    error.is_transient_write_fault()
}

/// Revert the in-memory run state to the last checkpoint. The persisted
/// record holds exactly that checkpoint's counters, records, and usage,
/// so restoring from it is the revert; `written`, the observed cancel,
/// the output check, a held location fault and the tags accepted at opens
/// are this process's and carry over. A restore failure keeps the in-memory state, with a log
/// line; the caller's terminal condition still applies.
pub(super) async fn revert_to_last_checkpoint(ctx: &RunContext<'_>, st: &mut RunState) {
    let RunContext { deps, record, .. } = *ctx;
    match deps
        .storage
        .get_bulk_operation(&record.account_id, &record.operation_id)
        .await
    {
        Ok(Some(current)) => match RunState::restore(&current) {
            Ok(mut fresh) => {
                fresh.written = st.written;
                fresh.cancelled = st.cancelled;
                fresh.output_checked = st.output_checked;
                fresh.skip_render = st.skip_render;
                fresh.held_location = st.held_location.take();
                fresh.lease_deadline = st.lease_deadline;
                fresh.opened_etags = std::mem::take(&mut st.opened_etags);
                fresh.key_info = std::mem::take(&mut st.key_info);
                fresh.settled = st.settled;
                fresh.recorded = std::mem::take(&mut st.recorded);
                fresh.recorded_next = st.recorded_next;
                *st = fresh;
            }
            Err(error) => {
                tracing::error!(
                    account_id = %record.account_id,
                    operation_id = %record.operation_id,
                    error = %error,
                    "run state could not be reverted to the last checkpoint"
                );
            }
        },
        Ok(None) | Err(_) => {
            tracing::error!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                "operation record could not be re-read for the checkpoint revert"
            );
        }
    }
}

/// Step 6: the key metadata, the caller's `s3:GetObject` on the file being
/// read, then the staged rows, then the checkpoint, then the cancel flag.
/// The metadata read comes first so the rows staged since the last boundary
/// are written against the table as it is now (a stream enabled or an index
/// added since then applies to them, as it would to a `PutItem` issued
/// now); it also verifies the table binding,
/// so a deleted or replaced table is noticed here. The flush comes before
/// the checkpoint, so no caller can persist a cursor past a row whose
/// write did not commit; a flush that fails is the terminal flow.
pub(super) async fn boundary(ctx: &RunContext<'_>, st: &mut RunState) -> Result<Flow, RunnerError> {
    if let Some(flow) = refresh_key_info(ctx, st).await {
        return Ok(flow);
    }
    if let Some(flow) = check_get_at_boundary(ctx, st).await {
        return Ok(flow);
    }
    if let Some(flow) = flush_staged(ctx, st).await? {
        return Ok(flow);
    }
    let RunContext { deps, record, .. } = *ctx;
    st.rows_since_checkpoint = 0;
    let progress = st.progress(&record.table_arn);
    let cursor = (progress.cursor_file_index, progress.cursor_row_index);
    // A server shutdown checkpoints with a lease that has already run out,
    // so the next claim takes the operation without waiting for it.
    let shutting_down = deps.shutdown.is_cancelled();
    let lease = if shutting_down {
        Duration::ZERO
    } else {
        deps.config.lease
    };
    // Taken before the call: the store grants the lease from its own
    // clock when the statement runs, so the local deadline must not credit
    // the time the round trip itself took.
    let renewed_at = Instant::now();
    match deps
        .storage
        .checkpoint(
            &record.account_id,
            &record.operation_id,
            &deps.owner,
            lease,
            progress,
        )
        .await
    {
        Err(error) => return Err(RunnerError::Storage(error)),
        Ok(LeaseState::Lost) => {
            tracing::info!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                "lease lost at a checkpoint; the runner stops"
            );
            return Ok(Flow::Stop);
        }
        Ok(LeaseState::Held) => {
            st.lease_deadline = Some(renewed_at + lease);
            st.settled = cursor;
        }
    }
    if shutting_down {
        tracing::info!(
            account_id = %record.account_id,
            operation_id = %record.operation_id,
            cursor_file_index = cursor.0,
            cursor_row_index = cursor.1,
            "server shutting down: the bulk operation checkpointed and released its lease"
        );
        return Ok(Flow::Stop);
    }
    match deps
        .storage
        .get_bulk_operation_flags(&record.account_id, &record.operation_id)
        .await
    {
        Err(error) => {
            tracing::error!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                error = %error,
                "operation record could not be read at a boundary"
            );
            st.failure = Some(Failure::internal());
            Ok(Flow::Finish)
        }
        Ok(None) => {
            tracing::warn!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                "operation record is gone; the runner stops"
            );
            Ok(Flow::Stop)
        }
        Ok(Some(current)) => {
            if current.cancel_requested {
                st.cancelled = true;
                return Ok(Flow::Finish);
            }
            Ok(Flow::NextRow)
        }
    }
}

/// Re-read the table's key metadata and verify the binding. `Some(flow)`
/// ends the run: the table is gone or replaced under its name, or the
/// catalog could not be read. A table found `UPDATING` keeps the current
/// snapshot (the data plane refuses to describe it until the update
/// commits) provided its binding still holds; the next boundary reads
/// again.
pub(super) async fn refresh_key_info(ctx: &RunContext<'_>, st: &mut RunState) -> Option<Flow> {
    let RunContext { deps, record, .. } = *ctx;
    match deps
        .storage
        .table_key_info(&record.account_id, &record.table_name)
        .await
    {
        Ok(info) if info.table_id == record.table_id => {
            st.key_info = info;
            None
        }
        Ok(_) | Err(StorageError::TableNotFound(_)) => {
            st.failure = Some(Failure::table_not_found());
            Some(Flow::Finish)
        }
        Err(StorageError::TableNotActive(_)) => match table_binding_holds(deps, record).await {
            Ok(true) => None,
            Ok(false) => {
                st.failure = Some(Failure::table_not_found());
                Some(Flow::Finish)
            }
            Err(error) => {
                tracing::error!(
                    account_id = %record.account_id,
                    operation_id = %record.operation_id,
                    error = %error,
                    "table binding check failed at a boundary"
                );
                st.failure = Some(Failure::internal());
                Some(Flow::Finish)
            }
        },
        Err(error) => {
            tracing::error!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                error = %error,
                "table key info read failed at a boundary"
            );
            st.failure = Some(Failure::internal());
            Some(Flow::Finish)
        }
    }
}
