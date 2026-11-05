// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Reading one input file: opening it through the source, reading its
//! lines under the line and item-count caps, and turning each read row
//! into a staged write or a fault record.

use super::*;

/// Read one input file from `skip_rows` to its end, or to an abandon or a
/// terminal condition. Ends with the end-of-file bookkeeping and boundary
/// checkpoint on every path that finishes the file.
///
/// Whatever path the read loop leaves by, no row stays staged: a flow that
/// continues or finishes the operation writes the staged rows first (a
/// failed flush becomes the terminal flow), and a `Stop` (the lease is
/// lost or the record is gone) discards them, because another owner may
/// already be driving the operation from the last checkpoint and these
/// rows are behind it.
pub(super) async fn read_file(
    ctx: &RunContext<'_>,
    st: &mut RunState,
    file: &InputFile,
    file_index: usize,
    file_size: u64,
    skip_rows: u64,
) -> Result<Flow, RunnerError> {
    let flow = read_file_lines(ctx, st, file, file_index, file_size, skip_rows).await?;
    if st.staged.is_empty() {
        return Ok(flow);
    }
    if matches!(flow, Flow::Stop) {
        st.staged.clear();
        return Ok(flow);
    }
    match flush_staged(ctx, st).await? {
        Some(failed) => Ok(failed),
        None => Ok(flow),
    }
}

/// The read loop of `read_file`. Staged rows are flushed before every
/// boundary, before the output-configuration check (a condition record
/// can be the first report record), and at the end of the file, so a
/// checkpoint never persists a cursor past an uncommitted write.
#[allow(clippy::too_many_lines)] // one file's read loop, kept in one place
async fn read_file_lines(
    ctx: &RunContext<'_>,
    st: &mut RunState,
    file: &InputFile,
    file_index: usize,
    file_size: u64,
    skip_rows: u64,
) -> Result<Flow, RunnerError> {
    let RunContext {
        deps,
        record,
        config,
        ..
    } = *ctx;
    if let Some(flow) = check_get_at_open(ctx, st, file, file_index).await {
        return Ok(flow);
    }
    let input = config.input_at(&record.account_id);
    let opened =
        match keeping_lease(ctx, st, deps.source.open(&input, file, config.compression)).await? {
            Waited::Answer(opened) => opened,
            Waited::Flow(flow) => return Ok(flow),
        };
    let mut reader = match opened {
        Ok(reader) => reader,
        // Not a compressed object under GZIP: the file counts one processed
        // and failed row with its own record and the run moves to the next
        // file; the operation ends FAILED (measured on the service). The
        // file's bytes are not counted as processed input.
        Err(SourceError::NotCompressed) => {
            if let Some(flow) = flush_staged(ctx, st).await? {
                return Ok(flow);
            }
            if st.consume_line(deps.config.max_item_count) {
                return Ok(Flow::Finish);
            }
            record_abandon(
                deps,
                config,
                st,
                file,
                file_index,
                Abandon {
                    row: 0,
                    kind: AbandonKind::NotCompressed,
                },
                rows::NOT_COMPRESSED_MESSAGE,
            );
            if let Some(flow) = check_output_config_at_first_record(ctx, st).await {
                return Ok(flow);
            }
            return end_of_file(ctx, st, file_index, 0, None).await;
        }
        // A zstd frame whose header does not decode: the file counts one
        // processed and failed row with the decompression message, its
        // bytes count as processed input, and the run moves to the next
        // file (measured on the service; see `SourceError::DecodeFailed`).
        Err(SourceError::DecodeFailed) => {
            if let Some(flow) = flush_staged(ctx, st).await? {
                return Ok(flow);
            }
            if st.consume_line(deps.config.max_item_count) {
                return Ok(Flow::Finish);
            }
            record_abandon(
                deps,
                config,
                st,
                file,
                file_index,
                Abandon {
                    row: 0,
                    kind: AbandonKind::Content,
                },
                rows::NOT_COMPRESSED_MESSAGE,
            );
            if let Some(flow) = check_output_config_at_first_record(ctx, st).await {
                return Ok(flow);
            }
            return end_of_file(ctx, st, file_index, file_size, None).await;
        }
        // The object at the key is no longer the one the operation listed
        // (measured on the service; see `SourceError::Overwritten`). Found at
        // a resumed open, the rows an earlier claim counted stand, and so do
        // their bytes.
        Err(SourceError::Overwritten) => {
            let counted = if skip_rows > 0 {
                st.bytes_floor.saturating_sub(st.completed_bytes)
            } else {
                0
            };
            return record_overwritten(ctx, st, file, file_index, skip_rows, counted, file_size)
                .await;
        }
        Err(error) => {
            tracing::error!(
                account_id = %record.account_id,
                operation_id = %record.operation_id,
                path = %file.path,
                error = ?error,
                "bulk input file failed to open"
            );
            // An object that is gone or unreadable at the open ends the
            // operation with the measured S3 code. The manifest reports it
            // `NOT_PROCESSED` when none of its rows was counted, and
            // `NOT_AVAILABLE` when an earlier claim had counted rows of it.
            if unavailable_at_open(&error, skip_rows) {
                st.unavailable_file = Some(file_index);
            }
            st.failure = Some(source_failure(&error));
            return Ok(Flow::Finish);
        }
    };
    st.current_file_opened = true;
    // A file listed without a tag is named in the manifest by the tag of
    // the first answer the store accepted for it, never by the object at
    // the key when the manifest is written.
    if file.etag.is_none()
        && let Some(etag) = reader.opened_etag()
    {
        st.opened_etags.entry(file_index).or_insert(etag);
    }

    let mut row_index: u64 = 0;
    if skip_rows > 0 {
        match skip_covered_region(
            ctx,
            st,
            reader.as_mut(),
            file,
            file_index,
            file_size,
            skip_rows,
        )
        .await?
        {
            Some(flow) => return Ok(flow),
            None => row_index = skip_rows,
        }
    }

    loop {
        let next =
            match keeping_lease(ctx, st, reader.next_line(deps.config.max_line_bytes)).await? {
                Waited::Answer(next) => next,
                Waited::Flow(flow) => return Ok(flow),
            };
        match next {
            Ok(None) => {
                let etag = reader.etag();
                return end_of_file(ctx, st, file_index, file_size, etag).await;
            }
            Err(
                fault @ (SourceError::LineTooLong
                | SourceError::InvalidUtf8
                | SourceError::Corrupt
                | SourceError::DecodeFailed),
            ) => {
                if let Some(flow) = flush_staged(ctx, st).await? {
                    return Ok(flow);
                }
                if st.consume_line(deps.config.max_item_count) {
                    return Ok(Flow::Finish);
                }
                // A line that is not UTF-8 carries its own measured message;
                // the per-line byte cap is ExtendDB's own and keeps the
                // shared abandon text.
                let message = match fault {
                    SourceError::InvalidUtf8 => rows::INVALID_UTF8_MESSAGE,
                    SourceError::Corrupt => rows::CORRUPT_STREAM_MESSAGE,
                    SourceError::DecodeFailed => rows::NOT_COMPRESSED_MESSAGE,
                    _ => rows::PARSE_ABANDON_MESSAGE,
                };
                record_abandon(
                    deps,
                    config,
                    st,
                    file,
                    file_index,
                    Abandon {
                        row: row_index,
                        kind: AbandonKind::Content,
                    },
                    message,
                );
                if let Some(flow) = check_output_config_at_first_record(ctx, st).await {
                    return Ok(flow);
                }
                // The faulting line was not delivered, so the offset of
                // the last delivered line is the furthest point a line
                // fault can be attributed to. A stream fault is found
                // where the decoder stopped reading: a frame cut short is
                // read to the end of the object before it fails (measured
                // on the service: a 12,943-byte zstd frame cut at 90
                // percent counts 12,943 and ends COMPLETED_WITH_ERRORS),
                // so the bytes the chain pulled are its offset.
                let fault_offset =
                    if matches!(fault, SourceError::Corrupt | SourceError::DecodeFailed) {
                        reader.bytes_consumed()
                    } else {
                        reader.last_line_end()
                    };
                let bytes = abandon_file_bytes(st, config.compression, fault_offset, file_size);
                return end_of_file(ctx, st, file_index, bytes, None).await;
            }
            // The object was overwritten while it was read: the rows it
            // delivered stand, and so do their bytes.
            Err(SourceError::Overwritten) => {
                let delivered = reader.last_line_end();
                return record_overwritten(
                    ctx, st, file, file_index, row_index, delivered, file_size,
                )
                .await;
            }
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
                return Ok(Flow::Finish);
            }
            Ok(Some(line)) => {
                let this_row = row_index;
                st.current_bytes = reader.bytes_consumed().min(file_size);
                // The cap is checked before the cursor moves: a line the run
                // fails on is not consumed, so a resume after a crash between
                // the final checkpoint and finish reads it again instead of
                // skipping past a line that was never applied.
                if st.consume_line(deps.config.max_item_count) {
                    return Ok(Flow::Finish);
                }
                row_index += 1;
                st.cursor_row = row_index;
                let step = process_row(ctx, st, file, file_index, this_row, &line).await?;
                // The staged rows are written when the write batch is full,
                // when the output-configuration check is pending (its first
                // report record was just produced), and before any flow
                // that leaves this loop; the boundary flushes on its own
                // before it checkpoints. So rows read before the first
                // report record are written before the operation can fail
                // on the output configuration, and the persisted cursor
                // reflects committed writes.
                let flush_now = !matches!(step, Flow::NextRow)
                    || st.staged.len() >= deps.config.write_batch_rows
                    || (!st.output_checked && st.kept_records > 0);
                if flush_now && let Some(flow) = flush_staged(ctx, st).await? {
                    return Ok(flow);
                }
                // The output-configuration check runs before the boundary
                // that would checkpoint the record's batch, and only on
                // the flows that continue: a Finish already carries its
                // own terminal condition.
                if matches!(step, Flow::NextRow | Flow::FileDone)
                    && let Some(flow) = check_output_config_at_first_record(ctx, st).await
                {
                    return Ok(flow);
                }
                match step {
                    Flow::NextRow => {}
                    Flow::FileDone => {
                        // The parse-level abandon class: the faulting line
                        // was delivered, so its end offset decides whether
                        // bytes of the file lie beyond the fault's read
                        // (measured on the service).
                        let bytes = abandon_file_bytes(
                            st,
                            config.compression,
                            reader.last_line_end(),
                            file_size,
                        );
                        return end_of_file(ctx, st, file_index, bytes, None).await;
                    }
                    Flow::Stop => return Ok(Flow::Stop),
                    Flow::Finish => return Ok(Flow::Finish),
                }
                // A server shutdown stops the run at this row: the boundary
                // writes the staged rows and checkpoints, then stops.
                if st.checkpoint_due(deps.config.batch_size) || deps.shutdown.is_cancelled() {
                    match boundary(ctx, st).await? {
                        Flow::NextRow => {}
                        other => return Ok(other),
                    }
                }
            }
        }
    }
}

/// Count and record an input object found overwritten after the operation
/// listed it, at `row_index`, the first row not counted, and end the file.
/// The record is the abandon record, with no `itemIndex`, carrying the
/// overwritten message. The file contributes `counted` bytes, the bytes of
/// the rows it delivered (none when the object was replaced before its
/// first byte, as measured on the service). The checkpoint that passes the
/// file stores the abandon with its kind, so a restart still ends the
/// operation `FAILED`.
pub(super) async fn record_overwritten(
    ctx: &RunContext<'_>,
    st: &mut RunState,
    file: &InputFile,
    file_index: usize,
    row_index: u64,
    counted: u64,
    file_size: u64,
) -> Result<Flow, RunnerError> {
    let RunContext { deps, config, .. } = *ctx;
    if let Some(flow) = flush_staged(ctx, st).await? {
        return Ok(flow);
    }
    if st.consume_line(deps.config.max_item_count) {
        return Ok(Flow::Finish);
    }
    record_abandon(
        deps,
        config,
        st,
        file,
        file_index,
        Abandon {
            row: row_index,
            kind: AbandonKind::Overwritten,
        },
        rows::OVERWRITTEN_MESSAGE,
    );
    if let Some(flow) = check_output_config_at_first_record(ctx, st).await {
        return Ok(flow);
    }
    let bytes = counted.min(file_size);
    st.bytes_floor = st.bytes_floor.min(st.completed_bytes.saturating_add(bytes));
    end_of_file(ctx, st, file_index, bytes, None).await
}

/// Whether a failed open leaves the file `NOT_AVAILABLE` in the manifest.
/// A store fault at an open that resumes the file after `skip_rows` of its
/// rows were counted is after the file's first byte, whatever the store
/// says of the GET that met it: the S3 store renders the first range of
/// every open as before the first byte. The filesystem store and the
/// service report such a file `NOT_AVAILABLE` with no range.
fn unavailable_at_open(error: &SourceError, skip_rows: u64) -> bool {
    match error {
        SourceError::Store(_) if skip_rows > 0 => true,
        other => is_unavailable_object(other),
    }
}

/// Step 5 for one physical line: parse, validate, and apply it, with the
/// row-outcome ledger consulted before any condition is evaluated.
#[allow(clippy::too_many_lines)] // one row's step table, kept in one place
async fn process_row(
    ctx: &RunContext<'_>,
    st: &mut RunState,
    file: &InputFile,
    file_index: usize,
    row_index: u64,
    line: &str,
) -> Result<Flow, RunnerError> {
    let RunContext {
        deps,
        record,
        config,
        ..
    } = *ctx;
    let bucket = &config.input_bucket;
    let path = &file.path;
    let item_index = to_i64(row_index);
    let record_cap = deps.config.max_report_records;
    let byte_cap = deps.config.max_report_bytes;

    let row = match rows::parse_row(line, deps.config.max_line_bytes) {
        // A blank line consumes an index and touches no counter. The
        // caller already counted it toward the batch boundary and the
        // line cap, so the lease renews and a cancel is observed
        // whatever the input contains.
        Ok(None) => {
            return Ok(Flow::NextRow);
        }
        Err(RowError::Parse(message)) => {
            record_abandon(
                deps,
                config,
                st,
                file,
                file_index,
                Abandon {
                    row: row_index,
                    kind: AbandonKind::Content,
                },
                &message,
            );
            return Ok(Flow::FileDone);
        }
        Err(RowError::Item(message)) => {
            st.processed += 1;
            st.failed += 1;
            st.push_record(
                &ReportSink {
                    max_records: record_cap,
                    max_bytes: byte_cap,
                    requested: &config.output_types,
                },
                ReportKind::Error,
                file_index,
                row_index,
                report::error_record(bucket, path, Some(item_index), &message, None),
            );
            return Ok(Flow::NextRow);
        }
        Ok(Some(row)) => row,
    };

    st.processed += 1;

    // Admission through the shared write core, condition fields first, the
    // order `handle_put_item` applies. A failure is an item-level record;
    // only the missing-key check carries the submitted image.
    let condition = match admit_row(&row, &st.key_info, &deps.limits) {
        Admission::Fault { message, kind } => {
            st.failed += 1;
            let new_image = kind.and_then(|kind| {
                // The item holds one copy of each set member; the limit
                // the row's own checks use counts the copies removed, so
                // the image rule judges the item as submitted.
                let limit = row.item_limits(&deps.limits).max_item_size_bytes;
                item_fault_new_image(kind, &row.item, limit)
            });
            let message = item_fault_message(kind, message);
            st.push_record(
                &ReportSink {
                    max_records: record_cap,
                    max_bytes: byte_cap,
                    requested: &config.output_types,
                },
                ReportKind::Error,
                file_index,
                row_index,
                report::error_record(bucket, path, Some(item_index), &message, new_image),
            );
            return Ok(Flow::NextRow);
        }
        Admission::Write(condition) => condition,
    };

    let has_indexes = !st.key_info.global_secondary_indexes.is_empty()
        || !st.key_info.local_secondary_indexes.is_empty()
        || !st.key_info.vector_indexes.is_empty();
    let item_bytes = item_size_bytes(&row.item);
    // The write is staged, not issued: rows go to storage in one batch at
    // the flush, and this row's counters, usage, and any condition record
    // are applied from the batch's results. Every row carries its marker,
    // so the backend records the row's outcome in the ledger inside the
    // batch's transaction and a resumed run does not apply it again. The
    // submitted image is kept when a condition record or an index charge
    // may need it; the ledger keeps the replaced item for the index charge,
    // and the item a failed condition was evaluated against in every case.
    let image = (condition.is_some() || has_indexes).then(|| row.item.clone());
    let marker = Some(BulkRowMarker {
        account_id: record.account_id.clone(),
        operation_id: record.operation_id.clone(),
        file_index: to_i64(u64::try_from(file_index).unwrap_or(u64::MAX)),
        row_index: item_index,
        owner: deps.owner.clone(),
        keep_old: has_indexes,
        settled: st.settled,
    });
    st.staged.push(Staged {
        row: BatchRow {
            item: row.item,
            condition,
            marker,
            // The old image is requested on every write: the table charge
            // on an overwrite is the larger of the old and new item sizes
            // (measured on the service), on a table with no index as well,
            // so the runner needs the replaced item's size for every row.
            // The storage layer already reads the old image on the batch
            // write path for PostgreSQL; on SQLite it reads it only when
            // asked, so this adds one read per overwritten row there (see
            // the report).
            return_old: true,
        },
        file_index,
        row_index,
        item_bytes,
        image,
    });
    Ok(Flow::NextRow)
}
