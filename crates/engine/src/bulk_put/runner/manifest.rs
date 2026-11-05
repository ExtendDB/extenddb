// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Rendering the manifest, `manifest-files.json`: one line per enumerated
//! input file with its processing status, last counted row and entity tag.

use super::*;

/// Render the manifest: one line per enumerated input file, in enumeration
/// order (measured on the service). A file before the cursor was read to its
/// end or abandoned; a file at the cursor was cut short by a cancel or a
/// failure; a file after the cursor was never reached. The last counted
/// row of a finished file is its row count minus one, or the abandon row
/// minus one; of the cursor file, the cursor row minus one. A finished
/// file's row count and entity tag, and an abandoned file's abandon, come
/// from the run state, filled by this incarnation or restored from the
/// figures an earlier incarnation's checkpoint stored on the record; a
/// finished file is read again only when neither is there (an abandoned or
/// unreached file is hashed, never counted).
pub(super) async fn render_manifest(
    deps: &RunnerDeps,
    record: &BulkOperationRecord,
    config: &OperationConfig,
    output_bucket: &str,
    st: &RunState,
) -> Result<(), Failure> {
    let account = &record.account_id;
    let operation = &record.operation_id;
    let mut lines = Vec::with_capacity(record.input_files.len());
    let input_at = config.input_at(account);
    for (index, file) in record.input_files.iter().enumerate() {
        let (status, last) = if st.unavailable_file == Some(index) {
            // The object was gone or unreadable when the runner opened or
            // read it: no processed range, whatever rows it had written.
            // Refused at its open before any of its rows was counted, it
            // is `NOT_PROCESSED`; refused after, `NOT_AVAILABLE` (both
            // measured on the service).
            let refused_unread =
                index == st.cursor_file && st.cursor_row == 0 && !st.current_file_opened;
            let status = if refused_unread {
                report::FileProcessingStatus::NotProcessed
            } else {
                report::FileProcessingStatus::NotAvailable
            };
            (status, None)
        } else if let Some(abandon) = st.abandoned.get(&index) {
            (
                report::FileProcessingStatus::PartiallyProcessed,
                abandon.row.checked_sub(1),
            )
        } else if index < st.cursor_file {
            // Finished: every line was consumed. The last counted row is the
            // row count minus one; a file with no rows has none. The count
            // is in the run state for a file this or an earlier incarnation
            // finished; a file with no recorded count is counted here.
            let count = match st.file_row_counts.get(&index) {
                Some(&count) => count,
                None => deps
                    .source
                    .line_count(&input_at, file, config.compression)
                    .await
                    .map_err(|error| {
                        tracing::error!(
                            account_id = %account,
                            operation_id = %operation,
                            path = %file.path,
                            error = ?error,
                            "input file could not be counted for the manifest"
                        );
                        source_failure(&error)
                    })?,
            };
            (
                report::FileProcessingStatus::FullyProcessed,
                count.checked_sub(1),
            )
        } else if index == st.cursor_file && (st.cursor_row > 0 || st.current_file_opened) {
            // The file the run was inside when it ended. ExtendDB
            // checkpoints at row boundaries and knows the range, so it is
            // reported with the range it processed. The hosted service
            // loses the range of a reader that was inside a read when a
            // cancel landed and reports `NOT_AVAILABLE` with none;
            // the range is kept here, a stated divergence.
            (
                report::FileProcessingStatus::PartiallyProcessed,
                st.cursor_row.checked_sub(1),
            )
        } else {
            // Never opened: the operation ended before it reached this
            // file (measured on the service).
            (report::FileProcessingStatus::NotProcessed, None)
        };
        let overwritten = st
            .abandoned
            .get(&index)
            .is_some_and(|a| a.kind == AbandonKind::Overwritten);
        // A file read to its end has the tag its read reported. A file
        // listed without a tag that this incarnation opened on a store whose
        // tags the service assigns has the tag of the first answer the
        // store accepted for it, the object the counted rows came from.
        let read_tag = st
            .file_etags
            .get(&index)
            .or_else(|| st.opened_etags.get(&index));
        let etag = match read_tag {
            Some(etag) => etag.clone(),
            // The object now at the key replaced the one the operation
            // listed, so the line names the listed object by the tag the
            // listing gave (measured on the service), and by none when the
            // listing gave none: asking the store would name the
            // replacement.
            None if overwritten => file.etag.clone().unwrap_or_default(),
            // A store whose tags the service assigns has no tag for a file
            // listed without one that the run never accepted an answer
            // for, and the object now at the key may be a replacement.
            None if file.etag.is_none() && deps.source.service_assigns_etags() => String::new(),
            None => match deps.source.etag(&input_at, file).await {
                Ok(etag) => etag,
                // An object that is gone or unreadable cannot be hashed,
                // and the measured manifest still carries a record for it,
                // so the record is written with no tag rather than failing
                // the render and replacing the operation's failure code.
                Err(error) if st.unavailable_file == Some(index) => {
                    tracing::info!(
                        account_id = %account,
                        operation_id = %operation,
                        path = %file.path,
                        error = ?error,
                        "unavailable input file could not be hashed for the manifest"
                    );
                    String::new()
                }
                Err(error) => {
                    tracing::error!(
                        account_id = %account,
                        operation_id = %operation,
                        path = %file.path,
                        error = ?error,
                        "input file could not be hashed for the manifest"
                    );
                    return Err(source_failure(&error));
                }
            },
        };
        lines.push(report::manifest_line(
            &config.input_bucket,
            &file.path,
            status,
            last,
            &etag,
        ));
    }
    deps.sink
        .write_manifest(
            &config.output_at(account, output_bucket),
            &config.output_prefix,
            operation,
            &deps.owner,
            &config.output_sse,
            lines,
        )
        .await
        .map(|_| ())
        .map_err(|error| match error {
            // A fault the store renders on the write itself, after the
            // location check passed (the bucket removed or its policy
            // changed in between), keeps the measured manifest form.
            SinkError::Store(fault) => Failure::manifest_unavailable(Failure::from_store(&fault)),
            other => render_sink_failure(account, operation, &other),
        })
}
