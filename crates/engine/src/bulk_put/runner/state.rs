// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Run state and terminal decisions: the mutable [`RunState`] restored
//! from the operation record, its configuration and staged-row types, and
//! the pure helpers that map state to a terminal status and a record kind.

use super::*;

/// The parsed input and output configuration of one operation.
pub(super) struct OperationConfig {
    pub(super) input_bucket: String,
    pub(super) input_bucket_owner: Option<String>,
    /// The request's `S3KeyPrefix` for the input, as given: the
    /// object-count quota message interpolates it verbatim, trailing
    /// slash included (measured on the service).
    pub(super) input_prefix: String,
    pub(super) compression: InputCodec,
    pub(super) output_bucket: Option<String>,
    pub(super) output_bucket_owner: Option<String>,
    pub(super) output_prefix: String,
    pub(super) output_types: Vec<OutputType>,
    pub(super) output_sse: OutputSse,
    /// The caller recorded at Start, whose S3 permissions the runner checks.
    pub(super) caller: Option<BulkPrincipal>,
}

impl OperationConfig {
    /// Parse the stored configuration values. The start handler validated
    /// and wrote them, so a failure here is an internal inconsistency, not
    /// client input.
    pub(super) fn parse(record: &BulkOperationRecord) -> Result<Self, String> {
        let input: InputS3Configuration = serde_json::from_value(record.input_config.clone())
            .map_err(|e| format!("stored input configuration does not parse: {e}"))?;
        let output: OutputS3Configuration = serde_json::from_value(record.output_config.clone())
            .map_err(|e| format!("stored output configuration does not parse: {e}"))?;
        let compression = match input.input_compression_type {
            Some(InputCompressionType::Gzip) => InputCodec::Gzip,
            Some(InputCompressionType::Zstd) => InputCodec::Zstd,
            None | Some(InputCompressionType::None) => InputCodec::None,
        };
        let output_sse = OutputSse::of(&output);
        Ok(Self {
            input_bucket: input.s3_bucket,
            input_bucket_owner: input.s3_bucket_owner,
            input_prefix: input.s3_key_prefix.unwrap_or_default(),
            compression,
            output_bucket: output.s3_bucket,
            output_bucket_owner: output.s3_bucket_owner,
            output_prefix: output.s3_key_prefix.unwrap_or_default(),
            output_types: output.output_type,
            output_sse,
            caller: None,
        })
    }

    /// The input bucket of the operation in `account`, as the source sees
    /// it.
    pub(super) fn input_at<'a>(&'a self, account: &'a str) -> BucketRef<'a> {
        BucketRef::new(
            account,
            &self.input_bucket,
            self.input_bucket_owner.as_deref(),
        )
    }

    /// The report bucket `bucket` of the operation in `account`, as the
    /// sink sees it.
    pub(super) fn output_at<'a>(&'a self, account: &'a str, bucket: &'a str) -> BucketRef<'a> {
        BucketRef::new(account, bucket, self.output_bucket_owner.as_deref())
    }
}

/// Mutable state of one run, restored from the record and persisted at
/// every checkpoint.
/// One admitted row waiting in the write batch, with what applying its
/// result needs: the location for its report record, its size for usage
/// and pacing, and the submitted image when a condition record or an index
/// charge may need it (a plain row on a table without indexes keeps none).
pub(super) struct Staged {
    pub(super) row: BatchRow,
    pub(super) file_index: usize,
    pub(super) row_index: u64,
    pub(super) item_bytes: usize,
    pub(super) image: Option<Item>,
}

/// How a file was abandoned: the row the abandon was recorded at, where
/// the file's processed range ends, and why the rest was not read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Abandon {
    pub(super) row: u64,
    pub(super) kind: AbandonKind,
}

impl Abandon {
    /// Whether the abandon ends the operation `FAILED` with the validation
    /// pair once the other files are read: a file that was not a
    /// compressed object, and an object overwritten after its listing
    /// (both measured on the service). An abandon of the `Content` kind
    /// does so only when it cut the file short, which `cut_file_seen`
    /// records, and the file's record keeps as the bytes it contributed.
    pub(super) fn fails_operation(self) -> bool {
        matches!(
            self.kind,
            AbandonKind::NotCompressed | AbandonKind::Overwritten
        )
    }
}

#[derive(Default)]
pub(super) struct RunState {
    pub(super) processed: u64,
    pub(super) failed: u64,
    /// Whether the output configuration was checked at the operation's
    /// first report record. Restored true when the record already holds
    /// persisted report records: an earlier claim produced the first one
    /// and checked before checkpointing it.
    pub(super) output_checked: bool,
    /// Set on the output-configuration failure path: the accumulated
    /// records were never validated against the output configuration, so
    /// nothing is rendered.
    pub(super) skip_render: bool,
    /// The output location fault found at the first report record, held
    /// to the end of the input (`runner::held`).
    pub(super) held_location: Option<Failure>,
    /// Report kinds whose object 0 this claim checked for `s3:PutObject`.
    pub(super) put_checked: Vec<ReportKind>,
    /// Rows whose write committed in this process. Not persisted: it feeds
    /// the `BulkPutItemsWritten` metric, which measures the work this
    /// process did, so rows an earlier claim applied (found in the ledger)
    /// and condition failures (nothing written) are not counted.
    pub(super) written: u64,
    /// Enumerated bytes of every file already finished or abandoned.
    pub(super) completed_bytes: u64,
    /// Raw bytes consumed in the file being read, capped at its
    /// enumerated size.
    pub(super) current_bytes: u64,
    /// The persisted `ProcessedInputBytes`; a checkpoint never reports
    /// below it, so a run that fails before reopening the resumed file
    /// cannot regress the counter.
    pub(super) bytes_floor: u64,
    pub(super) usage: UsageAccum,
    /// Rows admitted for writing and not yet handed to storage. Flushed as
    /// one `put_items_batch` call; their counters, usage, and condition
    /// records are applied from the results, after the batch committed. A
    /// checkpoint never runs with rows staged, so the persisted cursor
    /// never points past a row whose write did not commit.
    pub(super) staged: Vec<Staged>,
    /// Records produced since the last checkpoint.
    pub(super) pending: Vec<ReportRecord>,
    /// Serialized bytes currently buffered in `pending`, reset when the
    /// buffer drains at a checkpoint. Bounds the in-memory report buffer
    /// independent of `batch_size`: a checkpoint is forced once this
    /// crosses `PENDING_FLUSH_BYTES`, so a large `batch_size` on
    /// failing-heavy input cannot grow the heap without bound.
    pub(super) pending_bytes: u64,
    /// Records persisted or pending for this operation, against the count
    /// cap. Restored from the operation record on claim.
    pub(super) kept_records: u64,
    /// Serialized bytes of those records, against the byte cap. Restored
    /// from the operation record on claim, like the count.
    pub(super) kept_bytes: u64,
    pub(super) dropped_records: u64,
    pub(super) rows_since_checkpoint: usize,
    /// Physical lines consumed by this runner, blank and skipped lines
    /// included, compared to `max_item_count` as lines are read. Not
    /// persisted: a resume re-counts the skip region of the file it
    /// reopens, and lines of files already finished are behind committed
    /// checkpoints, so the count bounds what one claim can read without
    /// ever failing a resume that a crash-free run would have completed.
    pub(super) lines_consumed: u64,
    /// Next row to read, as (file index, physical line index).
    pub(super) cursor_file: usize,
    pub(super) cursor_row: u64,
    pub(super) cancelled: bool,
    pub(super) failure: Option<Failure>,
    /// The instant this process's lease on the operation runs out, as the
    /// runner last learned it: set when the claim is handed to `run` and
    /// moved forward after every checkpoint the store answered `Held`.
    /// Checked before every write batch, so a process that stalled past
    /// its lease (paused, swapped, blocked on a lock) stops instead of
    /// committing rows a new owner has already passed. `None` only in unit
    /// tests that build a state without a claim.
    pub(super) lease_deadline: Option<Instant>,
    /// The table's key metadata as the runner last read it: key schema,
    /// attribute definitions, indexes and stream specification. Read once
    /// when the claim starts and again at every checkpoint boundary before
    /// the staged rows are written, so a stream enabled or an index added
    /// while the operation runs applies to every row flushed from the next
    /// boundary on, the way a single `PutItem` reads it per request. A
    /// boundary read that finds the table `UPDATING` keeps this snapshot
    /// and reads again at the next boundary.
    pub(super) key_info: TableKeyInfo,
    /// Files abandoned before their end, by file index, with the row and
    /// the kind of the abandon: the ones this incarnation abandoned, plus
    /// the ones an earlier incarnation's checkpoints recorded on the
    /// operation's `input_files`, restored with the counters.
    pub(super) abandoned: std::collections::BTreeMap<usize, Abandon>,
    /// Row counts of files read to their end, by file index: the ones this
    /// incarnation finished, plus the ones an earlier incarnation recorded
    /// on the operation's `input_files` at its own checkpoints, restored
    /// with the counters. A file the manifest finds here is not read again.
    pub(super) file_row_counts: std::collections::BTreeMap<usize, u64>,
    /// Entity tags of files read to their end through the hashing reader,
    /// by file index, from this incarnation or restored from the record.
    /// The manifest hashes a file that has no entry here.
    pub(super) file_etags: std::collections::BTreeMap<usize, String>,
    /// For files listed without an entity tag on a store whose tags the
    /// service assigns: the tag of the first answer this incarnation
    /// accepted at each file's open, by file index. Kept in memory only, so
    /// a file opened by an earlier incarnation and not reopened by this one
    /// has no entry.
    pub(super) opened_etags: std::collections::BTreeMap<usize, String>,
    /// Files finished since the last checkpoint, carried to the next one,
    /// which stores their figures beside the cursor that passes them.
    pub(super) pending_finished: Vec<FinishedFile>,
    /// Files abandoned since the last checkpoint, carried to the next one
    /// the same way.
    pub(super) pending_abandoned: Vec<AbandonedFile>,
    /// Whether an abandon left bytes of its file beyond the read that held
    /// the fault, so the file's processed bytes stop at the end of that
    /// read. The operation runs on and ends `FAILED` with the same
    /// validation pair as the not-compressed class (measured on the
    /// service).
    pub(super) cut_file_seen: bool,
    /// Whether the file at the cursor was opened by this incarnation. A
    /// file the operation never opened is `NOT_PROCESSED` in the manifest
    /// (measured on the service), which this separates from a file
    /// whose read started and stopped part way.
    pub(super) current_file_opened: bool,
    /// The file whose read ended abnormally: the object was gone or
    /// unreadable when the runner opened or read it. The manifest reports
    /// it with no processed range: `NOT_PROCESSED` when its open refused it
    /// before any of its rows was counted, `NOT_AVAILABLE` otherwise
    /// (measured on the service).
    pub(super) unavailable_file: Option<usize>,
    /// The cursor of the last checkpoint the catalog committed, as
    /// `(file_index, row_index)`; every write batch passes it to the ledger,
    /// which deletes the rows it covers.
    pub(super) settled: (i64, i64),
    /// Set when the resume bound ended the operation (`runner::resume`): a
    /// store unavailable past its retries then ends it with the internal
    /// failure instead of leaving it for another resume.
    pub(super) resumes_spent: bool,
    /// Rows the ledger holds as applied past the restored checkpoint, by
    /// `(file_index, row_index)`, read a page at a time: the first page when
    /// the claim takes the operation over, the next ones as the run's
    /// flushes reach them (`runner::resume`). A staged row found here is
    /// accounted for from its entry, removed, and not written again.
    pub(super) recorded: std::collections::BTreeMap<(usize, u64), RecordedRow>,
    /// Where the next page of recorded rows starts, `(file_index,
    /// row_index)`, while the last page read was full; `None` once every
    /// recorded row of the file has been read.
    pub(super) recorded_next: Option<(usize, u64)>,
}

impl RunState {
    /// Restore the run state from the record's persisted counters. A
    /// negative stored counter is corruption, not a shape to model, and
    /// refuses the claim the way the record readers in both backends
    /// refuse a negative report total: `StorageError::Internal` naming
    /// the field, with a log line.
    pub(super) fn restore(record: &BulkOperationRecord) -> Result<Self, StorageError> {
        let non_negative = |field: &'static str, value: i64| -> Result<u64, StorageError> {
            u64::try_from(value).map_err(|_| {
                tracing::error!(
                    account_id = %record.account_id,
                    operation_id = %record.operation_id,
                    field,
                    value,
                    "bulk operation record counter is negative"
                );
                StorageError::Internal(format!("bulk operation {field} is negative"))
            })
        };
        let cursor_file = usize::try_from(non_negative(
            "cursor_file_index",
            record.cursor_file_index,
        )?)
        .map_err(|_| {
            StorageError::Internal("bulk operation cursor_file_index does not fit usize".to_owned())
        })?;
        let mut completed_bytes = 0u64;
        let mut cut_file_seen = false;
        // Whether a file before the cursor was abandoned by a build that
        // did not record the bytes an abandoned file contributed.
        let mut unrecorded_abandon = false;
        let mut file_row_counts = std::collections::BTreeMap::new();
        let mut file_etags = std::collections::BTreeMap::new();
        let mut abandoned = std::collections::BTreeMap::new();
        for (index, file) in record.input_files.iter().enumerate().take(cursor_file) {
            let size = non_negative("input file size", file.size)?;
            // Figures an earlier incarnation's checkpoint stored for a file
            // it finished or abandoned. A negative count or row is
            // corruption like the counters.
            if let Some(rows) = file.rows {
                file_row_counts.insert(index, non_negative("input file rows", rows)?);
            }
            if let Some(etag) = &file.etag {
                file_etags.insert(index, etag.clone());
            }
            // A finished file contributed its enumerated size; an abandoned
            // one the bytes its record keeps: the end of the read that held
            // the fault when the abandon cut it short, which ends the
            // operation with the items-failed-validation pair (measured on
            // the service), or what an overwritten or refused object
            // counted.
            let mut contributed = size;
            if let Some(abandon) = file.abandon {
                let kind = abandon.kind;
                abandoned.insert(
                    index,
                    Abandon {
                        row: non_negative("input file abandon row", abandon.row)?,
                        kind,
                    },
                );
                match abandon.bytes {
                    Some(bytes) => {
                        contributed = non_negative("input file abandon bytes", bytes)?.min(size);
                        cut_file_seen |= kind == AbandonKind::Content && contributed < size;
                    }
                    None => unrecorded_abandon = true,
                }
            }
            completed_bytes = completed_bytes.saturating_add(contributed);
        }
        let bytes_floor = non_negative("processed_input_bytes", record.processed_input_bytes)?;
        // A checkpoint never persists a figure below the bytes of the files
        // before its cursor, so the sum is clamped to the persisted figure
        // only where that figure is lower for another reason (a held output
        // location fault reports no bytes). An abandon recorded without its
        // bytes is restored from the comparison the figures allow: a
        // persisted figure below the sum of the sizes is a file that
        // contributed fewer bytes than its size. That comparison only holds
        // until the run reads into the next file, which is why the bytes
        // are recorded now.
        if unrecorded_abandon {
            cut_file_seen |= bytes_floor < completed_bytes;
        }
        let completed_bytes = completed_bytes.min(bytes_floor);
        Ok(Self {
            processed: non_negative("processed_item_count", record.processed_item_count)?,
            failed: non_negative("failed_item_count", record.failed_item_count)?,
            completed_bytes,
            bytes_floor,
            cut_file_seen,
            usage: UsageAccum::restore(&record.usage),
            output_checked: record.report_record_count > 0,
            kept_records: record.report_record_count,
            kept_bytes: record.report_bytes,
            cursor_file,
            cursor_row: non_negative("cursor_row_index", record.cursor_row_index)?,
            abandoned,
            file_row_counts,
            file_etags,
            ..Self::default()
        })
    }

    /// One checkpoint's worth of progress; drains the pending records.
    pub(super) fn progress(&mut self, table_arn: &str) -> Progress {
        let bytes = self
            .completed_bytes
            .saturating_add(self.current_bytes)
            .max(self.bytes_floor);
        let records = std::mem::take(&mut self.pending);
        self.pending_bytes = 0;
        let finished_files = std::mem::take(&mut self.pending_finished);
        let abandoned_files = std::mem::take(&mut self.pending_abandoned);
        Progress {
            cursor_file_index: to_i64(u64::try_from(self.cursor_file).unwrap_or(u64::MAX)),
            cursor_row_index: to_i64(self.cursor_row),
            processed_item_count: to_i64(self.processed),
            failed_item_count: to_i64(self.failed),
            processed_input_bytes: to_i64(bytes),
            report_record_count: self.kept_records,
            report_bytes: self.kept_bytes,
            usage: persisted_usage(self, table_arn),
            records,
            finished_files,
            abandoned_files,
        }
    }

    /// Keep one report record, or drop and count it past either cap: the
    /// record count against `max_report_records`, the serialized bytes
    /// against `max_report_bytes`.
    pub(super) fn push_record(
        &mut self,
        sink: &ReportSink<'_>,
        kind: ReportKind,
        file_index: usize,
        row_index: u64,
        record: Value,
    ) {
        // A record whose kind was not requested is not written anywhere
        // (measured on the service: with only ERROR requested
        // the condition record appears in no file, with only
        // CONDITION_CHECK_FAILURE requested the error record appears in no
        // file). It still counted toward the counters at the call site. No
        // record is kept once an output location fault is held.
        if !kind_requested(kind, sink.requested) || self.held_location.is_some() {
            return;
        }
        let bytes = report::record_bytes(&record);
        if self.kept_records >= sink.max_records
            || self.kept_bytes.saturating_add(bytes) > sink.max_bytes
        {
            self.dropped_records = self.dropped_records.saturating_add(1);
            return;
        }
        self.kept_records += 1;
        self.kept_bytes = self.kept_bytes.saturating_add(bytes);
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        self.pending.push(ReportRecord {
            kind,
            file_index: to_i64(u64::try_from(file_index).unwrap_or(u64::MAX)),
            row_index: to_i64(row_index),
            record,
        });
    }

    /// Count one consumed physical line toward the batch boundary and the
    /// per-operation line cap. Every consumed line counts: blank lines,
    /// lines inside the resume skip region, a line over the line limit
    /// and a line that is not valid UTF-8. Returns `true` past the cap,
    /// with the failure set; the
    /// caller finishes without processing the line.
    pub(super) fn consume_line(&mut self, max_item_count: u64) -> bool {
        self.lines_consumed = self.lines_consumed.saturating_add(1);
        self.rows_since_checkpoint += 1;
        if self.lines_consumed > max_item_count {
            self.failure = Some(Failure::row_limit(max_item_count));
            return true;
        }
        false
    }

    /// A checkpoint is due when the row window reaches `batch_size`, or
    /// when the in-memory report buffer reaches `PENDING_FLUSH_BYTES`. The
    /// byte trigger bounds heap use on failing-heavy input independent of
    /// `batch_size`; an extra checkpoint is always safe (it only persists
    /// progress already made and drains the buffer).
    pub(super) fn checkpoint_due(&self, batch_size: usize) -> bool {
        self.rows_since_checkpoint >= batch_size || self.pending_bytes >= PENDING_FLUSH_BYTES
    }
}

pub(super) fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

/// The terminal status of a run: a set failure wins, then an observed
/// cancel, then failed rows; condition failures count in none of them.
pub(super) fn terminal_status(
    failure: Option<&Failure>,
    cancelled: bool,
    failed_item_count: u64,
) -> &'static str {
    if failure.is_some() {
        STATUS_FAILED
    } else if cancelled {
        STATUS_CANCELLED
    } else if failed_item_count > 0 {
        STATUS_COMPLETED_WITH_ERRORS
    } else {
        STATUS_COMPLETED
    }
}

/// What one report record is pushed against: the per-operation record and
/// byte caps and the kinds the request asked for.
pub(super) struct ReportSink<'a> {
    pub(super) max_records: u64,
    pub(super) max_bytes: u64,
    pub(super) requested: &'a [OutputType],
}

/// Whether the request asked for this report kind. A record of a kind that
/// was not requested is written to no file (measured on the service); the
/// handler refuses an empty `OutputType`, so at least one kind
/// is always requested.
pub(super) fn kind_requested(kind: ReportKind, requested: &[OutputType]) -> bool {
    match kind {
        ReportKind::Error => requested.contains(&OutputType::Error),
        ReportKind::ConditionCheckFailure => requested.contains(&OutputType::ConditionCheckFailure),
    }
}
