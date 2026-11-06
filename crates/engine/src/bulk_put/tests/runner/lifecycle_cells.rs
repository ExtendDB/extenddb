// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Runner lifecycle behaviour measured on the hosted service:
// the terminal status and byte accounting after an abandon, an input
// object that becomes unreadable mid-run, the four manifest statuses,
// the inclusive row-index ranges, and the two input quota classes.

/// The abandon message a row with an unknown attribute type tag carries.
const PARSE_ABANDON: &str = "Expected DynamoDB type to be one of [S, N, B, BOOL, NULL, L, M, SS, \
                             NS, BS]. Remainder of the file will not be processed.";
const ITEMS_FAILED_VALIDATION: &str =
    "Some of the items failed validation checks and were not ingested";

/// One well-formed row of exactly `bytes` characters, padded with a filler
/// attribute, so a file of them has an exact byte size and a row index
/// maps to a fixed byte offset.
fn sized_row(index: usize, bytes: usize) -> String {
    let head = format!(r#"{{"Item":{{"pk":{{"S":"k{index}"}},"sk":{{"S":"s"}},"p":{{"S":""#);
    let tail = r#""}}}"#;
    let pad = bytes
        .checked_sub(head.len() + tail.len())
        .expect("the row does not fit the requested length");
    format!("{head}{}{tail}", "x".repeat(pad))
}

/// One row whose attribute value carries an unknown type tag, padded the
/// same way: a parse-level fault that abandons the rest of the file.
fn sized_fault_row(index: usize, bytes: usize) -> String {
    let head = format!(r#"{{"Item":{{"pk":{{"S":"k{index}"}},"sk":{{"S":"s"}},"p":{{"X":""#);
    let tail = r#""}}}"#;
    let pad = bytes
        .checked_sub(head.len() + tail.len())
        .expect("the row does not fit the requested length");
    format!("{head}{}{tail}", "x".repeat(pad))
}

/// A file of `lines` rows of `line_bytes` bytes each, terminator
/// included, with the row at `fault_at` carrying the parse-level fault.
/// The file's size is exactly `lines * line_bytes`.
fn file_with_fault(lines: usize, line_bytes: usize, fault_at: usize) -> Vec<u8> {
    let mut out = String::new();
    for index in 0..lines {
        if index == fault_at {
            out.push_str(&sized_fault_row(index, line_bytes - 1));
        } else {
            out.push_str(&sized_row(index, line_bytes - 1));
        }
        out.push('\n');
    }
    assert_eq!(out.len(), lines * line_bytes, "file size must be exact");
    out.into_bytes()
}

/// Hands out lines from the real reader until `at` lines have been
/// yielded, then answers every later call with an I/O error of `kind`:
/// the input object has become unreadable mid-run.
struct UnreadableAfterSource {
    inner: FsSource,
    at: u64,
    kind: std::io::ErrorKind,
    handed: Arc<AtomicU64>,
}

struct UnreadableAfterReader {
    inner: Box<dyn LineReader>,
    at: u64,
    kind: std::io::ErrorKind,
    handed: Arc<AtomicU64>,
}

impl BulkSource for UnreadableAfterSource {
    fn check_bucket(
        &self,
        bucket: &BucketRef<'_>,
        prefix: &str,
    ) -> BoxFuture<'_, Result<(), SourceError>> {
        self.inner.check_bucket(bucket, prefix)
    }

    fn etag(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
    ) -> BoxFuture<'_, Result<String, SourceError>> {
        self.inner.etag(bucket, file)
    }

    fn line_count(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<u64, SourceError>> {
        self.inner.line_count(bucket, file, compression)
    }

    fn list(
        &self,
        bucket: &BucketRef<'_>,
        prefix: &str,
        max_files: u64,
    ) -> BoxFuture<'_, Result<Vec<SourceFile>, SourceError>> {
        self.inner.list(bucket, prefix, max_files)
    }

    fn open(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<Box<dyn LineReader>, SourceError>> {
        let fut = self.inner.open(bucket, file, compression);
        let at = self.at;
        let kind = self.kind;
        let handed = Arc::clone(&self.handed);
        Box::pin(async move {
            let inner = fut.await?;
            Ok(Box::new(UnreadableAfterReader {
                inner,
                at,
                kind,
                handed,
            }) as Box<dyn LineReader>)
        })
    }
}

impl LineReader for UnreadableAfterReader {
    fn next_line(
        &mut self,
        max_line_bytes: usize,
    ) -> BoxFuture<'_, Result<Option<String>, SourceError>> {
        Box::pin(async move {
            if self.handed.load(Ordering::SeqCst) >= self.at {
                return Err(SourceError::Io(std::io::Error::new(
                    self.kind,
                    "input object is no longer readable",
                )));
            }
            let line = self.inner.next_line(max_line_bytes).await;
            if matches!(&line, Ok(Some(_))) {
                self.handed.fetch_add(1, Ordering::SeqCst);
            }
            line
        })
    }

    fn bytes_consumed(&self) -> u64 {
        self.inner.bytes_consumed()
    }

    fn last_line_end(&self) -> u64 {
        self.inner.last_line_end()
    }
}

/// Deps with the input quota limits replaced, so a record enumerated under
/// wider caps meets the narrower ones the runner enforces.
fn deps_with_limits(h: &Harness, owner: &str, limits: LimitsConfig) -> RunnerDeps {
    let mut deps = h.deps(owner, config());
    deps.limits = Arc::new(limits);
    deps
}

fn manifest_status(line: &Value) -> &str {
    line["fileProcessingStatus"]
        .as_str()
        .expect("the manifest line carries a status")
}

// The terminal status and byte accounting after an abandon.

#[tokio::test]
async fn an_abandon_more_than_one_read_from_the_end_cuts_the_file_and_fails() {
    // A fault on line 1 of a 23,840-byte file leaves
    // ProcessedInputBytes at 8,000 and the operation FAILED.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_with_fault(149, 160, 0));
    let files = h.enumerate().await;
    assert_eq!(files[0].size, 23_840);
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.processed_input_bytes, 8_000);
    assert_eq!(record.total_input_bytes, Some(23_840));
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some(ITEMS_FAILED_VALIDATION)
    );
    assert_eq!(record.processed_item_count, 1);
    assert_eq!(record.failed_item_count, 1);
}

#[tokio::test]
async fn an_abandon_in_a_file_inside_one_read_counts_every_byte_and_completes_with_errors() {
    // A fault on line 1 of a 1,980-byte file counts the whole
    // file and ends COMPLETED_WITH_ERRORS.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_with_fault(11, 180, 0));
    let files = h.enumerate().await;
    assert_eq!(files[0].size, 1_980);
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.processed_input_bytes, 1_980);
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.failure_code, None);
    assert_eq!(record.failed_item_count, 1);
}

#[tokio::test]
async fn an_abandon_in_the_last_read_of_a_file_counts_every_byte_and_completes_with_errors() {
    // A fault on the last line of a 23,840-byte file counts the
    // whole file and ends COMPLETED_WITH_ERRORS.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_with_fault(149, 160, 148));
    let files = h.enumerate().await;
    assert_eq!(files[0].size, 23_840);
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.processed_input_bytes, 23_840);
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.failure_code, None);
    assert_eq!(
        record.processed_item_count, 149,
        "every line of the file was consumed"
    );
    assert_eq!(record.failed_item_count, 1);
}

#[tokio::test]
async fn an_abandon_early_in_a_two_read_file_stops_the_counter_at_the_first_read() {
    // A fault at about byte 1,100 of a 9,990-byte file leaves
    // ProcessedInputBytes at 8,000 and the operation FAILED.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_with_fault(90, 111, 9));
    let files = h.enumerate().await;
    assert_eq!(files[0].size, 9_990);
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.processed_input_bytes, 8_000);
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(record.processed_item_count, 10);
    assert_eq!(record.failed_item_count, 1);
}

#[tokio::test]
async fn a_cut_file_carries_its_abandon_record_and_partial_manifest_entry() {
    // The abandon record and the manifest entry are the same whether the
    // file was cut or not.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_with_fault(149, 160, 0));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-1", config()), record)
        .await
        .expect("runner run");

    let report = h
        .read_report(&op, ReportKind::Error)
        .expect("the abandon record is rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(report[0]["errorMessages"], json!([PARSE_ABANDON]));
    assert!(
        report[0]["itemLocation"].get("itemIndex").is_none(),
        "an abandon record carries no itemIndex"
    );
    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(manifest_status(&manifest[0]), "PARTIALLY_PROCESSED");
    assert_eq!(manifest[0]["processedRanges"], json!([]));
}

#[tokio::test]
async fn a_cut_file_stays_cut_across_a_restart() {
    // The cut is part of the file's finished state: a resumed run neither
    // re-counts the bytes beyond the fault's read nor loses the terminal
    // failure.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_with_fault(149, 160, 0));
    h.write_input("part-001.json", &file_of(&plain_rows(4)));
    let files = h.enumerate().await;
    let second_size = files[1].size;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // The first runner cuts file 0, checkpoints its end of file, and dies
    // inside the open of file 1.
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAtOpenSource {
        inner: h.fs_source(),
        pend_at: 2,
        opens: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let mut cfg = config();
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the open pends"),
        () = notify.notified() => {}
    }
    let mid = h.record(&op).await;
    assert_eq!(mid.cursor_file_index, 1);
    assert_eq!(
        mid.processed_input_bytes, 8_000,
        "the checkpoint persisted the cut, not the file's size"
    );

    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-2", config()), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(
        record.processed_input_bytes,
        8_000 + second_size,
        "the resumed run adds the second file's bytes to the cut total"
    );
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some(ITEMS_FAILED_VALIDATION),
        "the cut survives the restart and still ends the operation"
    );
}

// An input object that becomes unreadable after Start.

#[tokio::test]
async fn an_input_object_deleted_mid_run_fails_with_the_no_such_key_code() {
    // The object is deleted mid-run; the operation ends FAILED
    // with S3NoSuchKey, keeps the rows processed before the fault, and the
    // manifest marks the file NOT_AVAILABLE with no processed range.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(30)));
    h.write_input("part-001.json", &file_of(&plain_rows(4)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let source = Arc::new(UnreadableAfterSource {
        inner: h.fs_source(),
        at: 10,
        kind: std::io::ErrorKind::NotFound,
        handed: Arc::new(AtomicU64::new(0)),
    });
    let mut cfg = config();
    cfg.batch_size = 5;
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchKey"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("The specified key does not exist.")
    );
    assert_eq!(
        record.processed_item_count, 10,
        "the rows read before the fault stand"
    );
    assert_eq!(h.count_items().await, 10, "every row written stays written");

    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(manifest.len(), 2);
    assert_eq!(manifest_status(&manifest[0]), "NOT_AVAILABLE");
    assert_eq!(manifest[0]["processedRanges"], json!([]));
    assert_eq!(
        manifest_status(&manifest[1]),
        "NOT_PROCESSED",
        "a file the operation never opened is not reported as partially processed"
    );
    assert_eq!(manifest[1]["processedRanges"], json!([]));
    assert_eq!(
        manifest[1]["etag"],
        json!(h.input_md5("part-001.json")),
        "an unreached file is still hashed"
    );
}

#[tokio::test]
async fn an_input_object_that_loses_read_permission_mid_run_fails_with_access_denied() {
    // The read is denied mid-run; the operation ends FAILED with
    // S3AccessDenied and the file is NOT_AVAILABLE.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(30)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let source = Arc::new(UnreadableAfterSource {
        inner: h.fs_source(),
        at: 10,
        kind: std::io::ErrorKind::PermissionDenied,
        handed: Arc::new(AtomicU64::new(0)),
    });
    let mut cfg = config();
    cfg.batch_size = 5;
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(record.failure_message.as_deref(), Some("Access Denied"));
    assert_eq!(record.processed_item_count, 10);
    let usage = record.usage.to_string();
    assert!(
        usage.contains("10240"),
        "usage keeps the ten rows already written: {usage}"
    );

    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(manifest_status(&manifest[0]), "NOT_AVAILABLE");
    assert_eq!(manifest[0]["processedRanges"], json!([]));
}

// The processed ranges are inclusive row indexes.

#[tokio::test]
async fn a_processed_range_ends_at_the_last_row_index_the_table_holds() {
    // A range is an inclusive pair of row indexes
    // starting at 0, and the table holds exactly rows 0 to the range end.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(7)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-1", config()), record)
        .await
        .expect("runner run");

    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(manifest_status(&manifest[0]), "FULLY_PROCESSED");
    assert_eq!(
        manifest[0]["processedRanges"],
        json!([[0, 6]]),
        "seven rows end at inclusive row index six"
    );
    assert!(h.get("k6").await.is_some(), "the range end row is present");
    assert!(
        h.get("k7").await.is_none(),
        "no row beyond the range end is present"
    );
    assert_eq!(h.count_items().await, 7);
}

// The per-operation input quotas.

#[tokio::test]
async fn more_input_objects_than_the_count_quota_fails_the_operation() {
    // The message interpolates the prefix as the request gave it
    // and the cap as a bare number, and nothing is written under the
    // output prefix.
    let h = Harness::new().await;
    h.write_input("h1/o-000000.json", &file_of(&plain_rows(2)));
    h.write_input("h1/o-000001.json", &file_of(&plain_rows(2)));
    let files = h.enumerate().await;
    let input = json!({
        "S3Bucket": IN_BUCKET,
        "S3KeyPrefix": "h1/",
        "InputFormat": "DYNAMODB_JSON",
        "InputCompressionType": "NONE",
    });
    let op = h.start(files, input, both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let limits = LimitsConfig {
        max_bulk_input_files: 1,
        ..LimitsConfig::default()
    };
    runner::run(&deps_with_limits(&h, "runner-1", limits), record)
        .await
        .expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(
        record.failure_code.as_deref(),
        Some("S3ObjectsCountLimitExceeded")
    );
    assert_eq!(
        record.failure_message.as_deref(),
        Some(
            "You have reached the maximum number of objects allowed in a single bulk operation \
             for the specified prefix h1/, which is 1. You can reach out to AWS Support to \
             request a quota increase."
        )
    );
    assert_eq!(record.processed_item_count, 0);
    assert_eq!(record.processed_input_bytes, 0);
    assert_eq!(h.count_items().await, 0, "no file was read");
    assert!(
        h.read_manifest(&op).is_none(),
        "the count class writes nothing under the output prefix"
    );
}

#[tokio::test]
async fn an_input_object_above_the_byte_quota_fails_the_operation_and_writes_the_manifest() {
    // The per-object class reports TotalInputBytes from the
    // listing, ProcessedInputBytes 0, and a manifest marking the file
    // NOT_PROCESSED.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(2)));
    // The quota is decided on the enumerated size, before any open, so the
    // record carries the measured size over a small staged file.
    let files = vec![InputFile {
        path: "part-000.json".to_owned(),
        size: 5_369_000_000,
        rows: None,
        etag: None,
        abandon: None,
    }];
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let limits = LimitsConfig {
        max_bulk_input_bytes: 5_368_709_120,
        ..LimitsConfig::default()
    };
    runner::run(&deps_with_limits(&h, "runner-1", limits), record)
        .await
        .expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("InputObjectTooLarge"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some(
            "This bulk operation contains one or more input objects larger than the allowed \
             per-object size of 5.0 GB. Split those objects into smaller parts and retry."
        )
    );
    assert_eq!(record.total_input_bytes, Some(5_369_000_000));
    assert_eq!(record.processed_input_bytes, 0);
    assert_eq!(record.processed_item_count, 0);
    assert_eq!(h.count_items().await, 0, "no file was read");

    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(manifest.len(), 1);
    assert_eq!(manifest_status(&manifest[0]), "NOT_PROCESSED");
    assert_eq!(manifest[0]["processedRanges"], json!([]));
}

// The object-count quota admits exactly the cap
// (the comparison is `>`, not `>=`). The hosted service's exact edge is
// unmeasured (the measured case was over the quota); this pins the code's edge. Two
// objects under a cap of two are read.
#[tokio::test]
async fn exactly_the_count_quota_of_objects_is_admitted() {
    let h = Harness::new().await;
    h.write_input("q/o-0.json", &file_of(&plain_rows(2)));
    h.write_input("q/o-1.json", &file_of(&(2..4).map(row).collect::<Vec<_>>()));
    let input = json!({
        "S3Bucket": IN_BUCKET,
        "S3KeyPrefix": "q/",
        "InputFormat": "DYNAMODB_JSON",
        "InputCompressionType": "NONE",
    });
    let files = h.enumerate().await;
    assert_eq!(files.len(), 2);
    let op = h.start(files, input, both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let limits = LimitsConfig {
        max_bulk_input_files: 2,
        ..LimitsConfig::default()
    };
    runner::run(&deps_with_limits(&h, "runner-1", limits), record)
        .await
        .expect("runner run");
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED", "exactly the cap is admitted");
    assert_ne!(
        record.failure_code.as_deref(),
        Some("S3ObjectsCountLimitExceeded")
    );
    assert_eq!(h.count_items().await, 4);
}

// The per-object size quota admits a file exactly
// at the cap (the comparison is `>`, not `>=`). Unmeasured on the service at
// the edge; this pins the code. The cap is set to the file's own enumerated
// size.
#[tokio::test]
async fn an_input_object_exactly_at_the_byte_quota_is_admitted() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(2)));
    let files = h.enumerate().await;
    let size = u64::try_from(files[0].size).unwrap();
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let limits = LimitsConfig {
        max_bulk_input_object_bytes: size,
        ..LimitsConfig::default()
    };
    runner::run(&deps_with_limits(&h, "runner-1", limits), record)
        .await
        .expect("runner run");
    let record = h.record(&op).await;
    assert_eq!(
        record.status, "COMPLETED",
        "a file exactly at the cap is admitted"
    );
    assert_ne!(record.failure_code.as_deref(), Some("InputObjectTooLarge"));
    assert_eq!(h.count_items().await, 2);
}
