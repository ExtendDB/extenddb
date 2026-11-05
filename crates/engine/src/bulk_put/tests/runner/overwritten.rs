// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// An input object overwritten after the operation listed it. Measured on the
// service for an object replaced after the listing and before its read: the
// file counts one processed and failed row with the overwritten message and
// no `itemIndex`, none of its bytes are processed input, the manifest marks
// it `PARTIALLY_PROCESSED` with no range and its listed entity tag, the run
// goes on with the next file, and the operation ends `FAILED` with
// `ValidationException`. An object replaced during its read, or between a
// crash and the resume, is ExtendDB's extension of the same rule: the rows
// it delivered stand.

const OVERWRITTEN: &str = "The S3 object could not be imported because it was overwritten";
const ITEMS_FAILED: &str = "Some of the items failed validation checks and were not ingested";

/// Answers `SourceError::Overwritten` for the input file at `path`: at its
/// open when `after` is `None`, otherwise once `after` of its lines were
/// handed out. Every other call goes to the real source.
struct OverwrittenSource {
    inner: FsSource,
    path: &'static str,
    after: Option<u64>,
}

struct OverwrittenAfterReader {
    inner: Box<dyn LineReader>,
    after: u64,
    handed: u64,
}

impl BulkSource for OverwrittenSource {
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
        if file.path != self.path {
            return self.inner.open(bucket, file, compression);
        }
        let Some(after) = self.after else {
            return Box::pin(async { Err(SourceError::Overwritten) });
        };
        let fut = self.inner.open(bucket, file, compression);
        Box::pin(async move {
            let inner = fut.await?;
            Ok(Box::new(OverwrittenAfterReader {
                inner,
                after,
                handed: 0,
            }) as Box<dyn LineReader>)
        })
    }
}

impl LineReader for OverwrittenAfterReader {
    fn next_line(
        &mut self,
        max_line_bytes: usize,
    ) -> BoxFuture<'_, Result<Option<String>, SourceError>> {
        Box::pin(async move {
            if self.handed >= self.after {
                return Err(SourceError::Overwritten);
            }
            let line = self.inner.next_line(max_line_bytes).await;
            if matches!(&line, Ok(Some(_))) {
                self.handed += 1;
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

fn size_of(rows: &[String]) -> i64 {
    i64::try_from(file_of(rows).len()).unwrap()
}

/// The one error record of `path`: its location names the object alone,
/// with no `itemIndex`, and it carries the overwritten message.
fn assert_overwritten_record(records: &[Value], path: &str) {
    let mine: Vec<_> = records
        .iter()
        .filter(|r| {
            r["itemLocation"]["inputSourceArn"]
                .as_str()
                .is_some_and(|arn| arn.ends_with(&format!("/{path}")))
        })
        .collect();
    assert_eq!(mine.len(), 1, "{records:?}");
    assert!(
        mine[0]["itemLocation"].get("itemIndex").is_none(),
        "{mine:?}"
    );
    assert_eq!(mine[0]["errorMessages"], json!([OVERWRITTEN]));
}

#[tokio::test]
async fn an_object_overwritten_before_its_read_counts_one_failed_row_and_the_run_goes_on() {
    let h = Harness::new().await;
    let (first, second, third) = (plain_rows(5), plain_rows(3), plain_rows(4));
    h.write_input("part-000.json", &file_of(&first));
    h.write_input("part-001.json", &file_of(&second));
    h.write_input("part-002.json", &file_of(&third));
    let mut files = h.enumerate().await;
    // The tag an S3 listing gives; the manifest line of the overwritten
    // object carries it.
    files[1].etag = Some(h.input_md5("part-001.json"));
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let source = Arc::new(OverwrittenSource {
        inner: h.fs_source(),
        path: "part-001.json",
        after: None,
    });
    let deps = h.deps_with("runner-1", config(), source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(record.failure_message.as_deref(), Some(ITEMS_FAILED));
    assert_eq!(record.processed_item_count, 5 + 1 + 4);
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(
        record.processed_input_bytes,
        size_of(&first) + size_of(&third),
        "none of the overwritten object's bytes are processed input"
    );

    let errors = h.read_report(&op, ReportKind::Error).expect("error report");
    assert_overwritten_record(&errors, "part-001.json");
    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(manifest.len(), 3);
    assert_eq!(
        manifest[0]["fileProcessingStatus"],
        json!("FULLY_PROCESSED")
    );
    assert_eq!(
        manifest[1]["fileProcessingStatus"],
        json!("PARTIALLY_PROCESSED")
    );
    assert_eq!(manifest[1]["processedRanges"], json!([]));
    assert_eq!(manifest[1]["etag"], json!(h.input_md5("part-001.json")));
    assert_eq!(
        manifest[2]["fileProcessingStatus"],
        json!("FULLY_PROCESSED")
    );
    assert_eq!(manifest[2]["processedRanges"], json!([[0, 3]]));
}

#[tokio::test]
async fn an_object_overwritten_during_its_read_keeps_the_rows_it_delivered() {
    let h = Harness::new().await;
    let rows = plain_rows(8);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let source = Arc::new(OverwrittenSource {
        inner: h.fs_source(),
        path: "part-000.json",
        after: Some(3),
    });
    let deps = h.deps_with("runner-1", config(), source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(record.failure_message.as_deref(), Some(ITEMS_FAILED));
    assert_eq!(record.processed_item_count, 3 + 1);
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(h.count_items().await, 3, "the delivered rows stand");
    assert_eq!(
        record.processed_input_bytes,
        size_of(&rows[..3]),
        "the bytes of the delivered rows"
    );
    let errors = h.read_report(&op, ReportKind::Error).expect("error report");
    assert_overwritten_record(&errors, "part-000.json");
    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(
        manifest[0]["fileProcessingStatus"],
        json!("PARTIALLY_PROCESSED")
    );
    assert_eq!(manifest[0]["processedRanges"], json!([[0, 2]]));
}

#[tokio::test]
async fn an_object_overwritten_before_a_resumed_open_keeps_the_counted_rows() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(12)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;

    // The first claim checkpoints 5 rows and stops.
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after: 7,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let mut cfg = config();
    cfg.batch_size = 5;
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
    let checkpointed = h.record(&op).await;
    assert_eq!(checkpointed.cursor_row_index, 5);

    // The resumed claim finds the object replaced at its open.
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let source = Arc::new(OverwrittenSource {
        inner: h.fs_source(),
        path: "part-000.json",
        after: None,
    });
    let deps = h.deps_with("runner-2", config(), source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(record.processed_item_count, 5 + 1);
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(
        record.processed_input_bytes, checkpointed.processed_input_bytes,
        "the counted bytes stand"
    );
    let errors = h.read_report(&op, ReportKind::Error).expect("error report");
    assert_overwritten_record(&errors, "part-000.json");
    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(
        manifest[0]["fileProcessingStatus"],
        json!("PARTIALLY_PROCESSED")
    );
    assert_eq!(manifest[0]["processedRanges"], json!([[0, 4]]));
}
