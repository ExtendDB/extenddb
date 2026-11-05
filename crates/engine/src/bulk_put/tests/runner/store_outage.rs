// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A store unavailable past its retries leaves the operation in progress:
//! the next claim resumes it from its checkpoint, as after a transient
//! database fault, under the bound on resumes without progress.

use super::*;
use crate::bulk_put::source::StoreUnavailable;

fn outage() -> std::io::Error {
    StoreUnavailable::io("S3 read failed".to_owned())
}

/// Serves lines from the real reader until `at` lines have been handed
/// out, then answers the next read with the store's failure past its
/// retries, once.
struct OutageSource {
    inner: FsSource,
    at: u64,
    handed: Arc<AtomicU64>,
    fired: Arc<AtomicBool>,
}

struct OutageReader {
    inner: Box<dyn LineReader>,
    at: u64,
    handed: Arc<AtomicU64>,
    fired: Arc<AtomicBool>,
}

impl BulkSource for OutageSource {
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
        let (at, handed, fired) = (self.at, Arc::clone(&self.handed), Arc::clone(&self.fired));
        Box::pin(async move {
            let inner = fut.await?;
            Ok(Box::new(OutageReader {
                inner,
                at,
                handed,
                fired,
            }) as Box<dyn LineReader>)
        })
    }
}

impl LineReader for OutageReader {
    fn next_line(
        &mut self,
        max_line_bytes: usize,
    ) -> BoxFuture<'_, Result<Option<String>, SourceError>> {
        Box::pin(async move {
            if self.handed.load(Ordering::SeqCst) >= self.at
                && !self.fired.swap(true, Ordering::SeqCst)
            {
                return Err(SourceError::Io(outage()));
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

    fn etag(&self) -> Option<String> {
        self.inner.etag()
    }
}

/// Answers the first `failures` report opens with the store's failure past
/// its retries, then renders through the real sink.
struct OutageSink {
    inner: FsSink,
    failures: Arc<AtomicU64>,
}

impl BulkSink for OutageSink {
    fn write_manifest(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
        operation_id: &str,
        owner: &str,
        sse: &OutputSse,
        lines: Vec<Value>,
    ) -> BoxFuture<'_, Result<String, SinkError>> {
        self.inner
            .write_manifest(at, prefix, operation_id, owner, sse, lines)
    }

    fn check_location(
        &self,
        at: &BucketRef<'_>,
        key: &str,
        sse: &OutputSse,
    ) -> BoxFuture<'_, Result<(), SinkError>> {
        self.inner.check_location(at, key, sse)
    }

    fn open(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
        operation_id: &str,
        kind: ReportKind,
        owner: &str,
        sse: &OutputSse,
    ) -> BoxFuture<'_, Result<Box<dyn ReportWriter>, SinkError>> {
        let failing = self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok();
        if failing {
            return Box::pin(async { Err(SinkError::Io(outage())) });
        }
        self.inner.open(at, prefix, operation_id, kind, owner, sse)
    }
}

/// Twenty rows, two of them failed rows with error records.
fn rows_with_two_errors() -> Vec<String> {
    let mut rows = plain_rows(20);
    rows[3] = row_missing_sk(3);
    rows[14] = row_missing_sk(14);
    rows
}

fn checkpoint_every_five() -> RunnerConfig {
    RunnerConfig {
        batch_size: 5,
        ..config()
    }
}

#[tokio::test]
async fn an_input_store_outage_past_the_retries_leaves_the_operation_to_resume() {
    let rows = rows_with_two_errors();
    let fresh = Harness::new().await;
    fresh.write_input("part-000.json", &file_of(&rows));
    let files = fresh.enumerate().await;
    let expected = fresh
        .run_to_end(
            files,
            input_config(false),
            both_reports(),
            checkpoint_every_five(),
        )
        .await;

    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let source = Arc::new(OutageSource {
        inner: h.fs_source(),
        at: 12,
        handed: Arc::new(AtomicU64::new(0)),
        fired: Arc::new(AtomicBool::new(false)),
    });
    let deps = h.with_short_store_lease(h.deps_with(
        "runner-1",
        checkpoint_every_five(),
        source,
        Arc::new(h.fs_sink()),
    ));
    runner::run(&deps, record)
        .await
        .expect("the outage stops the run cleanly");

    let mid = h.record(&op).await;
    assert_eq!(mid.status, "IN_PROGRESS", "the outage is no outcome");
    assert_eq!(mid.failure_code, None);
    assert_eq!(
        mid.cursor_row_index, 12,
        "the rows read before the outage are written and checkpointed"
    );
    assert_eq!(h.count_items().await, 11);
    assert!(h.read_manifest(&op).is_none(), "no output is written yet");

    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-2", checkpoint_every_five()), record)
        .await
        .expect("resumed run");
    let record = h.record(&op).await;
    assert_eq!(record.status, expected.status);
    assert_eq!(record.processed_item_count, expected.processed_item_count);
    assert_eq!(record.failed_item_count, expected.failed_item_count);
    assert_eq!(record.processed_input_bytes, expected.processed_input_bytes);
    assert_eq!(record.usage, expected.usage);
    assert_eq!(h.count_items().await, 18);
    assert_eq!(
        h.read_report(&op, ReportKind::Error).map(|r| r.len()),
        Some(2)
    );
}

#[tokio::test]
async fn a_report_store_outage_past_the_retries_leaves_the_operation_to_resume() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&rows_with_two_errors()));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let sink = Arc::new(OutageSink {
        inner: h.fs_sink(),
        failures: Arc::new(AtomicU64::new(1)),
    });
    let deps = h.with_short_store_lease(h.deps_with(
        "runner-1",
        checkpoint_every_five(),
        Arc::new(h.fs_source()),
        sink,
    ));
    runner::run(&deps, record)
        .await
        .expect("the outage stops the run cleanly");

    let mid = h.record(&op).await;
    assert_eq!(mid.status, "IN_PROGRESS", "the outage is no outcome");
    assert_eq!(mid.failure_code, None);
    assert_eq!(mid.processed_item_count, 20, "every row was read");
    assert_eq!(h.count_items().await, 18, "every row was written");
    assert!(h.read_report(&op, ReportKind::Error).is_none());

    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-2", checkpoint_every_five()), record)
        .await
        .expect("resumed run");
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 20);
    assert_eq!(record.failed_item_count, 2);
    assert_eq!(
        h.read_report(&op, ReportKind::Error).map(|r| r.len()),
        Some(2),
        "the resumed run renders the report"
    );
    assert!(h.read_manifest(&op).is_some());
}

#[tokio::test]
async fn an_outage_that_outlasts_the_resume_bound_ends_the_operation_failed() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&rows_with_two_errors()));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let failures = Arc::new(AtomicU64::new(u64::MAX));
    let mut claims = 0;
    while h.record(&op).await.status == "IN_PROGRESS" && claims < 13 {
        let owner = format!("runner-{claims}");
        let record = h.claim(&owner, Duration::from_millis(100)).await;
        claims += 1;
        let sink = Arc::new(OutageSink {
            inner: h.fs_sink(),
            failures: Arc::clone(&failures),
        });
        let deps = h.with_short_store_lease(h.deps_with(
            &owner,
            checkpoint_every_five(),
            Arc::new(h.fs_source()),
            sink,
        ));
        runner::run(&deps, record)
            .await
            .expect("each run stops cleanly");
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    let record = h.record(&op).await;
    assert_eq!(
        claims, 12,
        "the first claim reads every row, the first resume finds the checkpoint it \
         left, and ten more resumes find that checkpoint unmoved"
    );
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("InternalServerError"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("Internal server error")
    );
    assert_eq!(record.processed_item_count, 20);
}
