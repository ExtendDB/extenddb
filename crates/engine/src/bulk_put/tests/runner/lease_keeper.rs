// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

use tokio::sync::Notify;

// A source call can wait longer than one lease: an S3 request that gets no
// answer is retried for up to 120 seconds against a 60-second lease. While a
// call waits, the runner runs a boundary every third of the lease, so the
// lease is kept, the rows read before the call are written, and a cancel is
// seen. These tests use a 300 ms lease and calls that wait over a second.

/// What a `WaitSource` does at its chosen point.
#[derive(Clone)]
pub(super) enum Wait {
    /// Signal `frozen`, then answer only after `resume`. A test that stops
    /// polling the runner future once `frozen` fires models a paused or
    /// swapped process: nothing in the runner runs during the freeze, the
    /// lease keeper included, so the lease runs out as it would under a
    /// stopped process.
    Freeze {
        frozen: Arc<Notify>,
        resume: Arc<Notify>,
    },
    /// Signal `waiting` and never answer, as an S3 request held open.
    Pend { waiting: Arc<Notify> },
    /// Answer an I/O error after `hold`, as an S3 call retried until its
    /// bound.
    FailAfter { hold: Duration },
}

impl Wait {
    pub(super) fn freeze() -> (Self, Arc<Notify>, Arc<Notify>) {
        let frozen = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let wait = Self::Freeze {
            frozen: Arc::clone(&frozen),
            resume: Arc::clone(&resume),
        };
        (wait, frozen, resume)
    }

    fn pend() -> (Self, Arc<Notify>) {
        let waiting = Arc::new(Notify::new());
        (
            Self::Pend {
                waiting: Arc::clone(&waiting),
            },
            waiting,
        )
    }

    async fn run(&self) -> Result<(), SourceError> {
        match self {
            Self::Freeze { frozen, resume } => {
                frozen.notify_one();
                resume.notified().await;
                Ok(())
            }
            Self::Pend { waiting } => {
                waiting.notify_one();
                std::future::pending::<()>().await;
                Ok(())
            }
            Self::FailAfter { hold } => {
                tokio::time::sleep(*hold).await;
                Err(SourceError::Io(std::io::Error::other(
                    "the call was retried until its bound",
                )))
            }
        }
    }
}

/// The filesystem source with one `Wait`: at every open when `at_open` is
/// set, and once, before handing out line index `at_line.0` (counted over
/// the whole run), when `at_line` is set.
pub(super) struct WaitSource {
    pub(super) inner: FsSource,
    pub(super) at_open: Option<Wait>,
    pub(super) at_line: Option<(u64, Wait)>,
    pub(super) handed: Arc<AtomicU64>,
    pub(super) fired: Arc<AtomicBool>,
}

impl WaitSource {
    pub(super) fn at_line(inner: FsSource, at: u64, wait: Wait) -> Self {
        Self {
            inner,
            at_open: None,
            at_line: Some((at, wait)),
            handed: Arc::new(AtomicU64::new(0)),
            fired: Arc::new(AtomicBool::new(false)),
        }
    }
}

struct WaitReader {
    inner: Box<dyn LineReader>,
    at_line: Option<(u64, Wait)>,
    handed: Arc<AtomicU64>,
    fired: Arc<AtomicBool>,
}

impl BulkSource for WaitSource {
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
        let at_open = self.at_open.clone();
        let at_line = self.at_line.clone();
        let handed = Arc::clone(&self.handed);
        let fired = Arc::clone(&self.fired);
        Box::pin(async move {
            if let Some(wait) = at_open {
                wait.run().await?;
            }
            let inner = fut.await?;
            Ok(Box::new(WaitReader {
                inner,
                at_line,
                handed,
                fired,
            }) as Box<dyn LineReader>)
        })
    }
}

impl LineReader for WaitReader {
    fn next_line(
        &mut self,
        max_line_bytes: usize,
    ) -> BoxFuture<'_, Result<Option<String>, SourceError>> {
        Box::pin(async move {
            if let Some((at, wait)) = &self.at_line
                && self.handed.load(Ordering::SeqCst) >= *at
                && !self.fired.swap(true, Ordering::SeqCst)
            {
                wait.run().await?;
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

const LEASE: Duration = Duration::from_millis(300);

/// A run with one file of 12 rows, a 300 ms lease, and no write batch or
/// checkpoint due inside the file, so the only boundary while a call waits
/// is the one the wait itself runs.
async fn twelve_rows(h: &Harness) -> (String, BulkOperationRecord, RunnerConfig) {
    h.write_input("part-000.json", &file_of(&plain_rows(12)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", LEASE).await;
    let mut cfg = config();
    cfg.batch_size = 100;
    cfg.write_batch_rows = 100;
    cfg.lease = LEASE;
    (op, record, cfg)
}

#[tokio::test]
async fn a_read_that_waits_past_the_lease_keeps_it_and_writes_the_failure() {
    let h = Harness::new().await;
    let (op, record, cfg) = twelve_rows(&h).await;
    let wait = Wait::FailAfter {
        hold: Duration::from_millis(1200),
    };
    let source = Arc::new(WaitSource::at_line(h.fs_source(), 5, wait));
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    tokio::time::timeout(Duration::from_secs(10), runner::run(&deps, record))
        .await
        .expect("the run ends")
        .expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED", "{record:?}");
    assert_eq!(record.failure_code.as_deref(), Some("InternalServerError"));
    assert_eq!(record.processed_item_count, 5);
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(
        h.count_items().await,
        5,
        "the rows read before the call are written, not dropped"
    );
}

#[tokio::test]
async fn a_cancel_is_seen_while_a_read_waits() {
    let h = Harness::new().await;
    let (op, record, cfg) = twelve_rows(&h).await;
    let (wait, waiting) = Wait::pend();
    let source = Arc::new(WaitSource::at_line(h.fs_source(), 5, wait));
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    let run = runner::run(&deps, record);
    tokio::pin!(run);
    tokio::select! {
        _ = &mut run => panic!("the runner must not finish before the read waits"),
        () = waiting.notified() => {}
    }
    h.storage
        .request_cancel(ACCOUNT, &op)
        .await
        .expect("request cancel");
    tokio::time::timeout(Duration::from_secs(5), &mut run)
        .await
        .expect("the cancel is seen while the read waits")
        .expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED", "{record:?}");
    assert_eq!(record.processed_item_count, 5);
    assert_eq!(h.count_items().await, 5);
}

#[tokio::test]
async fn a_cancel_is_seen_while_an_open_waits() {
    let h = Harness::new().await;
    let (op, record, cfg) = twelve_rows(&h).await;
    let (wait, waiting) = Wait::pend();
    let source = Arc::new(WaitSource {
        inner: h.fs_source(),
        at_open: Some(wait),
        at_line: None,
        handed: Arc::new(AtomicU64::new(0)),
        fired: Arc::new(AtomicBool::new(false)),
    });
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    let run = runner::run(&deps, record);
    tokio::pin!(run);
    tokio::select! {
        _ = &mut run => panic!("the runner must not finish before the open waits"),
        () = waiting.notified() => {}
    }
    h.storage
        .request_cancel(ACCOUNT, &op)
        .await
        .expect("request cancel");
    tokio::time::timeout(Duration::from_secs(5), &mut run)
        .await
        .expect("the cancel is seen while the open waits")
        .expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED", "{record:?}");
    assert_eq!(record.processed_item_count, 0);
    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(manifest[0]["fileProcessingStatus"], json!("NOT_PROCESSED"));
}

#[tokio::test]
async fn a_cancel_is_seen_while_a_resumed_claim_reads_the_covered_rows_again() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(12)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;

    // The first claim checkpoints 5 rows and stops.
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let notify = Arc::new(Notify::new());
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
    assert_eq!(h.record(&op).await.cursor_row_index, 5);

    // The resumed claim's read of the covered rows waits at row 2.
    let record = h.claim("runner-2", LEASE).await;
    let (wait, waiting) = Wait::pend();
    let source = Arc::new(WaitSource::at_line(h.fs_source(), 2, wait));
    let mut cfg = config();
    cfg.batch_size = 100;
    cfg.lease = LEASE;
    let deps = h.deps_with("runner-2", cfg, source, Arc::new(h.fs_sink()));
    let run = runner::run(&deps, record);
    tokio::pin!(run);
    tokio::select! {
        _ = &mut run => panic!("the runner must not finish before the read waits"),
        () = waiting.notified() => {}
    }
    h.storage
        .request_cancel(ACCOUNT, &op)
        .await
        .expect("request cancel");
    tokio::time::timeout(Duration::from_secs(5), &mut run)
        .await
        .expect("the cancel is seen while the covered rows are read again")
        .expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED", "{record:?}");
    assert_eq!(record.processed_item_count, 5);
    assert_eq!(h.count_items().await, 5);
}
