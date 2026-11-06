// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Report object rotation through the runner: a run whose error report
//! spans more than one render page rolls to `output-00001.json.gz` once
//! the compressed bytes of the first object reach the threshold,
//! and a resumed render after a crash mid-roll leaves exactly the expected
//! objects and no temp file.

use super::*;

/// Renders through the real sink with the writer's rotation threshold
/// lowered to `threshold` bytes, so a run of a few thousand records rolls.
/// With `pend_after_page` set, the writer signals `reached` after that many
/// `write_page` calls have completed and then waits for `release`, holding
/// the runner mid-render with its temp files in place.
pub(super) struct RollingSink {
    pub(super) inner: FsSink,
    pub(super) threshold: u64,
    pub(super) pend_after_page: Option<usize>,
    pub(super) reached: Arc<tokio::sync::Notify>,
    pub(super) release: Arc<tokio::sync::Notify>,
}

impl RollingSink {
    pub(super) fn new(inner: FsSink, threshold: u64) -> Self {
        Self {
            inner,
            threshold,
            pend_after_page: None,
            reached: Arc::new(tokio::sync::Notify::new()),
            release: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

struct RollingWriter {
    inner: Box<dyn ReportWriter>,
    pages: usize,
    pend_after_page: Option<usize>,
    reached: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

impl BulkSink for RollingSink {
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
        let fut = self.inner.open(at, prefix, operation_id, kind, owner, sse);
        let threshold = self.threshold;
        let pend_after_page = self.pend_after_page;
        let reached = Arc::clone(&self.reached);
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            let mut inner = fut.await?;
            inner.set_rotation_threshold(threshold);
            Ok(Box::new(RollingWriter {
                inner,
                pages: 0,
                pend_after_page,
                reached,
                release,
            }) as Box<dyn ReportWriter>)
        })
    }
}

impl ReportWriter for RollingWriter {
    fn write_page(&mut self, records: Vec<Value>) -> BoxFuture<'_, Result<(), SinkError>> {
        Box::pin(async move {
            self.inner.write_page(records).await?;
            self.pages += 1;
            if self.pend_after_page == Some(self.pages) {
                self.reached.notify_one();
                self.release.notified().await;
            }
            Ok(())
        })
    }

    fn finish(self: Box<Self>) -> BoxFuture<'static, Result<String, SinkError>> {
        let this = *self;
        this.inner.finish()
    }

    fn object_index(&self) -> u32 {
        self.inner.object_index()
    }
}

/// Rows that each produce one item-level error record.
fn error_rows(n: usize) -> Vec<String> {
    (0..n).map(row_missing_sk).collect()
}

/// The render pages records out of storage 1,000 at a time, so 1,500
/// error records arrive as a page of 1,000 and a page of 500. With the
/// threshold at one byte the first page trips it and the second page
/// opens the next object.
#[tokio::test]
async fn an_error_report_past_the_threshold_spans_numbered_objects() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&error_rows(1500)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let sink = Arc::new(RollingSink::new(h.fs_sink(), 1));
    let deps = h.deps_with("runner-1", config(), Arc::new(h.fs_source()), sink);
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.failed_item_count, 1500);
    assert_eq!(
        h.report_objects(&op, ReportKind::Error),
        vec!["output-00000.json.gz", "output-00001.json.gz"],
        "two objects and nothing else under the kind directory"
    );
    let report = h
        .read_report(&op, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(
        report.len(),
        1500,
        "every record lands in exactly one object"
    );
    let dir = h.report_path(&op, ReportKind::Error);
    let dir = dir.parent().unwrap();
    let first = gunzip_lines(&std::fs::read(dir.join("output-00000.json.gz")).unwrap());
    let second = gunzip_lines(&std::fs::read(dir.join("output-00001.json.gz")).unwrap());
    assert_eq!(
        first.len(),
        1000,
        "the page that tripped the threshold stays in the first object"
    );
    assert_eq!(second.len(), 500);
    assert!(
        report
            .iter()
            .all(|r| r["errorMessages"] == json!([MISSING_KEY])),
        "every record is the missing-key fault"
    );
    let indexes: Vec<i64> = report
        .iter()
        .map(|r| r["itemLocation"]["itemIndex"].as_i64().unwrap())
        .collect();
    assert_eq!(
        indexes,
        (0..1500).collect::<Vec<i64>>(),
        "records keep their stored order across the objects"
    );
    assert!(
        std::fs::read_dir(dir).unwrap().all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".tmp-")),
        "no temp file remains"
    );
}

/// The first runner rolls, writes its second page into the second
/// object's temp, and is frozen there, as a paused process is. The next
/// owner's open sweeps both temps, its render writes both objects, and the
/// stale runner, once released, observes the lost lease and installs
/// nothing.
#[tokio::test]
async fn a_resumed_render_after_a_crash_mid_roll_leaves_exactly_the_expected_objects() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&error_rows(1500)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    let mut sink = RollingSink::new(h.fs_sink(), 1);
    sink.pend_after_page = Some(2);
    let reached = Arc::clone(&sink.reached);
    let release = Arc::clone(&sink.release);
    // Not polled once its page is written, as a paused process is: a
    // runner that is polled keeps its lease while a write waits. Its own
    // lease is 60 seconds, so no renewal is due while the page waits and
    // none is under way when polling stops; a renewal stopped inside a
    // storage call could hold the engine's write lock, and the next claim
    // would wait for it without end. Its checkpoints ask the store for
    // 100 ms, so the store's lease runs out behind it.
    let deps = h.with_short_store_lease(h.deps_with(
        "runner-1",
        config(),
        Arc::new(h.fs_source()),
        Arc::new(sink),
    ));
    let stale = runner::run(&deps, record);
    tokio::pin!(stale);
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::select! {
            _ = &mut stale => panic!("the first runner must not finish before its second page"),
            () = reached.notified() => {}
        }
    })
    .await
    .expect("the first runner must reach its second page");

    let dir = h.report_path(&op, ReportKind::Error);
    let dir = dir.parent().unwrap().to_path_buf();
    let temps: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".tmp-"))
        .collect();
    assert_eq!(
        temps.len(),
        2,
        "the first object's closed temp and the second object's open temp: {temps:?}"
    );
    assert!(
        h.report_objects(&op, ReportKind::Error).is_empty(),
        "nothing installed yet"
    );

    // The lease expires; the next owner claims, renders both objects, and
    // finishes.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let sink = Arc::new(RollingSink::new(h.fs_sink(), 1));
    let deps = h.deps_with("runner-2", config(), Arc::new(h.fs_source()), sink);
    runner::run(&deps, record).await.expect("resumed run");
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");

    let names = |dir: &std::path::Path| {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    };
    assert_eq!(
        names(&dir),
        vec!["output-00000.json.gz", "output-00001.json.gz"],
        "exactly the expected objects and no temp"
    );
    assert_eq!(h.read_report(&op, ReportKind::Error).unwrap().len(), 1500);

    // Released, the stale runner re-reads the lease, finds it lost, drops
    // its writer, and leaves the directory as the new owner wrote it.
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(10), stale)
        .await
        .expect("the stale runner must stop once released")
        .expect("a lost lease is a clean stop, not an error");
    assert_eq!(
        names(&dir),
        vec!["output-00000.json.gz", "output-00001.json.gz"],
        "the stale runner installed nothing and left no temp"
    );
    assert_eq!(h.read_report(&op, ReportKind::Error).unwrap().len(), 1500);
}
