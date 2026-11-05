// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A run that was cancelled, or lost its table, while it read input still
//! writes its reports and its manifest when the writes outlast a lease
//! renewal: the renewals during the writes act only on a lost lease.

use super::source_faults::{ActAtSource, LineAction};
use super::*;

const LEASE: Duration = Duration::from_millis(300);

/// Longer than one lease, so every write spans at least one renewal.
const SLOW: Duration = Duration::from_millis(800);

/// The filesystem sink with every report page write and the manifest
/// write answering after `SLOW`.
struct SlowSink {
    inner: FsSink,
}

struct SlowWriter {
    inner: Box<dyn ReportWriter>,
}

impl BulkSink for SlowSink {
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
        let opened = self.inner.open(at, prefix, operation_id, kind, owner, sse);
        Box::pin(async move {
            let inner = opened.await?;
            Ok(Box::new(SlowWriter { inner }) as Box<dyn ReportWriter>)
        })
    }

    fn write_manifest(
        &self,
        at: &BucketRef<'_>,
        prefix: &str,
        operation_id: &str,
        owner: &str,
        sse: &OutputSse,
        lines: Vec<Value>,
    ) -> BoxFuture<'_, Result<String, SinkError>> {
        let written = self
            .inner
            .write_manifest(at, prefix, operation_id, owner, sse, lines);
        Box::pin(async move {
            tokio::time::sleep(SLOW).await;
            written.await
        })
    }
}

impl ReportWriter for SlowWriter {
    fn write_page(&mut self, records: Vec<Value>) -> BoxFuture<'_, Result<(), SinkError>> {
        Box::pin(async move {
            tokio::time::sleep(SLOW).await;
            self.inner.write_page(records).await
        })
    }

    fn finish(self: Box<Self>) -> BoxFuture<'static, Result<String, SinkError>> {
        self.inner.finish()
    }

    fn object_index(&self) -> u32 {
        self.inner.object_index()
    }
}

#[derive(Clone, Copy)]
enum During {
    Cancel,
    DeleteTable,
}

/// Ten rows with row 2 missing its sort key, batch size 5; `during` runs
/// before line 7 is handed to the runner. Runs to the end.
async fn run_with(during: During) -> (Harness, String) {
    let h = Harness::new().await;
    let mut rows = plain_rows(10);
    rows[2] = row_missing_sk(2);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", LEASE).await;
    let storage = Arc::clone(&h.storage);
    let acted_on = op.clone();
    let action: LineAction = Arc::new(move || {
        let storage = Arc::clone(&storage);
        let op = acted_on.clone();
        Box::pin(async move {
            match during {
                During::Cancel => {
                    storage
                        .request_cancel(ACCOUNT, &op)
                        .await
                        .expect("request cancel");
                }
                During::DeleteTable => {
                    storage
                        .delete_table(
                            ACCOUNT,
                            DeleteTableInput {
                                table_name: TABLE.to_owned(),
                            },
                        )
                        .await
                        .expect("delete table");
                }
            }
        })
    });
    let source = Arc::new(ActAtSource {
        inner: h.fs_source(),
        at: 7,
        handed: Arc::new(AtomicU64::new(0)),
        fired: Arc::new(AtomicBool::new(false)),
        action,
    });
    let mut cfg = config();
    cfg.lease = LEASE;
    cfg.batch_size = 5;
    let sink = Arc::new(SlowSink { inner: h.fs_sink() });
    let deps = h.deps_with("runner-1", cfg, source, sink);
    tokio::time::timeout(Duration::from_secs(20), runner::run(&deps, record))
        .await
        .expect("the run ends")
        .expect("runner run");
    (h, op)
}

#[tokio::test]
async fn a_run_cancelled_while_it_read_writes_its_reports_and_manifest() {
    let (h, op) = run_with(During::Cancel).await;
    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED", "{record:?}");
    let report = h.read_report(&op, ReportKind::Error).expect("error report");
    assert_eq!(report.len(), 1);
    assert!(h.read_manifest(&op).is_some(), "the manifest is written");
}

#[tokio::test]
async fn a_run_whose_table_was_deleted_writes_its_reports_and_manifest() {
    let (h, op) = run_with(During::DeleteTable).await;
    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED", "{record:?}");
    assert_eq!(
        record.failure_code.as_deref(),
        Some("DynamoDBResourceNotFoundException")
    );
    let report = h.read_report(&op, ReportKind::Error).expect("error report");
    assert_eq!(report.len(), 1);
    assert!(h.read_manifest(&op).is_some(), "the manifest is written");
}
