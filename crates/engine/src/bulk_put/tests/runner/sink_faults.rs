// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Renders through the real sink, then signals and pends forever once the
/// first report file is finished, modelling a crash between rendering and
/// the terminal transition: the file is on disk, the record is still
/// non-terminal, and the report records are still in the catalog.
pub(super) struct PendAfterRenderSink {
    pub(super) inner: FsSink,
    pub(super) notify: Arc<tokio::sync::Notify>,
}

struct PendAfterFinishWriter {
    inner: Box<dyn ReportWriter>,
    notify: Arc<tokio::sync::Notify>,
}

impl BulkSink for PendAfterRenderSink {
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
        let notify = Arc::clone(&self.notify);
        Box::pin(async move {
            let inner = fut.await?;
            Ok(Box::new(PendAfterFinishWriter { inner, notify }) as Box<dyn ReportWriter>)
        })
    }
}

impl ReportWriter for PendAfterFinishWriter {
    fn write_page(&mut self, records: Vec<Value>) -> BoxFuture<'_, Result<(), SinkError>> {
        self.inner.write_page(records)
    }

    fn finish(self: Box<Self>) -> BoxFuture<'static, Result<String, SinkError>> {
        let this = *self;
        Box::pin(async move {
            let _key = this.inner.finish().await?;
            this.notify.notify_one();
            std::future::pending::<()>().await;
            unreachable!("pending never resolves")
        })
    }

    fn object_index(&self) -> u32 {
        self.inner.object_index()
    }
}

/// Delegates rendering to the real sink, but the first `write_page` call
/// signals `reached` and then waits for `release` before writing, holding
/// the runner mid-render with its temp file already created. The runner
/// future stays alive, so a test can let the lease expire, drive a second
/// runner to a terminal state, and only then let the first runner's render
/// continue as a stale writer. The other wrappers pend inside `open` or
/// `finish`; the lease check under test sits between `write_page` and
/// `finish`, so this hook is the point that exercises it.
pub(super) struct PendInWritePageSink {
    pub(super) inner: FsSink,
    pub(super) reached: Arc<tokio::sync::Notify>,
    pub(super) release: Arc<tokio::sync::Notify>,
}

struct PendInWritePageWriter {
    inner: Box<dyn ReportWriter>,
    reached: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    fired: AtomicBool,
}

impl BulkSink for PendInWritePageSink {
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
        let reached = Arc::clone(&self.reached);
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            let inner = fut.await?;
            Ok(Box::new(PendInWritePageWriter {
                inner,
                reached,
                release,
                fired: AtomicBool::new(false),
            }) as Box<dyn ReportWriter>)
        })
    }
}

impl ReportWriter for PendInWritePageWriter {
    fn write_page(&mut self, records: Vec<Value>) -> BoxFuture<'_, Result<(), SinkError>> {
        Box::pin(async move {
            if !self.fired.swap(true, Ordering::SeqCst) {
                self.reached.notify_one();
                self.release.notified().await;
            }
            self.inner.write_page(records).await
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

/// Requests a cancel of the operation inside the first `BulkSink::open`,
/// then renders through the real sink. Rendering runs after the runner's
/// last batch boundary, so the cancel lands after every boundary check and
/// only the terminal transition can observe it.
pub(super) struct CancelOnOpenSink {
    pub(super) inner: FsSink,
    pub(super) storage: Arc<dyn StorageEngine>,
    pub(super) operation_id: String,
    pub(super) fired: Arc<AtomicBool>,
}

impl BulkSink for CancelOnOpenSink {
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
        let storage = Arc::clone(&self.storage);
        let operation = self.operation_id.clone();
        let fired = Arc::clone(&self.fired);
        Box::pin(async move {
            if !fired.swap(true, Ordering::SeqCst) {
                storage
                    .request_cancel(ACCOUNT, &operation)
                    .await
                    .expect("request cancel");
            }
            fut.await
        })
    }
}

// A regular file where a report-path directory
// component must be, and a directory the server may not write into, end the
// operation S3AccessDenied, the same class as the existing symlink and jail
// refusals; before this they ended InternalServerError. The location class
// for every other local-path fault is already S3AccessDenied.
#[tokio::test]
async fn a_regular_file_on_the_report_path_fails_with_s3_access_denied() {
    let h = Harness::new().await;
    // A regular file where the AWSDynamoDB directory component must be
    // created, under the output prefix.
    h.store.file(
        ACCOUNT,
        OUT_BUCKET,
        &format!("{OUT_PREFIX}/AWSDynamoDB"),
        b"not a directory",
    );
    // One failing row so an error report is rendered and the sink opens the
    // report path, hitting the regular file.
    let rows = [row(0), row_missing_sk(1), row(2)];
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(
        record.failure_code.as_deref(),
        Some("S3AccessDenied"),
        "a regular file on the report path is the caller's location fault"
    );
    assert_eq!(record.failure_message.as_deref(), Some("Access Denied"));
}

#[tokio::test]
async fn an_unwritable_report_directory_fails_with_s3_access_denied() {
    use std::os::unix::fs::PermissionsExt;
    let h = Harness::new().await;
    // The AWSDynamoDB component exists as a directory the server may not
    // write into, so the next component cannot be created.
    let awsddb = h
        .store
        .0
        .join(ACCOUNT)
        .join(OUT_BUCKET)
        .join(OUT_PREFIX)
        .join("AWSDynamoDB");
    std::fs::create_dir_all(&awsddb).unwrap();
    std::fs::set_permissions(&awsddb, std::fs::Permissions::from_mode(0o500)).unwrap();
    // Root ignores the mode bits; if a create still succeeds the guard does
    // not apply and the case cannot be driven here.
    let probe = awsddb.join(".probe");
    if std::fs::create_dir(&probe).is_ok() {
        let _ = std::fs::remove_dir(&probe);
        std::fs::set_permissions(&awsddb, std::fs::Permissions::from_mode(0o700)).unwrap();
        return;
    }

    let rows = [row(0), row_missing_sk(1), row(2)];
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    // Let the Root drop clean up the tree.
    std::fs::set_permissions(&awsddb, std::fs::Permissions::from_mode(0o700)).ok();
    assert_eq!(record.status, "FAILED");
    assert_eq!(
        record.failure_code.as_deref(),
        Some("S3AccessDenied"),
        "a report directory the server cannot write into is the caller's location fault"
    );
    assert_eq!(record.failure_message.as_deref(), Some("Access Denied"));
}
