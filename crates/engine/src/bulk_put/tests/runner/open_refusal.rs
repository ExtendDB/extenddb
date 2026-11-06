// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::bulk_put::source::StoreFault;

// An input file refused when the runner opens it. Measured on the service:
// an object the operation could not read before any of its rows was counted
// is `NOT_PROCESSED` in the manifest, with no processed range and its entity
// tag; an object refused after rows of it were counted is `NOT_AVAILABLE`.

/// Refuses every open of the input file at `path` with an I/O error of
/// `kind`; every other call goes to the real source, so the refused file can
/// still be hashed for the manifest.
struct RefuseAtOpenSource {
    inner: FsSource,
    path: &'static str,
    kind: std::io::ErrorKind,
}

impl BulkSource for RefuseAtOpenSource {
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
        if file.path == self.path {
            let kind = self.kind;
            return Box::pin(async move {
                Err(SourceError::Io(std::io::Error::new(
                    kind,
                    "input object refused at its open",
                )))
            });
        }
        self.inner.open(bucket, file, compression)
    }
}

fn status_of(line: &Value) -> &str {
    line["fileProcessingStatus"]
        .as_str()
        .expect("the manifest line carries a status")
}

#[tokio::test]
async fn a_file_refused_at_its_open_is_not_processed_and_keeps_its_entity_tag() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(5)));
    h.write_input("part-001.json", &file_of(&plain_rows(3)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let source = Arc::new(RefuseAtOpenSource {
        inner: h.fs_source(),
        path: "part-000.json",
        kind: std::io::ErrorKind::PermissionDenied,
    });
    let deps = h.deps_with("runner-1", config(), source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(record.failure_message.as_deref(), Some("Access Denied"));
    assert_eq!(record.processed_item_count, 0);
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(record.processed_input_bytes, 0);
    assert_eq!(h.count_items().await, 0);

    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(manifest.len(), 2);
    assert_eq!(
        status_of(&manifest[0]),
        "NOT_PROCESSED",
        "a file refused before any of its rows was counted is not processed"
    );
    assert_eq!(manifest[0]["processedRanges"], json!([]));
    assert_eq!(manifest[0]["etag"], json!(h.input_md5("part-000.json")));
    assert_eq!(status_of(&manifest[1]), "NOT_PROCESSED");
    assert_eq!(manifest[1]["etag"], json!(h.input_md5("part-001.json")));
}

#[tokio::test]
async fn a_file_deleted_before_its_open_is_not_processed_with_no_entity_tag() {
    // The first file is read to its end; the second is gone by the time the
    // runner reaches it. None of the second file's rows was counted, so it is
    // NOT_PROCESSED, and since it cannot be hashed its line carries an empty
    // tag rather than the render failing and dropping the manifest.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(4)));
    h.write_input("part-001.json", &file_of(&plain_rows(3)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    std::fs::remove_file(
        h.store
            .0
            .join(ACCOUNT)
            .join(IN_BUCKET)
            .join("part-001.json"),
    )
    .unwrap();
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-1", config()), record)
        .await
        .expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchKey"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("The specified key does not exist.")
    );
    assert_eq!(
        record.processed_item_count, 4,
        "the first file's rows stand"
    );
    assert_eq!(h.count_items().await, 4);

    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(manifest.len(), 2);
    assert_eq!(status_of(&manifest[0]), "FULLY_PROCESSED");
    assert_eq!(manifest[0]["processedRanges"], json!([[0, 3]]));
    assert_eq!(status_of(&manifest[1]), "NOT_PROCESSED");
    assert_eq!(manifest[1]["processedRanges"], json!([]));
    assert_eq!(manifest[1]["etag"], json!(""));
}

#[tokio::test]
async fn a_file_refused_at_a_resumed_open_after_rows_of_it_were_counted_is_not_available() {
    // The first claim checkpoints five rows of the file and dies; the file is
    // then deleted. The resumed claim's open fails, after rows of the file
    // were counted, so the file is NOT_AVAILABLE.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(12)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;

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
    assert_eq!(h.record(&op).await.cursor_row_index, 5);

    std::fs::remove_file(
        h.store
            .0
            .join(ACCOUNT)
            .join(IN_BUCKET)
            .join("part-000.json"),
    )
    .unwrap();
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-2", config()), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchKey"));
    assert_eq!(
        record.processed_item_count, 5,
        "the checkpointed rows stand"
    );

    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(manifest.len(), 1);
    assert_eq!(status_of(&manifest[0]), "NOT_AVAILABLE");
    assert_eq!(manifest[0]["processedRanges"], json!([]));
    assert_eq!(manifest[0]["etag"], json!(""));
}

/// Answers the open of the input file at `path` with the store fault the S3
/// store renders for a GET of the first range (`after_first_byte` false);
/// every other call goes to the real source.
struct StoreFaultAtOpenSource {
    inner: FsSource,
    path: &'static str,
}

impl BulkSource for StoreFaultAtOpenSource {
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
        if file.path == self.path {
            return Box::pin(async {
                Err(SourceError::Store(StoreFault {
                    code: "S3NoSuchKey",
                    message: S3_GONE.to_owned(),
                    after_first_byte: false,
                }))
            });
        }
        self.inner.open(bucket, file, compression)
    }
}

const S3_GONE: &str = "The specified key does not exist. (Service: Amazon S3; Status Code: 404; \
                       Error Code: NoSuchKey; Request ID: 4KQS8G3V7XJ2M1PA; S3 Extended Request \
                       ID: null; Proxy: null)";

#[tokio::test]
async fn a_store_fault_at_a_resumed_open_after_rows_of_it_were_counted_is_not_available() {
    // As above, with the fault the S3 store renders for the first range of
    // the resumed open: a GET that reports itself before the first byte.
    // Rows of the file were counted by the first claim, so the fault is
    // after the file's first byte and the file is NOT_AVAILABLE.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(12)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;

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
    assert_eq!(h.record(&op).await.cursor_row_index, 5);

    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let source = Arc::new(StoreFaultAtOpenSource {
        inner: h.fs_source(),
        path: "part-000.json",
    });
    let deps = h.deps_with("runner-2", config(), source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchKey"));
    assert_eq!(record.failure_message.as_deref(), Some(S3_GONE));
    assert_eq!(
        record.processed_item_count, 5,
        "the checkpointed rows stand"
    );

    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(manifest.len(), 1);
    assert_eq!(status_of(&manifest[0]), "NOT_AVAILABLE");
    assert_eq!(manifest[0]["processedRanges"], json!([]));
    assert_eq!(manifest[0]["etag"], json!(h.input_md5("part-000.json")));
}
