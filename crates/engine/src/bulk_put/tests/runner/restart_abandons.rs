// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// A file the runner abandoned keeps its outcome across a restart. The
// checkpoint that moves the cursor past an abandoned file stores the
// abandon on the file's record entry, so a later claim knows the file's
// last counted row and whether the abandon ends the operation `FAILED`,
// whatever the file's size and whatever report kinds were requested.

const ITEMS_FAILED: &str = "Some of the items failed validation checks and were not ingested";

/// The first claim's source: answers `SourceError::Overwritten` at the open
/// of `overwritten` and hands out the other files' lines until `after` of
/// them were handed, then pends. Dropping the runner at that point models a
/// crash after the checkpoint that passed the overwritten file.
struct OverwrittenThenPend {
    pend: PendAfterSource,
    overwritten: &'static str,
}

impl BulkSource for OverwrittenThenPend {
    fn check_bucket(
        &self,
        bucket: &BucketRef<'_>,
        prefix: &str,
    ) -> BoxFuture<'_, Result<(), SourceError>> {
        self.pend.check_bucket(bucket, prefix)
    }

    fn etag(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
    ) -> BoxFuture<'_, Result<String, SourceError>> {
        self.pend.etag(bucket, file)
    }

    fn line_count(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<u64, SourceError>> {
        self.pend.line_count(bucket, file, compression)
    }

    fn list(
        &self,
        bucket: &BucketRef<'_>,
        prefix: &str,
        max_files: u64,
    ) -> BoxFuture<'_, Result<Vec<SourceFile>, SourceError>> {
        self.pend.list(bucket, prefix, max_files)
    }

    fn open(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<Box<dyn LineReader>, SourceError>> {
        if file.path == self.overwritten {
            return Box::pin(async { Err(SourceError::Overwritten) });
        }
        self.pend.open(bucket, file, compression)
    }
}

fn pend_after(h: &Harness, after: u64) -> (PendAfterSource, Arc<tokio::sync::Notify>) {
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = PendAfterSource {
        inner: h.fs_source(),
        after,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    };
    (source, notify)
}

/// Run the first claim with checkpoints every 5 rows and a 100 ms lease
/// until `notify` fires, drop it there as a crash would, and wait for the
/// lease to run out. Returns the record as the crash left it.
async fn crash_first_claim(
    h: &Harness,
    op: &str,
    source: Arc<dyn BulkSource>,
    notify: &tokio::sync::Notify,
) -> BulkOperationRecord {
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let mut cfg = config();
    cfg.batch_size = 5;
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
    h.record(op).await
}

/// Claim the operation again with the plain source and run it to its end.
async fn resume_to_end(h: &Harness, op: &str) -> BulkOperationRecord {
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let deps = h.deps_with(
        "runner-2",
        config(),
        Arc::new(h.fs_source()),
        Arc::new(h.fs_sink()),
    );
    runner::run(&deps, record).await.expect("resumed run");
    h.record(op).await
}

#[tokio::test]
async fn an_object_listed_empty_and_overwritten_fails_the_operation_after_a_restart() {
    let h = Harness::new().await;
    h.write_input("part-000.json", b"");
    h.write_input("part-001.json", &file_of(&plain_rows(12)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let (pend, notify) = pend_after(&h, 7);
    let source = Arc::new(OverwrittenThenPend {
        pend,
        overwritten: "part-000.json",
    });
    let crashed = crash_first_claim(&h, &op, source, &notify).await;
    assert_eq!(
        crashed.cursor_file_index, 1,
        "the checkpoint passed the file"
    );
    assert_eq!(crashed.cursor_row_index, 5);

    let record = resume_to_end(&h, &op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(record.failure_message.as_deref(), Some(ITEMS_FAILED));
    assert_eq!(record.processed_item_count, 1 + 12);
    assert_eq!(record.failed_item_count, 1);
    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(
        manifest[0]["fileProcessingStatus"],
        json!("PARTIALLY_PROCESSED")
    );
    assert_eq!(manifest[0]["processedRanges"], json!([]));
    assert_eq!(
        manifest[1]["fileProcessingStatus"],
        json!("FULLY_PROCESSED")
    );
}

#[tokio::test]
async fn an_empty_object_refused_as_not_compressed_fails_the_operation_after_a_restart() {
    let h = Harness::new().await;
    h.write_input("part-000.json.gz", b"");
    h.write_input("part-001.json.gz", &gzip_bytes(&file_of(&plain_rows(12))));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(true), both_reports()).await;
    let (pend, notify) = pend_after(&h, 7);
    let crashed = crash_first_claim(&h, &op, Arc::new(pend), &notify).await;
    assert_eq!(
        crashed.cursor_file_index, 1,
        "the checkpoint passed the file"
    );

    let record = resume_to_end(&h, &op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(record.failure_message.as_deref(), Some(ITEMS_FAILED));
    assert_eq!(record.processed_item_count, 1 + 12);
    assert_eq!(record.failed_item_count, 1);
    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(
        manifest[0]["fileProcessingStatus"],
        json!("PARTIALLY_PROCESSED")
    );
    assert_eq!(manifest[0]["processedRanges"], json!([]));
}

#[tokio::test]
async fn an_abandon_keeps_its_manifest_line_after_a_restart_without_an_error_report() {
    let h = Harness::new().await;
    let bad_item = r#"{"Item": "pk"}"#.to_owned();
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row(1), bad_item, row(3), row(4)]),
    );
    h.write_input("part-001.json", &file_of(&plain_rows(12)));
    let files = h.enumerate().await;
    // Only condition records are requested, so the abandon's error record
    // is written nowhere, here or in a report.
    let op = h
        .start(
            files,
            input_config(false),
            output_config(Some(OUT_BUCKET), &["CONDITION_CHECK_FAILURE"]),
        )
        .await;
    let (pend, notify) = pend_after(&h, 9);
    let crashed = crash_first_claim(&h, &op, Arc::new(pend), &notify).await;
    assert_eq!(
        crashed.cursor_file_index, 1,
        "the checkpoint passed the file"
    );

    let record = resume_to_end(&h, &op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 3 + 12);
    assert_eq!(record.failed_item_count, 1);
    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(
        manifest[0]["fileProcessingStatus"],
        json!("PARTIALLY_PROCESSED")
    );
    assert_eq!(manifest[0]["processedRanges"], json!([[0, 1]]));
    assert_eq!(
        manifest[1]["fileProcessingStatus"],
        json!("FULLY_PROCESSED")
    );
    assert_eq!(manifest[1]["processedRanges"], json!([[0, 11]]));
}

#[tokio::test]
async fn the_manifest_names_an_overwritten_object_by_its_listed_entity_tag_or_by_none() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(3)));
    h.write_input("part-001.json", &file_of(&plain_rows(4)));
    let mut files = h.enumerate().await;
    // The first object's listing gave an entity tag and the second's gave
    // none, as an S3-compatible service may answer. Both objects are
    // replaced before their read, so the object now at each key is not the
    // one the operation listed.
    files[0].etag = Some("0123456789abcdef0123456789abcdef".to_owned());
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let source = Arc::new(BothOverwritten {
        inner: h.fs_source(),
    });
    let deps = h.deps_with("runner-1", config(), source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(record.processed_item_count, 2);
    assert_eq!(record.failed_item_count, 2);
    let manifest = h.read_manifest(&op).expect("the manifest is written");
    assert_eq!(
        manifest[0]["etag"],
        json!("0123456789abcdef0123456789abcdef"),
        "the listed tag, not {REPLACEMENT_ETAG}"
    );
    assert_eq!(
        manifest[1]["etag"],
        json!(""),
        "no tag, not {REPLACEMENT_ETAG}"
    );
}

/// The entity tag of the object now at each key, the replacement.
const REPLACEMENT_ETAG: &str = "ffffffffffffffffffffffffffffffff";

/// Answers `SourceError::Overwritten` at every open, and the replacement's
/// tag for any request for an entity tag.
struct BothOverwritten {
    inner: FsSource,
}

impl BulkSource for BothOverwritten {
    fn check_bucket(
        &self,
        bucket: &BucketRef<'_>,
        prefix: &str,
    ) -> BoxFuture<'_, Result<(), SourceError>> {
        self.inner.check_bucket(bucket, prefix)
    }

    fn etag(
        &self,
        _bucket: &BucketRef<'_>,
        _file: &InputFile,
    ) -> BoxFuture<'_, Result<String, SourceError>> {
        Box::pin(async { Ok(REPLACEMENT_ETAG.to_owned()) })
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
        _bucket: &BucketRef<'_>,
        _file: &InputFile,
        _compression: InputCodec,
    ) -> BoxFuture<'_, Result<Box<dyn LineReader>, SourceError>> {
        Box::pin(async { Err(SourceError::Overwritten) })
    }
}
