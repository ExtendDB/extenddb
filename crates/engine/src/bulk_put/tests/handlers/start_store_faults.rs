// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Start against a store whose enumeration fails after the bucket check
//! passed: a fault the store rendered on a later listing page (the bucket
//! removed or access lost in between) starts the operation terminal, as
//! the check would have; any other enumeration failure stays a request
//! error. The same holds for the Start-time check of an output SSE-KMS
//! key.

use extenddb_storage::{BoxedFuture, InputFile};

use super::*;
use crate::bulk_put::source::{
    BucketRef, BulkSink, BulkSource, FsSink, FsSource, InputCodec, LineReader, OutputSse,
    ReportWriter, SinkError, SourceError, SourceFile, StoreFault,
};

const LATER_PAGE_MESSAGE: &str = "The specified bucket does not exist (Service: Amazon S3; \
     Status Code: 404; Error Code: NoSuchBucket; Request ID: R1; S3 Extended Request ID: X1; \
     Proxy: null)";

/// A source whose bucket check passes and whose listing fails with
/// `fault`, or with an I/O error when `fault` is `None`.
struct ListingFails {
    fault: Option<StoreFault>,
}

impl BulkSource for ListingFails {
    fn check_bucket(
        &self,
        _bucket: &BucketRef<'_>,
        _prefix: &str,
    ) -> BoxedFuture<'_, Result<(), SourceError>> {
        Box::pin(async { Ok(()) })
    }

    fn list(
        &self,
        _bucket: &BucketRef<'_>,
        _prefix: &str,
        _max_files: u64,
    ) -> BoxedFuture<'_, Result<Vec<SourceFile>, SourceError>> {
        let error = match &self.fault {
            Some(fault) => SourceError::Store(fault.clone()),
            None => SourceError::Io(std::io::Error::other("connection reset")),
        };
        Box::pin(async move { Err(error) })
    }

    fn open(
        &self,
        _bucket: &BucketRef<'_>,
        _file: &InputFile,
        _compression: InputCodec,
    ) -> BoxedFuture<'_, Result<Box<dyn LineReader>, SourceError>> {
        Box::pin(async { Err(SourceError::Io(std::io::Error::other("not read"))) })
    }

    fn etag(
        &self,
        _bucket: &BucketRef<'_>,
        _file: &InputFile,
    ) -> BoxedFuture<'_, Result<String, SourceError>> {
        Box::pin(async { Err(SourceError::Io(std::io::Error::other("not read"))) })
    }

    fn line_count(
        &self,
        _bucket: &BucketRef<'_>,
        _file: &InputFile,
        _compression: InputCodec,
    ) -> BoxedFuture<'_, Result<u64, SourceError>> {
        Box::pin(async { Err(SourceError::Io(std::io::Error::other("not read"))) })
    }
}

pub(super) fn ctx_with_listing(h: &Harness, fault: Option<StoreFault>) -> OperationContext {
    let mut ctx = base_ctx(h);
    let root = h.store.root();
    ctx.bulk = Some(Arc::new(BulkStore::with_parts(
        Arc::clone(&root),
        Arc::new(ListingFails { fault }),
        Arc::new(FsSink { root }),
    )));
    ctx
}

#[tokio::test]
async fn a_store_fault_on_a_later_listing_page_starts_the_operation_terminal_failed() {
    let h = Harness::new().await;
    let ctx = ctx_with_listing(
        &h,
        Some(StoreFault {
            code: "S3NoSuchBucket",
            message: LATER_PAGE_MESSAGE.to_owned(),
            after_first_byte: false,
        }),
    );
    let body = with_top(start_body(), "ClientToken", json!("tok-later-page"));
    let description = start_ok(&ctx, body.clone()).await;
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(description["FailureCode"], json!("S3NoSuchBucket"));
    assert_eq!(description["FailureMessage"], json!(LATER_PAGE_MESSAGE));
    assert_eq!(description["ProcessedItemCount"], json!(0));
    assert!(description.get("TotalInputBytes").is_none());
    assert_eq!(description["EndTime"], description["StartTime"]);
    let replay = start_ok(&ctx, body).await;
    assert_eq!(replay["BulkOperationArn"], description["BulkOperationArn"]);
}

#[tokio::test]
async fn another_enumeration_failure_stays_an_internal_request_error() {
    let h = Harness::new().await;
    let ctx = ctx_with_listing(&h, None);
    let error = start(&ctx, start_body()).await.unwrap_err();
    assert!(
        matches!(&error, DynamoDbError::InternalServerError(message) if message == "Internal server error"),
        "{error:?}"
    );
}

/// A sink whose Start-time encryption check fails with `fault`, or with an
/// I/O error when `fault` is `None`; it records the key it was asked about.
struct EncryptionFails {
    fault: Option<StoreFault>,
    asked: std::sync::Mutex<Vec<(String, String, OutputSse)>>,
}

impl BulkSink for EncryptionFails {
    fn check_location(
        &self,
        _bucket: &BucketRef<'_>,
        _key: &str,
        _sse: &OutputSse,
    ) -> BoxedFuture<'_, Result<(), SinkError>> {
        Box::pin(async { Ok(()) })
    }

    fn check_encryption(
        &self,
        bucket: &BucketRef<'_>,
        key: &str,
        sse: &OutputSse,
    ) -> BoxedFuture<'_, Result<(), SinkError>> {
        self.asked
            .lock()
            .unwrap()
            .push((bucket.bucket.to_owned(), key.to_owned(), sse.clone()));
        let error = match &self.fault {
            Some(fault) => SinkError::Store(fault.clone()),
            None => SinkError::Io(std::io::Error::other("connection reset")),
        };
        Box::pin(async move { Err(error) })
    }

    fn open(
        &self,
        _bucket: &BucketRef<'_>,
        _prefix: &str,
        _operation_id: &str,
        _kind: extenddb_storage::ReportKind,
        _owner: &str,
        _sse: &OutputSse,
    ) -> BoxedFuture<'_, Result<Box<dyn ReportWriter>, SinkError>> {
        Box::pin(async { Err(SinkError::Io(std::io::Error::other("not written"))) })
    }

    fn write_manifest(
        &self,
        _bucket: &BucketRef<'_>,
        _prefix: &str,
        _operation_id: &str,
        _owner: &str,
        _sse: &OutputSse,
        _lines: Vec<Value>,
    ) -> BoxedFuture<'_, Result<String, SinkError>> {
        Box::pin(async { Err(SinkError::Io(std::io::Error::other("not written"))) })
    }
}

#[tokio::test]
async fn an_output_kms_key_the_store_refuses_starts_the_operation_terminal_failed() {
    let h = Harness::new().await;
    let root = h.store.root();
    let message = "arn:aws:kms:us-east-1:111122223333:key/k1 is disabled. (Service: Amazon S3; \
                   Status Code: 400; Error Code: KMS.DisabledException; Request ID: R1; S3 \
                   Extended Request ID: X1; Proxy: null)";
    let sink = Arc::new(EncryptionFails {
        fault: Some(StoreFault {
            code: "KmsDisabledException",
            message: message.to_owned(),
            after_first_byte: false,
        }),
        asked: std::sync::Mutex::new(Vec::new()),
    });
    let mut ctx = base_ctx(&h);
    ctx.bulk = Some(Arc::new(BulkStore::with_parts(
        Arc::clone(&root),
        Arc::new(FsSource {
            root: Arc::clone(&root),
        }),
        Arc::clone(&sink) as Arc<dyn BulkSink>,
    )));
    let body = with_output(
        with_output(start_body(), "S3SseAlgorithm", json!("KMS")),
        "S3SseKmsKeyId",
        json!("arn:aws:kms:us-east-1:111122223333:key/k1"),
    );
    let description = start_ok(&ctx, body).await;
    assert_eq!(description["BulkOperationStatus"], json!("FAILED"));
    assert_eq!(description["FailureCode"], json!("KmsDisabledException"));
    assert_eq!(description["FailureMessage"], json!(message));
    assert!(description.get("TotalInputBytes").is_none());
    assert!(description.get("ProcessedInputBytes").is_none());
    let asked = sink.asked.lock().unwrap().clone();
    assert_eq!(asked.len(), 1);
    let operation_id = description["BulkOperationArn"]
        .as_str()
        .and_then(|arn| arn.rsplit('/').next())
        .unwrap()
        .to_owned();
    assert!(
        asked[0].1.ends_with(&format!(
            "{ACCOUNT}:{operation_id}-0/output/errors/output-00000.json.gz"
        )),
        "{asked:?}"
    );
    assert_eq!(
        asked[0].2,
        OutputSse::Kms {
            key_id: Some("arn:aws:kms:us-east-1:111122223333:key/k1".to_owned())
        }
    );

    // A failure of the check that the store did not render is the
    // server's: the request answers InternalServerError and no operation
    // is created.
    let mut ctx = base_ctx(&h);
    ctx.bulk = Some(Arc::new(BulkStore::with_parts(
        Arc::clone(&root),
        Arc::new(FsSource {
            root: Arc::clone(&root),
        }),
        Arc::new(EncryptionFails {
            fault: None,
            asked: std::sync::Mutex::new(Vec::new()),
        }),
    )));
    let error = start(&ctx, start_body()).await.unwrap_err();
    assert!(
        matches!(&error, DynamoDbError::InternalServerError(_)),
        "{error:?}"
    );
}
