// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use extenddb_storage::{BoxedFuture, InputFile};

use super::*;
use crate::bulk_put::source::{
    BucketRef, BulkSource, FsSink, InputCodec, LineReader, SourceError, SourceFile,
};

// A Start that replays a ClientToken is answered from the operation the
// token started, before any store call, the listing, the byte bound and the
// failed-start cap (measured on the service: the existing
// description with HTTP 200, or the mismatch error, and nothing else
// consulted).

const TOKEN: &str = "tok-replay";

/// A source every call of which fails as an unreachable store does.
struct Unreachable;

fn unreachable_error() -> SourceError {
    SourceError::Io(std::io::Error::other("connection refused"))
}

impl BulkSource for Unreachable {
    fn check_bucket(
        &self,
        _bucket: &BucketRef<'_>,
        _prefix: &str,
    ) -> BoxedFuture<'_, Result<(), SourceError>> {
        Box::pin(async { Err(unreachable_error()) })
    }

    fn list(
        &self,
        _bucket: &BucketRef<'_>,
        _prefix: &str,
        _max_files: u64,
    ) -> BoxedFuture<'_, Result<Vec<SourceFile>, SourceError>> {
        Box::pin(async { Err(unreachable_error()) })
    }

    fn open(
        &self,
        _bucket: &BucketRef<'_>,
        _file: &InputFile,
        _compression: InputCodec,
    ) -> BoxedFuture<'_, Result<Box<dyn LineReader>, SourceError>> {
        Box::pin(async { Err(unreachable_error()) })
    }

    fn etag(
        &self,
        _bucket: &BucketRef<'_>,
        _file: &InputFile,
    ) -> BoxedFuture<'_, Result<String, SourceError>> {
        Box::pin(async { Err(unreachable_error()) })
    }

    fn line_count(
        &self,
        _bucket: &BucketRef<'_>,
        _file: &InputFile,
        _compression: InputCodec,
    ) -> BoxedFuture<'_, Result<u64, SourceError>> {
        Box::pin(async { Err(unreachable_error()) })
    }
}

fn ctx_unreachable(h: &Harness) -> OperationContext {
    let mut ctx = base_ctx(h);
    let root = h.store.root();
    ctx.bulk = Some(Arc::new(BulkStore::with_parts(
        Arc::clone(&root),
        Arc::new(Unreachable),
        Arc::new(FsSink { root }),
    )));
    ctx
}

fn tokened() -> Value {
    with_top(start_body(), "ClientToken", json!(TOKEN))
}

#[tokio::test]
async fn a_replay_is_answered_while_every_store_call_fails() {
    let h = Harness::new().await;
    h.write_input("part-0.json", &file_of(&plain_rows(2)));
    let first = start_ok(&base_ctx(&h), tokened()).await;
    let unreachable = ctx_unreachable(&h);
    // The store is consulted for a request with no replay to answer.
    let error = start(&unreachable, start_body()).await.unwrap_err();
    assert!(
        matches!(error, DynamoDbError::InternalServerError(_)),
        "{error:?}"
    );
    let replay = start_ok(&unreachable, tokened()).await;
    assert_eq!(replay, first);
}

#[tokio::test]
async fn a_mismatched_replay_is_answered_while_every_store_call_fails() {
    let h = Harness::new().await;
    h.write_input("part-0.json", &file_of(&plain_rows(2)));
    start_ok(&base_ctx(&h), tokened()).await;
    let body = with_input(tokened(), "S3KeyPrefix", json!("other/"));
    match start(&ctx_unreachable(&h), body).await {
        Err(DynamoDbError::IdempotentParameterMismatchException(message)) => assert_eq!(
            message,
            "Specified idempotent token was already used with different request parameters \
             within the idempotency window"
        ),
        other => panic!("expected the mismatch error, got {other:?}"),
    }
}

#[tokio::test]
async fn a_replay_is_answered_after_the_prefix_grew_past_the_byte_bound() {
    let h = Harness::new().await;
    h.write_input("part-0.json", &file_of(&plain_rows(2)));
    let size = std::fs::metadata(h.store.0.join(ACCOUNT).join(IN_BUCKET).join("part-0.json"))
        .unwrap()
        .len();
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[("max_bulk_input_bytes", json!(size + 10))]),
    );
    let first = start_ok(&ctx, tokened()).await;
    h.write_input("part-1.json", &file_of(&plain_rows(2)));
    let fresh = expect_validation(start(&ctx, start_body()).await);
    assert!(
        fresh.starts_with("Input exceeds max_bulk_input_bytes"),
        "{fresh}"
    );
    let replay = start_ok(&ctx, tokened()).await;
    assert_eq!(replay["BulkOperationArn"], first["BulkOperationArn"]);
    assert_eq!(replay["TotalInputBytes"], json!(size));
}

#[tokio::test]
async fn a_replay_is_answered_at_the_failed_start_cap() {
    let h = Harness::new().await;
    h.write_input("part-0.json", &file_of(&plain_rows(2)));
    Harness::create_table(&h.storage, "second").await;
    let ctx = ctx_with_limits(
        &h,
        bulk_limits(&[("max_bulk_failed_starts_per_hour", json!(1))]),
    );
    let first = start_ok(&ctx, tokened()).await;
    // One terminal-born start reaches the cap. It names another table: the
    // operation just started keeps this one busy, and a busy table answers
    // busy rather than inserting the failed row.
    let missing = with_input(
        with_top(start_body(), "TableName", json!("second")),
        "S3Bucket",
        json!("absent-loads"),
    );
    let born = start_ok(&ctx, missing.clone()).await;
    assert_eq!(born["BulkOperationStatus"], json!("FAILED"));
    expect_throttling(start(&ctx, missing).await);
    // The input bucket is gone, so the bucket check of a new Start would
    // fail: on this table, which the first operation keeps busy, it would
    // answer busy, and on an idle one it would be refused at the cap. The
    // replay is answered from its row before the check.
    std::fs::remove_dir_all(h.store.0.join(ACCOUNT).join(IN_BUCKET)).unwrap();
    let replay = start_ok(&ctx, tokened()).await;
    assert_eq!(replay["BulkOperationArn"], first["BulkOperationArn"]);
    assert_eq!(replay["BulkOperationStatus"], first["BulkOperationStatus"]);
}
