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
    let replay = start_ok(&ctx_unreachable(&h), tokened()).await;
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
