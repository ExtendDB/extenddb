// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use extenddb_storage::{BoxedFuture, InputFile};

use super::*;
use crate::bulk_put::source::{
    BucketRef, BulkSource, FsSink, InputCodec, LineReader, SourceError, SourceFile,
};

// The S3KeyPrefix path rules belong to the filesystem store, which joins a
// prefix onto a directory path. On the S3 store every such prefix is a valid
// key prefix; the service's length rule applies on both stores.

/// A source with an existing input bucket that lists nothing.
struct EmptyBucket;

fn not_read() -> SourceError {
    SourceError::Io(std::io::Error::other("not read"))
}

impl BulkSource for EmptyBucket {
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
        Box::pin(async { Ok(Vec::new()) })
    }

    fn open(
        &self,
        _bucket: &BucketRef<'_>,
        _file: &InputFile,
        _compression: InputCodec,
    ) -> BoxedFuture<'_, Result<Box<dyn LineReader>, SourceError>> {
        Box::pin(async { Err(not_read()) })
    }

    fn etag(
        &self,
        _bucket: &BucketRef<'_>,
        _file: &InputFile,
    ) -> BoxedFuture<'_, Result<String, SourceError>> {
        Box::pin(async { Err(not_read()) })
    }

    fn line_count(
        &self,
        _bucket: &BucketRef<'_>,
        _file: &InputFile,
        _compression: InputCodec,
    ) -> BoxedFuture<'_, Result<u64, SourceError>> {
        Box::pin(async { Err(not_read()) })
    }
}

fn s3_ctx(h: &Harness) -> OperationContext {
    let mut ctx = base_ctx(h);
    ctx.bulk = Some(Arc::new(BulkStore::s3_with_parts(
        Arc::new(EmptyBucket),
        Arc::new(FsSink {
            root: h.store.root(),
        }),
    )));
    ctx
}

/// Bodies whose one prefix breaks one path rule.
fn path_rule_cases() -> Vec<(&'static str, Value)> {
    vec![
        (
            "input ..",
            with_input(start_body(), "S3KeyPrefix", json!("s0/../s0/")),
        ),
        (
            "input backslash",
            with_input(start_body(), "S3KeyPrefix", json!("s0\\x/")),
        ),
        (
            "input control",
            with_input(start_body(), "S3KeyPrefix", json!("s0\u{1}/")),
        ),
        (
            "output leading /",
            with_output(start_body(), "S3KeyPrefix", json!("/out-lead/")),
        ),
        (
            "output ..",
            with_output(start_body(), "S3KeyPrefix", json!("out/../up/")),
        ),
    ]
}

#[tokio::test]
async fn the_s3_store_accepts_prefixes_the_path_rules_refuse() {
    for (case, body) in path_rule_cases() {
        // A table admits one operation at a time, so each case gets its own.
        let h = Harness::new().await;
        let description = start_ok(&s3_ctx(&h), body.clone()).await;
        assert_eq!(
            description["BulkOperationStatus"],
            json!("IN_PROGRESS"),
            "{case}"
        );
        for config in ["InputS3Configuration", "OutputS3Configuration"] {
            assert_eq!(
                description[config]["S3KeyPrefix"], body[config]["S3KeyPrefix"],
                "{case}: the prefix is kept as given"
            );
        }
    }
}

#[tokio::test]
async fn the_filesystem_store_still_refuses_them() {
    let h = Harness::new().await;
    for (case, body) in path_rule_cases() {
        let message = expect_validation(start(&base_ctx(&h), body).await);
        assert_eq!(message, BAD_PREFIX, "{case}");
    }
}

#[tokio::test]
async fn the_length_rule_holds_on_the_s3_store() {
    let h = Harness::new().await;
    let long = "p".repeat(1025);
    let body = with_input(start_body(), "S3KeyPrefix", json!(long));
    let message = expect_validation(start(&s3_ctx(&h), body).await);
    assert!(message.contains("s3KeyPrefix"), "{message}");
}

fn s3_endpoint_ctx(h: &Harness) -> OperationContext {
    let mut ctx = base_ctx(h);
    ctx.bulk = Some(Arc::new(BulkStore::s3_endpoint_with_parts(
        Arc::new(EmptyBucket),
        Arc::new(FsSink {
            root: h.store.root(),
        }),
    )));
    ctx
}

/// Bodies whose one prefix has more `..` segments than segments before
/// them, so resolving it goes above the bucket.
fn climb_cases() -> Vec<(&'static str, Value)> {
    vec![
        (
            "input ../",
            with_input(start_body(), "S3KeyPrefix", json!("../victim/")),
        ),
        (
            "input a/../../",
            with_input(start_body(), "S3KeyPrefix", json!("a/../../victim/cc/")),
        ),
        (
            "output ../",
            with_output(start_body(), "S3KeyPrefix", json!("../victim/x")),
        ),
        (
            "output ./ then ..",
            with_output(start_body(), "S3KeyPrefix", json!("./../victim")),
        ),
    ]
}

#[tokio::test]
async fn an_s3_compatible_endpoint_refuses_a_prefix_that_climbs_above_the_bucket() {
    let h = Harness::new().await;
    for (case, body) in climb_cases() {
        let message = expect_validation(start(&s3_endpoint_ctx(&h), body).await);
        assert_eq!(message, BAD_PREFIX, "{case}");
    }
}

#[tokio::test]
async fn aws_s3_keeps_a_climbing_prefix_as_given() {
    for (case, body) in climb_cases() {
        let h = Harness::new().await;
        let description = start_ok(&s3_ctx(&h), body).await;
        assert_eq!(
            description["BulkOperationStatus"],
            json!("IN_PROGRESS"),
            "{case}"
        );
    }
}

#[tokio::test]
async fn an_s3_compatible_endpoint_accepts_the_prefixes_the_path_rules_refuse() {
    // Each `..` in these resolves against a segment before it.
    for (case, body) in path_rule_cases() {
        let h = Harness::new().await;
        let description = start_ok(&s3_endpoint_ctx(&h), body).await;
        assert_eq!(
            description["BulkOperationStatus"],
            json!("IN_PROGRESS"),
            "{case}"
        );
    }
}
