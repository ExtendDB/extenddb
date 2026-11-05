// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

use extenddb_storage::{BoxedFuture as BoxFuture, InputFile};

use crate::bulk_put::authorize::{AuthorizeError, BulkPrincipal, S3Action};
use crate::bulk_put::source::{
    BucketRef, BulkSource, FsSink, FsSource, InputCodec, LineReader, SourceError, SourceFile,
    stored_form,
};
use crate::bulk_put::tests::runner::{FakeAuthorizer, caller, denied};

// s3:ListBucket is evaluated with s3:prefix set to the request's S3KeyPrefix
// and, when the store lists another prefix for it, to that prefix. The
// filesystem store's walk matches the prefix as given against the relative
// paths it finds, so it is evaluated once there. An S3-compatible endpoint
// resolves empty, `.` and `..` segments, so it is evaluated on the resolved
// prefix as well.

const BUCKET_ARN: &str = "arn:aws:s3:::loads";

/// Deny `s3:ListBucket` when `s3:prefix` is `prefix`; allow everything else.
struct DenyPrefix(&'static str);

impl BulkAuthorizer for DenyPrefix {
    fn authorize<'a>(
        &'a self,
        _principal: &'a BulkPrincipal,
        action: S3Action,
        _resource_arn: &'a str,
        s3_prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), AuthorizeError>> {
        let denied = action == S3Action::ListBucket && s3_prefix == Some(self.0);
        Box::pin(async move {
            if denied {
                Err(AuthorizeError::Denied { explicit: false })
            } else {
                Ok(())
            }
        })
    }
}

/// The filesystem source with the listing prefix of an S3-compatible
/// endpoint (`S3Source::listed_prefix` against a configured endpoint).
struct EndpointListing(FsSource);

impl BulkSource for EndpointListing {
    fn check_bucket(
        &self,
        bucket: &BucketRef<'_>,
        prefix: &str,
    ) -> BoxFuture<'_, Result<(), SourceError>> {
        self.0.check_bucket(bucket, prefix)
    }

    fn list(
        &self,
        bucket: &BucketRef<'_>,
        prefix: &str,
        max_files: u64,
    ) -> BoxFuture<'_, Result<Vec<SourceFile>, SourceError>> {
        self.0.list(bucket, prefix, max_files)
    }

    fn open(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<Box<dyn LineReader>, SourceError>> {
        self.0.open(bucket, file, compression)
    }

    fn etag(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
    ) -> BoxFuture<'_, Result<String, SourceError>> {
        self.0.etag(bucket, file)
    }

    fn line_count(
        &self,
        bucket: &BucketRef<'_>,
        file: &InputFile,
        compression: InputCodec,
    ) -> BoxFuture<'_, Result<u64, SourceError>> {
        self.0.line_count(bucket, file, compression)
    }

    fn listed_prefix(&self, prefix: &str) -> String {
        stored_form(prefix, true)
    }
}

fn ctx_with(h: &Harness, authorizer: Arc<dyn BulkAuthorizer>) -> OperationContext {
    let mut ctx = base_ctx(h);
    ctx.bulk_authorizer = Some(authorizer);
    ctx.bulk_principal = Some(caller());
    ctx
}

fn endpoint_ctx_with(h: &Harness, authorizer: Arc<dyn BulkAuthorizer>) -> OperationContext {
    let mut ctx = ctx_with(h, authorizer);
    ctx.bulk = Some(Arc::new(BulkStore::s3_endpoint_with_parts(
        Arc::new(EndpointListing(h.fs_source())),
        Arc::new(FsSink {
            root: h.store.root(),
        }),
    )));
    ctx
}

fn list_prefixes(fake: &FakeAuthorizer) -> Vec<Option<String>> {
    fake.checks()
        .into_iter()
        .filter(|(action, _, _)| *action == S3Action::ListBucket)
        .map(|(_, _, prefix)| prefix)
        .collect()
}

#[tokio::test]
async fn the_filesystem_store_checks_the_listing_once_on_the_prefix_as_given() {
    let h = Harness::new().await;
    h.write_input("orders/part-0.json", &file_of(&plain_rows(1)));
    let fake = Arc::new(FakeAuthorizer::default());
    let body = with_input(start_body(), "S3KeyPrefix", json!(".//orders/"));
    let description = start_ok(&ctx_with(&h, Arc::clone(&fake) as _), body).await;
    assert_eq!(list_prefixes(&fake), vec![Some(".//orders/".to_owned())]);
    assert_eq!(
        description["TotalInputBytes"],
        json!(0),
        "no relative path starts with .//, so nothing is listed"
    );
}

#[tokio::test]
async fn a_prefix_the_store_lists_as_given_is_checked_once() {
    let h = Harness::new().await;
    h.write_input("orders/part-0.json", &file_of(&plain_rows(1)));
    let fake = Arc::new(FakeAuthorizer::default());
    let body = with_input(start_body(), "S3KeyPrefix", json!("orders/"));
    start_ok(&ctx_with(&h, Arc::clone(&fake) as _), body).await;
    assert_eq!(fake.count(S3Action::ListBucket), 1);
}

#[tokio::test]
async fn an_s3_compatible_endpoint_checks_the_listing_as_given_and_as_it_resolves() {
    let h = Harness::new().await;
    h.write_input("orders/part-0.json", &file_of(&plain_rows(1)));
    let fake = Arc::new(FakeAuthorizer::default());
    let body = with_input(start_body(), "S3KeyPrefix", json!("x/..//./orders/"));
    start_ok(&endpoint_ctx_with(&h, Arc::clone(&fake) as _), body).await;
    assert_eq!(
        list_prefixes(&fake),
        vec![
            Some("x/..//./orders/".to_owned()),
            Some("orders/".to_owned())
        ]
    );
}

#[tokio::test]
async fn a_listing_denied_on_the_resolved_prefix_is_born_failed() {
    for given in ["./orders/", ".//orders/", "orders//", "x/../orders/"] {
        let h = Harness::new().await;
        h.write_input("orders/part-0.json", &file_of(&plain_rows(1)));
        let body = with_input(start_body(), "S3KeyPrefix", json!(given));
        let ctx = endpoint_ctx_with(&h, Arc::new(DenyPrefix("orders/")));
        let description = start_ok(&ctx, body).await;
        assert_eq!(
            description["BulkOperationStatus"],
            json!("FAILED"),
            "{given}"
        );
        assert_eq!(description["FailureCode"], json!("S3AccessDenied"));
        assert_eq!(
            description["FailureMessage"],
            json!(denied("s3:ListBucket", BUCKET_ARN))
        );
        assert!(description.get("TotalInputBytes").is_none());
    }
}
