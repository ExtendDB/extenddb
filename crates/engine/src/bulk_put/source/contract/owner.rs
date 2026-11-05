// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cases for an `S3BucketOwner` that names an account other than the
//! caller's. The filesystem store refuses it; the S3 store sends it and
//! lets the service decide.

use serde_json::json;

use extenddb_storage::ReportKind;

use super::super::{InputCodec, OutputSse};
use super::*;
use crate::bulk_put::report::{manifest_key, report_key};

const OTHER_OWNER: &str = "999999999999";

fn first_report_key() -> String {
    report_key("reports", ACCOUNT, OPERATION, ReportKind::Error, 0)
}

/// On the filesystem store the caller's account is the only owner a bucket
/// can have: an expected owner of any other account is `S3AccessDenied`,
/// `Access Denied`, at the input check and at the write check, decided
/// before existence. An owner equal to the caller's account sets no
/// expectation.
pub(super) async fn on_the_filesystem_store_an_expected_owner_is_access_denied<F: StoreFixture>(
    f: &F,
) {
    let bucket = f.bucket().await;
    let missing = f.missing_bucket();
    for name in [&bucket, &missing] {
        let owned = BucketRef::new(ACCOUNT, name, Some(OTHER_OWNER));
        let error = f
            .source()
            .check_bucket(&owned, "in/")
            .await
            .expect_err("another owner");
        assert_location_fault::<F>(
            &source_fault(error),
            "S3AccessDenied",
            "Access Denied",
            Tail::AmazonS3 {
                status: 403,
                code: "AccessDenied",
            },
        );
        for key in [
            first_report_key(),
            manifest_key("reports", ACCOUNT, OPERATION),
        ] {
            let error = f
                .sink()
                .check_location(&owned, &key, &OutputSse::None)
                .await
                .expect_err("another owner");
            assert_location_fault::<F>(
                &sink_fault(error),
                "S3AccessDenied",
                "Access Denied",
                Tail::S3 { status: 403 },
            );
        }
    }
    let own = BucketRef::new(ACCOUNT, &bucket, Some(ACCOUNT));
    f.source().check_bucket(&own, "in/").await.unwrap();
    f.sink()
        .check_location(&own, &first_report_key(), &OutputSse::None)
        .await
        .unwrap();
}

/// Every call the store makes for one bucket, input and output, each
/// answer ignored.
async fn every_call(f: &S3Fixture, at: &BucketRef<'_>, file: &InputFile) {
    let _ = f.source().check_bucket(at, "in/").await;
    let _ = f.source().list(at, "in/", 10).await;
    if let Ok(mut reader) = f.source().open(at, file, InputCodec::None).await {
        while let Ok(Some(_)) = reader.next_line(MAX_LINE).await {}
    }
    // The file carries no listed tag, so the store reads one byte for it.
    let _ = f.source().etag(at, file).await;
    let _ = f.source().line_count(at, file, InputCodec::None).await;
    let _ = f
        .sink()
        .check_location(at, &first_report_key(), &OutputSse::None)
        .await;
    if let Ok(mut writer) = f
        .sink()
        .open(
            at,
            "reports",
            OPERATION,
            ReportKind::Error,
            LEASE,
            &OutputSse::None,
        )
        .await
        && writer
            .write_page(vec![json!({"errorMessages": ["e"]})])
            .await
            .is_ok()
    {
        let _ = writer.finish().await;
    }
    let _ = f
        .sink()
        .write_manifest(
            at,
            "reports",
            OPERATION,
            LEASE,
            &OutputSse::None,
            vec![json!({"etag": "e"})],
        )
        .await;
}

/// On S3 an expected owner travels as `x-amz-expected-bucket-owner` on
/// every request the store sends for the bucket, and no request carries
/// the header when none is set. A service may ignore the header or refuse
/// the request, so the requests are read as sent, whatever the service
/// answered.
pub(super) async fn on_s3_every_request_carries_the_expected_owner_when_one_is_set(f: &S3Fixture) {
    let bucket = f.bucket().await;
    let body = rows(0, 2);
    f.put(&bucket, "in/part.json", &body).await;
    let file = InputFile {
        path: "in/part.json".to_owned(),
        size: i64::try_from(body.len()).unwrap(),
        rows: None,
        etag: None,
        abandon: None,
    };
    for owner in [Some(OTHER_OWNER), None] {
        let before = f.sent().len();
        every_call(f, &BucketRef::new(ACCOUNT, &bucket, owner), &file).await;
        let sent = f.sent().split_off(before);
        // One request at least per call: the bucket check, the listing,
        // the read, the tag, the line count, the write check, the report
        // and the manifest.
        assert!(sent.len() >= 8, "{sent:?}");
        for request in &sent {
            assert_eq!(request.owner.as_deref(), owner, "{}", request.request);
        }
    }
}
