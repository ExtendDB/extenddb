// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A `CompleteMultipartUpload` whose answer was lost after S3 completed the
//! upload: the request is sent again and S3 answers `NoSuchUpload`, because
//! a completed upload is gone. The report object is then in place.

use std::time::Duration;

use extenddb_storage::ReportKind;
use tracing::Level;

use super::logs::Capture;
use super::write::{
    OPERATION, PART, created, gzip_size, method_and_uri, out, part_uploaded, records, store_fault,
};
use super::*;
use crate::bulk_put::s3::S3Sink;
use crate::bulk_put::source::{BulkSink, OutputSse, SinkError};

/// Retries a few milliseconds apart, within half a second.
const QUICK: RetryPolicy = RetryPolicy {
    first: Duration::from_millis(5),
    max: Duration::from_millis(20),
    total: Duration::from_millis(500),
};

fn no_such_upload() -> HttpResponse {
    s3_error(
        404,
        "NoSuchUpload",
        "The specified upload does not exist. The upload ID may be invalid, or the upload \
         may have been aborted or completed.",
    )
}

/// A `HeadObject` answer for an object of `length` bytes.
fn head(status: u16, length: usize) -> HttpResponse {
    with_headers(
        HttpResponse::new(status.try_into().unwrap(), SdkBody::empty()),
        &[
            ("etag", "\"done-2\""),
            ("content-length", &length.to_string()),
        ],
    )
}

/// The pages every test writes.
fn pages() -> Vec<Vec<serde_json::Value>> {
    vec![records(2000), records(2000), records(2000)]
}

/// The size of the report object the pages make.
fn written() -> usize {
    gzip_size(&pages())
}

/// Write a report that takes several parts, with `answers` after the
/// creation and the parts, and finish it.
async fn finish_with(
    answers: Vec<HttpResponse>,
) -> (Result<String, SinkError>, StaticReplayClient) {
    finish_under(RetryPolicy::NONE, answers).await
}

/// [`finish_with`], the store's calls retried under `retry`.
async fn finish_under(
    retry: RetryPolicy,
    answers: Vec<HttpResponse>,
) -> (Result<String, SinkError>, StaticReplayClient) {
    let pages = pages();
    let parts = gzip_size(&pages).div_ceil(usize::try_from(PART).unwrap());
    let mut responses = vec![created("up-0")];
    responses.extend((1..=parts).map(|i| part_uploaded(&format!("p{i}"))));
    responses.extend(answers);
    let (clients, replay) = clients(responses);
    let sink = S3Sink {
        clients: shared(clients.with_part_bytes(PART).with_retry(retry)),
    };
    let mut writer = sink
        .open(
            &out(None),
            "reports",
            OPERATION,
            ReportKind::Error,
            "lease",
            &OutputSse::Aes256,
        )
        .await
        .unwrap();
    for page in pages {
        writer.write_page(page).await.unwrap();
    }
    (writer.finish().await, replay)
}

fn path(uri: &str) -> &str {
    uri.split('?').next().unwrap_or(uri)
}

#[tokio::test]
async fn a_completion_answered_no_such_upload_with_the_object_in_place_is_installed() {
    let (key, replay) = finish_with(vec![no_such_upload(), head(200, written())]).await;
    key.expect("the report is installed");
    let count = replay.actual_requests().count();
    let (complete, complete_uri) = method_and_uri(&replay, count - 2);
    let (check, check_uri) = method_and_uri(&replay, count - 1);
    assert_eq!(complete, "POST");
    assert!(complete_uri.contains("uploadId=up-0"), "{complete_uri}");
    assert_eq!(check, "HEAD");
    assert_eq!(
        path(&check_uri),
        path(&complete_uri),
        "the object at the key"
    );
}

#[tokio::test]
async fn a_completion_answered_no_such_upload_without_the_object_fails() {
    let (key, replay) = finish_with(vec![no_such_upload(), head(404, 0)]).await;
    let fault = store_fault(key.unwrap_err());
    assert_eq!(fault.code, "S3NoSuchUpload");
    let count = replay.actual_requests().count();
    assert_eq!(method_and_uri(&replay, count - 1).0, "HEAD");
}

#[tokio::test]
async fn a_completion_answered_no_such_upload_with_an_object_of_another_size_fails() {
    for length in [0, 10, written() - 1, written() + 1] {
        let (key, replay) = finish_with(vec![no_such_upload(), head(200, length)]).await;
        let fault = store_fault(key.unwrap_err());
        assert_eq!(fault.code, "S3NoSuchUpload", "{length} bytes");
        let count = replay.actual_requests().count();
        assert_eq!(
            method_and_uri(&replay, count - 1).0,
            "HEAD",
            "{length} bytes"
        );
    }
}

#[tokio::test]
async fn another_completion_fault_is_not_checked_against_the_object() {
    let (key, replay) = finish_with(vec![s3_error(
        400,
        "InvalidPart",
        "One or more of the specified parts could not be found.",
    )])
    .await;
    let fault = store_fault(key.unwrap_err());
    assert_eq!(fault.code, "S3InvalidPart");
    let count = replay.actual_requests().count();
    assert_eq!(method_and_uri(&replay, count - 1).0, "POST");
}

#[tokio::test]
async fn a_completion_check_that_fails_transiently_past_its_bound_is_the_servers_failure() {
    // HeadObject answers 500 until the bound of its retries has passed.
    let (capture, _guard) = Capture::install();
    let mut answers = vec![no_such_upload()];
    answers.extend((0..200).map(|_| s3_error(500, "InternalError", "Please try again.")));
    let (key, replay) = finish_under(QUICK, answers).await;
    let error = key.unwrap_err();
    assert!(
        matches!(&error, SinkError::Io(io) if io.to_string() == "S3 report completion check failed"),
        "{error:?}"
    );
    let heads = (0..replay.actual_requests().count())
        .filter(|&i| method_and_uri(&replay, i).0 == "HEAD")
        .count();
    assert!(heads > 2 && heads < 200, "{heads} HeadObject requests");
    let errors: Vec<_> = capture
        .events()
        .into_iter()
        .filter(|event| event.level == Level::ERROR && event.message().starts_with("S3 "))
        .collect();
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].message(), "S3 report completion check failed");
    assert_eq!(errors[0].field("s3_code"), Some("InternalError"));

    // A 403, the answer without s3:GetObject, still surfaces the
    // NoSuchUpload, at once.
    let (key, replay) = finish_under(
        QUICK,
        vec![
            no_such_upload(),
            with_headers(
                HttpResponse::new(403.try_into().unwrap(), SdkBody::empty()),
                &[("x-amz-request-id", REQUEST_ID)],
            ),
        ],
    )
    .await;
    assert_eq!(store_fault(key.unwrap_err()).code, "S3NoSuchUpload");
    let count = replay.actual_requests().count();
    assert_eq!(method_and_uri(&replay, count - 1).0, "HEAD");
}
