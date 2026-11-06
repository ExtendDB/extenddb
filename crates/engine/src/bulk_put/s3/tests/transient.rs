// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Transient S3 failures: sent again by the store after the SDK's own
//! attempts, with a bound on each call; a ranged GET whose body is cut is
//! requested again for the same range with the same `If-Match`.

use std::time::{Duration, Instant};

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::primitives::SdkBody;

use super::read::{LINES, drain, part, parts};
use super::stub::{self, Answer, Stub, stub_clients};
use super::*;
use crate::bulk_put::s3::client::Phase;
use crate::bulk_put::s3::error::Tail;
use crate::bulk_put::s3::list::check_bucket;
use crate::bulk_put::s3::probe::probe;
use crate::bulk_put::s3::read::open;
use crate::bulk_put::s3::retry::RetryPolicy;
use crate::bulk_put::source::{InputCodec, OutputSse, SinkError, StoreUnavailable};

/// Retries a few milliseconds apart, within half a second.
const QUICK: RetryPolicy = RetryPolicy {
    first: Duration::from_millis(5),
    max: Duration::from_millis(20),
    total: Duration::from_millis(500),
};

fn empty_listing() -> HttpResponse {
    HttpResponse::new(
        200.try_into().unwrap(),
        SdkBody::from(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult><Name>loads</Name>\
             <IsTruncated>false</IsTruncated></ListBucketResult>",
        ),
    )
}

/// The server's failure, and not one the runner resumes after.
fn is_internal(error: &SourceError) -> bool {
    matches!(error, SourceError::Io(io)
        if io.kind() == std::io::ErrorKind::Other && !StoreUnavailable::of_io(io))
}

/// A transient failure that outlasted the retries, which the runner
/// resumes after.
fn is_unavailable(error: &SourceError) -> bool {
    matches!(error, SourceError::Io(io) if StoreUnavailable::of_io(io))
}

#[tokio::test]
async fn a_transient_answer_is_sent_again_and_the_call_succeeds() {
    for (status, code) in [
        (500, "InternalError"),
        (502, "BadGateway"),
        (503, "SlowDown"),
        (503, "ServiceUnavailable"),
        (504, "GatewayTimeout"),
        (400, "RequestTimeout"),
        (429, "TooManyRequests"),
    ] {
        let (clients, replay) = clients(vec![
            s3_error(status, code, "Please try again."),
            empty_listing(),
        ]);
        let clients = clients.with_retry(QUICK);
        check_bucket(&clients, &location("loads", None), "in/")
            .await
            .unwrap_or_else(|error| panic!("{code}: {error:?}"));
        assert_eq!(replay.actual_requests().count(), 2, "{code}");
    }
}

#[tokio::test]
async fn a_connection_closed_without_an_answer_is_sent_again() {
    let listing = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult><Name>loads</Name>\
                   <IsTruncated>false</IsTruncated></ListBucketResult>";
    let stub = Stub::start(vec![
        Answer::Close,
        Answer::Send {
            head: stub::head(
                "200 OK",
                &[
                    ("content-type", "application/xml"),
                    ("content-length", &listing.len().to_string()),
                ],
            ),
            body: listing.as_bytes().to_vec(),
        },
    ])
    .await;
    let clients = stub_clients(&stub.url, stub::short_timeouts()).with_retry(QUICK);
    check_bucket(&clients, &location("loads", None), "in/")
        .await
        .unwrap();
    assert_eq!(stub.heads().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cut_body_is_requested_again_for_the_same_range_with_if_match() {
    // The first range's body ends before its Content-Length, then the
    // third range's; each is requested again and the read completes.
    let mut cut_first = part(&LINES[..16], 0, LINES.len(), "listed-etag");
    *cut_first.body_mut() = SdkBody::from(LINES[..10].to_vec());
    let mut cut_third = part(&LINES[32..], 32, LINES.len(), "listed-etag");
    *cut_third.body_mut() = SdkBody::from(LINES[32..35].to_vec());
    let mut good = parts(LINES, 16, "listed-etag").into_iter();
    let mut next = || good.next().unwrap();
    let responses = vec![cut_first, next(), next(), cut_third, next()];
    let (clients, replay) = clients(responses);
    let mut reader = open(
        shared(clients.with_part_bytes(16).with_retry(QUICK)),
        location("loads", None),
        &input_file("in/part-0.json", LINES.len(), Some("listed-etag")),
        InputCodec::None,
    )
    .await
    .unwrap();
    let (lines, error) = drain(&mut reader).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(lines, ["line-0001", "line-0002", "line-0003", "line-0004"]);
    let ranges: Vec<_> = (0..5).map(|i| header(&replay, i, "range")).collect();
    assert_eq!(
        ranges,
        [
            Some("bytes=0-15".to_owned()),
            Some("bytes=0-15".to_owned()),
            Some("bytes=16-31".to_owned()),
            Some("bytes=32-39".to_owned()),
            Some("bytes=32-39".to_owned()),
        ]
    );
    for i in 0..5 {
        assert_eq!(
            header(&replay, i, "if-match").as_deref(),
            Some("\"listed-etag\""),
            "request {i}"
        );
    }
}

/// Ten times the bound of [`QUICK`]: a call still retrying by then has a
/// bound that does not end.
const PAST_THE_BOUND: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_failures_past_the_bound_are_answered_as_the_store_unavailable() {
    // Every answer is a 503: the call is sent again until the bound has
    // passed, then fails as the store unavailable, which the runner
    // resumes after.
    let answers = (0..200)
        .map(|_| s3_error(503, "SlowDown", "Please reduce your request rate."))
        .collect();
    let (listing, replay) = clients(answers);
    let listing = listing.with_retry(QUICK);
    let started = Instant::now();
    let error = tokio::time::timeout(
        PAST_THE_BOUND,
        check_bucket(&listing, &location("loads", None), "in/"),
    )
    .await
    .expect("the bound ends the retries")
    .unwrap_err();
    let elapsed = started.elapsed();
    assert!(is_unavailable(&error), "{error:?}");
    assert!(elapsed >= QUICK.total, "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    let sent = replay.actual_requests().count();
    assert!(sent > 2 && sent < 200, "{sent} requests");

    // A body cut on every request of the range: the same bound, then the
    // store unavailable.
    let mut cuts = Vec::new();
    for _ in 0..200 {
        let mut cut = part(&LINES[..16], 0, LINES.len(), "listed-etag");
        *cut.body_mut() = SdkBody::from(LINES[..10].to_vec());
        cuts.push(cut);
    }
    let (reading, replay) = clients(cuts);
    let started = Instant::now();
    let opened = tokio::time::timeout(
        PAST_THE_BOUND,
        open(
            shared(reading.with_part_bytes(16).with_retry(QUICK)),
            location("loads", None),
            &input_file("in/part-0.json", LINES.len(), Some("listed-etag")),
            InputCodec::None,
        ),
    )
    .await
    .expect("the bound ends the retries");
    let error = opened.err().expect("refused");
    assert!(is_unavailable(&error), "{error:?}");
    assert!(started.elapsed() >= QUICK.total);
    let sent = replay.actual_requests().count();
    assert!(sent > 2 && sent < 200, "{sent} requests");

    // A report-side call answered 503 past the bound: the same.
    let answers = (0..200)
        .map(|_| s3_error(503, "SlowDown", "Please reduce your request rate."))
        .collect();
    let (writing, _replay) = clients(answers);
    let error = tokio::time::timeout(
        PAST_THE_BOUND,
        probe(
            &writing.with_retry(QUICK),
            &location("reports", None),
            "run/AWSDynamoDB/BulkOperation/report-0.json.gz",
            &OutputSse::None,
            Tail::S3,
            Phase::Run,
        ),
    )
    .await
    .expect("the bound ends the retries")
    .unwrap_err();
    assert!(
        matches!(&error, SinkError::Io(io) if StoreUnavailable::of_io(io)),
        "{error:?}"
    );
}

#[tokio::test]
async fn client_errors_outside_the_transient_codes_are_answered_at_once() {
    for (status, code, expected) in [
        (403, "AccessDenied", "S3AccessDenied"),
        (404, "NoSuchBucket", "S3NoSuchBucket"),
        (400, "InvalidRequest", "S3InvalidRequest"),
    ] {
        let (clients, replay) = clients(vec![s3_error(status, code, "Refused."), empty_listing()]);
        let clients = clients.with_retry(QUICK);
        let fault = fault(
            check_bucket(&clients, &location("loads", None), "in/")
                .await
                .unwrap_err(),
        );
        assert_eq!(fault.code, expected);
        assert_eq!(replay.actual_requests().count(), 1, "{code}");
    }
    // A 4xx code outside the table is the server's failure, at once.
    let (clients, replay) = clients(vec![
        s3_error(409, "SomeFutureConflict", "Something new."),
        empty_listing(),
    ]);
    let error = check_bucket(&clients.with_retry(QUICK), &location("loads", None), "in/")
        .await
        .unwrap_err();
    assert!(is_internal(&error), "{error:?}");
    assert_eq!(replay.actual_requests().count(), 1);
}
