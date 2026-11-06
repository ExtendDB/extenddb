// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The Start deadline: each S3 call a Start makes (the bucket check, every
//! page of the listing, the encryption check and its abort) ends within
//! it, its retries included, and then fails as the server's. The calls the
//! runner makes are bound by the retry total alone.

use std::time::{Duration, Instant};

use super::stub::{self, Answer, Stub, stub_clients};
use super::*;
use crate::bulk_put::s3::client::{Phase, START_DEADLINE, timeouts};
use crate::bulk_put::s3::list::{check_bucket, list};
use crate::bulk_put::s3::probe::probe;
use crate::bulk_put::s3::{S3Sink, error::Tail};
use crate::bulk_put::source::{BucketRef, BulkSink, OutputSse, SinkError};

/// The deadline the tests use in place of [`START_DEADLINE`].
const SHORT: Duration = Duration::from_millis(400);

/// How long a test waits before it fails: past [`SHORT`], well inside the
/// read deadline of the clients below, so a call that ignores the Start
/// deadline fails the test instead of holding it.
const PATIENCE: Duration = Duration::from_secs(5);

/// A store's clients against `stub`, with the store's read deadline (60 s)
/// and the Start deadline at [`SHORT`].
fn held_clients(stub: &Stub) -> S3Clients {
    stub_clients(&stub.url, timeouts()).with_start_deadline(SHORT)
}

fn kms() -> OutputSse {
    OutputSse::Kms {
        key_id: Some("arn:aws:kms:us-east-1:000000000001:key/k1".to_owned()),
    }
}

fn is_internal_sink(error: &SinkError) -> bool {
    matches!(error, SinkError::Io(io) if io.kind() == std::io::ErrorKind::Other)
}

#[test]
fn the_start_deadline_is_ten_seconds() {
    assert_eq!(START_DEADLINE, Duration::from_secs(10));
}

#[tokio::test]
async fn a_bucket_check_with_no_answer_ends_at_the_start_deadline() {
    let stub = Stub::start(vec![Answer::Hold]).await;
    let clients = held_clients(&stub);
    let started = Instant::now();
    let checked = tokio::time::timeout(
        PATIENCE,
        check_bucket(&clients, &location("loads", None), "in/"),
    )
    .await
    .expect("the bucket check ends at the Start deadline");
    assert!(
        matches!(&checked, Err(SourceError::Io(io)) if io.kind() == std::io::ErrorKind::Other),
        "{checked:?}"
    );
    assert!(started.elapsed() >= SHORT, "{:?}", started.elapsed());
    assert_eq!(stub.heads().len(), 1);
}

#[tokio::test]
async fn a_listing_page_with_no_answer_ends_at_the_start_deadline() {
    // The first page answers with a continuation token; the second never
    // answers. Each page is one call under the deadline.
    let first = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
        <ListBucketResult><Name>loads</Name><Prefix>in/</Prefix><KeyCount>1</KeyCount>\
        <MaxKeys>1000</MaxKeys><EncodingType>url</EncodingType><IsTruncated>true</IsTruncated>\
        <NextContinuationToken>t2</NextContinuationToken>\
        <Contents><Key>in/a.json</Key><Size>3</Size><ETag>&quot;e1&quot;</ETag></Contents>\
        </ListBucketResult>";
    let head = stub::head(
        "200 OK",
        &[
            ("content-type", "application/xml"),
            ("content-length", &first.len().to_string()),
        ],
    );
    let stub = Stub::start(vec![
        Answer::Send {
            head,
            body: first.as_bytes().to_vec(),
        },
        Answer::Hold,
    ])
    .await;
    let clients = held_clients(&stub);
    let listed = tokio::time::timeout(
        PATIENCE,
        list(&clients, &location("loads", None), "in/", 100),
    )
    .await
    .expect("the listing ends at the Start deadline");
    assert!(
        matches!(&listed, Err(SourceError::Io(io)) if io.kind() == std::io::ErrorKind::Other),
        "{listed:?}"
    );
    assert_eq!(stub.heads().len(), 2, "both pages were asked for");
}

#[tokio::test]
async fn an_encryption_check_with_no_answer_ends_at_the_start_deadline() {
    let stub = Stub::start(vec![Answer::Hold]).await;
    let sink = S3Sink {
        clients: shared(held_clients(&stub)),
    };
    let at = BucketRef {
        account: "000000000001",
        bucket: "reports",
        expected_owner: None,
    };
    let checked = tokio::time::timeout(
        PATIENCE,
        sink.check_encryption(
            &at,
            "run/AWSDynamoDB/BulkOperation/k/output/errors/0.json.gz",
            &kms(),
        ),
    )
    .await
    .expect("the encryption check ends at the Start deadline");
    assert!(
        matches!(&checked, Err(error) if is_internal_sink(error)),
        "{checked:?}"
    );
}

#[tokio::test]
async fn a_call_the_runner_makes_is_not_bound_by_the_start_deadline() {
    // The write check at the first report record waits past the Start
    // deadline for its answer, here the read deadline of one second.
    let stub = Stub::start(vec![Answer::Hold]).await;
    let short_read = timeouts()
        .to_builder()
        .read_timeout(Duration::from_secs(1))
        .build();
    let clients =
        stub_clients(&stub.url, short_read).with_start_deadline(Duration::from_millis(100));
    let started = Instant::now();
    let checked = tokio::time::timeout(
        PATIENCE,
        probe(
            &clients,
            &location("reports", None),
            "run/AWSDynamoDB/BulkOperation/k/output/errors/0.json.gz",
            &OutputSse::None,
            Tail::S3,
            Phase::Run,
        ),
    )
    .await
    .expect("the write check ends at the read deadline");
    let elapsed = started.elapsed();
    assert!(
        matches!(&checked, Err(error) if is_internal_sink(error)),
        "{checked:?}"
    );
    assert!(elapsed >= Duration::from_secs(1), "{elapsed:?}");
}
