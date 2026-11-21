// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A read the caller drops: the GET in flight is dropped with it and no
//! further request is sent for the object, though the ranged reader runs
//! on the blocking pool, which a dropped future does not stop.

use std::future::Future;
use std::time::{Duration, Instant};

use aws_sdk_s3::config::timeout::TimeoutConfig;

use super::read::{LINES, drain};
use super::stub::{self, Answer, Stub, stub_clients};
use super::*;
use crate::bulk_put::s3::read::open;
use crate::bulk_put::s3::retry::RetryPolicy;
use crate::bulk_put::source::{InputCodec, LineReader};

/// A held GET ends at the read deadline after one second and is sent again
/// after a wait of a fifth of a second, so a read that keeps going after
/// its drop sends its next request about 1.2 seconds after the hold.
const BACKOFF: RetryPolicy = RetryPolicy {
    first: Duration::from_millis(200),
    max: Duration::from_millis(200),
    total: Duration::from_secs(10),
};

/// How long the tests watch the listener after a drop: past the read
/// deadline and more than two waits of [`BACKOFF`].
const WATCH: Duration = Duration::from_secs(2);

fn timeouts() -> TimeoutConfig {
    TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(1))
        .read_timeout(Duration::from_secs(1))
        .build()
}

/// The 206 answer for bytes `start..end` of [`LINES`].
fn range(start: usize, end: usize) -> Answer {
    let body = LINES[start..end].to_vec();
    Answer::Send {
        head: stub::head(
            "206 Partial Content",
            &[
                ("content-length", &body.len().to_string()),
                (
                    "content-range",
                    &format!("bytes {start}-{}/{}", end - 1, LINES.len()),
                ),
                ("etag", "\"e1\""),
            ],
        ),
        body,
    }
}

fn holds(count: usize) -> impl Iterator<Item = Answer> {
    (0..count).map(|_| Answer::Hold)
}

async fn reading(stub: &Stub) -> Arc<S3Clients> {
    shared(
        stub_clients(&stub.url, timeouts())
            .with_part_bytes(16)
            .with_retry(BACKOFF),
    )
}

/// Run `call` until the listener has received `requests` requests, then
/// drop it; a call that answers first fails the test.
async fn drop_when_held<F: Future>(stub: &Stub, requests: usize, call: F) {
    let held = async {
        while stub.heads().len() < requests {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::select! {
        _ = call => panic!("the call answered while its GET was held"),
        held = tokio::time::timeout(Duration::from_secs(5), held) => {
            held.expect("the GET reaches the listener");
        }
    }
}

/// Wait until the client has closed `count` held connections; the time it
/// took.
async fn released(stub: &Stub, count: u64) -> Duration {
    let started = Instant::now();
    while stub.released() < count && started.elapsed() < WATCH {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    started.elapsed()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_line_read_dropped_while_its_range_is_held_sends_no_further_request() {
    // The open reads the first 16 bytes; the first line read then asks for
    // the second range, which the listener holds.
    let answers = std::iter::once(range(0, 16)).chain(holds(8)).collect();
    let stub = Stub::start(answers).await;
    let mut reader = open(
        reading(&stub).await,
        location("loads", None),
        &input_file("in/part-0.json", LINES.len(), Some("e1")),
        InputCodec::None,
    )
    .await
    .unwrap();
    drop_when_held(&stub, 2, reader.next_line(1 << 20)).await;
    // The reader stays alive until the end, so only the dropped call can
    // stop the read: dropping the reader would stop it on its own.
    let closed = released(&stub, 1).await;
    assert!(
        closed < Duration::from_millis(500),
        "closed after {closed:?}"
    );
    tokio::time::sleep(WATCH).await;
    assert_eq!(stub.heads().len(), 2, "requests after the drop");
    drop(reader);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_line_read_dropped_while_its_range_is_held_answers_the_next_call_with_a_fault() {
    let answers = std::iter::once(range(0, 16)).chain(holds(8)).collect();
    let stub = Stub::start(answers).await;
    let mut reader = open(
        reading(&stub).await,
        location("loads", None),
        &input_file("in/part-0.json", LINES.len(), Some("e1")),
        InputCodec::None,
    )
    .await
    .unwrap();
    drop_when_held(&stub, 2, reader.next_line(1 << 20)).await;
    // The next call awaits the fill the dropped call started: the line it
    // read, then the stop as a fault, never an end of file before the
    // object's end.
    let (lines, error) = tokio::time::timeout(Duration::from_secs(5), drain(&mut reader))
        .await
        .expect("the next call answers");
    assert_eq!(lines, ["line-0001"]);
    assert!(
        matches!(error, Some(SourceError::Io(_))),
        "the stopped read is a fault: {error:?}"
    );
    assert_eq!(stub.heads().len(), 2, "requests after the drop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_dropped_while_its_first_range_is_held_sends_no_further_request() {
    let stub = Stub::start(holds(8).collect()).await;
    let clients = reading(&stub).await;
    let file = input_file("in/part-0.json", LINES.len(), Some("e1"));
    drop_when_held(
        &stub,
        1,
        open(clients, location("loads", None), &file, InputCodec::None),
    )
    .await;
    let closed = released(&stub, 1).await;
    assert!(
        closed < Duration::from_millis(500),
        "closed after {closed:?}"
    );
    tokio::time::sleep(WATCH).await;
    assert_eq!(stub.heads().len(), 1, "requests after the drop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_line_read_that_is_not_dropped_retries_a_held_range_and_completes() {
    let answers = vec![
        range(0, 16),
        Answer::Hold,
        range(16, 32),
        range(32, LINES.len()),
    ];
    let stub = Stub::start(answers).await;
    let mut reader = open(
        reading(&stub).await,
        location("loads", None),
        &input_file("in/part-0.json", LINES.len(), Some("e1")),
        InputCodec::None,
    )
    .await
    .unwrap();
    let (lines, error) = tokio::time::timeout(Duration::from_secs(10), drain(&mut reader))
        .await
        .expect("the read completes");
    assert!(error.is_none(), "{error:?}");
    assert_eq!(lines, ["line-0001", "line-0002", "line-0003", "line-0004"]);
    assert_eq!(stub.heads().len(), 4);
    assert_eq!(stub.header(1, "range"), stub.header(2, "range"));
}
