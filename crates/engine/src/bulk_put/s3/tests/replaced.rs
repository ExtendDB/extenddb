// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `If-Match` with the listed `ETag` on every GetObject, and a replaced
//! object, answered 412, reported as overwritten wherever the read found
//! it.

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::primitives::SdkBody;

use super::read::{LINES, drain, open_with, part, parts};
use super::*;
use crate::bulk_put::s3::read::open;
use crate::bulk_put::source::{InputCodec, LineReader};

const MOVED: &str = "At least one of the pre-conditions you specified did not hold";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resumed_open_sends_the_listed_etag_on_its_first_range() {
    // The first claim opens the object and reads one range; the resumed
    // claim opens it again. Both first ranges carry the listed ETag.
    let mut responses = parts(LINES, 16, "listed-etag");
    responses.truncate(1);
    responses.extend(parts(LINES, 16, "listed-etag"));
    let (clients, replay) = clients(responses);
    let clients = shared(clients.with_part_bytes(16));
    let file = input_file("in/part-0.json", LINES.len(), Some("listed-etag"));
    let first = open(
        Arc::clone(&clients),
        location("loads", None),
        &file,
        InputCodec::None,
    )
    .await
    .unwrap();
    drop(first);
    let mut resumed = open(clients, location("loads", None), &file, InputCodec::None)
        .await
        .unwrap();
    let (lines, error) = drain(&mut resumed).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(lines.len(), 4);
    assert_eq!(replay.actual_requests().count(), 4);
    for i in 0..4 {
        assert_eq!(
            header(&replay, i, "if-match").as_deref(),
            Some("\"listed-etag\""),
            "request {i}"
        );
    }
    assert_eq!(header(&replay, 1, "range").as_deref(), Some("bytes=0-15"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_listed_without_an_etag_matches_later_ranges_to_the_first_answer() {
    let (clients, replay) = clients(parts(LINES, 16, "first-answer"));
    let mut reader = open(
        shared(clients.with_part_bytes(16)),
        location("loads", None),
        &input_file("in/part-0.json", LINES.len(), None),
        InputCodec::None,
    )
    .await
    .unwrap();
    let (lines, error) = drain(&mut reader).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(lines.len(), 4);
    assert_eq!(header(&replay, 0, "if-match"), None);
    for i in 1..3 {
        assert_eq!(
            header(&replay, i, "if-match").as_deref(),
            Some("\"first-answer\""),
            "request {i}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_without_a_listed_etag_reports_its_first_answers_tag() {
    let (clients, _replay) = clients(parts(LINES, 16, "first-answer"));
    let reader = open(
        shared(clients.with_part_bytes(16)),
        location("loads", None),
        &input_file("in/part-0.json", LINES.len(), None),
        InputCodec::None,
    )
    .await
    .unwrap();
    assert_eq!(reader.opened_etag().as_deref(), Some("first-answer"));
    assert_eq!(
        reader.etag(),
        None,
        "no read tag before the end of the object"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replaced_object_at_its_first_range_is_overwritten() {
    let (reader, replay) = open_with(
        vec![s3_error(412, "PreconditionFailed", MOVED)],
        16,
        LINES.len(),
        InputCodec::None,
    )
    .await;
    assert!(
        matches!(reader, Err(SourceError::Overwritten)),
        "{:?}",
        reader.err()
    );
    assert_eq!(
        header(&replay, 0, "if-match").as_deref(),
        Some("\"listed-etag\"")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replaced_object_at_a_later_range_is_overwritten_after_the_lines_it_delivered() {
    let mut responses = vec![part(&LINES[..16], 0, LINES.len(), "listed-etag")];
    responses.push(s3_error(412, "PreconditionFailed", MOVED));
    let (reader, replay) = open_with(responses, 16, LINES.len(), InputCodec::None).await;
    let mut reader = reader.unwrap();
    let (lines, error) = drain(&mut reader).await;
    assert_eq!(lines, ["line-0001"]);
    assert!(matches!(error, Some(SourceError::Overwritten)), "{error:?}");
    assert_eq!(
        header(&replay, 1, "if-match").as_deref(),
        Some("\"listed-etag\"")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_listed_without_an_etag_and_answered_at_another_size_is_overwritten() {
    // Listed at 0 bytes and replaced by a 400 MiB object, whose first byte
    // answers the one-byte range; and listed at 40 bytes, answered from a
    // 49-byte object.
    for (listed, first, answered) in [
        (0, &LINES[..1], 419_430_400),
        (LINES.len(), &LINES[..16], LINES.len() + 9),
    ] {
        let (clients, replay) = clients(vec![part(first, 0, answered, "new-object")]);
        let reader = open(
            shared(clients.with_part_bytes(16)),
            location("loads", None),
            &input_file("in/part-0.json", listed, None),
            InputCodec::None,
        )
        .await;
        assert!(
            matches!(reader, Err(SourceError::Overwritten)),
            "listed {listed}: {:?}",
            reader.err()
        );
        assert_eq!(replay.actual_requests().count(), 1, "listed {listed}");
    }
}

/// A 200 answer with the whole of `body` and no `Content-Range`, as from a
/// service that ignores `Range`.
fn whole(body: &[u8], etag: &str) -> HttpResponse {
    with_headers(
        HttpResponse::new(200.try_into().unwrap(), SdkBody::from(body.to_vec())),
        &[
            ("content-length", &body.len().to_string()),
            ("etag", &format!("\"{etag}\"")),
            ("x-amz-request-id", REQUEST_ID),
        ],
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_listed_without_an_etag_and_answered_whole_at_another_size_is_overwritten() {
    // Listed at 40 bytes, answered with a 20-byte object and no
    // Content-Range: the answer's length is the object's size.
    let (replaced, replay) = clients(vec![whole(&LINES[..20], "new-object")]);
    let reader = open(
        shared(replaced.with_part_bytes(64)),
        location("loads", None),
        &input_file("in/part-0.json", LINES.len(), None),
        InputCodec::None,
    )
    .await;
    assert!(
        matches!(reader, Err(SourceError::Overwritten)),
        "{:?}",
        reader.err()
    );
    assert_eq!(replay.actual_requests().count(), 1);

    // The listed size, answered the same way, is read.
    let (same, _) = clients(vec![whole(LINES, "same-object")]);
    let mut reader = open(
        shared(same.with_part_bytes(64)),
        location("loads", None),
        &input_file("in/part-0.json", LINES.len(), None),
        InputCodec::None,
    )
    .await
    .unwrap();
    let (lines, error) = drain(&mut reader).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(lines, ["line-0001", "line-0002", "line-0003", "line-0004"]);
}
