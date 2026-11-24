// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Reading input objects with GetObject: the ranged reads, `If-Match`,
//! the read's `ETag`, and the fault rendering before and after the first
//! byte.

use std::io::Write;

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::primitives::SdkBody;

use super::stub::{self, Answer, Stub, stub_clients};
use super::*;
use crate::bulk_put::s3::read::{S3LineReader, etag, line_count, open};
use crate::bulk_put::source::{InputCodec, LineReader};

pub(super) const LINES: &[u8] = b"line-0001\nline-0002\nline-0003\nline-0004\n";

/// A 206 answer for bytes `start..start + body.len()` of an object of
/// `total` bytes.
pub(super) fn part(body: &[u8], start: usize, total: usize, etag: &str) -> HttpResponse {
    let end = start + body.len() - 1;
    with_headers(
        HttpResponse::new(206.try_into().unwrap(), SdkBody::from(body.to_vec())),
        &[
            ("content-length", &body.len().to_string()),
            ("content-range", &format!("bytes {start}-{end}/{total}")),
            ("etag", &format!("\"{etag}\"")),
            ("x-amz-request-id", REQUEST_ID),
        ],
    )
}

/// The responses for reading `object` whole in `chunk`-byte ranges.
pub(super) fn parts(object: &[u8], chunk: usize, etag: &str) -> Vec<HttpResponse> {
    object
        .chunks(chunk)
        .enumerate()
        .map(|(i, body)| part(body, i * chunk, object.len(), etag))
        .collect()
}

pub(super) async fn open_with(
    responses: Vec<HttpResponse>,
    part_bytes: u64,
    size: usize,
    codec: InputCodec,
) -> (Result<S3LineReader, SourceError>, StaticReplayClient) {
    let (clients, replay) = clients(responses);
    let reader = open(
        shared(clients.with_part_bytes(part_bytes)),
        location("loads", None),
        &input_file("in/part-0.json", size, Some("listed-etag")),
        codec,
    )
    .await;
    (reader, replay)
}

/// Every line until the end or the first fault.
pub(super) async fn drain(reader: &mut S3LineReader) -> (Vec<String>, Option<SourceError>) {
    let mut lines = Vec::new();
    loop {
        match reader.next_line(1 << 20).await {
            Ok(Some(line)) => lines.push(line),
            Ok(None) => return (lines, None),
            Err(error) => return (lines, Some(error)),
        }
    }
}

fn gzip(content: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(content).unwrap();
    encoder.finish().unwrap()
}

fn many_lines() -> Vec<u8> {
    (0..400)
        .map(|i| format!("{{\"Item\":{{\"pk\":{{\"S\":\"row-{i:05}\"}}}}}}\n"))
        .collect::<String>()
        .into_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_object_is_read_in_ranges_with_the_listed_etag_as_if_match() {
    // The answers carry an ETag of their own: If-Match still names the
    // listed one, and the manifest records the read's.
    let (reader, replay) = open_with(
        parts(LINES, 16, "etag-of-read"),
        16,
        LINES.len(),
        InputCodec::None,
    )
    .await;
    let mut reader = reader.unwrap();
    let (lines, fault) = drain(&mut reader).await;
    assert!(fault.is_none(), "{fault:?}");
    assert_eq!(lines, ["line-0001", "line-0002", "line-0003", "line-0004"]);
    assert_eq!(reader.bytes_consumed(), 40);
    // The manifest records the ETag of the read for a file read to its end.
    assert_eq!(reader.etag().as_deref(), Some("etag-of-read"));
    let ranges: Vec<Option<String>> = (0..3).map(|i| header(&replay, i, "range")).collect();
    assert_eq!(
        ranges,
        [
            Some("bytes=0-15".to_owned()),
            Some("bytes=16-31".to_owned()),
            Some("bytes=32-39".to_owned())
        ]
    );
    for i in 0..3 {
        assert_eq!(
            header(&replay, i, "if-match").as_deref(),
            Some("\"listed-etag\""),
            "request {i}"
        );
    }
    assert_eq!(replay.actual_requests().count(), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn faults_on_the_first_range_are_rendered_before_the_first_byte() {
    let kms_disabled = "arn:aws:kms:us-east-1:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab \
                        is disabled.";
    let get_denied = "User: arn:aws:sts::111122223333:assumed-role/extenddb/server is not \
                      authorized to perform: s3:GetObject on resource: \
                      \"arn:aws:s3:::loads/in/part-0.json\" with an explicit deny in a \
                      resource-based policy";
    let decrypt_denied = "User: arn:aws:sts::111122223333:assumed-role/extenddb/server is not \
                          authorized to perform: kms:Decrypt on this resource because the \
                          resource does not exist in this Region, no resource-based policies \
                          allow access, or a resource-based policy explicitly denies access";
    let cases = [
        (403, "AccessDenied", get_denied, "S3AccessDenied"),
        (
            404,
            "NoSuchKey",
            "The specified key does not exist.",
            "S3NoSuchKey",
        ),
        (
            400,
            "KMS.DisabledException",
            kms_disabled,
            "KmsDisabledException",
        ),
        (403, "AccessDenied", decrypt_denied, "S3AccessDenied"),
        (
            400,
            "KMS.KMSInvalidStateException",
            "The key is pending deletion.",
            "KmsInvalidStateException",
        ),
        (
            400,
            "KMS.NotFoundException",
            "Invalid keyId 'alias/gone'",
            "KmsNotFoundException",
        ),
        (
            403,
            "InvalidObjectState",
            "The operation is not valid for the object's storage class",
            "S3InvalidObjectState",
        ),
    ];
    for (status, code, message, expected) in cases {
        let (reader, _replay) = open_with(
            vec![s3_error(status, code, message)],
            16,
            LINES.len(),
            InputCodec::None,
        )
        .await;
        let fault = fault(reader.err().expect(code));
        assert_eq!(fault.code, expected, "{code}");
        assert_eq!(
            fault.message,
            format!("{message}{}", amazon_s3_tail(status, code))
        );
        assert!(!fault.after_first_byte, "{code}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fault_on_a_later_range_is_after_the_first_byte() {
    for (status, code, message, expected) in [
        (403, "AccessDenied", "Access Denied", "S3AccessDenied"),
        (
            404,
            "NoSuchKey",
            "The specified key does not exist.",
            "S3NoSuchKey",
        ),
    ] {
        let mut responses = parts(LINES, 16, "e1");
        responses.truncate(1);
        responses.push(s3_error(status, code, message));
        let (reader, _replay) = open_with(responses, 16, LINES.len(), InputCodec::None).await;
        let mut reader = reader.unwrap();
        let (lines, error) = drain(&mut reader).await;
        assert_eq!(lines, ["line-0001"], "{code}");
        let fault = fault(error.expect(code));
        assert_eq!(fault.code, expected);
        assert_eq!(
            fault.message,
            format!("{message}{}", amazon_s3_tail(status, code))
        );
        assert!(fault.after_first_byte, "{code}");
        assert_eq!(reader.etag(), None, "an abandoned read reports no ETag");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mid_object_fault_under_gzip_and_zstd_keeps_its_code() {
    let content = many_lines();
    let compressed = [
        (InputCodec::Gzip, gzip(&content)),
        (
            InputCodec::Zstd,
            ruzstd::encoding::compress_to_vec(
                content.as_slice(),
                ruzstd::encoding::CompressionLevel::Fastest,
            ),
        ),
    ];
    for (codec, bytes) in compressed {
        let mut responses = parts(&bytes, 64, "e1");
        responses.truncate(1);
        responses.push(s3_error(403, "AccessDenied", "Access Denied"));
        let (reader, _replay) = open_with(responses, 64, bytes.len(), codec).await;
        let (_, error) = drain(&mut reader.unwrap()).await;
        let fault = fault(error.expect("a fault"));
        assert_eq!(fault.code, "S3AccessDenied", "{codec:?}");
        assert!(fault.after_first_byte, "{codec:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_short_body_is_internal_and_never_a_corrupt_stream() {
    // A body shorter than its Content-Length on the first range.
    let mut short = part(&LINES[..16], 0, LINES.len(), "e1");
    *short.body_mut() = SdkBody::from(LINES[..10].to_vec());
    let (reader, _replay) = open_with(vec![short], 16, LINES.len(), InputCodec::None).await;
    let error = reader.err().expect("refused");
    assert!(
        matches!(&error, SourceError::Io(io) if io.kind() == std::io::ErrorKind::Other),
        "{error:?}"
    );

    // A whole-object answer (no Content-Range) whose body ends before its
    // Content-Length.
    let whole = with_headers(
        HttpResponse::new(200.try_into().unwrap(), SdkBody::from(LINES[..10].to_vec())),
        &[("content-length", "40"), ("etag", "\"e1\"")],
    );
    let (reader, _replay) = open_with(vec![whole], 64, LINES.len(), InputCodec::None).await;
    let error = reader.err().expect("refused");
    assert!(
        matches!(&error, SourceError::Io(io) if io.kind() == std::io::ErrorKind::Other),
        "{error:?}"
    );

    // Under GZIP, a later range that ends before the range asked for while
    // the object goes on.
    let bytes = gzip(&many_lines());
    let mut responses = parts(&bytes, 64, "e1");
    responses.truncate(1);
    let cut = with_headers(
        HttpResponse::new(
            206.try_into().unwrap(),
            SdkBody::from(bytes[64..100].to_vec()),
        ),
        &[
            ("content-range", &format!("bytes 64-99/{}", bytes.len())),
            ("etag", "\"e1\""),
        ],
    );
    responses.push(cut);
    let (reader, _replay) = open_with(responses, 64, bytes.len(), InputCodec::Gzip).await;
    let (_, error) = drain(&mut reader.unwrap()).await;
    let error = error.expect("a fault");
    assert!(
        matches!(&error, SourceError::Io(io) if io.kind() == std::io::ErrorKind::Other),
        "{error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_object_shorter_than_enumerated_ends_early() {
    // Row 11: a range past the object's end is end of file.
    let (reader, _replay) = open_with(
        vec![s3_error(
            416,
            "InvalidRange",
            "The requested range is not satisfiable",
        )],
        16,
        LINES.len(),
        InputCodec::None,
    )
    .await;
    let mut reader = reader.unwrap();
    let (lines, error) = drain(&mut reader).await;
    assert!(lines.is_empty() && error.is_none(), "{lines:?} {error:?}");
    assert_eq!(reader.etag().as_deref(), Some("listed-etag"));

    // An object that shrank answers fewer bytes with its new total.
    let (reader, replay) = open_with(
        vec![part(&LINES[..20], 0, 20, "e2")],
        64,
        LINES.len(),
        InputCodec::None,
    )
    .await;
    let mut reader = reader.unwrap();
    let (lines, error) = drain(&mut reader).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(lines, ["line-0001", "line-0002"]);
    assert_eq!(replay.actual_requests().count(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_and_unknown_errors_on_a_read_are_internal() {
    for response in [
        s3_error(500, "InternalError", "We encountered an internal error."),
        s3_error(503, "SlowDown", "Please reduce your request rate."),
        s3_error(400, "SomeFutureCode", "Something new."),
    ] {
        let (reader, _replay) = open_with(vec![response], 16, LINES.len(), InputCodec::None).await;
        let error = reader.err().expect("refused");
        assert!(
            matches!(&error, SourceError::Io(io) if io.kind() == std::io::ErrorKind::Other),
            "{error:?}"
        );
    }
    // The replay client has no answer: a dispatch failure.
    let (reader, _replay) = open_with(Vec::new(), 16, LINES.len(), InputCodec::None).await;
    assert!(matches!(reader.err(), Some(SourceError::Io(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_expected_owner_travels_on_every_range_and_zero_byte_objects_read_one_byte() {
    let (clients, replay) = clients(parts(LINES, 16, "e1"));
    let mut reader = open(
        shared(clients.with_part_bytes(16)),
        location("loads", Some("444455556666")),
        &input_file("in/part-0.json", LINES.len(), None),
        InputCodec::None,
    )
    .await
    .unwrap();
    drain(&mut reader).await;
    for i in 0..3 {
        assert_eq!(
            header(&replay, i, "x-amz-expected-bucket-owner").as_deref(),
            Some("444455556666")
        );
    }

    // An endpoint that ignores Range answers an empty object whole.
    let empty = with_headers(
        HttpResponse::new(200.try_into().unwrap(), SdkBody::empty()),
        &[
            ("content-length", "0"),
            ("etag", "\"d41d8cd98f00b204e9800998ecf8427e\""),
        ],
    );
    let (reader, replay) = open_with(vec![empty], 16, 0, InputCodec::None).await;
    let mut reader = reader.unwrap();
    let (lines, error) = drain(&mut reader).await;
    assert!(lines.is_empty() && error.is_none());
    assert_eq!(header(&replay, 0, "range").as_deref(), Some("bytes=0-0"));
    assert_eq!(
        reader.etag().as_deref(),
        Some("d41d8cd98f00b204e9800998ecf8427e")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_zero_size_record_is_read_as_one_byte_whatever_the_object_holds() {
    let grown = vec![b'x'; 1 << 20];
    let total = grown.len().to_string();
    // S3's 416 for an empty object is end of file.
    let (reader, replay) = open_with(
        vec![s3_error(
            416,
            "InvalidRange",
            "The requested range is not satisfiable",
        )],
        16,
        0,
        InputCodec::None,
    )
    .await;
    let (lines, error) = drain(&mut reader.unwrap()).await;
    assert!(lines.is_empty() && error.is_none(), "{lines:?} {error:?}");
    assert_eq!(header(&replay, 0, "range").as_deref(), Some("bytes=0-0"));

    // The object grew after it was listed and the answer is longer than
    // the byte asked for: one byte is kept, and the read stays empty.
    let longer = with_headers(
        HttpResponse::new(206.try_into().unwrap(), SdkBody::from(grown.clone())),
        &[
            ("content-length", total.as_str()),
            ("content-range", &format!("bytes 0-0/{total}")),
            ("etag", "\"e2\""),
        ],
    );
    let (reader, replay) = open_with(vec![longer], 16, 0, InputCodec::None).await;
    let mut reader = reader.unwrap();
    let (lines, error) = drain(&mut reader).await;
    assert!(lines.is_empty() && error.is_none(), "{lines:?} {error:?}");
    assert_eq!(reader.bytes_consumed(), 0);
    assert_eq!(header(&replay, 0, "range").as_deref(), Some("bytes=0-0"));
    assert_eq!(replay.actual_requests().count(), 1);

    // The same object served whole by an endpoint that ignores Range is
    // the endpoint's fault.
    let whole = with_headers(
        HttpResponse::new(200.try_into().unwrap(), SdkBody::from(grown)),
        &[("content-length", total.as_str()), ("etag", "\"e2\"")],
    );
    let (reader, replay) = open_with(vec![whole], 16, 0, InputCodec::None).await;
    let error = reader.err().expect("refused");
    assert!(
        matches!(&error, SourceError::Io(io) if io.kind() == std::io::ErrorKind::Other),
        "{error:?}"
    );
    assert_eq!(header(&replay, 0, "range").as_deref(), Some("bytes=0-0"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_whole_object_answer_longer_than_the_range_is_the_endpoints_fault() {
    // The first range asks for 16 bytes of a 40-byte object; an endpoint
    // that ignores Range answers all 40 without Content-Range.
    let whole = with_headers(
        HttpResponse::new(200.try_into().unwrap(), SdkBody::from(LINES.to_vec())),
        &[("content-length", "40"), ("etag", "\"e1\"")],
    );
    let (reader, replay) = open_with(vec![whole], 16, LINES.len(), InputCodec::None).await;
    let error = reader.err().expect("refused");
    assert!(
        matches!(&error, SourceError::Io(io) if io.kind() == std::io::ErrorKind::Other),
        "{error:?}"
    );
    assert_eq!(header(&replay, 0, "range").as_deref(), Some("bytes=0-15"));

    // A whole-object answer that fits in the range is the object.
    let fits = with_headers(
        HttpResponse::new(200.try_into().unwrap(), SdkBody::from(LINES.to_vec())),
        &[("content-length", "40"), ("etag", "\"e1\"")],
    );
    let (reader, _replay) = open_with(vec![fits], 64, LINES.len(), InputCodec::None).await;
    let (lines, error) = drain(&mut reader.unwrap()).await;
    assert!(error.is_none(), "{error:?}");
    assert_eq!(lines.len(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_longer_than_the_range_is_read_no_further_than_one_byte_past_it() {
    // Over a real connection: the zero-size record's GET is answered with
    // 64 MiB and no Content-Range. The read takes two bytes, refuses the
    // answer and drops the connection, so the listener can write no more
    // than the socket buffers hold.
    let total: u64 = 64 << 20;
    let stub = Stub::start(vec![Answer::Flood {
        head: stub::head(
            "200 OK",
            &[("content-length", &total.to_string()), ("etag", "\"e2\"")],
        ),
        total,
    }])
    .await;
    let clients = shared(stub_clients(&stub.url, stub::short_timeouts()));
    let opened = open(
        clients,
        location("loads", None),
        &input_file("in/empty.json", 0, None),
        InputCodec::None,
    )
    .await;
    assert!(
        matches!(&opened, Err(SourceError::Io(io)) if io.kind() == std::io::ErrorKind::Other),
        "{:?}",
        opened.err()
    );
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(stub.header(0, "range").as_deref(), Some("bytes=0-0"));
    assert!(
        stub.flooded() < 16 << 20,
        "{} bytes were sent before the read stopped",
        stub.flooded()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn line_count_reads_the_object_and_etag_prefers_the_listed_tag() {
    let (counting, _replay) = clients(parts(LINES, 16, "e1"));
    let count = line_count(
        shared(counting.with_part_bytes(16)),
        location("loads", None),
        &input_file("in/part-0.json", LINES.len(), None),
        InputCodec::None,
    )
    .await
    .unwrap();
    assert_eq!(count, 4);

    let (unused, replay) = clients(Vec::new());
    let listed = etag(
        &shared(unused),
        &location("loads", None),
        &input_file("in/part-0.json", 40, Some("listed")),
    )
    .await
    .unwrap();
    assert_eq!(listed, "listed");
    assert_eq!(replay.actual_requests().count(), 0);

    let (probing, replay) = clients(vec![part(&LINES[..1], 0, 40, "probed")]);
    let probed = etag(
        &shared(probing),
        &location("loads", None),
        &input_file("in/part-0.json", 40, None),
    )
    .await
    .unwrap();
    assert_eq!(probed, "probed");
    assert_eq!(header(&replay, 0, "range").as_deref(), Some("bytes=0-0"));

    // A record listed at size 0 is probed with the same one-byte range.
    let (probing, replay) = clients(vec![s3_error(
        416,
        "InvalidRange",
        "The requested range is not satisfiable",
    )]);
    let probed = etag(
        &shared(probing),
        &location("loads", None),
        &input_file("in/empty.json", 0, None),
    )
    .await
    .unwrap();
    assert_eq!(probed, "");
    assert_eq!(header(&replay, 0, "range").as_deref(), Some("bytes=0-0"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_etag_probe_takes_the_tag_of_a_whole_object_answer() {
    // A service that ignores Range answers the probe with the whole object
    // and no Content-Range; only the tag is used.
    let whole = with_headers(
        HttpResponse::new(200.try_into().unwrap(), SdkBody::from(LINES.to_vec())),
        &[("content-length", "40"), ("etag", "\"whole\"")],
    );
    let (probing, replay) = clients(vec![whole]);
    let probed = etag(
        &shared(probing),
        &location("loads", None),
        &input_file("in/part-0.json", 40, None),
    )
    .await
    .unwrap();
    assert_eq!(probed, "whole");
    assert_eq!(replay.actual_requests().count(), 1);
    assert_eq!(header(&replay, 0, "range").as_deref(), Some("bytes=0-0"));
}
