// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The checksum settings of each mode, on the clients `clients_from`
//! builds: against a configured endpoint no request carries a checksum and
//! no read asks for one, and multipart uploads declare no algorithm (the
//! choice in `s3/write.rs`); on AWS S3 the SDK's defaults apply. The SDK
//! sends the checksum of an in-memory body in a header, not a trailer, so
//! both are looked for. Requests go through the replay client.

use std::sync::Arc;

use aws_sdk_s3::config::http::{HttpRequest, HttpResponse};
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::primitives::SdkBody;
use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};

use extenddb_storage::ReportKind;

use super::deadlines::{sdk_config, settings};
use super::read::{LINES, part};
use super::write::{
    OPERATION, PART, completed, created, gzip_size, method_and_uri, out, part_uploaded, put,
    records,
};
use super::*;
use crate::bulk_put::s3::S3Sink;
use crate::bulk_put::s3::client::clients_from;
use crate::bulk_put::s3::read::open;
use crate::bulk_put::source::{BulkSink, InputCodec, OutputSse};

/// The store's clients for `endpoint` over a replay client answering
/// `responses`, with no retries by the SDK or by the store.
fn mode_clients(
    endpoint: Option<&str>,
    responses: Vec<HttpResponse>,
) -> (Arc<S3Clients>, StaticReplayClient) {
    let replay = StaticReplayClient::new(
        responses
            .into_iter()
            .map(|response| ReplayEvent::new(HttpRequest::new(SdkBody::empty()), response))
            .collect(),
    );
    let http = replay.clone();
    let clients = clients_from(&settings(endpoint), &sdk_config(), move |builder| {
        builder
            .http_client(http)
            .retry_config(RetryConfig::disabled())
    })
    .with_retry(RetryPolicy::NONE)
    .with_part_bytes(PART);
    (shared(clients), replay)
}

/// The requests of each kind the store sends, in one mode.
struct Sent {
    /// CreateMultipartUpload, then each UploadPart, then
    /// CompleteMultipartUpload of one report object, then the PutObject of
    /// a report object that never filled a part.
    writes: StaticReplayClient,
    parts: usize,
    /// The GetObject of an input object's first range.
    read: StaticReplayClient,
}

async fn send(endpoint: Option<&str>) -> Sent {
    let pages = vec![records(2000)];
    let parts = gzip_size(&pages).div_ceil(usize::try_from(PART).unwrap());
    assert!(parts >= 2, "the report fills more than one part");
    let mut responses = vec![created("up-0")];
    responses.extend((1..=parts).map(|i| part_uploaded(&format!("p{i}"))));
    responses.push(completed());
    responses.push(put());
    let (clients, writes) = mode_clients(endpoint, responses);
    let sink = S3Sink { clients };
    let mut multipart = sink
        .open(
            &out(None),
            "reports",
            OPERATION,
            ReportKind::Error,
            "lease",
            &OutputSse::None,
        )
        .await
        .unwrap();
    for page in pages {
        multipart.write_page(page).await.unwrap();
    }
    multipart.finish().await.unwrap();
    let mut single = sink
        .open(
            &out(None),
            "reports",
            OPERATION,
            ReportKind::ConditionCheckFailure,
            "lease",
            &OutputSse::None,
        )
        .await
        .unwrap();
    single.write_page(records(2)).await.unwrap();
    single.finish().await.unwrap();

    let (reading, read) = mode_clients(endpoint, vec![part(LINES, 0, LINES.len(), "listed-etag")]);
    open(
        reading,
        location("loads", None),
        &input_file("in/part-0.json", LINES.len(), Some("listed-etag")),
        InputCodec::None,
    )
    .await
    .unwrap();

    let count = writes.actual_requests().count();
    assert_eq!(count, parts + 3, "create, every part, complete, put");
    let (method, uri) = method_and_uri(&writes, 0);
    assert!(
        method == "POST" && uri.ends_with("?uploads"),
        "{method} {uri}"
    );
    for i in 1..=parts {
        let (method, uri) = method_and_uri(&writes, i);
        assert!(
            method == "PUT" && uri.contains("x-id=UploadPart"),
            "{method} {uri}"
        );
    }
    let (method, uri) = method_and_uri(&writes, parts + 1);
    assert!(
        method == "POST" && uri.contains("uploadId=up-0"),
        "{method} {uri}"
    );
    let (method, uri) = method_and_uri(&writes, parts + 2);
    assert!(
        method == "PUT" && uri.contains("x-id=PutObject"),
        "{method} {uri}"
    );
    Sent {
        writes,
        parts,
        read,
    }
}

/// The checksum headers of one request: any `x-amz-checksum-*`,
/// `x-amz-sdk-checksum-algorithm` or `x-amz-trailer`, as `name: value`.
fn checksum_headers(replay: &StaticReplayClient, index: usize) -> Vec<String> {
    let request = replay.actual_requests().nth(index).expect("request");
    let mut found: Vec<String> = request
        .headers()
        .iter()
        .filter(|(name, _)| {
            name.starts_with("x-amz-checksum-")
                || *name == "x-amz-sdk-checksum-algorithm"
                || *name == "x-amz-trailer"
        })
        .map(|(name, value)| format!("{name}: {value}"))
        .collect();
    found.sort();
    found
}

#[tokio::test]
async fn against_a_configured_endpoint_no_request_carries_or_asks_for_a_checksum() {
    let sent = send(Some("http://127.0.0.1:9")).await;
    for i in 0..sent.parts + 3 {
        assert_eq!(
            checksum_headers(&sent.writes, i),
            Vec::<String>::new(),
            "write request {i}"
        );
    }
    assert_eq!(checksum_headers(&sent.read, 0), Vec::<String>::new());
}

#[tokio::test]
async fn on_aws_s3_writes_carry_the_sdks_crc32_and_reads_ask_for_checksums() {
    let sent = send(None).await;
    assert_eq!(
        checksum_headers(&sent.writes, 0),
        ["x-amz-checksum-algorithm: CRC32"],
        "CreateMultipartUpload"
    );
    let with_body = (1..=sent.parts).chain([sent.parts + 2]);
    for i in with_body {
        let found = checksum_headers(&sent.writes, i);
        assert_eq!(found.len(), 2, "request {i}: {found:?}");
        assert!(
            found[0].starts_with("x-amz-checksum-crc32: "),
            "request {i}: {found:?}"
        );
        assert_eq!(
            found[1], "x-amz-sdk-checksum-algorithm: CRC32",
            "request {i}"
        );
    }
    assert_eq!(
        checksum_headers(&sent.read, 0),
        ["x-amz-checksum-mode: ENABLED"]
    );
}
