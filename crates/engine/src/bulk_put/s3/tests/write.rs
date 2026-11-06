// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The output side: the write check at the first report record and at the
//! manifest, the Start-time encryption check, the multipart report writer
//! with rotation, and the manifest.

use std::io::Read;

use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::primitives::SdkBody;
use serde_json::json;

use extenddb_storage::ReportKind;

use super::*;
use crate::bulk_put::s3::S3Sink;
use crate::bulk_put::source::{BucketRef, BulkSink, OutputSse, SinkError};

pub(super) const OPERATION: &str = "01792940454300-c5f2ec39";
const ACCOUNT: &str = "000000000001";

fn ok(status: u16, body: &str, headers: &[(&str, &str)]) -> HttpResponse {
    with_headers(
        HttpResponse::new(status.try_into().unwrap(), SdkBody::from(body.to_owned())),
        headers,
    )
}

pub(super) fn created(upload_id: &str) -> HttpResponse {
    ok(
        200,
        &format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult>\
             <Bucket>out</Bucket><Key>k</Key><UploadId>{upload_id}</UploadId>\
             </InitiateMultipartUploadResult>"
        ),
        &[],
    )
}

pub(super) fn part_uploaded(etag: &str) -> HttpResponse {
    ok(200, "", &[("etag", &format!("\"{etag}\""))])
}

pub(super) fn completed() -> HttpResponse {
    ok(
        200,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult>\
         <Bucket>out</Bucket><Key>k</Key><ETag>\"done-2\"</ETag></CompleteMultipartUploadResult>",
        &[],
    )
}

fn aborted() -> HttpResponse {
    ok(204, "", &[])
}

pub(super) fn put() -> HttpResponse {
    ok(200, "", &[("etag", "\"put\"")])
}

pub(super) fn s3_sink(
    responses: Vec<HttpResponse>,
    part_bytes: u64,
) -> (S3Sink, StaticReplayClient) {
    let (clients, replay) = clients(responses);
    (
        S3Sink {
            clients: shared(clients.with_part_bytes(part_bytes)),
        },
        replay,
    )
}

pub(super) fn out(owner: Option<&str>) -> BucketRef<'_> {
    BucketRef {
        account: ACCOUNT,
        bucket: "out",
        expected_owner: owner,
    }
}

fn report_key(index: u32) -> String {
    crate::bulk_put::report::report_key("reports", ACCOUNT, OPERATION, ReportKind::Error, index)
}

fn manifest_key() -> String {
    crate::bulk_put::report::manifest_key("reports", ACCOUNT, OPERATION)
}

pub(super) fn method_and_uri(replay: &StaticReplayClient, index: usize) -> (String, String) {
    let request = replay.actual_requests().nth(index).expect("request");
    (request.method().to_owned(), request.uri().to_owned())
}

fn body(replay: &StaticReplayClient, index: usize) -> Vec<u8> {
    let request = replay.actual_requests().nth(index).expect("request");
    request.body().bytes().expect("an in-memory body").to_vec()
}

fn gunzip(bytes: &[u8]) -> String {
    let mut text = String::new();
    flate2::read::MultiGzDecoder::new(bytes)
        .read_to_string(&mut text)
        .unwrap();
    text
}

pub(super) fn store_fault(error: SinkError) -> crate::bulk_put::source::StoreFault {
    match error {
        SinkError::Store(fault) => fault,
        other => panic!("expected a store fault, got {other:?}"),
    }
}

/// `count` report records with pseudo-random content, so the gzip stream
/// grows with the input and parts fill while pages are written.
pub(super) fn records(count: usize) -> Vec<serde_json::Value> {
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut noise = move || -> String {
        (0..4)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                format!("{state:016x}")
            })
            .collect()
    };
    (0..count)
        .map(|i| json!({"itemLocation": {"itemIndex": i}, "errorMessages": [format!("row {i:04} failed {}", noise())]}))
        .collect()
}

/// The part size the multipart tests use.
pub(super) const PART: u64 = 16 * 1024;

#[tokio::test]
async fn the_first_record_check_starts_and_aborts_an_upload_with_owner_and_sse() {
    let (sink, replay) = s3_sink(vec![created("probe-1"), aborted()], 1 << 20);
    let sse = OutputSse::Kms {
        key_id: Some("alias/reports".to_owned()),
    };
    sink.check_location(&out(Some("444455556666")), &report_key(0), &sse)
        .await
        .unwrap();
    let (method, uri) = method_and_uri(&replay, 0);
    assert_eq!(method, "POST");
    assert!(uri.contains("output-00000.json.gz?uploads"), "{uri}");
    assert_eq!(
        header(&replay, 0, "x-amz-server-side-encryption").as_deref(),
        Some("aws:kms")
    );
    assert_eq!(
        header(&replay, 0, "x-amz-server-side-encryption-aws-kms-key-id").as_deref(),
        Some("alias/reports")
    );
    assert_eq!(
        header(&replay, 0, "x-amz-expected-bucket-owner").as_deref(),
        Some("444455556666")
    );
    let (method, uri) = method_and_uri(&replay, 1);
    assert_eq!(method, "DELETE");
    assert!(uri.contains("uploadId=probe-1"), "{uri}");
    assert_eq!(
        header(&replay, 1, "x-amz-expected-bucket-owner").as_deref(),
        Some("444455556666")
    );
}

#[tokio::test]
async fn first_record_check_faults_carry_the_report_writers_tail() {
    let put_denied = format!(
        "User: arn:aws:sts::111122223333:assumed-role/extenddb/server is not authorized to \
         perform: s3:PutObject on resource: \"arn:aws:s3:::out/{}\" with an explicit deny in a \
         resource-based policy",
        report_key(0)
    );
    let cases = [
        (
            404,
            "NoSuchBucket",
            "The specified bucket does not exist".to_owned(),
            "S3NoSuchBucket",
        ),
        (
            403,
            "AccessDenied",
            "Access Denied".to_owned(),
            "S3AccessDenied",
        ),
        (403, "AccessDenied", put_denied, "S3AccessDenied"),
        (
            400,
            "KMS.InvalidKeyUsageException",
            "The request was rejected because the specified KMS key is not enabled for this \
             operation."
                .to_owned(),
            "KmsInvalidKeyUsageException",
        ),
    ];
    for (status, code, message, expected) in cases {
        let (sink, replay) = s3_sink(vec![s3_error(status, code, &message)], 1 << 20);
        let fault = store_fault(
            sink.check_location(&out(None), &report_key(0), &OutputSse::None)
                .await
                .unwrap_err(),
        );
        assert_eq!(fault.code, expected, "{code}");
        assert_eq!(fault.message, format!("{message}{}", s3_tail(status)));
        assert!(!fault.after_first_byte);
        assert_eq!(
            replay.actual_requests().count(),
            1,
            "no abort without an upload"
        );
    }
}

#[tokio::test]
async fn manifest_check_faults_carry_the_amazon_s3_tail() {
    for (status, code, message, expected) in [
        (
            404,
            "NoSuchBucket",
            "The specified bucket does not exist",
            "S3NoSuchBucket",
        ),
        (403, "AccessDenied", "Access Denied", "S3AccessDenied"),
    ] {
        let (sink, _replay) = s3_sink(vec![s3_error(status, code, message)], 1 << 20);
        let fault = store_fault(
            sink.check_location(&out(None), &manifest_key(), &OutputSse::None)
                .await
                .unwrap_err(),
        );
        assert_eq!(fault.code, expected);
        assert_eq!(
            fault.message,
            format!("{message}{}", amazon_s3_tail(status, code))
        );
    }
}

#[tokio::test]
async fn the_start_encryption_check_reports_kms_faults_only() {
    let kms = OutputSse::Kms {
        key_id: Some("arn:aws:kms:us-east-1:111122223333:key/k1".to_owned()),
    };
    let disabled = "arn:aws:kms:us-east-1:111122223333:key/k1 is disabled.";
    let (sink, _replay) = s3_sink(
        vec![s3_error(400, "KMS.DisabledException", disabled)],
        1 << 20,
    );
    let fault = store_fault(
        sink.check_encryption(&out(None), &report_key(0), &kms)
            .await
            .unwrap_err(),
    );
    assert_eq!(fault.code, "KmsDisabledException");
    assert_eq!(
        fault.message,
        format!("{disabled}{}", amazon_s3_tail(400, "KMS.DisabledException"))
    );
    // A bucket fault is left to the first report record.
    let (sink, _replay) = s3_sink(
        vec![s3_error(
            404,
            "NoSuchBucket",
            "The specified bucket does not exist",
        )],
        1 << 20,
    );
    sink.check_encryption(&out(None), &report_key(0), &kms)
        .await
        .unwrap();
    // A usable key: the probe's upload is aborted.
    let (sink, replay) = s3_sink(vec![created("probe-2"), aborted()], 1 << 20);
    sink.check_encryption(&out(None), &report_key(0), &kms)
        .await
        .unwrap();
    assert_eq!(replay.actual_requests().count(), 2);
    // Without SSE-KMS nothing is sent.
    for sse in [OutputSse::None, OutputSse::Aes256] {
        let (sink, replay) = s3_sink(Vec::new(), 1 << 20);
        sink.check_encryption(&out(None), &report_key(0), &sse)
            .await
            .unwrap();
        assert_eq!(replay.actual_requests().count(), 0);
    }
}

#[tokio::test]
async fn a_report_that_never_fills_a_part_is_one_put_object_at_finish() {
    let (sink, replay) = s3_sink(vec![put()], 1 << 20);
    let mut writer = sink
        .open(
            &out(Some("444455556666")),
            "reports",
            OPERATION,
            ReportKind::Error,
            "lease",
            &OutputSse::Aes256,
        )
        .await
        .unwrap();
    writer.write_page(records(3)).await.unwrap();
    assert_eq!(
        writer.object_index(),
        0,
        "the open object is still object 0"
    );
    assert_eq!(
        replay.actual_requests().count(),
        0,
        "nothing is visible before finish"
    );
    let key = writer.finish().await.unwrap();
    assert_eq!(key, report_key(0));
    let (method, uri) = method_and_uri(&replay, 0);
    assert_eq!(method, "PUT");
    assert!(
        path_of(&uri).ends_with(&urlencoded_key(&report_key(0))),
        "{uri}"
    );
    assert_eq!(
        header(&replay, 0, "x-amz-server-side-encryption").as_deref(),
        Some("AES256")
    );
    assert_eq!(
        header(&replay, 0, "x-amz-expected-bucket-owner").as_deref(),
        Some("444455556666")
    );
    let text = gunzip(&body(&replay, 0));
    assert_eq!(text.lines().count(), 3);
    assert!(text.lines().next().unwrap().contains("row 0000 failed"));
}

/// The path of a report key as the SDK sends it: `:` escaped.
fn urlencoded_key(key: &str) -> String {
    key.replace(':', "%3A")
}

/// The path of a request URI, without its query.
fn path_of(uri: &str) -> &str {
    uri.split('?').next().unwrap_or(uri)
}

/// The compressed size of `pages` written the way the writer writes them.
pub(super) fn gzip_size(pages: &[Vec<serde_json::Value>]) -> usize {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    for page in pages {
        crate::bulk_put::report::encode_ndjson_page(&mut encoder, page).unwrap();
    }
    encoder.finish().unwrap().len()
}

#[tokio::test]
async fn a_large_report_is_uploaded_in_parts_and_completed_at_finish() {
    let pages = vec![records(2000), records(2000), records(2000)];
    let parts = gzip_size(&pages).div_ceil(usize::try_from(PART).unwrap());
    let mut responses = vec![created("up-0")];
    responses.extend((1..=parts).map(|i| part_uploaded(&format!("p{i}"))));
    responses.push(completed());
    let (sink, replay) = s3_sink(responses, PART);
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
    let before = replay.actual_requests().count();
    assert!(before >= 2, "parts are sent as they fill");
    let key = writer.finish().await.unwrap();
    assert_eq!(key, report_key(0));
    let count = replay.actual_requests().count();
    assert_eq!(count, parts + 2, "create, every part, complete");
    let (method, uri) = method_and_uri(&replay, 0);
    assert_eq!(method, "POST");
    assert!(uri.contains("?uploads"), "{uri}");
    assert_eq!(
        header(&replay, 0, "x-amz-server-side-encryption").as_deref(),
        Some("AES256")
    );
    for i in 1..=parts {
        let (method, uri) = method_and_uri(&replay, i);
        assert_eq!(method, "PUT");
        assert!(uri.contains(&format!("partNumber={i}")), "{uri}");
        assert!(uri.contains("uploadId=up-0"), "{uri}");
    }
    let (method, uri) = method_and_uri(&replay, parts + 1);
    assert_eq!(method, "POST");
    assert!(uri.contains("uploadId=up-0"), "{uri}");
    let complete = String::from_utf8(body(&replay, parts + 1)).unwrap();
    assert!(
        complete.contains(&format!("<PartNumber>{parts}</PartNumber>")),
        "{complete}"
    );
    assert!(complete.contains("p1"), "{complete}");
    // The parts reassemble into the report.
    let uploaded: Vec<u8> = (1..=parts).flat_map(|i| body(&replay, i)).collect();
    assert_eq!(gunzip(&uploaded).lines().count(), 6000);
}

#[tokio::test]
async fn rotation_closes_an_object_at_the_threshold_and_finish_installs_them_in_order() {
    let (sink, replay) = s3_sink(vec![put(), put()], 1 << 20);
    let mut writer = sink
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
    // Every page reaches the threshold once the gzip header is written,
    // so each page closes its object.
    writer.set_rotation_threshold(1);
    assert_eq!(writer.object_index(), 0, "the first page opens object 0");
    writer.write_page(records(2)).await.unwrap();
    assert_eq!(writer.open_object_bytes(), 0, "the page closed its object");
    assert_eq!(writer.object_index(), 1, "the next page opens object 1");
    writer.write_page(records(3)).await.unwrap();
    assert_eq!(writer.object_index(), 2, "after the second roll, object 2");
    assert_eq!(
        replay.actual_requests().count(),
        0,
        "nothing is visible before finish"
    );
    assert_eq!(writer.finish().await.unwrap(), report_key(0));
    let first = method_and_uri(&replay, 0).1;
    let second = method_and_uri(&replay, 1).1;
    assert!(
        path_of(&first).ends_with(&urlencoded_key(&report_key(0))),
        "{first}"
    );
    assert!(
        path_of(&second).ends_with(&urlencoded_key(&report_key(1))),
        "{second}"
    );
    assert_eq!(gunzip(&body(&replay, 0)).lines().count(), 2);
    assert_eq!(gunzip(&body(&replay, 1)).lines().count(), 3);
}

#[tokio::test]
async fn a_writer_dropped_without_finish_aborts_its_uploads() {
    // Enough part answers for the page; the abort takes the next answer.
    let mut responses = vec![created("up-9")];
    responses.extend((0..40).map(|i| part_uploaded(&format!("p{i}"))));
    let (sink, replay) = s3_sink(responses, PART);
    let mut writer = sink
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
    // One page whose gzip output passes one 64-byte part.
    writer.write_page(records(2000)).await.unwrap();
    assert!(replay.actual_requests().count() >= 2);
    drop(writer);
    for _ in 0..200 {
        if replay
            .actual_requests()
            .any(|request| request.method() == "DELETE")
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let deleted: Vec<String> = replay
        .actual_requests()
        .filter(|request| request.method() == "DELETE")
        .map(|request| request.uri().to_owned())
        .collect();
    assert_eq!(deleted.len(), 1, "{deleted:?}");
    assert!(deleted[0].contains("uploadId=up-9"), "{deleted:?}");
}

#[tokio::test]
async fn report_write_faults_carry_the_report_writers_tail() {
    // A part upload refused.
    let (sink, _replay) = s3_sink(
        vec![
            created("up-1"),
            s3_error(403, "AccessDenied", "Access Denied"),
            aborted(),
        ],
        PART,
    );
    let mut writer = sink
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
    let fault = store_fault(writer.write_page(records(2000)).await.unwrap_err());
    assert_eq!(fault.code, "S3AccessDenied");
    assert_eq!(fault.message, format!("Access Denied{}", s3_tail(403)));
    assert!(
        writer.write_page(records(1)).await.is_err(),
        "a failed writer writes no more"
    );
    drop(writer);

    // The single PutObject at finish finds the bucket gone.
    let (sink, _replay) = s3_sink(
        vec![s3_error(
            404,
            "NoSuchBucket",
            "The specified bucket does not exist",
        )],
        1 << 20,
    );
    let mut writer = sink
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
    writer.write_page(records(2)).await.unwrap();
    let fault = store_fault(writer.finish().await.unwrap_err());
    assert_eq!(fault.code, "S3NoSuchBucket");
    assert_eq!(
        fault.message,
        format!("The specified bucket does not exist{}", s3_tail(404))
    );
}

#[tokio::test]
async fn the_manifest_is_one_put_object_with_sse_and_its_faults_carry_the_amazon_s3_tail() {
    let lines = vec![
        json!({"inputSourceArn": "arn:aws:s3:::in/a.json", "fileProcessingStatus": "FULLY_PROCESSED", "processedRanges": [[0, 9]], "etag": "e1"}),
        json!({"inputSourceArn": "arn:aws:s3:::in/b.json", "fileProcessingStatus": "NOT_PROCESSED", "processedRanges": [], "etag": "e2"}),
    ];
    let sse = OutputSse::Kms { key_id: None };
    let (sink, replay) = s3_sink(vec![put()], 1 << 20);
    let key = sink
        .write_manifest(
            &out(None),
            "reports",
            OPERATION,
            "lease",
            &sse,
            lines.clone(),
        )
        .await
        .unwrap();
    assert_eq!(key, manifest_key());
    assert_eq!(
        header(&replay, 0, "x-amz-server-side-encryption").as_deref(),
        Some("aws:kms")
    );
    assert_eq!(
        header(&replay, 0, "x-amz-server-side-encryption-aws-kms-key-id"),
        None
    );
    let text = String::from_utf8(body(&replay, 0)).unwrap();
    assert_eq!(text.lines().count(), 2);
    assert!(
        text.starts_with("{\"inputSourceArn\":\"arn:aws:s3:::in/a.json\""),
        "{text}"
    );
    assert!(text.ends_with('\n'));

    let (sink, _replay) = s3_sink(
        vec![s3_error(
            404,
            "NoSuchBucket",
            "The specified bucket does not exist",
        )],
        1 << 20,
    );
    let fault = store_fault(
        sink.write_manifest(&out(None), "reports", OPERATION, "lease", &sse, lines)
            .await
            .unwrap_err(),
    );
    assert_eq!(fault.code, "S3NoSuchBucket");
    assert_eq!(
        fault.message,
        format!(
            "The specified bucket does not exist{}",
            amazon_s3_tail(404, "NoSuchBucket")
        )
    );
}
