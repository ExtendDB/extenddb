// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A smoke run of the S3 store against an S3-compatible service, gated on
//! `EXTENDDB_TEST_S3_SMOKE_ENDPOINT` (for example `http://127.0.0.1:7070`).
//! Credentials come from the standard AWS credential chain, as the server
//! takes them. Without the variable the test prints a skip line and
//! passes. It creates two buckets named after the process, reads an input
//! object through the source, writes a report in two parts plus a second
//! rotated object, a manifest, and a writer dropped without `finish`, and
//! reads every object back.

use std::io::Read;

use aws_sdk_s3::primitives::ByteStream;
use serde_json::json;

use extenddb_storage::ReportKind;

use super::*;
use crate::bulk_put::s3::{S3Sink, S3Source, connect};
use crate::bulk_put::source::{BucketRef, BulkSink, BulkSource, InputCodec, OutputSse};
use crate::bulk_put::store::S3StoreSettings;

const ACCOUNT: &str = "000000000001";

async fn get(clients: &S3Clients, bucket: &str, key: &str) -> Vec<u8> {
    clients
        .call(bucket, |client| {
            client.get_object().bucket(bucket).key(key).send()
        })
        .await
        .expect("object present")
        .body
        .collect()
        .await
        .unwrap()
        .to_vec()
}

pub(crate) fn gunzip(bytes: &[u8]) -> String {
    let mut text = String::new();
    flate2::read::MultiGzDecoder::new(bytes)
        .read_to_string(&mut text)
        .unwrap();
    text
}

/// Report records whose gzip stream is close to their size, so 9 MiB of
/// them fill more than one 8 MiB part.
pub(crate) fn noisy_records(count: usize, seed: u64) -> Vec<serde_json::Value> {
    let mut state = seed;
    (0..count)
        .map(|i| {
            let noise: String = (0..16)
                .map(|_| {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1_442_695_040_888_963_407);
                    format!("{state:016x}")
                })
                .collect();
            json!({"itemLocation": {"itemIndex": i}, "errorMessages": [noise]})
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_s3_store_reads_and_writes_against_an_s3_compatible_service() {
    let Ok(endpoint) = std::env::var("EXTENDDB_TEST_S3_SMOKE_ENDPOINT") else {
        println!("SKIP: EXTENDDB_TEST_S3_SMOKE_ENDPOINT is not set");
        return;
    };
    let settings = S3StoreSettings {
        region: "us-east-1".to_owned(),
        endpoint_url: Some(endpoint),
        force_path_style: true,
    };
    let clients = Arc::new(connect(&settings).await);
    let input = format!("smoke-in-{}", std::process::id());
    let output = format!("smoke-out-{}", std::process::id());
    for bucket in [&input, &output] {
        clients
            .call(bucket, |client| {
                client.create_bucket().bucket(bucket.as_str()).send()
            })
            .await
            .expect("bucket created");
    }
    let lines = b"{\"Item\":{\"pk\":{\"S\":\"a\"}}}\n{\"Item\":{\"pk\":{\"S\":\"b\"}}}\n";
    for key in ["in/part-0.json", "in/a b+c.json", "other/x.json"] {
        clients
            .call(&input, |client| {
                client
                    .put_object()
                    .bucket(&input)
                    .key(key)
                    .body(ByteStream::from(lines.to_vec()))
                    .send()
            })
            .await
            .expect("input written");
    }

    // The source: the bucket check, the listing, a read to the end.
    let source = S3Source {
        clients: Arc::clone(&clients),
    };
    let at = BucketRef::new(ACCOUNT, &input, None);
    source.check_bucket(&at, "in/").await.unwrap();
    let files = source.list(&at, "in/", 10).await.unwrap();
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    // The posix backend of the service the smoke run uses stores keys as
    // paths, so a doubled `/` is not part of this run; the replay tests
    // cover it.
    assert_eq!(paths, ["in/a b+c.json", "in/part-0.json"]);
    let listed = files[1].etag.clone().expect("listed ETag");
    let file = input_file(&files[1].path, lines.len(), Some(&listed));
    let mut reader = source.open(&at, &file, InputCodec::None).await.unwrap();
    let mut read = Vec::new();
    while let Some(line) = reader.next_line(1 << 20).await.unwrap() {
        read.push(line);
    }
    assert_eq!(read.len(), 2);
    assert_eq!(reader.etag().as_deref(), Some(listed.as_str()));
    assert_eq!(
        source
            .line_count(&at, &file, InputCodec::None)
            .await
            .unwrap(),
        2
    );
    let missing = BucketRef::new(ACCOUNT, "smoke-no-such-bucket", None);
    let error = source.check_bucket(&missing, "").await.unwrap_err();
    let fault = fault(error);
    assert_eq!(fault.code, "S3NoSuchBucket");
    assert!(
        fault
            .message
            .contains("(Service: Amazon S3; Status Code: 404; Error Code: NoSuchBucket"),
        "{}",
        fault.message
    );
    let gone = input_file("in/not-there.json", 10, None);
    let fault = fault_of_open(source.open(&at, &gone, InputCodec::None).await);
    assert_eq!(fault.code, "S3NoSuchKey");
    assert!(!fault.after_first_byte);

    // The sink: the write check, a report of two parts and a rotated
    // second object, the manifest.
    let sink = S3Sink {
        clients: Arc::clone(&clients),
    };
    let out = BucketRef::new(ACCOUNT, &output, None);
    let operation = "01792940454300-c5f2ec39";
    let first_key =
        crate::bulk_put::report::report_key("reports", ACCOUNT, operation, ReportKind::Error, 0);
    sink.check_location(&out, &first_key, &OutputSse::None)
        .await
        .unwrap();
    let mut writer = sink
        .open(
            &out,
            "reports",
            operation,
            ReportKind::Error,
            "lease",
            &OutputSse::None,
        )
        .await
        .unwrap();
    writer.set_rotation_threshold(9 * 1024 * 1024);
    let mut written = 0;
    while writer.open_object_bytes() < 9 * 1024 * 1024 && written < 200_000 {
        writer
            .write_page(noisy_records(2_000, written as u64 + 1))
            .await
            .unwrap();
        written += 2_000;
        if writer.open_object_bytes() == 0 {
            break;
        }
    }
    writer.write_page(noisy_records(10, 7)).await.unwrap();
    assert_eq!(writer.finish().await.unwrap(), first_key);
    let second_key =
        crate::bulk_put::report::report_key("reports", ACCOUNT, operation, ReportKind::Error, 1);
    let first = gunzip(&get(&clients, &output, &first_key).await);
    let second = gunzip(&get(&clients, &output, &second_key).await);
    assert_eq!(first.lines().count(), written);
    assert_eq!(second.lines().count(), 10);
    let manifest_lines = vec![
        json!({"inputSourceArn": "arn:aws:s3:::in/part-0.json", "fileProcessingStatus": "FULLY_PROCESSED", "processedRanges": [[0, 1]], "etag": listed}),
    ];
    let manifest_key = sink
        .write_manifest(
            &out,
            "reports",
            operation,
            "lease",
            &OutputSse::None,
            manifest_lines,
        )
        .await
        .unwrap();
    let manifest = String::from_utf8(get(&clients, &output, &manifest_key).await).unwrap();
    assert_eq!(manifest.lines().count(), 1);

    // A writer dropped without finish leaves no upload behind.
    let mut dropped = sink
        .open(
            &out,
            "dropped",
            operation,
            ReportKind::Error,
            "lease",
            &OutputSse::None,
        )
        .await
        .unwrap();
    for seed in 0..30 {
        dropped
            .write_page(noisy_records(2_000, 100 + seed))
            .await
            .unwrap();
    }
    drop(dropped);
    let mut left = usize::MAX;
    for _ in 0..100 {
        let uploads = clients
            .call(&output, |client| {
                client.list_multipart_uploads().bucket(&output).send()
            })
            .await
            .unwrap();
        left = uploads.uploads().len();
        if left == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(left, 0, "the dropped writer's upload was aborted");
    println!("smoke: {written} report records in two objects, input read, faults rendered");
}

fn fault_of_open(
    opened: Result<Box<dyn crate::bulk_put::source::LineReader>, SourceError>,
) -> StoreFault {
    match opened {
        Ok(_) => panic!("the open must fail"),
        Err(error) => fault(error),
    }
}
