// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// The manifest (measured on the service): one line
// per input file in enumeration order, written after the reports under the
// operation directory with no shard suffix; FULLY_PROCESSED with
// processedRanges [[0, last]] for a file read to its end, PARTIALLY_PROCESSED
// for an abandoned, cut-short or never-reached file, with the range of the
// rows counted before the stop and [] when none; etag is the S3 ETag, the
// MD5 of the object.

#[tokio::test]
async fn the_manifest_lists_each_file_fully_processed_with_its_range_and_etag() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(3)));
    h.write_input(
        "part-001.json",
        &file_of(&(3..7).map(row).collect::<Vec<_>>()),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED");
    let manifest = h
        .read_manifest(&record.operation_id)
        .expect("manifest written");
    assert_eq!(
        manifest,
        vec![
            json!({
                "inputSourceArn": format!("arn:aws:s3:::{IN_BUCKET}/part-000.json"),
                "fileProcessingStatus": "FULLY_PROCESSED",
                "processedRanges": [[0, 2]],
                "etag": h.input_md5("part-000.json"),
            }),
            json!({
                "inputSourceArn": format!("arn:aws:s3:::{IN_BUCKET}/part-001.json"),
                "fileProcessingStatus": "FULLY_PROCESSED",
                "processedRanges": [[0, 3]],
                "etag": h.input_md5("part-001.json"),
            }),
        ]
    );
    // The members are written in the service's order (measured), which the
    // parsed values above cannot see: read the first line's raw text.
    let raw = std::fs::read_to_string(h.manifest_path(&record.operation_id)).unwrap();
    let first = raw.lines().next().unwrap();
    assert_eq!(
        first,
        format!(
            "{{\"inputSourceArn\":\"arn:aws:s3:::{IN_BUCKET}/part-000.json\",\
             \"fileProcessingStatus\":\"FULLY_PROCESSED\",\"processedRanges\":[[0,2]],\
             \"etag\":\"{}\"}}",
            h.input_md5("part-000.json")
        )
    );
}

#[tokio::test]
async fn the_manifest_marks_an_abandoned_file_partially_processed_up_to_the_row_before_the_fault() {
    let h = Harness::new().await;
    let mut rows = plain_rows(5);
    rows.push(row_bad_type_tag(5));
    rows.extend((6..9).map(row));
    h.write_input("part-000.json", &file_of(&rows));
    h.write_input("part-001.json", &file_of(&[row_bad_type_tag(0), row(1)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    let manifest = h
        .read_manifest(&record.operation_id)
        .expect("manifest written");
    assert_eq!(
        manifest[0]["fileProcessingStatus"],
        json!("PARTIALLY_PROCESSED")
    );
    assert_eq!(manifest[0]["processedRanges"], json!([[0, 4]]));
    // Abandoned at its first row: no row was counted before the fault.
    assert_eq!(
        manifest[1]["fileProcessingStatus"],
        json!("PARTIALLY_PROCESSED")
    );
    assert_eq!(manifest[1]["processedRanges"], json!([]));
}

#[tokio::test]
async fn the_manifest_after_a_cancel_marks_the_cut_file_partial_and_the_unreached_file_unprocessed()
{
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(20)));
    h.write_input(
        "part-001.json",
        &file_of(&(20..23).map(row).collect::<Vec<_>>()),
    );
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let storage = Arc::clone(&h.storage);
    let op_for_action = op.clone();
    let action: LineAction = Arc::new(move || {
        let storage = Arc::clone(&storage);
        let op = op_for_action.clone();
        Box::pin(async move {
            storage
                .request_cancel(ACCOUNT, &op)
                .await
                .expect("request cancel");
        })
    });
    let source = Arc::new(ActAtSource {
        inner: h.fs_source(),
        at: 7,
        handed: Arc::new(AtomicU64::new(0)),
        fired: Arc::new(AtomicBool::new(false)),
        action,
    });
    let mut cfg = config();
    cfg.batch_size = 5;
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");
    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED");
    assert_eq!(record.processed_item_count, 10);
    let manifest = h
        .read_manifest(&record.operation_id)
        .expect("manifest written");
    assert_eq!(manifest.len(), 2);
    assert_eq!(
        manifest[0]["fileProcessingStatus"],
        json!("PARTIALLY_PROCESSED")
    );
    assert_eq!(manifest[0]["processedRanges"], json!([[0, 9]]));
    assert_eq!(
        manifest[1]["fileProcessingStatus"],
        json!("NOT_PROCESSED"),
        "the cancel landed before this file was opened, which is its own \
         status (measured on the service for an input object the \
         operation never opened)"
    );
    assert_eq!(manifest[1]["processedRanges"], json!([]));
    assert_eq!(manifest[1]["etag"], json!(h.input_md5("part-001.json")));
}

#[tokio::test]
async fn the_manifest_etag_of_a_gzip_file_is_the_md5_of_the_compressed_bytes() {
    let h = Harness::new().await;
    h.write_input("part-000.json.gz", &gzip_bytes(&file_of(&plain_rows(4))));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(true), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED");
    let manifest = h
        .read_manifest(&record.operation_id)
        .expect("manifest written");
    assert_eq!(manifest[0]["processedRanges"], json!([[0, 3]]));
    assert_eq!(manifest[0]["etag"], json!(h.input_md5("part-000.json.gz")));
}

#[tokio::test]
async fn the_manifest_after_a_resume_counts_a_file_the_earlier_incarnation_finished() {
    // The first incarnation finishes part-000 and crashes inside part-001
    // before any checkpoint of that file; the resumed incarnation never
    // reads part-000. Its row count and entity tag come from the figures
    // the end-of-file checkpoint stored on the record, so the manifest
    // reads no file a second time: neither `line_count` nor `etag` is
    // called on the source, for either file.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(4)));
    h.write_input(
        "part-001.json",
        &file_of(&(4..12).map(row).collect::<Vec<_>>()),
    );
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after: 6,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let mut cfg = config();
    cfg.batch_size = 25;
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }
    // The end-of-file checkpoint of part-000 stored its figures.
    let persisted = h.record(&op).await;
    assert_eq!(persisted.cursor_file_index, 1);
    assert_eq!(persisted.input_files[0].rows, Some(4));
    assert_eq!(
        persisted.input_files[0].etag.as_deref(),
        Some(h.input_md5("part-000.json").as_str())
    );
    assert_eq!(persisted.input_files[1].rows, None);
    assert_eq!(persisted.input_files[1].etag, None);

    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let source = Arc::new(CountingSource::new(h.fs_source()));
    let counts = Arc::clone(&source.counts);
    let etags = Arc::clone(&source.etags);
    let deps = h.deps_with("runner-2", config(), source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("resumed run");
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 12);
    let manifest = h
        .read_manifest(&record.operation_id)
        .expect("manifest written");
    assert_eq!(
        manifest[0]["fileProcessingStatus"],
        json!("FULLY_PROCESSED")
    );
    assert_eq!(manifest[0]["processedRanges"], json!([[0, 3]]));
    assert_eq!(manifest[0]["etag"], json!(h.input_md5("part-000.json")));
    assert_eq!(
        manifest[1]["fileProcessingStatus"],
        json!("FULLY_PROCESSED")
    );
    assert_eq!(manifest[1]["processedRanges"], json!([[0, 7]]));
    assert_eq!(manifest[1]["etag"], json!(h.input_md5("part-001.json")));
    assert_eq!(
        counts.load(Ordering::SeqCst),
        0,
        "no finished file is counted again for the manifest"
    );
    assert_eq!(
        etags.load(Ordering::SeqCst),
        0,
        "no finished file is hashed again for the manifest"
    );
}

/// A plain file under GZIP is not a compressed object: it counts one
/// processed and failed row with its own message, the run continues with
/// the next file, and the operation ends FAILED with the validation pair.
/// The refused file's bytes are not counted as processed input (measured
/// on the service: a plain 150-byte file alone ends FAILED, 1 processed, 1
/// failed, ProcessedInputBytes 12 of 150; followed by a 69-byte gzip file
/// of four rows, 5 processed, 1 failed, 81 of 219).
#[tokio::test]
async fn a_plain_file_under_gzip_is_refused_and_the_operation_ends_failed_after_the_rest() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(3)));
    h.write_input(
        "part-001.json.gz",
        &gzip_bytes(&file_of(&(3..7).map(row).collect::<Vec<_>>())),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(true), both_reports(), config())
        .await;

    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("Some of the items failed validation checks and were not ingested")
    );
    assert_eq!(
        record.processed_item_count, 5,
        "one for the refused file, four rows"
    );
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(
        h.count_items().await,
        4,
        "the gzip file after it is fully written"
    );
    assert!(h.get("k0").await.is_none());
    let gz_size = i64::try_from(
        std::fs::metadata(
            h.store
                .0
                .join(ACCOUNT)
                .join(IN_BUCKET)
                .join("part-001.json.gz"),
        )
        .unwrap()
        .len(),
    )
    .unwrap();
    assert_eq!(
        record.processed_input_bytes, gz_size,
        "the refused file's bytes are not processed input"
    );
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!([crate::bulk_put::rows::NOT_COMPRESSED_MESSAGE])
    );
    assert!(report[0]["itemLocation"].get("itemIndex").is_none());
    let manifest = h
        .read_manifest(&record.operation_id)
        .expect("manifest written");
    assert_eq!(
        manifest[0]["fileProcessingStatus"],
        json!("PARTIALLY_PROCESSED")
    );
    assert_eq!(manifest[0]["processedRanges"], json!([]));
    assert_eq!(
        manifest[1]["fileProcessingStatus"],
        json!("FULLY_PROCESSED")
    );
    assert_eq!(manifest[1]["processedRanges"], json!([[0, 3]]));
}

/// Gzip bytes read as text (compression NONE) are not JSON: the file is
/// abandoned at its first line with the corrupt-stream message (measured
/// "Unexpected token." for gzip bytes under NONE and under an
/// absent compression type).
#[tokio::test]
async fn gzip_bytes_under_no_compression_abandon_the_file_with_the_unexpected_token_message() {
    let h = Harness::new().await;
    h.write_input("part-000.json.gz", &gzip_bytes(&file_of(&plain_rows(3))));
    h.write_input(
        "part-001.json",
        &file_of(&(3..5).map(row).collect::<Vec<_>>()),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(h.count_items().await, 2);
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!([crate::bulk_put::rows::CORRUPT_STREAM_MESSAGE])
    );
}

/// Where the first invalid byte sits decides the message (measured
/// ): inside a string literal it is the UTF-8 message; at the
/// start of the line, between tokens, or in plain non-JSON text it is the
/// unexpected-token message.
#[tokio::test]
async fn an_invalid_byte_outside_a_string_literal_is_an_unexpected_token() {
    for (label, bad_line) in [
        (
            "leading bytes",
            b"\xff\xfe{\"Item\":{\"pk\":{\"S\":\"b\"},\"sk\":{\"S\":\"s\"}}}".to_vec(),
        ),
        (
            "between tokens",
            b"{\"Item\":{\"pk\":{\"S\":\"b\"},\"sk\":{\"S\":\"s\"}},\"x\":\xff\xfe}".to_vec(),
        ),
        ("plain text", b"nonsense text here".to_vec()),
        // Measured on the service: an odd count of
        // quotes before the invalid byte does not make it a string-literal
        // fault when the prefix is not a well-formed JSON prefix.
        ("csv-ish with one quote", b"name,\"value\xff\xfe".to_vec()),
        ("bareword then quote", b"plain junk x\"y\xff".to_vec()),
        (
            "complete object then stray quote",
            b"{\"Item\":{\"pk\":{\"S\":\"b\"},\"sk\":{\"S\":\"s\"}}}\"\xff".to_vec(),
        ),
    ] {
        let h = Harness::new().await;
        let mut bytes = file_of(&[row(0)]);
        bytes.extend_from_slice(&bad_line);
        bytes.push(b'\n');
        bytes.extend_from_slice(&file_of(&[row(2)]));
        h.write_input("part-000.json", &bytes);
        let files = h.enumerate().await;
        let record = h
            .run_to_end(files, input_config(false), both_reports(), config())
            .await;
        assert_eq!(record.status, "COMPLETED_WITH_ERRORS", "{label}");
        assert_eq!(record.processed_item_count, 2, "{label}");
        let report = h
            .read_report(&record.operation_id, ReportKind::Error)
            .expect("error report rendered");
        assert_eq!(
            report[0]["errorMessages"],
            json!([crate::bulk_put::rows::CORRUPT_STREAM_MESSAGE]),
            "{label}"
        );
    }
}

#[tokio::test]
async fn a_truncated_utf8_sequence_inside_a_string_is_the_utf8_fault() {
    let h = Harness::new().await;
    let mut bytes = file_of(&[row(0)]);
    bytes.extend_from_slice(b"{\"Item\":{\"pk\":{\"S\":\"b\xc3\"},\"sk\":{\"S\":\"s\"}}}\n");
    bytes.extend_from_slice(&file_of(&[row(2)]));
    h.write_input("part-000.json", &bytes);
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(
        report[0]["errorMessages"],
        json!([crate::bulk_put::rows::INVALID_UTF8_MESSAGE])
    );
}

/// A binary value that is not base64 did not decode; it abandons the file
/// like a bad type tag, the class `PutItem`'s `SerializationException`
/// answer places it in.
#[tokio::test]
async fn a_binary_value_that_is_not_base64_abandons_the_file() {
    let h = Harness::new().await;
    let rows = [
        row(0),
        r#"{"Item": {"pk": {"S": "k1"}, "sk": {"S": "s"}, "b": {"B": "!!!notbase64"}}}"#.to_owned(),
        row(2),
    ];
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 2);
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(h.count_items().await, 1);
    assert!(
        h.get("k2").await.is_none(),
        "the rest of the file is not read"
    );
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!([crate::bulk_put::rows::BINARY_NOT_BASE64_MESSAGE])
    );
}

// A file the run opened but ended inside before
// counting any row (cursor_row == 0) is PARTIALLY_PROCESSED with an empty
// range, told apart from a file the run never opened (NOT_PROCESSED) by the
// current_file_opened flag. The single input file's first read fails with an
// I/O error, which opens the file and fails the operation before any row is
// counted; the manifest marks it PARTIALLY_PROCESSED []. An I/O error is not
// a changed-file signal, so the file is not abandoned, and it is not an
// unavailable object, so it is not NOT_AVAILABLE.
#[tokio::test]
async fn a_file_opened_but_ended_before_its_first_row_is_partially_processed_with_no_range() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(5)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let source = Arc::new(IoAtSource {
        inner: h.fs_source(),
        at: 0,
        handed: Arc::new(AtomicU64::new(0)),
    });
    let deps = h.deps_with("runner-1", config(), source, Arc::new(h.fs_sink()));
    runner::run(&deps, record)
        .await
        .expect("the io failure is recorded as a terminal state, not returned");
    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("InternalServerError"));
    assert_eq!(record.cursor_row_index, 0, "no row was counted");
    let manifest = h.read_manifest(&op).expect("manifest written");
    assert_eq!(
        manifest[0]["fileProcessingStatus"],
        json!("PARTIALLY_PROCESSED"),
        "a file opened with no row counted is partially processed, not unprocessed"
    );
    assert_eq!(manifest[0]["processedRanges"], json!([]));
}
