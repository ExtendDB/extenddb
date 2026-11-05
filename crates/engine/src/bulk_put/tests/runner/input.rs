// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Gzip input.

#[tokio::test]
async fn gzip_input_is_decompressed_and_counted_in_compressed_bytes() {
    let h = Harness::new().await;
    let plain = file_of(&plain_rows(5));
    let compressed = gzip_bytes(&plain);
    let compressed_len = i64::try_from(compressed.len()).unwrap();
    h.write_input("part-000.json.gz", &compressed);
    let files = h.enumerate().await;
    assert_eq!(files[0].size, compressed_len, "enumeration sees gz size");
    let record = h
        .run_to_end(files, input_config(true), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 5);
    assert_eq!(h.count_items().await, 5);
    assert_eq!(record.total_input_bytes, Some(compressed_len));
    assert_eq!(
        Some(record.processed_input_bytes),
        record.total_input_bytes,
        "byte counters are compressed bytes for GZIP input"
    );
}

/// A zstd input is decompressed while it is read; the byte counters and the
/// manifest etag describe the compressed object (measured on the service: a
/// 65-byte zstd file of three rows ends COMPLETED
/// with TotalInputBytes 65 and the manifest etag of the compressed bytes).
#[tokio::test]
async fn zstd_input_is_decompressed_and_counted_in_compressed_bytes() {
    let h = Harness::new().await;
    let plain = file_of(&plain_rows(5));
    let compressed = zstd_bytes(&plain);
    let compressed_len = i64::try_from(compressed.len()).unwrap();
    h.write_input("part-000.json.zst", &compressed);
    let files = h.enumerate().await;
    assert_eq!(files[0].size, compressed_len, "enumeration sees zst size");
    let record = h
        .run_to_end(files, zstd_input_config(), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 5);
    assert_eq!(h.count_items().await, 5);
    assert_eq!(record.total_input_bytes, Some(compressed_len));
    assert_eq!(
        Some(record.processed_input_bytes),
        record.total_input_bytes,
        "byte counters are compressed bytes for ZSTD input"
    );
    let manifest = h
        .read_manifest(&record.operation_id)
        .expect("manifest written");
    assert_eq!(manifest[0]["processedRanges"], json!([[0, 4]]));
    assert_eq!(manifest[0]["etag"], json!(h.input_md5("part-000.json.zst")));
}

/// A zstd frame that does not decode is one failed row with the
/// decompression message, its bytes count as processed input, the run
/// continues with the next file, and the operation ends by its other rows
/// (measured on the service: a truncated 32-byte frame beside a good 65-byte
/// file ends COMPLETED_WITH_ERRORS, 4 processed, 1 failed, ProcessedInputBytes
/// 97 of 97, the truncated file PARTIALLY_PROCESSED with no ranges).
#[tokio::test]
async fn a_truncated_zstd_frame_fails_its_file_and_the_operation_completes_with_errors() {
    let h = Harness::new().await;
    let whole = zstd_bytes(&file_of(&plain_rows(3)));
    h.write_input("part-000.json.zst", &whole[..whole.len() / 2]);
    h.write_input(
        "part-001.json.zst",
        &zstd_bytes(&file_of(&(3..7).map(row).collect::<Vec<_>>())),
    );
    let files = h.enumerate().await;
    let total: i64 = files.iter().map(|f| f.size).sum();
    let record = h
        .run_to_end(files, zstd_input_config(), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.failure_code, None);
    assert_eq!(
        record.processed_item_count, 5,
        "one for the undecodable file, four rows"
    );
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(h.count_items().await, 4);
    assert_eq!(
        record.processed_input_bytes, total,
        "the undecodable file's bytes are processed input"
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

/// A frame of many blocks cut short: the rows of the blocks before the cut
/// are applied, the cut row is the one failed row with the decompression
/// message, and the manifest range ends at the row before it (measured on
/// the service: a 20,000-row frame cut at 90 percent of its bytes ends
/// COMPLETED_WITH_ERRORS with 16,990 rows on the table, 16,991 processed,
/// 1 failed, processedRanges [[0, 16989]], every byte counted).
#[tokio::test]
async fn a_zstd_frame_cut_mid_way_applies_the_rows_before_the_cut_and_fails_the_cut_row() {
    let h = Harness::new().await;
    // 20,000 rows compress to about ten 128 KiB blocks (see the source
    // tests), so the cut lands after several complete blocks.
    let rows: Vec<String> = (0..20000).map(row).collect();
    let whole = zstd_bytes(&file_of(&rows));
    let truncated = &whole[..whole.len() * 9 / 10];
    h.write_input("part-000.json.zst", truncated);
    let files = h.enumerate().await;
    let size = files[0].size;
    let record = h
        .run_to_end(files, zstd_input_config(), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    let applied = i64::try_from(h.count_items().await).unwrap();
    assert!(
        applied > 0 && applied < 20000,
        "the complete blocks' rows are applied ({applied}), the rest are not"
    );
    assert_eq!(record.processed_item_count, applied + 1);
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(record.processed_input_bytes, size);
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!([crate::bulk_put::rows::NOT_COMPRESSED_MESSAGE])
    );
    let manifest = h
        .read_manifest(&record.operation_id)
        .expect("manifest written");
    assert_eq!(
        manifest[0]["fileProcessingStatus"],
        json!("PARTIALLY_PROCESSED")
    );
    assert_eq!(manifest[0]["processedRanges"], json!([[0, applied - 1]]));
}

/// Under `ZSTD`, a file without the zstd magic is not a compressed object
/// and takes the same path as a plain file under `GZIP`: FAILED with the
/// validation pair, bytes excluded (measured on the service for a plain file
/// and for gzip bytes under ZSTD, and for zstd bytes under GZIP).
#[tokio::test]
async fn gzip_bytes_under_zstd_are_refused_as_not_compressed() {
    let h = Harness::new().await;
    h.write_input("part-000.json.gz", &gzip_bytes(&file_of(&plain_rows(3))));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, zstd_input_config(), both_reports(), config())
        .await;

    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(record.processed_item_count, 1);
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(record.processed_input_bytes, 0);
    assert_eq!(h.count_items().await, 0);
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(
        report[0]["errorMessages"],
        json!([crate::bulk_put::rows::NOT_COMPRESSED_MESSAGE])
    );
}

// Input enumeration and format.

#[tokio::test]
async fn a_file_grown_after_enumeration_contributes_only_the_enumerated_rows() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(5)));
    let files = h.enumerate().await;
    let enumerated_size = files[0].size;

    // The file grows after enumeration; the stored list is authoritative.
    {
        use std::io::Write;
        let path = h
            .store
            .0
            .join(ACCOUNT)
            .join(IN_BUCKET)
            .join("part-000.json");
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(&file_of(&(5..10).map(row).collect::<Vec<_>>()))
            .unwrap();
    }

    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(
        record.processed_item_count, 5,
        "only the enumerated rows are read"
    );
    assert!(h.get("k4").await.is_some());
    assert!(h.get("k5").await.is_none(), "appended rows are never read");
    assert_eq!(h.count_items().await, 5);
    assert_eq!(record.total_input_bytes, Some(enumerated_size));
    assert_eq!(record.processed_input_bytes, enumerated_size);
}

#[tokio::test]
async fn a_two_member_gzip_file_is_read_whole() {
    let h = Harness::new().await;
    let mut bytes = gzip_bytes(&file_of(&plain_rows(5)));
    bytes.extend(gzip_bytes(&file_of(&(5..10).map(row).collect::<Vec<_>>())));
    h.write_input("part-000.json.gz", &bytes);
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(true), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED");
    assert_eq!(
        record.processed_item_count, 10,
        "both concatenated gzip members are read"
    );
    assert_eq!(h.count_items().await, 10);
    assert_eq!(Some(record.processed_input_bytes), record.total_input_bytes);
}

/// A gzip file cut before its trailer is abandoned at the cut with its own
/// measured message, counted as one processed and failed row, and the
/// operation continues with the next file (measured on the service: a half
/// file alone ends COMPLETED_WITH_ERRORS 1/1; followed by a whole file,
/// 4 processed, 1 failed). The rows decoded before the cut are written.
#[tokio::test]
async fn a_truncated_gzip_file_is_abandoned_and_the_next_file_is_read() {
    let h = Harness::new().await;
    let whole = gzip_bytes(&file_of(&plain_rows(50)));
    let cut = &whole[..whole.len() / 2];
    h.write_input("part-000.json.gz", cut);
    h.write_input(
        "part-001.json.gz",
        &gzip_bytes(&file_of(&(50..53).map(row).collect::<Vec<_>>())),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(true), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(
        record.failed_item_count, 1,
        "the cut counts as one failed row"
    );
    assert!(
        record.processed_item_count >= 4,
        "rows before the cut and the next file count"
    );
    assert!(
        h.get("k50").await.is_some(),
        "the next file is read after the abandon"
    );
    assert!(h.get("k52").await.is_some());
    assert!(
        h.get("k49").await.is_none(),
        "nothing past the cut is written"
    );
    assert_eq!(Some(record.processed_input_bytes), record.total_input_bytes);
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!([crate::bulk_put::rows::CORRUPT_STREAM_MESSAGE])
    );
    assert!(report[0]["itemLocation"].get("itemIndex").is_none());
}

#[tokio::test]
async fn an_empty_input_file_is_finished_and_the_run_continues() {
    let h = Harness::new().await;
    h.write_input("a.json", &file_of(&plain_rows(3)));
    h.write_input("b.json", b"");
    h.write_input("c.json", &file_of(&(3..6).map(row).collect::<Vec<_>>()));
    let files = h.enumerate().await;
    assert_eq!(files.len(), 3, "the empty file is enumerated");
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 6);
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(h.count_items().await, 6);
    assert_eq!(Some(record.processed_input_bytes), record.total_input_bytes);
}
