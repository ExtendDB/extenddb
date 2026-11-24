// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// The abandon cut on a compressed object, measured on the hosted service
// the object is read through its decoder in
// one header read then 65,536-byte reads of the compressed stream, and an
// abandon leaves ProcessedInputBytes at the end of the read that held the
// fault. Bytes beyond that read are never counted and the operation ends
// FAILED; a fault whose read reaches the end of the object counts every
// byte and ends COMPLETED_WITH_ERRORS.

const PARSE_ABANDON: &str = "Expected DynamoDB type to be one of [S, N, B, BOOL, NULL, L, M, SS, \
                             NS, BS]. Remainder of the file will not be processed.";
const ITEMS_FAILED_VALIDATION: &str =
    "Some of the items failed validation checks and were not ingested";

/// 128 hex characters from a fixed-seed xorshift sequence, so the rows do
/// not compress away and a 2,000-row file stays well over one decoder read
/// when compressed.
fn noise(seed: &mut u64) -> String {
    let mut out = String::with_capacity(128);
    for _ in 0..8 {
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        out.push_str(&format!("{:016x}", *seed));
    }
    out
}

/// `rows` padded rows with the parse-level fault (an unknown type tag) at
/// `fault_at`, newline-terminated.
fn noisy_file_with_fault(rows: usize, fault_at: usize) -> Vec<u8> {
    let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
    let mut out = String::new();
    for index in 0..rows {
        let pad = noise(&mut seed);
        if index == fault_at {
            out.push_str(r#"{"Item":{"pk":{"S":"x"},"v":{"XX":"1"}}}"#);
        } else {
            out.push_str(&format!(
                r#"{{"Item":{{"pk":{{"S":"k{index}"}},"sk":{{"S":"s"}},"p":{{"S":"{pad}"}}}}}}"#
            ));
        }
        out.push('\n');
    }
    out.into_bytes()
}

#[tokio::test]
async fn a_gzip_abandon_on_line_one_stops_the_counter_after_the_first_decoder_read() {
    // A fault on line 1 of a 204,490-byte gzip object leaves
    // ProcessedInputBytes at 65,546 (the 10-byte header read plus one
    // 65,536-byte read) and the operation FAILED.
    let h = Harness::new().await;
    let compressed = gzip_bytes(&noisy_file_with_fault(2_000, 0));
    let size = i64::try_from(compressed.len()).unwrap();
    assert!(size > 2 * 65_536, "the fixture spans several decoder reads");
    h.write_input("part-000.json.gz", &compressed);
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(true), both_reports(), config())
        .await;

    assert_eq!(record.processed_input_bytes, 10 + 65_536);
    assert_eq!(record.total_input_bytes, Some(size));
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some(ITEMS_FAILED_VALIDATION)
    );
    assert_eq!(record.processed_item_count, 1);
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(
        h.count_items().await,
        0,
        "nothing past the fault is written"
    );
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report[0]["errorMessages"], json!([PARSE_ABANDON]));
}

#[tokio::test]
async fn a_gzip_abandon_after_one_decoder_read_stops_the_counter_after_the_second() {
    // A fault after 1,000 rows (about 100 KB of compressed input
    // consumed) of a 204,488-byte gzip object leaves the counter at 131,082.
    let h = Harness::new().await;
    let compressed = gzip_bytes(&noisy_file_with_fault(2_000, 1_000));
    let size = i64::try_from(compressed.len()).unwrap();
    h.write_input("part-000.json.gz", &compressed);
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(true), both_reports(), config())
        .await;

    assert_eq!(record.processed_input_bytes, 10 + 2 * 65_536);
    assert_eq!(record.total_input_bytes, Some(size));
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.processed_item_count, 1_001);
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(
        h.count_items().await,
        1_000,
        "the rows before the fault are written"
    );
}

#[tokio::test]
async fn a_gzip_abandon_inside_the_first_decoder_read_counts_every_byte_and_completes_with_errors()
{
    // A fault on line 1 of a 30,758-byte gzip object, more than
    // one plain read long and under one decoder read, counts the whole
    // object and ends COMPLETED_WITH_ERRORS.
    let h = Harness::new().await;
    let compressed = gzip_bytes(&noisy_file_with_fault(300, 0));
    let size = i64::try_from(compressed.len()).unwrap();
    assert!(
        size > 8_000 && size < 65_536,
        "between the plain and decoder read sizes"
    );
    h.write_input("part-000.json.gz", &compressed);
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(true), both_reports(), config())
        .await;

    assert_eq!(record.processed_input_bytes, size);
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.failure_code, None);
    assert_eq!(record.failed_item_count, 1);
}

#[tokio::test]
async fn a_gzip_abandon_on_the_last_line_counts_every_byte_and_completes_with_errors() {
    // A fault on the last line of a 204,561-byte gzip object
    // counts the whole object, writes the 2,000 rows before it, and ends
    // COMPLETED_WITH_ERRORS.
    let h = Harness::new().await;
    let compressed = gzip_bytes(&noisy_file_with_fault(2_001, 2_000));
    let size = i64::try_from(compressed.len()).unwrap();
    h.write_input("part-000.json.gz", &compressed);
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(true), both_reports(), config())
        .await;

    assert_eq!(record.processed_input_bytes, size);
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 2_001);
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(h.count_items().await, 2_000);
}

#[tokio::test]
async fn a_zstd_abandon_on_line_one_stops_the_counter_at_the_read_that_held_its_block() {
    // A fault on line 1 of a 198,728-byte zstd object leaves the
    // counter at 131,075, one zstd read (the largest block plus its header),
    // and the operation FAILED.
    let h = Harness::new().await;
    let compressed = zstd_bytes(&noisy_file_with_fault(2_000, 0));
    let size = i64::try_from(compressed.len()).unwrap();
    assert!(size > 131_075, "the fixture spans more than one zstd read");
    h.write_input("part-000.json.zst", &compressed);
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, zstd_input_config(), both_reports(), config())
        .await;

    assert_eq!(record.processed_input_bytes, 131_075);
    assert_eq!(record.total_input_bytes, Some(size));
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(record.failed_item_count, 1);
}

#[tokio::test]
async fn a_zstd_abandon_inside_the_second_read_counts_every_byte_and_completes_with_errors() {
    // A fault after 1,000 rows of a 198,831-byte zstd object sits
    // in the frame's second block, which the second 131,075-byte read
    // holds along with the end of the object: every byte counts and the
    // operation ends COMPLETED_WITH_ERRORS with the 1,000 rows written.
    let h = Harness::new().await;
    let compressed = zstd_bytes(&noisy_file_with_fault(2_000, 1_000));
    let size = i64::try_from(compressed.len()).unwrap();
    assert!(
        size > 131_075 && size < 2 * 131_075,
        "the fixture spans two zstd reads"
    );
    h.write_input("part-000.json.zst", &compressed);
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, zstd_input_config(), both_reports(), config())
        .await;

    assert_eq!(record.processed_input_bytes, size);
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.failure_code, None);
    assert_eq!(record.processed_item_count, 1_001);
    assert_eq!(h.count_items().await, 1_000);
}

// A zstd frame declaring a window larger than the
// decoder's 128 MiB maximum does not load. ExtendDB abandons the file with
// the decompression message and the operation completes with errors (the
// row is processed and failed). The hosted service instead leaves the
// operation non-terminal with the table locked for a frame needing more
// than a 128 MiB window, a stated divergence. A frame declaring a 256 MiB
// window (descriptor 0x90) is the case; the 128 MiB edge is pinned at the
// decoder in the codec tests.
#[tokio::test]
async fn a_zstd_frame_over_the_window_bound_is_abandoned_with_the_decompression_message() {
    let h = Harness::new().await;
    // zstd magic, a frame header descriptor with a window descriptor
    // following, then a 256 MiB window descriptor.
    let frame = vec![0x28u8, 0xb5, 0x2f, 0xfd, 0x00, 0x90];
    h.write_input("part-000.json.zst", &frame);
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, zstd_input_config(), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 1);
    assert_eq!(record.failed_item_count, 1);
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(
        report[0]["errorMessages"],
        json!(["The S3 object could not be decompressed"])
    );
}
