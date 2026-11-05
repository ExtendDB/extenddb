// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Unit tests for the state concern of the runner.

use super::super::*;
use serde_json::json;

const BOTH: &[OutputType] = &[OutputType::Error, OutputType::ConditionCheckFailure];

// Terminal status selection.

#[test]
fn no_failures_end_completed() {
    assert_eq!(terminal_status(None, false, 0), "COMPLETED");
}

#[test]
fn failed_rows_end_completed_with_errors() {
    assert_eq!(terminal_status(None, false, 3), "COMPLETED_WITH_ERRORS");
}

#[test]
fn an_observed_cancel_ends_cancelled_even_with_failed_rows() {
    assert_eq!(terminal_status(None, true, 0), "CANCELLED");
    assert_eq!(terminal_status(None, true, 3), "CANCELLED");
}

#[test]
fn a_set_failure_wins_over_cancel_and_failed_rows() {
    let failure = Failure::internal();
    assert_eq!(terminal_status(Some(&failure), true, 3), "FAILED");
    assert_eq!(terminal_status(Some(&failure), false, 0), "FAILED");
}

// Record kinds by output type (measured on the service).

#[test]
fn a_kind_is_written_only_when_it_was_requested() {
    let both = [OutputType::Error, OutputType::ConditionCheckFailure];
    assert!(kind_requested(ReportKind::Error, &both));
    assert!(kind_requested(ReportKind::ConditionCheckFailure, &both));
    assert!(kind_requested(ReportKind::Error, &[OutputType::Error]));
    assert!(!kind_requested(
        ReportKind::ConditionCheckFailure,
        &[OutputType::Error]
    ));
    assert!(kind_requested(
        ReportKind::ConditionCheckFailure,
        &[OutputType::ConditionCheckFailure]
    ));
    assert!(!kind_requested(
        ReportKind::Error,
        &[OutputType::ConditionCheckFailure]
    ));
}

// The failure-path cursor arithmetic.

// The per-operation line cap.

#[test]
fn every_consumed_line_advances_the_cap_count_and_the_boundary() {
    let mut st = RunState::default();
    assert!(!st.consume_line(10));
    assert!(!st.consume_line(10));
    assert_eq!(st.lines_consumed, 2);
    assert_eq!(st.rows_since_checkpoint, 2);
    assert!(st.failure.is_none());
}

#[test]
fn the_line_past_the_cap_sets_the_row_limit_failure() {
    let mut st = RunState::default();
    for _ in 0..10 {
        assert!(!st.consume_line(10), "lines within the cap pass");
    }
    assert!(st.consume_line(10), "the eleventh line trips a cap of ten");
    let failure = st.failure.expect("the failure is set");
    assert_eq!(failure.code, CODE_VALIDATION);
    assert_eq!(
        failure.message,
        "Operation exceeded max_bulk_item_count (10 rows)"
    );
}

#[test]
fn the_cap_count_does_not_reset_across_files_or_counters() {
    // The count is physical lines consumed, independent of the
    // processed and failed counters a row may or may not touch.
    let mut st = RunState {
        processed: 3,
        failed: 2,
        ..RunState::default()
    };
    assert!(!st.consume_line(4));
    assert!(!st.consume_line(4));
    assert!(!st.consume_line(4));
    assert!(!st.consume_line(4));
    assert!(st.consume_line(4));
    assert_eq!(st.processed, 3, "the cap count leaves processed alone");
    assert_eq!(st.failed, 2, "the cap count leaves failed alone");
}

// The report-record caps.

#[test]
fn the_report_buffer_forces_a_checkpoint_by_bytes_under_a_huge_batch_size() {
    // A large batch_size would otherwise let `pending` grow to
    // batch_size records before a flush. The byte trigger bounds the
    // buffer regardless: once pending bytes cross PENDING_FLUSH_BYTES,
    // a checkpoint is due even though the row window is far below the
    // batch size.
    let mut st = RunState::default();
    let huge_batch = 1_000_000usize;
    assert!(!st.checkpoint_due(huge_batch), "nothing pending yet");

    // One ~1 MiB record per failing row; well under the count/byte caps
    // so every record is kept and buffered.
    let big = json!({"errorMessages": ["x".repeat(1024 * 1024)]});
    let mut rows = 0u64;
    while !st.checkpoint_due(huge_batch) {
        st.push_record(
            &ReportSink {
                max_records: u64::MAX,
                max_bytes: u64::MAX,
                requested: BOTH,
            },
            ReportKind::Error,
            0,
            rows,
            big.clone(),
        );
        st.rows_since_checkpoint += 1;
        rows += 1;
        assert!(
            rows < 1_000,
            "byte flush must trigger well before the batch size"
        );
    }
    assert!(
        st.pending_bytes >= PENDING_FLUSH_BYTES,
        "the checkpoint became due because the byte threshold was crossed"
    );
    assert!(
        st.rows_since_checkpoint < huge_batch,
        "the flush fired on bytes, not on the row window"
    );

    // Draining at a checkpoint resets the byte accounting.
    let _ = st.progress("arn:aws:dynamodb:us-east-1:000000000000:table/t");
    assert_eq!(st.pending_bytes, 0);
    assert!(st.pending.is_empty());
}

#[test]
fn records_beyond_the_count_cap_are_dropped_and_counted() {
    let mut st = RunState::default();
    for row in 0..5 {
        st.push_record(
            &ReportSink {
                max_records: 3,
                max_bytes: u64::MAX,
                requested: BOTH,
            },
            ReportKind::Error,
            0,
            row,
            json!({"errorMessages": ["x"]}),
        );
    }
    assert_eq!(st.pending.len(), 3);
    assert_eq!(st.kept_records, 3);
    assert_eq!(st.dropped_records, 2);
}

#[test]
fn a_seeded_kept_count_tightens_the_cap_for_a_resumed_run() {
    let mut st = RunState {
        kept_records: 2,
        ..RunState::default()
    };
    st.push_record(
        &ReportSink {
            max_records: 3,
            max_bytes: u64::MAX,
            requested: BOTH,
        },
        ReportKind::Error,
        0,
        0,
        json!({"errorMessages": ["x"]}),
    );
    st.push_record(
        &ReportSink {
            max_records: 3,
            max_bytes: u64::MAX,
            requested: BOTH,
        },
        ReportKind::Error,
        0,
        1,
        json!({"errorMessages": ["y"]}),
    );
    assert_eq!(st.pending.len(), 1, "only one slot was left under the cap");
    assert_eq!(st.dropped_records, 1);
}

#[test]
fn records_beyond_the_byte_cap_are_dropped_and_counted() {
    // Each record serializes to 21 bytes; the cap admits exactly two.
    let record = json!({"errorMessages": ["x"]});
    let bytes = report::record_bytes(&record);
    let mut st = RunState::default();
    for row in 0..3 {
        st.push_record(
            &ReportSink {
                max_records: u64::MAX,
                max_bytes: bytes * 2,
                requested: BOTH,
            },
            ReportKind::Error,
            0,
            row,
            record.clone(),
        );
    }
    assert_eq!(st.pending.len(), 2);
    assert_eq!(st.kept_bytes, bytes * 2);
    assert_eq!(st.dropped_records, 1);
}

#[test]
fn seeded_kept_bytes_tighten_the_byte_cap_for_a_resumed_run() {
    let record = json!({"errorMessages": ["x"]});
    let bytes = report::record_bytes(&record);
    let mut st = RunState {
        kept_bytes: bytes,
        ..RunState::default()
    };
    st.push_record(
        &ReportSink {
            max_records: u64::MAX,
            max_bytes: bytes * 2,
            requested: BOTH,
        },
        ReportKind::Error,
        0,
        0,
        record.clone(),
    );
    st.push_record(
        &ReportSink {
            max_records: u64::MAX,
            max_bytes: bytes * 2,
            requested: BOTH,
        },
        ReportKind::Error,
        0,
        1,
        record,
    );
    assert_eq!(
        st.pending.len(),
        1,
        "only one record's bytes were left under the cap"
    );
    assert_eq!(st.dropped_records, 1);
}

// Restore from a persisted record, and the checkpoint byte floor.

/// A record persisted mid-run: two of three files finished, the cursor
/// five rows into the third, and 350 processed input bytes on the row,
/// more than the 300 the two finished files account for because the
/// checkpoint that wrote it was partway through the third file.
fn persisted_record() -> BulkOperationRecord {
    BulkOperationRecord {
        account_id: "111122223333".to_owned(),
        operation_id: "01789060589767-da9ddd4f".to_owned(),
        table_id: "tid-1".to_owned(),
        table_name: "orders".to_owned(),
        table_arn: "arn:aws:dynamodb:us-east-1:111122223333:table/orders".to_owned(),
        status: "IN_PROGRESS".to_owned(),
        client_token: None,
        request_fingerprint: "f".repeat(64),
        input_config: json!({}),
        output_config: json!({}),
        input_files: vec![
            extenddb_storage::InputFile {
                path: "a.json".to_owned(),
                size: 100,
                rows: None,
                etag: None,
                abandon: None,
            },
            extenddb_storage::InputFile {
                path: "b.json".to_owned(),
                size: 200,
                rows: None,
                etag: None,
                abandon: None,
            },
            extenddb_storage::InputFile {
                path: "c.json".to_owned(),
                size: 400,
                rows: None,
                etag: None,
                abandon: None,
            },
        ],
        max_write_request_units: None,
        performance_tier: Some("STANDARD".to_owned()),
        start_time_ms: 1_789_060_589_767,
        end_time_ms: None,
        processed_item_count: 7,
        failed_item_count: 1,
        processed_input_bytes: 350,
        total_input_bytes: Some(700),
        report_record_count: 2,
        report_bytes: 64,
        usage: json!({}),
        failure_code: None,
        failure_message: None,
        cancel_requested: false,
        cursor_file_index: 2,
        cursor_row_index: 5,
        lease_owner: Some("worker-1".to_owned()),
        lease_until_ms: Some(1_789_060_649_767),
        run_context: None,
    }
}

#[test]
fn restore_sums_the_finished_files_and_seeds_the_cursor_and_caps() {
    let st = RunState::restore(&persisted_record()).expect("the record restores");
    assert_eq!(
        st.completed_bytes, 300,
        "the sizes of the files before the cursor, and only those"
    );
    assert_eq!(st.bytes_floor, 350, "the persisted ProcessedInputBytes");
    assert_eq!(st.cursor_file, 2);
    assert_eq!(st.cursor_row, 5);
    assert_eq!(st.processed, 7);
    assert_eq!(st.failed, 1);
    assert_eq!(st.kept_records, 2, "the report count cap resumes seeded");
    assert_eq!(st.kept_bytes, 64, "the report byte cap resumes seeded");
}

#[test]
fn a_checkpoint_after_restore_never_reports_bytes_below_the_persisted_floor() {
    // A run that fails before reopening the resumed file has counted only
    // the two finished files (300 bytes); the floor keeps the persisted
    // 350 from regressing. Once fresh reads pass the floor, the larger
    // figure wins.
    let mut st = RunState::restore(&persisted_record()).expect("the record restores");
    let arn = "arn:aws:dynamodb:us-east-1:111122223333:table/orders";
    assert_eq!(st.progress(arn).processed_input_bytes, 350);
    st.current_bytes = 120;
    assert_eq!(st.progress(arn).processed_input_bytes, 420);
}

#[test]
fn restore_refuses_a_negative_persisted_counter_naming_the_field() {
    let mut record = persisted_record();
    record.processed_item_count = -1;
    let Err(err) = RunState::restore(&record) else {
        panic!("a negative counter is corruption and must refuse the claim");
    };
    match err {
        StorageError::Internal(message) => {
            assert!(
                message.contains("processed_item_count"),
                "the refusal names the field: {message}"
            );
        }
        other => panic!("expected StorageError::Internal, got {other:?}"),
    }
}

// The abandon cut (measured on the service).

#[test]
fn the_cut_follows_the_read_that_held_the_faulting_line() {
    // A fault on line 1 of 23,840 bytes: the first read, 8,000 counted.
    assert_eq!(cut_processed_bytes(10, 23_840), Some(8_000));
    // A fault ending near byte 1,100 of 9,990: 8,000 counted.
    assert_eq!(cut_processed_bytes(1_100, 9_990), Some(8_000));
    // The whole file fits one read: no cut.
    assert_eq!(cut_processed_bytes(10, 1_980), None);
    // A fault on the last line of 23,840 bytes: no cut.
    assert_eq!(cut_processed_bytes(23_840, 23_840), None);
    // A line whose last byte is the last byte of a read belongs to that
    // read, not the next one: the cut lands at 8,000, not 16,000.
    assert_eq!(cut_processed_bytes(8_000, 23_840), Some(8_000));
    // The first byte of the next read is the next read.
    assert_eq!(cut_processed_bytes(8_001, 23_840), Some(16_000));
    assert_eq!(cut_processed_bytes(0, 0), None);
}

// The compressed-object cut (measured on the service).

#[test]
fn the_gzip_cut_follows_the_header_read_then_the_decoder_reads() {
    let gzip = ReadGeometry::of(InputCodec::Gzip);
    // The first line decoded from the first read of a 204,490-byte
    // object: the header read plus one read, 65,546.
    assert_eq!(gzip.cut_processed_bytes(8_192, 204_490), Some(65_546));
    // A fault once about 100 KB of the stream was pulled: 131,082.
    assert_eq!(gzip.cut_processed_bytes(106_496, 204_488), Some(131_082));
    // A 30,758-byte object fits the first read: no cut.
    assert_eq!(gzip.cut_processed_bytes(8_192, 30_758), None);
    // The fault on the last line: no cut.
    assert_eq!(gzip.cut_processed_bytes(204_561, 204_561), None);
    // The last byte of a read belongs to it; the next byte starts the next.
    assert_eq!(gzip.cut_processed_bytes(65_546, 204_490), Some(65_546));
    assert_eq!(gzip.cut_processed_bytes(65_547, 204_490), Some(131_082));
    // A fault attributed to the header read itself cuts at the header.
    assert_eq!(gzip.cut_processed_bytes(10, 204_490), Some(10));
}

#[test]
fn the_zstd_cut_follows_whole_block_sized_reads() {
    let zstd = ReadGeometry::of(InputCodec::Zstd);
    // The first block pulled (about 66 KB) sits in the first read of
    // a 198,728-byte object: 131,075.
    assert_eq!(zstd.cut_processed_bytes(73_728, 198_728), Some(131_075));
    // The second block (about 132 KB pulled) sits in the second read,
    // which holds the end of the 198,831-byte object: no cut.
    assert_eq!(zstd.cut_processed_bytes(139_264, 198_831), None);
    // A 99,334-byte object fits the first read: no cut.
    assert_eq!(zstd.cut_processed_bytes(73_728, 99_334), None);
    assert_eq!(zstd.cut_processed_bytes(131_075, 300_000), Some(131_075));
    assert_eq!(zstd.cut_processed_bytes(131_076, 300_000), Some(262_150));
}

#[test]
fn the_plain_geometry_is_the_eight_thousand_byte_read() {
    assert_eq!(
        ReadGeometry::of(InputCodec::None),
        ReadGeometry {
            head: 0,
            chunk: READ_CHUNK_BYTES
        }
    );
    assert_eq!(
        ReadGeometry::of(InputCodec::None).cut_processed_bytes(1_100, 9_990),
        cut_processed_bytes(1_100, 9_990)
    );
}
