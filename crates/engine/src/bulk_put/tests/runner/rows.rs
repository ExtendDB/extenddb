// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Row classes and counters.

#[tokio::test]
async fn a_bad_last_row_leaves_25_written_and_ends_completed_with_errors() {
    let h = Harness::new().await;
    let mut rows = plain_rows(25);
    rows.push(row_missing_sk(25));
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 26);
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(h.count_items().await, 25);
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(report[0]["errorMessages"], json!([MISSING_KEY]));
}

#[tokio::test]
async fn a_parse_fault_at_line_11_abandons_the_rest_of_the_file() {
    let h = Harness::new().await;
    let mut rows = plain_rows(10);
    rows.push(row_bad_type_tag(10));
    rows.extend((11..31).map(row));
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 11);
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(h.count_items().await, 10);
    assert!(h.get("k9").await.is_some(), "the 10th row is written");
    assert!(h.get("k11").await.is_none(), "row 12 is never read");
    assert!(h.get("k30").await.is_none(), "row 31 is never read");
    assert_eq!(
        Some(record.processed_input_bytes),
        record.total_input_bytes,
        "ProcessedInputBytes advances to the end of the abandoned file"
    );
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(report[0]["errorMessages"], json!([ABANDON]));
}

#[tokio::test]
async fn a_missing_output_bucket_found_at_the_first_record_is_held_to_the_end_of_the_input() {
    // Measured on the service: the output configuration is checked when the
    // first report record is produced, the fault is held, and the run reads
    // on to the end of the input. It ends FAILED with the fault, the rows
    // written as ProcessedItemCount, no failed row, no processed byte, no
    // usage, and nothing rendered.
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row_missing_sk(1), row(2)]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(
            files,
            input_config(false),
            output_config(Some("absent-reports"), &["ERROR"]),
            config(),
        )
        .await;

    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchBucket"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("The specified bucket does not exist")
    );
    assert_eq!(
        record.processed_item_count, 2,
        "the rows written, before and after the faulting row"
    );
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(record.processed_input_bytes, 0);
    assert_eq!(record.usage["BulkPutBytes"], json!(0));
    assert_eq!(record.report_record_count, 0);
    assert!(h.get("k0").await.is_some());
    assert!(
        h.get("k2").await.is_some(),
        "the row after the fault is written"
    );
    assert!(
        !h.store.0.join(ACCOUNT).join("absent-reports").exists(),
        "nothing renders on this path"
    );
}

#[tokio::test]
async fn a_mismatched_output_owner_found_at_the_first_record_is_held_to_the_end_of_the_input() {
    // The output bucket exists; only the recorded owner differs from the
    // account. Ownership is decided before existence, as at Start.
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row_missing_sk(1), row(2)]),
    );
    let files = h.enumerate().await;
    let mut output = output_config(Some(OUT_BUCKET), &["ERROR"]);
    output["S3BucketOwner"] = json!("999999999999");
    let record = h
        .run_to_end(files, input_config(false), output, config())
        .await;

    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(record.failure_message.as_deref(), Some("Access Denied"));
    assert_eq!(record.processed_item_count, 2);
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(record.processed_input_bytes, 0);
    assert_eq!(h.count_items().await, 2);
    assert!(
        h.read_report(&record.operation_id, ReportKind::Error)
            .is_none(),
        "no report is written for an unvalidated output configuration"
    );
    assert!(h.read_manifest(&record.operation_id).is_none());
}

#[tokio::test]
async fn a_location_fault_found_in_a_later_batch_is_held_to_the_end_of_the_input() {
    // The first report record lands in the second batch (batch size 25),
    // after a checkpoint at row 25. The fault is held as in the first
    // batch: every good row is written and counted.
    let h = Harness::new().await;
    let mut rows = plain_rows(30);
    rows.push(row_missing_sk(30));
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(
            files,
            input_config(false),
            output_config(Some("absent-reports"), &["ERROR"]),
            config(),
        )
        .await;

    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchBucket"));
    assert_eq!(record.processed_item_count, 30, "the rows written");
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(record.processed_input_bytes, 0);
    assert_eq!(h.count_items().await, 30, "written rows stay written");
}

#[tokio::test]
async fn a_missing_output_bucket_with_clean_rows_fails_at_the_manifest() {
    // Measured on the service: with no failing rows the output bucket
    // is first touched by the manifest write, and a missing bucket ends the
    // operation FAILED with the bucket code and the manifest prefix; the
    // rows written stay written and counted.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(3)));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(
            files,
            input_config(false),
            output_config(Some("absent-reports"), &["ERROR"]),
            config(),
        )
        .await;

    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchBucket"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("Manifest is not available. The specified bucket does not exist")
    );
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(h.count_items().await, 3);
    assert!(
        !h.store.0.join(ACCOUNT).join("absent-reports").exists(),
        "nothing creates the missing report bucket"
    );
}

#[tokio::test]
async fn a_mismatched_output_owner_with_clean_rows_fails_at_the_manifest() {
    // Measured on the service: an output owner that is not the caller
    // fails the operation at the manifest write with S3AccessDenied and the
    // manifest prefix.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(2)));
    let files = h.enumerate().await;
    let mut output = output_config(Some(OUT_BUCKET), &["ERROR"]);
    output["S3BucketOwner"] = json!("111122223333");
    let record = h
        .run_to_end(files, input_config(false), output, config())
        .await;

    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("Manifest is not available. Access Denied")
    );
    assert_eq!(record.processed_item_count, 2);
    assert_eq!(h.count_items().await, 2);
}

#[tokio::test]
async fn a_row_without_an_item_abandons_the_file_with_its_own_message() {
    let h = Harness::new().await;
    let no_item = r#"{"ConditionExpression": "attribute_not_exists(pk)"}"#.to_owned();
    h.write_input("part-000.json", &file_of(&[row(0), no_item, row(2)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 2, "row 3 is never read");
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(h.count_items().await, 1);
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!([
            "Expected 'Item' top level container. Remainder of the file will not be processed."
        ])
    );
}

#[tokio::test]
async fn a_non_object_item_abandons_the_file_with_its_own_message() {
    let h = Harness::new().await;
    let bad_item = r#"{"Item": "pk"}"#.to_owned();
    h.write_input("part-000.json", &file_of(&[row(0), bad_item, row(2)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 2, "row 3 is never read");
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(h.count_items().await, 1);
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!(["Expected character {. Remainder of the file will not be processed."])
    );
}

#[tokio::test]
async fn an_abandoned_first_file_does_not_stop_the_second_file() {
    let h = Harness::new().await;
    h.write_input("a.json", &file_of(&[row(0), row_bad_type_tag(1), row(2)]));
    h.write_input("b.json", &file_of(&[row(10), row(11), row(12)]));
    let files = h.enumerate().await;
    assert_eq!(files.len(), 2, "lexical order: a.json then b.json");
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 5);
    assert_eq!(record.failed_item_count, 1);
    assert!(h.get("k0").await.is_some(), "row before the fault written");
    assert!(h.get("k2").await.is_none(), "rest of the first file unread");
    assert!(h.get("k10").await.is_some(), "second file fully processed");
    assert!(h.get("k12").await.is_some(), "second file fully processed");
    assert_eq!(Some(record.processed_input_bytes), record.total_input_bytes);
}

// Item-level classes, one operation each.

#[tokio::test]
async fn a_row_missing_a_key_attribute_fails_alone_with_the_put_item_message() {
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row_missing_sk(1), row(2)]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(record.failed_item_count, 1);
    assert!(h.get("k0").await.is_some());
    assert!(h.get("k2").await.is_some(), "the file continues");
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(report[0]["errorMessages"], json!([MISSING_KEY]));
    assert_eq!(report[0]["itemLocation"]["itemIndex"], json!(1));
    assert_eq!(
        report[0]["newImage"]["Item"]["pk"]["S"],
        json!("k1"),
        "the missing-key record carries the submitted item"
    );
}

#[tokio::test]
async fn a_row_with_a_wrong_key_type_fails_alone_with_the_put_item_message() {
    let h = Harness::new().await;
    let bad = "{\"Item\": {\"pk\": {\"N\": \"7\"}, \"sk\": {\"S\": \"s\"}}}".to_owned();
    h.write_input("part-000.json", &file_of(&[row(0), bad, row(2)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(record.failed_item_count, 1);
    assert!(h.get("k2").await.is_some(), "the file continues");
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(report[0]["errorMessages"], json!([TYPE_MISMATCH]));
    assert!(
        report[0].get("newImage").is_none(),
        "type-mismatch records carry no newImage"
    );
}

#[tokio::test]
async fn an_oversized_item_fails_alone_with_the_put_item_message() {
    let h = Harness::new().await;
    // The item size limit is 409,600 bytes (400 KiB); this blob alone
    // exceeds it.
    let blob = "x".repeat(410_000);
    let oversized = format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"big\"}}, \"sk\": {{\"S\": \"s\"}}, \"v\": {{\"S\": \"{blob}\"}}}}}}"
    );
    h.write_input("part-000.json", &file_of(&[row(0), oversized, row(2)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(record.failed_item_count, 1);
    assert!(
        h.get("big").await.is_none(),
        "the oversized row not written"
    );
    assert!(h.get("k2").await.is_some(), "the file continues");
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(report[0]["errorMessages"], json!([OVERSIZED]));
    assert!(
        report[0].get("newImage").is_none(),
        "oversized records carry no newImage"
    );
}
