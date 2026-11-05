// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Empty input.

#[tokio::test]
async fn an_empty_input_file_list_completes_with_zero_counters() {
    let h = Harness::new().await;
    let record = h
        .run_to_end(Vec::new(), input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 0);
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(record.processed_input_bytes, 0);
    assert_eq!(record.total_input_bytes, Some(0));
    assert!(record.end_time_ms.is_some(), "the operation is terminal");
}

// Report layout.

#[tokio::test]
async fn report_files_land_under_a_separator_inserted_before_awsdynamodb() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&[row_missing_sk(0)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    // OUT_PREFIX carries no trailing slash, so the separator between it and
    // AWSDynamoDB exists only because the layout inserts one.
    let path = h.report_path(&record.operation_id, ReportKind::Error);
    assert!(
        path.is_file(),
        "report file at {}: the key inserts a separator after the prefix",
        path.display()
    );
}

#[tokio::test]
async fn item_index_is_zero_based_and_absent_on_abandon_records() {
    let h = Harness::new().await;
    h.write_input("a.json", &file_of(&[row(0), row(1), row_missing_sk(2)]));
    h.write_input("b.json", &file_of(&["garbage".to_owned()]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 2);
    let item_level = report
        .iter()
        .find(|r| {
            r["itemLocation"]["inputSourceArn"] == json!(format!("arn:aws:s3:::{IN_BUCKET}/a.json"))
        })
        .expect("record for the item-level fault");
    assert_eq!(
        item_level["itemLocation"]["itemIndex"],
        json!(2),
        "itemIndex is the zero-based line index"
    );
    let abandon = report
        .iter()
        .find(|r| {
            r["itemLocation"]["inputSourceArn"] == json!(format!("arn:aws:s3:::{IN_BUCKET}/b.json"))
        })
        .expect("record for the abandoned file");
    assert!(
        abandon["itemLocation"].get("itemIndex").is_none(),
        "abandon records carry no itemIndex"
    );
}

#[tokio::test]
async fn records_past_the_byte_cap_are_dropped_while_counters_stand() {
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&(0..4).map(row_missing_sk).collect::<Vec<_>>()),
    );

    // Calibration: the same input under a roomy cap yields the record size.
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("calibration report rendered");
    assert_eq!(report.len(), 4);
    let size = calibrated_record_bytes(&report);

    // The capped run keeps exactly two records' worth of bytes.
    let files = h.enumerate().await;
    let mut cfg = config();
    cfg.max_report_bytes = 2 * size;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), cfg)
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(
        record.failed_item_count, 4,
        "the counters are unaffected by the byte cap"
    );
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 2, "records past the byte cap are dropped");
    let mut indexes: Vec<i64> = report
        .iter()
        .map(|r| r["itemLocation"]["itemIndex"].as_i64().unwrap())
        .collect();
    indexes.sort_unstable();
    assert_eq!(indexes, [0, 1], "the earliest records are the kept ones");
}

#[tokio::test]
async fn a_crash_resume_continues_the_byte_cap_from_the_persisted_total() {
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&(0..4).map(row_missing_sk).collect::<Vec<_>>()),
    );

    // Calibration run under a roomy cap.
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("calibration report rendered");
    let size = calibrated_record_bytes(&report);

    // Rows 0 and 1 checkpoint with two kept records, exhausting the byte
    // cap; the runner dies with rows 2 and 3 unread.
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after: 2,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let mut cfg = config();
    cfg.batch_size = 2;
    cfg.max_report_bytes = 2 * size;
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }
    let mid = h.record(&op).await;
    assert_eq!(mid.cursor_row_index, 2);
    assert_eq!(mid.failed_item_count, 2);

    // The resumed runner restores the kept byte total from the record
    // instead of starting from zero, so rows 2 and 3 are dropped.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let mut cfg2 = config();
    cfg2.batch_size = 2;
    cfg2.max_report_bytes = 2 * size;
    runner::run(&h.deps("runner-2", cfg2), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.failed_item_count, 4, "counters are unaffected");
    let report = h
        .read_report(&op, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 2, "the byte cap holds across the resume");
    let mut indexes: Vec<i64> = report
        .iter()
        .map(|r| r["itemLocation"]["itemIndex"].as_i64().unwrap())
        .collect();
    indexes.sort_unstable();
    assert_eq!(indexes, [0, 1]);
}

#[tokio::test]
async fn a_blank_line_keeps_the_physical_item_index() {
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), String::new(), row_missing_sk(2)]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(
        record.processed_item_count, 2,
        "the blank line touches no counter"
    );
    assert_eq!(record.failed_item_count, 1);
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["itemLocation"]["itemIndex"],
        json!(2),
        "the blank line consumed a physical line index"
    );
}

#[tokio::test]
async fn attribute_names_without_an_expression_fail_the_row_alone() {
    let h = Harness::new().await;
    let bad = "{\"Item\": {\"pk\": {\"S\": \"k1\"}, \"sk\": {\"S\": \"s\"}}, \
               \"ExpressionAttributeNames\": {\"#v\": \"v\"}}"
        .to_owned();
    h.write_input("part-000.json", &file_of(&[row(0), bad, row(2)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(record.failed_item_count, 1);
    assert!(
        h.get("k1").await.is_none(),
        "the faulted row is not written"
    );
    assert!(h.get("k2").await.is_some(), "the file continues");
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!(["ExpressionAttributeNames can only be specified when using expressions"])
    );
    assert_eq!(report[0]["itemLocation"]["itemIndex"], json!(1));
    assert!(
        report[0].get("newImage").is_none(),
        "condition-field faults carry no newImage"
    );
}

#[tokio::test]
async fn attribute_values_without_an_expression_fail_the_row_alone() {
    let h = Harness::new().await;
    let bad = "{\"Item\": {\"pk\": {\"S\": \"k1\"}, \"sk\": {\"S\": \"s\"}}, \
               \"ExpressionAttributeValues\": {\":m\": {\"N\": \"1\"}}}"
        .to_owned();
    h.write_input("part-000.json", &file_of(&[row(0), bad, row(2)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.failed_item_count, 1);
    assert!(h.get("k2").await.is_some(), "the file continues");
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["errorMessages"],
        json!([
            "ExpressionAttributeValues can only be specified when using expressions: \
             ConditionExpression is null"
        ])
    );
    assert!(report[0].get("newImage").is_none());
}

#[tokio::test]
async fn an_invalid_condition_expression_fails_the_row_with_the_put_item_message() {
    let h = Harness::new().await;
    let bad = "{\"Item\": {\"pk\": {\"S\": \"k1\"}, \"sk\": {\"S\": \"s\"}}, \
               \"ConditionExpression\": \"v <\"}"
        .to_owned();
    h.write_input("part-000.json", &file_of(&[row(0), bad, row(2)]));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    // The expected message is what PutItem produces for the same fault
    // (the row goes through the same parsing functions) without the
    // `Invalid ConditionExpression: ` prefix, which the bulk record does
    // not carry (measured on the service).
    let put_item = resolve_condition(
        Some("v <"),
        None,
        None,
        None,
        None,
        &LimitsConfig::default(),
    )
    .expect_err("the condition expression is invalid")
    .to_string();
    let expected = put_item
        .strip_prefix("Invalid ConditionExpression: ")
        .expect("the single-item path prefixes the condition fault")
        .to_owned();

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(record.failed_item_count, 1);
    assert!(
        h.get("k1").await.is_none(),
        "the faulted row is not written"
    );
    assert!(h.get("k2").await.is_some(), "the file continues");
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(report[0]["errorMessages"], json!([expected]));
    assert_eq!(report[0]["itemLocation"]["itemIndex"], json!(1));
    assert!(report[0].get("newImage").is_none());
}
