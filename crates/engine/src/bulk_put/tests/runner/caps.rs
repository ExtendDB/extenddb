// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Usage.

#[tokio::test]
async fn twenty_five_small_items_report_25600_bulk_put_bytes() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(25)));
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;

    assert_eq!(record.status, "COMPLETED");
    assert_eq!(
        record.usage["TableName"],
        json!("orders"),
        "the usage entry names the table by its bare name, as the service does"
    );
    assert_eq!(
        record.usage["BulkPutBytes"],
        json!(25_600),
        "25 items below 1 KB round up to 25 x 1024 bytes"
    );
    assert_eq!(record.usage["Table"]["BulkPutBytes"], json!(25_600));
}

// Configured caps.

#[tokio::test]
async fn a_line_over_the_byte_cap_abandons_the_file() {
    let h = Harness::new().await;
    let long = format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"long\"}}, \"sk\": {{\"S\": \"s\"}}, \"v\": {{\"S\": \"{}\"}}}}}}",
        "y".repeat(400)
    );
    h.write_input("part-000.json", &file_of(&[row(0), long, row(2)]));
    let files = h.enumerate().await;
    let mut cfg = config();
    cfg.max_line_bytes = 256;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), cfg)
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 2);
    assert_eq!(record.failed_item_count, 1);
    assert!(h.get("k0").await.is_some());
    assert!(h.get("long").await.is_none());
    assert!(h.get("k2").await.is_none(), "the file is abandoned");
    assert_eq!(Some(record.processed_input_bytes), record.total_input_bytes);
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(report[0]["errorMessages"], json!([ABANDON]));
}

#[tokio::test]
async fn rows_past_the_item_count_cap_fail_the_operation() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(12)));
    let files = h.enumerate().await;
    let mut cfg = config();
    cfg.max_item_count = 10;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), cfg)
        .await;

    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("Operation exceeded max_bulk_item_count (10 rows)")
    );
    assert!(
        h.get("k11").await.is_none(),
        "no row past the cap is written"
    );
}

// Configured caps, continued.

#[tokio::test]
async fn a_parse_fault_on_the_row_past_the_item_count_cap_fails_the_operation() {
    let h = Harness::new().await;
    let mut rows = plain_rows(10);
    rows.push("this is not json {{{".to_owned());
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let mut cfg = config();
    cfg.max_item_count = 10;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), cfg)
        .await;

    assert_eq!(
        record.status, "FAILED",
        "the cap fires on a parse-fault row too, not an abandon"
    );
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("Operation exceeded max_bulk_item_count (10 rows)")
    );
}

#[tokio::test]
async fn the_item_count_cap_stops_the_run_at_exactly_the_cap() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(12)));
    let files = h.enumerate().await;
    let mut cfg = config();
    cfg.max_item_count = 10;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), cfg)
        .await;

    assert_eq!(record.status, "FAILED");
    assert_eq!(
        record.processed_item_count, 10,
        "the line that crosses the cap is consumed but never processed"
    );
    assert_eq!(
        record.failed_item_count, 0,
        "the cap check returns before any row is failed or recorded"
    );
    assert_eq!(h.count_items().await, 10, "exactly the cap is written");
    assert!(
        h.get("k10").await.is_none(),
        "the row that trips the cap is never written"
    );
}

#[tokio::test]
async fn records_past_the_report_cap_are_dropped_from_the_rendered_file() {
    let h = Harness::new().await;
    let mut rows: Vec<String> = (0..4).map(row_missing_sk).collect();
    rows.push(row(4));
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let mut cfg = config();
    cfg.max_report_records = 2;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), cfg)
        .await;

    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 5);
    assert_eq!(
        record.failed_item_count, 4,
        "the counters are unaffected by the report cap"
    );
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 2, "records past the cap are dropped");
    let mut indexes: Vec<i64> = report
        .iter()
        .map(|r| r["itemLocation"]["itemIndex"].as_i64().unwrap())
        .collect();
    indexes.sort_unstable();
    assert_eq!(indexes, [0, 1], "the earliest records are the kept ones");
}

#[tokio::test]
async fn a_crash_resume_continues_the_record_cap_from_the_persisted_count() {
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&(0..4).map(row_missing_sk).collect::<Vec<_>>()),
    );
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // Rows 0 and 1 checkpoint with two kept records, exhausting the cap;
    // the runner dies with rows 2 and 3 unread.
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after: 2,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let mut cfg = config();
    cfg.batch_size = 2;
    cfg.max_report_records = 2;
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }
    let mid = h.record(&op).await;
    assert_eq!(mid.cursor_row_index, 2);
    assert_eq!(mid.failed_item_count, 2);

    // The resumed runner restores the kept count from the record instead
    // of starting from zero, so rows 2 and 3 are dropped from the report.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let mut cfg2 = config();
    cfg2.batch_size = 2;
    cfg2.max_report_records = 2;
    runner::run(&h.deps("runner-2", cfg2), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.failed_item_count, 4, "counters are unaffected");
    let report = h
        .read_report(&op, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(report.len(), 2, "the cap holds across the resume");
    let mut indexes: Vec<i64> = report
        .iter()
        .map(|r| r["itemLocation"]["itemIndex"].as_i64().unwrap())
        .collect();
    indexes.sort_unstable();
    assert_eq!(indexes, [0, 1]);
}
