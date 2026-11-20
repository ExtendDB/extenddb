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

// Report caps.

/// Which report cap a row of `REPORT_CAPS` sets to two records' worth.
#[derive(Clone, Copy, Debug)]
enum ReportCap {
    Records,
    Bytes,
}

/// The report cap table. Each row runs six rows that fail item validation,
/// one error record each and all of one size, with batches of two rows,
/// under a cap of two records. A row with a crash point stops its first
/// runner once that many lines were read, past two checkpoints, as a crash
/// does, and a second runner finishes the operation.
const REPORT_CAPS: &[(&str, ReportCap, Option<u64>)] = &[
    ("record cap", ReportCap::Records, None),
    ("byte cap", ReportCap::Bytes, None),
    ("record cap across a crash", ReportCap::Records, Some(4)),
    ("byte cap across a crash", ReportCap::Bytes, Some(4)),
];

/// The `BulkReportRecordsDropped` series a collector holds.
fn dropped_series(metrics: &MetricsCollector) -> Vec<extenddb_core::metrics::MetricSnapshot> {
    metrics.query(&MetricsQuery {
        metric: Some(MetricName::BulkReportRecordsDropped),
        window: Some(TimeWindow::AllTime),
        ..MetricsQuery::default()
    })
}

/// Records past either cap are dropped while the counters stand, and the
/// earliest records are the kept ones. The operation keeps the count of the
/// dropped records with its checkpoints, so a resumed claim continues it,
/// and the terminal transition records the total once under
/// `BulkReportRecordsDropped`.
#[tokio::test]
async fn records_past_a_report_cap_are_dropped_and_counted() {
    let rows: Vec<String> = (0..6).map(row_missing_sk).collect();
    // Calibration: the same input under the roomy caps of `config()` gives
    // the size of one record.
    let calibration = Harness::new().await;
    calibration.write_input("part-000.json", &file_of(&rows));
    let files = calibration.enumerate().await;
    let record = calibration
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    let report = calibration
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("calibration report rendered");
    assert_eq!(report.len(), 6);
    let size = calibrated_record_bytes(&report);

    for &(name, cap, crash_after) in REPORT_CAPS {
        let h = Harness::new().await;
        h.write_input("part-000.json", &file_of(&rows));
        let files = h.enumerate().await;
        let op = h.start(files, input_config(false), both_reports()).await;
        let capped = || {
            let mut cfg = config();
            cfg.batch_size = 2;
            match cap {
                ReportCap::Records => cfg.max_report_records = 2,
                ReportCap::Bytes => cfg.max_report_bytes = 2 * size,
            }
            cfg
        };
        if let Some(after) = crash_after {
            // Rows 0 to 3 checkpoint in two batches: two kept records and
            // two dropped ones. The runner dies with rows 4 and 5 unread.
            let record = h.claim("runner-1", STORE_LEASE).await;
            let notify = Arc::new(tokio::sync::Notify::new());
            let source = Arc::new(PendAfterSource {
                inner: h.fs_source(),
                after,
                handed: Arc::new(AtomicU64::new(0)),
                notify: Arc::clone(&notify),
            });
            let deps = h.with_short_store_lease(h.deps_with(
                "runner-1",
                capped(),
                source,
                Arc::new(h.fs_sink()),
            ));
            tokio::select! {
                _ = runner::run(&deps, record) => panic!("{name}: the runner must not finish while the reader pends"),
                () = notify.notified() => {}
            }
            let mid = h.record(&op).await;
            assert_eq!(mid.cursor_row_index, 4, "{name}");
            assert_eq!(mid.failed_item_count, 4, "{name}");
            assert_eq!(
                crate::bulk_put::usage::dropped_report_records(&mid.usage),
                2,
                "{name}: the checkpoint stores the dropped count"
            );
            assert!(
                dropped_series(&deps.metrics).is_empty(),
                "{name}: a claim that stops records no drop"
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let record = h.claim("runner-2", Duration::from_secs(60)).await;
        let deps = h.deps("runner-2", capped());
        runner::run(&deps, record).await.expect("runner run");

        let record = h.record(&op).await;
        assert_eq!(record.status, "COMPLETED_WITH_ERRORS", "{name}");
        assert_eq!(
            record.failed_item_count, 6,
            "{name}: the counters are unaffected by the cap"
        );
        let report = h
            .read_report(&op, ReportKind::Error)
            .expect("error report rendered");
        let mut indexes: Vec<i64> = report
            .iter()
            .map(|r| r["itemLocation"]["itemIndex"].as_i64().unwrap())
            .collect();
        indexes.sort_unstable();
        assert_eq!(
            indexes,
            [0, 1],
            "{name}: the earliest records are the kept ones"
        );
        assert_eq!(
            crate::bulk_put::usage::dropped_report_records(&record.usage),
            4,
            "{name}: the terminal record counts every dropped record"
        );
        let series = dropped_series(&deps.metrics);
        assert_eq!(series.len(), 1, "{name}: one series: {series:?}");
        assert_eq!(series[0].sum, 4.0, "{name}");
        assert!(
            series[0]
                .dimensions
                .contains(&Dimension::TableName(TABLE.to_owned())),
            "{name}: {:?}",
            series[0].dimensions
        );
    }
}
