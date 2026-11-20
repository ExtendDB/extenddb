// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::lease_in_place::{HoldAt, HoldingSink};
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
    let record = h.claim("runner-1", STORE_LEASE).await;
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
    let deps =
        h.with_short_store_lease(h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink())));
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

// When the reports are written.

/// One row of `REPORT_ORDER`: a name, the requested `OutputType`, the line
/// cap of the run, and the record count of each report object the run
/// writes, by kind.
type OrderRow = (
    &'static str,
    &'static [&'static str],
    u64,
    &'static [(ReportKind, usize)],
);

/// The order table. Each row reads one file of a plain row, a row missing
/// its sort key and a row whose condition fails, and ends another way:
/// with both report kinds, with only `ERROR` requested (the condition
/// record goes to no object), and `FAILED` at a line cap of two.
const REPORT_ORDER: &[OrderRow] = &[
    (
        "both kinds",
        &["ERROR", "CONDITION_CHECK_FAILURE"],
        1_000_000,
        &[
            (ReportKind::Error, 1),
            (ReportKind::ConditionCheckFailure, 1),
        ],
    ),
    (
        "errors only",
        &["ERROR"],
        1_000_000,
        &[(ReportKind::Error, 1)],
    ),
    (
        "a failed run",
        &["ERROR", "CONDITION_CHECK_FAILURE"],
        2,
        &[(ReportKind::Error, 1)],
    ),
];

/// Every report object of `op` a client listing the output prefix finds:
/// its kind and its bytes, in kind and name order.
fn listed_objects(h: &Harness, op: &str) -> Vec<(ReportKind, Vec<u8>)> {
    let mut objects = Vec::new();
    for kind in [ReportKind::Error, ReportKind::ConditionCheckFailure] {
        let path = h.report_path(op, kind);
        let dir = path.parent().expect("the report path has a directory");
        for name in h.report_objects(op, kind) {
            objects.push((kind, std::fs::read(dir.join(name)).expect("read object")));
        }
    }
    objects
}

/// A client that lists the output prefix while the run ends finds each
/// report object once and complete: every object holds all its records
/// when the manifest is written, the manifest is in place before the
/// terminal transition, and no object changes after the manifest. The
/// service measured the same order: each report object appeared once with
/// one entity tag, 2.6 to 48 s before `EndTime`, and the manifest at
/// `EndTime`.
#[tokio::test]
async fn each_report_object_is_complete_before_the_manifest_and_the_terminal_transition() {
    for &(name, types, max_item_count, expected) in REPORT_ORDER {
        let h = Harness::new().await;
        h.put_direct(item("c", "1")).await;
        h.write_input(
            "part-000.json",
            &file_of(&[
                row(0),
                row_missing_sk(1),
                condition_row("2", "attribute_not_exists(pk)"),
            ]),
        );
        let files = h.enumerate().await;
        let output = output_config(Some(OUT_BUCKET), types);
        let op = h.start(files, input_config(false), output).await;
        let record = h.claim("runner-1", Duration::from_secs(60)).await;
        let sink = Arc::new(HoldingSink::new(&h, HoldAt::Manifest));
        let at_manifest = Arc::clone(&sink.reached);
        let release = Arc::clone(&sink.release);
        let at_finish = Arc::new(tokio::sync::Notify::new());
        let mut storage = FaultingStorage::new(&h.storage);
        storage.pend_finish = Some(Arc::clone(&at_finish));
        let mut cfg = config();
        cfg.max_item_count = max_item_count;
        let mut deps = h.deps_with("runner-1", cfg, Arc::new(h.fs_source()), sink);
        deps.storage = Arc::new(storage);
        let run = runner::run(&deps, record);
        tokio::pin!(run);

        tokio::select! {
            _ = &mut run => panic!("{name}: the run must reach the manifest"),
            () = at_finish.notified() => panic!("{name}: the terminal transition came before the manifest"),
            () = at_manifest.notified() => {}
        }
        assert_eq!(h.record(&op).await.status, "IN_PROGRESS", "{name}");
        assert!(
            h.read_manifest(&op).is_none(),
            "{name}: the manifest follows the reports"
        );
        let objects = listed_objects(&h, &op);
        let records: Vec<(ReportKind, usize)> = objects
            .iter()
            .map(|(kind, bytes)| (*kind, gunzip_lines(bytes).len()))
            .collect();
        assert_eq!(
            records, expected,
            "{name}: each object holds all its records before the manifest"
        );
        release.notify_one();

        tokio::select! {
            _ = &mut run => panic!("{name}: the run must reach the terminal transition"),
            () = at_finish.notified() => {}
        }
        assert_eq!(h.record(&op).await.status, "IN_PROGRESS", "{name}");
        assert_eq!(
            h.read_manifest(&op).map(|lines| lines.len()),
            Some(1),
            "{name}: the manifest precedes the terminal transition"
        );
        assert_eq!(
            listed_objects(&h, &op),
            objects,
            "{name}: no object is written again"
        );
    }
}
