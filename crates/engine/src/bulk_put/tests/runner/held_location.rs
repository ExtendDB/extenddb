// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// The output location fault found at the first report record is held while
// the run reads on to the end of the input (measured on the service for a
// report write the output bucket refuses, a missing output bucket and an
// output owner that is not the caller). The operation then ends FAILED with
// the fault, ProcessedItemCount the rows written, FailedItemCount 0,
// ProcessedInputBytes 0, zero usage, and no report and no manifest. A cancel
// or another fault met before the end still decides the ending.

fn assert_zero_usage(record: &BulkOperationRecord) {
    assert_eq!(record.usage["BulkPutBytes"], json!(0), "{}", record.usage);
    assert_eq!(
        record.usage["Table"]["BulkPutBytes"],
        json!(0),
        "{}",
        record.usage
    );
}

fn cancel_at(h: &Harness, op: &str, at: u64) -> Arc<ActAtSource> {
    let storage = Arc::clone(&h.storage);
    let op = op.to_owned();
    let action: LineAction = Arc::new(move || {
        let storage = Arc::clone(&storage);
        let op = op.clone();
        Box::pin(async move {
            storage
                .request_cancel(ACCOUNT, &op)
                .await
                .expect("request cancel");
        })
    });
    Arc::new(ActAtSource {
        inner: h.fs_source(),
        at,
        handed: Arc::new(AtomicU64::new(0)),
        fired: Arc::new(AtomicBool::new(false)),
        action,
    })
}

#[tokio::test]
async fn every_good_row_before_and_after_the_faulting_row_is_written() {
    // Five good rows, the faulting row, four good rows.
    let h = Harness::new().await;
    let mut rows = plain_rows(10);
    rows[5] = row_missing_sk(5);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let total = files[0].size;
    let mut cfg = config();
    cfg.batch_size = 3;
    let record = h
        .run_to_end(
            files,
            input_config(false),
            output_config(Some("absent-reports"), &["ERROR"]),
            cfg,
        )
        .await;

    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchBucket"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("The specified bucket does not exist")
    );
    assert_eq!(record.processed_item_count, 9);
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(record.processed_input_bytes, 0);
    assert_eq!(record.total_input_bytes, Some(total));
    assert_zero_usage(&record);
    assert_eq!(record.report_record_count, 0, "no record is kept");
    assert_eq!(h.count_items().await, 9);
    assert!(h.get("k4").await.is_some());
    assert!(h.get("k9").await.is_some(), "the last row is written");
    assert!(
        !h.store.0.join(ACCOUNT).join("absent-reports").exists(),
        "nothing renders"
    );
}

#[tokio::test]
async fn later_files_are_read_and_later_failing_rows_keep_no_record() {
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row_missing_sk(1), row(2)]),
    );
    h.write_input(
        "part-001.json",
        &file_of(&[row(10), row_missing_sk(11), row(12), row(13)]),
    );
    let files = h.enumerate().await;
    let mut output = both_reports();
    output["S3BucketOwner"] = json!("999999999999");
    let mut cfg = config();
    cfg.batch_size = 2;
    let record = h.run_to_end(files, input_config(false), output, cfg).await;

    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3AccessDenied"));
    assert_eq!(record.failure_message.as_deref(), Some("Access Denied"));
    assert_eq!(record.processed_item_count, 5, "the five rows written");
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(record.processed_input_bytes, 0);
    assert_zero_usage(&record);
    assert_eq!(record.report_record_count, 0);
    assert_eq!(record.cursor_file_index, 2, "both files were read");
    assert_eq!(h.count_items().await, 5);
    assert!(h.get("k13").await.is_some());
    assert!(
        h.read_report(&record.operation_id, ReportKind::Error)
            .is_none()
    );
    assert!(h.read_manifest(&record.operation_id).is_none());
}

#[tokio::test]
async fn a_cancel_after_the_fault_is_held_ends_cancelled() {
    let h = Harness::new().await;
    let mut rows = plain_rows(15);
    rows[1] = row_missing_sk(1);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h
        .start(
            files,
            input_config(false),
            output_config(Some("absent-reports"), &["ERROR"]),
        )
        .await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let mut cfg = config();
    cfg.batch_size = 5;
    let deps = h.deps_with(
        "runner-1",
        cfg,
        cancel_at(&h, &op, 7),
        Arc::new(h.fs_sink()),
    );
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED");
    assert_eq!(record.failure_code.as_deref(), Some("RequestCancelled"));
    assert_eq!(
        record.processed_item_count, 10,
        "the counters a cancel reports, the faulting row included"
    );
    assert_eq!(record.failed_item_count, 1);
    assert!(
        record.processed_input_bytes > 0,
        "the bytes read before the stop"
    );
    assert!(
        !h.store.0.join(ACCOUNT).join("absent-reports").exists(),
        "nothing renders into the location the check refused"
    );
}

#[tokio::test]
async fn another_fault_after_the_fault_is_held_decides_the_ending() {
    // The second file is gone when the runner reaches it: that fault ends
    // the operation, with the counters as they stand.
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row_missing_sk(1), row(2)]),
    );
    h.write_input("part-001.json", &file_of(&plain_rows(2)));
    let files = h.enumerate().await;
    let op = h
        .start(
            files,
            input_config(false),
            output_config(Some("absent-reports"), &["ERROR"]),
        )
        .await;
    std::fs::remove_file(
        h.store
            .0
            .join(ACCOUNT)
            .join(IN_BUCKET)
            .join("part-001.json"),
    )
    .unwrap();
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-1", config()), record)
        .await
        .expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchKey"));
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(record.failed_item_count, 1);
    assert!(h.read_manifest(&op).is_none());
    assert!(!h.store.0.join(ACCOUNT).join("absent-reports").exists());
}

#[tokio::test]
async fn a_claim_that_resumes_after_a_crash_holds_the_fault_again() {
    // The first claim holds the fault at row 1, meets another failing row at
    // row 3, checkpoints at rows 5 and 10 and dies. No record was kept, so
    // the resumed claim checks the output location again at its own first
    // record (row 15) and holds the fault again; the ending is the one an
    // uninterrupted run reaches.
    let h = Harness::new().await;
    let mut rows = plain_rows(20);
    rows[1] = row_missing_sk(1);
    rows[3] = row_missing_sk(3);
    rows[15] = row_missing_sk(15);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h
        .start(
            files,
            input_config(false),
            output_config(Some("absent-reports"), &["ERROR"]),
        )
        .await;

    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after: 12,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let mut cfg = config();
    cfg.batch_size = 5;
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
    let mid = h.record(&op).await;
    assert_eq!(mid.cursor_row_index, 10);
    assert_eq!(mid.processed_item_count, 10);
    assert_eq!(mid.failed_item_count, 2);
    assert_eq!(mid.report_record_count, 0, "the held fault keeps no record");

    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let mut cfg = config();
    cfg.batch_size = 5;
    runner::run(&h.deps("runner-2", cfg), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchBucket"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("The specified bucket does not exist")
    );
    assert_eq!(record.processed_item_count, 17);
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(record.processed_input_bytes, 0);
    assert_zero_usage(&record);
    assert_eq!(h.count_items().await, 17);
}
