// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Durability and restart.

#[tokio::test]
async fn a_resumed_run_matches_an_uninterrupted_run() {
    let h = Harness::new().await;
    let mut rows = plain_rows(26);
    rows[2] = row_missing_sk(2);
    rows[12] = row_missing_sk(12);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h
        .start(files.clone(), input_config(false), both_reports())
        .await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // First runner: checkpoints at rows 5 and 10, then dies while rows 10
    // to 12 sit in an uncommitted batch (row 12 is a failed row whose
    // report record must not be lost or duplicated by the replay).
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after: 13,
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

    let mid = h.record(&op).await;
    assert_eq!(mid.status, "IN_PROGRESS", "the operation is not terminal");
    assert_eq!(
        mid.cursor_row_index, 10,
        "the cursor is the last checkpoint"
    );
    assert_eq!(mid.processed_item_count, 10);
    assert_eq!(
        mid.failed_item_count, 1,
        "row 12's record was not persisted"
    );

    // The lease expires; a new worker claims and resumes from the cursor.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let mut cfg2 = config();
    cfg2.batch_size = 5;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-2", cfg2), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 26, "no double count on replay");
    assert_eq!(record.failed_item_count, 2);
    assert_eq!(h.count_items().await, 24);
    let report = h
        .read_report(&record.operation_id, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(
        report.len(),
        2,
        "the report record count equals FailedItemCount"
    );
    let mut indexes: Vec<i64> = report
        .iter()
        .map(|r| r["itemLocation"]["itemIndex"].as_i64().unwrap())
        .collect();
    indexes.sort_unstable();
    assert_eq!(indexes, [2, 12], "one record per failed row, no duplicates");
}

#[tokio::test]
async fn a_replayed_passing_condition_row_is_not_re_evaluated_on_resume() {
    let h = Harness::new().await;
    let mut rows = plain_rows(12);
    rows[10] =
        "{\"Item\": {\"pk\": {\"S\": \"c\"}, \"sk\": {\"S\": \"s\"}, \"v\": {\"N\": \"1\"}}, \
                \"ConditionExpression\": \"attribute_not_exists(pk)\"}"
            .to_owned();
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // The condition row at index 10 is applied (write plus ledger marker in
    // one data transaction), then the runner dies before the checkpoint
    // that would cover it.
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after: 11,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let mut cfg = config();
    cfg.batch_size = 5;
    // One row per storage transaction, so the row's write commits as
    // soon as it is read and the crash lands between that commit and
    // the checkpoint that would cover it.
    cfg.write_batch_rows = 1;
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }

    let mid = h.record(&op).await;
    assert_eq!(mid.cursor_row_index, 10, "row 10 is past the cursor");
    assert!(
        h.get("c").await.is_some(),
        "the conditional write committed"
    );
    assert_eq!(
        h.storage
            .bulk_row_outcome(ACCOUNT, &op, 0, 10)
            .await
            .expect("ledger read"),
        Some(RowOutcome::Applied),
        "the ledger marker rode the write transaction"
    );

    // On resume the row is replayed. Its condition would now be false
    // against the operation's own write; the ledger must answer instead.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let mut cfg2 = config();
    cfg2.batch_size = 5;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-2", cfg2), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(
        record.processed_item_count, 12,
        "the row counts exactly once"
    );
    assert_eq!(record.failed_item_count, 0);
    assert!(
        h.read_report(&record.operation_id, ReportKind::ConditionCheckFailure)
            .is_none(),
        "no false condition-failure record from the replay"
    );
    assert!(
        h.read_report(&record.operation_id, ReportKind::Error)
            .is_none()
    );
    let stored = h.get("c").await.expect("item present");
    assert_eq!(stored.get("v"), Some(&AttributeValue::N("1".to_owned())));
    assert_eq!(
        h.storage
            .bulk_row_outcome(ACCOUNT, &op, 0, 10)
            .await
            .expect("ledger read"),
        None,
        "finish deletes the operation's ledger rows"
    );
}

/// An Applied-outcome replay must not re-evaluate the condition or touch the
/// table, even when the item it wrote has since been deleted. This isolates
/// the pre-write ledger consult: with that guard present the replay follows
/// the recorded Applied outcome and the deleted item stays deleted; without
/// it the row's `attribute_not_exists(pk)` is re-evaluated against the now
/// absent item, passes, and the row is resurrected. The symmetric
/// ConditionFailed-after-delete case already has a test; this is its Applied
/// twin, and it fails if the pre-write Applied branch is removed, which the
/// existing passing-condition resume test does not (the post-ConditionFailed
/// re-read masks that single reversion).
#[tokio::test]
async fn a_replayed_applied_condition_row_does_not_resurrect_an_externally_deleted_item() {
    let h = Harness::new().await;
    let mut rows = plain_rows(12);
    rows[10] =
        "{\"Item\": {\"pk\": {\"S\": \"c\"}, \"sk\": {\"S\": \"s\"}, \"v\": {\"N\": \"1\"}}, \
                \"ConditionExpression\": \"attribute_not_exists(pk)\"}"
            .to_owned();
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // Row 10 is applied (write plus Applied marker in one transaction),
    // then the runner dies before the covering checkpoint.
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after: 11,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let mut cfg = config();
    cfg.batch_size = 5;
    // One row per storage transaction, so the row's write commits as
    // soon as it is read and the crash lands between that commit and
    // the checkpoint that would cover it.
    cfg.write_batch_rows = 1;
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }

    assert!(
        h.get("c").await.is_some(),
        "the conditional write committed"
    );
    assert_eq!(
        h.storage
            .bulk_row_outcome(ACCOUNT, &op, 0, 10)
            .await
            .expect("ledger read"),
        Some(RowOutcome::Applied),
        "the marker rode the write transaction"
    );

    // The item is deleted between the crash and the resume. A re-evaluation
    // of attribute_not_exists(pk) would now pass and re-insert it; the
    // ledger must answer Applied instead and leave the table alone.
    {
        let info = h.key_info().await;
        h.storage
            .delete_item(
                &info,
                &key("c"),
                false,
                None,
                &ExpressionMaps::default(),
                None,
            )
            .await
            .expect("delete the applied item");
    }
    assert!(h.get("c").await.is_none(), "the item is gone before resume");

    tokio::time::sleep(Duration::from_millis(250)).await;
    let mut cfg2 = config();
    cfg2.batch_size = 5;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-2", cfg2), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(
        record.processed_item_count, 12,
        "the row counts exactly once"
    );
    assert_eq!(record.failed_item_count, 0);
    assert!(
        h.get("c").await.is_none(),
        "an Applied replay must not resurrect the externally deleted item"
    );
    assert!(
        h.read_report(&op, ReportKind::ConditionCheckFailure)
            .is_none(),
        "no condition-failure record from the replay"
    );
    assert!(h.read_report(&op, ReportKind::Error).is_none());
}

#[tokio::test]
async fn a_re_render_after_a_crash_between_rendering_and_finish_replaces_the_file() {
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row_missing_sk(1), row(2)]),
    );
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // The first runner renders the error report, then dies before finish.
    let notify = Arc::new(tokio::sync::Notify::new());
    let sink = Arc::new(PendAfterRenderSink {
        inner: h.fs_sink(),
        notify: Arc::clone(&notify),
    });
    let mut cfg = config();
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with("runner-1", cfg, Arc::new(h.fs_source()), sink);
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the sink pends"),
        () = notify.notified() => {}
    }

    let mid = h.record(&op).await;
    assert_eq!(mid.status, "IN_PROGRESS", "finish never ran");
    let first = h
        .read_report(&op, ReportKind::Error)
        .expect("the file is already on disk");
    assert_eq!(first.len(), 1);

    // A new worker claims the still-running record and re-renders; the
    // rename replaces the earlier file rather than colliding with it.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-2", config()), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 3);
    assert_eq!(record.failed_item_count, 1);
    let report = h
        .read_report(&op, ReportKind::Error)
        .expect("error report rendered");
    assert_eq!(
        report.len(),
        1,
        "the replaced file holds exactly one record"
    );
    assert_eq!(report[0]["errorMessages"], json!([MISSING_KEY]));
}
