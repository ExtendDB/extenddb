// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[tokio::test]
async fn a_replayed_failed_condition_row_reemits_the_record_without_evaluation() {
    let h = Harness::new().await;
    h.put_direct(item("c", "1")).await;
    let mut rows = plain_rows(12);
    rows[10] = condition_row("2", "attribute_not_exists(pk)");
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // The condition at row 10 fails against the seeded item; its marker
    // commits in its own transaction, then the runner dies before the
    // checkpoint that would cover the row's report record.
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
    assert_eq!(
        h.storage
            .bulk_row_outcome(ACCOUNT, &op, 0, 10)
            .await
            .expect("ledger read"),
        Some(RowOutcome::ConditionFailed),
        "the failure outcome was recorded in its own transaction"
    );

    // On resume the row is replayed; the ledger answers and the record is
    // re-emitted with the stored item as oldImage, without an evaluation.
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
    assert_eq!(
        record.failed_item_count, 0,
        "a condition failure is not a failed row"
    );
    let stored = h.get("c").await.expect("target item present");
    assert_eq!(
        stored.get("v"),
        Some(&AttributeValue::N("1".to_owned())),
        "the target item is untouched"
    );
    let report = h
        .read_report(&op, ReportKind::ConditionCheckFailure)
        .expect("condition report rendered");
    assert_eq!(report.len(), 1, "exactly one record, not duplicated");
    assert_eq!(report[0]["errorMessages"], json!([CONDITION_FAILED]));
    assert_eq!(report[0]["itemLocation"]["itemIndex"], json!(10));
    assert_eq!(report[0]["newImage"]["Item"]["v"]["N"], json!("2"));
    assert_eq!(
        report[0]["oldImage"]["Item"]["v"]["N"],
        json!("1"),
        "oldImage is the currently stored item"
    );
}

#[tokio::test]
async fn a_storage_error_row_resumes_at_that_row_and_counts_it_once() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(12)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // Row 7's write fails with a transient storage error. The batch it was
    // in does not commit, so the operation stays non-terminal at the last
    // checkpoint (row 5) and the next claim resumes it: a transient write
    // fault is a runner storage error, not a terminal
    // transition.
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.fail_put_pk = Some("k7".to_owned());
    let mut cfg = config();
    cfg.batch_size = 5;
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with_storage("runner-1", cfg, Arc::new(faulting));
    assert!(
        matches!(
            runner::run(&deps, record).await,
            Err(runner::RunnerError::Storage(_))
        ),
        "a transient write fault leaves the operation to resume, not a terminal transition"
    );

    let mid = h.record(&op).await;
    assert_eq!(mid.status, "IN_PROGRESS", "the operation is not terminal");
    assert_eq!(
        mid.cursor_row_index, 5,
        "the cursor is the last checkpoint: nothing from the failed batch committed"
    );
    assert_eq!(
        mid.processed_item_count, 5,
        "counters exclude the batch that did not commit"
    );
    assert_eq!(mid.failed_item_count, 0);
    assert!(h.get("k7").await.is_none(), "row 7 was never written");

    // The lease expires; a new worker resumes at the checkpoint, re-reads
    // the window (plain rows are idempotent), and the retry succeeds.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let mut cfg2 = config();
    cfg2.batch_size = 5;
    runner::run(&h.deps("runner-2", cfg2), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 12, "no double count on replay");
    assert_eq!(record.failed_item_count, 0);
    assert!(h.get("k7").await.is_some(), "the retried row is written");
    assert_eq!(h.count_items().await, 12);
    assert_eq!(Some(record.processed_input_bytes), record.total_input_bytes);
    assert_eq!(
        record.usage["BulkPutBytes"],
        json!(12_288),
        "usage counts every written row exactly once"
    );
    assert!(h.read_report(&op, ReportKind::Error).is_none());
}
