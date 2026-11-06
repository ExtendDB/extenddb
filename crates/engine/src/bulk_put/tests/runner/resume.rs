// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Resume read errors, ledger retention, the line cap, and index charges.

#[tokio::test]
async fn an_io_error_inside_the_resumed_skip_region_fails_the_operation() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(30)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // First runner checkpoints at row 10 and dies.
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
    let mid = h.record(&op).await;
    assert_eq!(mid.cursor_row_index, 10);
    assert_eq!(mid.processed_item_count, 10);

    // The resumed runner's reader returns an I/O error at line 5, inside
    // the skip region the checkpoint at row 10 already covered. An I/O
    // error is not a changed-file signal, so it fails the operation
    // exactly as it would on a first read, rather than abandoning the file
    // the way an early end of file or an over-cap line does.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let source = Arc::new(IoAtSource {
        inner: h.fs_source(),
        at: 5,
        handed: Arc::new(AtomicU64::new(0)),
    });
    let mut cfg2 = config();
    cfg2.batch_size = 5;
    let deps = h.deps_with("runner-2", cfg2, source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("InternalServerError"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("Internal server error"),
        "no read detail reaches the client"
    );
    assert_eq!(
        record.processed_item_count, 10,
        "the checkpoint's counters stand; the fault dropped no rows silently"
    );
    assert_eq!(record.failed_item_count, 0);
    assert!(
        h.read_report(&op, ReportKind::Error).is_none(),
        "an I/O fault inside the skip region records nothing"
    );
}

#[tokio::test]
async fn a_crash_between_the_final_checkpoint_and_finish_keeps_the_retried_rows_marker() {
    let h = Harness::new().await;
    let mut rows = plain_rows(3);
    rows[1] = condition_row("2", "attribute_not_exists(pk)");
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // Row 1's marked write commits the item and its Applied ledger marker
    // in one data transaction, then reports a storage error, the way an
    // acknowledged commit can be lost on the wire. The run reverts to the
    // last checkpoint (none yet, so row 0), and the runner pends inside
    // finish after the final checkpoint, modelling a crash between the
    // checkpoint and the terminal transition.
    let notify = Arc::new(tokio::sync::Notify::new());
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.commit_then_error_row = Some((0, 1));
    faulting.pend_finish = Some(Arc::clone(&notify));
    let mut cfg = config();
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with_storage("runner-1", cfg, Arc::new(faulting));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the terminal write pends"),
        () = notify.notified() => {}
    }

    let mid = h.record(&op).await;
    assert_eq!(mid.status, "IN_PROGRESS", "finish never ran");
    assert_eq!(
        mid.cursor_row_index, 0,
        "the cursor is the last checkpoint, before the batch that errored"
    );
    assert_eq!(
        h.storage
            .bulk_row_outcome(ACCOUNT, &op, 0, 1)
            .await
            .expect("ledger read"),
        Some(RowOutcome::Applied),
        "the marker survived the crash before finish, so the retry does not \
         re-evaluate the condition against the operation's own write"
    );
    assert!(
        h.get("c").await.is_some(),
        "the committed write is on disk before the retry"
    );

    // The lease expires; a new worker resumes at the retried row. The
    // ledger answers Applied, so the condition is not re-evaluated, the row
    // is counted once, and no false condition-failure record is produced.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-2", config()), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(
        record.processed_item_count, 3,
        "the retried row counts exactly once"
    );
    assert_eq!(record.failed_item_count, 0);
    assert!(
        h.read_report(&op, ReportKind::ConditionCheckFailure)
            .is_none(),
        "no condition-failure record for a row the ledger says was applied"
    );
    assert!(h.read_report(&op, ReportKind::Error).is_none());
    let stored = h.get("c").await.expect("item present once");
    assert_eq!(stored.get("v"), Some(&AttributeValue::N("2".to_owned())));
    assert_eq!(
        h.storage
            .bulk_row_outcome(ACCOUNT, &op, 0, 1)
            .await
            .expect("ledger read"),
        None,
        "finish deletes the ledger rows only after the terminal transition"
    );
}

#[tokio::test]
async fn a_crash_after_the_cap_trips_resumes_at_the_tripping_line() {
    let h = Harness::new().await;
    // Ten rows in the first file fit the cap; the first line of the second
    // file trips it. Files behind the cursor are never re-read, so a
    // resume re-counts only the second file, which fits the cap. If the
    // tripping line had been consumed, the resume would skip it and end
    // COMPLETED with that line never written.
    h.write_input("part-000.json", &file_of(&plain_rows(10)));
    let tail: Vec<String> = (10..12).map(row).collect();
    h.write_input("part-001.json", &file_of(&tail));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // The runner pends inside finish after the final checkpoint, modelling
    // a crash between the checkpoint and the terminal transition.
    let notify = Arc::new(tokio::sync::Notify::new());
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.pend_finish = Some(Arc::clone(&notify));
    let mut cfg = config();
    cfg.max_item_count = 10;
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with_storage("runner-1", cfg, Arc::new(faulting));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the terminal write pends"),
        () = notify.notified() => {}
    }

    let mid = h.record(&op).await;
    assert_eq!(mid.status, "IN_PROGRESS", "finish never ran");
    assert_eq!(mid.cursor_file_index, 1);
    assert_eq!(
        mid.cursor_row_index, 0,
        "the cursor points at the line that tripped the cap, not past it"
    );
    assert_eq!(mid.processed_item_count, 10);
    assert_eq!(h.count_items().await, 10);

    // The lease expires; a new worker resumes at the tripping line with a
    // fresh per-claim count. The second file's two lines fit the cap, so
    // the line the first run stopped on is applied rather than skipped.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let mut cfg = config();
    cfg.max_item_count = 10;
    runner::run(&h.deps("runner-2", cfg), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(
        record.processed_item_count, 12,
        "every line is applied or reported once across the crash"
    );
    assert_eq!(h.count_items().await, 12);
    assert!(
        h.get("k10").await.is_some(),
        "the line the first run stopped on is written by the resume"
    );
}

#[tokio::test]
async fn blank_lines_count_toward_the_item_cap() {
    let h = Harness::new().await;
    let blanks: Vec<String> = (0..12).map(|_| String::new()).collect();
    h.write_input("part-000.json", &file_of(&blanks));
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
        Some("Operation exceeded max_bulk_item_count (10 rows)"),
        "blank lines are consumed physical lines and count toward the cap"
    );
    assert_eq!(
        record.processed_item_count, 0,
        "blank lines touch no processed counter even as they trip the cap"
    );
    assert_eq!(record.failed_item_count, 0);
}

#[tokio::test]
async fn an_item_level_fault_on_the_row_past_the_cap_fails_the_operation() {
    let h = Harness::new().await;
    let mut rows = plain_rows(10);
    rows.push(row_missing_sk(10));
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let mut cfg = config();
    cfg.max_item_count = 10;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), cfg)
        .await;

    assert_eq!(
        record.status, "FAILED",
        "the cap fires on an item-level fault row too, not a per-row record"
    );
    assert_eq!(record.failure_code.as_deref(), Some("ValidationException"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("Operation exceeded max_bulk_item_count (10 rows)")
    );
    assert_eq!(
        record.failed_item_count, 0,
        "the cap check returns before the row is validated or recorded"
    );
    assert!(
        h.read_report(&record.operation_id, ReportKind::Error)
            .is_none(),
        "no record for the row that trips the cap"
    );
}

#[tokio::test]
async fn a_replayed_failed_condition_row_is_reemitted_even_after_the_item_is_deleted() {
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
    // covering checkpoint.
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

    assert_eq!(
        h.storage
            .bulk_row_outcome(ACCOUNT, &op, 0, 10)
            .await
            .expect("ledger read"),
        Some(RowOutcome::ConditionFailed),
        "the failure outcome was recorded in its own transaction"
    );

    // Between the crash and the resume the seeded item is deleted, so a
    // re-evaluation of attribute_not_exists(pk) would now pass and write.
    // The ledger must answer instead: the failure record is re-emitted and
    // the item is not written, so the resume follows the recorded outcome,
    // not a fresh evaluation.
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
            .expect("delete the seeded item");
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
    assert_eq!(record.processed_item_count, 12, "the row counts once");
    assert_eq!(record.failed_item_count, 0);
    assert!(
        h.get("c").await.is_none(),
        "the ledger replay does not re-evaluate the condition or write the row"
    );
    let report = h
        .read_report(&op, ReportKind::ConditionCheckFailure)
        .expect("condition report rendered");
    assert_eq!(report.len(), 1, "the recorded failure is still emitted");
    assert_eq!(report[0]["errorMessages"], json!([CONDITION_FAILED]));
    assert_eq!(report[0]["itemLocation"]["itemIndex"], json!(10));
    assert_eq!(report[0]["newImage"]["Item"]["v"]["N"], json!("2"));
    assert_eq!(
        report[0]["oldImage"]["Item"]["v"]["N"],
        json!("1"),
        "the record carries the item the condition was evaluated against, as an \
         uninterrupted run's does, not the item stored at the resume"
    );
}

#[tokio::test]
async fn a_condition_overwrite_charges_the_index_like_a_plain_overwrite() {
    // A row keyed `pk`/`sk` with a GSI hash attribute `g`, either plain or
    // carrying a passing condition.
    fn indexed_row(pk: &str, g: &str, condition: Option<&str>) -> String {
        let base = format!(
            "\"Item\": {{\"pk\": {{\"S\": \"{pk}\"}}, \"sk\": {{\"S\": \"s\"}}, \
             \"g\": {{\"S\": \"{g}\"}}}}"
        );
        match condition {
            Some(expr) => format!("{{{base}, \"ConditionExpression\": \"{expr}\"}}"),
            None => format!("{{{base}}}"),
        }
    }
    fn seed(pk: &str, g: &str) -> Item {
        let mut item = Item::new();
        item.insert("pk".to_owned(), AttributeValue::S(pk.to_owned()));
        item.insert("sk".to_owned(), AttributeValue::S("s".to_owned()));
        item.insert("g".to_owned(), AttributeValue::S(g.to_owned()));
        item
    }

    // Run one operation that overwrites a seeded item and return the GSI
    // usage map. The row optionally carries attribute_exists(pk), which
    // passes against the seeded item, so the passing-condition path and the
    // plain path apply the same overwrite.
    async fn gsi_usage(condition: Option<&str>) -> Value {
        let h = Harness::new_with_gsi().await;
        h.put_direct(seed("i", "old")).await;
        h.write_input(
            "part-000.json",
            &file_of(&[indexed_row("i", "new", condition)]),
        );
        let files = h.enumerate().await;
        let record = h
            .run_to_end(files, input_config(false), both_reports(), config())
            .await;
        assert_eq!(record.status, "COMPLETED");
        assert_eq!(record.processed_item_count, 1);
        assert_eq!(record.failed_item_count, 0);
        record.usage["GlobalSecondaryIndexes"].clone()
    }

    let plain = gsi_usage(None).await;
    let conditional = gsi_usage(Some("attribute_exists(pk)")).await;

    assert!(
        plain["gsi1"]["BulkPutBytes"]
            .as_u64()
            .is_some_and(|b| b > 0),
        "the overwrite charges the index"
    );
    assert_eq!(
        conditional, plain,
        "a passing-condition overwrite charges the index by the old-versus-new \
         delta, the same figure a plain overwrite reports"
    );
}

/// A bulk run into a table with a vector index charges the index in
/// `BulkPutVectorBytes` under `VectorIndexes`, and the top-level
/// `BulkPutBytes` excludes the vector figure: two items under 1 KB report
/// 2048 at the top level whatever the index consumed. Every other usage
/// case runs on plain or GSI tables, so the vector plumbing from the write
/// loop through `capacity_helpers` is otherwise unobserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bulk_run_over_a_vector_indexed_table_charges_vector_bytes_separately() {
    fn vector_row(pk: &str) -> String {
        json!({"Item": {
            "pk": {"S": pk},
            "sk": {"S": "1"},
            "emb": {"L": [
                {"N": "0.1"}, {"N": "0.2"}, {"N": "0.3"}, {"N": "0.4"}
            ]}
        }})
        .to_string()
    }

    let h = Harness::new_with_vector().await;
    h.write_input(
        "part-000.json",
        &file_of(&[vector_row("a"), vector_row("b")]),
    );
    let files = h.enumerate().await;
    let record = h
        .run_to_end(files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 2);
    assert_eq!(record.failed_item_count, 0);

    let vector_bytes = record.usage["VectorIndexes"]["vidx"]["BulkPutVectorBytes"]
        .as_u64()
        .expect("the vector index must appear in the usage map");
    assert!(
        vector_bytes > 0,
        "a write carrying the vector attribute charges the index"
    );
    assert_eq!(
        record.usage["BulkPutBytes"].as_u64(),
        Some(2048),
        "two items under 1 KB round to 1 KB each; the vector figure stays \
         out of the top-level bytes"
    );
    assert_eq!(
        record.usage["Table"]["BulkPutBytes"].as_u64(),
        Some(2048),
        "the table member carries the same figure as the top level"
    );
}

// One transient storage fault on a write batch (a
// connection reset, an admin or crash shutdown, a serialization or deadlock
// abort, a lock timeout, a busy or locked database, a pool timeout) left
// the whole operation FAILED InternalServerError, while the same class of
// fault on a checkpoint left the operation non-terminal and the next claim
// resumed it. The two now take the same resumable path: the batch rolled
// back, nothing since the last checkpoint committed, so the operation stays
// in progress and a later claim finishes it from the last checkpoint.
//
// The storage double fails the first write of k3 with a transient storage
// error. The first claim's run returns that as a runner storage error with
// the operation still IN_PROGRESS and no terminal record; a second claim
// after the lease expires finishes it with the same rows, counters, usage
// and reports as an uninterrupted run.
#[tokio::test]
async fn a_transient_write_batch_fault_leaves_the_operation_to_resume() {
    // Uninterrupted truth on a clean harness.
    let truth_h = Harness::new().await;
    truth_h.write_input("part-000.json", &file_of(&plain_rows(10)));
    let truth_files = truth_h.enumerate().await;
    let truth = truth_h
        .run_to_end(truth_files, input_config(false), both_reports(), config())
        .await;
    assert_eq!(truth.status, "COMPLETED");

    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(10)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;

    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.fail_put_pk = Some("k3".to_owned());
    let storage: Arc<dyn StorageEngine> = Arc::new(faulting);

    // First claim: the transient fault on the write batch surfaces as a
    // runner storage error, not a terminal transition.
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let deps = h.deps_with_storage("runner-1", config(), Arc::clone(&storage));
    let result = runner::run(&deps, record).await;
    assert!(
        matches!(result, Err(runner::RunnerError::Storage(_))),
        "a transient write fault is a runner storage error, not a FAILED operation"
    );
    let mid = h.record(&op).await;
    assert_eq!(
        mid.status, "IN_PROGRESS",
        "the operation stays non-terminal after a transient write fault"
    );
    assert!(
        mid.failure_code.is_none(),
        "no terminal failure was written"
    );

    // Second claim after the lease expires: the fault does not fire again
    // and the resume finishes the operation.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let deps = h.deps_with_storage("runner-2", config(), Arc::clone(&storage));
    runner::run(&deps, record)
        .await
        .expect("the resume finishes");

    let final_record = h.record(&op).await;
    assert_eq!(final_record.status, "COMPLETED");
    assert_eq!(h.count_items().await, 10);
    assert_eq!(
        final_record.processed_item_count, truth.processed_item_count,
        "the resumed run processed the same rows as the uninterrupted one"
    );
    assert_eq!(final_record.failed_item_count, truth.failed_item_count);
    assert_eq!(
        final_record.usage, truth.usage,
        "usage matches the uninterrupted run"
    );
    assert_eq!(
        h.read_manifest(&op),
        truth_h.read_manifest(&truth.operation_id),
        "the manifest matches the uninterrupted run"
    );
}
