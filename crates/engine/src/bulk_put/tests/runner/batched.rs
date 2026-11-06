// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Durability boundaries.

// Batched writes.

#[tokio::test]
async fn rows_staged_at_a_crash_are_unwritten_and_written_once_on_resume() {
    // Checkpoints every 5 rows flush the write batch, so when the reader
    // pends after handing 8 lines, rows 5 to 7 are staged and not written.
    // The persisted cursor is the checkpoint (5), not past the staged rows,
    // so a resume re-reads and writes them exactly once.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(12)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after: 8,
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
    assert_eq!(mid.cursor_row_index, 5, "the cursor is the last checkpoint");
    assert_eq!(mid.processed_item_count, 5);
    assert_eq!(
        h.count_items().await,
        5,
        "rows 0 to 4 were flushed at the checkpoint"
    );
    assert!(
        h.get("k5").await.is_none(),
        "a staged row is not written until its batch flushes"
    );
    assert!(h.get("k7").await.is_none());

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
        "every row counts exactly once"
    );
    assert_eq!(h.count_items().await, 12);
    assert_eq!(
        record.usage["BulkPutBytes"],
        json!(12_288),
        "usage counts every written row exactly once"
    );
}

#[tokio::test]
async fn the_write_batch_and_the_checkpoint_both_bound_a_storage_call() {
    // Ten plain rows. With three rows per write batch and a checkpoint far
    // away, storage sees batches of 3, 3, 3, 1 (the last at end of file).
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(10)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let faulting = FaultingStorage::new(&h.storage);
    let sizes = Arc::clone(&faulting.batch_sizes);
    let mut cfg = config();
    cfg.batch_size = 25;
    cfg.write_batch_rows = 3;
    let deps = h.deps_with_storage("runner-1", cfg, Arc::new(faulting));
    runner::run(&deps, record).await.expect("runner run");
    assert_eq!(h.record(&op).await.status, "COMPLETED");
    assert_eq!(*sizes.lock().expect("recorder"), vec![3, 3, 3, 1]);
    assert_eq!(h.count_items().await, 10);

    // The same rows with a checkpoint every 4 rows and a write batch of 25:
    // the checkpoint flushes, so storage sees 4, 4, 2.
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(10)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let faulting = FaultingStorage::new(&h.storage);
    let sizes = Arc::clone(&faulting.batch_sizes);
    let mut cfg = config();
    cfg.batch_size = 4;
    cfg.write_batch_rows = 25;
    let deps = h.deps_with_storage("runner-1", cfg, Arc::new(faulting));
    runner::run(&deps, record).await.expect("runner run");
    assert_eq!(h.record(&op).await.status, "COMPLETED");
    assert_eq!(*sizes.lock().expect("recorder"), vec![4, 4, 2]);
    assert_eq!(h.count_items().await, 10);
}

#[tokio::test]
async fn a_pending_output_check_flushes_the_staged_rows_first() {
    // Row 2's admission fault is the first report record. The two rows
    // staged before it are written before the output configuration is
    // checked, so the check's verdict (here, the bucket exists) is reached
    // with those writes committed and the batch recorder shows a flush of
    // exactly two rows at that point.
    let h = Harness::new().await;
    h.write_input(
        "part-000.json",
        &file_of(&[row(0), row(1), row_missing_sk(2), row(3)]),
    );
    let files = h.enumerate().await;
    let op = h
        .start(
            files,
            input_config(false),
            output_config(Some(OUT_BUCKET), &["ERROR"]),
        )
        .await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    let faulting = FaultingStorage::new(&h.storage);
    let sizes = Arc::clone(&faulting.batch_sizes);
    let deps = h.deps_with_storage("runner-1", config(), Arc::new(faulting));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(record.processed_item_count, 4);
    assert_eq!(record.failed_item_count, 1);
    assert_eq!(
        *sizes.lock().expect("recorder"),
        vec![2, 1],
        "the first flush is forced by the pending output check, the second is end of file"
    );
    assert_eq!(h.count_items().await, 3);
}

/// The central durability claim of batching: a write batch LARGER than one
/// row commits in one storage transaction, the runner crashes before the
/// checkpoint that would cover it, and the resume replays the whole
/// committed batch, counting every row exactly once. Plain rows re-put
/// idempotently; a condition row inside the committed batch answers from the
/// in-transaction ledger rather than being re-evaluated against the
/// operation's own writes.
///
/// `write_batch_rows = 3` with a far checkpoint (`batch_size = 25`) makes the
/// flush fire mid-read-loop, so batches of three rows commit with no
/// checkpoint behind them. The reader pends after two batches have
/// committed, so the crash lands with committed-but-uncheckpointed batches
/// and a persisted cursor still at the start. The existing crash tests use
/// either the default write batch (the staged rows never commit) or
/// `write_batch_rows = 1` (one row per transaction), so neither exercises a
/// multi-row committed batch replayed through the ledger. This does.
#[tokio::test]
async fn a_committed_multi_row_batch_crashing_before_the_checkpoint_replays_once() {
    let h = Harness::new().await;
    let mut rows = plain_rows(9);
    // A passing condition row inside the first committed batch (row index 1,
    // key `c`), so the replay must take the ledger path, not re-evaluation.
    rows[1] = condition_row("1", "attribute_not_exists(pk)");
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // Rows 0,1,2 commit as one batch, then 3,4,5 as a second; row 6 is read
    // and staged, then the next read pends. No checkpoint has run.
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after: 7,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let mut cfg = config();
    cfg.batch_size = 25;
    cfg.write_batch_rows = 3;
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }

    let mid = h.record(&op).await;
    assert_eq!(mid.status, "IN_PROGRESS", "the operation is not terminal");
    assert_eq!(
        mid.cursor_row_index, 0,
        "no checkpoint committed before the crash"
    );
    assert_eq!(
        mid.processed_item_count, 0,
        "the persisted counters are the start"
    );
    assert_eq!(
        h.count_items().await,
        6,
        "two batches of three committed to the table with no checkpoint behind them"
    );
    assert!(
        h.get("c").await.is_some(),
        "the committed condition row is on disk before the resume"
    );
    assert_eq!(
        h.storage
            .bulk_row_outcome(ACCOUNT, &op, 0, 1)
            .await
            .expect("ledger read"),
        Some(RowOutcome::Applied),
        "the condition row's marker rode its batch transaction"
    );

    // The lease expires; the resume re-reads from cursor zero. Plain rows are
    // idempotent; the condition row at index 1 answers Applied from the
    // ledger and is not re-evaluated.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let mut cfg2 = config();
    cfg2.batch_size = 25;
    cfg2.write_batch_rows = 3;
    runner::run(&h.deps("runner-2", cfg2), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(
        record.processed_item_count, 9,
        "every row counts exactly once across the crash"
    );
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(h.count_items().await, 9);
    assert_eq!(
        record.usage["BulkPutBytes"],
        json!(9216),
        "nine rows under 1 KB each report 9 x 1024; the replayed committed \
         batch is charged once, not twice"
    );
    assert!(
        h.read_report(&op, ReportKind::ConditionCheckFailure)
            .is_none(),
        "the replayed condition row is answered from the ledger, not re-evaluated"
    );
    assert!(h.read_report(&op, ReportKind::Error).is_none());
    let cond = h.get("c").await.expect("the condition row is present");
    assert_eq!(cond.get("v"), Some(&AttributeValue::N("1".to_owned())));
}

/// Rows a crashed incarnation committed with no checkpoint behind them are
/// found in the ledger on resume and charged from what their writes
/// replaced, so the crashed run charges the table and the index what an
/// uninterrupted run of the same input does: nine one-kilobyte inserts on
/// each.
#[tokio::test]
async fn a_crashed_run_charges_the_index_as_an_uninterrupted_run_does() {
    let h = Harness::new_with_gsi().await;
    let rows: Vec<String> = (0..9)
        .map(|i| {
            json!({"Item": {
                "pk": {"S": format!("k{i}")}, "sk": {"S": "s"}, "g": {"S": format!("g{i}")}
            }})
            .to_string()
        })
        .collect();
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // Rows 0 to 5 commit as two batches of three with no checkpoint behind
    // them; the read of row 7 pends and the first incarnation is abandoned.
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after: 7,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let mut cfg = config();
    cfg.batch_size = 25;
    cfg.write_batch_rows = 3;
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }
    assert_eq!(
        h.count_items().await,
        6,
        "two batches committed, no checkpoint"
    );

    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let mut cfg2 = config();
    cfg2.batch_size = 25;
    cfg2.write_batch_rows = 3;
    runner::run(&h.deps("runner-2", cfg2), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 9);
    assert_eq!(h.count_items().await, 9);
    assert_eq!(record.usage["Table"]["BulkPutBytes"], json!(9216));
    assert_eq!(
        record.usage["GlobalSecondaryIndexes"]["gsi1"]["BulkPutBytes"],
        json!(9216),
        "the six rows applied before the crash are charged as the inserts they were"
    );
}

#[tokio::test]
async fn a_blank_line_only_file_checkpoints_and_observes_a_cancel() {
    let h = Harness::new().await;
    let blanks: Vec<String> = (0..30).map(|_| String::new()).collect();
    h.write_input("part-000.json", &file_of(&blanks));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;

    let storage = Arc::clone(&h.storage);
    let op_for_action = op.clone();
    let action: LineAction = Arc::new(move || {
        let storage = Arc::clone(&storage);
        let op = op_for_action.clone();
        Box::pin(async move {
            storage
                .request_cancel(ACCOUNT, &op)
                .await
                .expect("request cancel");
        })
    });
    let handed = Arc::new(AtomicU64::new(0));
    let source = Arc::new(ActAtSource {
        inner: h.fs_source(),
        at: 12,
        handed: Arc::clone(&handed),
        fired: Arc::new(AtomicBool::new(false)),
        action,
    });
    let mut cfg = config();
    cfg.batch_size = 5;
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    runner::run(&deps, record).await.expect("runner run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED");
    assert_eq!(
        record.processed_item_count, 0,
        "blank lines touch no counter"
    );
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(h.count_items().await, 0);
    assert_eq!(
        handed.load(Ordering::SeqCst),
        15,
        "the cancel is observed at the first boundary after it lands, so \
         reading stops at 15 physical lines, not at end of file"
    );
    assert_eq!(
        record.cursor_row_index, 15,
        "the boundary committed its checkpoint: the cursor is the 15 blank \
         lines consumed, not merely that the cancel was observed"
    );
}

#[tokio::test]
async fn a_fault_inside_the_resumed_skip_region_abandons_the_file_without_records() {
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

    // The resumed runner reads with a line cap below every line, so the
    // fault fires inside the skip region: rows a committed checkpoint
    // already counted. The file is abandoned with no record and no counter
    // change.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let mut cfg2 = config();
    cfg2.batch_size = 5;
    cfg2.max_line_bytes = 8;
    runner::run(&h.deps("runner-2", cfg2), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(
        record.processed_item_count, 10,
        "no counter change from the fault inside the skip region"
    );
    assert_eq!(record.failed_item_count, 0);
    assert!(
        h.read_report(&op, ReportKind::Error).is_none(),
        "no abandon record for a fault inside the skip region"
    );
    assert_eq!(
        Some(record.processed_input_bytes),
        record.total_input_bytes,
        "the abandoned file's bytes are consumed"
    );
}

#[tokio::test]
async fn a_resume_at_a_file_boundary_opens_the_next_file_without_double_count() {
    let h = Harness::new().await;
    h.write_input("a.json", &file_of(&plain_rows(10)));
    h.write_input("b.json", &file_of(&(10..15).map(row).collect::<Vec<_>>()));
    let files = h.enumerate().await;
    let op = h
        .start(files.clone(), input_config(false), both_reports())
        .await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // The runner finishes the first file (its end-of-file checkpoint
    // commits) and dies opening the second.
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAtOpenSource {
        inner: h.fs_source(),
        pend_at: 2,
        opens: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let mut cfg = config();
    cfg.batch_size = 5;
    cfg.lease = Duration::from_millis(100);
    let deps = h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink()));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the open pends"),
        () = notify.notified() => {}
    }
    let mid = h.record(&op).await;
    assert_eq!(mid.status, "IN_PROGRESS");
    assert_eq!(
        mid.processed_item_count, 10,
        "the first file's rows are all checkpointed"
    );
    assert_eq!(
        mid.cursor_file_index, 1,
        "end_of_file advanced the cursor to the second file before the \
         checkpoint"
    );
    assert_eq!(
        mid.cursor_row_index, 0,
        "the row cursor resets at a file boundary"
    );

    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let mut cfg2 = config();
    cfg2.batch_size = 5;
    runner::run(&h.deps("runner-2", cfg2), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 15, "no double count");
    assert_eq!(h.count_items().await, 15);
    assert_eq!(Some(record.processed_input_bytes), record.total_input_bytes);
}

#[tokio::test]
async fn a_resumed_run_reports_full_input_bytes_and_usage() {
    let h = Harness::new().await;
    let mut rows = plain_rows(26);
    rows[2] = row_missing_sk(2);
    rows[12] = row_missing_sk(12);
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

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

    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let mut cfg2 = config();
    cfg2.batch_size = 5;
    runner::run(&h.deps("runner-2", cfg2), record)
        .await
        .expect("resumed run");

    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(
        Some(record.processed_input_bytes),
        record.total_input_bytes,
        "ProcessedInputBytes equals TotalInputBytes on a resumed terminal run"
    );
    assert_eq!(
        record.usage["BulkPutBytes"],
        json!(24_576),
        "24 written rows below 1 KB round up to 24 x 1024 bytes, replays \
         counted once"
    );
    assert_eq!(record.usage["Table"]["BulkPutBytes"], json!(24_576));
}
