// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Every row is applied once. After a kill past a checkpoint, a transient
//! batch fault inside a checkpoint window, or a run of faults that never
//! clears, the table, the counters, the usage and the reports are those of
//! an uninterrupted run of the same input, and a client write that landed
//! after a row's batch committed is never overwritten by that row again.

use super::*;

fn kv(pk: &str, v: &str) -> String {
    format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"{pk}\"}}, \"sk\": {{\"S\": \"s\"}}, \"v\": {{\"N\": \"{v}\"}}}}}}"
    )
}

fn kv_if(pk: &str, v: &str, condition: &str) -> String {
    format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"{pk}\"}}, \"sk\": {{\"S\": \"s\"}}, \"v\": {{\"N\": \"{v}\"}}}}, \
         \"ConditionExpression\": \"{condition}\"}}"
    )
}

/// Every stored item, ordered by partition key.
async fn all_items(h: &Harness) -> Vec<Item> {
    let info = h.key_info().await;
    let mut items = Vec::new();
    let mut start: Option<Item> = None;
    loop {
        let (page, next) = h
            .storage
            .scan(&info, None, start.as_ref(), None, None, None)
            .await
            .expect("scan");
        items.extend(page);
        match next {
            Some(k) => start = Some(k),
            None => break,
        }
    }
    items.sort_by_key(|item| format!("{:?}", item.get("pk")));
    items
}

/// What a run leaves behind, compared field by field with an uninterrupted
/// run of the same input.
#[derive(Debug, PartialEq)]
struct Outcome {
    status: String,
    processed: i64,
    failed: i64,
    processed_bytes: i64,
    usage: Value,
    items: Vec<Item>,
    conditions: Option<Vec<Value>>,
    errors: Option<Vec<Value>>,
}

async fn outcome(h: &Harness, op: &str) -> Outcome {
    let record = h.record(op).await;
    Outcome {
        status: record.status,
        processed: record.processed_item_count,
        failed: record.failed_item_count,
        processed_bytes: record.processed_input_bytes,
        usage: record.usage,
        items: all_items(h).await,
        conditions: h.read_report(op, ReportKind::ConditionCheckFailure),
        errors: h.read_report(op, ReportKind::Error),
    }
}

fn window(batch_size: usize, write_batch_rows: usize) -> RunnerConfig {
    RunnerConfig {
        batch_size,
        write_batch_rows,
        ..config()
    }
}

/// The run of `rows` with no interruption, from a table seeded with `seed`.
async fn uninterrupted(
    backend: Backend,
    rows: &[String],
    seed: &[Item],
    cfg: RunnerConfig,
) -> Outcome {
    let h = Harness::on(backend).await;
    for item in seed {
        h.put_direct(item.clone()).await;
    }
    h.write_input("part-000.json", &file_of(rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-1", cfg), record)
        .await
        .expect("uninterrupted run");
    let out = outcome(&h, &op).await;
    h.cleanup().await;
    out
}

/// Run `rows` until `after` lines were handed out, then drop the runner, as
/// a killed process stops; the lease runs out before this returns.
async fn kill_after(h: &Harness, after: u64, cfg: RunnerConfig) {
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let notify = Arc::new(tokio::sync::Notify::new());
    let source = Arc::new(PendAfterSource {
        inner: h.fs_source(),
        after,
        handed: Arc::new(AtomicU64::new(0)),
        notify: Arc::clone(&notify),
    });
    let deps =
        h.with_short_store_lease(h.deps_with("runner-1", cfg, source, Arc::new(h.fs_sink())));
    tokio::select! {
        _ = runner::run(&deps, record) => panic!("the runner must not finish while the reader pends"),
        () = notify.notified() => {}
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
}

async fn resume(h: &Harness, owner: &str, cfg: RunnerConfig) {
    let record = h.claim(owner, Duration::from_secs(60)).await;
    runner::run(&h.deps(owner, cfg), record)
        .await
        .expect("resumed run");
}

/// Twelve plain rows with a plain put of `dup` at row 6 and a put of `dup`
/// conditional on the item existing at row 8.
fn plain_then_conditional() -> Vec<String> {
    let mut rows = plain_rows(12);
    rows[6] = kv("dup", "1");
    rows[8] = kv_if("dup", "2", "attribute_exists(pk)");
    rows
}

async fn a_kill_past_the_checkpoint_leaves_the_later_image_of_a_key_written_twice_case(
    backend: Backend,
) {
    let rows = plain_then_conditional();
    let cfg = window(5, 1);
    let expected = uninterrupted(backend, &rows, &[], window(5, 1)).await;

    let h = Harness::on(backend).await;
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    // Rows 0 to 8 are written one batch each; the checkpoint at row 5 is
    // the last one, so rows 5 to 8, the plain and the conditional put of
    // `dup` among them, are applied past it when the process is killed.
    kill_after(&h, 9, window(5, 1)).await;
    let mid = h.record(&op).await;
    assert_eq!(mid.cursor_row_index, 5, "the last checkpoint is at row 5");
    resume(&h, "runner-2", cfg).await;

    let dup = h.get("dup").await.expect("dup is stored");
    assert_eq!(
        dup.get("v"),
        Some(&AttributeValue::N("2".to_owned())),
        "the conditional put, applied after the plain one, stands"
    );
    assert_eq!(outcome(&h, &op).await, expected);
    h.cleanup().await;
}

async fn a_client_write_after_a_killed_batch_is_not_undone_by_the_resume_case(backend: Backend) {
    let rows = plain_rows(12);
    let h = Harness::on(backend).await;
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    kill_after(&h, 9, window(5, 1)).await;
    assert_eq!(h.record(&op).await.cursor_row_index, 5);

    // A client overwrites k6 and deletes k7 after their bulk writes
    // committed and before the operation resumes.
    h.put_direct(item("k6", "100")).await;
    let info = h.key_info().await;
    h.storage
        .delete_item(
            &info,
            &key("k7"),
            false,
            None,
            &ExpressionMaps::default(),
            None,
        )
        .await
        .expect("client delete");
    resume(&h, "runner-2", window(5, 1)).await;

    let k6 = h.get("k6").await.expect("k6 is stored");
    assert_eq!(
        k6.get("v"),
        Some(&AttributeValue::N("100".to_owned())),
        "the client's put stands"
    );
    assert!(h.get("k7").await.is_none(), "the client's delete stands");
    let record = h.record(&op).await;
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 12);
    let expected = uninterrupted(backend, &rows, &[], window(5, 1)).await;
    assert_eq!(
        record.usage, expected.usage,
        "the rows applied before the kill are charged as the uninterrupted run charged them"
    );
    h.cleanup().await;
}

async fn a_transient_fault_on_a_later_write_batch_of_the_window_applies_no_row_twice_case(
    backend: Backend,
) {
    let mut rows = plain_rows(12);
    rows[1] = kv("dup", "1");
    rows[2] = kv_if("dup", "2", "attribute_exists(pk)");
    let expected = uninterrupted(backend, &rows, &[], window(10, 2)).await;

    let h = Harness::on(backend).await;
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    // The checkpoint window is ten rows and a write batch two: the batches
    // of rows 0 and 1 and of rows 2 and 3 commit, and the batch of rows 4
    // and 5 fails with a transient fault before the window's checkpoint.
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.fail_put_pk = Some("k5".to_owned());
    faulting.checkpoint_lease = Some(Duration::from_millis(100));
    let deps = h.deps_with_storage("runner-1", window(10, 2), Arc::new(faulting));
    assert!(
        runner::run(&deps, record).await.is_err(),
        "the transient fault leaves the operation to resume"
    );
    assert_eq!(h.record(&op).await.cursor_row_index, 0);
    tokio::time::sleep(Duration::from_millis(250)).await;
    resume(&h, "runner-2", window(10, 2)).await;

    let dup = h.get("dup").await.expect("dup is stored");
    assert_eq!(dup.get("v"), Some(&AttributeValue::N("2".to_owned())));
    assert_eq!(outcome(&h, &op).await, expected);
    h.cleanup().await;
}

async fn a_condition_failure_applied_before_a_kill_is_reported_as_the_uninterrupted_run_reports_it_case(
    backend: Backend,
) {
    // Row 3 fails its condition against the seeded item, row 4 then
    // replaces that item, and the kill comes before the checkpoint that
    // covers both.
    let mut rows = plain_rows(10);
    rows[3] = condition_row("5", "attribute_not_exists(pk)");
    rows[4] = kv("c", "7");
    let seed = [item("c", "1")];
    let expected = uninterrupted(backend, &rows, &seed, window(6, 1)).await;

    let h = Harness::on(backend).await;
    h.put_direct(item("c", "1")).await;
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    kill_after(&h, 5, window(6, 1)).await;
    assert_eq!(h.record(&op).await.cursor_row_index, 0);
    resume(&h, "runner-2", window(6, 1)).await;

    let got = outcome(&h, &op).await;
    let report = got.conditions.as_ref().expect("condition report rendered");
    assert_eq!(report.len(), 1);
    assert_eq!(
        report[0]["oldImage"]["Item"]["v"]["N"],
        json!("1"),
        "the record carries the item the condition was evaluated against"
    );
    assert_eq!(got, expected);
    h.cleanup().await;
}

async fn ten_resumes_without_progress_end_the_operation_failed_case(backend: Backend) {
    let h = Harness::on(backend).await;
    h.write_input("part-000.json", &file_of(&plain_rows(6)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;

    // Every attempt's first batch fails with a transient fault, the way a
    // statement timeout shorter than one batch fails every attempt, so no
    // checkpoint ever moves.
    let mut claims = 0;
    loop {
        let owner = format!("runner-{claims}");
        let record = h.claim(&owner, Duration::from_millis(100)).await;
        claims += 1;
        let mut faulting = FaultingStorage::new(&h.storage);
        faulting.fail_put_pk = Some("k0".to_owned());
        faulting.checkpoint_lease = Some(Duration::from_millis(100));
        let deps = h.deps_with_storage(&owner, window(5, 5), Arc::new(faulting));
        let result = runner::run(&deps, record).await;
        let status = h.record(&op).await.status;
        if status != "IN_PROGRESS" || claims > 11 {
            assert!(
                result.is_ok(),
                "the run that ends the operation returns cleanly"
            );
            break;
        }
        assert!(
            result.is_err(),
            "claim {claims} leaves the operation to resume"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    let record = h.record(&op).await;
    assert_eq!(
        claims, 11,
        "the first claim and ten resumes ran; the tenth resume without progress ends it"
    );
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("InternalServerError"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("Internal server error")
    );
    assert_eq!(h.count_items().await, 0);
    h.cleanup().await;
}

on_both_backends!(
    a_kill_past_the_checkpoint_leaves_the_later_image_of_a_key_written_twice_case,
    a_kill_past_the_checkpoint_leaves_the_later_image_of_a_key_written_twice,
    a_kill_past_the_checkpoint_leaves_the_later_image_of_a_key_written_twice_on_postgres
);

on_both_backends!(
    a_client_write_after_a_killed_batch_is_not_undone_by_the_resume_case,
    a_client_write_after_a_killed_batch_is_not_undone_by_the_resume,
    a_client_write_after_a_killed_batch_is_not_undone_by_the_resume_on_postgres
);

on_both_backends!(
    a_transient_fault_on_a_later_write_batch_of_the_window_applies_no_row_twice_case,
    a_transient_fault_on_a_later_write_batch_of_the_window_applies_no_row_twice,
    a_transient_fault_on_a_later_write_batch_of_the_window_applies_no_row_twice_on_postgres
);

on_both_backends!(
    a_condition_failure_applied_before_a_kill_is_reported_as_the_uninterrupted_run_reports_it_case,
    a_condition_failure_applied_before_a_kill_is_reported_as_the_uninterrupted_run_reports_it,
    a_condition_failure_applied_before_a_kill_is_reported_as_the_uninterrupted_run_reports_it_on_postgres
);

on_both_backends!(
    ten_resumes_without_progress_end_the_operation_failed_case,
    ten_resumes_without_progress_end_the_operation_failed,
    ten_resumes_without_progress_end_the_operation_failed_on_postgres
);

/// A checkpoint window of 400 rows written in batches of ten, killed after
/// 300 rows: the ledger holds about 300 rows past the checkpoint. The
/// resumed run reads them a page at a time as its flushes reach them, never
/// more than one page per read, and ends as an uninterrupted run does.
async fn a_resume_past_more_recorded_rows_than_a_page_reads_them_a_page_at_a_time_case(
    backend: Backend,
) {
    let rows = plain_rows(400);
    let expected = uninterrupted(backend, &rows, &[], window(400, 10)).await;

    let h = Harness::on(backend).await;
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    kill_after(&h, 300, window(400, 10)).await;
    assert_eq!(h.record(&op).await.cursor_row_index, 0);

    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let faulting = FaultingStorage::new(&h.storage);
    let reads = Arc::clone(&faulting.recorded_reads);
    let deps = h.deps_with_storage("runner-2", window(400, 10), Arc::new(faulting));
    runner::run(&deps, record).await.expect("resumed run");

    let reads = reads.lock().unwrap().clone();
    let page = usize::try_from(runner::RECORDED_PAGE).unwrap();
    assert!(
        reads.len() >= 3,
        "the recorded rows took several pages: {reads:?}"
    );
    assert!(reads.iter().all(|&n| n <= page), "{reads:?}");
    assert!(reads.iter().sum::<usize>() >= 290, "{reads:?}");
    assert_eq!(outcome(&h, &op).await, expected);
    h.cleanup().await;
}

on_both_backends!(
    a_resume_past_more_recorded_rows_than_a_page_reads_them_a_page_at_a_time_case,
    a_resume_past_more_recorded_rows_than_a_page_reads_them_a_page_at_a_time,
    a_resume_past_more_recorded_rows_than_a_page_reads_them_a_page_at_a_time_on_postgres
);
