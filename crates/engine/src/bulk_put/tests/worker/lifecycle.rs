// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// End to end through the worker entry point.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_started_operation_reaches_completed_with_items_written_and_the_report_rendered() {
    let h = Harness::new().await;
    // Three clean rows plus one conditional row against a pre-existing
    // item, so the run stays COMPLETED (condition failures do not count as
    // failed items) while the condition report has a record to render.
    h.put_direct(item("c", "1")).await;
    let mut rows = plain_rows(3);
    rows.push(
        "{\"Item\": {\"pk\": {\"S\": \"c\"}, \"sk\": {\"S\": \"s\"}, \"v\": {\"N\": \"2\"}}, \
         \"ConditionExpression\": \"attribute_not_exists(pk)\"}"
            .to_owned(),
    );
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = start_on(
        &h.storage,
        TABLE,
        &h.table_id,
        files,
        input_config(""),
        both_reports(),
    )
    .await;

    let (token, handle) = spawn_worker(
        &h,
        Arc::clone(&h.storage),
        LimitsConfig::default(),
        Arc::new(MetricsCollector::new()),
    );
    let status = wait_terminal(&h, &op, Duration::from_secs(5)).await;
    assert_eq!(
        status.as_deref(),
        Some("COMPLETED"),
        "the worker must claim the operation and run it to COMPLETED"
    );

    let record = h.record(&op).await;
    assert_eq!(record.processed_item_count, 4);
    assert_eq!(record.failed_item_count, 0);
    assert!(h.get("k0").await.is_some(), "the first row is written");
    assert!(h.get("k2").await.is_some(), "the last clean row is written");
    let stored = h.get("c").await.expect("the conditional target exists");
    assert_eq!(
        stored.get("v"),
        Some(&AttributeValue::N("1".to_owned())),
        "the failed condition leaves the target untouched"
    );
    assert_eq!(h.count_items().await, 4);
    let report = h
        .read_report(&op, ReportKind::ConditionCheckFailure)
        .expect("the condition report file is on disk");
    assert_eq!(report.len(), 1);
    stop_worker(token, handle).await;
}

// Concurrency, observed through the real runner rather than the claim
// loop's bookkeeping.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_operations_on_two_tables_overlap_under_concurrency_two() {
    let h = Harness::new().await;
    let second_table_id = Harness::create_table(&h.storage, "invoices").await;
    h.write_input("a/part-000.json", &file_of(&plain_rows(2)));
    h.write_input("b/part-000.json", &file_of(&plain_rows(2)));
    let split = |prefix: &str, files: Vec<InputFile>| -> Vec<InputFile> {
        files
            .into_iter()
            .filter(|f| f.path.starts_with(prefix))
            .collect()
    };
    let all = h.enumerate().await;
    let op_a = start_on(
        &h.storage,
        TABLE,
        &h.table_id,
        split("a/", all.clone()),
        input_config("a/"),
        both_reports(),
    )
    .await;
    let op_b = start_on(
        &h.storage,
        "invoices",
        &second_table_id,
        split("b/", all),
        input_config("b/"),
        both_reports(),
    )
    .await;

    // The first checkpoint of whichever run reaches one first pends
    // forever, holding its slot mid-run; the other operation can only
    // finish if both runs are in flight at once.
    let notify = Arc::new(tokio::sync::Notify::new());
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.pend_checkpoint = Some(Arc::clone(&notify));
    let faulting: Arc<dyn StorageEngine> = Arc::new(faulting);

    let limits = LimitsConfig {
        bulk_operation_worker_concurrency: 2,
        ..LimitsConfig::default()
    };
    let (token, handle) = spawn_worker(&h, faulting, limits, Arc::new(MetricsCollector::new()));

    tokio::time::timeout(Duration::from_secs(5), notify.notified())
        .await
        .expect("one run must reach its first checkpoint");
    // The pended run holds one slot forever; only an overlapping second
    // run can terminate.
    let one_completed = {
        let start = std::time::Instant::now();
        let mut done = None;
        while start.elapsed() < Duration::from_secs(5) && done.is_none() {
            for op in [&op_a, &op_b] {
                if h.record(op).await.status == "COMPLETED" {
                    done = Some(op.clone());
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        done
    };
    let completed = one_completed
        .expect("with two slots, the second operation must complete while the first pends");
    let pended = if completed == op_a { &op_b } else { &op_a };
    assert_eq!(
        h.record(pended).await.status,
        "IN_PROGRESS",
        "the pended operation stays mid-run"
    );
    stop_worker(token, handle).await;
}

// Lease-based recovery of a failed run, through the real runner.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_operation_whose_run_errors_is_reclaimed_after_the_lease_expires_and_finishes() {
    let h = Harness::new().await;
    h.write_input("part-000.json", &file_of(&plain_rows(3)));
    let files = h.enumerate().await;
    let op = start_on(
        &h.storage,
        TABLE,
        &h.table_id,
        files,
        input_config(""),
        both_reports(),
    )
    .await;

    // The first checkpoint fails with a transient storage error, so the
    // first run returns an error and the operation stays non-terminal,
    // leased for the configured second. One slot: a worker that leaked
    // the failed run's slot could never claim again.
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.fail_checkpoint_once = true;
    let faulting: Arc<dyn StorageEngine> = Arc::new(faulting);
    // One slot, and the smallest lease the configuration can express;
    // the re-claim happens on the first tick after the lease expires.
    let limits = LimitsConfig {
        bulk_operation_worker_concurrency: 1,
        bulk_operation_lease_seconds: 1,
        ..LimitsConfig::default()
    };
    let (token, handle) = spawn_worker(&h, faulting, limits, Arc::new(MetricsCollector::new()));

    let status = wait_terminal(&h, &op, Duration::from_secs(10)).await;
    assert_eq!(
        status.as_deref(),
        Some("COMPLETED"),
        "the operation must be re-claimed after its lease expires and finish"
    );
    let record = h.record(&op).await;
    assert_eq!(record.processed_item_count, 3, "no double count on resume");
    assert_eq!(h.count_items().await, 3);
    stop_worker(token, handle).await;
}
