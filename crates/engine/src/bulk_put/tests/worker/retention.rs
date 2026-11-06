// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

use super::*;

// Retention: the cutoff separates an old terminal record from a young one.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_keeps_a_younger_terminal_operation_while_purging_an_older_one() {
    let h = Harness::new().await;
    let owner = "retention-e2e";
    let young_table_id = Harness::create_table(&h.storage, "young_table").await;
    let live_table_id = Harness::create_table(&h.storage, "live_table").await;

    // Each operation is claimed, given one Applied ledger row through a
    // marked put, and finished with a chosen end time; the third stays
    // IN_PROGRESS. Terminal transitions clear each table's guard before
    // the next start, so the claims are unambiguous.
    // Forty days old: past the thirty day default window.
    let op_old = start_claim_mark_finish(
        &h,
        owner,
        TABLE,
        &h.table_id,
        "old",
        now_ms() - 40 * 86_400_000,
    )
    .await;
    // Terminal a moment ago: inside the window.
    let op_young =
        start_claim_mark_finish(&h, owner, "young_table", &young_table_id, "young", now_ms()).await;
    let op_live = start_on(
        &h.storage,
        "live_table",
        &live_table_id,
        Vec::new(),
        input_config(""),
        both_reports(),
    )
    .await;

    let token = CancellationToken::new();
    let handle = tokio::spawn(bulk_operation_retention_worker(
        Arc::clone(&h.storage),
        Arc::new(LimitsConfig::default()),
        Arc::new(MetricsCollector::new()),
        token.clone(),
    ));
    let purged = {
        let start = std::time::Instant::now();
        let mut gone = false;
        while start.elapsed() < Duration::from_secs(3) && !gone {
            gone = h
                .storage
                .get_bulk_operation(ACCOUNT, &op_old)
                .await
                .expect("get")
                .is_none();
            if !gone {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        gone
    };
    assert!(
        purged,
        "the startup pass must purge the old terminal record"
    );
    assert!(
        h.storage
            .bulk_row_outcome(ACCOUNT, &op_old, 0, 0)
            .await
            .expect("ledger read")
            .is_none(),
        "the purged operation's ledger rows go with it"
    );
    assert!(
        h.storage
            .get_bulk_operation(ACCOUNT, &op_young)
            .await
            .expect("get")
            .is_some(),
        "the younger terminal operation survives"
    );
    assert!(
        h.storage
            .bulk_row_outcome(ACCOUNT, &op_young, 0, 0)
            .await
            .expect("ledger read")
            .is_some(),
        "the younger operation's ledger rows survive"
    );
    assert!(
        h.storage
            .get_bulk_operation(ACCOUNT, &op_live)
            .await
            .expect("get")
            .is_some(),
        "the in-progress operation survives"
    );
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("the retention worker stops at its next tick")
        .expect("the retention worker panicked");
}

// Metrics: one operation started through dispatch and finished by the
// worker leaves exactly the three counters the spec names.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_collector_shows_started_finished_and_items_written_for_one_operation() {
    let h = Harness::new().await;
    let metrics = Arc::new(MetricsCollector::new());
    h.put_direct(item("c", "1")).await;
    let mut rows = plain_rows(3);
    rows.push(
        "{\"Item\": {\"pk\": {\"S\": \"c\"}, \"sk\": {\"S\": \"s\"}, \"v\": {\"N\": \"2\"}}, \
         \"ConditionExpression\": \"attribute_not_exists(pk)\"}"
            .to_owned(),
    );
    h.write_input("part-000.json", &file_of(&rows));

    // Started is recorded by the start handler on the created outcome, so
    // the operation starts through dispatch with the collector in the
    // context.
    let ctx = OperationContext {
        storage: Arc::clone(&h.storage),
        backup: Arc::new(crate::BackupRuntime::new(4)),
        limits: Arc::new(LimitsConfig::default()),
        region: Arc::from(REGION),
        account_id: Arc::from(ACCOUNT),
        import_paths: Vec::new().into(),
        export_paths: Vec::new().into(),
        bulk: Some(Arc::new(BulkStore::filesystem(h.store.root()))),
        bulk_authorizer: None,
        bulk_principal: None,
        authorized_key_info: Default::default(),
        auth_cache: AuthCacheRegistry::empty(),
        table_key_info_lookup: None,
        metrics: Arc::clone(&metrics),
    };
    let body = json!({
        "TableName": TABLE,
        "InputS3Configuration": input_config(""),
        "OutputS3Configuration": both_reports(),
    });
    let response = crate::dispatch("StartBulkPutItems", body, &ctx, "127.0.0.1:0")
        .await
        .expect("the start must succeed");
    let op_arn = response.body["BulkOperationDescription"]["BulkOperationArn"]
        .as_str()
        .expect("the description carries an ARN")
        .to_owned();
    let op = op_arn
        .rsplit_once('/')
        .expect("the ARN carries the operation id")
        .1
        .to_owned();

    let (token, handle) = spawn_worker(
        &h,
        Arc::clone(&h.storage),
        LimitsConfig::default(),
        Arc::clone(&metrics),
    );
    let status = wait_terminal(&h, &op, Duration::from_secs(5)).await;
    assert_eq!(status.as_deref(), Some("COMPLETED"));
    wait_for_metric(
        &metrics,
        MetricName::BulkOperationsFinished,
        Duration::from_secs(2),
    )
    .await;
    stop_worker(token, handle).await;

    let started = one_snapshot(&metrics, MetricName::BulkOperationsStarted);
    assert_eq!(count(&started), 1, "one operation was started");
    assert!(
        started
            .dimensions
            .contains(&Dimension::TableName(TABLE.to_owned())),
        "Started carries the table dimension: {:?}",
        started.dimensions
    );

    let finished = one_snapshot(&metrics, MetricName::BulkOperationsFinished);
    assert_eq!(count(&finished), 1, "one operation finished");
    assert!(
        finished
            .dimensions
            .contains(&Dimension::Operation("COMPLETED".to_owned())),
        "Finished carries the terminal status: {:?}",
        finished.dimensions
    );
    assert!(
        finished
            .dimensions
            .contains(&Dimension::TableName(TABLE.to_owned())),
        "Finished carries the table dimension: {:?}",
        finished.dimensions
    );

    // Three of the four processed rows were applied; the condition-failed
    // row wrote nothing, so it is not an applied row.
    let written = one_snapshot(&metrics, MetricName::BulkPutItemsWritten);
    assert_eq!(
        count(&written),
        3,
        "BulkPutItemsWritten equals the applied rows"
    );
    assert!(
        written
            .dimensions
            .contains(&Dimension::TableName(TABLE.to_owned())),
        "ItemsWritten carries the table dimension: {:?}",
        written.dimensions
    );
}

// The stored terminal status in metrics, and the ledger-first retention
// ordering.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_operation_records_finished_with_the_failed_status() {
    let h = Harness::new().await;
    let metrics = Arc::new(MetricsCollector::new());
    h.write_input("part-000.json", &file_of(&plain_rows(12)));
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

    // Twelve rows against a ten row cap: the run fails with
    // ValidationException, so `finish` stores FAILED.
    let limits = LimitsConfig {
        max_bulk_item_count: 10,
        ..LimitsConfig::default()
    };
    let (token, handle) = spawn_worker(&h, Arc::clone(&h.storage), limits, Arc::clone(&metrics));
    let status = wait_terminal(&h, &op, Duration::from_secs(5)).await;
    assert_eq!(
        status.as_deref(),
        Some("FAILED"),
        "the worker must run the operation to FAILED"
    );
    wait_for_metric(
        &metrics,
        MetricName::BulkOperationsFinished,
        Duration::from_secs(2),
    )
    .await;
    stop_worker(token, handle).await;

    // Exactly one Finished series exists, so no COMPLETED series was
    // recorded beside it, and its status dimension is the stored FAILED.
    let finished = one_snapshot(&metrics, MetricName::BulkOperationsFinished);
    assert_eq!(count(&finished), 1, "one operation finished");
    assert!(
        finished
            .dimensions
            .contains(&Dimension::Operation("FAILED".to_owned())),
        "the Finished dimension is the stored FAILED: {:?}",
        finished.dimensions
    );
    assert!(
        !finished
            .dimensions
            .contains(&Dimension::Operation("COMPLETED".to_owned())),
        "no COMPLETED dimension on a failed operation: {:?}",
        finished.dimensions
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_that_fails_a_ledger_delete_keeps_the_record_for_the_next_pass() {
    let h = Harness::new().await;
    let owner = "retention-fail";
    let op = start_claim_mark_finish(
        &h,
        owner,
        TABLE,
        &h.table_id,
        "old",
        now_ms() - 40 * 86_400_000,
    )
    .await;

    let purge_calls = Arc::new(AtomicU64::new(0));
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.fail_ledger_delete_once = true;
    faulting.purge_calls = Arc::clone(&purge_calls);
    let faulting: Arc<dyn StorageEngine> = Arc::new(faulting);

    // Pass one: the ledger delete fails, so the pair is excluded from the
    // purge and the record survives the pass.
    let token = CancellationToken::new();
    let handle = tokio::spawn(bulk_operation_retention_worker(
        Arc::clone(&faulting),
        Arc::new(LimitsConfig::default()),
        Arc::new(MetricsCollector::new()),
        token.clone(),
    ));
    let pass_done = {
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(3) && purge_calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        purge_calls.load(Ordering::SeqCst) >= 1
    };
    assert!(pass_done, "the startup pass must run its purge");
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("the retention worker stops at its next tick")
        .expect("the retention worker panicked");
    assert!(
        h.storage
            .get_bulk_operation(ACCOUNT, &op)
            .await
            .expect("get")
            .is_some(),
        "a failed ledger delete keeps the record for the next pass"
    );
    assert!(
        h.storage
            .bulk_row_outcome(ACCOUNT, &op, 0, 0)
            .await
            .expect("ledger read")
            .is_some(),
        "the ledger rows survive the failed delete"
    );

    // Pass two: the injected failure is spent, so the ledger delete
    // succeeds and the pair is purged with its rows.
    let token = CancellationToken::new();
    let handle = tokio::spawn(bulk_operation_retention_worker(
        Arc::clone(&faulting),
        Arc::new(LimitsConfig::default()),
        Arc::new(MetricsCollector::new()),
        token.clone(),
    ));
    let purged = {
        let start = std::time::Instant::now();
        let mut gone = false;
        while start.elapsed() < Duration::from_secs(3) && !gone {
            gone = h
                .storage
                .get_bulk_operation(ACCOUNT, &op)
                .await
                .expect("get")
                .is_none();
            if !gone {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        gone
    };
    assert!(purged, "the next pass must purge the re-listed record");
    assert!(
        h.storage
            .bulk_row_outcome(ACCOUNT, &op, 0, 0)
            .await
            .expect("ledger read")
            .is_none(),
        "the ledger rows are gone once the purge lands"
    );
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("the retention worker stops at its next tick")
        .expect("the retention worker panicked");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_between_ledger_delete_and_purge_leaves_no_orphaned_ledger_rows() {
    let h = Harness::new().await;
    let owner = "retention-crash";
    let op = start_claim_mark_finish(
        &h,
        owner,
        TABLE,
        &h.table_id,
        "old",
        now_ms() - 40 * 86_400_000,
    )
    .await;

    // Pass one dies between the ledger delete and the purge: the delete
    // commits, then the worker task is aborted while it pends.
    let notify = Arc::new(tokio::sync::Notify::new());
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.pend_after_ledger_delete = Some(Arc::clone(&notify));
    let faulting: Arc<dyn StorageEngine> = Arc::new(faulting);
    let token = CancellationToken::new();
    let handle = tokio::spawn(bulk_operation_retention_worker(
        Arc::clone(&faulting),
        Arc::new(LimitsConfig::default()),
        Arc::new(MetricsCollector::new()),
        token.clone(),
    ));
    tokio::time::timeout(Duration::from_secs(3), notify.notified())
        .await
        .expect("the pass must reach the ledger delete");
    handle.abort();
    let _ = handle.await;

    assert!(
        h.storage
            .get_bulk_operation(ACCOUNT, &op)
            .await
            .expect("get")
            .is_some(),
        "the crash landed before the purge, so the record still exists"
    );
    assert!(
        h.storage
            .bulk_row_outcome(ACCOUNT, &op, 0, 0)
            .await
            .expect("ledger read")
            .is_none(),
        "the ledger delete committed before the crash"
    );

    // The next pass re-lists the pair because the record still exists,
    // re-runs the idempotent ledger delete, and purges: nothing is
    // orphaned by the crash.
    let token = CancellationToken::new();
    let handle = tokio::spawn(bulk_operation_retention_worker(
        Arc::clone(&faulting),
        Arc::new(LimitsConfig::default()),
        Arc::new(MetricsCollector::new()),
        token.clone(),
    ));
    let purged = {
        let start = std::time::Instant::now();
        let mut gone = false;
        while start.elapsed() < Duration::from_secs(3) && !gone {
            gone = h
                .storage
                .get_bulk_operation(ACCOUNT, &op)
                .await
                .expect("get")
                .is_none();
            if !gone {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        gone
    };
    assert!(purged, "the next pass must purge the re-listed record");
    assert!(
        h.storage
            .bulk_row_outcome(ACCOUNT, &op, 0, 0)
            .await
            .expect("ledger read")
            .is_none(),
        "no ledger row remains after the recovery pass"
    );
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("the retention worker stops at its next tick")
        .expect("the retention worker panicked");
}
