// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A claim that lost the operation commits nothing, whichever claim ended
//! it (a run, a store mismatch, the no-store cancel drain), and a server
//! shutdown stops a run at the next row with its rows written, a checkpoint
//! and the lease released.

use super::*;

use crate::bulk_put::run_context::STORE_S3;
use crate::bulk_put::worker::bulk_operation_worker_with_interval;

fn kv(pk: &str, v: &str) -> String {
    format!(
        "{{\"Item\": {{\"pk\": {{\"S\": \"{pk}\"}}, \"sk\": {{\"S\": \"s\"}}, \"v\": {{\"N\": \"{v}\"}}}}}}"
    )
}

fn window(batch_size: usize, write_batch_rows: usize) -> RunnerConfig {
    RunnerConfig {
        batch_size,
        write_batch_rows,
        ..config()
    }
}

/// The counters, usage and stored values of a finished operation.
async fn summary(h: &Harness, op: &str) -> (String, i64, i64, Value, Vec<Option<Item>>) {
    let record = h.record(op).await;
    let mut items = Vec::new();
    for pk in ["dup", "k1", "k5", "k9", "k20", "k29"] {
        items.push(h.get(pk).await);
    }
    (
        record.status,
        record.processed_item_count,
        record.processed_input_bytes,
        record.usage,
        items,
    )
}

async fn uninterrupted(
    backend: Backend,
    rows: &[String],
    cfg: RunnerConfig,
) -> (String, i64, i64, Value, Vec<Option<Item>>) {
    let h = Harness::on(backend).await;
    h.write_input("part-000.json", &file_of(rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-1", cfg), record)
        .await
        .expect("uninterrupted run");
    let out = summary(&h, &op).await;
    h.cleanup().await;
    out
}

async fn a_batch_still_in_flight_when_another_claim_takes_over_writes_nothing_case(
    backend: Backend,
) {
    let mut rows = plain_rows(10);
    rows[0] = kv("dup", "1");
    rows[6] = kv("dup", "2");
    let expected = uninterrupted(backend, &rows, window(5, 1)).await;

    let h = Harness::on(backend).await;
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;

    // Runner-1's first batch, row 0's put of `dup`, is held between the
    // runner and storage, past runner-1's own lease check, the way a batch
    // of a process that stalled is still in flight when its lease runs out.
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.hold_first_batch = Some((Arc::clone(&entered), Arc::clone(&release)));
    faulting.checkpoint_lease = Some(Duration::from_millis(100));
    let deps = h.deps_with_storage("runner-1", window(5, 1), Arc::new(faulting));
    let run = runner::run(&deps, record);
    tokio::pin!(run);
    tokio::select! {
        _ = &mut run => panic!("runner-1 must reach its held batch"),
        () = entered.notified() => {}
    }

    // The lease runs out; runner-2 takes the operation over and finishes
    // it, writing row 6's later put of `dup`.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-2", window(5, 1)), record)
        .await
        .expect("runner-2's run");
    assert_eq!(h.record(&op).await.status, "COMPLETED");

    // Runner-1's batch now reaches storage and is refused.
    release.notify_one();
    let late = run.await;
    assert!(
        late.is_ok(),
        "a claim that lost the operation stops cleanly, got {late:?}"
    );
    let dup = h.get("dup").await.expect("dup is stored");
    assert_eq!(
        dup.get("v"),
        Some(&AttributeValue::N("2".to_owned())),
        "the late batch of the earlier claim wrote nothing"
    );
    assert_eq!(summary(&h, &op).await, expected);
    h.cleanup().await;
}

async fn a_shutdown_checkpoints_at_the_next_row_and_releases_the_lease_case(backend: Backend) {
    let mut rows = plain_rows(30);
    rows[6] = kv("dup", "1");
    rows[14] = kv("dup", "2");
    let expected = uninterrupted(backend, &rows, window(25, 3)).await;

    let h = Harness::on(backend).await;
    h.write_input("part-000.json", &file_of(&rows));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_secs(60)).await;

    // The server's shutdown token is cancelled just before line 12 is read.
    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    let action: LineAction = Arc::new(move || {
        let token = token.clone();
        Box::pin(async move { token.cancel() })
    });
    let source = Arc::new(ActAtSource {
        inner: h.fs_source(),
        at: 12,
        handed: Arc::new(AtomicU64::new(0)),
        fired: Arc::new(AtomicBool::new(false)),
        action,
    });
    let mut deps = h.deps_with("runner-1", window(25, 3), source, Arc::new(h.fs_sink()));
    deps.shutdown = shutdown;
    runner::run(&deps, record)
        .await
        .expect("a shutdown is a clean stop");

    let mid = h.record(&op).await;
    assert_eq!(
        mid.status, "IN_PROGRESS",
        "a shutdown is not a terminal transition"
    );
    assert_eq!(
        mid.cursor_row_index, 13,
        "the run stopped after row 12 and checkpointed every row it read"
    );
    assert_eq!(mid.processed_item_count, 13);
    assert_eq!(
        h.count_items().await,
        13,
        "the staged rows were written first"
    );
    assert!(
        mid.lease_until_ms.is_some_and(|until| until <= now_ms()),
        "the checkpoint released the lease, got {:?}",
        mid.lease_until_ms
    );

    // The next process claims at once, without waiting out a lease.
    tokio::time::sleep(Duration::from_millis(5)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    runner::run(&h.deps("runner-2", window(25, 3)), record)
        .await
        .expect("resumed run");
    assert_eq!(summary(&h, &op).await, expected);
    h.cleanup().await;
}

/// Runner-1's storage: the first batch is held between the runner and
/// storage until `release`, after it signals `entered`, the way a batch of
/// a process that stalled past its lease is still in flight when another
/// claim ends the operation.
fn holding_first_batch(
    h: &Harness,
    entered: &Arc<tokio::sync::Notify>,
    release: &Arc<tokio::sync::Notify>,
) -> RunnerDeps {
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.hold_first_batch = Some((Arc::clone(entered), Arc::clone(release)));
    faulting.checkpoint_lease = Some(Duration::from_millis(100));
    h.deps_with_storage("runner-1", window(5, 5), Arc::new(faulting))
}

/// Storage for the claim that ends the operation: its terminal `finish`
/// commits, then signals `finished` and waits for `resume`, so the earlier
/// claim's batch can be released right after the terminal transition.
fn holding_after_finish(
    h: &Harness,
    finished: &Arc<tokio::sync::Notify>,
    resume: &Arc<tokio::sync::Notify>,
) -> Arc<dyn StorageEngine> {
    let mut faulting = FaultingStorage::new(&h.storage);
    faulting.hold_after_finish = Some((Arc::clone(finished), Arc::clone(resume)));
    Arc::new(faulting)
}

/// A claim on a server whose bulk store is not the one the operation started
/// on ends it `FAILED` unread. It records itself as the write owner first,
/// so a batch of the claim before it that is still in flight writes nothing
/// after the terminal transition.
async fn a_batch_in_flight_writes_nothing_after_a_store_mismatch_fails_the_operation_case(
    backend: Backend,
) {
    let h = Harness::on(backend).await;
    h.write_input("part-000.json", &file_of(&plain_rows(10)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let deps = holding_first_batch(&h, &entered, &release);
    let run = runner::run(&deps, record);
    tokio::pin!(run);
    tokio::select! {
        _ = &mut run => panic!("runner-1 must reach its held batch"),
        () = entered.notified() => {}
    }

    // The lease runs out. Runner-2 runs on a server configured with the S3
    // store and ends the filesystem operation FAILED; it is held right after
    // its terminal transition commits.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let record = h.claim("runner-2", Duration::from_secs(60)).await;
    let finished = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let mut deps2 = h.deps_with_storage(
        "runner-2",
        window(5, 5),
        holding_after_finish(&h, &finished, &resume),
    );
    deps2.store_kind = STORE_S3;
    let run2 = runner::run(&deps2, record);
    tokio::pin!(run2);
    tokio::select! {
        _ = &mut run2 => panic!("runner-2 must reach its terminal transition"),
        () = finished.notified() => {}
    }
    assert_eq!(h.record(&op).await.status, "FAILED");

    // Runner-1's batch of five rows now reaches storage.
    release.notify_one();
    let late = run.await;
    assert!(
        late.is_ok(),
        "a claim that lost the operation stops cleanly, got {late:?}"
    );
    resume.notify_one();
    run2.await.expect("runner-2's run");

    assert_eq!(
        h.count_items().await,
        0,
        "the earlier claim's batch wrote nothing after the operation was FAILED"
    );
    let record = h.record(&op).await;
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("InternalServerError"));
    assert_eq!(record.processed_item_count, 0);
    h.cleanup().await;
}

/// The worker of a server with no bulk store drains a cancel to `CANCELLED`.
/// It records itself as the write owner first, so a batch of the claim
/// before it that is still in flight writes nothing after the terminal
/// transition.
async fn a_batch_in_flight_writes_nothing_after_the_no_store_cancel_drain_case(backend: Backend) {
    let h = Harness::on(backend).await;
    h.write_input("part-000.json", &file_of(&plain_rows(10)));
    let files = h.enumerate().await;
    let op = h.start(files, input_config(false), both_reports()).await;
    let record = h.claim("runner-1", Duration::from_millis(100)).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let deps = holding_first_batch(&h, &entered, &release);
    let run = runner::run(&deps, record);
    tokio::pin!(run);
    tokio::select! {
        _ = &mut run => panic!("runner-1 must reach its held batch"),
        () = entered.notified() => {}
    }

    // The caller cancels and runner-1's lease runs out. The no-store
    // worker claims the operation, ends it CANCELLED, and is held right
    // after its terminal transition commits.
    h.storage
        .request_cancel(ACCOUNT, &op)
        .await
        .expect("request cancel");
    tokio::time::sleep(Duration::from_millis(250)).await;
    let finished = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let token = CancellationToken::new();
    let drain = tokio::spawn(bulk_operation_worker_with_interval(
        holding_after_finish(&h, &finished, &resume),
        Arc::new(LimitsConfig::default()),
        Arc::from(REGION),
        None,
        None,
        Arc::new(MetricsCollector::new()),
        token.clone(),
        Duration::from_millis(50),
    ));
    tokio::time::timeout(Duration::from_secs(10), finished.notified())
        .await
        .expect("the cancel drain reaches its terminal transition");
    assert_eq!(h.record(&op).await.status, "CANCELLED");

    // Runner-1's batch of five rows now reaches storage.
    release.notify_one();
    let late = run.await;
    assert!(
        late.is_ok(),
        "a claim that lost the operation stops cleanly, got {late:?}"
    );
    resume.notify_one();
    token.cancel();
    tokio::time::timeout(Duration::from_secs(10), drain)
        .await
        .expect("the worker stops at its next tick")
        .expect("the worker did not panic");

    assert_eq!(
        h.count_items().await,
        0,
        "the earlier claim's batch wrote nothing after the operation was CANCELLED"
    );
    let record = h.record(&op).await;
    assert_eq!(record.status, "CANCELLED");
    assert_eq!(record.processed_item_count, 0);
    h.cleanup().await;
}

on_both_backends!(
    a_batch_still_in_flight_when_another_claim_takes_over_writes_nothing_case,
    a_batch_still_in_flight_when_another_claim_takes_over_writes_nothing,
    a_batch_still_in_flight_when_another_claim_takes_over_writes_nothing_on_postgres
);

on_both_backends!(
    a_batch_in_flight_writes_nothing_after_a_store_mismatch_fails_the_operation_case,
    a_batch_in_flight_writes_nothing_after_a_store_mismatch_fails_the_operation,
    a_batch_in_flight_writes_nothing_after_a_store_mismatch_fails_the_operation_on_postgres
);

on_both_backends!(
    a_batch_in_flight_writes_nothing_after_the_no_store_cancel_drain_case,
    a_batch_in_flight_writes_nothing_after_the_no_store_cancel_drain,
    a_batch_in_flight_writes_nothing_after_the_no_store_cancel_drain_on_postgres
);

on_both_backends!(
    a_shutdown_checkpoints_at_the_next_row_and_releases_the_lease_case,
    a_shutdown_checkpoints_at_the_next_row_and_releases_the_lease,
    a_shutdown_checkpoints_at_the_next_row_and_releases_the_lease_on_postgres
);
