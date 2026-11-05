// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A claim that lost the operation commits nothing, and a server shutdown
//! stops a run at the next row with its rows written, a checkpoint and the
//! lease released.

use super::*;

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

on_both_backends!(
    a_batch_still_in_flight_when_another_claim_takes_over_writes_nothing_case,
    a_batch_still_in_flight_when_another_claim_takes_over_writes_nothing,
    a_batch_still_in_flight_when_another_claim_takes_over_writes_nothing_on_postgres
);

on_both_backends!(
    a_shutdown_checkpoints_at_the_next_row_and_releases_the_lease_case,
    a_shutdown_checkpoints_at_the_next_row_and_releases_the_lease,
    a_shutdown_checkpoints_at_the_next_row_and_releases_the_lease_on_postgres
);
