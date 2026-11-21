// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the claim loop: the concurrency and per-account bounds, the
//! self-process re-claim exclusion, slot release on error, the distinct
//! owner id, the claim liveness and error metrics, and the abort of a run
//! still running at the drain deadline.

use std::sync::atomic::AtomicBool;

use super::super::bulk_operation_worker_with_interval;
use super::super::claim::{RUN_DRAIN, claim_loop};
use super::*;
use crate::bulk_put::store::BulkStore;

/// The public worker path end to end: a record inserted through
/// `start_bulk_operation` is claimed and run to a terminal state within
/// three ticks of the worker's interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_started_operation_is_claimed_and_run_to_terminal_within_three_ticks() {
    let storage = engine().await;
    let store = Root::new();
    let table_id = create_table(&storage, ACCOUNT_A, "orders").await;
    let contents = b"{\"Item\": {\"pk\": {\"S\": \"k0\"}}}\n\
                     {\"Item\": {\"pk\": {\"S\": \"k1\"}}}\n\
                     {\"Item\": {\"pk\": {\"S\": \"k2\"}}}\n";
    store.file(ACCOUNT_A, IN_BUCKET, "data.json", contents);
    let files = vec![InputFile {
        path: "data.json".to_owned(),
        size: i64::try_from(contents.len()).unwrap(),
        rows: None,
        etag: None,
        abandon: None,
    }];
    let op = start_op(&storage, ACCOUNT_A, "orders", &table_id, now_ms(), files).await;

    let tick = Duration::from_millis(250);
    let token = CancellationToken::new();
    let handle = tokio::spawn(bulk_operation_worker_with_interval(
        Arc::clone(&storage),
        Arc::new(LimitsConfig::default()),
        Arc::from(REGION),
        Some(Arc::new(BulkStore::filesystem(store.root()))),
        None,
        Arc::new(MetricsCollector::new()),
        token.clone(),
        tick,
    ));

    let done = wait_until(3 * tick, || {
        let storage = Arc::clone(&storage);
        let op = op.clone();
        async move {
            storage
                .get_bulk_operation(ACCOUNT_A, &op)
                .await
                .expect("get")
                .is_some_and(|r| r.status != "IN_PROGRESS" && r.status != "CANCELLING")
        }
    })
    .await;
    assert!(
        done,
        "the operation must reach a terminal state within three ticks"
    );

    let record = storage
        .get_bulk_operation(ACCOUNT_A, &op)
        .await
        .expect("get")
        .expect("record");
    assert_eq!(record.status, "COMPLETED");
    assert_eq!(record.processed_item_count, 3);
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("worker stops at its next tick")
        .expect("worker panicked");
}

/// With three runnable operations and two slots, only two runs start
/// while both are pended; the third starts once a slot frees.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_concurrency_bound_holds_with_more_runnable_records_than_slots() {
    let storage = engine().await;
    for (i, t) in [1000_i64, 2000, 3000].iter().enumerate() {
        let name = format!("orders_{i}");
        let table_id = create_table(&storage, ACCOUNT_A, &name).await;
        start_op(&storage, ACCOUNT_A, &name, &table_id, *t, Vec::new()).await;
    }
    let probe = RunProbe::new();
    let token = CancellationToken::new();
    let store: Arc<dyn BulkOperationStore> = storage.clone();
    let handle = tokio::spawn(claim_loop(
        store,
        2,
        3,
        Duration::from_secs(60),
        Duration::from_millis(25),
        "worker-test".to_owned(),
        Arc::new(MetricsCollector::new()),
        token.clone(),
        pending_run(Arc::clone(&probe)),
    ));

    let two_started = wait_until(Duration::from_secs(2), || {
        let probe = Arc::clone(&probe);
        async move { probe.started().len() == 2 }
    })
    .await;
    assert!(two_started, "two runs must start");
    // Several more ticks pass; the pended runs hold both slots.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        probe.started().len(),
        2,
        "the third run must not start while both slots are held"
    );

    probe.release.add_permits(1);
    let third_started = wait_until(Duration::from_secs(2), || {
        let probe = Arc::clone(&probe);
        async move { probe.started().len() == 3 }
    })
    .await;
    assert!(third_started, "the freed slot must admit the third run");

    probe.release.add_permits(2);
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("loop stops at its next tick")
        .expect("loop panicked");
}

/// Account A's four older operations cannot take every slot: once A
/// holds its per-account share, the next claim excludes A and takes
/// B's row, inside the first tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_per_account_bound_admits_the_second_account_in_the_first_tick() {
    let storage = engine().await;
    for i in 0..4_i64 {
        let name = format!("orders_a{i}");
        let table_id = create_table(&storage, ACCOUNT_A, &name).await;
        start_op(&storage, ACCOUNT_A, &name, &table_id, 1000 + i, Vec::new()).await;
    }
    let table_id = create_table(&storage, ACCOUNT_B, "orders_b").await;
    let op_b = start_op(&storage, ACCOUNT_B, "orders_b", &table_id, 5000, Vec::new()).await;

    let probe = RunProbe::new();
    let token = CancellationToken::new();
    let store: Arc<dyn BulkOperationStore> = storage.clone();
    let handle = tokio::spawn(claim_loop(
        store,
        4,
        2,
        Duration::from_secs(60),
        Duration::from_millis(50),
        "worker-test".to_owned(),
        Arc::new(MetricsCollector::new()),
        token.clone(),
        pending_run(Arc::clone(&probe)),
    ));

    let filled = wait_until(Duration::from_secs(2), || {
        let probe = Arc::clone(&probe);
        async move { probe.started().len() == 3 }
    })
    .await;
    assert!(filled, "the first tick must claim three runs");
    let started = probe.started();
    let from_a = started.iter().filter(|(a, _)| a == ACCOUNT_A).count();
    assert_eq!(from_a, 2, "account A stops at its per-account share");
    assert!(
        started.iter().any(|(_, op)| op == &op_b),
        "account B's row is claimed in the first tick"
    );
    // A has two more runnable rows and a free slot exists, but A is at
    // its share: later ticks must not admit a fourth run.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(probe.started().len(), 3);

    probe.release.add_permits(8);
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("loop stops at its next tick")
        .expect("loop panicked");
}

/// One operation, one process, a run that stalls past its lease: the
/// claim loop must not start a second incarnation of an operation it is
/// already running. If it did, and the incarnations shared one owner id,
/// every lease-conditional write of the stalled incarnation (the
/// checkpoint, `finish`, the lease re-read before the report rename)
/// would pass the owner predicate as if it still held the lease, and
/// both incarnations' report temp files would carry the same name. The
/// claim exclusion keeps the second incarnation from starting at all;
/// the per-claim owner id keeps the guards discriminating if one ever
/// does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_that_outlives_its_lease_is_not_claimed_again_by_its_own_process() {
    let storage = engine().await;
    let table_id = create_table(&storage, ACCOUNT_A, "orders").await;
    let op = start_op(&storage, ACCOUNT_A, "orders", &table_id, 1000, Vec::new()).await;
    let probe = RunProbe::new();
    let token = CancellationToken::new();
    let store: Arc<dyn BulkOperationStore> = storage.clone();
    // The lease is several ticks shorter than the pended run, so the
    // row's lease expires while its first run is still in flight.
    let handle = tokio::spawn(claim_loop(
        store,
        4,
        2,
        Duration::from_millis(100),
        Duration::from_millis(25),
        "worker-test".to_owned(),
        Arc::new(MetricsCollector::new()),
        token.clone(),
        pending_run(Arc::clone(&probe)),
    ));
    let one_started = wait_until(Duration::from_secs(2), || {
        let probe = Arc::clone(&probe);
        async move { !probe.started().is_empty() }
    })
    .await;
    assert!(one_started, "the operation must be claimed once");
    // Several full lease lengths pass while the run is pended; a loop
    // that treats its own expired lease as another process's would
    // claim the row again here.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let starts = probe.started().iter().filter(|(_, o)| o == &op).count();
    assert_eq!(
        starts, 1,
        "an operation whose run is still in flight in this process must \
         not be claimed a second time under the same owner id"
    );
    probe.release.add_permits(8);
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("loop stops at its next tick")
        .expect("loop panicked");
}

/// A run that answers `Err` frees its slot: with one slot, the second
/// operation is claimed on a later tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_that_returns_err_releases_its_slot() {
    let storage = engine().await;
    for (i, t) in [1000_i64, 2000].iter().enumerate() {
        let name = format!("orders_{i}");
        let table_id = create_table(&storage, ACCOUNT_A, &name).await;
        start_op(&storage, ACCOUNT_A, &name, &table_id, *t, Vec::new()).await;
    }
    let started: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let run_started = Arc::clone(&started);
    let token = CancellationToken::new();
    let store: Arc<dyn BulkOperationStore> = storage.clone();
    let handle = tokio::spawn(claim_loop(
        store,
        1,
        2,
        Duration::from_secs(60),
        Duration::from_millis(25),
        "worker-test".to_owned(),
        Arc::new(MetricsCollector::new()),
        token.clone(),
        move |record: BulkOperationRecord, _owner: String| {
            let started = Arc::clone(&run_started);
            Box::pin(async move {
                started.lock().expect("lock").push(record.operation_id);
                Err(RunnerError::Storage(StorageError::Internal(
                    "injected run failure".to_owned(),
                )))
            }) as BoxFuture<'static, Result<(), RunnerError>>
        },
    ));

    let both = wait_until(Duration::from_secs(2), || {
        let started = Arc::clone(&started);
        async move { started.lock().expect("lock").len() == 2 }
    })
    .await;
    assert!(
        both,
        "the failed run's slot must admit the second operation"
    );
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("loop stops at its next tick")
        .expect("loop panicked");
}

/// The claim loop records a WorkerLastSuccess point under the BulkClaim
/// source on each completed pass, the liveness signal the other workers
/// publish.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_claim_loop_records_a_worker_success_metric() {
    let storage = engine().await;
    let table_id = create_table(&storage, ACCOUNT_A, "orders").await;
    start_op(&storage, ACCOUNT_A, "orders", &table_id, 1000, Vec::new()).await;
    let metrics = Arc::new(MetricsCollector::new());
    let token = CancellationToken::new();
    let store: Arc<dyn BulkOperationStore> = storage.clone();
    let handle = tokio::spawn(claim_loop(
        store,
        2,
        2,
        Duration::from_secs(60),
        Duration::from_millis(25),
        "worker-test".to_owned(),
        Arc::clone(&metrics),
        token.clone(),
        |_record: BulkOperationRecord, _owner: String| {
            Box::pin(async { Ok(()) }) as BoxFuture<'static, Result<(), RunnerError>>
        },
    ));
    let published = wait_until(Duration::from_secs(2), || {
        let metrics = Arc::clone(&metrics);
        async move {
            worker_metric_present(
                &metrics,
                extenddb_core::metrics::MetricName::WorkerLastSuccess,
                "BulkClaim",
            )
        }
    })
    .await;
    assert!(
        published,
        "the claim loop must record WorkerLastSuccess for BulkClaim"
    );
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("loop stops at its next tick")
        .expect("loop panicked");
}

/// A claim failure records a WorkerErrorCount point under the BulkClaim
/// source, instead of the loop failing silently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_claim_loop_records_a_worker_error_metric_on_a_claim_failure() {
    let store = Arc::new(UnsupportedStore {
        claims: AtomicUsize::new(0),
        claim_internal: true,
    });
    let metrics = Arc::new(MetricsCollector::new());
    let token = CancellationToken::new();
    let handle = tokio::spawn(claim_loop(
        Arc::clone(&store) as Arc<dyn BulkOperationStore>,
        4,
        2,
        Duration::from_secs(60),
        Duration::from_millis(20),
        "worker-test".to_owned(),
        Arc::clone(&metrics),
        token.clone(),
        |_record: BulkOperationRecord, _owner: String| {
            Box::pin(async { Ok(()) }) as BoxFuture<'static, Result<(), RunnerError>>
        },
    ));
    let published = wait_until(Duration::from_secs(2), || {
        let metrics = Arc::clone(&metrics);
        async move {
            worker_metric_present(
                &metrics,
                extenddb_core::metrics::MetricName::WorkerErrorCount,
                "BulkClaim",
            )
        }
    })
    .await;
    assert!(
        published,
        "a claim failure must record WorkerErrorCount for BulkClaim"
    );
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("loop stops at its next tick")
        .expect("loop panicked");
}

/// A backend answering `Validation` from the claim stops the loop at
/// the first attempt: the loop returns on its own, without the token,
/// and never claims again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_validation_answer_from_claim_stops_the_loop() {
    let store = Arc::new(UnsupportedStore {
        claims: AtomicUsize::new(0),
        claim_internal: false,
    });
    let token = CancellationToken::new();
    let handle = tokio::spawn(claim_loop(
        Arc::clone(&store) as Arc<dyn BulkOperationStore>,
        4,
        2,
        Duration::from_secs(60),
        Duration::from_millis(20),
        "worker-test".to_owned(),
        Arc::new(MetricsCollector::new()),
        token,
        |_record: BulkOperationRecord, _owner: String| {
            Box::pin(async { Ok(()) }) as BoxFuture<'static, Result<(), RunnerError>>
        },
    ));
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("the loop must stop on its own, with no cancellation")
        .expect("loop panicked");
    assert_eq!(
        store.claims.load(Ordering::SeqCst),
        1,
        "the loop must not claim again after the refusal"
    );
}

/// Every claim carries its own owner id: the process prefix plus a
/// sequence number, so no two runs ever share the owner string their
/// lease-conditional writes bind on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_claim_carries_a_distinct_owner_id_under_the_process_prefix() {
    let storage = engine().await;
    for (i, t) in [1000_i64, 2000].iter().enumerate() {
        let name = format!("orders_{i}");
        let table_id = create_table(&storage, ACCOUNT_A, &name).await;
        start_op(&storage, ACCOUNT_A, &name, &table_id, *t, Vec::new()).await;
    }
    let owners: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let run_owners = Arc::clone(&owners);
    let token = CancellationToken::new();
    let store: Arc<dyn BulkOperationStore> = storage.clone();
    let handle = tokio::spawn(claim_loop(
        store,
        4,
        4,
        Duration::from_secs(60),
        Duration::from_millis(25),
        "worker-test".to_owned(),
        Arc::new(MetricsCollector::new()),
        token.clone(),
        move |_record: BulkOperationRecord, owner: String| {
            let owners = Arc::clone(&run_owners);
            Box::pin(async move {
                owners.lock().expect("lock").push(owner);
                Ok(())
            }) as BoxFuture<'static, Result<(), RunnerError>>
        },
    ));
    let both = wait_until(Duration::from_secs(2), || {
        let owners = Arc::clone(&owners);
        async move { owners.lock().expect("lock").len() == 2 }
    })
    .await;
    assert!(both, "both operations must be claimed and run");
    let seen = owners.lock().expect("lock").clone();
    assert_ne!(
        seen[0], seen[1],
        "two claims must never share an owner string"
    );
    for owner in &seen {
        assert!(
            owner.starts_with("worker-test-"),
            "the claim owner must be the process prefix plus a sequence, got {owner}"
        );
    }
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("loop stops at its next tick")
        .expect("loop panicked");
}

/// Sets its flag when dropped, so a test can tell that a run's future was
/// dropped.
struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// A run that never returns and never reads the token is aborted at the
/// drain deadline: the claim loop returns within `RUN_DRAIN` and a margin,
/// and the run's future has been dropped by the time it does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_still_running_at_the_drain_deadline_is_aborted() {
    let storage = engine().await;
    let table_id = create_table(&storage, ACCOUNT_A, "orders").await;
    start_op(&storage, ACCOUNT_A, "orders", &table_id, 1000, Vec::new()).await;
    let started = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let run_started = Arc::clone(&started);
    let run_dropped = Arc::clone(&dropped);
    let token = CancellationToken::new();
    let store: Arc<dyn BulkOperationStore> = storage.clone();
    let handle = tokio::spawn(claim_loop(
        store,
        1,
        1,
        Duration::from_secs(60),
        Duration::from_millis(25),
        "worker-test".to_owned(),
        Arc::new(MetricsCollector::new()),
        token.clone(),
        move |_record: BulkOperationRecord, _owner: String| {
            let started = Arc::clone(&run_started);
            let flag = DropFlag(Arc::clone(&run_dropped));
            Box::pin(async move {
                let _flag = flag;
                started.store(true, Ordering::SeqCst);
                std::future::pending::<()>().await;
                Ok(())
            }) as BoxFuture<'static, Result<(), RunnerError>>
        },
    ));
    let running = wait_until(Duration::from_secs(2), || {
        let started = Arc::clone(&started);
        async move { started.load(Ordering::SeqCst) }
    })
    .await;
    assert!(running, "the run must start");
    token.cancel();
    tokio::time::timeout(RUN_DRAIN + Duration::from_secs(2), handle)
        .await
        .expect("the loop returns within the drain and a margin")
        .expect("loop panicked");
    assert!(
        dropped.load(Ordering::SeqCst),
        "the held run's future must be dropped before the loop returns"
    );
}
