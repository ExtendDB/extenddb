// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Tests for the retention worker: purging an old terminal operation and
//! its ledger rows, the retention-days floor, and the retention liveness
//! metric.

use super::super::bulk_operation_retention_worker;
use super::super::floored;
use super::*;

/// The retention worker's startup pass deletes a terminal operation
/// past the window together with its ledger rows, and leaves an
/// in-progress operation and its ledger untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_purges_an_old_terminal_operation_and_its_ledger_rows() {
    let storage = engine().await;
    let owner = "retention-test";

    let table_id = create_table(&storage, ACCOUNT_A, "old_table").await;
    let old_op = start_op(
        &storage,
        ACCOUNT_A,
        "old_table",
        &table_id,
        1000,
        Vec::new(),
    )
    .await;
    let table_id = create_table(&storage, ACCOUNT_A, "live_table").await;
    let live_op = start_op(
        &storage,
        ACCOUNT_A,
        "live_table",
        &table_id,
        2000,
        Vec::new(),
    )
    .await;
    let claimed = storage
        .claim_runnable(owner, Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 2);
    write_ledger_row(&storage, ACCOUNT_A, "old_table", &old_op).await;
    write_ledger_row(&storage, ACCOUNT_A, "live_table", &live_op).await;
    // Terminal forty days ago, past the thirty day default window. The
    // live operation stays IN_PROGRESS.
    let old_end = now_ms() - 40 * 86_400_000;
    let state = storage
        .finish(
            ACCOUNT_A,
            &old_op,
            owner,
            Terminal {
                status: "COMPLETED".to_owned(),
                end_time_ms: old_end,
                failure_code: None,
                failure_message: None,
                usage: json!({}),
            },
        )
        .await
        .expect("finish");
    assert!(matches!(state, FinishState::Held(_)));

    let token = CancellationToken::new();
    let handle = tokio::spawn(bulk_operation_retention_worker(
        Arc::clone(&storage),
        Arc::new(LimitsConfig::default()),
        Arc::new(MetricsCollector::new()),
        token.clone(),
    ));
    let purged = wait_until(Duration::from_secs(2), || {
        let storage = Arc::clone(&storage);
        let old_op = old_op.clone();
        async move {
            storage
                .get_bulk_operation(ACCOUNT_A, &old_op)
                .await
                .expect("get")
                .is_none()
        }
    })
    .await;
    assert!(
        purged,
        "the startup pass must purge the old terminal record"
    );

    assert!(
        storage
            .bulk_row_outcome(ACCOUNT_A, &old_op, 0, 0)
            .await
            .expect("ledger read")
            .is_none(),
        "the purged operation's ledger rows must be deleted"
    );
    assert!(
        storage
            .get_bulk_operation(ACCOUNT_A, &live_op)
            .await
            .expect("get")
            .is_some(),
        "the in-progress operation must survive"
    );
    assert!(
        storage
            .bulk_row_outcome(ACCOUNT_A, &live_op, 0, 0)
            .await
            .expect("ledger read")
            .is_some(),
        "the in-progress operation's ledger rows must survive"
    );
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("retention worker stops at its next tick")
        .expect("retention worker panicked");
}

/// A retention window configured to zero is floored to one day, so the
/// startup pass keeps a record that finished moments ago. An operation
/// that finished long ago is purged whatever the window, so it is the
/// sign the pass ran.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_floors_zero_days_and_keeps_a_recent_terminal_operation() {
    let storage = engine().await;
    let owner = "retention-floor-test";

    let old_table = create_table(&storage, ACCOUNT_A, "old_table").await;
    let old_op = start_op(
        &storage,
        ACCOUNT_A,
        "old_table",
        &old_table,
        1000,
        Vec::new(),
    )
    .await;
    let recent_table = create_table(&storage, ACCOUNT_A, "recent_table").await;
    let recent_op = start_op(
        &storage,
        ACCOUNT_A,
        "recent_table",
        &recent_table,
        2000,
        Vec::new(),
    )
    .await;

    let claimed = storage
        .claim_runnable(owner, Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 2);

    let old_end = now_ms() - 40 * 86_400_000;
    storage
        .finish(
            ACCOUNT_A,
            &old_op,
            owner,
            Terminal {
                status: "COMPLETED".to_owned(),
                end_time_ms: old_end,
                failure_code: None,
                failure_message: None,
                usage: json!({}),
            },
        )
        .await
        .expect("finish old");
    // One hour ago: inside a one day window (kept once zero is floored to
    // one), outside a zero window (its cutoff is now, so it would be
    // purged). The margin keeps the test off the cutoff boundary.
    let recent_end = now_ms() - 3_600_000;
    storage
        .finish(
            ACCOUNT_A,
            &recent_op,
            owner,
            Terminal {
                status: "COMPLETED".to_owned(),
                end_time_ms: recent_end,
                failure_code: None,
                failure_message: None,
                usage: json!({}),
            },
        )
        .await
        .expect("finish recent");

    let limits = Arc::new(LimitsConfig {
        bulk_operation_retention_days: 0,
        ..LimitsConfig::default()
    });
    let token = CancellationToken::new();
    let handle = tokio::spawn(bulk_operation_retention_worker(
        Arc::clone(&storage),
        limits,
        Arc::new(MetricsCollector::new()),
        token.clone(),
    ));

    let old_purged = wait_until(Duration::from_secs(2), || {
        let storage = Arc::clone(&storage);
        let old_op = old_op.clone();
        async move {
            storage
                .get_bulk_operation(ACCOUNT_A, &old_op)
                .await
                .expect("get")
                .is_none()
        }
    })
    .await;
    assert!(old_purged, "the old terminal record must be purged");

    assert!(
        storage
            .get_bulk_operation(ACCOUNT_A, &recent_op)
            .await
            .expect("get")
            .is_some(),
        "a record that finished moments ago must survive a zero window floored to one day"
    );

    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("retention worker stops at its next tick")
        .expect("retention worker panicked");
}

/// The retention floor clamps a zero window to one and emits the shared
/// clamp warning naming the key; a configured value passes through with
/// no warning.
#[test]
fn the_retention_floor_clamps_zero_to_one_and_warns() {
    assert_eq!(floored(0u32, "bulk_operation_retention_days"), 1);
    assert_eq!(floored(30u32, "bulk_operation_retention_days"), 30);

    let captured: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let subscriber = CaptureWarnings {
        lines: Arc::clone(&captured),
    };
    tracing::subscriber::with_default(subscriber, || {
        let _ = floored(0u32, "bulk_operation_retention_days");
        let _ = floored(30u32, "bulk_operation_retention_days");
    });
    let lines = captured.lock().expect("lock").clone();
    assert_eq!(lines.len(), 1, "only the zero value warns");
    assert!(
        lines[0].contains("bulk worker setting floored at 1"),
        "the warn carries the shared clamp message: {}",
        lines[0]
    );
    assert!(
        lines[0].contains("bulk_operation_retention_days"),
        "the warn names the key: {}",
        lines[0]
    );
}

/// The retention worker records a WorkerLastSuccess point under the
/// BulkRetention source on a completed pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_retention_worker_records_a_worker_success_metric() {
    let storage = engine().await;
    let metrics = Arc::new(MetricsCollector::new());
    let token = CancellationToken::new();
    let handle = tokio::spawn(bulk_operation_retention_worker(
        Arc::clone(&storage),
        Arc::new(LimitsConfig::default()),
        Arc::clone(&metrics),
        token.clone(),
    ));
    let published = wait_until(Duration::from_secs(2), || {
        let metrics = Arc::clone(&metrics);
        async move {
            worker_metric_present(
                &metrics,
                extenddb_core::metrics::MetricName::WorkerLastSuccess,
                "BulkRetention",
            )
        }
    })
    .await;
    assert!(
        published,
        "the retention worker must record WorkerLastSuccess for BulkRetention"
    );
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("retention worker stops at its next tick")
        .expect("retention worker panicked");
}
