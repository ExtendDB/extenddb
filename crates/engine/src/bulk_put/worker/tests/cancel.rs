// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Test for the no-store cancel drain: a CANCELLING operation reaches
//! CANCELLED with no bulk store configured while an uncancelled run stays
//! non-terminal.

use super::super::bulk_operation_worker_with_interval;
use super::*;

/// With no `[bulk]` store the worker finishes a CANCELLING operation as
/// CANCELLED within a bounded time, and leaves an `IN_PROGRESS`
/// operation that was never cancelled non-terminal. This is the off
/// switch path: an operator removes the section, a customer's cancel
/// still reaches a terminal state, and an operation that is running
/// with no cancel does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_no_store_the_worker_finishes_a_cancel_and_leaves_an_in_progress_row() {
    let storage = engine().await;
    let cancel_table = create_table(&storage, ACCOUNT_A, "cancel_me").await;
    let op_cancel = start_op(
        &storage,
        ACCOUNT_A,
        "cancel_me",
        &cancel_table,
        1000,
        Vec::new(),
    )
    .await;
    let live_table = create_table(&storage, ACCOUNT_A, "still_running").await;
    let op_live = start_op(
        &storage,
        ACCOUNT_A,
        "still_running",
        &live_table,
        2000,
        Vec::new(),
    )
    .await;

    // Move the first to CANCELLING, the way a customer's
    // CancelBulkOperation does.
    let outcome = storage
        .request_cancel(ACCOUNT_A, &op_cancel)
        .await
        .expect("request cancel");
    assert!(
        matches!(outcome, CancelOutcome::Cancelling),
        "the cancel must put the row in CANCELLING"
    );

    let tick = Duration::from_millis(100);
    let token = CancellationToken::new();
    let handle = tokio::spawn(bulk_operation_worker_with_interval(
        Arc::clone(&storage),
        Arc::new(LimitsConfig::default()),
        Arc::from(REGION),
        None, // no [bulk] store configured
        None,
        Arc::new(MetricsCollector::new()),
        token.clone(),
        tick,
    ));

    let cancelled = wait_until(Duration::from_secs(3), || {
        let storage = Arc::clone(&storage);
        let op = op_cancel.clone();
        async move {
            storage
                .get_bulk_operation(ACCOUNT_A, &op)
                .await
                .expect("get")
                .is_some_and(|r| r.status == "CANCELLED")
        }
    })
    .await;
    assert!(
        cancelled,
        "the CANCELLING operation must reach CANCELLED with no store configured"
    );

    let cancelled_record = storage
        .get_bulk_operation(ACCOUNT_A, &op_cancel)
        .await
        .expect("get")
        .expect("record");
    assert_eq!(cancelled_record.status, "CANCELLED");
    assert!(
        cancelled_record.end_time_ms.is_some(),
        "the cancelled record carries an end time"
    );
    assert_eq!(
        cancelled_record.failure_code.as_deref(),
        Some(extenddb_storage::CANCELLED_FAILURE_CODE),
        "a cancelled record carries the measured code"
    );
    assert_eq!(
        cancelled_record.failure_message.as_deref(),
        Some(extenddb_storage::CANCELLED_FAILURE_MESSAGE)
    );

    let live = storage
        .get_bulk_operation(ACCOUNT_A, &op_live)
        .await
        .expect("get")
        .expect("record");
    assert_eq!(
        live.status, "IN_PROGRESS",
        "an operation with no cancel stays non-terminal with no store"
    );

    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("worker stops at its next tick")
        .expect("worker panicked");
}
