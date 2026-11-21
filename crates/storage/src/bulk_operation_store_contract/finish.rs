// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Finish and cancel: terminal states, the cancel transitions, and the stored
//! failure fields.

use super::*;

/// A finish from a non-owner reports Lost and changes nothing.
pub async fn a_finish_from_a_non_owner_is_lost_and_changes_nothing<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "finish_lost").await;
    let id = started_and_claimed(fx, &table, "owner-1", 1000).await;

    let state = fx
        .engine
        .finish(&fx.account_a, &id, "impostor", terminal("COMPLETED", 9000))
        .await
        .expect("finish");
    assert!(matches!(state, FinishState::Lost));

    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.status, "IN_PROGRESS");
    assert_eq!(row.end_time_ms, None);
    assert_eq!(row.lease_owner.as_deref(), Some("owner-1"));
}

/// A CANCELLING row finishes CANCELLED whatever terminal status the runner
/// computed.
pub async fn finishing_a_cancelling_operation_lands_cancelled_whatever_the_outcome_says<
    E: StorageEngine,
>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "finish_cancelling").await;
    let owner = "owner-1";
    let id = started_and_claimed(fx, &table, owner, 1000).await;
    let cancel = fx
        .engine
        .request_cancel(&fx.account_a, &id)
        .await
        .expect("request_cancel");
    assert!(matches!(cancel, CancelOutcome::Cancelling));
    // A checkpoint under the cancel: its byte figure is what the terminal
    // description reports.
    let state = fx
        .engine
        .checkpoint(
            &fx.account_a,
            &id,
            owner,
            Duration::from_secs(60),
            progress(Vec::new()),
        )
        .await
        .expect("checkpoint");
    assert!(matches!(state, LeaseState::Held));

    let state = fx
        .engine
        .finish(&fx.account_a, &id, owner, terminal("COMPLETED", 9000))
        .await
        .expect("finish");
    match state {
        FinishState::Held(status) => assert_eq!(
            status, "CANCELLED",
            "finish answers with the status it stored, not the one it was sent"
        ),
        FinishState::Lost => panic!("the owner held the lease"),
    }

    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.status, "CANCELLED", "CANCELLING always lands CANCELLED");
    assert_eq!(row.end_time_ms, Some(9000));
    // The store supplies the cancelled pair whatever the runner sent and
    // keeps the checkpointed input bytes: a cancelled operation reports
    // what was read before the stop (measured on the service).
    assert_eq!(row.failure_code.as_deref(), Some(CANCELLED_FAILURE_CODE));
    assert_eq!(
        row.failure_message.as_deref(),
        Some(CANCELLED_FAILURE_MESSAGE)
    );
    assert_eq!(row.processed_input_bytes, 2500);
}

/// Finishing clears the lease, stores the terminal fields, and deletes the
/// operation's report records.
pub async fn finish_clears_the_lease_and_deletes_report_records<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "finish_cleanup").await;
    let owner = "owner-1";
    let id = started_and_claimed(fx, &table, owner, 1000).await;
    fx.engine
        .checkpoint(
            &fx.account_a,
            &id,
            owner,
            Duration::from_secs(60),
            progress(vec![error_record(3)]),
        )
        .await
        .expect("checkpoint");

    let outcome = Terminal {
        status: "COMPLETED_WITH_ERRORS".to_owned(),
        end_time_ms: 9000,
        failure_code: None,
        failure_message: None,
        usage: json!({"WriteRequestUnits": 51200}),
    };
    let state = fx
        .engine
        .finish(&fx.account_a, &id, owner, outcome)
        .await
        .expect("finish");
    assert!(matches!(state, FinishState::Held(status) if status == "COMPLETED_WITH_ERRORS"));

    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.status, "COMPLETED_WITH_ERRORS");
    assert_eq!(row.end_time_ms, Some(9000));
    assert_eq!(row.usage, json!({"WriteRequestUnits": 51200}));
    assert_eq!(row.lease_owner, None, "finish clears the lease");
    assert_eq!(row.lease_until_ms, None);

    for kind in [ReportKind::Error, ReportKind::ConditionCheckFailure] {
        let records = all_report_records(fx, &fx.account_a, &id, kind).await;
        assert!(records.is_empty(), "finish deletes the report records");
    }

    // A second finish is refused: the row is terminal and the lease is gone.
    let again = fx
        .engine
        .finish(&fx.account_a, &id, owner, terminal("FAILED", 9500))
        .await
        .expect("finish");
    assert!(matches!(again, FinishState::Lost));
    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(
        row.status, "COMPLETED_WITH_ERRORS",
        "a terminal row does not move"
    );
}

/// Cancelling a running operation moves it to CANCELLING; a second cancel is
/// the same answer, not an error.
pub async fn cancel_moves_a_running_operation_to_cancelling_and_repeats_harmlessly<
    E: StorageEngine,
>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "cancel_running").await;
    let record = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, None, "fp", 1000),
            50,
        )
        .await,
    );
    let id = record.operation_id;

    for _ in 0..2 {
        let cancel = fx
            .engine
            .request_cancel(&fx.account_a, &id)
            .await
            .expect("request_cancel");
        assert!(matches!(cancel, CancelOutcome::Cancelling));
    }
    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.status, "CANCELLING");
    assert!(row.cancel_requested);
}

/// Cancelling a terminal operation answers with the row's status and moves
/// nothing.
pub async fn cancel_on_a_terminal_operation_reports_the_status_and_changes_nothing<
    E: StorageEngine,
>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "cancel_terminal").await;
    let owner = "owner-1";
    let id = started_and_claimed(fx, &table, owner, 1000).await;
    fx.engine
        .finish(&fx.account_a, &id, owner, terminal("COMPLETED", 5000))
        .await
        .expect("finish");

    let cancel = fx
        .engine
        .request_cancel(&fx.account_a, &id)
        .await
        .expect("request_cancel");
    match cancel {
        CancelOutcome::AlreadyTerminal(status) => assert_eq!(status, "COMPLETED"),
        CancelOutcome::Cancelling => panic!("a terminal row must not move to CANCELLING"),
        CancelOutcome::NotFound => panic!("the row exists"),
    }
    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.status, "COMPLETED");
    assert!(
        !row.cancel_requested,
        "the refused cancel must not set the flag"
    );
}

/// Cancelling an id that does not exist reports `NotFound`.
pub async fn cancel_on_an_unknown_operation_reports_not_found<E: StorageEngine>(fx: &Fixture<E>) {
    let cancel = fx
        .engine
        .request_cancel(&fx.account_a, "00000000000000deadbeef")
        .await
        .expect("request_cancel");
    assert!(matches!(cancel, CancelOutcome::NotFound));
}

/// A FAILED finish carries its failure code and message through to the
/// stored record.
pub async fn finish_with_failed_carries_the_failure_code_and_message<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "finish_failed").await;
    let owner = "owner-1";
    let id = started_and_claimed(fx, &table, owner, 1000).await;

    let outcome = Terminal {
        status: "FAILED".to_owned(),
        end_time_ms: 9000,
        failure_code: Some("ResourceNotFoundException".to_owned()),
        failure_message: Some("Requested resource not found".to_owned()),
        usage: json!({}),
    };
    let state = fx
        .engine
        .finish(&fx.account_a, &id, owner, outcome)
        .await
        .expect("finish");
    assert!(matches!(state, FinishState::Held(status) if status == "FAILED"));

    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.status, "FAILED");
    assert_eq!(
        row.failure_code.as_deref(),
        Some("ResourceNotFoundException")
    );
    assert_eq!(
        row.failure_message.as_deref(),
        Some("Requested resource not found")
    );
    assert_eq!(row.end_time_ms, Some(9000));
}
