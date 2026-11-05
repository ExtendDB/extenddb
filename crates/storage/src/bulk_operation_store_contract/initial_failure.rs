// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Rows born terminal from an initial failure, and the terminal-born cap the
//! start transaction enforces.

use super::*;

/// Turn a new-operation request into one carrying an initial failure, the
/// shape the engine builds for a named bucket directory that does not
/// exist: no input files, no enumerated total, the S3 code and message.
fn with_initial_failure(mut req: NewBulkOperation) -> NewBulkOperation {
    req.input_files = Vec::new();
    req.total_input_bytes = None;
    req.initial_failure = Some(crate::InitialFailure {
        code: "S3NoSuchBucket".to_owned(),
        message: "The specified bucket does not exist".to_owned(),
        // A high cap so existing cases that create a handful of
        // terminal-born rows are never refused by the cap; the cap itself
        // is exercised by its own case.
        cap: u64::MAX,
        window_start_ms: 0,
    });
    req
}

/// Like `with_initial_failure` but with an explicit terminal-born cap and
/// window, for the case that exercises the cap itself.
fn with_initial_failure_capped(
    mut req: NewBulkOperation,
    cap: u64,
    window_start_ms: i64,
) -> NewBulkOperation {
    req.input_files = Vec::new();
    req.total_input_bytes = None;
    req.initial_failure = Some(crate::InitialFailure {
        code: "S3NoSuchBucket".to_owned(),
        message: "The specified bucket does not exist".to_owned(),
        cap,
        window_start_ms,
    });
    req
}

/// The terminal-born cap is enforced inside the start transaction, under
/// the per-account lock, so a run of absent-bucket starts is refused once
/// the account is at the cap: it is exact, not a best-effort pre-check a
/// concurrent burst can race past. With a cap of 3, the first three starts
/// are `Created` and the fourth is `FailedStartLimit`.
pub async fn the_terminal_born_cap_is_enforced_in_the_start_transaction<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "cap_table").await;
    let cap = 3u64;
    for i in 0..cap {
        let req = with_initial_failure_capped(
            new_op(
                fx,
                &fx.account_a,
                &table,
                None,
                &format!("fp-{i}"),
                1_000 + i as i64,
            ),
            cap,
            0,
        );
        let outcome = start(fx, &fx.account_a, req, 50).await;
        assert!(
            matches!(outcome, StartOutcome::Created(_)),
            "terminal-born start {i} is below the cap, got {}",
            start_kind(&outcome)
        );
    }
    let over = with_initial_failure_capped(
        new_op(fx, &fx.account_a, &table, None, "fp-over", 2_000),
        cap,
        0,
    );
    let outcome = start(fx, &fx.account_a, over, 50).await;
    assert!(
        matches!(outcome, StartOutcome::FailedStartLimit),
        "the start at the cap is refused in the store, got {}",
        start_kind(&outcome)
    );

    // A window that excludes the earlier rows admits a new terminal-born
    // start again: the cap counts only rows inside the trailing window.
    let fresh = with_initial_failure_capped(
        new_op(fx, &fx.account_a, &table, None, "fp-fresh", 3_000),
        cap,
        2_500,
    );
    let outcome = start(fx, &fx.account_a, fresh, 50).await;
    assert!(
        matches!(outcome, StartOutcome::Created(_)),
        "a start whose window excludes the earlier rows is admitted, got {}",
        start_kind(&outcome)
    );
}

/// A start carrying an initial failure inserts the row already terminal:
/// FAILED with the code and message, end time equal to start time, zero
/// counters, no lease, and the claim loop never returns it.
pub async fn an_initial_failure_starts_terminal_and_is_never_claimed<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "initial_failure_terminal").await;
    let record = created(
        start(
            fx,
            &fx.account_a,
            with_initial_failure(new_op(fx, &fx.account_a, &table, None, "fp", 1000)),
            50,
        )
        .await,
    );
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchBucket"));
    assert_eq!(
        record.failure_message.as_deref(),
        Some("The specified bucket does not exist")
    );
    assert_eq!(record.end_time_ms, Some(record.start_time_ms));
    assert_eq!(record.processed_item_count, 0);
    assert_eq!(record.failed_item_count, 0);
    assert_eq!(record.processed_input_bytes, 0);
    // Never enumerated, so no total: Describe omits TotalInputBytes, as the
    // hosted service does for a failure born before the input was read.
    assert_eq!(record.total_input_bytes, None);
    assert!(record.lease_owner.is_none());
    assert!(record.lease_until_ms.is_none());
    let claimed = fx
        .engine
        .claim_runnable("claimer", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert!(
        claimed.is_empty(),
        "a row born terminal must not be claimable, got {} rows",
        claimed.len()
    );
}

/// A row born terminal never counts against the account's non-terminal
/// operations.
pub async fn an_initial_failure_does_not_count_as_non_terminal<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "initial_failure_count").await;
    created(
        start(
            fx,
            &fx.account_a,
            with_initial_failure(new_op(fx, &fx.account_a, &table, None, "fp", 1000)),
            50,
        )
        .await,
    );
    let count = fx
        .engine
        .count_non_terminal(&fx.account_a)
        .await
        .expect("count");
    assert_eq!(count, 0, "a FAILED row is terminal and must not be counted");
}

/// The per-table guard sees only non-terminal rows, so a table whose only
/// operation was born terminal accepts a new start.
pub async fn a_table_with_an_initial_failure_row_accepts_a_new_start<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "initial_failure_guard").await;
    created(
        start(
            fx,
            &fx.account_a,
            with_initial_failure(new_op(fx, &fx.account_a, &table, None, "fp", 1000)),
            50,
        )
        .await,
    );
    let second = start(
        fx,
        &fx.account_a,
        new_op(fx, &fx.account_a, &table, None, "fp", 2000),
        50,
    )
    .await;
    assert!(
        matches!(second, StartOutcome::Created(_)),
        "a terminal row must not trip the table guard, got {}",
        start_kind(&second)
    );
}

/// The token of a row born terminal is consumed like any other: a second
/// start with the same token and fingerprint replays the FAILED record,
/// whether or not the retry carries an initial failure of its own.
pub async fn a_reused_token_replays_to_an_initial_failure_row<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "initial_failure_replay").await;
    let first = created(
        start(
            fx,
            &fx.account_a,
            with_initial_failure(new_op(
                fx,
                &fx.account_a,
                &table,
                Some("tok-failed"),
                "fp",
                1000,
            )),
            50,
        )
        .await,
    );
    let second = start(
        fx,
        &fx.account_a,
        new_op(fx, &fx.account_a, &table, Some("tok-failed"), "fp", 2000),
        50,
    )
    .await;
    match second {
        StartOutcome::IdempotentReplay(record) => {
            assert_eq!(record.operation_id, first.operation_id);
            assert_eq!(record.status, "FAILED");
            assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchBucket"));
        }
        other => panic!("expected IdempotentReplay, got {}", start_kind(&other)),
    }
}

/// `count_terminal_born_since` counts rows that are `FAILED` with
/// `end_time_ms` equal to `start_time_ms` and a `start_time_ms` at or
/// after the cutoff, and only those: a terminal-born row older than the
/// cutoff, a running row, a row failed by the runner (whose end time
/// differs from its start), and another account's terminal-born row all
/// stay out of the count.
pub async fn count_terminal_born_since_counts_only_terminal_born_rows_in_the_window<
    E: StorageEngine,
>(
    fx: &Fixture<E>,
) {
    let count = |account: &str, since_ms: i64| {
        let account = account.to_owned();
        async move {
            fx.engine
                .count_terminal_born_since(&account, since_ms)
                .await
                .expect("count_terminal_born_since")
        }
    };
    assert_eq!(
        count(&fx.account_a, 0).await,
        0,
        "a fresh account holds none"
    );

    // One terminal-born row inside the window, one before it.
    let table_in = create_table(fx, &fx.account_a, "born_in_window").await;
    created(
        start(
            fx,
            &fx.account_a,
            with_initial_failure(new_op(fx, &fx.account_a, &table_in, None, "fp", 5000)),
            50,
        )
        .await,
    );
    let table_old = create_table(fx, &fx.account_a, "born_before_window").await;
    created(
        start(
            fx,
            &fx.account_a,
            with_initial_failure(new_op(fx, &fx.account_a, &table_old, None, "fp", 1000)),
            50,
        )
        .await,
    );

    // A running row and a row the runner failed (end time differs from
    // start time) are not terminal-born.
    let table_run = create_table(fx, &fx.account_a, "born_running").await;
    created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table_run, None, "fp", 5000),
            50,
        )
        .await,
    );
    let table_failed = create_table(fx, &fx.account_a, "born_runner_failed").await;
    let failed = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table_failed, None, "fp", 5000),
            50,
        )
        .await,
    );
    let owner = "born-counter";
    fx.engine
        .claim_runnable(owner, Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    fx.engine
        .finish(
            &fx.account_a,
            &failed.operation_id,
            owner,
            terminal("FAILED", 9000),
        )
        .await
        .expect("finish");

    // Another account's terminal-born row is invisible to account A.
    let table_b = create_table(fx, &fx.account_b, "born_other_account").await;
    created(
        start(
            fx,
            &fx.account_b,
            with_initial_failure(new_op(fx, &fx.account_b, &table_b, None, "fp", 5000)),
            50,
        )
        .await,
    );

    assert_eq!(
        count(&fx.account_a, 2000).await,
        1,
        "only the terminal-born row at or after the cutoff counts"
    );
    assert_eq!(
        count(&fx.account_a, 0).await,
        2,
        "a cutoff before both terminal-born rows counts both"
    );
    assert_eq!(
        count(&fx.account_a, 5001).await,
        0,
        "a cutoff after every terminal-born row counts none"
    );
}

/// The account limit binds on non-terminal rows only: an account already
/// at its limit refuses a normal start but still accepts a start carrying
/// an initial failure, and the inserted row is terminal.
pub async fn an_initial_failure_is_inserted_despite_a_saturated_account<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let busy = create_table(fx, &fx.account_a, "initial_failure_saturating").await;
    created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &busy, None, "fp-running", 1000),
            1,
        )
        .await,
    );
    let second_table = create_table(fx, &fx.account_a, "initial_failure_saturated").await;
    // The saturation premise: a normal start against the limit of one is
    // refused, so the acceptance below can only come from the skip.
    let refused = start(
        fx,
        &fx.account_a,
        new_op(fx, &fx.account_a, &second_table, None, "fp-refused", 2000),
        1,
    )
    .await;
    assert!(
        matches!(refused, StartOutcome::AccountLimitReached),
        "a normal start must be refused at the limit, got {}",
        start_kind(&refused)
    );
    let record = created(
        start(
            fx,
            &fx.account_a,
            with_initial_failure(new_op(
                fx,
                &fx.account_a,
                &second_table,
                None,
                "fp-terminal",
                3000,
            )),
            1,
        )
        .await,
    );
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchBucket"));
}

/// The per-table guard binds on a start carrying an initial failure too
/// (measured on the hosted service: a Start naming an input bucket that
/// does not exist, on a table with an operation running, answered the busy
/// message and created nothing). On a busy table the start answers
/// `TableBusy` and inserts nothing, before the terminal-born cap even when
/// the cap would also refuse, and the running operation is untouched. Once
/// the operation has finished, the same start is born `FAILED`, and the cap
/// applies again.
pub async fn an_initial_failure_on_a_busy_table_answers_busy_and_inserts_nothing<
    E: StorageEngine,
>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "initial_failure_busy_table").await;
    let running = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, None, "fp-running", 1000),
            50,
        )
        .await,
    );
    let busy = start(
        fx,
        &fx.account_a,
        with_initial_failure(new_op(fx, &fx.account_a, &table, None, "fp-terminal", 2000)),
        50,
    )
    .await;
    assert!(
        matches!(busy, StartOutcome::TableBusy),
        "a start with an initial failure on a busy table answers busy, got {}",
        start_kind(&busy)
    );
    let capped = start(
        fx,
        &fx.account_a,
        with_initial_failure_capped(
            new_op(fx, &fx.account_a, &table, None, "fp-capped", 2500),
            0,
            0,
        ),
        50,
    )
    .await;
    assert!(
        matches!(capped, StartOutcome::TableBusy),
        "the per-table guard answers before the terminal-born cap, got {}",
        start_kind(&capped)
    );
    let (records, _) = fx
        .engine
        .list_bulk_operations(&fx.account_a, 10, None)
        .await
        .expect("list");
    let ids: Vec<&str> = records.iter().map(|r| r.operation_id.as_str()).collect();
    assert_eq!(
        ids,
        vec![running.operation_id.as_str()],
        "the refused starts inserted nothing"
    );
    let untouched = get(fx, &fx.account_a, &running.operation_id)
        .await
        .expect("the running operation exists");
    assert_eq!(
        untouched.status, "IN_PROGRESS",
        "the refused starts must not disturb the running operation"
    );

    let claimed = fx
        .engine
        .claim_runnable("owner-1", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert!(
        claimed
            .iter()
            .any(|r| r.operation_id == running.operation_id)
    );
    let state = fx
        .engine
        .finish(
            &fx.account_a,
            &running.operation_id,
            "owner-1",
            terminal("COMPLETED", 3000),
        )
        .await
        .expect("finish");
    assert!(matches!(state, FinishState::Held(_)));

    let record = created(
        start(
            fx,
            &fx.account_a,
            with_initial_failure(new_op(fx, &fx.account_a, &table, None, "fp-terminal", 4000)),
            50,
        )
        .await,
    );
    assert_eq!(record.status, "FAILED");
    assert_eq!(record.failure_code.as_deref(), Some("S3NoSuchBucket"));
    let capped = start(
        fx,
        &fx.account_a,
        with_initial_failure_capped(
            new_op(fx, &fx.account_a, &table, None, "fp-capped", 5000),
            0,
            0,
        ),
        50,
    )
    .await;
    assert!(
        matches!(capped, StartOutcome::FailedStartLimit),
        "on an idle table the terminal-born cap still refuses, got {}",
        start_kind(&capped)
    );
}
