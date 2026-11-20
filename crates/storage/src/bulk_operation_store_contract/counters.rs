// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The non-terminal operation counts, the account's limit in each tier, and
//! the corrupt-counter refusal.

use super::*;

/// The account's limit applies to each `PerformanceTier` on its own
/// (measured on the service: five `STANDARD` operations refuse a sixth and
/// admit an `ACCELERATED` one). A row with no tier counts as `STANDARD`, and
/// a `CANCELLING` operation counts until it ends. Each step starts on a
/// table of its own under a limit of two, so only the limit can refuse it.
pub async fn the_account_limit_counts_each_tier_separately<E: StorageEngine>(fx: &Fixture<E>) {
    let steps = [
        (None, "Created"),
        (Some("STANDARD"), "Created"),
        (Some("STANDARD"), "AccountLimitReached"),
        (Some("ACCELERATED"), "Created"),
        (Some("ACCELERATED"), "Created"),
        (Some("ACCELERATED"), "AccountLimitReached"),
        // The first operation is cancelled before this step.
        (Some("STANDARD"), "AccountLimitReached"),
        // The first operation has ended before this step.
        (None, "Created"),
    ];
    let mut first = None;
    for (step, ((tier, expected), time)) in steps.into_iter().zip(1000_i64..).enumerate() {
        if step == 6 {
            let first: &String = first.as_ref().expect("the first step creates");
            fx.engine
                .request_cancel(&fx.account_a, first)
                .await
                .expect("request_cancel");
        }
        if step == 7 {
            let first: &String = first.as_ref().expect("the first step creates");
            fx.engine
                .claim_runnable("tiers", Duration::from_secs(60), 10, &[], &[])
                .await
                .expect("claim");
            let state = fx
                .engine
                .finish(&fx.account_a, first, "tiers", terminal("CANCELLED", 5000))
                .await
                .expect("finish");
            assert!(matches!(state, FinishState::Held(_)));
        }
        let table = create_table(fx, &fx.account_a, &format!("tier_{time}")).await;
        let mut op = new_op(fx, &fx.account_a, &table, None, "fp", time);
        op.performance_tier = tier.map(str::to_owned);
        let outcome = start(fx, &fx.account_a, op, 2).await;
        assert_eq!(start_kind(&outcome), expected, "step {step}, tier {tier:?}");
        if let StartOutcome::Created(record) = outcome {
            first.get_or_insert(record.operation_id);
        }
    }
}

/// `count_non_terminal` counts the account's `IN_PROGRESS` and CANCELLING
/// rows of both tiers only: a terminal row leaves the count and another
/// account's rows never enter it.
pub async fn count_non_terminal_counts_only_the_accounts_running_and_cancelling_rows<
    E: StorageEngine,
>(
    fx: &Fixture<E>,
) {
    let count = |account: &str| {
        let account = account.to_owned();
        async move {
            fx.engine
                .count_non_terminal(&account)
                .await
                .expect("count_non_terminal")
        }
    };
    assert_eq!(count(&fx.account_a).await, 0, "a fresh account holds none");

    let table_one = create_table(fx, &fx.account_a, "count_one").await;
    let table_two = create_table(fx, &fx.account_a, "count_two").await;
    let first = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table_one, None, "fp", 1000),
            50,
        )
        .await,
    );
    let mut accelerated = new_op(fx, &fx.account_a, &table_two, None, "fp", 1001);
    accelerated.performance_tier = Some("ACCELERATED".to_owned());
    let second = created(start(fx, &fx.account_a, accelerated, 50).await);
    assert_eq!(
        count(&fx.account_a).await,
        2,
        "a STANDARD and an ACCELERATED operation"
    );

    // CANCELLING is non-terminal, so a cancel request changes nothing.
    fx.engine
        .request_cancel(&fx.account_a, &first.operation_id)
        .await
        .expect("request_cancel");
    assert_eq!(count(&fx.account_a).await, 2, "cancelling still counts");

    // A terminal row leaves the count.
    let owner = "counter";
    let claimed = fx
        .engine
        .claim_runnable(owner, Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert!(
        claimed
            .iter()
            .any(|r| r.operation_id == second.operation_id),
        "the second operation must be claimable"
    );
    fx.engine
        .finish(
            &fx.account_a,
            &second.operation_id,
            owner,
            terminal("COMPLETED", 2000),
        )
        .await
        .expect("finish");
    assert_eq!(count(&fx.account_a).await, 1, "terminal rows do not count");

    // Another account's operation never enters the count.
    let foreign_table = create_table(fx, &fx.account_b, "count_foreign").await;
    created(
        start(
            fx,
            &fx.account_b,
            new_op(fx, &fx.account_b, &foreign_table, None, "fp", 1002),
            50,
        )
        .await,
    );
    assert_eq!(count(&fx.account_a).await, 1, "accounts count separately");
    assert_eq!(count(&fx.account_b).await, 1);
}

/// A negative stored counter refuses the read with `StorageError::Internal`
/// naming the column, the same policy the two report totals already carry.
/// The checkpoint plants each counter directly, one field at a time, so
/// every refusal discriminates its own column; a negative enumerated total
/// is planted through the start request and refused by the same reader.
pub async fn a_negative_stored_counter_refuses_the_read<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "corrupt_counters").await;
    let owner = "corruptor";
    let id = started_and_claimed(fx, &table, owner, 1000).await;

    let planted = |field: &'static str| {
        let mut progress = Progress {
            cursor_file_index: 0,
            cursor_row_index: 0,
            processed_item_count: 0,
            failed_item_count: 0,
            processed_input_bytes: 0,
            report_record_count: 0,
            report_bytes: 0,
            usage: json!({}),
            records: Vec::new(),
            finished_files: Vec::new(),
            abandoned_files: Vec::new(),
        };
        match field {
            "processed_item_count" => progress.processed_item_count = -1,
            "failed_item_count" => progress.failed_item_count = -1,
            "processed_input_bytes" => progress.processed_input_bytes = -1,
            "cursor_file_index" => progress.cursor_file_index = -1,
            "cursor_row_index" => progress.cursor_row_index = -1,
            other => panic!("unknown field {other}"),
        }
        progress
    };
    for field in [
        "processed_item_count",
        "failed_item_count",
        "processed_input_bytes",
        "cursor_file_index",
        "cursor_row_index",
    ] {
        fx.engine
            .checkpoint(
                &fx.account_a,
                &id,
                owner,
                Duration::from_secs(60),
                planted(field),
            )
            .await
            .expect("the checkpoint itself writes whatever it is handed");
        let err = fx
            .engine
            .get_bulk_operation(&fx.account_a, &id)
            .await
            .expect_err("a negative stored counter must refuse the read");
        match err {
            StorageError::Internal(message) => assert!(
                message.contains(field),
                "the refusal must name {field}, got: {message}"
            ),
            other => panic!("expected StorageError::Internal, got {other:?}"),
        }
    }

    // `total_input_bytes` is written at insert, so the plant goes through
    // the start request and the created-row read refuses it in the same
    // transaction.
    let clean_table = create_table(fx, &fx.account_a, "corrupt_total").await;
    let mut op = new_op(fx, &fx.account_a, &clean_table, None, "fp", 1001);
    op.total_input_bytes = Some(-1);
    let err = match fx.engine.start_bulk_operation(&fx.account_a, op, 50).await {
        Err(err) => err,
        Ok(outcome) => panic!(
            "a negative enumerated total must refuse the created-row read, got {}",
            start_kind(&outcome)
        ),
    };
    match err {
        StorageError::Internal(message) => assert!(
            message.contains("total_input_bytes"),
            "the refusal must name total_input_bytes, got: {message}"
        ),
        other => panic!("expected StorageError::Internal, got {other:?}"),
    }
}
