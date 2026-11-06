// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The start transaction: the table guard, client-token idempotency, concurrent
//! starts, and the account limit.

use super::*;

/// A second start on a table with a non-terminal operation is refused
/// synchronously, on the guard, not on the account limit.
pub async fn second_start_on_a_busy_table_is_refused<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "busy_guard").await;
    created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, None, "fp", 1000),
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
        matches!(second, StartOutcome::TableBusy),
        "expected TableBusy, got {}",
        start_kind(&second)
    );
}

/// When both the table guard and the account limit would reject, the guard
/// answers: a second start on a busy table under an account limit of one is
/// `TableBusy`, not `AccountLimitReached`.
pub async fn the_table_guard_answers_before_the_account_limit<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "guard_before_limit").await;
    created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, None, "fp", 1000),
            1,
        )
        .await,
    );
    let second = start(
        fx,
        &fx.account_a,
        new_op(fx, &fx.account_a, &table, None, "fp", 2000),
        1,
    )
    .await;
    assert!(
        matches!(second, StartOutcome::TableBusy),
        "expected TableBusy when the guard and the limit both reject, got {}",
        start_kind(&second)
    );
}

/// Twenty starts released by one barrier on one table, each with its own token:
/// exactly one is admitted and the other nineteen see the table guard.
pub async fn twenty_concurrent_starts_with_distinct_tokens_admit_exactly_one<
    E: StorageEngine + 'static,
>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "race_distinct").await;
    let barrier = Arc::new(tokio::sync::Barrier::new(20));
    let mut tasks = Vec::new();
    for i in 0..20 {
        let engine = Arc::clone(&fx.engine);
        let account = fx.account_a.clone();
        let req = new_op(
            fx,
            &fx.account_a,
            &table,
            Some(&format!("token-{i}")),
            "fp",
            1000 + i,
        );
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            engine.start_bulk_operation(&account, req, 50).await
        }));
    }
    let mut counts: HashMap<&'static str, usize> = HashMap::new();
    for task in tasks {
        let outcome = task.await.expect("join").expect("start");
        *counts.entry(start_kind(&outcome)).or_default() += 1;
    }
    assert_eq!(counts.get("Created"), Some(&1), "counts: {counts:?}");
    assert_eq!(counts.get("TableBusy"), Some(&19), "counts: {counts:?}");
}

/// Twenty starts released by one barrier with one shared token and identical
/// parameters: one row is created, the other nineteen replay it, and none of
/// them is ever answered with the table guard.
pub async fn twenty_concurrent_starts_with_one_token_create_once_and_replay_the_rest<
    E: StorageEngine + 'static,
>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "race_same_token").await;
    let barrier = Arc::new(tokio::sync::Barrier::new(20));
    let mut tasks = Vec::new();
    for i in 0..20 {
        let engine = Arc::clone(&fx.engine);
        let account = fx.account_a.clone();
        // Each request carries its own operation id, as each HTTP request
        // would; the winner's id is the one every replay must come back with.
        let req = new_op(
            fx,
            &fx.account_a,
            &table,
            Some("shared-token"),
            "fp",
            1000 + i,
        );
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            engine.start_bulk_operation(&account, req, 50).await
        }));
    }
    let mut created_ids = Vec::new();
    let mut replayed_ids = Vec::new();
    for task in tasks {
        match task.await.expect("join").expect("start") {
            StartOutcome::Created(r) => created_ids.push(r.operation_id),
            StartOutcome::IdempotentReplay(r) => replayed_ids.push(r.operation_id),
            other => panic!(
                "a same-token start must never see {}, least of all TableBusy",
                start_kind(&other)
            ),
        }
    }
    assert_eq!(created_ids.len(), 1);
    assert_eq!(replayed_ids.len(), 19);
    assert!(
        replayed_ids.iter().all(|id| id == &created_ids[0]),
        "every replay must return the winner's record"
    );
}

/// Once the running operation reaches a terminal status, the table guard
/// releases and a new start on the same table is admitted.
pub async fn a_table_is_startable_again_after_its_operation_finishes<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "guard_release").await;
    let first = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, None, "fp", 1000),
            50,
        )
        .await,
    );
    let claimed = fx
        .engine
        .claim_runnable("owner-1", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert!(claimed.iter().any(|r| r.operation_id == first.operation_id));
    let state = fx
        .engine
        .finish(
            &fx.account_a,
            &first.operation_id,
            "owner-1",
            terminal("COMPLETED", 5000),
        )
        .await
        .expect("finish");
    assert!(matches!(state, FinishState::Held(_)));
    let second = start(
        fx,
        &fx.account_a,
        new_op(fx, &fx.account_a, &table, None, "fp", 6000),
        50,
    )
    .await;
    created(second);
}

/// The per-table non-terminal count follows the operation's lifecycle and
/// is keyed by table id within the account: one while the operation runs
/// or is cancelling, zero for another table, another account, and after
/// the finish.
pub async fn the_per_table_non_terminal_count_follows_the_operation<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "guard_count").await;
    let other = create_table(fx, &fx.account_a, "guard_count_other").await;
    let count = |account: &str, table_id: &str| {
        let account = account.to_owned();
        let table_id = table_id.to_owned();
        async move {
            fx.engine
                .count_non_terminal_for_table(&account, &table_id)
                .await
                .expect("count")
        }
    };
    assert_eq!(count(&fx.account_a, &table.table_id).await, 0);
    let first = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, None, "fp", 1000),
            50,
        )
        .await,
    );
    assert_eq!(count(&fx.account_a, &table.table_id).await, 1);
    assert_eq!(count(&fx.account_a, &other.table_id).await, 0);
    assert_eq!(count(&fx.account_b, &table.table_id).await, 0);
    let claimed = fx
        .engine
        .claim_runnable("owner-1", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert!(claimed.iter().any(|r| r.operation_id == first.operation_id));
    fx.engine
        .finish(
            &fx.account_a,
            &first.operation_id,
            "owner-1",
            terminal("COMPLETED", 5000),
        )
        .await
        .expect("finish");
    assert_eq!(count(&fx.account_a, &table.table_id).await, 0);
}

/// A reused token with a different fingerprint is refused as a mismatch and
/// no new row appears, even on a table with no running operation.
pub async fn a_reused_token_with_different_parameters_is_refused_and_creates_nothing<
    E: StorageEngine,
>(
    fx: &Fixture<E>,
) {
    let table_one = create_table(fx, &fx.account_a, "mismatch_one").await;
    let table_two = create_table(fx, &fx.account_a, "mismatch_two").await;
    created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table_one, Some("tok"), "fp-a", 1000),
            50,
        )
        .await,
    );
    // A different table, so the only thing that can refuse this is the token.
    let second = start(
        fx,
        &fx.account_a,
        new_op(fx, &fx.account_a, &table_two, Some("tok"), "fp-b", 2000),
        50,
    )
    .await;
    assert!(
        matches!(second, StartOutcome::IdempotentMismatch),
        "expected IdempotentMismatch, got {}",
        start_kind(&second)
    );
    let (all, _) = fx
        .engine
        .list_bulk_operations(&fx.account_a, 100, None)
        .await
        .expect("list");
    assert_eq!(all.len(), 1, "the mismatch must not have created a row");
}

/// A replayed token answers with the existing record whatever its status,
/// before the table guard is consulted: the replay succeeds even while a
/// different operation runs on the same table.
pub async fn a_reused_token_replays_even_when_terminal_and_skips_the_table_guard<
    E: StorageEngine,
>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "replay_terminal").await;
    let first = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, Some("tok"), "fp", 1000),
            50,
        )
        .await,
    );
    fx.engine
        .claim_runnable("owner-1", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    fx.engine
        .finish(
            &fx.account_a,
            &first.operation_id,
            "owner-1",
            terminal("COMPLETED", 2000),
        )
        .await
        .expect("finish");
    // A second, unrelated operation now occupies the table.
    created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, Some("tok-2"), "fp-2", 3000),
            50,
        )
        .await,
    );
    let replay = start(
        fx,
        &fx.account_a,
        new_op(fx, &fx.account_a, &table, Some("tok"), "fp", 4000),
        50,
    )
    .await;
    match replay {
        StartOutcome::IdempotentReplay(r) => {
            assert_eq!(r.operation_id, first.operation_id);
            assert_eq!(
                r.status, "COMPLETED",
                "the replay carries the current status"
            );
        }
        other => panic!("expected IdempotentReplay, got {}", start_kind(&other)),
    }
}

/// A token replays its operation for [`CLIENT_TOKEN_WINDOW_MS`] from the
/// operation's start, then reads as unseen: the same request re-sent at or
/// past the window creates a new operation (measured on the service: a
/// replay within seconds, a new operation at 25 hours;
/// the window itself is the eight hours the service documents). The aged
/// record keeps its token for Describe and no longer occupies the token
/// index, so the new row inserts without a conflict.
pub async fn a_token_replays_inside_the_window_and_starts_anew_past_it<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "token_window").await;
    let t0 = 1_000_000;
    let first = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, Some("tok-w"), "fp", t0),
            50,
        )
        .await,
    );
    fx.engine
        .claim_runnable("owner-1", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    fx.engine
        .finish(
            &fx.account_a,
            &first.operation_id,
            "owner-1",
            terminal("COMPLETED", t0 + 1),
        )
        .await
        .expect("finish");

    // One millisecond inside the window: a replay.
    let inside = start(
        fx,
        &fx.account_a,
        new_op(
            fx,
            &fx.account_a,
            &table,
            Some("tok-w"),
            "fp",
            t0 + CLIENT_TOKEN_WINDOW_MS - 1,
        ),
        50,
    )
    .await;
    match inside {
        StartOutcome::IdempotentReplay(r) => assert_eq!(r.operation_id, first.operation_id),
        other => panic!(
            "expected IdempotentReplay inside the window, got {}",
            start_kind(&other)
        ),
    }
    // A different fingerprint inside the window is still a mismatch.
    let mismatch = start(
        fx,
        &fx.account_a,
        new_op(
            fx,
            &fx.account_a,
            &table,
            Some("tok-w"),
            "fp-other",
            t0 + CLIENT_TOKEN_WINDOW_MS - 1,
        ),
        50,
    )
    .await;
    assert!(
        matches!(mismatch, StartOutcome::IdempotentMismatch),
        "expected IdempotentMismatch inside the window, got {}",
        start_kind(&mismatch)
    );

    // At the window: a new operation, with the same or a different
    // fingerprint.
    let second = created(
        start(
            fx,
            &fx.account_a,
            new_op(
                fx,
                &fx.account_a,
                &table,
                Some("tok-w"),
                "fp",
                t0 + CLIENT_TOKEN_WINDOW_MS,
            ),
            50,
        )
        .await,
    );
    assert_ne!(second.operation_id, first.operation_id);
    assert_eq!(second.client_token.as_deref(), Some("tok-w"));
    let aged = fx
        .engine
        .get_bulk_operation(&fx.account_a, &first.operation_id)
        .await
        .expect("get")
        .expect("the aged record still exists");
    assert_eq!(
        aged.client_token.as_deref(),
        Some("tok-w"),
        "the aged record keeps its token for Describe"
    );
    // The token now replays the new operation.
    let replay = start(
        fx,
        &fx.account_a,
        new_op(
            fx,
            &fx.account_a,
            &table,
            Some("tok-w"),
            "fp",
            t0 + CLIENT_TOKEN_WINDOW_MS + 5,
        ),
        50,
    )
    .await;
    match replay {
        StartOutcome::IdempotentReplay(r) => assert_eq!(r.operation_id, second.operation_id),
        other => panic!(
            "expected IdempotentReplay of the new operation, got {}",
            start_kind(&other)
        ),
    }
}

/// Two starts without a token never collide on the token index: a NULL hash
/// is not a duplicate of another NULL hash on either backend.
pub async fn tokenless_starts_do_not_collide_on_the_token_index<E: StorageEngine>(fx: &Fixture<E>) {
    let table_one = create_table(fx, &fx.account_a, "tokenless_one").await;
    let table_two = create_table(fx, &fx.account_a, "tokenless_two").await;
    created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table_one, None, "fp", 1000),
            50,
        )
        .await,
    );
    created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table_two, None, "fp", 2000),
            50,
        )
        .await,
    );
}

/// Sixty starts released by one barrier on sixty distinct tables against an account
/// limit of fifty: exactly fifty are admitted and ten refused on the limit.
pub async fn sixty_concurrent_starts_against_a_limit_of_fifty_admit_exactly_fifty<
    E: StorageEngine + 'static,
>(
    fx: &Fixture<E>,
) {
    let mut tables = Vec::new();
    for i in 0..60 {
        tables.push(create_table(fx, &fx.account_a, &format!("limit_{i}")).await);
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(60));
    let mut tasks = Vec::new();
    for (i, table) in tables.iter().enumerate() {
        let engine = Arc::clone(&fx.engine);
        let account = fx.account_a.clone();
        let req = new_op(
            fx,
            &fx.account_a,
            table,
            None,
            "fp",
            1000 + i64::try_from(i).expect("task index fits in i64"),
        );
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            engine.start_bulk_operation(&account, req, 50).await
        }));
    }
    let mut counts: HashMap<&'static str, usize> = HashMap::new();
    for task in tasks {
        let outcome = task.await.expect("join").expect("start");
        *counts.entry(start_kind(&outcome)).or_default() += 1;
    }
    assert_eq!(counts.get("Created"), Some(&50), "counts: {counts:?}");
    assert_eq!(
        counts.get("AccountLimitReached"),
        Some(&10),
        "counts: {counts:?}"
    );
}

/// The token index is account-scoped: the same token hash in two accounts
/// creates an operation in each, with no replay, mismatch, or guard answer
/// across the boundary.
pub async fn a_token_hash_shared_across_accounts_creates_in_each_account<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let table_a = create_table(fx, &fx.account_a, "token_scope_a").await;
    let table_b = create_table(fx, &fx.account_b, "token_scope_b").await;

    created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table_a, Some("shared"), "fp-a", 1000),
            50,
        )
        .await,
    );
    let in_b = start(
        fx,
        &fx.account_b,
        new_op(fx, &fx.account_b, &table_b, Some("shared"), "fp-b", 2000),
        50,
    )
    .await;
    created(in_b);
}
