// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Retention: the purgeable list, purge scoping, and token reuse after a purge.

use super::*;

/// The list names terminal rows ending before the cutoff, and only those:
/// a terminal row ending after the cutoff and a running row are both
/// absent from the list and both survive the purge of the listed pairs.
pub async fn purge_removes_only_terminal_operations_older_than_the_cutoff<E: StorageEngine>(
    fx: &Fixture<E>,
) {
    let owner = "owner-1";

    let old_table = create_table(fx, &fx.account_a, "purge_old").await;
    let old_id = started_and_claimed(fx, &old_table, owner, 1000).await;
    fx.engine
        .finish(&fx.account_a, &old_id, owner, terminal("COMPLETED", 1000))
        .await
        .expect("finish the old row");

    let recent_table = create_table(fx, &fx.account_a, "purge_recent").await;
    let recent_id = started_and_claimed(fx, &recent_table, owner, 2000).await;
    fx.engine
        .finish(
            &fx.account_a,
            &recent_id,
            owner,
            terminal("COMPLETED", 9000),
        )
        .await
        .expect("finish the recent row");

    let running_table = create_table(fx, &fx.account_a, "purge_running").await;
    let running = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &running_table, None, "fp", 500),
            50,
        )
        .await,
    );

    let cutoff = UNIX_EPOCH + Duration::from_millis(5000);
    let purgeable = fx
        .engine
        .list_purgeable_before(cutoff, usize::MAX)
        .await
        .expect("list_purgeable_before");
    assert_eq!(
        purgeable,
        vec![(fx.account_a.clone(), old_id.clone())],
        "exactly the old terminal row is listed, named so the ledger can go first"
    );

    fx.engine
        .purge_operations(&purgeable)
        .await
        .expect("purge_operations");

    assert!(
        get(fx, &fx.account_a, &old_id).await.is_none(),
        "the old row is gone"
    );
    assert!(
        get(fx, &fx.account_a, &recent_id).await.is_some(),
        "a terminal row ending after the cutoff survives"
    );
    assert!(
        get(fx, &fx.account_a, &running.operation_id)
            .await
            .is_some(),
        "a running row survives whatever its age"
    );

    let records = all_report_records(fx, &fx.account_a, &old_id, ReportKind::Error).await;
    assert!(
        records.is_empty(),
        "the purged row leaves no report records behind"
    );
}

/// `list_purgeable_before` returns no more than `limit` rows, so one
/// retention pass is bounded and the worker drains a backlog in separately
/// committed chunks rather than one unbounded transaction. The oldest
/// terminal rows come first, so successive bounded passes make progress.
pub async fn list_purgeable_before_honors_the_limit<E: StorageEngine>(fx: &Fixture<E>) {
    let owner = "owner-1";
    let mut ids = Vec::new();
    for (i, name) in ["lim_a", "lim_b", "lim_c", "lim_d", "lim_e"]
        .iter()
        .enumerate()
    {
        let table = create_table(fx, &fx.account_a, name).await;
        let id = started_and_claimed(fx, &table, owner, 1000 + i as i64).await;
        // Distinct end_time_ms so the oldest-first order is well defined.
        fx.engine
            .finish(
                &fx.account_a,
                &id,
                owner,
                terminal("COMPLETED", 1000 + i as i64),
            )
            .await
            .expect("finish");
        ids.push(id);
    }

    let cutoff = UNIX_EPOCH + Duration::from_millis(5000);

    // A limit below the backlog returns exactly that many rows.
    let page = fx
        .engine
        .list_purgeable_before(cutoff, 2)
        .await
        .expect("list_purgeable_before with a limit");
    assert_eq!(page.len(), 2, "the limit bounds the pass to two rows");

    // The whole backlog is reachable with a generous limit.
    let all = fx
        .engine
        .list_purgeable_before(cutoff, usize::MAX)
        .await
        .expect("list_purgeable_before unbounded");
    assert_eq!(all.len(), 5, "every terminal row is eligible");
}

/// The purge deletes exactly the pairs it is given: a listed pair held
/// back from the call keeps its record until a later purge names it.
pub async fn purge_deletes_only_the_given_pairs<E: StorageEngine>(fx: &Fixture<E>) {
    let owner = "owner-1";
    let mut ids = Vec::new();
    for name in ["purge_pair_a", "purge_pair_b", "purge_pair_c"] {
        let table = create_table(fx, &fx.account_a, name).await;
        let id = started_and_claimed(fx, &table, owner, 1000).await;
        fx.engine
            .finish(&fx.account_a, &id, owner, terminal("COMPLETED", 1000))
            .await
            .expect("finish");
        ids.push(id);
    }

    let cutoff = UNIX_EPOCH + Duration::from_millis(5000);
    let listed: HashSet<(String, String)> = fx
        .engine
        .list_purgeable_before(cutoff, usize::MAX)
        .await
        .expect("list_purgeable_before")
        .into_iter()
        .collect();
    let expected: HashSet<(String, String)> = ids
        .iter()
        .map(|id| (fx.account_a.clone(), id.clone()))
        .collect();
    assert_eq!(listed, expected, "all three terminal rows are listed");

    // Purge two of the three; the held-back pair keeps its record, the
    // way a pair whose ledger delete failed is held back for the next
    // pass.
    let first_two: Vec<(String, String)> = ids[..2]
        .iter()
        .map(|id| (fx.account_a.clone(), id.clone()))
        .collect();
    fx.engine
        .purge_operations(&first_two)
        .await
        .expect("purge_operations");
    assert!(get(fx, &fx.account_a, &ids[0]).await.is_none());
    assert!(get(fx, &fx.account_a, &ids[1]).await.is_none());
    assert!(
        get(fx, &fx.account_a, &ids[2]).await.is_some(),
        "a pair the purge was not given survives"
    );

    // A later purge naming the held-back pair (and one already-purged
    // pair, which is skipped) removes it.
    let rest = vec![
        (fx.account_a.clone(), ids[0].clone()),
        (fx.account_a.clone(), ids[2].clone()),
    ];
    fx.engine
        .purge_operations(&rest)
        .await
        .expect("purge_operations retries");
    assert!(
        get(fx, &fx.account_a, &ids[2]).await.is_none(),
        "the retried pair is purged"
    );
}

/// Deleting an operation's record releases its `ClientToken`: a token that
/// replays while the terminal record exists creates a fresh operation once
/// the purge has removed the record.
pub async fn a_purged_operations_token_is_free_for_a_new_start<E: StorageEngine>(fx: &Fixture<E>) {
    let owner = "owner-1";
    let table = create_table(fx, &fx.account_a, "purge_token").await;
    let first = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, Some("tok-held"), "fp", 1000),
            50,
        )
        .await,
    );
    let claimed = fx
        .engine
        .claim_runnable(owner, Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert!(
        claimed.iter().any(|r| r.operation_id == first.operation_id),
        "the operation must be claimable"
    );
    fx.engine
        .finish(
            &fx.account_a,
            &first.operation_id,
            owner,
            terminal("COMPLETED", 1000),
        )
        .await
        .expect("finish");

    // The token is retained for as long as the record is: the same start
    // replays rather than creating.
    let held = start(
        fx,
        &fx.account_a,
        new_op(fx, &fx.account_a, &table, Some("tok-held"), "fp", 2000),
        50,
    )
    .await;
    match held {
        StartOutcome::IdempotentReplay(record) => {
            assert_eq!(
                record.operation_id, first.operation_id,
                "the retained token must answer the terminal operation"
            );
        }
        other => panic!(
            "expected IdempotentReplay before the purge, got {}",
            start_kind(&other)
        ),
    }

    fx.engine
        .purge_operations(&[(fx.account_a.clone(), first.operation_id.clone())])
        .await
        .expect("purge_operations");
    assert!(
        get(fx, &fx.account_a, &first.operation_id).await.is_none(),
        "the record is gone"
    );

    let fresh = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, Some("tok-held"), "fp", 3000),
            50,
        )
        .await,
    );
    assert_ne!(
        fresh.operation_id, first.operation_id,
        "the released token must create a fresh operation, not replay the purged one"
    );
}

/// The cutoff conversion helper the entry points may want: a `SystemTime`
/// from epoch milliseconds.
#[must_use]
pub fn cutoff_at_ms(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}

/// The listed (`account_id`, `operation_id`) pairs name exactly the ledger
/// rows the retention worker must delete before it purges: driving
/// `delete_bulk_row_outcomes` with them removes the purged operation's
/// ledger and leaves the other operation's alone, and the purge that
/// follows removes the record.
pub async fn purged_pairs_name_the_ledger_rows_to_delete<E: StorageEngine>(fx: &Fixture<E>) {
    let owner = "owner-1";
    let table = create_table(fx, &fx.account_a, "purge_ledger").await;
    let ki = key_info(fx, &table.table_name).await;
    let maps = ExpressionMaps::new(HashMap::new(), HashMap::new());

    let old_table = create_table(fx, &fx.account_a, "purge_ledger_old").await;
    let old_id = started_and_claimed(fx, &old_table, owner, 1000).await;
    for (pk, row) in [("a1", 1_i64), ("a2", 2)] {
        fx.engine
            .put_item(
                &ki,
                item(pk, "1"),
                false,
                None,
                &maps,
                None,
                Some(&marker(&fx.account_a, &old_id, row)),
            )
            .await
            .expect("a marked put for the old operation");
    }
    fx.engine
        .finish(&fx.account_a, &old_id, owner, terminal("COMPLETED", 1000))
        .await
        .expect("finish the old operation");

    let live_table = create_table(fx, &fx.account_a, "purge_ledger_live").await;
    let live_id = started_and_claimed(fx, &live_table, owner, 2000).await;
    fx.engine
        .put_item(
            &ki,
            item("b1", "1"),
            false,
            None,
            &maps,
            None,
            Some(&marker(&fx.account_a, &live_id, 1)),
        )
        .await
        .expect("a marked put for the live operation");

    let purgeable = fx
        .engine
        .list_purgeable_before(cutoff_at_ms(5000), usize::MAX)
        .await
        .expect("list_purgeable_before");
    assert_eq!(
        purgeable,
        vec![(fx.account_a.clone(), old_id.clone())],
        "exactly the old terminal operation is named"
    );

    // The worker's ordering: ledger rows first, while the record still
    // exists, then the purge.
    for (account_id, operation_id) in &purgeable {
        fx.engine
            .delete_bulk_row_outcomes(account_id, operation_id)
            .await
            .expect("delete the purged operation's ledger rows");
    }

    for row in [1, 2] {
        assert_eq!(
            fx.engine
                .bulk_row_outcome(&fx.account_a, &old_id, 0, row)
                .await
                .expect("read"),
            None,
            "the purged operation's ledger rows are gone"
        );
    }
    assert_eq!(
        fx.engine
            .bulk_row_outcome(&fx.account_a, &live_id, 0, 1)
            .await
            .expect("read"),
        Some(RowOutcome::Applied),
        "the live operation's ledger survives"
    );

    fx.engine
        .purge_operations(&purgeable)
        .await
        .expect("purge_operations");
    assert!(
        get(fx, &fx.account_a, &old_id).await.is_none(),
        "the record follows its ledger rows"
    );
}
