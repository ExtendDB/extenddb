// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Claim and lease: ownership, fencing, takeover, claim ordering, and lease expiry.

use super::*;

/// Two owners claiming from one barrier never claim the same row, and
/// between them they claim every runnable row exactly once.
pub async fn two_owners_claiming_concurrently_share_no_operation<E: StorageEngine + 'static>(
    fx: &Fixture<E>,
) {
    let mut ids = HashSet::new();
    for i in 0..6 {
        let table = create_table(fx, &fx.account_a, &format!("claim_race_{i}")).await;
        let record = created(
            start(
                fx,
                &fx.account_a,
                new_op(fx, &fx.account_a, &table, None, "fp", 1000 + i),
                50,
            )
            .await,
        );
        ids.insert(record.operation_id);
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let mut tasks = Vec::new();
    for owner in ["owner-1", "owner-2"] {
        let engine = Arc::clone(&fx.engine);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            engine
                .claim_runnable(owner, Duration::from_secs(60), 6, &[], &[])
                .await
                .map(|rows| {
                    rows.into_iter()
                        .map(|r| (r.operation_id, r.lease_owner))
                        .collect::<Vec<_>>()
                })
        }));
    }
    let mut all_claimed = Vec::new();
    for (task, owner) in tasks.into_iter().zip(["owner-1", "owner-2"]) {
        for (id, lease_owner) in task.await.expect("join").expect("claim") {
            assert_eq!(
                lease_owner.as_deref(),
                Some(owner),
                "a claimed row carries its claimer"
            );
            all_claimed.push(id);
        }
    }
    let distinct: HashSet<_> = all_claimed.iter().cloned().collect();
    assert_eq!(
        distinct.len(),
        all_claimed.len(),
        "no row may be claimed by both owners"
    );
    assert_eq!(distinct, ids, "every runnable row is claimed exactly once");
}

/// The claim predicate covers CANCELLING rows, so a cancelled operation whose
/// runner died is picked up and can be finished.
pub async fn claim_returns_cancelling_operations_too<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "claim_cancelling").await;
    let record = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, None, "fp", 1000),
            50,
        )
        .await,
    );
    let cancel = fx
        .engine
        .request_cancel(&fx.account_a, &record.operation_id)
        .await
        .expect("request_cancel");
    assert!(matches!(cancel, CancelOutcome::Cancelling));

    let claimed = fx
        .engine
        .claim_runnable("owner-1", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    let row = claimed
        .iter()
        .find(|r| r.operation_id == record.operation_id)
        .expect("a CANCELLING row with no lease must be claimable");
    assert_eq!(row.status, "CANCELLING");
}

/// A held lease blocks a second claim; once it expires the row is claimable
/// again by another owner.
pub async fn a_live_lease_blocks_claim_until_it_expires<E: StorageEngine>(fx: &Fixture<E>) {
    let table = create_table(fx, &fx.account_a, "lease_expiry").await;
    let record = created(
        start(
            fx,
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, None, "fp", 1000),
            50,
        )
        .await,
    );

    // The blocked claim below must run before this lease expires. Two
    // seconds gives that one round trip a wide margin under host load;
    // anything the lease outlives here it also outlives in the expiry
    // sleep, which only needs to exceed it.
    let first = fx
        .engine
        .claim_runnable("owner-1", Duration::from_secs(2), 10, &[], &[])
        .await
        .expect("claim");
    assert!(first.iter().any(|r| r.operation_id == record.operation_id));

    let blocked = fx
        .engine
        .claim_runnable("owner-2", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert!(
        !blocked
            .iter()
            .any(|r| r.operation_id == record.operation_id),
        "a live lease must block the claim"
    );

    tokio::time::sleep(Duration::from_millis(2500)).await;

    let reclaimed = fx
        .engine
        .claim_runnable("owner-2", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    let row = reclaimed
        .iter()
        .find(|r| r.operation_id == record.operation_id)
        .expect("an expired lease must release the row");
    assert_eq!(row.lease_owner.as_deref(), Some("owner-2"));
}

/// Claiming honours `max` and takes the oldest starts first.
pub async fn claim_prefers_the_oldest_start_and_honours_max<E: StorageEngine>(fx: &Fixture<E>) {
    let mut ids_by_time = Vec::new();
    for (i, t) in [3000_i64, 1000, 2000].iter().enumerate() {
        let table = create_table(fx, &fx.account_a, &format!("claim_order_{i}")).await;
        let record = created(
            start(
                fx,
                &fx.account_a,
                new_op(fx, &fx.account_a, &table, None, "fp", *t),
                50,
            )
            .await,
        );
        ids_by_time.push((*t, record.operation_id));
    }
    ids_by_time.sort();

    let claimed = fx
        .engine
        .claim_runnable("owner-1", Duration::from_secs(60), 2, &[], &[])
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 2, "claim must honour max");
    let claimed_ids: HashSet<_> = claimed.iter().map(|r| r.operation_id.clone()).collect();
    let oldest_two: HashSet<_> = ids_by_time[..2].iter().map(|(_, id)| id.clone()).collect();
    assert_eq!(
        claimed_ids, oldest_two,
        "claim takes the oldest starts first"
    );
}

/// Rows of an excluded account are skipped even when they are the oldest
/// and `max` has room for them: the claim takes only the other account's.
pub async fn claim_skips_the_rows_of_excluded_accounts<E: StorageEngine>(fx: &Fixture<E>) {
    let mut ids_by_account: HashMap<String, HashSet<String>> = HashMap::new();
    // The excluded account's rows start older, so a claim that merely
    // orders by start time without the filter would take them first.
    for (account, base_time) in [(&fx.account_a, 1000_i64), (&fx.account_b, 5000)] {
        for i in 0..2 {
            let table = create_table(fx, account, &format!("claim_exclude_{i}")).await;
            let record = created(
                start(
                    fx,
                    account,
                    new_op(fx, account, &table, None, "fp", base_time + i),
                    50,
                )
                .await,
            );
            ids_by_account
                .entry(account.clone())
                .or_default()
                .insert(record.operation_id);
        }
    }

    let claimed = fx
        .engine
        .claim_runnable(
            "owner-1",
            Duration::from_secs(60),
            4,
            std::slice::from_ref(&fx.account_a),
            &[],
        )
        .await
        .expect("claim");
    let claimed_ids: HashSet<_> = claimed.iter().map(|r| r.operation_id.clone()).collect();
    assert_eq!(
        claimed_ids, ids_by_account[&fx.account_b],
        "the claim must take exactly the non-excluded account's rows"
    );
}

/// Operation ids in the exclude list are skipped even when they are the
/// oldest rows and `max` has room for them: the claim takes exactly the
/// others and leaves the excluded rows unleased. The worker passes the
/// operation ids running in its process, so a run that outlives its lease
/// is never claimed a second time by the process still running it.
pub async fn claim_skips_the_operations_in_the_exclude_list<E: StorageEngine>(fx: &Fixture<E>) {
    let mut ids = Vec::new();
    // The excluded rows start oldest, so a claim that merely orders by
    // start time without the filter would take them first.
    for i in 0..4_i64 {
        let table = create_table(fx, &fx.account_a, &format!("claim_exclude_op_{i}")).await;
        let record = created(
            start(
                fx,
                &fx.account_a,
                new_op(fx, &fx.account_a, &table, None, "fp", 1000 + i),
                50,
            )
            .await,
        );
        ids.push(record.operation_id);
    }
    let excluded = vec![ids[0].clone(), ids[1].clone()];

    let claimed = fx
        .engine
        .claim_runnable("owner-1", Duration::from_secs(60), 10, &[], &excluded)
        .await
        .expect("claim");
    let claimed_ids: HashSet<_> = claimed.iter().map(|r| r.operation_id.clone()).collect();
    let expected: HashSet<_> = ids[2..].iter().cloned().collect();
    assert_eq!(
        claimed_ids, expected,
        "the claim must skip exactly the excluded operation ids"
    );
    for id in &excluded {
        let record = get(fx, &fx.account_a, id).await.expect("record");
        assert_eq!(
            record.lease_owner, None,
            "an excluded row must not be leased"
        );
    }
}

/// Claimed rows come back oldest start first in the returned Vec, not merely
/// as the right set: a capped runner drains claims in Vec order, so the
/// order is the fairness policy.
pub async fn claim_returns_records_oldest_start_first_in_order<E: StorageEngine>(fx: &Fixture<E>) {
    let mut by_time = Vec::new();
    for (i, t) in [3000_i64, 1000, 2000].iter().enumerate() {
        let table = create_table(fx, &fx.account_a, &format!("claim_vec_order_{i}")).await;
        let record = created(
            start(
                fx,
                &fx.account_a,
                new_op(fx, &fx.account_a, &table, None, "fp", *t),
                50,
            )
            .await,
        );
        by_time.push((*t, record.operation_id));
    }
    by_time.sort();

    let claimed = fx
        .engine
        .claim_runnable("owner-1", Duration::from_secs(60), 3, &[], &[])
        .await
        .expect("claim");
    let returned: Vec<(i64, String)> = claimed
        .iter()
        .map(|r| (r.start_time_ms, r.operation_id.clone()))
        .collect();
    assert_eq!(
        returned, by_time,
        "the returned Vec is ascending by (start_time_ms, operation_id)"
    );
}

/// Two rows sharing a start time are claimed in `operation_id` order on every
/// backend, so the tiebreak does not depend on database collation or plan.
pub async fn claim_breaks_start_time_ties_by_operation_id<E: StorageEngine>(fx: &Fixture<E>) {
    let mut ids = Vec::new();
    for i in 0..2 {
        let table = create_table(fx, &fx.account_a, &format!("claim_tie_{i}")).await;
        let record = created(
            start(
                fx,
                &fx.account_a,
                new_op(fx, &fx.account_a, &table, None, "fp", 1000),
                50,
            )
            .await,
        );
        ids.push(record.operation_id);
    }
    ids.sort();

    let claimed = fx
        .engine
        .claim_runnable("owner-1", Duration::from_secs(60), 2, &[], &[])
        .await
        .expect("claim");
    let returned: Vec<String> = claimed.iter().map(|r| r.operation_id.clone()).collect();
    assert_eq!(
        returned, ids,
        "equal start times order by operation_id ascending"
    );
}

/// The checkpoint and `finish` bind on `lease_owner`, never on the lease
/// clock: an owner whose lease has expired, on an operation no other owner
/// has claimed, still checkpoints and finishes as `Held`. Expiry gates
/// claiming; ownership gates writing. A runner stalled past its lease on a
/// loaded host must keep its run unless a takeover actually happened, and
/// every existing lease case exercises either a live lease or a takeover,
/// so this boundary would otherwise go unpinned.
pub async fn an_expired_unclaimed_lease_still_accepts_the_owners_checkpoint_and_finish<
    E: StorageEngine,
>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "expired_owner_writes").await;
    let record = created(
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
        .claim_runnable("owner-1", Duration::from_millis(50), 10, &[], &[])
        .await
        .expect("claim");
    assert!(
        claimed
            .iter()
            .any(|r| r.operation_id == record.operation_id)
    );
    let id = record.operation_id;

    // Long past the 50 ms lease. The sleep only needs to exceed the lease,
    // so a slower machine widens the margin instead of eroding it.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let state = fx
        .engine
        .checkpoint(
            &fx.account_a,
            &id,
            "owner-1",
            Duration::from_secs(60),
            progress(Vec::new()),
        )
        .await
        .expect("checkpoint");
    assert!(
        matches!(state, LeaseState::Held),
        "an expired lease no one claimed must not turn the owner's checkpoint into Lost"
    );

    let state = fx
        .engine
        .finish(&fx.account_a, &id, "owner-1", terminal("COMPLETED", 2000))
        .await
        .expect("finish");
    assert!(
        matches!(&state, FinishState::Held(status) if status == "COMPLETED"),
        "the owner's finish must run after its lease expired unclaimed"
    );
    let row = get(fx, &fx.account_a, &id).await.expect("row");
    assert_eq!(row.status, "COMPLETED");
    assert_eq!(row.lease_owner, None, "finish clears the lease");
}
