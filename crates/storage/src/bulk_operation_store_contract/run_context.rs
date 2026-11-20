// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The run context the engine records at Start: stored as given and handed
//! back by every read, and absent on a row started without one.

use super::*;

/// A run context recorded at Start comes back unchanged from the start
/// outcome, `get`, `list` and `claim_runnable`; an operation started without
/// one reads back `None` on each of them.
pub async fn the_run_context_round_trips_through_every_read<E: StorageEngine>(fx: &Fixture<E>) {
    let with = create_table(fx, &fx.account_a, "run_context_with").await;
    let without = create_table(fx, &fx.account_a, "run_context_without").await;
    let context = json!({
        "store": "filesystem",
        "principal": {"identity": {"User": {"account_id": "111111111111", "user_name": "l\u{e9}a"}}},
        "values": [1, "two", null, {"nested": true}],
    });
    let mut req = new_op(fx, &fx.account_a, &with, None, "fp-with", 1_000);
    req.run_context = Some(context.clone());
    let first = created(start(fx, &fx.account_a, req, 50).await);
    assert_eq!(first.run_context.as_ref(), Some(&context));
    let req = new_op(fx, &fx.account_a, &without, None, "fp-without", 2_000);
    let second = created(start(fx, &fx.account_a, req, 50).await);
    assert_eq!(second.run_context, None);

    let expected = |operation_id: &str| {
        if operation_id == first.operation_id {
            Some(context.clone())
        } else {
            None
        }
    };
    for id in [&first.operation_id, &second.operation_id] {
        let record = get(fx, &fx.account_a, id).await.expect("the record exists");
        assert_eq!(record.run_context, expected(id), "get, {id}");
    }
    let (listed, _) = fx
        .engine
        .list_bulk_operations(&fx.account_a, 100, None)
        .await
        .expect("list");
    assert_eq!(listed.len(), 2);
    for record in &listed {
        assert_eq!(
            record.run_context,
            expected(&record.operation_id),
            "list, {}",
            record.operation_id
        );
    }
    let claimed = fx
        .engine
        .claim_runnable("owner-1", Duration::from_secs(60), 10, &[], &[])
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 2);
    for record in &claimed {
        assert_eq!(
            record.run_context,
            expected(&record.operation_id),
            "claim, {}",
            record.operation_id
        );
    }
}
