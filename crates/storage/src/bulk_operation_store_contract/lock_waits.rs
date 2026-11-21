// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Starts whose locks are held by another transaction, and starts that hold
//! locks others need. The cases that need a lock held from outside take a
//! `hold` from the backend's test file, which keeps the lock from a
//! connection of its own for a number of milliseconds; a backend whose start
//! takes no such lock does not run them.

use std::future::Future;
use std::time::Instant;

use extenddb_core::types::DeleteTableInput;

use super::*;

/// How long `hold` keeps the lock in these cases.
const HOLD_MS: u64 = 3_000;

/// The bound on a start that waits for no lock but the account's, held by
/// other starts for a few statements at a time.
const UNHELD_BOUND: Duration = Duration::from_millis(500);

/// The table row of table U is held. Eight starts on U retry while it is
/// held. Five starts of the same account on tables of their own, sent
/// through the hold, each answer within [`UNHELD_BOUND`]. Once the row is
/// free one start on U is created and the others answer busy.
pub async fn a_start_waiting_on_a_held_row_does_not_delay_another_table<E, F, Fut>(
    fx: &Fixture<E>,
    hold: F,
) where
    E: StorageEngine + 'static,
    F: FnOnce(String, u64) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let u = create_table(fx, &fx.account_a, "held_row_u").await;
    let mut others = Vec::new();
    for i in 0..5 {
        others.push(create_table(fx, &fx.account_a, &format!("held_row_v{i}")).await);
    }
    let holder = tokio::spawn(hold(u.table_id.clone(), HOLD_MS));
    tokio::time::sleep(Duration::from_millis(100)).await;

    let waiting: Vec<_> = (0..8)
        .map(|i| {
            let engine = Arc::clone(&fx.engine);
            let account = fx.account_a.clone();
            let req = new_op(fx, &fx.account_a, &u, None, "fp", 2000 + i);
            tokio::spawn(async move { engine.start_bulk_operation(&account, req, 50).await })
        })
        .collect();

    for (i, other) in others.iter().enumerate() {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let called = Instant::now();
        let req = new_op(
            fx,
            &fx.account_a,
            other,
            None,
            "fp",
            3000 + 10 * i64::try_from(i).unwrap(),
        );
        let outcome = fx
            .engine
            .start_bulk_operation(&fx.account_a, req, 50)
            .await
            .expect("a start on another table");
        let elapsed = called.elapsed();
        assert_eq!(start_kind(&outcome), "Created", "the start on table v{i}");
        assert!(
            elapsed < UNHELD_BOUND,
            "the start on table v{i} waited {elapsed:?} while another table's row was held"
        );
    }

    holder.await.expect("join the holder");
    let mut kinds = Vec::new();
    for task in waiting {
        let outcome = task
            .await
            .expect("join a start")
            .expect("a start on the held table answers");
        kinds.push(start_kind(&outcome));
    }
    kinds.sort_unstable();
    let mut expected = vec!["TableBusy"; 7];
    expected.insert(0, "Created");
    assert_eq!(kinds, expected, "the starts on the held table");
}

/// Eight tasks send starts on table T in a loop for three seconds, each
/// refused by an account limit of 0 after it has taken its locks. A
/// `DeleteTable` of T sent into the stream answers within a second.
pub async fn a_stream_of_starts_does_not_hold_off_delete_table<E: StorageEngine + 'static>(
    fx: &Fixture<E>,
) {
    let table = create_table(fx, &fx.account_a, "start_stream").await;
    let until = Instant::now() + Duration::from_millis(HOLD_MS);
    let senders: Vec<_> = (0..8_i64)
        .map(|i| {
            let engine = Arc::clone(&fx.engine);
            let account = fx.account_a.clone();
            // More than a sender gets through in the stream's time.
            let mut reqs: Vec<NewBulkOperation> = (0..2_000_i64)
                .map(|k| new_op(fx, &fx.account_a, &table, None, "fp", 10_000 * (i + 1) + k))
                .collect();
            tokio::spawn(async move {
                let mut sent = 0u32;
                while Instant::now() < until {
                    let Some(req) = reqs.pop() else { break };
                    // The answers do not matter here: a limit refusal while
                    // the table exists, the deleting answer after.
                    let _ = engine.start_bulk_operation(&account, req, 0).await;
                    sent += 1;
                }
                sent
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(300)).await;
    let called = Instant::now();
    fx.engine
        .delete_table(
            &fx.account_a,
            DeleteTableInput {
                table_name: table.table_name.clone(),
            },
        )
        .await
        .expect("DeleteTable during the stream of starts");
    let elapsed = called.elapsed();

    let mut sent = 0;
    for sender in senders {
        sent += sender.await.expect("join a sender");
    }
    assert!(sent > 8, "the stream sent {sent} starts");
    assert!(
        elapsed < Duration::from_millis(1_000),
        "DeleteTable answered after {elapsed:?} with {sent} starts sent on the table"
    );
}

/// The catalog's index table is held while a start reads the table's index
/// states, after it has its locks. The start waits for the read and is
/// created; it is not answered with a fault.
pub async fn a_start_whose_index_read_waits_is_created<E, F, Fut>(fx: &Fixture<E>, hold: F)
where
    E: StorageEngine + 'static,
    F: FnOnce(String, u64) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let table = create_table(fx, &fx.account_a, "held_index_read").await;
    let holder = tokio::spawn(hold(table.table_id.clone(), 1_000));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let outcome = fx
        .engine
        .start_bulk_operation(
            &fx.account_a,
            new_op(fx, &fx.account_a, &table, None, "fp", 1000),
            50,
        )
        .await
        .expect("a start whose index read waits answers");
    assert_eq!(start_kind(&outcome), "Created");
    holder.await.expect("join the holder");
}
