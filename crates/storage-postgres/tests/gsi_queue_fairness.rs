// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level tests for the GSI queue serving tables in turn.
//!
//! Queue partitions are shared by every table. A worker that claimed the lowest
//! `id` across its partition applied one table's whole backlog before any row a
//! second table enqueued later, so a bulk write to one table delayed another
//! table's index rows by the time the backlog took to drain. The first test puts
//! a 20,000 row backlog for table A on the queue, then writes 8 items to table B
//! with a 300 ms propagation delay, and requires B's rows to apply while more than
//! three quarters of A's backlog is still queued. The second test checks that a
//! running worker leaves a held table's rows queued (the claim's hold check) and
//! applies them once the hold is gone.
//!
//! Not covered: a listed table whose claim finds nothing leaves the rotation for
//! the rest of the batch. Ending the batch instead would apply the same rows in
//! the same order; the only difference is the drain rate while a held table is
//! listed (a build in progress), one extra listing per claim. Pinning that needs a
//! relative drain measurement that is not stable on a shared host, so it is left
//! to the unit tests on the rotation. A table whose rows are all inside their
//! delay is not listed at all, so it does not reach that path.
//!
//! See `gsi_queue_support` for the scratch database and the skip convention.

mod gsi_queue_support;

use std::time::{Duration, Instant};

use extenddb_core::types::TableKeyInfo;
use gsi_queue_support::{
    Scratch, base_conn, enqueue_rows, index_rows, pending, put, scratch, set_delay_ms,
    table_with_gsi, template_row,
};

const BACKLOG: i64 = 20_000;
const B_ROWS: usize = 8;
/// The configured propagation delay for table B's rows.
const DELAY_MS: u64 = 300;
/// Table B's rows must apply before this much of table A's backlog has drained.
/// Both sides scale with the per-row cost of the host, so the bound does not move
/// with its speed; with the fix 97 percent or more of the backlog is still queued,
/// behind the backlog in `id` order none of it is.
const A_STILL_QUEUED: i64 = BACKLOG * 3 / 4;
/// Loop ceiling for table B's rows. The delay plus one worker wakeup (1 s idle
/// backstop) is the expectation; behind a 20,000 row backlog the rows took over
/// 10 s on the test host, and over 60 s on a slow one.
const B_CEILING: Duration = Duration::from_secs(60);

/// Create two tables and return `(a, b)` with `a` the one whose `table_id` sorts
/// first. The rotation lists tables in `table_id` order, so the backlog table is
/// always the one served first; a rotation that never advanced would then serve
/// the backlog alone and fail the test on every run, not only when the ids
/// happened to sort that way.
async fn two_tables_backlog_first(s: &Scratch) -> (TableKeyInfo, TableKeyInfo) {
    let t1 = table_with_gsi(s, "t_one").await;
    let t2 = table_with_gsi(s, "t_two").await;
    if t1.table_id < t2.table_id {
        (t1, t2)
    } else {
        (t2, t1)
    }
}

/// Put `BACKLOG` ready rows for table A on the queue, spread over the four
/// partitions.
async fn enqueue_backlog(s: &Scratch, a: &TableKeyInfo) {
    set_delay_ms(&s.db, 60_000).await;
    let template = template_row(s, a).await;
    enqueue_rows(s, a, &template, "a", BACKLOG, None, Duration::ZERO).await;
    set_delay_ms(&s.db, DELAY_MS).await;
}

/// Wait until `table` has no pending rows, up to `ceiling`; returns the time it
/// took. `context` is evaluated for the failure message.
async fn wait_applied(
    s: &Scratch,
    table: &TableKeyInfo,
    ceiling: Duration,
    what: &str,
) -> Duration {
    let started = Instant::now();
    loop {
        let left = pending(&s.db, &table.table_id).await;
        if left == 0 {
            return started.elapsed();
        }
        assert!(
            started.elapsed() < ceiling,
            "{what} still has {left} rows pending after {ceiling:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn another_tables_backlog_does_not_delay_this_tables_rows() {
    let test = "another_tables_backlog_does_not_delay_this_tables_rows";
    if base_conn().is_none() {
        eprintln!("SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    let (a, b) = two_tables_backlog_first(&s).await;
    enqueue_backlog(&s, &a).await;
    assert_eq!(pending(&s.db, &a.table_id).await, BACKLOG);

    for i in 0..B_ROWS {
        put(&s, &b, &format!("b{i}"), &format!("g{i}")).await;
    }
    assert_eq!(pending(&s.db, &b.table_id).await, B_ROWS as i64);

    let applied_after = wait_applied(&s, &b, B_CEILING, "table B").await;
    let a_left = pending(&s.db, &a.table_id).await;
    eprintln!(
        "{test}: table B's {B_ROWS} rows applied {applied_after:?} after the writes with {a_left} \
         of table A's {BACKLOG} rows still queued"
    );
    assert!(
        a_left > A_STILL_QUEUED,
        "table B's {B_ROWS} rows applied {applied_after:?} after the writes, with only {a_left} \
         of table A's {BACKLOG} rows still queued: they waited behind the backlog (the bound is \
         more than {A_STILL_QUEUED} still queued)"
    );
    assert_eq!(index_rows(&s.db, &b.table_id).await, B_ROWS as i64);

    // The backlog itself still applies in full.
    let drain_started = Instant::now();
    while pending(&s.db, &a.table_id).await > 0 {
        assert!(
            drain_started.elapsed() < Duration::from_secs(180),
            "table A's backlog did not drain within 180 s"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(index_rows(&s.db, &a.table_id).await, BACKLOG);
    s.cleanup().await;
}

/// A row in `vector_index_holds` for a table keeps the workers off that table's
/// rows: the claim statement checks the hold for the table it is bound to. Table
/// B is held, table C is not; both get 4 writes. C's rows apply, B's stay queued
/// with no index row written; removing the hold releases them.
#[tokio::test]
async fn a_held_tables_rows_stay_queued_until_the_hold_is_removed() {
    let test = "a_held_tables_rows_stay_queued_until_the_hold_is_removed";
    if base_conn().is_none() {
        eprintln!("SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    const ROWS: usize = 4;
    let s = scratch().await;
    set_delay_ms(&s.db, DELAY_MS).await;
    let b = table_with_gsi(&s, "t_held").await;
    let c = table_with_gsi(&s, "t_free").await;
    sqlx::query("INSERT INTO vector_index_holds (table_id, index_id) VALUES ($1, 'building')")
        .bind(&b.table_id)
        .execute(&s.db)
        .await
        .expect("place the hold");

    for i in 0..ROWS {
        put(&s, &b, &format!("b{i}"), &format!("g{i}")).await;
        put(&s, &c, &format!("c{i}"), &format!("g{i}")).await;
    }
    assert_eq!(pending(&s.db, &b.table_id).await, ROWS as i64);

    // C's rows apply as usual; then past the delay plus two idle backstops, B's
    // rows are still queued and nothing reached its index.
    wait_applied(&s, &c, Duration::from_secs(30), "table C").await;
    tokio::time::sleep(Duration::from_millis(DELAY_MS + 2_000)).await;
    assert_eq!(
        pending(&s.db, &b.table_id).await,
        ROWS as i64,
        "held rows were claimed"
    );
    assert_eq!(
        index_rows(&s.db, &b.table_id).await,
        0,
        "held rows reached the index"
    );
    assert_eq!(index_rows(&s.db, &c.table_id).await, ROWS as i64);

    sqlx::query("DELETE FROM vector_index_holds WHERE table_id = $1")
        .bind(&b.table_id)
        .execute(&s.db)
        .await
        .expect("remove the hold");
    // The workers' 1 s idle backstop picks the release up; the flip to ACTIVE
    // would also notify them.
    let released_after = wait_applied(&s, &b, Duration::from_secs(30), "table B").await;
    eprintln!(
        "{test}: table B's {ROWS} rows applied {released_after:?} after the hold was removed"
    );
    assert_eq!(index_rows(&s.db, &b.table_id).await, ROWS as i64);
    s.cleanup().await;
}
