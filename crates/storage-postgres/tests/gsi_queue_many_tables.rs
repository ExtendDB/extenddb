// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level test for the GSI queue with more tables in one partition than
//! one batch of claims.
//!
//! A worker claims up to 100 rows per batch, one per listed table in turn. With
//! more than 100 tables holding ready rows in a partition, a batch that always
//! began at the lowest `table_id` served the same first hundred tables every time
//! and reached the rest only once those were empty; under steady writes to the
//! first hundred, never. The worker now carries the table it served last from one
//! batch to the next and begins after it, so every listed table gets a claim once
//! per pass around the ring.
//!
//! This test puts 104 tables' rows in partition 0 (one worker): 100 rows for each
//! of the hundred tables whose ids sort first, 5 rows for each of the four that
//! sort last. It requires the four tail tables' rows to apply while more than
//! three quarters of the head tables' rows are still queued; with a rotation that
//! restarted at the lowest id, none of the head rows would be left.
//!
//! See `gsi_queue_support` for the scratch database and the skip convention.

mod gsi_queue_support;

use std::time::{Duration, Instant};

use gsi_queue_support::{
    base_conn, enqueue_rows, index_rows, pending_in, scratch, set_delay_ms, table_with_gsi,
    template_row,
};

/// More tables than one batch of claims (100).
const TABLES: usize = 104;
const TAIL: usize = 4;
const HEAD_ROWS: i64 = 100;
const TAIL_ROWS: i64 = 5;
/// The partition every row is placed in, so one worker serves them all.
const PARTITION: i32 = 0;

#[tokio::test]
async fn tables_past_one_batch_of_claims_are_served_within_the_next_passes() {
    let test = "tables_past_one_batch_of_claims_are_served_within_the_next_passes";
    if base_conn().is_none() {
        eprintln!("SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return;
    }
    let s = scratch().await;
    // Far enough out that each table's template row is still there to read.
    set_delay_ms(&s.db, 60_000).await;
    let mut tables = Vec::with_capacity(TABLES);
    for i in 0..TABLES {
        tables.push(table_with_gsi(&s, &format!("t_many_{i:03}")).await);
    }
    // The rotation lists tables in `table_id` order; the tail is the end of it.
    tables.sort_by(|x, y| x.table_id.cmp(&y.table_id));
    let (head, tail) = tables.split_at(TABLES - TAIL);
    // Seeded an hour out so the workers see every table's rows at the same
    // moment, then released together.
    for (i, t) in tables.iter().enumerate() {
        let template = template_row(&s, t).await;
        let rows = if i < TABLES - TAIL {
            HEAD_ROWS
        } else {
            TAIL_ROWS
        };
        let hour = Duration::from_secs(3_600);
        enqueue_rows(&s, t, &template, "k", rows, Some(PARTITION), hour).await;
    }
    set_delay_ms(&s.db, 300).await;
    let head_ids: Vec<String> = head.iter().map(|t| t.table_id.clone()).collect();
    let tail_ids: Vec<String> = tail.iter().map(|t| t.table_id.clone()).collect();
    let head_total = HEAD_ROWS * head.len() as i64;
    assert_eq!(pending_in(&s.db, &head_ids).await, head_total);
    assert_eq!(pending_in(&s.db, &tail_ids).await, TAIL_ROWS * TAIL as i64);

    let started = Instant::now();
    sqlx::query("UPDATE gsi_pending SET ready_at = NOW() WHERE worker_partition = $1")
        .bind(PARTITION)
        .execute(&s.db)
        .await
        .expect("release every row");
    let (tail_applied_after, head_left) = loop {
        let left = pending_in(&s.db, &tail_ids).await;
        if left == 0 {
            break (started.elapsed(), pending_in(&s.db, &head_ids).await);
        }
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "the {TAIL} tail tables still have {left} rows pending after 120 s; the head tables \
             have {} of {head_total} left",
            pending_in(&s.db, &head_ids).await
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    eprintln!(
        "{test}: the {TAIL} tail tables' rows applied {tail_applied_after:?} after the workers saw \
         them with {head_left} of the head tables' {head_total} rows still queued"
    );
    assert!(
        head_left > head_total * 3 / 4,
        "the tail tables' rows applied with only {head_left} of the head tables' {head_total} \
         rows still queued: they waited for the head tables to empty"
    );

    // Everything still applies in full.
    let drain_started = Instant::now();
    while pending_in(&s.db, &head_ids).await > 0 {
        assert!(
            drain_started.elapsed() < Duration::from_secs(240),
            "the head tables' rows did not drain within 240 s"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    for t in head {
        assert_eq!(
            index_rows(&s.db, &t.table_id).await,
            HEAD_ROWS,
            "{}",
            t.table_name
        );
    }
    for t in tail {
        assert_eq!(
            index_rows(&s.db, &t.table_id).await,
            TAIL_ROWS,
            "{}",
            t.table_name
        );
    }
    s.cleanup().await;
}
