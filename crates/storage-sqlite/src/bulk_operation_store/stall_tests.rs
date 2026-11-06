// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! How long the `SQLite` store keeps other writers waiting, and how report
//! paging scales: the end of an operation deletes its report records in
//! chunks that release the engine write lock between them, a report page
//! seeks to its cursor, and a claim that finds nothing to claim takes no
//! write lock.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use super::tests::{engine, new_op};
use super::*;
use crate::store::SqliteEngine;

const ACCOUNT: &str = "000000000000";

/// The cases here time themselves, so they run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Insert `count` error records for one file of `operation_id`, row
/// indexes `0..count`, in one statement.
async fn insert_records(engine: &SqliteEngine, operation_id: &str, count: i64) {
    sqlx::query(
        "INSERT INTO bulk_operation_report_records \
             (account_id, operation_id, kind, file_index, row_index, record) \
         WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i + 1 < ?) \
         SELECT ?, ?, 'error', 0, i, '{\"itemIndex\": 1}' FROM n",
    )
    .bind(count)
    .bind(ACCOUNT)
    .bind(operation_id)
    .execute(&engine.pool)
    .await
    .expect("insert report records");
}

/// Page through every error record of one operation the way the report
/// renderer does, 1,000 at a time, and return how long it took.
async fn page_through(engine: &SqliteEngine, operation_id: &str, expected: usize) -> Duration {
    let began = Instant::now();
    let mut after = None;
    let mut read = 0;
    loop {
        let page = engine
            .report_records_page(ACCOUNT, operation_id, ReportKind::Error, after, 1000)
            .await
            .expect("report page");
        let Some(last) = page.last() else {
            break;
        };
        after = Some((last.file_index, last.row_index));
        read += page.len();
    }
    assert_eq!(read, expected, "every record is read once");
    began.elapsed()
}

/// Reading a 300,000-record report costs no more per record than reading a
/// 30,000-record one. A page whose cursor test the index cannot seek to
/// scans every record before the cursor, so the cost of a page grows with
/// its position and the larger report costs about ten times more per
/// record.
#[tokio::test]
async fn report_paging_costs_the_same_per_record_at_any_report_size() {
    const SMALL: i64 = 30_000;
    const LARGE: i64 = 300_000;
    let _serial = SERIAL.lock().await;
    let engine = engine().await;
    insert_records(&engine, "op-small", SMALL).await;
    insert_records(&engine, "op-large", LARGE).await;
    // The smaller report is read twice and the faster read kept, so a
    // pause of the host during one read does not decide the comparison.
    let small = page_through(&engine, "op-small", 30_000)
        .await
        .min(page_through(&engine, "op-small", 30_000).await);
    let large = page_through(&engine, "op-large", 300_000).await;
    eprintln!("paging {SMALL} records took {small:?}, {LARGE} records {large:?}");
    #[allow(clippy::cast_precision_loss)] // the counts fit f64 exactly
    let (per_small, per_large) = (
        small.as_secs_f64() / SMALL as f64,
        large.as_secs_f64() / LARGE as f64,
    );
    assert!(
        large < Duration::from_secs(30),
        "paging {LARGE} records took {large:?}"
    );
    assert!(
        per_large < per_small * 3.0,
        "a record of a {LARGE}-record report cost {:.2} us against {:.2} us in a \
         {SMALL}-record report",
        per_large * 1e6,
        per_small * 1e6
    );
}

/// Finishing an operation that holds 300,000 report records keeps no other
/// writer waiting for the engine write lock longer than a quarter of a
/// second, and deletes every record before it returns. One delete
/// statement holds the lock for the whole delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finishing_an_operation_with_many_records_keeps_other_writers_waiting_briefly() {
    let _serial = SERIAL.lock().await;
    let engine = Arc::new(engine().await);
    let outcome = engine
        .start_bulk_operation(ACCOUNT, new_op("op-many", "table-1", None, "fp"), 50)
        .await
        .expect("start");
    assert!(matches!(outcome, StartOutcome::Created(_)));
    let claimed = engine
        .claim_runnable("owner", Duration::from_secs(60), 1, &[], &[])
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);
    insert_records(&engine, "op-many", 300_000).await;

    // A writer that takes the write lock every few milliseconds while the
    // finish runs, and keeps the longest wait it saw.
    let done = Arc::new(AtomicBool::new(false));
    let lock = Arc::clone(&engine.write_lock);
    let stop = Arc::clone(&done);
    let probe = tokio::spawn(async move {
        let mut worst = Duration::ZERO;
        while !stop.load(Ordering::SeqCst) {
            let asked = Instant::now();
            let guard = lock.lock().await;
            worst = worst.max(asked.elapsed());
            drop(guard);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        worst
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let state = engine
        .finish(
            ACCOUNT,
            "op-many",
            "owner",
            Terminal {
                status: "COMPLETED_WITH_ERRORS".to_owned(),
                end_time_ms: 2_000,
                failure_code: None,
                failure_message: None,
                usage: serde_json::json!({}),
            },
        )
        .await
        .expect("finish");
    done.store(true, Ordering::SeqCst);
    let worst = probe.await.expect("probe");
    assert!(
        matches!(state, FinishState::Held(ref status) if status == "COMPLETED_WITH_ERRORS"),
        "the finish holds the lease"
    );
    let left: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM bulk_operation_report_records WHERE operation_id = 'op-many'",
    )
    .fetch_one(&engine.pool)
    .await
    .expect("count");
    assert_eq!(left, 0, "the finish deletes every report record");
    assert!(
        worst < Duration::from_millis(250),
        "another writer waited {worst:?} for the write lock during the finish"
    );
}

/// A claim that finds nothing to claim takes no write lock: it answers
/// while another writer holds the lock. With a runnable row it claims the
/// row as before.
#[tokio::test]
async fn an_idle_claim_does_not_take_the_write_lock() {
    let _serial = SERIAL.lock().await;
    let engine = engine().await;
    let held = engine.write_lock.lock().await;
    let claimed = tokio::time::timeout(
        Duration::from_secs(2),
        engine.claim_runnable("owner", Duration::from_secs(60), 1, &[], &[]),
    )
    .await
    .expect("an idle claim answers while the write lock is held")
    .expect("claim");
    assert!(claimed.is_empty());
    drop(held);

    let outcome = engine
        .start_bulk_operation(ACCOUNT, new_op("op-claim", "table-1", None, "fp"), 50)
        .await
        .expect("start");
    assert!(matches!(outcome, StartOutcome::Created(_)));
    let claimed = engine
        .claim_runnable("owner", Duration::from_secs(60), 1, &[], &[])
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1, "a runnable row is claimed");
    // Leased now, and excluded by account: nothing is claimable, and the
    // claim again answers without the lock.
    let held = engine.write_lock.lock().await;
    let claimed = tokio::time::timeout(
        Duration::from_secs(2),
        engine.claim_runnable("other", Duration::from_secs(60), 1, &[], &[]),
    )
    .await
    .expect("a claim over a leased row answers while the write lock is held")
    .expect("claim");
    assert!(claimed.is_empty());
    drop(held);
}
