// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The `PostgreSQL` bulk operation store against a live server, for the
//! behaviour the store's own clock and transactions decide: lease expiry
//! is read on the database's clock, a process stopped inside a store
//! transaction does not hold the operation row past a bound, and a claim
//! that finds nothing to claim takes no write lock.
//!
//! Each test builds a throwaway catalog database with the shipped catalog
//! migrations and drops it when it passes. Needs
//! `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component; without it every test reports a skip and passes, unless
//! `EXTENDDB_TEST_PG_REQUIRED` is set.

use std::cell::Cell;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use super::*;
use crate::PostgresConfig;

const ACCOUNT: &str = "111111111111";

thread_local! {
    static CLOCK_SKEW_MS: Cell<i64> = const { Cell::new(0) };
}

/// How far this thread's view of the wall clock runs from the host's, in
/// milliseconds; `now_ms` adds it under test. The cases that set it run on
/// a current-thread runtime, so every store call they make reads it.
pub(super) fn clock_skew_ms() -> i64 {
    CLOCK_SKEW_MS.with(Cell::get)
}

fn set_clock_skew_ms(ms: i64) {
    CLOCK_SKEW_MS.with(|skew| skew.set(ms));
}

struct Scratch {
    engine: PostgresEngine,
    raw: PgPool,
    admin: PgPool,
    name: String,
}

impl Scratch {
    async fn cleanup(self) {
        let Scratch {
            engine,
            raw,
            admin,
            name,
        } = self;
        engine.pool.close().await;
        engine.data_pool.close().await;
        raw.close().await;
        let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
            .execute(&admin)
            .await;
        admin.close().await;
    }
}

async fn scratch(test: &str) -> Option<Scratch> {
    let Some(base) = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        assert!(
            std::env::var("EXTENDDB_TEST_PG_REQUIRED").map_or(true, |v| v.is_empty()),
            "{test}: EXTENDDB_TEST_PG_REQUIRED is set but EXTENDDB_TEST_PG_CONNECTION_STRING is not"
        );
        eprintln!("SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
        return None;
    };
    let base = base.trim_end_matches('/').to_owned();
    let name = format!("eddb_bstore_{}", uuid::Uuid::new_v4().simple())[..26].to_owned();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{base}/postgres"))
        .await
        .expect("connect to the maintenance database");
    sqlx::query(&format!("CREATE DATABASE \"{name}\""))
        .execute(&admin)
        .await
        .expect("create the scratch database");
    let url = format!("{base}/{name}");
    let raw = PgPoolOptions::new()
        .max_connections(3)
        .connect(&url)
        .await
        .expect("connect to the scratch database");
    // The shipped catalog and data migration files, in name order, on one
    // database that stands in for both.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for dir in ["migrations", "data_migrations"] {
        let mut files: Vec<_> = std::fs::read_dir(root.join(dir))
            .expect("read a migrations directory")
            .map(|entry| entry.expect("a directory entry").path())
            .filter(|p| p.extension().is_some_and(|e| e == "sql"))
            .collect();
        files.sort();
        for file in files {
            let sql = std::fs::read_to_string(&file).expect("read a migration file");
            sqlx::raw_sql(&sql)
                .execute(&raw)
                .await
                .unwrap_or_else(|e| panic!("apply {}: {e}", file.display()));
        }
    }
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: url,
            pool_size: 10,
            max_item_size_bytes: 400_000,
        },
        "us-east-1",
    )
    .await
    .expect("open an engine on the scratch database");
    Some(Scratch {
        engine,
        raw,
        admin,
        name,
    })
}

/// The database server's clock, in epoch milliseconds.
async fn db_now_ms(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint")
        .fetch_one(pool)
        .await
        .expect("read the database clock")
}

/// Insert an `IN_PROGRESS` operation with no input files and the given
/// lease, as the start transaction leaves one once a claim has leased it.
async fn insert_running(
    pool: &PgPool,
    operation_id: &str,
    lease_owner: Option<&str>,
    lease_until_ms: Option<i64>,
) {
    sqlx::query(
        "INSERT INTO bulk_operations (account_id, operation_id, table_id, table_name, \
             table_arn, status, request_fingerprint, input_config, output_config, input_files, \
             start_time_ms, usage, lease_owner, lease_until_ms) \
         VALUES ($1, $2, 'table-1', 't', 'arn:aws:dynamodb:us-east-1:111111111111:table/t', \
             'IN_PROGRESS', 'fp', '{}', '{}', '[]', 1000, '{}', $3, $4)",
    )
    .bind(ACCOUNT)
    .bind(operation_id)
    .bind(lease_owner)
    .bind(lease_until_ms)
    .execute(pool)
    .await
    .expect("insert a running operation");
}

async fn stored_lease(pool: &PgPool, operation_id: &str) -> (Option<String>, Option<i64>) {
    sqlx::query_as(
        "SELECT lease_owner, lease_until_ms FROM bulk_operations \
         WHERE account_id = $1 AND operation_id = $2",
    )
    .bind(ACCOUNT)
    .bind(operation_id)
    .fetch_one(pool)
    .await
    .expect("read the lease")
}

fn progress_with_one_record() -> Progress {
    Progress {
        cursor_file_index: 0,
        cursor_row_index: 2,
        processed_item_count: 2,
        failed_item_count: 1,
        processed_input_bytes: 20,
        report_record_count: 1,
        report_bytes: 10,
        usage: serde_json::json!({}),
        records: vec![ReportRecord {
            kind: ReportKind::Error,
            file_index: 0,
            row_index: 1,
            record: serde_json::json!({"itemIndex": 1}),
        }],
        finished_files: Vec::new(),
        abandoned_files: Vec::new(),
    }
}

/// A lease another process wrote is live until the database's clock
/// passes it: a process whose own clock runs two minutes ahead does not
/// take the operation over while the lease has a minute left.
#[tokio::test]
async fn a_claim_reads_lease_expiry_on_the_database_clock() {
    let Some(s) = scratch("a_claim_reads_lease_expiry_on_the_database_clock").await else {
        return;
    };
    let now = db_now_ms(&s.raw).await;
    insert_running(&s.raw, "op-live", Some("holder"), Some(now + 60_000)).await;

    set_clock_skew_ms(120_000);
    let claimed = s
        .engine
        .claim_runnable("ahead", Duration::from_secs(60), 1, &[], &[])
        .await;
    set_clock_skew_ms(0);
    let claimed = claimed.expect("claim");
    assert!(
        claimed.is_empty(),
        "a lease the database clock holds live was taken over by a process whose clock runs ahead"
    );
    assert_eq!(
        stored_lease(&s.raw, "op-live").await.0.as_deref(),
        Some("holder")
    );
    s.cleanup().await;
}

/// A lease granted by a process whose clock runs two minutes behind lasts
/// its full length on the database's clock, at the claim and at every
/// checkpoint, so other processes do not find it already run out.
#[tokio::test]
async fn a_lease_granted_by_a_slow_clock_runs_its_full_length() {
    let Some(s) = scratch("a_lease_granted_by_a_slow_clock_runs_its_full_length").await else {
        return;
    };
    insert_running(&s.raw, "op-new", None, None).await;

    set_clock_skew_ms(-120_000);
    let claimed = s
        .engine
        .claim_runnable("behind", Duration::from_secs(60), 1, &[], &[])
        .await;
    set_clock_skew_ms(0);
    assert_eq!(claimed.expect("claim").len(), 1, "the operation is claimed");
    let (_, until) = stored_lease(&s.raw, "op-new").await;
    let now = db_now_ms(&s.raw).await;
    let until = until.expect("the claim set a lease");
    assert!(
        until >= now + 55_000,
        "the claimed lease runs {} ms past the database clock",
        until - now
    );
    let taken = s
        .engine
        .claim_runnable("other", Duration::from_secs(60), 1, &[], &[])
        .await
        .expect("claim");
    assert!(taken.is_empty(), "the fresh lease is not taken over");

    set_clock_skew_ms(-120_000);
    let state = s
        .engine
        .checkpoint(
            ACCOUNT,
            "op-new",
            "behind",
            Duration::from_secs(60),
            progress_with_one_record(),
        )
        .await;
    set_clock_skew_ms(0);
    assert!(matches!(state.expect("checkpoint"), LeaseState::Held));
    let (_, until) = stored_lease(&s.raw, "op-new").await;
    let now = db_now_ms(&s.raw).await;
    let until = until.expect("the checkpoint renewed the lease");
    assert!(
        until >= now + 55_000,
        "the renewed lease runs {} ms past the database clock",
        until - now
    );
    s.cleanup().await;
}

/// A process stopped inside a checkpoint transaction, after its update of
/// the operation row and before its commit, holds that row locked. A cancel
/// from another process answers `CANCELLING` within the bound the store
/// puts on an idle transaction, and once the stopped process's lease runs
/// out another process claims the operation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_process_stopped_inside_a_checkpoint_releases_the_operation_row() {
    let Some(s) = scratch("a_process_stopped_inside_a_checkpoint_releases_the_operation_row").await
    else {
        return;
    };
    insert_running(&s.raw, "op-stopped", None, None).await;
    let claimed = s
        .engine
        .claim_runnable("stopped", Duration::from_secs(2), 1, &[], &[])
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);

    // A transaction of the test's own holds the report record key the
    // checkpoint inserts, so the checkpoint stops at that insert with the
    // operation row already updated.
    let mut blocker = s.raw.begin().await.expect("begin the blocker");
    sqlx::query(
        "INSERT INTO bulk_operation_report_records \
             (account_id, operation_id, kind, file_index, row_index, record) \
         VALUES ($1, 'op-stopped', 'error', 0, 1, '{}')",
    )
    .bind(ACCOUNT)
    .execute(&mut *blocker)
    .await
    .expect("hold the record key");
    let mut checkpoint = Box::pin(s.engine.checkpoint(
        ACCOUNT,
        "op-stopped",
        "stopped",
        Duration::from_secs(2),
        progress_with_one_record(),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(500), checkpoint.as_mut())
            .await
            .is_err(),
        "the checkpoint waits at its record insert"
    );
    // The insert now completes on the server, and the checkpoint is never
    // polled again: its connection sits idle inside the transaction,
    // holding the operation row, as a process stopped there leaves it.
    blocker.rollback().await.expect("release the record key");
    tokio::time::sleep(Duration::from_millis(300)).await;

    let asked = Instant::now();
    let cancel = tokio::time::timeout(
        Duration::from_secs(40),
        s.engine.request_cancel(ACCOUNT, "op-stopped"),
    )
    .await
    .expect("the cancel answers while the stopped transaction is open")
    .expect("cancel");
    let waited = asked.elapsed();
    assert!(
        matches!(cancel, CancelOutcome::Cancelling),
        "the cancel answers CANCELLING"
    );
    assert!(
        waited < Duration::from_secs(25),
        "the cancel waited {waited:?} for the stopped transaction"
    );
    let taken = s
        .engine
        .claim_runnable("taker", Duration::from_secs(60), 1, &[], &[])
        .await
        .expect("claim");
    assert_eq!(
        taken
            .iter()
            .map(|r| r.operation_id.as_str())
            .collect::<Vec<_>>(),
        vec!["op-stopped"],
        "the operation is claimed once the stopped process's lease has run out"
    );
    drop(checkpoint);
    s.cleanup().await;
}

/// A claim that finds nothing to claim takes no write lock: it answers
/// while another session holds `bulk_operations` locked against writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_claim_takes_no_write_lock() {
    let Some(s) = scratch("an_idle_claim_takes_no_write_lock").await else {
        return;
    };
    let now = db_now_ms(&s.raw).await;
    insert_running(&s.raw, "op-leased", Some("holder"), Some(now + 60_000)).await;
    let mut holder = s.raw.begin().await.expect("begin the holder");
    sqlx::query("LOCK TABLE bulk_operations IN SHARE MODE")
        .execute(&mut *holder)
        .await
        .expect("lock the table against writes");
    let claimed = tokio::time::timeout(
        Duration::from_secs(5),
        s.engine
            .claim_runnable("idle", Duration::from_secs(60), 1, &[], &[]),
    )
    .await
    .expect("an idle claim answers while the table is locked against writes")
    .expect("claim");
    assert!(claimed.is_empty());
    holder.rollback().await.expect("release the table");
    s.cleanup().await;
}
