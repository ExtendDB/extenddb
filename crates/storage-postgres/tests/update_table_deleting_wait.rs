// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0
//! Storage-level tests for an `UpdateTable` that lands while the delete poller
//! holds the table's catalog row.
//!
//! The poller claims a `DELETING` row with `FOR UPDATE SKIP LOCKED` and holds
//! that row lock while its data drop waits out a 3 s `lock_timeout`. An
//! `UpdateTable` on that table is refused either way, and these tests pin how
//! long the refusal takes: `update_table` reads the status without a row lock
//! before it takes `FOR UPDATE`, so the refusal does not queue behind the
//! poller's attempt.
//!
//! The lock is held here by a second transaction running the poller's own claim
//! statement, so the wait under test is the one the poller produces rather than
//! an approximation of it. The hold releases itself after 3 s, the poller's own
//! `lock_timeout`, so a call that does queue behind the lock returns with the
//! wait it took instead of hanging.
//!
//! The last test pins the `READ COMMITTED` isolation `update_table` opens its
//! transaction with: on a database whose default is `REPEATABLE READ`, an
//! `UpdateTable` that waits for the row lock while another transaction updates
//! the row still applies.
//!
//! Each test builds its own throwaway database with the shared scratch fixture,
//! applies the shipped migrations to it, and drops it when it passes. A failing
//! test leaves its database behind on purpose, named `eddb_sdis_*`, so the state
//! that failed can be inspected.
//!
//! Requires `EXTENDDB_TEST_PG_CONNECTION_STRING`, a base URL with no database
//! component (for example `postgresql://postgres@127.0.0.1:5432`), pointing at a
//! server whose role may create and drop databases. Without it every test here
//! reports a skip and passes. With `EXTENDDB_TEST_PG_REQUIRED` set to any value
//! but `0`, the fixture's `base_conn` fails each test instead.

// The fixture re-exports the types the stream disable tests need; this file
// uses its scratch database and table input only.
#[path = "common/stream_disable_fixture.rs"]
#[allow(unused_imports)]
mod fixture;

use std::time::{Duration, Instant};

use extenddb_core::types::{DeleteTableInput, TableStatus, UpdateTableInput};
use extenddb_storage::TableEngine;
use extenddb_storage::error::StorageError;
use extenddb_storage_postgres::{PostgresConfig, PostgresEngine};
use fixture::{ACCOUNT, REGION, Scratch, base_conn, scratch, table_input};
use sqlx::{PgPool, Postgres, Transaction};

const TABLE: &str = "t_update_table_deleting_wait";

/// The poller's `lock_timeout` for the data drop, which is how long it can hold
/// the catalog row lock in one attempt. This is a copy of the `'3s'` literal in
/// `worker_store.rs` (`SET LOCAL lock_timeout`); if that changes, change this,
/// since the test discriminates only while the hold is long against the budget.
const POLLER_HOLD: Duration = Duration::from_secs(3);

/// What the refusal has to come back inside while the row lock is held. The
/// measured refusal is one round trip against a loopback server (0.9 to 1.4 ms
/// idle, 19 to 22 ms under a host load average of 27), so this bound is wide
/// for a loaded runner and still six times under `POLLER_HOLD`.
const REFUSAL_BUDGET: Duration = Duration::from_millis(500);

fn skip(test: &str) {
    eprintln!(
        "SKIP {test}: EXTENDDB_TEST_PG_CONNECTION_STRING is not set, so there is no PostgreSQL \
         to build a scratch catalog in."
    );
}

async fn set_delay(db: &PgPool, secs: f64) {
    sqlx::query(
        "INSERT INTO settings (key, value) VALUES ('control_plane_delay_seconds', $1) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(secs.to_string())
    .execute(db)
    .await
    .expect("set the control-plane delay");
}

/// Create `TABLE` as ACTIVE, keyed on `pk`.
async fn active_table(s: &Scratch) {
    s.engine
        .create_table(ACCOUNT, table_input(TABLE, false))
        .await
        .expect("create the table");
    let status = table_status(&s.db).await;
    assert_eq!(status.as_deref(), Some("ACTIVE"), "the table starts ACTIVE");
}

async fn table_status(db: &PgPool) -> Option<String> {
    sqlx::query_scalar("SELECT table_status FROM tables WHERE account_id = $1 AND table_name = $2")
        .bind(ACCOUNT)
        .bind(TABLE)
        .fetch_optional(db)
        .await
        .expect("read the table status")
}

/// Put the table into its `DELETING` window through `delete_table`, so the
/// status and `status_transition_at` are the ones the crate writes, then wait
/// for the scheduled transition time to pass so the poller's claim statement
/// matches the row.
async fn deleting_and_claimable(s: &Scratch) {
    set_delay(&s.db, 1.0).await;
    let desc = s
        .engine
        .delete_table(
            ACCOUNT,
            DeleteTableInput {
                table_name: TABLE.to_owned(),
            },
        )
        .await
        .expect("delete the table");
    assert_eq!(desc.table_status, TableStatus::Deleting);
    assert_eq!(
        table_status(&s.db).await.as_deref(),
        Some("DELETING"),
        "the row is in its DELETING window"
    );
    // The poller claims rows whose `status_transition_at` has passed, so the
    // hold below cannot be taken before then.
    for _ in 0..100 {
        let due: bool = sqlx::query_scalar(
            "SELECT status_transition_at <= NOW() FROM tables \
             WHERE account_id = $1 AND table_name = $2",
        )
        .bind(ACCOUNT)
        .bind(TABLE)
        .fetch_one(&s.db)
        .await
        .expect("read the scheduled transition time");
        if due {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the scheduled transition time never passed");
}

/// Hold the `DELETING` row exactly as the delete poller's pass does: the
/// poller's own claim statement, inside a transaction that stays open.
async fn poller_hold(db: &PgPool) -> Transaction<'static, Postgres> {
    let mut held = db.begin().await.expect("begin the poller's transaction");
    let claimed: Vec<(String, String, String)> = sqlx::query_as(
        r"SELECT table_name, table_arn, table_id FROM tables
           WHERE table_status = 'DELETING' AND status_transition_at <= NOW()
           FOR UPDATE SKIP LOCKED",
    )
    .fetch_all(&mut *held)
    .await
    .expect("claim the DELETING row");
    assert_eq!(
        claimed.len(),
        1,
        "the poller's claim statement takes the row lock"
    );
    held
}

fn update_input() -> UpdateTableInput {
    UpdateTableInput {
        table_name: TABLE.to_owned(),
        billing_mode: None,
        table_throughput_mode: None,
        provisioned_throughput: None,
        deletion_protection_enabled: Some(true),
        global_secondary_index_updates: None,
        attribute_definitions: None,
        stream_specification: None,
        table_class: None,
        on_demand_throughput: None,
        vector_index_updates: None,
        warm_throughput: None,
        sse_specification: None,
    }
}

/// An `UpdateTable` sent while the poller holds the `DELETING` row is refused
/// without waiting for the hold to end.
#[tokio::test]
async fn update_table_is_refused_without_waiting_for_the_poller() {
    if base_conn().is_none() {
        skip("update_table_is_refused_without_waiting_for_the_poller");
        return;
    }
    let s = scratch().await;
    active_table(&s).await;
    deleting_and_claimable(&s).await;
    let held = poller_hold(&s.db).await;

    // The poller's attempt ends when its data drop hits the 3 s lock_timeout,
    // and the row lock goes with it. Releasing the hold the same way means a
    // call that does queue behind the lock still returns, after the wait it
    // took, instead of hanging; the signal releases it as soon as the call is
    // back, so a refusal that does not wait does not pay the 3 s.
    let (release_now, release_signal) = tokio::sync::oneshot::channel::<()>();
    let releaser = tokio::spawn(async move {
        tokio::select! {
            () = tokio::time::sleep(POLLER_HOLD) => {}
            _ = release_signal => {}
        }
        held.rollback().await.expect("release the poller's hold");
    });

    let started = Instant::now();
    let result = tokio::time::timeout(
        POLLER_HOLD + Duration::from_secs(20),
        s.engine.update_table(ACCOUNT, update_input()),
    )
    .await;
    let elapsed = started.elapsed();
    let _ = release_now.send(());
    releaser.await.expect("the hold was released");

    let result = result.unwrap_or_else(|_| {
        panic!("update_table did not return within {elapsed:?}");
    });

    // The refusal is the one the same call gives with nothing holding the row:
    // the unlocked pre-read shortens the wait, it does not change the answer.
    // Both the comparison and the named variant are asserted: the comparison
    // alone holds for any variant the pre-read returns, since both calls go
    // through it. A later change that gives a DELETING table its own variant
    // updates the named assertion here.
    let unheld = s.engine.update_table(ACCOUNT, update_input()).await;
    assert!(
        matches!(result, Err(StorageError::TableNotActive(ref name)) if name == TABLE),
        "expected TableNotActive({TABLE:?}) while the table is DELETING, got {result:?}"
    );
    assert_eq!(
        format!("{result:?}"),
        format!("{unheld:?}"),
        "the refusal under the poller's hold differs from the refusal without it"
    );
    assert!(
        elapsed < REFUSAL_BUDGET,
        "the refusal took {elapsed:?}, over the {REFUSAL_BUDGET:?} budget; a refusal that \
         queues behind the poller's row lock waits out the poller's {POLLER_HOLD:?} hold"
    );
    assert_eq!(
        table_status(&s.db).await.as_deref(),
        Some("DELETING"),
        "a refused UpdateTable leaves the status alone"
    );
    eprintln!("refused in {elapsed:?} with the poller's hold held, answering {result:?}");
    s.cleanup().await;
}

/// The unlocked pre-read refuses nothing on an ACTIVE table: the same
/// `UpdateTable` applies.
#[tokio::test]
async fn update_table_still_applies_on_an_active_table() {
    if base_conn().is_none() {
        skip("update_table_still_applies_on_an_active_table");
        return;
    }
    let s = scratch().await;
    active_table(&s).await;

    let desc = s
        .engine
        .update_table(ACCOUNT, update_input())
        .await
        .expect("update the ACTIVE table");
    assert_eq!(desc.table_status, TableStatus::Active);
    let protected: bool = sqlx::query_scalar(
        "SELECT deletion_protection_enabled FROM tables \
         WHERE account_id = $1 AND table_name = $2",
    )
    .bind(ACCOUNT)
    .bind(TABLE)
    .fetch_one(&s.db)
    .await
    .expect("read deletion protection");
    assert!(protected, "the update was applied");
    assert_eq!(
        table_status(&s.db).await.as_deref(),
        Some("ACTIVE"),
        "the table is left ACTIVE"
    );
    s.cleanup().await;
}

/// An `UpdateTable` naming a table that does not exist is still a
/// `TableNotFound`: the pre-read leaves a missing row to the locked read.
#[tokio::test]
async fn update_table_on_a_missing_table_is_not_found() {
    if base_conn().is_none() {
        skip("update_table_on_a_missing_table_is_not_found");
        return;
    }
    let s = scratch().await;
    let result = s.engine.update_table(ACCOUNT, update_input()).await;
    match result {
        Err(StorageError::TableNotFound(name)) => assert_eq!(name, TABLE),
        other => panic!("expected TableNotFound for a table that was never created, got {other:?}"),
    }
    s.cleanup().await;
}

/// On a database whose default isolation is REPEATABLE READ, an `UpdateTable`
/// that waits for the row lock while another transaction updates the row and
/// commits still applies. The transaction is pinned to READ COMMITTED, so the
/// locked read returns the committed version; at the database's default the
/// locked read fails with 40001 and the request answers 500.
#[tokio::test]
async fn update_table_applies_after_a_concurrent_row_update_under_a_repeatable_read_default() {
    let Some(base) = base_conn() else {
        skip("update_table_applies_after_a_concurrent_row_update_under_a_repeatable_read_default");
        return;
    };
    let s = scratch().await;
    active_table(&s).await;
    sqlx::query(&format!(
        "ALTER DATABASE \"{}\" SET default_transaction_isolation = 'repeatable read'",
        s.db_name
    ))
    .execute(&s.admin)
    .await
    .expect("set the scratch database's default isolation");
    // A second engine, so every connection it opens starts under that default.
    let engine = PostgresEngine::new(
        &PostgresConfig {
            connection_string: format!("{base}/{}", s.db_name),
            pool_size: 10,
            max_item_size_bytes: 400_000,
        },
        REGION,
    )
    .await
    .expect("open an engine under the REPEATABLE READ default");

    let mut holder = s.db.begin().await.expect("begin the holding transaction");
    sqlx::query("SELECT 1 FROM tables WHERE account_id = $1 AND table_name = $2 FOR UPDATE")
        .bind(ACCOUNT)
        .bind(TABLE)
        .execute(&mut *holder)
        .await
        .expect("hold the ACTIVE row");
    let call = tokio::spawn(async move { engine.update_table(ACCOUNT, update_input()).await });
    let mut queued = false;
    for _ in 0..500 {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock'",
        )
        .fetch_one(&s.db)
        .await
        .expect("read pg_stat_activity");
        if waiting > 0 {
            queued = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(queued, "update_table never queued on the held row lock");
    sqlx::query(
        "UPDATE tables SET item_count = item_count + 1 WHERE account_id = $1 AND table_name = $2",
    )
    .bind(ACCOUNT)
    .bind(TABLE)
    .execute(&mut *holder)
    .await
    .expect("update the held row");
    holder
        .commit()
        .await
        .expect("commit the holding transaction");

    let result = call.await.expect("join the update_table call");
    let desc = result.unwrap_or_else(|e| {
        panic!("expected the update to apply after the concurrent update committed, got {e:?}")
    });
    assert_eq!(desc.table_status, TableStatus::Active);
    let protected: bool = sqlx::query_scalar(
        "SELECT deletion_protection_enabled FROM tables \
         WHERE account_id = $1 AND table_name = $2",
    )
    .bind(ACCOUNT)
    .bind(TABLE)
    .fetch_one(&s.db)
    .await
    .expect("read deletion protection");
    assert!(protected, "the update was applied");
    s.cleanup().await;
}
