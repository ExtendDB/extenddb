// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

#![allow(dead_code)]

//! The insert gate and the lock waits that the create-race tests share
//! (`put_create_race.rs` and `create_only_put.rs`).
//!
//! A `BEFORE INSERT` trigger makes every insert into a data table pass an
//! advisory lock, the gate. A test session holds the gate, so a put's insert
//! parks there while the test commits a competing item. When the test opens
//! the gate, the insert goes on and loses to that item.
//!
//! Each function takes the pool of the test's scratch database. A session
//! outside the pool comes from `connect`, which uses the pool's own connection
//! options.

use std::time::Duration;

use sqlx::{Connection, PgConnection, PgPool};

/// The advisory lock key of the gate. Advisory locks belong to one database,
/// and each test has its own scratch database, so one key serves every test.
pub const GATE: i64 = 0x5075_7452;

/// The statement that lets the inserts of its transaction pass the gate
/// without waiting.
pub const BYPASS_GATE: &str = "SET LOCAL test.bypass_gate = 'on'";

/// Open a connection to the database of `db`, outside the pool. Returns it and
/// its backend pid.
pub async fn connect(db: &PgPool) -> (PgConnection, i32) {
    let mut conn = PgConnection::connect_with(&db.connect_options())
        .await
        .expect("connect to the scratch database");
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut conn)
        .await
        .expect("read the backend pid");
    (conn, pid)
}

/// Make every insert into `table` pass the gate first, unless its transaction
/// ran `BYPASS_GATE`. Passing takes the gate shared and releases it, so an
/// insert parks while a session holds the gate exclusively.
pub async fn install_gate(db: &PgPool, table: &str) {
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION pass_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
           IF current_setting('test.bypass_gate', true) = 'on' THEN RETURN NEW; END IF; \
           PERFORM pg_advisory_lock_shared({GATE}); \
           PERFORM pg_advisory_unlock_shared({GATE}); \
           RETURN NEW; \
         END $$; \
         CREATE TRIGGER pass_gate BEFORE INSERT ON {table} \
           FOR EACH ROW EXECUTE FUNCTION pass_gate();"
    ))
    .execute(db)
    .await
    .expect("install the gate trigger");
}

/// Open a session that holds the gate exclusively: the gate is closed. Returns
/// the session and its backend pid.
pub async fn close_gate(db: &PgPool) -> (PgConnection, i32) {
    let (mut gate, gate_pid) = connect(db).await;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(GATE)
        .execute(&mut gate)
        .await
        .expect("close the gate");
    (gate, gate_pid)
}

/// Open the gate, so that the parked insert goes on. With `rearm`, close it
/// again behind that insert, so the put's next insert parks too.
pub async fn open_gate(gate: &mut PgConnection, rearm: bool) {
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(GATE)
        .execute(&mut *gate)
        .await
        .expect("open the gate");
    if rearm {
        // Granted only after the parked insert has passed the gate.
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(GATE)
            .execute(&mut *gate)
            .await
            .expect("close the gate again");
    }
}

/// Release every advisory lock `gate` holds. A driver that stops early calls
/// this, so that a parked insert goes on and the put can report its result.
pub async fn open_gate_fully(gate: &mut PgConnection) {
    sqlx::query("SELECT pg_advisory_unlock_all()")
        .execute(gate)
        .await
        .expect("open the gate");
}

/// Wait until a backend waits for a lock of kind `event` (`transactionid`,
/// `advisory` or `relation`) that the backend `blocker` holds. Fails after
/// 5 s, so the caller can report what the put did instead.
pub async fn wait_until_blocked_by(db: &PgPool, blocker: i32, event: &str) -> Result<(), String> {
    if lock_wait(db, None, Some(blocker), event).await {
        return Ok(());
    }
    Err(format!(
        "no backend waited on the {event} lock of backend {blocker}"
    ))
}

/// Wait until the backend `pid` waits for a lock of kind `event`. Fails after
/// 5 s, like `wait_until_blocked_by`.
pub async fn wait_until_waiting(db: &PgPool, pid: i32, event: &str) -> Result<(), String> {
    if lock_wait(db, Some(pid), None, event).await {
        return Ok(());
    }
    Err(format!("backend {pid} did not wait on a {event} lock"))
}

/// Poll `pg_stat_activity` for up to 5 s for a wait on a lock of kind `event`:
/// by the backend `pid` when it is given, and on a lock that `blocker` holds
/// when that is given.
async fn lock_wait(db: &PgPool, pid: Option<i32>, blocker: Option<i32>, event: &str) -> bool {
    for _ in 0..500 {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE datname = current_database() AND wait_event_type = 'Lock' \
             AND wait_event = $3 \
             AND ($1::int IS NULL OR pid = $1) \
             AND ($2::int IS NULL OR $2 = ANY(pg_blocking_pids(pid)))",
        )
        .bind(pid)
        .bind(blocker)
        .bind(event)
        .fetch_one(db)
        .await
        .expect("read pg_stat_activity");
        if waiting > 0 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}
