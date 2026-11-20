// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A `begin_with` future dropped after its `BEGIN IMMEDIATE` ran, while sqlx
//! checks that the transaction is open, leaves the connection inside the
//! transaction. The pool rolls it back when the connection comes back
//! (`roll_back_on_release`), so the next `begin_with` works.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Wake, Waker};
use std::time::{Duration, Instant};

use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use sqlx::{ConnectOptions, Executor};

use crate::migrations::tests::temp_db;
use crate::store::SqliteEngine;

/// A waker that records that it was woken.
#[derive(Default)]
struct Woken(AtomicBool);

impl Wake for Woken {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl Woken {
    /// Wait until the future polled with this waker is woken.
    async fn wait(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !self.0.swap(false, Ordering::SeqCst) {
            assert!(Instant::now() < deadline, "the polled future was not woken");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

/// Whether `probe` can take the write lock of the database file now. The
/// probe gives the lock back at once, and its busy timeout is zero, so a
/// lock that another connection holds answers SQLITE_BUSY at once.
async fn write_lock_free(probe: &mut SqliteConnection) -> bool {
    match probe.execute("BEGIN IMMEDIATE").await {
        Ok(_) => {
            probe.execute("ROLLBACK").await.expect("probe rollback");
            true
        }
        Err(sqlx::Error::Database(e))
            if e.code()
                .and_then(|c| c.parse::<i64>().ok())
                .is_some_and(|c| c & 0xff == 5) =>
        {
            false
        }
        Err(e) => panic!("probe BEGIN IMMEDIATE: {e}"),
    }
}

#[tokio::test]
async fn a_begin_dropped_after_its_statement_ran_leaves_no_transaction_open() {
    let db = temp_db("release");
    let path = db.to_str().expect("utf-8 path");
    let engine = SqliteEngine::new(path, 2, "us-east-1", 400 * 1024)
        .await
        .expect("engine");
    let mut probe = SqliteConnectOptions::new()
        .filename(path)
        .busy_timeout(Duration::ZERO)
        .connect()
        .await
        .expect("probe connection");

    // Poll `begin_with` by hand. Each poll is followed by the wake of the
    // step it waits on. Once the write lock is taken, its BEGIN IMMEDIATE has
    // run and the connection has answered. One more poll takes the answer
    // and starts the check of the open transaction, which waits for the
    // connection handle. The future is dropped there.
    let woken = Arc::new(Woken::default());
    let waker = Waker::from(Arc::clone(&woken));
    let mut cx = Context::from_waker(&waker);
    let mut begin = Box::pin(engine.pool.begin_with("BEGIN IMMEDIATE"));
    loop {
        assert!(
            begin.as_mut().poll(&mut cx).is_pending(),
            "begin_with ended"
        );
        woken.wait().await;
        if !write_lock_free(&mut probe).await {
            break;
        }
    }
    assert!(
        begin.as_mut().poll(&mut cx).is_pending(),
        "the check of the open transaction waits for the connection handle"
    );
    drop(begin);

    let tx = tokio::time::timeout(
        Duration::from_secs(30),
        engine.pool.begin_with("BEGIN IMMEDIATE"),
    )
    .await
    .expect("the next begin_with answers")
    .expect("the next begin_with opens a transaction");
    tx.rollback().await.expect("rollback");
    assert!(
        write_lock_free(&mut probe).await,
        "no connection holds the write lock"
    );
}
