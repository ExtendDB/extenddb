// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A `begin_with` future dropped after its `BEGIN IMMEDIATE` ran, while sqlx
//! checks that the transaction is open, leaves the connection inside the
//! transaction. The engine pool and the catalog pool roll it back when the
//! connection comes back (`roll_back_on_release`), so the next `begin_with`
//! works.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection, SqlitePool};
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

/// Poll `begin_with` on `pool` by hand and drop it while sqlx checks that
/// the transaction is open. Each poll is followed by the wake of the step it
/// waits on. Once the write lock is taken, its BEGIN IMMEDIATE has run and
/// the connection has answered. One more poll takes the answer and starts
/// the check of the open transaction, which waits for the connection
/// handle. The future is dropped there. A wake can also come before the
/// answer, and the check can then be done at that poll: the attempt rolls
/// its transaction back and runs again.
async fn drop_a_begin_inside_its_check(
    name: &str,
    pool: &SqlitePool,
    probe: &mut SqliteConnection,
) {
    for _ in 0..20 {
        let woken = Arc::new(Woken::default());
        let waker = Waker::from(Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);
        let mut begin = Box::pin(pool.begin_with("BEGIN IMMEDIATE"));
        loop {
            assert!(
                begin.as_mut().poll(&mut cx).is_pending(),
                "{name}: begin_with ended"
            );
            woken.wait().await;
            if !write_lock_free(probe).await {
                break;
            }
        }
        match begin.as_mut().poll(&mut cx) {
            Poll::Pending => {
                drop(begin);
                return;
            }
            Poll::Ready(tx) => tx.expect("begin_with").rollback().await.expect("rollback"),
        }
    }
    panic!("{name}: no begin_with was dropped while it waited for the connection handle");
}

#[tokio::test]
async fn a_begin_dropped_after_its_statement_ran_leaves_no_transaction_open() {
    let db = temp_db("release");
    let path = db.to_str().expect("utf-8 path");
    let engine = SqliteEngine::new(path, 2, "us-east-1", 400 * 1024)
        .await
        .expect("engine");
    let catalog = crate::catalog_pool(path).await.expect("catalog pool");
    let mut probe = SqliteConnectOptions::new()
        .filename(path)
        .busy_timeout(Duration::ZERO)
        .connect()
        .await
        .expect("probe connection");

    for (name, pool) in [("engine", &engine.pool), ("catalog", &catalog)] {
        drop_a_begin_inside_its_check(name, pool, &mut probe).await;
        let tx = tokio::time::timeout(Duration::from_secs(30), pool.begin_with("BEGIN IMMEDIATE"))
            .await
            .unwrap_or_else(|_| panic!("{name}: the next begin_with answers"))
            .unwrap_or_else(|e| panic!("{name}: the next begin_with opens a transaction: {e:?}"));
        tx.rollback().await.expect("rollback");
        assert!(
            write_lock_free(&mut probe).await,
            "{name}: no connection holds the write lock"
        );
    }
}
