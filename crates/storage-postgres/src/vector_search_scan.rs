// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The connection a filtered vector search reads through.
//!
//! The search runs several statements, and they have to agree on which rows
//! exist and in which order, so they share one `REPEATABLE READ READ ONLY`
//! transaction and therefore one snapshot. A read-only transaction at that
//! level never fails to serialize.
//!
//! A connection that leaves this type normally has committed. Every other
//! exit, an error or a dropped future, closes the connection instead of
//! returning it to the pool, so the pool never hands out a connection that is
//! inside this transaction or still running one of its statements. On a drop
//! the backend is also sent `pg_cancel_backend`, so a request the caller has
//! abandoned stops its statement in the server instead of running it to the
//! end on a connection nobody reads.
//!
//! The cancel names the backend by process id and by the start time of the
//! search transaction: it signals the backend only when `pg_stat_activity`
//! shows that backend in that transaction. A transaction-pooling proxy can
//! hand the backend to another client as soon as this one disconnects, and a
//! process id alone would then cancel that client's statement. The check
//! narrows that window and does not close it. `pg_stat_activity` is a copy
//! of the backend status taken once per transaction of the cancelling
//! session, and `pg_cancel_backend` signals the process id without looking
//! at its transaction, so a backend that ends the search transaction between
//! the copy and the signal, and starts another client's, still gets the
//! signal.

use sqlx::pool::PoolConnection;
use sqlx::{PgConnection, PgPool, Postgres};

/// One pooled connection inside the search transaction.
pub(crate) struct ScanConnection {
    conn: Option<PoolConnection<Postgres>>,
    /// The backend's process id and the transaction's start, in
    /// microseconds since the epoch.
    backend: Option<(i32, i64)>,
    pool: PgPool,
}

impl ScanConnection {
    /// Take a connection from `pool` and open the transaction on it.
    ///
    /// # Errors
    ///
    /// Returns the error of the acquire or of the opening statement. The
    /// connection is closed in that case.
    pub(crate) async fn begin(pool: &PgPool) -> Result<Self, sqlx::Error> {
        let conn = pool.acquire().await?;
        // Built before the first statement, so a drop while it runs is
        // handled like any other drop.
        let mut scan = Self {
            conn: Some(conn),
            backend: None,
            pool: pool.clone(),
        };
        sqlx::query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(scan.connection())
            .await?;
        // `now()` is the transaction's start, the value `pg_stat_activity`
        // reports as `xact_start` while the transaction runs.
        let backend: (i32, i64) = sqlx::query_as(XACT_OF_THIS_BACKEND)
            .fetch_one(scan.connection())
            .await?;
        scan.backend = Some(backend);
        Ok(scan)
    }

    /// The connection, for running a statement of the search.
    pub(crate) fn connection(&mut self) -> &mut PgConnection {
        // Only `commit` and `drop` take the connection, and both consume or
        // end `self`.
        self.conn
            .as_deref_mut()
            .expect("the connection is held until commit or drop")
    }

    /// Commit and return the connection to the pool.
    ///
    /// # Errors
    ///
    /// Returns the error of the commit. The connection is closed in that case.
    pub(crate) async fn commit(mut self) -> Result<(), sqlx::Error> {
        sqlx::query("COMMIT").execute(self.connection()).await?;
        // Released here, so `drop` finds nothing to close or cancel.
        drop(self.conn.take());
        Ok(())
    }
}

impl Drop for ScanConnection {
    fn drop(&mut self) {
        let Some(conn) = self.conn.take() else {
            return;
        };
        // Detached first, so the pool can lend this connection to no other
        // request.
        drop(conn.detach());
        let (Some(backend), Ok(runtime)) = (self.backend, tokio::runtime::Handle::try_current())
        else {
            return;
        };
        let pool = self.pool.clone();
        runtime.spawn(cancel_if_in_transaction(pool, backend));
    }
}

/// The backend's process id and its transaction's start.
const XACT_OF_THIS_BACKEND: &str =
    "SELECT pg_backend_pid(), (extract(epoch FROM now()) * 1000000)::bigint";

/// Cancel the statement of backend `pid` if that backend still runs the
/// transaction that started at `xact_start`. Returns whether a cancel was
/// sent.
///
/// No row matches when the status copy shows the backend exited, out of the
/// transaction, or in another client's transaction; then nothing is
/// signalled. A backend that changes transaction after the copy is
/// signalled all the same.
async fn cancel_if_in_transaction(pool: PgPool, (pid, xact_start): (i32, i64)) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT pg_cancel_backend(pid) FROM pg_stat_activity \
         WHERE pid = $1 AND (extract(epoch FROM xact_start) * 1000000)::bigint = $2",
    )
    .bind(pid)
    .bind(xact_start)
    .fetch_optional(&pool)
    .await
    .ok()
    .flatten()
    .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sqlx::postgres::PgPoolOptions;

    use super::{ScanConnection, XACT_OF_THIS_BACKEND, cancel_if_in_transaction};

    /// A pool on the server's maintenance database, or `None` to skip. With
    /// `EXTENDDB_TEST_PG_REQUIRED` set to any value but `0`, a job that must
    /// reach a server fails here instead.
    async fn pool(max: u32) -> Option<sqlx::PgPool> {
        let base = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").unwrap_or_default();
        if base.trim().is_empty() {
            let required =
                std::env::var("EXTENDDB_TEST_PG_REQUIRED").is_ok_and(|v| !v.is_empty() && v != "0");
            assert!(
                !required,
                "EXTENDDB_TEST_PG_CONNECTION_STRING is not set, and EXTENDDB_TEST_PG_REQUIRED \
                 says this job must reach a PostgreSQL server"
            );
            eprintln!("SKIP: EXTENDDB_TEST_PG_CONNECTION_STRING is not set");
            return None;
        }
        let url = format!("{}/postgres", base.trim().trim_end_matches('/'));
        Some(
            PgPoolOptions::new()
                .max_connections(max)
                .connect(&url)
                .await
                .expect("connect to the server"),
        )
    }

    /// Whether any backend still runs `query`, polled until `deadline`.
    async fn still_running(observer: &sqlx::PgPool, query: &str, deadline: Duration) -> bool {
        let start = std::time::Instant::now();
        loop {
            let running: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
                 WHERE state = 'active' AND query = $1)",
            )
            .bind(query)
            .fetch_one(observer)
            .await
            .expect("read pg_stat_activity");
            if !running || start.elapsed() > deadline {
                return running;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn dropping_a_scan_stops_its_statement_in_the_server() {
        let (Some(pool), Some(observer)) = (pool(2).await, pool(1).await) else {
            return;
        };
        // Unique text, so the observer finds this statement and no other.
        let query = format!("SELECT pg_sleep(30), '{}'", uuid::Uuid::new_v4().simple());
        let scan_query = query.clone();
        let search = async {
            let mut scan = ScanConnection::begin(&pool).await.expect("begin");
            sqlx::query(&scan_query)
                .execute(scan.connection())
                .await
                .expect("the statement");
        };
        let dropped = tokio::time::timeout(Duration::from_millis(500), search).await;
        assert!(dropped.is_err(), "the statement finished before the drop");
        assert!(
            !still_running(&observer, &query, Duration::from_secs(5)).await,
            "the statement still runs 5 s after its scan was dropped"
        );
    }

    #[tokio::test]
    async fn a_dropped_scan_never_returns_its_transaction_to_the_pool() {
        let Some(pool) = pool(1).await else {
            return;
        };
        let mut scan = ScanConnection::begin(&pool).await.expect("begin");
        sqlx::query("SELECT 1")
            .execute(scan.connection())
            .await
            .expect("a statement");
        drop(scan);
        // The pool's one connection: a fresh one, or one that is no longer
        // inside the scan's transaction.
        let isolation: String =
            sqlx::query_scalar("SELECT current_setting('transaction_isolation')")
                .fetch_one(&pool)
                .await
                .expect("read the isolation level");
        assert_eq!(isolation, "read committed");
    }

    #[tokio::test]
    async fn a_committed_scan_returns_its_connection_outside_a_transaction() {
        let Some(pool) = pool(1).await else {
            return;
        };
        let mut scan = ScanConnection::begin(&pool).await.expect("begin");
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(scan.connection())
            .await
            .expect("the backend");
        scan.commit().await.expect("commit");
        let (again, isolation): (i32, String) =
            sqlx::query_as("SELECT pg_backend_pid(), current_setting('transaction_isolation')")
                .fetch_one(&pool)
                .await
                .expect("reuse the connection");
        assert_eq!(again, pid, "a committed scan keeps its connection pooled");
        assert_eq!(isolation, "read committed");
    }

    /// One connection that ends a transaction and starts another, as a
    /// transaction-pooling proxy hands a backend to its next client. Returns
    /// the first transaction's stamp, the second's, the connection running
    /// `pg_sleep(seconds)` in the second, and the pool to cancel through.
    async fn reused_backend(
        seconds: u32,
    ) -> Option<(
        (i32, i64),
        (i32, i64),
        tokio::task::JoinHandle<bool>,
        sqlx::PgPool,
    )> {
        let (Some(pool), Some(observer)) = (pool(1).await, pool(1).await) else {
            return None;
        };
        let mut conn = pool.acquire().await.expect("acquire");
        sqlx::query("BEGIN")
            .execute(&mut *conn)
            .await
            .expect("begin");
        let earlier: (i32, i64) = sqlx::query_as(XACT_OF_THIS_BACKEND)
            .fetch_one(&mut *conn)
            .await
            .expect("the first stamp");
        sqlx::query("COMMIT")
            .execute(&mut *conn)
            .await
            .expect("commit");
        tokio::time::sleep(Duration::from_millis(5)).await;
        sqlx::query("BEGIN")
            .execute(&mut *conn)
            .await
            .expect("begin again");
        let later: (i32, i64) = sqlx::query_as(XACT_OF_THIS_BACKEND)
            .fetch_one(&mut *conn)
            .await
            .expect("the second stamp");
        assert_eq!(earlier.0, later.0, "one backend serves both transactions");
        assert_ne!(earlier.1, later.1, "the transactions start apart");
        let query = format!(
            "SELECT pg_sleep({seconds}), '{}'",
            uuid::Uuid::new_v4().simple()
        );
        let statement = query.clone();
        let sleeper =
            tokio::spawn(async move { sqlx::query(&statement).execute(&mut *conn).await.is_ok() });
        let start = std::time::Instant::now();
        while !still_running(&observer, &query, Duration::ZERO).await {
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "the statement never started"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Some((earlier, later, sleeper, observer))
    }

    #[tokio::test]
    async fn a_cancel_spares_a_backend_that_serves_another_transaction() {
        let Some((earlier, _, sleeper, observer)) = reused_backend(2).await else {
            return;
        };
        assert!(
            !cancel_if_in_transaction(observer, earlier).await,
            "a cancel was sent for a transaction the backend no longer runs"
        );
        assert!(
            sleeper.await.expect("the statement task"),
            "the other transaction's statement was cancelled"
        );
    }

    #[tokio::test]
    async fn a_cancel_stops_the_statement_of_its_own_transaction() {
        // Long enough that the cancel reaches the statement while it runs on
        // a loaded host; a cancel that arrives between statements is dropped.
        let Some((_, later, sleeper, observer)) = reused_backend(30).await else {
            return;
        };
        let start = std::time::Instant::now();
        assert!(
            cancel_if_in_transaction(observer, later).await,
            "no cancel was sent for the running transaction"
        );
        assert!(
            !sleeper.await.expect("the statement task"),
            "the statement ran to the end"
        );
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "the statement stopped only after {:?}",
            start.elapsed()
        );
    }
}
