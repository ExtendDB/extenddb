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

use sqlx::pool::PoolConnection;
use sqlx::{PgConnection, PgPool, Postgres};

/// One pooled connection inside the search transaction.
pub(crate) struct ScanConnection {
    conn: Option<PoolConnection<Postgres>>,
    backend_pid: Option<i32>,
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
            backend_pid: None,
            pool: pool.clone(),
        };
        sqlx::query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(scan.connection())
            .await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(scan.connection())
            .await?;
        scan.backend_pid = Some(pid);
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
        // Detached first, so this backend can serve no other request: the
        // cancel below then reaches only the statement this search started.
        drop(conn.detach());
        let (Some(pid), Ok(runtime)) = (self.backend_pid, tokio::runtime::Handle::try_current())
        else {
            return;
        };
        let pool = self.pool.clone();
        runtime.spawn(async move {
            // The backend may already have exited on the closed socket, and
            // then PostgreSQL answers false. Neither answer changes anything.
            let _ = sqlx::query("SELECT pg_cancel_backend($1)")
                .bind(pid)
                .execute(&pool)
                .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sqlx::postgres::PgPoolOptions;

    use super::ScanConnection;

    /// A pool on the server's maintenance database, or `None` to skip.
    async fn pool(max: u32) -> Option<sqlx::PgPool> {
        let base = std::env::var("EXTENDDB_TEST_PG_CONNECTION_STRING").unwrap_or_default();
        if base.trim().is_empty() {
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
}
