// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The index helper for bodies that build indexes outside a transaction.
//!
//! `CREATE INDEX CONCURRENTLY` commits the index's catalog entry before it
//! builds, so a build that is cancelled, times out or loses its connection
//! leaves the index INVALID, and `IF NOT EXISTS` then skips it on every later
//! run. The helper reads `pg_index.indisvalid` first, drops an invalid index
//! with `DROP INDEX CONCURRENTLY`, builds it with `CREATE INDEX
//! CONCURRENTLY`, and checks it is valid afterwards; [`IndexHelper::finish`]
//! checks every index the body owns again, so the body's ledger row is
//! written only when all of them are valid. Before every statement it checks
//! that the migration lock is still held.
//!
//! A statement outside a transaction cannot take `SET LOCAL lock_timeout`,
//! and a session-level setting would reach whichever backend a
//! transaction-mode pooler picks. So while a statement runs, the helper
//! watches it from a second session of the runner on the same database,
//! which reaches that database's server as the statement's role: once it
//! has waited for a lock for as long as the transactional bodies' lock
//! timeout, it is cancelled, its index is dropped and built again, three
//! times, and then the body stops naming the sessions that hold the lock.

use std::time::Duration;

use extenddb_storage::migrations::MigrationError;
use sqlx::PgPool;

use super::PostgresStores;
use super::connect::sqlstate;

/// How long a statement waits for a lock before it is cancelled: the
/// transactional bodies' `lock_timeout`.
const LOCK_WAIT: Duration = Duration::from_secs(5);

/// How many times an index is built again after a cancelled wait.
const LOCK_RETRIES: u32 = 3;

/// How often a running statement is looked at.
const WATCH: Duration = Duration::from_millis(250);

/// SQLSTATE `query_canceled`.
const QUERY_CANCELED: &str = "57014";

/// What [`IndexHelper::ensure`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Built {
    /// The index existed and was valid.
    Existing,
    /// The index was missing, and was built.
    Created,
    /// The index was invalid, and was dropped and built again.
    Rebuilt,
}

/// One index's state in `pg_index`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Missing,
    Valid,
    Invalid,
}

/// How one statement ended.
enum Ran {
    Done,
    /// The helper cancelled it after it waited for a lock; the sessions that
    /// held the lock.
    LockWait(String),
}

/// Builds the indexes of one body in one database.
pub(crate) struct IndexHelper<'a> {
    stores: &'a PostgresStores,
    pool: &'a PgPool,
    /// Where the statements `pool` runs are watched and cancelled from.
    watch: &'a PgPool,
    owned: Vec<String>,
}

impl<'a> IndexHelper<'a> {
    pub(crate) fn new(stores: &'a PostgresStores, pool: &'a PgPool) -> Self {
        Self {
            stores,
            pool,
            watch: stores.watch_for(pool),
            owned: Vec::new(),
        }
    }

    async fn state(&self, index: &str) -> Result<State, MigrationError> {
        let valid: Option<bool> = sqlx::query_scalar(
            "SELECT i.indisvalid FROM pg_index i \
             WHERE i.indexrelid = to_regclass($1)",
        )
        .bind(format!("public.\"{index}\""))
        .fetch_optional(self.pool)
        .await
        .map_err(|e| MigrationError::store(format!("read the state of index {index}"), e))?;
        Ok(match valid {
            None => State::Missing,
            Some(true) => State::Valid,
            Some(false) => State::Invalid,
        })
    }

    /// The backend running `sql` for this runner when it waits for a lock,
    /// and the sessions it waits for.
    async fn lock_wait(&self, sql: &str) -> Result<Option<(i32, String)>, MigrationError> {
        sqlx::query_as(
            "SELECT a.pid, coalesce(string_agg(format('pid %s (%s, %s)', b.pid, \
                    coalesce(nullif(b.application_name, ''), 'no application name'), \
                    coalesce(b.state, 'state not visible to this role')), \
                    ', ' ORDER BY b.pid), 'no session found') \
             FROM pg_stat_activity a \
             LEFT JOIN LATERAL unnest(pg_blocking_pids(a.pid)) AS blocker(pid) ON TRUE \
             LEFT JOIN pg_stat_activity b ON b.pid = blocker.pid \
             WHERE a.application_name = $1 AND a.query = $2 AND a.state = 'active' \
               AND a.wait_event_type = 'Lock' \
             GROUP BY a.pid",
        )
        .bind(&self.stores.application)
        .bind(sql)
        .fetch_optional(self.watch)
        .await
        .map_err(|e| MigrationError::store("read the index build's lock wait", e))
    }

    /// Run one statement, cancelling it once it has waited [`LOCK_WAIT`] for
    /// a lock; `attempt` counts from 1, for the line that says so.
    async fn run(&self, sql: &str, attempt: u32) -> Result<Ran, MigrationError> {
        self.stores.check_lock().await?;
        let statement = sqlx::query(sql).execute(self.pool);
        tokio::pin!(statement);
        let mut waited = Duration::ZERO;
        let mut announced = false;
        let mut cancelled: Option<String> = None;
        loop {
            tokio::select! {
                result = &mut statement => {
                    return match (result, cancelled) {
                        (Ok(_), _) => Ok(Ran::Done),
                        (Err(e), Some(holders))
                            if sqlstate(&e).as_deref() == Some(QUERY_CANCELED) =>
                        {
                            Ok(Ran::LockWait(holders))
                        }
                        (Err(e), _) => Err(MigrationError::store(sql, e)),
                    };
                }
                () = tokio::time::sleep(WATCH), if cancelled.is_none() => {
                    let Some((pid, holders)) = self.lock_wait(sql).await? else {
                        waited = Duration::ZERO;
                        continue;
                    };
                    waited += WATCH;
                    if !announced && waited >= Duration::from_secs(1) {
                        println!(
                            "--- {sql} waits for a lock held by {holders}; attempt {attempt} of \
                             {} is cancelled once it has waited {} s",
                            LOCK_RETRIES + 1,
                            LOCK_WAIT.as_secs()
                        );
                        announced = true;
                    }
                    if waited >= LOCK_WAIT {
                        sqlx::query("SELECT pg_cancel_backend($1)")
                            .bind(pid)
                            .execute(self.watch)
                            .await
                            .map_err(|e| MigrationError::store("cancel the index build", e))?;
                        cancelled = Some(holders);
                    }
                }
            }
        }
    }

    /// Make `index` exist and be valid: `on` is the rest of the statement
    /// after the index name, such as `ON "_ddb_x" (base_pk, base_sk_s)`. The
    /// names are quoted by the caller's identifiers, which must be checked
    /// before they reach here.
    pub(crate) async fn ensure(&mut self, index: &str, on: &str) -> Result<Built, MigrationError> {
        self.owned.push(index.to_owned());
        let first = self.state(index).await?;
        if first == State::Valid {
            return Ok(Built::Existing);
        }
        let drop = format!("DROP INDEX CONCURRENTLY IF EXISTS \"{index}\"");
        let create = format!("CREATE INDEX CONCURRENTLY IF NOT EXISTS \"{index}\" {on}");
        let mut state = first;
        let mut holders = String::new();
        for attempt in 1..=LOCK_RETRIES + 1 {
            if attempt > 1 {
                state = self.state(index).await?;
            }
            let ran = match state {
                State::Valid => Ran::Done,
                State::Invalid => match self.run(&drop, attempt).await? {
                    Ran::Done => self.run(&create, attempt).await?,
                    waited @ Ran::LockWait(_) => waited,
                },
                State::Missing => self.run(&create, attempt).await?,
            };
            match ran {
                Ran::Done if self.state(index).await? == State::Valid => {
                    return Ok(if first == State::Invalid {
                        Built::Rebuilt
                    } else {
                        Built::Created
                    });
                }
                Ran::Done => {
                    return Err(MigrationError::Store(format!(
                        "index {index} is not valid after its build; the migration is not \
                         recorded, and the next run drops and builds it again"
                    )));
                }
                Ran::LockWait(held_by) => holders = held_by,
            }
        }
        Err(MigrationError::Store(format!(
            "index {index} could not be built: each of {} attempts waited more than {} s for a \
             lock, held by {holders}; nothing of this migration is recorded in the data \
             database, so stop those sessions and run again",
            LOCK_RETRIES + 1,
            LOCK_WAIT.as_secs()
        )))
    }

    /// Check every index the body asked for is valid now.
    pub(crate) async fn finish(self) -> Result<(), MigrationError> {
        self.stores.check_lock().await?;
        let mut invalid = Vec::new();
        for index in &self.owned {
            if self.state(index).await? != State::Valid {
                invalid.push(index.clone());
            }
        }
        if invalid.is_empty() {
            Ok(())
        } else {
            Err(MigrationError::Store(format!(
                "indexes not valid at the end of the migration: {}; it is not recorded, and the \
                 next run builds them again",
                invalid.join(", ")
            )))
        }
    }
}
