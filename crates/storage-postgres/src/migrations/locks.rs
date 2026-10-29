// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The migration lock and the serving registration, both transaction-level
//! advisory locks in the catalog database, each held by an explicit
//! transaction on a dedicated connection that stays open while it is held.
//! A transaction-pooling proxy keeps one backend for an open transaction, so
//! the lock cannot move between backends.
//!
//! - The migration lock is the `(NAMESPACE, 1)` key releases since v0.1.3
//!   take, exclusive. A migrator takes it and then the serving key
//!   exclusively, waiting up to its timeout for running servers to stop.
//! - A server holds the serving key `(NAMESPACE, 2)` shared, then checks that
//!   no migrator holds the migration lock. Each side takes its own key before
//!   it looks for the other, so two that start together cannot both miss
//!   each other.
//!
//! The holder runs `SELECT 1` on the connection every 15 s, which resets
//! `idle_in_transaction_session_timeout`, and the runner checks the
//! connection before each migration and each statement outside a
//! transaction. Each renewal waits at most 10 s for its answer, and a holder
//! whose last renewal succeeded more than 30 s ago counts its lock as lost:
//! a database that stops answering without closing the connection, such as a
//! paused or vanished host, ends a server within that time. The serving
//! session sets server-side TCP keepalives, which watch the database's
//! connection to its peer: connected directly, the database ends the
//! session, and frees the key, within 30 s of the serving host dying instead
//! of after the operating system's default of hours. Behind PgBouncer that
//! peer is PgBouncer, whose connection stays up when the serving host dies,
//! so the key stays until PgBouncer closes the dead host's connection, which
//! its own keepalive settings decide.

use std::sync::Arc;
use std::time::Duration;

use extenddb_storage::migrations::MigrationError;
use sqlx::postgres::PgConnectOptions;
use sqlx::{Connection, PgConnection};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::held::{Held, connect, spawn_keepalive, statement};

/// ExtendDB's advisory-lock namespace, `classid` of every key.
pub(crate) const NAMESPACE: i32 = 0x0045_4442;
/// `objid` of the migration lock.
pub(crate) const MIGRATION_KEY: i32 = 1;
/// `objid` of the serving registration.
pub(crate) const SERVING_KEY: i32 = 2;

/// How often a holder runs `SELECT 1` on its lock connection.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// Server-side keepalives of the serving session: the first probe after 10 s
/// of silence, then one every 5 s, and the session ends after 4 unanswered.
const TCP_KEEPALIVES: [(&str, &str); 3] = [
    ("tcp_keepalives_idle", "10"),
    ("tcp_keepalives_interval", "5"),
    ("tcp_keepalives_count", "4"),
];

/// What the runner keeps of the migration lock: a way to check that it is
/// still held, and how long the lock was allowed to wait.
#[derive(Clone)]
pub(crate) struct LockHandle {
    held: Arc<Held>,
    timeout: Duration,
}

impl LockHandle {
    pub(crate) async fn check_alive(&self) -> Result<(), MigrationError> {
        self.held.ping().await
    }

    /// The `--lock-timeout` the lock was taken with, which bounds the other
    /// waits of the run.
    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }
}

async fn try_key(conn: &mut PgConnection, function: &str, key: i32) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(&format!("SELECT {function}($1, $2)"))
        .bind(NAMESPACE)
        .bind(key)
        .fetch_one(&mut *conn)
        .await
}

/// The sessions holding a key in this database (advisory keys are per
/// database), for messages: pid, application, client and connection time.
/// PostgreSQL shows a role the client and connection time of another role's
/// session only with `pg_read_all_stats`; such a session is named as one of
/// that role whose details this role cannot see.
async fn holders(conn: &mut PgConnection, key: i32) -> Result<Vec<String>, sqlx::Error> {
    type Row = (i32, Option<String>, String, Option<String>, Option<String>);
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT a.pid, a.usename::text, coalesce(a.application_name, ''), host(a.client_addr), \
         to_char(a.backend_start AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') \
         FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
         WHERE l.locktype = 'advisory' AND l.classid = $1 AND l.objid = $2 \
           AND l.objsubid = 2 AND l.granted AND l.pid <> pg_backend_pid() \
           AND l.database = (SELECT oid FROM pg_database WHERE datname = current_database()) \
         ORDER BY a.pid",
    )
    .bind(NAMESPACE)
    .bind(key)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(pid, role, app, client, since)| match since {
            Some(since) => format!(
                "pid {pid} ({app:?} from {}, connected {since})",
                client.unwrap_or_else(|| "a local socket".to_owned())
            ),
            None => format!(
                "pid {pid}, a session of {} ({app:?}) whose address and connection time this \
                 role cannot see",
                role.map_or_else(|| "another role".to_owned(), |r| format!("the role {r}"))
            ),
        })
        .collect())
}

fn listed(names: &[String]) -> String {
    if names.is_empty() {
        "no session holds it now".to_owned()
    } else {
        names.join(", ")
    }
}

/// The migration lock, held for a run. Dropping it without
/// [`MigrationLock::release`] closes the connection, which also frees it.
pub struct MigrationLock {
    held: Arc<Held>,
    keepalive: JoinHandle<()>,
    timeout: Duration,
}

impl MigrationLock {
    /// Take the migration lock, then the serving key exclusively, waiting up
    /// to `timeout` for each, and keep the transaction alive every
    /// [`KEEPALIVE_INTERVAL`].
    ///
    /// # Errors
    ///
    /// [`MigrationError::Locked`] (exit 6) naming the holders when another
    /// migrator or a running server outlasts `timeout`.
    pub async fn acquire(
        catalog: &PgConnectOptions,
        timeout: Duration,
    ) -> Result<Self, MigrationError> {
        Self::acquire_keepalive(catalog, timeout, KEEPALIVE_INTERVAL).await
    }

    /// [`MigrationLock::acquire`] with another keepalive interval, for tests.
    ///
    /// # Errors
    ///
    /// As [`MigrationLock::acquire`].
    #[cfg(feature = "test-util")]
    pub async fn acquire_with(
        catalog: &PgConnectOptions,
        timeout: Duration,
        keepalive: Duration,
    ) -> Result<Self, MigrationError> {
        Self::acquire_keepalive(catalog, timeout, keepalive).await
    }

    async fn acquire_keepalive(
        catalog: &PgConnectOptions,
        timeout: Duration,
        keepalive: Duration,
    ) -> Result<Self, MigrationError> {
        let mut conn = connect(catalog, "extenddb migrate").await?;
        if let Err(e) = take_migration_keys(&mut conn, timeout).await {
            let _ = conn.close().await;
            return Err(e);
        }
        let held = Held::new(conn, "migration lock", keepalive);
        let keepalive = spawn_keepalive(Arc::clone(&held), keepalive, None);
        Ok(Self {
            held,
            keepalive,
            timeout,
        })
    }

    pub(crate) fn handle(&self) -> LockHandle {
        LockHandle {
            held: Arc::clone(&self.held),
            timeout: self.timeout,
        }
    }

    /// Whether the lock's connection still answers.
    ///
    /// # Errors
    ///
    /// The connection was lost.
    pub async fn check_alive(&self) -> Result<(), MigrationError> {
        self.held.ping().await
    }

    /// Roll the lock's transaction back and close its connection.
    pub async fn release(self) {
        self.keepalive.abort();
        self.held.release().await;
    }
}

impl Drop for MigrationLock {
    fn drop(&mut self) {
        self.keepalive.abort();
    }
}

async fn take_migration_keys(
    conn: &mut PgConnection,
    timeout: Duration,
) -> Result<(), MigrationError> {
    let fail = |e: sqlx::Error| MigrationError::store("take the migration lock", e);
    statement(conn, "BEGIN").await.map_err(fail)?;
    let _: String = sqlx::query_scalar("SELECT set_config('lock_timeout', $1, true)")
        .bind(format!("{}ms", timeout.as_millis()))
        .fetch_one(&mut *conn)
        .await
        .map_err(fail)?;
    if !try_key(conn, "pg_try_advisory_xact_lock", MIGRATION_KEY)
        .await
        .map_err(fail)?
    {
        let names = holders(conn, MIGRATION_KEY).await.unwrap_or_default();
        println!(
            "--- Another migrator holds the migration lock ({}); waiting up to {} s...",
            listed(&names),
            timeout.as_secs()
        );
        if let Err(e) = try_key_waiting(conn, MIGRATION_KEY).await {
            return Err(timed_out(
                conn,
                e,
                MIGRATION_KEY,
                timeout,
                "another migrator holds the migration lock",
            )
            .await);
        }
    }
    if !try_key(conn, "pg_try_advisory_xact_lock", SERVING_KEY)
        .await
        .map_err(fail)?
    {
        let names = holders(conn, SERVING_KEY).await.unwrap_or_default();
        println!(
            "--- Waiting up to {} s for the servers serving this catalog to stop: {}",
            timeout.as_secs(),
            listed(&names)
        );
        if let Err(e) = try_key_waiting(conn, SERVING_KEY).await {
            return Err(timed_out(
                conn,
                e,
                SERVING_KEY,
                timeout,
                "servers are serving this catalog",
            )
            .await);
        }
    }
    let both: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND classid = $1 \
         AND objid IN ($2, $3) AND objsubid = 2 AND mode = 'ExclusiveLock' AND granted \
         AND pid = pg_backend_pid()",
    )
    .bind(NAMESPACE)
    .bind(MIGRATION_KEY)
    .bind(SERVING_KEY)
    .fetch_one(&mut *conn)
    .await
    .map_err(fail)?;
    if both != 2 {
        return Err(MigrationError::Store(
            "PostgreSQL did not report the migration lock's advisory keys after taking them; \
             refusing to migrate without them"
                .to_owned(),
        ));
    }
    Ok(())
}

async fn try_key_waiting(conn: &mut PgConnection, key: i32) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind(NAMESPACE)
        .bind(key)
        .execute(&mut *conn)
        .await
        .map(|_| ())
}

/// The error after a wait for `key` failed: the holders when it timed out.
async fn timed_out(
    conn: &mut PgConnection,
    e: sqlx::Error,
    key: i32,
    timeout: Duration,
    what: &str,
) -> MigrationError {
    let _ = statement(conn, "ROLLBACK").await;
    let timed_out = e
        .as_database_error()
        .and_then(|d| d.code())
        .is_some_and(|c| c == "55P03");
    if !timed_out {
        return MigrationError::store("take the migration lock", e);
    }
    let names = match holders(conn, key).await {
        Ok(names) => listed(&names),
        Err(e) => format!("(pg_locks could not be read: {e})"),
    };
    MigrationError::Locked(format!(
        "{what}, still after {} s: {names}",
        timeout.as_secs()
    ))
}

/// What a server holds while it serves: the serving key, shared. A
/// migration cannot start while it is held, and the server must stop serving
/// once [`ServingRegistration::lost`] reports the connection lost.
pub struct ServingRegistration {
    held: Arc<Held>,
    keepalive: JoinHandle<()>,
    lost: watch::Receiver<Option<String>>,
}

impl ServingRegistration {
    /// Register, with the session named `extenddb serve`.
    ///
    /// # Errors
    ///
    /// [`MigrationError::Locked`] (exit 6) when a migration is running, so a
    /// restart loop retries.
    pub async fn register(catalog: &PgConnectOptions) -> Result<Self, MigrationError> {
        Self::register_keepalive(catalog, KEEPALIVE_INTERVAL).await
    }

    /// [`ServingRegistration::register`] with another keepalive interval, for
    /// tests.
    ///
    /// # Errors
    ///
    /// As [`ServingRegistration::register`].
    #[cfg(feature = "test-util")]
    pub async fn register_with(
        catalog: &PgConnectOptions,
        keepalive: Duration,
    ) -> Result<Self, MigrationError> {
        Self::register_keepalive(catalog, keepalive).await
    }

    async fn register_keepalive(
        catalog: &PgConnectOptions,
        keepalive: Duration,
    ) -> Result<Self, MigrationError> {
        let mut conn = connect(catalog, "extenddb serve").await?;
        match register_on(&mut conn).await {
            Ok(()) => {}
            Err(e) => {
                let _ = statement(&mut conn, "ROLLBACK").await;
                let _ = conn.close().await;
                return Err(e);
            }
        }
        let held = Held::new(conn, "serving registration", keepalive);
        let (tx, lost) = watch::channel(None);
        let keepalive = spawn_keepalive(Arc::clone(&held), keepalive, Some(tx));
        Ok(Self {
            held,
            keepalive,
            lost,
        })
    }

    /// Changes to `Some(reason)` when the registration's connection is lost.
    #[must_use]
    pub fn lost(&self) -> watch::Receiver<Option<String>> {
        self.lost.clone()
    }

    /// Whether the registration's connection still answers.
    ///
    /// # Errors
    ///
    /// The connection was lost.
    pub async fn check_alive(&self) -> Result<(), MigrationError> {
        self.held.ping().await
    }

    /// The serving session's `application_name` and TCP keepalive settings,
    /// by name, as the database reports them, for diagnostics.
    ///
    /// # Errors
    ///
    /// The connection was lost or released.
    #[cfg(feature = "test-util")]
    pub async fn session_settings(&self) -> Result<Vec<(String, String)>, MigrationError> {
        let mut guard = self.held.conn.lock().await;
        let Some(conn) = guard.as_mut() else {
            return Err(MigrationError::Store(
                "the serving registration was released".to_owned(),
            ));
        };
        sqlx::query_as(
            "SELECT name, setting FROM pg_settings WHERE name IN ('application_name', \
             'tcp_keepalives_idle', 'tcp_keepalives_interval', 'tcp_keepalives_count') \
             ORDER BY name",
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| MigrationError::store("read the serving session's settings", e))
    }

    /// End the registration.
    pub async fn release(self) {
        self.keepalive.abort();
        self.held.release().await;
    }
}

impl Drop for ServingRegistration {
    fn drop(&mut self) {
        self.keepalive.abort();
    }
}

async fn register_on(conn: &mut PgConnection) -> Result<(), MigrationError> {
    let fail = |e: sqlx::Error| MigrationError::store("register the server", e);
    statement(conn, "BEGIN").await.map_err(fail)?;
    // Inside the transaction that holds the key, so a transaction-mode pooler
    // applies them to the backend the key lives on; they last as long as the
    // transaction, which is the registration's life.
    for (setting, value) in TCP_KEEPALIVES {
        statement(conn, &format!("SET LOCAL {setting} = {value}"))
            .await
            .map_err(fail)?;
    }
    if !try_key(conn, "pg_try_advisory_xact_lock_shared", SERVING_KEY)
        .await
        .map_err(fail)?
    {
        let names = holders(conn, SERVING_KEY).await.unwrap_or_default();
        return Err(MigrationError::Locked(format!(
            "a migration is running on this catalog ({}); start the server when it has finished",
            listed(&names)
        )));
    }
    let migrators = holders(conn, MIGRATION_KEY).await.map_err(fail)?;
    if !migrators.is_empty() {
        return Err(MigrationError::Locked(format!(
            "a migration holds the migration lock on this catalog ({}); start the server when \
             it has finished",
            listed(&migrators)
        )));
    }
    Ok(())
}
