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
//!   exclusively, waiting for another migrator and then for running servers
//!   to stop, both within its one timeout.
//! - A server holds the serving key `(NAMESPACE, 2)` shared, then checks that
//!   no migrator holds the migration lock. Each side takes its own key before
//!   it looks for the other, so two that start together cannot both miss
//!   each other.
//!
//! Each of the two transactions turns off `idle_in_transaction_session_timeout`
//! for itself, and on PostgreSQL 17 and later `transaction_timeout`, so a
//! role or database setting of either does not end the session that holds
//! the key. The holder runs `SELECT 1` on the connection every 15 s, and the
//! runner checks the connection before each migration and each statement
//! outside a transaction. Each renewal waits at most 10 s for its answer, and
//! a holder whose last renewal succeeded more than 30 s ago counts its lock
//! as lost: a database that stops answering without closing the connection,
//! such as a paused or vanished host, is noticed within that time. The
//! serving session sets server-side TCP keepalives, which watch the
//! database's connection to its peer: connected directly, the database ends
//! the session, and frees the key, within 30 s of the serving host dying
//! instead of after the operating system's default of hours. Behind
//! PgBouncer that peer is PgBouncer, whose connection stays up when the
//! serving host dies, so the key stays until PgBouncer closes the dead
//! host's connection, which its own keepalive settings decide.

use std::sync::Arc;
use std::time::{Duration, Instant};

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

/// The `application_name` of a server's serving session. A command that
/// opens the catalog as a server does (`extenddb backup list`) names its
/// own session after itself, so a migration waiting for the registrations
/// names the command rather than a server.
pub const SERVE_APPLICATION: &str = "extenddb serve";

/// Server-side keepalives of the serving session: the first probe after 10 s
/// of silence, then one every 5 s, and the session ends after 4 unanswered.
const TCP_KEEPALIVES: [(&str, &str); 3] = [
    ("tcp_keepalives_idle", "10"),
    ("tcp_keepalives_interval", "5"),
    ("tcp_keepalives_count", "4"),
];

/// The longest wait a lock timeout can ask for that `Instant` can add.
const MAX_LOCK_WAIT: Duration = Duration::from_secs(86_400);

/// The statement that turns off `idle_in_transaction_session_timeout` for
/// the transaction that holds a key.
const IDLE_OFF: &str = "SET LOCAL idle_in_transaction_session_timeout = 0";

/// The statement that turns off `transaction_timeout` (PostgreSQL 17 and
/// later) for the transaction that holds a key.
const AGE_OFF: &str = "SET LOCAL transaction_timeout = 0";

/// What the transaction that holds a key sends after `BEGIN`, for a server
/// whose `server_version_num` is `version`. A role or database setting of
/// `idle_in_transaction_session_timeout` shorter than the renewal interval
/// would end the session between two renewals, and `transaction_timeout`,
/// which PostgreSQL 17 added, counts from the transaction's start, so no
/// renewal resets it.
fn timeouts_off(version: i32) -> &'static [&'static str] {
    if version >= 170_000 {
        &[IDLE_OFF, AGE_OFF]
    } else {
        &[IDLE_OFF]
    }
}

/// Send [`timeouts_off`] for this server in the transaction open on `conn`.
/// They are `SET LOCAL`s, so a transaction-mode pooler applies them to the
/// backend the key lives on, and they last as long as the key is held.
async fn keep_open(conn: &mut PgConnection) -> Result<(), sqlx::Error> {
    let version: String = sqlx::query_scalar("SELECT current_setting('server_version_num')")
        .fetch_one(&mut *conn)
        .await?;
    for sql in timeouts_off(version.trim().parse().unwrap_or(0)) {
        statement(conn, sql).await?;
    }
    Ok(())
}

/// What the runner keeps of the migration lock: a way to check that it is
/// still held, and the deadline its waits share.
#[derive(Clone)]
pub(crate) struct LockHandle {
    held: Arc<Held>,
    deadline: Instant,
}

impl LockHandle {
    pub(crate) async fn check_alive(&self) -> Result<(), MigrationError> {
        self.held.ping().await
    }

    /// The time left of the `--lock-timeout` the lock was taken with,
    /// counted from when it was asked for: the bound of the run's later
    /// waits, so every wait of a run together stays within the timeout.
    pub(crate) fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

/// `timeout` from now, or a day when the clock cannot add it.
fn deadline_after(timeout: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(timeout).unwrap_or(now + MAX_LOCK_WAIT)
}

/// Whole seconds for a message about a wait, rounded up, so a wait with
/// time left never reads as 0 s.
fn shown_secs(wait: Duration) -> u128 {
    wait.as_millis().div_ceil(1000)
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
    deadline: Instant,
}

impl MigrationLock {
    /// Take the migration lock, then the serving key exclusively, and keep
    /// the transaction alive every [`KEEPALIVE_INTERVAL`]. The waits for the
    /// two keys, and the runner's later wait for an earlier migrator's
    /// statements, share one deadline: `timeout` from this call. A key that
    /// is held when no time is left, as at a zero `timeout`, is not waited
    /// for.
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
        let deadline = deadline_after(timeout);
        let mut conn = connect(catalog, "extenddb migrate").await?;
        if let Err(e) = take_migration_keys(&mut conn, timeout, deadline).await {
            let _ = conn.close().await;
            return Err(e);
        }
        let held = Held::new(
            conn,
            "migration lock",
            "; nothing more is applied",
            keepalive,
        );
        let keepalive = spawn_keepalive(Arc::clone(&held), keepalive, None);
        Ok(Self {
            held,
            keepalive,
            deadline,
        })
    }

    pub(crate) fn handle(&self) -> LockHandle {
        LockHandle {
            held: Arc::clone(&self.held),
            deadline: self.deadline,
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

/// One of the two keys a migrator takes, as its messages name it.
struct Key {
    key: i32,
    /// What holds the key, in the error after the wait.
    held_by: &'static str,
    /// The line printed when the run starts to wait, given the holders and
    /// the seconds left.
    waiting: fn(&str, u128) -> String,
}

const MIGRATION: Key = Key {
    key: MIGRATION_KEY,
    held_by: "another migrator holds the migration lock",
    waiting: migrator_waiting,
};

const SERVING: Key = Key {
    key: SERVING_KEY,
    held_by: "servers are serving this catalog",
    waiting: servers_waiting,
};

fn migrator_waiting(names: &str, secs: u128) -> String {
    format!("--- Another migrator holds the migration lock ({names}); waiting up to {secs} s...")
}

fn servers_waiting(names: &str, secs: u128) -> String {
    format!("--- Waiting up to {secs} s for the servers serving this catalog to stop: {names}")
}

async fn take_migration_keys(
    conn: &mut PgConnection,
    timeout: Duration,
    deadline: Instant,
) -> Result<(), MigrationError> {
    let fail = |e: sqlx::Error| MigrationError::store("take the migration lock", e);
    statement(conn, "BEGIN").await.map_err(fail)?;
    keep_open(conn).await.map_err(fail)?;
    take_key(conn, &MIGRATION, timeout, deadline).await?;
    take_key(conn, &SERVING, timeout, deadline).await?;
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

/// Take `key` exclusively in the lock's transaction: at once when it is
/// free, else by waiting for it until `deadline`, with `lock_timeout` set to
/// the time left. With no time left, as at a zero `timeout` or after an
/// earlier wait used the time up, a held key is `Locked` at once:
/// PostgreSQL reads a `lock_timeout` of 0 as no limit, and counts it in
/// whole milliseconds.
async fn take_key(
    conn: &mut PgConnection,
    key: &Key,
    timeout: Duration,
    deadline: Instant,
) -> Result<(), MigrationError> {
    let fail = |e: sqlx::Error| MigrationError::store("take the migration lock", e);
    if try_key(conn, "pg_try_advisory_xact_lock", key.key)
        .await
        .map_err(fail)?
    {
        return Ok(());
    }
    let left = deadline.saturating_duration_since(Instant::now());
    if left.as_millis() == 0 {
        return Err(still_held(conn, key, timeout).await);
    }
    let names = holders(conn, key.key).await.unwrap_or_default();
    println!("{}", (key.waiting)(&listed(&names), shown_secs(left)));
    let _: String = sqlx::query_scalar("SELECT set_config('lock_timeout', $1, true)")
        .bind(format!("{}ms", left.as_millis()))
        .fetch_one(&mut *conn)
        .await
        .map_err(fail)?;
    let waited = sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
        .bind(NAMESPACE)
        .bind(key.key)
        .execute(&mut *conn)
        .await;
    match waited {
        Ok(_) => Ok(()),
        Err(e) if crate::data::lock_not_available(&e) => Err(still_held(conn, key, timeout).await),
        Err(e) => {
            let _ = statement(conn, "ROLLBACK").await;
            Err(fail(e))
        }
    }
}

/// The error when `key` is still held at the deadline: the lock's
/// transaction is rolled back, and the holders are named.
async fn still_held(conn: &mut PgConnection, key: &Key, timeout: Duration) -> MigrationError {
    let _ = statement(conn, "ROLLBACK").await;
    let names = match holders(conn, key.key).await {
        Ok(names) => listed(&names),
        Err(e) => format!("(pg_locks could not be read: {e})"),
    };
    MigrationError::Locked(format!(
        "{}, still after {} s: {names}",
        key.held_by,
        timeout.as_secs()
    ))
}

/// What a server holds while it serves: the serving key, shared. A
/// migration cannot start while it is held. Once
/// [`ServingRegistration::lost`] reports the connection lost, the server
/// admits no new request until it holds a registration again
/// ([`super::ServerRegistration`]).
pub struct ServingRegistration {
    held: Arc<Held>,
    keepalive: JoinHandle<()>,
    lost: watch::Receiver<Option<String>>,
}

impl ServingRegistration {
    /// Register, with the session named `extenddb serve`
    /// ([`SERVE_APPLICATION`]). The key is taken with a try: a migration
    /// that holds or waits for it is never waited for.
    ///
    /// # Errors
    ///
    /// [`MigrationError::Locked`] (exit 6) when a migration is running, so a
    /// restart loop retries.
    pub async fn register(catalog: &PgConnectOptions) -> Result<Self, MigrationError> {
        Self::register_keepalive(catalog, KEEPALIVE_INTERVAL, SERVE_APPLICATION).await
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
        Self::register_keepalive(catalog, keepalive, SERVE_APPLICATION).await
    }

    /// Register with the session named `application`: `extenddb serve` for
    /// a server, the command's name for a command.
    pub(crate) async fn register_keepalive(
        catalog: &PgConnectOptions,
        keepalive: Duration,
        application: &str,
    ) -> Result<Self, MigrationError> {
        let mut conn = connect(catalog, application).await?;
        match register_on(&mut conn, application).await {
            Ok(()) => {}
            Err(e) => {
                let _ = statement(&mut conn, "ROLLBACK").await;
                let _ = conn.close().await;
                return Err(e);
            }
        }
        let held = Held::new(conn, "serving registration", "", keepalive);
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

async fn register_on(conn: &mut PgConnection, application: &str) -> Result<(), MigrationError> {
    // What the refusal tells a server's operator to do once the migration is
    // done; a command's refusal ends at the holders, and the command's
    // caller adds that it is retried once the migration has finished.
    let then = if application == SERVE_APPLICATION {
        "; start the server when it has finished"
    } else {
        ""
    };
    let fail = |e: sqlx::Error| MigrationError::store("register the server", e);
    statement(conn, "BEGIN").await.map_err(fail)?;
    // Inside the transaction that holds the key, so a transaction-mode pooler
    // applies them to the backend the key lives on; they last as long as the
    // transaction, which is the registration's life.
    keep_open(conn).await.map_err(fail)?;
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
            "a migration is running on this catalog ({}){then}",
            listed(&names)
        )));
    }
    let migrators = holders(conn, MIGRATION_KEY).await.map_err(fail)?;
    if !migrators.is_empty() {
        return Err(MigrationError::Locked(format!(
            "a migration holds the migration lock on this catalog ({}){then}",
            listed(&migrators)
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{AGE_OFF, IDLE_OFF, shown_secs, timeouts_off};
    use std::time::Duration;

    #[test]
    fn transaction_timeout_is_turned_off_from_postgresql_17() {
        assert_eq!(timeouts_off(160_010), [IDLE_OFF]);
        assert_eq!(timeouts_off(169_999), [IDLE_OFF]);
        assert_eq!(timeouts_off(170_000), [IDLE_OFF, AGE_OFF]);
        assert_eq!(timeouts_off(180_001), [IDLE_OFF, AGE_OFF]);
        // A version that does not parse gets the setting every server has.
        assert_eq!(timeouts_off(0), [IDLE_OFF]);
    }

    #[test]
    fn a_wait_with_time_left_never_reads_as_zero_seconds() {
        assert_eq!(shown_secs(Duration::from_millis(1)), 1);
        assert_eq!(shown_secs(Duration::from_millis(1000)), 1);
        assert_eq!(shown_secs(Duration::from_millis(1001)), 2);
        assert_eq!(shown_secs(Duration::ZERO), 0);
    }
}
