// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! A connection whose open transaction holds an advisory key, and its
//! renewals: `SELECT 1` every keepalive interval, each waiting at most two
//! thirds of the interval for its answer, and the key counted as lost after
//! twice the interval with no successful renewal.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use extenddb_storage::migrations::MigrationError;
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, Connection, PgConnection};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

/// How long a lock or registration connection may take to open.
pub(super) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long one renewal waits for its answer: two thirds of the interval
/// (10 s for 15 s), and at least 1 s.
fn ping_timeout(interval: Duration) -> Duration {
    (interval * 2 / 3).max(Duration::from_secs(1))
}

/// How long a holder goes without a successful renewal before its lock
/// counts as lost: twice the interval (30 s for 15 s), and at least 2 s.
fn renewal_deadline(interval: Duration) -> Duration {
    (interval * 2).max(Duration::from_secs(2))
}

/// A connection whose open transaction holds a key.
pub(super) struct Held {
    pub(super) conn: Mutex<Option<PgConnection>>,
    lost: StdMutex<Option<String>>,
    pub(super) what: &'static str,
    pub(super) ping_timeout: Duration,
}

impl Held {
    pub(super) fn new(conn: PgConnection, what: &'static str, interval: Duration) -> Arc<Self> {
        Arc::new(Self {
            conn: Mutex::new(Some(conn)),
            lost: StdMutex::new(None),
            what,
            ping_timeout: ping_timeout(interval),
        })
    }

    fn lost_reason(&self) -> Option<String> {
        self.lost.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn mark_lost(&self, reason: String) -> MigrationError {
        let mut lost = self.lost.lock().unwrap_or_else(|e| e.into_inner());
        let reason = lost.get_or_insert(reason).clone();
        MigrationError::Store(reason)
    }

    /// `SELECT 1` on the connection, waiting at most `within`. A failed
    /// statement marks the connection lost, and every later call returns that
    /// reason; a statement with no answer in time returns
    /// [`Renewal::NoAnswer`] and marks nothing.
    async fn renew(&self, within: Duration) -> Renewal {
        if let Some(reason) = self.lost_reason() {
            return Renewal::Lost(MigrationError::Store(reason));
        }
        match tokio::time::timeout(within, select_one(&self.conn)).await {
            Ok(Ok(())) => Renewal::Renewed,
            Ok(Err(None)) => Renewal::Lost(MigrationError::Store(format!(
                "the {} was released",
                self.what
            ))),
            Ok(Err(Some(e))) => Renewal::Lost(self.mark_lost(format!(
                "the {}'s connection was lost ({e}), so another process may hold it now; \
                 nothing more is applied",
                self.what
            ))),
            Err(_) => Renewal::NoAnswer,
        }
    }

    /// One renewal for a caller about to act under the lock: no answer in
    /// time counts as lost, because the caller cannot tell it still holds
    /// the key.
    pub(super) async fn ping(&self) -> Result<(), MigrationError> {
        match self.renew(self.ping_timeout).await {
            Renewal::Renewed => Ok(()),
            Renewal::Lost(e) => Err(e),
            Renewal::NoAnswer => Err(self.mark_lost(format!(
                "the {}'s connection did not answer within {} s, so this process cannot tell \
                 that it still holds it; nothing more is applied",
                self.what,
                self.ping_timeout.as_secs_f32()
            ))),
        }
    }

    /// Roll back and close, waiting at most the ping timeout for each: a
    /// database that does not answer has the connection dropped instead.
    pub(super) async fn release(&self) {
        let conn = match tokio::time::timeout(self.ping_timeout, self.conn.lock()).await {
            Ok(mut guard) => guard.take(),
            Err(_) => None,
        };
        if let Some(conn) = conn {
            let _ = tokio::time::timeout(self.ping_timeout * 2, rollback_and_close(conn)).await;
        }
    }
}

/// `SELECT 1` on the held connection; `Err(None)` once it was released.
async fn select_one(conn: &Mutex<Option<PgConnection>>) -> Result<(), Option<sqlx::Error>> {
    let mut guard = conn.lock().await;
    let Some(conn) = guard.as_mut() else {
        return Err(None);
    };
    statement(conn, "SELECT 1").await.map_err(Some)
}

async fn rollback_and_close(mut conn: PgConnection) {
    if statement(&mut conn, "ROLLBACK").await.is_ok() {
        let _ = conn.close().await;
    }
}

/// The outcome of one renewal.
enum Renewal {
    Renewed,
    /// The statement failed, or the key was released.
    Lost(MigrationError),
    /// No answer within the ping timeout.
    NoAnswer,
}

/// Renew every `interval`. A renewal that fails, or no successful renewal
/// for the renewal deadline, ends the task and reports the reason on `lost`.
/// After a renewal with no answer the next one starts at once.
pub(super) fn spawn_keepalive(
    held: Arc<Held>,
    interval: Duration,
    lost: Option<watch::Sender<Option<String>>>,
) -> JoinHandle<()> {
    let deadline = renewal_deadline(interval);
    tokio::spawn(async move {
        let mut last_renewed = Instant::now();
        let mut wait = interval;
        loop {
            tokio::time::sleep(wait).await;
            let left = deadline.saturating_sub(last_renewed.elapsed());
            let within = held.ping_timeout.min(left).max(Duration::from_millis(1));
            let reason = match held.renew(within).await {
                Renewal::Renewed => {
                    last_renewed = Instant::now();
                    wait = interval;
                    continue;
                }
                Renewal::NoAnswer if last_renewed.elapsed() < deadline => {
                    wait = Duration::ZERO;
                    continue;
                }
                Renewal::NoAnswer => held
                    .mark_lost(format!(
                        "the {} could not be renewed for {} s: the database has not answered \
                         its connection since, so the database may have freed it",
                        held.what,
                        last_renewed.elapsed().as_secs()
                    ))
                    .to_string(),
                Renewal::Lost(e) => e.to_string(),
            };
            if let Some(lost) = &lost {
                let _ = lost.send(Some(reason));
            }
            return;
        }
    })
}

pub(super) async fn connect(
    options: &PgConnectOptions,
    app: &str,
) -> Result<PgConnection, MigrationError> {
    let what = "connect to the catalog database to take a lock";
    // The holder prints its own lines; sqlx's slow-statement warning for a
    // lock wait would print SQL with terminal colour codes.
    let options = options
        .clone()
        .application_name(app)
        .disable_statement_logging();
    match tokio::time::timeout(CONNECT_TIMEOUT, PgConnection::connect_with(&options)).await {
        Ok(Ok(conn)) => Ok(conn),
        Ok(Err(e)) => Err(MigrationError::store(what, e)),
        Err(_) => Err(MigrationError::Store(format!(
            "{what}: no answer within {} s",
            CONNECT_TIMEOUT.as_secs()
        ))),
    }
}

pub(super) async fn statement(conn: &mut PgConnection, sql: &str) -> Result<(), sqlx::Error> {
    sqlx::query(sql).execute(&mut *conn).await.map(|_| ())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{ping_timeout, renewal_deadline};

    /// At the production interval of 15 s a renewal waits 10 s for its
    /// answer, and the key counts as lost 30 s after the last renewal; the
    /// floors hold for the 1 s interval tests use.
    #[test]
    fn the_production_interval_gives_a_30_s_deadline() {
        let interval = super::super::KEEPALIVE_INTERVAL;
        assert_eq!(interval, Duration::from_secs(15));
        assert_eq!(ping_timeout(interval), Duration::from_secs(10));
        assert_eq!(renewal_deadline(interval), Duration::from_secs(30));
        assert_eq!(ping_timeout(Duration::from_secs(3)), Duration::from_secs(2));
        assert_eq!(
            renewal_deadline(Duration::from_secs(3)),
            Duration::from_secs(6)
        );
        assert_eq!(ping_timeout(Duration::from_secs(1)), Duration::from_secs(1));
        assert_eq!(
            renewal_deadline(Duration::from_secs(1)),
            Duration::from_secs(2)
        );
    }
}
