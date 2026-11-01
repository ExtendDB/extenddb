// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The serving registration.
//!
//! A server registers in `extenddb_servers`: one row, written at `QUORUM`
//! with a TTL of 45 s and again every 5 s at `LOCAL_QUORUM`. A renewal that
//! fails is tried again every second. The registration counts as lost only
//! when no renewal has succeeded for 30 s, after a last attempt at that
//! deadline, so the server stops admitting requests at least 10 s before its
//! row expires and `migrate --yes`, which waits while a row exists, can see
//! it gone. The server then registers again with a new row
//! ([`super::registrar`]).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use cdrs_tokio::consistency::Consistency;
use extenddb_storage::migrations::MigrationError;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::connect::{nullable, rows, send, table_exists};
use super::locks::MigrationLock;
use crate::cassandra_util::CassandraSession;

/// The times of a serving registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistrationTimes {
    /// The row's TTL, in seconds.
    pub ttl_secs: i32,
    /// How often the row is written again.
    pub renew_every: Duration,
    /// How soon a renewal that failed is tried again.
    pub retry_every: Duration,
    /// How long after the last successful renewal the registration counts
    /// as lost. Each renewal is bounded by `renew_every`, so the loss is
    /// reported by `lost_after + renew_every`, which must stay under the TTL.
    pub lost_after: Duration,
}

impl RegistrationTimes {
    /// A 45 s row renewed every 5 s, tried again every second after a
    /// failure, and lost after 30 s without a renewal.
    pub const DEFAULT: Self = Self {
        ttl_secs: 45,
        renew_every: Duration::from_secs(5),
        retry_every: Duration::from_secs(1),
        lost_after: Duration::from_secs(30),
    };
}

/// One running server, as `migrate --yes` names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Server {
    pub(crate) host: String,
    pub(crate) pid: i32,
    pub(crate) release: String,
    pub(crate) started_at: String,
}

impl std::fmt::Display for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "extenddb serve {} on {} (process {}, started {})",
            self.release, self.host, self.pid, self.started_at
        )
    }
}

/// The servers registered in `keyspace`, read at `ALL`: every replica of
/// the catalog must answer. A catalog without the table has none.
///
/// # Errors
///
/// A replica did not answer.
pub(crate) async fn registered_servers(
    session: &CassandraSession,
    keyspace: &str,
) -> Result<Vec<Server>, MigrationError> {
    if !table_exists(session, keyspace, "extenddb_servers").await? {
        return Ok(Vec::new());
    }
    let found = rows(
        session,
        &format!("SELECT host, pid, release, started_at FROM {keyspace}.extenddb_servers"),
        cdrs_tokio::query_values!(),
        Consistency::All,
        "read the running servers at consistency ALL",
    )
    .await?;
    found
        .iter()
        .map(|row| {
            Ok(Server {
                host: nullable::<String>(row, "host", "read a server")?.unwrap_or_default(),
                pid: nullable::<i32>(row, "pid", "read a server")?.unwrap_or_default(),
                release: nullable::<String>(row, "release", "read a server")?.unwrap_or_default(),
                started_at: nullable::<String>(row, "started_at", "read a server")?
                    .unwrap_or_default(),
            })
        })
        .collect()
}

/// Wait, while holding `lock`, for every registered server to go away, for
/// up to `timeout`.
///
/// # Errors
///
/// [`MigrationError::Locked`] (exit 6) naming the servers, or the error of
/// the read at `ALL`, when `timeout` passes first.
pub(crate) async fn wait_for_no_servers(
    lock: &MigrationLock,
    timeout: Duration,
) -> Result<(), MigrationError> {
    let started = Instant::now();
    loop {
        lock.check()?;
        let read = registered_servers(lock.session(), lock.keyspace()).await;
        lock.progress();
        let outcome = match read {
            Ok(servers) if servers.is_empty() => return Ok(()),
            Ok(servers) => format!(
                "{} server(s) serve this catalog: {}. Stop them, then run the migration again",
                servers.len(),
                servers
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
            Err(e) => format!(
                "the running servers could not be read at consistency ALL ({e}); a migration \
                 needs every replica of {} reachable. Nodes the driver reports down: {}",
                lock.keyspace(),
                down_nodes(lock.session())
            ),
        };
        if started.elapsed() >= timeout {
            return Err(MigrationError::Locked(format!(
                "after waiting {} s, {outcome}",
                timeout.as_secs()
            )));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// The nodes the driver reports down, or "none".
fn down_nodes(session: &CassandraSession) -> String {
    let metadata = session.cluster_metadata();
    let mut down: Vec<String> = metadata
        .nodes()
        .values()
        .filter(|n| n.state() == cdrs_tokio::cluster::topology::NodeState::Down)
        .map(|n| n.broadcast_rpc_address().to_string())
        .collect();
    down.sort();
    if down.is_empty() {
        "none".to_owned()
    } else {
        down.join(", ")
    }
}

/// A server's registration, renewed until it is released or lost.
pub struct ServingRegistration {
    session: Arc<CassandraSession>,
    keyspace: String,
    server_id: String,
    lost: watch::Receiver<Option<String>>,
    renewal: JoinHandle<()>,
    ttl_secs: i32,
    released: AtomicBool,
}

impl ServingRegistration {
    /// Write this server's row at `QUORUM`, so starting needs a majority of
    /// the catalog's replicas, as migrating does, and start renewing it.
    ///
    /// # Errors
    ///
    /// The write fails.
    pub async fn register(
        session: Arc<CassandraSession>,
        keyspace: &str,
        host: &str,
        release: &str,
    ) -> Result<Self, MigrationError> {
        Self::register_timed(session, keyspace, host, release, RegistrationTimes::DEFAULT).await
    }

    /// [`ServingRegistration::register`] with other times, for tests.
    ///
    /// # Errors
    ///
    /// As [`ServingRegistration::register`].
    pub async fn register_timed(
        session: Arc<CassandraSession>,
        keyspace: &str,
        host: &str,
        release: &str,
        times: RegistrationTimes,
    ) -> Result<Self, MigrationError> {
        let server_id = uuid::Uuid::new_v4().to_string();
        let started_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let pid = i32::try_from(std::process::id()).unwrap_or(i32::MAX);
        let cql = format!(
            "INSERT INTO {keyspace}.extenddb_servers (server_id, host, pid, release, started_at) \
             VALUES (?, ?, ?, ?, ?) USING TTL {}",
            times.ttl_secs
        );
        let values = || {
            cdrs_tokio::query_values!(server_id.as_str(), host, pid, release, started_at.as_str())
        };
        send(
            &session,
            &cql,
            values(),
            Consistency::Quorum,
            None,
            "register this server in extenddb_servers",
        )
        .await?;
        let (lost_tx, lost) = watch::channel(None);
        let renewal = tokio::spawn(renew_registration(
            session.clone(),
            cql,
            values(),
            times,
            lost_tx,
        ));
        Ok(Self {
            session,
            keyspace: keyspace.to_owned(),
            server_id,
            lost,
            renewal,
            ttl_secs: times.ttl_secs,
            released: AtomicBool::new(false),
        })
    }

    /// Changes to `Some(reason)` once no renewal has succeeded for
    /// [`RegistrationTimes::lost_after`].
    #[must_use]
    pub fn lost(&self) -> watch::Receiver<Option<String>> {
        self.lost.clone()
    }

    /// Stop renewing and delete the row, so a migration need not wait for
    /// its TTL.
    pub async fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.renewal.abort();
        if let Err(e) = delete_row(&self.session, &self.keyspace, &self.server_id).await {
            tracing::warn!("{e}; the row expires within {} s", self.ttl_secs);
        }
    }
}

/// A registration dropped without [`ServingRegistration::release`], as an
/// attempt to register again that the server drops at its bound, deletes
/// its row from a task of its own, so it leaves no registration behind.
impl Drop for ServingRegistration {
    fn drop(&mut self) {
        self.renewal.abort();
        if self.released.load(Ordering::Acquire) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let (session, keyspace, server_id) = (
            self.session.clone(),
            self.keyspace.clone(),
            self.server_id.clone(),
        );
        runtime.spawn(async move {
            if let Err(e) = delete_row(&session, &keyspace, &server_id).await {
                tracing::warn!("{e}; the row expires by its TTL");
            }
        });
    }
}

/// Delete a server's row at `QUORUM`.
async fn delete_row(
    session: &CassandraSession,
    keyspace: &str,
    server_id: &str,
) -> Result<(), MigrationError> {
    send(
        session,
        &format!("DELETE FROM {keyspace}.extenddb_servers WHERE server_id = ?"),
        cdrs_tokio::query_values!(server_id),
        Consistency::Quorum,
        None,
        "remove this server from extenddb_servers",
    )
    .await
    .map(drop)
}

/// Write the row again every `renew_every`, each write bounded by
/// `renew_every`; after a failure try again every `retry_every`. Report the
/// registration lost once the attempt at `lost_after` after the last
/// successful renewal fails too.
async fn renew_registration(
    session: Arc<CassandraSession>,
    cql: String,
    values: cdrs_tokio::query::QueryValues,
    times: RegistrationTimes,
    lost: watch::Sender<Option<String>>,
) {
    let mut last_ok = Instant::now();
    let mut next = last_ok + times.renew_every;
    loop {
        let deadline = last_ok + times.lost_after;
        tokio::time::sleep_until(next.min(deadline)).await;
        let sent = Instant::now();
        // LOCAL_QUORUM, so a running server stays up while a remote
        // datacenter is unreachable.
        let renewal = send(
            &session,
            &cql,
            values.clone(),
            Consistency::LocalQuorum,
            None,
            "renew the serving registration",
        );
        let why = match tokio::time::timeout(times.renew_every, renewal).await {
            Ok(Ok(_)) => {
                last_ok = sent;
                next = sent + times.renew_every;
                continue;
            }
            Ok(Err(e)) => e.to_string(),
            Err(_) => format!("no answer within {} s", times.renew_every.as_secs()),
        };
        if sent >= deadline {
            let _ = lost.send(Some(format!(
                "the serving registration was not renewed for {} s (the last attempt: {why})",
                sent.duration_since(last_ok).as_secs()
            )));
            return;
        }
        tracing::warn!("renewing the serving registration failed: {why}; retrying");
        next = Instant::now() + times.retry_every;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    /// The loss is reported by `lost_after + renew_every`, the last
    /// attempt's bound, before the row's TTL ends.
    #[test]
    fn a_server_stops_admitting_before_its_row_expires() {
        let times = super::RegistrationTimes::DEFAULT;
        let ttl = Duration::from_secs(u64::try_from(times.ttl_secs).expect("ttl"));
        assert!(times.lost_after + times.renew_every < ttl);
        assert!(times.retry_every < times.renew_every);
        assert!(times.renew_every < times.lost_after);
    }

    /// The times ADR-0023 (decision 5) and the module documentation give the
    /// lock and the registration: a 60 s lock row renewed every 15 s, acted
    /// on until 25 s before it expires and no longer renewed after 30 s
    /// without progress; a 45 s registration renewed every 5 s and every
    /// second after a failure, lost after 30 s without a renewal.
    #[test]
    fn the_lease_and_registration_times_are_adr_0023s() {
        use crate::migrations::LeaseTimes;

        assert_eq!(
            LeaseTimes::DEFAULT,
            LeaseTimes {
                ttl: Duration::from_secs(60),
                renew_every: Duration::from_secs(15),
                margin: Duration::from_secs(25),
                stall: Duration::from_secs(30),
            }
        );
        assert_eq!(
            super::RegistrationTimes::DEFAULT,
            super::RegistrationTimes {
                ttl_secs: 45,
                renew_every: Duration::from_secs(5),
                retry_every: Duration::from_secs(1),
                lost_after: Duration::from_secs(30),
            }
        );
    }
}
