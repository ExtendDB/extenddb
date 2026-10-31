// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The serving registration.
//!
//! A server registers in `extenddb_servers`: one row, written at `QUORUM`
//! with a TTL of 45 s and again every 15 s at `LOCAL_QUORUM`. A server that
//! cannot renew for 30 s stops (before its row expires), so `migrate --yes`,
//! which waits while a row exists, cannot run beside it.

use std::sync::Arc;
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
const REGISTRATION_TTL_SECS: i32 = 45;
const REGISTRATION_RENEW: Duration = Duration::from_secs(15);
const REGISTRATION_LOST_AFTER: Duration = Duration::from_secs(30);

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
        let server_id = uuid::Uuid::new_v4().to_string();
        let started_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let pid = i32::try_from(std::process::id()).unwrap_or(i32::MAX);
        let cql = format!(
            "INSERT INTO {keyspace}.extenddb_servers (server_id, host, pid, release, started_at) \
             VALUES (?, ?, ?, ?, ?) USING TTL {REGISTRATION_TTL_SECS}"
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
        let renewal = tokio::spawn(renew_registration(session.clone(), cql, values(), lost_tx));
        Ok(Self {
            session,
            keyspace: keyspace.to_owned(),
            server_id,
            lost,
            renewal,
        })
    }

    /// Changes to `Some(reason)` when the registration could not be renewed
    /// for 30 s.
    #[must_use]
    pub fn lost(&self) -> watch::Receiver<Option<String>> {
        self.lost.clone()
    }

    /// Stop renewing and delete the row, so a migration need not wait for
    /// its TTL.
    pub async fn release(&self) {
        self.renewal.abort();
        let cql = format!(
            "DELETE FROM {}.extenddb_servers WHERE server_id = ?",
            self.keyspace
        );
        if let Err(e) = send(
            &self.session,
            &cql,
            cdrs_tokio::query_values!(self.server_id.as_str()),
            Consistency::Quorum,
            None,
            "remove this server from extenddb_servers",
        )
        .await
        {
            tracing::warn!("{e}; the row expires within {REGISTRATION_TTL_SECS} s");
        }
    }
}

impl Drop for ServingRegistration {
    fn drop(&mut self) {
        self.renewal.abort();
    }
}

async fn renew_registration(
    session: Arc<CassandraSession>,
    cql: String,
    values: cdrs_tokio::query::QueryValues,
    lost: watch::Sender<Option<String>>,
) {
    let mut last_ok = Instant::now();
    let mut next = last_ok + REGISTRATION_RENEW;
    loop {
        tokio::time::sleep_until(next.min(last_ok + REGISTRATION_LOST_AFTER)).await;
        if last_ok.elapsed() >= REGISTRATION_LOST_AFTER {
            let _ = lost.send(Some(format!(
                "the serving registration was not renewed for {} s",
                REGISTRATION_LOST_AFTER.as_secs()
            )));
            return;
        }
        if Instant::now() < next {
            continue;
        }
        let sent = Instant::now();
        // LOCAL_QUORUM, so a running server stays up while a remote
        // datacenter is unreachable.
        match send(
            &session,
            &cql,
            values.clone(),
            Consistency::LocalQuorum,
            None,
            "renew the serving registration",
        )
        .await
        {
            Ok(_) => {
                last_ok = sent;
                next = sent + REGISTRATION_RENEW;
            }
            Err(e) => {
                tracing::warn!("{e}; retrying");
                next = Instant::now() + REGISTRATION_RENEW / 3;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[test]
    fn a_server_stops_before_its_row_expires() {
        assert!(
            super::REGISTRATION_LOST_AFTER
                < Duration::from_secs(super::REGISTRATION_TTL_SECS as u64)
        );
        assert!(super::REGISTRATION_RENEW < super::REGISTRATION_LOST_AFTER);
    }

    /// The times ADR-0023 (decision 5) and the module documentation give the
    /// lock and the registration: a 60 s lock row renewed every 15 s, acted
    /// on until 25 s before it expires and no longer renewed after 30 s
    /// without progress; a 45 s registration renewed every 15 s, whose server
    /// stops after 30 s without a renewal.
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
        assert_eq!(super::REGISTRATION_TTL_SECS, 45);
        assert_eq!(super::REGISTRATION_RENEW, Duration::from_secs(15));
        assert_eq!(super::REGISTRATION_LOST_AFTER, Duration::from_secs(30));
    }
}
