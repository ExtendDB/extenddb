// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The migration lock.
//!
//! The lock is the `migration_lock` row of the catalog's `settings` table,
//! written only by lightweight transactions at `SERIAL`: taken with
//! `INSERT ... IF NOT EXISTS USING TTL 60`, renewed every 15 s with an
//! `UPDATE` that sets the whole row (its one value column) to the holder's
//! token on the condition that it still holds it, and released with
//! `DELETE ... IF value = <token>`. The token is drawn when the lock is
//! created and never changes, so a renewal whose result is unknown after a
//! timeout is retried with the same condition. `SERIAL` runs Paxos across
//! every datacenter: with two datacenters and the link between them cut,
//! `LOCAL_SERIAL` let one holder in each take the lock.
//!
//! The holder keeps a deadline on a monotonic clock, the last successful
//! renewal plus the TTL minus a 25 s margin, and the runner checks it before
//! every statement and every ledger write ([`MigrationLock::check`]). Each
//! request is bounded by 10 s, shorter than the margin, so nothing the holder
//! sent before its deadline still runs when the row expires. The renewal
//! stops when the runner has made no progress for 30 s, so a hung holder
//! loses the lock instead of keeping it.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::types::rows::Row;
use extenddb_storage::migrations::MigrationError;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::agreement::{AGREEMENT_TIMEOUT, wait_for_agreement, wait_for_agreement_with};
use super::connect::{column, nullable, retryable, rows, send, table_exists};
use crate::cassandra_util::CassandraSession;

/// The `settings` key of the lock row.
pub(crate) const LOCK_KEY: &str = "migration_lock";

/// The times of the lock's lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseTimes {
    /// The row's TTL.
    pub ttl: Duration,
    /// How often the holder renews.
    pub renew_every: Duration,
    /// Subtracted from the TTL to give the holder's deadline.
    pub margin: Duration,
    /// How long the runner may make no progress before the renewal stops.
    pub stall: Duration,
}

impl LeaseTimes {
    /// 60 s TTL, renewed every 15 s, a 25 s margin and a 30 s stall.
    pub const DEFAULT: Self = Self {
        ttl: Duration::from_secs(60),
        renew_every: Duration::from_secs(15),
        margin: Duration::from_secs(25),
        stall: Duration::from_secs(30),
    };

    /// The holder's deadline after a renewal sent at `sent`.
    #[must_use]
    pub fn deadline_after(&self, sent: Instant) -> Instant {
        sent + self.ttl.saturating_sub(self.margin)
    }

    fn ttl_secs(&self) -> i32 {
        i32::try_from(self.ttl.as_secs().max(1)).unwrap_or(i32::MAX)
    }
}

/// What the holder and its renewal task share.
struct Lease {
    deadline: Mutex<Instant>,
    progress: Mutex<Instant>,
    lost: watch::Sender<bool>,
}

impl Lease {
    fn deadline(&self) -> Instant {
        *self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn set_deadline(&self, deadline: Instant) {
        *self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = deadline;
    }

    fn last_progress(&self) -> Instant {
        *self
            .progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn lost_error(keyspace: &str) -> MigrationError {
    MigrationError::Store(format!(
        "the migration lock in {keyspace}.settings was not renewed in time, so another migration \
         may hold it; this run stopped before its next write. Run 'extenddb migrate --yes' again"
    ))
}

/// A lock's lease as seen by code that runs under the lock without holding
/// it: the deadline check before a request, and the progress a completed
/// request records.
#[derive(Clone)]
pub(crate) struct LeaseHandle {
    lease: Arc<Lease>,
    keyspace: String,
}

impl LeaseHandle {
    /// Stop before a request when the holder's deadline has passed.
    ///
    /// # Errors
    ///
    /// The deadline has passed: the lease may have lapsed, and another
    /// migrator may hold the lock.
    pub(crate) fn check(&self) -> Result<(), MigrationError> {
        if Instant::now() >= self.lease.deadline() {
            return Err(lost_error(&self.keyspace));
        }
        Ok(())
    }

    /// Record progress, which keeps the renewal going.
    pub(crate) fn progress(&self) {
        *self
            .lease
            .progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Instant::now();
    }

    /// A handle on a lease whose deadline has already passed: for the tests
    /// of code that must refuse a request after it.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn lapsed(keyspace: &str) -> Self {
        let (lost, _) = watch::channel(true);
        Self {
            lease: Arc::new(Lease {
                deadline: Mutex::new(Instant::now()),
                progress: Mutex::new(Instant::now()),
                lost,
            }),
            keyspace: keyspace.to_owned(),
        }
    }
}

/// The catalog's migration lock, held until [`MigrationLock::release`] or
/// until it is dropped, after which its row expires within one TTL.
pub struct MigrationLock {
    session: Arc<CassandraSession>,
    keyspace: String,
    token: String,
    lease: Arc<Lease>,
    renewal: JoinHandle<()>,
}

impl std::fmt::Debug for MigrationLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MigrationLock")
            .field("keyspace", &self.keyspace)
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

/// Whether a lightweight transaction applied, and the lock value it found.
fn lwt_outcome(rows: &[Row], context: &str) -> Result<(bool, Option<String>), MigrationError> {
    let row = rows
        .first()
        .ok_or_else(|| MigrationError::Store(format!("{context}: no [applied] row")))?;
    let applied: bool = column(row, "[applied]", context)?;
    // A condition on a row that does not exist answers with `[applied]`
    // alone, without the row's columns.
    let value: Option<String> = if applied {
        None
    } else {
        nullable::<String>(row, "value", context).unwrap_or(None)
    };
    Ok((applied, value))
}

async fn lwt(
    session: &CassandraSession,
    cql: &str,
    values: cdrs_tokio::query::QueryValues,
    context: &str,
) -> Result<(bool, Option<String>), MigrationError> {
    let envelope = send(
        session,
        cql,
        values,
        Consistency::Quorum,
        Some(Consistency::Serial),
        context,
    )
    .await?;
    let rows = envelope
        .response_body()
        .map_err(|e| MigrationError::store(context, e))?
        .into_rows()
        .ok_or_else(|| MigrationError::Store(format!("{context}: the answer holds no rows")))?;
    lwt_outcome(&rows, context)
}

impl MigrationLock {
    /// Take the lock in `keyspace`'s `settings` table, retrying every second
    /// for up to `timeout` while another process holds it, then wait for
    /// schema agreement: a holder that took an expired lock may follow one
    /// whose statements have not reached every node.
    ///
    /// # Errors
    ///
    /// [`MigrationError::Locked`] (exit 6) when the lock is still held after
    /// `timeout`; a store error when Cassandra cannot reach a majority of the
    /// catalog's replicas at `SERIAL` for that long.
    pub async fn acquire(
        session: Arc<CassandraSession>,
        keyspace: &str,
        times: LeaseTimes,
        timeout: Duration,
    ) -> Result<Self, MigrationError> {
        let token = uuid::Uuid::new_v4().to_string();
        let started = Instant::now();
        let cql = format!(
            "INSERT INTO {keyspace}.settings (key, value) VALUES ('{LOCK_KEY}', ?) IF NOT EXISTS \
             USING TTL {}",
            times.ttl_secs()
        );
        loop {
            let sent = Instant::now();
            let outcome = lwt(
                &session,
                &cql,
                cdrs_tokio::query_values!(token.as_str()),
                "take the migration lock",
            )
            .await;
            match outcome {
                // A retry after an unknown result finds this lock's own token.
                Ok((true, _)) => return Self::start(session, keyspace, token, times, sent).await,
                Ok((false, Some(value))) if value == token => {
                    return Self::start(session, keyspace, token, times, sent).await;
                }
                Ok((false, holder)) => {
                    if started.elapsed() >= timeout {
                        return Err(MigrationError::Locked(format!(
                            "another migration holds the catalog's migration lock (the \
                             {LOCK_KEY} row of {keyspace}.settings, holder {}) after {} s; \
                             retry when it finishes, or after the row's {} s TTL if its \
                             process died",
                            holder.unwrap_or_else(|| "unknown".to_owned()),
                            timeout.as_secs(),
                            times.ttl.as_secs()
                        )));
                    }
                }
                Err(e) if !retryable(&e) => return Err(e),
                Err(e) => {
                    if started.elapsed() >= timeout {
                        return Err(MigrationError::Locked(format!(
                            "the catalog's migration lock could not be taken within {} s: {e}; \
                             a migration needs a majority of every datacenter's replicas of \
                             {keyspace}",
                            timeout.as_secs()
                        )));
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1).min(times.renew_every)).await;
        }
    }

    async fn start(
        session: Arc<CassandraSession>,
        keyspace: &str,
        token: String,
        times: LeaseTimes,
        sent: Instant,
    ) -> Result<Self, MigrationError> {
        let (lost, _) = watch::channel(false);
        let lease = Arc::new(Lease {
            deadline: Mutex::new(times.deadline_after(sent)),
            progress: Mutex::new(Instant::now()),
            lost,
        });
        let renewal = tokio::spawn(renew(
            session.clone(),
            keyspace.to_owned(),
            token.clone(),
            times,
            lease.clone(),
        ));
        let lock = Self {
            session,
            keyspace: keyspace.to_owned(),
            token,
            lease,
            renewal,
        };
        wait_for_agreement_with(&lock.session, AGREEMENT_TIMEOUT, || lock.progress()).await?;
        lock.progress();
        Ok(lock)
    }

    /// The keyspace whose `settings` table holds the lock.
    #[must_use]
    pub fn keyspace(&self) -> &str {
        &self.keyspace
    }

    /// The session the lock was taken on.
    #[must_use]
    pub fn session(&self) -> &Arc<CassandraSession> {
        &self.session
    }

    /// Stop before writing when the holder's deadline has passed.
    ///
    /// # Errors
    ///
    /// The deadline has passed: the lease may have lapsed, and another
    /// migrator may hold the lock.
    pub fn check(&self) -> Result<(), MigrationError> {
        self.handle().check()
    }

    fn lost_error(&self) -> MigrationError {
        lost_error(&self.keyspace)
    }

    /// A handle on this lock's lease for code that runs under the lock
    /// without holding it: the store a code body's requests go through.
    #[must_use]
    pub(crate) fn handle(&self) -> LeaseHandle {
        LeaseHandle {
            lease: Arc::clone(&self.lease),
            keyspace: self.keyspace.clone(),
        }
    }

    /// Record that the runner made progress, which keeps the renewal going.
    pub fn progress(&self) {
        self.handle().progress();
    }

    /// Resolves once the holder's deadline has passed.
    pub async fn expired(&self) {
        let mut lost = self.lease.lost.subscribe();
        // `wait_for` returns at once when the value already holds.
        let _ = lost.wait_for(|lost| *lost).await;
    }

    /// Run `work` while the lease holds: `work` is dropped, and the run
    /// stops with an error, when the holder's deadline passes first. Each
    /// time `work` is woken counts as progress, so a body that is waiting
    /// on Cassandra keeps the lock and one that hangs loses it.
    ///
    /// # Errors
    ///
    /// The deadline passed, or `work` failed.
    pub async fn guard<T>(
        &self,
        work: impl std::future::Future<Output = Result<T, MigrationError>>,
    ) -> Result<T, MigrationError> {
        self.check()?;
        let progressing = Progressing {
            inner: std::pin::pin!(work),
            lock: self,
        };
        tokio::select! {
            result = progressing => result,
            () = self.expired() => Err(self.lost_error()),
        }
    }

    /// Release the lock: wait for schema agreement, so the next holder
    /// starts on a schema every node has, then delete the row if this lock
    /// still holds it. A second call does nothing.
    pub async fn release(&self) {
        self.renewal.abort();
        if self.check().is_err() {
            return;
        }
        // Later calls and checks see the lease as ended.
        self.lease.set_deadline(Instant::now());
        if let Err(e) = wait_for_agreement(&self.session, AGREEMENT_TIMEOUT).await {
            tracing::warn!("schema agreement before releasing the migration lock: {e}");
        }
        let cql = format!(
            "DELETE FROM {}.settings WHERE key = '{LOCK_KEY}' IF value = ?",
            self.keyspace
        );
        if let Err(e) = lwt(
            &self.session,
            &cql,
            cdrs_tokio::query_values!(self.token.as_str()),
            "release the migration lock",
        )
        .await
        {
            tracing::warn!("{e}; the lock expires within its TTL");
        }
    }
}

impl Drop for MigrationLock {
    fn drop(&mut self) {
        self.renewal.abort();
    }
}

impl MigrationLock {
    /// Stop renewing and leave the row, as a holder whose process was
    /// killed does: the lease lapses within one TTL. For the lease tests.
    #[cfg(any(test, feature = "test-util"))]
    pub fn abandon(self) {
        self.renewal.abort();
    }
}

/// A future that marks progress on the lock each time it is polled.
struct Progressing<'a, F> {
    inner: std::pin::Pin<&'a mut F>,
    lock: &'a MigrationLock,
}

impl<F: std::future::Future> std::future::Future for Progressing<'_, F> {
    type Output = F::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.lock.progress();
        self.inner.as_mut().poll(cx)
    }
}

/// The renewal task: renew every `renew_every` while the runner makes
/// progress, and mark the lease lost once its deadline passes.
async fn renew(
    session: Arc<CassandraSession>,
    keyspace: String,
    token: String,
    times: LeaseTimes,
    lease: Arc<Lease>,
) {
    let cql = format!(
        "UPDATE {keyspace}.settings USING TTL {} SET value = ? WHERE key = '{LOCK_KEY}' IF value \
         = ?",
        times.ttl_secs()
    );
    let mut next = Instant::now() + times.renew_every;
    loop {
        tokio::time::sleep_until(next.min(lease.deadline())).await;
        let now = Instant::now();
        if now >= lease.deadline() {
            let _ = lease.lost.send(true);
            return;
        }
        if now < next {
            continue;
        }
        if now.duration_since(lease.last_progress()) >= times.stall {
            tracing::warn!(
                "the migration has made no progress for {} s; the migration lock is no longer \
                 renewed",
                times.stall.as_secs()
            );
            next = lease.deadline();
            continue;
        }
        let sent = Instant::now();
        let outcome = lwt(
            &session,
            &cql,
            cdrs_tokio::query_values!(token.as_str(), token.as_str()),
            "renew the migration lock",
        )
        .await;
        match outcome {
            Ok((true, _)) => {
                lease.set_deadline(times.deadline_after(sent));
                next = sent + times.renew_every;
            }
            Ok((false, holder)) => {
                tracing::error!(
                    "the migration lock was lost (it is held by {}); stopping",
                    holder.unwrap_or_else(|| "nobody".to_owned())
                );
                lease.set_deadline(Instant::now());
                let _ = lease.lost.send(true);
                return;
            }
            // Unknown result: retry soon with the same condition, which the
            // token makes safe.
            Err(e) => {
                tracing::warn!("{e}; retrying");
                next = Instant::now() + times.renew_every / 3;
            }
        }
    }
}

/// Whether a migration holds the lock, read at `SERIAL`; `None` when the
/// catalog has no `settings` table.
///
/// # Errors
///
/// The read fails.
pub(crate) async fn lock_holder(
    session: &CassandraSession,
    keyspace: &str,
) -> Result<Option<String>, MigrationError> {
    if !table_exists(session, keyspace, "settings").await? {
        return Ok(None);
    }
    let found = rows(
        session,
        &format!("SELECT value FROM {keyspace}.settings WHERE key = '{LOCK_KEY}'"),
        cdrs_tokio::query_values!(),
        Consistency::Serial,
        "read the migration lock",
    )
    .await?;
    found
        .first()
        .map(|row| nullable::<String>(row, "value", "read the migration lock"))
        .transpose()
        .map(Option::flatten)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::Instant;

    use super::LeaseTimes;

    #[test]
    fn the_deadline_is_the_ttl_minus_the_margin_after_the_renewal_was_sent() {
        let sent = Instant::now();
        let times = LeaseTimes::DEFAULT;
        assert_eq!(times.deadline_after(sent), sent + Duration::from_secs(35));
        // Each request's bound is shorter than the margin, so a statement
        // sent before the deadline is done before the row can expire.
        assert!(super::super::connect::REQUEST_TIMEOUT < times.margin);
        // The holder renews several times before its deadline.
        assert!(times.renew_every * 2 < times.ttl.saturating_sub(times.margin));
        assert_eq!(times.ttl_secs(), 60);
    }
}
