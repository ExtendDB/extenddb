// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The migration lock and the serving registration.
//!
//! The lock is the document `_id: "migration_lock"` in the catalog's
//! `settings` collection, which exists before the first migrator runs (the
//! claim's upsert creates it in an empty database). A claim is one
//! `findOneAndUpdate` with the filter on `_id` alone and the condition in a
//! pipeline update, because MongoDB refuses `$expr` in an upsert's filter:
//! it sets `owner` to the claimant's token and `expires_at` to the server's
//! `$$NOW` plus the lease, only when the document has no `expires_at` or one
//! that has passed. The claimant holds the lock when the returned `owner` is
//! its token. A renewal moves `expires_at` with `updateOne({_id, owner})`,
//! and a match count of 0 means the lock was lost; the release is
//! `deleteOne({_id, owner})`. Every write uses write concern majority and
//! every request waits at most [`REQUEST_TIMEOUT`](super::REQUEST_TIMEOUT).
//!
//! The holder keeps a deadline on the monotonic clock: the send time of its
//! last successful claim or renewal plus the lease minus a margin of 25 s,
//! which is longer than a request may take. The runner checks it before
//! every request, so a holder that was paused past it stops without
//! recording anything. The renewal task stops renewing when the holder has
//! made no progress for 30 s, so a hung holder loses the lock.
//!
//! A server registers with one document in `extenddb_servers` whose
//! `expires_at` is the server's `$$NOW` plus 45 s, renewed every 15 s, and
//! stops (exit 1) once it has not renewed for 30 s. `migrate --yes` takes
//! the lock and then waits for every registration whose `expires_at` has
//! not passed; a server registers and then reads the lock. Each takes its
//! own first and then looks for the other, so two that start together
//! cannot both miss each other.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bson::{Bson, Document, doc};
use extenddb_storage::migrations::MigrationError;
use mongodb::options::ReturnDocument;
use tokio::task::JoinHandle;

use super::registrations::live_registrations;
use super::{LOCK_ID, MongoTarget, REQUEST_TIMEOUT, SERVERS, connect, driver_error};

/// How long the lock, its renewals and the registration last.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    /// A claim or renewal sets `expires_at` this far after the server's
    /// clock.
    pub lease: Duration,
    /// How often the holder renews.
    pub renew_every: Duration,
    /// The holder stops acting this long before its lease ends.
    pub margin: Duration,
    /// The renewal task stops renewing when the holder has made no progress
    /// for this long.
    pub idle_limit: Duration,
    /// A registration's `expires_at` after its server's last renewal.
    pub registration: Duration,
    /// A server that has not renewed its registration for this long stops.
    pub registration_deadline: Duration,
    /// How often a claimant tries again while another process holds the
    /// lock, and `migrate --yes` reads the registrations again.
    pub retry_every: Duration,
}

impl Timing {
    /// The production timings: a 60 s lease renewed every 15 s with a 25 s
    /// margin, and 45 s registrations renewed every 15 s.
    pub const PRODUCTION: Self = Self {
        lease: Duration::from_secs(60),
        renew_every: Duration::from_secs(15),
        margin: Duration::from_secs(25),
        idle_limit: Duration::from_secs(30),
        registration: Duration::from_secs(45),
        registration_deadline: Duration::from_secs(30),
        retry_every: Duration::from_millis(500),
    };

    /// How long after a successful claim or renewal the holder may act.
    fn valid_for(self) -> Duration {
        self.lease.saturating_sub(self.margin)
    }
}

/// Milliseconds of a duration, for `$add` with `$$NOW`.
pub(super) fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// A random owner token, so a restarted process cannot inherit a claim.
pub(super) fn token() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// The holder's view of its lease: when it last renewed, when it last made
/// progress, and why it lost the lock.
pub(crate) struct Lease {
    renewed: Mutex<Instant>,
    progress: Mutex<Instant>,
    lost: Mutex<Option<String>>,
    timing: Timing,
}

impl Lease {
    pub(crate) fn new(renewed: Instant, timing: Timing) -> Arc<Self> {
        Arc::new(Self {
            renewed: Mutex::new(renewed),
            progress: Mutex::new(Instant::now()),
            lost: Mutex::new(None),
            timing,
        })
    }

    /// Whether the holder may still act: the lock was not lost, and its last
    /// successful renewal was sent less than the lease minus the margin ago.
    pub(crate) fn check(&self) -> Result<(), MigrationError> {
        if let Some(reason) = self.lost.lock().unwrap_or_else(|e| e.into_inner()).clone() {
            return Err(MigrationError::Store(reason));
        }
        let since = self
            .renewed
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .elapsed();
        if since >= self.timing.valid_for() {
            return Err(self.mark_lost(format!(
                "the migration lock was last renewed {} s ago, and this process acts on it only \
                 for {} s after a renewal; another process may hold it now, so nothing more is \
                 applied",
                since.as_secs(),
                self.timing.valid_for().as_secs()
            )));
        }
        Ok(())
    }

    /// Record that the holder made progress (a request completed).
    pub(crate) fn progressed(&self) {
        *self.progress.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.progress
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .elapsed()
    }

    fn renewed_at(&self, sent: Instant) {
        *self.renewed.lock().unwrap_or_else(|e| e.into_inner()) = sent;
    }

    fn mark_lost(&self, reason: String) -> MigrationError {
        let mut lost = self.lost.lock().unwrap_or_else(|e| e.into_inner());
        MigrationError::Store(lost.get_or_insert(reason).clone())
    }
}

/// The lock document's state, as a claim returns it.
fn owner_of(lock: Option<&Document>) -> Option<&str> {
    lock.and_then(|d| d.get_str("owner").ok())
}

/// Claim the lock for `token`: one `findOneAndUpdate` that takes a missing
/// or expired lock and leaves a live one as it is. Returns whether `token`
/// holds it, and the document after the update.
async fn claim(
    settings: &mongodb::Collection<Document>,
    token: &str,
    lease: Duration,
) -> Result<(bool, Option<Document>), MigrationError> {
    let pipeline = vec![
        doc! { "$set": { "free": { "$or": [
            { "$eq": [{ "$type": "$expires_at" }, "missing"] },
            { "$lt": ["$expires_at", "$$NOW"] },
        ] } } },
        doc! { "$set": {
            "owner": { "$cond": ["$free", token, "$owner"] },
            "expires_at": { "$cond": ["$free", { "$add": ["$$NOW", millis(lease)] }, "$expires_at"] },
        } },
        doc! { "$unset": "free" },
    ];
    let request = settings
        .find_one_and_update(doc! { "_id": LOCK_ID }, pipeline)
        .upsert(true)
        .return_document(ReturnDocument::After);
    match tokio::time::timeout(REQUEST_TIMEOUT, request).await {
        Ok(Ok(after)) => Ok((owner_of(after.as_ref()) == Some(token), after)),
        // Two claims upserting a missing document at once: the second one
        // fails on the `_id`, and claims again.
        Ok(Err(e)) if is_duplicate_key(&e) => Ok((false, None)),
        Ok(Err(e)) => Err(driver_error("take the migration lock", &e)),
        Err(_) => Err(timed_out("take the migration lock")),
    }
}

pub(crate) fn is_duplicate_key(error: &mongodb::error::Error) -> bool {
    use mongodb::error::{ErrorKind, WriteFailure};
    match error.kind.as_ref() {
        ErrorKind::Write(WriteFailure::WriteError(e)) => e.code == 11000,
        ErrorKind::Command(e) => e.code == 11000,
        _ => false,
    }
}

pub(super) fn timed_out(what: &str) -> MigrationError {
    MigrationError::Store(format!(
        "{what}: no answer within {} s",
        REQUEST_TIMEOUT.as_secs()
    ))
}

/// Who holds the lock, for a message.
pub(super) fn holder(lock: Option<&Document>) -> String {
    match lock {
        Some(d) => format!(
            "another migration (owner {}, expires {})",
            d.get_str("owner").unwrap_or("unknown"),
            d.get("expires_at")
                .map_or_else(|| "never".to_owned(), describe_time)
        ),
        None => "another migration".to_owned(),
    }
}

pub(super) fn describe_time(value: &Bson) -> String {
    match value {
        Bson::DateTime(t) => t.try_to_rfc3339_string().unwrap_or_else(|_| t.to_string()),
        other => other.to_string(),
    }
}

/// The migration lock, held for a run. Dropping it releases it; a process
/// that dies without releasing it leaves it to expire with its lease.
pub struct MigrationLock {
    target: MongoTarget,
    settings: mongodb::Collection<Document>,
    servers: mongodb::Collection<Document>,
    token: String,
    lease: Arc<Lease>,
    renewal: JoinHandle<()>,
    released: bool,
}

impl MigrationLock {
    /// Take the migration lock, waiting up to `timeout` while another
    /// process holds it, and renew it every [`Timing::renew_every`].
    ///
    /// # Errors
    ///
    /// [`MigrationError::Locked`] (exit 6) naming the holder when it
    /// outlasts `timeout`, or a request failed.
    pub async fn acquire(
        target: &MongoTarget,
        timeout: Duration,
        timing: Timing,
    ) -> Result<Self, MigrationError> {
        let client = connect(&target.connection_string).await?;
        let db = super::catalog_handle(&client, &target.catalog);
        let settings = db.collection::<Document>("settings");
        let token = token();
        let started = Instant::now();
        loop {
            let sent = Instant::now();
            let (held, lock) = claim(&settings, &token, timing.lease).await?;
            if held {
                let lease = Lease::new(sent, timing);
                let renewal =
                    spawn_renewal(settings.clone(), token.clone(), Arc::clone(&lease), timing);
                return Ok(Self {
                    target: target.clone(),
                    settings,
                    servers: db.collection::<Document>(SERVERS),
                    token,
                    lease,
                    renewal,
                    released: false,
                });
            }
            if started.elapsed() >= timeout {
                return Err(MigrationError::Locked(format!(
                    "{} held the migration lock for the whole {} s lock timeout; nothing was \
                     changed. Run the command again once it finishes, or with a longer \
                     --lock-timeout",
                    holder(lock.as_ref()),
                    timeout.as_secs()
                )));
            }
            tokio::time::sleep(timing.retry_every).await;
        }
    }

    /// The lease the runner checks before every request.
    pub(crate) fn lease(&self) -> Arc<Lease> {
        Arc::clone(&self.lease)
    }

    /// Whether this process may still act under the lock.
    ///
    /// # Errors
    ///
    /// The lock was lost, or its lease is past the margin.
    pub fn check(&self) -> Result<(), MigrationError> {
        self.lease.check()
    }

    /// Wait up to `timeout` for every running server's registration to end.
    ///
    /// # Errors
    ///
    /// [`MigrationError::Locked`] naming the servers when one outlasts
    /// `timeout`, or a request failed.
    pub async fn wait_for_servers(&self, timeout: Duration) -> Result<(), MigrationError> {
        let started = Instant::now();
        loop {
            self.check()?;
            let live = live_registrations(&self.servers).await?;
            self.lease.progressed();
            if live.is_empty() {
                return Ok(());
            }
            if started.elapsed() >= timeout {
                return Err(MigrationError::Locked(format!(
                    "a server is serving this catalog: {}; migrate --yes waited {} s for it to \
                     stop and changed nothing. Stop every server that uses the catalog, then run \
                     the command again",
                    live.join(", "),
                    timeout.as_secs()
                )));
            }
            tokio::time::sleep(self.lease.timing.retry_every).await;
        }
    }

    /// Delete the lock document if this process still owns it.
    pub async fn release(mut self) {
        self.renewal.abort();
        self.released = true;
        let _ = tokio::time::timeout(
            REQUEST_TIMEOUT,
            self.settings
                .delete_one(doc! { "_id": LOCK_ID, "owner": &self.token }),
        )
        .await;
    }

    /// Stop renewing and keep the document, as a process that was killed
    /// does: the lock then ends when its lease does.
    #[cfg(feature = "test-util")]
    pub fn abandon(mut self) {
        self.renewal.abort();
        self.released = true;
    }
}

impl Drop for MigrationLock {
    /// A lock the caller did not release is released here. The commands run
    /// on a single-threaded runtime that this call blocks, and the driver's
    /// connections are served by tasks on that runtime, so the release runs
    /// on a thread of its own with a client of its own.
    fn drop(&mut self) {
        self.renewal.abort();
        if self.released {
            return;
        }
        let target = self.target.clone();
        let token = self.token.clone();
        let released = std::thread::spawn(move || {
            let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            else {
                return;
            };
            runtime.block_on(async move {
                let Ok(client) = connect(&target.connection_string).await else {
                    return;
                };
                let settings = super::catalog_handle(&client, &target.catalog)
                    .collection::<Document>("settings");
                let _ = tokio::time::timeout(
                    REQUEST_TIMEOUT,
                    settings.delete_one(doc! { "_id": LOCK_ID, "owner": &token }),
                )
                .await;
            });
        });
        let _ = released.join();
    }
}

/// Renew the lock every `renew_every` until the lease is lost: a renewal
/// that matches no document, or no successful renewal before the holder's
/// deadline, or a holder with no progress for `idle_limit`. A renewal with
/// no answer is retried at once with the same token.
fn spawn_renewal(
    settings: mongodb::Collection<Document>,
    token: String,
    lease: Arc<Lease>,
    timing: Timing,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut wait = timing.renew_every;
        loop {
            tokio::time::sleep(wait).await;
            if lease.idle_for() >= timing.idle_limit {
                lease.mark_lost(format!(
                    "the migration made no progress for {} s, so this process stopped renewing \
                     the migration lock; nothing more is applied",
                    lease.idle_for().as_secs()
                ));
                return;
            }
            if lease.check().is_err() {
                return;
            }
            let sent = Instant::now();
            let renewal = settings.update_one(
                doc! { "_id": LOCK_ID, "owner": &token },
                vec![
                    doc! { "$set": { "expires_at": { "$add": ["$$NOW", millis(timing.lease)] } } },
                ],
            );
            match tokio::time::timeout(REQUEST_TIMEOUT, renewal).await {
                Ok(Ok(result)) if result.matched_count == 1 => {
                    lease.renewed_at(sent);
                    wait = timing.renew_every;
                }
                Ok(Ok(_)) => {
                    lease.mark_lost(
                        "the migration lock document no longer names this process, so another \
                         process may hold it; nothing more is applied"
                            .to_owned(),
                    );
                    return;
                }
                // No answer, or an error: try again at once until the
                // holder's deadline passes, which `check` then reports.
                Ok(Err(_)) | Err(_) => wait = timing.retry_every,
            }
        }
    })
}

#[cfg(test)]
pub(super) mod tests {
    use std::time::Duration;

    use super::Timing;

    /// The production timings are the ones ADR-0023's lock table and the
    /// upgrade manual give, which the Cassandra lock module carries under
    /// its own names: a 60 s lease renewed every 15 s and acted on until
    /// 25 s before it ends, renewal stopped after 30 s without progress,
    /// 45 s registrations renewed every 15 s, and a server that has not
    /// renewed for 30 s stops. A change here is a change to both documents.
    #[test]
    fn the_production_timings_are_the_documented_ones() {
        let timing = Timing::PRODUCTION;
        assert_eq!(timing.lease, Duration::from_secs(60));
        assert_eq!(timing.renew_every, Duration::from_secs(15));
        assert_eq!(timing.margin, Duration::from_secs(25));
        assert_eq!(timing.valid_for(), Duration::from_secs(35));
        assert_eq!(timing.idle_limit, Duration::from_secs(30));
        assert_eq!(timing.registration, Duration::from_secs(45));
        assert_eq!(timing.registration_deadline, Duration::from_secs(30));
    }

    /// A database that stopped answering: it accepts connections and never
    /// replies, so every request waits until its timeout.
    pub(crate) async fn silent_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        let accepting = tokio::spawn(async move {
            let mut open = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                open.push(socket);
            }
        });
        (address, accepting)
    }
}
