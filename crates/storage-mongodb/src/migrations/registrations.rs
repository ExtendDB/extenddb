// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The serving registration: the document a server holds in
//! `extenddb_servers` while it serves, and the read of every live one that
//! `migrate --yes` waits on. The lock it is paired with is in `locks.rs`,
//! whose module doc describes both.

use std::time::Instant;

use bson::{Document, doc};
use extenddb_storage::migrations::MigrationError;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::locks::{Timing, describe_time, holder, millis, timed_out, token};
use super::{LOCK_ID, MongoTarget, REQUEST_TIMEOUT, SERVERS, connect, driver_error};

/// The registrations whose `expires_at` has not passed, by the server's
/// clock, each named for a message.
pub(crate) async fn live_registrations(
    servers: &mongodb::Collection<Document>,
) -> Result<Vec<String>, MigrationError> {
    use futures::TryStreamExt;
    let read = async {
        let cursor = servers
            .find(doc! { "$expr": { "$gt": ["$expires_at", "$$NOW"] } })
            .await?;
        cursor.try_collect::<Vec<Document>>().await
    };
    let docs = match tokio::time::timeout(REQUEST_TIMEOUT, read).await {
        Ok(Ok(docs)) => docs,
        Ok(Err(e)) => return Err(driver_error("read the serving registrations", &e)),
        Err(_) => return Err(timed_out("read the serving registrations")),
    };
    Ok(docs
        .iter()
        .map(|d| {
            format!(
                "pid {} on {} (release {}, registered {}, expires {})",
                d.get("pid")
                    .map_or_else(|| "unknown".to_owned(), ToString::to_string),
                d.get_str("host").unwrap_or("an unknown host"),
                d.get_str("release").unwrap_or("unknown"),
                d.get("registered_at")
                    .map_or_else(|| "unknown".to_owned(), describe_time),
                d.get("expires_at")
                    .map_or_else(|| "unknown".to_owned(), describe_time),
            )
        })
        .collect())
}

/// This host's name, for the registration.
fn host_name() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .or_else(|| std::env::var("HOSTNAME").ok())
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .unwrap_or_else(|| "an unknown host".to_owned())
}

/// What a server holds while it serves: its document in `extenddb_servers`,
/// renewed every [`Timing::renew_every`].
pub struct ServingRegistration {
    servers: mongodb::Collection<Document>,
    token: String,
    renewal: JoinHandle<()>,
    lost: watch::Receiver<Option<String>>,
}

impl ServingRegistration {
    /// Register as a server of `target`'s catalog, then read the migration
    /// lock: a migration that holds it makes this server refuse with exit 6.
    ///
    /// # Errors
    ///
    /// [`MigrationError::Locked`] while a migration holds the lock, or a
    /// request failed. The registration is deleted on every error.
    pub async fn register(
        target: &MongoTarget,
        release: &str,
        timing: Timing,
    ) -> Result<Self, MigrationError> {
        let client = connect(&target.connection_string).await?;
        let db = super::catalog_handle(&client, &target.catalog);
        let servers = db.collection::<Document>(SERVERS);
        let token = token();
        let insert = servers
            .update_one(
                doc! { "_id": &token },
                vec![doc! { "$set": {
                    "host": host_name(),
                    "pid": i64::from(std::process::id()),
                    "release": release,
                    "registered_at": "$$NOW",
                    "expires_at": { "$add": ["$$NOW", millis(timing.registration)] },
                } }],
            )
            .upsert(true);
        let sent = Instant::now();
        match tokio::time::timeout(REQUEST_TIMEOUT, insert).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(driver_error("register this server", &e)),
            Err(_) => return Err(timed_out("register this server")),
        }
        let (tx, lost) = watch::channel(None);
        let renewal = spawn_registration_renewal(servers.clone(), token.clone(), timing, sent, tx);
        let registration = Self {
            servers,
            token,
            renewal,
            lost,
        };
        // The lock holds while its `expires_at` has not passed by the
        // database server's clock, as the claim decides.
        let settings = db.collection::<Document>("settings");
        let live = settings
            .find_one(doc! { "_id": LOCK_ID, "$expr": { "$gt": ["$expires_at", "$$NOW"] } });
        let lock = match tokio::time::timeout(REQUEST_TIMEOUT, live).await {
            Ok(Ok(lock)) => lock,
            Ok(Err(e)) => {
                registration.release().await;
                return Err(driver_error("read the migration lock", &e));
            }
            Err(_) => {
                registration.release().await;
                return Err(timed_out("read the migration lock"));
            }
        };
        if let Some(lock) = lock {
            registration.release().await;
            return Err(MigrationError::Locked(format!(
                "{} holds the migration lock on this catalog, so the server does not start; \
                 start it again once the migration finishes",
                holder(Some(&lock))
            )));
        }
        Ok(registration)
    }

    /// Changes to `Some(reason)` once the registration is lost.
    #[must_use]
    pub fn lost(&self) -> watch::Receiver<Option<String>> {
        self.lost.clone()
    }

    /// Stop renewing and delete the registration.
    pub async fn release(self) {
        self.renewal.abort();
        let _ = tokio::time::timeout(
            REQUEST_TIMEOUT,
            self.servers.delete_one(doc! { "_id": &self.token }),
        )
        .await;
    }
}

impl Drop for ServingRegistration {
    fn drop(&mut self) {
        self.renewal.abort();
    }
}

/// Renew a registration every `renew_every`. A renewal that matches no
/// document, or no successful renewal for `registration_deadline`, reports
/// the registration lost on `lost` and ends. No wait and no renewal outlasts
/// the deadline, so the loss is reported `registration_deadline` after the
/// send time of the last successful renewal, not a request timeout later.
fn spawn_registration_renewal(
    servers: mongodb::Collection<Document>,
    token: String,
    timing: Timing,
    registered: Instant,
    lost: watch::Sender<Option<String>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut renewed = registered;
        let mut wait = timing.renew_every;
        let left = |renewed: Instant| {
            timing
                .registration_deadline
                .saturating_sub(renewed.elapsed())
        };
        loop {
            tokio::time::sleep(wait.min(left(renewed))).await;
            let sent = Instant::now();
            let renewal = servers.update_one(
                doc! { "_id": &token },
                vec![doc! { "$set": { "expires_at": { "$add": ["$$NOW", millis(timing.registration)] } } }],
            );
            let answer = tokio::time::timeout(REQUEST_TIMEOUT.min(left(renewed)), renewal).await;
            let reason = match answer {
                Ok(Ok(result)) if result.matched_count == 1 => {
                    renewed = sent;
                    wait = timing.renew_every;
                    continue;
                }
                Ok(Ok(_)) => {
                    "this server's registration in extenddb_servers was deleted".to_owned()
                }
                Ok(Err(_)) | Err(_) if renewed.elapsed() < timing.registration_deadline => {
                    wait = timing.retry_every;
                    continue;
                }
                Ok(Err(e)) => format!(
                    "this server could not renew its registration for {} s ({e})",
                    renewed.elapsed().as_secs()
                ),
                Err(_) => format!(
                    "this server could not renew its registration for {} s (no answer)",
                    renewed.elapsed().as_secs()
                ),
            };
            let _ = lost.send(Some(reason));
            return;
        }
    })
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use bson::Document;
    use tokio::sync::watch;

    use super::super::locks::tests::silent_server;
    use super::{Timing, spawn_registration_renewal};

    #[tokio::test]
    async fn a_registration_whose_renewals_get_no_answer_is_lost_at_its_deadline() {
        let (address, accepting) = silent_server().await;
        let mut options = mongodb::options::ClientOptions::parse(format!(
            "mongodb://{address}/?directConnection=true"
        ))
        .await
        .expect("options");
        options.connect_timeout = Some(Duration::from_secs(60));
        options.server_selection_timeout = Some(Duration::from_secs(60));
        let client = mongodb::Client::with_options(options).expect("client");
        let servers = client
            .database("silent")
            .collection::<Document>("extenddb_servers");
        let timing = Timing {
            renew_every: Duration::from_millis(200),
            registration_deadline: Duration::from_secs(1),
            retry_every: Duration::from_millis(50),
            ..Timing::PRODUCTION
        };
        let (tx, mut lost) = watch::channel(None);
        let registered = Instant::now();
        let renewal =
            spawn_registration_renewal(servers, "token".to_owned(), timing, registered, tx);
        // Without the bound, the first renewal would wait the whole 10 s
        // request timeout before the loss could be reported.
        tokio::time::timeout(Duration::from_secs(5), lost.changed())
            .await
            .expect("reported well before the request timeout")
            .expect("changed");
        let after = registered.elapsed();
        assert!(
            after >= timing.registration_deadline
                && after < timing.registration_deadline + Duration::from_millis(500),
            "the loss was reported {after:?} after the registration"
        );
        let reason = lost.borrow().clone().expect("a reason");
        assert!(
            reason.contains("could not renew its registration for 1 s"),
            "{reason}"
        );
        renewal.abort();
        accepting.abort();
    }
}
