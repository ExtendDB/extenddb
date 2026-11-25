// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The registration a server holds for its life, and the backend's side of
//! registering again once it is lost ([`ServingRegistrar`]).
//!
//! A registration is lost when no renewal of its row has succeeded for 30 s
//! (a node restart, an outage or a partition), before the row expires. The
//! server then refuses requests and asks for a new registration: a new row,
//! then a read of the migration lock, so a migration that holds it, or that
//! waits for the servers to stop while holding it, makes the attempt fail.
//! The catalog's status is read again under the new row, and the server
//! serves again only when it is the status the server started with;
//! otherwise a migration ran in between, and the server stops.

use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use extenddb_storage::hooks::{Reregistration, ServingRegistrar};
use extenddb_storage::migrations::CatalogStatus;

use super::registrations::{RegistrationTimes, ServingRegistration};
use super::{catalog_status, locks};
use crate::cassandra_util::CassandraSession;

/// The serving registration of a running server, with what it needs to
/// take it again: the session, the catalog, the row's contents and the
/// status the server started with.
pub struct ServerRegistration {
    session: Arc<CassandraSession>,
    keyspace_prefix: String,
    host: String,
    release: String,
    times: RegistrationTimes,
    started: CatalogStatus,
    current: Mutex<Arc<ServingRegistration>>,
}

impl ServerRegistration {
    pub(super) fn new(
        session: Arc<CassandraSession>,
        keyspace_prefix: &str,
        host: &str,
        release: &str,
        times: RegistrationTimes,
        started: CatalogStatus,
        registration: ServingRegistration,
    ) -> Self {
        Self {
            session,
            keyspace_prefix: keyspace_prefix.to_owned(),
            host: host.to_owned(),
            release: release.to_owned(),
            times,
            started,
            current: Mutex::new(Arc::new(registration)),
        }
    }

    /// The status the server started with.
    #[must_use]
    pub fn started(&self) -> &CatalogStatus {
        &self.started
    }

    fn current(&self) -> Arc<ServingRegistration> {
        self.current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn catalog(&self) -> String {
        format!("{}_catalog", self.keyspace_prefix)
    }
}

#[async_trait]
impl ServingRegistrar for ServerRegistration {
    async fn lost(&self) -> String {
        let mut lost = self.current().lost();
        loop {
            let reason = lost.borrow_and_update().clone();
            if let Some(reason) = reason {
                return reason;
            }
            if lost.changed().await.is_err() {
                // Released without being lost: nothing more is reported on
                // this registration.
                std::future::pending::<()>().await;
            }
        }
    }

    async fn register_again(&self) -> Reregistration {
        let catalog = self.catalog();
        let registration = match ServingRegistration::register_timed(
            self.session.clone(),
            &catalog,
            &self.host,
            &self.release,
            self.times,
        )
        .await
        {
            Ok(registration) => registration,
            Err(e) => return Reregistration::Unavailable(e.to_string()),
        };
        match locks::lock_holder(&self.session, &catalog).await {
            Ok(None) => {}
            Ok(Some(holder)) => {
                registration.release().await;
                return Reregistration::Refused(format!(
                    "a migration holds the catalog's migration lock (holder {holder})"
                ));
            }
            Err(e) => {
                registration.release().await;
                return Reregistration::Unavailable(format!(
                    "the migration lock could not be read: {e}"
                ));
            }
        }
        let status = match catalog_status(self.session.clone(), &self.keyspace_prefix).await {
            Ok(status) => status,
            Err(e) => {
                registration.release().await;
                return Reregistration::Unavailable(format!(
                    "the catalog's status could not be read: {e}"
                ));
            }
        };
        if status != self.started {
            registration.release().await;
            return Reregistration::Refused(format!(
                "the catalog's status changed while the server held no registration (when it \
                 started: {}; now: {status}); start the server again to serve the catalog as it \
                 is now",
                self.started
            ));
        }
        // Dropping the lost registration deletes its row, if it is still
        // there, from a task of its own.
        *self.current.lock().unwrap_or_else(PoisonError::into_inner) = Arc::new(registration);
        Reregistration::Registered
    }

    async fn release(&self) {
        self.current().release().await;
    }
}
