// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The registration a server holds for its life, and the backend's side of
//! registering again once it is lost ([`ServingRegistrar`]).
//!
//! A registration is lost when its document was deleted, or when no renewal
//! has succeeded for 30 s (a primary that stopped answering, an election
//! that did not finish, a partition), 15 s before the document expires. The
//! server then refuses requests and asks for a new registration: a new
//! document, then a read of the migration lock, so a migration that holds
//! it makes the attempt fail. The catalog's status is read again under the
//! new document, and the server serves again only when it is the status the
//! server started with; otherwise a migration ran in between, and the
//! server stops.

use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use extenddb_storage::hooks::{Reregistration, ServingRegistrar};
use extenddb_storage::migrations::{CatalogStatus, MigrationError};

use super::locks::Timing;
use super::registrations::ServingRegistration;
use super::{MongoTarget, catalog_status};

/// The serving registration of a running server, with what it needs to
/// take it again: the catalog, the release it registers as, its timings and
/// the status the server started with.
pub struct ServerRegistration {
    target: MongoTarget,
    release: String,
    timing: Timing,
    started: CatalogStatus,
    current: Mutex<Arc<ServingRegistration>>,
}

impl ServerRegistration {
    pub(super) fn new(
        target: &MongoTarget,
        release: &str,
        timing: Timing,
        started: CatalogStatus,
        registration: ServingRegistration,
    ) -> Self {
        Self {
            target: target.clone(),
            release: release.to_owned(),
            timing,
            started,
            current: Mutex::new(Arc::new(registration)),
        }
    }

    /// The status the server started with.
    #[must_use]
    pub fn started(&self) -> &CatalogStatus {
        &self.started
    }

    /// The registration the server holds now.
    #[must_use]
    pub fn current(&self) -> Arc<ServingRegistration> {
        self.current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
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
        let registration =
            match ServingRegistration::register(&self.target, &self.release, self.timing).await {
                Ok(registration) => registration,
                Err(MigrationError::Locked(why)) => return Reregistration::Refused(why),
                Err(e) => return Reregistration::Unavailable(e.to_string()),
            };
        let status = match catalog_status(&self.target).await {
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
        // Dropping the lost registration deletes its document, if it is
        // still there, from a task of its own.
        *self.current.lock().unwrap_or_else(PoisonError::into_inner) = Arc::new(registration);
        Reregistration::Registered
    }

    async fn release(&self) {
        self.current().release().await;
    }
}
