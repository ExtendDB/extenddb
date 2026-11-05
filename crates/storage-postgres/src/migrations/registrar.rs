// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! The registration a server holds for its life: the check `serve` makes
//! before it opens the engine, and the backend's side of registering again
//! after the database ended the registration's session
//! ([`ServingRegistrar`]).
//!
//! The database ends the session on a restart, a failover, a
//! `pg_terminate_backend`, or a pooler that drops it, and frees the serving
//! key with it, so a migration can start. The server then asks for a new
//! registration, which is taken on a new session with a try of the shared
//! key, never a wait: a migration that holds or waits for the exclusive key
//! makes the attempt fail at once. The catalog's status is read again under
//! the new registration, and the server serves again only when it is the
//! status the server started with; otherwise a migration ran in between,
//! and the server stops.

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use extenddb_storage::hooks::{Reregistration, ServingRegistrar};
use extenddb_storage::migrations::{CatalogStatus, MigrationError, Target};

use super::locks::{KEEPALIVE_INTERVAL, ServingRegistration};
use super::{PostgresTarget, catalog_status, require_data_connection_row, resolve};

/// The serving registration of a running server, with what it needs to
/// take it again: the catalog's databases and the status the server started
/// with.
pub struct ServerRegistration {
    target: PostgresTarget,
    started: CatalogStatus,
    keepalive: Duration,
    current: Mutex<ServingRegistration>,
}

impl ServerRegistration {
    /// Register as the catalog's server and read its status, before anything
    /// opens the engine: a server that refuses has written nothing. The
    /// registration is taken before the status is read, so a migration that
    /// starts in between waits for this server or makes it refuse.
    ///
    /// # Errors
    ///
    /// [`MigrationError::Status`] for a catalog the server does not serve,
    /// [`MigrationError::Locked`] (exit 6) while a migration runs, or the
    /// error of a database that cannot be reached or read.
    pub async fn register(connection_string: &str) -> Result<Self, MigrationError> {
        Self::register_keepalive(connection_string, KEEPALIVE_INTERVAL).await
    }

    /// [`ServerRegistration::register`] with another keepalive interval, for
    /// tests.
    ///
    /// # Errors
    ///
    /// As [`ServerRegistration::register`].
    #[cfg(feature = "test-util")]
    pub async fn register_with(
        connection_string: &str,
        keepalive: Duration,
    ) -> Result<Self, MigrationError> {
        Self::register_keepalive(connection_string, keepalive).await
    }

    async fn register_keepalive(
        connection_string: &str,
        keepalive: Duration,
    ) -> Result<Self, MigrationError> {
        let Some(target) = resolve(&Target::at(connection_string)).await? else {
            return Err(MigrationError::Status(CatalogStatus::Uninitialized));
        };
        let registration =
            ServingRegistration::register_keepalive(&target.catalog, keepalive).await?;
        let status = match catalog_status(&target).await {
            Ok(status) => status,
            Err(e) => {
                registration.release().await;
                return Err(e);
            }
        };
        match status {
            CatalogStatus::Current { .. } | CatalogStatus::AheadCompatible { .. } => {
                if let Err(e) = require_data_connection_row(&target).await {
                    registration.release().await;
                    return Err(e);
                }
                if matches!(status, CatalogStatus::AheadCompatible { .. }) {
                    tracing::warn!(
                        "{status}; serving it, because a binary may serve migrations it does \
                         not know when every one of them is additive"
                    );
                }
                Ok(Self {
                    target,
                    started: status,
                    keepalive,
                    current: Mutex::new(registration),
                })
            }
            status => {
                registration.release().await;
                Err(MigrationError::Status(status))
            }
        }
    }
}

#[async_trait]
impl ServingRegistrar for ServerRegistration {
    async fn lost(&self) -> String {
        let mut lost = self
            .current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .lost();
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
            match ServingRegistration::register_keepalive(&self.target.catalog, self.keepalive)
                .await
            {
                Ok(registration) => registration,
                Err(MigrationError::Locked(why)) => return Reregistration::Refused(why),
                Err(e) => return Reregistration::Unavailable(e.to_string()),
            };
        let status = match catalog_status(&self.target).await {
            Ok(status) => status,
            Err(e) => {
                // Dropping it closes the connection, which frees the key.
                drop(registration);
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
        // The lost registration's connection closes as it is dropped here.
        *self.current.lock().unwrap_or_else(PoisonError::into_inner) = registration;
        Reregistration::Registered
    }
}
