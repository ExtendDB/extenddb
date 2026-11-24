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

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use extenddb_storage::hooks::{Reregistration, ServingRegistrar};
use extenddb_storage::migrations::{CatalogStatus, MigrationError, Target};
use extenddb_storage::secret_keys::SecretKeys;
use extenddb_storage::server_components::{BackendError, KeySource, load_secret_keys};
use zeroize::Zeroizing;

use super::locks::{KEEPALIVE_INTERVAL, SERVE_APPLICATION, ServingRegistration};
use super::sessions::{close_status, status_session};
use super::{PostgresTarget, catalog_status, require_data_connection_row, resolve};
use crate::catalog_store::PostgresCatalogStore;

/// The serving registration of a running server, with what it needs to
/// take it again: the catalog's databases, the status the server started
/// with, and the name its session carries; and the key holder loaded under
/// it, which the server's stores share.
pub struct ServerRegistration {
    target: PostgresTarget,
    started: CatalogStatus,
    keepalive: Duration,
    application: &'static str,
    current: Mutex<ServingRegistration>,
    keys: Option<Arc<SecretKeys>>,
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
        Self::register_as(connection_string, SERVE_APPLICATION).await
    }

    /// [`ServerRegistration::register`] for the process `application` names
    /// (`extenddb serve`, or a command such as `extenddb backup list`): its
    /// session carries the name as `application_name`, which a migration
    /// waiting for the registrations prints, and a refusal while a
    /// migration runs names the migration's sessions without the server's
    /// advice to start the server.
    ///
    /// # Errors
    ///
    /// As [`ServerRegistration::register`].
    pub async fn register_as(
        connection_string: &str,
        application: &'static str,
    ) -> Result<Self, MigrationError> {
        Self::register_keepalive(connection_string, KEEPALIVE_INTERVAL, application).await
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
        Self::register_keepalive(connection_string, keepalive, SERVE_APPLICATION).await
    }

    /// [`ServerRegistration::register_as`] with another keepalive interval,
    /// for tests.
    ///
    /// # Errors
    ///
    /// As [`ServerRegistration::register`].
    #[cfg(feature = "test-util")]
    pub async fn register_as_with(
        connection_string: &str,
        application: &'static str,
        keepalive: Duration,
    ) -> Result<Self, MigrationError> {
        Self::register_keepalive(connection_string, keepalive, application).await
    }

    async fn register_keepalive(
        connection_string: &str,
        keepalive: Duration,
        application: &'static str,
    ) -> Result<Self, MigrationError> {
        Self::register_loading(connection_string, keepalive, application, KeySource::None)
            .await
            .map_err(|e| match e {
                BackendError::Catalog(e) => e,
                other => MigrationError::Refused(other.to_string()),
            })
    }

    /// [`ServerRegistration::register_as`] for the server factory, which
    /// also loads the key (RFC-0000 sections 7 and 12): after the serving
    /// key is taken and before the status is read, the key rows are read
    /// and `key_source` loads the one key holder that serves the status
    /// read, the data database's connection string and the stores. A KEK
    /// in `key_source` is wiped when the load returns.
    ///
    /// # Errors
    ///
    /// The refusals of [`load_secret_keys`], and those of
    /// [`ServerRegistration::register`] as [`BackendError::Catalog`].
    pub async fn register_keyed(
        connection_string: &str,
        application: &'static str,
        key_source: KeySource,
    ) -> Result<Self, BackendError> {
        Self::register_loading(
            connection_string,
            KEEPALIVE_INTERVAL,
            application,
            key_source,
        )
        .await
    }

    async fn register_loading(
        connection_string: &str,
        keepalive: Duration,
        application: &'static str,
        key_source: KeySource,
    ) -> Result<Self, BackendError> {
        let Some(target) = resolve(&Target::at(connection_string))
            .await
            .map_err(BackendError::Catalog)?
        else {
            return Err(BackendError::Catalog(MigrationError::Status(
                CatalogStatus::Uninitialized,
            )));
        };
        let registration =
            ServingRegistration::register_keepalive(&target.catalog, keepalive, application)
                .await
                .map_err(BackendError::Catalog)?;
        let keys = match load_keys(&target, key_source).await {
            Ok(keys) => keys,
            Err(e) => {
                registration.release().await;
                return Err(e);
            }
        };
        let target = match &keys {
            Some(keys) => target.with_secret_keys(Arc::clone(keys)),
            None => target,
        };
        let status = match catalog_status(&target).await {
            Ok(status) => status,
            Err(e) => {
                registration.release().await;
                return Err(BackendError::Catalog(e));
            }
        };
        match status {
            CatalogStatus::Current { .. } | CatalogStatus::AheadCompatible { .. } => {
                if let Err(e) = require_data_connection_row(&target).await {
                    registration.release().await;
                    return Err(BackendError::Catalog(e));
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
                    application,
                    current: Mutex::new(registration),
                    keys,
                })
            }
            status => {
                registration.release().await;
                Err(BackendError::Catalog(MigrationError::Status(status)))
            }
        }
    }

    /// The key holder loaded under this registration, or `None` when it
    /// was taken with `KeySource::None`.
    #[must_use]
    pub fn secret_keys(&self) -> Option<Arc<SecretKeys>> {
        self.keys.clone()
    }

    /// The catalog's data database connection string, opened with the key
    /// holder of this registration in a catalog with a key ring; `None`
    /// when the catalog has no such row.
    ///
    /// # Errors
    ///
    /// The catalog cannot be read, or the key holder does not open the row.
    pub async fn data_connection_string(
        &self,
    ) -> Result<Option<Zeroizing<String>>, MigrationError> {
        super::data_connection_string(&self.target).await
    }
}

/// Load the key holder of `key_source` from the catalog's key rows, on a
/// read-only session of its own. `KeySource::None` opens no session.
async fn load_keys(
    target: &PostgresTarget,
    key_source: KeySource,
) -> Result<Option<Arc<SecretKeys>>, BackendError> {
    if matches!(key_source, KeySource::None) {
        return Ok(None);
    }
    let pool = status_session(&target.catalog, target.connect_timeout)
        .await
        .map_err(BackendError::Catalog)?;
    let keys = load_secret_keys(&PostgresCatalogStore::new(pool.clone()), key_source).await;
    close_status(pool).await;
    keys
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
        let registration = match ServingRegistration::register_keepalive(
            &self.target.catalog,
            self.keepalive,
            self.application,
        )
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
