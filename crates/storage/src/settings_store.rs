// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Settings store factory registry for backend-agnostic instantiation.

use crate::management_store::SettingsStore;
use futures::future::BoxFuture;

/// Error type for settings store creation.
#[derive(Debug)]
pub enum SettingsStoreError {
    BackendNotFound(String),
    ConnectionFailed(String),
}

impl std::fmt::Display for SettingsStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BackendNotFound(backend) => {
                write!(
                    f,
                    "No settings store factory registered for backend '{backend}'"
                )
            }
            Self::ConnectionFailed(msg) => write!(f, "Failed to connect: {msg}"),
        }
    }
}

impl std::error::Error for SettingsStoreError {}

/// Factory function type for creating settings stores.
pub type SettingsStoreFactory =
    fn(&str) -> BoxFuture<'static, Result<Box<dyn SettingsStore>, SettingsStoreError>>;

/// Registration entry for a settings store factory.
pub struct SettingsStoreRegistration {
    pub backend: &'static str,
    pub factory: SettingsStoreFactory,
}

inventory::collect!(SettingsStoreRegistration);

/// Create a settings store for the given backend and connection string.
pub async fn create_settings_store(
    backend: &str,
    connection_string: &str,
) -> Result<Box<dyn SettingsStore>, SettingsStoreError> {
    for registration in inventory::iter::<SettingsStoreRegistration> {
        if registration.backend == backend {
            return (registration.factory)(connection_string).await;
        }
    }
    Err(SettingsStoreError::BackendNotFound(backend.to_string()))
}
