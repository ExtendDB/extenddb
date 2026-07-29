// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Settings store factory registry for backend-agnostic instantiation.

use crate::management_store::SettingsStore;
use futures::future::BoxFuture;

/// Error type for settings store creation.
#[derive(Debug)]
pub enum SettingsStoreError {
    /// No storage backend has been installed (set_backend was not called).
    BackendNotInstalled,
    ConnectionFailed(String),
}

impl std::fmt::Display for SettingsStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BackendNotInstalled => {
                write!(
                    f,
                    "no storage backend installed (set_backend was not called)"
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

/// Create a settings store for the installed backend.
pub async fn create_settings_store(
    connection_string: &str,
) -> Result<Box<dyn SettingsStore>, SettingsStoreError> {
    let backend = crate::backend::try_backend().ok_or(SettingsStoreError::BackendNotInstalled)?;
    (backend.settings_store)(connection_string).await
}
