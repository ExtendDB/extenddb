// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Management store factory registry for backend-agnostic instantiation.
//!
//! The `migrate` command drives the resource-tag policy rewrite over a
//! [`ManagementStore`] without standing up a full server. This factory opens a
//! catalog-only store from a connection string, mirroring
//! [`crate::settings_store`].

use crate::management_store::ManagementStore;
use futures::future::BoxFuture;

/// Error type for management store creation.
#[derive(Debug)]
pub enum ManagementStoreError {
    /// No storage backend has been installed (`set_backend` was not called).
    BackendNotInstalled,
    ConnectionFailed(String),
}

impl std::fmt::Display for ManagementStoreError {
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

impl std::error::Error for ManagementStoreError {}

/// Factory function type for creating management stores.
pub type ManagementStoreFactory =
    fn(&str) -> BoxFuture<'static, Result<Box<dyn ManagementStore>, ManagementStoreError>>;

/// Create a management store for the installed backend.
///
/// # Errors
///
/// Returns [`ManagementStoreError::BackendNotInstalled`] when no backend has
/// been installed, or [`ManagementStoreError::ConnectionFailed`] on a
/// connection error.
pub async fn create_management_store(
    connection_string: &str,
) -> Result<Box<dyn ManagementStore>, ManagementStoreError> {
    let backend = crate::backend::try_backend().ok_or(ManagementStoreError::BackendNotInstalled)?;
    (backend.management_store)(connection_string).await
}
