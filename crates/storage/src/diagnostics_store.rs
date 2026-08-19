// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Diagnostics store factory registry for backend-agnostic instantiation.

use crate::diagnostics::DiagnosticsStore;
use futures::future::BoxFuture;

/// Error type for diagnostics store creation.
#[derive(Debug)]
pub enum DiagnosticsStoreError {
    /// No storage backend has been installed (set_backend was not called).
    BackendNotInstalled,
    ConnectionFailed(String),
}

impl std::fmt::Display for DiagnosticsStoreError {
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

impl std::error::Error for DiagnosticsStoreError {}

/// Factory function type for creating diagnostics stores.
pub type DiagnosticsStoreFactory =
    fn(&str) -> BoxFuture<'static, Result<Box<dyn DiagnosticsStore>, DiagnosticsStoreError>>;

/// Create a diagnostics store for the installed backend.
pub async fn create_diagnostics_store(
    connection_string: &str,
) -> Result<Box<dyn DiagnosticsStore>, DiagnosticsStoreError> {
    let backend =
        crate::backend::try_backend().ok_or(DiagnosticsStoreError::BackendNotInstalled)?;
    (backend.diagnostics_store)(connection_string).await
}
