// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Diagnostics store factory registry for backend-agnostic instantiation.

use crate::diagnostics::DiagnosticsStore;
use futures::future::BoxFuture;

/// Error type for diagnostics store creation.
#[derive(Debug)]
pub enum DiagnosticsStoreError {
    BackendNotFound(String),
    ConnectionFailed(String),
}

impl std::fmt::Display for DiagnosticsStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BackendNotFound(backend) => {
                write!(
                    f,
                    "No diagnostics store factory registered for backend '{backend}'"
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

/// Registration entry for a diagnostics store factory.
pub struct DiagnosticsStoreRegistration {
    pub backend: &'static str,
    pub factory: DiagnosticsStoreFactory,
}

inventory::collect!(DiagnosticsStoreRegistration);

/// Create a diagnostics store for the given backend and connection string.
pub async fn create_diagnostics_store(
    backend: &str,
    connection_string: &str,
) -> Result<Box<dyn DiagnosticsStore>, DiagnosticsStoreError> {
    for registration in inventory::iter::<DiagnosticsStoreRegistration> {
        if registration.backend == backend {
            return (registration.factory)(connection_string).await;
        }
    }
    Err(DiagnosticsStoreError::BackendNotFound(backend.to_string()))
}
