// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backend factory infrastructure for creating server components.
//!
//! This module provides the factory pattern for creating storage backends.
//! Backends register themselves via the inventory crate, allowing `cmd_serve`
//! to remain backend-agnostic.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use extenddb_auth::CredentialStore;

use crate::config::StorageConfig;
use crate::hooks::ServerRuntimeHooks;
use crate::{CatalogStore, StorageEngine};

/// Components needed to run the extenddb server.
///
/// Returned by backend factories. Contains all the trait objects needed
/// by `cmd_serve` to start the HTTP server and spawn workers.
pub struct ServerComponents {
    /// Storage engine implementing all data/metadata operations
    pub engine: Arc<dyn StorageEngine>,

    /// Catalog store for management API operations
    pub catalog_store: Arc<dyn CatalogStore>,

    /// Raw (uncached) credential store. The bin layer wraps this in
    /// `CachedCredentialStore` using the operator-configured TTL before
    /// constructing the auth provider.
    pub credential_store: Arc<dyn CredentialStore>,

    /// Optional backend-specific runtime hooks for worker spawning
    pub runtime_hooks: Option<Box<dyn ServerRuntimeHooks>>,
}

/// Errors that can occur during backend initialization.
#[derive(Debug)]
pub enum BackendError {
    /// No storage backend has been installed (set_backend was not called).
    BackendNotInstalled,

    /// Failed to connect to backend database
    ConnectionFailed { backend: String, details: String },

    /// Catalog schema version mismatch
    CatalogVersionMismatch { expected: String, found: String },

    /// Encryption key not found in settings table
    MissingEncryptionKey,

    /// Generic initialization failure
    InitializationFailed(String),
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BackendNotInstalled => {
                write!(
                    f,
                    "no storage backend installed (set_backend was not called)"
                )
            }
            Self::ConnectionFailed { backend, details } => {
                write!(f, "Failed to connect to {backend}: {details}")
            }
            Self::CatalogVersionMismatch { expected, found } => write!(
                f,
                "Catalog version mismatch: expected {expected}, found {found}. Run 'extenddb migrate'"
            ),
            Self::MissingEncryptionKey => write!(
                f,
                "Encryption key not found in settings table. Run 'extenddb init'"
            ),
            Self::InitializationFailed(msg) => write!(f, "Backend initialization failed: {msg}"),
        }
    }
}

impl std::error::Error for BackendError {}

/// Options for server-component creation, beyond the storage config itself.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct ServerComponentsOptions {
    /// Bootstrap the catalog at serve time when it is not initialized,
    /// instead of failing with `InitializationFailed`. Set by dev-mode builds
    /// so `serve` works with no prior `init` (zero-config local/CI use).
    /// Backends whose bootstrap requires operator input ignore this and fail
    /// as usual; backends with a self-contained bootstrap (SQLite) honor it.
    pub bootstrap_if_uninitialized: bool,
}

/// Factory function type for creating server components.
///
/// Takes a `StorageConfig` trait object, region string, and options; returns
/// a Future that resolves to `ServerComponents` or `BackendError`.
pub type ServerComponentsFactory =
    fn(
        &dyn StorageConfig,
        &str,
        ServerComponentsOptions,
    ) -> Pin<Box<dyn Future<Output = Result<ServerComponents, BackendError>> + Send>>;

/// Create server components using the installed backend.
///
/// Calls the factory of the [`Backend`](crate::Backend) installed via
/// [`set_backend`](crate::set_backend). Returns `BackendNotInstalled` if no
/// backend has been installed.
pub async fn create_server_components(
    config: &dyn StorageConfig,
    region: &str,
    options: ServerComponentsOptions,
) -> Result<ServerComponents, BackendError> {
    let backend = crate::backend::try_backend().ok_or(BackendError::BackendNotInstalled)?;
    (backend.server_components)(config, region, options).await
}
