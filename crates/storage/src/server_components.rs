// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backend factory infrastructure for creating server components.
//!
//! This module provides the factory pattern for creating storage backends.
//! A binary serves exactly one backend, installed from its thin `main` via
//! [`crate::set_backend`]; this factory is reached through the installed
//! [`crate::Backend`] value, which is what keeps `cmd_serve` backend-agnostic
//! without a registry to look anything up in.

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
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct ServerComponentsOptions {
    /// Bootstrap the catalog at serve time when it is not initialized,
    /// instead of failing with `InitializationFailed`. Set by dev-mode builds
    /// so `serve` works with no prior `init` (zero-config local/CI use).
    /// Backends whose bootstrap requires operator input ignore this and fail
    /// as usual; backends with a self-contained bootstrap (SQLite) honor it.
    pub bootstrap_if_uninitialized: bool,
    /// The item size limit the storage layer applies to the finished item of
    /// an update, standalone or transacted. Comes from `limits.max_item_size_bytes`
    /// so the storage layer and the engine's put path enforce one figure.
    pub max_item_size_bytes: usize,
}

impl Default for ServerComponentsOptions {
    fn default() -> Self {
        Self {
            bootstrap_if_uninitialized: false,
            max_item_size_bytes: extenddb_core::limits::LimitsConfig::default().max_item_size_bytes,
        }
    }
}

/// Factory function type for creating server components.
///
/// Takes a `StorageConfig` trait object, region string, and options; returns
/// a Future that resolves to `ServerComponents` or `BackendError`.
///
/// The trait object is bound `+ 'static` (rather than the reference's own
/// elided lifetime) so factories can use [`StorageConfig::as_any`] to
/// downcast to a concrete backend config and read backend-specific settings
/// (e.g. the mongo backend's `transaction_read_concern`) — `Any::downcast_ref`
/// requires the pointee to be provably `'static`, which an unannotated `&dyn
/// StorageConfig` does not guarantee even though every real implementor owns
/// its data.
pub type ServerComponentsFactory =
    fn(
        &(dyn StorageConfig + 'static),
        &str,
        ServerComponentsOptions,
    ) -> Pin<Box<dyn Future<Output = Result<ServerComponents, BackendError>> + Send>>;

/// Create server components using the installed backend.
///
/// Calls the factory of the [`Backend`](crate::Backend) installed via
/// [`set_backend`](crate::set_backend). Returns `BackendNotInstalled` if no
/// backend has been installed.
///
/// A `region` the `BackupArn` parser would refuse is logged at warn first
/// ([`crate::config::server_region_warning`]): the server still starts, but
/// the operator is told its own backup ARNs will not resolve.
pub async fn create_server_components(
    config: &(dyn StorageConfig + 'static),
    region: &str,
    options: ServerComponentsOptions,
) -> Result<ServerComponents, BackendError> {
    if let Some(warning) = crate::config::server_region_warning(region) {
        tracing::warn!("{}", warning.log_message());
    }
    let backend = crate::backend::try_backend().ok_or(BackendError::BackendNotInstalled)?;
    (backend.server_components)(config, region, options).await
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    use super::*;

    /// Collects the message of every warn-level event.
    #[derive(Default)]
    struct Warnings(Arc<Mutex<Vec<String>>>);

    struct Message(String);

    impl Visit for Message {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                let _ = write!(self.0, "{value:?}");
            }
        }
    }

    impl<S: Subscriber> Layer<S> for Warnings {
        fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
            if *event.metadata().level() == Level::WARN {
                let mut message = Message(String::new());
                event.record(&mut message);
                self.0.lock().unwrap().push(message.0);
            }
        }
    }

    #[derive(Debug)]
    struct NoConfig;

    impl StorageConfig for NoConfig {
        fn connection_config(&self) -> &str {
            ""
        }
        fn max_connections(&self) -> u32 {
            1
        }
        fn max_catalog_connections(&self) -> u32 {
            1
        }
        fn clone_box(&self) -> Box<dyn StorageConfig> {
            Box::new(NoConfig)
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    /// No backend is installed in this crate's tests, so the call fails
    /// after the region check; the warning is what is under test.
    async fn warnings_for(region: &str) -> Vec<String> {
        let warnings = Warnings::default();
        let collected = Arc::clone(&warnings.0);
        let subscriber = tracing_subscriber::registry().with(warnings);
        let _guard = tracing::subscriber::set_default(subscriber);
        let result =
            create_server_components(&NoConfig, region, ServerComponentsOptions::default()).await;
        assert!(matches!(result, Err(BackendError::BackendNotInstalled)));
        collected.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn a_region_outside_the_backup_arn_class_is_warned_about_at_startup() {
        let warnings = warnings_for("LOCAL").await;
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("server.region"), "{warnings:?}");
        assert!(warnings[0].contains("LOCAL"), "{warnings:?}");
        assert!(
            warnings[0].contains("BackupArn is not valid"),
            "{warnings:?}"
        );
        assert!(warnings_for("us-east-1").await.is_empty());
    }
}
