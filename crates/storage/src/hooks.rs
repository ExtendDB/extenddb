// Copyright 2026 DynamoDB Open contributors
// SPDX-License-Identifier: Apache-2.0

//! Backend-specific runtime hooks for worker spawning and initialization.

use std::sync::Arc;

use async_trait::async_trait;
use tracing_subscriber::{EnvFilter, Registry, reload};

/// Context passed to ServerRuntimeHooks::spawn_workers.
///
/// Contains shared resources that backend-specific workers might need.
pub struct WorkerContext {
    pub metrics: Arc<extenddb_core::metrics::MetricsCollector>,
    pub catalog_store: Arc<dyn crate::CatalogStore>,
    pub reload_handle: reload::Handle<EnvFilter, Registry>,
    pub config_log_level: String,
}

/// Backend readiness failure returned by [`ServerRuntimeHooks::health_check`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BackendHealthError {
    message: String,
}

impl BackendHealthError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for BackendHealthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for BackendHealthError {}

/// Backend-specific runtime hooks for worker spawning and initialization.
///
/// Backends implement this trait to spawn workers that are tightly coupled
/// to their implementation details (e.g., control plane pollers, pool metrics,
/// backend-native retention workers).
#[async_trait]
pub trait ServerRuntimeHooks: Send + Sync {
    /// Spawn backend-specific workers.
    ///
    /// Called after server components are created but before the HTTP server
    /// starts. Backends can spawn workers that need access to backend-specific
    /// state (connection pools, notify handles, etc.).
    async fn spawn_workers(&self, ctx: &WorkerContext);

    /// Check the backend resources owned by this frontend.
    ///
    /// HTTP `/health` calls this so load balancers observe the selected
    /// backend's real readiness instead of only the web process state.
    async fn health_check(&self) -> Result<(), BackendHealthError> {
        Ok(())
    }

    /// Get backend-specific info for logging (optional).
    ///
    /// Example: "data_db=extenddb_data"
    fn backend_info(&self) -> Option<String> {
        None
    }
}
