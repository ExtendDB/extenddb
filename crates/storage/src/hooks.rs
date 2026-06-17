// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backend-specific runtime hooks for worker spawning and initialization.

use std::sync::Arc;

use async_trait::async_trait;
use tracing_subscriber::{EnvFilter, Registry, reload};

/// Context passed to `ServerRuntimeHooks::spawn_workers`.
///
/// Contains shared resources that backend-specific workers might need.
pub struct WorkerContext {
    pub metrics: Arc<extenddb_core::metrics::MetricsCollector>,
    pub catalog_store: Arc<dyn crate::CatalogStore>,
    pub reload_handle: reload::Handle<EnvFilter, Registry>,
    pub config_log_level: String,
}

/// Backend-specific runtime hooks for worker spawning and initialization.
///
/// Backends implement this trait to spawn workers that are tightly coupled
/// to their implementation details (e.g., `PostgreSQL`'s control plane poller,
/// pool metrics, GSI delay polling).
#[async_trait]
pub trait ServerRuntimeHooks: Send + Sync {
    /// Spawn backend-specific workers.
    ///
    /// Called after server components are created but before the HTTP server
    /// starts. Backends can spawn workers that need access to backend-specific
    /// state (connection pools, notify handles, etc.).
    async fn spawn_workers(&self, ctx: &WorkerContext);

    /// Get backend-specific info for logging (optional).
    ///
    /// Example: "`data_db=ddbo_data`" for `PostgreSQL`
    fn backend_info(&self) -> Option<String> {
        None
    }
}
