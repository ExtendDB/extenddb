// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Backend-specific runtime hooks for worker spawning and initialization.

use std::sync::Arc;

use async_trait::async_trait;
pub use tokio_util::sync::CancellationToken;
use tracing_subscriber::{EnvFilter, Registry, reload};

/// Context passed to `ServerRuntimeHooks::spawn_workers`.
///
/// Contains shared resources that backend-specific workers might need.
pub struct WorkerContext {
    pub metrics: Arc<extenddb_core::metrics::MetricsCollector>,
    pub catalog_store: Arc<dyn crate::CatalogStore>,
    pub reload_handle: reload::Handle<EnvFilter, Registry>,
    pub config_log_level: String,
    /// Cancelled when the server begins shutting down. Backend workers should
    /// select on [`CancellationToken::cancelled`] alongside their own timer and
    /// return once it fires, so shutdown drains them instead of dropping them
    /// mid-cycle. [`sleep_or_shutdown`] does this for the common
    /// `loop { sleep(interval); work(); }` shape. Re-exported here so a backend
    /// crate does not need its own `tokio-util` dependency.
    pub shutdown: CancellationToken,
}

/// Sleep for `interval`, returning early if the server is shutting down.
///
/// Returns `true` when the interval elapsed (run another cycle) and `false`
/// when `token` was cancelled (stop looping), which makes the canonical worker
/// loop `while sleep_or_shutdown(&token, INTERVAL).await { work().await; }`.
pub async fn sleep_or_shutdown(token: &CancellationToken, interval: std::time::Duration) -> bool {
    tokio::select! {
        () = token.cancelled() => false,
        () = tokio::time::sleep(interval) => true,
    }
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
    ///
    /// Return the spawned tasks' join handles so the server can await them
    /// during shutdown after cancelling [`WorkerContext::shutdown`]. Returning
    /// an empty vector opts out of the drain (the tasks are then dropped when
    /// the runtime shuts down).
    async fn spawn_workers(&self, ctx: &WorkerContext) -> Vec<tokio::task::JoinHandle<()>>;

    /// Get backend-specific info for logging (optional).
    ///
    /// Example: "`data_db=ddbo_data`" for `PostgreSQL`
    fn backend_info(&self) -> Option<String> {
        None
    }
}
