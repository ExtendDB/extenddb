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

    /// The serving registration, for a backend whose database can end it
    /// while the server runs: PostgreSQL ends the session that holds the
    /// serving key on a restart, a failover or `pg_terminate_backend`, and a
    /// registration row is lost when its renewals fail. `None` (the
    /// default) for a backend whose registration lasts as long as the
    /// process, such as a file lock.
    ///
    /// `serve` calls this once, before [`ServerRuntimeHooks::spawn_workers`].
    /// When [`ServingRegistrar::lost`] returns, the server answers every new
    /// request with `ServiceUnavailable` and calls
    /// [`ServingRegistrar::register_again`] at once and then every second.
    /// It serves again on [`Reregistration::Registered`]. On
    /// [`Reregistration::Refused`], or when 30 s pass without a
    /// registration, it stops as a shutdown signal stops it (the listener
    /// drains, [`WorkerContext::shutdown`] fires, the PID file is removed)
    /// and exits 1. On every shutdown it calls [`ServingRegistrar::release`]
    /// once the background workers have stopped.
    fn serving_registrar(&self) -> Option<Arc<dyn ServingRegistrar>> {
        None
    }
}

/// A backend's side of the serving registration: it reports the loss of
/// the registration the server holds, and makes one attempt to register
/// again when asked. The server decides the rest: it refuses requests while
/// it holds no registration, chooses how often and how long to try, and
/// stops.
#[async_trait]
pub trait ServingRegistrar: Send + Sync {
    /// Resolve once the registration the server holds now is lost, that is
    /// once a migration could take the catalog, with the reason in words
    /// for the log. The server calls it again only after
    /// [`ServingRegistrar::register_again`] returned
    /// [`Reregistration::Registered`], and the call then waits on the new
    /// registration.
    async fn lost(&self) -> String;

    /// One attempt to register again. Take a new registration without
    /// waiting: a migration that holds the migration lock, or waits for the
    /// servers to stop, makes the attempt [`Reregistration::Refused`]. Then
    /// read the catalog's status under the new registration, and return
    /// [`Reregistration::Registered`] only when it equals the status the
    /// server started with; the new registration then replaces the lost
    /// one. The server bounds each attempt with a timeout and drops it when
    /// the timeout passes, so a dropped attempt must leave no registration
    /// behind.
    async fn register_again(&self) -> Reregistration;

    /// Give up the registration the server holds, so a migration need not
    /// wait for it to expire. `serve` calls it once, after the background
    /// workers have stopped, so a migration cannot start while one of them
    /// still writes to the catalog. The default does nothing, for a
    /// registration that ends with the process, such as PostgreSQL's key,
    /// which the database frees when the session closes.
    async fn release(&self) {}
}

/// The outcome of one [`ServingRegistrar::register_again`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reregistration {
    /// The server holds a new registration, and the catalog's status is the
    /// one it started with.
    Registered,
    /// No registration was taken, and a later attempt may take one: the
    /// database did not answer or refused the connection, or the status
    /// could not be read. The server tries again while its window lasts.
    Unavailable(String),
    /// The server must stop: a migration holds the migration lock or waits
    /// for the servers to stop, or the catalog's status changed while the
    /// server held no registration.
    Refused(String),
}
