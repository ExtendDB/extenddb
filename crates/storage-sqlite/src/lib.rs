// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite storage backend for ExtendDB.
//!
//! Implements the full `StorageEngine` + `CatalogStore` trait surface using
//! SQLite via `sqlx`, persisting to a single file (or `:memory:` for testing).
//! WAL mode enables concurrent readers; writers are serialized by the engine
//! (see `store`).
//!
//! # Design decisions vs the PostgreSQL backend
//!
//! - Placeholders: `?` positional (not `$N`).
//! - Concurrency: single WAL-mode pool; an engine-level write lock plus
//!   `BEGIN IMMEDIATE` replaces `SERIALIZABLE` / `FOR UPDATE`.
//! - Numbers: `N` sort keys stored as order-preserving TEXT (never `REAL`),
//!   full precision retained in the item JSON. See `docs/design-decisions.md`.
//! - One file holds catalog and data (no separate catalog/data databases).
//!
//! NOTE: this is the catalog-layer build slice. The data-plane engine modules
//! and the `ServerComponents` registration are added in the engine slice.

mod admin_store;
mod authorization_store;
mod backup;
mod bootstrapper;
mod catalog_store;
pub mod config;
mod create_table;
mod credential_store;
mod data;
mod delete_table;
mod hooks;
mod management_store;
mod metadata;
mod number_key;
mod operations;
mod schema;
mod sqlite_util;
mod store;
mod stream;
mod table_engine;
mod table_helpers;
mod update_table;
mod worker;
mod workers;

/// Default GSI propagation delay (milliseconds) when the
/// `gsi_propagation_delay_ms` setting is absent. Mirrors the value seeded by
/// the catalog schema, and is the single definition used by both the live read
/// on the write path and the background refresh worker.
pub(crate) const DEFAULT_GSI_PROPAGATION_DELAY_MS: u64 = 10;

pub use bootstrapper::SqliteBootstrapper;
pub use catalog_store::SqliteCatalogStore;
pub use config::SqliteConfig;
pub use credential_store::SqliteCredentialStore;
pub use schema::CATALOG_VERSION;
pub use store::SqliteEngine;

/// The SQLite backend, for installation via
/// [`extenddb_storage::set_backend`] in a thin bin.
///
/// Bundles the bootstrapper, operations engine, config deserializer, and the
/// settings/diagnostics/server-components factories under the `sqlite` name,
/// which also selects the `[storage.sqlite]` config section.
#[must_use]
pub fn backend() -> extenddb_storage::Backend {
    extenddb_storage::Backend {
        name: "sqlite",
        bootstrapper: sqlite_bootstrapper_factory,
        storage_config: config::deserialize_config,
        operations: &operations::SqliteOperationsEngine,
        settings_store: sqlite_settings_store_factory,
        diagnostics_store: sqlite_diagnostics_store_factory,
        server_components: sqlite_server_components_factory,
    }
}

use extenddb_storage::diagnostics::DiagnosticsStore;
use extenddb_storage::diagnostics_store::DiagnosticsStoreError;
use extenddb_storage::management_store::SettingsStore;
use extenddb_storage::settings_store::SettingsStoreError;

use crate::sqlite_util::sqlite_url;

/// Open a small catalog-only pool for the settings/diagnostics factories,
/// applying the same PRAGMAs as the engine.
async fn catalog_pool(connection_string: &str) -> Result<sqlx::SqlitePool, sqlx::Error> {
    sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(4)
        .after_connect(|conn, _| {
            Box::pin(async move {
                use sqlx::Executor;
                conn.execute("PRAGMA journal_mode = WAL").await?;
                conn.execute("PRAGMA foreign_keys = ON").await?;
                conn.execute("PRAGMA busy_timeout = 5000").await?;
                Ok(())
            })
        })
        .connect(&sqlite_url(connection_string))
        .await
}

// ── Bootstrapper ───────────────────────────────────────────────────────

fn sqlite_bootstrapper_factory(
    config_path: String,
    cli_args: Vec<String>,
) -> futures::future::BoxFuture<
    'static,
    Result<
        Box<dyn extenddb_storage::bootstrapper::Bootstrapper>,
        extenddb_storage::error::StorageError,
    >,
> {
    Box::pin(async move {
        let store = SqliteBootstrapper::from_config(&config_path, &cli_args).await?;
        Ok(Box::new(store) as Box<dyn extenddb_storage::bootstrapper::Bootstrapper>)
    })
}

// ── Operations engine ──────────────────────────────────────────────────

// ── Config deserializer ────────────────────────────────────────────────

// ── Settings store factory ─────────────────────────────────────────────

fn sqlite_settings_store_factory(
    connection_string: &str,
) -> futures::future::BoxFuture<'static, Result<Box<dyn SettingsStore>, SettingsStoreError>> {
    let connection_string = connection_string.to_owned();
    Box::pin(async move {
        let pool = catalog_pool(&connection_string)
            .await
            .map_err(|e| SettingsStoreError::ConnectionFailed(e.to_string()))?;
        Ok(Box::new(SqliteCatalogStore::new(pool)) as Box<dyn SettingsStore>)
    })
}

// ── Diagnostics store factory ──────────────────────────────────────────

fn sqlite_diagnostics_store_factory(
    connection_string: &str,
) -> futures::future::BoxFuture<'static, Result<Box<dyn DiagnosticsStore>, DiagnosticsStoreError>> {
    let connection_string = connection_string.to_owned();
    Box::pin(async move {
        let pool = catalog_pool(&connection_string)
            .await
            .map_err(|e| DiagnosticsStoreError::ConnectionFailed(e.to_string()))?;
        Ok(Box::new(SqliteCatalogStore::new(pool)) as Box<dyn DiagnosticsStore>)
    })
}

// ── Server components factory ──────────────────────────────────────────

use std::sync::Arc;

use extenddb_auth::CredentialStore;
use extenddb_storage::server_components::{BackendError, ServerComponents};

use crate::hooks::SqliteRuntimeHooks;

/// Maximum DynamoDB item size in bytes (post-update validation bound).
const MAX_ITEM_SIZE_BYTES: usize = 400_000;

fn sqlite_server_components_factory(
    config: &dyn extenddb_storage::config::StorageConfig,
    region: &str,
    options: extenddb_storage::server_components::ServerComponentsOptions,
) -> futures::future::BoxFuture<'static, Result<ServerComponents, BackendError>> {
    let db_path = config.connection_config().to_owned();
    let pool_size = config.max_connections();
    let region = region.to_owned();
    Box::pin(async move {
        let engine = SqliteEngine::new(&db_path, pool_size, &region, MAX_ITEM_SIZE_BYTES)
            .await
            .map_err(|e| BackendError::ConnectionFailed {
                backend: "sqlite".to_owned(),
                details: e.to_string(),
            })?;

        // In-memory databases do not persist across the `init` process, so the
        // catalog must be bootstrapped here, at serve time, on the engine's own
        // shared connection (a second pool would open a separate empty DB).
        // A dev-mode build extends the same bootstrap to an uninitialized FILE
        // database (`bootstrap_if_uninitialized`), so zero-config dev serve
        // works with a persistent path too; every bootstrap step is guarded
        // (IF NOT EXISTS / INSERT OR IGNORE), so re-running on an initialized
        // file is a no-op. Production builds keep the explicit-`init` contract.
        let in_memory = db_path == ":memory:"
            || db_path.starts_with("file::memory:")
            || db_path.contains("mode=memory");
        if in_memory || options.bootstrap_if_uninitialized {
            let admin_user = std::env::var("EXTENDDB_ADMIN_USER").ok();
            let admin_password = std::env::var("EXTENDDB_ADMIN_PASSWORD").ok();
            if let Some(password) = engine
                .bootstrap_ephemeral(admin_user.as_deref(), admin_password.as_deref())
                .await
                .map_err(|e| BackendError::InitializationFailed(e.to_string()))?
            {
                let user = admin_user.as_deref().unwrap_or("admin");
                let durability = if in_memory {
                    "In-memory backend: ephemeral admin credentials (lost on restart)"
                } else {
                    "Bootstrapped file backend: admin credentials (stored in the database)"
                };
                println!("\n  {durability}\n  Username: {user}\n  Password: {password}\n");
            }
        }

        engine.check_catalog_version().await.map_err(|e| match e {
            extenddb_storage::error::StorageError::CatalogVersionMismatch { expected, found } => {
                BackendError::CatalogVersionMismatch { expected, found }
            }
            other => BackendError::InitializationFailed(other.to_string()),
        })?;

        // Recover any in-flight control-plane transitions from a prior run.
        match engine.process_control_plane_transitions().await {
            Ok(transitions) => {
                for (name, transition) in &transitions {
                    tracing::info!("Recovered table '{name}': {transition}");
                }
            }
            Err(e) => tracing::error!("Failed to recover control plane transitions: {e}"),
        }

        // Rebuild any GSI left mid-backfill (status CREATING) by a prior crash.
        match engine.reconcile_incomplete_gsis().await {
            Ok(n) if n > 0 => tracing::info!("Reconciled {n} incomplete GSI(s) at startup"),
            Ok(_) => {}
            Err(e) => tracing::error!("Failed to reconcile incomplete GSIs: {e}"),
        }

        let control_plane_notify = engine.control_plane_notify();
        let engine = Arc::new(engine);

        // Catalog + auth queries: a file-backed database opens a dedicated
        // catalog pool over the same file; an in-memory database must reuse the
        // engine's single shared connection (its data lives only there).
        let catalog_pool = if in_memory {
            engine.pool.clone()
        } else {
            catalog_pool(&db_path)
                .await
                .map_err(|e| BackendError::ConnectionFailed {
                    backend: "sqlite".to_owned(),
                    details: format!("catalog pool: {e}"),
                })?
        };

        let enc_key: Option<String> =
            sqlx::query_scalar("SELECT value FROM settings WHERE key = 'encryption_key'")
                .fetch_optional(&catalog_pool)
                .await
                .map_err(|e| {
                    BackendError::InitializationFailed(format!("fetch encryption key: {e}"))
                })?;
        let enc_key = enc_key.ok_or(BackendError::MissingEncryptionKey)?;

        let catalog_store: Arc<dyn extenddb_storage::CatalogStore> = Arc::new(
            SqliteCatalogStore::with_encryption_key(catalog_pool.clone(), enc_key.clone()),
        );
        let credential_store: Arc<dyn CredentialStore> =
            Arc::new(SqliteCredentialStore::new(catalog_pool, enc_key));

        let runtime_hooks = Box::new(SqliteRuntimeHooks {
            engine: engine.clone(),
            control_plane_notify,
        });

        Ok(ServerComponents {
            engine,
            catalog_store,
            credential_store,
            runtime_hooks: Some(runtime_hooks),
        })
    })
}
