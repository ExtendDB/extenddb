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
#[cfg(test)]
mod gsi_backfilling_tests;
mod hooks;
mod management_store;
mod metadata;
mod number_key;
mod operations;
mod schema;
mod serve_lock;
mod snapshot_engine;
mod sqlite_util;
mod store;
mod stream;
#[cfg(test)]
mod stream_tests;
mod table_engine;
mod table_helpers;
mod update_table;
mod vector_bench;
mod vector_search;
mod worker;
mod workers;

/// Default secondary-index propagation delay (milliseconds) when the
/// `index_propagation_delay_ms` setting is absent. Mirrors the value seeded by
/// the catalog schema, and is the single definition used by both the live read
/// on the write path and the background refresh worker.
pub(crate) const DEFAULT_INDEX_PROPAGATION_DELAY_MS: u64 = 10;

/// Read the propagation-delay setting, preferring the canonical key and falling back
/// to the pre-rename one.
///
/// A catalog created before the rename holds the operator's value under the old name,
/// and the server refuses to start on a catalog-version mismatch rather than migrating,
/// so no upgrade step ever rewrites that row. Reading past it would silently reset a
/// configured delay to the default; since 0 means synchronous, the silent change would
/// be from strict to eventually consistent. `ORDER BY ... DESC` makes the preference
/// deterministic when both rows exist.
pub(crate) const INDEX_PROPAGATION_DELAY_QUERY: &str = "SELECT value FROM settings \
     WHERE key IN ('index_propagation_delay_ms', 'gsi_propagation_delay_ms') \
     ORDER BY key = 'index_propagation_delay_ms' DESC LIMIT 1";

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
        management_store: sqlite_management_store_factory,
        diagnostics_store: sqlite_diagnostics_store_factory,
        server_components: sqlite_server_components_factory,
    }
}

use extenddb_storage::diagnostics::DiagnosticsStore;
use extenddb_storage::diagnostics_store::DiagnosticsStoreError;
use extenddb_storage::management_store::{ManagementStore, SettingsStore};
use extenddb_storage::management_store_factory::ManagementStoreError;
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

// ── Management store factory ───────────────────────────────────────────

fn sqlite_management_store_factory(
    connection_string: &str,
) -> futures::future::BoxFuture<'static, Result<Box<dyn ManagementStore>, ManagementStoreError>> {
    let connection_string = connection_string.to_owned();
    Box::pin(async move {
        let pool = catalog_pool(&connection_string)
            .await
            .map_err(|e| ManagementStoreError::ConnectionFailed(e.to_string()))?;
        Ok(Box::new(SqliteCatalogStore::new(pool)) as Box<dyn ManagementStore>)
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
    config: &(dyn extenddb_storage::config::StorageConfig + 'static),
    region: &str,
    options: extenddb_storage::server_components::ServerComponentsOptions,
) -> futures::future::BoxFuture<'static, Result<ServerComponents, BackendError>> {
    let db_path = config.connection_config().to_owned();
    let pool_size = config.max_connections();
    let region = region.to_owned();
    Box::pin(async move {
        // One server per database file, taken before the database is opened:
        // startup recovery below assumes no other server is using it. A file
        // database only; an in-memory one belongs to this process alone.
        let serve_lock = serve_lock::ServeLock::acquire_for_location(&db_path)
            .map_err(BackendError::InitializationFailed)?
            .map(|lock| {
                tracing::debug!("holding serve lock {}", lock.path().display());
                std::sync::Arc::new(lock)
            });
        let mut engine = SqliteEngine::new(&db_path, pool_size, &region, MAX_ITEM_SIZE_BYTES)
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
        let in_memory = engine.in_memory;
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
            // The unreadable-version refusal: printed as is, not as
            // "Internal error: ...".
            extenddb_storage::error::StorageError::Internal(refusal) => {
                BackendError::InitializationFailed(refusal)
            }
            other => BackendError::InitializationFailed(other.to_string()),
        })?;

        // The version string matches, but a table can still be in an earlier
        // 0.0.4 build's shape (on which every DescribeTable fails). Refuse, as
        // for a version mismatch: the steps can rewrite a table (the
        // backup_items rebuild locks the database for writes while it runs),
        // so they run under `extenddb migrate --yes`, not at startup.
        let pending = schema::pending_shape_migrations(&engine.pool)
            .await
            .map_err(|e| {
                use extenddb_storage::management_store::OpError;
                let (OpError::Validation(m)
                | OpError::AlreadyExists(m)
                | OpError::NotFound(m)
                | OpError::HasDependents(m)
                | OpError::Internal(m)) = e;
                BackendError::InitializationFailed(m)
            })?;
        if !pending.is_empty() {
            return Err(BackendError::InitializationFailed(format!(
                "the catalog's tables need migration steps this server does not run at \
                 startup: {}. Stop the server and run 'extenddb migrate --yes'.",
                pending.join(", ")
            )));
        }

        // Recover any in-flight control-plane transitions from a prior run.
        match engine.process_control_plane_transitions().await {
            Ok(transitions) => {
                for (name, transition) in &transitions {
                    tracing::info!("Recovered table '{name}': {transition}");
                }
            }
            Err(e) => tracing::error!("Failed to recover control plane transitions: {e}"),
        }

        // Remove any restore target a prior crash left mid-copy. Before the
        // server takes requests, so no restore can be in flight.
        match engine.sweep_abandoned_restores().await {
            Ok(names) => {
                for name in &names {
                    tracing::warn!(
                        "removed table '{name}': its restore did not finish before the last \
                         shutdown"
                    );
                }
            }
            Err(e) => tracing::error!("Failed to sweep abandoned restores: {e}"),
        }

        // Likewise a backup a prior crash left CREATING: its items were only
        // partly copied, and nothing will finish them.
        match engine.sweep_incomplete_backups().await {
            Ok(arns) => {
                for arn in &arns {
                    tracing::warn!(
                        "removed backup {arn}: its copy did not finish before the last shutdown"
                    );
                }
            }
            Err(e) => tracing::error!("Failed to sweep incomplete backups: {e}"),
        }

        // Rebuild any GSI left mid-backfill (status CREATING) by a prior crash.
        match engine.reconcile_incomplete_gsis().await {
            Ok(n) if n > 0 => tracing::info!("Reconciled {n} incomplete GSI(s) at startup"),
            Ok(_) => {}
            Err(e) => tracing::error!("Failed to reconcile incomplete GSIs: {e}"),
        }

        // Same for a vector index. A separate pass rather than a shared one, because
        // the two live in different catalog tables and are built by different code;
        // a failure to reconcile one must not skip the other.
        match engine.reconcile_incomplete_vector_indexes().await {
            Ok(n) if n > 0 => {
                tracing::info!("Reconciled {n} incomplete vector index(es) at startup");
            }
            Ok(_) => {}
            Err(e) => tracing::error!("Failed to reconcile incomplete vector indexes: {e}"),
        }

        engine.serve_lock = serve_lock;
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

        // The catalog store writes through the second pool over the same
        // file, so its writers must hold the SAME lock instance as the
        // engine's (design decision D1); an unshared lock would leave the two
        // pools contending at the SQLite level, where a slow commit exhausts
        // a concurrent writer's busy_timeout ("database is locked" -> 500).
        // For an in-memory database the pools are one and the sharing is
        // harmless. The credential store only reads and needs no lock.
        let catalog_store: Arc<dyn extenddb_storage::CatalogStore> =
            Arc::new(SqliteCatalogStore::with_encryption_key(
                catalog_pool.clone(),
                enc_key.clone(),
                Arc::clone(&engine.write_lock),
            ));
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

#[cfg(all(test, unix))]
mod serve_lock_tests {
    /// The server factory itself takes the lock: a second server on the same
    /// file fails to start while the first is up, and starts once it is gone.
    #[tokio::test]
    async fn a_second_server_on_one_file_is_refused() {
        let dir = std::env::temp_dir().join(format!(
            "extenddb-serve-lock-factory-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).expect("dir");
        let config = crate::config::SqliteConfig {
            path: dir.join("db.sqlite").to_string_lossy().into_owned(),
            pool_size: 2,
        };
        let mut options = extenddb_storage::server_components::ServerComponentsOptions::default();
        options.bootstrap_if_uninitialized = true;
        let first = super::sqlite_server_components_factory(&config, "us-east-1", options)
            .await
            .expect("first server");
        let Err(err) = super::sqlite_server_components_factory(&config, "us-east-1", options).await
        else {
            panic!("a second server on the same file started");
        };
        assert!(
            err.to_string().contains("another extenddb process"),
            "{err}"
        );
        drop(first);
        let again = super::sqlite_server_components_factory(&config, "us-east-1", options)
            .await
            .expect("starts once the first is gone");
        drop(again);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod shape_gate_tests {
    /// `serve` on a file database whose catalog version matches but whose
    /// `table_restores` is in the earlier 0.0.4 shape refuses to start,
    /// naming the step and `extenddb migrate --yes`; after the migrate step
    /// it starts.
    #[tokio::test]
    async fn serve_refuses_a_file_database_with_pending_shape_migrations() {
        let dir = std::env::temp_dir().join(format!(
            "extenddb-shape-gate-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).expect("dir");
        let config = crate::config::SqliteConfig {
            path: dir.join("db.sqlite").to_string_lossy().into_owned(),
            pool_size: 2,
        };
        let mut bootstrap = extenddb_storage::server_components::ServerComponentsOptions::default();
        bootstrap.bootstrap_if_uninitialized = true;
        let plain = extenddb_storage::server_components::ServerComponentsOptions::default();

        // A current database, then the earlier shape: no source_table_arn.
        drop(
            super::sqlite_server_components_factory(&config, "us-east-1", bootstrap)
                .await
                .expect("bootstrap a file database"),
        );
        {
            let pool = sqlx::SqlitePool::connect(&format!("sqlite://{}", config.path))
                .await
                .expect("open");
            sqlx::query("ALTER TABLE table_restores DROP COLUMN source_table_arn")
                .execute(&pool)
                .await
                .expect("the earlier table_restores shape");
            pool.close().await;
        }
        let Err(err) = super::sqlite_server_components_factory(&config, "us-east-1", plain).await
        else {
            panic!("serve started on the earlier shape");
        };
        assert!(
            err.to_string()
                .contains("add table_restores.source_table_arn"),
            "{err}"
        );
        assert!(err.to_string().contains("extenddb migrate --yes"), "{err}");

        let pool = sqlx::SqlitePool::connect(&format!("sqlite://{}", config.path))
            .await
            .expect("open");
        crate::schema::apply_shape_migrations(&pool)
            .await
            .expect("migrate --yes");
        pool.close().await;
        drop(
            super::sqlite_server_components_factory(&config, "us-east-1", plain)
                .await
                .expect("serve starts after migrate"),
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
