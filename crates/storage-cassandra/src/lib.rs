// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra storage backend for ExtendDB.
//!
//! This crate implements the ExtendDB storage traits using Apache Cassandra
//! as the underlying database. It provides a DynamoDB-compatible API backed
//! by Cassandra's distributed architecture.

/// Default control-plane delay (seconds) when the `control_plane_delay_seconds`
/// setting is absent or unparseable: CreateTable, the control-plane worker
/// and the console's default all read this one value.
pub(crate) const DEFAULT_CONTROL_PLANE_DELAY_SECONDS: f64 = 0.25;

/// Default secondary-index propagation delay (milliseconds) when the
/// `index_propagation_delay_ms` setting is absent or unparseable, as it is on
/// a catalog the migrations created. The same value as SQLite and PostgreSQL,
/// and the single definition used by the live read on the write path, the
/// background refresh worker, and the engine's starting value.
pub(crate) const DEFAULT_INDEX_PROPAGATION_DELAY_MS: u64 = 10;

mod admin_store;
mod authorization_store;
mod backup_catalog;
mod backup_engine;
pub mod bootstrapper;
pub mod cassandra_util;
pub mod catalog_store;
pub mod config;
pub mod create_table;
mod credential_store;
pub mod data;
mod delete_table;
pub mod engine;
pub mod gsi_queue;
mod management_store;
mod metadata_engine;
pub mod migrations;
pub mod operations;
mod password_hash_store;
pub(crate) mod propagation_hold;
mod restore_engine;
mod snapshot_engine;
mod stream_engine;
mod stream_lifecycle;
pub mod stream_util;
pub mod table_engine;
mod table_helpers;
#[doc(hidden)]
pub mod test_hooks;
pub mod ttl_worker;
mod update_table;
mod warm_throughput;
mod worker_store;
pub mod workers;

pub use backup_catalog::heartbeat_backup_row;
pub use bootstrapper::CassandraBootstrapper;
pub use catalog_store::CassandraCatalogStore;
pub use config::CassandraStorageConfig;
pub use engine::{CassandraEngine, CassandraSession};
pub use metadata_engine::TtlBackfillCursor;

use cdrs_tokio::types::IntoRustByName;
use extenddb_storage::hooks::{ServerRuntimeHooks, ServingRegistrar, WorkerContext};
use extenddb_storage::server_components::{BackendError, ServerComponents};
use std::sync::Arc;

// ============================================================================
// Backend-Specific Runtime Hooks
// ============================================================================

/// Backend-specific runtime hooks for Cassandra.
struct CassandraRuntimeHooks {
    engine: Arc<CassandraEngine>,
    control_plane_notify: Arc<tokio::sync::Notify>,
    /// The registration this server holds while it serves, which the
    /// server takes again when it is lost and releases after its workers
    /// stop.
    registration: Arc<migrations::ServerRegistration>,
}

#[async_trait::async_trait]
impl ServerRuntimeHooks for CassandraRuntimeHooks {
    async fn spawn_workers(&self, ctx: &WorkerContext) -> Vec<tokio::task::JoinHandle<()>> {
        // Backend-specific workers that need Cassandra internals. Each one
        // returns once `ctx.shutdown` fires, and its handle is returned so
        // `serve` can drain it.

        let storage_for_poller = self.engine.clone();
        let cp_notify = self.control_plane_notify.clone();
        let catalog_store = ctx.catalog_store.clone();
        let token = ctx.shutdown.clone();
        let control_plane = tokio::spawn(async move {
            workers::poll_control_plane_transitions(
                storage_for_poller,
                cp_notify,
                catalog_store,
                token,
            )
            .await;
        });

        let engine_for_recovery = self.engine.clone();
        let token = ctx.shutdown.clone();
        let transaction_recovery = tokio::spawn(async move {
            workers::poll_transaction_recovery(
                engine_for_recovery,
                std::time::Duration::from_secs(60),
                std::time::Duration::from_secs(30),
                token,
            )
            .await;
        });

        // Read the GSI delay before requests are admitted, so the atomic is
        // correct from the first request and not from the poller's first
        // read 5 s later. Bounded by `INITIAL_GSI_DELAY_READ`; past it the
        // atomic keeps its value until the poller's read.
        let gsi_delay = self.engine.gsi_default_delay_ms.clone();
        if let Some(ms) =
            workers::read_gsi_delay(&*ctx.catalog_store, workers::INITIAL_GSI_DELAY_READ).await
        {
            gsi_delay.store(ms, std::sync::atomic::Ordering::Relaxed);
        }
        let catalog_store_for_gsi = ctx.catalog_store.clone();
        let token = ctx.shutdown.clone();
        let gsi_delay_poller = tokio::spawn(async move {
            workers::poll_gsi_delay(catalog_store_for_gsi, gsi_delay, token).await;
        });

        // GSI propagation workers (one per partition).
        let gsi_propagation = workers::spawn_gsi_workers_until(self.engine.clone(), &ctx.shutdown);

        // GSI backfill recovery worker.
        let engine_for_gsi_recovery = self.engine.clone();
        let gsi_backfill_notify = self.engine.gsi_backfill_notify.clone();
        let token = ctx.shutdown.clone();
        let gsi_backfill_recovery = tokio::spawn(async move {
            workers::poll_gsi_backfill_recovery(
                engine_for_gsi_recovery,
                gsi_backfill_notify,
                std::time::Duration::from_secs(120), // stale threshold
                std::time::Duration::from_secs(60),  // scan interval backstop
                None,                                // every account keyspace
                token,
            )
            .await;
        });

        let ttl_engine = self.engine.clone();
        let ttl_metrics = ctx.metrics.clone();
        let ttl_shutdown = ctx.shutdown.clone();
        let ttl_cleanup = tokio::spawn(async move {
            ttl_worker::ttl_cleanup_worker(ttl_engine, ttl_metrics, ttl_shutdown).await;
        });
        let ttl_repair_engine = self.engine.clone();
        let ttl_repair_metrics = ctx.metrics.clone();
        let ttl_repair_shutdown = ctx.shutdown.clone();
        let ttl_repair = tokio::spawn(async move {
            ttl_worker::ttl_repair_worker(
                ttl_repair_engine,
                ttl_repair_metrics,
                ttl_repair_shutdown,
            )
            .await;
        });
        let ttl_audit_engine = self.engine.clone();
        let ttl_audit_metrics = ctx.metrics.clone();
        let ttl_audit_shutdown = ctx.shutdown.clone();
        let ttl_audit = tokio::spawn(async move {
            ttl_worker::ttl_audit_worker(ttl_audit_engine, ttl_audit_metrics, ttl_audit_shutdown)
                .await
        });

        let mut handles = vec![
            control_plane,
            transaction_recovery,
            gsi_delay_poller,
            gsi_backfill_recovery,
            ttl_cleanup,
            ttl_repair,
            ttl_audit,
        ];
        handles.extend(gsi_propagation);
        handles
    }

    fn backend_info(&self) -> Option<String> {
        Some(format!("keyspace_prefix={}", self.engine.keyspace_prefix))
    }

    /// The registration's row is lost when it cannot be renewed for 30 s;
    /// the server then registers again or stops.
    fn serving_registrar(&self) -> Option<Arc<dyn ServingRegistrar>> {
        Some(self.registration.clone() as Arc<dyn ServingRegistrar>)
    }
}

// ============================================================================
// Backend Registration
// ============================================================================

/// Returns the Cassandra storage backend descriptor.
///
/// The thin `main` installs it before dispatching any subcommand:
///
/// ```ignore
/// extenddb_storage::set_backend(extenddb_storage_cassandra::backend())?;
/// ```
#[must_use]
pub fn backend() -> extenddb_storage::Backend {
    extenddb_storage::Backend {
        name: "cassandra",
        bootstrapper: |config_path, cli_args| {
            Box::pin(async move {
                let store = CassandraBootstrapper::from_config(&config_path, &cli_args).await?;
                Ok(Box::new(store) as Box<dyn extenddb_storage::bootstrapper::Bootstrapper>)
            })
        },
        operations: &operations::CassandraOperationsEngine,
        storage_config: |table| {
            let mut config: CassandraStorageConfig = table
                .clone()
                .try_into()
                .map_err(|e: toml::de::Error| format!("Failed to parse cassandra config: {e}"))?;
            config.ensure_cached_connection_string();
            Ok(Box::new(config) as Box<dyn extenddb_storage::config::StorageConfig>)
        },
        settings_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let catalog_store = make_catalog_store_from_connection_string(&connection_string)
                    .await
                    .map_err(
                        extenddb_storage::settings_store::SettingsStoreError::ConnectionFailed,
                    )?;
                Ok(Box::new(catalog_store)
                    as Box<
                        dyn extenddb_storage::management_store::SettingsStore,
                    >)
            })
        },
        management_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let catalog_store = make_catalog_store_from_connection_string(&connection_string)
                    .await
                    .map_err(
                        extenddb_storage::management_store_factory::ManagementStoreError::ConnectionFailed,
                    )?;
                Ok(Box::new(catalog_store)
                    as Box<
                        dyn extenddb_storage::management_store::ManagementStore,
                    >)
            })
        },
        diagnostics_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let catalog_store = make_catalog_store_from_connection_string(&connection_string)
                    .await
                    .map_err(extenddb_storage::diagnostics_store::DiagnosticsStoreError::ConnectionFailed)?;
                Ok(Box::new(catalog_store)
                    as Box<dyn extenddb_storage::diagnostics::DiagnosticsStore>)
            })
        },
        server_components: server_components_factory,
        catalog_migrations: &migrations::CassandraCatalogMigrations,
    }
}

/// Build a `CassandraCatalogStore` from a bare connection string.
///
/// Used by the `settings_store`, `management_store` and `diagnostics_store`
/// factories, which receive only a connection string (no full config
/// object): `extenddb settings`, `verify` and the diagnostics. The string
/// carries the role the config file names, so they connect as the server
/// does, with its 10 s bound on the connect.
async fn make_catalog_store_from_connection_string(
    connection_string: &str,
) -> Result<CassandraCatalogStore, String> {
    let deployment = migrations::Deployment::from_target(
        &extenddb_storage::migrations::Target::at(connection_string),
    )
    .map_err(|e| e.to_string())?;
    let session = deployment.connect().await.map_err(|e| e.to_string())?;
    Ok(CassandraCatalogStore::catalog_only(
        session,
        deployment.keyspace_prefix,
    ))
}

/// The warnings for the deployment's keyspaces whose replication differs
/// from the config's `replication_factor` and `datacenter`. `mismatches`
/// holds each such keyspace with its replication, and `datacenters` the
/// datacenters of the cluster's nodes.
///
/// Either the keyspace or the config can be wrong. When the config's
/// datacenter is not one of the cluster's, the config is wrong, and one
/// warning names the keyspaces and says to fix `datacenter`. Otherwise each
/// keyspace gets a warning that gives the fix for each case, with the steps
/// in the order that keeps every write for that keyspace's change
/// ([`migrations::alter_steps`]).
pub(crate) fn replication_warnings(
    mismatches: &[(String, String, migrations::ReplicationChange)],
    datacenters: &std::collections::BTreeSet<String>,
    datacenter: &str,
    replication_factor: u32,
) -> Vec<String> {
    if mismatches.is_empty() {
        return Vec::new();
    }
    let expected = catalog_store::network_topology(datacenter, replication_factor);
    if !datacenters.contains(datacenter) {
        let keyspaces: Vec<&str> = mismatches.iter().map(|(k, _, _)| k.as_str()).collect();
        let known: Vec<&str> = datacenters.iter().map(String::as_str).collect();
        return vec![format!(
            "The config sets datacenter = \"{datacenter}\", but this cluster's datacenters \
             are {}. Set datacenter in [storage.cassandra] to the datacenter of the \
             keyspaces {}. Do not change the keyspaces to match the config \
             (docs/cassandra-setup.md, Keyspace replication).",
            known.join(", "),
            keyspaces.join(", ")
        )];
    }
    mismatches
        .iter()
        .map(|(keyspace, found, change)| {
            format!(
                "Keyspace {keyspace} has replication {found}, but the config asks for \
                 {expected}. If the keyspace is right, set replication_factor and \
                 datacenter in [storage.cassandra] to match it. If the config is right, \
                 {} (docs/cassandra-setup.md, Keyspace replication).",
                migrations::alter_steps(*change)
            )
        })
        .collect()
}

/// Log a warning for each of the deployment's keyspaces whose replication
/// differs from the config's `replication_factor` and `datacenter`
/// ([`replication_warnings`]), and return the warnings. `serve` starts
/// anyway: the keyspaces still work. A failed read is logged and skipped.
async fn warn_of_replication_mismatches(
    session: &CassandraSession,
    cassandra_config: &CassandraStorageConfig,
) -> Vec<String> {
    let read = async {
        let mismatches = migrations::replication_mismatches(
            session,
            &cassandra_config.keyspace_prefix,
            &cassandra_config.datacenter,
            cassandra_config.replication_factor,
        )
        .await?;
        let datacenters = migrations::cluster_datacenters(session).await?;
        Ok::<_, extenddb_storage::migrations::MigrationError>(replication_warnings(
            &mismatches,
            &datacenters,
            &cassandra_config.datacenter,
            cassandra_config.replication_factor,
        ))
    };
    match read.await {
        Ok(warnings) => {
            for warning in &warnings {
                tracing::warn!("{warning}");
            }
            warnings
        }
        Err(e) => {
            let warning = format!("Could not check the keyspaces' replication: {e}");
            tracing::warn!("{warning}");
            vec![warning]
        }
    }
}

/// The warnings that `serve` logs at start-up for the deployment's keyspaces
/// at another replication, for the integration tests.
#[cfg(any(test, feature = "test-util"))]
pub async fn startup_replication_warnings(
    session: &CassandraSession,
    cassandra_config: &CassandraStorageConfig,
) -> Vec<String> {
    warn_of_replication_mismatches(session, cassandra_config).await
}

fn server_components_factory(
    config: &dyn extenddb_storage::config::StorageConfig,
    region: &str,
    // `bootstrap_if_uninitialized` is not honored: an uninitialized catalog
    // fails with the explicit-`init` guidance, as on PostgreSQL.
    options: extenddb_storage::server_components::ServerComponentsOptions,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<ServerComponents, BackendError>> + Send>,
> {
    let config = config.clone_box();
    let region = region.to_string();

    Box::pin(async move {
        let cassandra_config = config
            .as_any()
            .downcast_ref::<CassandraStorageConfig>()
            .ok_or_else(|| {
                BackendError::InitializationFailed("Expected CassandraStorageConfig".to_string())
            })?;

        // Register and read the catalog's status before the engine starts:
        // creating it runs the control-plane recovery, which writes to the
        // catalog. The status session uses the engine's role and settings.
        let deployment = migrations::Deployment::from_config(cassandra_config);
        let status_session =
            deployment
                .connect()
                .await
                .map_err(|e| BackendError::ConnectionFailed {
                    backend: "cassandra".to_string(),
                    details: e.to_string(),
                })?;
        let host = cassandra_config
            .instance_id
            .clone()
            .unwrap_or_else(|| "unknown".to_owned());
        // Every caller registers, a command too, so a migration does not run
        // under one; a command's registration carries its own name.
        let registration = migrations::register_server_as(
            status_session,
            &cassandra_config.keyspace_prefix,
            &host,
            options.release,
            options.command.unwrap_or(migrations::SERVE_COMMAND),
        )
        .await
        .map_err(BackendError::Catalog)?;
        let registration = Arc::new(registration);

        let engine = match CassandraEngine::new(cassandra_config, &region).await {
            Ok(engine) => engine,
            Err(e) => {
                ServingRegistrar::release(registration.as_ref()).await;
                return Err(BackendError::ConnectionFailed {
                    backend: "cassandra".to_string(),
                    details: e.to_string(),
                });
            }
        };

        if options.command.is_none() {
            let _ = warn_of_replication_mismatches(engine.session(), cassandra_config).await;
        }

        let control_plane_notify = engine.control_plane_notify.clone();
        let engine = Arc::new(engine);

        // The startup recovery is a server's; a command reading beside one,
        // or one that writes beside the live servers (`backup purge`,
        // `options.startup_recovery` off), leaves it to the server.
        if !options.beside_server && options.startup_recovery {
            match engine.process_control_plane_transitions().await {
                Ok(ref t) if t.is_empty() => {}
                Ok(transitions) => {
                    for (name, transition) in &transitions {
                        tracing::info!("Recovered table '{name}': {transition}");
                    }
                }
                Err(e) => tracing::error!("Failed to recover control plane transitions: {e}"),
            }
        }

        // Load encryption key from catalog.
        let catalog_keyspace = format!("{}_catalog", cassandra_config.keyspace_prefix);
        let enc_key_query =
            format!("SELECT value FROM {catalog_keyspace}.settings WHERE key = 'encryption_key'");
        let enc_key_result = engine.session.query(&enc_key_query).await.map_err(|e| {
            BackendError::InitializationFailed(format!("Failed to load encryption key: {e}"))
        })?;
        let enc_key_body = enc_key_result.response_body().map_err(|e| {
            BackendError::InitializationFailed(format!(
                "Failed to parse encryption key response: {e}"
            ))
        })?;
        let enc_key_rows = enc_key_body
            .into_rows()
            .ok_or(BackendError::MissingEncryptionKey)?;
        let enc_key_row = enc_key_rows
            .into_iter()
            .next()
            .ok_or(BackendError::MissingEncryptionKey)?;
        let enc_key: String = enc_key_row.get_r_by_name("value").map_err(|e| {
            BackendError::InitializationFailed(format!("Failed to parse encryption key: {e}"))
        })?;

        let catalog_store = Arc::new(
            CassandraCatalogStore::with_encryption_key(
                engine.session.clone(),
                cassandra_config.keyspace_prefix.clone(),
                cassandra_config.datacenter.clone(),
                cassandra_config.replication_factor,
                enc_key.clone(),
            )
            .recording_release(options.release),
        );

        let credential_store: Arc<dyn extenddb_auth::CredentialStore> =
            Arc::new(credential_store::CassandraCredentialStore::new(
                engine.session.clone(),
                cassandra_config.keyspace_prefix.clone(),
                enc_key,
            ));

        let runtime_hooks = Box::new(CassandraRuntimeHooks {
            engine: engine.clone(),
            control_plane_notify,
            registration,
        });

        Ok(ServerComponents {
            engine: engine as Arc<dyn extenddb_storage::StorageEngine>,
            catalog_store: catalog_store as Arc<dyn extenddb_storage::CatalogStore>,
            credential_store,
            runtime_hooks: Some(runtime_hooks),
        })
    })
}

#[cfg(test)]
mod replication_warning_tests {
    use super::replication_warnings;
    use crate::migrations::ReplicationChange;
    use std::collections::BTreeSet;

    fn datacenters(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|n| (*n).to_owned()).collect()
    }

    fn mismatch(
        keyspace: &str,
        found: &str,
        change: ReplicationChange,
    ) -> (String, String, ReplicationChange) {
        (keyspace.to_owned(), found.to_owned(), change)
    }

    /// Where `first` and `second` appear in `text`, in that order.
    fn in_order(text: &str, first: &str, second: &str) -> bool {
        matches!((text.find(first), text.find(second)), (Some(a), Some(b)) if a < b)
    }

    #[test]
    fn no_mismatch_gives_no_warning() {
        assert!(replication_warnings(&[], &datacenters(&["datacenter1"]), "dc2", 1).is_empty());
    }

    #[test]
    fn a_datacenter_the_cluster_lacks_blames_the_config() {
        let mismatches = [
            mismatch(
                "p_catalog",
                "{'datacenter1': '1'}",
                ReplicationChange::Other,
            ),
            mismatch(
                "p_account_111111111111",
                "{'datacenter1': '1'}",
                ReplicationChange::Other,
            ),
        ];
        let warnings = replication_warnings(
            &mismatches,
            &datacenters(&["datacenter1"]),
            "datacenter2",
            1,
        );
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        let warning = &warnings[0];
        for part in [
            "datacenter = \"datacenter2\"",
            "datacenters are datacenter1",
            "Set datacenter in [storage.cassandra]",
            "p_catalog, p_account_111111111111",
            "Do not change the keyspaces",
        ] {
            assert!(warning.contains(part), "{part}: {warning}");
        }
        assert!(!warning.contains("ALTER KEYSPACE"), "{warning}");
    }

    #[test]
    fn each_keyspace_gets_both_fixes() {
        let mismatches = [
            mismatch(
                "p_catalog",
                "{'datacenter1': '1'}",
                ReplicationChange::Raise,
            ),
            mismatch(
                "p_account_111111111111",
                "{'datacenter1': '1'}",
                ReplicationChange::Raise,
            ),
        ];
        let warnings = replication_warnings(
            &mismatches,
            &datacenters(&["datacenter1"]),
            "datacenter1",
            3,
        );
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        for (warning, keyspace) in warnings.iter().zip(["p_catalog", "p_account_111111111111"]) {
            for part in [
                format!("Keyspace {keyspace} has replication {{'datacenter1': '1'}}"),
                "If the keyspace is right, set replication_factor and datacenter".to_owned(),
                "If the config is right,".to_owned(),
            ] {
                assert!(warning.contains(&part), "{part}: {warning}");
            }
        }
    }

    /// The steps for a raise repair after the ALTER, so the new replicas get
    /// the data. The steps for a lower repair before it, so each remaining
    /// replica holds every write. A move to another datacenter gets no ALTER
    /// and repair steps: the new replicas have no copy to repair from.
    #[test]
    fn the_steps_keep_every_write_for_each_change() {
        let warning = |found: &str, change, datacenter: &str, replication_factor| {
            replication_warnings(
                &[mismatch("p_catalog", found, change)],
                &datacenters(&["dc1", "dc2"]),
                datacenter,
                replication_factor,
            )
            .remove(0)
        };

        let raise = warning("{'dc1': '1'}", ReplicationChange::Raise, "dc1", 3);
        assert!(
            in_order(&raise, "ALTER KEYSPACE", "nodetool repair -full"),
            "{raise}"
        );

        let lower = warning("{'dc1': '3'}", ReplicationChange::Lower, "dc1", 1);
        assert!(
            in_order(&lower, "nodetool repair -full", "ALTER KEYSPACE")
                && in_order(&lower, "ALTER KEYSPACE", "nodetool cleanup"),
            "{lower}"
        );

        let other = warning("{'dc1': '3'}", ReplicationChange::Other, "dc2", 3);
        assert!(!other.contains("nodetool repair"), "{other}");
        assert!(
            other.contains("as the Cassandra documentation describes"),
            "{other}"
        );

        // Each change starts with stopping the servers.
        for steps in [&raise, &lower, &other] {
            assert!(
                steps.contains("If the config is right, stop every ExtendDB server"),
                "{steps}"
            );
        }
    }
}
