// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `MongoDB` storage backend for extenddb.
//!
//! Implements the storage traits from `extenddb-storage` using `MongoDB`
//! as the backing store.

mod admin_store;
mod authorization_store;
mod backup_engine;
pub use backup_engine::BackupSweep;
mod bootstrapper;
mod catalog_store;
pub mod condition;
pub mod config;
mod credential_store;
mod data;
mod data_engine;
mod error_class;
mod management_store;
mod metadata_engine;
mod operations;
pub mod pushdown;
mod snapshot_engine;
mod stream_engine;
mod table_engine;
mod ttl_worker;
mod worker_store;

pub use bootstrapper::MongoBootstrapper;
pub use catalog_store::MongoCatalogStore;
pub use config::MongoStorageConfig;
pub use credential_store::MongoCredentialStore;

use std::sync::Arc;

use extenddb_storage::error::StorageError;

// ============================================================================
// Backend registration
// ============================================================================

use extenddb_storage::hooks::{ServerRuntimeHooks, WorkerContext};
use extenddb_storage::server_components::{BackendError, ServerComponents};

/// Backend-specific runtime hooks for `MongoDB`.
struct MongoRuntimeHooks {
    engine: Arc<MongoEngine>,
}

#[async_trait::async_trait]
impl ServerRuntimeHooks for MongoRuntimeHooks {
    async fn spawn_workers(&self, ctx: &WorkerContext) -> Vec<tokio::task::JoinHandle<()>> {
        let storage_for_ttl = self.engine.clone();
        let metrics = ctx.metrics.clone();
        let ttl = tokio::spawn(async move {
            ttl_worker::ttl_cleanup_worker(storage_for_ttl, metrics).await;
        });
        let storage_for_stream = self.engine.clone();
        let stream = tokio::spawn(async move {
            ttl_worker::stream_record_cleanup_worker(storage_for_stream).await;
        });
        let storage_for_backfill = self.engine.clone();
        let backfill = tokio::spawn(async move {
            ttl_worker::gsi_backfill_worker(storage_for_backfill).await;
        });
        let storage_for_control_plane = self.engine.clone();
        let control_plane = tokio::spawn(async move {
            ttl_worker::control_plane_worker(storage_for_control_plane).await;
        });
        tracing::info!(
            "MongoDB backend: TTL, stream cleanup, GSI backfill, and control-plane workers spawned"
        );
        vec![ttl, stream, backfill, control_plane]
    }

    fn backend_info(&self) -> Option<String> {
        Some("mongodb".to_string())
    }
}

/// Build the assembled server components for the mongo backend (`serve`).
fn server_components_factory(
    config: &(dyn extenddb_storage::config::StorageConfig + 'static),
    region: &str,
    // MongoDB bootstrap needs operator input (databases, admin credentials), so
    // `bootstrap_if_uninitialized` is not honored here: an uninitialized catalog
    // fails with the explicit-`init` guidance, matching the PostgreSQL backend.
    _options: extenddb_storage::server_components::ServerComponentsOptions,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<ServerComponents, BackendError>> + Send>,
> {
    let connection_string = config.connection_config().to_string();
    let max_connections = config.max_connections_override();
    let max_catalog_connections = config.max_catalog_connections_override();
    let region = region.to_string();
    // Backend-specific settings (transaction_read_concern) aren't exposed on
    // the generic StorageConfig trait, so downcast to the concrete mongo
    // config registered alongside this factory.
    let raw_read_concern = config
        .as_any()
        .downcast_ref::<config::MongoStorageConfig>()
        .map(|c| c.transaction_read_concern.clone())
        .ok_or_else(|| {
            BackendError::InitializationFailed(
                "MongoDB server component factory received a non-MongoStorageConfig; \
                 backend registration invariant violated"
                    .to_owned(),
            )
        });
    Box::pin(async move {
        let raw_read_concern = raw_read_concern?;
        let tx_read_concern = config::parse_transaction_read_concern(&raw_read_concern)
            .map_err(BackendError::InitializationFailed)?;

        // Create MongoEngine
        let engine = MongoEngine::new(
            &connection_string,
            &region,
            max_connections,
            tx_read_concern,
        )
        .await
        .map_err(|e| BackendError::ConnectionFailed {
            backend: "mongodb".to_string(),
            details: e.to_string(),
        })?;

        // A crash can leave a backup's metadata claimed by a restore or a
        // delete that will never finish, or a restore target nothing will
        // finish. The sweep removes only state whose heartbeat has stopped, so
        // it is safe while another process shares this catalog; the GSI
        // backfill worker repeats it periodically.
        match engine.sweep_stale_backup_claims().await {
            Ok(sweep) if sweep.is_empty() => {}
            Ok(sweep) => tracing::warn!("startup backup sweep: {sweep}"),
            Err(e) => tracing::error!("Failed to sweep stale backup claims: {e}"),
        }

        let engine = Arc::new(engine);

        // Create one shared catalog/management client. MongoDB Client clones
        // share the underlying pool, so max_catalog_connections limits the
        // combined catalog and auth traffic rather than creating two
        // independent pools with twice the configured ceiling.
        let catalog_client = connect_guarded(&connection_string, max_catalog_connections, false)
            .await
            .map_err(|e| BackendError::ConnectionFailed {
                backend: "mongodb".to_string(),
                details: format!("Failed to create catalog client: {e}"),
            })?;

        // Load encryption key from settings collection. A missing key must be
        // a hard failure: an empty key would base64-decode to zero bytes and
        // panic in aes_gcm (`Key::from_slice` requires 32 bytes). Refuse to
        // start instead, matching the postgres backend.
        let catalog_db = catalog_client.database("extenddb_catalog");
        let settings_coll = catalog_db.collection::<mongodb::bson::Document>("settings");
        let enc_key = settings_coll
            .find_one(mongodb::bson::doc! { "_id": "encryption_key" })
            .await
            .map_err(|e| BackendError::InitializationFailed(format!("Load encryption key: {e}")))?
            .and_then(|d| d.get_str("value").ok().map(std::borrow::ToOwned::to_owned))
            .ok_or(BackendError::MissingEncryptionKey)?;

        let auth_client = catalog_client.clone();
        let catalog_store = Arc::new(MongoCatalogStore::with_encryption_key(
            catalog_client,
            enc_key.clone(),
        )) as Arc<dyn extenddb_storage::CatalogStore>;

        // Create credential store. The bin layer wraps this in
        // CachedCredentialStore using the operator-configured TTL
        // before constructing the auth provider.
        let cred_store: Arc<dyn extenddb_auth::CredentialStore> =
            Arc::new(MongoCredentialStore::new(auth_client, enc_key));

        // Create runtime hooks
        let runtime_hooks = Box::new(MongoRuntimeHooks {
            engine: engine.clone(),
        });

        Ok(ServerComponents {
            engine,
            catalog_store,
            credential_store: cred_store,
            runtime_hooks: Some(runtime_hooks),
        })
    })
}

/// The MongoDB storage backend. A thin bin installs it via
/// `extenddb_storage::set_backend(extenddb_storage_mongodb::backend())`.
pub fn backend() -> extenddb_storage::Backend {
    extenddb_storage::Backend {
        name: "mongodb",
        bootstrapper: |config_path, cli_args| {
            Box::pin(async move {
                let store = MongoBootstrapper::from_config(&config_path, &cli_args).await?;
                Ok(Box::new(store) as Box<dyn extenddb_storage::bootstrapper::Bootstrapper>)
            })
        },
        storage_config: |table| {
            let config: MongoStorageConfig = table
                .clone()
                .try_into()
                .map_err(|e: toml::de::Error| format!("Failed to parse mongodb config: {e}"))?;
            Ok(Box::new(config) as Box<dyn extenddb_storage::config::StorageConfig>)
        },
        operations: &operations::MongoOperationsEngine,
        settings_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let client = connect_guarded(&connection_string, None, false)
                    .await
                    .map_err(|e| {
                        extenddb_storage::settings_store::SettingsStoreError::ConnectionFailed(
                            e.to_string(),
                        )
                    })?;
                Ok(Box::new(MongoCatalogStore::new(client))
                    as Box<
                        dyn extenddb_storage::management_store::SettingsStore,
                    >)
            })
        },
        management_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let client = connect_guarded(&connection_string, None, false)
                    .await
                    .map_err(|e| {
                        extenddb_storage::management_store_factory::ManagementStoreError::ConnectionFailed(
                            e.to_string(),
                        )
                    })?;
                Ok(Box::new(MongoCatalogStore::new(client))
                    as Box<
                        dyn extenddb_storage::management_store::ManagementStore,
                    >)
            })
        },
        diagnostics_store: |connection_string| {
            let connection_string = connection_string.to_string();
            Box::pin(async move {
                let client = connect_guarded(&connection_string, None, false)
                    .await
                    .map_err(|e| {
                        extenddb_storage::diagnostics_store::DiagnosticsStoreError::ConnectionFailed(
                            e.to_string(),
                        )
                    })?;
                Ok(Box::new(MongoCatalogStore::new(client))
                    as Box<dyn extenddb_storage::diagnostics::DiagnosticsStore>)
            })
        },
        server_components: server_components_factory,
    }
}

// ============================================================================
// MongoEngine
// ============================================================================

/// TTL for entries in [`MongoEngine::gsi_cache`].
///
/// The GSI cache is per-process. When multiple ExtendDB instances share a
/// catalog, an admin creating or dropping a GSI on instance A does not
/// invalidate instance B's cache. Bounding cache entries by wall-clock age
/// gives eventual convergence at a small cost (one catalog `find` per table
/// per TTL window), which is far cheaper than the cost of silently skipping
/// index updates on tables where GSIs were added out-of-band.
const GSI_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// Per-process cache of whether a table has any GSIs.
///
/// The generation map prevents a slow catalog observation from overwriting a
/// newer local mutation. For example, a write can observe no indexes while an
/// UpdateTable concurrently creates one; the observation must not publish
/// `false` after the create has published `true`.
#[derive(Default)]
struct GsiCache {
    entries: dashmap::DashMap<String, (bool, std::time::Instant)>,
    generations: dashmap::DashMap<String, u64>,
}

impl GsiCache {
    fn get_fresh(&self, table_id: &str) -> Option<bool> {
        let entry = self.entries.get(table_id)?;
        let (has_gsi, inserted) = *entry;
        if inserted.elapsed() <= GSI_CACHE_TTL {
            Some(has_gsi)
        } else {
            None
        }
    }

    /// Return the generation that a catalog observation should validate
    /// before publishing its result.
    fn generation(&self, table_id: &str) -> u64 {
        self.generations
            .get(table_id)
            .map(|generation| *generation)
            .unwrap_or(0)
    }

    /// Record a local catalog mutation and advance the generation.
    fn set(&self, table_id: &str, has_gsi: bool) {
        let mut generation = self.generations.entry(table_id.to_owned()).or_insert(0);
        *generation = generation.saturating_add(1);
        self.entries
            .insert(table_id.to_owned(), (has_gsi, std::time::Instant::now()));
    }

    /// Publish a catalog observation only if no newer local mutation occurred
    /// while the observation was in flight.
    fn set_if_generation(&self, table_id: &str, expected_generation: u64, has_gsi: bool) -> bool {
        let generation = self.generations.entry(table_id.to_owned()).or_insert(0);
        if *generation != expected_generation {
            return false;
        }

        self.entries
            .insert(table_id.to_owned(), (has_gsi, std::time::Instant::now()));
        true
    }

    /// Remove a cache entry and advance its generation so in-flight catalog
    /// observations cannot repopulate it with stale data.
    fn invalidate(&self, table_id: &str) {
        let mut generation = self.generations.entry(table_id.to_owned()).or_insert(0);
        *generation = generation.saturating_add(1);
        self.entries.remove(table_id);
    }
}

/// `MongoDB` storage backend.
///
/// Cloning is cheap and shares the connection pool and the GSI cache; a
/// detached restore copy owns a clone so it outlives the request future.
#[derive(Clone)]
pub struct MongoEngine {
    client: mongodb::Client,
    pub(crate) catalog_db: mongodb::Database,
    data_db: mongodb::Database,
    region: String,
    /// Cache of `table_id` -> (`has_gsi`, insertion time). Avoids catalog
    /// queries on every write for tables with no GSIs. Entries older than
    /// [`GSI_CACHE_TTL`] are treated as misses and re-read from the catalog,
    /// so GSI additions/removals on other ExtendDB instances converge within
    /// the TTL window.
    gsi_cache: Arc<GsiCache>,
    /// Read concern applied to every multi-document transaction this engine
    /// opens. Defaults to `snapshot` (real MongoDB); configurable via
    /// [`crate::config::MongoStorageConfig::transaction_read_concern`] for
    /// MongoDB-wire-compatible backends that don't support snapshot reads.
    tx_read_concern: mongodb::options::ReadConcern,
    /// Heartbeat timing for backup claims and restore targets
    /// (see [`backup_engine`]).
    pub(crate) backup_liveness: backup_engine::BackupLiveness,
    /// Restores this process may run at once across accounts, and the
    /// semaphore that enforces it; see
    /// [`backup_engine::restore_server_slot_count`].
    pub(crate) restore_server_slot_count: usize,
    pub(crate) restore_server_slots: Arc<tokio::sync::Semaphore>,
    /// Restores each account may run at once in this process; see
    /// [`backup_engine::RestoreAccountSlots`].
    pub(crate) restore_account_slots: backup_engine::RestoreAccountSlots,
}

/// Build a MongoDB client from a connection string, applying the shared
/// connection guards so every client in the backend is protected, not just the
/// data client: reject non-primary read preferences (they route reads to
/// replicas and silently break `ConsistentRead=true`), and reject a write
/// concern other than `majority` (see [`check_write_concern`]).
///
/// `max_pool_size` is applied when provided. The long-lived data, catalog, and
/// authentication clients pass their explicitly configured pool limits;
/// short-lived CLI and bootstrapper clients pass `None`, preserving any URI
/// option.
///
/// `warn_on_no_tls` gates the no-TLS warning to the long-running server data
/// client only. Short-lived CLI/management clients (settings, catalog checks,
/// bootstrapper) pass `false`: they all share the same connection string, so a
/// single warning at server startup is enough, and emitting it on every CLI
/// invocation both spams logs and pollutes command stdout that tooling parses.
pub(crate) async fn connect_guarded(
    connection_string: &str,
    max_pool_size: Option<u32>,
    warn_on_no_tls: bool,
) -> Result<mongodb::Client, StorageError> {
    let options = guarded_client_options(connection_string, max_pool_size, warn_on_no_tls).await?;
    mongodb::Client::with_options(options).map_err(|e| StorageError::Connection(e.to_string()))
}

/// The checked [`mongodb::options::ClientOptions`] behind [`connect_guarded`],
/// for a caller that needs to read an effective option (the pool size) before
/// building the client.
async fn guarded_client_options(
    connection_string: &str,
    max_pool_size: Option<u32>,
    warn_on_no_tls: bool,
) -> Result<mongodb::options::ClientOptions, StorageError> {
    let options = client_options_with_pool_size(connection_string, max_pool_size).await?;

    if let Some(sel) = options.selection_criteria.as_ref() {
        use mongodb::options::{ReadPreference, SelectionCriteria};
        let is_non_primary = match sel {
            SelectionCriteria::ReadPreference(rp) => !matches!(rp, ReadPreference::Primary),
            _ => false,
        };
        if is_non_primary {
            return Err(StorageError::Connection(
                "MongoDB connection string must use readPreference=primary. \
                 Non-primary read preferences (secondary, secondaryPreferred, \
                 nearest, primaryPreferred) route reads to replicas and \
                 silently break ConsistentRead=true."
                    .to_owned(),
            ));
        }
    }

    check_write_concern(options.write_concern.as_ref())?;

    if warn_on_no_tls && !matches!(options.tls, Some(mongodb::options::Tls::Enabled(_))) {
        tracing::warn!(
            "MongoDB connection is not using TLS; credentials and data will \
                 traverse the network in cleartext. Enable TLS with `?tls=true` \
             in the connection string, or use a `mongodb+srv://` URI."
        );
    }

    Ok(options)
}

/// The connection pool size the driver will use for `options`: the explicit
/// `maxPoolSize`, else the driver's default of 10 (`mongodb` 3.x). The driver
/// does not enforce `waitQueueTimeoutMS` (its `ClientOptions::wait_queue_timeout`
/// documents it as not supported), so a checkout waits for as long as every
/// connection stays checked out; the work this backend admits at once is
/// therefore bounded from the pool size instead (see
/// [`backup_engine::restore_server_slot_count`]).
pub(crate) fn effective_pool_size(options: &mongodb::options::ClientOptions) -> u32 {
    options
        .max_pool_size
        .unwrap_or(DRIVER_DEFAULT_MAX_POOL_SIZE)
}

/// `mongodb::cmap::DEFAULT_MAX_POOL_SIZE`, which the driver does not export.
const DRIVER_DEFAULT_MAX_POOL_SIZE: u32 = 10;

async fn client_options_with_pool_size(
    connection_string: &str,
    max_pool_size: Option<u32>,
) -> Result<mongodb::options::ClientOptions, StorageError> {
    let mut options = mongodb::options::ClientOptions::parse(connection_string)
        .await
        .map_err(|e| StorageError::Connection(e.to_string()))?;
    apply_max_pool_size(&mut options, max_pool_size);

    Ok(options)
}

fn apply_max_pool_size(options: &mut mongodb::options::ClientOptions, max_pool_size: Option<u32>) {
    if let Some(n) = max_pool_size {
        options.max_pool_size = Some(n);
    }
}

/// Refuse a connection-string write concern weaker than `majority`.
///
/// The backup claim protocol (DeleteBackup, restores and the stale-claim
/// sweep, see [`backup_engine`]) treats an acknowledged claim or CREATING
/// target as permanent. A write acknowledged under `w=1` (or any node count or
/// tag, whose relation to a majority this cannot see) can be rolled back when
/// the primary fails over, and `journal=false` lets a majority acknowledge a
/// write none of them has journaled. Accepted: no write concern (the server
/// default, `majority` on a replica set without arbiters), `w=majority`,
/// either with `wtimeoutMS`, and `journal=true`.
///
/// The catalog and the backup and restore copies use `majority` whatever the
/// connection string or the server default say (see
/// [`majority_write_concern`]); this check keeps a weaker setting in the
/// connection string from being silently overridden there and honoured
/// everywhere else.
fn check_write_concern(wc: Option<&mongodb::options::WriteConcern>) -> Result<(), StorageError> {
    use mongodb::options::Acknowledgment;
    let Some(wc) = wc else {
        return Ok(());
    };
    let weak = match &wc.w {
        None | Some(Acknowledgment::Majority) => None,
        Some(Acknowledgment::Nodes(n)) => Some(format!("w={n}")),
        Some(Acknowledgment::Custom(tag)) => Some(format!("w={tag}")),
        Some(other) => Some(format!("w={other:?}")),
    }
    .or_else(|| (wc.journal == Some(false)).then(|| "journal=false".to_owned()));
    match weak {
        None => Ok(()),
        Some(found) => Err(StorageError::Connection(format!(
            "MongoDB connection string must leave the write concern at the server \
             default or set w=majority (found {found}). Backup claims and restore \
             targets assume an acknowledged write survives a primary failover, \
             which a weaker write concern does not guarantee."
        ))),
    }
}

/// `w: majority`, keeping the client's `wtimeout` and `journal`.
///
/// Set on the catalog database and on the backup and restore `$out` copies so
/// they do not follow a weaker server default (`setDefaultRWConcern`, or the
/// implicit `w: 1` of a replica set with arbiters). [`check_write_concern`]
/// has already refused a connection string whose `journal` is `false`.
pub(crate) fn majority_write_concern(
    client: Option<&mongodb::options::WriteConcern>,
) -> mongodb::options::WriteConcern {
    let mut wc = client.cloned().unwrap_or_default();
    wc.w = Some(mongodb::options::Acknowledgment::Majority);
    wc
}

/// The engine's handle on the catalog database, with read preference
/// `primary`, read concern `local` and write concern `majority` set on the
/// database, so they apply to every catalog operation outside a transaction
/// whatever the client's options or the server default say (inside a
/// transaction the transaction's own options apply).
///
/// The backup claim protocol depends on this: DeleteBackup, a restore, and
/// the stale-claim sweep each check the other's marker or CREATING target
/// with a `find_one` after writing their own, and that check must see every
/// write the primary has acknowledged. A secondary can lag, and a `majority`
/// or `snapshot` read can miss a write acknowledged with `w: 1`; a write
/// acknowledged with `w: 1` can also be rolled back on failover, after the
/// other side has acted on it.
/// `connect_guarded` also refuses a non-primary read preference and a write
/// concern other than `majority` in the connection string; this does not rely
/// on it.
fn catalog_database(client: &mongodb::Client) -> mongodb::Database {
    catalog_database_named(client, "extenddb_catalog")
}

/// [`catalog_database`] under another name, for test fixtures that must run
/// the production option set on a scratch database.
pub(crate) fn catalog_database_named(client: &mongodb::Client, name: &str) -> mongodb::Database {
    use mongodb::options::{DatabaseOptions, ReadConcern, ReadPreference, SelectionCriteria};
    client.database_with_options(
        name,
        DatabaseOptions::builder()
            .selection_criteria(SelectionCriteria::ReadPreference(ReadPreference::Primary))
            .read_concern(ReadConcern::local())
            .write_concern(majority_write_concern(client.write_concern()))
            .build(),
    )
}

impl MongoEngine {
    pub async fn new(
        connection_string: &str,
        region: &str,
        max_connections: Option<u32>,
        tx_read_concern: mongodb::options::ReadConcern,
    ) -> Result<Self, StorageError> {
        let options = guarded_client_options(connection_string, max_connections, true).await?;
        let pool_size = effective_pool_size(&options);
        let client = mongodb::Client::with_options(options)
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let catalog_db = catalog_database(&client);
        let data_db = client.database("extenddb_data");

        let restore_server_slot_count = backup_engine::restore_server_slot_count(pool_size);
        Ok(Self {
            client,
            catalog_db,
            data_db,
            region: region.to_owned(),
            gsi_cache: Arc::default(),
            tx_read_concern,
            backup_liveness: backup_engine::BackupLiveness::default(),
            restore_server_slot_count,
            restore_server_slots: Arc::new(tokio::sync::Semaphore::new(restore_server_slot_count)),
            restore_account_slots: backup_engine::RestoreAccountSlots::default(),
        })
    }

    /// Read concern to use for a newly-opened multi-document transaction.
    /// See [`MongoEngine::tx_read_concern`] / `transaction_read_concern`
    /// config for why this isn't always `snapshot`.
    pub(crate) fn transaction_read_concern(&self) -> mongodb::options::ReadConcern {
        self.tx_read_concern.clone()
    }

    /// Look up a fresh GSI-cache entry for `table_id`.
    ///
    /// Returns `Some(has_gsi)` when a cache entry exists and is younger than
    /// [`GSI_CACHE_TTL`], `None` otherwise (either no entry or expired).
    /// Callers that get `None` must fall back to reading the catalog.
    pub(crate) fn gsi_cache_get_fresh(&self, table_id: &str) -> Option<bool> {
        self.gsi_cache.get_fresh(table_id)
    }

    /// Return the generation for an in-flight catalog observation.
    pub(crate) fn gsi_cache_generation(&self, table_id: &str) -> u64 {
        self.gsi_cache.generation(table_id)
    }

    /// Record a fresh GSI-cache observation for `table_id`.
    pub(crate) fn gsi_cache_set(&self, table_id: &str, has_gsi: bool) {
        self.gsi_cache.set(table_id, has_gsi);
    }

    /// Publish a catalog observation only if no local mutation superseded it.
    pub(crate) fn gsi_cache_set_if_generation(
        &self,
        table_id: &str,
        expected_generation: u64,
        has_gsi: bool,
    ) -> bool {
        self.gsi_cache
            .set_if_generation(table_id, expected_generation, has_gsi)
    }

    /// Remove a GSI-cache entry (e.g., on GSI drop or table delete).
    pub(crate) fn gsi_cache_invalidate(&self, table_id: &str) {
        self.gsi_cache.invalidate(table_id);
    }

    /// Validate `account_id` against injection attacks.
    fn validate_account_id(account_id: &str) -> Result<(), StorageError> {
        if account_id.contains('$')
            || account_id.contains('.')
            || account_id.contains('\0')
            || !account_id.is_ascii()
        {
            return Err(StorageError::Validation(format!(
                "Invalid account_id: {account_id}"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::GsiCache;
    use super::client_options_with_pool_size;
    use super::effective_pool_size;
    use std::time::Duration;

    /// The restore bound is sized from the pool the driver will really use:
    /// the URI's maxPoolSize, the configured override, or the driver default.
    #[tokio::test]
    async fn effective_pool_size_follows_uri_override_and_default() {
        let uri = client_options_with_pool_size("mongodb://localhost:27017/?maxPoolSize=30", None)
            .await
            .expect("parse");
        assert_eq!(effective_pool_size(&uri), 30);
        let configured =
            client_options_with_pool_size("mongodb://localhost:27017/?maxPoolSize=30", Some(8))
                .await
                .expect("parse");
        assert_eq!(effective_pool_size(&configured), 8);
        let default = client_options_with_pool_size("mongodb://localhost:27017", None)
            .await
            .expect("parse");
        assert_eq!(effective_pool_size(&default), 10);
    }

    #[tokio::test]
    async fn absent_pool_override_preserves_uri_max_pool_size() {
        let options =
            client_options_with_pool_size("mongodb://localhost:27017/?maxPoolSize=100", None)
                .await
                .expect("MongoDB URI must parse");

        assert_eq!(options.max_pool_size, Some(100));
    }

    #[tokio::test]
    async fn configured_pool_override_reaches_client_options() {
        let options =
            client_options_with_pool_size("mongodb://localhost:27017/?maxPoolSize=100", Some(20))
                .await
                .expect("MongoDB URI must parse");

        assert_eq!(options.max_pool_size, Some(20));
    }

    #[test]
    fn stale_observation_cannot_overwrite_newer_mutation() {
        let cache = GsiCache::default();
        let observed_generation = cache.generation("table-1");

        cache.set("table-1", true);

        assert!(!cache.set_if_generation("table-1", observed_generation, false));
        assert_eq!(cache.get_fresh("table-1"), Some(true));
    }

    #[test]
    fn invalidation_blocks_in_flight_observation() {
        let cache = GsiCache::default();

        cache.set("table-1", true);
        let observed_generation = cache.generation("table-1");
        cache.invalidate("table-1");

        assert!(!cache.set_if_generation("table-1", observed_generation, true));
        assert_eq!(cache.get_fresh("table-1"), None);
    }

    fn assert_primary_local(db: &mongodb::Database, from: &str) {
        use mongodb::options::{ReadConcernLevel, ReadPreference, SelectionCriteria};
        assert!(
            matches!(
                db.selection_criteria(),
                Some(SelectionCriteria::ReadPreference(ReadPreference::Primary))
            ),
            "{from}: catalog read preference {:?}",
            db.selection_criteria()
        );
        assert_eq!(
            db.read_concern().map(|rc| rc.level.clone()),
            Some(ReadConcernLevel::Local),
            "{from}: catalog read concern"
        );
    }

    /// R4B-3: the catalog database reads from the primary with read concern
    /// `local` whatever the client's options say. The client here is built
    /// without `connect_guarded`, so its secondary read preference is not
    /// refused first. No server is contacted.
    #[tokio::test]
    async fn the_catalog_database_pins_primary_reads_over_the_client_options() {
        let options = mongodb::options::ClientOptions::parse(
            "mongodb://127.0.0.1:1/?readPreference=secondaryPreferred&readConcernLevel=majority",
        )
        .await
        .expect("MongoDB URI must parse");
        let client = mongodb::Client::with_options(options).expect("client");
        assert!(
            client.selection_criteria().is_some(),
            "the client must carry the URI's read preference for this test to mean anything"
        );
        assert_primary_local(&super::catalog_database(&client), "catalog_database");
    }

    /// R4B-3: `MongoEngine::new` uses that database handle for the catalog.
    #[tokio::test]
    async fn the_engine_catalog_database_reads_from_the_primary() {
        let engine = super::MongoEngine::new(
            "mongodb://127.0.0.1:1/?readConcernLevel=majority",
            "us-east-1",
            None,
            mongodb::options::ReadConcern::snapshot(),
        )
        .await
        .expect("engine (no server is contacted)");
        assert_primary_local(&engine.catalog_db, "MongoEngine::new");
        assert_majority(&engine.catalog_db, None, "MongoEngine::new");
    }

    fn assert_majority(db: &mongodb::Database, w_timeout: Option<Duration>, from: &str) {
        let wc = db.write_concern().expect("catalog write concern set");
        assert_eq!(
            wc.w,
            Some(mongodb::options::Acknowledgment::Majority),
            "{from}: catalog w"
        );
        assert_eq!(wc.w_timeout, w_timeout, "{from}: catalog wtimeout");
        assert_eq!(wc.journal, None, "{from}: catalog journal");
    }

    /// Every client goes through `connect_guarded`, which
    /// accepts only the server default or `w=majority` and refuses anything
    /// weaker, as it refuses a non-primary read preference. No server is
    /// contacted.
    #[tokio::test]
    async fn connect_guarded_refuses_a_write_concern_weaker_than_majority() {
        let base = "mongodb://127.0.0.1:1/?replicaSet=rs0";
        for accepted in [
            "",
            "&w=majority",
            "&w=majority&wtimeoutMS=5000",
            "&wtimeoutMS=5000",
            "&journal=true",
            "&w=majority&journal=true",
        ] {
            let uri = format!("{base}{accepted}");
            if let Err(e) = super::connect_guarded(&uri, None, false).await {
                panic!("{uri}: refused: {e}");
            }
        }
        for (refused, found) in [
            ("&w=0", "w=0"),
            ("&w=1", "w=1"),
            ("&w=2", "w=2"),
            ("&w=dc", "w=dc"),
            ("&journal=false", "journal=false"),
            ("&w=majority&journal=false", "journal=false"),
            ("&w=1&wtimeoutMS=5000", "w=1"),
        ] {
            let uri = format!("{base}{refused}");
            match super::connect_guarded(&uri, None, false).await {
                Ok(_) => panic!("{uri}: accepted"),
                Err(super::StorageError::Connection(msg)) => {
                    assert!(
                        msg.contains(&format!("(found {found})")) && msg.contains("w=majority"),
                        "{uri}: {msg}"
                    );
                }
                Err(e) => panic!("{uri}: unexpected error {e:?}"),
            }
        }
    }

    /// The catalog writes with `w: majority` even when the client's own write
    /// concern is weaker (built here without `connect_guarded`, standing for a
    /// weaker server default), keeping the client's `wtimeout`.
    #[tokio::test]
    async fn the_catalog_database_pins_majority_writes_over_the_client_options() {
        let options =
            mongodb::options::ClientOptions::parse("mongodb://127.0.0.1:1/?w=1&wtimeoutMS=2500")
                .await
                .expect("MongoDB URI must parse");
        let client = mongodb::Client::with_options(options).expect("client");
        assert_eq!(
            client.write_concern().and_then(|wc| wc.w.clone()),
            Some(mongodb::options::Acknowledgment::Nodes(1)),
            "the client must carry the URI's w=1 for this test to mean anything"
        );
        assert_majority(
            &super::catalog_database(&client),
            Some(Duration::from_millis(2500)),
            "catalog_database",
        );
        // No client write concern at all: majority, no timeout.
        let bare = mongodb::Client::with_options(
            mongodb::options::ClientOptions::parse("mongodb://127.0.0.1:1/")
                .await
                .expect("MongoDB URI must parse"),
        )
        .expect("client");
        assert!(bare.write_concern().is_none());
        assert_majority(&super::catalog_database(&bare), None, "catalog_database");
    }
}
