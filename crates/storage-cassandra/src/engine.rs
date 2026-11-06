// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra storage engine implementation.

use std::sync::Arc;

use cdrs_tokio::authenticators::{NoneAuthenticatorProvider, StaticPasswordAuthenticatorProvider};
use cdrs_tokio::cluster::connection_pool::ConnectionPoolConfigBuilder;
use cdrs_tokio::cluster::session::{Session, SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::{NodeTcpConfigBuilder, TcpConnectionManager};
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::query_values;
use cdrs_tokio::retry::ExponentialReconnectionPolicy;
use cdrs_tokio::transport::TransportTcp;

use extenddb_storage::error::StorageError;

use crate::config::CassandraStorageConfig;

pub type CassandraSession = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

/// Cassandra storage engine.
///
/// Implements ExtendDB storage traits using Apache Cassandra as the backend.
///
/// `Clone` shares every handle (session, caches, notifiers, clocks): a clone
/// is the same engine, which is what a detached task such as the restore copy
/// needs to outlive the request that started it.
#[derive(Clone)]
pub struct CassandraEngine {
    /// Cassandra session (connection pool)
    pub(crate) session: Arc<CassandraSession>,

    /// AWS region for ARN construction
    pub(crate) region: String,

    /// Keyspace prefix for catalog and account keyspaces
    pub(crate) keyspace_prefix: String,

    /// Replication factor for new keyspaces
    replication_factor: u32,

    /// Datacenter name for `NetworkTopologyStrategy`
    datacenter: String,

    /// Wakes the control plane poller when a table enters CREATING or DELETING state
    pub(crate) control_plane_notify: Arc<tokio::sync::Notify>,

    /// Last-known system GSI propagation delay in milliseconds. Writes read
    /// the setting live through [`Self::index_propagation_delay`]; this is
    /// its fallback when that read fails, re-warmed by every successful read
    /// and by the background poller.
    pub(crate) gsi_default_delay_ms: Arc<std::sync::atomic::AtomicU64>,

    /// Catalog reads [`Self::index_propagation_delay`] has made, for tests
    /// that check how often the write paths read the setting.
    index_delay_reads: Arc<std::sync::atomic::AtomicU64>,

    /// Short-lived cache of per-table TTL configuration for the write path.
    ///
    /// Every Put/Update/Delete needs the config, and reading it from the
    /// catalog per write is one of the two remaining per-write catalog costs.
    /// Entries live [`TTL_CONFIG_CACHE_TTL`]; staleness is bounded and safe in
    /// both directions *because the audit exists*: a stale `None` makes a
    /// write skip its queue mutation (the audit re-registers the item), and a
    /// stale generation enqueues into a retired generation (dead rows;
    /// the audit registers the live one). The enable-quiescence window in the
    /// ADR must exceed this TTL, and `update_ttl` invalidates locally so the
    /// issuing host is coherent immediately.
    pub(crate) ttl_config_cache: TtlConfigCache,

    /// Bumped by every TTL-config invalidation. A cache miss snapshots it
    /// before the catalog read and declines to repopulate if it moved — so a
    /// lifecycle change that lands mid-read cannot be shadowed by the stale
    /// value being inserted after the invalidation ran.
    pub(crate) ttl_config_cache_epoch: Arc<std::sync::atomic::AtomicU64>,

    /// GSI queue handle for waking workers after async enqueues
    pub(crate) gsi_queue: Arc<crate::gsi_queue::GsiQueue>,

    /// Wakes the GSI backfill recovery worker when a new CREATING index is committed.
    pub(crate) gsi_backfill_notify: Arc<tokio::sync::Notify>,

    /// Hybrid Logical Clock for stream sequence number generation
    pub(crate) hlc: crate::stream_util::SharedHlc,

    /// Stream record TTL in seconds (default: 30 hours = 108000)
    pub(crate) stream_retention_seconds: u32,

    /// Bounds in-flight detached TTL claim releases. Each successful write on
    /// a TTL-enabled table fires one off the request path; without a bound,
    /// degraded LWT latency would let them accumulate without limit. When
    /// saturated, releases are dropped — the claim's own TTL bounds the
    /// residue, so dropping is safe by construction.
    pub(crate) ttl_release_permits: Arc<tokio::sync::Semaphore>,

    /// Per-(keyspace, partition) resume cursor for the inflight repair-marker
    /// scan, so every marker is deterministically visited within a bounded
    /// number of cycles regardless of partition size. In-process on purpose:
    /// a restart merely restarts the traversal from the front.
    pub(crate) ttl_repair_scan_cursors:
        Arc<std::sync::Mutex<std::collections::HashMap<(String, i32), uuid::Uuid>>>,
}

/// Cap on concurrently in-flight detached TTL claim releases.
const TTL_RELEASE_MAX_IN_FLIGHT: usize = 1_024;

// Reconnection backoff bounds for the driver (issue #403).
//
// cdrs-tokio's default policy doubles a 1s base up to 60s, and each outage
// starts a new schedule, so it reaches 60s after about a minute of failed
// attempts. We cap it at 5s: the waits are about 2s, about 4s, then 4.25 to
// 5s, then a fixed 5s with no retry limit. The pool and the control
// connection both use this policy. Declared as constants so they are easy to
// surface as config later if needed.
const RECONNECT_BASE_DELAY: std::time::Duration = std::time::Duration::from_secs(1);
const RECONNECT_MAX_DELAY: std::time::Duration = std::time::Duration::from_secs(5);
/// Attempts after which the exponential delay has saturated at
/// `RECONNECT_MAX_DELAY`. With a 1s base and 5s cap, the backoff delay will hit
/// the cap by the third attempt.
const RECONNECT_MAX_ATTEMPTS: usize = 3;

/// `(account_id, table_name)` → `(fetched_at, config)`; see the field docs.
pub(crate) type TtlConfigCache = Arc<
    std::sync::Mutex<
        std::collections::HashMap<
            (String, String),
            (std::time::Instant, Option<crate::data::ttl::TtlConfig>),
        >,
    >,
>;

impl CassandraEngine {
    /// Create a new Cassandra storage engine.
    pub async fn new(config: &CassandraStorageConfig, region: &str) -> Result<Self, StorageError> {
        // Build Cassandra session
        let session = Self::create_session(config).await?;

        Ok(Self {
            session: Arc::new(session),
            region: region.to_string(),
            keyspace_prefix: config.keyspace_prefix.clone(),
            replication_factor: config.replication_factor,
            datacenter: config.datacenter.clone(),
            control_plane_notify: Arc::new(tokio::sync::Notify::new()),
            gsi_default_delay_ms: Arc::new(std::sync::atomic::AtomicU64::new(
                crate::DEFAULT_INDEX_PROPAGATION_DELAY_MS,
            )),
            index_delay_reads: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            ttl_config_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            ttl_config_cache_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            gsi_queue: crate::gsi_queue::GsiQueue::new(),
            gsi_backfill_notify: Arc::new(tokio::sync::Notify::new()),
            hlc: crate::stream_util::new_shared_hlc(
                config.instance_id.as_deref().unwrap_or("default"),
            ),
            stream_retention_seconds: 108_000, // 30 hours; overridden by spawn_workers (Step 7)
            ttl_release_permits: Arc::new(tokio::sync::Semaphore::new(TTL_RELEASE_MAX_IN_FLIGHT)),
            ttl_repair_scan_cursors: Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
        })
    }

    /// Create a Cassandra session with connection pool.
    pub async fn create_session(
        config: &CassandraStorageConfig,
    ) -> Result<CassandraSession, StorageError> {
        if config.contact_points.is_empty() {
            return Err(StorageError::Connection(
                "No contact points configured".to_string(),
            ));
        }

        // Build node config with contact points
        let mut node_builder = NodeTcpConfigBuilder::new();
        for contact_point in &config.contact_points {
            node_builder = node_builder.with_contact_point(contact_point.clone().into());
        }

        // Add authentication if configured
        let node_builder =
            if let (Some(username), Some(password)) = (&config.username, &config.password) {
                let auth_provider =
                    Arc::new(StaticPasswordAuthenticatorProvider::new(username, password));
                node_builder.with_authenticator_provider(auth_provider)
            } else {
                node_builder.with_authenticator_provider(Arc::new(NoneAuthenticatorProvider))
            };

        // Build cluster config
        let cluster_config = node_builder.build().await.map_err(|e| {
            StorageError::Connection(format!("Failed to build cluster config: {e}"))
        })?;

        // Create session with round-robin load balancing.
        //
        // Pool size: cdrs-tokio defaults to ONE connection per node
        // (ConnectionPoolConfig::default is local_size=1, remote_size=1). One
        // connection serializes the whole CQL stream-id space onto a single
        // socket and gives no headroom if that socket drops, so we size the pool
        // from `max_connections` (default 10) and scatter requests across it.
        // This also eases the cdrs-tokio stream-id reuse hazard (issue #403):
        // more connections divide the id space. A single-node
        // deployment is "local", so local_size is the one that matters; we set
        // remote_size the same for multi-node clusters.
        let pool_size = pool_size_for(config.max_connections);
        let pool_config = ConnectionPoolConfigBuilder::new()
            .with_local_size(pool_size)
            .with_remote_size(pool_size)
            .build();
        // Wrap in timeout to prevent indefinite hangs on connection/auth failures
        const CONNECTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        let lb = RoundRobinLoadBalancingStrategy::new();
        let session_future = TcpSessionBuilder::new(lb, cluster_config)
            .with_connection_pool_config(pool_config)
            // Cap the reconnection delay at 5s instead of the driver's default
            // 60s (issue #403). See the RECONNECT_* constants.
            .with_reconnection_policy(std::sync::Arc::new(ExponentialReconnectionPolicy::new(
                RECONNECT_BASE_DELAY,
                RECONNECT_MAX_DELAY,
                RECONNECT_MAX_ATTEMPTS,
            )))
            .build();

        let session = tokio::time::timeout(CONNECTION_TIMEOUT, session_future)
            .await
            .map_err(|_| StorageError::Connection(
                format!(
                    "Connection timeout after {:?}. Check that Cassandra is running and accessible at {:?}. \
                     If authentication is enabled, verify username/password are correct.",
                    CONNECTION_TIMEOUT,
                    config.contact_points
                )
            ))?
            .map_err(|e| {
                // Enhance error message for common authentication failures
                let err_str = e.to_string();
                if err_str.contains("authentication") || err_str.contains("Authentication") {
                    StorageError::Connection(format!(
                        "Authentication failed: {e}. Verify username/password in config."
                    ))
                } else {
                    StorageError::Connection(format!("Failed to build session: {e}"))
                }
            })?;

        Ok(session)
    }

    /// Get a reference to the Cassandra session.
    #[must_use]
    pub fn session(&self) -> &CassandraSession {
        &self.session
    }

    /// Get an Arc clone of the Cassandra session.
    #[must_use]
    pub fn session_arc(&self) -> Arc<CassandraSession> {
        Arc::clone(&self.session)
    }

    /// Get the catalog keyspace name.
    #[must_use]
    pub fn catalog_keyspace(&self) -> String {
        format!("{}_catalog", self.keyspace_prefix)
    }

    /// Get the account keyspace name for a given account ID.
    #[must_use]
    pub fn account_keyspace(&self, account_id: &str) -> String {
        format!("{}_account_{}", self.keyspace_prefix, account_id)
    }

    /// Current system GSI propagation delay (ms); `0` means synchronous.
    ///
    /// Reads the `index_propagation_delay_ms` setting live from the catalog,
    /// as SQLite and PostgreSQL do, so a change made with `extenddb settings
    /// set` from another process applies to the next write rather than up to
    /// 5 s later, when the poller refreshes the cached value (#407). Callers
    /// call this only for a table with an index that can be asynchronous, so
    /// a table without one pays nothing.
    ///
    /// On a read error the cached value is used and the error is logged, so a
    /// degraded catalog serves a stale delay loudly rather than silently. On
    /// success the cache is re-warmed.
    pub(crate) async fn index_propagation_delay(&self) -> u64 {
        use cdrs_tokio::types::IntoRustByName as _;
        use std::sync::atomic::Ordering;

        self.index_delay_reads.fetch_add(1, Ordering::Relaxed);
        // Read at the driver default, ONE, and `settings set` writes at ONE
        // too. With replication factor above 1, a change applies once the
        // replica this read reaches has it. One consistency policy for every
        // statement is #770.
        let query = format!(
            "SELECT key, value FROM {}.settings \
             WHERE key IN ('index_propagation_delay_ms', 'gsi_propagation_delay_ms')",
            self.catalog_keyspace()
        );
        let rows = crate::cassandra_util::query_rows::<StorageError>(
            &self.session,
            &query,
            cdrs_tokio::query::QueryValues::SimpleValues(vec![]),
            "index_propagation_delay",
        )
        .await;
        match rows {
            Ok(rows) => {
                let (mut canonical, mut legacy) = (None, None);
                for row in &rows {
                    let key: Option<String> = row.get_by_name("key").ok().flatten();
                    let value: Option<String> = row.get_by_name("value").ok().flatten();
                    match key.as_deref() {
                        Some("index_propagation_delay_ms") => canonical = value,
                        Some("gsi_propagation_delay_ms") => legacy = value,
                        _ => {}
                    }
                }
                let ms = propagation_delay_from_settings(canonical.as_deref(), legacy.as_deref());
                self.gsi_default_delay_ms.store(ms, Ordering::Relaxed);
                ms
            }
            Err(e) => {
                let ms = self.gsi_default_delay_ms.load(Ordering::Relaxed);
                tracing::warn!(
                    "Failed to read index_propagation_delay_ms, using the last known value \
                     {ms} ms: {e}"
                );
                ms
            }
        }
    }

    /// How many times this engine has read `index_propagation_delay_ms` from
    /// the catalog.
    #[doc(hidden)]
    pub fn index_delay_reads(&self) -> u64 {
        self.index_delay_reads
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The system delay a write to a table with `indexes` needs: the live
    /// [`Self::index_propagation_delay`] when some GSI follows the system
    /// setting, else `0` with no catalog read.
    ///
    /// `0` is safe to return in the second case because nothing reads it: an
    /// LSI is always synchronous, and a GSI with a delay of its own uses that
    /// (see [`crate::data::index::effective_delay`]). Write calls read it at
    /// most once, however often they retry, and a transaction once for all
    /// its items. BatchWriteItem makes one write call per item, so it reads
    /// once per item, as SQLite and PostgreSQL do.
    pub(crate) async fn system_index_delay(
        &self,
        indexes: &[crate::data::index::IndexMeta],
    ) -> u64 {
        if indexes.iter().any(follows_system_delay) {
            self.index_propagation_delay().await
        } else {
            0
        }
    }

    /// [`Self::system_index_delay`] for a write that retries: the setting is
    /// read at most once into `read`, which the retry loop owns, so every
    /// attempt of one write call uses the same value.
    pub(crate) async fn system_index_delay_once(
        &self,
        indexes: &[crate::data::index::IndexMeta],
        read: &tokio::sync::OnceCell<u64>,
    ) -> u64 {
        if indexes.iter().any(follows_system_delay) {
            *read.get_or_init(|| self.index_propagation_delay()).await
        } else {
            0
        }
    }

    /// Create a keyspace with `NetworkTopologyStrategy`.
    pub async fn create_keyspace(&self, keyspace_name: &str) -> Result<(), StorageError> {
        let cql = format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': 'NetworkTopologyStrategy', '{}': {}}}",
            keyspace_name, self.datacenter, self.replication_factor
        );

        self.session
            .query(cql)
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to create keyspace: {e}")))?;

        Ok(())
    }

    /// Drop a keyspace.
    pub async fn drop_keyspace(&self, keyspace_name: &str) -> Result<(), StorageError> {
        let cql = format!("DROP KEYSPACE IF EXISTS {keyspace_name}");

        self.session
            .query(cql)
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to drop keyspace: {e}")))?;

        Ok(())
    }

    /// Check if a keyspace exists.
    pub async fn keyspace_exists(&self, keyspace_name: &str) -> Result<bool, StorageError> {
        let cql = "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?";

        let rows = self
            .session
            .query_with_values(cql, query_values!(keyspace_name))
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to query keyspaces: {e}")))?
            .response_body()
            .map_err(|e| StorageError::Internal(format!("Failed to get response body: {e}")))?
            .into_rows()
            .ok_or_else(|| StorageError::Internal("Failed to parse rows".to_string()))?;

        Ok(!rows.is_empty())
    }

    /// Defense-in-depth: validate `account_id` before use in CQL identifiers.
    ///
    /// `account_id` is interpolated into CQL identifiers via keyspace names.
    /// Reject values that could break identifiers.
    pub(crate) fn validate_account_id(account_id: &str) -> Result<(), StorageError> {
        if account_id.contains('"') || account_id.contains('\0') || !account_id.is_ascii() {
            return Err(StorageError::Internal(
                "account_id contains invalid characters for use in CQL identifiers".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Map the configured `max_connections` to the per-node pool size we request
/// from cdrs-tokio.
///
/// cdrs-tokio defaults to a single connection per node; we instead size the
/// pool from `max_connections`. The driver takes a `usize`, and a pool of zero
/// connections is unusable, so clamp to at least 1. This is the only decision
/// ExtendDB owns here — the driver then opens and manages that many connections.
/// It is a free function so it can be unit-tested without a session, a driver,
/// or a node.
fn pool_size_for(max_connections: u32) -> usize {
    (max_connections.max(1)) as usize
}

/// The propagation delay a catalog's settings give, matching SQLite and
/// PostgreSQL: the canonical `index_propagation_delay_ms` wins when its row
/// exists, else the pre-rename `gsi_propagation_delay_ms`; a missing or
/// unparseable value is [`crate::DEFAULT_INDEX_PROPAGATION_DELAY_MS`].
pub(crate) fn propagation_delay_from_settings(
    canonical: Option<&str>,
    legacy: Option<&str>,
) -> u64 {
    canonical
        .or(legacy)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(crate::DEFAULT_INDEX_PROPAGATION_DELAY_MS)
}

/// Whether `index` takes its delay from the system setting: a GSI that sets
/// no delay of its own (or a negative one, which
/// [`crate::data::index::effective_delay`] reads the same way). An LSI never
/// does: it is always written synchronously.
pub(crate) fn follows_system_delay(index: &crate::data::index::IndexMeta) -> bool {
    index.index_type != "LSI" && index.propagation_delay_ms.is_none_or(|ms| ms < 0)
}

#[cfg(test)]
mod propagation_delay_tests {
    use super::follows_system_delay;
    use super::propagation_delay_from_settings as delay;
    use crate::DEFAULT_INDEX_PROPAGATION_DELAY_MS as DEFAULT;

    fn index(index_type: &str, propagation_delay_ms: Option<i32>) -> crate::data::index::IndexMeta {
        crate::data::index::IndexMeta {
            index_name: "i".to_owned(),
            index_id: "i".to_owned(),
            index_type: index_type.to_owned(),
            key_schema: Vec::new(),
            projection: extenddb_core::types::Projection {
                projection_type: extenddb_core::types::ProjectionType::All,
                non_key_attributes: None,
            },
            propagation_delay_ms,
        }
    }

    #[test]
    fn only_a_gsi_without_its_own_delay_follows_the_setting() {
        assert!(follows_system_delay(&index("GSI", None)));
        assert!(follows_system_delay(&index("GSI", Some(-1))));
        assert!(!follows_system_delay(&index("GSI", Some(0))));
        assert!(!follows_system_delay(&index("GSI", Some(500))));
        assert!(!follows_system_delay(&index("LSI", None)));
    }

    #[test]
    fn canonical_key_wins() {
        assert_eq!(delay(Some("0"), Some("999")), 0);
        assert_eq!(delay(Some("250"), None), 250);
    }

    #[test]
    fn legacy_key_is_read_when_canonical_is_absent() {
        assert_eq!(delay(None, Some("0")), 0);
    }

    #[test]
    fn missing_or_unparseable_is_the_default() {
        assert_eq!(delay(None, None), DEFAULT);
        assert_eq!(delay(Some("soon"), Some("0")), DEFAULT);
        assert_eq!(delay(None, Some("-1")), DEFAULT);
    }
}

// TODO: Implement storage traits:
// - TableEngine
// - DataEngine
// - MetadataEngine
// - StreamEngine
// - WorkerStore
// - BackupEngine
// - StorageEngine (composite trait)
// - ManagementStore
// - AdminStore
// - SettingsStore
// - MetricsStore
// - RateLimitStore
// - AuthorizationStore
// - CatalogStore (composite trait)

#[cfg(test)]
mod pool_size_tests {
    use super::pool_size_for;

    #[test]
    fn maps_max_connections_to_pool_size() {
        // The configured value passes through unchanged for any usable size.
        assert_eq!(pool_size_for(1), 1);
        assert_eq!(pool_size_for(2), 2);
        assert_eq!(pool_size_for(8), 8);
        assert_eq!(pool_size_for(10), 10);
    }

    #[test]
    fn clamps_zero_to_one() {
        // A zero-size pool is unusable; we request at least one connection.
        assert_eq!(pool_size_for(0), 1);
    }
}
