// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra storage engine implementation.

use std::sync::Arc;

use cdrs_tokio::authenticators::{NoneAuthenticatorProvider, StaticPasswordAuthenticatorProvider};
use cdrs_tokio::cluster::session::{Session, SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::{NodeTcpConfigBuilder, TcpConnectionManager};
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::query_values;
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
pub struct CassandraEngine {
    /// Cassandra session (connection pool)
    pub(crate) session: Arc<CassandraSession>,

    /// AWS region for ARN construction
    pub(crate) region: String,

    /// Keyspace prefix for catalog and account keyspaces
    pub(crate) keyspace_prefix: String,

    /// Replication factor for new keyspaces
    replication_factor: u32,

    /// Datacenter name for NetworkTopologyStrategy
    datacenter: String,

    /// Wakes the control plane poller when a table enters CREATING or DELETING state
    pub(crate) control_plane_notify: Arc<tokio::sync::Notify>,

    /// Default GSI propagation delay in milliseconds (for async indexes)
    pub(crate) gsi_default_delay_ms: Arc<std::sync::atomic::AtomicU64>,

    /// GSI queue handle for waking workers after async enqueues
    pub(crate) gsi_queue: Arc<crate::gsi_queue::GsiQueue>,

    /// Hybrid Logical Clock for stream sequence number generation
    pub(crate) hlc: crate::stream_util::SharedHlc,

    /// Stream record TTL in seconds (default: 30 hours = 108000)
    pub(crate) stream_retention_seconds: u32,
}

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
            gsi_default_delay_ms: Arc::new(std::sync::atomic::AtomicU64::new(10)), // Default 10ms
            gsi_queue: crate::gsi_queue::GsiQueue::new(),
            hlc: crate::stream_util::new_shared_hlc(
                config.instance_id.as_deref().unwrap_or("default"),
            ),
            stream_retention_seconds: 108_000, // 30 hours; overridden by spawn_workers (Step 7)
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

        // Create session with round-robin load balancing
        // Wrap in timeout to prevent indefinite hangs on connection/auth failures
        const CONNECTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        let lb = RoundRobinLoadBalancingStrategy::new();
        let session_future = TcpSessionBuilder::new(lb, cluster_config).build();

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
    pub fn session(&self) -> &CassandraSession {
        &self.session
    }

    /// Get an Arc clone of the Cassandra session.
    pub fn session_arc(&self) -> Arc<CassandraSession> {
        Arc::clone(&self.session)
    }

    /// Get the catalog keyspace name.
    pub fn catalog_keyspace(&self) -> String {
        format!("{}_catalog", self.keyspace_prefix)
    }

    /// Get the account keyspace name for a given account ID.
    pub fn account_keyspace(&self, account_id: &str) -> String {
        format!("{}_account_{}", self.keyspace_prefix, account_id)
    }

    /// Create a keyspace with NetworkTopologyStrategy.
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
