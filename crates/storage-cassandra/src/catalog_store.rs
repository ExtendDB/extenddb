// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra implementations of `SettingsStore`, `MetricsStore`, and
//! `RateLimitStore`.
//!
//! `CassandraCatalogStore` wraps a Cassandra session connected to the catalog
//! keyspace and implements the operational traits defined in `extenddb_storage`.

use std::sync::Arc;

use cdrs_tokio::cluster::TcpConnectionManager;
use cdrs_tokio::cluster::session::Session;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::query_values;
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::types::IntoRustByName;
use extenddb_storage::management_store::{OpError, OpResult};
use futures::future::BoxFuture;

type CassandraSession = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

/// Cassandra-backed catalog store for settings, metrics, and rate limiting.
///
/// Holds a session to the catalog keyspace. Created once at startup
/// and shared (via `Arc`) across management API handlers and background workers.
pub struct CassandraCatalogStore {
    session: Arc<CassandraSession>,
    keyspace_prefix: String,
    datacenter: String,
    replication_factor: u32,
    /// Cached encryption key (immutable after bootstrap). Avoids
    /// per-request DB query on access key and assume-role operations.
    encryption_key: Option<Arc<str>>,
}

impl CassandraCatalogStore {
    /// Create a new catalog store wrapping the given session.
    pub fn new(
        session: Arc<CassandraSession>,
        keyspace_prefix: String,
        datacenter: String,
        replication_factor: u32,
    ) -> Self {
        Self {
            session,
            keyspace_prefix,
            datacenter,
            replication_factor,
            encryption_key: None,
        }
    }

    /// Create a new catalog store with a pre-loaded encryption key.
    pub fn with_encryption_key(
        session: Arc<CassandraSession>,
        keyspace_prefix: String,
        datacenter: String,
        replication_factor: u32,
        encryption_key: String,
    ) -> Self {
        Self {
            session,
            keyspace_prefix,
            datacenter,
            replication_factor,
            encryption_key: Some(Arc::from(encryption_key.as_str())),
        }
    }

    /// Borrow the underlying session (escape hatch for callers not yet migrated).
    pub fn session(&self) -> &Arc<CassandraSession> {
        &self.session
    }

    /// Get the cached encryption key. Returns `None` if not loaded at startup.
    pub fn encryption_key(&self) -> Option<&Arc<str>> {
        self.encryption_key.as_ref()
    }

    /// Get the catalog keyspace name.
    pub(crate) fn catalog_keyspace(&self) -> String {
        format!("{}_catalog", self.keyspace_prefix)
    }

    /// Get the account keyspace name.
    pub(crate) fn account_keyspace(&self, account_id: &str) -> String {
        format!("{}_account_{}", self.keyspace_prefix, account_id)
    }

    /// Ensure an account keyspace exists (idempotent).
    /// Creates the keyspace with NetworkTopologyStrategy if it doesn't exist.
    pub(crate) async fn ensure_account_keyspace(&self, account_id: &str) -> OpResult<()> {
        let keyspace_name = self.account_keyspace(account_id);

        // Check if keyspace already exists to avoid re-running migrations on every call.
        let exists_result = self
            .session
            .query_with_values(
                "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?",
                cdrs_tokio::query_values!(keyspace_name.as_str()),
            )
            .await
            .ok()
            .and_then(|r| r.response_body().ok())
            .is_some_and(|b| !b.into_rows().unwrap_or_default().is_empty());

        if exists_result {
            // Keyspace exists but may have been created by a concurrent caller that
            // hasn't finished running migrations yet. Always run migrations — they are
            // idempotent (CREATE TABLE IF NOT EXISTS) so re-running is safe.
            return crate::migrations::run_data_migrations(&self.session, &keyspace_name)
                .await
                .map_err(|e| {
                    tracing::error!(
                        "Failed to run data migrations for {}: {:?}",
                        keyspace_name,
                        e
                    );
                    OpError::Internal("Failed to initialize account storage".to_owned())
                });
        }

        let cql = format!(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = {{'class': 'NetworkTopologyStrategy', '{}': {}}}",
            keyspace_name, self.datacenter, self.replication_factor
        );

        self.session.query(cql).await.map_err(|e| {
            tracing::error!("Failed to create account keyspace {}: {}", keyspace_name, e);
            OpError::Internal("Failed to initialize account storage".to_owned())
        })?;

        crate::migrations::run_data_migrations(&self.session, &keyspace_name)
            .await
            .map_err(|e| {
                tracing::error!(
                    "Failed to run data migrations for {}: {:?}",
                    keyspace_name,
                    e
                );
                OpError::Internal("Failed to initialize account storage".to_owned())
            })?;

        Ok(())
    }

    /// Drop an account keyspace (idempotent).
    pub(crate) async fn drop_account_keyspace(&self, account_id: &str) -> OpResult<()> {
        let keyspace_name = self.account_keyspace(account_id);
        let cql = format!("DROP KEYSPACE IF EXISTS {keyspace_name}");

        self.session.query(cql).await.map_err(|e| {
            tracing::error!("Failed to drop account keyspace {}: {}", keyspace_name, e);
            OpError::Internal("Failed to drop account storage".to_owned())
        })?;

        Ok(())
    }

    /// Check if an account exists. Used to emulate foreign key checks.
    pub(crate) async fn account_exists(&self, account_id: &str) -> Result<bool, OpError> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT account_id FROM {catalog_keyspace}.accounts WHERE account_id = ?"
        );

        let rows = crate::cassandra_util::query_rows(
            &self.session,
            &query,
            cdrs_tokio::query_values!(account_id),
            "account_exists",
        )
        .await?;

        Ok(!rows.is_empty())
    }

    /// Check if a user exists. Used to emulate foreign key checks.
    pub(crate) async fn user_exists(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> Result<bool, OpError> {
        let catalog_keyspace = self.catalog_keyspace();
        let query = format!(
            "SELECT user_name FROM {catalog_keyspace}.iam_users WHERE account_id = ? AND user_name = ?"
        );

        let rows = crate::cassandra_util::query_rows(
            &self.session,
            &query,
            cdrs_tokio::query_values!(account_id, user_name),
            "user_exists",
        )
        .await?;

        Ok(!rows.is_empty())
    }
}

// ── SettingsStore ──────────────────────────────────────────────────────

impl extenddb_storage::management_store::SettingsStore for CassandraCatalogStore {
    fn get_setting(&self, key: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let key = key.to_string();
        let session = self.session.clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let query = format!(
                "SELECT value FROM {catalog_keyspace}.settings WHERE key = ?"
            );

            let row = crate::cassandra_util::query_optional(
                &session,
                &query,
                query_values!(key.as_str()),
                "get_setting",
            )
            .await?;

            if let Some(row) = row {
                let value: String =
                    crate::cassandra_util::get_column(&row, "value", "get_setting")?;
                Ok(Some(value))
            } else {
                Ok(None)
            }
        })
    }

    fn set_setting(&self, key: &str, value: &str) -> BoxFuture<'_, OpResult<()>> {
        let key = key.to_string();
        let value = value.to_string();
        let session = self.session.clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let query = format!(
                "INSERT INTO {catalog_keyspace}.settings (key, value) VALUES (?, ?)"
            );

            crate::cassandra_util::execute(
                &session,
                &query,
                query_values!(key.as_str(), value.as_str()),
                "set_setting",
            )
            .await
        })
    }

    fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let session = self.session.clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let query = format!("SELECT key, value FROM {catalog_keyspace}.settings");

            let rows = crate::cassandra_util::query_rows(
                &session,
                &query,
                query_values!(),
                "list_settings",
            )
            .await?;

            let mut settings = crate::cassandra_util::map_rows(
                rows,
                |row| {
                    use crate::cassandra_util::get_column;
                    Ok::<_, extenddb_storage::management_store::OpError>((
                        get_column::<String, _>(row, "key", "list_settings")?,
                        get_column::<String, _>(row, "value", "list_settings")?,
                    ))
                },
                "list_settings",
            )?;

            // Sort by key for consistency with PostgreSQL
            settings.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(settings)
        })
    }

    fn cached_encryption_key(&self) -> Option<String> {
        self.encryption_key.as_ref().map(std::string::ToString::to_string)
    }
}

// ── DiagnosticsStore ───────────────────────────────────────────────────

impl extenddb_storage::diagnostics::DiagnosticsStore for CassandraCatalogStore {
    fn count_tables(&self) -> BoxFuture<'_, extenddb_storage::diagnostics::DiagResult<i64>> {
        let session = self.session.clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let query = format!("SELECT COUNT(*) FROM {catalog_keyspace}.tables");
            let result = session.query(&query).await.map_err(|e| {
                extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
            })?;

            let body = result.response_body().map_err(|e| {
                extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
            })?;

            if let Some(rows) = body.into_rows()
                && let Some(row) = rows.first() {
                    let count: i64 = row.get_r_by_name("count").map_err(|e| {
                        extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
                    })?;
                    return Ok(count);
                }

            Ok(0)
        })
    }

    fn count_indexes(&self) -> BoxFuture<'_, extenddb_storage::diagnostics::DiagResult<i64>> {
        let session = self.session.clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let query = format!("SELECT COUNT(*) FROM {catalog_keyspace}.indexes");
            let result = session.query(&query).await.map_err(|e| {
                extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
            })?;

            let body = result.response_body().map_err(|e| {
                extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
            })?;

            if let Some(rows) = body.into_rows()
                && let Some(row) = rows.first() {
                    let count: i64 = row.get_r_by_name("count").map_err(|e| {
                        extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
                    })?;
                    return Ok(count);
                }

            Ok(0)
        })
    }

    fn test_data_database_connection(
        &self,
    ) -> BoxFuture<'_, extenddb_storage::diagnostics::DiagResult<String>> {
        let session = self.session.clone();
        let catalog_keyspace = self.catalog_keyspace();
        let keyspace_prefix = self.keyspace_prefix.clone();
        Box::pin(async move {
            // For Cassandra, we test connection to account keyspaces
            // Get a sample account keyspace name from accounts table
            let query = format!(
                "SELECT account_id FROM {catalog_keyspace}.accounts LIMIT 1"
            );
            let result = session.query(&query).await.map_err(|e| {
                extenddb_storage::diagnostics::DiagError::QueryFailed(format!(
                    "Failed to query accounts: {e}"
                ))
            })?;

            let body = result.response_body().map_err(|e| {
                extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
            })?;

            if let Some(rows) = body.into_rows()
                && let Some(row) = rows.first() {
                    let account_id: String = row.get_r_by_name("account_id").map_err(|e| {
                        extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
                    })?;

                    // Test connection to account keyspace by querying schema_history
                    let account_keyspace = format!("{keyspace_prefix}_account_{account_id}");
                    let test_query =
                        format!("SELECT COUNT(*) FROM {account_keyspace}.schema_history");

                    session.query(&test_query).await.map_err(|e| {
                        extenddb_storage::diagnostics::DiagError::ConnectionFailed(format!(
                            "Failed to query account keyspace {account_keyspace}: {e}"
                        ))
                    })?;

                    return Ok(account_keyspace);
                }

            // No accounts exist yet - that's okay, just return a message
            Ok("No account keyspaces exist yet".to_string())
        })
    }
}

// ── Stub implementations for remaining catalog traits ──────────────────

use extenddb_storage::management_store::{MetricsStore, RateLimitStore};

impl RateLimitStore for CassandraCatalogStore {
    fn count_principal_failures(
        &self,
        _principal: &str,
        _window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        Box::pin(async move { Ok(0) })
    }

    fn count_ip_failures(
        &self,
        _source_ip: &str,
        _window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        Box::pin(async move { Ok(0) })
    }

    fn record_failed_login(&self, _principal: &str, _source_ip: Option<&str>) -> BoxFuture<'_, ()> {
        Box::pin(async move {})
    }

    fn cleanup_old_attempts(&self, _max_age_seconds: i64) -> BoxFuture<'_, ()> {
        Box::pin(async move {})
    }
}

use extenddb_storage::management_store::MetricsRow;

impl MetricsStore for CassandraCatalogStore {
    fn insert_metrics(&self, _rows: &[MetricsRow]) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async move { Ok(()) })
    }

    fn query_metrics(
        &self,
        _start: time::OffsetDateTime,
        _end: time::OffsetDateTime,
        _table_name: Option<&str>,
        _metric: Option<&str>,
    ) -> BoxFuture<'_, OpResult<Vec<MetricsRow>>> {
        Box::pin(async move { Ok(vec![]) })
    }

    fn prune_metrics(&self, _retention: std::time::Duration) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async move { Ok(()) })
    }
}

impl extenddb_storage::CatalogStore for CassandraCatalogStore {
    fn cached_encryption_key(&self) -> Option<String> {
        self.encryption_key.as_ref().map(std::string::ToString::to_string)
    }
}
