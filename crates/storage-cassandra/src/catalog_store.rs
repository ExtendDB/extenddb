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

/// The replication map of a keyspace with `NetworkTopologyStrategy` and
/// `replication_factor` replicas in `datacenter`.
pub(crate) fn network_topology(datacenter: &str, replication_factor: u32) -> String {
    format!("{{'class': 'NetworkTopologyStrategy', '{datacenter}': {replication_factor}}}")
}

/// Cassandra-backed catalog store for settings, metrics, and rate limiting.
///
/// Holds a session to the catalog keyspace. Created once at startup
/// and shared (via `Arc`) across management API handlers and background workers.
pub struct CassandraCatalogStore {
    session: Arc<CassandraSession>,
    keyspace_prefix: String,
    /// The replication CreateAccount gives a new account keyspace, as a CQL
    /// map literal; `None` for a store opened from a connection string,
    /// which knows no replication and refuses to create a keyspace.
    replication: Option<String>,
    /// The release recorded in the ledger rows of a new account keyspace.
    release: String,
    /// Cached encryption key (immutable after bootstrap). Avoids
    /// per-request DB query on access key and assume-role operations.
    encryption_key: Option<Arc<str>>,
    /// TTL in seconds applied to every metrics row on insert.
    /// Cassandra expires rows natively; `prune_metrics` is a no-op.
    metrics_ttl_seconds: i32,
    /// The crash-and-rerun tests' request budget, shared with the migration
    /// backend, which fails a policy write once it is spent.
    #[cfg(any(test, feature = "test-util"))]
    stop_after: Option<Arc<std::sync::atomic::AtomicUsize>>,
}

impl CassandraCatalogStore {
    /// Create a new catalog store wrapping the given session.
    #[must_use]
    pub fn new(
        session: Arc<CassandraSession>,
        keyspace_prefix: String,
        datacenter: String,
        replication_factor: u32,
    ) -> Self {
        Self {
            session,
            keyspace_prefix,
            replication: Some(network_topology(&datacenter, replication_factor)),
            release: env!("CARGO_PKG_VERSION").to_owned(),
            encryption_key: None,
            metrics_ttl_seconds: 86400,
            #[cfg(any(test, feature = "test-util"))]
            stop_after: None,
        }
    }

    /// A store for the catalog's rows alone, opened from a connection string
    /// (`settings`, `verify`, the diagnostics, a migration's code body). It
    /// knows no replication, so CreateAccount on it fails instead of
    /// creating a keyspace with a guessed one.
    #[must_use]
    pub fn catalog_only(session: Arc<CassandraSession>, keyspace_prefix: String) -> Self {
        Self {
            session,
            keyspace_prefix,
            replication: None,
            release: env!("CARGO_PKG_VERSION").to_owned(),
            encryption_key: None,
            metrics_ttl_seconds: 86400,
            #[cfg(any(test, feature = "test-util"))]
            stop_after: None,
        }
    }

    /// Share the crash-and-rerun tests' request budget: each policy write
    /// takes one request, and fails once the budget is spent.
    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn stop_after(&mut self, left: Arc<std::sync::atomic::AtomicUsize>) {
        self.stop_after = Some(left);
    }

    /// Fail a policy write once the crash-and-rerun tests' budget is spent,
    /// as a migrator killed before the write stops.
    pub(crate) fn test_stop(&self) -> OpResult<()> {
        #[cfg(any(test, feature = "test-util"))]
        if crate::migrations::stopped(self.stop_after.as_deref()) {
            return Err(OpError::Internal(
                "stopped by the test hook before a policy write".to_owned(),
            ));
        }
        Ok(())
    }

    /// Record `release` in the ledger rows of the account keyspaces this
    /// store creates: the server's own release.
    #[must_use]
    pub fn recording_release(mut self, release: &str) -> Self {
        release.clone_into(&mut self.release);
        self
    }

    /// Create a new catalog store with a pre-loaded encryption key.
    #[must_use]
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
            replication: Some(network_topology(&datacenter, replication_factor)),
            release: env!("CARGO_PKG_VERSION").to_owned(),
            encryption_key: Some(Arc::from(encryption_key.as_str())),
            metrics_ttl_seconds: 86400,
            #[cfg(any(test, feature = "test-util"))]
            stop_after: None,
        }
    }

    /// Borrow the underlying session (escape hatch for callers not yet migrated).
    #[must_use]
    pub fn session(&self) -> &Arc<CassandraSession> {
        &self.session
    }

    /// Get the cached encryption key. Returns `None` if not loaded at startup.
    #[must_use]
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

    /// Ensure an account keyspace exists with every account migration
    /// (idempotent): create it when missing, then run the account
    /// migrations on it through the migration runner, which waits for
    /// schema agreement after each statement and records each one in the
    /// keyspace's ledger. A retried CreateAccount whose earlier call stopped
    /// after the `accounts` row repairs the keyspace the same way.
    pub(crate) async fn ensure_account_keyspace(&self, account_id: &str) -> OpResult<()> {
        let keyspace_name = self.account_keyspace(account_id);
        let failed = |e: &dyn std::fmt::Display| {
            tracing::error!("Failed to initialize account keyspace {keyspace_name}: {e}");
            OpError::Internal("Failed to initialize account storage".to_owned())
        };
        let Some(replication) = &self.replication else {
            return Err(failed(
                &"this store was opened without the deployment's replication",
            ));
        };
        let cql = format!(
            "CREATE KEYSPACE IF NOT EXISTS {keyspace_name} WITH replication = {replication}"
        );
        self.session.query(cql).await.map_err(|e| failed(&e))?;
        crate::migrations::migrate_account(
            self.session.clone(),
            &self.keyspace_prefix,
            &keyspace_name,
            &self.release,
        )
        .await
        .map_err(|e| failed(&e))?;
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
        let query =
            format!("SELECT account_id FROM {catalog_keyspace}.accounts WHERE account_id = ?");

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
            let query = format!("SELECT value FROM {catalog_keyspace}.settings WHERE key = ?");

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
            let query =
                format!("INSERT INTO {catalog_keyspace}.settings (key, value) VALUES (?, ?)");

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
        self.encryption_key
            .as_ref()
            .map(std::string::ToString::to_string)
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
                && let Some(row) = rows.first()
            {
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
                && let Some(row) = rows.first()
            {
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
            let query = format!("SELECT account_id FROM {catalog_keyspace}.accounts LIMIT 1");
            let result = session.query(&query).await.map_err(|e| {
                extenddb_storage::diagnostics::DiagError::QueryFailed(format!(
                    "Failed to query accounts: {e}"
                ))
            })?;

            let body = result.response_body().map_err(|e| {
                extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
            })?;

            if let Some(rows) = body.into_rows()
                && let Some(row) = rows.first()
            {
                let account_id: String = row.get_r_by_name("account_id").map_err(|e| {
                    extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
                })?;

                // Test connection to account keyspace by querying schema_history
                let account_keyspace = format!("{keyspace_prefix}_account_{account_id}");
                let test_query = format!("SELECT COUNT(*) FROM {account_keyspace}.schema_history");

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
        principal: &str,
        window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        let principal = principal.to_owned();
        let session = self.session.clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let cutoff = chrono::Utc::now() - chrono::Duration::seconds(window_seconds);
            let cutoff_ms = cutoff.timestamp_millis();
            let query = format!(
                "SELECT COUNT(*) FROM {catalog_keyspace}.login_attempts \
                 WHERE principal = ? AND attempted_at > ? AND success = false \
                 ALLOW FILTERING"
            );
            let result = session
                .query_with_values(
                    &query,
                    cdrs_tokio::query_values!(principal.as_str(), cutoff_ms),
                )
                .await
                .map_err(|e| {
                    tracing::error!("count_principal_failures: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let count: i64 = result
                .response_body()
                .map_err(|e| OpError::Internal(e.to_string()))?
                .into_rows()
                .and_then(|mut rows| rows.pop())
                .and_then(|row| row.get_r_by_name("count").ok())
                .unwrap_or(0);
            Ok(count)
        })
    }

    fn count_ip_failures(
        &self,
        source_ip: &str,
        window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        let source_ip = source_ip.to_owned();
        let session = self.session.clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let cutoff = chrono::Utc::now() - chrono::Duration::seconds(window_seconds);
            let cutoff_ms = cutoff.timestamp_millis();
            // No partition key available for source_ip; ALLOW FILTERING is
            // acceptable because cleanup_old_attempts keeps the table bounded.
            let query = format!(
                "SELECT COUNT(*) FROM {catalog_keyspace}.login_attempts \
                 WHERE source_ip = ? AND attempted_at > ? AND success = false \
                 ALLOW FILTERING"
            );
            let result = session
                .query_with_values(
                    &query,
                    cdrs_tokio::query_values!(source_ip.as_str(), cutoff_ms),
                )
                .await
                .map_err(|e| {
                    tracing::error!("count_ip_failures: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            let count: i64 = result
                .response_body()
                .map_err(|e| OpError::Internal(e.to_string()))?
                .into_rows()
                .and_then(|mut rows| rows.pop())
                .and_then(|row| row.get_r_by_name("count").ok())
                .unwrap_or(0);
            Ok(count)
        })
    }

    fn record_failed_login(&self, principal: &str, source_ip: Option<&str>) -> BoxFuture<'_, ()> {
        let principal = principal.to_owned();
        let source_ip = source_ip.map(str::to_owned);
        let session = self.session.clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let now_ms = chrono::Utc::now().timestamp_millis();
            let query = format!(
                "INSERT INTO {catalog_keyspace}.login_attempts \
                 (principal, attempted_at, success, source_ip) VALUES (?, ?, false, ?)"
            );
            if let Err(e) = session
                .query_with_values(
                    &query,
                    cdrs_tokio::query_values!(principal.as_str(), now_ms, source_ip.as_deref()),
                )
                .await
            {
                tracing::error!("record_failed_login: {e}");
            }
        })
    }

    #[allow(clippy::result_large_err)] // response-body closure; boxing would ripple through cdrs plumbing
    fn cleanup_old_attempts(&self, max_age_seconds: i64) -> BoxFuture<'_, ()> {
        let session = self.session.clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let cutoff = chrono::Utc::now() - chrono::Duration::seconds(max_age_seconds);
            let cutoff_ms = cutoff.timestamp_millis();
            // Fetch principals with old records, then delete by partition key.
            // Cassandra does not support DELETE ... WHERE non-pk < ? without
            // ALLOW FILTERING; fetching principals first avoids a full scan on delete.
            let select =
                format!("SELECT DISTINCT principal FROM {catalog_keyspace}.login_attempts");
            let principals = match session
                .query(&select)
                .await
                .and_then(|r| r.response_body())
                .map(|b| b.into_rows().unwrap_or_default())
            {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::error!("cleanup_old_attempts fetch principals: {e}");
                    return;
                }
            };
            let delete = format!(
                "DELETE FROM {catalog_keyspace}.login_attempts \
                 WHERE principal = ? AND attempted_at < ?"
            );
            for row in principals {
                let Ok(principal): Result<String, _> = row.get_r_by_name("principal") else {
                    continue;
                };
                if let Err(e) = session
                    .query_with_values(
                        &delete,
                        cdrs_tokio::query_values!(principal.as_str(), cutoff_ms),
                    )
                    .await
                {
                    tracing::error!("cleanup_old_attempts delete for {principal}: {e}");
                }
            }
        })
    }
}

use extenddb_storage::management_store::MetricsRow;

impl MetricsStore for CassandraCatalogStore {
    fn insert_metrics(&self, rows: &[MetricsRow]) -> BoxFuture<'_, OpResult<()>> {
        let rows = rows.to_vec();
        let session = self.session.clone();
        let catalog_keyspace = self.catalog_keyspace();
        let ttl = self.metrics_ttl_seconds;
        Box::pin(async move {
            let query = format!(
                "INSERT INTO {catalog_keyspace}.metrics \
                 (bucket, metric, table_name, index_name, operation, sum, count, min, max) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL {ttl}"
            );
            for row in &rows {
                #[allow(clippy::cast_possible_truncation)]
                let bucket_ms = (row.bucket.unix_timestamp_nanos() / 1_000_000) as i64;
                if let Err(e) = session
                    .query_with_values(
                        &query,
                        cdrs_tokio::query_values!(
                            bucket_ms,
                            row.metric.as_str(),
                            row.table_name.as_deref().unwrap_or(""),
                            row.index_name.as_deref().unwrap_or(""),
                            row.operation.as_deref().unwrap_or(""),
                            row.sum,
                            row.count,
                            row.min,
                            row.max
                        ),
                    )
                    .await
                {
                    tracing::warn!("Failed to insert metrics row: {e}");
                }
            }
            Ok(())
        })
    }

    fn query_metrics(
        &self,
        start: time::OffsetDateTime,
        end: time::OffsetDateTime,
        table_name: Option<&str>,
        metric: Option<&str>,
    ) -> BoxFuture<'_, OpResult<Vec<MetricsRow>>> {
        let table_name = table_name.map(str::to_owned);
        let metric = metric.map(str::to_owned);
        let session = self.session.clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            #[allow(clippy::cast_possible_truncation)]
            let start_ms = (start.unix_timestamp_nanos() / 1_000_000) as i64;
            #[allow(clippy::cast_possible_truncation)]
            let end_ms = (end.unix_timestamp_nanos() / 1_000_000) as i64;

            // bucket is part of the composite partition key so a range scan
            // requires ALLOW FILTERING; the time window keeps the result bounded.
            let query = format!(
                "SELECT bucket, metric, table_name, index_name, operation, \
                 sum, count, min, max \
                 FROM {catalog_keyspace}.metrics \
                 WHERE bucket >= ? AND bucket <= ? ALLOW FILTERING"
            );
            let result = session
                .query_with_values(&query, cdrs_tokio::query_values!(start_ms, end_ms))
                .await
                .map_err(|e| {
                    tracing::warn!("query_metrics: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let rows = result
                .response_body()
                .map_err(|e| OpError::Internal(e.to_string()))?
                .into_rows()
                .unwrap_or_default();

            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                use crate::cassandra_util::get_column;
                let bucket_ms: i64 = get_column(&row, "bucket", "query_metrics")?;
                let bucket = time::OffsetDateTime::from_unix_timestamp(bucket_ms / 1000)
                    .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
                let metric_val: String = get_column(&row, "metric", "query_metrics")?;
                let tn: String = get_column(&row, "table_name", "query_metrics")?;
                let idx: String = get_column(&row, "index_name", "query_metrics")?;
                let op: String = get_column(&row, "operation", "query_metrics")?;

                if table_name.as_deref().is_some_and(|f| f != tn) {
                    continue;
                }
                if metric.as_deref().is_some_and(|f| f != metric_val) {
                    continue;
                }

                out.push(MetricsRow {
                    bucket,
                    metric: metric_val,
                    table_name: if tn.is_empty() { None } else { Some(tn) },
                    index_name: if idx.is_empty() { None } else { Some(idx) },
                    operation: if op.is_empty() { None } else { Some(op) },
                    sum: get_column(&row, "sum", "query_metrics")?,
                    count: get_column(&row, "count", "query_metrics")?,
                    min: get_column(&row, "min", "query_metrics")?,
                    max: get_column(&row, "max", "query_metrics")?,
                });
            }
            out.sort_by_key(|r| r.bucket);
            Ok(out)
        })
    }

    /// No-op: Cassandra native TTL (set on insert) handles metrics expiry.
    fn prune_metrics(&self, _retention: std::time::Duration) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async move { Ok(()) })
    }
}

impl extenddb_storage::CatalogStore for CassandraCatalogStore {
    fn cached_encryption_key(&self) -> Option<String> {
        self.encryption_key
            .as_ref()
            .map(std::string::ToString::to_string)
    }
}
