// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL implementations of `SettingsStore`, `MetricsStore`, and
//! `RateLimitStore`.
//!
//! `PostgresCatalogStore` wraps a `PgPool` connected to the catalog database
//! and implements the three operational traits defined in `extenddb_storage`.
//! This decouples callers from direct `sqlx::PgPool` usage, enabling
//! alternative storage backends.

use std::sync::Arc;

use extenddb_storage::management_store::{MetricsRow, OpError, OpResult};
use futures::future::BoxFuture;
use sqlx::PgPool;

/// PostgreSQL-backed catalog store for settings, metrics, and rate limiting.
///
/// Holds a connection pool to the catalog database. Created once at startup
/// and shared (via `Arc`) across management API handlers and background workers.
pub struct PostgresCatalogStore {
    pool: PgPool,
    /// P119: Cached encryption key (immutable after bootstrap). Avoids
    /// per-request DB query on access key and assume-role operations.
    encryption_key: Option<Arc<str>>,
}

impl PostgresCatalogStore {
    /// Create a new catalog store wrapping the given pool.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            encryption_key: None,
        }
    }

    /// Create a new catalog store with a pre-loaded encryption key (P119).
    pub fn with_encryption_key(pool: PgPool, encryption_key: String) -> Self {
        Self {
            pool,
            encryption_key: Some(Arc::from(encryption_key.as_str())),
        }
    }

    /// Borrow the underlying pool (escape hatch for callers not yet migrated).
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get the cached encryption key. Returns `None` if not loaded at startup.
    pub fn encryption_key(&self) -> Option<&Arc<str>> {
        self.encryption_key.as_ref()
    }
}

// ── SettingsStore ──────────────────────────────────────────────────────

impl extenddb_storage::management_store::SettingsStore for PostgresCatalogStore {
    fn get_setting(&self, key: &str) -> futures::future::BoxFuture<'_, OpResult<Option<String>>> {
        let key = key.to_string();
        let pool = self.pool.clone();
        Box::pin(async move {
            let row: Option<(String,)> =
                sqlx::query_as("SELECT value FROM settings WHERE key = $1")
                    .bind(&key)
                    .fetch_optional(&pool)
                    .await
                    .map_err(|e| {
                        tracing::error!("get_setting: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
            Ok(row.map(|(v,)| v))
        })
    }

    fn set_setting(&self, key: &str, value: &str) -> futures::future::BoxFuture<'_, OpResult<()>> {
        let key = key.to_string();
        let value = value.to_string();
        let pool = self.pool.clone();
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO settings (key, value) VALUES ($1, $2) \
                 ON CONFLICT (key) DO UPDATE SET value = $2",
            )
            .bind(&key)
            .bind(&value)
            .execute(&pool)
            .await
            .map_err(|e| {
                tracing::error!("set_setting: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(())
        })
    }

    fn list_settings(&self) -> futures::future::BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            sqlx::query_as("SELECT key, value FROM settings ORDER BY key")
                .fetch_all(&pool)
                .await
                .map_err(|e| {
                    tracing::error!("list_settings: {e}");
                    OpError::Internal("Database error".to_owned())
                })
        })
    }

    fn cached_encryption_key(&self) -> Option<String> {
        self.encryption_key.as_ref().map(|k| k.to_string())
    }
}

// ── DiagnosticsStore ───────────────────────────────────────────────────

impl extenddb_storage::diagnostics::DiagnosticsStore for PostgresCatalogStore {
    fn count_tables(
        &self,
    ) -> futures::future::BoxFuture<'_, extenddb_storage::diagnostics::DiagResult<i64>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM tables")
                .fetch_one(&pool)
                .await
                .map_err(|e| {
                    extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
                })?;
            Ok(count)
        })
    }

    fn count_indexes(
        &self,
    ) -> futures::future::BoxFuture<'_, extenddb_storage::diagnostics::DiagResult<i64>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM indexes")
                .fetch_one(&pool)
                .await
                .map_err(|e| {
                    extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
                })?;
            Ok(count)
        })
    }

    fn test_data_database_connection(
        &self,
    ) -> futures::future::BoxFuture<'_, extenddb_storage::diagnostics::DiagResult<String>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            // Get data database connection string and name from settings
            let conn_row: Option<(String,)> = sqlx::query_as(
                "SELECT value FROM settings WHERE key = 'data_database_connection_string'",
            )
            .fetch_optional(&pool)
            .await
            .map_err(|e| extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string()))?;

            let name_row: Option<(String,)> =
                sqlx::query_as("SELECT value FROM settings WHERE key = 'data_database_name'")
                    .fetch_optional(&pool)
                    .await
                    .map_err(|e| {
                        extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
                    })?;

            match (conn_row, name_row) {
                (Some((conn,)), Some((name,))) => {
                    // Test connection
                    sqlx::postgres::PgPoolOptions::new()
                        .max_connections(1)
                        .connect(&conn)
                        .await
                        .map_err(|e| {
                            extenddb_storage::diagnostics::DiagError::ConnectionFailed(
                                e.to_string(),
                            )
                        })?;
                    Ok(name)
                }
                _ => Err(extenddb_storage::diagnostics::DiagError::QueryFailed(
                    "Data database not configured".to_string(),
                )),
            }
        })
    }
}

// ── MetricsStore ───────────────────────────────────────────────────────

impl extenddb_storage::management_store::MetricsStore for PostgresCatalogStore {
    fn insert_metrics(&self, rows: &[MetricsRow]) -> BoxFuture<'_, OpResult<()>> {
        let rows = rows.to_vec();
        Box::pin(async move {
            for row in &rows {
                let result = sqlx::query(
                    "INSERT INTO metrics \
                     (bucket, metric, table_name, index_name, operation, sum, count, min, max) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
                     ON CONFLICT (bucket, metric, table_name, index_name, operation) \
                     DO UPDATE SET sum = metrics.sum + EXCLUDED.sum, \
                                   count = metrics.count + EXCLUDED.count, \
                                   min = LEAST(metrics.min, EXCLUDED.min), \
                                   max = GREATEST(metrics.max, EXCLUDED.max)",
                )
                .bind(row.bucket)
                .bind(&row.metric)
                .bind(row.table_name.as_deref().unwrap_or(""))
                .bind(row.index_name.as_deref().unwrap_or(""))
                .bind(row.operation.as_deref().unwrap_or(""))
                .bind(row.sum)
                .bind(row.count)
                .bind(row.min)
                .bind(row.max)
                .execute(&self.pool)
                .await;
                if let Err(e) = result {
                    tracing::warn!("Failed to upsert metrics row: {e}");
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
        let table_name = table_name.map(|s| s.to_owned());
        let metric = metric.map(|s| s.to_owned());
        Box::pin(async move {
            use std::fmt::Write as _;

            let mut sql = String::from(
                "SELECT bucket, metric, table_name, index_name, operation, \
                 sum, count, min, max \
                 FROM metrics WHERE bucket >= $1 AND bucket <= $2",
            );
            let mut param_idx = 3u32;

            let table_filter = table_name.as_deref().filter(|s| !s.is_empty());
            if table_filter.is_some() {
                let _ = write!(sql, " AND table_name = ${param_idx}");
                param_idx += 1;
            }
            if metric.is_some() {
                let _ = write!(sql, " AND metric = ${param_idx}");
            }
            let _ = param_idx;
            sql.push_str(" ORDER BY bucket");

            // Build the query with dynamic binds.
            let mut query = sqlx::query_as::<_, DbMetricsRow>(&sql)
                .bind(start)
                .bind(end);
            if let Some(tn) = table_filter {
                query = query.bind(tn);
            }
            if let Some(mn) = metric.as_deref() {
                query = query.bind(mn);
            }

            let rows = query.fetch_all(&self.pool).await.map_err(|e| {
                tracing::warn!("query_metrics: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            Ok(rows
                .into_iter()
                .map(|r| MetricsRow {
                    bucket: r.bucket,
                    metric: r.metric,
                    table_name: if r.table_name.is_empty() {
                        None
                    } else {
                        Some(r.table_name)
                    },
                    index_name: if r.index_name.is_empty() {
                        None
                    } else {
                        Some(r.index_name)
                    },
                    operation: if r.operation.is_empty() {
                        None
                    } else {
                        Some(r.operation)
                    },
                    sum: r.sum,
                    count: r.count,
                    min: r.min,
                    max: r.max,
                })
                .collect())
        })
    }
}

/// Internal row type for `sqlx::FromRow` derivation.
#[derive(sqlx::FromRow)]
struct DbMetricsRow {
    bucket: time::OffsetDateTime,
    metric: String,
    table_name: String,
    index_name: String,
    operation: String,
    sum: f64,
    count: i64,
    min: f64,
    max: f64,
}

// ── RateLimitStore ─────────────────────────────────────────────────────

impl extenddb_storage::management_store::RateLimitStore for PostgresCatalogStore {
    fn count_principal_failures(
        &self,
        principal: &str,
        window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        let principal = principal.to_owned();
        Box::pin(async move {
            let row: (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM login_attempts \
                 WHERE principal = $1 AND success = false \
                 AND attempted_at > NOW() - make_interval(secs => $2)",
            )
            .bind(&principal)
            .bind(window_seconds)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                tracing::error!("count_principal_failures: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(row.0)
        })
    }

    fn count_ip_failures(
        &self,
        source_ip: &str,
        window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        let source_ip = source_ip.to_owned();
        Box::pin(async move {
            let row: (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM login_attempts \
                 WHERE source_ip = $1 AND success = false \
                 AND attempted_at > NOW() - make_interval(secs => $2)",
            )
            .bind(&source_ip)
            .bind(window_seconds)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                tracing::error!("count_ip_failures: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(row.0)
        })
    }

    fn record_failed_login(&self, principal: &str, source_ip: Option<&str>) -> BoxFuture<'_, ()> {
        let principal = principal.to_owned();
        let source_ip = source_ip.map(|s| s.to_owned());
        Box::pin(async move {
            let result = sqlx::query(
                "INSERT INTO login_attempts (principal, success, source_ip) VALUES ($1, false, $2)",
            )
            .bind(&principal)
            .bind(source_ip.as_deref())
            .execute(&self.pool)
            .await;
            if let Err(e) = result {
                tracing::error!("Failed to record login attempt: {e}");
            }
        })
    }
}

// Implement CatalogStore supertrait
impl extenddb_storage::CatalogStore for PostgresCatalogStore {
    fn cached_encryption_key(&self) -> Option<String> {
        self.encryption_key.as_ref().map(|arc| arc.to_string())
    }
}
