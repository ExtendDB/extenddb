// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TiDB implementations of `SettingsStore`, `MetricsStore`, and
//! `RateLimitStore`.
//!
//! `TidbCatalogStore` wraps a `MySqlPool` connected to the catalog database
//! and implements the three operational traits defined in `extenddb_storage`.
//! This decouples callers from direct `sqlx::MySqlPool` usage, enabling
//! alternative storage backends.

use extenddb_storage::management_store::{MetricsRow, OpError, OpResult};
use futures::future::BoxFuture;
use sqlx::{MySql, MySqlPool, QueryBuilder};
use std::sync::Arc;

use crate::tidb_util::tidb_pool_options;

const METRICS_INSERT_BATCH_ROWS: usize = 200;

/// TiDB-backed catalog store for settings, metrics, and rate limiting.
///
/// Holds a connection pool to the catalog database. Created once at startup
/// and shared (via `Arc`) across management API handlers and background workers.
pub struct TidbCatalogStore {
    pool: MySqlPool,
    /// Cached encryption key (immutable after bootstrap). Avoids per-request
    /// DB query on access key and assume-role operations.
    encryption_key: Option<Arc<str>>,
}

impl TidbCatalogStore {
    /// Create a new catalog store wrapping the given pool.
    pub fn new(pool: MySqlPool) -> Self {
        Self {
            pool,
            encryption_key: None,
        }
    }

    /// Create a new catalog store with a pre-loaded encryption key.
    pub fn with_encryption_key(pool: MySqlPool, encryption_key: String) -> Self {
        Self {
            pool,
            encryption_key: Some(Arc::from(encryption_key.as_str())),
        }
    }

    /// Borrow the underlying pool (escape hatch for callers not yet migrated).
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    /// Get the cached encryption key. Returns `None` if not loaded at startup.
    pub fn encryption_key(&self) -> Option<&Arc<str>> {
        self.encryption_key.as_ref()
    }
}

fn metrics_query_sql(has_table_filter: bool, has_metric_filter: bool) -> String {
    let mut sql = String::from(
        "SELECT bucket, metric, table_name, index_name, operation, \
         SUM(sum) AS sum, CAST(SUM(count) AS SIGNED) AS count, \
         MIN(min) AS min, MAX(max) AS max \
         FROM metrics_samples \
         WHERE bucket >= ? AND bucket <= ?",
    );

    if has_table_filter {
        sql.push_str(" AND table_name = ?");
    }
    if has_metric_filter {
        sql.push_str(" AND metric = ?");
    }
    sql.push_str(" GROUP BY bucket, metric, table_name, index_name, operation ORDER BY bucket");

    sql
}

fn metrics_insert_query(rows: &[MetricsRow]) -> QueryBuilder<'_, MySql> {
    let mut query = QueryBuilder::<MySql>::new(
        "INSERT INTO metrics_samples \
         (bucket, metric, table_name, index_name, operation, sum, count, min, max) ",
    );
    query.push_values(rows, |mut values, row| {
        values
            .push_bind(row.bucket)
            .push_bind(&row.metric)
            .push_bind(row.table_name.as_deref().unwrap_or(""))
            .push_bind(row.index_name.as_deref().unwrap_or(""))
            .push_bind(row.operation.as_deref().unwrap_or(""))
            .push_bind(row.sum)
            .push_bind(row.count)
            .push_bind(row.min)
            .push_bind(row.max);
    });
    query
}

fn metrics_insert_chunks(rows: &[MetricsRow]) -> std::slice::Chunks<'_, MetricsRow> {
    rows.chunks(METRICS_INSERT_BATCH_ROWS)
}

// ── SettingsStore ──────────────────────────────────────────────────────

impl extenddb_storage::management_store::SettingsStore for TidbCatalogStore {
    fn get_setting(&self, key: &str) -> futures::future::BoxFuture<'_, OpResult<Option<String>>> {
        let key = key.to_string();
        let pool = self.pool.clone();
        Box::pin(async move {
            let row: Option<(String,)> =
                sqlx::query_as("SELECT value FROM settings WHERE `key` = ?")
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
                "INSERT INTO settings (`key`, value) VALUES (?, ?) \
                 ON DUPLICATE KEY UPDATE value = VALUES(value)",
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
            sqlx::query_as("SELECT `key`, value FROM settings ORDER BY `key`")
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

impl extenddb_storage::diagnostics::DiagnosticsStore for TidbCatalogStore {
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
                "SELECT value FROM settings WHERE `key` = 'data_database_connection_string'",
            )
            .fetch_optional(&pool)
            .await
            .map_err(|e| extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string()))?;

            let name_row: Option<(String,)> =
                sqlx::query_as("SELECT value FROM settings WHERE `key` = 'data_database_name'")
                    .fetch_optional(&pool)
                    .await
                    .map_err(|e| {
                        extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
                    })?;

            match (conn_row, name_row) {
                (Some((conn,)), Some((name,))) => {
                    // Test connection
                    tidb_pool_options(1, 0).connect(&conn).await.map_err(|e| {
                        extenddb_storage::diagnostics::DiagError::ConnectionFailed(e.to_string())
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

impl extenddb_storage::management_store::MetricsStore for TidbCatalogStore {
    fn insert_metrics(&self, rows: &[MetricsRow]) -> BoxFuture<'_, OpResult<()>> {
        let rows = rows.to_vec();
        Box::pin(async move {
            if rows.is_empty() {
                return Ok(());
            }

            for chunk in metrics_insert_chunks(&rows) {
                let result = metrics_insert_query(chunk)
                    .build()
                    .execute(&self.pool)
                    .await;
                if let Err(e) = result {
                    tracing::warn!("Failed to insert metrics sample batch: {e}");
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
            let table_filter = table_name.as_deref().filter(|s| !s.is_empty());
            let sql = metrics_query_sql(table_filter.is_some(), metric.is_some());

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

impl extenddb_storage::management_store::RateLimitStore for TidbCatalogStore {
    fn count_principal_failures(
        &self,
        principal: &str,
        window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        let principal = principal.to_owned();
        Box::pin(async move {
            let row: (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM login_attempts \
                 WHERE principal = ? AND success = false \
                 AND attempted_at > DATE_SUB(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND)",
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
                 WHERE source_ip = ? AND success = false \
                 AND attempted_at > DATE_SUB(CURRENT_TIMESTAMP(6), INTERVAL ? SECOND)",
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
                "INSERT INTO login_attempts (principal, success, source_ip) VALUES (?, false, ?)",
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
impl extenddb_storage::CatalogStore for TidbCatalogStore {
    fn cached_encryption_key(&self) -> Option<String> {
        self.encryption_key.as_ref().map(|arc| arc.to_string())
    }
}

#[cfg(test)]
mod tests {
    use time::OffsetDateTime;

    use super::{
        METRICS_INSERT_BATCH_ROWS, metrics_insert_chunks, metrics_insert_query, metrics_query_sql,
    };

    #[test]
    fn metrics_query_aggregates_append_only_samples_only() {
        let sql = metrics_query_sql(true, true);

        assert!(sql.contains("FROM metrics_samples"));
        assert!(sql.contains("SUM(sum) AS sum"));
        assert!(sql.contains("CAST(SUM(count) AS SIGNED) AS count"));
        assert!(sql.contains("GROUP BY bucket, metric, table_name, index_name, operation"));
        assert!(!sql.contains("UNION ALL"));
        assert!(!sql.contains("FROM metrics "));
        assert!(!sql.contains("ON DUPLICATE"));
        assert!(!sql.contains("writer_id"));
        assert!(!sql.contains("INSERT"));
    }

    #[test]
    fn metrics_insert_uses_one_append_only_multi_row_statement() {
        let rows = vec![sample_metric_row("put_item"), sample_metric_row("query")];
        let query = metrics_insert_query(&rows);
        let sql = query.sql();

        assert!(sql.starts_with("INSERT INTO metrics_samples"));
        assert_eq!(sql.matches("VALUES").count(), 1);
        assert_eq!(sql.matches("(?, ?, ?, ?, ?, ?, ?, ?, ?)").count(), 2);
        assert!(!sql.contains("ON DUPLICATE"));
        assert!(!sql.contains("UPDATE metrics"));
    }

    #[test]
    fn metrics_insert_batches_are_bounded_for_tidb_transactions() {
        let rows = (0..=METRICS_INSERT_BATCH_ROWS)
            .map(|i| sample_metric_row(&format!("op_{i}")))
            .collect::<Vec<_>>();

        let chunk_sizes = metrics_insert_chunks(&rows)
            .map(<[extenddb_storage::management_store::MetricsRow]>::len)
            .collect::<Vec<_>>();

        assert_eq!(chunk_sizes.len(), 2);
        assert_eq!(chunk_sizes[0], METRICS_INSERT_BATCH_ROWS);
        assert_eq!(chunk_sizes[1], 1);
    }

    fn sample_metric_row(operation: &str) -> extenddb_storage::management_store::MetricsRow {
        extenddb_storage::management_store::MetricsRow {
            bucket: OffsetDateTime::UNIX_EPOCH,
            metric: "latency".to_owned(),
            table_name: Some("table".to_owned()),
            index_name: None,
            operation: Some(operation.to_owned()),
            sum: 10.0,
            count: 2,
            min: 1.0,
            max: 9.0,
        }
    }
}
