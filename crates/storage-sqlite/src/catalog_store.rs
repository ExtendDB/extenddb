// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `SqliteCatalogStore`: the catalog/management store backing the management
//! API and authorization. Implements `SettingsStore`, `MetricsStore`,
//! `RateLimitStore`, `DiagnosticsStore`, `AdminStore` (in `admin_store`),
//! `AuthorizationStore` (in `authorization_store`), `ManagementStore` (in the
//! `management_store` module), and the `CatalogStore` supertrait.

use std::sync::Arc;

use extenddb_storage::CatalogStore;
use extenddb_storage::diagnostics::{DiagError, DiagResult, DiagnosticsStore};
use extenddb_storage::management_store::{
    MetricsRow, MetricsStore, OpError, OpResult, RateLimitStore, SettingsStore,
};
use futures::future::BoxFuture;
use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::sqlite_util::{format_timestamp, parse_timestamp};

/// Catalog store over the shared SQLite pool.
///
/// The encryption key is cached at construction (from the `settings` table) so
/// access-key creation/import can encrypt secrets without an extra query.
pub struct SqliteCatalogStore {
    pool: SqlitePool,
    encryption_key: Option<Arc<str>>,
}

impl SqliteCatalogStore {
    /// Construct without a cached encryption key (settings/diagnostics-only use).
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            encryption_key: None,
        }
    }

    /// Construct with the cached AES-256-GCM encryption key (base64).
    pub fn with_encryption_key(pool: SqlitePool, encryption_key: String) -> Self {
        Self {
            pool,
            encryption_key: Some(Arc::from(encryption_key.as_str())),
        }
    }

    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub(crate) fn encryption_key(&self) -> Option<&Arc<str>> {
        self.encryption_key.as_ref()
    }
}

// ── SettingsStore ──────────────────────────────────────────────────────

impl SettingsStore for SqliteCatalogStore {
    fn get_setting(&self, key: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let key = key.to_owned();
        Box::pin(async move {
            let row: Option<(String,)> = sqlx::query_as("SELECT value FROM settings WHERE key = ?")
                .bind(&key)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| OpError::Internal(format!("get_setting: {e}")))?;
            Ok(row.map(|(v,)| v))
        })
    }

    fn set_setting(&self, key: &str, value: &str) -> BoxFuture<'_, OpResult<()>> {
        let key = key.to_owned();
        let value = value.to_owned();
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO settings (key, value) VALUES (?, ?) \
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            )
            .bind(&key)
            .bind(&value)
            .execute(&self.pool)
            .await
            .map_err(|e| OpError::Internal(format!("set_setting: {e}")))?;
            Ok(())
        })
    }

    fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        Box::pin(async move {
            sqlx::query_as("SELECT key, value FROM settings ORDER BY key")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| OpError::Internal(format!("list_settings: {e}")))
        })
    }

    fn cached_encryption_key(&self) -> Option<String> {
        self.encryption_key.as_ref().map(|k| k.to_string())
    }
}

// ── MetricsStore ───────────────────────────────────────────────────────

#[derive(sqlx::FromRow)]
struct DbMetricsRow {
    bucket: String,
    metric: String,
    table_name: String,
    index_name: String,
    operation: String,
    sum: f64,
    count: i64,
    min: f64,
    max: f64,
}

impl MetricsStore for SqliteCatalogStore {
    fn insert_metrics(&self, rows: &[MetricsRow]) -> BoxFuture<'_, OpResult<()>> {
        let rows = rows.to_vec();
        Box::pin(async move {
            for row in &rows {
                let bucket = format_timestamp(row.bucket);
                let result = sqlx::query(
                    "INSERT INTO metrics \
                     (bucket, metric, table_name, index_name, operation, sum, count, min, max) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
                     ON CONFLICT(bucket, metric, table_name, index_name, operation) DO UPDATE SET \
                       sum = metrics.sum + excluded.sum, \
                       count = metrics.count + excluded.count, \
                       min = MIN(metrics.min, excluded.min), \
                       max = MAX(metrics.max, excluded.max)",
                )
                .bind(&bucket)
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
                    tracing::warn!("insert_metrics row failed: {e}");
                }
            }
            Ok(())
        })
    }

    fn query_metrics(
        &self,
        start: OffsetDateTime,
        end: OffsetDateTime,
        table_name: Option<&str>,
        metric: Option<&str>,
    ) -> BoxFuture<'_, OpResult<Vec<MetricsRow>>> {
        let table_name = table_name.map(str::to_owned);
        let metric = metric.map(str::to_owned);
        let start_str = format_timestamp(start);
        let end_str = format_timestamp(end);
        Box::pin(async move {
            let mut sql = String::from(
                "SELECT bucket, metric, table_name, index_name, operation, sum, count, min, max \
                 FROM metrics WHERE bucket >= ? AND bucket <= ?",
            );
            let table_filter = table_name.as_deref().filter(|s| !s.is_empty());
            if table_filter.is_some() {
                sql.push_str(" AND table_name = ?");
            }
            if metric.is_some() {
                sql.push_str(" AND metric = ?");
            }
            sql.push_str(" ORDER BY bucket");

            let mut q = sqlx::query_as::<_, DbMetricsRow>(&sql)
                .bind(&start_str)
                .bind(&end_str);
            if let Some(tn) = table_filter {
                q = q.bind(tn.to_owned());
            }
            if let Some(m) = metric.as_deref() {
                q = q.bind(m.to_owned());
            }

            let rows = q
                .fetch_all(&self.pool)
                .await
                .map_err(|e| OpError::Internal(format!("query_metrics: {e}")))?;

            Ok(rows
                .into_iter()
                .filter_map(|r| {
                    Some(MetricsRow {
                        bucket: parse_timestamp(&r.bucket).ok()?,
                        metric: r.metric,
                        table_name: (!r.table_name.is_empty()).then_some(r.table_name),
                        index_name: (!r.index_name.is_empty()).then_some(r.index_name),
                        operation: (!r.operation.is_empty()).then_some(r.operation),
                        sum: r.sum,
                        count: r.count,
                        min: r.min,
                        max: r.max,
                    })
                })
                .collect())
        })
    }

    fn prune_metrics(&self, retention: std::time::Duration) -> BoxFuture<'_, OpResult<()>> {
        Box::pin(async move {
            let cutoff = OffsetDateTime::now_utc()
                - time::Duration::seconds(i64::try_from(retention.as_secs()).unwrap_or(i64::MAX));
            let cutoff_str = format_timestamp(cutoff);
            sqlx::query("DELETE FROM metrics WHERE bucket < ?")
                .bind(&cutoff_str)
                .execute(&self.pool)
                .await
                .map_err(|e| OpError::Internal(format!("prune_metrics: {e}")))?;
            Ok(())
        })
    }
}

// ── RateLimitStore ─────────────────────────────────────────────────────

impl RateLimitStore for SqliteCatalogStore {
    fn count_principal_failures(
        &self,
        principal: &str,
        window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        let principal = principal.to_owned();
        Box::pin(async move {
            let cutoff = format_timestamp(
                OffsetDateTime::now_utc() - time::Duration::seconds(window_seconds),
            );
            let row: (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM login_attempts \
                 WHERE principal = ? AND success = 0 AND attempted_at > ?",
            )
            .bind(&principal)
            .bind(&cutoff)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| OpError::Internal(format!("count_principal_failures: {e}")))?;
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
            let cutoff = format_timestamp(
                OffsetDateTime::now_utc() - time::Duration::seconds(window_seconds),
            );
            let row: (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM login_attempts \
                 WHERE source_ip = ? AND success = 0 AND attempted_at > ?",
            )
            .bind(&source_ip)
            .bind(&cutoff)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| OpError::Internal(format!("count_ip_failures: {e}")))?;
            Ok(row.0)
        })
    }

    fn record_failed_login(&self, principal: &str, source_ip: Option<&str>) -> BoxFuture<'_, ()> {
        let principal = principal.to_owned();
        let source_ip = source_ip.map(str::to_owned);
        Box::pin(async move {
            let now = format_timestamp(OffsetDateTime::now_utc());
            let result = sqlx::query(
                "INSERT INTO login_attempts (principal, attempted_at, success, source_ip) \
                 VALUES (?, ?, 0, ?)",
            )
            .bind(&principal)
            .bind(&now)
            .bind(source_ip.as_deref())
            .execute(&self.pool)
            .await;
            if let Err(e) = result {
                tracing::error!("record_failed_login: {e}");
            }
        })
    }

    fn cleanup_old_attempts(&self, max_age_seconds: i64) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let cutoff = format_timestamp(
                OffsetDateTime::now_utc() - time::Duration::seconds(max_age_seconds),
            );
            if let Err(e) = sqlx::query("DELETE FROM login_attempts WHERE attempted_at < ?")
                .bind(&cutoff)
                .execute(&self.pool)
                .await
            {
                tracing::error!("cleanup_old_attempts: {e}");
            }
        })
    }
}

// ── DiagnosticsStore ───────────────────────────────────────────────────

impl DiagnosticsStore for SqliteCatalogStore {
    fn count_tables(&self) -> BoxFuture<'_, DiagResult<i64>> {
        Box::pin(async move {
            let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM tables")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| DiagError::QueryFailed(e.to_string()))?;
            Ok(count)
        })
    }

    fn count_indexes(&self) -> BoxFuture<'_, DiagResult<i64>> {
        Box::pin(async move {
            let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM indexes")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| DiagError::QueryFailed(e.to_string()))?;
            Ok(count)
        })
    }

    fn test_data_database_connection(&self) -> BoxFuture<'_, DiagResult<String>> {
        Box::pin(async move {
            // Catalog and data share one SQLite file; report its recorded name.
            let row: Option<(String,)> =
                sqlx::query_as("SELECT value FROM settings WHERE key = 'data_database_name'")
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| DiagError::QueryFailed(e.to_string()))?;
            Ok(row.map_or_else(|| "sqlite (embedded)".to_owned(), |(n,)| n))
        })
    }
}

// ── CatalogStore supertrait ────────────────────────────────────────────

impl CatalogStore for SqliteCatalogStore {
    fn cached_encryption_key(&self) -> Option<String> {
        self.encryption_key.as_ref().map(|k| k.to_string())
    }
}
