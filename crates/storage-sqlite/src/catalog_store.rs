// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite implementations of `SettingsStore`, `MetricsStore`, `RateLimitStore`,
//! `DiagnosticsStore`, and `CatalogStore`.

use std::sync::Arc;

use extenddb_storage::management_store::{MetricsRow, OpError, OpResult};
use futures::future::BoxFuture;
use sqlx::SqlitePool;

/// SQLite-backed catalog store for settings, metrics, and rate limiting.
pub struct SqliteCatalogStore {
    pool: SqlitePool,
    encryption_key: Option<Arc<str>>,
}

impl SqliteCatalogStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            encryption_key: None,
        }
    }

    pub fn with_encryption_key(pool: SqlitePool, encryption_key: String) -> Self {
        Self {
            pool,
            encryption_key: Some(Arc::from(encryption_key.as_str())),
        }
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub fn encryption_key(&self) -> Option<&Arc<str>> {
        self.encryption_key.as_ref()
    }
}

// ── SettingsStore ──────────────────────────────────────────────────────

impl extenddb_storage::management_store::SettingsStore for SqliteCatalogStore {
    fn get_setting(&self, key: &str) -> BoxFuture<'_, OpResult<Option<String>>> {
        let key = key.to_string();
        let pool = self.pool.clone();
        Box::pin(async move {
            let row: Option<(String,)> =
                sqlx::query_as("SELECT value FROM settings WHERE key = ?")
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

    fn set_setting(&self, key: &str, value: &str) -> BoxFuture<'_, OpResult<()>> {
        let key = key.to_string();
        let value = value.to_string();
        let pool = self.pool.clone();
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO settings (key, value) VALUES (?, ?)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
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

    fn list_settings(&self) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
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

impl extenddb_storage::diagnostics::DiagnosticsStore for SqliteCatalogStore {
    fn count_tables(
        &self,
    ) -> BoxFuture<'_, extenddb_storage::diagnostics::DiagResult<i64>> {
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
    ) -> BoxFuture<'_, extenddb_storage::diagnostics::DiagResult<i64>> {
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
    ) -> BoxFuture<'_, extenddb_storage::diagnostics::DiagResult<String>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            // For SQLite, catalog and data are in the same database.
            let name_row: Option<(String,)> =
                sqlx::query_as("SELECT value FROM settings WHERE key = 'data_database_name'")
                    .fetch_optional(&pool)
                    .await
                    .map_err(|e| {
                        extenddb_storage::diagnostics::DiagError::QueryFailed(e.to_string())
                    })?;
            Ok(name_row
                .map(|(n,)| n)
                .unwrap_or_else(|| "sqlite (embedded)".to_owned()))
        })
    }
}

// ── MetricsStore ───────────────────────────────────────────────────────

impl extenddb_storage::management_store::MetricsStore for SqliteCatalogStore {
    fn insert_metrics(&self, rows: &[MetricsRow]) -> BoxFuture<'_, OpResult<()>> {
        let rows = rows.to_vec();
        let pool = self.pool.clone();
        Box::pin(async move {
            for row in &rows {
                let bucket = crate::sqlite_util::format_timestamp(row.bucket);
                let result = sqlx::query(
                    "INSERT INTO metrics \
                     (bucket, metric, table_name, index_name, operation, sum, count, min, max) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
                     ON CONFLICT(bucket, metric, table_name, index_name, operation) \
                     DO UPDATE SET \
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
                .execute(&pool)
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
        let start_str = crate::sqlite_util::format_timestamp(start);
        let end_str = crate::sqlite_util::format_timestamp(end);
        let pool = self.pool.clone();
        Box::pin(async move {
            let mut sql = String::from(
                "SELECT bucket, metric, table_name, index_name, operation, \
                 sum, count, min, max \
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
                q = q.bind(tn);
            }
            if let Some(mn) = metric.as_deref() {
                q = q.bind(mn);
            }

            let rows = q.fetch_all(&pool).await.map_err(|e| {
                tracing::warn!("query_metrics: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            Ok(rows
                .into_iter()
                .filter_map(|r| {
                    let bucket = crate::sqlite_util::parse_timestamp(&r.bucket).ok()?;
                    Some(MetricsRow {
                        bucket,
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
                })
                .collect())
        })
    }

    fn prune_metrics(&self, retention: std::time::Duration) -> BoxFuture<'_, OpResult<()>> {
        let pool = self.pool.clone();
        Box::pin(async move {
            #[allow(clippy::cast_possible_wrap)]
            let cutoff = time::OffsetDateTime::now_utc()
                - time::Duration::seconds(retention.as_secs() as i64);
            let cutoff_str = crate::sqlite_util::format_timestamp(cutoff);
            sqlx::query("DELETE FROM metrics WHERE bucket < ?")
                .bind(&cutoff_str)
                .execute(&pool)
                .await
                .map_err(|e| {
                    tracing::warn!("prune_metrics: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
            Ok(())
        })
    }
}

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

// ── RateLimitStore ─────────────────────────────────────────────────────

impl extenddb_storage::management_store::RateLimitStore for SqliteCatalogStore {
    fn count_principal_failures(
        &self,
        principal: &str,
        window_seconds: i64,
    ) -> BoxFuture<'_, OpResult<i64>> {
        let principal = principal.to_owned();
        let pool = self.pool.clone();
        Box::pin(async move {
            let cutoff = time::OffsetDateTime::now_utc()
                - time::Duration::seconds(window_seconds);
            let cutoff_str = crate::sqlite_util::format_timestamp(cutoff);
            let row: (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM login_attempts \
                 WHERE principal = ? AND success = 0 AND attempted_at > ?",
            )
            .bind(&principal)
            .bind(&cutoff_str)
            .fetch_one(&pool)
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
        let pool = self.pool.clone();
        Box::pin(async move {
            let cutoff = time::OffsetDateTime::now_utc()
                - time::Duration::seconds(window_seconds);
            let cutoff_str = crate::sqlite_util::format_timestamp(cutoff);
            let row: (i64,) = sqlx::query_as(
                "SELECT COUNT(*) FROM login_attempts \
                 WHERE source_ip = ? AND success = 0 AND attempted_at > ?",
            )
            .bind(&source_ip)
            .bind(&cutoff_str)
            .fetch_one(&pool)
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
        let pool = self.pool.clone();
        Box::pin(async move {
            let now = crate::sqlite_util::format_timestamp(time::OffsetDateTime::now_utc());
            let result = sqlx::query(
                "INSERT INTO login_attempts (principal, attempted_at, success, source_ip) \
                 VALUES (?, ?, 0, ?)",
            )
            .bind(&principal)
            .bind(&now)
            .bind(source_ip.as_deref())
            .execute(&pool)
            .await;
            if let Err(e) = result {
                tracing::error!("Failed to record login attempt: {e}");
            }
        })
    }

    fn cleanup_old_attempts(&self, max_age_seconds: i64) -> BoxFuture<'_, ()> {
        let pool = self.pool.clone();
        Box::pin(async move {
            let cutoff = time::OffsetDateTime::now_utc()
                - time::Duration::seconds(max_age_seconds);
            let cutoff_str = crate::sqlite_util::format_timestamp(cutoff);
            let result = sqlx::query("DELETE FROM login_attempts WHERE attempted_at < ?")
                .bind(&cutoff_str)
                .execute(&pool)
                .await;
            match result {
                Ok(r) => {
                    if r.rows_affected() > 0 {
                        tracing::debug!(
                            "Cleaned up {} old login attempt records",
                            r.rows_affected()
                        );
                    }
                }
                Err(e) => tracing::error!("Login attempt cleanup failed: {e}"),
            }
        })
    }
}

// Implement CatalogStore supertrait
impl extenddb_storage::CatalogStore for SqliteCatalogStore {
    fn cached_encryption_key(&self) -> Option<String> {
        self.encryption_key.as_ref().map(|arc| arc.to_string())
    }
}
