// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite storage engine struct and construction.

use std::sync::Arc;

use extenddb_storage::error::StorageError;
use sqlx::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;

/// Expected catalog version for the SQLite backend.
pub const CATALOG_VERSION: extenddb_core::version::CatalogVersion =
    extenddb_core::version::CatalogVersion::new(0, 0, 2);

/// SQLite storage backend.
///
/// Uses a single SQLite pool for all catalog and data operations.
/// WAL mode is enabled for concurrent reads alongside writes.
pub struct SqliteEngine {
    pub(crate) pool: SqlitePool,
    pub(crate) region: String,
    pub(crate) max_item_size_bytes: usize,
    pub(crate) control_plane_notify: Arc<tokio::sync::Notify>,
    #[allow(dead_code)]
    pub(crate) gsi_default_delay_ms: Arc<std::sync::atomic::AtomicU64>,
}

impl SqliteEngine {
    pub async fn new(config: &SqliteConfig, region: &str) -> Result<Self, StorageError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(config.pool_size)
            .min_connections(2)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    use sqlx::Executor;
                    conn.execute("PRAGMA journal_mode=WAL").await?;
                    conn.execute("PRAGMA foreign_keys=ON").await?;
                    conn.execute("PRAGMA synchronous=NORMAL").await?;
                    conn.execute("PRAGMA busy_timeout=5000").await?;
                    conn.execute("PRAGMA cache_size=-32000").await?;
                    Ok(())
                })
            })
            .connect(&config.connection_string)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let initial_gsi_delay: u64 = sqlx::query_as::<_, (String,)>(
            "SELECT value FROM settings WHERE key = 'gsi_propagation_delay_ms'",
        )
        .fetch_optional(&pool)
        .await
        .ok()
        .flatten()
        .and_then(|(v,)| v.parse::<u64>().ok())
        .unwrap_or(10);

        Ok(Self {
            pool,
            region: region.to_owned(),
            max_item_size_bytes: config.max_item_size_bytes,
            control_plane_notify: Arc::new(tokio::sync::Notify::new()),
            gsi_default_delay_ms: Arc::new(std::sync::atomic::AtomicU64::new(initial_gsi_delay)),
        })
    }

    #[allow(dead_code)]
    pub fn control_plane_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.control_plane_notify)
    }

    pub(crate) fn validate_account_id(account_id: &str) -> Result<(), StorageError> {
        if account_id.contains('"') || account_id.contains('\0') || !account_id.is_ascii() {
            return Err(StorageError::Internal(
                "account_id contains invalid characters for use in SQL identifiers".to_owned(),
            ));
        }
        Ok(())
    }

    pub async fn check_catalog_version(&self) -> Result<(), StorageError> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='settings')",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !exists {
            return Err(StorageError::CatalogNotInitialized);
        }

        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM settings WHERE key = 'catalog_version'")
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?;

        let found_str = row.ok_or(StorageError::CatalogNotInitialized)?.0;

        let found = found_str
            .parse::<extenddb_core::version::CatalogVersion>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if found != CATALOG_VERSION {
            return Err(StorageError::CatalogVersionMismatch {
                expected: CATALOG_VERSION.to_string(),
                found: found_str,
            });
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

/// Configuration for the SQLite storage backend.
pub struct SqliteConfig {
    /// SQLite connection string. e.g. `sqlite:///path/to/db.sqlite`
    pub connection_string: String,
    /// Maximum pool size.
    pub pool_size: u32,
    /// Maximum item size in bytes.
    pub max_item_size_bytes: usize,
}
