// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite connection configuration.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteStorageConfig {
    /// Path to the SQLite database file.
    /// Use `:memory:` for an in-memory database.
    #[serde(default = "default_path")]
    pub path: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
}

impl Default for SqliteStorageConfig {
    fn default() -> Self {
        Self {
            path: default_path(),
            pool_size: default_pool_size(),
        }
    }
}

fn default_path() -> String {
    "extenddb.sqlite".to_owned()
}

fn default_pool_size() -> u32 {
    10
}

impl SqliteStorageConfig {
    /// Build the sqlx connection string from the path.
    pub fn connection_string(&self) -> String {
        if self.path == ":memory:" {
            "sqlite::memory:".to_owned()
        } else {
            format!("sqlite://{}?mode=rwc", self.path)
        }
    }
}

// ── StorageConfig trait implementation ────────────────────────────────

impl extenddb_storage::config::StorageConfig for SqliteStorageConfig {
    fn connection_config(&self) -> &str {
        &self.path
    }

    fn max_connections(&self) -> u32 {
        self.pool_size
    }

    fn max_catalog_connections(&self) -> u32 {
        self.pool_size
    }

    fn clone_box(&self) -> Box<dyn extenddb_storage::config::StorageConfig> {
        Box::new(self.clone())
    }
}
