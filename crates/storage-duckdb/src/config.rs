// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage configuration for the DuckDB backend (`[storage.duckdb]`).

use std::any::Any;

use extenddb_storage::config::StorageConfig;
use serde::Deserialize;

/// DuckDB backend configuration.
///
/// `path` is the database file location; `:memory:` selects an ephemeral
/// in-memory database. `pool_size` bounds the read connection pool (writes are
/// serialized by the engine regardless).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuckDbConfig {
    #[serde(default = "default_path")]
    pub path: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
}

impl Default for DuckDbConfig {
    fn default() -> Self {
        Self {
            path: default_path(),
            pool_size: default_pool_size(),
        }
    }
}

/// The compiled-in default database location.
///
/// With the `memory` feature the default is `:memory:`, producing an ephemeral,
/// bootstrap-on-serve deployment with no file on disk. Otherwise the default is
/// a file in the working directory.
fn default_path() -> String {
    if cfg!(feature = "memory") {
        ":memory:".to_owned()
    } else {
        "extenddb.duckdb".to_owned()
    }
}

fn default_pool_size() -> u32 {
    10
}

impl StorageConfig for DuckDbConfig {
    fn connection_config(&self) -> &str {
        &self.path
    }

    fn max_connections(&self) -> u32 {
        self.pool_size
    }

    fn max_catalog_connections(&self) -> u32 {
        // Single DuckDB file — catalog and data share one connection pool.
        self.pool_size
    }

    fn clone_box(&self) -> Box<dyn StorageConfig> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Deserialize a `[storage.duckdb]` TOML table into a boxed `StorageConfig`.
pub fn deserialize_config(table: &toml::Table) -> Result<Box<dyn StorageConfig>, String> {
    let mut config: DuckDbConfig = table
        .clone()
        .try_into()
        .map_err(|e: toml::de::Error| format!("Failed to parse duckdb config: {e}"))?;
    config.path = resolve_path(config.path);
    Ok(Box::new(config))
}

/// Resolve a relative database file path to absolute, against the current
/// working directory.
///
/// This runs at config load — before `serve` daemonizes. Daemonization changes
/// the process working directory, so a relative `path` would otherwise resolve
/// differently (or fail to open) in the daemonized child. In-memory and URI
/// paths are left untouched.
fn resolve_path(path: String) -> String {
    if path.contains(":memory:") || path.starts_with("file:") || path.starts_with("duckdb:") {
        return path;
    }
    let p = std::path::Path::new(&path);
    if p.is_absolute() {
        return path;
    }
    match std::env::current_dir() {
        Ok(cwd) => cwd.join(p).to_string_lossy().into_owned(),
        Err(_) => path,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let c = DuckDbConfig::default();
        #[cfg(not(feature = "memory"))]
        assert_eq!(c.path, "extenddb.duckdb");
        #[cfg(feature = "memory")]
        assert_eq!(c.path, ":memory:");
        assert_eq!(c.pool_size, 10);
        assert_eq!(c.max_connections(), 10);
        assert_eq!(c.max_catalog_connections(), 10);
    }

    #[cfg(feature = "memory")]
    #[test]
    fn memory_feature_defaults_to_in_memory() {
        assert_eq!(DuckDbConfig::default().path, ":memory:");
        assert_eq!(default_path(), ":memory:");
    }

    #[test]
    fn deserialize_full() {
        let mut t = toml::Table::new();
        t.insert("path".into(), toml::Value::String(":memory:".into()));
        t.insert("pool_size".into(), toml::Value::Integer(4));
        let boxed = deserialize_config(&t).expect("parse");
        assert_eq!(boxed.connection_config(), ":memory:");
        assert_eq!(boxed.max_connections(), 4);
    }

    #[test]
    fn deserialize_rejects_unknown_field() {
        let mut t = toml::Table::new();
        t.insert("bogus".into(), toml::Value::Integer(1));
        assert!(deserialize_config(&t).is_err());
    }
}
