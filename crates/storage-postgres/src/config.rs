// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL connection configuration.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresStorageConfig {
    #[serde(default = "default_connection_string")]
    pub connection_string: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
    /// Maximum connections for the management/catalog pool (authz, IAM, console).
    /// Defaults to `pool_size` if not set.
    #[serde(default)]
    pub catalog_pool_size: Option<u32>,
}

impl Default for PostgresStorageConfig {
    fn default() -> Self {
        Self {
            connection_string: default_connection_string(),
            pool_size: default_pool_size(),
            catalog_pool_size: None,
        }
    }
}

fn default_connection_string() -> String {
    "postgresql://extenddb:extenddb-local-dev@localhost:5432/extenddb_catalog".to_owned()
}

fn default_pool_size() -> u32 {
    20
}

/// Parsed components of a `PostgreSQL` connection string.
pub struct ConnParts {
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub database: String,
}

/// Parse host, port, user, password, and database from a `PostgreSQL` connection string.
///
/// Handles the standard `postgresql://user:pass@host:port/db` format.
///
/// # Errors
///
/// Returns an error if the connection string doesn't match the expected format.
pub fn parse_connection_string(conn: &str) -> anyhow::Result<ConnParts> {
    let rest = conn
        .strip_prefix("postgresql://")
        .or_else(|| conn.strip_prefix("postgres://"))
        .ok_or_else(|| {
            anyhow::anyhow!("Connection string must start with postgresql:// or postgres://")
        })?;

    let (userpass, hostdb) = rest
        .split_once('@')
        .ok_or_else(|| anyhow::anyhow!("Connection string missing '@' separator"))?;

    let (user, password) = userpass.split_once(':').map_or_else(
        || (userpass.to_owned(), String::new()),
        |(u, p)| (u.to_owned(), p.to_owned()),
    );

    let (hostport, database) = hostdb
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("Connection string missing /database"))?;

    let (host, port_str) = hostport
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("Connection string missing :port"))?;

    let port: u16 = port_str
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid port: {port_str}"))?;

    Ok(ConnParts {
        user,
        password,
        host: host.to_owned(),
        port,
        database: database.to_owned(),
    })
}

// ── StorageConfig trait implementation ────────────────────────────────

impl extenddb_storage::config::StorageConfig for PostgresStorageConfig {
    fn connection_config(&self) -> &str {
        &self.connection_string
    }

    fn max_connections(&self) -> u32 {
        self.pool_size
    }

    fn max_catalog_connections(&self) -> u32 {
        self.catalog_pool_size.unwrap_or(self.pool_size)
    }

    fn clone_box(&self) -> Box<dyn extenddb_storage::config::StorageConfig> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
