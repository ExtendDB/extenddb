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

/// Build a `PostgreSQL` connection URL from discrete components.
///
/// When `host` starts with `/` (a Unix socket directory), the host is moved into
/// the `?host=` query parameter and the URL host is set to `localhost`. This is
/// the PostgreSQL-documented form for socket connections and is accepted by
/// `sqlx::postgres::PgConnectOptions::from_url` and libpq. For TCP hosts the
/// classic `postgresql://user:password@host:port/database` form is emitted.
pub fn build_connection_url(
    user: &str,
    password: &str,
    host: &str,
    port: u16,
    database: &str,
) -> String {
    if host.starts_with('/') {
        format!("postgresql://{user}:{password}@localhost:{port}/{database}?host={host}")
    } else {
        format!("postgresql://{user}:{password}@{host}:{port}/{database}")
    }
}

/// Parse host, port, user, password, and database from a `PostgreSQL` connection string.
///
/// Handles the standard `postgresql://user:pass@host:port/db` format and the
/// socket-style `postgresql://user:pass@localhost:port/db?host=/path` form
/// produced by [`build_connection_url`] when the host is a Unix socket path.
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

    let (rest, query) = rest.split_once('?').map_or((rest, ""), |(r, q)| (r, q));

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

    // `?host=<path>` (libpq/sqlx idiom for Unix sockets) overrides the URL host
    // when present and non-empty. Last value wins, matching libpq semantics.
    let socket_host = query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .filter(|(k, _)| *k == "host")
        .map(|(_, v)| v)
        .next_back()
        .filter(|v| !v.is_empty());

    let host = socket_host.map_or_else(|| host.to_owned(), |v| v.to_owned());

    Ok(ConnParts {
        user,
        password,
        host,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgConnectOptions;
    use std::str::FromStr;

    #[test]
    fn build_url_tcp_host_uses_classic_form() {
        let url = build_connection_url("alice", "pw", "db.example.com", 5432, "extenddb_catalog");
        assert_eq!(
            url,
            "postgresql://alice:pw@db.example.com:5432/extenddb_catalog"
        );
    }

    #[test]
    fn build_url_unix_socket_uses_host_query() {
        let url = build_connection_url(
            "alice",
            "pw",
            "/var/run/postgresql",
            5432,
            "extenddb_catalog",
        );
        assert_eq!(
            url,
            "postgresql://alice:pw@localhost:5432/extenddb_catalog?host=/var/run/postgresql"
        );
    }

    #[test]
    fn parse_tcp_url_unchanged() {
        let parts =
            parse_connection_string("postgresql://alice:pw@db.example.com:5432/extenddb_catalog")
                .expect("parse");
        assert_eq!(parts.user, "alice");
        assert_eq!(parts.password, "pw");
        assert_eq!(parts.host, "db.example.com");
        assert_eq!(parts.port, 5432);
        assert_eq!(parts.database, "extenddb_catalog");
    }

    #[test]
    fn parse_socket_url_extracts_host_query() {
        let parts = parse_connection_string(
            "postgresql://alice:pw@localhost:5432/extenddb_catalog?host=/var/run/postgresql",
        )
        .expect("parse");
        assert_eq!(parts.user, "alice");
        assert_eq!(parts.password, "pw");
        assert_eq!(parts.host, "/var/run/postgresql");
        assert_eq!(parts.port, 5432);
        assert_eq!(parts.database, "extenddb_catalog");
    }

    #[test]
    fn parse_empty_host_query_falls_back_to_url_host() {
        let parts = parse_connection_string("postgresql://alice:pw@db.example.com:5432/db?host=")
            .expect("parse");
        assert_eq!(parts.host, "db.example.com");
    }

    #[test]
    fn parse_socket_url_ignores_unknown_query_params() {
        let parts = parse_connection_string(
            "postgresql://u:p@localhost:5432/db?application_name=extenddb&host=/sock&sslmode=disable",
        )
        .expect("parse");
        assert_eq!(parts.host, "/sock");
    }

    #[test]
    fn round_trip_socket_url_via_custom_parser() {
        let original_host = "/var/run/postgresql";
        let url = build_connection_url("alice", "pw", original_host, 5432, "extenddb_catalog");
        let parts = parse_connection_string(&url).expect("parse");
        assert_eq!(parts.host, original_host);
        assert_eq!(parts.port, 5432);
        assert_eq!(parts.database, "extenddb_catalog");
        assert_eq!(parts.user, "alice");
        assert_eq!(parts.password, "pw");
    }

    #[test]
    fn round_trip_tcp_url_via_custom_parser() {
        let url = build_connection_url("alice", "pw", "db.example.com", 5432, "extenddb_catalog");
        let parts = parse_connection_string(&url).expect("parse");
        assert_eq!(parts.host, "db.example.com");
        assert_eq!(parts.port, 5432);
        assert_eq!(parts.database, "extenddb_catalog");
    }

    /// Regression for issue #52: the URL the daemon reads must parse cleanly
    /// through `sqlx::postgres::PgConnectOptions::from_url`. The pre-fix URL
    /// `postgresql://u:p@/var/run/postgresql:5432/db` fails with "empty host".
    #[test]
    fn sqlx_accepts_socket_url_from_builder() {
        let url = build_connection_url("alice", "pw", "/var/run/postgresql", 5432, "db");
        PgConnectOptions::from_str(&url)
            .expect("sqlx PgConnectOptions::from_str must accept builder output for Unix sockets");
    }

    #[test]
    fn sqlx_accepts_tcp_url_from_builder() {
        let url = build_connection_url("alice", "pw", "db.example.com", 5432, "db");
        PgConnectOptions::from_str(&url)
            .expect("sqlx PgConnectOptions::from_str must accept builder output for TCP hosts");
    }
}
