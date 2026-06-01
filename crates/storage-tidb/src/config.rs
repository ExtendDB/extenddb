// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TiDB connection configuration.

use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use serde::Deserialize;
use url::Url;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TidbStorageConfig {
    #[serde(default = "default_connection_string")]
    pub connection_string: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
    /// Maximum connections for catalog metadata, control-plane, authz, IAM, and console work.
    /// Defaults to `pool_size` if not set.
    #[serde(default)]
    pub catalog_pool_size: Option<u32>,
    #[serde(default)]
    pub backup: TidbBackupConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TidbBackupConfig {
    /// Backup executable. Defaults to `tiup`; use `br` with `component = ""`
    /// when the BR binary is installed directly.
    #[serde(default)]
    pub binary: Option<String>,
    /// Optional component/subcommand after `binary`. Defaults to `br`.
    #[serde(default)]
    pub component: Option<String>,
    /// PD endpoint passed to BR, for example `127.0.0.1:2379`.
    #[serde(default)]
    pub pd_endpoint: Option<String>,
    /// Base external storage URI for snapshot backups.
    #[serde(default)]
    pub storage_uri: Option<String>,
    /// Base external storage URI for log backup / PITR.
    #[serde(default)]
    pub log_storage_uri: Option<String>,
    /// Maps to BR's `--send-credentials-to-tikv` flag.
    #[serde(default)]
    pub send_credentials_to_tikv: Option<bool>,
}

impl Default for TidbStorageConfig {
    fn default() -> Self {
        Self {
            connection_string: default_connection_string(),
            pool_size: default_pool_size(),
            catalog_pool_size: None,
            backup: TidbBackupConfig::default(),
        }
    }
}

fn default_connection_string() -> String {
    "mysql://extenddb:extenddb-local-dev@localhost:4000/extenddb_catalog".to_owned()
}

fn default_pool_size() -> u32 {
    20
}

/// Parsed components of a `TiDB` connection string.
pub struct ConnParts {
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub database: String,
}

fn decode_url_component(value: &str, label: &str) -> anyhow::Result<String> {
    percent_decode_str(value)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|e| anyhow::anyhow!("Invalid percent-encoding in {label}: {e}"))
}

fn encode_url_component(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

pub(crate) fn connection_url(
    user: &str,
    password: &str,
    host: &str,
    port: u16,
    database: &str,
) -> String {
    format!(
        "mysql://{}:{}@{}:{}/{}",
        encode_url_component(user),
        encode_url_component(password),
        host,
        port,
        encode_url_component(database),
    )
}

/// Parse host, port, user, password, and database from a `TiDB` connection string.
///
/// Handles the standard `mysql://user:pass@host:port/db` format used by TiDB's
/// MySQL-compatible wire protocol. `tidb://` is accepted for CLI parsing, but
/// generated configs use `mysql://` so `sqlx` can connect directly.
///
/// # Errors
///
/// Returns an error if the connection string doesn't match the expected format.
pub fn parse_connection_string(conn: &str) -> anyhow::Result<ConnParts> {
    let normalized = sqlx_connection_string(conn);
    let url = Url::parse(&normalized)
        .map_err(|e| anyhow::anyhow!("Invalid TiDB connection string: {e}"))?;

    if url.scheme() != "mysql" {
        return Err(anyhow::anyhow!(
            "Connection string must start with mysql:// or tidb://"
        ));
    }

    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("Connection string missing host"))?
        .to_owned();
    let port = url
        .port()
        .ok_or_else(|| anyhow::anyhow!("Connection string missing :port"))?;
    let database = url.path().strip_prefix('/').unwrap_or(url.path());
    if database.is_empty() {
        return Err(anyhow::anyhow!("Connection string missing /database"));
    }
    if database.contains('/') {
        return Err(anyhow::anyhow!("Database name must not contain '/'"));
    }

    let user = decode_url_component(url.username(), "username")?;
    let password = decode_url_component(url.password().unwrap_or_default(), "password")?;
    let database = decode_url_component(database, "database")?;

    Ok(ConnParts {
        user,
        password,
        host,
        port,
        database,
    })
}

/// Convert a TiDB-friendly URL into the `mysql://` URL scheme expected by sqlx.
pub fn sqlx_connection_string(conn: &str) -> String {
    conn.strip_prefix("tidb://")
        .map_or_else(|| conn.to_owned(), |rest| format!("mysql://{rest}"))
}

/// Redact the password in a TiDB/MySQL connection string without exposing
/// credentials that contain `@` or `:` characters.
pub fn redact_connection_string(conn: &str) -> String {
    let uses_tidb_scheme = conn.starts_with("tidb://");
    let normalized = sqlx_connection_string(conn);
    if let Ok(mut url) = Url::parse(&normalized) {
        if url.password().is_some() {
            let _ = url.set_password(Some("***"));
        }
        let redacted = url.to_string();
        return if uses_tidb_scheme {
            redacted
                .strip_prefix("mysql://")
                .map_or(redacted.clone(), |rest| format!("tidb://{rest}"))
        } else {
            redacted
        };
    }

    let Some(scheme_end) = conn.find("://").map(|i| i + 3) else {
        return conn.to_owned();
    };
    let Some(at) = conn.rfind('@') else {
        return conn.to_owned();
    };
    let Some(colon) = conn[scheme_end..at].rfind(':').map(|i| scheme_end + i) else {
        return conn.to_owned();
    };
    format!("{}:***@{}", &conn[..colon], &conn[at + 1..])
}

// ── StorageConfig trait implementation ────────────────────────────────

impl extenddb_storage::config::StorageConfig for TidbStorageConfig {
    fn connection_config(&self) -> &str {
        &self.connection_string
    }

    fn max_connections(&self) -> u32 {
        self.pool_size
    }

    fn max_catalog_connections(&self) -> u32 {
        self.catalog_pool_size.unwrap_or(self.pool_size)
    }

    fn native_backup_config(&self) -> Option<extenddb_storage::config::NativeBackupConfig> {
        Some(extenddb_storage::config::NativeBackupConfig {
            binary: self.backup.binary.clone(),
            component: self.backup.component.clone(),
            coordinator_endpoint: self.backup.pd_endpoint.clone(),
            storage_uri: self.backup.storage_uri.clone(),
            log_storage_uri: self.backup.log_storage_uri.clone(),
            send_credentials_to_storage_nodes: self.backup.send_credentials_to_tikv,
        })
    }

    fn uses_backend_native_capacity_control(&self) -> bool {
        true
    }

    fn clone_box(&self) -> Box<dyn extenddb_storage::config::StorageConfig> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        TidbStorageConfig, connection_url, parse_connection_string, redact_connection_string,
    };
    use extenddb_storage::config::StorageConfig;

    #[test]
    fn parses_percent_encoded_credentials() {
        let parts =
            parse_connection_string("mysql://extend%40db:p%40ss%2Fword@localhost:4000/db%2D1")
                .expect("connection string should parse");
        assert_eq!(parts.user, "extend@db");
        assert_eq!(parts.password, "p@ss/word");
        assert_eq!(parts.host, "localhost");
        assert_eq!(parts.port, 4000);
        assert_eq!(parts.database, "db-1");
    }

    #[test]
    fn generated_urls_round_trip_special_credentials() {
        let url = connection_url(
            "extend@db",
            "p@ss/word:#",
            "127.0.0.1",
            4000,
            "extenddb_data",
        );
        let parts = parse_connection_string(&url).expect("generated URL should parse");
        assert_eq!(parts.user, "extend@db");
        assert_eq!(parts.password, "p@ss/word:#");
        assert_eq!(parts.database, "extenddb_data");
    }

    #[test]
    fn redaction_uses_the_last_userinfo_separator() {
        let redacted = redact_connection_string("mysql://extenddb:p@ss@localhost:4000/db");
        assert_eq!(redacted, "mysql://extenddb:***@localhost:4000/db");
    }

    #[test]
    fn tidb_uses_backend_native_capacity_control() {
        assert!(TidbStorageConfig::default().uses_backend_native_capacity_control());
    }
}
