// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra storage backend configuration.

use serde::Deserialize;

/// Cassandra storage backend configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct CassandraStorageConfig {
    /// Cassandra contact points (host:port)
    pub contact_points: Vec<String>,

    /// Username for authentication
    pub username: Option<String>,

    /// Password for authentication
    pub password: Option<String>,

    /// Keyspace prefix (default: "extenddb")
    #[serde(default = "default_keyspace_prefix")]
    pub keyspace_prefix: String,

    /// Replication factor for keyspaces (default: 3)
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,

    /// Datacenter name for `NetworkTopologyStrategy` (default: "datacenter1")
    #[serde(default = "default_datacenter")]
    pub datacenter: String,

    /// Maximum connections per host (default: 10)
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,

    /// Cached connection string (JDBC-style: `host1,host2/keyspace_prefix`)
    /// This is computed after deserialization and cached for `connection_config()`
    #[serde(skip)]
    pub cached_connection_string: Option<String>,

    /// Stable identifier for this server instance, set by the server before
    /// constructing the engine. Used to derive the HLC node ID.
    #[serde(skip)]
    pub instance_id: Option<String>,
}

fn default_keyspace_prefix() -> String {
    "extenddb".to_string()
}

fn default_replication_factor() -> u32 {
    3
}

fn default_datacenter() -> String {
    "datacenter1".to_string()
}

fn default_max_connections() -> u32 {
    10
}

/// The parts of a Cassandra connection string.
#[derive(Clone, PartialEq, Eq)]
pub struct ConnectionParts {
    pub contact_points: Vec<String>,
    pub keyspace_prefix: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl std::fmt::Debug for ConnectionParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionParts")
            .field("contact_points", &self.contact_points)
            .field("keyspace_prefix", &self.keyspace_prefix)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Every byte outside ASCII letters, digits and `-._~` as `%XX`, so a role or
/// password holding `@`, `:`, `/` or `,` cannot be read as another part.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// The inverse of [`percent_encode`]; a `%` not followed by two hex digits
/// stays as it is.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = bytes
            .get(i + 1..i + 3)
            .and_then(|h| std::str::from_utf8(h).ok())
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match (bytes[i], hex) {
            (b'%', Some(byte)) => {
                out.push(byte);
                i += 3;
            }
            (byte, _) => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

impl CassandraStorageConfig {
    /// Build the connection string: `[user[:password]@]host1,host2/keyspace_prefix`.
    ///
    /// The role and password are the config's, percent-encoded, so a command
    /// that receives only the connection string (`verify`, `settings`,
    /// `migrate`) connects with the role the server uses.
    /// [`Self::redact_connection_string`] hides the password wherever the
    /// string is shown.
    fn build_connection_string(&self) -> String {
        let hosts = self.contact_points.join(",");
        let userinfo = match (&self.username, &self.password) {
            (Some(user), Some(password)) => {
                format!("{}:{}@", percent_encode(user), percent_encode(password))
            }
            (Some(user), None) => format!("{}@", percent_encode(user)),
            (None, _) => String::new(),
        };
        format!("{userinfo}{hosts}/{}", self.keyspace_prefix)
    }

    /// Ensure the cached connection string is populated.
    /// Call this after deserialization or construction.
    pub fn ensure_cached_connection_string(&mut self) {
        if self.cached_connection_string.is_none() {
            self.cached_connection_string = Some(self.build_connection_string());
        }
    }

    /// Rebuild the cached connection string.
    /// Use this after modifying `contact_points` or `keyspace_prefix`.
    pub fn rebuild_cached_connection_string(&mut self) {
        self.cached_connection_string = Some(self.build_connection_string());
    }

    /// Parse a JDBC-style connection string into contact points and keyspace prefix.
    /// Format: "`[user[:password]@]host1:port1,host2:port2/keyspace_prefix`"
    /// Returns (`contact_points`, `keyspace_prefix`)
    #[must_use]
    pub fn parse_connection_string(conn_str: &str) -> (Vec<String>, String) {
        let parts = Self::connection_parts(conn_str);
        (parts.contact_points, parts.keyspace_prefix)
    }

    /// Every part of a connection string [`Self::build_connection_string`]
    /// wrote, the role and password decoded.
    #[must_use]
    pub fn connection_parts(conn_str: &str) -> ConnectionParts {
        let (userinfo, rest) = match conn_str.rsplit_once('@') {
            Some((userinfo, rest)) => (Some(userinfo), rest),
            None => (None, conn_str),
        };
        let (username, password) = match userinfo {
            Some(info) => match info.split_once(':') {
                Some((user, password)) => {
                    (Some(percent_decode(user)), Some(percent_decode(password)))
                }
                None => (Some(percent_decode(info)), None),
            },
            None => (None, None),
        };
        let (hosts, keyspace_prefix) = match rest.split_once('/') {
            Some((hosts, keyspace)) => (hosts, keyspace.trim().to_string()),
            None => (rest, default_keyspace_prefix()),
        };
        ConnectionParts {
            contact_points: hosts.split(',').map(|s| s.trim().to_string()).collect(),
            keyspace_prefix,
            username,
            password,
        }
    }

    /// The connection string with its password replaced by `***`.
    #[must_use]
    pub fn redact_connection_string(conn_str: &str) -> String {
        match conn_str.rsplit_once('@') {
            Some((userinfo, rest)) => match userinfo.split_once(':') {
                Some((user, _)) => format!("{user}:***@{rest}"),
                None => conn_str.to_owned(),
            },
            None => conn_str.to_owned(),
        }
    }

    /// Create default config programmatically
    #[must_use]
    pub fn new(contact_points: Vec<String>) -> Self {
        let mut config = Self {
            contact_points,
            username: None,
            password: None,
            keyspace_prefix: default_keyspace_prefix(),
            replication_factor: default_replication_factor(),
            datacenter: default_datacenter(),
            max_connections: default_max_connections(),
            cached_connection_string: None,
            instance_id: None,
        };
        config.ensure_cached_connection_string();
        config
    }
}

impl extenddb_storage::config::StorageConfig for CassandraStorageConfig {
    fn connection_config(&self) -> &str {
        // Return JDBC-style connection string: host1,host2/keyspace_prefix
        // This allows factories to parse both contact points and keyspace
        self.cached_connection_string
            .as_deref()
            .unwrap_or("(no connection string)")
    }

    fn max_connections(&self) -> u32 {
        self.max_connections
    }

    fn max_catalog_connections(&self) -> u32 {
        // Cassandra uses same connection pool for catalog and data
        self.max_connections
    }

    fn clone_box(&self) -> Box<dyn extenddb_storage::config::StorageConfig> {
        Box::new(self.clone())
    }

    fn set_instance_id(&mut self, instance_id: &str) {
        self.instance_id = Some(instance_id.to_string());
    }

    fn instance_id(&self) -> Option<&str> {
        self.instance_id.as_deref()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_storage::config::StorageConfig;

    #[test]
    fn test_config_defaults() {
        let config = CassandraStorageConfig::new(vec!["localhost:9042".to_string()]);

        assert_eq!(config.keyspace_prefix, "extenddb");
        assert_eq!(config.replication_factor, 3);
        assert_eq!(config.datacenter, "datacenter1");
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.username, None);
        assert_eq!(config.password, None);
    }

    #[test]
    fn test_connection_config() {
        let config =
            CassandraStorageConfig::new(vec!["host1:9042".to_string(), "host2:9042".to_string()]);

        // Should return JDBC-style connection string with keyspace
        assert_eq!(config.connection_config(), "host1:9042,host2:9042/extenddb");
    }

    #[test]
    fn test_connection_config_empty() {
        let config = CassandraStorageConfig::new(vec![]);
        // Empty contact points should still include keyspace
        assert_eq!(config.connection_config(), "/extenddb");
    }

    #[test]
    fn the_connection_string_carries_the_role_and_hides_the_password_when_shown() {
        let mut config = CassandraStorageConfig::new(vec!["h1:9042".to_string()]);
        config.username = Some("app:role".to_string());
        config.password = Some("p@ss/w,rd%".to_string());
        config.rebuild_cached_connection_string();
        let conn = config.connection_config();
        assert_eq!(conn, "app%3Arole:p%40ss%2Fw%2Crd%25@h1:9042/extenddb");
        let parts = CassandraStorageConfig::connection_parts(conn);
        assert_eq!(parts.username.as_deref(), Some("app:role"));
        assert_eq!(parts.password.as_deref(), Some("p@ss/w,rd%"));
        assert_eq!(parts.contact_points, ["h1:9042"]);
        assert_eq!(parts.keyspace_prefix, "extenddb");
        assert_eq!(
            CassandraStorageConfig::parse_connection_string(conn),
            (vec!["h1:9042".to_string()], "extenddb".to_string())
        );
        let shown = CassandraStorageConfig::redact_connection_string(conn);
        assert_eq!(shown, "app%3Arole:***@h1:9042/extenddb");
        assert!(!format!("{parts:?}").contains("p@ss"));
        // Without a password nothing is hidden, and without a role nothing
        // is added.
        config.password = None;
        config.rebuild_cached_connection_string();
        assert_eq!(config.connection_config(), "app%3Arole@h1:9042/extenddb");
        config.username = None;
        config.rebuild_cached_connection_string();
        assert_eq!(config.connection_config(), "h1:9042/extenddb");
        assert_eq!(
            CassandraStorageConfig::redact_connection_string("h1:9042/extenddb"),
            "h1:9042/extenddb"
        );
    }

    #[test]
    fn test_toml_deserialization() {
        let toml_str = r#"
            contact_points = ["host1:9042", "host2:9042"]
            username = "cassandra"
            password = "secret"
            keyspace_prefix = "myapp"
            replication_factor = 5
            datacenter = "dc1"
            max_connections = 20
        "#;

        let config: CassandraStorageConfig = toml::from_str(toml_str).unwrap();

        assert_eq!(config.contact_points, vec!["host1:9042", "host2:9042"]);
        assert_eq!(config.username, Some("cassandra".to_string()));
        assert_eq!(config.password, Some("secret".to_string()));
        assert_eq!(config.keyspace_prefix, "myapp");
        assert_eq!(config.replication_factor, 5);
        assert_eq!(config.datacenter, "dc1");
        assert_eq!(config.max_connections, 20);
    }

    #[test]
    fn test_toml_deserialization_minimal() {
        let toml_str = r#"
            contact_points = ["localhost:9042"]
        "#;

        let config: CassandraStorageConfig = toml::from_str(toml_str).unwrap();

        // Verify defaults are applied
        assert_eq!(config.keyspace_prefix, "extenddb");
        assert_eq!(config.replication_factor, 3);
        assert_eq!(config.datacenter, "datacenter1");
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.username, None);
        assert_eq!(config.password, None);
    }

    #[test]
    fn test_build_connection_string() {
        let mut config = CassandraStorageConfig::new(vec![
            "127.0.0.1:9042".to_string(),
            "127.0.0.2:9042".to_string(),
        ]);
        config.keyspace_prefix = "my_keyspace".to_string();
        config.rebuild_cached_connection_string();

        assert_eq!(
            config.cached_connection_string.as_ref().unwrap(),
            "127.0.0.1:9042,127.0.0.2:9042/my_keyspace"
        );
    }

    #[test]
    fn test_build_connection_string_default_keyspace() {
        let config = CassandraStorageConfig::new(vec!["localhost:9042".to_string()]);

        assert_eq!(
            config.cached_connection_string.as_ref().unwrap(),
            "localhost:9042/extenddb"
        );
    }

    #[test]
    fn test_parse_connection_string_with_keyspace() {
        let (contact_points, keyspace) = CassandraStorageConfig::parse_connection_string(
            "127.0.0.1:9042,127.0.0.2:9042/my_keyspace",
        );

        assert_eq!(contact_points.len(), 2);
        assert_eq!(contact_points[0], "127.0.0.1:9042");
        assert_eq!(contact_points[1], "127.0.0.2:9042");
        assert_eq!(keyspace, "my_keyspace");
    }

    #[test]
    fn test_parse_connection_string_without_keyspace() {
        let (contact_points, keyspace) =
            CassandraStorageConfig::parse_connection_string("127.0.0.1:9042,127.0.0.2:9042");

        assert_eq!(contact_points.len(), 2);
        assert_eq!(contact_points[0], "127.0.0.1:9042");
        assert_eq!(contact_points[1], "127.0.0.2:9042");
        assert_eq!(keyspace, "extenddb"); // Default
    }

    #[test]
    fn test_parse_connection_string_single_host() {
        let (contact_points, keyspace) =
            CassandraStorageConfig::parse_connection_string("localhost:9042/test_keyspace");

        assert_eq!(contact_points.len(), 1);
        assert_eq!(contact_points[0], "localhost:9042");
        assert_eq!(keyspace, "test_keyspace");
    }

    #[test]
    fn test_parse_connection_string_with_whitespace() {
        let (contact_points, keyspace) = CassandraStorageConfig::parse_connection_string(
            "  127.0.0.1:9042  ,  127.0.0.2:9042  /  my_keyspace  ",
        );

        assert_eq!(contact_points.len(), 2);
        assert_eq!(contact_points[0], "127.0.0.1:9042");
        assert_eq!(contact_points[1], "127.0.0.2:9042");
        assert_eq!(keyspace, "my_keyspace");
    }

    #[test]
    fn test_roundtrip_connection_string() {
        // Build a connection string
        let mut config =
            CassandraStorageConfig::new(vec!["host1:9042".to_string(), "host2:9042".to_string()]);
        config.keyspace_prefix = "test_ks".to_string();
        config.rebuild_cached_connection_string();

        let conn_str = config.cached_connection_string.unwrap();

        // Parse it back
        let (contact_points, keyspace) = CassandraStorageConfig::parse_connection_string(&conn_str);

        assert_eq!(contact_points.len(), 2);
        assert_eq!(contact_points[0], "host1:9042");
        assert_eq!(contact_points[1], "host2:9042");
        assert_eq!(keyspace, "test_ks");
    }
}
