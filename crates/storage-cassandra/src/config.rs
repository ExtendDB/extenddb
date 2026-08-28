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

    /// Datacenter name for NetworkTopologyStrategy (default: "datacenter1")
    #[serde(default = "default_datacenter")]
    pub datacenter: String,

    /// Maximum connections per host (default: 10)
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,

    /// Cached connection string (JDBC-style: host1,host2/keyspace_prefix)
    /// This is computed after deserialization and cached for connection_config()
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

impl CassandraStorageConfig {
    /// Build the connection string in JDBC-style format: host1,host2/keyspace_prefix
    fn build_connection_string(&self) -> String {
        let hosts = self.contact_points.join(",");
        format!("{}/{}", hosts, self.keyspace_prefix)
    }

    /// Ensure the cached connection string is populated.
    /// Call this after deserialization or construction.
    pub fn ensure_cached_connection_string(&mut self) {
        if self.cached_connection_string.is_none() {
            self.cached_connection_string = Some(self.build_connection_string());
        }
    }

    /// Rebuild the cached connection string.
    /// Use this after modifying contact_points or keyspace_prefix.
    pub fn rebuild_cached_connection_string(&mut self) {
        self.cached_connection_string = Some(self.build_connection_string());
    }

    /// Parse a JDBC-style connection string into contact points and keyspace prefix.
    /// Format: "host1:port1,host2:port2/keyspace_prefix"
    /// Returns (contact_points, keyspace_prefix)
    pub fn parse_connection_string(conn_str: &str) -> (Vec<String>, String) {
        if let Some((hosts, keyspace)) = conn_str.split_once('/') {
            let contact_points = hosts.split(',').map(|s| s.trim().to_string()).collect();
            (contact_points, keyspace.trim().to_string())
        } else {
            // No keyspace specified, use default
            let contact_points = conn_str.split(',').map(|s| s.trim().to_string()).collect();
            (contact_points, default_keyspace_prefix())
        }
    }

    /// Create default config programmatically
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
            .as_ref()
            .map(|s| s.as_str())
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
