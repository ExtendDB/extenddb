// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for `MongoDB` storage backend.

use serde::Deserialize;

/// `MongoDB` storage backend configuration.
///
/// Deliberately does **not** derive `Serialize`: `connection_string` may carry
/// `user:pass@` credentials, and a `Serialize` impl would let them leave the
/// process on any serialize path. Matches the postgres backend, which derives
/// only `Debug, Clone, Deserialize`.
#[derive(Debug, Clone, Deserialize)]
pub struct MongoStorageConfig {
    /// `MongoDB` connection string (mongodb://...)
    pub connection_string: String,
    /// Maximum concurrent connections for data operations
    #[serde(
        default,
        deserialize_with = "extenddb_storage::config::string_coerce::positive_opt_u32"
    )]
    pub max_connections: Option<u32>,
    /// Maximum concurrent connections for catalog/management operations
    #[serde(
        default,
        deserialize_with = "extenddb_storage::config::string_coerce::positive_opt_u32"
    )]
    pub max_catalog_connections: Option<u32>,
}

impl extenddb_storage::config::StorageConfig for MongoStorageConfig {
    fn connection_config(&self) -> &str {
        &self.connection_string
    }

    fn max_connections(&self) -> u32 {
        self.max_connections.unwrap_or(50)
    }

    fn max_catalog_connections(&self) -> u32 {
        self.max_catalog_connections.unwrap_or(20)
    }

    fn max_connections_override(&self) -> Option<u32> {
        self.max_connections
    }

    fn max_catalog_connections_override(&self) -> Option<u32> {
        self.max_catalog_connections
    }

    fn clone_box(&self) -> Box<dyn extenddb_storage::config::StorageConfig> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl TryFrom<toml::Table> for MongoStorageConfig {
    type Error = toml::de::Error;

    fn try_from(table: toml::Table) -> Result<Self, Self::Error> {
        let value = toml::Value::Table(table);
        value.try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::MongoStorageConfig;
    use extenddb_storage::config::StorageConfig;

    #[test]
    fn numeric_connection_limits_accept_string_values() {
        let config: MongoStorageConfig = toml::from_str(
            r#"connection_string = "mongodb://localhost:27017"
max_connections = "31"
max_catalog_connections = "7""#,
        )
        .expect("string-valued connection limits must deserialize");

        assert_eq!(config.max_connections, Some(31));
        assert_eq!(config.max_catalog_connections, Some(7));
    }

    #[test]
    fn numeric_connection_limits_accept_native_values() {
        let config: MongoStorageConfig = toml::from_str(
            r#"connection_string = "mongodb://localhost:27017"
max_connections = 31
max_catalog_connections = 7"#,
        )
        .expect("native connection limits must deserialize");

        assert_eq!(config.max_connections, Some(31));
        assert_eq!(config.max_catalog_connections, Some(7));
    }

    #[test]
    fn connection_limits_have_expected_defaults() {
        let config: MongoStorageConfig =
            toml::from_str(r#"connection_string = "mongodb://localhost:27017""#)
                .expect("default connection limits must deserialize");

        assert_eq!(config.max_connections, None);
        assert_eq!(config.max_catalog_connections, None);
        assert_eq!(config.max_connections_override(), None);
        assert_eq!(config.max_catalog_connections_override(), None);
        assert_eq!(config.max_connections(), 50);
        assert_eq!(config.max_catalog_connections(), 20);
    }

    #[test]
    fn zero_data_connection_limit_is_rejected_at_deserialization() {
        let error = toml::from_str::<MongoStorageConfig>(
            r#"connection_string = "mongodb://localhost:27017"
max_connections = 0"#,
        )
        .expect_err("zero data connection limit must be rejected");

        assert!(error.to_string().contains("at least 1"));
    }

    #[test]
    fn zero_catalog_connection_limit_is_rejected_at_deserialization() {
        let error = toml::from_str::<MongoStorageConfig>(
            r#"connection_string = "mongodb://localhost:27017"
max_catalog_connections = "0""#,
        )
        .expect_err("zero catalog connection limit must be rejected");

        assert!(error.to_string().contains("at least 1"));
    }
}
