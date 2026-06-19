// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DynamoDB backend configuration.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DynamoStorageConfig {
    pub region: String,
    #[serde(default)]
    pub endpoint_url: Option<String>,
    #[serde(default = "default_table_prefix")]
    pub table_prefix: String,
    pub catalog_connection_string: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
    #[serde(default)]
    pub catalog_pool_size: Option<u32>,
}

fn default_table_prefix() -> String {
    "athome_".to_owned()
}

fn default_pool_size() -> u32 {
    20
}

impl DynamoStorageConfig {
    /// Deserialize a `DynamoStorageConfig` from a TOML table.
    ///
    /// # Errors
    ///
    /// Returns an error string if the table cannot be deserialized into
    /// `DynamoStorageConfig` (e.g. missing required fields, unknown fields).
    pub fn from_table(t: &toml::Table) -> Result<Self, String> {
        t.clone()
            .try_into()
            .map_err(|e: toml::de::Error| e.to_string())
    }
}

// ── StorageConfig trait implementation ────────────────────────────────

impl extenddb_storage::config::StorageConfig for DynamoStorageConfig {
    fn connection_config(&self) -> &str {
        &self.catalog_connection_string
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_section() {
        let toml_str = r#"
region = "us-east-1"
endpoint_url = "http://localhost:8000"
table_prefix = "athome_"
catalog_connection_string = "postgresql://u:p@localhost/cat"
"#;
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let c = DynamoStorageConfig::from_table(&t).unwrap();
        assert_eq!(c.region, "us-east-1");
        assert_eq!(c.table_prefix, "athome_");
        assert_eq!(c.endpoint_url.as_deref(), Some("http://localhost:8000"));
    }

    #[test]
    fn table_prefix_defaults_to_athome() {
        let toml_str = r#"
region = "us-east-1"
catalog_connection_string = "postgresql://x"
"#;
        let t: toml::Table = toml::from_str(toml_str).unwrap();
        let c = DynamoStorageConfig::from_table(&t).unwrap();
        assert_eq!(c.table_prefix, "athome_");
    }
}
