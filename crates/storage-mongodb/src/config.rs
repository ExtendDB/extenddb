// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for `MongoDB` storage backend.

use serde::{Deserialize, Serialize};

/// `MongoDB` storage backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MongoStorageConfig {
    /// `MongoDB` connection string (mongodb://...)
    pub connection_string: String,
    /// Maximum concurrent connections for data operations
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Maximum concurrent connections for catalog/management operations
    #[serde(default = "default_max_catalog_connections")]
    pub max_catalog_connections: u32,
}

fn default_max_connections() -> u32 {
    50
}

fn default_max_catalog_connections() -> u32 {
    20
}

impl extenddb_storage::config::StorageConfig for MongoStorageConfig {
    fn connection_config(&self) -> &str {
        &self.connection_string
    }

    fn max_connections(&self) -> u32 {
        self.max_connections
    }

    fn max_catalog_connections(&self) -> u32 {
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
