// Copyright 2026 DynamoDB Open contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage configuration trait and registry for storage backends.

/// Configuration interface for storage backends.
///
/// Each backend implements this trait to expose connection parameters
/// in a backend-agnostic way. The bin crate uses these methods without
/// knowing the concrete backend type.
pub trait StorageConfig: Send + Sync + std::fmt::Debug {
    /// Backend-specific connection configuration as a string.
    ///
    /// For PostgreSQL: connection string (postgresql://...)
    fn connection_config(&self) -> &str;

    /// Maximum concurrent connections for data operations.
    fn max_connections(&self) -> u32;

    /// Maximum concurrent connections for catalog/management operations.
    fn max_catalog_connections(&self) -> u32;

    /// Clone this config into a boxed trait object.
    fn clone_box(&self) -> Box<dyn StorageConfig>;

    /// Enable downcasting to specific storage engine config types to allow
    /// access to engine-specific configuration (e.g. `keyspace_prefix` for
    /// the Cassandra backend).
    fn as_any(&self) -> &dyn std::any::Any
    where
        Self: 'static;
}

impl Clone for Box<dyn StorageConfig> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Deserializer function type for storage configurations.
///
/// Takes a TOML table and returns a boxed `StorageConfig` trait object.
pub type StorageConfigDeserializer = fn(&toml::Table) -> Result<Box<dyn StorageConfig>, String>;

/// Registration entry for a storage config deserializer.
pub struct StorageConfigRegistration {
    pub backend: &'static str,
    pub deserializer: StorageConfigDeserializer,
}

inventory::collect!(StorageConfigRegistration);

/// Deserialize a storage configuration from a TOML table.
///
/// Looks up the registered deserializer for the given backend name
/// and invokes it with the provided TOML table.
pub fn deserialize_storage_config(
    backend: &str,
    table: &toml::Table,
) -> Result<Box<dyn StorageConfig>, String> {
    for reg in inventory::iter::<StorageConfigRegistration> {
        if reg.backend == backend {
            return (reg.deserializer)(table);
        }
    }
    Err(format!("Unknown backend: {}", backend))
}
