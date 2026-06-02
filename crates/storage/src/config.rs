// Copyright 2026 DynamoDB Open contributors
// SPDX-License-Identifier: Apache-2.0

//! Storage configuration trait and registry for storage backends.

use extenddb_core::limits::LimitsConfig;

/// Backend-native backup tool configuration.
///
/// This is intentionally storage-agnostic enough for the server factory layer:
/// concrete backends decide how to interpret the coordinator endpoint and
/// command prefix. Backends that do not expose native physical backups return
/// `None` from [`StorageConfig::native_backup_config`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NativeBackupConfig {
    /// Backup executable.
    pub binary: Option<String>,
    /// Optional subcommand/component inserted after `binary`; set it to an
    /// empty string when `binary` is already the native backup executable.
    pub component: Option<String>,
    /// Cluster coordinator endpoint.
    pub coordinator_endpoint: Option<String>,
    /// Base URI for snapshot backups.
    pub storage_uri: Option<String>,
    /// Base URI for log backups / PITR.
    pub log_storage_uri: Option<String>,
    /// Whether to send object-store credentials to storage nodes.
    pub send_credentials_to_storage_nodes: Option<bool>,
}

/// Configuration interface for storage backends.
///
/// Each backend implements this trait to expose connection parameters
/// in a backend-agnostic way. The bin crate uses these methods without
/// knowing the concrete backend type.
pub trait StorageConfig: Send + Sync + std::fmt::Debug {
    /// Backend-specific connection configuration as a string.
    fn connection_config(&self) -> &str;

    /// Maximum concurrent connections for data operations.
    fn max_connections(&self) -> u32;

    /// Maximum concurrent connections for catalog/management operations.
    fn max_catalog_connections(&self) -> u32;

    /// Runtime request limits visible to storage backends that must enforce
    /// post-mutation invariants.
    fn runtime_limits(&self) -> Option<&LimitsConfig> {
        None
    }

    /// Optional backend-native physical backup configuration.
    fn native_backup_config(&self) -> Option<NativeBackupConfig> {
        None
    }

    /// Whether this backend owns table and index lifecycle through native
    /// distributed online DDL instead of frontend-simulated control-plane delay.
    fn uses_backend_native_control_plane(&self) -> bool {
        false
    }

    /// Whether this backend maintains secondary indexes natively from base-row
    /// writes instead of frontend-managed companion index propagation.
    fn uses_backend_native_secondary_indexes(&self) -> bool {
        false
    }

    /// Whether this backend should use its own cluster-native capacity control
    /// instead of the frontend process-local token bucket.
    ///
    /// A backend returns `true` when capacity should be enforced by the storage
    /// cluster so multiple ExtendDB frontends observe one shared quota and
    /// scheduler. The server still records consumed-capacity metrics, but it
    /// does not reject requests from its in-memory token buckets.
    fn uses_backend_native_capacity_control(&self) -> bool {
        false
    }

    /// Optional storage-native resource group used for capacity governance.
    ///
    /// Backends that support session-level cluster scheduling can expose an
    /// operator-selected resource group here. The server passes it back to the
    /// backend factory; backends without such a concept keep the default `None`.
    fn native_capacity_resource_group(&self) -> Option<&str> {
        None
    }

    /// Clone this config into a boxed trait object.
    fn clone_box(&self) -> Box<dyn StorageConfig>;
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

/// Default config factory for a registered storage backend.
pub type StorageConfigDefaultFactory = fn() -> Box<dyn StorageConfig>;

/// Registration entry for a storage config deserializer.
pub struct StorageConfigRegistration {
    pub backend: &'static str,
    pub deserializer: StorageConfigDeserializer,
    pub default_config: StorageConfigDefaultFactory,
    /// Default-backend priority. Higher wins; `None` means this backend is
    /// never selected as the implicit default.
    pub default_priority: Option<u16>,
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

/// Create the default configuration for a registered storage backend.
pub fn default_storage_config(backend: &str) -> Result<Box<dyn StorageConfig>, String> {
    for reg in inventory::iter::<StorageConfigRegistration> {
        if reg.backend == backend {
            return Ok((reg.default_config)());
        }
    }
    Err(format!("Unknown backend: {}", backend))
}

/// Return the registered backend selected as the implicit default.
pub fn default_backend_name() -> Result<&'static str, String> {
    let mut defaults: Vec<(u16, &'static str)> = inventory::iter::<StorageConfigRegistration>
        .into_iter()
        .filter_map(|reg| reg.default_priority.map(|priority| (priority, reg.backend)))
        .collect();
    defaults.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(right.1)));
    defaults
        .first()
        .map(|(_, backend)| *backend)
        .ok_or_else(|| "No default storage backend registered".to_owned())
}
