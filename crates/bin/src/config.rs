// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Shared configuration types for the extenddb binary.

use extenddb_core::limits::LimitsConfig;
use serde::Deserialize;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    /// Auth provider configuration. `provider = "builtin"` for `SigV4` with
    /// local credential store. The server refuses to start with `provider = "none"`.
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
    /// Import configuration. Lists allowed source directories for import
    /// operations. If empty or absent, imports are denied (secure default).
    #[serde(default, rename = "import")]
    pub import_config: ImportExportPathConfig,
    /// Export configuration. Lists allowed destination directories for export
    /// operations. If empty or absent, exports are denied (secure default).
    #[serde(default, rename = "export")]
    pub export_config: ImportExportPathConfig,
    /// Maximum import file size in bytes. Defaults to 10 GB.
    pub max_import_bytes: Option<u64>,
    /// Path to the rendered documentation directory (`/console/docs`).
    pub docs_dir: Option<String>,
    /// Deprecated: single root for both import and export. Superseded by
    /// `[import]` and `[export]` sections. If set and the new sections are
    /// empty, this value is used for both import and export paths.
    pub import_export_root: Option<String>,
}

/// Configuration for import or export allowed paths.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportExportPathConfig {
    /// Allowed directories. All file paths are canonicalized and must resolve
    /// under one of these roots. Symlinks escaping a root are rejected.
    #[serde(default)]
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_bind_addr")]
    pub bind_addr: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_region")]
    pub region: String,
    /// Directory for runtime files (PID file). Defaults to `~/.extenddb/run`.
    #[serde(default = "default_run_dir")]
    pub run_dir: String,
    /// TLS configuration. When enabled, the server serves HTTPS.
    #[serde(default)]
    pub tls: TlsConfig,
    /// Enable frontend provisioned-throughput throttling via token buckets.
    /// Backends with native distributed capacity control, such as TiDB, ignore
    /// this process-local limiter.
    pub throttling_enabled: Option<bool>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_bind_addr(),
            port: default_port(),
            region: default_region(),
            run_dir: default_run_dir(),
            tls: TlsConfig::default(),
            throttling_enabled: None,
        }
    }
}

/// TLS configuration for HTTPS.
///
/// TLS is always enabled. The `enabled` field is accepted for backward
/// compatibility but the server refuses to start if set to `false`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// TLS is mandatory. Accepted for backward compatibility; the server
    /// refuses to start if explicitly set to `false`.
    #[serde(default = "default_tls_enabled")]
    pub enabled: bool,
    /// Path to the TLS certificate file (PEM). Defaults to `~/.extenddb/tls/cert.pem`.
    #[serde(default = "default_tls_cert_path")]
    pub cert_path: String,
    /// Path to the TLS private key file (PEM). Defaults to `~/.extenddb/tls/key.pem`.
    #[serde(default = "default_tls_key_path")]
    pub key_path: String,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cert_path: default_tls_cert_path(),
            key_path: default_tls_key_path(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Storage backend selector.
    pub _backend: String,
    /// Backend-specific configuration (trait object).
    config: Box<dyn extenddb_storage::config::StorageConfig>,
}

impl StorageConfig {
    /// Get the connection configuration string for this backend.
    ///
    /// Delegates to the backend-specific config trait object. This wrapper
    /// provides a clean API without exposing the trait object to callers.
    pub fn connection_config(&self) -> &str {
        self.config.connection_config()
    }

    /// Get the maximum connections for data operations.
    pub fn max_connections(&self) -> u32 {
        self.config.max_connections()
    }

    /// Get the maximum connections for catalog operations.
    pub fn max_catalog_connections(&self) -> u32 {
        self.config.max_catalog_connections()
    }

    /// Get a reference to the underlying trait object for factory calls.
    pub fn as_trait(&self) -> &dyn extenddb_storage::config::StorageConfig {
        &*self.config
    }

    /// Optional backend-native resource group used for capacity governance.
    pub fn native_capacity_resource_group(&self) -> Option<&str> {
        self.config.native_capacity_resource_group()
    }
}

/// Storage config view enriched with runtime limits from the top-level config.
#[derive(Debug)]
pub struct RuntimeStorageConfig<'a> {
    base: &'a dyn extenddb_storage::config::StorageConfig,
    limits: LimitsConfig,
}

impl<'a> RuntimeStorageConfig<'a> {
    pub fn new(
        base: &'a dyn extenddb_storage::config::StorageConfig,
        limits: &LimitsConfig,
    ) -> Self {
        Self {
            base,
            limits: limits.clone(),
        }
    }
}

#[derive(Debug)]
struct OwnedRuntimeStorageConfig {
    base: Box<dyn extenddb_storage::config::StorageConfig>,
    limits: LimitsConfig,
}

impl extenddb_storage::config::StorageConfig for RuntimeStorageConfig<'_> {
    fn connection_config(&self) -> &str {
        self.base.connection_config()
    }

    fn max_connections(&self) -> u32 {
        self.base.max_connections()
    }

    fn max_catalog_connections(&self) -> u32 {
        self.base.max_catalog_connections()
    }

    fn runtime_limits(&self) -> Option<&LimitsConfig> {
        Some(&self.limits)
    }

    fn native_backup_config(&self) -> Option<extenddb_storage::config::NativeBackupConfig> {
        self.base.native_backup_config()
    }

    fn uses_backend_native_capacity_control(&self) -> bool {
        self.base.uses_backend_native_capacity_control()
    }

    fn native_capacity_resource_group(&self) -> Option<&str> {
        self.base.native_capacity_resource_group()
    }

    fn clone_box(&self) -> Box<dyn extenddb_storage::config::StorageConfig> {
        Box::new(OwnedRuntimeStorageConfig {
            base: self.base.clone_box(),
            limits: self.limits.clone(),
        })
    }
}

impl extenddb_storage::config::StorageConfig for OwnedRuntimeStorageConfig {
    fn connection_config(&self) -> &str {
        self.base.connection_config()
    }

    fn max_connections(&self) -> u32 {
        self.base.max_connections()
    }

    fn max_catalog_connections(&self) -> u32 {
        self.base.max_catalog_connections()
    }

    fn runtime_limits(&self) -> Option<&LimitsConfig> {
        Some(&self.limits)
    }

    fn native_backup_config(&self) -> Option<extenddb_storage::config::NativeBackupConfig> {
        self.base.native_backup_config()
    }

    fn uses_backend_native_capacity_control(&self) -> bool {
        self.base.uses_backend_native_capacity_control()
    }

    fn native_capacity_resource_group(&self) -> Option<&str> {
        self.base.native_capacity_resource_group()
    }

    fn clone_box(&self) -> Box<dyn extenddb_storage::config::StorageConfig> {
        Box::new(Self {
            base: self.base.clone_box(),
            limits: self.limits.clone(),
        })
    }
}

impl<'de> serde::Deserialize<'de> for StorageConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        // Deserialize into a raw TOML value first
        let value: toml::Value = toml::Value::deserialize(deserializer)?;

        // Extract the backend field
        let backend = value
            .get("backend")
            .and_then(|v| v.as_str())
            .map_or_else(default_backend, str::to_owned);

        // Get the backend-specific table matching `backend`.
        let backend_table: &toml::Table = value
            .get(&backend)
            .and_then(|v| v.as_table())
            .ok_or_else(|| {
                D::Error::custom(format!("Missing [storage.{backend}] section in config"))
            })?;

        // Use the registry to deserialize the backend config
        let config = extenddb_storage::config::deserialize_storage_config(&backend, backend_table)
            .map_err(D::Error::custom)?;

        Ok(StorageConfig {
            _backend: backend,
            config,
        })
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        let backend = default_backend();
        let config =
            extenddb_storage::config::default_storage_config(&backend).unwrap_or_else(|e| {
                panic!("Default storage backend '{backend}' is not registered: {e}")
            });
        Self {
            _backend: backend,
            config,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    /// Auth provider: `"builtin"` (`SigV4` + IAM policies). The `"none"` value
    /// is no longer accepted at startup — the server refuses to start without
    /// authentication enabled.
    #[serde(default = "default_provider")]
    pub provider: String,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            provider: default_provider(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_format")]
    pub format: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            format: default_log_format(),
        }
    }
}

fn default_bind_addr() -> String {
    "127.0.0.1".to_owned()
}
fn default_port() -> u16 {
    8000
}
fn default_region() -> String {
    "us-east-1".to_owned()
}
fn default_run_dir() -> String {
    std::env::var("HOME").map_or_else(
        |_| "/tmp".to_owned(),
        |home| format!("{home}/.extenddb/run"),
    )
}

/// Expand a leading `~` in a path to `$HOME`. Returns the input unchanged
/// if `$HOME` is unset or the path does not start with `~`.
pub fn expand_tilde(path: &str) -> String {
    if let Some(rest) = path.strip_prefix('~') {
        if rest.is_empty() || rest.starts_with('/') {
            if let Ok(home) = std::env::var("HOME") {
                return format!("{home}{rest}");
            }
        }
    }
    path.to_owned()
}
pub(crate) fn default_backend() -> String {
    extenddb_storage::config::default_backend_name()
        .unwrap_or_else(|e| panic!("No default storage backend registered: {e}"))
        .to_owned()
}
fn default_tls_enabled() -> bool {
    true
}
fn default_tls_cert_path() -> String {
    std::env::var("HOME").map_or_else(
        |_| "/tmp/extenddb-cert.pem".to_owned(),
        |home| format!("{home}/.extenddb/tls/cert.pem"),
    )
}
fn default_tls_key_path() -> String {
    std::env::var("HOME").map_or_else(
        |_| "/tmp/extenddb-key.pem".to_owned(),
        |home| format!("{home}/.extenddb/tls/key.pem"),
    )
}
fn default_provider() -> String {
    "builtin".to_owned()
}
fn default_log_level() -> String {
    "info".to_owned()
}
fn default_log_format() -> String {
    "pretty".to_owned()
}

/// Load `AppConfig` from a config file (optional) and environment variables.
///
/// # Errors
///
/// Returns an error if the config file exists but is malformed, or if
/// environment variable values cannot be deserialized.
pub fn load(config_path: &str) -> anyhow::Result<AppConfig> {
    let config = config::Config::builder()
        .add_source(config::File::with_name(config_path).required(false))
        .add_source(config::Environment::with_prefix("EXTENDDB").separator("__"))
        .build()?;
    Ok(config.try_deserialize()?)
}

/// Redact password from a connection string for safe logging (REQ-LOG-002).
///
/// Uses the backend-specific operations engine to handle different connection
/// string formats.
pub fn redact_password(backend: &str, conn: &str) -> String {
    extenddb_storage::operations::redact_connection_string(backend, conn)
        .unwrap_or_else(|_| conn.to_owned())
}

/// Validate that a string is safe to use as a database identifier for DDL.
///
/// Delegates to the backend-specific operations engine. Rejects strings
/// containing characters unsafe for `format!`-based DDL where parameterized
/// queries are not supported (e.g. `CREATE DATABASE`, `DROP DATABASE`).
///
/// # Errors
///
/// Returns an error describing the invalid character found.
pub fn validate_identifier(backend: &str, name: &str, label: &str) -> anyhow::Result<()> {
    extenddb_storage::operations::validate_identifier(backend, name, label)
        .map_err(|e| anyhow::anyhow!("{e:?}"))
}

/// Keys whose values must be redacted in configuration displays.
///
/// Canonical list — keep in sync with `REDACTED_KEYS` in
/// `crates/server/src/console/pages/settings_pages.rs`.
const REDACTED_CONFIG_KEYS: &[&str] = &[
    "connection_string",
    "encryption_key",
    "password",
    "secret",
    "token",
];

/// Return `"••••••••"` if `key` matches a redaction pattern, else `val`.
fn redact_if_sensitive(key: &str, val: &str) -> String {
    let lower = key.to_lowercase();
    if REDACTED_CONFIG_KEYS.iter().any(|p| lower.contains(p)) {
        "••••••••".to_owned()
    } else {
        val.to_owned()
    }
}

/// D9: Build static configuration entries for the console settings page.
///
/// Extracts key-value pairs from the parsed `AppConfig` and pre-redacts
/// sensitive values (connection strings, passwords, keys).
pub fn build_config_entries(cfg: &AppConfig) -> Vec<(String, String)> {
    let r = redact_if_sensitive;
    let backend = &cfg.storage._backend;
    let mut entries = vec![
        ("server.bind_addr".into(), cfg.server.bind_addr.clone()),
        ("server.port".into(), cfg.server.port.to_string()),
        ("server.region".into(), cfg.server.region.clone()),
        ("server.run_dir".into(), cfg.server.run_dir.clone()),
        (
            "server.tls.enabled".into(),
            cfg.server.tls.enabled.to_string(),
        ),
        (
            "server.tls.cert_path".into(),
            cfg.server.tls.cert_path.clone(),
        ),
        (
            "server.tls.key_path".into(),
            cfg.server.tls.key_path.clone(),
        ),
        (
            "server.throttling_enabled".into(),
            cfg.server
                .throttling_enabled
                .map_or("none".into(), |b| b.to_string()),
        ),
        (
            format!("storage.{backend}.connection_string"),
            r("connection_string", cfg.storage.connection_config()),
        ),
        (
            format!("storage.{backend}.pool_size"),
            cfg.storage.max_connections().to_string(),
        ),
        (
            format!("storage.{backend}.catalog_pool_size"),
            cfg.storage.max_catalog_connections().to_string(),
        ),
        ("auth.provider".into(), cfg.auth.provider.clone()),
        ("logging.level".into(), cfg.logging.level.clone()),
        ("logging.format".into(), cfg.logging.format.clone()),
        ("docs_dir".into(), cfg.docs_dir.clone().unwrap_or_default()),
        (
            "import.paths".into(),
            if cfg.import_config.paths.is_empty() {
                "none".into()
            } else {
                cfg.import_config.paths.join(", ")
            },
        ),
        (
            "export.paths".into(),
            if cfg.export_config.paths.is_empty() {
                "none".into()
            } else {
                cfg.export_config.paths.join(", ")
            },
        ),
    ];

    if let Some(resource_group) = cfg.storage.native_capacity_resource_group() {
        entries.push((
            format!("storage.{backend}.resource_group"),
            resource_group.to_owned(),
        ));
    }

    // Commonly adjusted limits (full list in [limits] section of extenddb.sample.toml).
    let lim = &cfg.limits;
    entries.extend([
        (
            "limits.max_item_size_bytes".into(),
            lim.max_item_size_bytes.to_string(),
        ),
        (
            "limits.max_tables_per_account".into(),
            lim.max_tables_per_account.to_string(),
        ),
        (
            "limits.max_gsis_per_table".into(),
            lim.max_gsis_per_table.to_string(),
        ),
        (
            "limits.allow_multipart_table_keys".into(),
            lim.allow_multipart_table_keys.to_string(),
        ),
        (
            "limits.max_import_file_bytes".into(),
            lim.max_import_file_bytes.to_string(),
        ),
    ]);

    entries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_tilde_with_home() {
        let home = std::env::var("HOME").unwrap();
        assert_eq!(expand_tilde("~/foo/bar"), format!("{home}/foo/bar"));
        assert_eq!(expand_tilde("~"), home);
    }

    #[test]
    fn expand_tilde_no_tilde() {
        assert_eq!(expand_tilde("/absolute/path"), "/absolute/path");
        assert_eq!(expand_tilde("relative/path"), "relative/path");
    }

    #[test]
    fn expand_tilde_not_home_prefix() {
        // ~user should NOT be expanded (we only handle ~/...)
        assert_eq!(expand_tilde("~user/foo"), "~user/foo");
    }
}
