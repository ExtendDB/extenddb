// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Configuration types and loading for ExtendDB.
//!
//! Owns [`AppConfig`] and its subsections, config-file loading, redaction
//! helpers, and runtime-path helpers (PID file). This crate is
//! backend-agnostic: it depends only on the `extenddb-storage` trait surface
//! (never on a concrete backend), so both `extenddb-server` (for `serve`) and
//! the CLI/app layer can depend on it without pulling in a backend or forming
//! a dependency cycle.

use std::path::PathBuf;

use extenddb_core::limits::LimitsConfig;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
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
    /// Enable provisioned throughput throttling via token buckets.
    /// When `None` or `false`, all requests are allowed regardless of capacity.
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
    /// Storage backend selector (e.g. "postgres").
    pub backend: String,
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
}

impl<'de> serde::Deserialize<'de> for StorageConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        // Deserialize into a raw TOML value first
        let value: toml::Value = toml::Value::deserialize(deserializer)?;

        // The installed backend is authoritative: it supplies the name used to
        // locate its `[storage.<name>]` section. The file's `backend` key is
        // therefore optional, and when present it is validated against the
        // compiled-in backend rather than used to choose one. A binary contains
        // exactly one backend, so a mismatch is an operator error worth naming
        // instead of silently ignoring.
        let backend = extenddb_storage::backend_name().ok_or_else(|| {
            D::Error::custom("no storage backend installed (set_backend was not called)")
        })?;

        if let Some(requested) = value.get("backend").and_then(|v| v.as_str())
            && requested != backend
        {
            return Err(D::Error::custom(format!(
                "[storage] backend = \"{requested}\" does not match this binary's \
                 compiled-in backend \"{backend}\". This binary can only serve \
                 \"{backend}\"; either remove the key or install the \
                 extenddb-{requested} binary."
            )));
        }

        // Get the backend-specific table (e.g., [storage.postgres])
        let backend_table: &toml::Table = value
            .get(backend)
            .and_then(|v| v.as_table())
            .ok_or_else(|| {
                D::Error::custom(format!("Missing [storage.{backend}] section in config"))
            })?;

        // Hand the section to the installed backend's deserializer.
        let config = extenddb_storage::config::deserialize_storage_config(backend_table)
            .map_err(D::Error::custom)?;

        Ok(StorageConfig {
            backend: backend.to_owned(),
            config,
        })
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
    /// Configuration for the in-memory auth/authz caches. Optional; defaults
    /// apply when absent.
    #[serde(default)]
    pub cache: AuthCacheConfigToml,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            provider: default_provider(),
            cache: AuthCacheConfigToml::default(),
        }
    }
}

/// Configuration for the in-memory auth/authz caches (`[auth.cache]` section).
///
/// Each `*_seconds` field has a sensible default; operators only need to
/// override values when they want to deviate from the docs. The configuration
/// is static — values are read once at startup.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthCacheConfigToml {
    /// Master switch. When `false`, all caches operate in pass-through mode
    /// (fall through to the underlying store on every request) — useful as a
    /// kill switch during incident response.
    #[serde(default = "default_cache_enabled")]
    pub enabled: bool,
    /// Hard TTL: cached values older than this trigger a fresh load.
    #[serde(default = "default_cache_ttl_seconds")]
    pub ttl_seconds: u64,
    /// Soft TTL: cached values older than this trigger a background refresh
    /// while still returning the cached value to the caller.
    #[serde(default = "default_cache_soft_ttl_seconds")]
    pub soft_ttl_seconds: u64,
    /// TTL applied to negative results (`Ok(None)` from the loader). Typically
    /// shorter than `ttl_seconds` so newly-created entities become visible
    /// quickly.
    #[serde(default = "default_cache_negative_ttl_seconds")]
    pub negative_ttl_seconds: u64,
    /// Maximum entries per cache. When exceeded, LRU eviction kicks in.
    #[serde(default = "default_cache_max_entries")]
    pub max_entries: u64,
}

impl Default for AuthCacheConfigToml {
    fn default() -> Self {
        Self {
            enabled: default_cache_enabled(),
            ttl_seconds: default_cache_ttl_seconds(),
            soft_ttl_seconds: default_cache_soft_ttl_seconds(),
            negative_ttl_seconds: default_cache_negative_ttl_seconds(),
            max_entries: default_cache_max_entries(),
        }
    }
}

fn default_cache_enabled() -> bool {
    true
}
fn default_cache_ttl_seconds() -> u64 {
    60
}
fn default_cache_soft_ttl_seconds() -> u64 {
    30
}
fn default_cache_negative_ttl_seconds() -> u64 {
    5
}
fn default_cache_max_entries() -> u64 {
    10_000
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
    18443
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
#[must_use]
pub fn expand_tilde(path: &str) -> String {
    if let Some(rest) = path.strip_prefix('~')
        && (rest.is_empty() || rest.starts_with('/'))
        && let Ok(home) = std::env::var("HOME")
    {
        return format!("{home}{rest}");
    }
    path.to_owned()
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
/// string formats (`PostgreSQL`).
#[must_use]
pub fn redact_password(conn: &str) -> String {
    extenddb_storage::operations::redact_connection_string(conn).unwrap_or_else(|_| conn.to_owned())
}

/// Return the current OS username, falling back to given default username: e.g. `"postgres"`.
#[must_use]
pub fn whoami(default: &str) -> String {
    std::env::var("USER").unwrap_or_else(|_| default.to_owned())
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
pub fn validate_identifier(name: &str, label: &str) -> anyhow::Result<()> {
    extenddb_storage::operations::validate_identifier(name, label)
        .map_err(|e| anyhow::anyhow!("{e:?}"))
}

/// PID file path for a given port and run directory.
///
/// Used by `serve` (write) and `status`/`stop` (read).
#[must_use]
pub fn pid_file_path(run_dir: &str, port: u16) -> PathBuf {
    PathBuf::from(format!("{run_dir}/extenddb-{port}.pid"))
}

/// PID file path using the default run directory. Used by `status` when
/// no config file is loaded.
#[must_use]
pub fn pid_file_path_default(port: u16) -> PathBuf {
    pid_file_path(&ServerConfig::default().run_dir, port)
}

mod display;
pub use display::{build_config_entries, should_redact};

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

    #[test]
    fn pid_file_path_formats_port() {
        assert_eq!(
            pid_file_path("/run/extenddb", 18443),
            PathBuf::from("/run/extenddb/extenddb-18443.pid")
        );
    }
}
