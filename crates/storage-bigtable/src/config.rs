// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Cloud Bigtable backend configuration.

use std::sync::OnceLock;

use extenddb_storage::config::{StartupWarning, StorageConfig};
use serde::Deserialize;

/// Connection settings for a Cloud Bigtable backend deployment.
///
/// Loaded from the `[storage.bigtable]` block in `extenddb.toml`. If
/// `emulator_host` (or `BIGTABLE_EMULATOR_HOST`) is set, the backend connects
/// to the local Bigtable emulator over plaintext HTTP/2 without credentials;
/// otherwise it connects to Google Cloud Bigtable over TLS using Application
/// Default Credentials (or `credentials_path`).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BigtableStorageConfig {
    /// GCP project ID (or an arbitrary project identifier when using the emulator).
    pub project_id: String,

    /// Cloud Bigtable instance ID within `project_id` holding `__extenddb_catalog__`.
    pub instance_id: String,

    /// Optional separate Cloud Bigtable instance ID for data tables,
    /// `__extenddb_txn_log__`, `__extenddb_ttl_index__`, and `__extenddb_data_ledger__`.
    #[serde(default)]
    pub data_instance_id: Option<String>,

    /// Optional Cloud Bigtable application profile ID (defaults to `"default"`).
    ///
    /// # Routing Requirements
    /// Cloud Bigtable supports single-row transactions (`CheckAndMutateRow`)
    /// **only** on application profiles configured with **single-cluster routing**
    /// and `allow_transactional_writes = true`. Multi-cluster routing profiles do
    /// not support single-row transactions and are refused at server startup.
    #[serde(default, alias = "app_profile")]
    pub app_profile_id: Option<String>,

    /// Optional path to a GCP service account JSON key file.
    #[serde(default)]
    pub credentials_path: Option<String>,

    /// If set, point clients at this `host:port` instead of Google Cloud Bigtable.
    /// Equivalent to setting `BIGTABLE_EMULATOR_HOST=host:port`.
    #[serde(default)]
    pub emulator_host: Option<String>,

    /// Max concurrent gRPC channels on the data path.
    #[serde(
        default = "default_pool_size",
        deserialize_with = "extenddb_storage::config::string_coerce::u32"
    )]
    pub pool_size: u32,

    /// Max concurrent gRPC channels on the catalog/auth path. Defaults to `pool_size`.
    #[serde(
        default,
        deserialize_with = "extenddb_storage::config::string_coerce::opt_u32"
    )]
    pub catalog_pool_size: Option<u32>,

    /// How often the 2PC recovery sweeper scans for stale intents. Default 30s.
    #[serde(
        default = "default_sweeper_cadence",
        deserialize_with = "extenddb_storage::config::string_coerce::u64"
    )]
    pub sweeper_cadence_secs: u64,

    /// How long an unresolved 2PC intent stays in PENDING before the sweeper
    /// rolls it back. Default 60s.
    #[serde(
        default = "default_intent_timeout",
        deserialize_with = "extenddb_storage::config::string_coerce::u64"
    )]
    pub intent_timeout_secs: u64,

    /// How often the TTL scanner runs. Default 60s.
    #[serde(
        default = "default_ttl_scan_cadence",
        deserialize_with = "extenddb_storage::config::string_coerce::u64"
    )]
    pub ttl_scan_cadence_secs: u64,

    /// How often the GSI reconciler worker checks `CREATING` indexes. Default 300s.
    #[serde(
        default = "default_gsi_reconcile_cadence",
        deserialize_with = "extenddb_storage::config::string_coerce::u64"
    )]
    pub gsi_reconcile_cadence_secs: u64,

    /// Lazily materialized connection string so `StorageConfig::connection_config`
    /// returns a borrowed `&str` without leaking memory on repeated calls.
    #[doc(hidden)]
    #[serde(skip)]
    pub cached_url: OnceLock<String>,
}

impl Clone for BigtableStorageConfig {
    fn clone(&self) -> Self {
        Self {
            project_id: self.project_id.clone(),
            instance_id: self.instance_id.clone(),
            data_instance_id: self.data_instance_id.clone(),
            app_profile_id: self.app_profile_id.clone(),
            credentials_path: self.credentials_path.clone(),
            emulator_host: self.emulator_host.clone(),
            pool_size: self.pool_size,
            catalog_pool_size: self.catalog_pool_size,
            sweeper_cadence_secs: self.sweeper_cadence_secs,
            intent_timeout_secs: self.intent_timeout_secs,
            ttl_scan_cadence_secs: self.ttl_scan_cadence_secs,
            gsi_reconcile_cadence_secs: self.gsi_reconcile_cadence_secs,
            cached_url: OnceLock::new(),
        }
    }
}

impl Default for BigtableStorageConfig {
    fn default() -> Self {
        Self {
            project_id: String::new(),
            instance_id: String::new(),
            data_instance_id: None,
            app_profile_id: None,
            credentials_path: None,
            emulator_host: None,
            pool_size: default_pool_size(),
            catalog_pool_size: None,
            sweeper_cadence_secs: default_sweeper_cadence(),
            intent_timeout_secs: default_intent_timeout(),
            ttl_scan_cadence_secs: default_ttl_scan_cadence(),
            gsi_reconcile_cadence_secs: default_gsi_reconcile_cadence(),
            cached_url: OnceLock::new(),
        }
    }
}

fn default_pool_size() -> u32 {
    20
}

fn default_sweeper_cadence() -> u64 {
    30
}

fn default_intent_timeout() -> u64 {
    60
}

fn default_ttl_scan_cadence() -> u64 {
    60
}

fn default_gsi_reconcile_cadence() -> u64 {
    300
}

/// Read `BIGTABLE_EMULATOR_HOST` from the process environment in one place,
/// returning `None` when unset or blank.
#[must_use]
pub fn ambient_emulator_host() -> Option<String> {
    std::env::var("BIGTABLE_EMULATOR_HOST")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

impl BigtableStorageConfig {
    /// Resolve the effective emulator endpoint (`emulator_host` in config,
    /// falling back to `BIGTABLE_EMULATOR_HOST` in the environment).
    #[must_use]
    pub fn resolve_emulator_host(&self) -> Option<String> {
        self.emulator_host
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .or_else(ambient_emulator_host)
    }

    /// Resolve the effective Bigtable instance ID for data tables (`data_instance_id`
    /// when non-empty, otherwise `instance_id`).
    #[must_use]
    pub fn effective_data_instance_id(&self) -> &str {
        self.data_instance_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(&self.instance_id)
    }

    /// Returns a synthesised connection string of the form
    /// `bigtable://<project>/<instance>[?emulator=host:port&...]`. Used by the
    /// `StorageConfig::connection_config()` trait method.
    pub fn connection_string(&self) -> String {
        let mut params: Vec<String> = Vec::new();
        if let Some(host) = self
            .emulator_host
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            params.push(format!("emulator={}", urlencoding::encode(host)));
        }
        if let Some(data_inst) = self
            .data_instance_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            params.push(format!(
                "data_instance_id={}",
                urlencoding::encode(data_inst)
            ));
        }
        if let Some(app_profile) = self
            .app_profile_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            params.push(format!(
                "app_profile_id={}",
                urlencoding::encode(app_profile)
            ));
        }
        if let Some(cred_path) = self
            .credentials_path
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            params.push(format!(
                "credentials_path={}",
                urlencoding::encode(cred_path)
            ));
        }
        if self.pool_size != default_pool_size() {
            params.push(format!("pool_size={}", self.pool_size));
        }
        if let Some(cat_pool) = self.catalog_pool_size {
            params.push(format!("catalog_pool_size={cat_pool}"));
        }
        if self.ttl_scan_cadence_secs != default_ttl_scan_cadence() {
            params.push(format!("ttl_scan={}", self.ttl_scan_cadence_secs));
        }
        if self.sweeper_cadence_secs != default_sweeper_cadence() {
            params.push(format!("sweeper={}", self.sweeper_cadence_secs));
        }
        if self.intent_timeout_secs != default_intent_timeout() {
            params.push(format!("intent_timeout={}", self.intent_timeout_secs));
        }
        if self.gsi_reconcile_cadence_secs != default_gsi_reconcile_cadence() {
            params.push(format!("gsi_reconcile={}", self.gsi_reconcile_cadence_secs));
        }
        let query = if params.is_empty() {
            String::new()
        } else {
            format!("?{}", params.join("&"))
        };
        format!("bigtable://{}/{}{query}", self.project_id, self.instance_id)
    }

    /// Round-trip parse from the string produced by [`Self::connection_string`].
    ///
    /// # Errors
    ///
    /// Returns an error string if `s` is not a valid `bigtable://<project>/<instance>` URL
    /// or contains out-of-range numeric query parameters.
    pub fn from_connection_string(s: &str) -> Result<Self, String> {
        let stripped = s
            .strip_prefix("bigtable://")
            .ok_or_else(|| format!("not a bigtable URL: {s}"))?;
        let (path, query) = match stripped.split_once('?') {
            Some((p, q)) => (p, Some(q)),
            None => (stripped, None),
        };
        let (project_id, instance_id) = path
            .split_once('/')
            .map(|(p, i)| (p.to_owned(), i.to_owned()))
            .ok_or_else(|| format!("missing instance in URL: {s}"))?;
        if project_id.is_empty() || instance_id.is_empty() {
            return Err(format!("empty project or instance in URL: {s}"));
        }

        let mut emulator_host = None;
        let mut data_instance_id = None;
        let mut app_profile_id = None;
        let mut credentials_path = None;
        let mut pool_size = default_pool_size();
        let mut catalog_pool_size = None;
        let mut ttl_scan = default_ttl_scan_cadence();
        let mut sweeper = default_sweeper_cadence();
        let mut intent_timeout = default_intent_timeout();
        let mut gsi_reconcile = default_gsi_reconcile_cadence();

        if let Some(q) = query {
            for pair in q.split('&') {
                if pair.is_empty() {
                    continue;
                }
                if let Some((k, raw_v)) = pair.split_once('=') {
                    let v = urlencoding::decode(raw_v)
                        .map_or_else(|_| raw_v.to_owned(), std::borrow::Cow::into_owned);
                    match k {
                        "emulator" | "emulator_host" => {
                            let trimmed = v.trim();
                            if !trimmed.is_empty() {
                                emulator_host = Some(trimmed.to_owned());
                            }
                        }
                        "data_instance_id" => {
                            let trimmed = v.trim();
                            if !trimmed.is_empty() {
                                data_instance_id = Some(trimmed.to_owned());
                            }
                        }
                        "app_profile_id" | "app_profile" => {
                            let trimmed = v.trim();
                            if !trimmed.is_empty() {
                                app_profile_id = Some(trimmed.to_owned());
                            }
                        }
                        "credentials_path" => {
                            let trimmed = v.trim();
                            if !trimmed.is_empty() {
                                credentials_path = Some(trimmed.to_owned());
                            }
                        }
                        "pool_size" => pool_size = v.parse().unwrap_or(pool_size),
                        "catalog_pool_size" => catalog_pool_size = v.parse().ok(),
                        "ttl_scan" => ttl_scan = v.parse().unwrap_or(ttl_scan),
                        "sweeper" => sweeper = v.parse().unwrap_or(sweeper),
                        "intent_timeout" => intent_timeout = v.parse().unwrap_or(intent_timeout),
                        "gsi_reconcile" => gsi_reconcile = v.parse().unwrap_or(gsi_reconcile),
                        _ => {}
                    }
                }
            }
        }
        Ok(Self {
            project_id,
            instance_id,
            data_instance_id,
            app_profile_id,
            credentials_path,
            emulator_host,
            pool_size,
            catalog_pool_size,
            sweeper_cadence_secs: sweeper,
            intent_timeout_secs: intent_timeout,
            ttl_scan_cadence_secs: ttl_scan,
            gsi_reconcile_cadence_secs: gsi_reconcile,
            cached_url: OnceLock::new(),
        })
    }
}

impl StorageConfig for BigtableStorageConfig {
    fn connection_config(&self) -> &str {
        self.cached_url.get_or_init(|| self.connection_string())
    }

    fn max_connections(&self) -> u32 {
        self.pool_size
    }

    fn max_catalog_connections(&self) -> u32 {
        self.catalog_pool_size.unwrap_or(self.pool_size)
    }

    fn startup_warnings(&self) -> Vec<StartupWarning> {
        let mut out = Vec::new();
        let configured_emu = self
            .emulator_host
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        if let Some(host) = self.resolve_emulator_host() {
            let source = if configured_emu.is_some() {
                "storage.bigtable.emulator_host"
            } else {
                "BIGTABLE_EMULATOR_HOST"
            };
            out.push(StartupWarning {
                title: format!(
                    "Bigtable emulator mode active ({source} = {host})"
                ),
                details: vec![
                    format!(
                        "Connecting to {host} over plaintext HTTP/2 without GCP credentials (project '{}', instance '{}').",
                        self.project_id, self.instance_id
                    ),
                    "Unset BIGTABLE_EMULATOR_HOST and remove emulator_host before connecting to Google Cloud Bigtable.".to_owned(),
                ],
            });
        }
        out
    }

    fn clone_box(&self) -> Box<dyn StorageConfig> {
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
    fn default_config_does_not_point_at_emulator() {
        let cfg = BigtableStorageConfig::default();
        assert!(cfg.project_id.is_empty());
        assert!(cfg.instance_id.is_empty());
        assert_eq!(cfg.emulator_host, None);
    }

    #[test]
    fn config_url_roundtrip_preserves_app_profile_and_pool_sizes() {
        let cfg = BigtableStorageConfig {
            project_id: "my-proj".to_string(),
            instance_id: "my-inst".to_string(),
            data_instance_id: Some("my-data-inst".to_string()),
            app_profile_id: Some("single-cluster-profile".to_string()),
            credentials_path: Some("/var/secrets/gcp key & sa.json".to_string()),
            pool_size: 48,
            catalog_pool_size: Some(12),
            ..BigtableStorageConfig::default()
        };
        let url = cfg.connection_string();
        assert!(url.contains("app_profile_id=single-cluster-profile"));
        assert!(url.contains("pool_size=48"));
        assert!(url.contains("catalog_pool_size=12"));

        let parsed = BigtableStorageConfig::from_connection_string(&url).unwrap();
        assert_eq!(parsed.project_id, "my-proj");
        assert_eq!(parsed.instance_id, "my-inst");
        assert_eq!(parsed.data_instance_id.as_deref(), Some("my-data-inst"));
        assert_eq!(
            parsed.app_profile_id.as_deref(),
            Some("single-cluster-profile")
        );
        assert_eq!(
            parsed.credentials_path.as_deref(),
            Some("/var/secrets/gcp key & sa.json")
        );
        assert_eq!(parsed.pool_size, 48);
        assert_eq!(parsed.catalog_pool_size, Some(12));

        let p1 = cfg.connection_config().as_ptr();
        let p2 = cfg.connection_config().as_ptr();
        assert_eq!(p1, p2);
    }

    #[test]
    fn config_deserializes_app_profile_alias_and_emulator_aliases() {
        let raw = r#"
            project_id = "p1"
            instance_id = "i1"
            app_profile = "single-cluster"
        "#;
        let cfg: BigtableStorageConfig = toml::from_str(raw).unwrap();
        assert_eq!(cfg.app_profile_id.as_deref(), Some("single-cluster"));

        let p2 = BigtableStorageConfig::from_connection_string(
            "bigtable://proj/inst?emulator_host=127.0.0.1:9090&app_profile=prof1",
        )
        .unwrap();
        assert_eq!(p2.emulator_host.as_deref(), Some("127.0.0.1:9090"));
        assert_eq!(p2.app_profile_id.as_deref(), Some("prof1"));
    }

    #[test]
    fn toml_deserialization_requires_project_and_instance_and_denies_unknown_fields() {
        let missing: Result<BigtableStorageConfig, _> = toml::Table::new().try_into();
        assert!(
            missing.is_err(),
            "missing project_id/instance_id must be rejected"
        );

        let unknown: Result<BigtableStorageConfig, _> = toml::from_str(
            r#"
            project_id = "p"
            instance_id = "i"
            dev_mode = true
            "#,
        );
        assert!(
            unknown.is_err(),
            "unknown fields such as dev_mode must be rejected"
        );

        let valid: toml::Table = toml::from_str(
            r#"
            project_id = "my-gcp-project"
            instance_id = "my-bt-instance"
            "#,
        )
        .unwrap();
        let cfg: BigtableStorageConfig = valid.try_into().expect("valid bigtable config");
        assert_eq!(cfg.project_id, "my-gcp-project");
        assert_eq!(cfg.instance_id, "my-bt-instance");
        assert_eq!(cfg.emulator_host, None);
    }

    #[test]
    fn string_coercion_accepts_env_var_overrides() {
        let raw = r#"
            project_id = "p-env"
            instance_id = "i-env"
            pool_size = "32"
            catalog_pool_size = "10"
            sweeper_cadence_secs = "15"
            intent_timeout_secs = "45"
            ttl_scan_cadence_secs = "90"
            gsi_reconcile_cadence_secs = "120"
        "#;
        let cfg: BigtableStorageConfig = toml::from_str(raw).expect("string-coerced config");
        assert_eq!(cfg.pool_size, 32);
        assert_eq!(cfg.catalog_pool_size, Some(10));
        assert_eq!(cfg.sweeper_cadence_secs, 15);
        assert_eq!(cfg.intent_timeout_secs, 45);
        assert_eq!(cfg.ttl_scan_cadence_secs, 90);
        assert_eq!(cfg.gsi_reconcile_cadence_secs, 120);
    }
}
