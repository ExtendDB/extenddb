// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! BigTable backend configuration.

use std::sync::OnceLock;

use extenddb_storage::config::StorageConfig;
use serde::Deserialize;

/// Connection settings for a BigTable backend deployment.
///
/// Loaded from the `[storage.bigtable]` block in `extenddb.toml`. If
/// `emulator_host` is set, the backend uses the local BigTable emulator and
/// skips ADC-based credential discovery; otherwise it connects to real GCP
/// using Application Default Credentials.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BigtableStorageConfig {
    /// GCP project id (or anything when using the emulator).
    pub project_id: String,

    /// BigTable instance id within the project.
    pub instance_id: String,

    /// Optional instance ID specifically for data tables (separating catalog/data).
    #[serde(default)]
    pub data_instance_id: Option<String>,

    /// Optional BigTable application profile ID.
    ///
    /// # Routing Requirements
    /// Cloud Bigtable requires single-cluster routing (or multi-cluster routing
    /// with single-row transactions enabled) for conditional mutations
    /// (`CheckAndMutateRow`). Multi-cluster routing with eventual consistency
    /// does not support `CheckAndMutateRow` operations.
    #[serde(default, alias = "app_profile")]
    pub app_profile_id: Option<String>,

    /// Path to a GCP service account JSON key file.
    #[serde(default)]
    pub credentials_path: Option<String>,

    /// If set, point clients at this `host:port` instead of real GCP.
    /// Equivalent to exporting `BIGTABLE_EMULATOR_HOST=host:port`.
    #[serde(default)]
    pub emulator_host: Option<String>,

    /// Max concurrent connections on the data path.
    #[serde(
        default = "default_pool_size",
        deserialize_with = "extenddb_storage::config::string_coerce::u32"
    )]
    pub pool_size: u32,

    /// Max concurrent connections on the catalog path. Defaults to `pool_size`.
    #[serde(
        default,
        deserialize_with = "extenddb_storage::config::string_coerce::opt_u32"
    )]
    pub catalog_pool_size: Option<u32>,

    /// How often the 2PC recovery sweeper scans for stale intents. Default 30s.
    #[serde(default = "default_sweeper_cadence", deserialize_with = "coerce_u64")]
    pub sweeper_cadence_secs: u64,

    /// How long an unresolved 2PC intent stays in PENDING before the sweeper
    /// rolls it back. Default 60s.
    #[serde(default = "default_intent_timeout", deserialize_with = "coerce_u64")]
    pub intent_timeout_secs: u64,

    /// How often the TTL scanner runs. Default 60s.
    #[serde(default = "default_ttl_scan_cadence", deserialize_with = "coerce_u64")]
    pub ttl_scan_cadence_secs: u64,

    /// How often the GSI reconciler worker runs. Default 300s (5 minutes).
    #[serde(
        default = "default_gsi_reconcile_cadence",
        deserialize_with = "coerce_u64"
    )]
    pub gsi_reconcile_cadence_secs: u64,

    /// Dev-mode auth bypass for local testing. DO NOT enable in production.
    #[serde(
        default,
        deserialize_with = "extenddb_storage::config::string_coerce::bool"
    )]
    pub dev_mode: bool,

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
            dev_mode: self.dev_mode,
            cached_url: OnceLock::new(),
        }
    }
}

impl Default for BigtableStorageConfig {
    fn default() -> Self {
        Self {
            project_id: "extenddb-dev".to_string(),
            instance_id: "extenddb-dev".to_string(),
            data_instance_id: None,
            app_profile_id: None,
            credentials_path: None,
            emulator_host: Some("localhost:8086".to_string()),
            pool_size: default_pool_size(),
            catalog_pool_size: None,
            sweeper_cadence_secs: default_sweeper_cadence(),
            intent_timeout_secs: default_intent_timeout(),
            ttl_scan_cadence_secs: default_ttl_scan_cadence(),
            gsi_reconcile_cadence_secs: default_gsi_reconcile_cadence(),
            dev_mode: false,
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

impl BigtableStorageConfig {
    /// Returns a synthesised connection string of the form
    /// `bigtable://<project>/<instance>[?emulator=host:port&...]`. Used by the
    /// `StorageConfig::connection_config()` trait method.
    pub fn connection_string(&self) -> String {
        let mut params: Vec<String> = Vec::new();
        if let Some(host) = &self.emulator_host {
            params.push(format!("emulator={}", urlencoding::encode(host)));
        }
        if self.dev_mode {
            params.push("dev_mode=true".to_string());
        }
        if let Some(data_inst) = &self.data_instance_id {
            params.push(format!(
                "data_instance_id={}",
                urlencoding::encode(data_inst)
            ));
        }
        if let Some(app_profile) = &self.app_profile_id {
            params.push(format!(
                "app_profile_id={}",
                urlencoding::encode(app_profile)
            ));
        }
        if let Some(cred_path) = &self.credentials_path {
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

    /// Round-trip parse from the string produced by `connection_string`.
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
        let mut dev_mode = false;
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
                        .map(|c| c.into_owned())
                        .unwrap_or_else(|_| raw_v.to_owned());
                    match k {
                        "emulator" | "emulator_host" => emulator_host = Some(v),
                        "dev_mode" => dev_mode = v == "true" || v == "1",
                        "data_instance_id" => data_instance_id = Some(v),
                        "app_profile_id" | "app_profile" => app_profile_id = Some(v),
                        "credentials_path" => credentials_path = Some(v),
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
            dev_mode,
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

    fn clone_box(&self) -> Box<dyn StorageConfig> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn coerce_u64<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum MaybeStringU64 {
        Native(u64),
        Text(String),
    }
    match MaybeStringU64::deserialize(deserializer)? {
        MaybeStringU64::Native(v) => Ok(v),
        MaybeStringU64::Text(s) => s
            .trim()
            .parse()
            .map_err(|e| serde::de::Error::custom(format!("invalid u64 value \"{s}\": {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        // Verify cached_url returns a stable borrowed pointer without leaking.
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
            "bigtable://proj/inst?emulator_host=127.0.0.1:9090&app_profile=prof1&dev_mode=true",
        )
        .unwrap();
        assert_eq!(p2.emulator_host.as_deref(), Some("127.0.0.1:9090"));
        assert_eq!(p2.app_profile_id.as_deref(), Some("prof1"));
        assert!(p2.dev_mode);
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
            bogus_field = 123
            "#,
        );
        assert!(unknown.is_err(), "unknown fields must be rejected");

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
        assert!(!cfg.dev_mode);
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
            dev_mode = "true"
        "#;
        let cfg: BigtableStorageConfig = toml::from_str(raw).expect("string-coerced config");
        assert_eq!(cfg.pool_size, 32);
        assert_eq!(cfg.catalog_pool_size, Some(10));
        assert_eq!(cfg.sweeper_cadence_secs, 15);
        assert_eq!(cfg.intent_timeout_secs, 45);
        assert_eq!(cfg.ttl_scan_cadence_secs, 90);
        assert_eq!(cfg.gsi_reconcile_cadence_secs, 120);
        assert!(cfg.dev_mode);
    }
}
