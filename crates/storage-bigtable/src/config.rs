//! BigTable backend configuration.

use extenddb_storage::config::StorageConfig;
use serde::Deserialize;

/// Connection settings for a BigTable backend deployment.
///
/// Loaded from the `[storage.bigtable]` block in `extenddb.toml`. If
/// `emulator_host` is set, the backend uses the local BigTable emulator and
/// skips ADC-based credential discovery; otherwise it connects to real GCP
/// using Application Default Credentials.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BigtableStorageConfig {
    /// GCP project id (or anything when using the emulator).
    pub project_id: String,

    /// BigTable instance id within the project.
    pub instance_id: String,

    /// Optional instance ID specifically for data tables (separating catalog/data).
    #[serde(default)]
    pub data_instance_id: Option<String>,

    /// Path to a GCP service account JSON key file.
    #[serde(default)]
    pub credentials_path: Option<String>,

    /// If set, point clients at this `host:port` instead of real GCP.
    /// Equivalent to exporting `BIGTABLE_EMULATOR_HOST=host:port`.
    #[serde(default)]
    pub emulator_host: Option<String>,

    /// Max concurrent connections on the data path.
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,

    /// Max concurrent connections on the catalog path. Defaults to `pool_size`.
    #[serde(default)]
    pub catalog_pool_size: Option<u32>,

    /// How often the 2PC recovery sweeper scans for stale intents. Default 30s.
    #[serde(default = "default_sweeper_cadence")]
    pub sweeper_cadence_secs: u64,

    /// How long an unresolved 2PC intent stays in PENDING before the sweeper
    /// rolls it back. Default 60s.
    #[serde(default = "default_intent_timeout")]
    pub intent_timeout_secs: u64,

    /// How often the emulator TTL scanner runs (real BigTable uses GC rules
    /// instead and ignores this). Default 60s.
    #[serde(default = "default_ttl_scan_cadence")]
    pub ttl_scan_cadence_secs: u64,

    /// How often the GSI reconciler worker runs. Default 300s (5 minutes).
    #[serde(default = "default_gsi_reconcile_cadence")]
    pub gsi_reconcile_cadence_secs: u64,

    /// Dev-mode auth bypass. When true, every DDB request authenticates as a
    /// synthetic admin identity and every authz check returns `dynamodb:*`.
    /// Lets us validate the data path without finishing IAM in phase 9.
    /// DO NOT enable on a production deployment.
    #[serde(default)]
    pub dev_mode: bool,
}

impl Default for BigtableStorageConfig {
    fn default() -> Self {
        Self {
            project_id: "extenddb-dev".to_string(),
            instance_id: "extenddb-dev".to_string(),
            data_instance_id: None,
            credentials_path: None,
            emulator_host: Some("localhost:8086".to_string()),
            pool_size: default_pool_size(),
            catalog_pool_size: None,
            sweeper_cadence_secs: default_sweeper_cadence(),
            intent_timeout_secs: default_intent_timeout(),
            ttl_scan_cadence_secs: default_ttl_scan_cadence(),
            gsi_reconcile_cadence_secs: default_gsi_reconcile_cadence(),
            dev_mode: false,
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
    /// `bigtable://<project>/<instance>[?emulator=host:port]`. Used by the
    /// `StorageConfig::connection_config()` trait method for display.
    pub fn connection_string(&self) -> String {
        let mut params: Vec<String> = Vec::new();
        if let Some(host) = &self.emulator_host {
            params.push(format!("emulator={host}"));
        }
        if self.dev_mode {
            params.push("dev_mode=true".to_string());
        }
        if let Some(data_inst) = &self.data_instance_id {
            params.push(format!("data_instance_id={data_inst}"));
        }
        if let Some(cred_path) = &self.credentials_path {
            params.push(format!("credentials_path={cred_path}"));
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
        let mut emulator_host = None;
        let mut dev_mode = false;
        let mut data_instance_id = None;
        let mut credentials_path = None;
        let mut ttl_scan = default_ttl_scan_cadence();
        let mut sweeper = default_sweeper_cadence();
        let mut intent_timeout = default_intent_timeout();
        let mut gsi_reconcile = default_gsi_reconcile_cadence();
        if let Some(q) = query {
            for pair in q.split('&') {
                match pair.split_once('=') {
                    Some(("emulator", v)) => emulator_host = Some(v.to_owned()),
                    Some(("dev_mode", v)) => dev_mode = v == "true" || v == "1",
                    Some(("data_instance_id", v)) => data_instance_id = Some(v.to_owned()),
                    Some(("credentials_path", v)) => credentials_path = Some(v.to_owned()),
                    Some(("ttl_scan", v)) => ttl_scan = v.parse().unwrap_or(ttl_scan),
                    Some(("sweeper", v)) => sweeper = v.parse().unwrap_or(sweeper),
                    Some(("intent_timeout", v)) => intent_timeout = v.parse().unwrap_or(intent_timeout),
                    Some(("gsi_reconcile", v)) => gsi_reconcile = v.parse().unwrap_or(gsi_reconcile),
                    _ => {}
                }
            }
        }
        Ok(Self {
            project_id,
            instance_id,
            data_instance_id,
            credentials_path,
            emulator_host,
            pool_size: default_pool_size(),
            catalog_pool_size: None,
            sweeper_cadence_secs: sweeper,
            intent_timeout_secs: intent_timeout,
            ttl_scan_cadence_secs: ttl_scan,
            gsi_reconcile_cadence_secs: gsi_reconcile,
            dev_mode,
        })
    }
}

impl StorageConfig for BigtableStorageConfig {
    fn connection_config(&self) -> &str {
        Box::leak(self.connection_string().into_boxed_str())
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
