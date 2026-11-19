// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! BigTable implementation of `Bootstrapper`.

use async_trait::async_trait;
use extenddb_storage::bootstrapper::{
    AdminBootstrapResult, Bootstrapper,
    helpers::{
        generate_account_id, generate_encryption_key, generate_random_password, hash_password_async,
    },
};
use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::{OpError, OpResult};
use extenddb_storage::secret_keys::CATALOG_KEY_SETTING;
use serde_json::json;
use tokio::sync::OnceCell;

use crate::catalog::{CATALOG_TABLE, CF, Catalog, keys};
use crate::config::BigtableStorageConfig;
use crate::data::admin::AdminClient;
use crate::data::client::BigtableClient;

const DEFAULT_ADMIN_USER: &str = "admin";
const DEFAULT_ACCOUNT_SETTING: &str = "default_account_id";

pub struct BigtableBootstrapper {
    config: BigtableStorageConfig,
    client: OnceCell<BigtableClient>,
}

impl BigtableBootstrapper {
    pub fn new(config: BigtableStorageConfig) -> Self {
        Self {
            config,
            client: OnceCell::new(),
        }
    }

    pub async fn from_config(
        config_path: &str,
        _cli_args: &[String],
    ) -> Result<Self, StorageError> {
        let env_non_empty = |keys: &[&str]| -> Option<String> {
            keys.iter().find_map(|k| {
                std::env::var(k)
                    .ok()
                    .map(|v| v.trim().to_owned())
                    .filter(|v| !v.is_empty())
            })
        };

        let mut config = match std::fs::read_to_string(config_path) {
            Ok(raw) => {
                let parsed: toml::Value = toml::from_str(&raw)
                    .map_err(|e| StorageError::Internal(format!("parse {config_path}: {e}")))?;
                if let Some(bt_val) = parsed.get("storage").and_then(|s| s.get("bigtable")) {
                    let bt_table = bt_val
                        .as_table()
                        .ok_or_else(|| {
                            StorageError::Internal("[storage.bigtable] is not a table".into())
                        })?
                        .clone();
                    bt_table.try_into().map_err(|e: toml::de::Error| {
                        StorageError::Internal(format!("invalid [storage.bigtable]: {e}"))
                    })?
                } else {
                    BigtableStorageConfig {
                        project_id: String::new(),
                        instance_id: String::new(),
                        emulator_host: None,
                        ..BigtableStorageConfig::default()
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => BigtableStorageConfig {
                project_id: String::new(),
                instance_id: String::new(),
                emulator_host: None,
                ..BigtableStorageConfig::default()
            },
            Err(e) => return Err(StorageError::Internal(format!("read {config_path}: {e}"))),
        };

        if let Some(emu) = env_non_empty(&[
            "EXTENDDB__STORAGE__BIGTABLE__EMULATOR_HOST",
            "BIGTABLE_EMULATOR_HOST",
        ]) {
            config.emulator_host = Some(emu);
        }
        if let Some(proj) = env_non_empty(&[
            "EXTENDDB__STORAGE__BIGTABLE__PROJECT_ID",
            "BIGTABLE_PROJECT_ID",
            "GOOGLE_CLOUD_PROJECT",
        ]) {
            config.project_id = proj;
        } else if config.project_id.is_empty() && config.emulator_host.is_some() {
            config.project_id = "extenddb-dev".to_owned();
        }
        if let Some(inst) = env_non_empty(&[
            "EXTENDDB__STORAGE__BIGTABLE__INSTANCE_ID",
            "BIGTABLE_INSTANCE_ID",
        ]) {
            config.instance_id = inst;
        } else if config.instance_id.is_empty() && config.emulator_host.is_some() {
            config.instance_id = "extenddb-dev".to_owned();
        }
        if let Some(data_inst) = env_non_empty(&[
            "EXTENDDB__STORAGE__BIGTABLE__DATA_INSTANCE_ID",
            "BIGTABLE_DATA_INSTANCE_ID",
        ]) {
            config.data_instance_id = Some(data_inst);
        }
        if let Some(profile) = env_non_empty(&[
            "EXTENDDB__STORAGE__BIGTABLE__APP_PROFILE_ID",
            "BIGTABLE_APP_PROFILE_ID",
        ]) {
            config.app_profile_id = Some(profile);
        }
        if let Some(creds) = env_non_empty(&["EXTENDDB__STORAGE__BIGTABLE__CREDENTIALS_PATH"]) {
            config.credentials_path = Some(creds);
        }
        if let Some(pool) = env_non_empty(&["EXTENDDB__STORAGE__BIGTABLE__POOL_SIZE"])
            && let Ok(parsed) = pool.parse::<u32>()
        {
            config.pool_size = parsed;
        }
        if let Some(cat_pool) = env_non_empty(&["EXTENDDB__STORAGE__BIGTABLE__CATALOG_POOL_SIZE"])
            && let Ok(parsed) = cat_pool.parse::<u32>()
        {
            config.catalog_pool_size = Some(parsed);
        }
        if let Some(dev) = env_non_empty(&["EXTENDDB__STORAGE__BIGTABLE__DEV_MODE"]) {
            config.dev_mode = dev.eq_ignore_ascii_case("true") || dev == "1";
        }

        if config.project_id.trim().is_empty() || config.instance_id.trim().is_empty() {
            return Err(StorageError::Validation(format!(
                "Bigtable bootstrap requires project_id and instance_id in {config_path} [storage.bigtable] (or via EXTENDDB__STORAGE__BIGTABLE__PROJECT_ID and EXTENDDB__STORAGE__BIGTABLE__INSTANCE_ID)"
            )));
        }

        Ok(Self::new(config))
    }

    async fn client(&self) -> OpResult<&BigtableClient> {
        self.client
            .get_or_try_init(|| async {
                BigtableClient::connect(&self.config)
                    .await
                    .map_err(OpError::Internal)
            })
            .await
    }

    fn catalog(client: &BigtableClient) -> Catalog<'_> {
        Catalog::new(client)
    }

    /// Check whether `__extenddb_catalog__` holds a `version:catalog` record.
    pub async fn is_catalog_initialized(&self) -> OpResult<bool> {
        let client = self.client().await?;
        let cat = Self::catalog(client);
        match cat.get(keys::VERSION).await {
            Ok(Some(_)) => Ok(true),
            Ok(None) | Err(_) => Ok(false),
        }
    }

    /// Read the recorded catalog version string from `version:catalog`.
    pub async fn read_catalog_version(&self) -> OpResult<Option<String>> {
        let client = self.client().await?;
        let cat = Self::catalog(client);
        let row = cat.get(keys::VERSION).await.map_err(OpError::Internal)?;
        Ok(row.and_then(|v| {
            v.get("catalog_version")
                .and_then(|s| s.as_str())
                .map(str::to_owned)
        }))
    }
}

#[async_trait]
impl Bootstrapper for BigtableBootstrapper {
    async fn ensure_app_user(&self) -> OpResult<()> {
        Ok(())
    }

    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        Ok(())
    }

    async fn create_catalog_db(&self) -> OpResult<()> {
        let client = self.client().await?;
        let mut admin = AdminClient::connect(client)
            .await
            .map_err(OpError::Internal)?;
        admin
            .create_table(
                CATALOG_TABLE,
                &[(CF, Some(crate::data::admin::gc_max_versions(1)))],
            )
            .await
            .map_err(OpError::Internal)?;
        Ok(())
    }

    async fn create_data_db(&self) -> OpResult<()> {
        let data_client_owned = match self.config.data_instance_id.as_deref() {
            Some(data_inst) if !data_inst.is_empty() && data_inst != self.config.instance_id => {
                let mut data_cfg = self.config.clone();
                data_cfg.instance_id = data_inst.to_owned();
                Some(
                    BigtableClient::connect(&data_cfg)
                        .await
                        .map_err(OpError::Internal)?,
                )
            }
            _ => None,
        };
        let target = match data_client_owned.as_ref() {
            Some(c) => c,
            None => self.client().await?,
        };
        crate::transact::ensure_txn_log_table(target)
            .await
            .map_err(OpError::Internal)?;
        crate::ttl_worker::ensure_ttl_index_table(target)
            .await
            .map_err(OpError::Internal)?;
        Ok(())
    }

    async fn record_data_connection(&self) -> OpResult<()> {
        Ok(())
    }

    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        let client = self.client().await?;
        let cat = Self::catalog(client);
        let setting_key = keys::setting(CATALOG_KEY_SETTING);
        if cat
            .get(&setting_key)
            .await
            .map_err(OpError::Internal)?
            .is_some()
        {
            return Ok(());
        }
        let b64 = generate_encryption_key().map_err(OpError::Internal)?;
        let _ = cat
            .put_if_absent(
                &setting_key,
                &json!({
                    "key": CATALOG_KEY_SETTING,
                    "value": b64,
                }),
            )
            .await
            .map_err(OpError::Internal)?;
        Ok(())
    }

    async fn bootstrap_default_account(&self) -> OpResult<()> {
        let client = self.client().await?;
        let cat = Self::catalog(client);
        let existing = cat
            .scan_prefix(keys::ACCOUNT_SCAN_PREFIX)
            .await
            .map_err(OpError::Internal)?;
        let account_id = if let Some((row_key, val)) = existing.first() {
            val.get("account_id")
                .and_then(|s| s.as_str())
                .or_else(|| row_key.strip_prefix(keys::ACCOUNT_SCAN_PREFIX))
                .unwrap_or("")
                .to_owned()
        } else {
            let id = generate_account_id();
            let now = time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap_or_else(|_| time::OffsetDateTime::now_utc().to_string());
            cat.put(
                &keys::account(&id),
                &json!({
                    "account_id": id,
                    "account_name": "default",
                    "created_at": now,
                }),
            )
            .await
            .map_err(OpError::Internal)?;
            id
        };

        if !account_id.is_empty() {
            let _ = cat
                .put_if_absent(
                    &keys::setting(DEFAULT_ACCOUNT_SETTING),
                    &json!({
                        "key": DEFAULT_ACCOUNT_SETTING,
                        "value": account_id,
                    }),
                )
                .await
                .map_err(OpError::Internal)?;
        }
        Ok(())
    }

    async fn bootstrap_admin_user(
        &self,
        env_user: Option<&str>,
        env_password: Option<&str>,
    ) -> OpResult<AdminBootstrapResult> {
        let client = self.client().await?;
        let cat = Self::catalog(client);
        let username = env_user.unwrap_or(DEFAULT_ADMIN_USER).to_owned();

        if let Some(existing) = cat
            .get(&keys::admin(&username))
            .await
            .map_err(OpError::Internal)?
        {
            return Ok(AdminBootstrapResult {
                username,
                generated_password: None,
                already_existed: true,
                from_env: existing
                    .get("from_env")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
            });
        }

        let (password, generated, from_env) = match env_password {
            Some(p) => (p.to_owned(), None, true),
            None => {
                let p = generate_random_password();
                (p.clone(), Some(p), false)
            }
        };
        let hashed = hash_password_async(password).await?;
        let now = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| time::OffsetDateTime::now_utc().to_string());
        cat.put(
            &keys::admin(&username),
            &json!({
                "username": username,
                "password_hash": hashed,
                "from_env": from_env,
                "created_at": now,
            }),
        )
        .await
        .map_err(OpError::Internal)?;
        Ok(AdminBootstrapResult {
            username,
            generated_password: generated,
            already_existed: false,
            from_env,
        })
    }

    async fn has_admin_user(&self) -> OpResult<bool> {
        let client = self.client().await?;
        let cat = Self::catalog(client);
        let admins = cat.scan_prefix("admin:").await.map_err(OpError::Internal)?;
        Ok(!admins.is_empty())
    }

    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        let client = self.client().await?;
        let cat = Self::catalog(client);
        let rows = cat
            .scan_prefix(keys::TABLE_META_SCAN_PREFIX)
            .await
            .map_err(OpError::Internal)?;
        let mut names = Vec::new();
        for (key, _) in rows {
            let mut parts = key.splitn(3, ':');
            let _ = parts.next();
            if let (Some(acct_id), Some(table_name)) = (parts.next(), parts.next()) {
                names.push(format!("{acct_id}/{table_name}"));
            }
        }
        Ok(names)
    }

    async fn get_data_db_name(&self) -> OpResult<Option<String>> {
        Ok(None)
    }

    async fn drop_databases(&self, _data_db: &str) -> OpResult<()> {
        let client = self.client().await?;
        let mut admin = AdminClient::connect(client)
            .await
            .map_err(OpError::Internal)?;
        let tables = admin.list_tables().await.map_err(OpError::Internal)?;
        for t in tables {
            if t == CATALOG_TABLE
                || t.starts_with("__extenddb_")
                || (t.starts_with('t') && t.len() >= 17)
            {
                admin.delete_table(&t).await.map_err(OpError::Internal)?;
            }
        }
        if let Some(ref data_inst) = self.config.data_instance_id
            && data_inst != &self.config.instance_id
        {
            let mut data_cfg = self.config.clone();
            data_cfg.instance_id = data_inst.clone();
            let data_client = BigtableClient::connect(&data_cfg)
                .await
                .map_err(OpError::Internal)?;
            let mut data_admin = AdminClient::connect(&data_client)
                .await
                .map_err(OpError::Internal)?;
            let data_tables = data_admin.list_tables().await.map_err(OpError::Internal)?;
            for t in data_tables {
                if t.starts_with("__extenddb_") || (t.starts_with('t') && t.len() >= 17) {
                    data_admin
                        .delete_table(&t)
                        .await
                        .map_err(OpError::Internal)?;
                }
            }
        }
        Ok(())
    }

    fn catalog_database_name(&self) -> String {
        format!(
            "{} (bigtable://{}/{})",
            CATALOG_TABLE, self.config.project_id, self.config.instance_id
        )
    }

    fn endpoint_info(&self) -> String {
        match &self.config.emulator_host {
            Some(host) => format!("bigtable emulator @ {host}"),
            None => format!(
                "bigtable @ projects/{}/instances/{}",
                self.config.project_id, self.config.instance_id
            ),
        }
    }

    fn catalog_connection_url(&self) -> String {
        self.config.connection_string()
    }

    fn generate_backend_config_section(&self) -> String {
        let data_inst_line = match &self.config.data_instance_id {
            Some(v) if !v.is_empty() => format!("data_instance_id = {v:?}"),
            _ => "# data_instance_id = \"\"          # Optional instance ID for data tables"
                .to_owned(),
        };
        let app_profile_line = match &self.config.app_profile_id {
            Some(v) if !v.is_empty() => format!("app_profile_id = {v:?}"),
            _ => "# app_profile_id = \"\"            # Optional app profile ID (single-cluster routing required)"
                .to_owned(),
        };
        let creds_line = match &self.config.credentials_path {
            Some(v) if !v.is_empty() => format!("credentials_path = {v:?}"),
            _ => {
                "# credentials_path = \"\"          # Optional path to a service account JSON file"
                    .to_owned()
            }
        };
        let emulator_line = match &self.config.emulator_host {
            Some(v) if !v.is_empty() => format!("emulator_host = {v:?}"),
            _ => "# emulator_host = \"localhost:8086\" # Optional host:port for local Bigtable emulator"
                .to_owned(),
        };
        let catalog_pool_line = match self.config.catalog_pool_size {
            Some(v) => format!("catalog_pool_size = {v}"),
            None => "# catalog_pool_size = 8          # Max concurrent catalog/auth gRPC channels"
                .to_owned(),
        };
        let dev_mode_line = if self.config.dev_mode {
            "dev_mode = true".to_owned()
        } else {
            "# dev_mode = false               # Bypass authentication for local testing (default false)"
                .to_owned()
        };

        format!(
            "[storage.bigtable]\n\
             project_id = {:?}\n\
             instance_id = {:?}\n\
             {data_inst_line}\n\
             {app_profile_line}\n\
             {creds_line}\n\
             {emulator_line}\n\
             pool_size = {}\n\
             {catalog_pool_line}\n\
             sweeper_cadence_secs = {}\n\
             intent_timeout_secs = {}\n\
             ttl_scan_cadence_secs = {}\n\
             gsi_reconcile_cadence_secs = {}\n\
             {dev_mode_line}",
            self.config.project_id,
            self.config.instance_id,
            self.config.pool_size,
            self.config.sweeper_cadence_secs,
            self.config.intent_timeout_secs,
            self.config.ttl_scan_cadence_secs,
            self.config.gsi_reconcile_cadence_secs,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_backend_config_section_preserves_production_vs_emulator_settings() {
        let prod_cfg = BigtableStorageConfig {
            project_id: "prod-proj".to_owned(),
            instance_id: "prod-inst".to_owned(),
            app_profile_id: Some("single-cluster".to_owned()),
            credentials_path: Some("/etc/extenddb/sa.json".to_owned()),
            emulator_host: None,
            ..BigtableStorageConfig::default()
        };
        let prod_bs = BigtableBootstrapper::new(prod_cfg);
        let section = prod_bs.generate_backend_config_section();
        assert!(section.contains("# emulator_host ="));
        assert!(!section.contains("\nemulator_host ="));
        assert!(section.contains("app_profile_id = \"single-cluster\""));
        assert!(section.contains("credentials_path = \"/etc/extenddb/sa.json\""));

        let parsed: toml::Value = toml::from_str(&section).expect("valid TOML");
        let reparsed: BigtableStorageConfig = parsed["storage"]["bigtable"]
            .clone()
            .try_into()
            .expect("round-trip BigtableStorageConfig");
        assert_eq!(reparsed.project_id, "prod-proj");
        assert_eq!(reparsed.instance_id, "prod-inst");
        assert_eq!(reparsed.app_profile_id.as_deref(), Some("single-cluster"));
        assert_eq!(reparsed.emulator_host, None);
    }
}
