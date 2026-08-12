//! BigTable implementation of `Bootstrapper`.

use async_trait::async_trait;
use base64::Engine;
use extenddb_storage::bootstrapper::{AdminBootstrapResult, Bootstrapper};
use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::{OpError, OpResult};
use rand::{TryRngCore, rngs::OsRng};
use serde_json::json;
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::catalog::{Catalog, CATALOG_TABLE, CF, keys};
use crate::config::BigtableStorageConfig;
use crate::data::admin::AdminClient;
use crate::data::client::BigtableClient;

const CATALOG_VERSION: &str = "0.1.0";
const DEFAULT_ADMIN_USER: &str = "admin";
const ADMIN_PASSWORD_LEN: usize = 24;

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
        let raw = std::fs::read_to_string(config_path)
            .map_err(|e| StorageError::Internal(format!("read {config_path}: {e}")))?;
        let parsed: toml::Value = toml::from_str(&raw)
            .map_err(|e| StorageError::Internal(format!("parse {config_path}: {e}")))?;
        let bt_table = parsed
            .get("storage")
            .and_then(|s| s.get("bigtable"))
            .ok_or_else(|| StorageError::Internal("missing [storage.bigtable]".into()))?
            .as_table()
            .ok_or_else(|| StorageError::Internal("[storage.bigtable] is not a table".into()))?
            .clone();
        let config: BigtableStorageConfig = bt_table.try_into().map_err(|e: toml::de::Error| {
            StorageError::Internal(format!("invalid [storage.bigtable]: {e}"))
        })?;
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

    fn catalog<'a>(client: &'a BigtableClient) -> Catalog<'a> {
        Catalog::new(client)
    }
}

#[async_trait]
impl Bootstrapper for BigtableBootstrapper {
    async fn ensure_app_user(&self) -> OpResult<()> {
        // BigTable has no DB-level user concept; ExtendDB IAM lives in our catalog.
        Ok(())
    }

    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        Ok(())
    }

    async fn create_catalog_db(&self) -> OpResult<()> {
        let client = self.client().await?;
        let mut admin = AdminClient::connect(client).await.map_err(OpError::Internal)?;
        admin
            .create_table(CATALOG_TABLE, &[(CF, None)])
            .await
            .map_err(OpError::Internal)?;
        Ok(())
    }

    async fn create_data_db(&self) -> OpResult<()> {
        // Per-data-table creation happens in CreateTable, not at bootstrap.
        Ok(())
    }

    async fn run_catalog_migrations(&self) -> OpResult<()> {
        let client = self.client().await?;
        let cat = Self::catalog(client);
        cat.put(
            keys::VERSION,
            &json!({
                "catalog_version": CATALOG_VERSION,
                "applied_at": time::OffsetDateTime::now_utc().to_string(),
            }),
        )
        .await
        .map_err(OpError::Internal)?;
        Ok(())
    }

    async fn run_data_migrations(&self) -> OpResult<()> {
        Ok(())
    }

    async fn pending_data_migrations(&self) -> OpResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn record_data_connection(&self) -> OpResult<()> {
        // Same instance hosts both catalog and data — nothing to record.
        Ok(())
    }

    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        let client = self.client().await?;
        let cat = Self::catalog(client);
        if cat.get(keys::KEY_MATERIAL_ENC).await.map_err(OpError::Internal)?.is_some() {
            return Ok(());
        }
        let mut key = [0u8; 32];
        OsRng.try_fill_bytes(&mut key).map_err(|e| OpError::Internal(format!("rng: {e}")))?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(&key);
        cat.put(
            keys::KEY_MATERIAL_ENC,
            &json!({
                "algorithm": "AES-256-GCM",
                "key_b64": b64,
                "created_at": time::OffsetDateTime::now_utc().to_string(),
            }),
        )
        .await
        .map_err(OpError::Internal)?;
        Ok(())
    }

    async fn bootstrap_default_account(&self) -> OpResult<()> {
        let client = self.client().await?;
        let cat = Self::catalog(client);
        // Skip if any account exists.
        let existing = cat
            .scan_prefix(keys::ACCOUNT_SCAN_PREFIX)
            .await
            .map_err(OpError::Internal)?;
        if !existing.is_empty() {
            return Ok(());
        }
        // 12-digit numeric account id, matching AWS's format. Random for now.
        let account_id = random_account_id();
        cat.put(
            &keys::account(&account_id),
            &json!({
                "account_id": account_id,
                "created_at": time::OffsetDateTime::now_utc().to_string(),
            }),
        )
        .await
        .map_err(OpError::Internal)?;
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
                from_env: existing.get("from_env").and_then(|v| v.as_bool()).unwrap_or(false),
            });
        }

        let (password, generated, from_env) = match env_password {
            Some(p) => (p.to_owned(), None, true),
            None => {
                let p = random_password(ADMIN_PASSWORD_LEN);
                (p.clone(), Some(p), false)
            }
        };
        let hashed = bcrypt::hash(&password, bcrypt::DEFAULT_COST)
            .map_err(|e| OpError::Internal(format!("bcrypt: {e}")))?;
        cat.put(
            &keys::admin(&username),
            &json!({
                "username": username,
                "password_hash": hashed,
                "from_env": from_env,
                "created_at": time::OffsetDateTime::now_utc().to_string(),
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

    async fn is_catalog_initialized(&self) -> OpResult<bool> {
        // Try to read the version row; if the catalog table itself doesn't
        // exist yet the read will fail and we report "not initialized."
        let client = self.client().await?;
        let cat = Self::catalog(client);
        match cat.get(keys::VERSION).await {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(_) => Ok(false),
        }
    }

    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        let client = self.client().await?;
        let cat = Self::catalog(client);
        let accounts = cat
            .scan_prefix(keys::ACCOUNT_SCAN_PREFIX)
            .await
            .map_err(OpError::Internal)?;
        let mut names = Vec::new();
        for (acct_key, _) in accounts {
            let acct_id = acct_key.strip_prefix("acct:").unwrap_or(&acct_key);
            let rows = cat
                .scan_prefix(&keys::table_meta_scan_prefix(acct_id))
                .await
                .map_err(OpError::Internal)?;
            for (key, _) in rows {
                if let Some(table_name) = key.rsplit_once(':').map(|(_, last)| last) {
                    names.push(format!("{acct_id}/{table_name}"));
                }
            }
        }
        Ok(names)
    }

    async fn get_data_db_name(&self) -> OpResult<Option<String>> {
        Ok(None)
    }

    async fn drop_databases(&self, _data_db: &str) -> OpResult<()> {
        let client = self.client().await?;
        let mut admin = AdminClient::connect(client).await.map_err(OpError::Internal)?;
        // Drop every BigTable table we created. Start with the catalog itself.
        let tables = admin.list_tables().await.map_err(OpError::Internal)?;
        for t in tables {
            if t == CATALOG_TABLE || t.starts_with("__extenddb_") || t.contains("__") {
                admin.delete_table(&t).await.map_err(OpError::Internal)?;
            }
        }
        Ok(())
    }

    async fn read_catalog_version(&self) -> OpResult<Option<String>> {
        let client = self.client().await?;
        let cat = Self::catalog(client);
        let row = cat.get(keys::VERSION).await.map_err(OpError::Internal)?;
        Ok(row
            .and_then(|v| v.get("catalog_version").and_then(|s| s.as_str()).map(str::to_owned)))
    }

    fn expected_catalog_version(&self) -> String {
        CATALOG_VERSION.to_string()
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
        format!(
            r#"[storage.bigtable]
project_id = "{}"
instance_id = "{}"
# data_instance_id = ""          # Optional instance ID for data tables
# credentials_path = ""          # Optional path to a service account JSON file
emulator_host = "{}"
# pool_size = 20                 # Max concurrent connections (default 20)
# dev_mode = false               # Bypass authentication for local testing (default false)"#,
            self.config.project_id,
            self.config.instance_id,
            self.config.emulator_host.as_deref().unwrap_or("localhost:8086")
        )
    }
}

fn random_account_id() -> String {
    let mut buf = [0u8; 8];
    OsRng.try_fill_bytes(&mut buf).expect("OS RNG");
    let n = u64::from_be_bytes(buf) % 1_000_000_000_000;
    format!("{n:012}")
}

fn random_password(len: usize) -> String {
    let mut buf = vec![0u8; len.div_ceil(4) * 3 + 4];
    OsRng.try_fill_bytes(&mut buf).expect("OS RNG");
    let mut s = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&buf);
    s.truncate(len);
    s
}

// `_` for use of Uuid (placeholder if I want it later); current code doesn't
// strictly need it. Keep it imported to make admin user ergonomic with role
// session names downstream.
#[allow(dead_code)]
fn _session_id() -> String {
    Uuid::new_v4().to_string()
}
