// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! SQLite implementation of `Bootstrapper`.
//!
//! SQLite has no server concept — initialization simply creates the database
//! file and runs schema migrations. No user provisioning is needed.

use async_trait::async_trait;
use extenddb_storage::bootstrapper::{AdminBootstrapResult, Bootstrapper};
use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;

use crate::engine::CATALOG_VERSION;
use crate::migrations;

/// SQLite bootstrapper.
pub struct SqliteBootstrapper {
    pub(crate) path: String,
}

impl SqliteBootstrapper {
    pub fn new(path: String) -> Self {
        Self { path }
    }

    fn connection_string(&self) -> String {
        if self.path == ":memory:" {
            "sqlite::memory:".to_owned()
        } else {
            format!("sqlite://{}?mode=rwc", self.path)
        }
    }

    async fn pool(&self) -> OpResult<SqlitePool> {
        let conn = self.connection_string();
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    use sqlx::Executor;
                    conn.execute("PRAGMA journal_mode=WAL").await?;
                    conn.execute("PRAGMA foreign_keys=ON").await?;
                    conn.execute("PRAGMA synchronous=NORMAL").await?;
                    Ok(())
                })
            })
            .connect(&conn)
            .await
            .map_err(|e| OpError::Internal(format!("Cannot open SQLite database: {e}")))?;
        Ok(pool)
    }
}

#[async_trait]
impl Bootstrapper for SqliteBootstrapper {
    async fn ensure_app_user(&self) -> OpResult<()> {
        // SQLite has no user concept.
        Ok(())
    }

    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        // SQLite has no role concept.
        Ok(())
    }

    async fn create_catalog_db(&self) -> OpResult<()> {
        // SQLite creates the file on first connect.
        println!("--- SQLite database: {}", self.path);
        let exists = std::path::Path::new(&self.path).exists();
        if exists && self.path != ":memory:" {
            return Err(OpError::AlreadyExists(format!(
                "SQLite database '{}' already exists. Run 'destroy' first, then re-run 'init'.",
                self.path
            )));
        }
        Ok(())
    }

    async fn create_data_db(&self) -> OpResult<()> {
        // SQLite: catalog and data in same file. No-op.
        Ok(())
    }

    async fn run_catalog_migrations(&self) -> OpResult<()> {
        let pool = self.pool().await?;
        migrations::run_migrations(&pool).await
    }

    async fn run_data_migrations(&self) -> OpResult<()> {
        // SQLite: catalog and data schema are in the same migration file. No-op.
        Ok(())
    }

    async fn record_data_connection(&self) -> OpResult<()> {
        // SQLite: no separate data database. Record path as data_database_name for info.
        let pool = self.pool().await?;
        sqlx::query(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('data_database_name', ?)",
        )
        .bind(&self.path)
        .execute(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("Record data db name: {e}")))?;
        Ok(())
    }

    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        use aes_gcm::KeyInit;
        use base64::Engine;

        let pool = self.pool().await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM settings WHERE key = 'encryption_key')",
        )
        .fetch_one(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("Check encryption key: {e}")))?;

        if exists {
            println!("--- Encryption key already exists, skipping.");
            return Ok(());
        }

        println!("--- Generating AES-256-GCM encryption key...");
        let key = aes_gcm::Aes256Gcm::generate_key(&mut aes_gcm::aead::OsRng);
        let key_b64 = base64::engine::general_purpose::STANDARD.encode(key);

        sqlx::query(
            "INSERT OR IGNORE INTO settings (key, value) VALUES ('encryption_key', ?)",
        )
        .bind(&key_b64)
        .execute(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("Store encryption key: {e}")))?;

        println!("    Encryption key stored.");
        Ok(())
    }

    async fn bootstrap_default_account(&self) -> OpResult<()> {
        let pool = self.pool().await?;
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM accounts)")
            .fetch_one(&pool)
            .await
            .map_err(|e| OpError::Internal(format!("Check accounts: {e}")))?;

        if exists {
            println!("--- Default account already exists, skipping.");
            return Ok(());
        }

        let account_id = generate_account_id();
        println!("--- Creating default account '{account_id}'...");
        sqlx::query(
            "INSERT OR IGNORE INTO accounts (account_id, account_name) VALUES (?, ?)",
        )
        .bind(&account_id)
        .bind("default")
        .execute(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("Create account: {e}")))?;

        println!("    Account ID: {account_id}");
        Ok(())
    }

    async fn bootstrap_admin_user(
        &self,
        env_user: Option<&str>,
        env_password: Option<&str>,
    ) -> OpResult<AdminBootstrapResult> {
        let pool = self.pool().await?;
        let admin_name = env_user.filter(|s| !s.is_empty()).unwrap_or("admin");

        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM admin_users WHERE admin_name = ?)")
                .bind(admin_name)
                .fetch_one(&pool)
                .await
                .map_err(|e| OpError::Internal(format!("Check admin user: {e}")))?;

        if exists {
            println!("--- Admin user '{admin_name}' already exists, skipping.");
            return Ok(AdminBootstrapResult {
                username: admin_name.to_owned(),
                generated_password: None,
                already_existed: true,
                from_env: false,
            });
        }

        println!("--- Creating admin user '{admin_name}'...");
        let (password, from_env) = match env_password {
            Some(p) if !p.is_empty() => (p.to_owned(), true),
            _ => (generate_random_password(), false),
        };
        let pw_clone = password.clone();
        let hash =
            tokio::task::spawn_blocking(move || bcrypt::hash(pw_clone, bcrypt::DEFAULT_COST))
                .await
                .map_err(|e| OpError::Internal(format!("bcrypt hash task failed: {e}")))?
                .map_err(|e| OpError::Internal(format!("bcrypt hash failed: {e}")))?;

        sqlx::query(
            "INSERT OR IGNORE INTO admin_users (admin_name, password_hash) VALUES (?, ?)",
        )
        .bind(admin_name)
        .bind(&hash)
        .execute(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("Create admin user: {e}")))?;

        Ok(AdminBootstrapResult {
            username: admin_name.to_owned(),
            generated_password: if from_env { None } else { Some(password) },
            already_existed: false,
            from_env,
        })
    }

    async fn is_catalog_initialized(&self) -> OpResult<bool> {
        // The file must exist and the settings table must be present.
        if self.path != ":memory:" && !std::path::Path::new(&self.path).exists() {
            return Ok(false);
        }
        let pool = match self.pool().await {
            Ok(p) => p,
            Err(_) => return Ok(false),
        };
        migrations::table_exists(&pool, "settings").await
    }

    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        let pool = match self.pool().await {
            Ok(p) => p,
            Err(_) => return Ok(Vec::new()),
        };
        let tables: Vec<(String,)> =
            sqlx::query_as("SELECT table_name FROM tables ORDER BY table_name")
                .fetch_all(&pool)
                .await
                .unwrap_or_default();
        Ok(tables.into_iter().map(|(n,)| n).collect())
    }

    async fn get_data_db_name(&self) -> OpResult<Option<String>> {
        let pool = match self.pool().await {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };
        let row = sqlx::query_as::<_, (String,)>(
            "SELECT value FROM settings WHERE key = 'data_database_name'",
        )
        .fetch_optional(&pool)
        .await
        .unwrap_or(None);
        Ok(row.map(|(v,)| v))
    }

    async fn drop_databases(&self, _data_db: &str) -> OpResult<()> {
        if self.path != ":memory:" {
            println!("--- Removing SQLite database file '{}'...", self.path);
            if std::path::Path::new(&self.path).exists() {
                std::fs::remove_file(&self.path)
                    .map_err(|e| OpError::Internal(format!("Remove database file: {e}")))?;
            }
            // Also remove WAL and SHM files if present.
            let wal = format!("{}-wal", self.path);
            let shm = format!("{}-shm", self.path);
            let _ = std::fs::remove_file(&wal);
            let _ = std::fs::remove_file(&shm);
        }
        Ok(())
    }

    async fn read_catalog_version(&self) -> OpResult<Option<String>> {
        let pool = match self.pool().await {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };

        if !migrations::table_exists(&pool, "settings").await? {
            return Ok(None);
        }

        let row = sqlx::query_as::<_, (String,)>(
            "SELECT value FROM settings WHERE key = 'catalog_version'",
        )
        .fetch_optional(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("Read catalog version: {e}")))?;

        Ok(row.map(|(v,)| v))
    }

    fn expected_catalog_version(&self) -> String {
        CATALOG_VERSION.to_string()
    }

    fn catalog_database_name(&self) -> String {
        self.path.clone()
    }

    fn endpoint_info(&self) -> String {
        format!("sqlite:{}", self.path)
    }

    fn catalog_connection_url(&self) -> String {
        self.connection_string()
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

fn generate_account_id() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let id: u64 = rng.random_range(100_000_000_000..1_000_000_000_000);
    id.to_string()
}

fn generate_random_password() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..24)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}

impl SqliteBootstrapper {
    pub async fn from_config(
        config_path: &str,
        cli_args: &[String],
    ) -> Result<Self, extenddb_storage::error::StorageError> {
        use extenddb_storage::error::StorageError;

        let sqlite_path = extract_arg(cli_args, "--sqlite-path");

        let path = if let Some(p) = sqlite_path {
            p
        } else if std::path::Path::new(config_path).exists() {
            println!("--- Loading defaults from {}", config_path);

            let config_content = std::fs::read_to_string(config_path)
                .map_err(|e| StorageError::Internal(format!("Failed to read config: {e}")))?;
            let app_config: toml::Value = toml::from_str(&config_content)
                .map_err(|e| StorageError::Internal(format!("Failed to parse config: {e}")))?;

            app_config
                .get("storage")
                .and_then(|s| s.get("sqlite"))
                .and_then(|p| p.get("path"))
                .and_then(|c| c.as_str())
                .unwrap_or("extenddb.sqlite")
                .to_owned()
        } else {
            "extenddb.sqlite".to_owned()
        };

        Ok(Self::new(path))
    }
}

fn extract_arg(args: &[String], flag: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
}
