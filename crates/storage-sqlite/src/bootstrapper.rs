// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `Bootstrapper` implementation for the SQLite backend.
//!
//! SQLite has no server, users, or roles, and catalog and data live in one
//! file. Initialization therefore reduces to: create the file, apply the
//! catalog schema, and seed the encryption key, default account, and admin
//! user. Destruction removes the database file and its WAL/SHM sidecars.

use async_trait::async_trait;
use extenddb_storage::bootstrapper::{AdminBootstrapResult, Bootstrapper, helpers};
use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;

use crate::schema::{self, CATALOG_VERSION};
use crate::sqlite_util::sqlite_url;

/// SQLite backend bootstrapper.
///
/// Holds the database file path. Each operation opens a short-lived
/// single-connection pool, since `init`/`destroy`/`migrate` are one-shot CLI
/// paths, not the hot serving path.
pub struct SqliteBootstrapper {
    path: String,
}

impl SqliteBootstrapper {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }

    /// Build a `SqliteBootstrapper` from the config file and CLI args.
    ///
    /// Resolution order for the database path: `--sqlite-path <p>` CLI flag,
    /// then `[storage.sqlite].path` in the config file, then the default
    /// `extenddb.sqlite`.
    pub async fn from_config(config_path: &str, cli_args: &[String]) -> Result<Self, StorageError> {
        if let Some(p) = helpers::extract_arg(cli_args, "--sqlite-path") {
            return Ok(Self::new(p));
        }

        let path = if std::path::Path::new(config_path).exists() {
            let content = std::fs::read_to_string(config_path)
                .map_err(|e| StorageError::Internal(format!("read config {config_path}: {e}")))?;
            let parsed: toml::Value = toml::from_str(&content)
                .map_err(|e| StorageError::Internal(format!("parse config {config_path}: {e}")))?;
            parsed
                .get("storage")
                .and_then(|s| s.get("sqlite"))
                .and_then(|s| s.get("path"))
                .and_then(|v| v.as_str())
                .unwrap_or("extenddb.sqlite")
                .to_owned()
        } else {
            "extenddb.sqlite".to_owned()
        };

        Ok(Self::new(path))
    }

    fn is_memory(&self) -> bool {
        self.path == ":memory:" || self.path.starts_with("file::memory:")
    }

    /// Open a short-lived single-connection pool with the standard PRAGMAs.
    async fn pool(&self) -> OpResult<SqlitePool> {
        SqlitePoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    use sqlx::Executor;
                    conn.execute("PRAGMA journal_mode = WAL").await?;
                    conn.execute("PRAGMA foreign_keys = ON").await?;
                    conn.execute("PRAGMA synchronous = NORMAL").await?;
                    conn.execute("PRAGMA busy_timeout = 5000").await?;
                    Ok(())
                })
            })
            .connect(&sqlite_url(&self.path))
            .await
            .map_err(|e| OpError::Internal(format!("open SQLite database '{}': {e}", self.path)))
    }
}

#[async_trait]
impl Bootstrapper for SqliteBootstrapper {
    async fn ensure_app_user(&self) -> OpResult<()> {
        Ok(()) // SQLite has no user concept.
    }

    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        Ok(()) // SQLite has no role concept.
    }

    async fn create_catalog_db(&self) -> OpResult<()> {
        // Connecting with mode=rwc creates the file; refuse to clobber an
        // existing database so `init` is not silently destructive.
        if !self.is_memory() && std::path::Path::new(&self.path).exists() {
            return Err(OpError::AlreadyExists(format!(
                "SQLite database '{}' already exists. Run 'destroy' first, then 'init'.",
                self.path
            )));
        }
        Ok(())
    }

    async fn create_data_db(&self) -> OpResult<()> {
        Ok(()) // Catalog and data share one file.
    }

    async fn run_catalog_migrations(&self) -> OpResult<()> {
        let pool = self.pool().await?;
        schema::apply(&pool).await
    }

    async fn run_data_migrations(&self) -> OpResult<()> {
        Ok(()) // Single file — schema applied in run_catalog_migrations.
    }

    async fn pending_data_migrations(&self) -> OpResult<Vec<String>> {
        // SQLite applies its complete schema in run_catalog_migrations (a single
        // file), so there are no separately-tracked data migrations that can be
        // pending.
        Ok(Vec::new())
    }

    async fn record_data_connection(&self) -> OpResult<()> {
        let pool = self.pool().await?;
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ('data_database_name', ?) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )
        .bind(&self.path)
        .execute(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("record data db name: {e}")))?;
        Ok(())
    }

    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        let pool = self.pool().await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM settings WHERE key = 'encryption_key')",
        )
        .fetch_one(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("check encryption key: {e}")))?;
        if exists {
            return Ok(());
        }
        let key = helpers::generate_encryption_key();
        sqlx::query("INSERT OR IGNORE INTO settings (key, value) VALUES ('encryption_key', ?)")
            .bind(&key)
            .execute(&pool)
            .await
            .map_err(|e| OpError::Internal(format!("store encryption key: {e}")))?;
        Ok(())
    }

    async fn bootstrap_default_account(&self) -> OpResult<()> {
        let pool = self.pool().await?;
        // Reuse the existing account when already bootstrapped, otherwise create
        // the single default account. Either way, record its id as the canonical
        // default account so callers never infer it from list ordering. The
        // settings write is idempotent (INSERT OR IGNORE), so it also backfills
        // the marker for catalogs bootstrapped before it existed.
        let account_id: String = match sqlx::query_scalar(
            "SELECT account_id FROM accounts ORDER BY account_id LIMIT 1",
        )
        .fetch_optional(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("check accounts: {e}")))?
        {
            Some(id) => id,
            None => {
                let id = helpers::generate_account_id();
                sqlx::query(
                    "INSERT OR IGNORE INTO accounts (account_id, account_name) VALUES (?, 'default')",
                )
                .bind(&id)
                .execute(&pool)
                .await
                .map_err(|e| OpError::Internal(format!("create default account: {e}")))?;
                id
            }
        };
        sqlx::query("INSERT OR IGNORE INTO settings (key, value) VALUES ('default_account_id', ?)")
            .bind(&account_id)
            .execute(&pool)
            .await
            .map_err(|e| OpError::Internal(format!("record default account id: {e}")))?;
        Ok(())
    }

    async fn bootstrap_admin_user(
        &self,
        env_user: Option<&str>,
        env_password: Option<&str>,
    ) -> OpResult<AdminBootstrapResult> {
        let pool = self.pool().await?;
        let username = env_user
            .filter(|s| !s.is_empty())
            .unwrap_or("admin")
            .to_owned();

        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM admin_users WHERE admin_name = ?)")
                .bind(&username)
                .fetch_one(&pool)
                .await
                .map_err(|e| OpError::Internal(format!("check admin user: {e}")))?;
        if exists {
            return Ok(AdminBootstrapResult {
                username,
                generated_password: None,
                already_existed: true,
                from_env: env_user.is_some(),
            });
        }

        let (password, from_env) = match env_password.filter(|s| !s.is_empty()) {
            Some(p) => (p.to_owned(), true),
            None => (helpers::generate_random_password(), false),
        };
        let password_hash = helpers::hash_password_async(password.clone()).await?;

        sqlx::query("INSERT INTO admin_users (admin_name, password_hash) VALUES (?, ?)")
            .bind(&username)
            .bind(&password_hash)
            .execute(&pool)
            .await
            .map_err(|e| OpError::Internal(format!("create admin user: {e}")))?;

        Ok(AdminBootstrapResult {
            username,
            generated_password: if from_env { None } else { Some(password) },
            already_existed: false,
            from_env,
        })
    }

    async fn is_catalog_initialized(&self) -> OpResult<bool> {
        if !self.is_memory() && !std::path::Path::new(&self.path).exists() {
            return Ok(false);
        }
        let Ok(pool) = self.pool().await else {
            return Ok(false);
        };
        schema::table_exists(&pool, "settings").await
    }

    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        let Ok(pool) = self.pool().await else {
            return Ok(Vec::new());
        };
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT table_name FROM tables ORDER BY table_name")
                .fetch_all(&pool)
                .await
                .unwrap_or_default();
        Ok(rows.into_iter().map(|(n,)| n).collect())
    }

    async fn get_data_db_name(&self) -> OpResult<Option<String>> {
        let Ok(pool) = self.pool().await else {
            return Ok(None);
        };
        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM settings WHERE key = 'data_database_name'")
                .fetch_optional(&pool)
                .await
                .unwrap_or(None);
        Ok(row.map(|(v,)| v))
    }

    async fn drop_databases(&self, _data_db: &str) -> OpResult<()> {
        if self.is_memory() {
            return Ok(());
        }
        if std::path::Path::new(&self.path).exists() {
            std::fs::remove_file(&self.path)
                .map_err(|e| OpError::Internal(format!("remove database file: {e}")))?;
        }
        // Remove WAL and shared-memory sidecars if present.
        let _ = std::fs::remove_file(format!("{}-wal", self.path));
        let _ = std::fs::remove_file(format!("{}-shm", self.path));
        Ok(())
    }

    async fn read_catalog_version(&self) -> OpResult<Option<String>> {
        let Ok(pool) = self.pool().await else {
            return Ok(None);
        };
        if !schema::table_exists(&pool, "settings").await? {
            return Ok(None);
        }
        let row: Option<(String,)> =
            sqlx::query_as("SELECT value FROM settings WHERE key = 'catalog_version'")
                .fetch_optional(&pool)
                .await
                .map_err(|e| OpError::Internal(format!("read catalog version: {e}")))?;
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
        sqlite_url(&self.path)
    }

    fn generate_backend_config_section(&self) -> String {
        format!(
            "[storage.sqlite]\n\
             path = \"{}\"\n\
             # pool_size = 10",
            self.path
        )
    }
}
