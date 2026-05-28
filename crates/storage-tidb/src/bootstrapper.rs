// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! TiDB implementation of `Bootstrapper`.
//!
//! Handles `CREATE DATABASE`, schema migrations, user provisioning, and
//! teardown using TiDB-specific DDL. Connection pools are created
//! lazily as needed during the bootstrap sequence.

use async_trait::async_trait;
use extenddb_storage::bootstrapper::{
    AdminBootstrapResult, BootstrapConfig, BootstrapOptions, Bootstrapper,
};
use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::MySqlPool;
use sqlx::mysql::{MySqlConnectOptions, MySqlPoolOptions};
use tokio::sync::OnceCell;

use crate::CATALOG_VERSION;
use crate::migrations;

/// Utilities for bootstrapping a TiDB backend store.
///
/// Holds the bootstrap configuration and lazily-created connection pools.
/// The admin pool connects without selecting a database and is created lazily
/// on first use. Commands that only need the catalog database (e.g. `migrate`)
/// never open an admin connection.
pub struct TidbBootstrapper {
    config: BootstrapConfig,
    admin_pool: OnceCell<MySqlPool>,
}

impl TidbBootstrapper {
    /// Create a new bootstrapper. The admin pool is created lazily on
    /// first use, so this constructor never opens a database connection.
    pub fn new(config: BootstrapConfig) -> Self {
        Self {
            config,
            admin_pool: OnceCell::new(),
        }
    }

    /// Connect to TiDB as the admin user eagerly.
    /// Equivalent to `new()` followed by an immediate admin pool init.
    pub async fn connect(config: BootstrapConfig) -> OpResult<Self> {
        let store = Self::new(config);
        // Force admin pool creation to fail fast on connection errors.
        store.admin_pool().await?;
        Ok(store)
    }

    /// Get or create the admin pool.
    async fn admin_pool(&self) -> OpResult<&MySqlPool> {
        self.admin_pool
            .get_or_try_init(|| async {
                let opts = MySqlConnectOptions::new()
                    .host(&self.config.host)
                    .port(self.config.port)
                    .username(&self.config.admin_user);
                let opts = if let Some(ref pass) = self.config.admin_password {
                    opts.password(pass)
                } else {
                    opts
                };
                MySqlPoolOptions::new()
                    .max_connections(1)
                    .connect_with(opts)
                    .await
                    .map_err(|e| OpError::Internal(format!("Cannot connect as admin: {e}")))
            })
            .await
    }

    /// Build `MySqlConnectOptions` for the application user connecting to a named database.
    fn app_connect_opts(&self, database: &str) -> MySqlConnectOptions {
        MySqlConnectOptions::new()
            .host(&self.config.host)
            .port(self.config.port)
            .username(&self.config.app_user)
            .password(&self.config.app_password)
            .database(database)
    }

    /// Build the connection URL for the application user and a named database.
    fn app_connection_url(&self, database: &str) -> String {
        crate::config::connection_url(
            &self.config.app_user,
            &self.config.app_password,
            &self.config.host,
            self.config.port,
            database,
        )
    }

    /// Open a one-shot pool to the given database as the application user.
    async fn app_pool(&self, database: &str) -> OpResult<MySqlPool> {
        MySqlPoolOptions::new()
            .max_connections(1)
            .connect_with(self.app_connect_opts(database))
            .await
            .map_err(|e| OpError::Internal(format!("Cannot connect to {database}: {e}")))
    }

    /// Return the catalog connection URL (for config file generation).
    pub fn catalog_connection_url(&self) -> String {
        self.app_connection_url(&self.config.catalog_db)
    }
}

#[async_trait]
impl Bootstrapper for TidbBootstrapper {
    async fn ensure_app_user(&self) -> OpResult<()> {
        let user = &self.config.app_user;
        let password = &self.config.app_password;
        let admin = self.admin_pool().await?;

        println!("--- Ensuring application user '{user}' exists...");
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM mysql.user WHERE User = ?)")
                .bind(user)
                .fetch_one(admin)
                .await
                .map_err(|e| OpError::Internal(format!("Check user exists: {e}")))?;

        if exists {
            println!("    User '{user}' already exists.");
            return Ok(());
        }

        // CREATE USER doesn't support parameterized account names/passwords in
        // TiDB/MySQL DDL, so keep a strict allowlist before formatting.
        // Strict allowlist prevents SQL injection via backslash, NUL, semicolon, newline.
        if !user
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
        {
            return Err(OpError::Validation(
                "Application user contains disallowed characters. \
                 Only ASCII letters, digits, underscore, and hyphen are permitted."
                    .to_owned(),
            ));
        }
        if !password
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_.,!@#$%^&*()+=~` ".contains(c))
        {
            return Err(OpError::Validation(
                "Application password contains disallowed characters. \
                 Only ASCII letters, digits, and -_.,!@#$%^&*()+=~` space are permitted."
                    .to_owned(),
            ));
        }
        let sql = format!("CREATE USER IF NOT EXISTS '{user}'@'%' IDENTIFIED BY '{password}'");
        sqlx::query(&sql)
            .execute(admin)
            .await
            .map_err(|e| OpError::Internal(format!("Create user: {e}")))?;
        println!("    Created user '{user}'.");
        Ok(())
    }

    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        Ok(())
    }

    async fn create_catalog_db(&self) -> OpResult<()> {
        create_database(
            self.admin_pool().await?,
            &self.config.catalog_db,
            &self.config.app_user,
        )
        .await
    }

    async fn create_data_db(&self) -> OpResult<()> {
        create_database(
            self.admin_pool().await?,
            &self.config.data_db,
            &self.config.app_user,
        )
        .await
    }

    async fn run_catalog_migrations(&self) -> OpResult<()> {
        let pool = self.app_pool(&self.config.catalog_db).await?;
        migrations::run_catalog_migrations(&pool).await
    }

    async fn run_data_migrations(&self) -> OpResult<()> {
        let pool = self.app_pool(&self.config.data_db).await?;
        migrations::run_data_migrations(&pool).await
    }

    async fn record_data_connection(&self) -> OpResult<()> {
        let pool = self.app_pool(&self.config.catalog_db).await?;
        let data_conn = self.app_connection_url(&self.config.data_db);

        println!("--- Recording data database connection in catalog...");
        sqlx::query(
            "INSERT INTO settings (`key`, value) VALUES ('data_database_connection_string', ?) \
             ON DUPLICATE KEY UPDATE value = VALUES(value)",
        )
        .bind(&data_conn)
        .execute(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("Record data connection: {e}")))?;

        sqlx::query(
            "INSERT INTO settings (`key`, value) VALUES ('data_database_name', ?) \
             ON DUPLICATE KEY UPDATE value = VALUES(value)",
        )
        .bind(&self.config.data_db)
        .execute(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("Record data db name: {e}")))?;

        Ok(())
    }

    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        use aes_gcm::KeyInit;
        use base64::Engine;

        let pool = self.app_pool(&self.config.catalog_db).await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM settings WHERE `key` = 'encryption_key')",
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

        sqlx::query("INSERT IGNORE INTO settings (`key`, value) VALUES ('encryption_key', ?)")
            .bind(&key_b64)
            .execute(&pool)
            .await
            .map_err(|e| OpError::Internal(format!("Store encryption key: {e}")))?;

        println!("    Encryption key stored.");
        Ok(())
    }

    async fn bootstrap_default_account(&self) -> OpResult<()> {
        let pool = self.app_pool(&self.config.catalog_db).await?;
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
            "INSERT INTO accounts (account_id, account_name) VALUES (?, ?) \
             ON DUPLICATE KEY UPDATE account_id = account_id",
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
        let pool = self.app_pool(&self.config.catalog_db).await?;
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
            "INSERT INTO admin_users (admin_name, password_hash) VALUES (?, ?) \
             ON DUPLICATE KEY UPDATE admin_name = admin_name",
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
        let pool = self.app_pool(&self.config.catalog_db).await?;
        migrations::table_exists(&pool, "settings").await
    }

    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        let pool = match self.app_pool(&self.config.catalog_db).await {
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
        let pool = match self.app_pool(&self.config.catalog_db).await {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };
        let row = sqlx::query_as::<_, (String,)>(
            "SELECT value FROM settings WHERE `key` = 'data_database_name'",
        )
        .fetch_optional(&pool)
        .await
        .unwrap_or(None);
        Ok(row.map(|(v,)| v))
    }

    async fn drop_databases(&self, data_db: &str) -> OpResult<()> {
        let admin = self.admin_pool().await?;
        if !data_db.is_empty() {
            println!("--- Dropping data database '{data_db}'...");
            let sql = format!("DROP DATABASE IF EXISTS {}", quote_identifier(data_db)?);
            sqlx::query(&sql)
                .execute(admin)
                .await
                .map_err(|e| OpError::Internal(format!("Drop data database: {e}")))?;
        }

        let catalog = &self.config.catalog_db;
        println!("--- Dropping catalog database '{catalog}'...");
        let sql = format!("DROP DATABASE IF EXISTS {}", quote_identifier(catalog)?);
        sqlx::query(&sql)
            .execute(admin)
            .await
            .map_err(|e| OpError::Internal(format!("Drop catalog database: {e}")))?;

        Ok(())
    }

    async fn read_catalog_version(&self) -> OpResult<Option<String>> {
        let pool = self.app_pool(&self.config.catalog_db).await?;

        if !migrations::table_exists(&pool, "settings").await? {
            return Ok(None);
        }

        let row = sqlx::query_as::<_, (String,)>(
            "SELECT value FROM settings WHERE `key` = 'catalog_version'",
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
        self.config.catalog_db.clone()
    }

    fn endpoint_info(&self) -> String {
        format!("{}:{}", self.config.host, self.config.port)
    }

    fn catalog_connection_url(&self) -> String {
        self.app_connection_url(&self.config.catalog_db)
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Create a database, aborting if it already exists.
async fn create_database(pool: &MySqlPool, name: &str, owner: &str) -> OpResult<()> {
    println!("--- Creating database '{name}'...");
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = ?)",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("Check database exists: {e}")))?;

    if exists {
        return Err(OpError::AlreadyExists(format!(
            "Database '{name}' already exists. Run 'destroy' first, then re-run 'init'."
        )));
    }

    // CREATE DATABASE doesn't support parameterized names.
    let sql = format!("CREATE DATABASE {}", quote_identifier(name)?);
    sqlx::query(&sql)
        .execute(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Create database '{name}': {e}")))?;
    if owner
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
    {
        let grant_sql = format!(
            "GRANT ALL PRIVILEGES ON {}.* TO '{}'@'%'",
            quote_identifier(name)?,
            owner
        );
        sqlx::query(&grant_sql)
            .execute(pool)
            .await
            .map_err(|e| OpError::Internal(format!("Grant database '{name}': {e}")))?;
    }
    println!("    Created.");
    Ok(())
}

fn quote_identifier(name: &str) -> OpResult<String> {
    if name.contains('`') || name.contains('\0') || !name.is_ascii() {
        return Err(OpError::Validation(
            "Database name contains invalid characters for TiDB identifiers".to_owned(),
        ));
    }
    Ok(format!("`{name}`"))
}

/// Generate a random 12-digit numeric account ID (matches AWS account ID format).
fn generate_account_id() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let id: u64 = rng.random_range(100_000_000_000..1_000_000_000_000);
    id.to_string()
}

/// Generate a 24-character random password using alphanumeric characters only.
///
/// Restricted to `[a-zA-Z0-9]` to avoid URL-encoding issues in form submissions,
/// shell copy-paste problems, and other contexts where special characters break.
/// At 24 characters from a 62-char alphabet, entropy is ~143 bits — more than sufficient.
fn generate_random_password() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..24)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}

impl TidbBootstrapper {
    /// Create a bootstrapper from config file and typed CLI options.
    pub async fn from_config(
        config_path: &str,
        options: BootstrapOptions,
    ) -> Result<Self, extenddb_storage::error::StorageError> {
        use extenddb_storage::error::StorageError;

        // Load config file if it exists
        let (host, port, user, password, catalog_db_name) = if std::path::Path::new(config_path)
            .exists()
        {
            println!("--- Loading defaults from {}", config_path);

            // Parse connection string from config
            let config_content = std::fs::read_to_string(config_path)
                .map_err(|e| StorageError::Internal(format!("Failed to read config: {e}")))?;
            let app_config: toml::Value = toml::from_str(&config_content)
                .map_err(|e| StorageError::Internal(format!("Failed to parse config: {e}")))?;

            let conn_str = app_config
                .get("storage")
                .and_then(|s| s.get("tidb"))
                .and_then(|p| p.get("connection_string"))
                .and_then(|c| c.as_str())
                .ok_or_else(|| {
                    StorageError::Internal("Missing storage.tidb.connection_string".into())
                })?;

            let parts = crate::config::parse_connection_string(conn_str)
                .map_err(|e| StorageError::Internal(format!("Invalid connection string: {e}")))?;

            // Check for conflicts between CLI args and config values
            check_conflict(options.storage_host.as_ref(), &parts.host, "--storage-host")?;
            check_conflict(options.storage_port.as_ref(), &parts.port, "--storage-port")?;
            check_conflict(options.app_user.as_ref(), &parts.user, "--extenddb-user")?;
            check_conflict(
                options.app_password.as_ref(),
                &parts.password,
                "--extenddb-pass",
            )?;

            if let Some(ref cli_catalog) = options.catalog_db {
                if cli_catalog != &parts.database {
                    return Err(StorageError::Internal(format!(
                        "--catalog-db '{}' conflicts with config file catalog database '{}'",
                        cli_catalog, parts.database
                    )));
                }
            }

            (
                parts.host,
                parts.port,
                parts.user,
                parts.password,
                parts.database,
            )
        } else {
            // No config file - use defaults
            (
                "localhost".to_string(),
                4000,
                "extenddb".to_string(),
                "extenddb-local-dev".to_string(),
                "extenddb_catalog".to_string(),
            )
        };

        // CLI args override config (or use config values if no CLI arg provided)
        let resolved_host = options.storage_host.unwrap_or(host);
        let resolved_port = options.storage_port.unwrap_or(port);
        let resolved_admin_user = options.admin_user.unwrap_or_else(|| "root".to_owned());
        let resolved_catalog_db = options.catalog_db.unwrap_or(catalog_db_name);
        let final_data_db = options.data_db.unwrap_or_else(|| {
            resolved_catalog_db
                .strip_suffix("_catalog")
                .unwrap_or(&resolved_catalog_db)
                .to_owned()
        });
        let resolved_app_user = options.app_user.unwrap_or(user);
        let resolved_app_password = options.app_password.unwrap_or(password);

        let config = BootstrapConfig {
            host: resolved_host,
            port: resolved_port,
            admin_user: resolved_admin_user,
            admin_password: options.admin_password,
            app_user: resolved_app_user,
            app_password: resolved_app_password,
            catalog_db: resolved_catalog_db,
            data_db: final_data_db,
        };

        Ok(Self::new(config))
    }
}

/// Check that a CLI arg, if provided, matches the config value.
fn check_conflict<T: PartialEq + std::fmt::Display>(
    cli_val: Option<&T>,
    config_val: &T,
    flag: &str,
) -> Result<(), extenddb_storage::error::StorageError> {
    if let Some(v) = cli_val {
        if v != config_val {
            return Err(extenddb_storage::error::StorageError::Internal(format!(
                "{} value '{}' conflicts with config file value '{}'",
                flag, v, config_val
            )));
        }
    }
    Ok(())
}
