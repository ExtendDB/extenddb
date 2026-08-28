// Copyright 2026 ExtendDB Contributors
// SPDX-License-Identifier: Apache-2.0

//! Cassandra implementation of `Bootstrapper`.

use async_trait::async_trait;
use cdrs_tokio::TryFromRow;
use cdrs_tokio::frame::TryFromRow as TryFromRowTrait;
use extenddb_storage::bootstrapper::{
    AdminBootstrapResult, BootstrapConfig, Bootstrapper,
    helpers::{
        check_conflict, extract_arg, generate_account_id, generate_encryption_key,
        generate_random_password, hash_password_async,
    },
};
use extenddb_storage::management_store::{OpError, OpResult};

use crate::config::CassandraStorageConfig;
use crate::engine::CassandraEngine;
use crate::migrations;

const CATALOG_VERSION: &str = "0.0.3";

// Helper structs for parsing Cassandra rows
#[derive(Debug, Clone, TryFromRow)]
struct TableNameRow {
    table_name: String,
}

#[derive(Debug, Clone, TryFromRow)]
struct KeyspaceNameRow {
    keyspace_name: String,
}

#[derive(Debug, Clone, TryFromRow)]
struct SettingRow {
    value: String,
}

/// Cassandra bootstrapper for init/destroy/migrate operations.
pub struct CassandraBootstrapper {
    engine: CassandraEngine,
    config: BootstrapConfig,
}

impl CassandraBootstrapper {
    /// Create a new bootstrapper from Cassandra config.
    pub async fn new(
        cassandra_config: &CassandraStorageConfig,
        bootstrap_config: BootstrapConfig,
    ) -> OpResult<Self> {
        let engine = CassandraEngine::new(cassandra_config, "us-east-1")
            .await
            .map_err(|e| OpError::Internal(format!("Create engine: {e}")))?;

        Ok(Self {
            engine,
            config: bootstrap_config,
        })
    }

    /// Create a bootstrapper from config file and CLI args. Parses
    /// Cassandra-specific arguments and merges with config.
    pub async fn from_config(
        config_path: &str,
        cli_args: &[String],
    ) -> Result<Self, extenddb_storage::error::StorageError> {
        use extenddb_storage::error::StorageError;

        // Extract Cassandra-specific CLI args
        let cassandra_contact_points = extract_arg(cli_args, "--cassandra-contact-points");
        let cassandra_user = extract_arg(cli_args, "--cassandra-user");
        let cassandra_pass = extract_arg(cli_args, "--cassandra-pass");
        let keyspace_prefix = extract_arg(cli_args, "--keyspace-prefix");
        let replication_factor = extract_arg(cli_args, "--replication-factor");
        let extenddb_user = extract_arg(cli_args, "--extenddb-user");
        let extenddb_pass = extract_arg(cli_args, "--extenddb-pass");

        // Load config file if it exists
        let (contact_points, user, password, prefix, rf_from_config) = if std::path::Path::new(
            config_path,
        )
        .exists()
        {
            println!("--- Loading defaults from {config_path}");

            // Parse Cassandra config from file
            let config_content = std::fs::read_to_string(config_path)
                .map_err(|e| StorageError::Internal(format!("Failed to read config: {e}")))?;
            let app_config: toml::Value = toml::from_str(&config_content)
                .map_err(|e| StorageError::Internal(format!("Failed to parse config: {e}")))?;

            let cassandra_config = app_config
                .get("storage")
                .and_then(|s| s.get("cassandra"))
                .ok_or_else(|| {
                    StorageError::Internal("Missing storage.cassandra section".into())
                })?;

            let config: CassandraStorageConfig =
                cassandra_config
                    .clone()
                    .try_into()
                    .map_err(|e: toml::de::Error| {
                        StorageError::Internal(format!("Invalid cassandra config: {e}"))
                    })?;

            // Check for conflicts between CLI args and config values
            if let Some(ref cli_cp) = cassandra_contact_points {
                let cli_list: Vec<&str> = cli_cp.split(',').collect();
                let config_list: Vec<&str> =
                    config.contact_points.iter().map(std::string::String::as_str).collect();
                if cli_list != config_list {
                    return Err(StorageError::Internal(format!(
                        "--cassandra-contact-points '{}' conflicts with config file contact points '{}'",
                        cli_cp,
                        config.contact_points.join(",")
                    )));
                }
            }

            check_conflict(
                extenddb_user.as_ref(),
                config.username.as_ref().unwrap_or(&"extenddb".to_string()),
                "--extenddb-user",
            )?;
            check_conflict(
                extenddb_pass.as_ref(),
                config
                    .password
                    .as_ref()
                    .unwrap_or(&"extenddb-local-dev".to_string()),
                "--extenddb-pass",
            )?;

            if let Some(ref cli_prefix) = keyspace_prefix
                && cli_prefix != &config.keyspace_prefix {
                    return Err(StorageError::Internal(format!(
                        "--keyspace-prefix '{}' conflicts with config file keyspace prefix '{}'",
                        cli_prefix, config.keyspace_prefix
                    )));
                }

            if let Some(ref cli_rf) = replication_factor {
                let cli_rf_val = cli_rf.parse::<u32>().map_err(|_| {
                    StorageError::Internal(format!(
                        "Invalid --replication-factor '{cli_rf}': must be a positive integer"
                    ))
                })?;
                if cli_rf_val != config.replication_factor {
                    return Err(StorageError::Internal(format!(
                        "--replication-factor {} conflicts with config file replication_factor {}",
                        cli_rf_val, config.replication_factor
                    )));
                }
            }

            (
                config.contact_points,
                config.username.unwrap_or_else(|| "extenddb".to_string()),
                config
                    .password
                    .unwrap_or_else(|| "extenddb-local-dev".to_string()),
                config.keyspace_prefix,
                config.replication_factor,
            )
        } else {
            // No config file - use defaults (single-node dev environment)
            (
                vec!["localhost:9042".to_string()],
                "extenddb".to_string(),
                "extenddb-local-dev".to_string(),
                "extenddb".to_string(),
                1, // RF=1 for single-node dev
            )
        };

        // CLI args override config (or use config values if no CLI arg provided)
        let resolved_contact_points = cassandra_contact_points
            .map_or(contact_points, |cp| cp.split(',').map(std::string::ToString::to_string).collect());
        let resolved_admin_user = cassandra_user
            .unwrap_or_else(|| std::env::var("USER").unwrap_or_else(|_| "cassandra".to_owned()));
        let resolved_keyspace_prefix = keyspace_prefix.unwrap_or(prefix);
        let resolved_replication_factor = replication_factor
            .map_or(rf_from_config, |rf| rf.parse::<u32>().unwrap_or(1));
        let resolved_app_user = extenddb_user.unwrap_or(user);
        let resolved_app_password = extenddb_pass.unwrap_or(password);

        // Extract host and port from first contact point
        let (host, port) = resolved_contact_points
            .first()
            .and_then(|cp| {
                let parts: Vec<&str> = cp.split(':').collect();
                if parts.len() == 2 {
                    Some((parts[0].to_string(), parts[1].parse().ok()?))
                } else {
                    None
                }
            })
            .unwrap_or_else(|| ("localhost".to_string(), 9042));

        let bootstrap_config = BootstrapConfig {
            host,
            port,
            admin_user: resolved_admin_user.clone(),
            admin_password: cassandra_pass.clone(),
            app_user: resolved_app_user,
            app_password: resolved_app_password,
            catalog_db: format!("{resolved_keyspace_prefix}_catalog"),
            data_db: String::new(), // Not used for Cassandra
        };

        // For bootstrap operations, connect as admin user
        let mut cassandra_config = CassandraStorageConfig {
            contact_points: resolved_contact_points,
            username: Some(resolved_admin_user),
            password: cassandra_pass,
            keyspace_prefix: resolved_keyspace_prefix,
            replication_factor: resolved_replication_factor,
            datacenter: "datacenter1".to_string(),
            max_connections: 10,
            cached_connection_string: None,
            instance_id: None,
        };
        cassandra_config.ensure_cached_connection_string();

        Self::new(&cassandra_config, bootstrap_config)
            .await
            .map_err(|e| StorageError::Internal(format!("{e:?}")))
    }
}

#[async_trait]
impl Bootstrapper for CassandraBootstrapper {
    async fn ensure_app_user(&self) -> OpResult<()> {
        let user = &self.config.app_user;
        let password = &self.config.app_password;

        println!("--- Ensuring application user '{user}' exists...");

        // Check if user exists
        let check_cql = "SELECT role FROM system_auth.roles WHERE role = ?";
        let exists = self
            .engine
            .session()
            .query_with_values(check_cql, cdrs_tokio::query_values!(user.as_str()))
            .await
            .ok()
            .and_then(|frame| frame.response_body().ok())
            .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
            .is_some_and(|rows| !rows.is_empty());

        if exists {
            println!("    User '{user}' already exists.");
            return Ok(());
        }

        // Validate password character set
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

        // Create user with password
        let create_cql = format!(
            "CREATE ROLE IF NOT EXISTS '{user}' WITH PASSWORD = '{password}' AND LOGIN = true"
        );
        self.engine
            .session()
            .query(create_cql)
            .await
            .map_err(|e| OpError::Internal(format!("Create user: {e}")))?;

        println!("    Created user '{user}'.");
        Ok(())
    }

    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        if self.config.admin_user == self.config.app_user {
            return Ok(());
        }

        // Check if admin already has the app role
        let check_cql =
            "SELECT role, member FROM system_auth.role_members WHERE role = ? AND member = ?";
        let already_granted = self
            .engine
            .session()
            .query_with_values(
                check_cql,
                cdrs_tokio::query_values!(
                    self.config.app_user.as_str(),
                    self.config.admin_user.as_str()
                ),
            )
            .await
            .ok()
            .and_then(|frame| frame.response_body().ok())
            .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
            .is_some_and(|rows| !rows.is_empty());

        if already_granted {
            return Ok(());
        }

        let grant_cql = format!(
            "GRANT '{}' TO '{}'",
            self.config.app_user, self.config.admin_user
        );
        self.engine.session().query(grant_cql).await.map_err(|e| {
            OpError::Internal(format!(
                "Cannot grant {} to {}: {}",
                self.config.app_user, self.config.admin_user, e
            ))
        })?;
        Ok(())
    }

    async fn create_catalog_db(&self) -> OpResult<()> {
        let keyspace = self.engine.catalog_keyspace();
        println!("--- Creating catalog keyspace '{keyspace}'...");

        if self
            .engine
            .keyspace_exists(&keyspace)
            .await
            .map_err(|e| OpError::Internal(e.to_string()))?
        {
            return Err(OpError::AlreadyExists(format!(
                "Catalog keyspace '{keyspace}' already exists. Run 'destroy' first, then re-run 'init'."
            )));
        }

        self.engine
            .create_keyspace(&keyspace)
            .await
            .map_err(|e| OpError::Internal(format!("Create catalog keyspace: {e}")))?;

        println!("    Created.");
        Ok(())
    }

    async fn create_data_db(&self) -> OpResult<()> {
        // Cassandra uses per-account keyspaces, not a single data keyspace
        println!("--- Cassandra uses per-account keyspaces (created on demand)");
        Ok(())
    }

    async fn run_catalog_migrations(&self) -> OpResult<()> {
        let keyspace = self.engine.catalog_keyspace();
        migrations::run_catalog_migrations(&self.engine.session_arc(), &keyspace).await
    }

    async fn run_data_migrations(&self) -> OpResult<()> {
        // Data schema is isolated per account, so upgrades must visit every
        // existing account keyspace. New accounts receive the same migration
        // list from ensure_account_keyspace/bootstrap_default_account.
        let query = format!(
            "SELECT account_id FROM {}.accounts",
            self.engine.catalog_keyspace()
        );
        let rows = self
            .engine
            .session()
            .query(query)
            .await
            .map_err(|e| OpError::Internal(format!("List accounts for migration: {e}")))?
            .response_body()
            .map_err(|e| OpError::Internal(format!("Parse accounts for migration: {e}")))?
            .into_rows()
            .unwrap_or_default();

        for row in rows {
            let account_id: String = crate::cassandra_util::get_column::<String, OpError>(
                &row,
                "account_id",
                "run_data_migrations",
            )?;
            let keyspace = self.engine.account_keyspace(&account_id);
            migrations::run_data_migrations(&self.engine.session_arc(), &keyspace).await?;
        }
        Ok(())
    }

    async fn pending_data_migrations(&self) -> OpResult<Vec<String>> {
        migrations::pending_data_migrations(
            &self.engine.session_arc(),
            &self.engine.catalog_keyspace(),
            |account_id| self.engine.account_keyspace(account_id),
        )
        .await
    }

    async fn record_data_connection(&self) -> OpResult<()> {
        // Not applicable for Cassandra (no separate data connection)
        Ok(())
    }

    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        let keyspace = self.engine.catalog_keyspace();
        println!("--- Generating AES-256-GCM encryption key...");

        // Check if key already exists
        let check_cql = format!(
            "SELECT value FROM {keyspace}.settings WHERE key = 'encryption_key'"
        );
        let exists = self
            .engine
            .session()
            .query(check_cql)
            .await
            .ok()
            .and_then(|frame| frame.response_body().ok())
            .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
            .is_some_and(|rows| !rows.is_empty());

        if exists {
            println!("--- Encryption key already exists, skipping.");
            return Ok(());
        }

        let key_b64 = generate_encryption_key();

        // Store key
        let insert_cql = format!(
            "INSERT INTO {keyspace}.settings (key, value) VALUES (?, ?)"
        );
        self.engine
            .session()
            .query_with_values(
                insert_cql,
                cdrs_tokio::query_values!("encryption_key", key_b64),
            )
            .await
            .map_err(|e| OpError::Internal(format!("Store encryption key: {e}")))?;

        println!("    Encryption key stored.");
        Ok(())
    }

    async fn bootstrap_default_account(&self) -> OpResult<()> {
        let keyspace = self.engine.catalog_keyspace();

        // Check if any accounts exist
        let check_cql = format!("SELECT account_id FROM {keyspace}.accounts LIMIT 1");
        let has_accounts = self
            .engine
            .session()
            .query(check_cql)
            .await
            .ok()
            .and_then(|frame| frame.response_body().ok())
            .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
            .is_some_and(|rows| !rows.is_empty());

        if has_accounts {
            println!("--- Default account already exists, skipping.");
            return Ok(());
        }

        // Create default account
        let account_id = generate_account_id();
        println!("--- Creating default account '{account_id}'...");
        let account_name = "default";

        let insert_cql = format!(
            "INSERT INTO {keyspace}.accounts (account_id, account_name, created_at) VALUES (?, ?, toTimestamp(now()))"
        );

        self.engine
            .session()
            .query_with_values(
                insert_cql,
                cdrs_tokio::query_values!(account_id.as_str(), account_name),
            )
            .await
            .map_err(|e| OpError::Internal(format!("Create account: {e}")))?;

        println!("    Account ID: {account_id}");

        // Create account-specific keyspace
        let account_keyspace = self.engine.account_keyspace(&account_id);
        println!("--- Creating account keyspace '{account_keyspace}'...");
        self.engine
            .create_keyspace(&account_keyspace)
            .await
            .map_err(|e| OpError::Internal(format!("Create account keyspace: {e}")))?;

        // Run data migrations in account keyspace
        println!("--- Running data migrations for account '{account_id}'...");
        migrations::run_data_migrations(&self.engine.session_arc(), &account_keyspace).await?;

        Ok(())
    }

    async fn bootstrap_admin_user(
        &self,
        env_user: Option<&str>,
        env_password: Option<&str>,
    ) -> OpResult<AdminBootstrapResult> {
        let keyspace = self.engine.catalog_keyspace();

        let username = env_user.unwrap_or("admin");
        let from_env = env_user.is_some() && env_password.is_some();

        // Check if user exists
        let check_cql = format!(
            "SELECT admin_name FROM {keyspace}.admin_users WHERE admin_name = ?"
        );
        let exists = self
            .engine
            .session()
            .query_with_values(check_cql, cdrs_tokio::query_values!(username))
            .await
            .ok()
            .and_then(|frame| frame.response_body().ok())
            .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
            .is_some_and(|rows| !rows.is_empty());

        if exists {
            println!("--- Admin user '{username}' already exists, skipping.");
            return Ok(AdminBootstrapResult {
                username: username.to_string(),
                generated_password: None,
                already_existed: true,
                from_env,
            });
        }

        println!("--- Creating admin user '{username}'...");

        // Generate or use provided password
        let password = if let Some(pw) = env_password {
            pw.to_string()
        } else {
            generate_random_password()
        };

        // Hash password (using bcrypt in blocking task to avoid blocking async runtime)
        let password_hash = hash_password_async(password.clone()).await?;

        // Insert admin user
        let insert_cql = format!(
            "INSERT INTO {keyspace}.admin_users (admin_name, password_hash) VALUES (?, ?)"
        );

        self.engine
            .session()
            .query_with_values(
                insert_cql,
                cdrs_tokio::query_values!(username, password_hash),
            )
            .await
            .map_err(|e| OpError::Internal(format!("Create admin user: {e}")))?;

        let generated_password = if from_env { None } else { Some(password) };

        Ok(AdminBootstrapResult {
            username: username.to_string(),
            generated_password,
            already_existed: false,
            from_env,
        })
    }

    async fn is_catalog_initialized(&self) -> OpResult<bool> {
        let keyspace = self.engine.catalog_keyspace();
        migrations::table_exists(&self.engine.session_arc(), &keyspace, "settings").await
    }

    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        let keyspace = self.engine.catalog_keyspace();

        let cql = format!(
            "SELECT table_name FROM {keyspace}.tables ORDER BY table_name"
        );
        let rows = match self.engine.session().query(cql).await {
            Ok(frame) => frame
                .response_body()
                .ok()
                .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
                .unwrap_or_default(),
            Err(_) => return Ok(Vec::new()),
        };

        let table_names: Vec<String> = rows
            .into_iter()
            .filter_map(|row| {
                TryFromRowTrait::try_from_row(row)
                    .ok()
                    .map(|r: TableNameRow| r.table_name)
            })
            .collect();

        Ok(table_names)
    }

    async fn get_data_db_name(&self) -> OpResult<Option<String>> {
        // Cassandra doesn't have a single data database
        Ok(None)
    }

    async fn drop_databases(&self, _data_db: &str) -> OpResult<()> {
        println!("--- Dropping all ExtendDB keyspaces...");

        // List all keyspaces with our prefix
        let prefix = format!(
            "{}_",
            self.engine.catalog_keyspace().trim_end_matches("_catalog")
        );

        let cql = "SELECT keyspace_name FROM system_schema.keyspaces";
        let rows = self
            .engine
            .session()
            .query(cql)
            .await
            .map_err(|e| OpError::Internal(format!("List keyspaces: {e}")))?
            .response_body()
            .map_err(|e| OpError::Internal(format!("Get response: {e}")))?
            .into_rows()
            .ok_or_else(|| OpError::Internal("No rows returned".to_string()))?;

        let keyspaces: Vec<String> = rows
            .into_iter()
            .filter_map(|row| {
                TryFromRowTrait::try_from_row(row)
                    .ok()
                    .map(|r: KeyspaceNameRow| r.keyspace_name)
            })
            .filter(|name| name.starts_with(&prefix))
            .collect();

        for keyspace in keyspaces {
            println!("    Dropping keyspace: {keyspace}");
            self.engine
                .drop_keyspace(&keyspace)
                .await
                .map_err(|e| OpError::Internal(e.to_string()))?;
        }

        println!("    All ExtendDB keyspaces dropped");
        Ok(())
    }

    async fn read_catalog_version(&self) -> OpResult<Option<String>> {
        let keyspace = self.engine.catalog_keyspace();

        if !migrations::table_exists(&self.engine.session_arc(), &keyspace, "settings").await? {
            return Ok(None);
        }

        let query = format!(
            "SELECT value FROM {keyspace}.settings WHERE key = 'catalog_version'"
        );
        let version = self
            .engine
            .session()
            .query(query)
            .await
            .ok()
            .and_then(|frame| frame.response_body().ok())
            .and_then(cdrs_tokio::frame::message_response::ResponseBody::into_rows)
            .and_then(|mut rows| rows.pop())
            .and_then(|row| TryFromRowTrait::try_from_row(row).ok())
            .map(|r: SettingRow| r.value);

        Ok(version)
    }

    fn expected_catalog_version(&self) -> String {
        CATALOG_VERSION.to_string()
    }

    fn catalog_database_name(&self) -> String {
        self.engine.catalog_keyspace()
    }

    fn endpoint_info(&self) -> String {
        self.config.host.clone() + ":" + &self.config.port.to_string()
    }

    fn catalog_connection_url(&self) -> String {
        format!("{}:{}", self.config.host, self.config.port)
    }

    fn generate_backend_config_section(&self) -> String {
        format!(
            r#"[storage.cassandra]
contact_points = ["{}"]
# username = "cassandra"          # Application user
# password = "cassandra-password" # Application password
keyspace_prefix = "extenddb"
replication_factor = 1           # Single node (use 3+ for production)
datacenter = "datacenter1"
max_connections = 10"#,
            self.catalog_connection_url()
        )
    }
}
