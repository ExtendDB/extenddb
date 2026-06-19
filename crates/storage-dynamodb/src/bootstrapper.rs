// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! DynamoDB backend implementation of `Bootstrapper`.
//!
//! The DynamoDB backend stores its metadata (catalog, IAM, settings) in a
//! Postgres database, exactly like `extenddb-storage-postgres`. All catalog
//! bootstrap operations are therefore delegated to an inner
//! `PostgresBootstrapper`. DynamoDB-specific "data" bootstrapping is a no-op
//! because DynamoDB tables are created lazily by the CreateTable API — there
//! is no SQL `CREATE DATABASE` equivalent to issue at init time.

use async_trait::async_trait;
use extenddb_storage::bootstrapper::{AdminBootstrapResult, BootstrapConfig, Bootstrapper};
use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::OpResult;
use extenddb_storage_postgres::PostgresBootstrapper;

use crate::config::DynamoStorageConfig;

/// DynamoDB-at-home bootstrapper.
///
/// Delegates all catalog operations (user provisioning, migrations, encryption
/// key, admin user, etc.) to an inner `PostgresBootstrapper`. DynamoDB data
/// operations that have no DDL analogue are no-ops.
pub struct DynamoBootstrapper {
    /// Delegates catalog/IAM bootstrap to Postgres.
    inner: PostgresBootstrapper,
    /// Stored for `endpoint_info()` display.
    dynamo_config: DynamoStorageConfig,
}

impl DynamoBootstrapper {
    /// Build a `DynamoBootstrapper` from the config file at `config_path`.
    ///
    /// Reads `storage.dynamodb` from the TOML file, parses it into a
    /// `DynamoStorageConfig`, then builds an inner `PostgresBootstrapper`
    /// from `catalog_connection_string`.
    pub async fn from_config(
        config_path: &str,
        _cli_args: &[String],
    ) -> Result<Self, StorageError> {
        // Read and parse the TOML config file.
        let config_content = std::fs::read_to_string(config_path)
            .map_err(|e| StorageError::Internal(format!("Failed to read config file: {e}")))?;
        let app_config: toml::Value = toml::from_str(&config_content)
            .map_err(|e| StorageError::Internal(format!("Failed to parse config file: {e}")))?;

        let dynamo_table = app_config
            .get("storage")
            .and_then(|s| s.get("dynamodb"))
            .and_then(|d| d.as_table())
            .ok_or_else(|| {
                StorageError::Internal("Missing [storage.dynamodb] section in config".into())
            })?;

        let dynamo_config = DynamoStorageConfig::from_table(dynamo_table).map_err(|e| {
            StorageError::Internal(format!("Invalid [storage.dynamodb] config: {e}"))
        })?;

        let inner =
            Self::build_inner_bootstrapper(&dynamo_config.catalog_connection_string).await?;

        Ok(Self {
            inner,
            dynamo_config,
        })
    }

    /// Build an inner `PostgresBootstrapper` from a Postgres connection string.
    ///
    /// The `BootstrapConfig` is constructed with `admin_user = app_user` because
    /// the connection string encodes the app credentials. This mirrors how the
    /// Postgres bootstrapper handles connection strings that already carry
    /// app-level credentials.
    async fn build_inner_bootstrapper(
        catalog_conn: &str,
    ) -> Result<PostgresBootstrapper, StorageError> {
        let parts =
            extenddb_storage_postgres::parse_connection_string(catalog_conn).map_err(|e| {
                StorageError::Internal(format!("invalid catalog connection string: {e}"))
            })?;

        // Derive the data_db name: strip the `_catalog` suffix if present.
        let data_db = parts
            .database
            .strip_suffix("_catalog")
            .unwrap_or(&parts.database)
            .to_owned();

        let bc = BootstrapConfig {
            host: parts.host,
            port: parts.port,
            // Use the app credentials as admin credentials too. For DynamoDB
            // deployments the Postgres instance is typically the catalog-only
            // sidecar, and the connection string already carries sufficient
            // privileges for DDL.
            admin_user: parts.user.clone(),
            admin_password: Some(parts.password.clone()),
            app_user: parts.user,
            app_password: parts.password,
            catalog_db: parts.database.clone(),
            data_db,
        };

        PostgresBootstrapper::connect(bc)
            .await
            .map_err(|e| StorageError::Internal(format!("Failed to connect to catalog: {e:?}")))
    }
}

#[async_trait]
impl Bootstrapper for DynamoBootstrapper {
    // ── Delegated to inner PostgresBootstrapper ──────────────────────────

    async fn ensure_app_user(&self) -> OpResult<()> {
        self.inner.ensure_app_user().await
    }

    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        self.inner.grant_app_role_to_admin().await
    }

    async fn create_catalog_db(&self) -> OpResult<()> {
        self.inner.create_catalog_db().await
    }

    async fn run_catalog_migrations(&self) -> OpResult<()> {
        self.inner.run_catalog_migrations().await
    }

    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        self.inner.bootstrap_encryption_key().await
    }

    async fn bootstrap_default_account(&self) -> OpResult<()> {
        self.inner.bootstrap_default_account().await
    }

    async fn bootstrap_admin_user(
        &self,
        env_user: Option<&str>,
        env_password: Option<&str>,
    ) -> OpResult<AdminBootstrapResult> {
        self.inner
            .bootstrap_admin_user(env_user, env_password)
            .await
    }

    async fn is_catalog_initialized(&self) -> OpResult<bool> {
        self.inner.is_catalog_initialized().await
    }

    async fn read_catalog_version(&self) -> OpResult<Option<String>> {
        self.inner.read_catalog_version().await
    }

    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        self.inner.list_table_names().await
    }

    async fn drop_databases(&self, data_db: &str) -> OpResult<()> {
        self.inner.drop_databases(data_db).await
    }

    async fn get_data_db_name(&self) -> OpResult<Option<String>> {
        self.inner.get_data_db_name().await
    }

    fn expected_catalog_version(&self) -> String {
        self.inner.expected_catalog_version()
    }

    fn catalog_database_name(&self) -> String {
        self.inner.catalog_database_name()
    }

    fn catalog_connection_url(&self) -> String {
        self.inner.catalog_connection_url()
    }

    // ── DynamoDB data no-ops ─────────────────────────────────────────────

    /// DynamoDB has no `CREATE DATABASE` equivalent.
    ///
    /// Tables are created lazily via the CreateTable API when the user issues
    /// their first `CreateTable` request. Nothing to provision at init time.
    async fn create_data_db(&self) -> OpResult<()> {
        println!("--- [DynamoDB] create_data_db: skipped (tables created lazily via CreateTable)");
        Ok(())
    }

    /// DynamoDB has no SQL schema migrations.
    ///
    /// The data plane speaks DynamoDB's native wire protocol; there are no
    /// `CREATE TABLE` statements to run against DynamoDB itself.
    async fn run_data_migrations(&self) -> OpResult<()> {
        println!("--- [DynamoDB] run_data_migrations: skipped (no SQL data schema)");
        Ok(())
    }

    /// Record the data connection in the catalog.
    ///
    /// Delegated to the inner Postgres bootstrapper, which writes a
    /// `data_database_connection_string` entry into the catalog `settings`
    /// table. For DynamoDB this records the Postgres catalog URL (there is no
    /// separate DynamoDB connection string to store), which keeps the catalog
    /// coherent for callers that read `get_data_db_name()`.
    async fn record_data_connection(&self) -> OpResult<()> {
        self.inner.record_data_connection().await
    }

    // ── Display ──────────────────────────────────────────────────────────

    /// Return endpoint information combining the DynamoDB endpoint/region and
    /// the inner Postgres catalog endpoint.
    fn endpoint_info(&self) -> String {
        let dynamo_endpoint = self
            .dynamo_config
            .endpoint_url
            .as_deref()
            .unwrap_or("aws-dynamodb");
        let region = &self.dynamo_config.region;
        let catalog = self.inner.endpoint_info();
        format!("dynamodb endpoint: {dynamo_endpoint} (region {region}), catalog: {catalog}")
    }
}
