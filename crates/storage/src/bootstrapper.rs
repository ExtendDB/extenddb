// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bootstrapper storage trait for init/destroy/migrate operations.
//!
//! These operations are inherently backend-specific because database, schema,
//! and role DDL differs across storage engines. The trait abstracts the
//! high-level operations so the CLI commands don't depend on a specific
//! storage backend.

use async_trait::async_trait;

use crate::management_store::OpResult;

/// Connection parameters for bootstrap operations.
///
/// These are the raw parameters needed to connect to the storage backend
/// before any databases or schemas exist.
#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    pub host: String,
    pub port: u16,
    pub admin_user: String,
    pub admin_password: Option<String>,
    pub app_user: String,
    pub app_password: String,
    pub catalog_db: String,
    pub data_db: String,
}

/// CLI-provided bootstrap overrides after argument parsing.
///
/// Storage backends merge these typed values with their config-file defaults.
/// The CLI owns spelling, aliases, and `--flag=value` parsing; backend crates
/// should not inspect raw process arguments.
#[derive(Debug, Clone, Default)]
pub struct BootstrapOptions {
    pub storage_host: Option<String>,
    pub storage_port: Option<u16>,
    pub admin_user: Option<String>,
    pub admin_password: Option<String>,
    pub data_db: Option<String>,
    pub catalog_db: Option<String>,
    pub app_user: Option<String>,
    pub app_password: Option<String>,
}

/// Result of a bootstrap admin user creation.
#[derive(Debug)]
pub struct AdminBootstrapResult {
    /// The admin username that was created or already existed.
    pub username: String,
    /// The password, if a new one was generated (not returned for pre-existing
    /// users or environment-sourced credentials).
    pub generated_password: Option<String>,
    /// Whether the user already existed (skipped creation).
    pub already_existed: bool,
    /// Whether credentials came from environment variables.
    pub from_env: bool,
}

/// High-level bootstrap operations for storage backends.
///
/// Covers the init, destroy, and migrate command paths. Implementations
/// handle backend-specific DDL such as creating databases, schemas, users,
/// and grants.
#[async_trait]
pub trait Bootstrapper: Send + Sync {
    /// Ensure the application user exists in the storage backend.
    async fn ensure_app_user(&self) -> OpResult<()>;

    /// Grant the application role to the admin user (needed for managed
    /// databases like RDS/Aurora where the admin is not a superuser).
    async fn grant_app_role_to_admin(&self) -> OpResult<()>;

    /// Create the catalog database. Returns error if it already exists.
    async fn create_catalog_db(&self) -> OpResult<()>;

    /// Create the data database. Returns error if it already exists.
    async fn create_data_db(&self) -> OpResult<()>;

    /// Run catalog schema migrations (creates tables, indexes, etc.).
    async fn run_catalog_migrations(&self) -> OpResult<()>;

    /// Run data schema migrations (stream tables, sequences, etc.).
    async fn run_data_migrations(&self) -> OpResult<()>;

    /// Record the data database connection string in the catalog.
    async fn record_data_connection(&self) -> OpResult<()>;

    /// Generate and store an encryption key for secret storage.
    /// Idempotent — skips if already present.
    async fn bootstrap_encryption_key(&self) -> OpResult<()>;

    /// Create the default account. Idempotent — skips if any account exists.
    async fn bootstrap_default_account(&self) -> OpResult<()>;

    /// Create the initial admin user.
    async fn bootstrap_admin_user(
        &self,
        env_user: Option<&str>,
        env_password: Option<&str>,
    ) -> OpResult<AdminBootstrapResult>;

    /// Check if the catalog is already initialized (has schema).
    async fn is_catalog_initialized(&self) -> OpResult<bool>;

    /// List table names in the catalog (for destroy confirmation display).
    async fn list_table_names(&self) -> OpResult<Vec<String>>;

    /// Get the data database name from the catalog settings.
    async fn get_data_db_name(&self) -> OpResult<Option<String>>;

    /// Drop both catalog and data databases. Destructive and irreversible.
    async fn drop_databases(&self, data_db: &str) -> OpResult<()>;

    /// Read the current catalog schema version.
    async fn read_catalog_version(&self) -> OpResult<Option<String>>;

    /// Get the expected catalog version for this binary.
    fn expected_catalog_version(&self) -> String;

    /// Return the catalog database name for display.
    fn catalog_database_name(&self) -> String;

    /// Return endpoint information (host:port or contact points) for display.
    fn endpoint_info(&self) -> String;

    /// Return the catalog connection URL for config file generation.
    fn catalog_connection_url(&self) -> String;
}

use std::future::Future;
use std::pin::Pin;

use crate::error::StorageError;

/// Factory function type for creating backend-specific bootstrappers.
pub type BootstrapperFactory =
    fn(
        String,
        BootstrapOptions,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn Bootstrapper>, StorageError>> + Send>>;

/// Backend bootstrapper registration entry.
///
/// Backend crates submit instances of this struct using `inventory::submit!`
/// to register their bootstrappers at compile time.
pub struct BackendRegistration {
    pub name: &'static str,
    pub factory: BootstrapperFactory,
}

inventory::collect!(BackendRegistration);

/// Create a bootstrapper for the given backend.
///
/// Looks up the backend in the compile-time registry and calls its bootstrapper factory.
pub async fn create_bootstrapper(
    backend: &str,
    config_path: &str,
    options: BootstrapOptions,
) -> Result<Box<dyn Bootstrapper>, StorageError> {
    for registration in inventory::iter::<BackendRegistration> {
        if registration.name == backend {
            tracing::info!("Found registered backend: {}", backend);
            return (registration.factory)(config_path.to_string(), options.clone()).await;
        }
    }

    let available: Vec<&str> = inventory::iter::<BackendRegistration>()
        .map(|r| r.name)
        .collect();

    tracing::error!(
        "Unknown backend: {}. Available: {}",
        backend,
        available.join(", ")
    );

    Err(StorageError::Internal(format!(
        "Unknown backend: {backend}. Available backends: {}",
        available.join(", ")
    )))
}

/// List all registered backends.
pub fn list_backends() -> Vec<&'static str> {
    inventory::iter::<BackendRegistration>()
        .map(|r| r.name)
        .collect()
}
