// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Bootstrapper storage trait for init/destroy/migrate operations.
//!
//! These operations are inherently backend-specific (e.g., `CREATE DATABASE`
//! is `PostgreSQL` DDL). The trait abstracts the high-level operations so the
//! CLI commands don't depend on a specific storage backend.

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

/// Outcome of [`Bootstrapper::repair_lost_sort_key_definitions`].
#[derive(Debug, Default, Clone)]
pub struct KeyDefinitionRepair {
    /// Tables whose sort key definitions were restored, as
    /// `table_name: attribute (TYPE)` lines. When the caller passed `apply =
    /// false` these are the repairs that WOULD be made.
    pub repaired: Vec<String>,
    /// Tables that are damaged but could not be repaired automatically, because
    /// no physical sort key column was found to read the type back from. These
    /// need a human: guessing a type would resolve the key to the wrong column.
    pub needs_attention: Vec<String>,
}

/// High-level bootstrap operations for storage backends.
///
/// Covers the init, destroy, and migrate command paths. Implementations
/// handle backend-specific DDL (e.g., `CREATE DATABASE` for `PostgreSQL`).
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

    /// Restore sort key attribute definitions that issue #259 deleted from table
    /// metadata, reading each type back from the physical sort key column.
    ///
    /// Before the fix, `UpdateTable` replaced a table's stored attribute
    /// definitions with the request's subset, so adding a GSI dropped the base
    /// table's own key definitions and keyed reads silently returned another item
    /// in the same partition. The write path no longer does that, but a table
    /// damaged by an older build stays damaged, and it now fails its reads loudly
    /// instead (see `TableKeyInfo::validate_sort_key_definitions`). This repairs
    /// such tables so they serve correct reads again.
    ///
    /// With `apply` false this only detects and reports, writing nothing, so a
    /// `migrate` run that has not been confirmed with `--yes` never mutates the
    /// catalog. With `apply` true the repair is written.
    ///
    /// Idempotent: a table whose definitions are intact is left untouched, so
    /// running it on a healthy deployment reports nothing and changes nothing.
    ///
    /// The default implementation repairs nothing, which is correct for a backend
    /// that never wrote the definitions in the first place.
    async fn repair_lost_sort_key_definitions(&self, apply: bool) -> OpResult<KeyDefinitionRepair> {
        let _ = apply;
        Ok(KeyDefinitionRepair::default())
    }

    /// Acquire a cross-process lock so that concurrent `migrate` runs, such as
    /// several replicas starting at once, don't race each other applying schema
    /// changes. Blocks until the lock is available.
    ///
    /// The default is a no-op, which means a backend that does not override this
    /// gets no serialization at all. Implement it if concurrent bootstrap is a
    /// supported deployment for your backend.
    ///
    /// Implementations need not be re-entrant, and callers must not acquire
    /// twice without releasing. Every acquire must be paired with
    /// [`Bootstrapper::release_migration_lock`], and the backend must also
    /// release the lock if the process dies without doing so.
    async fn acquire_migration_lock(&self) -> OpResult<()> {
        Ok(())
    }

    /// Release the lock taken by [`Bootstrapper::acquire_migration_lock`], and
    /// do nothing if no lock is held. Defaults to a no-op.
    async fn release_migration_lock(&self) -> OpResult<()> {
        Ok(())
    }

    /// Filenames of data-database migrations that have not yet been applied.
    ///
    /// Excludes a pre-tracking baseline migration that already exists and will
    /// be adopted (recorded without re-running) rather than applied. Data
    /// migrations are tracked in the data database's own ledger, independent of
    /// the catalog version, so `migrate` uses this to report accurately and to
    /// decide whether confirmation is required — without running anything.
    async fn pending_data_migrations(&self) -> OpResult<Vec<String>>;

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

    /// True if the catalog was created by the pre-sqlx migration runner and
    /// therefore cannot be upgraded in place (ADR-0003 requires destroy + init).
    /// Backends without a legacy runner return false.
    async fn catalog_predates_sqlx(&self) -> OpResult<bool> {
        Ok(false)
    }

    /// Get the expected catalog version for this binary.
    fn expected_catalog_version(&self) -> String;

    /// Return the catalog database name for display.
    fn catalog_database_name(&self) -> String;

    /// Return endpoint information (host:port or contact points) for display.
    fn endpoint_info(&self) -> String;

    /// Return the catalog connection URL for config file generation.
    fn catalog_connection_url(&self) -> String;

    /// Generate the backend-specific configuration subsection for extenddb.toml.
    /// Returns only the [`storage.backend_name`] section content, not the [storage] header.
    fn generate_backend_config_section(&self) -> String;
}

use std::future::Future;
use std::pin::Pin;

use crate::error::StorageError;

/// Factory function type for creating backend-specific bootstrappers.
///
/// # Parameters
///
/// * `config_path` - Path and file name of the extenddb configuration file (e.g. "extenddb.toml")
/// * `cli_args` - Raw commandline arguments from `std::env::args().collect`
///
/// # Returns
///
/// A pinned future that resolves to either a boxed `Bootstrapper` or a `StorageError`.
pub type BootstrapperFactory =
    fn(
        String,
        Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn Bootstrapper>, StorageError>> + Send>>;

/// Create a bootstrapper using the installed backend.
///
/// Calls the bootstrapper factory of the [`Backend`](crate::Backend) installed
/// via [`set_backend`](crate::set_backend).
pub async fn create_bootstrapper(
    config_path: &str,
    cli_args: &[String],
) -> Result<Box<dyn Bootstrapper>, StorageError> {
    let backend = crate::backend::try_backend().ok_or_else(|| {
        StorageError::Internal("no storage backend installed (set_backend was not called)".into())
    })?;
    tracing::info!("Using compiled-in backend: {}", backend.name);
    (backend.bootstrapper)(config_path.to_string(), cli_args.to_vec()).await
}

/// Helper functions for bootstrapper implementations.
pub mod helpers {
    use crate::management_store::OpError;
    use crate::management_store::OpResult;

    /// Generate a random 12-digit numeric account ID (matches AWS account ID format).
    #[must_use]
    pub fn generate_account_id() -> String {
        use rand::Rng;
        let mut rng = rand::rng();
        let id: u64 = rng.random_range(100_000_000_000..1_000_000_000_000);
        id.to_string()
    }

    /// Generate a 24-character random password using alphanumeric characters only.
    ///
    /// Restricted to `[a-zA-Z0-9]` to avoid URL-encoding issues in form submissions,
    /// shell copy-paste problems, and other contexts where special characters break.
    /// At 24 characters from a 62-char alphabet, entropy is ~143 bits.
    #[must_use]
    pub fn generate_random_password() -> String {
        use rand::Rng;
        const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let mut rng = rand::rng();
        (0..24)
            .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
            .collect()
    }

    /// Hash a password using bcrypt in a blocking task.
    ///
    /// bcrypt is CPU-intensive and should not block the async runtime.
    /// This function spawns a blocking task to perform the hash operation.
    pub async fn hash_password_async(password: String) -> OpResult<String> {
        tokio::task::spawn_blocking(move || bcrypt::hash(password, bcrypt::DEFAULT_COST))
            .await
            .map_err(|e| OpError::Internal(format!("bcrypt hash task failed: {e}")))?
            .map_err(|e| OpError::Internal(format!("bcrypt hash failed: {e}")))
    }

    /// Generate a 256-bit AES-GCM encryption key and return it as base64.
    ///
    /// Uses `aes_gcm::Aes256Gcm::generate_key` with `OsRng` for cryptographically
    /// secure random key generation.
    pub fn generate_encryption_key() -> String {
        use aes_gcm::KeyInit;
        use base64::Engine;

        let key = aes_gcm::Aes256Gcm::generate_key(&mut aes_gcm::aead::OsRng);
        base64::engine::general_purpose::STANDARD.encode(key)
    }

    /// Check that a CLI arg, if provided, matches the config value.
    ///
    /// The error message includes both values, so this must only be used for
    /// non-sensitive settings (hosts, ports, usernames). For secrets, use
    /// [`check_conflict_redacted`].
    pub fn check_conflict<T: PartialEq + std::fmt::Display>(
        cli_val: Option<&T>,
        config_val: &T,
        flag: &str,
    ) -> Result<(), crate::error::StorageError> {
        if let Some(v) = cli_val
            && v != config_val
        {
            return Err(crate::error::StorageError::Internal(format!(
                "{flag} value '{v}' conflicts with config file value '{config_val}'"
            )));
        }
        Ok(())
    }

    /// Like [`check_conflict`], but the error message never contains either
    /// value. Use for passwords and other secrets: the message lands on
    /// stderr, and in a container stderr is the log stream, which is commonly
    /// shipped to aggregators.
    pub fn check_conflict_redacted<T: PartialEq>(
        cli_val: Option<&T>,
        config_val: &T,
        flag: &str,
    ) -> Result<(), crate::error::StorageError> {
        if let Some(v) = cli_val
            && v != config_val
        {
            return Err(crate::error::StorageError::Internal(format!(
                "{flag} conflicts with the value in the config file (values redacted)"
            )));
        }
        Ok(())
    }

    /// Extract a CLI argument value by flag name.
    #[must_use]
    pub fn extract_arg(args: &[String], flag: &str) -> Option<String> {
        args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_generate_account_id_format() {
            let id = generate_account_id();

            // Should be exactly 12 characters
            assert_eq!(id.len(), 12, "Account ID should be 12 characters");

            // Should be all digits
            assert!(
                id.chars().all(|c| c.is_ascii_digit()),
                "Account ID should contain only digits"
            );
        }

        #[test]
        fn test_generate_account_id_range() {
            let id = generate_account_id();
            let id_num: u64 = id.parse().expect("Account ID should be a valid number");

            // Should be in range [100000000000, 999999999999]
            assert!(
                id_num >= 100_000_000_000,
                "Account ID should be >= 100000000000"
            );
            assert!(
                id_num < 1_000_000_000_000,
                "Account ID should be < 1000000000000"
            );
        }

        #[test]
        fn test_generate_account_id_uniqueness() {
            // Generate multiple IDs and check they're different
            let ids: Vec<String> = (0..100).map(|_| generate_account_id()).collect();
            let unique_ids: std::collections::HashSet<_> = ids.iter().collect();

            // With 100 IDs from a space of 900 billion, collisions are astronomically unlikely
            assert_eq!(ids.len(), unique_ids.len(), "Account IDs should be unique");
        }

        #[test]
        fn test_generate_random_password_length() {
            let password = generate_random_password();
            assert_eq!(
                password.len(),
                24,
                "Password should be exactly 24 characters"
            );
        }

        #[test]
        fn test_generate_random_password_charset() {
            let password = generate_random_password();

            // Should only contain alphanumeric characters
            assert!(
                password.chars().all(|c| c.is_ascii_alphanumeric()),
                "Password should contain only alphanumeric characters"
            );
        }

        #[test]
        fn test_generate_random_password_uniqueness() {
            // Generate multiple passwords and check they're different
            let passwords: Vec<String> = (0..100).map(|_| generate_random_password()).collect();
            let unique_passwords: std::collections::HashSet<_> = passwords.iter().collect();

            // With 100 passwords from 62^24 possibilities, collisions are astronomically unlikely
            assert_eq!(
                passwords.len(),
                unique_passwords.len(),
                "Passwords should be unique"
            );
        }

        #[tokio::test]
        async fn test_hash_password_async_format() {
            let password = "test_password_123".to_string();
            let hash = hash_password_async(password)
                .await
                .expect("Hash should succeed");

            // bcrypt hashes start with $2b$ or $2a$ or $2y$ and are 60 characters
            assert!(
                hash.starts_with("$2"),
                "Hash should start with bcrypt prefix"
            );
            assert_eq!(hash.len(), 60, "bcrypt hash should be 60 characters");
        }

        #[tokio::test]
        async fn test_hash_password_async_different_salts() {
            let password = "same_password".to_string();
            let hash1 = hash_password_async(password.clone())
                .await
                .expect("Hash should succeed");
            let hash2 = hash_password_async(password)
                .await
                .expect("Hash should succeed");

            // Same password should produce different hashes due to different salts
            assert_ne!(
                hash1, hash2,
                "Same password should produce different hashes"
            );
        }

        #[test]
        fn test_generate_encryption_key_length() {
            let key_b64 = generate_encryption_key();

            // Decode to verify it's valid base64 and correct length
            use base64::Engine;
            let key_bytes = base64::engine::general_purpose::STANDARD
                .decode(&key_b64)
                .expect("Key should be valid base64");

            assert_eq!(
                key_bytes.len(),
                32,
                "Encryption key should be 32 bytes (256 bits)"
            );
        }

        #[test]
        fn test_generate_encryption_key_uniqueness() {
            let key1 = generate_encryption_key();
            let key2 = generate_encryption_key();

            // Multiple calls should produce different keys
            assert_ne!(key1, key2, "Encryption keys should be unique");
        }

        #[test]
        fn test_check_conflict_no_cli_arg() {
            let result = check_conflict(None, &"config_value", "--test-flag");
            assert!(result.is_ok(), "No conflict when CLI arg is None");
        }

        #[test]
        fn test_check_conflict_matching_values() {
            let cli_value = "same_value".to_string();
            let config_value = "same_value".to_string();
            let result = check_conflict(Some(&cli_value), &config_value, "--test-flag");
            assert!(result.is_ok(), "No conflict when values match");
        }

        #[test]
        fn test_check_conflict_different_values() {
            let cli_value = "cli_value".to_string();
            let config_value = "config_value".to_string();
            let result = check_conflict(Some(&cli_value), &config_value, "--test-flag");

            assert!(result.is_err(), "Should error on conflict");
            let err = result.unwrap_err();
            assert!(err.to_string().contains("--test-flag"));
            assert!(err.to_string().contains("cli_value"));
            assert!(err.to_string().contains("config_value"));
        }

        #[test]
        fn test_check_conflict_with_numbers() {
            let cli_port: u16 = 5432;
            let config_port: u16 = 5432;
            let result = check_conflict(Some(&cli_port), &config_port, "--port");
            assert!(result.is_ok(), "No conflict when numeric values match");

            let different_port: u16 = 9042;
            let result = check_conflict(Some(&different_port), &config_port, "--port");
            assert!(result.is_err(), "Should error when numeric values differ");
        }

        #[test]
        fn test_check_conflict_redacted_never_leaks_either_value() {
            let cli_secret = "hunter2-cli".to_string();
            let config_secret = "hunter2-config".to_string();
            let result =
                check_conflict_redacted(Some(&cli_secret), &config_secret, "--extenddb-pass");
            let err = result.unwrap_err().to_string();
            assert!(err.contains("--extenddb-pass"), "flag must be named: {err}");
            assert!(!err.contains("hunter2-cli"), "CLI secret leaked: {err}");
            assert!(
                !err.contains("hunter2-config"),
                "config secret leaked: {err}"
            );
        }

        #[test]
        fn test_check_conflict_redacted_ok_paths() {
            let same = "s3cret".to_string();
            assert!(check_conflict_redacted(Some(&same), &same, "--extenddb-pass").is_ok());
            assert!(check_conflict_redacted(None, &same, "--extenddb-pass").is_ok());
        }

        #[test]
        fn test_extract_arg_found() {
            let args = vec![
                "program".to_string(),
                "--host".to_string(),
                "localhost".to_string(),
                "--port".to_string(),
                "5432".to_string(),
            ];

            assert_eq!(extract_arg(&args, "--host"), Some("localhost".to_string()));
            assert_eq!(extract_arg(&args, "--port"), Some("5432".to_string()));
        }

        #[test]
        fn test_extract_arg_not_found() {
            let args = vec![
                "program".to_string(),
                "--host".to_string(),
                "localhost".to_string(),
            ];

            assert_eq!(extract_arg(&args, "--port"), None);
            assert_eq!(extract_arg(&args, "--missing"), None);
        }

        #[test]
        fn test_extract_arg_empty_args() {
            let args: Vec<String> = vec![];
            assert_eq!(extract_arg(&args, "--host"), None);
        }

        #[test]
        fn test_extract_arg_flag_at_end() {
            let args = vec!["program".to_string(), "--host".to_string()];

            // Flag at end with no value should return None
            assert_eq!(extract_arg(&args, "--host"), None);
        }

        #[test]
        fn test_extract_arg_multiple_occurrences() {
            let args = vec![
                "program".to_string(),
                "--host".to_string(),
                "first".to_string(),
                "--host".to_string(),
                "second".to_string(),
            ];

            // Should return first occurrence
            assert_eq!(extract_arg(&args, "--host"), Some("first".to_string()));
        }
    }
}

/// Compile-time and behaviour guard for out-of-tree backends.
///
/// `MinimalBootstrapper` implements only the methods the trait requires. If a
/// new required method is added, or a defaulted one loses its default, this
/// stops compiling, which is the same breakage an out-of-tree backend crate
/// would hit. It also pins object safety by boxing as `dyn Bootstrapper`, and
/// checks that the defaulted migration-lock methods are usable no-ops.
#[cfg(test)]
mod out_of_tree_compat_tests {
    use super::*;
    use crate::management_store::OpResult;

    struct MinimalBootstrapper;

    #[async_trait]
    impl Bootstrapper for MinimalBootstrapper {
        async fn ensure_app_user(&self) -> OpResult<()> {
            Ok(())
        }
        async fn grant_app_role_to_admin(&self) -> OpResult<()> {
            Ok(())
        }
        async fn create_catalog_db(&self) -> OpResult<()> {
            Ok(())
        }
        async fn create_data_db(&self) -> OpResult<()> {
            Ok(())
        }
        async fn run_catalog_migrations(&self) -> OpResult<()> {
            Ok(())
        }
        async fn run_data_migrations(&self) -> OpResult<()> {
            Ok(())
        }
        async fn pending_data_migrations(&self) -> OpResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn record_data_connection(&self) -> OpResult<()> {
            Ok(())
        }
        async fn bootstrap_encryption_key(&self) -> OpResult<()> {
            Ok(())
        }
        async fn bootstrap_default_account(&self) -> OpResult<()> {
            Ok(())
        }
        async fn bootstrap_admin_user(
            &self,
            env_user: Option<&str>,
            _env_password: Option<&str>,
        ) -> OpResult<AdminBootstrapResult> {
            Ok(AdminBootstrapResult {
                username: env_user.unwrap_or("admin").to_owned(),
                generated_password: None,
                already_existed: false,
                from_env: false,
            })
        }
        async fn is_catalog_initialized(&self) -> OpResult<bool> {
            Ok(true)
        }
        async fn list_table_names(&self) -> OpResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn get_data_db_name(&self) -> OpResult<Option<String>> {
            Ok(None)
        }
        async fn drop_databases(&self, _data_db: &str) -> OpResult<()> {
            Ok(())
        }
        async fn read_catalog_version(&self) -> OpResult<Option<String>> {
            Ok(None)
        }
        fn expected_catalog_version(&self) -> String {
            "0.0.0".to_owned()
        }
        fn catalog_database_name(&self) -> String {
            "minimal".to_owned()
        }
        fn endpoint_info(&self) -> String {
            "in-memory".to_owned()
        }
        fn catalog_connection_url(&self) -> String {
            "minimal://".to_owned()
        }
        fn generate_backend_config_section(&self) -> String {
            String::new()
        }
    }

    /// A backend that does not implement the migration lock still works, and
    /// the defaults are no-ops rather than errors.
    #[tokio::test]
    async fn migration_lock_defaults_to_a_usable_no_op() {
        let bootstrapper: Box<dyn Bootstrapper> = Box::new(MinimalBootstrapper);
        assert!(bootstrapper.acquire_migration_lock().await.is_ok());
        assert!(bootstrapper.release_migration_lock().await.is_ok());
        // Releasing without acquiring must also be harmless.
        assert!(bootstrapper.release_migration_lock().await.is_ok());
    }
}
