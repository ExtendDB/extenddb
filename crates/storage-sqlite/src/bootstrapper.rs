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

use crate::serve_lock::ServeLock;
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

    /// Take the exclusive lock a running `extenddb serve` holds on this
    /// database, or explain which process holds it. `Ok(None)` for an
    /// in-memory database.
    fn exclusive_lock(&self, what: &str) -> OpResult<Option<ServeLock>> {
        // The lock file lives next to the database; `init` may be the one
        // creating that directory (see `pool`).
        if !self.is_memory()
            && let Some(parent) = std::path::Path::new(&self.path).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| {
                OpError::Internal(format!(
                    "create parent directory for SQLite database '{}': {e}",
                    self.path
                ))
            })?;
        }
        ServeLock::acquire_for_location(&self.path).map_err(|e| {
            OpError::Internal(format!(
                "{what} needs exclusive use of the SQLite database and cannot have it: {e}"
            ))
        })
    }

    /// Build a `SqliteBootstrapper` from the config file and CLI args.
    ///
    /// Resolution order for the database path (bootstrapper commands: init,
    /// destroy, migrate): the `--sqlite-path <p>` CLI flag (declared on
    /// `InitArgs`), then `[storage.sqlite].path` in the config file, then the
    /// default `extenddb.sqlite`. `serve` does not use this path; it reads
    /// the config (with `EXTENDDB__STORAGE__SQLITE__PATH` overriding).
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
    /// For a real filesystem path, the parent directory is created first:
    /// `init --sqlite-path` may point into a directory that does not exist
    /// yet, and bootstrapping it is exactly init's job.
    async fn pool(&self) -> OpResult<SqlitePool> {
        if self.path != ":memory:"
            && let Some(parent) = std::path::Path::new(&self.path).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| {
                OpError::Internal(format!(
                    "create parent directory for SQLite database '{}': {e}",
                    self.path
                ))
            })?;
        }
        SqlitePoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _| {
                Box::pin(
                    async move { crate::sqlite_util::set_connection_pragmas(conn, true).await },
                )
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

    /// Create the database file, empty and with mode 0600, and its directory.
    /// An existing file is refused so `init` is not silently destructive.
    /// The commands' migration lock opens only an existing file, so the file
    /// exists before `init` takes it.
    async fn create_catalog_db(&self) -> OpResult<()> {
        if self.is_memory() {
            return Ok(());
        }
        let exists = || {
            OpError::AlreadyExists(format!(
                "SQLite database '{}' already exists. Run 'destroy' first, then 'init'.",
                self.path
            ))
        };
        let path = std::path::Path::new(&self.path);
        if path.exists() {
            return Err(exists());
        }
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| {
                OpError::Internal(format!(
                    "create parent directory for SQLite database '{}': {e}",
                    self.path
                ))
            })?;
        }
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(path) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(exists()),
            Err(e) => Err(OpError::Internal(format!(
                "create SQLite database '{}': {e}",
                self.path
            ))),
        }
    }

    async fn create_data_db(&self) -> OpResult<()> {
        Ok(()) // Catalog and data share one file.
    }

    async fn run_catalog_migrations(&self) -> OpResult<()> {
        Err(through_the_runner("run_catalog_migrations"))
    }

    async fn run_data_migrations(&self) -> OpResult<()> {
        Err(through_the_runner("run_data_migrations"))
    }

    async fn pending_data_migrations(&self) -> OpResult<Vec<String>> {
        Err(through_the_runner("pending_data_migrations"))
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
        Err(through_the_runner("is_catalog_initialized"))
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
        // A server that still has the file open would keep serving an unlinked
        // database, and a later `init` would create a second one at the same
        // path. Refuse while any other process holds the file, and hold it
        // ourselves until the files are gone.
        let _lock = self.exclusive_lock("destroy")?;
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
        Err(through_the_runner("read_catalog_version"))
    }

    fn expected_catalog_version(&self) -> String {
        extenddb_storage::migrations::MigrationId::LATEST
            .number()
            .to_string()
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

/// The error of a bootstrapper method the commands do not call on this
/// backend: its catalog migrates through
/// [`crate::migrations::SqliteCatalogMigrations`] and the shared runner.
fn through_the_runner(method: &str) -> OpError {
    OpError::Internal(format!(
        "sqlite migrates through its CatalogMigrations; the bootstrapper method {method} is \
         not used"
    ))
}

#[cfg(all(test, unix))]
mod lock_tests {
    use super::SqliteBootstrapper;
    use crate::serve_lock::ServeLock;
    use extenddb_storage::bootstrapper::Bootstrapper;

    /// `destroy` is refused while a server holds the database, and runs once
    /// the server has gone.
    #[tokio::test]
    async fn destroy_is_refused_while_a_server_holds_the_file() {
        let dir = std::env::temp_dir().join(format!(
            "extenddb-bootstrap-lock-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).expect("dir");
        let db = dir.join("db.sqlite");
        std::fs::write(&db, b"").expect("db file");
        let bootstrap = SqliteBootstrapper::new(db.to_string_lossy().into_owned());

        let server = ServeLock::acquire(&db).expect("server lock");
        let err = bootstrap
            .drop_databases("unused")
            .await
            .expect_err("destroy under a server");
        assert!(
            format!("{err:?}").contains("another extenddb process"),
            "{err:?}"
        );
        assert!(
            db.exists(),
            "destroy must not unlink a file a server has open"
        );
        drop(server);

        bootstrap.drop_databases("unused").await.expect("destroy");
        assert!(!db.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
