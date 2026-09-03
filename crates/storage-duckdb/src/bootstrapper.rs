// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `Bootstrapper` implementation for the DuckDB backend.
//!
//! DuckDB has no server, users, or roles, and catalog and data live in one
//! file. Initialization therefore reduces to: create the file, apply the
//! catalog schema, and seed the encryption key, default account, and admin
//! user. Destruction removes the database file and its WAL/SHM sidecars.

use crate::db;
use async_trait::async_trait;
use extenddb_core::types::{AttributeDefinition, KeySchemaElement, KeyType};
use extenddb_storage::bootstrapper::{
    AdminBootstrapResult, Bootstrapper, KeyDefinitionRepair, helpers,
};
use extenddb_storage::error::StorageError;
use extenddb_storage::management_store::{OpError, OpResult};
use extenddb_storage::util::recover_sort_key_definitions;

use crate::duckdb_util::duckdb_path;
use crate::schema::{self, CATALOG_VERSION};

/// DuckDB backend bootstrapper.
///
/// Holds the database file path. Each operation opens a short-lived
/// single-connection pool, since `init`/`destroy`/`migrate` are one-shot CLI
/// paths, not the hot serving path.
pub struct DuckDbBootstrapper {
    path: String,
}

impl DuckDbBootstrapper {
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }

    /// Build a `DuckDbBootstrapper` from the config file and CLI args.
    ///
    /// Resolution order for the database path (bootstrapper commands: init,
    /// destroy, migrate): the `--duckdb-path <p>` CLI flag (declared on
    /// `InitArgs`), then `[storage.duckdb].path` in the config file, then the
    /// default `extenddb.duckdb`. `serve` does not use this path; it reads
    /// the config (with `EXTENDDB__STORAGE__SQLITE__PATH` overriding).
    pub async fn from_config(config_path: &str, cli_args: &[String]) -> Result<Self, StorageError> {
        if let Some(p) = helpers::extract_arg(cli_args, "--duckdb-path") {
            return Ok(Self::new(p));
        }

        let path = if std::path::Path::new(config_path).exists() {
            let content = std::fs::read_to_string(config_path)
                .map_err(|e| StorageError::Internal(format!("read config {config_path}: {e}")))?;
            let parsed: toml::Value = toml::from_str(&content)
                .map_err(|e| StorageError::Internal(format!("parse config {config_path}: {e}")))?;
            parsed
                .get("storage")
                .and_then(|s| s.get("duckdb"))
                .and_then(|s| s.get("path"))
                .and_then(|v| v.as_str())
                .unwrap_or("extenddb.duckdb")
                .to_owned()
        } else {
            "extenddb.duckdb".to_owned()
        };

        Ok(Self::new(path))
    }

    fn is_memory(&self) -> bool {
        self.path == ":memory:" || self.path.starts_with("file::memory:")
    }

    /// Open a short-lived single-connection pool with the standard PRAGMAs.
    /// For a real filesystem path, the parent directory is created first:
    /// `init --duckdb-path` may point into a directory that does not exist
    /// yet, and bootstrapping it is exactly init's job.
    async fn pool(&self) -> OpResult<db::Pool> {
        if self.path != ":memory:"
            && let Some(parent) = std::path::Path::new(&self.path).parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| {
                OpError::Internal(format!(
                    "create parent directory for DuckDB database '{}': {e}",
                    self.path
                ))
            })?;
        }
        let pool = db::Pool::open(&duckdb_path(&self.path), 1)
            .await
            .map_err(|e| OpError::Internal(format!("open DuckDB database '{}': {e}", self.path)))?;
        // The database stores the encryption key next to the secrets it
        // protects; keep the file and its WAL owner-only from the moment they
        // exist rather than waiting for the first `serve`.
        #[cfg(unix)]
        if !self.is_memory() {
            use std::os::unix::fs::PermissionsExt;
            for suffix in ["", ".wal"] {
                let f = format!("{}{suffix}", self.path);
                if std::path::Path::new(&f).exists() {
                    let _ = std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600));
                }
            }
        }
        Ok(pool)
    }
}

#[async_trait]
impl Bootstrapper for DuckDbBootstrapper {
    async fn ensure_app_user(&self) -> OpResult<()> {
        Ok(()) // DuckDB has no user concept.
    }

    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        Ok(()) // DuckDB has no role concept.
    }

    async fn create_catalog_db(&self) -> OpResult<()> {
        // Connecting with mode=rwc creates the file; refuse to clobber an
        // existing database so `init` is not silently destructive.
        if !self.is_memory() && std::path::Path::new(&self.path).exists() {
            return Err(OpError::AlreadyExists(format!(
                "DuckDB database '{}' already exists. Run 'destroy' first, then 'init'.",
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
        // DuckDB applies its complete schema in run_catalog_migrations (a single
        // file), so there are no separately-tracked data migrations that can be
        // pending.
        Ok(Vec::new())
    }

    async fn record_data_connection(&self) -> OpResult<()> {
        let pool = self.pool().await?;
        db::query(
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
        let exists: bool =
            db::query_scalar("SELECT EXISTS(SELECT 1 FROM settings WHERE key = 'encryption_key')")
                .fetch_one(&pool)
                .await
                .map_err(|e| OpError::Internal(format!("check encryption key: {e}")))?;
        if exists {
            return Ok(());
        }
        let key = helpers::generate_encryption_key();
        db::query("INSERT OR IGNORE INTO settings (key, value) VALUES ('encryption_key', ?)")
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
        let account_id: String = match db::query_scalar(
            "SELECT account_id FROM accounts ORDER BY account_id LIMIT 1",
        )
        .fetch_optional(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("check accounts: {e}")))?
        {
            Some(id) => id,
            None => {
                let id = helpers::generate_account_id();
                db::query(
                    "INSERT OR IGNORE INTO accounts (account_id, account_name) VALUES (?, 'default')",
                )
                .bind(&id)
                .execute(&pool)
                .await
                .map_err(|e| OpError::Internal(format!("create default account: {e}")))?;
                id
            }
        };
        db::query("INSERT OR IGNORE INTO settings (key, value) VALUES ('default_account_id', ?)")
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
            db::query_scalar("SELECT EXISTS(SELECT 1 FROM admin_users WHERE admin_name = ?)")
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

        db::query("INSERT INTO admin_users (admin_name, password_hash) VALUES (?, ?)")
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

    /// Repair table metadata damaged by the pre-fix `UpdateTable` (#259).
    ///
    /// DuckDB keeps the catalog and the data tables in one file, so the physical
    /// sort key columns are read with `PRAGMA table_info`. Otherwise identical to
    /// the PostgreSQL implementation: for any base sort key with no attribute
    /// definition, recover the type from its column name.
    async fn repair_lost_sort_key_definitions(&self, apply: bool) -> OpResult<KeyDefinitionRepair> {
        let mut report = KeyDefinitionRepair::default();
        let Ok(pool) = self.pool().await else {
            return Ok(report);
        };
        // A never-initialised catalog has no `tables` table, and `pool()` opens
        // with mode=rwc, which silently creates an empty database file, so the
        // SELECT below would hard-error rather than find nothing. PostgreSQL
        // degrades gracefully through its get_data_db_name() guard; this is the
        // DuckDB equivalent.
        if !schema::table_exists(&pool, "tables").await? {
            return Ok(report);
        }

        let rows: Vec<(String, String, String, String, String)> = db::query_as(
            "SELECT account_id, table_name, table_id, key_schema, attribute_definitions \
             FROM tables ORDER BY table_name",
        )
        .fetch_all(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("Cannot read tables: {e}")))?;

        for (account_id, table_name, table_id, ks_json, ad_json) in rows {
            let key_schema: Vec<KeySchemaElement> = match serde_json::from_str(&ks_json) {
                Ok(v) => v,
                Err(e) => {
                    report
                        .needs_attention
                        .push(format!("{table_name}: unreadable key_schema ({e})"));
                    continue;
                }
            };
            let attr_defs: Vec<AttributeDefinition> =
                serde_json::from_str(&ad_json).unwrap_or_default();

            // Reported for every table on every run, not only when a sort key is
            // repaired: the partition key definition is dropped by the same write and
            // is not recoverable from the schema, because the pk column is always TEXT.
            // It is not needed for correctness either, since partition key values are
            // encoded from the key schema alone, so it is reported rather than guessed
            // and keeps being reported until a human restores it.
            let pk_missing: Vec<&str> = key_schema
                .iter()
                .filter(|ks| ks.key_type == KeyType::Hash)
                .filter(|ks| {
                    !attr_defs
                        .iter()
                        .any(|ad| ad.attribute_name == ks.attribute_name)
                })
                .map(|ks| ks.attribute_name.as_str())
                .collect();
            if !pk_missing.is_empty() {
                report.needs_attention.push(format!(
                    "{table_name}: partition key definition(s) [{}] are absent; reads and \
                     writes are unaffected, but index key type validation cannot check them",
                    pk_missing.join(", ")
                ));
            }

            let missing: Vec<&KeySchemaElement> = key_schema
                .iter()
                .filter(|ks| ks.key_type == KeyType::Range)
                .filter(|ks| {
                    !attr_defs
                        .iter()
                        .any(|ad| ad.attribute_name == ks.attribute_name)
                })
                .collect();
            if missing.is_empty() {
                continue;
            }

            // PRIMARY KEY columns only, in key order. Every data table carries all
            // three typed columns for each sort key position (sk_s, sk_n, sk_b), so
            // column existence says nothing about the declared type: only the pk
            // position reported by table_info does. `table_id` is interpolated
            // because PRAGMA takes no bind parameters; it is checked as a UUID first
            // so a malformed catalog row cannot reach the statement.
            if uuid::Uuid::parse_str(&table_id).is_err() {
                report
                    .needs_attention
                    .push(format!("{table_name}: table_id {table_id:?} is not a UUID"));
                continue;
            }
            let mut key_columns: Vec<(i64, String)> =
                db::query_as::<(i64, String, String, i64, Option<String>, i64)>(&format!(
                    "PRAGMA table_info(\"_ddb_{table_id}\")"
                ))
                .fetch_all(&pool)
                .await
                .map_err(|e| {
                    OpError::Internal(format!("Cannot read columns of {table_name}: {e}"))
                })?
                .into_iter()
                .filter(|(.., pk_pos)| *pk_pos > 0)
                .map(|(_, name, _, _, _, pk_pos)| (pk_pos, name))
                .collect();
            key_columns.sort_unstable();
            let columns: Vec<String> = key_columns.into_iter().map(|(_, name)| name).collect();

            let recovered = recover_sort_key_definitions(&key_schema, &attr_defs, &columns);
            if recovered.is_empty() {
                report.needs_attention.push(format!(
                    "{table_name}: sort key(s) [{}] have no attribute definition and no \
                     matching PRIMARY KEY column was found to recover the type from",
                    missing
                        .iter()
                        .map(|ks| ks.attribute_name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                continue;
            }

            let mut merged = attr_defs;
            merged.extend(recovered.iter().cloned());
            let merged_json = serde_json::to_string(&merged)
                .map_err(|e| OpError::Internal(format!("Cannot serialize definitions: {e}")))?;
            if apply {
                db::query(
                    "UPDATE tables SET attribute_definitions = $1 \
                     WHERE account_id = $2 AND table_name = $3",
                )
                .bind(&merged_json)
                .bind(&account_id)
                .bind(&table_name)
                .execute(&pool)
                .await
                .map_err(|e| OpError::Internal(format!("Cannot repair {table_name}: {e}")))?;
            }

            for def in &recovered {
                report.repaired.push(format!(
                    "{table_name}: {} ({:?})",
                    def.attribute_name, def.attribute_type
                ));
            }
        }

        Ok(report)
    }

    async fn list_table_names(&self) -> OpResult<Vec<String>> {
        let Ok(pool) = self.pool().await else {
            return Ok(Vec::new());
        };
        let rows: Vec<(String,)> =
            db::query_as("SELECT table_name FROM tables ORDER BY table_name")
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
            db::query_as("SELECT value FROM settings WHERE key = 'data_database_name'")
                .fetch_optional(&pool)
                .await
                .unwrap_or(None);
        Ok(row.map(|(v,)| v))
    }

    async fn drop_databases(&self, _data_db: &str) -> OpResult<()> {
        if self.is_memory() {
            return Ok(());
        }
        // Release this process's handle on the database first: DuckDB holds a
        // file lock for as long as an instance is open.
        db::forget_path(&self.path);
        if std::path::Path::new(&self.path).exists() {
            std::fs::remove_file(&self.path)
                .map_err(|e| OpError::Internal(format!("remove database file: {e}")))?;
        }
        // Remove the write-ahead log sidecar if present.
        let _ = std::fs::remove_file(format!("{}.wal", self.path));
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
            db::query_as("SELECT value FROM settings WHERE key = 'catalog_version'")
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
        format!("duckdb:{}", self.path)
    }

    fn catalog_connection_url(&self) -> String {
        duckdb_path(&self.path)
    }

    fn generate_backend_config_section(&self) -> String {
        format!(
            "[storage.duckdb]\n\
             path = \"{}\"\n\
             # pool_size = 10",
            self.path
        )
    }
}
