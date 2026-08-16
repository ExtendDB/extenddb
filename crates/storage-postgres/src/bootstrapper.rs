// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `PostgreSQL` implementation of `Bootstrapper`.
//!
//! Handles `CREATE DATABASE`, schema migrations, user provisioning, and
//! teardown using PostgreSQL-specific DDL. Connection pools are created
//! lazily as needed during the bootstrap sequence.

use std::time::Duration;

use async_trait::async_trait;
use extenddb_core::types::{AttributeDefinition, KeySchemaElement};
use extenddb_storage::bootstrapper::{
    AdminBootstrapResult, BootstrapConfig, Bootstrapper, KeyDefinitionRepair,
    helpers::{
        check_conflict, check_conflict_redacted, extract_arg, generate_account_id,
        generate_encryption_key, generate_random_password, hash_password_async,
    },
};
use extenddb_storage::management_store::{OpError, OpResult};
use extenddb_storage::util::recover_sort_key_definitions;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::sync::{Mutex, OnceCell};

use crate::CATALOG_VERSION;
use crate::migrations;

/// ExtendDB's advisory-lock namespace. PostgreSQL's two-argument form,
/// `pg_advisory_xact_lock(classid, objid)`, lets ExtendDB reserve one stable
/// `classid` by convention and assign a distinct `objid` to each internal lock.
/// Other applications in the same database must avoid choosing the same keys.
///
/// The value itself is arbitrary. It only has to stay stable across releases and
/// differ from any other namespace we add later.
const ADVISORY_LOCK_NAMESPACE: i32 = 0x0045_4442; // 'E', 'D', 'B'
/// `objid` for the schema-migration lock, which serializes concurrent `migrate`
/// runs (for example several replicas starting at once).
const MIGRATION_LOCK_OBJID: i32 = 1;

/// Maximum time to wait for another migrator. Normal contention should clear
/// well within this window; expiry indicates a peer that is likely wedged and
/// needs operator attention rather than an indefinitely stuck init Job.
const MIGRATION_LOCK_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Utilities for bootstrapping a `PostgreSQL` backend store.
///
/// Holds the bootstrap configuration and lazily-created connection pools.
/// The admin pool connects to the `postgres` database for DDL operations
/// and is created lazily on first use. Commands that only need the catalog
/// database (e.g. `migrate`) never open an admin connection.
pub struct PostgresBootstrapper {
    config: BootstrapConfig,
    admin_pool: OnceCell<PgPool>,
    /// Dedicated connection whose open transaction holds the migration advisory
    /// lock. `None` when no migration lock is held.
    lock_conn: Mutex<Option<sqlx::PgConnection>>,
}

impl PostgresBootstrapper {
    /// Create a new bootstrapper. The admin pool is created lazily on
    /// first use, so this constructor never opens a database connection.
    #[must_use]
    pub fn new(config: BootstrapConfig) -> Self {
        Self {
            config,
            admin_pool: OnceCell::new(),
            lock_conn: Mutex::new(None),
        }
    }

    /// Connect to the `postgres` database as the admin user eagerly.
    /// Equivalent to `new()` followed by an immediate admin pool init.
    pub async fn connect(config: BootstrapConfig) -> OpResult<Self> {
        let store = Self::new(config);
        // Force admin pool creation to fail fast on connection errors.
        store.admin_pool().await?;
        Ok(store)
    }

    /// Get or create the admin pool (connects to the `postgres` database).
    async fn admin_pool(&self) -> OpResult<&PgPool> {
        self.admin_pool
            .get_or_try_init(|| async {
                let opts = PgConnectOptions::new()
                    .host(&self.config.host)
                    .port(self.config.port)
                    .username(&self.config.admin_user)
                    .database("postgres");
                let opts = if let Some(ref pass) = self.config.admin_password {
                    opts.password(pass)
                } else {
                    opts
                };
                PgPoolOptions::new()
                    .max_connections(1)
                    .connect_with(opts)
                    .await
                    .map_err(|e| OpError::Internal(format!("Cannot connect as admin: {e}")))
            })
            .await
    }

    /// Build `PgConnectOptions` for the application user connecting to a named database.
    fn app_connect_opts(&self, database: &str) -> PgConnectOptions {
        PgConnectOptions::new()
            .host(&self.config.host)
            .port(self.config.port)
            .username(&self.config.app_user)
            .password(&self.config.app_password)
            .database(database)
    }

    /// Build the connection URL for the application user and a named database.
    ///
    /// URL-encodes all components to handle special characters:
    /// - Unix socket paths (e.g., `/var/run/postgresql` → `%2Fvar%2Frun%2Fpostgresql`)
    /// - Passwords with special chars (e.g., `pass@word` → `pass%40word`)
    /// - Database names with special chars
    ///
    /// `PostgreSQL`'s libpq automatically decodes percent-encoded values per RFC 3986.
    fn app_connection_url(&self, database: &str) -> String {
        let host_encoded = urlencoding::encode(&self.config.host);
        let user_encoded = urlencoding::encode(&self.config.app_user);
        let pass_encoded = urlencoding::encode(&self.config.app_password);
        let db_encoded = urlencoding::encode(database);

        format!(
            "postgresql://{}:{}@{}:{}/{}",
            user_encoded, pass_encoded, host_encoded, self.config.port, db_encoded,
        )
    }

    /// Open a one-shot pool to the given database as the application user.
    async fn app_pool(&self, database: &str) -> OpResult<PgPool> {
        PgPoolOptions::new()
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
impl Bootstrapper for PostgresBootstrapper {
    async fn ensure_app_user(&self) -> OpResult<()> {
        let user = &self.config.app_user;
        let password = &self.config.app_password;
        let admin = self.admin_pool().await?;

        println!("--- Ensuring application user '{user}' exists...");
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)")
                .bind(user)
                .fetch_one(admin)
                .await
                .map_err(|e| OpError::Internal(format!("Check user exists: {e}")))?;

        if exists {
            println!("    User '{user}' already exists.");
            return Ok(());
        }

        // CREATE ROLE doesn't support parameterized passwords, so we use format!.
        // Strict allowlist prevents SQL injection via backslash, NUL, semicolon, newline.
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
        let sql = format!("CREATE USER \"{user}\" WITH PASSWORD '{password}'");
        sqlx::query(&sql)
            .execute(admin)
            .await
            .map_err(|e| OpError::Internal(format!("Create user: {e}")))?;
        println!("    Created user '{user}'.");
        Ok(())
    }

    async fn grant_app_role_to_admin(&self) -> OpResult<()> {
        let admin = self.admin_pool().await?;
        if self.config.admin_user == self.config.app_user {
            return Ok(());
        }
        let grant_sql = format!(
            "GRANT \"{}\" TO \"{}\"",
            self.config.app_user, self.config.admin_user
        );
        sqlx::query(&grant_sql).execute(admin).await.map_err(|e| {
            OpError::Internal(format!(
                "Cannot grant {} to {}: {e}",
                self.config.app_user, self.config.admin_user
            ))
        })?;
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

    async fn acquire_migration_lock(&self) -> OpResult<()> {
        use std::io::Write as _;

        use sqlx::Connection;

        // This is not re-entrant. A second call would open a second transaction
        // and block on the lock the first one holds, deadlocking against itself.
        let mut held = self.lock_conn.lock().await;
        if held.is_some() {
            return Err(OpError::Internal(
                "Migration lock is already held by this process".to_owned(),
            ));
        }

        // Keep an explicit transaction open on a dedicated catalog connection
        // for the whole migration. Transaction-level advisory locks are released
        // automatically when that transaction ends or the connection dies.
        //
        // The explicit transaction is important for transaction-pooling proxies:
        // they must retain one PostgreSQL backend until COMMIT/ROLLBACK, so the
        // lock cannot silently move between backends as separate autocommit
        // statements can. RDS Proxy also supports this path without session
        // pinning. The migration statements can use separate connections because
        // advisory locks coordinate globally within the catalog database.
        //
        // Advisory locks are scoped to a database, so migrators only serialize
        // if they share this catalog database. ExtendDB replicas normally do,
        // because they use the same catalog connection string.
        let mut conn =
            sqlx::PgConnection::connect_with(&self.app_connect_opts(&self.config.catalog_db))
                .await
                .map_err(|e| {
                    OpError::Internal(format!("Cannot connect to take migration lock: {e}"))
                })?;
        sqlx::query("BEGIN")
            .execute(&mut conn)
            .await
            .map_err(|e| OpError::Internal(format!("Cannot begin migration lock: {e}")))?;

        // Equivalent to SET LOCAL lock_timeout, but parameterized so the Rust
        // duration remains the single source of truth. This applies only to the
        // dedicated transaction and is discarded by rollback on release.
        let lock_timeout = format!("{}ms", MIGRATION_LOCK_TIMEOUT.as_millis());
        let _: String = sqlx::query_scalar("SELECT set_config('lock_timeout', $1, true)")
            .bind(&lock_timeout)
            .fetch_one(&mut conn)
            .await
            .map_err(|e| {
                OpError::Internal(format!("Cannot configure migration lock timeout: {e}"))
            })?;

        // Try first so that a migrator which has to wait can say so, rather
        // than sitting silent for as long as the other migration takes.
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1, $2)")
            .bind(ADVISORY_LOCK_NAMESPACE)
            .bind(MIGRATION_LOCK_OBJID)
            .fetch_one(&mut conn)
            .await
            .map_err(|e| OpError::Internal(format!("Cannot acquire migration lock: {e}")))?;
        if !acquired {
            println!("--- Another migrator holds the migration lock; waiting for it to finish...");
            std::io::stdout()
                .flush()
                .map_err(|e| OpError::Internal(format!("Cannot report migration wait: {e}")))?;
            sqlx::query("SELECT pg_advisory_xact_lock($1, $2)")
                .bind(ADVISORY_LOCK_NAMESPACE)
                .bind(MIGRATION_LOCK_OBJID)
                .execute(&mut conn)
                .await
                .map_err(|e| {
                    if let sqlx::Error::Database(db_err) = &e
                        && db_err.code().as_deref() == Some("55P03")
                    {
                        return OpError::Internal(format!(
                            "Timed out after {}s waiting for the migration advisory lock; \
                             another migrator may be wedged. Check the holder's logs or \
                             terminate it before retrying: {e}",
                            MIGRATION_LOCK_TIMEOUT.as_secs(),
                        ));
                    }
                    OpError::Internal(format!("Cannot acquire migration lock: {e}"))
                })?;
            println!("--- Migration lock acquired.");
            std::io::stdout()
                .flush()
                .map_err(|e| OpError::Internal(format!("Cannot report migration lock: {e}")))?;
        }

        // Guard against a key or lock-mode mismatch in the acquisition queries.
        // `objsubid = 2` distinguishes the two-i32 advisory-lock keyspace from
        // the one-i64 form, and `granted` excludes a merely waiting request.
        let held_by_this_transaction: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_locks \
             WHERE locktype = 'advisory' AND classid = $1 AND objid = $2 \
               AND objsubid = 2 AND mode = 'ExclusiveLock' AND granted \
               AND pid = pg_backend_pid())",
        )
        .bind(ADVISORY_LOCK_NAMESPACE)
        .bind(MIGRATION_LOCK_OBJID)
        .fetch_one(&mut conn)
        .await
        .map_err(|e| OpError::Internal(format!("Cannot verify migration lock: {e}")))?;
        if !held_by_this_transaction {
            return Err(OpError::Internal(
                "PostgreSQL did not report the migration transaction's advisory lock after \
                 acquisition; refusing to run migrations without verified serialization"
                    .to_owned(),
            ));
        }

        *held = Some(conn);
        Ok(())
    }

    async fn release_migration_lock(&self) -> OpResult<()> {
        use sqlx::Connection;

        let Some(mut conn) = self.lock_conn.lock().await.take() else {
            return Ok(());
        };
        // This transaction contains only the advisory lock, so rollback is the
        // safest release: it cannot accidentally commit future work added to the
        // dedicated connection. Closing is a backstop if rollback fails.
        let release = sqlx::query("ROLLBACK").execute(&mut conn).await;
        let _ = conn.close().await;
        release
            .map(|_| ())
            .map_err(|e| OpError::Internal(format!("Cannot release migration lock: {e}")))
    }

    async fn pending_data_migrations(&self) -> OpResult<Vec<String>> {
        let pool = self.app_pool(&self.config.data_db).await?;
        migrations::pending_data_migrations(&pool).await
    }

    async fn record_data_connection(&self) -> OpResult<()> {
        let pool = self.app_pool(&self.config.catalog_db).await?;
        let data_conn = self.app_connection_url(&self.config.data_db);

        println!("--- Recording data database connection in catalog...");
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ('data_database_connection_string', $1) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(&data_conn)
        .execute(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("Record data connection: {e}")))?;

        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ('data_database_name', $1) \
             ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(&self.config.data_db)
        .execute(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("Record data db name: {e}")))?;

        Ok(())
    }

    async fn bootstrap_encryption_key(&self) -> OpResult<()> {
        let pool = self.app_pool(&self.config.catalog_db).await?;
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
        let key_b64 = generate_encryption_key();

        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ('encryption_key', $1) \
             ON CONFLICT (key) DO NOTHING",
        )
        .bind(&key_b64)
        .execute(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("Store encryption key: {e}")))?;

        println!("    Encryption key stored.");
        Ok(())
    }

    async fn bootstrap_default_account(&self) -> OpResult<()> {
        let pool = self.app_pool(&self.config.catalog_db).await?;
        // Reuse the existing account when already bootstrapped, otherwise create
        // the single default account. Either way, record its id as the canonical
        // default account so callers never infer it from list ordering. The
        // settings write is idempotent, so it also backfills the marker for
        // catalogs bootstrapped before it existed.
        let account_id: String =
            match sqlx::query_scalar("SELECT account_id FROM accounts ORDER BY account_id LIMIT 1")
                .fetch_optional(&pool)
                .await
                .map_err(|e| OpError::Internal(format!("Check accounts: {e}")))?
            {
                Some(id) => {
                    println!("--- Default account already exists, skipping.");
                    id
                }
                None => {
                    let id = generate_account_id();
                    println!("--- Creating default account '{id}'...");
                    sqlx::query(
                        "INSERT INTO accounts (account_id, account_name) VALUES ($1, $2) \
                     ON CONFLICT (account_id) DO NOTHING",
                    )
                    .bind(&id)
                    .bind("default")
                    .execute(&pool)
                    .await
                    .map_err(|e| OpError::Internal(format!("Create account: {e}")))?;
                    println!("    Account ID: {id}");
                    id
                }
            };
        sqlx::query(
            "INSERT INTO settings (key, value) VALUES ('default_account_id', $1) \
             ON CONFLICT (key) DO NOTHING",
        )
        .bind(&account_id)
        .execute(&pool)
        .await
        .map_err(|e| OpError::Internal(format!("Record default account id: {e}")))?;
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
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM admin_users WHERE admin_name = $1)")
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
        let hash = hash_password_async(password.clone()).await?;

        sqlx::query(
            "INSERT INTO admin_users (admin_name, password_hash) VALUES ($1, $2) \
             ON CONFLICT (admin_name) DO NOTHING",
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

    /// Repair table metadata damaged by the pre-fix `UpdateTable` (#259).
    ///
    /// Reads the catalog's key schemas and attribute definitions, and for any base
    /// sort key with no definition recovers the type from the data table's
    /// physical sort key column (`sk_s`/`sk_n`/`sk_b`, or the numbered variants for
    /// multiple sort keys). Runs under the migration lock the caller holds.
    async fn repair_lost_sort_key_definitions(&self, apply: bool) -> OpResult<KeyDefinitionRepair> {
        let mut report = KeyDefinitionRepair::default();
        let Ok(catalog) = self.app_pool(&self.config.catalog_db).await else {
            return Ok(report);
        };
        let Some(data_db) = self.get_data_db_name().await? else {
            return Ok(report);
        };
        let data = self.app_pool(&data_db).await?;

        let rows: Vec<(String, String, String, serde_json::Value, serde_json::Value)> =
            sqlx::query_as(
                "SELECT account_id, table_name, table_id, key_schema, attribute_definitions \
                 FROM tables ORDER BY table_name",
            )
            .fetch_all(&catalog)
            .await
            .map_err(|e| OpError::Internal(format!("Cannot read tables: {e}")))?;

        for (account_id, table_name, table_id, ks_json, ad_json) in rows {
            let key_schema: Vec<KeySchemaElement> = match serde_json::from_value(ks_json) {
                Ok(v) => v,
                Err(e) => {
                    report
                        .needs_attention
                        .push(format!("{table_name}: unreadable key_schema ({e})"));
                    continue;
                }
            };
            let attr_defs: Vec<AttributeDefinition> =
                serde_json::from_value(ad_json).unwrap_or_default();

            // Nothing to do unless a sort key has lost its definition.
            // Reported for every table on every run, not only when a sort key is
            // repaired: the partition key definition is dropped by the same write and
            // is not recoverable from the schema, because the pk column is always TEXT.
            // It is not needed for correctness either, since partition key values are
            // encoded from the key schema alone, so it is reported rather than guessed
            // and keeps being reported until a human restores it.
            let pk_missing: Vec<&str> = key_schema
                .iter()
                .filter(|ks| ks.key_type == extenddb_core::types::KeyType::Hash)
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
                .filter(|ks| ks.key_type == extenddb_core::types::KeyType::Range)
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
            // column existence says nothing about the declared type: only PRIMARY KEY
            // membership does. `to_regclass` resolves the name through the current
            // search_path, so this cannot match a same-named table in another schema,
            // and returns NULL (no rows) rather than raising when the data table is
            // absent.
            let columns: Vec<String> = sqlx::query_as::<_, (String,)>(
                "SELECT a.attname \
                 FROM pg_index i \
                 JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE \
                 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
                 WHERE i.indrelid = to_regclass($1) AND i.indisprimary \
                 ORDER BY k.ord",
            )
            .bind(format!("\"_ddb_{table_id}\""))
            .fetch_all(&data)
            .await
            .map_err(|e| OpError::Internal(format!("Cannot read columns of {table_name}: {e}")))?
            .into_iter()
            .map(|(c,)| c)
            .collect();

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
            let merged_json = serde_json::to_value(&merged)
                .map_err(|e| OpError::Internal(format!("Cannot serialize definitions: {e}")))?;
            if apply {
                sqlx::query(
                    "UPDATE tables SET attribute_definitions = $1 \
                     WHERE account_id = $2 AND table_name = $3",
                )
                .bind(&merged_json)
                .bind(&account_id)
                .bind(&table_name)
                .execute(&catalog)
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
        let Ok(pool) = self.app_pool(&self.config.catalog_db).await else {
            return Ok(Vec::new());
        };
        let tables: Vec<(String,)> =
            sqlx::query_as("SELECT table_name FROM tables ORDER BY table_name")
                .fetch_all(&pool)
                .await
                .unwrap_or_default();
        Ok(tables.into_iter().map(|(n,)| n).collect())
    }

    async fn get_data_db_name(&self) -> OpResult<Option<String>> {
        let Ok(pool) = self.app_pool(&self.config.catalog_db).await else {
            return Ok(None);
        };
        let row = sqlx::query_as::<_, (String,)>(
            "SELECT value FROM settings WHERE key = 'data_database_name'",
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
            let sql = format!("DROP DATABASE IF EXISTS \"{data_db}\"");
            sqlx::query(&sql)
                .execute(admin)
                .await
                .map_err(|e| OpError::Internal(format!("Drop data database: {e}")))?;
        }

        let catalog = &self.config.catalog_db;
        println!("--- Dropping catalog database '{catalog}'...");
        let sql = format!("DROP DATABASE IF EXISTS \"{catalog}\"");
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
        self.config.catalog_db.clone()
    }

    fn endpoint_info(&self) -> String {
        format!("{}:{}", self.config.host, self.config.port)
    }

    fn catalog_connection_url(&self) -> String {
        self.app_connection_url(&self.config.catalog_db)
    }

    fn generate_backend_config_section(&self) -> String {
        format!(
            r#"[storage.postgres]
connection_string = "{}"
# pool_size = 20                 # Max connections for data operations (default 20, min 10)
# catalog_pool_size =            # Max connections for management/catalog ops (defaults to pool_size, min 10)"#,
            self.catalog_connection_url()
        )
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

/// Create a database, aborting if it already exists.
async fn create_database(pool: &PgPool, name: &str, owner: &str) -> OpResult<()> {
    println!("--- Creating database '{name}'...");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
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
    let sql = format!("CREATE DATABASE \"{name}\" OWNER \"{owner}\"");
    sqlx::query(&sql)
        .execute(pool)
        .await
        .map_err(|e| OpError::Internal(format!("Create database '{name}': {e}")))?;
    println!("    Created.");
    Ok(())
}

impl PostgresBootstrapper {
    /// Create a bootstrapper from config file and CLI args. Parses
    /// Postgres-specific arguments and merges with config.
    pub async fn from_config(
        config_path: &str,
        cli_args: &[String],
    ) -> Result<Self, extenddb_storage::error::StorageError> {
        use extenddb_storage::error::StorageError;

        // Extract Postgres-specific CLI args
        let pg_host = extract_arg(cli_args, "--pg-host");
        let pg_port = extract_arg(cli_args, "--pg-port").and_then(|s| s.parse().ok());
        let pg_user = extract_arg(cli_args, "--pg-user");
        let pg_pass = extract_arg(cli_args, "--pg-pass");
        let data_db = extract_arg(cli_args, "--data-db");
        let catalog_db = extract_arg(cli_args, "--catalog-db");
        let extenddb_user = extract_arg(cli_args, "--extenddb-user");
        let extenddb_pass = extract_arg(cli_args, "--extenddb-pass");

        // Load config file if it exists
        let (host, port, user, password, catalog_db_name) = if std::path::Path::new(config_path)
            .exists()
        {
            println!("--- Loading defaults from {config_path}");

            // Parse connection string from config
            let config_content = std::fs::read_to_string(config_path)
                .map_err(|e| StorageError::Internal(format!("Failed to read config: {e}")))?;
            let app_config: toml::Value = toml::from_str(&config_content)
                .map_err(|e| StorageError::Internal(format!("Failed to parse config: {e}")))?;

            let conn_str = app_config
                .get("storage")
                .and_then(|s| s.get("postgres"))
                .and_then(|p| p.get("connection_string"))
                .and_then(|c| c.as_str())
                .ok_or_else(|| {
                    StorageError::Internal("Missing storage.postgres.connection_string".into())
                })?;

            let parts = crate::config::parse_connection_string(conn_str)
                .map_err(|e| StorageError::Internal(format!("Invalid connection string: {e}")))?;

            // Check for conflicts between CLI args and config values
            check_conflict(pg_host.as_ref(), &parts.host, "--pg-host")?;
            check_conflict(pg_port.as_ref(), &parts.port, "--pg-port")?;
            check_conflict(extenddb_user.as_ref(), &parts.user, "--extenddb-user")?;
            check_conflict_redacted(extenddb_pass.as_ref(), &parts.password, "--extenddb-pass")?;

            if let Some(ref cli_catalog) = catalog_db
                && cli_catalog != &parts.database
            {
                return Err(StorageError::Internal(format!(
                    "--catalog-db '{}' conflicts with config file catalog database '{}'",
                    cli_catalog, parts.database
                )));
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
                5432,
                "extenddb".to_string(),
                "extenddb-local-dev".to_string(),
                "extenddb_catalog".to_string(),
            )
        };

        // CLI args override config (or use config values if no CLI arg provided)
        let resolved_host = pg_host.unwrap_or(host);
        let resolved_port = pg_port.unwrap_or(port);
        let resolved_admin_user = pg_user
            .unwrap_or_else(|| std::env::var("USER").unwrap_or_else(|_| "postgres".to_owned()));
        let resolved_catalog_db = catalog_db.unwrap_or(catalog_db_name);
        let final_data_db = data_db.unwrap_or_else(|| {
            resolved_catalog_db
                .strip_suffix("_catalog")
                .unwrap_or(&resolved_catalog_db)
                .to_owned()
        });
        let resolved_app_user = extenddb_user.unwrap_or(user);
        let resolved_app_password = extenddb_pass.unwrap_or(password);

        let config = BootstrapConfig {
            host: resolved_host,
            port: resolved_port,
            admin_user: resolved_admin_user,
            admin_password: pg_pass,
            app_user: resolved_app_user,
            app_password: resolved_app_password,
            catalog_db: resolved_catalog_db,
            data_db: final_data_db,
        };

        Self::connect(config)
            .await
            .map_err(|e| StorageError::Internal(format!("{e:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use extenddb_storage::bootstrapper::BootstrapConfig;

    #[test]
    fn test_connection_url_tcp_host() {
        let config = BootstrapConfig {
            host: "localhost".to_string(),
            port: 5432,
            admin_user: "postgres".to_string(),
            admin_password: None,
            app_user: "extenddb".to_string(),
            app_password: "testpass".to_string(),
            catalog_db: "extenddb_catalog".to_string(),
            data_db: "extenddb".to_string(),
        };
        let bootstrapper = PostgresBootstrapper::new(config);
        let url = bootstrapper.catalog_connection_url();

        assert_eq!(
            url,
            "postgresql://extenddb:testpass@localhost:5432/extenddb_catalog"
        );
    }

    #[test]
    fn test_connection_url_unix_socket() {
        let config = BootstrapConfig {
            host: "/var/run/postgresql".to_string(),
            port: 5432,
            admin_user: "postgres".to_string(),
            admin_password: None,
            app_user: "extenddb".to_string(),
            app_password: "testpass".to_string(),
            catalog_db: "extenddb_catalog".to_string(),
            data_db: "extenddb".to_string(),
        };
        let bootstrapper = PostgresBootstrapper::new(config);
        let url = bootstrapper.catalog_connection_url();

        assert_eq!(
            url,
            "postgresql://extenddb:testpass@%2Fvar%2Frun%2Fpostgresql:5432/extenddb_catalog"
        );
    }

    #[test]
    fn test_connection_url_password_with_special_chars() {
        let config = BootstrapConfig {
            host: "localhost".to_string(),
            port: 5432,
            admin_user: "postgres".to_string(),
            admin_password: None,
            app_user: "extenddb".to_string(),
            app_password: "pass@word:with/special".to_string(),
            catalog_db: "extenddb_catalog".to_string(),
            data_db: "extenddb".to_string(),
        };
        let bootstrapper = PostgresBootstrapper::new(config);
        let url = bootstrapper.catalog_connection_url();

        assert_eq!(
            url,
            "postgresql://extenddb:pass%40word%3Awith%2Fspecial@localhost:5432/extenddb_catalog"
        );
    }

    #[tokio::test]
    async fn test_connection_url_round_trip_tcp() {
        let config = BootstrapConfig {
            host: "localhost".to_string(),
            port: 5432,
            admin_user: "postgres".to_string(),
            admin_password: None,
            app_user: "extenddb".to_string(),
            app_password: "testpass".to_string(),
            catalog_db: "extenddb_catalog".to_string(),
            data_db: "extenddb".to_string(),
        };
        let bootstrapper = PostgresBootstrapper::new(config);
        let url = bootstrapper.catalog_connection_url();

        // Should parse without error
        let opts = url
            .parse::<sqlx::postgres::PgConnectOptions>()
            .expect("Generated URL should parse");

        // Verify parsed values match original config
        assert_eq!(opts.get_host(), "localhost");
        assert_eq!(opts.get_port(), 5432);
        assert_eq!(opts.get_username(), "extenddb");
        assert_eq!(opts.get_database().unwrap(), "extenddb_catalog");
    }

    #[tokio::test]
    async fn test_connection_url_round_trip_unix_socket() {
        let config = BootstrapConfig {
            host: "/var/run/postgresql".to_string(),
            port: 5432,
            admin_user: "postgres".to_string(),
            admin_password: None,
            app_user: "extenddb".to_string(),
            app_password: "testpass".to_string(),
            catalog_db: "extenddb_catalog".to_string(),
            data_db: "extenddb".to_string(),
        };
        let bootstrapper = PostgresBootstrapper::new(config);
        let url = bootstrapper.catalog_connection_url();

        // Should parse without error - this is the key test
        let opts = url
            .parse::<sqlx::postgres::PgConnectOptions>()
            .expect("Generated URL should parse");

        // Note: sqlx may show "localhost" for get_host() even with a Unix socket path,
        // but the actual connection uses the percent-encoded socket path correctly.
        // The important thing is that the URL parses and the connection will work.
        assert_eq!(opts.get_port(), 5432);
        assert_eq!(opts.get_username(), "extenddb");
        assert_eq!(opts.get_database().unwrap(), "extenddb_catalog");
    }
}
