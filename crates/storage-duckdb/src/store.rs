// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Central DuckDB storage engine: connection pool, write serialization, and
//! construction.
//!
//! # Concurrency model (design decision D1)
//!
//! WAL mode allows many concurrent readers alongside a single writer. DuckDB
//! offers no `SERIALIZABLE` isolation knob, so to make condition-check-then-
//! write atomic and free of write-skew the engine serializes all writers
//! through `write_lock` and opens every write transaction with `BEGIN
//! IMMEDIATE` via `pool.begin()`, so the condition read
//! and its write hold DuckDB's reserved write lock up front. This is
//! multi-pool-safe: the catalog/credential store opens a second pool over the
//! same file, and `BEGIN IMMEDIATE` + `busy_timeout` prevent
//! `SQLITE_BUSY_SNAPSHOT` (a deferred read-then-write whose snapshot is
//! invalidated by another pool committing) rather than surfacing it as a 500.
//! Reads run concurrently from the pool against WAL snapshots and take no lock.

use crate::db;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use extenddb_storage::error::StorageError;

use tokio::sync::Mutex;

use crate::INDEX_PROPAGATION_DELAY_QUERY;
use crate::duckdb_util::duckdb_path;
use crate::schema::CATALOG_VERSION;

/// The DuckDB storage engine. Implements `StorageEngine` (all data/metadata/
/// stream/worker/backup traits) over a single DuckDB database.
#[derive(Clone)]
pub struct DuckDbEngine {
    pub(crate) pool: db::Pool,
    pub(crate) region: String,
    pub(crate) max_item_size_bytes: usize,
    /// Wakes the control-plane poller when a table enters CREATING / DELETING.
    pub(crate) control_plane_notify: Arc<tokio::sync::Notify>,
    /// Cached default GSI propagation delay (ms); refreshed by a worker and
    /// read on the write path to decide sync-vs-async index maintenance.
    pub(crate) index_propagation_delay_cache: Arc<AtomicU64>,
    /// Wakes the GSI propagation worker when a write enqueues into `gsi_pending`.
    pub(crate) gsi_notify: Arc<tokio::sync::Notify>,
    /// Serializes all writers (design decision D1). Held for the duration of
    /// every write transaction so condition checks and writes are atomic and
    /// `SQLITE_BUSY` cannot arise from competing writers.
    pub(crate) write_lock: Arc<Mutex<()>>,
    /// Index ids whose asynchronous backfill task is currently alive in THIS
    /// process. What tells a stuck `CREATING` index apart from one still
    /// building: a catalog row can say `CREATING` forever, but only a live task
    /// appears here, and the entry is removed by a drop guard so a panicking
    /// task deregisters too. The GSI worker recovers any `CREATING` index with
    /// no entry, because nothing else ever will until a restart, and until then
    /// the per-table queue hold blocks every write's index maintenance.
    pub(crate) vector_builds_running: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

impl DuckDbEngine {
    /// Open (creating if necessary) the DuckDB database and build the engine.
    ///
    /// `path_or_url` accepts a filesystem path, `:memory:`, or a `duckdb:` URL.
    pub async fn new(
        path_or_url: &str,
        pool_size: u32,
        region: &str,
        max_item_size_bytes: usize,
    ) -> Result<Self, StorageError> {
        let path = duckdb_path(path_or_url);
        // Every connection in the pool is a clone of one root connection, so
        // they share a single database instance. Unlike SQLite, an in-memory
        // DuckDB database is shared by all connections cloned from it, so the
        // ephemeral mode keeps a real pool rather than a single pinned handle.
        let in_memory = crate::db::is_memory_path(&path);
        let pool = crate::db::Pool::open(&path, pool_size.max(1))
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        // The database file stores the AES encryption key alongside the data it
        // protects (encrypted access-key secrets, password hashes). Restrict the
        // file and its WAL sidecar to owner read/write so other local users or
        // processes cannot read it and decrypt secrets. In-memory databases
        // have no file to protect.
        #[cfg(unix)]
        if !in_memory {
            use std::os::unix::fs::PermissionsExt;
            for suffix in ["", ".wal"] {
                let f = format!("{path}{suffix}");
                if std::path::Path::new(&f).exists() {
                    let _ = std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600));
                }
            }
        }

        let initial_index_delay: u64 = db::query_as::<(String,)>(INDEX_PROPAGATION_DELAY_QUERY)
            .fetch_optional(&pool)
            .await
            .ok()
            .flatten()
            .and_then(|(v,)| v.parse::<u64>().ok())
            .unwrap_or(10);

        Ok(Self {
            pool,
            region: region.to_owned(),
            max_item_size_bytes,
            control_plane_notify: Arc::new(tokio::sync::Notify::new()),
            index_propagation_delay_cache: Arc::new(AtomicU64::new(initial_index_delay)),
            gsi_notify: Arc::new(tokio::sync::Notify::new()),
            write_lock: Arc::new(Mutex::new(())),
            vector_builds_running: Arc::new(
                std::sync::Mutex::new(std::collections::HashSet::new()),
            ),
        })
    }

    /// Handle to the control-plane notifier, for the background poller.
    pub(crate) fn control_plane_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.control_plane_notify)
    }

    /// Bootstrap an ephemeral catalog on the engine's own (shared) connection,
    /// at serve time. Used for in-memory databases, whose state does not
    /// survive the `init` process, so schema, encryption key, default account,
    /// and admin user must be created when the server starts. Idempotent.
    ///
    /// Returns `Some(password)` when a new admin user was created with a
    /// generated password (so the caller can surface it once), or `None` when
    /// the admin already existed or the password came from the environment.
    pub(crate) async fn bootstrap_ephemeral(
        &self,
        admin_user: Option<&str>,
        admin_password: Option<&str>,
    ) -> Result<Option<String>, StorageError> {
        use extenddb_storage::bootstrapper::helpers;

        let internal = |e: String| StorageError::Internal(e);

        // Schema (creates settings, accounts, admin_users, … and seeds
        // catalog_version + index_propagation_delay_ms).
        crate::schema::apply(&self.pool)
            .await
            .map_err(|e| internal(format!("apply schema: {e:?}")))?;

        // Encryption key.
        let key_exists: bool =
            db::query_scalar("SELECT EXISTS(SELECT 1 FROM settings WHERE key = 'encryption_key')")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| internal(format!("check encryption key: {e}")))?;
        if !key_exists {
            let key = helpers::generate_encryption_key();
            db::query("INSERT OR IGNORE INTO settings (key, value) VALUES ('encryption_key', ?)")
                .bind(&key)
                .execute(&self.pool)
                .await
                .map_err(|e| internal(format!("store encryption key: {e}")))?;
        }

        // Default account + canonical marker. Record the default account id in
        // settings (idempotent, backfill-safe) so callers resolve it explicitly
        // rather than inferring it from account-list ordering.
        let account_id: String = match db::query_scalar(
            "SELECT account_id FROM accounts ORDER BY account_id LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal(format!("check accounts: {e}")))?
        {
            Some(id) => id,
            None => {
                let id = helpers::generate_account_id();
                db::query(
                    "INSERT OR IGNORE INTO accounts (account_id, account_name) VALUES (?, 'default')",
                )
                .bind(&id)
                .execute(&self.pool)
                .await
                .map_err(|e| internal(format!("create default account: {e}")))?;
                id
            }
        };
        db::query("INSERT OR IGNORE INTO settings (key, value) VALUES ('default_account_id', ?)")
            .bind(&account_id)
            .execute(&self.pool)
            .await
            .map_err(|e| internal(format!("record default account id: {e}")))?;

        // Admin user.
        let username = admin_user
            .filter(|s| !s.is_empty())
            .unwrap_or("admin")
            .to_owned();
        let admin_exists: bool =
            db::query_scalar("SELECT EXISTS(SELECT 1 FROM admin_users WHERE admin_name = ?)")
                .bind(&username)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| internal(format!("check admin user: {e}")))?;
        if admin_exists {
            return Ok(None);
        }
        let (password, from_env) = match admin_password.filter(|s| !s.is_empty()) {
            Some(p) => (p.to_owned(), true),
            None => (helpers::generate_random_password(), false),
        };
        let password_hash = helpers::hash_password_async(password.clone())
            .await
            .map_err(|e| internal(format!("hash admin password: {e:?}")))?;
        db::query("INSERT INTO admin_users (admin_name, password_hash) VALUES (?, ?)")
            .bind(&username)
            .bind(&password_hash)
            .execute(&self.pool)
            .await
            .map_err(|e| internal(format!("create admin user: {e}")))?;

        Ok(if from_env { None } else { Some(password) })
    }

    /// Current secondary-index propagation delay (ms); `0` means synchronous.
    ///
    /// Reads the `index_propagation_delay_ms` setting live from the catalog so
    /// out-of-process changes (`extenddb settings set`) take effect on the
    /// next write, not up to 30 s later when the poll worker refreshes the
    /// cache. `DuckDB` is a local file, so this is an indexed point lookup with
    /// negligible cost next to the write it precedes. On a read error the
    /// cached value (still refreshed by the poll worker) is the fallback; on
    /// success the cache is re-warmed so fallback reads stay fresh.
    pub(crate) async fn index_propagation_delay(&self) -> u64 {
        use std::sync::atomic::Ordering;
        let live: Result<Option<(String,)>, _> = db::query_as(INDEX_PROPAGATION_DELAY_QUERY)
            .fetch_optional(&self.pool)
            .await;
        match live {
            Ok(row) => {
                // Missing row means the default, matching poll_gsi_delay.
                let ms = row
                    .and_then(|(v,)| v.parse::<u64>().ok())
                    .unwrap_or(crate::DEFAULT_INDEX_PROPAGATION_DELAY_MS);
                self.index_propagation_delay_cache
                    .store(ms, Ordering::Relaxed);
                ms
            }
            Err(e) => {
                tracing::debug!("index_propagation_delay: live read failed, using cache: {e:?}");
                self.index_propagation_delay_cache.load(Ordering::Relaxed)
            }
        }
    }

    /// Milliseconds to pause between batches of a vector index backfill.
    ///
    /// Read live for the same reason the propagation delay is: a test sets it with
    /// `settings set` and needs it to apply to the next backfill, not up to 30 s
    /// later. Zero when unset or unparseable, which is the production value, so a
    /// malformed setting cannot slow a real backfill down.
    pub(crate) async fn vector_backfill_batch_delay(&self) -> u64 {
        let live: Result<Option<(String,)>, _> =
            db::query_as("SELECT value FROM settings WHERE key = ?")
                .bind(extenddb_core::settings_keys::VECTOR_BACKFILL_BATCH_DELAY_MS)
                .fetch_optional(&self.pool)
                .await;
        match live {
            Ok(row) => row.and_then(|(v,)| v.parse::<u64>().ok()).unwrap_or(0),
            Err(e) => {
                tracing::debug!("vector_backfill_batch_delay: live read failed, using 0: {e:?}");
                0
            }
        }
    }

    /// Minimum milliseconds an UpdateTable-created vector index stays `CREATING`
    /// before its `ACTIVE` flip. See
    /// [`extenddb_core::settings_keys::VECTOR_INDEX_MIN_CREATING_MS`] for why the
    /// hold exists. Defaults to 1000 when unset or unparseable; zero disables it.
    pub(crate) async fn vector_index_min_creating_ms(&self) -> u64 {
        const DEFAULT_MS: u64 = 1_000;
        let live: Result<Option<(String,)>, _> =
            db::query_as("SELECT value FROM settings WHERE key = ?")
                .bind(extenddb_core::settings_keys::VECTOR_INDEX_MIN_CREATING_MS)
                .fetch_optional(&self.pool)
                .await;
        match live {
            Ok(row) => row
                .and_then(|(v,)| v.parse::<u64>().ok())
                .unwrap_or(DEFAULT_MS),
            Err(e) => {
                tracing::debug!(
                    "vector_index_min_creating_ms: live read failed, using {DEFAULT_MS}: {e:?}"
                );
                DEFAULT_MS
            }
        }
    }

    /// Handle to the GSI propagation notifier, woken after an enqueue.
    pub(crate) fn gsi_notify(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.gsi_notify)
    }

    /// Defense-in-depth: reject `account_id` values unsafe for interpolation
    /// into quoted SQL identifiers (`_ddb_*` data-table names embed `table_id`,
    /// but `account_id` flows through the catalog and must stay clean).
    /// See `docs/adr/0002-sql-injection-defense.md` in the extenddb repo.
    pub(crate) fn validate_account_id(account_id: &str) -> Result<(), StorageError> {
        if account_id.contains('"') || account_id.contains('\0') || !account_id.is_ascii() {
            return Err(StorageError::Internal(
                "account_id contains invalid characters for use in SQL identifiers".to_owned(),
            ));
        }
        Ok(())
    }

    /// Validate the on-disk catalog version matches the compiled expectation.
    ///
    /// # Errors
    /// - [`StorageError::CatalogNotInitialized`] if the catalog is absent.
    /// - [`StorageError::CatalogVersionMismatch`] if versions differ.
    /// - [`StorageError::Internal`] if the stored version is malformed.
    pub async fn check_catalog_version(&self) -> Result<(), StorageError> {
        let exists: bool = db::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM duckdb_tables() WHERE table_name = 'settings')",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !exists {
            return Err(StorageError::CatalogNotInitialized);
        }

        let found_str =
            db::query_as::<(String,)>("SELECT value FROM settings WHERE key = 'catalog_version'")
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Connection(e.to_string()))?
                .ok_or(StorageError::CatalogNotInitialized)?
                .0;

        let found = found_str
            .parse::<extenddb_core::version::CatalogVersion>()
            .map_err(|e| StorageError::Internal(e.to_string()))?;

        if found != CATALOG_VERSION {
            return Err(StorageError::CatalogVersionMismatch {
                expected: CATALOG_VERSION.to_string(),
                found: found_str,
            });
        }
        Ok(())
    }

    /// Read the configured data database name (the file path) for the startup
    /// banner. Returns `"(not configured)"` when unset.
    pub async fn data_database_info(&self) -> String {
        db::query_as::<(String,)>("SELECT value FROM settings WHERE key = 'data_database_name'")
            .fetch_optional(&self.pool)
            .await
            .ok()
            .flatten()
            .map_or_else(|| "(not configured)".to_owned(), |(v,)| v)
    }
}
