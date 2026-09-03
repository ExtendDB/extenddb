// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Central SQLite storage engine: connection pool, write serialization, and
//! construction.
//!
//! # Concurrency model (design decision D1)
//!
//! WAL mode allows many concurrent readers alongside a single writer. SQLite
//! offers no `SERIALIZABLE` isolation knob, so to make condition-check-then-
//! write atomic and free of write-skew the engine serializes all writers
//! through `write_lock` and opens every write transaction with `BEGIN
//! IMMEDIATE` via `pool.begin_with("BEGIN IMMEDIATE")`, so the condition read
//! and its write hold SQLite's reserved write lock up front. This is
//! multi-pool-safe: the catalog/credential store opens a second pool over the
//! same file, and `BEGIN IMMEDIATE` + `busy_timeout` prevent
//! `SQLITE_BUSY_SNAPSHOT` (a deferred read-then-write whose snapshot is
//! invalidated by another pool committing) rather than surfacing it as a 500.
//! Reads run concurrently from the pool against WAL snapshots and take no lock.
//!
//! "All writers" includes the control-plane paths (CreateTable's DDL, TTL
//! metadata, tagging) and the periodic maintenance workers (table-size
//! refresh, TTL index creation, stream-record and idempotency-token cleanup),
//! not just the item write paths. A writer outside the lock contends at the
//! SQLite level instead, and when its commit is slow (a large `CREATE INDEX`,
//! a stalled fsync on a loaded CI host) a concurrent locked writer exhausts
//! `busy_timeout` and fails an unrelated request with `database is locked`,
//! which the engine maps to a 500. Measured 2026-08-27: an uncoordinated
//! writer holding the file lock fails a plain `PutItem` with exactly the
//! `InternalServerError` seen in the `run-integration-sqlite` CI flake.
//!
//! Deliberate exclusions from the lock, so the invariant stays auditable:
//! init-time bootstrap in this file (runs before the server serves traffic),
//! and the management/credential stores, which write through the separate
//! catalog pool in `lib.rs`. For a file-backed database that second pool
//! opens the same file, so its small single-row autocommit writes carry a
//! residual, much smaller, version of the same contention risk.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use extenddb_storage::error::StorageError;
use sqlx::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;
use tokio::sync::Mutex;

use crate::INDEX_PROPAGATION_DELAY_QUERY;
use crate::schema::CATALOG_VERSION;
use crate::sqlite_util::sqlite_url;

/// The SQLite storage engine. Implements `StorageEngine` (all data/metadata/
/// stream/worker/backup traits) over a single SQLite database.
#[derive(Clone)]
pub struct SqliteEngine {
    pub(crate) pool: SqlitePool,
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

impl SqliteEngine {
    /// Open (creating if necessary) the SQLite database and build the engine.
    ///
    /// `path_or_url` accepts a filesystem path, `:memory:`, or a `sqlite:` URL.
    pub async fn new(
        path_or_url: &str,
        pool_size: u32,
        region: &str,
        max_item_size_bytes: usize,
    ) -> Result<Self, StorageError> {
        let url = sqlite_url(path_or_url);
        // An in-memory database lives only inside its own connection: a second
        // connection opens a *separate* empty database. So for `:memory:` we pin
        // the pool to a single connection that is never recycled (idle and
        // lifetime timeouts disabled), guaranteeing one shared database for the
        // process lifetime. Writes are already serialized by `write_lock`, so a
        // single connection costs only read concurrency — acceptable for the
        // ephemeral in-memory use case. File-backed databases keep the full WAL
        // pool (concurrent readers alongside a single writer).
        let in_memory = url.contains(":memory:") || url.contains("mode=memory");
        let mut opts = SqlitePoolOptions::new()
            .max_connections(if in_memory { 1 } else { pool_size.max(1) })
            .min_connections(1);
        if in_memory {
            opts = opts.idle_timeout(None).max_lifetime(None);
        }
        let pool = opts
            .after_connect(|conn, _| {
                Box::pin(async move {
                    use sqlx::Executor;
                    conn.execute("PRAGMA journal_mode = WAL").await?;
                    conn.execute("PRAGMA foreign_keys = ON").await?;
                    conn.execute("PRAGMA synchronous = NORMAL").await?;
                    conn.execute("PRAGMA busy_timeout = 5000").await?;
                    conn.execute("PRAGMA cache_size = -32000").await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        // The database file stores the AES encryption key alongside the data it
        // protects (encrypted access-key secrets, password hashes). Restrict the
        // file and its WAL/SHM sidecars to owner read/write so other local users
        // or processes cannot read it and decrypt secrets. In-memory databases
        // have no file to protect.
        #[cfg(unix)]
        if !in_memory {
            use std::os::unix::fs::PermissionsExt;
            for suffix in ["", "-wal", "-shm"] {
                let f = format!("{path_or_url}{suffix}");
                if std::path::Path::new(&f).exists() {
                    let _ = std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600));
                }
            }
        }

        let initial_index_delay: u64 =
            sqlx::query_as::<_, (String,)>(INDEX_PROPAGATION_DELAY_QUERY)
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
        let key_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM settings WHERE key = 'encryption_key')",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| internal(format!("check encryption key: {e}")))?;
        if !key_exists {
            let key = helpers::generate_encryption_key();
            sqlx::query("INSERT OR IGNORE INTO settings (key, value) VALUES ('encryption_key', ?)")
                .bind(&key)
                .execute(&self.pool)
                .await
                .map_err(|e| internal(format!("store encryption key: {e}")))?;
        }

        // Default account + canonical marker. Record the default account id in
        // settings (idempotent, backfill-safe) so callers resolve it explicitly
        // rather than inferring it from account-list ordering.
        let account_id: String = match sqlx::query_scalar(
            "SELECT account_id FROM accounts ORDER BY account_id LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| internal(format!("check accounts: {e}")))?
        {
            Some(id) => id,
            None => {
                let id = helpers::generate_account_id();
                sqlx::query(
                    "INSERT OR IGNORE INTO accounts (account_id, account_name) VALUES (?, 'default')",
                )
                .bind(&id)
                .execute(&self.pool)
                .await
                .map_err(|e| internal(format!("create default account: {e}")))?;
                id
            }
        };
        sqlx::query("INSERT OR IGNORE INTO settings (key, value) VALUES ('default_account_id', ?)")
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
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM admin_users WHERE admin_name = ?)")
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
        sqlx::query("INSERT INTO admin_users (admin_name, password_hash) VALUES (?, ?)")
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
    /// cache. `SQLite` is a local file, so this is an indexed point lookup with
    /// negligible cost next to the write it precedes. On a read error the
    /// cached value (still refreshed by the poll worker) is the fallback; on
    /// success the cache is re-warmed so fallback reads stay fresh.
    pub(crate) async fn index_propagation_delay(&self) -> u64 {
        use std::sync::atomic::Ordering;
        let live: Result<Option<(String,)>, _> = sqlx::query_as(INDEX_PROPAGATION_DELAY_QUERY)
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
            sqlx::query_as("SELECT value FROM settings WHERE key = ?")
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
            sqlx::query_as("SELECT value FROM settings WHERE key = ?")
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

    /// Milliseconds to hold a new vector index in the resource-allocation phase.
    ///
    /// A test lever, zero in production, read live for the same reason the batch
    /// delay is. Held inside the detached build task rather than in the request
    /// path, because the phase is only observable to a client after `UpdateTable`
    /// has returned.
    pub(crate) async fn vector_allocation_phase_delay(&self) -> u64 {
        let live: Result<Option<(String,)>, _> =
            sqlx::query_as("SELECT value FROM settings WHERE key = ?")
                .bind(extenddb_core::settings_keys::VECTOR_ALLOCATION_PHASE_DELAY_MS)
                .fetch_optional(&self.pool)
                .await;
        match live {
            Ok(row) => row.and_then(|(v,)| v.parse::<u64>().ok()).unwrap_or(0),
            Err(e) => {
                tracing::debug!("vector_allocation_phase_delay: live read failed, using 0: {e:?}");
                0
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
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'settings')",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StorageError::Connection(e.to_string()))?;

        if !exists {
            return Err(StorageError::CatalogNotInitialized);
        }

        let found_str = sqlx::query_as::<_, (String,)>(
            "SELECT value FROM settings WHERE key = 'catalog_version'",
        )
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
        sqlx::query_as::<_, (String,)>(
            "SELECT value FROM settings WHERE key = 'data_database_name'",
        )
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten()
        .map_or_else(|| "(not configured)".to_owned(), |(v,)| v)
    }
}

#[cfg(test)]
mod d1_write_lock_tests {
    use super::SqliteEngine;
    use serde_json::json;
    use std::time::Duration;

    async fn engine() -> SqliteEngine {
        // The pool size is nominal: `SqliteEngine::new` pins in-memory
        // databases to a single connection regardless. The tests below never
        // hold a pool connection on the asserting side, so a writer that
        // (incorrectly) ignores the lock is stopped by nothing at all, which
        // is what the 200ms grace window detects.
        let engine = SqliteEngine::new(":memory:", 2, "us-east-1", 409_600)
            .await
            .expect("engine");
        crate::schema::apply(&engine.pool).await.expect("schema");
        sqlx::query(
            "INSERT INTO accounts (account_id, account_name) VALUES ('000000000000', 'default')",
        )
        .execute(&engine.pool)
        .await
        .expect("account");
        // Zero control-plane delay: tables become ACTIVE at create time, since
        // no transition poller runs inside a unit test.
        sqlx::query(
            "INSERT OR REPLACE INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0')",
        )
        .execute(&engine.pool)
        .await
        .expect("settings");
        engine
    }

    /// Assert the D1 invariant for one writer: while the engine write lock is
    /// held, the writer must not complete; after release, it must.
    ///
    /// This is the discriminating shape for the 2026-08-27 `run-integration-sqlite`
    /// flake (`PutItem` returning `InternalServerError`, server-side `database is
    /// locked`): a writer outside the lock contends at the SQLite level, where a
    /// slow commit exhausts a concurrent writer's 5s `busy_timeout`. Before the
    /// fix, each writer below completed while the lock was held; with it, they
    /// queue behind the lock and cannot collide.
    async fn assert_serialized<F>(engine: &SqliteEngine, writer: F, name: &str)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let guard = engine.write_lock.lock().await;
        let task = tokio::spawn(writer);
        // Generous grace period: a writer that ignores the lock finishes these
        // single-statement transactions in well under 200ms.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !task.is_finished(),
            "{name} completed while the engine write lock was held (D1 violation)"
        );
        drop(guard);
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap_or_else(|_| panic!("{name} did not complete after lock release"))
            .expect("writer task panicked");
    }

    #[tokio::test]
    async fn create_table_waits_for_the_write_lock() {
        let engine = engine().await;
        let e = engine.clone();
        assert_serialized(
            &engine,
            async move {
                create_table(&e, "d1-lock-t").await;
            },
            "create_table_impl",
        )
        .await;
    }

    #[tokio::test]
    async fn idempotency_token_cleanup_waits_for_the_write_lock() {
        let engine = engine().await;
        let e = engine.clone();
        assert_serialized(
            &engine,
            async move {
                e.cleanup_expired_idempotency_tokens_impl(0)
                    .await
                    .expect("cleanup");
            },
            "cleanup_expired_idempotency_tokens",
        )
        .await;
    }

    #[tokio::test]
    async fn tag_resource_waits_for_the_write_lock() {
        use extenddb_storage::MetadataEngine;
        let engine = engine().await;
        let e = engine.clone();
        assert_serialized(
            &engine,
            async move {
                MetadataEngine::tag_resource(
                    &e,
                    "arn:aws:dynamodb:us-east-1:000000000000:table/d1",
                    &[extenddb_core::types::Tag {
                        key: "k".to_owned(),
                        value: "v".to_owned(),
                    }],
                )
                .await
                .expect("tag");
            },
            "tag_resource",
        )
        .await;
    }

    #[tokio::test]
    async fn stream_record_cleanup_waits_for_the_write_lock() {
        use extenddb_storage::StreamEngine;
        let engine = engine().await;
        let e = engine.clone();
        assert_serialized(
            &engine,
            async move {
                StreamEngine::cleanup_expired_stream_records(&e, 0)
                    .await
                    .expect("cleanup");
            },
            "cleanup_expired_stream_records",
        )
        .await;
    }

    /// Create a plain table outside the lock window, for the writers whose
    /// pre-lock reads refuse to proceed without one.
    async fn create_table(engine: &SqliteEngine, name: &str) {
        let input: extenddb_core::types::CreateTableInput = serde_json::from_value(json!({
            "TableName": name,
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }))
        .expect("input");
        engine
            .create_table_impl("000000000000", input)
            .await
            .expect("create table");
    }

    #[tokio::test]
    async fn delete_backup_waits_for_the_write_lock() {
        use extenddb_storage::BackupEngine;
        let engine = engine().await;
        create_table(&engine, "d1-bkp-t").await;
        // The backup must exist before the lock window: delete_backup resolves
        // it with a read first and returns early when it is missing, which
        // would complete without ever reaching the writes under test.
        let details = BackupEngine::create_backup(&engine, "000000000000", "d1-bkp-t", "b")
            .await
            .expect("backup");
        let e = engine.clone();
        assert_serialized(
            &engine,
            async move {
                BackupEngine::delete_backup(&e, "000000000000", &details.backup_arn)
                    .await
                    .expect("delete backup");
            },
            "delete_backup",
        )
        .await;
    }

    #[tokio::test]
    async fn update_continuous_backups_waits_for_the_write_lock() {
        use extenddb_storage::BackupEngine;
        let engine = engine().await;
        // The table must exist before the lock window: the pre-lock existence
        // check returns TableNotFound otherwise, completing without reaching
        // the write under test.
        create_table(&engine, "d1-pitr-t").await;
        let e = engine.clone();
        assert_serialized(
            &engine,
            async move {
                BackupEngine::update_continuous_backups(&e, "000000000000", "d1-pitr-t", true)
                    .await
                    .expect("update continuous backups");
            },
            "update_continuous_backups",
        )
        .await;
    }
}
