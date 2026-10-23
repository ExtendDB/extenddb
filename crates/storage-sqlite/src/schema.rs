// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authoritative SQLite catalog schema.
//!
//! Mirrors the semantics of the reference PostgreSQL catalog schema
//! (`crates/storage-postgres/migrations/001_schema.sql`, catalog version
//! 0.0.2). Table and column names are kept identical to PostgreSQL so the
//! catalog/management stores remain portable across backends.
//!
//! Type mapping PostgreSQL → SQLite:
//! - `JSONB`        → `TEXT` (JSON serialized as a string)
//! - `BYTEA`        → `BLOB`
//! - `BOOLEAN`      → `INTEGER` (0/1)
//! - `BIGINT`       → `INTEGER`
//! - `DOUBLE PRECISION` → `REAL` (`±9e999` literals store as `±Infinity`)
//! - `TIMESTAMPTZ`  → `TEXT` in RFC 3339 UTC (`YYYY-MM-DDTHH:MM:SS.sssZ`)
//! - `SEQUENCE`     → a `seq_counters` row updated inside the write transaction
//!
//! Per-DynamoDB-table data tables (`_ddb_<table_id>`) are NOT created here; they
//! are created dynamically by `create_table`, exactly as in the PostgreSQL
//! backend.

use extenddb_storage::management_store::{OpError, OpResult};
use sqlx::SqlitePool;

/// Compiled-in catalog version. Single source of truth for the SQLite backend;
/// mirrors the PostgreSQL backend's `CATALOG_VERSION`.
pub const CATALOG_VERSION: extenddb_core::version::CatalogVersion =
    extenddb_core::version::CatalogVersion::new(0, 0, 4);

/// Complete catalog schema, applied once on a fresh database.
///
/// Every object uses `IF NOT EXISTS` so re-application is harmless. Foreign-key
/// enforcement requires `PRAGMA foreign_keys = ON` on each connection (set in
/// the engine's `after_connect` hook).
pub const SCHEMA_SQL: &str = r#"
-- Accounts — multi-account support (REQ-AUTH-005).
CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    account_name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- Table metadata.
CREATE TABLE IF NOT EXISTS tables (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    table_name TEXT NOT NULL,
    key_schema TEXT NOT NULL,
    attribute_definitions TEXT NOT NULL,
    billing_mode TEXT NOT NULL DEFAULT 'PAY_PER_REQUEST',
    provisioned_throughput TEXT,
    stream_specification TEXT,
    table_status TEXT NOT NULL DEFAULT 'CREATING',
    creation_date_time TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    table_size_bytes INTEGER NOT NULL DEFAULT 0,
    item_count INTEGER NOT NULL DEFAULT 0,
    table_arn TEXT NOT NULL,
    table_id TEXT NOT NULL,
    ttl_attribute TEXT,
    deletion_protection_enabled INTEGER NOT NULL DEFAULT 0,
    status_transition_at TEXT,
    stream_label TEXT,
    ttl_index_ready INTEGER NOT NULL DEFAULT 0,
    table_class TEXT,
    sse_specification TEXT,
    on_demand_throughput TEXT,
    PRIMARY KEY (account_id, table_name),
    CONSTRAINT tables_table_id_unique UNIQUE (table_id)
);

CREATE INDEX IF NOT EXISTS idx_tables_pending_transition
    ON tables (status_transition_at)
    WHERE status_transition_at IS NOT NULL;

-- Index metadata. index_id is always supplied by the engine (no DB-side UUID).
CREATE TABLE IF NOT EXISTS indexes (
    table_id TEXT NOT NULL,
    index_id TEXT NOT NULL,
    index_name TEXT NOT NULL,
    index_type TEXT NOT NULL,
    key_schema TEXT NOT NULL,
    projection TEXT NOT NULL,
    index_status TEXT NOT NULL DEFAULT 'ACTIVE',
    provisioned_throughput TEXT,
    propagation_delay_ms INTEGER,
    PRIMARY KEY (table_id, index_name),
    CONSTRAINT indexes_table_id_fkey
        FOREIGN KEY (table_id) REFERENCES tables(table_id) ON DELETE CASCADE,
    CONSTRAINT chk_propagation_delay_ms_non_negative
        CHECK (propagation_delay_ms IS NULL OR propagation_delay_ms >= 0)
);

-- Vector index metadata. Kept out of `indexes` deliberately: a vector index is
-- not described by a key schema, so reusing that table's `key_schema` column
-- would mean storing something meaningless in a NOT NULL column. The engine
-- supplies index_id, as it does for GSIs.
--
-- `search_schema` is nullable because the HASH element is optional (measured
-- against the live service): with one the search is partition-scoped and
-- SearchConditionExpression is required, without one it spans the table.
--
-- `backfilling` mirrors the measured lifecycle: false while CREATING before the
-- scan starts, true while it runs, and the member is absent once ACTIVE. Stored
-- as an integer so the ACTIVE state is representable as NULL rather than as a
-- third boolean value.
CREATE TABLE IF NOT EXISTS vector_indexes (
    table_id TEXT NOT NULL,
    index_id TEXT NOT NULL,
    index_name TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    distance_function TEXT NOT NULL,
    vector_attribute TEXT NOT NULL,
    search_schema TEXT,
    projection TEXT NOT NULL,
    index_status TEXT NOT NULL DEFAULT 'CREATING',
    backfilling INTEGER,
    -- Items the backfill skipped because their stored bytes cannot enter the
    -- index (unparseable row, malformed or wrong-dimension vector). NULL until
    -- a backfill has completed; 0 afterwards when nothing was skipped. Kept so
    -- an operator can see that an ACTIVE index deliberately omits rows, rather
    -- than the build looping forever on them or dying part-way.
    skipped_item_count INTEGER,
    PRIMARY KEY (table_id, index_name),
    CONSTRAINT vector_indexes_table_id_fkey
        FOREIGN KEY (table_id) REFERENCES tables(table_id) ON DELETE CASCADE,
    CONSTRAINT chk_vector_dimensions_positive CHECK (dimensions > 0),
    CONSTRAINT chk_vector_backfilling_bool
        CHECK (backfilling IS NULL OR backfilling IN (0, 1)),
    -- An ACTIVE index must not carry the member at all, which is what the
    -- service does. Enforced here as well as in core, so a bug in the backend
    -- cannot persist a state the wire contract forbids.
    CONSTRAINT chk_vector_active_has_no_backfilling
        CHECK (index_status <> 'ACTIVE' OR backfilling IS NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_vector_indexes_index_id
    ON vector_indexes (index_id);

-- Resource tags.
CREATE TABLE IF NOT EXISTS tags (
    resource_arn TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (resource_arn, tag_key)
);

-- Migration tracking.
CREATE TABLE IF NOT EXISTS schema_history (
    filename TEXT PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- Settings (catalog version, data database name, runtime config).
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Stream shards.
CREATE TABLE IF NOT EXISTS stream_shards (
    shard_id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL REFERENCES tables(table_id) ON DELETE CASCADE,
    parent_shard_id TEXT,
    starting_sequence_number TEXT NOT NULL,
    ending_sequence_number TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_stream_shards_table ON stream_shards (table_id);

-- Stream records.
CREATE TABLE IF NOT EXISTS stream_records (
    shard_id TEXT NOT NULL REFERENCES stream_shards(shard_id) ON DELETE CASCADE,
    sequence_number TEXT NOT NULL,
    table_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    record_data TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (shard_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_stream_records_created ON stream_records (created_at);

-- Admin users.
CREATE TABLE IF NOT EXISTS admin_users (
    admin_name TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

-- IAM users.
CREATE TABLE IF NOT EXISTS iam_users (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    user_name TEXT NOT NULL,
    user_arn TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (account_id, user_name)
);

-- IAM user tags.
CREATE TABLE IF NOT EXISTS iam_user_tags (
    account_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (account_id, user_name, tag_key),
    FOREIGN KEY (account_id, user_name)
        REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);

-- Access keys.
CREATE TABLE IF NOT EXISTS access_keys (
    access_key_id TEXT PRIMARY KEY,
    secret_key_encrypted BLOB NOT NULL,
    account_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    FOREIGN KEY (account_id, user_name)
        REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);

-- IAM groups.
CREATE TABLE IF NOT EXISTS iam_groups (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    group_name TEXT NOT NULL,
    group_arn TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (account_id, group_name)
);

-- IAM group membership.
CREATE TABLE IF NOT EXISTS iam_group_members (
    account_id TEXT NOT NULL,
    group_name TEXT NOT NULL,
    user_name TEXT NOT NULL,
    PRIMARY KEY (account_id, group_name, user_name),
    FOREIGN KEY (account_id, group_name)
        REFERENCES iam_groups(account_id, group_name) ON DELETE CASCADE,
    FOREIGN KEY (account_id, user_name)
        REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);

-- IAM roles.
CREATE TABLE IF NOT EXISTS iam_roles (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    role_name TEXT NOT NULL,
    role_arn TEXT NOT NULL UNIQUE,
    trust_policy TEXT NOT NULL,
    permissions_boundary_arn TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (account_id, role_name)
);

-- IAM role tags.
CREATE TABLE IF NOT EXISTS iam_role_tags (
    account_id TEXT NOT NULL,
    role_name TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (account_id, role_name, tag_key),
    FOREIGN KEY (account_id, role_name)
        REFERENCES iam_roles(account_id, role_name) ON DELETE CASCADE
);

-- IAM sessions.
CREATE TABLE IF NOT EXISTS iam_sessions (
    session_token TEXT PRIMARY KEY,
    access_key_id TEXT NOT NULL UNIQUE,
    secret_key_encrypted BLOB NOT NULL,
    account_id TEXT NOT NULL,
    role_name TEXT NOT NULL,
    session_name TEXT NOT NULL,
    session_tags TEXT,
    session_policy TEXT,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    FOREIGN KEY (account_id, role_name)
        REFERENCES iam_roles(account_id, role_name) ON DELETE CASCADE
);

-- IAM policies.
CREATE TABLE IF NOT EXISTS iam_policies (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    principal_type TEXT NOT NULL CHECK (principal_type IN ('user', 'group', 'role')),
    principal_name TEXT NOT NULL,
    policy_name TEXT NOT NULL,
    policy_document TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (account_id, principal_type, principal_name, policy_name)
);

-- IAM permissions boundaries.
CREATE TABLE IF NOT EXISTS iam_permissions_boundaries (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    principal_type TEXT NOT NULL CHECK (principal_type IN ('user', 'role')),
    principal_name TEXT NOT NULL,
    policy_document TEXT NOT NULL,
    PRIMARY KEY (account_id, principal_type, principal_name)
);

-- Idempotency tokens for TransactWriteItems.
CREATE TABLE IF NOT EXISTS idempotency_tokens (
    account_id  TEXT NOT NULL,
    token       TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (account_id, token)
);

CREATE INDEX IF NOT EXISTS idx_idempotency_tokens_created ON idempotency_tokens (created_at);

-- Metrics (1-minute aggregation). ±9e999 store as ±Infinity in SQLite REAL,
-- matching PostgreSQL's Infinity / -Infinity seed for min / max.
CREATE TABLE IF NOT EXISTS metrics (
    bucket TEXT NOT NULL,
    metric TEXT NOT NULL,
    table_name TEXT NOT NULL DEFAULT '',
    index_name TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL DEFAULT '',
    sum REAL NOT NULL DEFAULT 0,
    count INTEGER NOT NULL DEFAULT 0,
    min REAL NOT NULL DEFAULT 9e999,
    max REAL NOT NULL DEFAULT -9e999,
    PRIMARY KEY (bucket, metric, table_name, index_name, operation)
);

CREATE INDEX IF NOT EXISTS idx_metrics_bucket ON metrics (bucket);

-- Login attempt tracking.
CREATE TABLE IF NOT EXISTS login_attempts (
    principal     TEXT NOT NULL,
    attempted_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    success       INTEGER NOT NULL,
    source_ip     TEXT
);

CREATE INDEX IF NOT EXISTS idx_login_attempts_principal_time
    ON login_attempts (principal, attempted_at DESC);

CREATE INDEX IF NOT EXISTS idx_login_attempts_source_ip_time
    ON login_attempts (source_ip, attempted_at DESC)
    WHERE source_ip IS NOT NULL;

-- Backup metadata.
CREATE TABLE IF NOT EXISTS backups (
    backup_arn TEXT PRIMARY KEY,
    backup_name TEXT NOT NULL,
    table_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    account_id TEXT NOT NULL,
    backup_status TEXT NOT NULL DEFAULT 'AVAILABLE',
    backup_type TEXT NOT NULL DEFAULT 'USER',
    backup_size_bytes INTEGER NOT NULL DEFAULT 0,
    item_count INTEGER NOT NULL DEFAULT 0,
    key_schema TEXT NOT NULL,
    attribute_definitions TEXT NOT NULL,
    billing_mode TEXT NOT NULL DEFAULT 'PAY_PER_REQUEST',
    provisioned_throughput TEXT,
    stream_specification TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_backups_table ON backups (account_id, table_name);

-- Backup items. `id` is the stable restore cursor: unlike an implicit rowid,
-- an INTEGER PRIMARY KEY value is not renumbered by VACUUM.
CREATE TABLE IF NOT EXISTS backup_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    backup_arn TEXT NOT NULL REFERENCES backups(backup_arn) ON DELETE CASCADE,
    pk TEXT NOT NULL,
    sk TEXT,
    item_data TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_backup_items_arn ON backup_items (backup_arn);

-- A backup's table definition beyond its keys: secondary indexes, billing
-- mode and throughput, table class, encryption (catalog 0.0.4). JSON in the
-- wire's shape behind a version marker; see
-- `extenddb_storage::backup_definition`. A backup taken before 0.0.4 has no
-- row and restores as before.
CREATE TABLE IF NOT EXISTS backup_definitions (
    backup_arn TEXT PRIMARY KEY REFERENCES backups(backup_arn) ON DELETE CASCADE,
    definition TEXT NOT NULL
);

-- Which backup a restored table came from (catalog 0.0.4): reported by
-- DescribeTable as RestoreSummary, and used to refuse DeleteBackup while the
-- restore is running. Not a foreign key to backups, which may be deleted later.
-- `restore_date_time` is the source backup's creation time, as the service
-- reports it, supplied by the engine. `source_table_arn` is nullable only so
-- that `migrate_table_restores` can add it to a database an earlier 0.0.4
-- build created; every row this build writes sets it.
CREATE TABLE IF NOT EXISTS table_restores (
    table_id TEXT PRIMARY KEY REFERENCES tables(table_id) ON DELETE CASCADE,
    source_backup_arn TEXT NOT NULL,
    source_table_arn TEXT,
    restore_date_time TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_table_restores_backup ON table_restores (source_backup_arn);

-- Continuous backups / PITR status.
CREATE TABLE IF NOT EXISTS continuous_backups (
    account_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    pitr_enabled INTEGER NOT NULL DEFAULT 0,
    earliest_restorable TEXT,
    latest_restorable TEXT,
    PRIMARY KEY (account_id, table_name)
);

-- Persistent queue for async GSI propagation. A row is inserted inside the
-- base write transaction (zero crash window) and consumed by a background
-- worker once `ready_at` has passed; survives process crash/restart.
--
-- One row per async index: each row is self-describing — `index_context`
-- carries the base key schema, attribute definitions, and the single target
-- index definition captured at enqueue, so the worker applies with zero
-- catalog reads. `worker_partition` is a stable hash of the base table key;
-- all updates to a given base item share a partition and `ready_at` is kept
-- monotonically non-decreasing within it, so the worker (which drains in `id`
-- order) preserves per-key FIFO even with randomized propagation jitter.
CREATE TABLE IF NOT EXISTS gsi_pending (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_id TEXT NOT NULL,
    worker_partition INTEGER NOT NULL,
    old_item TEXT,
    new_item TEXT,
    index_context TEXT NOT NULL,
    ready_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_gsi_pending_claim
    ON gsi_pending (worker_partition, ready_at, id);

-- Monotonic counters. Replaces PostgreSQL sequences. The stream counter is
-- seeded to the current epoch in microseconds so sequence numbers are
-- time-ordered and survive restarts (mirrors the PostgreSQL setval seed).
CREATE TABLE IF NOT EXISTS seq_counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO seq_counters (name, value)
    VALUES ('stream', CAST(strftime('%s','now') AS INTEGER) * 1000000);

-- Seed settings (mirror PostgreSQL defaults).
-- Recorded catalog version. Upserts rather than INSERT OR IGNORE: this schema is
-- re-applied by `extenddb migrate`, and with IGNORE the recorded version would
-- never advance, so a migration could add objects and still leave the server
-- refusing to start on a version mismatch. Must stay in step with
-- `CATALOG_VERSION` above; they are checked against each other in a test.
INSERT INTO settings (key, value) VALUES ('catalog_version', '0.0.4')
    ON CONFLICT(key) DO UPDATE SET value = excluded.value;
INSERT OR IGNORE INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0.25');
INSERT OR IGNORE INTO settings (key, value) VALUES ('index_propagation_delay_ms', '10');
INSERT OR IGNORE INTO settings (key, value) VALUES ('resource_tag_migration', 'applied');
"#;

/// Apply the full catalog schema to a fresh (or existing) database.
///
/// Idempotent: every statement uses `IF NOT EXISTS` / `INSERT OR IGNORE`.
pub async fn apply(pool: &SqlitePool) -> OpResult<()> {
    refuse_unwritable_catalog(pool).await?;
    // Catalog 0.0.4 originally created backup_items without a stable cursor
    // and table_restores without source_table_arn. Converge both before
    // SCHEMA_SQL stamps 0.0.4, including databases that an earlier
    // 0.0.4-in-progress binary already stamped. SQLite cannot add an INTEGER
    // PRIMARY KEY with ALTER TABLE.
    converge_table_shapes(pool).await?;
    sqlx::raw_sql(SCHEMA_SQL)
        .execute(pool)
        .await
        .map_err(|e| OpError::Internal(format!("apply catalog schema: {e}")))?;
    Ok(())
}

/// Refuse to write to a catalog whose recorded version is newer than this
/// binary's or cannot be parsed.
async fn refuse_unwritable_catalog(pool: &SqlitePool) -> OpResult<()> {
    // The schema's last statement stamps the compiled version. Never stamp an
    // older version onto a catalog a newer binary has already upgraded: that
    // binary's startup gate would keep passing and this one's would start
    // passing against tables it does not know. `extenddb migrate` refuses such
    // a catalog before getting here; this guards every other caller.
    if table_exists(pool, "settings").await? {
        let stored: Option<(String,)> =
            sqlx::query_as("SELECT value FROM settings WHERE key = 'catalog_version'")
                .fetch_optional(pool)
                .await
                .map_err(|e| OpError::Internal(format!("read catalog_version: {e}")))?;
        // Also refuses a stored version this binary cannot parse, rather than
        // stamping over it (shared rule; see check_stored_catalog_version).
        extenddb_storage::util::check_stored_catalog_version(
            stored.as_ref().map(|(v,)| v.as_str()),
            CATALOG_VERSION,
        )
        .map_err(OpError::Internal)?;
    }
    Ok(())
}

/// Bring tables an earlier 0.0.4 build created to this build's shape. Each
/// step is a no-op when the table is absent or already converged.
async fn converge_table_shapes(pool: &SqlitePool) -> OpResult<()> {
    migrate_backup_items(pool).await?;
    migrate_table_restores(pool).await?;
    Ok(())
}

/// Run only the table-shape steps of [`apply`], under the same version guard.
/// `extenddb migrate` reaches this through `run_data_migrations` when the
/// catalog is already stamped 0.0.4 but a table still has an earlier 0.0.4
/// build's shape, which a version comparison alone cannot see.
pub async fn apply_shape_migrations(pool: &SqlitePool) -> OpResult<()> {
    refuse_unwritable_catalog(pool).await?;
    converge_table_shapes(pool).await
}

/// The table-shape steps [`apply`] would run, one line each, without
/// writing anything. Empty when every table is in this build's shape (or
/// the catalog does not exist yet).
pub async fn pending_shape_migrations(pool: &SqlitePool) -> OpResult<Vec<String>> {
    let mut pending = Vec::new();
    if backup_items_needs_rebuild(pool).await? {
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM backup_items")
            .fetch_one(pool)
            .await
            .map_err(|e| OpError::Internal(format!("count backup_items: {e}")))?;
        pending.push(format!("rebuild backup_items ({rows} rows)"));
    }
    if table_restores_lacks_source_table_arn(pool).await? {
        pending.push("add table_restores.source_table_arn".to_owned());
    }
    Ok(pending)
}

async fn has_column(pool: &SqlitePool, table: &'static str, column: &str) -> OpResult<bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pragma_table_info(?) WHERE name = ?)")
        .bind(table)
        .bind(column)
        .fetch_one(pool)
        .await
        .map_err(|e| OpError::Internal(format!("inspect {table} columns: {e}")))
}

async fn backup_items_needs_rebuild(pool: &SqlitePool) -> OpResult<bool> {
    Ok(
        table_exists(pool, "backup_items").await?
            && !has_column(pool, "backup_items", "id").await?,
    )
}

async fn table_restores_lacks_source_table_arn(pool: &SqlitePool) -> OpResult<bool> {
    Ok(table_exists(pool, "table_restores").await?
        && !has_column(pool, "table_restores", "source_table_arn").await?)
}

/// Add `source_table_arn` to a `table_restores` an earlier 0.0.4 build
/// created. Rows it wrote keep NULL, which DescribeTable reports as an
/// absent `SourceTableArn`.
async fn migrate_table_restores(pool: &SqlitePool) -> OpResult<()> {
    if !table_restores_lacks_source_table_arn(pool).await? {
        return Ok(());
    }
    tracing::info!("catalog upgrade: adding table_restores.source_table_arn");
    sqlx::query("ALTER TABLE table_restores ADD COLUMN source_table_arn TEXT")
        .execute(pool)
        .await
        .map_err(|e| OpError::Internal(format!("add table_restores.source_table_arn: {e}")))?;
    Ok(())
}

async fn migrate_backup_items(pool: &SqlitePool) -> OpResult<()> {
    if !backup_items_needs_rebuild(pool).await? {
        return Ok(());
    }
    let started = std::time::Instant::now();

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| OpError::Internal(format!("begin backup_items migration: {e}")))?;
    // Counted inside the rebuild's transaction, so the logged count is the
    // number of rows copied.
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM backup_items")
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| OpError::Internal(format!("count backup_items: {e}")))?;
    tracing::info!(
        "catalog upgrade: rebuilding backup_items with a stable restore cursor ({rows} rows); \
         the database is locked for writes until this finishes"
    );
    sqlx::query("DROP TABLE IF EXISTS backup_items_new")
        .execute(&mut *tx)
        .await
        .map_err(|e| OpError::Internal(format!("remove stale backup_items_new: {e}")))?;
    sqlx::query(
        "CREATE TABLE backup_items_new (\
         id INTEGER PRIMARY KEY AUTOINCREMENT, \
         backup_arn TEXT NOT NULL REFERENCES backups(backup_arn) ON DELETE CASCADE, \
         pk TEXT NOT NULL, sk TEXT, item_data TEXT NOT NULL)",
    )
    .execute(&mut *tx)
    .await
    .map_err(|e| OpError::Internal(format!("create backup_items_new: {e}")))?;
    sqlx::query(
        "INSERT INTO backup_items_new (backup_arn, pk, sk, item_data) \
         SELECT backup_arn, pk, sk, item_data FROM backup_items ORDER BY rowid",
    )
    .execute(&mut *tx)
    .await
    .map_err(|e| OpError::Internal(format!("copy backup_items: {e}")))?;
    sqlx::query("DROP INDEX IF EXISTS idx_backup_items_arn")
        .execute(&mut *tx)
        .await
        .map_err(|e| OpError::Internal(format!("drop old backup_items index: {e}")))?;
    sqlx::query("DROP TABLE backup_items")
        .execute(&mut *tx)
        .await
        .map_err(|e| OpError::Internal(format!("drop old backup_items: {e}")))?;
    sqlx::query("ALTER TABLE backup_items_new RENAME TO backup_items")
        .execute(&mut *tx)
        .await
        .map_err(|e| OpError::Internal(format!("rename backup_items_new: {e}")))?;
    sqlx::query("CREATE INDEX idx_backup_items_arn ON backup_items (backup_arn)")
        .execute(&mut *tx)
        .await
        .map_err(|e| OpError::Internal(format!("recreate backup_items index: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| OpError::Internal(format!("commit backup_items migration: {e}")))?;
    tracing::info!(
        "catalog upgrade: rebuilt backup_items ({rows} rows) in {:.3}s",
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Return whether a table with the given name exists in the catalog.
pub async fn table_exists(pool: &SqlitePool, name: &str) -> OpResult<bool> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)",
    )
    .bind(name)
    .fetch_one(pool)
    .await
    .map_err(|e| OpError::Internal(format!("table_exists({name}): {e}")))?;
    Ok(exists)
}

#[cfg(test)]
mod tests {
    use super::{CATALOG_VERSION, SCHEMA_SQL};

    /// The schema seeds the recorded catalog version as a SQL literal, and the
    /// server compares that recorded value against `CATALOG_VERSION` at startup.
    /// If the two drift, a freshly initialised deployment refuses to serve with a
    /// version mismatch, which is a confusing failure a long way from its cause.
    #[test]
    fn the_seeded_catalog_version_matches_the_compiled_constant() {
        let expected = format!(
            "INSERT INTO settings (key, value) VALUES ('catalog_version', '{CATALOG_VERSION}')"
        );
        assert!(
            SCHEMA_SQL.contains(&expected),
            "schema must seed catalog_version = {CATALOG_VERSION}; \
             update the literal in SCHEMA_SQL when bumping CATALOG_VERSION"
        );
    }

    /// The seed must upsert. With `INSERT OR IGNORE` the recorded version never
    /// advances, so `extenddb migrate` would add the new objects and still leave
    /// the server refusing to start.
    #[test]
    fn the_catalog_version_seed_upserts_rather_than_ignoring() {
        assert!(
            !SCHEMA_SQL
                .contains("INSERT OR IGNORE INTO settings (key, value) VALUES ('catalog_version'"),
            "catalog_version must not be seeded with INSERT OR IGNORE"
        );
    }

    /// A fresh catalog seeds the resource-tag migration marker, so a
    /// newly-initialised deployment serves without a separate migrate step.
    /// An older catalog that lacks it is caught by the server's startup guard.
    #[test]
    fn the_schema_seeds_the_resource_tag_migration_marker() {
        assert!(
            SCHEMA_SQL.contains(
                "INSERT OR IGNORE INTO settings (key, value) VALUES ('resource_tag_migration'"
            ),
            "schema must seed the resource_tag_migration marker on a fresh catalog"
        );
    }

    #[tokio::test]
    async fn applying_the_schema_records_the_marker_row() {
        use sqlx::sqlite::SqlitePoolOptions;
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        super::apply(&pool).await.unwrap();
        let value: Option<(String,)> =
            sqlx::query_as("SELECT value FROM settings WHERE key = 'resource_tag_migration'")
                .fetch_optional(&pool)
                .await
                .unwrap();
        assert_eq!(
            value.map(|(v,)| v).as_deref(),
            Some(extenddb_core::settings_keys::RESOURCE_TAG_MIGRATION_DONE)
        );
    }
}

#[cfg(test)]
mod apply_tests {
    use super::{CATALOG_VERSION, apply};
    use sqlx::sqlite::SqlitePoolOptions;

    #[tokio::test]
    async fn a_newer_catalog_is_not_stamped_down() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("pool");
        apply(&pool).await.expect("fresh apply");
        let newer = format!("{}.{}.{}", 0, 0, 999);
        sqlx::query("UPDATE settings SET value = ? WHERE key = 'catalog_version'")
            .bind(&newer)
            .execute(&pool)
            .await
            .expect("stamp newer");
        let err = apply(&pool).await.expect_err("re-apply on a newer catalog");
        assert!(
            format!("{err:?}").contains("newer than this binary"),
            "{err:?}"
        );
        let (stored,): (String,) =
            sqlx::query_as("SELECT value FROM settings WHERE key = 'catalog_version'")
                .fetch_one(&pool)
                .await
                .expect("read");
        assert_eq!(stored, newer, "stored version must be untouched");
        assert!(stored != CATALOG_VERSION.to_string());
    }

    #[tokio::test]
    async fn an_unparseable_catalog_version_is_not_overwritten() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("pool");
        apply(&pool).await.expect("fresh apply");
        sqlx::query("UPDATE settings SET value = 'garbage' WHERE key = 'catalog_version'")
            .execute(&pool)
            .await
            .expect("stamp garbage");
        let err = apply(&pool)
            .await
            .expect_err("re-apply on an unreadable version");
        assert!(
            format!("{err:?}").contains("refusing to overwrite"),
            "{err:?}"
        );
        let (stored,): (String,) =
            sqlx::query_as("SELECT value FROM settings WHERE key = 'catalog_version'")
                .fetch_one(&pool)
                .await
                .expect("read");
        assert_eq!(stored, "garbage", "stored version must be untouched");
    }

    #[tokio::test]
    async fn backup_items_migration_is_idempotent_and_preserves_row_order() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("pool");
        sqlx::raw_sql(
            "CREATE TABLE backups (\
                 backup_arn TEXT PRIMARY KEY, account_id TEXT NOT NULL, table_name TEXT NOT NULL); \
             CREATE TABLE backup_items (\
                 backup_arn TEXT NOT NULL REFERENCES backups(backup_arn) ON DELETE CASCADE, \
                 pk TEXT NOT NULL, sk TEXT, item_data TEXT NOT NULL); \
             CREATE INDEX idx_backup_items_arn ON backup_items (backup_arn); \
             INSERT INTO backups (backup_arn, account_id, table_name) VALUES ('a', '0', 't'); \
             INSERT INTO backup_items (backup_arn, pk, sk, item_data) VALUES \
                 ('a', 'z', NULL, 'first'), \
                 ('a', 'a', NULL, 'second'), \
                 ('a', 'm', NULL, 'third');",
        )
        .execute(&pool)
        .await
        .expect("old schema");

        apply(&pool).await.expect("first migration");
        let rows: Vec<(i64, String, String)> =
            sqlx::query_as("SELECT id, pk, item_data FROM backup_items ORDER BY id")
                .fetch_all(&pool)
                .await
                .expect("migrated rows");
        assert_eq!(
            rows,
            vec![
                (1, "z".to_owned(), "first".to_owned()),
                (2, "a".to_owned(), "second".to_owned()),
                (3, "m".to_owned(), "third".to_owned()),
            ]
        );

        apply(&pool).await.expect("second migration");
        let after: Vec<(i64, String, String)> =
            sqlx::query_as("SELECT id, pk, item_data FROM backup_items ORDER BY id")
                .fetch_all(&pool)
                .await
                .expect("rows after second apply");
        assert_eq!(after, rows);
        let primary_key: i64 = sqlx::query_scalar(
            "SELECT pk FROM pragma_table_info('backup_items') WHERE name = 'id'",
        )
        .fetch_one(&pool)
        .await
        .expect("id column");
        assert_eq!(primary_key, 1);
    }
}

/// Collect the `tracing` events emitted on the current thread, for tests
/// that assert an operator-facing log line. Each entry is `"<LEVEL> <message>"`.
///
/// One process-wide subscriber, installed on first use, forwards each event
/// to the buffer of the thread that emitted it, if that thread is capturing.
/// A global subscriber rather than `set_default`: callsite interest is cached
/// process-wide, and with per-thread subscribers a test on another thread
/// could leave a callsite cached as disabled.
#[cfg(test)]
pub(crate) mod log_capture {
    use std::cell::RefCell;
    use std::sync::{Arc, Mutex};

    type Lines = Arc<Mutex<Vec<String>>>;

    thread_local! {
        static CAPTURING: RefCell<Option<Lines>> = const { RefCell::new(None) };
    }

    struct Forward;

    struct Message(String);

    impl tracing::field::Visit for Message {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0 = format!("{value:?}");
            }
        }
    }

    impl tracing::Subscriber for Forward {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            CAPTURING.with(|c| {
                if let Some(lines) = c.borrow().as_ref() {
                    let mut message = Message(String::new());
                    event.record(&mut message);
                    if let Ok(mut lines) = lines.lock() {
                        lines.push(format!("{} {}", event.metadata().level(), message.0));
                    }
                }
            });
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// Stops capturing on drop.
    pub(crate) struct Guard;

    impl Drop for Guard {
        fn drop(&mut self) {
            CAPTURING.with(|c| c.borrow_mut().take());
        }
    }

    /// Capture this thread's events until the guard is dropped. Use it from a
    /// current-thread runtime (the `#[tokio::test]` default).
    pub(crate) fn capture() -> (Guard, Lines) {
        static INSTALL: std::sync::Once = std::sync::Once::new();
        INSTALL.call_once(|| {
            tracing::subscriber::set_global_default(Forward)
                .expect("no other global subscriber in this test binary");
        });
        let lines = Lines::default();
        CAPTURING.with(|c| *c.borrow_mut() = Some(Arc::clone(&lines)));
        (Guard, lines)
    }
}

#[cfg(test)]
mod shape_tests {
    use super::{apply, pending_shape_migrations};
    use sqlx::sqlite::SqlitePoolOptions;

    /// A database an earlier 0.0.4 build created has `table_restores` without
    /// `source_table_arn`, already stamped 0.0.4. The dry run names the
    /// column, apply adds it once and keeps the row, and a second apply is a
    /// no-op.
    #[tokio::test]
    async fn an_earlier_0_0_4_table_restores_gains_source_table_arn() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("pool");
        apply(&pool).await.expect("fresh apply");
        assert!(
            pending_shape_migrations(&pool)
                .await
                .expect("pending")
                .is_empty()
        );
        sqlx::raw_sql(
            "PRAGMA foreign_keys = OFF; \
             DROP TABLE table_restores; \
             CREATE TABLE table_restores (\
                 table_id TEXT PRIMARY KEY REFERENCES tables(table_id) ON DELETE CASCADE, \
                 source_backup_arn TEXT NOT NULL, \
                 restore_date_time TEXT NOT NULL \
                     DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))); \
             INSERT INTO table_restores (table_id, source_backup_arn) VALUES ('t1', 'arn:b');",
        )
        .execute(&pool)
        .await
        .expect("earlier 0.0.4 shape");
        assert_eq!(
            pending_shape_migrations(&pool).await.expect("pending"),
            vec!["add table_restores.source_table_arn".to_owned()]
        );

        apply(&pool).await.expect("converge");
        let row: (String, Option<String>) = sqlx::query_as(
            "SELECT source_backup_arn, source_table_arn FROM table_restores WHERE table_id = 't1'",
        )
        .fetch_one(&pool)
        .await
        .expect("row kept");
        assert_eq!(row, ("arn:b".to_owned(), None));
        assert!(
            pending_shape_migrations(&pool)
                .await
                .expect("pending")
                .is_empty()
        );

        apply(&pool).await.expect("second apply is a no-op");
        let columns: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('table_restores') \
             WHERE name = 'source_table_arn'",
        )
        .fetch_one(&pool)
        .await
        .expect("columns");
        assert_eq!(columns, 1);
    }

    /// The shape steps `migrate` runs through `run_data_migrations` carry the
    /// same version guard as `apply`.
    #[tokio::test]
    async fn shape_migrations_refuse_an_unreadable_catalog() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("pool");
        apply(&pool).await.expect("fresh apply");
        sqlx::query("UPDATE settings SET value = 'garbage' WHERE key = 'catalog_version'")
            .execute(&pool)
            .await
            .expect("stamp garbage");
        let err = super::apply_shape_migrations(&pool)
            .await
            .expect_err("refused");
        assert!(
            format!("{err:?}").contains("refusing to overwrite"),
            "{err:?}"
        );
    }
}
