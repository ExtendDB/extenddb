// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authoritative DuckDB catalog schema.
//!
//! Mirrors the semantics of the reference PostgreSQL catalog schema
//! (`crates/storage-postgres/migrations/001_schema.sql`, catalog version
//! 0.0.2). Table and column names are kept identical to PostgreSQL so the
//! catalog/management stores remain portable across backends.
//!
//! Type mapping PostgreSQL → DuckDB:
//! - `JSONB`        → `TEXT` (JSON serialized as a string)
//! - `BYTEA`        → `BLOB`
//! - `BOOLEAN`      → `BIGINT` (0/1)
//! - `BIGINT`       → `BIGINT`
//! - `DOUBLE PRECISION` → `DOUBLE` (`±Infinity` literals)
//! - `TIMESTAMPTZ`  → `TEXT` in RFC 3339 UTC (`YYYY-MM-DDTHH:MM:SS.sssZ`)
//! - `SEQUENCE`     → a `seq_counters` row updated inside the write transaction
//!
//! Per-DynamoDB-table data tables (`_ddb_<table_id>`) are NOT created here; they
//! are created dynamically by `create_table`, exactly as in the PostgreSQL
//! backend.

use crate::db;
use extenddb_storage::management_store::{OpError, OpResult};

/// Compiled-in catalog version. Single source of truth for the DuckDB backend;
/// mirrors the PostgreSQL backend's `CATALOG_VERSION`.
pub const CATALOG_VERSION: extenddb_core::version::CatalogVersion =
    extenddb_core::version::CatalogVersion::new(0, 0, 3);

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
    created_at TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ'))
);

-- Table metadata.
CREATE TABLE IF NOT EXISTS tables (
    account_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    key_schema TEXT NOT NULL,
    attribute_definitions TEXT NOT NULL,
    billing_mode TEXT NOT NULL DEFAULT 'PAY_PER_REQUEST',
    provisioned_throughput TEXT,
    stream_specification TEXT,
    table_status TEXT NOT NULL DEFAULT 'CREATING',
    creation_date_time TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ')),
    table_size_bytes BIGINT NOT NULL DEFAULT 0,
    item_count BIGINT NOT NULL DEFAULT 0,
    table_arn TEXT NOT NULL,
    table_id TEXT NOT NULL,
    ttl_attribute TEXT,
    deletion_protection_enabled BIGINT NOT NULL DEFAULT 0,
    status_transition_at TEXT,
    stream_label TEXT,
    ttl_index_ready BIGINT NOT NULL DEFAULT 0,
    table_class TEXT,
    sse_specification TEXT,
    on_demand_throughput TEXT,
    PRIMARY KEY (account_id, table_name),
    CONSTRAINT tables_table_id_unique UNIQUE (table_id)
);

CREATE INDEX IF NOT EXISTS idx_tables_pending_transition
    ON tables (status_transition_at);

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
    propagation_delay_ms BIGINT,
    PRIMARY KEY (table_id, index_name),
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
    dimensions BIGINT NOT NULL,
    distance_function TEXT NOT NULL,
    vector_attribute TEXT NOT NULL,
    search_schema TEXT,
    projection TEXT NOT NULL,
    index_status TEXT NOT NULL DEFAULT 'CREATING',
    backfilling BIGINT,
    -- Items the backfill skipped because their stored bytes cannot enter the
    -- index (unparseable row, malformed or wrong-dimension vector). NULL until
    -- a backfill has completed; 0 afterwards when nothing was skipped. Kept so
    -- an operator can see that an ACTIVE index deliberately omits rows, rather
    -- than the build looping forever on them or dying part-way.
    skipped_item_count BIGINT,
    PRIMARY KEY (table_id, index_name),
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
    applied_at TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ'))
);

-- Settings (catalog version, data database name, runtime config).
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Stream shards.
CREATE TABLE IF NOT EXISTS stream_shards (
    shard_id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    parent_shard_id TEXT,
    starting_sequence_number TEXT NOT NULL,
    ending_sequence_number TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ'))
);

CREATE INDEX IF NOT EXISTS idx_stream_shards_table ON stream_shards (table_id);

-- Stream records.
CREATE TABLE IF NOT EXISTS stream_records (
    shard_id TEXT NOT NULL,
    sequence_number TEXT NOT NULL,
    table_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    record_data TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ')),
    PRIMARY KEY (shard_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_stream_records_created ON stream_records (created_at);

-- Admin users.
CREATE TABLE IF NOT EXISTS admin_users (
    admin_name TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ'))
);

-- IAM users.
CREATE TABLE IF NOT EXISTS iam_users (
    account_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    user_arn TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ')),
    PRIMARY KEY (account_id, user_name)
);

-- IAM user tags.
CREATE TABLE IF NOT EXISTS iam_user_tags (
    account_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (account_id, user_name, tag_key)
);

-- Access keys.
CREATE TABLE IF NOT EXISTS access_keys (
    access_key_id TEXT PRIMARY KEY,
    secret_key_encrypted BLOB NOT NULL,
    account_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    is_active BIGINT NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ'))
);

-- IAM groups.
CREATE TABLE IF NOT EXISTS iam_groups (
    account_id TEXT NOT NULL,
    group_name TEXT NOT NULL,
    group_arn TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ')),
    PRIMARY KEY (account_id, group_name)
);

-- IAM group membership.
CREATE TABLE IF NOT EXISTS iam_group_members (
    account_id TEXT NOT NULL,
    group_name TEXT NOT NULL,
    user_name TEXT NOT NULL,
    PRIMARY KEY (account_id, group_name, user_name)
);

-- IAM roles.
CREATE TABLE IF NOT EXISTS iam_roles (
    account_id TEXT NOT NULL,
    role_name TEXT NOT NULL,
    role_arn TEXT NOT NULL UNIQUE,
    trust_policy TEXT NOT NULL,
    permissions_boundary_arn TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ')),
    PRIMARY KEY (account_id, role_name)
);

-- IAM role tags.
CREATE TABLE IF NOT EXISTS iam_role_tags (
    account_id TEXT NOT NULL,
    role_name TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (account_id, role_name, tag_key)
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
    created_at TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ'))
);

-- IAM policies.
CREATE TABLE IF NOT EXISTS iam_policies (
    account_id TEXT NOT NULL,
    principal_type TEXT NOT NULL CHECK (principal_type IN ('user', 'group', 'role')),
    principal_name TEXT NOT NULL,
    policy_name TEXT NOT NULL,
    policy_document TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ')),
    PRIMARY KEY (account_id, principal_type, principal_name, policy_name)
);

-- IAM permissions boundaries.
CREATE TABLE IF NOT EXISTS iam_permissions_boundaries (
    account_id TEXT NOT NULL,
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
    created_at  TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ')),
    PRIMARY KEY (account_id, token)
);

CREATE INDEX IF NOT EXISTS idx_idempotency_tokens_created ON idempotency_tokens (created_at);

-- Metrics (1-minute aggregation). ±Infinity seeds min / max in DuckDB DOUBLE,
-- matching PostgreSQL's Infinity / -Infinity seed for min / max.
CREATE TABLE IF NOT EXISTS metrics (
    bucket TEXT NOT NULL,
    metric TEXT NOT NULL,
    table_name TEXT NOT NULL DEFAULT '',
    index_name TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL DEFAULT '',
    sum DOUBLE NOT NULL DEFAULT 0,
    count BIGINT NOT NULL DEFAULT 0,
    min DOUBLE NOT NULL DEFAULT 'Infinity'::DOUBLE,
    max DOUBLE NOT NULL DEFAULT '-Infinity'::DOUBLE,
    PRIMARY KEY (bucket, metric, table_name, index_name, operation)
);

CREATE INDEX IF NOT EXISTS idx_metrics_bucket ON metrics (bucket);

-- Login attempt tracking.
CREATE TABLE IF NOT EXISTS login_attempts (
    principal     TEXT NOT NULL,
    attempted_at  TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ')),
    success       BIGINT NOT NULL,
    source_ip     TEXT
);

CREATE INDEX IF NOT EXISTS idx_login_attempts_principal_time
    ON login_attempts (principal, attempted_at DESC);

CREATE INDEX IF NOT EXISTS idx_login_attempts_source_ip_time
    ON login_attempts (source_ip, attempted_at DESC);

-- Backup metadata.
CREATE TABLE IF NOT EXISTS backups (
    backup_arn TEXT PRIMARY KEY,
    backup_name TEXT NOT NULL,
    table_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    account_id TEXT NOT NULL,
    backup_status TEXT NOT NULL DEFAULT 'AVAILABLE',
    backup_type TEXT NOT NULL DEFAULT 'USER',
    backup_size_bytes BIGINT NOT NULL DEFAULT 0,
    item_count BIGINT NOT NULL DEFAULT 0,
    key_schema TEXT NOT NULL,
    attribute_definitions TEXT NOT NULL,
    billing_mode TEXT NOT NULL DEFAULT 'PAY_PER_REQUEST',
    provisioned_throughput TEXT,
    stream_specification TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ'))
);

CREATE INDEX IF NOT EXISTS idx_backups_table ON backups (account_id, table_name);

-- Backup items.
CREATE TABLE IF NOT EXISTS backup_items (
    backup_arn TEXT NOT NULL,
    pk TEXT NOT NULL,
    sk TEXT,
    item_data TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_backup_items_arn ON backup_items (backup_arn);

-- Continuous backups / PITR status.
CREATE TABLE IF NOT EXISTS continuous_backups (
    account_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    pitr_enabled BIGINT NOT NULL DEFAULT 0,
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
CREATE SEQUENCE IF NOT EXISTS gsi_pending_seq;

CREATE TABLE IF NOT EXISTS gsi_pending (
    id BIGINT PRIMARY KEY DEFAULT nextval('gsi_pending_seq'),
    table_id TEXT NOT NULL,
    worker_partition BIGINT NOT NULL,
    old_item TEXT,
    new_item TEXT,
    index_context TEXT NOT NULL,
    ready_at TEXT NOT NULL DEFAULT (strftime(now()::TIMESTAMP, '%Y-%m-%dT%H:%M:%S.%gZ'))
);

CREATE INDEX IF NOT EXISTS idx_gsi_pending_claim
    ON gsi_pending (worker_partition, ready_at, id);

-- Monotonic counters. Replaces PostgreSQL sequences. The stream counter is
-- seeded to the current epoch in microseconds so sequence numbers are
-- time-ordered and survive restarts (mirrors the PostgreSQL setval seed).
CREATE TABLE IF NOT EXISTS seq_counters (
    name TEXT PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO seq_counters (name, value)
    VALUES ('stream', CAST(epoch(now()) AS BIGINT) * 1000000);

-- Seed settings (mirror PostgreSQL defaults).
-- Recorded catalog version. Upserts rather than INSERT OR IGNORE: this schema is
-- re-applied by `extenddb migrate`, and with IGNORE the recorded version would
-- never advance, so a migration could add objects and still leave the server
-- refusing to start on a version mismatch. Must stay in step with
-- `CATALOG_VERSION` above; they are checked against each other in a test.
INSERT INTO settings (key, value) VALUES ('catalog_version', '0.0.3')
    ON CONFLICT(key) DO UPDATE SET value = excluded.value;
INSERT OR IGNORE INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0.25');
INSERT OR IGNORE INTO settings (key, value) VALUES ('index_propagation_delay_ms', '10');
"#;

/// Apply the full catalog schema to a fresh (or existing) database.
///
/// Idempotent: every statement uses `IF NOT EXISTS` / `INSERT OR IGNORE`.
pub async fn apply(pool: &db::Pool) -> OpResult<()> {
    db::raw_sql(SCHEMA_SQL)
        .execute(pool)
        .await
        .map_err(|e| OpError::Internal(format!("apply catalog schema: {e}")))?;
    Ok(())
}

/// Return whether a table with the given name exists in the catalog.
pub async fn table_exists(pool: &db::Pool, name: &str) -> OpResult<bool> {
    let exists: bool =
        db::query_scalar("SELECT EXISTS(SELECT 1 FROM duckdb_tables() WHERE table_name = ?)")
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
}
