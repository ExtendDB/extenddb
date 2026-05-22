-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Consolidated schema for extenddb SQLite backend (catalog version 0.0.2).
-- All catalog and data tables live in one SQLite file.
-- JSON stored as TEXT, timestamps as TEXT (RFC 3339 / ISO 8601).

-- Accounts — multi-account support.
CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    account_name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
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
    creation_date_time TEXT NOT NULL DEFAULT (datetime('now')),
    table_size_bytes INTEGER NOT NULL DEFAULT 0,
    item_count INTEGER NOT NULL DEFAULT 0,
    table_arn TEXT NOT NULL,
    table_id TEXT NOT NULL UNIQUE,
    ttl_attribute TEXT,
    deletion_protection_enabled INTEGER NOT NULL DEFAULT 0,
    status_transition_at TEXT,
    stream_label TEXT,
    ttl_index_ready INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (account_id, table_name)
);

CREATE INDEX IF NOT EXISTS idx_tables_pending_transition
    ON tables (status_transition_at)
    WHERE status_transition_at IS NOT NULL;

-- Index metadata.
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
    FOREIGN KEY (table_id) REFERENCES tables(table_id) ON DELETE CASCADE
);

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
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Settings (catalog version, runtime config).
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
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_stream_shards_table ON stream_shards (table_id);

-- Stream records.
CREATE TABLE IF NOT EXISTS stream_records (
    shard_id TEXT NOT NULL REFERENCES stream_shards(shard_id) ON DELETE CASCADE,
    sequence_number TEXT NOT NULL,
    table_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    record_data TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (shard_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_stream_records_created ON stream_records (created_at);

-- Admin users.
CREATE TABLE IF NOT EXISTS admin_users (
    admin_name TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- IAM users.
CREATE TABLE IF NOT EXISTS iam_users (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    user_name TEXT NOT NULL,
    user_arn TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (account_id, user_name)
);

-- IAM user tags.
CREATE TABLE IF NOT EXISTS iam_user_tags (
    account_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (account_id, user_name, tag_key),
    FOREIGN KEY (account_id, user_name) REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);

-- Access keys.
CREATE TABLE IF NOT EXISTS access_keys (
    access_key_id TEXT PRIMARY KEY,
    secret_key_encrypted BLOB NOT NULL,
    account_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (account_id, user_name) REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);

-- IAM groups.
CREATE TABLE IF NOT EXISTS iam_groups (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    group_name TEXT NOT NULL,
    group_arn TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (account_id, group_name)
);

-- IAM group membership.
CREATE TABLE IF NOT EXISTS iam_group_members (
    account_id TEXT NOT NULL,
    group_name TEXT NOT NULL,
    user_name TEXT NOT NULL,
    PRIMARY KEY (account_id, group_name, user_name),
    FOREIGN KEY (account_id, group_name) REFERENCES iam_groups(account_id, group_name) ON DELETE CASCADE,
    FOREIGN KEY (account_id, user_name) REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);

-- IAM roles.
CREATE TABLE IF NOT EXISTS iam_roles (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    role_name TEXT NOT NULL,
    role_arn TEXT NOT NULL UNIQUE,
    trust_policy TEXT NOT NULL,
    permissions_boundary_arn TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (account_id, role_name)
);

-- IAM role tags.
CREATE TABLE IF NOT EXISTS iam_role_tags (
    account_id TEXT NOT NULL,
    role_name TEXT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (account_id, role_name, tag_key),
    FOREIGN KEY (account_id, role_name) REFERENCES iam_roles(account_id, role_name) ON DELETE CASCADE
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
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (account_id, role_name) REFERENCES iam_roles(account_id, role_name) ON DELETE CASCADE
);

-- IAM policies.
CREATE TABLE IF NOT EXISTS iam_policies (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    principal_type TEXT NOT NULL,
    principal_name TEXT NOT NULL,
    policy_name TEXT NOT NULL,
    policy_document TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (account_id, principal_type, principal_name, policy_name)
);

-- IAM permissions boundaries.
CREATE TABLE IF NOT EXISTS iam_permissions_boundaries (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    principal_type TEXT NOT NULL,
    principal_name TEXT NOT NULL,
    policy_document TEXT NOT NULL,
    PRIMARY KEY (account_id, principal_type, principal_name)
);

-- Idempotency tokens for TransactWriteItems.
CREATE TABLE IF NOT EXISTS idempotency_tokens (
    token       TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_idempotency_tokens_created ON idempotency_tokens (created_at);

-- Metrics (1-minute aggregation).
CREATE TABLE IF NOT EXISTS metrics (
    bucket TEXT NOT NULL,
    metric TEXT NOT NULL,
    table_name TEXT NOT NULL DEFAULT '',
    index_name TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL DEFAULT '',
    sum REAL NOT NULL DEFAULT 0,
    count INTEGER NOT NULL DEFAULT 0,
    min REAL NOT NULL DEFAULT 1e308,
    max REAL NOT NULL DEFAULT -1e308,
    PRIMARY KEY (bucket, metric, table_name, index_name, operation)
);

CREATE INDEX IF NOT EXISTS idx_metrics_bucket ON metrics (bucket);

-- Login attempt tracking.
CREATE TABLE IF NOT EXISTS login_attempts (
    principal     TEXT NOT NULL,
    attempted_at  TEXT NOT NULL DEFAULT (datetime('now')),
    success       INTEGER NOT NULL,
    source_ip     TEXT
);

CREATE INDEX IF NOT EXISTS idx_login_attempts_principal_time
    ON login_attempts (principal, attempted_at);

CREATE INDEX IF NOT EXISTS idx_login_attempts_source_ip_time
    ON login_attempts (source_ip, attempted_at)
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
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_backups_table ON backups (account_id, table_name);

-- Backup items.
CREATE TABLE IF NOT EXISTS backup_items (
    backup_arn TEXT NOT NULL REFERENCES backups(backup_arn) ON DELETE CASCADE,
    pk TEXT NOT NULL,
    sk TEXT,
    item_data TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_backup_items_arn ON backup_items (backup_arn);

-- Continuous backups / PITR status.
CREATE TABLE IF NOT EXISTS continuous_backups (
    account_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    pitr_enabled INTEGER NOT NULL DEFAULT 0,
    earliest_restorable TEXT,
    latest_restorable TEXT,
    PRIMARY KEY (account_id, table_name)
);

-- Stream sequence counter (replaces PostgreSQL sequence).
CREATE TABLE IF NOT EXISTS seq_counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO seq_counters (name, value) VALUES ('stream', 0);

-- Seed settings.
INSERT OR IGNORE INTO settings (key, value) VALUES ('catalog_version', '0.0.2');
INSERT OR IGNORE INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0.25');
INSERT OR IGNORE INTO settings (key, value) VALUES ('gsi_propagation_delay_ms', '10');
