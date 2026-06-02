-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Consolidated catalog schema for extenddb (catalog version 0.0.3).
-- This is the complete schema applied on fresh installs.

BEGIN;

-- Accounts — multi-account support (REQ-AUTH-005).
CREATE TABLE IF NOT EXISTS accounts (
    account_id TEXT PRIMARY KEY,
    account_name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Table metadata.
CREATE TABLE IF NOT EXISTS tables (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    table_name TEXT NOT NULL,
    key_schema JSONB NOT NULL,
    attribute_definitions JSONB NOT NULL,
    billing_mode TEXT NOT NULL DEFAULT 'PAY_PER_REQUEST',
    provisioned_throughput JSONB,
    stream_specification JSONB,
    table_status TEXT NOT NULL DEFAULT 'CREATING',
    creation_date_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    table_size_bytes BIGINT NOT NULL DEFAULT 0,
    item_count BIGINT NOT NULL DEFAULT 0,
    table_arn TEXT NOT NULL,
    table_id TEXT NOT NULL,
    ttl_attribute TEXT,
    deletion_protection_enabled BOOLEAN NOT NULL DEFAULT false,
    status_transition_at TIMESTAMPTZ,
    stream_label TEXT,
    ttl_index_ready BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (account_id, table_name),
    CONSTRAINT tables_table_id_unique UNIQUE (table_id)
);

CREATE INDEX idx_tables_pending_transition
    ON tables (status_transition_at)
    WHERE status_transition_at IS NOT NULL;

-- Index metadata.
CREATE TABLE IF NOT EXISTS indexes (
    table_id TEXT NOT NULL,
    index_id TEXT NOT NULL DEFAULT gen_random_uuid()::text,
    index_name TEXT NOT NULL,
    index_type TEXT NOT NULL,
    key_schema JSONB NOT NULL,
    projection JSONB NOT NULL,
    index_status TEXT NOT NULL DEFAULT 'ACTIVE',
    provisioned_throughput JSONB,
    propagation_delay_ms INTEGER,
    PRIMARY KEY (table_id, index_name),
    CONSTRAINT indexes_table_id_fkey
        FOREIGN KEY (table_id) REFERENCES tables(table_id) ON DELETE CASCADE,
    CONSTRAINT chk_propagation_delay_ms_non_negative
        CHECK (propagation_delay_ms IS NULL OR propagation_delay_ms >= 0)
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
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Settings (catalog version, data database connection, runtime config).
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
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_stream_shards_table ON stream_shards (table_id);

-- Stream records.
CREATE TABLE IF NOT EXISTS stream_records (
    shard_id TEXT NOT NULL REFERENCES stream_shards(shard_id) ON DELETE CASCADE,
    sequence_number TEXT NOT NULL,
    table_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    record_data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (shard_id, sequence_number)
);

CREATE INDEX idx_stream_records_created ON stream_records (created_at);

-- Admin users.
CREATE TABLE IF NOT EXISTS admin_users (
    admin_name TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- IAM users.
CREATE TABLE IF NOT EXISTS iam_users (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    user_name TEXT NOT NULL,
    user_arn TEXT NOT NULL UNIQUE,
    password_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
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
    secret_key_encrypted BYTEA NOT NULL,
    account_id TEXT NOT NULL,
    user_name TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (account_id, user_name) REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);

-- IAM groups.
CREATE TABLE IF NOT EXISTS iam_groups (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    group_name TEXT NOT NULL,
    group_arn TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
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
    trust_policy JSONB NOT NULL,
    permissions_boundary_arn TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
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
    secret_key_encrypted BYTEA NOT NULL,
    account_id TEXT NOT NULL,
    role_name TEXT NOT NULL,
    session_name TEXT NOT NULL,
    session_tags JSONB,
    session_policy JSONB,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (account_id, role_name) REFERENCES iam_roles(account_id, role_name) ON DELETE CASCADE
);

-- IAM policies.
CREATE TABLE IF NOT EXISTS iam_policies (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    principal_type TEXT NOT NULL CHECK (principal_type IN ('user', 'group', 'role')),
    principal_name TEXT NOT NULL,
    policy_name TEXT NOT NULL,
    policy_document JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, principal_type, principal_name, policy_name)
);

-- IAM permissions boundaries.
CREATE TABLE IF NOT EXISTS iam_permissions_boundaries (
    account_id TEXT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    principal_type TEXT NOT NULL CHECK (principal_type IN ('user', 'role')),
    principal_name TEXT NOT NULL,
    policy_document JSONB NOT NULL,
    PRIMARY KEY (account_id, principal_type, principal_name)
);

-- Idempotency tokens for TransactWriteItems.
CREATE TABLE IF NOT EXISTS idempotency_tokens (
    token       TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_idempotency_tokens_created ON idempotency_tokens (created_at);

-- Metrics (1-minute aggregation).
CREATE TABLE IF NOT EXISTS metrics (
    bucket TIMESTAMPTZ NOT NULL,
    metric TEXT NOT NULL,
    table_name TEXT NOT NULL DEFAULT '',
    index_name TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL DEFAULT '',
    sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    count BIGINT NOT NULL DEFAULT 0,
    min DOUBLE PRECISION NOT NULL DEFAULT 'Infinity',
    max DOUBLE PRECISION NOT NULL DEFAULT '-Infinity',
    PRIMARY KEY (bucket, metric, table_name, index_name, operation)
);

CREATE INDEX idx_metrics_bucket ON metrics (bucket);

-- Login attempt tracking.
CREATE TABLE IF NOT EXISTS login_attempts (
    principal     TEXT NOT NULL,
    attempted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    success       BOOLEAN NOT NULL,
    source_ip     TEXT
);

CREATE INDEX idx_login_attempts_principal_time
    ON login_attempts (principal, attempted_at DESC);

CREATE INDEX idx_login_attempts_source_ip_time
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
    backup_size_bytes BIGINT NOT NULL DEFAULT 0,
    item_count BIGINT NOT NULL DEFAULT 0,
    key_schema JSONB NOT NULL,
    attribute_definitions JSONB NOT NULL,
    billing_mode TEXT NOT NULL DEFAULT 'PAY_PER_REQUEST',
    provisioned_throughput JSONB,
    stream_specification JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_backups_table ON backups (account_id, table_name);

-- Backup items.
CREATE TABLE IF NOT EXISTS backup_items (
    backup_arn TEXT NOT NULL REFERENCES backups(backup_arn) ON DELETE CASCADE,
    pk TEXT NOT NULL,
    sk TEXT,
    item_data JSONB NOT NULL
);

CREATE INDEX idx_backup_items_arn ON backup_items (backup_arn);

-- Stream sequence (monotonic, starts at current epoch microseconds).
CREATE SEQUENCE IF NOT EXISTS stream_seq START 1;
SELECT setval('stream_seq', GREATEST(
    (EXTRACT(EPOCH FROM now()) * 1000000)::BIGINT,
    1
));

-- Seed settings.
INSERT INTO settings (key, value) VALUES ('catalog_version', '0.0.3')
ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0.25')
ON CONFLICT (key) DO NOTHING;
INSERT INTO settings (key, value) VALUES ('gsi_propagation_delay_ms', '10')
ON CONFLICT (key) DO NOTHING;

COMMIT;
