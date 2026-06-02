-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Consolidated catalog schema for extenddb (catalog version 0.0.23).
-- This is the complete schema applied on fresh installs.

-- Accounts — multi-account support (REQ-AUTH-005).
CREATE TABLE IF NOT EXISTS accounts (
    account_id VARCHAR(32) PRIMARY KEY CLUSTERED,
    account_name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Table metadata.
CREATE TABLE IF NOT EXISTS tables (
    account_id VARCHAR(32) NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    key_schema JSON NOT NULL,
    attribute_definitions JSON NOT NULL,
    billing_mode VARCHAR(32) NOT NULL DEFAULT 'PAY_PER_REQUEST',
    provisioned_throughput JSON,
    stream_specification JSON,
    table_status VARCHAR(32) NOT NULL DEFAULT 'CREATING',
    creation_date_time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    table_arn VARCHAR(512) NOT NULL,
    table_id VARCHAR(64) NOT NULL,
    ttl_attribute VARCHAR(255),
    ttl_status VARCHAR(32) NOT NULL DEFAULT 'DISABLED',
    deletion_protection_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    status_transition_at TIMESTAMP(6),
    stream_label VARCHAR(64),
    PRIMARY KEY (account_id, table_name) CLUSTERED,
    CONSTRAINT tables_table_id_unique UNIQUE (table_id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE INDEX idx_tables_pending_transition
    ON tables (status_transition_at);

CREATE INDEX idx_tables_control_plane_work
    ON tables (table_status, status_transition_at);

-- Index metadata.
CREATE TABLE IF NOT EXISTS indexes (
    table_id VARCHAR(64) NOT NULL,
    index_id VARCHAR(64) NOT NULL,
    index_name VARCHAR(255) NOT NULL,
    index_type VARCHAR(16) NOT NULL,
    key_schema JSON NOT NULL,
    projection JSON NOT NULL,
    index_status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    provisioned_throughput JSON,
    PRIMARY KEY (table_id, index_name) CLUSTERED,
    CONSTRAINT indexes_table_id_fkey
        FOREIGN KEY (table_id) REFERENCES tables(table_id) ON DELETE CASCADE
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Resource tags.
CREATE TABLE IF NOT EXISTS tags (
    resource_arn VARCHAR(512) NOT NULL,
    tag_key VARCHAR(255) NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (resource_arn, tag_key) CLUSTERED
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Migration tracking.
CREATE TABLE IF NOT EXISTS schema_history (
    filename VARCHAR(255) PRIMARY KEY CLUSTERED,
    applied_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Settings (catalog version, data database connection, runtime config).
CREATE TABLE IF NOT EXISTS settings (
    `key` VARCHAR(255) PRIMARY KEY CLUSTERED,
    value TEXT NOT NULL
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Admin users.
CREATE TABLE IF NOT EXISTS admin_users (
    admin_name VARCHAR(255) PRIMARY KEY CLUSTERED,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- IAM users.
CREATE TABLE IF NOT EXISTS iam_users (
    account_id VARCHAR(32) NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    user_name VARCHAR(255) NOT NULL,
    user_arn VARCHAR(512) NOT NULL UNIQUE,
    password_hash TEXT,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (account_id, user_name) CLUSTERED
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- IAM user tags.
CREATE TABLE IF NOT EXISTS iam_user_tags (
    account_id VARCHAR(32) NOT NULL,
    user_name VARCHAR(255) NOT NULL,
    tag_key VARCHAR(255) NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (account_id, user_name, tag_key) CLUSTERED,
    FOREIGN KEY (account_id, user_name) REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Access keys.
CREATE TABLE IF NOT EXISTS access_keys (
    access_key_id VARCHAR(128) PRIMARY KEY CLUSTERED,
    secret_key_encrypted BLOB NOT NULL,
    account_id VARCHAR(32) NOT NULL,
    user_name VARCHAR(255) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    FOREIGN KEY (account_id, user_name) REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- IAM groups.
CREATE TABLE IF NOT EXISTS iam_groups (
    account_id VARCHAR(32) NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    group_name VARCHAR(255) NOT NULL,
    group_arn VARCHAR(512) NOT NULL UNIQUE,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (account_id, group_name) CLUSTERED
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- IAM group membership.
CREATE TABLE IF NOT EXISTS iam_group_members (
    account_id VARCHAR(32) NOT NULL,
    group_name VARCHAR(255) NOT NULL,
    user_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (account_id, group_name, user_name) CLUSTERED,
    FOREIGN KEY (account_id, group_name) REFERENCES iam_groups(account_id, group_name) ON DELETE CASCADE,
    FOREIGN KEY (account_id, user_name) REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE INDEX idx_iam_group_members_user
    ON iam_group_members (account_id, user_name, group_name);

-- IAM roles.
CREATE TABLE IF NOT EXISTS iam_roles (
    account_id VARCHAR(32) NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    role_name VARCHAR(255) NOT NULL,
    role_arn VARCHAR(512) NOT NULL UNIQUE,
    trust_policy JSON NOT NULL,
    permissions_boundary_arn VARCHAR(512),
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (account_id, role_name) CLUSTERED
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- IAM role tags.
CREATE TABLE IF NOT EXISTS iam_role_tags (
    account_id VARCHAR(32) NOT NULL,
    role_name VARCHAR(255) NOT NULL,
    tag_key VARCHAR(255) NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (account_id, role_name, tag_key) CLUSTERED,
    FOREIGN KEY (account_id, role_name) REFERENCES iam_roles(account_id, role_name) ON DELETE CASCADE
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- IAM sessions.
CREATE TABLE IF NOT EXISTS iam_sessions (
    session_token VARCHAR(512) PRIMARY KEY CLUSTERED,
    access_key_id VARCHAR(128) NOT NULL UNIQUE,
    secret_key_encrypted BLOB NOT NULL,
    account_id VARCHAR(32) NOT NULL,
    role_name VARCHAR(255) NOT NULL,
    session_name VARCHAR(255) NOT NULL,
    session_tags JSON,
    session_policy JSON,
    expires_at TIMESTAMP(6) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    FOREIGN KEY (account_id, role_name) REFERENCES iam_roles(account_id, role_name) ON DELETE CASCADE
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  TTL = `expires_at` + INTERVAL 24 HOUR TTL_JOB_INTERVAL = '1h';

CREATE INDEX idx_iam_sessions_role_session
    ON iam_sessions (account_id, role_name, session_name, expires_at);

-- IAM policies.
CREATE TABLE IF NOT EXISTS iam_policies (
    account_id VARCHAR(32) NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    principal_type VARCHAR(16) NOT NULL CHECK (principal_type IN ('user', 'group', 'role')),
    principal_name VARCHAR(255) NOT NULL,
    policy_name VARCHAR(255) NOT NULL,
    policy_document JSON NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (account_id, principal_type, principal_name, policy_name) CLUSTERED
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- IAM permissions boundaries.
CREATE TABLE IF NOT EXISTS iam_permissions_boundaries (
    account_id VARCHAR(32) NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    principal_type VARCHAR(16) NOT NULL CHECK (principal_type IN ('user', 'role')),
    principal_name VARCHAR(255) NOT NULL,
    policy_document JSON NOT NULL,
    PRIMARY KEY (account_id, principal_type, principal_name) CLUSTERED
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Append-only metrics samples. TiDB frontends write one immutable row per
-- flushed in-memory aggregate with a native AUTO_RANDOM clustered key. Fresh
-- installs pre-split the row keyspace so multi-node flush bursts are scattered
-- from the first write instead of waiting for automatic Region growth.
-- Query paths aggregate by bucket/metric.
CREATE TABLE IF NOT EXISTS metrics_samples (
    sample_id BIGINT NOT NULL AUTO_RANDOM,
    bucket TIMESTAMP(6) NOT NULL,
    metric VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    table_name VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    index_name VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    operation VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    sum DOUBLE NOT NULL DEFAULT 0,
    count BIGINT NOT NULL DEFAULT 0,
    min DOUBLE NOT NULL DEFAULT 1.79e308,
    max DOUBLE NOT NULL DEFAULT -1.79e308,
    PRIMARY KEY (sample_id) CLUSTERED
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  PRE_SPLIT_REGIONS = 4
  TTL = `bucket` + INTERVAL 24 HOUR TTL_JOB_INTERVAL = '1h';

CREATE INDEX idx_metrics_samples_bucket
    ON metrics_samples (bucket, metric, table_name, index_name, operation);

-- Login attempt tracking. This append-only, TTL-owned table intentionally uses
-- TiDB sharded implicit row IDs with pre-split Regions so concurrent frontend
-- inserts do not hotspot one Region.
CREATE TABLE IF NOT EXISTS login_attempts (
    principal     VARCHAR(512) NOT NULL,
    attempted_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    success       BOOLEAN NOT NULL,
    source_ip     VARCHAR(255)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  SHARD_ROW_ID_BITS = 4
  PRE_SPLIT_REGIONS = 4
  TTL = `attempted_at` + INTERVAL 24 HOUR TTL_JOB_INTERVAL = '1h';

CREATE INDEX idx_login_attempts_principal_time
    ON login_attempts (principal, attempted_at DESC);

CREATE INDEX idx_login_attempts_source_ip_time
    ON login_attempts (source_ip, attempted_at DESC);

-- Backup metadata.
CREATE TABLE IF NOT EXISTS backups (
    backup_arn VARCHAR(512) PRIMARY KEY CLUSTERED,
    backup_name VARCHAR(255) NOT NULL,
    table_id VARCHAR(64) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    account_id VARCHAR(32) NOT NULL,
    backup_status VARCHAR(32) NOT NULL DEFAULT 'AVAILABLE',
    backup_type VARCHAR(32) NOT NULL DEFAULT 'USER',
    backup_size_bytes BIGINT NOT NULL DEFAULT 0,
    item_count BIGINT NOT NULL DEFAULT 0,
    key_schema JSON NOT NULL,
    attribute_definitions JSON NOT NULL,
    billing_mode VARCHAR(32) NOT NULL DEFAULT 'PAY_PER_REQUEST',
    provisioned_throughput JSON,
    stream_specification JSON,
    deletion_protection_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE INDEX idx_backups_table ON backups (account_id, table_name);

-- Backup index metadata snapshot.
CREATE TABLE IF NOT EXISTS backup_indexes (
    backup_arn VARCHAR(512) NOT NULL REFERENCES backups(backup_arn) ON DELETE CASCADE,
    index_id VARCHAR(64) NOT NULL,
    index_name VARCHAR(255) NOT NULL,
    index_type VARCHAR(16) NOT NULL,
    key_schema JSON NOT NULL,
    projection JSON NOT NULL,
    provisioned_throughput JSON,
    PRIMARY KEY (backup_arn, index_name) CLUSTERED
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Backup tag snapshot.
CREATE TABLE IF NOT EXISTS backup_tags (
    backup_arn VARCHAR(512) NOT NULL REFERENCES backups(backup_arn) ON DELETE CASCADE,
    tag_key VARCHAR(255) NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (backup_arn, tag_key) CLUSTERED
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- Seed settings.
INSERT IGNORE INTO settings (`key`, value) VALUES ('catalog_version', '0.0.23');
