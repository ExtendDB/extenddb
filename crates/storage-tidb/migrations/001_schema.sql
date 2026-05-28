-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Consolidated catalog schema for extenddb (catalog version 0.0.2).
-- This is the complete schema applied on fresh installs.

-- Accounts — multi-account support (REQ-AUTH-005).
CREATE TABLE IF NOT EXISTS accounts (
    account_id VARCHAR(32) PRIMARY KEY CLUSTERED,
    account_name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

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
    table_size_bytes BIGINT NOT NULL DEFAULT 0,
    item_count BIGINT NOT NULL DEFAULT 0,
    table_arn VARCHAR(512) NOT NULL,
    table_id VARCHAR(64) NOT NULL,
    ttl_attribute VARCHAR(255),
    deletion_protection_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    status_transition_at TIMESTAMP(6),
    stream_label VARCHAR(64),
    ttl_index_ready BOOLEAN NOT NULL DEFAULT FALSE,
    ttl_native_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    control_plane_token VARCHAR(64),
    control_plane_lease_until TIMESTAMP(6),
    PRIMARY KEY (account_id, table_name) CLUSTERED,
    CONSTRAINT tables_table_id_unique UNIQUE (table_id)
);

CREATE INDEX idx_tables_pending_transition
    ON tables (status_transition_at);

CREATE INDEX idx_tables_control_plane_work
    ON tables (table_status, status_transition_at, control_plane_lease_until);

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
);

-- Resource tags.
CREATE TABLE IF NOT EXISTS tags (
    resource_arn VARCHAR(512) NOT NULL,
    tag_key VARCHAR(255) NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (resource_arn, tag_key) CLUSTERED
);

-- Migration tracking.
CREATE TABLE IF NOT EXISTS schema_history (
    filename VARCHAR(255) PRIMARY KEY CLUSTERED,
    applied_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

-- Settings (catalog version, data database connection, runtime config).
CREATE TABLE IF NOT EXISTS settings (
    `key` VARCHAR(255) PRIMARY KEY CLUSTERED,
    value TEXT NOT NULL
);

-- Admin users.
CREATE TABLE IF NOT EXISTS admin_users (
    admin_name VARCHAR(255) PRIMARY KEY CLUSTERED,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

-- IAM users.
CREATE TABLE IF NOT EXISTS iam_users (
    account_id VARCHAR(32) NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    user_name VARCHAR(255) NOT NULL,
    user_arn VARCHAR(512) NOT NULL UNIQUE,
    password_hash TEXT,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (account_id, user_name) CLUSTERED
);

-- IAM user tags.
CREATE TABLE IF NOT EXISTS iam_user_tags (
    account_id VARCHAR(32) NOT NULL,
    user_name VARCHAR(255) NOT NULL,
    tag_key VARCHAR(255) NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (account_id, user_name, tag_key) CLUSTERED,
    FOREIGN KEY (account_id, user_name) REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);

-- Access keys.
CREATE TABLE IF NOT EXISTS access_keys (
    access_key_id VARCHAR(128) PRIMARY KEY CLUSTERED,
    secret_key_encrypted BLOB NOT NULL,
    account_id VARCHAR(32) NOT NULL,
    user_name VARCHAR(255) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    FOREIGN KEY (account_id, user_name) REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);

-- IAM groups.
CREATE TABLE IF NOT EXISTS iam_groups (
    account_id VARCHAR(32) NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    group_name VARCHAR(255) NOT NULL,
    group_arn VARCHAR(512) NOT NULL UNIQUE,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (account_id, group_name) CLUSTERED
);

-- IAM group membership.
CREATE TABLE IF NOT EXISTS iam_group_members (
    account_id VARCHAR(32) NOT NULL,
    group_name VARCHAR(255) NOT NULL,
    user_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (account_id, group_name, user_name) CLUSTERED,
    FOREIGN KEY (account_id, group_name) REFERENCES iam_groups(account_id, group_name) ON DELETE CASCADE,
    FOREIGN KEY (account_id, user_name) REFERENCES iam_users(account_id, user_name) ON DELETE CASCADE
);

-- IAM roles.
CREATE TABLE IF NOT EXISTS iam_roles (
    account_id VARCHAR(32) NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    role_name VARCHAR(255) NOT NULL,
    role_arn VARCHAR(512) NOT NULL UNIQUE,
    trust_policy JSON NOT NULL,
    permissions_boundary_arn VARCHAR(512),
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (account_id, role_name) CLUSTERED
);

-- IAM role tags.
CREATE TABLE IF NOT EXISTS iam_role_tags (
    account_id VARCHAR(32) NOT NULL,
    role_name VARCHAR(255) NOT NULL,
    tag_key VARCHAR(255) NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (account_id, role_name, tag_key) CLUSTERED,
    FOREIGN KEY (account_id, role_name) REFERENCES iam_roles(account_id, role_name) ON DELETE CASCADE
);

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
) TTL = `expires_at` + INTERVAL 24 HOUR TTL_JOB_INTERVAL = '1h';

-- IAM policies.
CREATE TABLE IF NOT EXISTS iam_policies (
    account_id VARCHAR(32) NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    principal_type VARCHAR(16) NOT NULL CHECK (principal_type IN ('user', 'group', 'role')),
    principal_name VARCHAR(255) NOT NULL,
    policy_name VARCHAR(255) NOT NULL,
    policy_document JSON NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (account_id, principal_type, principal_name, policy_name) CLUSTERED
);

-- IAM permissions boundaries.
CREATE TABLE IF NOT EXISTS iam_permissions_boundaries (
    account_id VARCHAR(32) NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    principal_type VARCHAR(16) NOT NULL CHECK (principal_type IN ('user', 'role')),
    principal_name VARCHAR(255) NOT NULL,
    policy_document JSON NOT NULL,
    PRIMARY KEY (account_id, principal_type, principal_name) CLUSTERED
);

-- Idempotency tokens for TransactWriteItems.
CREATE TABLE IF NOT EXISTS idempotency_tokens (
    token       VARCHAR(255) PRIMARY KEY CLUSTERED,
    fingerprint TEXT NOT NULL,
    created_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

CREATE INDEX idx_idempotency_tokens_created ON idempotency_tokens (created_at);

-- Metrics (1-minute aggregation).
CREATE TABLE IF NOT EXISTS metrics (
    bucket TIMESTAMP(6) NOT NULL,
    metric VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    table_name VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    index_name VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    operation VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    sum DOUBLE NOT NULL DEFAULT 0,
    count BIGINT NOT NULL DEFAULT 0,
    min DOUBLE NOT NULL DEFAULT 1.79e308,
    max DOUBLE NOT NULL DEFAULT -1.79e308,
    PRIMARY KEY (bucket, metric, table_name, index_name, operation) CLUSTERED
) TTL = `bucket` + INTERVAL 24 HOUR TTL_JOB_INTERVAL = '1h';

CREATE INDEX idx_metrics_bucket ON metrics (bucket);

-- Login attempt tracking.
CREATE TABLE IF NOT EXISTS login_attempts (
    principal     VARCHAR(512) NOT NULL,
    attempted_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    success       BOOLEAN NOT NULL,
    source_ip     VARCHAR(255)
) TTL = `attempted_at` + INTERVAL 24 HOUR TTL_JOB_INTERVAL = '1h';

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
);

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
);

-- Backup tag snapshot.
CREATE TABLE IF NOT EXISTS backup_tags (
    backup_arn VARCHAR(512) NOT NULL REFERENCES backups(backup_arn) ON DELETE CASCADE,
    tag_key VARCHAR(255) NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (backup_arn, tag_key) CLUSTERED
);

-- Continuous backups / PITR status.
CREATE TABLE IF NOT EXISTS continuous_backups (
    account_id VARCHAR(32) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    pitr_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    earliest_restorable TIMESTAMP(6),
    latest_restorable TIMESTAMP(6),
    PRIMARY KEY (account_id, table_name) CLUSTERED
);

-- Seed settings.
INSERT IGNORE INTO settings (`key`, value) VALUES ('catalog_version', '0.0.2');
INSERT IGNORE INTO settings (`key`, value) VALUES ('control_plane_delay_seconds', '0.25');
