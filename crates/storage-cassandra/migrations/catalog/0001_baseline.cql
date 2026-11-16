-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 1, baseline, Cassandra catalog keyspace.
--
-- Creates the v1.0 catalog schema in an empty keyspace and brings a catalog
-- created by any released version to the same schema. It is the statements
-- of the legacy catalog files V001 to V005 in order, unchanged apart from
-- these rules, so every statement is safe to run on a catalog that already
-- has some or all of their objects:
--
-- * every CREATE uses IF NOT EXISTS, and every column a later legacy file
--   added stays an ALTER TABLE ... ADD IF NOT EXISTS, because folding it into
--   the CREATE TABLE would leave every existing catalog without it
-- * no statement writes a settings row: every seeded setting has an equal
--   default in code, and the runner writes the version
-- * tables are named without a keyspace: the runner writes the catalog
--   keyspace in front of each one, and waits for every live node to agree on
--   the schema after each CREATE and ALTER
--
-- The runner splits this file on the semicolon character, so no comment holds
-- one.

-- The ledger of releases before 1.0, kept so their migrate finds every file
-- it knows recorded and runs none of them.
CREATE TABLE IF NOT EXISTS schema_history (
    version int PRIMARY KEY,
    description text,
    applied_at timestamp
);

-- Settings table (key-value store)
CREATE TABLE IF NOT EXISTS settings (
    key text PRIMARY KEY,
    value text
);

-- Accounts table
CREATE TABLE IF NOT EXISTS accounts (
    account_id text PRIMARY KEY,
    account_name text,
    created_at timestamp
);

-- Tables (per-account table definitions)
CREATE TABLE IF NOT EXISTS tables (
    account_id text,
    table_name text,
    table_id text,
    key_schema text,
    attribute_definitions text,
    table_status text,
    created_at timestamp,
    billing_mode text,
    provisioned_throughput text,
    on_demand_throughput text,
    table_class text,
    sse_specification text,
    stream_specification text,
    table_size_bytes bigint,
    item_count bigint,
    table_arn text,
    ttl_attribute text,
    deletion_protection_enabled boolean,
    status_transition_at timestamp,
    stream_label text,
    ttl_index_ready boolean,
    -- Fences TTL enable, backfill, sweep, and cleanup cycles against each other.
    ttl_generation uuid,
    ttl_sweep_owner uuid,
    ttl_cleanup_generation uuid,
    PRIMARY KEY ((account_id), table_name)
);
CREATE INDEX IF NOT EXISTS ON tables (table_id);

-- Indexes (GSI/LSI metadata)
CREATE TABLE IF NOT EXISTS indexes (
    table_id text,
    index_name text,
    index_id text,
    index_type text,
    key_schema text,
    projection text,
    index_status text,
    provisioned_throughput text,
    propagation_delay_ms int,
    backfill_heartbeat timestamp,
    PRIMARY KEY ((table_id), index_name)
);

-- Tags (resource tagging)
CREATE TABLE IF NOT EXISTS tags (
    resource_arn text,
    tag_key text,
    tag_value text,
    PRIMARY KEY ((resource_arn), tag_key)
);

-- Metrics (metrics collection)
CREATE TABLE IF NOT EXISTS metrics (
    bucket timestamp,
    metric text,
    table_name text,
    index_name text,
    operation text,
    sum double,
    count bigint,
    min double,
    max double,
    PRIMARY KEY ((bucket, metric, table_name, index_name, operation))
);

-- Login attempts (security auditing)
CREATE TABLE IF NOT EXISTS login_attempts (
    principal text,
    attempted_at timestamp,
    success boolean,
    source_ip text,
    PRIMARY KEY ((principal), attempted_at)
) WITH CLUSTERING ORDER BY (attempted_at DESC);

-- Admin users table
CREATE TABLE IF NOT EXISTS admin_users (
    admin_name text PRIMARY KEY,
    password_hash text,
    created_at timestamp
);

-- IAM users table
CREATE TABLE IF NOT EXISTS iam_users (
    account_id text,
    user_name text,
    user_arn text,
    password_hash text,
    created_at timestamp,
    PRIMARY KEY ((account_id), user_name)
);

-- IAM groups table
CREATE TABLE IF NOT EXISTS iam_groups (
    account_id text,
    group_name text,
    group_arn text,
    created_at timestamp,
    PRIMARY KEY ((account_id), group_name)
);

-- IAM roles table
CREATE TABLE IF NOT EXISTS iam_roles (
    account_id text,
    role_name text,
    role_arn text,
    trust_policy text,
    permissions_boundary_arn text,
    created_at timestamp,
    PRIMARY KEY ((account_id), role_name)
);

-- IAM policies table
CREATE TABLE IF NOT EXISTS iam_policies (
    account_id text,
    principal_type text,
    principal_name text,
    policy_name text,
    policy_document text,
    created_at timestamp,
    PRIMARY KEY ((account_id), principal_type, principal_name, policy_name)
);

-- Access keys table
CREATE TABLE IF NOT EXISTS access_keys (
    access_key_id text PRIMARY KEY,
    account_id text,
    user_name text,
    secret_key_encrypted blob,
    is_active boolean,
    created_at timestamp
);

-- IAM sessions table
CREATE TABLE IF NOT EXISTS iam_sessions (
    session_token text PRIMARY KEY,
    account_id text,
    role_name text,
    session_name text,
    access_key_id text,
    secret_key_encrypted blob,
    session_tags text,
    session_policy text,
    expires_at timestamp,
    created_at timestamp
);

-- IAM user tags table
CREATE TABLE IF NOT EXISTS iam_user_tags (
    account_id text,
    user_name text,
    tag_key text,
    tag_value text,
    PRIMARY KEY ((account_id, user_name), tag_key)
);

-- IAM role tags table
CREATE TABLE IF NOT EXISTS iam_role_tags (
    account_id text,
    role_name text,
    tag_key text,
    tag_value text,
    PRIMARY KEY ((account_id, role_name), tag_key)
);

-- IAM group members table
CREATE TABLE IF NOT EXISTS iam_group_members (
    account_id text,
    group_name text,
    user_name text,
    PRIMARY KEY ((account_id, group_name), user_name)
);

-- IAM permissions boundaries table
CREATE TABLE IF NOT EXISTS iam_permissions_boundaries (
    account_id text,
    principal_type text,
    principal_name text,
    policy_document text,
    PRIMARY KEY (account_id, principal_type, principal_name)
);

-- Legacy V002: logical backup metadata and continuous backup settings.

-- Authoritative lookup for ARN-addressed operations. Include account_id in the
-- partition key so a caller cannot even address another account's backup row.
CREATE TABLE IF NOT EXISTS backups_by_arn (
    account_id text,
    backup_arn text,
    backup_name text,
    table_id text,
    table_name text,
    table_arn text,
    backup_status text,
    backup_type text,
    backup_size_bytes bigint,
    item_count bigint,
    key_schema text,
    attribute_definitions text,
    billing_mode text,
    provisioned_throughput text,
    stream_specification text,
    table_created_at timestamp,
    created_at timestamp,
    PRIMARY KEY ((account_id, backup_arn))
);

-- Denormalized list access path for all backups owned by an account. table_id
-- records the exact source-table incarnation even if the name is later reused.
CREATE TABLE IF NOT EXISTS backups_by_account (
    account_id text,
    created_at timestamp,
    backup_arn text,
    backup_name text,
    table_id text,
    table_name text,
    table_arn text,
    backup_size_bytes bigint,
    PRIMARY KEY ((account_id), created_at, backup_arn)
) WITH CLUSTERING ORDER BY (created_at DESC, backup_arn ASC);

-- ListBackups filters by table name, and backups intentionally outlive their
-- source table. Keep the name in the partition key so backups from an earlier
-- table incarnation remain discoverable after delete/recreate. table_id retains
-- the immutable source identity used by the authoritative metadata.
CREATE TABLE IF NOT EXISTS backups_by_table (
    account_id text,
    table_name text,
    created_at timestamp,
    backup_arn text,
    backup_name text,
    table_id text,
    table_arn text,
    backup_size_bytes bigint,
    PRIMARY KEY ((account_id, table_name), created_at, backup_arn)
) WITH CLUSTERING ORDER BY (created_at DESC, backup_arn ASC);

CREATE TABLE IF NOT EXISTS continuous_backups (
    account_id text,
    table_name text,
    pitr_enabled boolean,
    PRIMARY KEY ((account_id), table_name)
);

-- Legacy V003: scope TransactWriteItems client tokens to their owning account.

CREATE TABLE IF NOT EXISTS idempotency_tokens_by_account (
    account_id text,
    "token" text,
    fingerprint text,
    created_at timestamp,
    PRIMARY KEY ((account_id), "token")
);

-- Legacy V004: durable resume point for the TTL enable backfill.
--
-- JSON blob {"generation": <uuid>, "last_key": <item>} written after each
-- scanned page. A backfill that dies mid-scan (host crash, deploy) resumes
-- from here instead of rescanning the table from the top. A cursor whose
-- generation does not match the table's current ttl_generation is ignored.
ALTER TABLE tables ADD IF NOT EXISTS ttl_backfill_cursor text;

-- Legacy V005: backup table definitions and restore registration.
--
-- backups_by_arn.definition holds the BackupTableDefinition JSON a backup
-- records at CreateBackup (GSIs, LSIs, billing, throughput, table class, SSE,
-- on-demand throughput, vector index names, TTL, stream specification). A row
-- written before this migration has no value here and restores as a legacy
-- backup (keys and billing only).
--
-- backups_by_arn.active_restores maps a restore target's table_id to its table
-- name while that restore copies from the backup. It is written and cleared
-- with lightweight transactions on the backup's own partition, the same
-- partition DeleteBackup moves to DELETING with a lightweight transaction, so
-- the two are serialized by Paxos on one partition.
--
-- tables.restore_* describe a restore target: the source backup and table, the
-- RestoreDateTime to report, and the owner and heartbeat of the process
-- copying into it. The heartbeat is refreshed while the copy runs, and the
-- abandoned-restore sweep removes a target whose heartbeat has gone stale.
ALTER TABLE backups_by_arn ADD IF NOT EXISTS definition text;
ALTER TABLE backups_by_arn ADD IF NOT EXISTS active_restores map<text, text>;
ALTER TABLE tables ADD IF NOT EXISTS restore_source_backup_arn text;
ALTER TABLE tables ADD IF NOT EXISTS restore_source_table_arn text;
ALTER TABLE tables ADD IF NOT EXISTS restore_date_time timestamp;
ALTER TABLE tables ADD IF NOT EXISTS restore_owner uuid;
ALTER TABLE tables ADD IF NOT EXISTS restore_heartbeat timestamp;

-- One row per running server, written at QUORUM when it starts and again
-- every 15 s with a TTL of 45 s. migrate --yes waits while a row exists.
-- started_at is the server's start time in RFC 3339, which every renewal
-- writes again unchanged.
CREATE TABLE IF NOT EXISTS extenddb_servers (
    server_id text PRIMARY KEY,
    host text,
    pid int,
    release text,
    started_at text
);

-- Releases before 1.0 refuse a catalog without this row and insert it again
-- from their migrate. Its presence beside a ledger tells v1.0 that one of
-- them wrote to the catalog.
DELETE FROM settings WHERE key = 'resource_tag_migration';

-- Every legacy catalog file, recorded in the legacy ledger. The insert names
-- no applied_at, so a row a release wrote keeps its time.
INSERT INTO schema_history (version, description) VALUES (1, 'initial_schema');
INSERT INTO schema_history (version, description) VALUES (2, 'backup_restore');
INSERT INTO schema_history (version, description) VALUES (3, 'account_scoped_idempotency');
INSERT INTO schema_history (version, description) VALUES (4, 'ttl_backfill_cursor');
INSERT INTO schema_history (version, description) VALUES (5, 'backup_definitions_and_restore_registration');
