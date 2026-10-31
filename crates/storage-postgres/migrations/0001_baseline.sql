-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 1, baseline, PostgreSQL catalog database.
--
-- Creates the v1.0 catalog schema on an empty database and brings a catalog
-- created by any released version to the same schema. It is the legacy
-- catalog files 001, 002 and 003 in order, with every statement safe to run
-- on a catalog that already has some or all of their objects:
--
-- * every CREATE uses IF NOT EXISTS, and the index names are 001's own, so a
--   release that replays 001 against this catalog stops at its first index
--   before it changes anything;
-- * the three `tables` columns added to 001 after it shipped are added with
--   ADD COLUMN IF NOT EXISTS, for catalogs created by v0.1.0 and v0.1.1;
-- * the shape steps earlier builds ran from `migrate` and `serve` are here;
-- * the stream sequence is set forward only, never below its current value.
--
-- No settings row is seeded: every one has an equal default in code. The
-- runner writes the version and runs this file and its ledger row in one
-- transaction, so the file has no transaction statements of its own.

-- Accounts.
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
    table_class TEXT,
    sse_specification JSONB,
    on_demand_throughput JSONB,
    PRIMARY KEY (account_id, table_name),
    CONSTRAINT tables_table_id_unique UNIQUE (table_id)
);

-- Catalogs created by v0.1.0 and v0.1.1 lack these three columns, which
-- CreateTable and DescribeTable name.
ALTER TABLE tables ADD COLUMN IF NOT EXISTS table_class TEXT;
ALTER TABLE tables ADD COLUMN IF NOT EXISTS sse_specification JSONB;
ALTER TABLE tables ADD COLUMN IF NOT EXISTS on_demand_throughput JSONB;

CREATE INDEX IF NOT EXISTS idx_tables_pending_transition
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

-- The ledger of releases before 1.0, kept so their `migrate` finds every
-- file it knows recorded and runs none of them.
CREATE TABLE IF NOT EXISTS schema_history (
    filename TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Settings (version, data database connection, runtime config).
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Stream shards. Unused in the catalog database: streams live in the data
-- database. Kept because every released catalog has them.
CREATE TABLE IF NOT EXISTS stream_shards (
    shard_id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL REFERENCES tables(table_id) ON DELETE CASCADE,
    parent_shard_id TEXT,
    starting_sequence_number TEXT NOT NULL,
    ending_sequence_number TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stream_shards_table ON stream_shards (table_id);

-- Stream records. Unused in the catalog database, as stream_shards.
CREATE TABLE IF NOT EXISTS stream_records (
    shard_id TEXT NOT NULL REFERENCES stream_shards(shard_id) ON DELETE CASCADE,
    sequence_number TEXT NOT NULL,
    table_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    record_data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (shard_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_stream_records_created ON stream_records (created_at);

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

-- Idempotency tokens. Unused in the catalog database: the data database
-- holds them, keyed on (account_id, token). Kept because every released
-- catalog has it.
CREATE TABLE IF NOT EXISTS idempotency_tokens (
    token       TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_idempotency_tokens_created ON idempotency_tokens (created_at);

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

CREATE INDEX IF NOT EXISTS idx_metrics_bucket ON metrics (bucket);

-- Login attempt tracking.
CREATE TABLE IF NOT EXISTS login_attempts (
    principal     TEXT NOT NULL,
    attempted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    success       BOOLEAN NOT NULL,
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
    backup_size_bytes BIGINT NOT NULL DEFAULT 0,
    item_count BIGINT NOT NULL DEFAULT 0,
    key_schema JSONB NOT NULL,
    attribute_definitions JSONB NOT NULL,
    billing_mode TEXT NOT NULL DEFAULT 'PAY_PER_REQUEST',
    provisioned_throughput JSONB,
    stream_specification JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_backups_table ON backups (account_id, table_name);

-- Backup items.
CREATE TABLE IF NOT EXISTS backup_items (
    backup_arn TEXT NOT NULL REFERENCES backups(backup_arn) ON DELETE CASCADE,
    pk TEXT NOT NULL,
    sk TEXT,
    item_data JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_backup_items_arn ON backup_items (backup_arn);

-- Continuous backups / PITR status.
CREATE TABLE IF NOT EXISTS continuous_backups (
    account_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    pitr_enabled BOOLEAN NOT NULL DEFAULT false,
    earliest_restorable TIMESTAMPTZ,
    latest_restorable TIMESTAMPTZ,
    PRIMARY KEY (account_id, table_name)
);

-- Stream sequence: starts at the current epoch microseconds, and a run on a
-- catalog that has one never moves it back.
CREATE SEQUENCE IF NOT EXISTS stream_seq START 1;
SELECT setval('stream_seq', GREATEST(
    (EXTRACT(EPOCH FROM now()) * 1000000)::BIGINT,
    (SELECT last_value FROM stream_seq),
    1
));

-- Vector index metadata (legacy 002). Kept out of `indexes`: a vector index
-- has no key schema. `search_schema` is nullable because the HASH element is
-- optional. `backfilling` is false while CREATING before the scan starts,
-- true while it runs, and NULL once ACTIVE. `skipped_item_count` counts items
-- a backfill could not index. `build_owner` and `build_heartbeat_at` record
-- which process builds the index.
CREATE TABLE IF NOT EXISTS vector_indexes (
    table_id            TEXT NOT NULL,
    index_id            TEXT NOT NULL,
    index_name          TEXT NOT NULL,
    dimensions          INTEGER NOT NULL,
    distance_function   TEXT NOT NULL,
    vector_attribute    JSONB NOT NULL,
    search_schema       JSONB,
    projection          JSONB NOT NULL,
    index_status        TEXT NOT NULL DEFAULT 'CREATING',
    backfilling         BOOLEAN,
    skipped_item_count  BIGINT,
    build_owner         TEXT,
    build_heartbeat_at  TIMESTAMPTZ,
    PRIMARY KEY (table_id, index_name),
    CONSTRAINT vector_indexes_table_id_fkey
        FOREIGN KEY (table_id) REFERENCES tables(table_id) ON DELETE CASCADE,
    CONSTRAINT chk_vector_dimensions_positive CHECK (dimensions > 0),
    CONSTRAINT chk_vector_active_has_no_backfilling
        CHECK (index_status <> 'ACTIVE' OR backfilling IS NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_vector_indexes_index_id ON vector_indexes (index_id);

-- The source table's vector index configuration at backup time; NULL for
-- backups taken before vector indexes existed.
ALTER TABLE backups ADD COLUMN IF NOT EXISTS vector_indexes JSONB;

-- A backup's table definition (legacy 003): one row per backup, in the
-- wire's shape behind a version marker. A backup without a row restores
-- from its key schema and attribute definitions alone.
CREATE TABLE IF NOT EXISTS backup_definitions (
    backup_arn TEXT PRIMARY KEY REFERENCES backups(backup_arn) ON DELETE CASCADE,
    definition JSONB NOT NULL
);

-- One row per table RestoreTableFromBackup created: the backup and source
-- table it came from, and the restore's RestoreDateTime. Not a foreign key
-- to backups: the summary keeps naming a backup deleted after the restore.
-- restore_date_time has no default: the engine always passes the value.
CREATE TABLE IF NOT EXISTS table_restores (
    table_id TEXT PRIMARY KEY REFERENCES tables(table_id) ON DELETE CASCADE,
    source_backup_arn TEXT NOT NULL,
    source_table_arn TEXT NOT NULL,
    restore_date_time TIMESTAMPTZ NOT NULL
);

-- A catalog that ran a pre-release revision of legacy 003 has table_restores
-- without source_table_arn (added here as nullable: its rows have no value)
-- and with a default on restore_date_time.
ALTER TABLE table_restores ADD COLUMN IF NOT EXISTS source_table_arn TEXT;
ALTER TABLE table_restores ALTER COLUMN restore_date_time DROP DEFAULT;

CREATE INDEX IF NOT EXISTS idx_table_restores_backup ON table_restores (source_backup_arn);

-- Releases before 1.0 refuse a catalog without this row and insert it again
-- from their `migrate`; its presence beside a ledger tells v1.0 that one of
-- them wrote to the catalog.
DELETE FROM settings WHERE key = 'resource_tag_migration';

-- Every legacy catalog file, recorded in the legacy ledger.
INSERT INTO schema_history (filename) VALUES
    ('001_schema.sql'),
    ('002_vector_indexes.sql'),
    ('003_backup_definitions.sql')
ON CONFLICT (filename) DO NOTHING;
