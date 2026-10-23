-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Logical backup metadata and continuous backup settings.

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

INSERT INTO settings (key, value) VALUES ('catalog_version', '0.0.2');
