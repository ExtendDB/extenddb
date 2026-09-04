-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Consolidated catalog schema for extenddb (catalog version 0.0.2).
-- This is the complete schema applied on fresh installs.

-- Migration tracking table (must be first)
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

-- Seed settings
INSERT INTO settings (key, value) VALUES ('catalog_version', '0.0.1');
INSERT INTO settings (key, value) VALUES ('control_plane_delay_seconds', '0.25');
