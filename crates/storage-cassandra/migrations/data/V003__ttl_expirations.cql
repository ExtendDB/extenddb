-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Generation-fenced expiry queue and durable key-only reconciliation outbox.

CREATE TABLE IF NOT EXISTS {{keyspace}}.ttl_expiration_buckets (
    table_id text,
    generation uuid,
    bucket bigint,
    shard int,
    PRIMARY KEY ((table_id), generation, bucket, shard)
) WITH CLUSTERING ORDER BY (generation ASC, bucket ASC, shard ASC);

CREATE TABLE IF NOT EXISTS {{keyspace}}.ttl_expirations (
    table_id text,
    generation uuid,
    bucket bigint,
    shard int,
    expires_at bigint,
    key_hash text,
    key_data text,
    state text,
    work_id uuid,
    work_data text,
    PRIMARY KEY ((table_id, generation, bucket, shard), expires_at, key_hash, key_data)
) WITH CLUSTERING ORDER BY (expires_at ASC, key_hash ASC, key_data ASC);

CREATE TABLE IF NOT EXISTS {{keyspace}}.ttl_reconcile_pending (
    worker_partition int,
    id timeuuid,
    table_id text,
    account_id text,
    table_name text,
    key_data text,
    PRIMARY KEY ((worker_partition), id)
) WITH CLUSTERING ORDER BY (id ASC);
