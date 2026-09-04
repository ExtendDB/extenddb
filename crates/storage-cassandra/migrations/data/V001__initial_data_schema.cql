-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Data schema for user-facing DynamoDB resources

-- Migration tracking table (must be first)
CREATE TABLE IF NOT EXISTS schema_history (
    version int PRIMARY KEY,
    description text,
    applied_at timestamp
);

-- Stream shards (DynamoDB Streams shard metadata)
CREATE TABLE IF NOT EXISTS stream_shards (
    shard_id text PRIMARY KEY,
    table_id text,
    parent_shard_id text,
    starting_sequence_number text,
    ending_sequence_number text,
    created_at timestamp
);

CREATE INDEX IF NOT EXISTS ON stream_shards (table_id);

-- Stream records table (DynamoDB Streams)
CREATE TABLE IF NOT EXISTS stream_records (
    shard_id text,
    sequence_number text,
    table_id text,
    event_name text,
    record_data text,
    created_at timestamp,
    PRIMARY KEY ((shard_id), sequence_number)
) WITH CLUSTERING ORDER BY (sequence_number ASC);

-- Transaction ledger (DynamoDB Transactions)
CREATE TABLE IF NOT EXISTS transaction_ledger (
    txn_id uuid PRIMARY KEY,
    state text,
    started_at bigint,
    client_token text,
    request_fingerprint text,
    items_blob text
);

-- Index for efficient age-based scanning during recovery
CREATE INDEX IF NOT EXISTS idx_txn_started_at ON transaction_ledger (started_at);

-- Persistent GSI propagation queue.
-- Rows are inserted inside the same logged batch as the base write, so they
-- survive a crash. Workers drain their partition in (ready_at, id) order
-- and ready_at enforces the per-GSI propagation delay.
CREATE TABLE IF NOT EXISTS gsi_pending (
    worker_partition int,
    last_ready_at    timestamp STATIC,
    ready_at         timestamp,
    id               timeuuid,
    table_id         text,
    old_item         text,
    new_item         text,
    index_context    text,
    PRIMARY KEY ((worker_partition), ready_at, id)
) WITH CLUSTERING ORDER BY (ready_at ASC, id ASC);

-- Generation-fenced expiration queue.
-- `ttl_expiration_buckets` registers which (day bucket, key shard) partitions of
-- `ttl_expirations` hold work, so a sweep can find due partitions without
-- scanning. Queue rows carry the durable deletion state machine: PENDING,
-- CLAIMED, EFFECTS_APPLIED.
CREATE TABLE IF NOT EXISTS ttl_expiration_buckets (
    table_id text,
    generation uuid,
    bucket bigint,
    shard int,
    PRIMARY KEY ((table_id), generation, bucket, shard)
) WITH CLUSTERING ORDER BY (generation ASC, bucket ASC, shard ASC);

CREATE TABLE IF NOT EXISTS ttl_expirations (
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

-- Durable key-only reconciliation outbox. Rows are written in the same logged
-- batch as the base mutation, so an ordinary write's queue entry survives a
-- crash even if the in-batch queue mutation is lost.
CREATE TABLE IF NOT EXISTS ttl_reconcile_pending (
    worker_partition int,
    id timeuuid,
    table_id text,
    account_id text,
    table_name text,
    key_data text,
    PRIMARY KEY ((worker_partition), id)
) WITH CLUSTERING ORDER BY (id ASC);
