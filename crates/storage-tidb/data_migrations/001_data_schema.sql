-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Data database schema for extenddb.
-- These tables live in the data database (separate from the catalog) so that
-- stream records and idempotency tokens can be written atomically with item
-- data within a single TiDB transaction (P54 Bug 1).

-- Stream shards — fixed shards per table, assigned by partition key hash.
-- No FK to catalog tables (cross-database FKs are not possible).
-- Application-level integrity ensures table_id validity.
CREATE TABLE IF NOT EXISTS stream_shards (
    shard_id VARCHAR(128) PRIMARY KEY CLUSTERED,
    table_id VARCHAR(64) NOT NULL,
    parent_shard_id VARCHAR(128),
    starting_sequence_number VARCHAR(64) NOT NULL,
    ending_sequence_number VARCHAR(64),
    next_sequence_number BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);

CREATE INDEX IF NOT EXISTS idx_stream_shards_table
    ON stream_shards (table_id);

-- Stream records — change data capture records.
CREATE TABLE IF NOT EXISTS stream_records (
    shard_id VARCHAR(128) NOT NULL REFERENCES stream_shards(shard_id) ON DELETE CASCADE,
    sequence_number VARCHAR(64) NOT NULL,
    table_id VARCHAR(64) NOT NULL,
    event_name VARCHAR(32) NOT NULL,
    record_data JSON NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (shard_id, sequence_number) CLUSTERED
) TTL = `created_at` + INTERVAL 24 HOUR TTL_JOB_INTERVAL = '1h';

CREATE INDEX IF NOT EXISTS idx_stream_records_created
    ON stream_records (created_at);

-- Idempotency token storage for TransactWriteItems.
CREATE TABLE IF NOT EXISTS idempotency_tokens (
    token       VARCHAR(255) PRIMARY KEY CLUSTERED,
    fingerprint TEXT NOT NULL,
    created_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) TTL = `created_at` + INTERVAL 600 SECOND TTL_JOB_INTERVAL = '10m';

CREATE INDEX IF NOT EXISTS idx_idempotency_tokens_created
    ON idempotency_tokens (created_at);
