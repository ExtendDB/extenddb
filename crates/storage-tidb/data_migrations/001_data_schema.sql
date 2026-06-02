-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Data database schema for extenddb.
-- These tables live in the data database (separate from the catalog) so that
-- stream records and idempotency tokens can be written atomically with item
-- data within a single TiDB transaction.

-- Stream records — change data capture records. TiDB derives fixed stream
-- shards from table_id. Rows are inserted atomically with item writes under a
-- transaction-local storage sequence, then finalized to the user-visible
-- sequence_number from TiDB MVCC commit_ts plus the in-transaction ordinal.
-- This keeps stream order tied to TiDB commit order without a shard counter.
CREATE TABLE IF NOT EXISTS stream_records (
    record_id BIGINT NOT NULL AUTO_RANDOM(4),
    shard_id VARCHAR(128) NOT NULL,
    sequence_number VARCHAR(64) NOT NULL,
    commit_sequence_number VARCHAR(64),
    table_id VARCHAR(64) NOT NULL,
    event_name VARCHAR(32) NOT NULL,
    record_data JSON NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (record_id) CLUSTERED,
    UNIQUE KEY uk_stream_records_storage_sequence (shard_id, sequence_number),
    INDEX idx_stream_records_commit_sequence (shard_id, commit_sequence_number)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  PRE_SPLIT_REGIONS = 4
  TTL = `created_at` + INTERVAL 24 HOUR TTL_JOB_INTERVAL = '1h';

-- Idempotency token storage for TransactWriteItems.
CREATE TABLE IF NOT EXISTS idempotency_tokens (
    token       VARCHAR(255) PRIMARY KEY CLUSTERED,
    fingerprint TEXT NOT NULL,
    claim_id    VARCHAR(36) NOT NULL,
    created_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  TTL = `created_at` + INTERVAL 600 SECOND TTL_JOB_INTERVAL = '10m';
