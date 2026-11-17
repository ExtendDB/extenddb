-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 1, baseline, PostgreSQL data database.
--
-- Creates the v1.0 data schema on an empty database and brings a data
-- database created by any released version to the same schema: the legacy
-- data files 001 to 004 in order, every statement safe to run again. Per-table
-- objects (`_ddb_<id>`) are not created here; CreateTable creates them, and
-- later migrations change them.
--
-- These tables live in the data database, beside item data, so stream
-- records and idempotency tokens commit in one transaction with the items.
-- No foreign key reaches the catalog, which is another database.

-- Stream shards: fixed shards per table, assigned by partition key hash.
CREATE TABLE IF NOT EXISTS stream_shards (
    shard_id TEXT PRIMARY KEY,
    table_id TEXT NOT NULL,
    parent_shard_id TEXT,
    starting_sequence_number TEXT NOT NULL,
    ending_sequence_number TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stream_shards_table
    ON stream_shards (table_id);

-- Stream records: change data capture records.
CREATE TABLE IF NOT EXISTS stream_records (
    shard_id TEXT NOT NULL REFERENCES stream_shards(shard_id) ON DELETE CASCADE,
    sequence_number TEXT NOT NULL,
    table_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    record_data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (shard_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_stream_records_created
    ON stream_records (created_at);

-- Monotonic sequence for stream record ordering. It starts at the current
-- epoch microseconds, and a run on a database that has one never moves it
-- back: a lower value would hand out sequence numbers already used.
CREATE SEQUENCE IF NOT EXISTS stream_seq START 1;
SELECT setval('stream_seq', GREATEST(
    (EXTRACT(EPOCH FROM now()) * 1000000)::BIGINT,
    (SELECT last_value FROM stream_seq),
    1
));

-- Persistent queue for asynchronous GSI propagation (legacy 002). Rows are
-- inserted in the base write's transaction and consumed by workers in one
-- claim, apply and delete transaction. `index_context` carries everything a
-- worker needs; `worker_partition` hashes the base key, so one worker applies
-- every update of an item in `id` order.
CREATE TABLE IF NOT EXISTS gsi_pending (
    id BIGSERIAL PRIMARY KEY,
    table_id TEXT NOT NULL,
    worker_partition INTEGER NOT NULL,
    old_item JSONB,
    new_item JSONB,
    index_context JSONB NOT NULL,
    ready_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gsi_pending_claim
    ON gsi_pending (worker_partition, ready_at, id);

-- Idempotency tokens for TransactWriteItems, keyed per account (legacy 003).
-- A data database created by v0.1.0 or v0.1.1 that no later release migrated
-- has the table keyed on the token alone. The tokens are a cache with a ten
-- minute lifetime, so that table is recreated rather than copied, as legacy
-- 003 did; a table that has `account_id` is left as it is, with its tokens.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'idempotency_tokens'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'idempotency_tokens'
          AND column_name = 'account_id'
    ) THEN
        DROP TABLE idempotency_tokens;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS idempotency_tokens (
    account_id  TEXT NOT NULL,
    token       TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, token)
);

CREATE INDEX IF NOT EXISTS idx_idempotency_tokens_created
    ON idempotency_tokens (created_at);

-- Vector index build holds the propagation queue reads (legacy 004): one row
-- per table whose vector backfill is running. The row is inserted before the
-- catalog's CREATING row commits and deleted after the index is ACTIVE, so
-- the queue never applies a write a running backfill would overwrite.
CREATE TABLE IF NOT EXISTS vector_index_holds (
    table_id   TEXT NOT NULL,
    index_id   TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (table_id, index_id)
);

CREATE INDEX IF NOT EXISTS idx_vector_index_holds_table
    ON vector_index_holds (table_id);

-- The ledger of releases before 1.0, with every legacy data file and the
-- legacy base key index step, so their `migrate` runs none of them: a replay
-- of legacy 003 would delete every idempotency token.
CREATE TABLE IF NOT EXISTS schema_history (
    filename TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO schema_history (filename) VALUES
    ('001_data_schema.sql'),
    ('002_gsi_pending.sql'),
    ('003_idempotency_account_scope.sql'),
    ('004_vector_index_state.sql'),
    ('003_gsi_base_key_index')
ON CONFLICT (filename) DO NOTHING;
