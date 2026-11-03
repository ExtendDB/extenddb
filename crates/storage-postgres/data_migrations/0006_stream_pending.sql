-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 6, stream_pending, PostgreSQL data database: the table a write
-- stores its stream record in, inside the write's own transaction. The record
-- gets its sequence number only after the write commits, when a numbering
-- step moves the shard's committed records into stream_records in `id` order
-- while it holds the shard's row (`number_shard_batch` in stream_engine.rs).
-- A number drawn inside the write's transaction could commit after a higher
-- number a reader had already read past (#808).
--
-- `id` orders the records of a shard. Two writes to one item take the item's
-- row lock in turn, so the second inserts its record after the first commits
-- and gets the higher `id`. IF NOT EXISTS makes a second run change nothing.

CREATE TABLE IF NOT EXISTS stream_pending (
    shard_id TEXT NOT NULL REFERENCES stream_shards(shard_id) ON DELETE CASCADE,
    id BIGSERIAL,
    table_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    record_data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (shard_id, id)
);
