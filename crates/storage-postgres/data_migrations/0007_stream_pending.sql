-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 7, stream_pending, PostgreSQL data database: the table a write
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
--
-- Every record is inserted here and deleted by its numbering, so the table
-- holds few live rows and many dead ones. Autovacuum takes it once 1,000 rows
-- are dead, whatever its size.

CREATE TABLE IF NOT EXISTS stream_pending (
    shard_id TEXT NOT NULL REFERENCES stream_shards(shard_id) ON DELETE CASCADE,
    id BIGSERIAL,
    table_id TEXT NOT NULL,
    event_name TEXT NOT NULL,
    record_data JSONB NOT NULL,
    -- The byte length of the record's JSON text as the write sent it. A
    -- numbering batch adds it up to bound the bytes it copies; it is the size
    -- before TOAST compression, which pg_column_size does not give.
    record_bytes BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (shard_id, id)
) WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000);

-- The order of a shard's records needs both sequences to hand out values in
-- the order of the calls across sessions, which a cache of more than one
-- value per session breaks. Both have a cache of 1 when they are created;
-- these statements set it again on a database where it was changed.
ALTER SEQUENCE stream_seq CACHE 1;
ALTER SEQUENCE stream_pending_id_seq CACHE 1;
