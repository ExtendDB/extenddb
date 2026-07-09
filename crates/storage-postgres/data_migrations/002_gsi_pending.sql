-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Persistent queue for async GSI propagation.
--
-- Rows are inserted atomically within the base write transaction and consumed
-- by background workers in a single claim+apply+delete transaction, so an
-- uncommitted claim rolls back and the row is reprocessed after a crash.
--
-- Each row is self-describing: `index_context` carries the base key schema,
-- attribute definitions, and the target index definitions captured at enqueue
-- time, so workers apply updates with zero catalog reads. A GSI's key schema
-- and projection are immutable after creation, so the snapshot can never be
-- stale relative to the live index definition.
--
-- `worker_partition` is a stable hash of the base table key. All updates to a
-- given base item share a partition and are consumed by a single worker in `id`
-- order, preserving per-key FIFO ordering (so successive updates to the same
-- item never race in the index and converge to the latest state).

CREATE TABLE IF NOT EXISTS gsi_pending (
    id BIGSERIAL PRIMARY KEY,
    table_id TEXT NOT NULL,
    worker_partition INTEGER NOT NULL,
    old_item JSONB,
    new_item JSONB,
    index_context JSONB NOT NULL,
    ready_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Serves the worker claim: WHERE worker_partition = $1 AND ready_at <= NOW()
-- ORDER BY id. The leading partition column also keeps each worker's scan
-- confined to its own rows.
CREATE INDEX IF NOT EXISTS idx_gsi_pending_claim
    ON gsi_pending (worker_partition, ready_at, id);
