-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Migration 13, gsi_pending_table_claim, PostgreSQL data database: the
-- per-table claim order for the propagation queue.
--
-- A queue worker serves the tables in its partition in turn, claiming each
-- table's oldest ready row: WHERE worker_partition = $1 AND table_id = $2 AND
-- ready_at <= NOW() ORDER BY id LIMIT 1. The existing claim index leads on
-- (worker_partition, ready_at), which cannot return one table's rows in id order
-- without reading and sorting every ready row in the partition. This index
-- returns them directly, so a claim reads one index entry whatever another
-- table's backlog is. Within a partition ready_at never decreases with id (the
-- enqueue floors it), so a table's first row in id order is also its earliest
-- ready one and the scan stops at the first entry.
--
-- The same index serves the per-batch listing of the tables with a ready row
-- in the partition: one probe per table to its lowest-id entry, whose row's
-- ready_at decides whether the table is listed.
--
-- With statistics present the planner serves the dominant table's claim from
-- gsi_pending_pkey (its rows are at the head of the id order); this index is
-- the plan for a small table in a partition another table dominates, and for a
-- table before its first ANALYZE. Both plans are under 0.05 ms.
--
-- `extenddb migrate --yes` applies it during the stop / migrate / start
-- upgrade sequence, and a server refuses a deployment where it is pending.
-- IF NOT EXISTS makes a second run change nothing.

CREATE INDEX IF NOT EXISTS idx_gsi_pending_table_claim
    ON gsi_pending (worker_partition, table_id, id);
