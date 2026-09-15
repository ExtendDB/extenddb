-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Vector index build state that the propagation queue has to read.
--
-- The queue must not apply a write to an index whose backfill is still running:
-- the backfill holds an older snapshot of the same item, so applying the newer
-- write first lets the backfill overwrite it, and the backfill's deliberately
-- plain INSERT would collide with the row the write left behind.
--
-- The SQLite backend answers this by joining its claim query against the catalog's
-- vector_indexes table. That is impossible here, because the catalog is a separate
-- database from the data one and a claim transaction cannot span them. So the
-- claim-time fact lives in the data database as its own row.
--
-- Ordering rules, which are what make it safe rather than merely present:
--   * the hold row is inserted BEFORE the catalog's CREATING row commits, so no
--     writer can enqueue against an index the queue does not yet know to hold;
--   * the hold row is deleted AFTER the catalog flips the index to ACTIVE, so the
--     queue never resumes against an index that is not yet published.
-- Held slightly too long is harmless: the rows wait. Released early is not.
--
-- Per TABLE, not per index, matching the shared lifecycle contract: a secondary
-- index row and a vector row for the same item must keep their relative order, so
-- the hold stops the table's queue rather than one index's.

BEGIN;

CREATE TABLE IF NOT EXISTS vector_index_holds (
    table_id   TEXT NOT NULL,
    index_id   TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (table_id, index_id)
);

-- The claim query filters on table_id, so that is what needs to be fast. A
-- crashed build leaves an orphan row, which the reconciler sweeps by age.
CREATE INDEX IF NOT EXISTS idx_vector_index_holds_table
    ON vector_index_holds (table_id);

COMMIT;
