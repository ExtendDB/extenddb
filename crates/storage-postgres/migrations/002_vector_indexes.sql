-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Vector index catalog metadata (catalog version 0.0.3).
--
-- Ports the SQLite catalog shape with PostgreSQL types: JSONB where SQLite
-- stores JSON text, BOOLEAN where it stores 0/1 (so no check constraint is
-- needed for booleanness), BIGINT for the skip counter.

BEGIN;

-- Vector index metadata. Kept out of `indexes` deliberately: a vector index is
-- not described by a key schema, so reusing that table's `key_schema` column
-- would mean storing something meaningless in a NOT NULL column. The engine
-- supplies index_id, as it does for GSIs.
--
-- Every statement here is written to tolerate a replay. The runner applies a
-- migration and records it in `schema_history` as two separate commits, so a
-- crash in between leaves this file applied but unrecorded, and the next
-- migrate would run it again. Without the guards that second run fails on
-- "relation already exists" and blocks every later migration, on a deployment
-- that is otherwise serving correctly because the version gate is satisfied.
-- Recovering from that needs a hand-written ledger row. 001 guards every one of
-- its tables the same way.
--
-- `search_schema` is nullable because the HASH element is optional (measured
-- against the live service): with one the search is partition-scoped and
-- SearchConditionExpression is required, without one it spans the table.
--
-- `backfilling` mirrors the measured lifecycle: false while CREATING before the
-- scan starts, true while it runs, and the member is absent (NULL) once ACTIVE.
CREATE TABLE IF NOT EXISTS vector_indexes (
    table_id            TEXT NOT NULL,
    index_id            TEXT NOT NULL,
    index_name          TEXT NOT NULL,
    dimensions          INTEGER NOT NULL,
    distance_function   TEXT NOT NULL,
    vector_attribute    JSONB NOT NULL,
    search_schema       JSONB,
    projection          JSONB NOT NULL,
    index_status        TEXT NOT NULL DEFAULT 'CREATING',
    backfilling         BOOLEAN,
    -- Items a backfill skipped because their stored bytes cannot enter the
    -- index (unparseable row, malformed or wrong-dimension vector). NULL until
    -- a backfill has completed; 0 afterwards when nothing was skipped. Kept so
    -- an operator can see that an ACTIVE index deliberately omits rows, rather
    -- than the build looping forever on them or dying part-way.
    skipped_item_count  BIGINT,
    -- Build ownership for multi-process deployments. Several front-ends can
    -- share one PostgreSQL, so an in-process registry cannot answer "is some
    -- process still building this index". The builder records its identity and
    -- renews the heartbeat per batch; a stuck-build sweep in any process reads
    -- both. Unused until the UpdateTable-create lifecycle lands; NULL until a
    -- build claims the row.
    build_owner         TEXT,
    build_heartbeat_at  TIMESTAMPTZ,
    PRIMARY KEY (table_id, index_name),
    CONSTRAINT vector_indexes_table_id_fkey
        FOREIGN KEY (table_id) REFERENCES tables(table_id) ON DELETE CASCADE,
    CONSTRAINT chk_vector_dimensions_positive CHECK (dimensions > 0),
    -- An ACTIVE index must not carry the member at all, which is what the
    -- service does. Enforced here as well as in core, so a bug in the backend
    -- cannot persist a state the wire contract forbids.
    CONSTRAINT chk_vector_active_has_no_backfilling
        CHECK (index_status <> 'ACTIVE' OR backfilling IS NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_vector_indexes_index_id ON vector_indexes (index_id);

-- Snapshot of the source table's vector index configuration at backup time,
-- the same way `key_schema` and `attribute_definitions` are snapshotted. NULL
-- for backups taken before this migration, which cannot have carried vector
-- indexes because the backend could not create them. Restore refuses a backup
-- whose snapshot is non-empty rather than silently dropping a declared index;
-- the snapshot also carries what a future vector-preserving restore needs.
ALTER TABLE backups ADD COLUMN IF NOT EXISTS vector_indexes JSONB;

UPDATE settings SET value = '0.0.3' WHERE key = 'catalog_version';

COMMIT;
