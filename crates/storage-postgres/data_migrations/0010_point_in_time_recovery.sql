-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 10, point_in_time_recovery, PostgreSQL data database. Creates the
-- two tables point-in-time recovery keeps, beside the item data, so a write's
-- history row commits in the write's transaction. No foreign key reaches the
-- catalog, which is another database. IF NOT EXISTS makes a second run change
-- nothing. The name continuous_backups is not reused: migration 5 drops it
-- again under --reconverge.
--
-- pitr_state: one row for each table whose recovery is enabled, holding the
-- retention in days (1 to 35) and the enable instant in microseconds since
-- the Unix epoch. No row means recovery is disabled. pruned_to_us is the
-- cutoff of the latest prune that removed a row (0 before one): no state
-- before it is held. pinned_at_us is the earliest time a reconstruct in
-- progress reads, which no prune passes until pin_expires_us; both are NULL
-- until the first reconstruct, and a pin past pin_expires_us holds nothing.
CREATE TABLE IF NOT EXISTS pitr_state (
    table_id TEXT PRIMARY KEY,
    recovery_period_days INTEGER NOT NULL,
    enabled_at_us BIGINT NOT NULL,
    pruned_to_us BIGINT NOT NULL DEFAULT 0,
    pinned_at_us BIGINT,
    pin_expires_us BIGINT
);

-- item_history: one row for each committed mutation of a table in
-- pitr_state. pk is the data table's pk; sk is the sort key as text (S as
-- stored, N normalized, B hex) and '' for a table without one; item_data
-- is the item after the mutation, or NULL for a delete.
--
-- seq orders the rows of one key in commit order: two writes to one item
-- take the item's row lock in turn, and each draws its seq after its data
-- write, so the second draws the higher value. That needs the sequence to
-- hand out values in the order of the calls across sessions, which a cache
-- of more than one value per session breaks; the ALTER sets the cache to 1
-- again on a database where it was changed.
CREATE TABLE IF NOT EXISTS item_history (
    seq BIGSERIAL PRIMARY KEY,
    table_id TEXT NOT NULL,
    pk TEXT COLLATE "C" NOT NULL,
    sk TEXT COLLATE "C" NOT NULL,
    commit_time_us BIGINT NOT NULL,
    item_data JSONB
);

ALTER SEQUENCE item_history_seq_seq CACHE 1;

CREATE INDEX IF NOT EXISTS idx_item_history_key ON item_history (table_id, pk, sk, seq);
CREATE INDEX IF NOT EXISTS idx_item_history_table_seq ON item_history (table_id, seq);
