-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 10, point_in_time_recovery, SQLite. Creates the two tables
-- point-in-time recovery keeps. IF NOT EXISTS makes a second run change
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
    table_id TEXT PRIMARY KEY REFERENCES tables(table_id) ON DELETE CASCADE,
    recovery_period_days INTEGER NOT NULL,
    enabled_at_us INTEGER NOT NULL,
    pruned_to_us INTEGER NOT NULL DEFAULT 0,
    pinned_at_us INTEGER,
    pin_expires_us INTEGER
);

-- item_history: one row for each committed mutation of a table in
-- pitr_state, written in the mutation's transaction. seq orders the rows of
-- the database in commit order. pk and sk hold the item's key as the table's
-- data table stores it (sk is NULL for a table without a sort key);
-- item_data is the item after the mutation as JSON, or NULL for a delete.
CREATE TABLE IF NOT EXISTS item_history (
    seq INTEGER PRIMARY KEY,
    table_id TEXT NOT NULL REFERENCES tables(table_id) ON DELETE CASCADE,
    pk TEXT NOT NULL,
    sk,
    commit_time_us INTEGER NOT NULL,
    item_data TEXT
);

CREATE INDEX IF NOT EXISTS idx_item_history_key ON item_history (table_id, pk, sk, seq);
CREATE INDEX IF NOT EXISTS idx_item_history_table_seq ON item_history (table_id, seq);
