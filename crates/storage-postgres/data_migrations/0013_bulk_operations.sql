-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 8, bulk_operations, PostgreSQL data database: exactly-once
-- application of bulk put rows. The catalog half is migrations/0008. Every
-- statement is IF NOT EXISTS, so a second run changes nothing.
--
-- A condition row is not idempotent under replay: a condition that passed
-- before a crash fails on replay against the operation's own first write.
-- The catalog checkpoint cannot be atomic with the data write because the
-- catalog and the data tables are separate databases (ADR-0003), so the fact
-- "this row already happened" lives here, beside the items, and rides the
-- same transaction as the item write. `gsi_pending` is the precedent: a
-- bookkeeping row inserted in the write transaction.

-- Every row a bulk write batch applies, plain or conditional, with the size
-- of the item it replaced and, when the runner asks, that item. A run
-- resumed after a crash, a lease takeover or a transient fault reads the
-- rows recorded past its checkpoint, does not write them again, and
-- accounts for them from what the ledger kept. `outcome` is 'applied'
-- (inserted with the write) or 'condition_failed' (inserted alone, because
-- a failed condition writes nothing else). `finish` and the retention purge
-- delete an operation's rows through DataEngine::delete_bulk_row_outcomes.
--
-- Keyed by account because the catalog primary key permits the same
-- operation_id in two accounts; an unscoped key would interleave two
-- operations' rows on an id collision.
CREATE TABLE IF NOT EXISTS bulk_row_outcomes (
    account_id   TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    file_index   BIGINT NOT NULL,
    row_index    BIGINT NOT NULL,
    outcome      TEXT NOT NULL,
    old_size     BIGINT,
    old_image    JSONB,
    PRIMARY KEY (account_id, operation_id, file_index, row_index)
);

-- The one claim whose batches commit for an operation. Each claim writes its
-- owner before its first batch; a batch locks the row and writes nothing when
-- the owner is not its own, so a runner that lost its lease cannot commit a
-- late batch. The row also keeps the checkpoint cursor the claim restored and
-- how many claims in a row found it unchanged, which bounds resumes that make
-- no progress.
CREATE TABLE IF NOT EXISTS bulk_write_owners (
    account_id   TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    owner        TEXT NOT NULL,
    cursor_file  BIGINT NOT NULL,
    cursor_row   BIGINT NOT NULL,
    stalled      BIGINT NOT NULL,
    PRIMARY KEY (account_id, operation_id)
);
