-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 7, restore_progress, PostgreSQL data database.
--
-- The progress record of a store-backed restore: one row per data file the
-- restore applied into its CREATING target, written in the data transaction
-- that wrote the file's rows, so a file is recorded exactly when its rows
-- are durable. The catalog's `table_restores` row names the target; this
-- table lives beside the target's rows because the two databases cannot
-- share a transaction. The restore's end deletes the target's rows, and the
-- reconciler removes rows of targets no RUNNING restore owns.
CREATE TABLE IF NOT EXISTS restore_progress (
    table_id TEXT NOT NULL,
    file_index BIGINT NOT NULL,
    items_applied BIGINT NOT NULL,
    PRIMARY KEY (table_id, file_index)
);
