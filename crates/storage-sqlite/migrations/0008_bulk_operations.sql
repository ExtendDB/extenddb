-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 8, bulk_operations, SQLite: the bulk put items tables. Column
-- names are kept identical to the PostgreSQL catalog and data databases,
-- whose 0008 files hold the comments on each table. Every statement is
-- IF NOT EXISTS, so a second run changes nothing.

-- One row per bulk operation: identity, configuration, counters, the resume
-- cursor, and the lease. No foreign key to tables(table_id): the record
-- outlives its table.
CREATE TABLE IF NOT EXISTS bulk_operations (
    account_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    table_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    table_arn TEXT NOT NULL,
    status TEXT NOT NULL,
    client_token TEXT,
    client_token_hash TEXT,
    request_fingerprint TEXT NOT NULL,
    input_config TEXT NOT NULL,
    output_config TEXT NOT NULL,
    input_files TEXT NOT NULL,
    max_write_request_units INTEGER,
    performance_tier TEXT,
    start_time_ms INTEGER NOT NULL,
    end_time_ms INTEGER,
    processed_item_count INTEGER NOT NULL DEFAULT 0,
    failed_item_count INTEGER NOT NULL DEFAULT 0,
    processed_input_bytes INTEGER NOT NULL DEFAULT 0,
    -- NULL until the input is enumerated; a failure born before the walk
    -- never sets it, and Describe then omits TotalInputBytes.
    total_input_bytes INTEGER,
    report_record_count INTEGER NOT NULL DEFAULT 0,
    report_bytes INTEGER NOT NULL DEFAULT 0,
    usage TEXT NOT NULL DEFAULT '{}',
    failure_code TEXT,
    failure_message TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    cursor_file_index INTEGER NOT NULL DEFAULT 0,
    cursor_row_index INTEGER NOT NULL DEFAULT 0,
    lease_owner TEXT,
    lease_until_ms INTEGER,
    -- JSON the engine records at Start for the runner (the store and the
    -- caller); NULL for a row written without one. Never rendered.
    run_context TEXT,
    PRIMARY KEY (account_id, operation_id)
);

-- At most one non-terminal operation per table. The insert that can violate
-- this runs inside the start transaction with INSERT OR IGNORE, so the
-- conflict surfaces as changes() = 0 rather than as an error.
CREATE UNIQUE INDEX IF NOT EXISTS idx_bulk_ops_table_guard
    ON bulk_operations (account_id, table_id)
    WHERE status IN ('IN_PROGRESS', 'CANCELLING');

-- Idempotency: one record per (account, token hash). NULL hashes are
-- distinct in SQLite unique indexes, so token-less operations never collide.
CREATE UNIQUE INDEX IF NOT EXISTS idx_bulk_ops_token
    ON bulk_operations (account_id, client_token_hash);

CREATE INDEX IF NOT EXISTS idx_bulk_ops_list
    ON bulk_operations (account_id, start_time_ms DESC, operation_id DESC);

CREATE INDEX IF NOT EXISTS idx_bulk_ops_claim
    ON bulk_operations (status, lease_until_ms);

-- The input files of a bulk operation, one row per file in list order.
-- Start inserts them; a checkpoint updates the rows of the files its
-- cursor passes, with the row count and tag of a file read to its end or
-- the abandon of one the runner stopped reading. The operation row's
-- input_files column holds '[]'.
CREATE TABLE IF NOT EXISTS bulk_operation_input_files (
    account_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    file_index INTEGER NOT NULL,
    path TEXT NOT NULL,
    size INTEGER NOT NULL,
    row_count INTEGER,
    etag TEXT,
    abandon TEXT,
    PRIMARY KEY (account_id, operation_id, file_index)
);

-- Report records, written with each checkpoint and rendered to files at the
-- end of the run. Deleted by finish and by the retention purge.
CREATE TABLE IF NOT EXISTS bulk_operation_report_records (
    account_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    file_index INTEGER NOT NULL,
    row_index INTEGER NOT NULL,
    record TEXT NOT NULL,
    PRIMARY KEY (account_id, operation_id, kind, file_index, row_index)
);

-- Row-outcome ledger for bulk put rows. Every row a bulk write batch
-- applies is inserted in the same transaction as the item write, with the
-- size of the item it replaced and, when the runner asks, that item, so a
-- run resumed after a crash finds the row applied and accounts for it
-- without writing it again. Keyed by account because the catalog primary
-- key permits the same operation_id in two accounts.
CREATE TABLE IF NOT EXISTS bulk_row_outcomes (
    account_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    file_index INTEGER NOT NULL,
    row_index INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    old_size INTEGER,
    old_image TEXT,
    PRIMARY KEY (account_id, operation_id, file_index, row_index)
);

-- The claim whose bulk write batches commit for an operation, written by
-- each claim before its first batch, and the checkpoint cursor the claim
-- restored with the count of claims in a row that found it unchanged.
CREATE TABLE IF NOT EXISTS bulk_write_owners (
    account_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    owner TEXT NOT NULL,
    cursor_file INTEGER NOT NULL,
    cursor_row INTEGER NOT NULL,
    stalled INTEGER NOT NULL,
    PRIMARY KEY (account_id, operation_id)
);
