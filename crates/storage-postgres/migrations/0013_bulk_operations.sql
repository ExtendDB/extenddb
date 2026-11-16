-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 8, bulk_operations, PostgreSQL catalog database: the bulk put
-- items records. The data database half is data_migrations/0008. Every
-- statement is IF NOT EXISTS, so a second run changes nothing.

-- One row per bulk operation, everything the wire surface and the runner
-- need: identity, configuration, counters, the resume cursor, and the lease.
--
-- No foreign key to tables(table_id) on purpose. The record must outlive its
-- table: a run whose table is deleted finishes FAILED and stays describable
-- until retention removes it. A cascading reference would erase the record
-- with the table, and a plain reference would block DeleteTable while a
-- terminal record is retained.
CREATE TABLE IF NOT EXISTS bulk_operations (
    account_id              TEXT NOT NULL,
    -- COLLATE "C" so the operation_id tiebreak in the list and claim
    -- orderings, and the exclusive list cursor, compare bytewise the way
    -- SQLite's memcmp does. Under a locale collation a future id shape with
    -- mixed case or punctuation would paginate differently per backend.
    operation_id            TEXT COLLATE "C" NOT NULL,
    table_id                TEXT NOT NULL,
    table_name              TEXT NOT NULL,
    table_arn               TEXT NOT NULL,
    status                  TEXT NOT NULL,
    client_token            TEXT,
    client_token_hash       TEXT,
    request_fingerprint     TEXT NOT NULL,
    input_config            JSONB NOT NULL,
    output_config           JSONB NOT NULL,
    -- Always '[]': the files are rows of bulk_operation_input_files.
    input_files             JSONB NOT NULL,
    max_write_request_units BIGINT,
    performance_tier        TEXT,
    start_time_ms           BIGINT NOT NULL,
    end_time_ms             BIGINT,
    processed_item_count    BIGINT NOT NULL DEFAULT 0,
    failed_item_count       BIGINT NOT NULL DEFAULT 0,
    processed_input_bytes   BIGINT NOT NULL DEFAULT 0,
    -- NULL until the input is enumerated; a failure born before the walk
    -- (missing input bucket, input owner mismatch) never sets it, and
    -- Describe then omits TotalInputBytes as the service does.
    total_input_bytes       BIGINT,
    report_record_count     BIGINT NOT NULL DEFAULT 0,
    report_bytes            BIGINT NOT NULL DEFAULT 0,
    usage                   JSONB NOT NULL,
    failure_code            TEXT,
    failure_message         TEXT,
    cancel_requested        BOOLEAN NOT NULL DEFAULT FALSE,
    cursor_file_index       BIGINT NOT NULL DEFAULT 0,
    cursor_row_index        BIGINT NOT NULL DEFAULT 0,
    lease_owner             TEXT,
    lease_until_ms          BIGINT,
    -- What the engine records at Start for the runner (the store and the
    -- caller); NULL for a row written without one. Never rendered.
    run_context             JSONB,
    PRIMARY KEY (account_id, operation_id)
);

-- At most one non-terminal operation per table. The start transaction's
-- INSERT ... ON CONFLICT DO NOTHING runs against this index, so two
-- concurrent starts on one table cannot both succeed and the loser is
-- answered synchronously.
CREATE UNIQUE INDEX IF NOT EXISTS idx_bulk_ops_table_guard
    ON bulk_operations (account_id, table_id)
    WHERE status IN ('IN_PROGRESS', 'CANCELLING');

-- ClientToken uniqueness, per account, on the SHA-256 hash so the unbounded
-- token never enters an index. NULL hashes are distinct, so tokenless
-- operations never collide here.
CREATE UNIQUE INDEX IF NOT EXISTS idx_bulk_ops_token
    ON bulk_operations (account_id, client_token_hash);

-- ListBulkOperations pages newest first with an exclusive cursor.
CREATE INDEX IF NOT EXISTS idx_bulk_ops_list
    ON bulk_operations (account_id, start_time_ms DESC, operation_id DESC);

-- The claim query filters on status and lease expiry across all accounts.
CREATE INDEX IF NOT EXISTS idx_bulk_ops_claim
    ON bulk_operations (status, lease_until_ms);

-- Report records a checkpoint persisted and the report renderer has not yet
-- consumed. `finish` deletes an operation's rows after rendering, and the
-- retention purge deletes whatever a crashed run left behind.
--
-- `record` is serialized JSON text, not jsonb, and TEXT is deliberate: a
-- record embeds submitted item values, a DynamoDB string may contain
-- U+0000, and jsonb refuses the \u0000 escape. A checkpoint the column
-- refuses would be refused again on every retry, leaving the operation
-- permanently non-terminal. The SQLite backend stores the same text.
CREATE TABLE IF NOT EXISTS bulk_operation_report_records (
    account_id   TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    kind         TEXT NOT NULL,
    file_index   BIGINT NOT NULL,
    row_index    BIGINT NOT NULL,
    record       TEXT NOT NULL,
    PRIMARY KEY (account_id, operation_id, kind, file_index, row_index)
);

-- The input files of a bulk operation, one row per file in list order.
-- Start inserts them, and a checkpoint updates the rows of the files its
-- cursor passes, so a checkpoint writes only the files it passes.
CREATE TABLE IF NOT EXISTS bulk_operation_input_files (
    account_id   TEXT NOT NULL,
    -- COLLATE "C" as on bulk_operations.operation_id.
    operation_id TEXT COLLATE "C" NOT NULL,
    file_index   BIGINT NOT NULL,
    path         TEXT NOT NULL,
    size         BIGINT NOT NULL,
    -- Set by the checkpoint that moves the cursor past a file read to its
    -- end, with the file's tag.
    row_count    BIGINT,
    etag         TEXT,
    -- Set by the checkpoint that moves the cursor past an abandoned file.
    abandon      JSONB,
    PRIMARY KEY (account_id, operation_id, file_index)
);
