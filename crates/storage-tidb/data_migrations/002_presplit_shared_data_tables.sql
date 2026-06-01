-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Pre-split shared TiDB data tables that receive concurrent writes from every
-- frontend. This uses TiDB's native Region split/scatter path instead of any
-- ExtendDB-side stream/idempotency sharding workers. The commit-sequence index
-- additions make the migration self-contained for older data schemas.

ALTER TABLE stream_records
    ADD COLUMN IF NOT EXISTS commit_sequence_number VARCHAR(64) NULL
    AFTER sequence_number;

ALTER TABLE stream_records
    ADD INDEX IF NOT EXISTS idx_stream_records_commit_sequence
    (shard_id, commit_sequence_number);

SPLIT TABLE stream_records
    BETWEEN ('shardId-', '') AND ('shardId-~', '999999999999999999999999999')
    REGIONS 16;

SPLIT TABLE stream_records INDEX idx_stream_records_commit_sequence
    BETWEEN ('shardId-', '') AND ('shardId-~', '999999999999999999999999999')
    REGIONS 16;

SPLIT TABLE idempotency_tokens
    BETWEEN ('') AND ('~')
    REGIONS 16;
