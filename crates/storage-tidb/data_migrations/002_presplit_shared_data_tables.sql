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

SPLIT TABLE stream_records INDEX idx_stream_records_commit_sequence BY
    ('shardId-000000000001-', ''),
    ('shardId-000000000002-', ''),
    ('shardId-000000000003-', ''),
    ('shardId-000000000004-', ''),
    ('shardId-000000000005-', ''),
    ('shardId-000000000006-', ''),
    ('shardId-000000000007-', ''),
    ('shardId-000000000008-', ''),
    ('shardId-000000000009-', ''),
    ('shardId-000000000010-', ''),
    ('shardId-000000000011-', ''),
    ('shardId-000000000012-', ''),
    ('shardId-000000000013-', ''),
    ('shardId-000000000014-', ''),
    ('shardId-000000000015-', '');

SPLIT TABLE idempotency_tokens BY
    ('10000000:'),
    ('20000000:'),
    ('30000000:'),
    ('40000000:'),
    ('50000000:'),
    ('60000000:'),
    ('70000000:'),
    ('80000000:'),
    ('90000000:'),
    ('a0000000:'),
    ('b0000000:'),
    ('c0000000:'),
    ('d0000000:'),
    ('e0000000:'),
    ('f0000000:');
