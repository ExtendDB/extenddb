-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Stream shard IDs now put the deterministic shard bucket before table_id.
-- Split the commit-sequence index at those bucket prefixes so one hot stream
-- table distributes index reads across TiDB Regions.
-- Fresh schemas use an AUTO_RANDOM clustered stream handle, so clustered
-- stream writes are scattered by TiDB rather than split by shard prefix.

ALTER TABLE stream_records ATTRIBUTES 'merge_option=deny';

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
