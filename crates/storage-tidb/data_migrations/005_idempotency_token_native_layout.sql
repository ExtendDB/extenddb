-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Keep the shared idempotency-token table pinned against Region merge.
--
-- Current schemas use a TiDB-native AUTO_RANDOM clustered row handle plus a
-- TIDB_SHARD unique token index. The runtime repair path rebuilds older
-- clustered-token tables into that native layout; this static migration is
-- intentionally limited to the idempotent table attribute so it remains safe
-- for both layouts.

ALTER TABLE idempotency_tokens ATTRIBUTES 'merge_option=deny';
