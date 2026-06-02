-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Keep the shared idempotency-token table pinned against Region merge.
--
-- Current schemas use a TiDB-native AUTO_RANDOM clustered row handle plus a
-- TIDB_SHARD unique token index. Startup validates that native layout instead
-- of rebuilding the table behind concurrent distributed writers; this static
-- migration is intentionally limited to the idempotent table attribute.

ALTER TABLE idempotency_tokens ATTRIBUTES 'merge_option=deny';
