-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- TiDB native TTL state is represented by ttl_status plus the physical table
-- TTL clause. The old ttl_index_ready/ttl_native_enabled booleans duplicated
-- that state and could drift from TiDB's SHOW CREATE TABLE truth.

ALTER TABLE tables
    DROP COLUMN IF EXISTS ttl_index_ready,
    DROP COLUMN IF EXISTS ttl_native_enabled;

UPDATE settings SET value = '0.0.17' WHERE `key` = 'catalog_version';
