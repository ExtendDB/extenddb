-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Track whether a table's DynamoDB TTL is delegated to TiDB native TTL.

ALTER TABLE tables
    ADD COLUMN IF NOT EXISTS ttl_native_enabled BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE settings SET value = '0.0.6' WHERE `key` = 'catalog_version';
