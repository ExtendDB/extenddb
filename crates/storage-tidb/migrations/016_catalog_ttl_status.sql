-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Make TiDB native TTL intent explicit so distributed repair can distinguish
-- enable, enabled, disable, and disabled states.

ALTER TABLE tables
    ADD COLUMN IF NOT EXISTS ttl_index_ready BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE tables
    ADD COLUMN IF NOT EXISTS ttl_native_enabled BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE tables
    ADD COLUMN IF NOT EXISTS ttl_status VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'DISABLED';

UPDATE tables
   SET ttl_status = CASE
       WHEN ttl_attribute IS NULL THEN 'DISABLED'
       WHEN ttl_index_ready AND ttl_native_enabled THEN 'ENABLED'
       ELSE 'ENABLING'
   END;

UPDATE settings SET value = '0.0.16' WHERE `key` = 'catalog_version';
