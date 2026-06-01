-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- TiDB exposes physical table statistics through information_schema. Keep the
-- catalog as control-plane metadata only instead of caching stale data-plane
-- row/size counters on every table row.

ALTER TABLE tables
    DROP COLUMN IF EXISTS table_size_bytes,
    DROP COLUMN IF EXISTS item_count;

UPDATE settings SET value = '0.0.14' WHERE `key` = 'catalog_version';
