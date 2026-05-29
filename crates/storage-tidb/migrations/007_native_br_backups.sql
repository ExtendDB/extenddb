-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Use TiDB BR as the TiDB backend's backup data plane.

ALTER TABLE backups
    ADD COLUMN IF NOT EXISTS backup_backend VARCHAR(32) NOT NULL DEFAULT 'legacy-logical';

ALTER TABLE backups
    ADD COLUMN IF NOT EXISTS storage_uri TEXT;

ALTER TABLE backups
    ADD COLUMN IF NOT EXISTS physical_table_name VARCHAR(255);

ALTER TABLE backups
    ADD COLUMN IF NOT EXISTS native_snapshot_tso VARCHAR(64);

-- Old TiDB logical backups depended on catalog-row copies. They cannot be
-- restored after moving TiDB to native BR semantics, so hide them from list
-- operations instead of pretending they are usable native backups.
UPDATE backups
SET backup_status = 'DELETED'
WHERE backup_backend = 'legacy-logical'
  AND storage_uri IS NULL
  AND backup_status != 'DELETED';

DROP TABLE IF EXISTS backup_items;

UPDATE settings SET value = '0.0.7' WHERE `key` = 'catalog_version';
