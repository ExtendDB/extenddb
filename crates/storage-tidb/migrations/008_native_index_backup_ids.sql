-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Preserve native TiDB generated-column/index identifiers in backup metadata.

ALTER TABLE backup_indexes
    ADD COLUMN IF NOT EXISTS index_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '';

UPDATE backup_indexes bi
JOIN backups b ON b.backup_arn = bi.backup_arn
JOIN indexes i ON i.table_id = b.table_id AND i.index_name = bi.index_name
SET bi.index_id = i.index_id
WHERE bi.index_id = '';

UPDATE settings SET value = '0.0.8' WHERE `key` = 'catalog_version';
