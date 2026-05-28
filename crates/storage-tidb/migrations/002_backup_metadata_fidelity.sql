-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Backup metadata fidelity for catalog version 0.0.3.

ALTER TABLE backups
    ADD COLUMN IF NOT EXISTS deletion_protection_enabled BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS backup_indexes (
    backup_arn VARCHAR(512) NOT NULL REFERENCES backups(backup_arn) ON DELETE CASCADE,
    index_id VARCHAR(64) NOT NULL,
    index_name VARCHAR(255) NOT NULL,
    index_type VARCHAR(16) NOT NULL,
    key_schema JSON NOT NULL,
    projection JSON NOT NULL,
    provisioned_throughput JSON,
    PRIMARY KEY (backup_arn, index_name) CLUSTERED
);

CREATE TABLE IF NOT EXISTS backup_tags (
    backup_arn VARCHAR(512) NOT NULL REFERENCES backups(backup_arn) ON DELETE CASCADE,
    tag_key VARCHAR(255) NOT NULL,
    tag_value TEXT NOT NULL,
    PRIMARY KEY (backup_arn, tag_key) CLUSTERED
);

UPDATE settings SET value = '0.0.3' WHERE `key` = 'catalog_version';
