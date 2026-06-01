-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Drop ExtendDB-level DDL ownership now that TiDB native online DDL is the
-- distributed schema-job coordinator.

DROP INDEX IF EXISTS idx_tables_control_plane_work ON tables;

ALTER TABLE tables
    DROP COLUMN IF EXISTS control_plane_token,
    DROP COLUMN IF EXISTS control_plane_lease_until;

CREATE INDEX IF NOT EXISTS idx_tables_control_plane_work
    ON tables (table_status, status_transition_at);

DELETE FROM settings WHERE `key` = 'control_plane_delay_seconds';

UPDATE settings SET value = '0.0.11' WHERE `key` = 'catalog_version';
