-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Keep TiDB control-plane work in the same order the distributed poller reads it.

DROP INDEX IF EXISTS idx_tables_control_plane_work ON tables;

CREATE INDEX IF NOT EXISTS idx_tables_control_plane_work
    ON tables (status_transition_at, table_name, table_status);

UPDATE settings SET value = '0.0.28' WHERE `key` = 'catalog_version';
