-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Durable, short-lived control-plane claims for TiDB background reconciliation.

ALTER TABLE tables
    ADD COLUMN IF NOT EXISTS control_plane_token VARCHAR(64),
    ADD COLUMN IF NOT EXISTS control_plane_lease_until TIMESTAMP(6);

CREATE INDEX IF NOT EXISTS idx_tables_control_plane_work
    ON tables (table_status, status_transition_at, control_plane_lease_until);

UPDATE settings SET value = '0.0.4' WHERE `key` = 'catalog_version';
