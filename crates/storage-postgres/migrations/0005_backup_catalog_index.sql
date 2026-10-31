-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 5, backup_catalog_index, PostgreSQL catalog database: the catalog
-- index of backups kept in a backup store, the restore job fields on the
-- restore registrations, the maintenance claim record and the catalog id.
-- Every statement is guarded, and the backfill touches only rows the new
-- columns are still empty on, so a second run changes nothing.

-- One row per backup in a backup store: the fast index for DescribeBackup
-- and ListBackups, and the state machine record of its job. Keyed by
-- (account_id, backup_id); the backup id is unique on its own. The backups
-- earlier releases wrote into the catalog stay in `backups`, which the
-- legacy reader alone consults. `backup_arn` is `COLLATE "C"` so the
-- listings' `ORDER BY created_at, backup_arn` is byte order, the order the
-- engine merges and pages by, whatever the database's collation.
CREATE TABLE IF NOT EXISTS backup_index (
    account_id TEXT NOT NULL,
    backup_id TEXT NOT NULL,
    table_name TEXT NOT NULL,
    backup_arn TEXT COLLATE "C" NOT NULL,
    backup_name TEXT NOT NULL,
    status TEXT NOT NULL,
    store_kind TEXT NOT NULL,
    store_root TEXT NOT NULL,
    store_id TEXT NOT NULL,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    item_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    failure_reason TEXT,
    source_table_details_json JSONB NOT NULL,
    owner_generation BIGINT NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (account_id, backup_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_backup_index_backup_id ON backup_index (backup_id);
CREATE INDEX IF NOT EXISTS idx_backup_index_account_table ON backup_index (account_id, table_name);
CREATE INDEX IF NOT EXISTS idx_backup_index_store_status
    ON backup_index (store_kind, store_root, store_id, status);

-- The restore registration becomes the restore job record: its status
-- (RUNNING, FAILED, DONE), how it reads its backup (legacy or manifest), the
-- store a manifest restore reads, the job claim (owner_generation,
-- heartbeat_at), the spool host, per-batch progress, and how it ended.
ALTER TABLE table_restores ADD COLUMN IF NOT EXISTS status TEXT;
ALTER TABLE table_restores ADD COLUMN IF NOT EXISTS kind TEXT;
ALTER TABLE table_restores ADD COLUMN IF NOT EXISTS store_kind TEXT;
ALTER TABLE table_restores ADD COLUMN IF NOT EXISTS store_root TEXT;
ALTER TABLE table_restores ADD COLUMN IF NOT EXISTS store_id TEXT;
ALTER TABLE table_restores ADD COLUMN IF NOT EXISTS owner_generation BIGINT;
ALTER TABLE table_restores ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ;
ALTER TABLE table_restores ADD COLUMN IF NOT EXISTS spool_host TEXT;
ALTER TABLE table_restores ADD COLUMN IF NOT EXISTS file_index BIGINT;
ALTER TABLE table_restores ADD COLUMN IF NOT EXISTS items_applied BIGINT;
ALTER TABLE table_restores ADD COLUMN IF NOT EXISTS failure_reason TEXT;
ALTER TABLE table_restores ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;

-- The registrations that exist were written by a release without the job
-- record: every one is a legacy restore. One whose table is not CREATING
-- finished (DONE). One whose table is CREATING is a restore the upgrade's
-- stop interrupted: RUNNING, with a heartbeat at the epoch (not NULL, so the
-- staleness test selects it) and an owner generation no job holds, so the
-- first reconciler pass fails it and removes its target.
UPDATE table_restores r
SET kind = 'legacy',
    owner_generation = 0,
    heartbeat_at = '1970-01-01T00:00:00Z',
    status = CASE WHEN t.table_status = 'CREATING' THEN 'RUNNING' ELSE 'DONE' END,
    completed_at = CASE WHEN t.table_status = 'CREATING' THEN NULL ELSE t.creation_date_time END
FROM tables t
WHERE t.table_id = r.table_id AND r.status IS NULL;

ALTER TABLE table_restores ALTER COLUMN status SET NOT NULL;
ALTER TABLE table_restores ALTER COLUMN kind SET NOT NULL;
ALTER TABLE table_restores ALTER COLUMN owner_generation SET NOT NULL;
ALTER TABLE table_restores ALTER COLUMN heartbeat_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_table_restores_status ON table_restores (status, heartbeat_at);

-- The catalog-level maintenance claim: at most one row, held by the
-- maintenance command that changes rows or bytes jobs may be using. Empty
-- until a command takes it.
CREATE TABLE IF NOT EXISTS backup_maintenance (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    holder TEXT NOT NULL,
    owner_generation BIGINT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The catalog's random id, which a backup store's identity file names so a
-- store can tell which catalog owns it. Written once, here, for a new and
-- an existing catalog alike.
INSERT INTO settings (key, value)
SELECT 'catalog_id', gen_random_uuid()::text
WHERE NOT EXISTS (SELECT 1 FROM settings WHERE key = 'catalog_id');
