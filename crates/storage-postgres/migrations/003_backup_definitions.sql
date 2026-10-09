-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Migration 003: record a backup's table definition, and which backup a
-- restored table came from (catalog version 0.0.4).
--
-- A backup kept the source table's key schema, attribute definitions, and
-- billing mode, and nothing else, so a restored table came back without its
-- global and local secondary indexes, with 5/5 provisioned throughput, and
-- without its table class or encryption settings. One row per backup holds
-- those, in the wire's own shape behind a version marker (see
-- `extenddb_storage::backup_definition`). A backup taken before this
-- migration has no row and restores as before.
--
-- Written to tolerate a replay, like 002: the runner applies a migration and
-- records it in `schema_history` as two separate commits, so a crash in
-- between leaves this file applied but unrecorded, and the next migrate runs
-- it again. CREATE TABLE IF NOT EXISTS and the version UPDATE are idempotent.

BEGIN;

CREATE TABLE IF NOT EXISTS backup_definitions (
    backup_arn TEXT PRIMARY KEY REFERENCES backups(backup_arn) ON DELETE CASCADE,
    definition JSONB NOT NULL
);

-- One row per table created by RestoreTableFromBackup: the backup it came
-- from and when, reported by DescribeTable as RestoreSummary, and used to
-- refuse DeleteBackup while the restore is still running. Not a foreign key
-- to backups: the backup may be deleted after the restore, and the summary
-- keeps naming it, as on the service.
CREATE TABLE IF NOT EXISTS table_restores (
    table_id TEXT PRIMARY KEY REFERENCES tables(table_id) ON DELETE CASCADE,
    source_backup_arn TEXT NOT NULL,
    restore_date_time TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_table_restores_backup ON table_restores (source_backup_arn);

UPDATE settings SET value = '0.0.4' WHERE key = 'catalog_version';

COMMIT;
