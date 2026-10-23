-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Backup table definitions and restore registration (catalog version 0.0.4).
--
-- backups_by_arn.definition holds the BackupTableDefinition JSON a backup
-- records at CreateBackup (GSIs, LSIs, billing, throughput, table class, SSE,
-- on-demand throughput, vector index names, TTL, stream specification). A row
-- written before this migration has no value here and restores as a legacy
-- backup (keys and billing only).
--
-- backups_by_arn.active_restores maps a restore target's table_id to its table
-- name while that restore copies from the backup. It is written and cleared
-- with lightweight transactions on the backup's own partition, the same
-- partition DeleteBackup moves to DELETING with a lightweight transaction, so
-- the two are serialized by Paxos on one partition.
--
-- tables.restore_* describe a restore target: the source backup and table, the
-- RestoreDateTime to report, and the owner and heartbeat of the process
-- copying into it. The heartbeat is refreshed while the copy runs, and the
-- abandoned-restore sweep removes a target whose heartbeat has gone stale.
--
-- NOTE: the migration runner splits files on the semicolon character,
-- including inside comments. Do not use one in a comment.
ALTER TABLE backups_by_arn ADD IF NOT EXISTS definition text;
ALTER TABLE backups_by_arn ADD IF NOT EXISTS active_restores map<text, text>;
ALTER TABLE tables ADD IF NOT EXISTS restore_source_backup_arn text;
ALTER TABLE tables ADD IF NOT EXISTS restore_source_table_arn text;
ALTER TABLE tables ADD IF NOT EXISTS restore_date_time timestamp;
ALTER TABLE tables ADD IF NOT EXISTS restore_owner uuid;
ALTER TABLE tables ADD IF NOT EXISTS restore_heartbeat timestamp;

INSERT INTO settings (key, value) VALUES ('catalog_version', '0.0.4');
