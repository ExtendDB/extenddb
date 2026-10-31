-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- The catalog index of backups and restore jobs (catalog version 0.0.5).
--
-- backups_by_arn is reused for the index rows of backups whose bytes live in
-- a backup store. A row with store_kind set is such a row and belongs to the
-- catalog index. A row with store_kind unset is an in-catalog backup written
-- by this or an earlier release and belongs to the legacy reader. Neither
-- path sees the other's rows. owner_generation and heartbeat_at are the claim
-- of the job that owns the row (both kinds). backup_size_bytes and item_count,
-- already present, are the index row's size_bytes and item_count.
--
-- backup_index_by_status gives the status-keyed read backups_by_arn cannot:
-- one partition per (store root, status), maintained beside every status
-- write of an index row.
--
-- tables.restore_* gain the restore job record: status, kind, the store the
-- restore reads, the claim (restore_owner_generation and restore_heartbeat_at,
-- beside the restore_owner and restore_heartbeat columns of the previous
-- release, which stop being written), spool and progress fields, the failure
-- reason and the completion time. The registrations that exist are backfilled
-- by the migration runner after these statements: kind legacy, DONE where the
-- table is not CREATING, RUNNING with the epoch heartbeat and the unowned
-- generation where it is.
--
-- backup_maintenance holds the catalog-level maintenance claim, one row.
--
-- settings.catalog_id is written by the runner after these statements, once.
-- settings.catalog_version is written by the runner too, after the backfill
-- and after this file is recorded in schema_history, so a run that fails in
-- the backfill leaves the version at 0.0.4 and migrate --yes runs this file
-- again.
--
-- NOTE: the migration runner splits files on the semicolon character,
-- including inside comments. Do not use one in a comment.
ALTER TABLE backups_by_arn ADD IF NOT EXISTS backup_id text;
ALTER TABLE backups_by_arn ADD IF NOT EXISTS store_kind text;
ALTER TABLE backups_by_arn ADD IF NOT EXISTS store_root text;
ALTER TABLE backups_by_arn ADD IF NOT EXISTS store_id text;
ALTER TABLE backups_by_arn ADD IF NOT EXISTS owner_generation bigint;
ALTER TABLE backups_by_arn ADD IF NOT EXISTS heartbeat_at timestamp;
ALTER TABLE backups_by_arn ADD IF NOT EXISTS completed_at timestamp;
ALTER TABLE backups_by_arn ADD IF NOT EXISTS failure_reason text;
ALTER TABLE backups_by_arn ADD IF NOT EXISTS source_table_details_json text;

CREATE TABLE IF NOT EXISTS backup_index_by_status (
    store_root text,
    backup_status text,
    backup_arn text,
    account_id text,
    store_kind text,
    store_id text,
    PRIMARY KEY ((store_root, backup_status), backup_arn)
);

ALTER TABLE tables ADD IF NOT EXISTS restore_status text;
ALTER TABLE tables ADD IF NOT EXISTS restore_kind text;
ALTER TABLE tables ADD IF NOT EXISTS restore_store_kind text;
ALTER TABLE tables ADD IF NOT EXISTS restore_store_root text;
ALTER TABLE tables ADD IF NOT EXISTS restore_store_id text;
ALTER TABLE tables ADD IF NOT EXISTS restore_owner_generation bigint;
ALTER TABLE tables ADD IF NOT EXISTS restore_heartbeat_at timestamp;
ALTER TABLE tables ADD IF NOT EXISTS restore_spool_host text;
ALTER TABLE tables ADD IF NOT EXISTS restore_file_index int;
ALTER TABLE tables ADD IF NOT EXISTS restore_items_applied bigint;
ALTER TABLE tables ADD IF NOT EXISTS restore_failure_reason text;
ALTER TABLE tables ADD IF NOT EXISTS restore_completed_at timestamp;

CREATE TABLE IF NOT EXISTS backup_maintenance (
    claim text,
    holder text,
    owner_generation bigint,
    claimed_at timestamp,
    heartbeat_at timestamp,
    PRIMARY KEY (claim)
);
