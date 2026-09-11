-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Migration 003: drop the continuous_backups table (catalog version 0.0.4).
--
-- Point-in-time recovery is reported as unsupported: DescribeContinuousBackups
-- always answers PointInTimeRecoveryStatus DISABLED, UpdateContinuousBackups
-- refuses to enable it, and RestoreTableToPointInTime refuses, all served by
-- the engine without consulting storage. The pitr_enabled flag this table
-- stored no longer drives any behavior, so the table is removed.
--
-- Written to tolerate a replay, like 002: the runner applies a migration and
-- records it in `schema_history` as two separate commits, so a crash in
-- between leaves this file applied but unrecorded, and the next migrate runs
-- it again. DROP TABLE IF EXISTS and the version UPDATE are both idempotent.

BEGIN;

DROP TABLE IF EXISTS continuous_backups;

UPDATE settings SET value = '0.0.4' WHERE key = 'catalog_version';

COMMIT;
