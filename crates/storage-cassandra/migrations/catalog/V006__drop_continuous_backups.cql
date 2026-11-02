-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Drop the continuous_backups table (catalog version 0.0.5).
--
-- Point-in-time recovery is reported as unsupported: DescribeContinuousBackups
-- always answers PointInTimeRecoveryStatus DISABLED, UpdateContinuousBackups
-- refuses to enable it, and RestoreTableToPointInTime refuses, all served by
-- the engine without consulting storage. The pitr_enabled flag this table
-- stored no longer drives any behavior, so the table is removed. DROP TABLE IF
-- EXISTS lets a replay of this file run again.
--
-- NOTE: the migration runner splits files on the semicolon character,
-- including inside comments. Do not use one in a comment.
DROP TABLE IF EXISTS continuous_backups;
INSERT INTO settings (key, value) VALUES ('catalog_version', '0.0.5');
