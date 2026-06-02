-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- TiDB does not persist unsupported DynamoDB PITR state. DescribeContinuousBackups
-- reports the stateless disabled PITR status after resolving the table catalog
-- row, and UpdateContinuousBackups(true) returns an explicit unsupported error.
-- Dropping this table removes mutable table-name state that could otherwise
-- survive delete/recreate cycles across distributed frontends.

DROP TABLE IF EXISTS continuous_backups;

UPDATE settings SET value = '0.0.23' WHERE `key` = 'catalog_version';
