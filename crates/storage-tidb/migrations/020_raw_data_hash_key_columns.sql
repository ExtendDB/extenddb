-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Existing per-table TiDB data key columns are repaired by the data-database
-- migration pass, which can inspect dynamic `_ddb_*` tables. This catalog
-- migration advances the TiDB backend version for the raw binary hash-key
-- physical layout.

UPDATE settings SET value = '0.0.20' WHERE `key` = 'catalog_version';
