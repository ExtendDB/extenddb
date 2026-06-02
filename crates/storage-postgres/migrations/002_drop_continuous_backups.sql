-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Unsupported PITR state is stateless in the storage layer. Enabling PITR
-- returns an explicit unsupported error, and disabling PITR only needs to
-- verify that the table exists before returning a disabled description.

DROP TABLE IF EXISTS continuous_backups;

UPDATE settings SET value = '0.0.3' WHERE key = 'catalog_version';
