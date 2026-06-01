-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Delegate expired assume-role session retention to TiDB native TTL.

ALTER TABLE iam_sessions
    TTL = `expires_at` + INTERVAL 24 HOUR
    TTL_JOB_INTERVAL = '1h';

ALTER TABLE iam_sessions TTL_ENABLE = 'ON';

UPDATE settings SET value = '0.0.10' WHERE `key` = 'catalog_version';
