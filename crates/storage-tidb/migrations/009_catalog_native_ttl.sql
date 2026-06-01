-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Delegate fixed-retention catalog tables to TiDB native TTL.

ALTER TABLE metrics
    TTL = `bucket` + INTERVAL 24 HOUR
    TTL_JOB_INTERVAL = '1h';

ALTER TABLE metrics TTL_ENABLE = 'ON';

ALTER TABLE login_attempts
    TTL = `attempted_at` + INTERVAL 24 HOUR
    TTL_JOB_INTERVAL = '1h';

ALTER TABLE login_attempts TTL_ENABLE = 'ON';

UPDATE settings SET value = '0.0.9' WHERE `key` = 'catalog_version';
