-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- RateLimitStore records only failed login attempts. TiDB native TTL owns
-- retention, and the hot count queries use principal/source_ip plus time
-- range indexes, so a success flag is redundant row payload and predicate
-- work.

ALTER TABLE login_attempts DROP COLUMN IF EXISTS success;

UPDATE settings SET value = '0.0.25' WHERE `key` = 'catalog_version';
