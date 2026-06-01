-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Metrics samples and login attempts are append-heavy catalog tables already
-- using TiDB-native randomized row handles (AUTO_RANDOM and SHARD_ROW_ID_BITS).
-- Split their row keyspace explicitly during upgrade so multi-node frontends
-- do not start from a single hot Region.

SPLIT TABLE metrics_samples
    BETWEEN (-9223372036854775808) AND (9223372036854775807)
    REGIONS 16;

SPLIT TABLE login_attempts
    BETWEEN (-9223372036854775808) AND (9223372036854775807)
    REGIONS 16;

UPDATE settings SET value = '0.0.21' WHERE `key` = 'catalog_version';
