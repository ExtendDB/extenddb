-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- TiDB metrics are append-only `metrics_samples` rows with native AUTO_RANDOM
-- placement and native TTL. The legacy aggregate `metrics` table serialized
-- concurrent frontends on one row per bucket and is no longer part of runtime
-- reads or startup TTL repair.

DROP TABLE IF EXISTS metrics;

UPDATE settings SET value = '0.0.18' WHERE `key` = 'catalog_version';
