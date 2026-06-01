-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Store TiDB metrics as append-only samples with a native AUTO_RANDOM
-- clustered key. This avoids a hot ON DUPLICATE aggregate row when multiple
-- ExtendDB frontends flush the same metric bucket concurrently.

CREATE TABLE IF NOT EXISTS metrics_samples (
    sample_id BIGINT NOT NULL AUTO_RANDOM,
    bucket TIMESTAMP(6) NOT NULL,
    metric VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    table_name VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    index_name VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    operation VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
    sum DOUBLE NOT NULL DEFAULT 0,
    count BIGINT NOT NULL DEFAULT 0,
    min DOUBLE NOT NULL DEFAULT 1.79e308,
    max DOUBLE NOT NULL DEFAULT -1.79e308,
    PRIMARY KEY (sample_id) CLUSTERED
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  TTL = `bucket` + INTERVAL 24 HOUR TTL_JOB_INTERVAL = '1h';

CREATE INDEX IF NOT EXISTS idx_metrics_samples_bucket
    ON metrics_samples (bucket, metric, table_name, index_name, operation);

ALTER TABLE metrics_samples TTL_ENABLE = 'ON';

UPDATE settings SET value = '0.0.13' WHERE `key` = 'catalog_version';
