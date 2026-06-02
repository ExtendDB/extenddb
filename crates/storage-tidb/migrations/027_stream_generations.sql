-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Keep DynamoDB Streams generations readable for 24 hours after disable/delete
-- using TiDB native TTL instead of frontend cleanup.

CREATE TABLE IF NOT EXISTS stream_generations (
    account_id VARCHAR(32) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    table_id VARCHAR(64) NOT NULL,
    stream_label VARCHAR(64) NOT NULL,
    key_schema JSON NOT NULL,
    stream_specification JSON NOT NULL,
    stream_status VARCHAR(32) NOT NULL DEFAULT 'ENABLED',
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    disabled_at TIMESTAMP(6),
    expires_at TIMESTAMP(6),
    PRIMARY KEY (account_id, table_name, stream_label) CLUSTERED,
    INDEX idx_stream_generations_table_id (table_id, stream_label)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  TTL = `expires_at` + INTERVAL 0 SECOND TTL_JOB_INTERVAL = '1h';

INSERT IGNORE INTO stream_generations
    (account_id, table_name, table_id, stream_label, key_schema,
     stream_specification, stream_status, created_at, disabled_at, expires_at)
SELECT account_id,
       table_name,
       table_id,
       stream_label,
       key_schema,
       stream_specification,
       CASE
           WHEN JSON_UNQUOTE(JSON_EXTRACT(stream_specification, '$.StreamEnabled')) = 'true'
           THEN 'ENABLED'
           ELSE 'DISABLED'
       END,
       creation_date_time,
       CASE
           WHEN JSON_UNQUOTE(JSON_EXTRACT(stream_specification, '$.StreamEnabled')) = 'true'
           THEN NULL
           ELSE CURRENT_TIMESTAMP(6)
       END,
       CASE
           WHEN JSON_UNQUOTE(JSON_EXTRACT(stream_specification, '$.StreamEnabled')) = 'true'
           THEN NULL
           ELSE CURRENT_TIMESTAMP(6) + INTERVAL 24 HOUR
       END
FROM tables
WHERE stream_label IS NOT NULL
  AND stream_specification IS NOT NULL;

UPDATE settings SET value = '0.0.27' WHERE `key` = 'catalog_version';
