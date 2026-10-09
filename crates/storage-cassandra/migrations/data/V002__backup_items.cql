-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Logical backup payloads. Each backup is split into bounded partitions so a
-- large DynamoDB table does not become one unbounded Cassandra partition.

CREATE TABLE IF NOT EXISTS backup_items (
    backup_arn text,
    bucket int,
    item_index bigint,
    item_data text,
    PRIMARY KEY ((backup_arn, bucket), item_index)
) WITH CLUSTERING ORDER BY (item_index ASC);
