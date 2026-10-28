-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- WarmThroughput on tables and global secondary indexes (catalog version 0.0.5).
--
-- tables.warm_throughput and indexes.warm_throughput hold the WarmThroughput a
-- client set, as one JSON text (see extenddb_core::types::WarmThroughputRecord).
-- The value is stored and reported by DescribeTable so clients that read the
-- member work. Nothing acts on it: there is no pre-warming and no scaling at
-- the database level, and it has no effect on throughput, throttling or
-- storage. A row written before this migration has no value and reports the
-- defaults for its billing mode.
--
-- NOTE: the migration runner splits files on the semicolon character,
-- including inside comments. Do not use one in a comment.
ALTER TABLE tables ADD IF NOT EXISTS warm_throughput text;
ALTER TABLE indexes ADD IF NOT EXISTS warm_throughput text;

INSERT INTO settings (key, value) VALUES ('catalog_version', '0.0.5');
