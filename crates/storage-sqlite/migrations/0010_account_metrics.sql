-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 10, account_metrics, SQLite: replaces the metrics table with
-- account_metrics, whose key has the account a row was recorded for, so two
-- accounts' tables of the same name keep their own rows (#728). account_id is
-- the empty string on a metric of the whole deployment. The rows of metrics
-- are dropped, not copied: they carry no account, and the server keeps
-- metrics for a day. The primary key starts with bucket, so its index serves
-- the reads and the prune, which filter on a range of buckets. IF NOT EXISTS
-- and IF EXISTS make a second run change nothing. ±9e999 store as ±Infinity
-- in SQLite REAL, as in migration 1.

CREATE TABLE IF NOT EXISTS account_metrics (
    bucket TEXT NOT NULL,
    account_id TEXT NOT NULL DEFAULT '',
    metric TEXT NOT NULL,
    table_name TEXT NOT NULL DEFAULT '',
    index_name TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL DEFAULT '',
    sum REAL NOT NULL DEFAULT 0,
    count INTEGER NOT NULL DEFAULT 0,
    min REAL NOT NULL DEFAULT 9e999,
    max REAL NOT NULL DEFAULT -9e999,
    PRIMARY KEY (bucket, account_id, metric, table_name, index_name, operation)
);

DROP TABLE IF EXISTS metrics;
