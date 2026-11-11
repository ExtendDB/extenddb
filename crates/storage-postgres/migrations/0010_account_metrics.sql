-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 10, account_metrics, PostgreSQL catalog database: replaces the
-- metrics table with account_metrics, whose key has the account a row was
-- recorded for, so two accounts' tables of the same name keep their own rows
-- (#728). account_id is the empty string on a metric of the whole
-- deployment. The rows of metrics are dropped, not copied: they carry no
-- account, and the server keeps metrics for a day. IF NOT EXISTS and
-- IF EXISTS make a second run change nothing.

CREATE TABLE IF NOT EXISTS account_metrics (
    bucket TIMESTAMPTZ NOT NULL,
    account_id TEXT NOT NULL DEFAULT '',
    metric TEXT NOT NULL,
    table_name TEXT NOT NULL DEFAULT '',
    index_name TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL DEFAULT '',
    sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    count BIGINT NOT NULL DEFAULT 0,
    min DOUBLE PRECISION NOT NULL DEFAULT 'Infinity',
    max DOUBLE PRECISION NOT NULL DEFAULT '-Infinity',
    PRIMARY KEY (bucket, account_id, metric, table_name, index_name, operation)
);

CREATE INDEX IF NOT EXISTS idx_account_metrics_bucket ON account_metrics (bucket);

DROP TABLE IF EXISTS metrics;
