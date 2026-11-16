-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 10, account_metrics, Cassandra catalog keyspace: replaces the
-- metrics table with account_metrics, whose partition key has the account a
-- row was recorded for, so two accounts' tables of the same name keep their
-- own rows (#728). A partition key cannot be changed and a table cannot be
-- renamed, so the table is new. account_id is the empty string on a metric of
-- the whole deployment. The rows of metrics are dropped, not copied: they
-- carry no account, and expire a day after they are written. IF NOT EXISTS
-- and IF EXISTS make a second run change nothing. The runner writes the
-- catalog keyspace in front of each table.

CREATE TABLE IF NOT EXISTS account_metrics (
    bucket timestamp,
    account_id text,
    metric text,
    table_name text,
    index_name text,
    operation text,
    sum double,
    count bigint,
    min double,
    max double,
    PRIMARY KEY ((bucket, account_id, metric, table_name, index_name, operation))
);

DROP TABLE IF EXISTS metrics;
