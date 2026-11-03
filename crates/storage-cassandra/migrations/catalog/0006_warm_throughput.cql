-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 6, warm_throughput, Cassandra catalog keyspace: a nullable
-- warm_throughput text column on tables and on indexes, holding the
-- WarmThroughput a client set (one JSON record, see
-- extenddb_core::types::WarmThroughputRecord). Stored and reported only:
-- nothing pre-warms or scales on it. A row without a value reports the
-- defaults for its billing mode. ADD IF NOT EXISTS makes a second run change
-- nothing. The runner writes the catalog keyspace in front of each table.

ALTER TABLE tables ADD IF NOT EXISTS warm_throughput text;
ALTER TABLE indexes ADD IF NOT EXISTS warm_throughput text;
