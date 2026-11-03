-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 6, warm_throughput, PostgreSQL catalog database: a nullable
-- warm_throughput column on tables and on indexes, holding the WarmThroughput
-- a client set (one JSON record, see extenddb_core::types::WarmThroughputRecord).
-- Stored and reported only: nothing pre-warms or scales on it. A row without
-- a value reports the defaults for its billing mode. IF NOT EXISTS makes a
-- second run change nothing.

ALTER TABLE tables ADD COLUMN IF NOT EXISTS warm_throughput JSONB;
ALTER TABLE indexes ADD COLUMN IF NOT EXISTS warm_throughput JSONB;
