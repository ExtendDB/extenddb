-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Fence TTL enable, backfill, sweep, and cleanup cycles.

ALTER TABLE {{keyspace}}.tables ADD IF NOT EXISTS ttl_generation uuid;
ALTER TABLE {{keyspace}}.tables ADD IF NOT EXISTS ttl_sweep_owner uuid;
ALTER TABLE {{keyspace}}.tables ADD IF NOT EXISTS ttl_cleanup_generation uuid;

INSERT INTO {{keyspace}}.settings (key, value) VALUES ('catalog_version', '0.0.4');
