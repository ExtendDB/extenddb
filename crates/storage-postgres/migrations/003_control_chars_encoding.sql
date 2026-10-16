-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Stored string encoding changed (catalog version 0.0.4).
--
-- This migration changes no schema. It moves the catalog version because the
-- form of the stored data changed: from this release, strings in the data
-- database and in `backup_items` are written through an order-preserving
-- escape (U+0000 as U+0001 U+0001, U+0001 as U+0001 U+0002), and the data
-- migration `004_escape_control_chars` re-encodes rows written before it.
--
-- A binary from an earlier release knows nothing about the escape and would
-- read an escaped row as literal data, silently. The catalog version check at
-- startup is the one check such a binary already performs, so moving the
-- version is what makes it refuse a migrated deployment instead of serving
-- wrong strings. The data migration itself is tracked in the data database's
-- `schema_history`; see `DATA_CODE_MIGRATIONS` in
-- `crates/storage-postgres/src/migrations.rs` and the upgrade manual.

BEGIN;

UPDATE settings SET value = '0.0.4' WHERE key = 'catalog_version';

COMMIT;
