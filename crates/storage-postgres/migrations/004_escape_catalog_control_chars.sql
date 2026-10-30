-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Migration 004: catalog attribute names stored through the control
-- character escape (catalog version 0.0.5).
--
-- This migration changes no schema. It moves the catalog version because the
-- stored form of seven catalog columns changed: from this release, the
-- attribute names in tables.key_schema, tables.attribute_definitions,
-- indexes.key_schema, indexes.projection, backups.key_schema,
-- backups.attribute_definitions and backup_definitions.definition are
-- written through an order-preserving escape (U+0000 as U+0001 U+0001,
-- U+0001 as U+0001 U+0002), so a key attribute name may contain U+0000 as it
-- may on DynamoDB. Before this file is applied, the runner re-encodes every
-- stored document that holds a raw U+0001 (crates/storage-postgres/src/
-- catalog_escape.rs), with every other session off the catalog.
--
-- A binary from an earlier release knows nothing about the escape and would
-- read an escaped name as literal data, silently. The catalog version check at
-- startup is the one check such a binary already performs, so moving the
-- version is what makes it refuse a migrated catalog instead of serving wrong
-- names.

BEGIN;

UPDATE settings SET value = '0.0.5' WHERE key = 'catalog_version';

COMMIT;
