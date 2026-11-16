-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
--
-- Migration 5, drop_continuous_backups, PostgreSQL catalog database: drops
-- the continuous_backups table. The engine answers the point-in-time recovery
-- operations without storage (#335), so no request reads or writes the table.
-- IF EXISTS makes a second run change nothing.

DROP TABLE IF EXISTS continuous_backups;
