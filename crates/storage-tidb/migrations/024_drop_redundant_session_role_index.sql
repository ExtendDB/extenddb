-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Session authorization is keyed by the authenticated temporary access key.
-- iam_sessions already has a native UNIQUE access_key_id index, and TiDB TTL
-- owns expired-session retention, so the role/session/time index only adds
-- write amplification.

DROP INDEX IF EXISTS idx_iam_sessions_role_session ON iam_sessions;

UPDATE settings SET value = '0.0.24' WHERE `key` = 'catalog_version';
