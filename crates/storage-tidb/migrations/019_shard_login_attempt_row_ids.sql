-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Login attempts are append-only and retained by TiDB native TTL. Without a
-- clustered primary key, TiDB stores rows under implicit `_tidb_rowid`; shard
-- those implicit row IDs so concurrent login failures from multiple frontends
-- are distributed across Regions instead of concentrated on one write Region.

ALTER TABLE login_attempts SHARD_ROW_ID_BITS = 4;

UPDATE settings SET value = '0.0.19' WHERE `key` = 'catalog_version';
