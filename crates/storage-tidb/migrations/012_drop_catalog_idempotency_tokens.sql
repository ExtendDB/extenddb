-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Idempotency tokens belong in the TiDB data database so the token claim and
-- item writes commit atomically in one transaction. Drop the obsolete catalog
-- copy left by early TiDB catalog schemas.

DROP TABLE IF EXISTS idempotency_tokens;

UPDATE settings SET value = '0.0.12' WHERE `key` = 'catalog_version';
