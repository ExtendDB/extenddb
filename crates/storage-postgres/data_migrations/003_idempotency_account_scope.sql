-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Scope idempotency tokens per account.
--
-- A ClientRequestToken is unique per account in Amazon DynamoDB, not globally.
-- The original table keyed on `token` alone, so tokens from different accounts
-- shared one keyspace: an identical token could be treated as a replay of, or a
-- mismatch against, an unrelated account's transaction. Re-key on
-- (account_id, token) to match every other account-scoped table.
--
-- Tokens are an ephemeral dedup cache with a ~10 minute TTL, so the table is
-- recreated rather than back-filled: any in-flight token simply re-registers
-- under its account on the next request. A client retry that straddles this
-- migration loses dedup and may re-apply a non-idempotent transaction, which is
-- acceptable for a short-lived cache. Apply data migrations during the
-- stop / migrate / restart upgrade sequence to bound that window.

BEGIN;

DROP TABLE IF EXISTS idempotency_tokens;

CREATE TABLE idempotency_tokens (
    account_id  TEXT NOT NULL,
    token       TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, token)
);

CREATE INDEX idx_idempotency_tokens_created
    ON idempotency_tokens (created_at);

COMMIT;
