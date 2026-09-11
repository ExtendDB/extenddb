-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Scope TransactWriteItems client tokens to their owning account.

CREATE TABLE IF NOT EXISTS idempotency_tokens_by_account (
    account_id text,
    "token" text,
    fingerprint text,
    created_at timestamp,
    PRIMARY KEY ((account_id), "token")
);

INSERT INTO settings (key, value) VALUES ('catalog_version', '0.0.3');
