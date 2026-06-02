-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Policy evaluation runs on every signed DynamoDB request. Keep those hot
-- user-group authorization joins on a TiDB native secondary index instead of
-- scanning all group memberships for an account. Session authorization uses
-- the native UNIQUE access_key_id index declared by iam_sessions.

CREATE INDEX IF NOT EXISTS idx_iam_group_members_user
    ON iam_group_members (account_id, user_name, group_name);

UPDATE settings SET value = '0.0.22' WHERE `key` = 'catalog_version';
