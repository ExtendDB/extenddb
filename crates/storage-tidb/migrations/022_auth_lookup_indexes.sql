-- Copyright 2026 ExtendDB contributors
-- SPDX-License-Identifier: Apache-2.0
-- Policy evaluation runs on every signed DynamoDB request. Keep those hot
-- authorization lookups on TiDB native secondary indexes instead of scanning
-- all group memberships or sessions for an account.

CREATE INDEX IF NOT EXISTS idx_iam_group_members_user
    ON iam_group_members (account_id, user_name, group_name);

CREATE INDEX IF NOT EXISTS idx_iam_sessions_role_session
    ON iam_sessions (account_id, role_name, session_name, expires_at);

UPDATE settings SET value = '0.0.22' WHERE `key` = 'catalog_version';
