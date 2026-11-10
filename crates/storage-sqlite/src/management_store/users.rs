// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! User management, console-password verification, and user tags for
//! `SqliteCatalogStore`.

use extenddb_storage::management_store::{OpError, OpResult, UserDetail, UserListEntry};

use crate::catalog_store::SqliteCatalogStore;
use crate::sqlite_util::{is_fk_violation, is_unique_violation, parse_timestamp};

use super::policies::{begin_write, delete_principal_attachments, finish};

impl SqliteCatalogStore {
    pub(crate) async fn create_user_impl(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: Option<&str>,
    ) -> OpResult<()> {
        let user_arn = format!("arn:aws:iam::{account_id}:user/{user_name}");

        // Seed a default self-service policy so the user can manage its own
        // access keys and password without an administrator attaching one
        // (parity with the PostgreSQL backend).
        let self_service_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": [
                    "iam:CreateAccessKey",
                    "iam:DeleteAccessKey",
                    "iam:ListAccessKeys",
                    "iam:ChangePassword"
                ],
                "Resource": &user_arn,
            }]
        })
        .to_string();

        // D1: every writer holds the write lock, acquired before the
        // transaction begins so the lock never waits on a pool connection.
        let _writer = self.lock_writes().await;
        let mut tx = begin_write(self.pool(), "create_user").await?;
        let result: OpResult<()> = async {
            sqlx::query(
                "INSERT INTO iam_users (account_id, user_name, user_arn, password_hash) \
                 VALUES (?, ?, ?, ?)",
            )
            .bind(account_id)
            .bind(user_name)
            .bind(&user_arn)
            .bind(password_hash)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                if is_unique_violation(&e) {
                    OpError::AlreadyExists("IAM user already exists".to_owned())
                } else if is_fk_violation(&e) {
                    OpError::NotFound("Account not found".to_owned())
                } else {
                    tracing::error!("create_user: {e}");
                    OpError::Internal("Database error".to_owned())
                }
            })?;

            // A user deleted before deletes removed these could have left its
            // inline policies and boundary under this name.
            delete_principal_attachments(&mut tx, account_id, "user", user_name).await?;

            sqlx::query(
                "INSERT INTO iam_policies \
                 (account_id, principal_type, principal_name, policy_name, policy_document) \
                 VALUES (?, 'user', ?, 'SelfServicePolicy', ?) ON CONFLICT DO NOTHING",
            )
            .bind(account_id)
            .bind(user_name)
            .bind(&self_service_policy)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                tracing::error!("seed self-service policy: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(())
        }
        .await;
        finish(tx, result, "create_user").await
    }

    pub(crate) async fn delete_user_impl(&self, account_id: &str, user_name: &str) -> OpResult<()> {
        // D1: every writer holds the write lock.
        let _writer = self.lock_writes().await;
        let mut tx = begin_write(self.pool(), "delete_user").await?;
        let result: OpResult<()> = async {
            // The foreign keys remove the user's access keys, tags and group
            // memberships. Its inline policies and boundary have none.
            let deleted =
                sqlx::query("DELETE FROM iam_users WHERE account_id = ? AND user_name = ?")
                    .bind(account_id)
                    .bind(user_name)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| {
                        tracing::error!("delete_user: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;
            if deleted.rows_affected() == 0 {
                return Err(OpError::NotFound("IAM user not found".to_owned()));
            }
            delete_principal_attachments(&mut tx, account_id, "user", user_name).await
        }
        .await;
        finish(tx, result, "delete_user").await
    }

    pub(crate) async fn list_users_impl(&self, account_id: &str) -> OpResult<Vec<UserListEntry>> {
        let rows: Vec<(String, String, String, bool, String)> = sqlx::query_as(
            "SELECT account_id, user_name, user_arn, (password_hash IS NOT NULL), created_at \
             FROM iam_users WHERE account_id = ? ORDER BY user_name",
        )
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("list_users: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        rows.into_iter()
            .map(|(aid, un, arn, has_pw, ts)| {
                Ok((
                    aid,
                    un,
                    arn,
                    has_pw,
                    parse_timestamp(&ts)
                        .map_err(|e| OpError::Internal(format!("parse created_at: {e}")))?,
                ))
            })
            .collect()
    }

    pub(crate) async fn get_user_detail_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<Option<UserDetail>> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM iam_users WHERE account_id = ? AND user_name = ?)",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_one(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_user_detail exists: {e}")))?;
        if !exists {
            return Ok(None);
        }

        let keys: Vec<(String, bool)> = sqlx::query_as(
            "SELECT access_key_id, is_active FROM access_keys \
             WHERE account_id = ? AND user_name = ? ORDER BY created_at",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_user_detail keys: {e}")))?;

        let policies: Vec<(String,)> = sqlx::query_as(
            "SELECT policy_name FROM iam_policies \
             WHERE account_id = ? AND principal_type = 'user' AND principal_name = ? \
             ORDER BY policy_name",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_user_detail policies: {e}")))?;

        let tags: Vec<(String, String)> = sqlx::query_as(
            "SELECT tag_key, tag_value FROM iam_user_tags \
             WHERE account_id = ? AND user_name = ? ORDER BY tag_key",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_user_detail tags: {e}")))?;

        let groups: Vec<(String,)> = sqlx::query_as(
            "SELECT group_name FROM iam_group_members \
             WHERE account_id = ? AND user_name = ? ORDER BY group_name",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_user_detail groups: {e}")))?;

        Ok(Some(UserDetail {
            keys,
            policies: policies.into_iter().map(|(n,)| n).collect(),
            tags,
            groups: groups.into_iter().map(|(n,)| n).collect(),
        }))
    }

    pub(crate) async fn verify_iam_user_password_impl(
        &self,
        account_id: &str,
        user_name: &str,
        password: &str,
    ) -> OpResult<bool> {
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT password_hash FROM iam_users WHERE account_id = ? AND user_name = ?",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("verify_iam_user_password: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        // `None` when the user is absent or has no console password, which
        // still runs one bcrypt check (see `verify_stored_password`), as a
        // wrong password does.
        let hash = row.and_then(|(hash,)| hash);
        extenddb_storage::password::verify_stored_password(password.to_owned(), hash).await
    }

    pub(crate) async fn change_user_password_impl(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: &str,
    ) -> OpResult<()> {
        // D1: every writer holds the write lock.
        let _writer = self.lock_writes().await;
        let result = sqlx::query(
            "UPDATE iam_users SET password_hash = ? WHERE account_id = ? AND user_name = ?",
        )
        .bind(password_hash)
        .bind(account_id)
        .bind(user_name)
        .execute(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("change_user_password: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        if result.rows_affected() == 0 {
            return Err(OpError::NotFound("IAM user not found".to_owned()));
        }
        Ok(())
    }

    pub(crate) async fn tag_user_impl(
        &self,
        account_id: &str,
        user_name: &str,
        tags: &[(String, String)],
    ) -> OpResult<()> {
        // D1: every writer holds the write lock, acquired before the
        // transaction begins.
        let _writer = self.lock_writes().await;
        let mut tx = self
            .pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| OpError::Internal(format!("tag_user begin: {e}")))?;
        for (k, v) in tags {
            sqlx::query(
                "INSERT INTO iam_user_tags (account_id, user_name, tag_key, tag_value) \
                 VALUES (?, ?, ?, ?) \
                 ON CONFLICT(account_id, user_name, tag_key) DO UPDATE SET tag_value = excluded.tag_value",
            )
            .bind(account_id)
            .bind(user_name)
            .bind(k)
            .bind(v)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                if is_fk_violation(&e) {
                    OpError::NotFound("IAM user not found".to_owned())
                } else {
                    tracing::error!("tag_user: {e}");
                    OpError::Internal("Database error".to_owned())
                }
            })?;
        }
        tx.commit()
            .await
            .map_err(|e| OpError::Internal(format!("tag_user commit: {e}")))?;
        Ok(())
    }

    pub(crate) async fn untag_user_impl(
        &self,
        account_id: &str,
        user_name: &str,
        tag_keys: &[String],
    ) -> OpResult<()> {
        // D1: every writer holds the write lock, acquired before the
        // transaction begins.
        let _writer = self.lock_writes().await;
        let mut tx = self
            .pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| OpError::Internal(format!("untag_user begin: {e}")))?;
        for k in tag_keys {
            sqlx::query(
                "DELETE FROM iam_user_tags WHERE account_id = ? AND user_name = ? AND tag_key = ?",
            )
            .bind(account_id)
            .bind(user_name)
            .bind(k)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                tracing::error!("untag_user: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
        }
        tx.commit()
            .await
            .map_err(|e| OpError::Internal(format!("untag_user commit: {e}")))?;
        Ok(())
    }

    pub(crate) async fn list_user_tags_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<Vec<(String, String)>> {
        sqlx::query_as(
            "SELECT tag_key, tag_value FROM iam_user_tags \
             WHERE account_id = ? AND user_name = ? ORDER BY tag_key",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("list_user_tags: {e}");
            OpError::Internal("Database error".to_owned())
        })
    }
}

#[cfg(test)]
mod tests {
    use extenddb_storage::management_store::ManagementStore;
    use extenddb_storage::password::count_verifications;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;

    const ACCOUNT: &str = "123456789012";

    /// bcrypt's lowest cost, for the test user's hash. It keeps the test fast
    /// and tells a check against a stored hash apart from one against the
    /// fixed hash, which is at the default cost.
    const TEST_COST: u32 = 4;

    async fn store() -> SqliteCatalogStore {
        // One connection keeps the in-memory database alive for the test.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite");
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&pool)
            .await
            .unwrap();
        crate::migrations::apply_for_tests(&pool).await;
        SqliteCatalogStore::new(pool)
    }

    #[tokio::test]
    async fn an_unknown_user_or_one_without_a_password_runs_one_bcrypt_check() {
        let s = store().await;
        s.create_account(ACCOUNT, "acct").await.unwrap();
        let hash = bcrypt::hash("correct horse", TEST_COST).unwrap();
        s.create_user(ACCOUNT, "alice", Some(&hash)).await.unwrap();
        s.create_user(ACCOUNT, "bob", None).await.unwrap();

        let (right, right_costs) =
            count_verifications(s.verify_iam_user_password(ACCOUNT, "alice", "correct horse"))
                .await;
        assert!(right.unwrap());
        assert_eq!(right_costs, [TEST_COST]);

        // The cost of the one check each refused login runs: alice's hash for
        // her wrong password, the fixed hash for the others.
        let cases = [
            (ACCOUNT, "alice", TEST_COST, "wrong password"),
            (
                ACCOUNT,
                "bob",
                bcrypt::DEFAULT_COST,
                "user without a console password",
            ),
            (ACCOUNT, "nobody", bcrypt::DEFAULT_COST, "unknown user"),
            (
                "111122223333",
                "alice",
                bcrypt::DEFAULT_COST,
                "unknown account",
            ),
        ];
        for (account_id, user_name, cost, case) in cases {
            let (verified, costs) = count_verifications(s.verify_iam_user_password(
                account_id,
                user_name,
                "battery staple",
            ))
            .await;
            assert!(!verified.unwrap(), "{case}: verified");
            assert_eq!(costs, [cost], "{case}: bcrypt checks and their cost");
        }
    }
}
