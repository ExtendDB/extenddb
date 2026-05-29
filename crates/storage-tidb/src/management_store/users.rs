// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! User management operations for `TidbCatalogStore`.

use extenddb_storage::management_store::{OpError, OpResult, UserDetail};

use crate::catalog_store::TidbCatalogStore;
use crate::tidb_util::{is_fk_violation, is_unique_violation};

impl TidbCatalogStore {
    pub(crate) async fn create_user_impl(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: Option<&str>,
    ) -> OpResult<()> {
        let user_arn = format!("arn:aws:iam::{account_id}:user/{user_name}");

        let mut tx = self.pool().begin().await.map_err(|e| {
            tracing::error!("create_user begin transaction: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        let result = sqlx::query(
            "INSERT INTO iam_users (account_id, user_name, user_arn, password_hash) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(account_id)
        .bind(user_name)
        .bind(&user_arn)
        .bind(password_hash)
        .execute(&mut *tx)
        .await;

        match result {
            Ok(_) => {}
            Err(e) if is_unique_violation(&e) => {
                return Err(OpError::AlreadyExists("IAM user already exists".to_owned()));
            }
            Err(e) if is_fk_violation(&e) => {
                return Err(OpError::NotFound("Account not found".to_owned()));
            }
            Err(e) => {
                tracing::error!("create_user failed: {e}");
                return Err(OpError::Internal("Database error".to_owned()));
            }
        }

        // Seed default self-service policy.
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
                "Resource": format!("arn:aws:iam::{account_id}:user/{user_name}")
            }]
        });

        if let Err(e) = sqlx::query(
            "INSERT INTO iam_policies (account_id, principal_type, principal_name, policy_name, policy_document) \
             VALUES (?, 'user', ?, 'SelfServicePolicy', ?) ON DUPLICATE KEY UPDATE policy_name = policy_name",
        )
        .bind(account_id)
        .bind(user_name)
        .bind(&self_service_policy)
        .execute(&mut *tx)
        .await
        {
            tracing::error!("seed self-service policy failed: {e}");
            return Err(OpError::Internal("Database error".to_owned()));
        }

        tx.commit().await.map_err(|e| {
            tracing::error!("create_user commit: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        Ok(())
    }

    pub(crate) async fn delete_user_impl(&self, account_id: &str, user_name: &str) -> OpResult<()> {
        let result = sqlx::query("DELETE FROM iam_users WHERE account_id = ? AND user_name = ?")
            .bind(account_id)
            .bind(user_name)
            .execute(self.pool())
            .await;
        match result {
            Ok(r) if r.rows_affected() == 0 => {
                Err(OpError::NotFound("IAM user not found".to_owned()))
            }
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::error!("delete_user failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }

    pub(crate) async fn list_users_impl(
        &self,
        account_id: &str,
    ) -> OpResult<Vec<(String, String, String, bool, time::OffsetDateTime)>> {
        let rows: Vec<(String, String, String, Option<String>, time::OffsetDateTime)> =
            sqlx::query_as(
                "SELECT account_id, user_name, user_arn, password_hash, created_at \
                 FROM iam_users WHERE account_id = ? ORDER BY user_name",
            )
            .bind(account_id)
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("list_users: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
        Ok(rows
            .into_iter()
            .map(|(aid, un, arn, pw, ca)| (aid, un, arn, pw.is_some(), ca))
            .collect())
    }

    pub(crate) async fn get_user_detail_impl(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> OpResult<Option<UserDetail>> {
        let exists: Option<(String,)> = sqlx::query_as(
            "SELECT user_name FROM iam_users WHERE account_id = ? AND user_name = ?",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_user_detail exists: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        if exists.is_none() {
            return Ok(None);
        }

        let keys: Vec<(String, bool)> = sqlx::query_as(
            "SELECT access_key_id, is_active FROM access_keys \
             WHERE account_id = ? AND user_name = ? ORDER BY access_key_id",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_user_detail keys: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        let policies: Vec<(String,)> = sqlx::query_as(
            "SELECT policy_name FROM iam_policies \
             WHERE account_id = ? AND principal_type = 'user' AND principal_name = ? \
             ORDER BY policy_name",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_user_detail policies: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        let tags: Vec<(String, String)> = sqlx::query_as(
            "SELECT tag_key, tag_value FROM iam_user_tags \
             WHERE account_id = ? AND user_name = ? ORDER BY tag_key",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_user_detail tags: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        let groups: Vec<(String,)> = sqlx::query_as(
            "SELECT group_name FROM iam_group_members \
             WHERE account_id = ? AND user_name = ? ORDER BY group_name",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_user_detail groups: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

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
        let row: Option<(String,)> = sqlx::query_as(
            "SELECT password_hash FROM iam_users \
             WHERE account_id = ? AND user_name = ? AND password_hash IS NOT NULL",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("verify_iam_user_password: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        let Some((hash,)) = row else {
            return Ok(false);
        };

        let pw = password.to_owned();
        Ok(
            tokio::task::spawn_blocking(move || bcrypt::verify(pw, &hash).unwrap_or(false))
                .await
                .unwrap_or(false),
        )
    }

    pub(crate) async fn change_user_password_impl(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: &str,
    ) -> OpResult<()> {
        let result = sqlx::query(
            "UPDATE iam_users SET password_hash = ? WHERE account_id = ? AND user_name = ?",
        )
        .bind(password_hash)
        .bind(account_id)
        .bind(user_name)
        .execute(self.pool())
        .await;
        match result {
            Ok(r) if r.rows_affected() == 0 => {
                Err(OpError::NotFound("IAM user not found".to_owned()))
            }
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::error!("change_user_password failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }

    // ── User tags ──────────────────────────────────────────────────

    pub(crate) async fn tag_user_impl(
        &self,
        account_id: &str,
        user_name: &str,
        tags: &[(String, String)],
    ) -> OpResult<()> {
        let mut tx = self.pool().begin().await.map_err(|e| {
            tracing::error!("tag_user begin: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        for (key, value) in tags {
            let result = sqlx::query(
                "INSERT INTO iam_user_tags (account_id, user_name, tag_key, tag_value) \
                 VALUES (?, ?, ?, ?) \
                 ON DUPLICATE KEY UPDATE tag_value = VALUES(tag_value)",
            )
            .bind(account_id)
            .bind(user_name)
            .bind(key)
            .bind(value)
            .execute(&mut *tx)
            .await;
            match result {
                Ok(_) => {}
                Err(e) if is_fk_violation(&e) => {
                    return Err(OpError::NotFound("IAM user not found".to_owned()));
                }
                Err(e) => {
                    tracing::error!("tag_user failed: {e}");
                    return Err(OpError::Internal("Database error".to_owned()));
                }
            }
        }
        tx.commit().await.map_err(|e| {
            tracing::error!("tag_user commit: {e}");
            OpError::Internal("Database error".to_owned())
        })
    }

    pub(crate) async fn untag_user_impl(
        &self,
        account_id: &str,
        user_name: &str,
        tag_keys: &[String],
    ) -> OpResult<()> {
        let mut tx = self.pool().begin().await.map_err(|e| {
            tracing::error!("untag_user begin: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        for key in tag_keys {
            sqlx::query(
                "DELETE FROM iam_user_tags WHERE account_id = ? AND user_name = ? AND tag_key = ?",
            )
            .bind(account_id)
            .bind(user_name)
            .bind(key)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                tracing::error!("untag_user failed: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
        }
        tx.commit().await.map_err(|e| {
            tracing::error!("untag_user commit: {e}");
            OpError::Internal("Database error".to_owned())
        })
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
