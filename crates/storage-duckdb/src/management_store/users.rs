// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! User management, console-password verification, and user tags for
//! `DuckDbCatalogStore`.

use crate::db;
use extenddb_storage::management_store::{OpError, OpResult, UserDetail, UserListEntry};

use crate::catalog_store::DuckDbCatalogStore;
use crate::duckdb_util::{is_fk_violation, is_unique_violation, parse_timestamp};

impl DuckDbCatalogStore {
    pub(crate) async fn create_user_impl(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: Option<&str>,
    ) -> OpResult<()> {
        let user_arn = format!("arn:aws:iam::{account_id}:user/{user_name}");

        let mut tx = self.pool().begin().await.map_err(|e| {
            tracing::error!("create_user begin: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        crate::referential::ensure_account_exists(&mut *tx, account_id).await?;
        db::query(
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
                "Resource": user_arn,
            }]
        })
        .to_string();

        db::query(
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

        tx.commit().await.map_err(|e| {
            tracing::error!("create_user commit: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        Ok(())
    }

    pub(crate) async fn delete_user_impl(&self, account_id: &str, user_name: &str) -> OpResult<()> {
        crate::referential::delete_with_children(
            self.pool(),
            "delete_user",
            async |tx| crate::referential::delete_user_children(tx, account_id, user_name).await,
            "DELETE FROM iam_users WHERE account_id = ? AND user_name = ?",
            &[account_id, user_name],
            "IAM user not found",
        )
        .await
    }

    pub(crate) async fn list_users_impl(&self, account_id: &str) -> OpResult<Vec<UserListEntry>> {
        let rows: Vec<(String, String, String, bool, String)> = db::query_as(
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
        let exists: bool = db::query_scalar(
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

        let keys: Vec<(String, bool)> = db::query_as(
            "SELECT access_key_id, is_active FROM access_keys \
             WHERE account_id = ? AND user_name = ? ORDER BY created_at",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_user_detail keys: {e}")))?;

        let policies: Vec<(String,)> = db::query_as(
            "SELECT policy_name FROM iam_policies \
             WHERE account_id = ? AND principal_type = 'user' AND principal_name = ? \
             ORDER BY policy_name",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_user_detail policies: {e}")))?;

        let tags: Vec<(String, String)> = db::query_as(
            "SELECT tag_key, tag_value FROM iam_user_tags \
             WHERE account_id = ? AND user_name = ? ORDER BY tag_key",
        )
        .bind(account_id)
        .bind(user_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| OpError::Internal(format!("get_user_detail tags: {e}")))?;

        let groups: Vec<(String,)> = db::query_as(
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
        let row: Option<(Option<String>,)> = db::query_as(
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

        let Some((Some(hash),)) = row else {
            // User absent or has no console password.
            return Ok(false);
        };
        let password = password.to_owned();
        let verified = tokio::task::spawn_blocking(move || bcrypt::verify(&password, &hash))
            .await
            .map_err(|e| OpError::Internal(format!("bcrypt task: {e}")))?
            .unwrap_or(false);
        Ok(verified)
    }

    pub(crate) async fn change_user_password_impl(
        &self,
        account_id: &str,
        user_name: &str,
        password_hash: &str,
    ) -> OpResult<()> {
        let result = db::query(
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
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| OpError::Internal(format!("tag_user begin: {e}")))?;
        crate::referential::ensure_user_exists(&mut *tx, account_id, user_name).await?;
        for (k, v) in tags {
            db::query(
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
        let mut tx = self
            .pool()
            .begin()
            .await
            .map_err(|e| OpError::Internal(format!("untag_user begin: {e}")))?;
        for k in tag_keys {
            db::query(
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
        db::query_as(
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
