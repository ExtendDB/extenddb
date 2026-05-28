// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Role management operations for `TidbCatalogStore`.

use extenddb_storage::management_store::{OpError, OpResult, RoleDetail};

use crate::catalog_store::TidbCatalogStore;
use crate::tidb_util::{is_fk_violation, is_unique_violation};

impl TidbCatalogStore {
    pub(crate) async fn create_role_impl(
        &self,
        account_id: &str,
        role_name: &str,
        trust_policy: &serde_json::Value,
    ) -> OpResult<()> {
        let role_arn = format!("arn:aws:iam::{account_id}:role/{role_name}");
        let result = sqlx::query(
            "INSERT INTO iam_roles (account_id, role_name, role_arn, trust_policy) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(account_id)
        .bind(role_name)
        .bind(&role_arn)
        .bind(trust_policy)
        .execute(self.pool())
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(e) if is_unique_violation(&e) => {
                Err(OpError::AlreadyExists("IAM role already exists".to_owned()))
            }
            Err(e) if is_fk_violation(&e) => Err(OpError::NotFound("Account not found".to_owned())),
            Err(e) => {
                tracing::error!("create_role failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }

    pub(crate) async fn delete_role_impl(&self, account_id: &str, role_name: &str) -> OpResult<()> {
        let result = sqlx::query("DELETE FROM iam_roles WHERE account_id = ? AND role_name = ?")
            .bind(account_id)
            .bind(role_name)
            .execute(self.pool())
            .await;
        match result {
            Ok(r) if r.rows_affected() == 0 => {
                Err(OpError::NotFound("IAM role not found".to_owned()))
            }
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::error!("delete_role failed: {e}");
                Err(OpError::Internal("Database error".to_owned()))
            }
        }
    }

    pub(crate) async fn list_roles_impl(
        &self,
        account_id: &str,
    ) -> OpResult<
        Vec<(
            String,
            String,
            String,
            serde_json::Value,
            time::OffsetDateTime,
        )>,
    > {
        sqlx::query_as(
            "SELECT account_id, role_name, role_arn, trust_policy, created_at \
             FROM iam_roles WHERE account_id = ? ORDER BY role_name",
        )
        .bind(account_id)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("list_roles: {e}");
            OpError::Internal("Database error".to_owned())
        })
    }

    pub(crate) async fn get_role_detail_impl(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> OpResult<Option<RoleDetail>> {
        let role: Option<(String, serde_json::Value)> = sqlx::query_as(
            "SELECT role_name, trust_policy FROM iam_roles \
             WHERE account_id = ? AND role_name = ?",
        )
        .bind(account_id)
        .bind(role_name)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_role_detail role: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        let Some((_, trust_policy)) = role else {
            return Ok(None);
        };

        let policies: Vec<(String,)> = sqlx::query_as(
            "SELECT policy_name FROM iam_policies \
             WHERE account_id = ? AND principal_type = 'role' AND principal_name = ? \
             ORDER BY policy_name",
        )
        .bind(account_id)
        .bind(role_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_role_detail policies: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        let tags: Vec<(String, String)> = sqlx::query_as(
            "SELECT tag_key, tag_value FROM iam_role_tags \
             WHERE account_id = ? AND role_name = ? ORDER BY tag_key",
        )
        .bind(account_id)
        .bind(role_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_role_detail tags: {e}");
            OpError::Internal("Database error".to_owned())
        })?;

        Ok(Some(RoleDetail {
            trust_policy,
            policies: policies.into_iter().map(|(n,)| n).collect(),
            tags,
        }))
    }

    pub(crate) async fn get_role_trust_policy_impl(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> OpResult<Option<serde_json::Value>> {
        let row: Option<(serde_json::Value,)> = sqlx::query_as(
            "SELECT trust_policy FROM iam_roles WHERE account_id = ? AND role_name = ?",
        )
        .bind(account_id)
        .bind(role_name)
        .fetch_optional(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("get_role_trust_policy: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        Ok(row.map(|(tp,)| tp))
    }

    // ── Role tags ──────────────────────────────────────────────────

    pub(crate) async fn tag_role_impl(
        &self,
        account_id: &str,
        role_name: &str,
        tags: &[(String, String)],
    ) -> OpResult<()> {
        let mut tx = self.pool().begin().await.map_err(|e| {
            tracing::error!("tag_role begin: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        for (key, value) in tags {
            let result = sqlx::query(
                "INSERT INTO iam_role_tags (account_id, role_name, tag_key, tag_value) \
                 VALUES (?, ?, ?, ?) \
                 ON DUPLICATE KEY UPDATE tag_value = VALUES(tag_value)",
            )
            .bind(account_id)
            .bind(role_name)
            .bind(key)
            .bind(value)
            .execute(&mut *tx)
            .await;
            match result {
                Ok(_) => {}
                Err(e) if is_fk_violation(&e) => {
                    return Err(OpError::NotFound("IAM role not found".to_owned()));
                }
                Err(e) => {
                    tracing::error!("tag_role failed: {e}");
                    return Err(OpError::Internal("Database error".to_owned()));
                }
            }
        }
        tx.commit().await.map_err(|e| {
            tracing::error!("tag_role commit: {e}");
            OpError::Internal("Database error".to_owned())
        })
    }

    pub(crate) async fn untag_role_impl(
        &self,
        account_id: &str,
        role_name: &str,
        tag_keys: &[String],
    ) -> OpResult<()> {
        let mut tx = self.pool().begin().await.map_err(|e| {
            tracing::error!("untag_role begin: {e}");
            OpError::Internal("Database error".to_owned())
        })?;
        for key in tag_keys {
            sqlx::query(
                "DELETE FROM iam_role_tags WHERE account_id = ? AND role_name = ? AND tag_key = ?",
            )
            .bind(account_id)
            .bind(role_name)
            .bind(key)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                tracing::error!("untag_role failed: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
        }
        tx.commit().await.map_err(|e| {
            tracing::error!("untag_role commit: {e}");
            OpError::Internal("Database error".to_owned())
        })
    }

    pub(crate) async fn list_role_tags_impl(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> OpResult<Vec<(String, String)>> {
        sqlx::query_as(
            "SELECT tag_key, tag_value FROM iam_role_tags \
             WHERE account_id = ? AND role_name = ? ORDER BY tag_key",
        )
        .bind(account_id)
        .bind(role_name)
        .fetch_all(self.pool())
        .await
        .map_err(|e| {
            tracing::error!("list_role_tags: {e}");
            OpError::Internal("Database error".to_owned())
        })
    }
}
