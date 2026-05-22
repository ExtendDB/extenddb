// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Authorization storage for IAM policy lookups (SQLite implementation).

use extenddb_storage::authorization_store::{AuthorizationStore, SessionData};
use extenddb_storage::management_store::{OpError, OpResult};
use futures::future::BoxFuture;

use crate::catalog_store::SqliteCatalogStore;

impl AuthorizationStore for SqliteCatalogStore {
    fn fetch_user_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let pool = self.pool().clone();
        Box::pin(async move {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_policies \
                 WHERE account_id = ? AND principal_type = 'user' AND principal_name = ?",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                tracing::error!("fetch_user_policies: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(rows.into_iter().map(|(d,)| d).collect())
        })
    }

    fn fetch_user_group_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let pool = self.pool().clone();
        Box::pin(async move {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT p.policy_document FROM iam_policies p \
                 JOIN iam_group_members m ON m.account_id = p.account_id AND m.group_name = p.principal_name \
                 WHERE p.account_id = ? AND p.principal_type = 'group' AND m.user_name = ?",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                tracing::error!("fetch_user_group_policies: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(rows.into_iter().map(|(d,)| d).collect())
        })
    }

    fn fetch_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let pool = self.pool().clone();
        Box::pin(async move {
            let row: Option<(String,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_permissions_boundaries \
                 WHERE account_id = ? AND principal_type = 'user' AND principal_name = ?",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_optional(&pool)
            .await
            .map_err(|e| {
                tracing::error!("fetch_user_boundary: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(row.map(|(d,)| d))
        })
    }

    fn fetch_role_policies(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let pool = self.pool().clone();
        Box::pin(async move {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_policies \
                 WHERE account_id = ? AND principal_type = 'role' AND principal_name = ?",
            )
            .bind(&account_id)
            .bind(&role_name)
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                tracing::error!("fetch_role_policies: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(rows.into_iter().map(|(d,)| d).collect())
        })
    }

    fn fetch_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let pool = self.pool().clone();
        Box::pin(async move {
            let row: Option<(String,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_permissions_boundaries \
                 WHERE account_id = ? AND principal_type = 'role' AND principal_name = ?",
            )
            .bind(&account_id)
            .bind(&role_name)
            .fetch_optional(&pool)
            .await
            .map_err(|e| {
                tracing::error!("fetch_role_boundary: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(row.map(|(d,)| d))
        })
    }

    fn fetch_session_data(
        &self,
        account_id: &str,
        role_name: &str,
        session_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<SessionData>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let session_name = session_name.to_owned();
        let pool = self.pool().clone();
        Box::pin(async move {
            let row: Option<(Option<String>, Option<String>)> = sqlx::query_as(
                "SELECT session_policy, session_tags FROM iam_sessions \
                 WHERE account_id = ? AND role_name = ? AND session_name = ?",
            )
            .bind(&account_id)
            .bind(&role_name)
            .bind(&session_name)
            .fetch_optional(&pool)
            .await
            .map_err(|e| {
                tracing::error!("fetch_session_data: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            let Some((policy_json, tags_json)) = row else {
                return Ok(None);
            };

            let session_tags: Vec<(String, String)> = tags_json
                .as_deref()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                .and_then(|v| {
                    v.as_object().map(|obj| {
                        obj.iter()
                            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_owned()))
                            .collect()
                    })
                })
                .unwrap_or_default();

            Ok(Some(SessionData {
                session_policy: policy_json,
                session_tags,
            }))
        })
    }

    fn fetch_user_tags(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let pool = self.pool().clone();
        Box::pin(async move {
            sqlx::query_as(
                "SELECT tag_key, tag_value FROM iam_user_tags \
                 WHERE account_id = ? AND user_name = ? ORDER BY tag_key",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                tracing::error!("fetch_user_tags: {e}");
                OpError::Internal("Database error".to_owned())
            })
        })
    }

    fn fetch_role_tags(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let pool = self.pool().clone();
        Box::pin(async move {
            sqlx::query_as(
                "SELECT tag_key, tag_value FROM iam_role_tags \
                 WHERE account_id = ? AND role_name = ? ORDER BY tag_key",
            )
            .bind(&account_id)
            .bind(&role_name)
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                tracing::error!("fetch_role_tags: {e}");
                OpError::Internal("Database error".to_owned())
            })
        })
    }

    fn fetch_resource_tags(&self, arn: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let arn = arn.to_owned();
        let pool = self.pool().clone();
        Box::pin(async move {
            sqlx::query_as(
                "SELECT tag_key, tag_value FROM tags WHERE resource_arn = ? ORDER BY tag_key",
            )
            .bind(&arn)
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                tracing::error!("fetch_resource_tags: {e}");
                OpError::Internal("Database error".to_owned())
            })
        })
    }
}
