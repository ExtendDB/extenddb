// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `AuthorizationStore` implementation for `PostgresCatalogStore`.

use extenddb_storage::authorization_store::{AuthorizationStore, SessionData};
use extenddb_storage::management_store::{OpError, OpResult};
use futures::future::BoxFuture;

use super::catalog_store::PostgresCatalogStore;

impl AuthorizationStore for PostgresCatalogStore {
    fn fetch_user_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let rows: Vec<(serde_json::Value,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_policies \
                 WHERE account_id = $1 AND principal_type = 'user' AND principal_name = $2",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_user_policies: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(rows.into_iter().map(|(v,)| v.to_string()).collect())
        })
    }

    fn fetch_user_group_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let rows: Vec<(serde_json::Value,)> = sqlx::query_as(
                "SELECT p.policy_document \
                 FROM iam_policies p \
                 JOIN iam_group_members gm ON p.account_id = gm.account_id \
                   AND p.principal_type = 'group' \
                   AND p.principal_name = gm.group_name \
                 WHERE gm.account_id = $1 AND gm.user_name = $2",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_user_group_policies: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(rows.into_iter().map(|(v,)| v.to_string()).collect())
        })
    }

    fn fetch_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let row: Option<(serde_json::Value,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_permissions_boundaries \
                 WHERE account_id = $1 AND principal_type = 'user' AND principal_name = $2",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_user_boundary: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(row.map(|(v,)| v.to_string()))
        })
    }

    fn fetch_role_policies(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            let rows: Vec<(serde_json::Value,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_policies \
                 WHERE account_id = $1 AND principal_type = 'role' AND principal_name = $2",
            )
            .bind(&account_id)
            .bind(&role_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_role_policies: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(rows.into_iter().map(|(v,)| v.to_string()).collect())
        })
    }

    fn fetch_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        Box::pin(async move {
            let row: Option<(serde_json::Value,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_permissions_boundaries \
                 WHERE account_id = $1 AND principal_type = 'role' AND principal_name = $2",
            )
            .bind(&account_id)
            .bind(&role_name)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_role_boundary: {e}");
                OpError::Internal("Database error".to_owned())
            })?;
            Ok(row.map(|(v,)| v.to_string()))
        })
    }

    fn fetch_session_data(
        &self,
        account_id: &str,
        role_name: &str,
        session_name: &str,
        access_key_id: &str,
    ) -> BoxFuture<'_, OpResult<Option<SessionData>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let session_name = session_name.to_owned();
        let access_key_id = access_key_id.to_owned();
        Box::pin(async move {
            let row: Option<(Option<serde_json::Value>, Option<serde_json::Value>)> =
                sqlx::query_as(
                    "SELECT session_policy, session_tags FROM iam_sessions \
                     WHERE account_id = $1 AND role_name = $2 AND session_name = $3 \
                     AND access_key_id = $4 \
                     AND expires_at > now()",
                )
                .bind(&account_id)
                .bind(&role_name)
                .bind(&session_name)
                .bind(&access_key_id)
                .fetch_optional(self.pool())
                .await
                .map_err(|e| {
                    tracing::error!("fetch_session_data: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let Some((policy_value, tags_value)) = row else {
                return Ok(None);
            };

            let session_policy = policy_value.map(|v| v.to_string());

            let mut session_tags = Vec::new();
            if let Some(tags_val) = tags_value {
                if let Some(arr) = tags_val.as_array() {
                    for tag in arr {
                        if let (Some(k), Some(v)) = (
                            tag.get("Key").and_then(|k| k.as_str()),
                            tag.get("Value").and_then(|v| v.as_str()),
                        ) {
                            session_tags.push((k.to_owned(), v.to_owned()));
                        }
                    }
                } else if let Some(obj) = tags_val.as_object() {
                    for (k, v) in obj {
                        if let Some(v_str) = v.as_str() {
                            session_tags.push((k.clone(), v_str.to_owned()));
                        }
                    }
                }
            }

            Ok(Some(SessionData {
                session_policy,
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
        Box::pin(async move {
            sqlx::query_as(
                "SELECT tag_key, tag_value FROM iam_user_tags \
                 WHERE account_id = $1 AND user_name = $2",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_all(self.pool())
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
        Box::pin(async move {
            sqlx::query_as(
                "SELECT tag_key, tag_value FROM iam_role_tags \
                 WHERE account_id = $1 AND role_name = $2",
            )
            .bind(&account_id)
            .bind(&role_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| {
                tracing::error!("fetch_role_tags: {e}");
                OpError::Internal("Database error".to_owned())
            })
        })
    }

    fn fetch_resource_tags(&self, arn: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let arn = arn.to_owned();
        Box::pin(async move {
            sqlx::query_as("SELECT tag_key, tag_value FROM tags WHERE resource_arn = $1")
                .bind(&arn)
                .fetch_all(self.pool())
                .await
                .map_err(|e| {
                    tracing::error!("fetch_resource_tags: {e}");
                    OpError::Internal("Database error".to_owned())
                })
        })
    }
}
