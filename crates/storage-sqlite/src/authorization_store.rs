// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `AuthorizationStore` implementation: read-only IAM policy, boundary,
//! session, and tag lookups used by the policy evaluator on every authorized
//! request. Policy documents are stored as JSON text and returned verbatim.

use extenddb_storage::authorization_store::{AuthorizationStore, SessionData};
use extenddb_storage::management_store::{OpError, OpResult};
use futures::future::BoxFuture;

use crate::catalog_store::SqliteCatalogStore;
use crate::sqlite_util::parse_timestamp;

/// Map any query error to a sanitized internal error, logging the detail.
fn db_err(ctx: &str, e: sqlx::Error) -> OpError {
    tracing::error!("{ctx}: {e}");
    OpError::Internal("Database error".to_owned())
}

impl AuthorizationStore for SqliteCatalogStore {
    fn fetch_user_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        Box::pin(async move {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_policies \
                 WHERE account_id = ? AND principal_type = 'user' AND principal_name = ?",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| db_err("fetch_user_policies", e))?;
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
        Box::pin(async move {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT p.policy_document FROM iam_policies p \
                 JOIN iam_group_members m \
                   ON m.account_id = p.account_id AND m.group_name = p.principal_name \
                 WHERE p.account_id = ? AND p.principal_type = 'group' AND m.user_name = ?",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| db_err("fetch_user_group_policies", e))?;
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
        Box::pin(async move {
            let row: Option<(String,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_permissions_boundaries \
                 WHERE account_id = ? AND principal_type = 'user' AND principal_name = ?",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| db_err("fetch_user_boundary", e))?;
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
        Box::pin(async move {
            let rows: Vec<(String,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_policies \
                 WHERE account_id = ? AND principal_type = 'role' AND principal_name = ?",
            )
            .bind(&account_id)
            .bind(&role_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| db_err("fetch_role_policies", e))?;
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
        Box::pin(async move {
            let row: Option<(String,)> = sqlx::query_as(
                "SELECT policy_document FROM iam_permissions_boundaries \
                 WHERE account_id = ? AND principal_type = 'role' AND principal_name = ?",
            )
            .bind(&account_id)
            .bind(&role_name)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| db_err("fetch_role_boundary", e))?;
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
        Box::pin(async move {
            let row: Option<(Option<String>, Option<String>, String)> = sqlx::query_as(
                "SELECT session_policy, session_tags, expires_at FROM iam_sessions \
                 WHERE account_id = ? AND role_name = ? AND session_name = ?",
            )
            .bind(&account_id)
            .bind(&role_name)
            .bind(&session_name)
            .fetch_optional(self.pool())
            .await
            .map_err(|e| db_err("fetch_session_data", e))?;

            let Some((session_policy, tags_json, expires_at)) = row else {
                return Ok(None);
            };

            // Treat an expired session as absent (parity with the Postgres
            // expiry filter). Authentication already rejects expired sessions,
            // so this is defense-in-depth; evaluate it with the same parser
            // lookup_session uses rather than a format-fragile SQL comparison.
            if let Ok(expires) = parse_timestamp(&expires_at)
                && expires < time::OffsetDateTime::now_utc()
            {
                return Ok(None);
            }

            // session_tags is stored as JSON: either an object {key: value} or
            // an array [{"Key": ..., "Value": ...}] (the form AWS SDKs send).
            // Handle both, matching the Postgres backend.
            let session_tags: Vec<(String, String)> = tags_json
                .as_deref()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                .map(|v| {
                    if let Some(arr) = v.as_array() {
                        arr.iter()
                            .filter_map(|tag| {
                                match (
                                    tag.get("Key").and_then(|k| k.as_str()),
                                    tag.get("Value").and_then(|x| x.as_str()),
                                ) {
                                    (Some(k), Some(val)) => Some((k.to_owned(), val.to_owned())),
                                    _ => None,
                                }
                            })
                            .collect()
                    } else if let Some(obj) = v.as_object() {
                        obj.iter()
                            .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_owned())))
                            .collect()
                    } else {
                        Vec::new()
                    }
                })
                .unwrap_or_default();

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
                 WHERE account_id = ? AND user_name = ? ORDER BY tag_key",
            )
            .bind(&account_id)
            .bind(&user_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| db_err("fetch_user_tags", e))
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
                 WHERE account_id = ? AND role_name = ? ORDER BY tag_key",
            )
            .bind(&account_id)
            .bind(&role_name)
            .fetch_all(self.pool())
            .await
            .map_err(|e| db_err("fetch_role_tags", e))
        })
    }

    fn fetch_resource_tags(&self, arn: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let arn = arn.to_owned();
        Box::pin(async move {
            sqlx::query_as(
                "SELECT tag_key, tag_value FROM tags WHERE resource_arn = ? ORDER BY tag_key",
            )
            .bind(&arn)
            .fetch_all(self.pool())
            .await
            .map_err(|e| db_err("fetch_resource_tags", e))
        })
    }
}
