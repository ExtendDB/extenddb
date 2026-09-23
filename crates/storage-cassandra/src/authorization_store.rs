// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! `AuthorizationStore` implementation for `CassandraCatalogStore`.

use cdrs_tokio::types::IntoRustByName;
use extenddb_storage::authorization_store::{AuthorizationStore, SessionData};
use extenddb_storage::management_store::{OpError, OpResult};
use futures::future::BoxFuture;

use super::catalog_store::CassandraCatalogStore;

impl AuthorizationStore for CassandraCatalogStore {
    fn fetch_user_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let session = self.session().clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let query = format!(
                "SELECT policy_document FROM {catalog_keyspace}.iam_policies \
                 WHERE account_id = ? AND principal_type = 'user' AND principal_name = ?"
            );
            let result = session
                .query_with_values(
                    &query,
                    cdrs_tokio::query_values!(account_id.as_str(), user_name.as_str()),
                )
                .await
                .map_err(|e| {
                    tracing::error!("fetch_user_policies: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let body = result.response_body().map_err(|e| {
                tracing::error!("fetch_user_policies response body: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            let rows = body.into_rows().unwrap_or_default();
            let mut policies = Vec::new();
            for row in rows {
                let policy_doc: String = row.get_r_by_name("policy_document").map_err(|e| {
                    tracing::error!("fetch_user_policies parse policy_document: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
                policies.push(policy_doc);
            }
            Ok(policies)
        })
    }

    fn fetch_user_group_policies(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let session = self.session().clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            // First, get groups the user belongs to
            // Uses ALLOW FILTERING for reverse lookup (user -> groups).
            // TODO: Consider denormalizing to iam_user_groups table for scale
            // See notes/performance-considerations.md
            let groups_query = format!(
                "SELECT group_name FROM {catalog_keyspace}.iam_group_members \
                 WHERE account_id = ? AND user_name = ? ALLOW FILTERING"
            );
            let groups_result = session
                .query_with_values(
                    &groups_query,
                    cdrs_tokio::query_values!(account_id.as_str(), user_name.as_str()),
                )
                .await
                .map_err(|e| {
                    tracing::error!("fetch_user_group_policies groups: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let groups_body = groups_result.response_body().map_err(|e| {
                tracing::error!("fetch_user_group_policies groups response body: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            let group_rows = groups_body.into_rows().unwrap_or_default();
            let mut policies = Vec::new();

            // For each group, fetch policies
            for row in group_rows {
                let group_name: String = row.get_r_by_name("group_name").map_err(|e| {
                    tracing::error!("fetch_user_group_policies parse group_name: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

                let policy_query = format!(
                    "SELECT policy_document FROM {catalog_keyspace}.iam_policies \
                     WHERE account_id = ? AND principal_type = 'group' AND principal_name = ?"
                );
                let policy_result = session
                    .query_with_values(
                        &policy_query,
                        cdrs_tokio::query_values!(account_id.as_str(), group_name.as_str()),
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!("fetch_user_group_policies policies: {e}");
                        OpError::Internal("Database error".to_owned())
                    })?;

                let policy_body = policy_result.response_body().map_err(|e| {
                    tracing::error!("fetch_user_group_policies policies response body: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

                if let Some(policy_rows) = policy_body.into_rows() {
                    for policy_row in policy_rows {
                        let policy_doc: String =
                            policy_row.get_r_by_name("policy_document").map_err(|e| {
                                tracing::error!(
                                    "fetch_user_group_policies parse policy_document: {e}"
                                );
                                OpError::Internal("Database error".to_owned())
                            })?;
                        policies.push(policy_doc);
                    }
                }
            }

            Ok(policies)
        })
    }

    fn fetch_user_boundary(
        &self,
        account_id: &str,
        user_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let account_id = account_id.to_owned();
        let user_name = user_name.to_owned();
        let session = self.session().clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let query = format!(
                "SELECT policy_document FROM {catalog_keyspace}.iam_permissions_boundaries \
                 WHERE account_id = ? AND principal_type = 'user' AND principal_name = ?"
            );
            let result = session
                .query_with_values(
                    &query,
                    cdrs_tokio::query_values!(account_id.as_str(), user_name.as_str()),
                )
                .await
                .map_err(|e| {
                    tracing::error!("fetch_user_boundary: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let body = result.response_body().map_err(|e| {
                tracing::error!("fetch_user_boundary response body: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            let rows = body.into_rows().unwrap_or_default();
            if let Some(row) = rows.first() {
                let policy_doc: String = row.get_r_by_name("policy_document").map_err(|e| {
                    tracing::error!("fetch_user_boundary parse policy_document: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
                Ok(Some(policy_doc))
            } else {
                Ok(None)
            }
        })
    }

    fn fetch_role_policies(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<String>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let session = self.session().clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let query = format!(
                "SELECT policy_document FROM {catalog_keyspace}.iam_policies \
                 WHERE account_id = ? AND principal_type = 'role' AND principal_name = ?"
            );
            let rows = crate::cassandra_util::query_rows(
                &session,
                &query,
                cdrs_tokio::query_values!(account_id.as_str(), role_name.as_str()),
                "fetch_role_policies",
            )
            .await?;

            crate::cassandra_util::map_rows(
                rows,
                |row| {
                    crate::cassandra_util::get_column(row, "policy_document", "fetch_role_policies")
                },
                "fetch_role_policies",
            )
        })
    }

    fn fetch_role_boundary(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Option<String>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let session = self.session().clone();
        let catalog_keyspace = self.catalog_keyspace();
        Box::pin(async move {
            let query = format!(
                "SELECT policy_document FROM {catalog_keyspace}.iam_permissions_boundaries \
                 WHERE account_id = ? AND principal_type = 'role' AND principal_name = ?"
            );
            let row = crate::cassandra_util::query_optional(
                &session,
                &query,
                cdrs_tokio::query_values!(account_id.as_str(), role_name.as_str()),
                "fetch_role_boundary",
            )
            .await?;

            match row {
                Some(r) => {
                    let doc = crate::cassandra_util::get_column(
                        &r,
                        "policy_document",
                        "fetch_role_boundary",
                    )?;
                    Ok(Some(doc))
                }
                None => Ok(None),
            }
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
        let catalog_keyspace = self.catalog_keyspace();
        let session = self.session().clone();

        Box::pin(async move {
            let query = format!(
                "SELECT session_policy, session_tags FROM {catalog_keyspace}.iam_sessions \
                 WHERE account_id = ? AND role_name = ? AND session_name = ? ALLOW FILTERING"
            );

            let result = session
                .query_with_values(
                    &query,
                    cdrs_tokio::query_values!(
                        account_id.as_str(),
                        role_name.as_str(),
                        session_name.as_str()
                    ),
                )
                .await
                .map_err(|e| {
                    tracing::error!("fetch_session_data query failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let body = result.response_body().map_err(|e| {
                tracing::error!("fetch_session_data response_body failed: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            let rows = match body.into_rows() {
                Some(r) if !r.is_empty() => r,
                _ => return Ok(None),
            };

            let row = &rows[0];

            // Parse session_policy (optional text)
            let session_policy: Option<String> = row.get_r_by_name("session_policy").ok();

            // Parse session_tags (optional text containing JSON)
            let session_tags_text: Option<String> = row.get_r_by_name("session_tags").ok();
            let mut session_tags = Vec::new();

            if let Some(tags_text) = session_tags_text
                && let Ok(tags_val) = serde_json::from_str::<serde_json::Value>(&tags_text)
            {
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
        let catalog_keyspace = self.catalog_keyspace();
        let session = self.session().clone();

        Box::pin(async move {
            let query = format!(
                "SELECT tag_key, tag_value FROM {catalog_keyspace}.iam_user_tags \
                 WHERE account_id = ? AND user_name = ?"
            );

            let result = session
                .query_with_values(
                    &query,
                    cdrs_tokio::query_values!(account_id.as_str(), user_name.as_str()),
                )
                .await
                .map_err(|e| {
                    tracing::error!("fetch_user_tags query failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let body = result.response_body().map_err(|e| {
                tracing::error!("fetch_user_tags response_body failed: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            let rows = match body.into_rows() {
                Some(r) => r,
                None => return Ok(Vec::new()),
            };

            let mut tags = Vec::new();
            for row in rows {
                let tag_key: String = row.get_r_by_name("tag_key").map_err(|e| {
                    tracing::error!("fetch_user_tags parse tag_key failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
                let tag_value: String = row.get_r_by_name("tag_value").map_err(|e| {
                    tracing::error!("fetch_user_tags parse tag_value failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
                tags.push((tag_key, tag_value));
            }

            Ok(tags)
        })
    }

    fn fetch_role_tags(
        &self,
        account_id: &str,
        role_name: &str,
    ) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let account_id = account_id.to_owned();
        let role_name = role_name.to_owned();
        let catalog_keyspace = self.catalog_keyspace();
        let session = self.session().clone();

        Box::pin(async move {
            let query = format!(
                "SELECT tag_key, tag_value FROM {catalog_keyspace}.iam_role_tags \
                 WHERE account_id = ? AND role_name = ?"
            );

            let result = session
                .query_with_values(
                    &query,
                    cdrs_tokio::query_values!(account_id.as_str(), role_name.as_str()),
                )
                .await
                .map_err(|e| {
                    tracing::error!("fetch_role_tags query failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let body = result.response_body().map_err(|e| {
                tracing::error!("fetch_role_tags response_body failed: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            let rows = match body.into_rows() {
                Some(r) => r,
                None => return Ok(Vec::new()),
            };

            let mut tags = Vec::new();
            for row in rows {
                let tag_key: String = row.get_r_by_name("tag_key").map_err(|e| {
                    tracing::error!("fetch_role_tags parse tag_key failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
                let tag_value: String = row.get_r_by_name("tag_value").map_err(|e| {
                    tracing::error!("fetch_role_tags parse tag_value failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
                tags.push((tag_key, tag_value));
            }

            Ok(tags)
        })
    }

    fn fetch_resource_tags(&self, arn: &str) -> BoxFuture<'_, OpResult<Vec<(String, String)>>> {
        let arn = arn.to_owned();
        let catalog_keyspace = self.catalog_keyspace();
        let session = self.session().clone();

        Box::pin(async move {
            let query = format!(
                "SELECT tag_key, tag_value FROM {catalog_keyspace}.tags WHERE resource_arn = ?"
            );

            let result = session
                .query_with_values(&query, cdrs_tokio::query_values!(arn.as_str()))
                .await
                .map_err(|e| {
                    tracing::error!("fetch_resource_tags query failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;

            let body = result.response_body().map_err(|e| {
                tracing::error!("fetch_resource_tags response_body failed: {e}");
                OpError::Internal("Database error".to_owned())
            })?;

            let rows = match body.into_rows() {
                Some(r) => r,
                None => return Ok(Vec::new()),
            };

            let mut tags = Vec::new();
            for row in rows {
                let tag_key: String = row.get_r_by_name("tag_key").map_err(|e| {
                    tracing::error!("fetch_resource_tags parse tag_key failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
                let tag_value: String = row.get_r_by_name("tag_value").map_err(|e| {
                    tracing::error!("fetch_resource_tags parse tag_value failed: {e}");
                    OpError::Internal("Database error".to_owned())
                })?;
                tags.push((tag_key, tag_value));
            }

            Ok(tags)
        })
    }
}
